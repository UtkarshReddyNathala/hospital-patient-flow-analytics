from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce, monotonically_increasing_id
from delta.tables import DeltaTable
from pyspark.sql import Window

# --- 1. ADLS CONFIGURATION ---
spark.conf.set(
    "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
    "<<Storage_Account_access_key>>"
)

# Paths
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_dim_patient = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_dim_department = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_fact = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

# --- 2. THE INCREMENTAL PROCESSING FUNCTION ---
def process_incremental_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # --- ADVANCEMENT: BUSINESS QUALITY GATE ---
    # Filter out medically impossible records before they hit Dims or Facts
    # 1. Discharge cannot be before admission
    # 2. Age must be in a humanly possible range
    # 3. Admission cannot be in the future (greater than current time)
    valid_batch_df = batch_df.filter(
        (col("discharge_time") >= col("admission_time")) & 
        (col("age").between(0, 120)) &
        (col("admission_time") <= current_timestamp()) &
        (col("patient_id").isNotNull())
    )

    # Log dropped records for visibility
    dropped_count = batch_df.count() - valid_batch_df.count()
    if dropped_count > 0:
        print(f"BATCH {batch_id}: Dropped {dropped_count} invalid records via Quality Gate.")

    # Deduplicate the valid batch to get the latest snapshot per patient
    w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())
    latest_silver_df = (
        valid_batch_df
        .withColumn("row_num", F.row_number().over(w))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # --- 3. PATIENT DIMENSION (SCD TYPE 2) ---
    incoming_patient = (latest_silver_df
                        .select("patient_id", "gender", "age")
                        .withColumn("effective_from", current_timestamp())
                        .withColumn("_hash", F.sha2(F.concat_ws("||", 
                            F.coalesce(col("gender"), lit("NA")), 
                            F.coalesce(col("age").cast("string"), lit("NA"))), 256))
                       )

    if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
        incoming_patient.withColumn("surrogate_key", F.monotonically_increasing_id()) \
                        .withColumn("effective_to", lit(None).cast("timestamp")) \
                        .withColumn("is_current", lit(True)) \
                        .write.format("delta").mode("overwrite").save(gold_dim_patient)

    target_patient = DeltaTable.forPath(spark, gold_dim_patient)

    # STEP A: EXPIRE OLD RECORDS
    target_patient.alias("t").merge(
        source = incoming_patient.alias("s"),
        condition = "t.patient_id = s.patient_id AND t.is_current = true"
    ).whenMatchedUpdate(
        condition = "t._hash <> s._hash",
        set = {
            "is_current": "false",
            "effective_to": "current_timestamp()"
        }
    ).execute()

    # STEP B: INSERT NEW RECORDS
    new_records_to_insert = incoming_patient.alias("s").join(
        target_patient.toDF().alias("t"),
        (F.col("s.patient_id") == F.col("t.patient_id")) & (F.col("t.is_current") == F.lit(True)),
        "left_anti"
    ).withColumn("surrogate_key", F.monotonically_increasing_id()) \
     .withColumn("effective_to", F.lit(None).cast("timestamp")) \
     .withColumn("is_current", F.lit(True))

    if new_records_to_insert.count() > 0:
        new_records_to_insert.write.format("delta").mode("append").save(gold_dim_patient)

    # --- 4. DEPARTMENT DIMENSION ---
    incoming_dept = (latest_silver_df
                     .select("department", "hospital_id")
                     .dropDuplicates(["department", "hospital_id"])
                     .withColumn("surrogate_key", monotonically_increasing_id())
                    )
    incoming_dept.write.format("delta").mode("overwrite").save(gold_dim_department)

    # --- 5. FACT TABLE ---
    dim_patient_current = (spark.read.format("delta").load(gold_dim_patient)
                           .filter(col("is_current") == True)
                           .select(col("surrogate_key").alias("patient_sk"), "patient_id"))

    dim_dept_current = (spark.read.format("delta").load(gold_dim_department)
                        .select(col("surrogate_key").alias("department_sk"), "department", "hospital_id"))

    fact_final = (latest_silver_df
                  .withColumn("admission_date", F.to_date("admission_time"))
                  .join(dim_patient_current, on="patient_id", how="left")
                  .join(dim_dept_current, on=["department", "hospital_id"], how="left")
                  .withColumn("length_of_stay_hours", 
                      (F.unix_timestamp(col("discharge_time")) - F.unix_timestamp(col("admission_time"))) / 3600.0)
                  .withColumn("is_currently_admitted", 
                      F.when(col("discharge_time") > current_timestamp(), lit(True)).otherwise(lit(False)))
                  .withColumn("event_ingestion_time", current_timestamp())
                  .select(
                      F.monotonically_increasing_id().alias("fact_id"),
                      "patient_sk",
                      "department_sk",
                      "admission_time",
                      "discharge_time",
                      "admission_date",
                      "length_of_stay_hours",
                      "is_currently_admitted",
                      "bed_id",
                      "event_ingestion_time"
                  )
                 )

    (fact_final.write
        .format("delta")
        .mode("append")
        .partitionBy("admission_date")
        .save(gold_fact)
    )

# --- 6. TRIGGER THE INCREMENTAL LOAD ---
(spark.readStream
    .format("delta")
    .option("readChangeData", "true") 
    .load(silver_path)
    .writeStream
    .foreachBatch(process_incremental_batch)
    .option("checkpointLocation", gold_fact + "/_checkpoints/gold_logic_v2")
    .trigger(availableNow=True) 
    .start()
    .awaitTermination()
)

# --- 7. SANITY CHECKS ---
print(f"Patient Dim (Active) count: {spark.read.format('delta').load(gold_dim_patient).filter('is_current = true').count()}")
print(f"Fact Table rows: {spark.read.format('delta').load(gold_fact).count()}")
