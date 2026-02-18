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

# --- 2. DATA PREPARATION ---
# Read silver data
silver_df = spark.read.format("delta").load(silver_path)

# Deduplicate to get the latest snapshot per patient in this specific batch
w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())
latest_silver_df = (
    silver_df
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# --- 3. PATIENT DIMENSION (SCD TYPE 2) ---

# Prepare incoming records with a change-detection hash
incoming_patient = (latest_silver_df
                    .select("patient_id", "gender", "age")
                    .withColumn("effective_from", current_timestamp())
                    .withColumn("_hash", F.sha2(F.concat_ws("||", 
                        F.coalesce(col("gender"), lit("NA")), 
                        F.coalesce(col("age").cast("string"), lit("NA"))), 256))
                   )

# Create table if it doesn't exist
if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    incoming_patient.withColumn("surrogate_key", F.monotonically_increasing_id()) \
                    .withColumn("effective_to", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True)) \
                    .write.format("delta").mode("overwrite").save(gold_dim_patient)

# Load target as DeltaTable for Merge operations
target_patient = DeltaTable.forPath(spark, gold_dim_patient)

# STEP A: EXPIRE OLD RECORDS (The performance fix replacing .collect())
# Update is_current to false for existing records where patient_id matches but attributes (hash) changed
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
# Find rows in incoming that are either brand new OR the updated version of an existing patient
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

# Prepare and deduplicate incoming departments
incoming_dept = (latest_silver_df
                 .select("department", "hospital_id")
                 .dropDuplicates(["department", "hospital_id"])
                 .withColumn("surrogate_key", monotonically_increasing_id())
                )

# Overwrite department dim (Small lookup table)
incoming_dept.write.format("delta").mode("overwrite").save(gold_dim_department)


# --- 5. FACT TABLE ---

# Load current dimensions for joining
dim_patient_current = (spark.read.format("delta").load(gold_dim_patient)
                       .filter(col("is_current") == True)
                       .select(col("surrogate_key").alias("patient_sk"), "patient_id"))

dim_dept_current = (spark.read.format("delta").load(gold_dim_department)
                    .select(col("surrogate_key").alias("department_sk"), "department", "hospital_id"))

# Build enriched Fact
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

# Persist fact table partitioned by date (Optimization for Synapse SQL Pool)
(fact_final.write
    .format("delta")
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("admission_date")
    .save(gold_fact)
)

# --- 6. SANITY CHECKS ---
print(f"Patient Dim (Active) count: {spark.read.format('delta').load(gold_dim_patient).filter('is_current = true').count()}")
print(f"Fact Table rows: {spark.read.format('delta').load(gold_fact).count()}")
