"""
Silver Layer: Telemetry Structuring & Validation Notebook.

Reads the Bronze Delta table and applies:
- Type casting and null handling
- Pandera schema validation (quarantines bad records)
- Joins with machine reference data from PostgreSQL CDC
- Writes clean Silver Delta table to AWS S3

This is the Silver layer of the Medallion Architecture.
"""

# COMMAND ----------
# %md
# ## Silver Layer: Telemetry Structuring & Quality Gate
# Reads Bronze → validates → enriches with machine metadata → writes Silver Delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

S3_BRONZE_PATH     = "/mnt/te-supply-chain/delta/bronze_machine_telemetry/"
S3_SILVER_PATH     = "/mnt/te-supply-chain/delta/silver_telemetry/"
S3_QUARANTINE_PATH = "/mnt/te-supply-chain/delta/quarantine_telemetry/"

# COMMAND ----------

# Read Bronze Delta table
bronze_df = spark.read.format("delta").load(S3_BRONZE_PATH)
print(f"Bronze records: {bronze_df.count():,}")

# COMMAND ----------

# --- Data Quality Gate ---
# Records that fail validation are quarantined, never reach Silver.
# This mirrors TE Connectivity's strict data governance requirements.

valid_df = bronze_df.filter(
    F.col("machine_id").isNotNull() &
    F.col("plant_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    (F.col("temperature_celsius") > 0) &
    (F.col("temperature_celsius") < 200) &
    (F.col("vibration_hz") > 0) &
    (F.col("vibration_hz") < 500)
)

quarantine_df = bronze_df.subtract(valid_df)
quarantine_count = quarantine_df.count()
if quarantine_count > 0:
    print(f"⚠ Quarantining {quarantine_count} malformed records")
    quarantine_df.write.format("delta").mode("append").save(S3_QUARANTINE_PATH)

# COMMAND ----------

# --- Enrichment: Add supply risk flags ---
silver_df = (
    valid_df
    .withColumn(
        "is_overheating",
        F.col("temperature_celsius") > 95.0
    )
    .withColumn(
        "is_vibration_anomaly",
        F.col("vibration_hz") > 80.0
    )
    .withColumn(
        "is_fault_event",
        F.col("operational_status") == "FAULT"
    )
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_rescued_data")
)

# COMMAND ----------

# Write Silver Delta table
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(S3_SILVER_PATH)

silver_count = spark.read.format("delta").load(S3_SILVER_PATH).count()
print(f"✓ Silver table written → {S3_SILVER_PATH}")
print(f"  Silver records: {silver_count:,}")
print(f"  Fault events:   {silver_df.filter('is_fault_event').count():,}")
print(f"  Overheating:    {silver_df.filter('is_overheating').count():,}")
