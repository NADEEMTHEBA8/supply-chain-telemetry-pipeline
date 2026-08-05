"""
Silver Layer: Telemetry Quality Gate & Sensor Feature Enrichment Notebook.

Validates Bronze telemetry against boundary constraints, quarantines malformed
readings, and computes operational status flags before persisting to Silver Delta.
"""

from pyspark.sql import functions as F

try:
    raw_bronze_df = spark.read.table("default.bronze_machine_telemetry")
except Exception:
    raw_bronze_df = spark.read.format("delta").load("s3a://te-supply-chain-telemetry-lake/delta/bronze_machine_telemetry/")

valid_telemetry_df = raw_bronze_df.filter(
    F.col("machine_id").isNotNull() &
    F.col("plant_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    (F.col("temperature_celsius") > 0) &
    (F.col("temperature_celsius") < 200) &
    (F.col("vibration_hz") > 0) &
    (F.col("vibration_hz") < 500)
)

quarantined_telemetry_df = raw_bronze_df.subtract(valid_telemetry_df)
if quarantined_telemetry_df.count() > 0:
    try:
        quarantined_telemetry_df.write.format("delta").mode("append").saveAsTable("default.quarantine_telemetry")
    except Exception:
        pass

enriched_silver_df = (
    valid_telemetry_df
    .withColumn("is_overheating", F.col("temperature_celsius") > 95.0)
    .withColumn("is_vibration_anomaly", F.col("vibration_hz") > 80.0)
    .withColumn("is_fault_event", F.col("operational_status") == "FAULT")
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_rescued_data")
)

(
    enriched_silver_df.write
    .format("delta")
    .partitionBy("plant_id", "event_date")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("default.silver_telemetry")
)
print("✓ Silver Delta Table materialized: default.silver_telemetry")
