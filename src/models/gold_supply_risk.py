"""
Gold Layer: Machine Supply Risk Analytics Notebook.

Aggregates Silver telemetry into rolling 24h risk scores.
Aligned with Amazon Connect Decisions Canonical Data Model (CDM) site entities.
"""

from pyspark.sql import functions as F

try:
    silver_telemetry_df = spark.read.table("default.silver_telemetry")
    silver_telemetry_df.take(1)
except Exception:
    silver_telemetry_df = spark.read.format("delta").load("s3a://te-supply-chain-telemetry-lake/delta/silver_telemetry/")

gold_supply_risk_df = (
    silver_telemetry_df
    .groupBy("machine_id", "plant_id", "event_date")
    .agg(
        F.avg("temperature_celsius").alias("avg_temp_24h"),
        F.max("temperature_celsius").alias("max_temp_24h"),
        F.stddev("temperature_celsius").alias("temp_stddev_24h"),
        F.avg("vibration_hz").alias("avg_vibration_24h"),
        F.max("vibration_hz").alias("max_vibration_24h"),
        F.avg("pressure_bar").alias("avg_pressure_24h"),
        F.sum(F.col("is_fault_event").cast("int")).alias("fault_event_count_24h"),
        F.sum(F.col("is_overheating").cast("int")).alias("overheat_count_24h"),
        F.sum(F.col("is_vibration_anomaly").cast("int")).alias("vibration_anomaly_count_24h"),
        F.count("error_code").alias("error_code_count_24h"),
        F.count("*").alias("total_readings_24h"),
    )
    .withColumn(
        "risk_score",
        F.least(
            F.lit(1.0),
            (
                F.col("fault_event_count_24h") * 0.4 +
                F.col("overheat_count_24h") * 0.3 +
                F.col("vibration_anomaly_count_24h") * 0.3
            ) / F.greatest(F.col("total_readings_24h"), F.lit(1))
        )
    )
    .withColumn("_dbt_loaded_at", F.current_timestamp())
)

(
    gold_supply_risk_df.write
    .format("delta")
    .partitionBy("plant_id", "event_date")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("default.gold_supply_risk")
)

print("✓ Gold Delta Table materialized: default.gold_supply_risk")
display(
    gold_supply_risk_df
    .orderBy(F.col("risk_score").desc())
    .limit(10)
)
