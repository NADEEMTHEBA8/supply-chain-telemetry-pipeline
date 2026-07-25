"""
Gold Layer: Machine Supply Risk Analytics Notebook.

Aggregates Silver telemetry into rolling 24h risk scores.
Aligned with Amazon Connect Decisions Canonical Data Model (CDM) site entities.
"""

from pyspark.sql import functions as F

S3_SILVER_PATH = "/mnt/te-supply-chain/delta/silver_telemetry/"
S3_GOLD_RISK_PATH = "/mnt/te-supply-chain/delta/gold_supply_risk/"

silver_telemetry_df = spark.read.format("delta").load(S3_SILVER_PATH)

gold_supply_risk_df = (
    silver_telemetry_df
    .filter(F.col("event_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 1 DAY"))
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
    .save(S3_GOLD_RISK_PATH)
)

display(
    gold_supply_risk_df
    .orderBy(F.col("risk_score").desc())
    .limit(10)
)
