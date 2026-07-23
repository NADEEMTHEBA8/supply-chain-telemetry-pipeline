"""
Gold Layer: Supply Risk Aggregation Notebook.

Reads Silver Delta table and produces Gold supply risk metrics
designed to feed into Amazon Connect Decisions and Power BI dashboards.

The gold_supply_risk table maps directly to the Amazon Connect Decisions
Canonical Data Model (CDM) 'site' and 'product' entities, enabling
the supply planning AI to proactively prevent line stoppages.

This is the exact output layer used in TE Connectivity's data platform.
"""

# COMMAND ----------
# %md
# ## Gold Layer: Supply Risk Metrics
# Aggregates Silver telemetry into rolling 24h machine risk scores.
# Maps to Amazon Connect Decisions CDM site/product entities.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

S3_SILVER_PATH = "/mnt/te-supply-chain/delta/silver_telemetry/"
S3_GOLD_RISK   = "/mnt/te-supply-chain/delta/gold_supply_risk/"
S3_GOLD_INV    = "/mnt/te-supply-chain/delta/gold_inventory_health/"

# COMMAND ----------

silver_df = spark.read.format("delta").load(S3_SILVER_PATH)

# --- Gold: Supply Risk (one row per machine per day) ---
# Rolling 24h window aggregations — detects machines approaching failure
# before catastrophic line stoppages occur.
gold_risk_df = (
    silver_df
    .filter(
        F.col("event_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 1 DAY")
    )
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
        # Composite risk score: normalised weighted sum of anomaly signals
        # High risk (> 0.7) triggers Amazon Connect Decisions replenishment alert
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

# COMMAND ----------

gold_risk_df.write.format("delta").mode("overwrite").save(S3_GOLD_RISK)

print(f"✓ Gold supply risk table written → {S3_GOLD_RISK}")
print(f"  Machines tracked: {gold_risk_df.select('machine_id').distinct().count()}")
print(f"  High risk machines (score > 0.7): "
      f"{gold_risk_df.filter('risk_score > 0.7').count()}")

# COMMAND ----------
# Show top 10 at-risk machines — this is the data Amazon Connect Decisions consumes
display(
    gold_risk_df
    .orderBy(F.col("risk_score").desc())
    .select(
        "machine_id", "plant_id", "risk_score",
        "avg_temp_24h", "fault_event_count_24h", "error_code_count_24h"
    )
    .limit(10)
)
