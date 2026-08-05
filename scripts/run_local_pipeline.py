"""
Supply Chain Telemetry Pipeline — PySpark Medallion Pipeline Execution Script.
Executes Bronze (50,000 events), Silver (Quality Gates), and Gold (24h Risk Scores).
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

print("=" * 70)
print("🚀 PREDICTIVE SUPPLY CHAIN TELEMETRY PIPELINE — MEDALLION ENGINE")
print("=" * 70)

# 1. Initialize PySpark with Delta Lake
spark = SparkSession.builder \
    .appName("SupplyChainMedallionEngine") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print(f"✓ PySpark Engine initialized: v{spark.version} with Delta Lake 3.0.0")

# 2. Execute Bronze Ingestion (50,000 Events)
print("\n[STAGE 1/3] Executing Bronze Ingestion (50,000 Machine Events)...")

def generate_production_telemetry(num_events=50000):
    return (
        spark.range(0, num_events)
        .withColumn("machine_id", F.concat(F.lit("MCH_"), F.lpad(((F.col("id") % 50) + 1).cast("string"), 4, "0")))
        .withColumn("plant_id", F.element_at(F.array(F.lit("PLANT_MX_01"), F.lit("PLANT_DE_02"), F.lit("PLANT_CN_03"), F.lit("PLANT_US_05")), ((F.col("id") % 4) + 1).cast("int")))
        .withColumn("event_timestamp", (F.current_timestamp() - (F.col("id") * F.lit(15)).cast("interval second")))
        .withColumn("temperature_celsius", F.round(F.lit(60.0) + (F.col("id") % 45) + (F.rand() * 15.0), 2))
        .withColumn("vibration_hz", F.round(F.lit(25.0) + (F.col("id") % 35) + (F.rand() * 25.0), 2))
        .withColumn("pressure_bar", F.round(F.lit(4.5) + (F.rand() * 2.5), 2))
        .withColumn("operational_status", F.when(F.col("temperature_celsius") > 95.0, F.lit("FAULT")).otherwise(F.lit("RUNNING")))
        .withColumn("error_code", F.when(F.col("temperature_celsius") > 95.0, F.lit("ERR_OVERHEAT")).otherwise(F.lit(None)))
        .withColumn("rpm", F.round(F.lit(1400.0) + (F.rand() * 200.0), 1))
        .withColumn("power_consumption_kw", F.round(F.lit(10.0) + (F.rand() * 5.0), 2))
        .drop("id")
    )

raw_df = generate_production_telemetry(num_events=50000)
bronze_df = (
    raw_df
    .withColumn("event_date", F.col("event_timestamp").cast("date"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("machine_id"))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("default.bronze_machine_telemetry")
bronze_count = spark.read.table("default.bronze_machine_telemetry").count()
print(f"✓ Bronze Delta Table Materialized: default.bronze_machine_telemetry ({bronze_count:,} events)")

# 3. Execute Silver Structuring & Quality Gate
print("\n[STAGE 2/3] Executing Silver Structuring & Quality Gate...")
raw_bronze_df = spark.read.table("default.bronze_machine_telemetry")

valid_telemetry_df = raw_bronze_df.filter(
    F.col("machine_id").isNotNull() &
    F.col("plant_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    (F.col("temperature_celsius") > 0) &
    (F.col("temperature_celsius") < 200) &
    (F.col("vibration_hz") > 0) &
    (F.col("vibration_hz") < 500)
)

quarantined_df = raw_bronze_df.subtract(valid_telemetry_df)
quarantine_count = quarantined_df.count()

enriched_silver_df = (
    valid_telemetry_df
    .withColumn("is_overheating", F.col("temperature_celsius") > 95.0)
    .withColumn("is_vibration_anomaly", F.col("vibration_hz") > 80.0)
    .withColumn("is_fault_event", F.col("operational_status") == "FAULT")
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

enriched_silver_df.write.format("delta").partitionBy("plant_id", "event_date").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.silver_telemetry")
silver_count = spark.read.table("default.silver_telemetry").count()
print(f"✓ Silver Delta Table Materialized: default.silver_telemetry ({silver_count:,} records partitioned by plant_id, event_date)")
print(f"✓ Quarantine Gate: {quarantine_count} malformed records quarantined")

# 4. Execute Gold Machine Risk Aggregations
print("\n[STAGE 3/3] Executing Gold Machine Risk Aggregations (24h Rolling Model)...")
silver_df = spark.read.table("default.silver_telemetry")

gold_df = (
    silver_df
    .groupBy("machine_id", "plant_id", "event_date")
    .agg(
        F.avg("temperature_celsius").alias("avg_temp_24h"),
        F.max("temperature_celsius").alias("max_temp_24h"),
        F.avg("vibration_hz").alias("avg_vibration_24h"),
        F.max("vibration_hz").alias("max_vibration_24h"),
        F.sum(F.col("is_fault_event").cast("int")).alias("fault_event_count_24h"),
        F.sum(F.col("is_overheating").cast("int")).alias("overheat_count_24h"),
        F.sum(F.col("is_vibration_anomaly").cast("int")).alias("vibration_anomaly_count_24h"),
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
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

gold_df.write.format("delta").partitionBy("plant_id", "event_date").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.gold_supply_risk")
gold_count = spark.read.table("default.gold_supply_risk").count()
print(f"✓ Gold Delta Table Materialized: default.gold_supply_risk ({gold_count:,} machine risk rows)")

print("\n" + "=" * 70)
print("🏆 TOP 5 HIGH-RISK FACTORY MACHINES (GOLD LEADERBOARD)")
print("=" * 70)
top_risk = spark.read.table("default.gold_supply_risk").select("machine_id", "plant_id", "risk_score", "fault_event_count_24h", "overheat_count_24h", "avg_temp_24h").orderBy(F.col("risk_score").desc()).limit(5)
top_risk.show(truncate=False)

print("=" * 70)
print("✅ MEDALLION ENGINE PIPELINE COMPLETE!")
print("=" * 70)
