"""
Databricks Auto Loader — Bronze Ingestion Notebook.

Ingests raw machine telemetry JSON events from AWS S3 using cloudFiles.
Applies explicit StructType schema and writes append-only to Delta Lake.
"""

from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_TELEMETRY_SCHEMA = StructType([
    StructField("machine_id", StringType(), False),
    StructField("plant_id", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("temperature_celsius", DoubleType(), True),
    StructField("vibration_hz", DoubleType(), True),
    StructField("pressure_bar", DoubleType(), True),
    StructField("operational_status", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("power_consumption_kw", DoubleType(), True),
])

S3_RAW_PATH = "/mnt/te-supply-chain/raw/machine-telemetry/"
S3_BRONZE_PATH = "/mnt/te-supply-chain/delta/bronze_machine_telemetry/"
S3_CHECKPOINT_PATH = "/mnt/te-supply-chain/checkpoints/bronze_machine_telemetry/"

unfiltered_telemetry_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.inferColumnTypes", "false")
    .schema(RAW_TELEMETRY_SCHEMA)
    .load(S3_RAW_PATH)
)

bronze_telemetry_df = (
    unfiltered_telemetry_stream
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    .withColumn("event_date", col("event_timestamp").cast("date"))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(
    bronze_telemetry_df.writeStream
    .format("delta")
    .partitionBy("plant_id", "event_date")
    .option("checkpointLocation", S3_CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .start(S3_BRONZE_PATH)
    .awaitTermination()
)
