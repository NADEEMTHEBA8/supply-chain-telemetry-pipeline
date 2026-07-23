"""
Databricks Auto Loader — Bronze Ingestion Notebook.

This notebook runs on Databricks Community Edition and reads machine
telemetry JSON events from AWS S3 using the cloudFiles (Auto Loader) format.

Auto Loader provides:
- Exactly-once ingestion semantics via RocksDB checkpoint state
- Automatic schema inference and evolution (rescue mode)
- Efficient incremental file discovery — only processes new files

Production equivalence: In TE Connectivity's architecture, Amazon Kinesis
Firehose delivers the Kinesis stream data directly to S3, which Auto Loader
then picks up. This notebook replicates that exact pattern.

Mount point assumed: /mnt/te-supply-chain → s3://te-supply-chain-telemetry
Configure via: dbutils.fs.mount() (see README Step 2.3)
"""

# COMMAND ----------
# %md
# ## Bronze Layer: Machine Telemetry Ingestion
# Reads raw JSON telemetry from S3 using Databricks Auto Loader.
# Writes to Delta Lake Bronze table with exactly-once guarantees.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Explicit schema prevents Auto Loader from inferring wrong types on first run.
# schemaEvolutionMode="rescue" captures unexpected fields in _rescued_data column
# rather than failing the stream — critical for schema drift from upstream IoT sensors.
TELEMETRY_SCHEMA = StructType([
    StructField("machine_id",           StringType(),    nullable=False),
    StructField("plant_id",             StringType(),    nullable=False),
    StructField("event_timestamp",      StringType(),    nullable=False),  # cast downstream
    StructField("temperature_celsius",  DoubleType(),    nullable=True),
    StructField("vibration_hz",         DoubleType(),    nullable=True),
    StructField("pressure_bar",         DoubleType(),    nullable=True),
    StructField("operational_status",   StringType(),    nullable=True),
    StructField("error_code",           StringType(),    nullable=True),
    StructField("rpm",                  DoubleType(),    nullable=True),
    StructField("power_consumption_kw", DoubleType(),    nullable=True),
])

S3_RAW_PATH        = "/mnt/te-supply-chain/raw/machine-telemetry/"
S3_BRONZE_PATH     = "/mnt/te-supply-chain/delta/bronze_machine_telemetry/"
S3_CHECKPOINT_PATH = "/mnt/te-supply-chain/checkpoints/bronze_machine_telemetry/"

# COMMAND ----------

# Read stream using Databricks Auto Loader (cloudFiles format)
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")    # Never fail on schema drift
    .option("cloudFiles.inferColumnTypes", "false")        # Use explicit schema above
    .schema(TELEMETRY_SCHEMA)
    .load(S3_RAW_PATH)
)

# COMMAND ----------

# Add pipeline metadata columns before writing to Bronze
bronze_df = (
    raw_stream
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file",  input_file_name())
)

# COMMAND ----------

# Write stream to Bronze Delta table on AWS S3
# trigger(availableNow=True) processes all pending files then stops —
# allows this to run on a schedule (e.g., Databricks Workflows every 15 min)
# while retaining stateful exactly-once guarantees of a continuous stream.
(
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", S3_CHECKPOINT_PATH)
    .option("mergeSchema", "true")                         # Allow schema evolution
    .outputMode("append")
    .trigger(availableNow=True)                            # Micro-batch: process & stop
    .start(S3_BRONZE_PATH)
    .awaitTermination()
)

print(f"✓ Bronze ingestion complete → {S3_BRONZE_PATH}")

# COMMAND ----------

# Verify Bronze table (run this cell after the stream completes)
bronze_count = spark.read.format("delta").load(S3_BRONZE_PATH).count()
print(f"Bronze table row count: {bronze_count:,}")
spark.read.format("delta").load(S3_BRONZE_PATH).printSchema()
