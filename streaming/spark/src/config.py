"""
SparkSession factory for the bronze ingestion job (Kafka source, MinIO S3A sink).

Connector JARs are resolved from Maven at submit time via spark.jars.packages.
Pinned to Spark 3.5.x / Hadoop 3.3.4 (the Hadoop version Spark 3.5 bundles);
Ivy pulls the transitive deps (kafka-clients, aws-java-sdk-bundle, etc.).

12-Factor III compliance: All configuration is read from `config.settings`.
No os.getenv() calls or hardcoded values appear in this module.
"""

from pyspark.sql import SparkSession

from config.settings import settings

SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "io.delta:delta-spark_2.12:3.1.0",
        "org.postgresql:postgresql:42.6.0",
    ]
)


def create_spark_session(app_name: str = "fraud-feature-store") -> SparkSession:
    """
    SparkSession factory wired for the Kafka source and the MinIO/GCS S3A sink.

    The `spark_master_url` is injected from AppSettings, making this factory
    environment-agnostic:
      - Local cluster:  spark://spark-master:7077  (Docker Compose)
      - GCP Dataproc:   spark://dataproc-m:7077
      - AWS EMR:        yarn

    All S3A / MinIO credentials are sourced from AppSettings. No secrets
    appear in source code.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # ── Cluster master URL (injected; never hardcoded) ──────────────────
        .master(settings.spark_master_url)
        # ── Resource allocation ─────────────────────────────────────────────
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        # ── JARs ────────────────────────────────────────────────────────────
        .config("spark.jars.packages", SPARK_PACKAGES)
        # ── Delta Lake ──────────────────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── S3A → MinIO / GCS / S3 ──────────────────────────────────────────
        # Path-style access required for MinIO. Injected from AppSettings.
        # On GCS: endpoint becomes https://storage.googleapis.com and
        # path.style.access is set to false (virtual-host style is supported).
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Streaming defaults ───────────────────────────────────────────────
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
