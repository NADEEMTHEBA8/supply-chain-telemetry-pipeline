"""
Centralised application settings for the Realtime Fraud Feature Store.

This module is the single source of truth for all environment-injected
configuration. No other module should call os.getenv() directly.

Governed by 12-Factor App — Factor III (Config):
    "The twelve-factor app stores config in environment variables."

Local:      values loaded from .env.local  (gitignored)
Production: values injected by Kubernetes Secrets / GCP Secret Manager

Usage:
    from config.settings import settings

    conn = redis.Redis(host=settings.redis_host, port=settings.redis_port)
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """
    Typed, validated application configuration.

    pydantic-settings raises a ValidationError at startup if any field
    without a default is missing from the environment — fast failure over
    silent degradation.
    """

    model_config = SettingsConfigDict(
        # Primary secrets file (local dev). Gitignored.
        env_file=".env.local",
        env_file_encoding="utf-8",
        # Fall back to the OS environment so CI/CD can inject vars directly.
        case_sensitive=False,
        extra="ignore",
    )

    # ── Kafka / Event Streaming ──────────────────────────────────────────────
    # Local: localhost:9092
    # GCP:   pkc-xxxxx.europe-west2.gcp.confluent.cloud:9092
    # AWS:   b-1.msk-cluster.xxxxx.kafka.eu-west-2.amazonaws.com:9092
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka broker bootstrap addresses.",
    )
    kafka_topic_raw: str = Field(
        default="transactions.raw",
        description="Topic that the transaction generator publishes to.",
    )
    kafka_topic_dead_letter: str = Field(
        default="transactions.dead_letter",
        description="Topic for malformed / unparseable events.",
    )

    # ── MinIO / Object Storage (S3-compatible) ───────────────────────────────
    # Local: http://minio:9000  (inside fraud_vpc)
    # GCP:   https://storage.googleapis.com
    # AWS:   https://s3.eu-west-2.amazonaws.com
    minio_endpoint: str = Field(
        default="http://localhost:9000",
        description="S3-compatible object storage endpoint URL.",
    )
    minio_access_key: str = Field(
        default="minioadmin",
        description="S3 access key / GCS HMAC access ID.",
    )
    minio_secret_key: str = Field(
        default="minioadmin",
        description="S3 secret key / GCS HMAC secret.",
    )
    minio_bucket_bronze: str = Field(
        default="bronze",
        description="Bucket name for the Delta Lake bronze layer.",
    )

    # ── PostgreSQL / OLTP Warehouse ──────────────────────────────────────────
    # Local:  host=postgres (Docker DNS), port=5432
    # GCP:    Cloud SQL private IP via VPC connector
    # AWS:    RDS endpoint
    pg_host: str = Field(default="127.0.0.1")
    pg_port: int = Field(default=5434)
    pg_database: str = Field(default="fraud_reference")
    pg_user: str = Field(default="fraud_admin")
    pg_password: str = Field(default="changeme_local_only")

    # ── Redis / Cloud Memorystore ────────────────────────────────────────────
    # Local: redis:6379  (Docker DNS)
    # GCP:   Memorystore private IP (e.g., 10.0.0.5)
    # AWS:   ElastiCache primary endpoint
    redis_host: str = Field(default="127.0.0.1")
    redis_port: int = Field(default=6379)

    # ── Apache Spark ─────────────────────────────────────────────────────────
    # local[2]:            host-side single-process (legacy, removed)
    # spark://spark-master:7077  →  local Docker cluster (this setup)
    # spark://dataproc-m:7077    →  GCP Dataproc
    # yarn                       →  EMR / on-prem Hadoop
    spark_master_url: str = Field(
        default="spark://spark-master:7077",
        description="Spark master URL. Injected to support local cluster, Dataproc, or YARN.",
    )
    spark_driver_memory: str = Field(
        default="2g",
        description="Spark driver memory allocation.",
    )
    spark_executor_memory: str = Field(
        default="1g",
        description="Spark executor memory allocation.",
    )

    # ── API Security ─────────────────────────────────────────────────────────
    # Never hardcoded. Injected via env. In production: rotate via Secrets Manager.
    api_key: str = Field(
        default="sk_test_local_only_change_in_prod",
        description="Feature serving API key. Override with a cryptographically "
        "random secret in prod.",
    )
    # Host/port for the serving API — used by mock_ml_scorer and integration tests.
    # Local:  localhost:8002  (host-side uvicorn process)
    # GCP:    Internal load balancer hostname or Cloud Run service URL
    api_host: str = Field(
        default="localhost",
        description="Feature Store API host.",
    )
    api_port: int = Field(
        default=8002,
        description="Feature Store API port.",
    )

    # ── dbt ──────────────────────────────────────────────────────────────────
    dbt_target: str = Field(default="dev")
    dbt_schema: str = Field(default="silver")
    dbt_threads: int = Field(default=4)

    # ── Computed Properties ──────────────────────────────────────────────────

    @property
    def pg_jdbc_url(self) -> str:
        """JDBC connection URL for Spark's JDBC writer."""
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @property
    def bronze_output_path(self) -> str:
        """S3A path for the Delta Lake bronze layer."""
        return f"s3a://{self.minio_bucket_bronze}/transactions_v2/"

    @property
    def bronze_checkpoint_path(self) -> str:
        """S3A path for Spark Structured Streaming checkpoints."""
        return f"s3a://{self.minio_bucket_bronze}/_checkpoints/bronze_ingest_v2/"

    @property
    def dead_letter_checkpoint_path(self) -> str:
        """S3A path for dead-letter stream checkpoints."""
        return f"s3a://{self.minio_bucket_bronze}/_checkpoints/dead_letter/"

    @field_validator("pg_port", "redis_port", "api_port", "dbt_threads", mode="before")
    @classmethod
    def coerce_int(cls, v):
        """Accept string integers from env vars."""
        return int(v)


# Singleton — import this, do not instantiate AppSettings yourself.
settings = AppSettings()
