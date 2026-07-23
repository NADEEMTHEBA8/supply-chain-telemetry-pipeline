"""
Centralised application settings for the Predictive Supply Chain Telemetry Pipeline.

This module is the single source of truth for all environment-injected
configuration. No other module should call os.getenv() directly.

Governed by 12-Factor App — Factor III (Config):
    "The twelve-factor app stores config in environment variables."

Local:      values loaded from .env.local  (gitignored)
Production: values injected by CI/CD secrets / AWS Secrets Manager

Usage:
    from config.settings import settings

    producer = KinesisProducer(stream_name=settings.kinesis_stream_name)
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """
    Typed, validated application configuration.

    pydantic-settings raises a ValidationError at startup if any required
    field is missing from the environment — fast failure over silent degradation.
    """

    model_config = SettingsConfigDict(
        env_file=".env.local",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Amazon Kinesis — Real-Time Telemetry Stream ───────────────────────────
    # Local dev: point to the Kinesis stream provisioned by Terraform
    # Production: Amazon MSK (identical producer API, different bootstrap config)
    kinesis_stream_name: str = Field(
        default="te-machine-telemetry",
        description="Kinesis Data Stream name for machine telemetry events.",
    )
    aws_access_key_id: str = Field(
        default="",
        description="AWS Access Key ID for Kinesis and S3 access.",
    )
    aws_secret_access_key: str = Field(
        default="",
        description="AWS Secret Access Key for Kinesis and S3 access.",
    )
    aws_default_region: str = Field(
        default="us-east-1",
        description="AWS region where Kinesis stream and S3 bucket are provisioned.",
    )

    # ── AWS S3 — Delta Lakehouse Storage ─────────────────────────────────────
    # All Bronze / Silver / Gold Delta tables land here.
    # In Databricks, this is accessed via the /mnt/te-supply-chain mount point.
    s3_bucket_name: str = Field(
        default="te-supply-chain-telemetry-lake",
        description="S3 bucket name for the Delta Lakehouse.",
    )

    # ── PostgreSQL — ERP Reference Data (CDC Source) ──────────────────────────
    # Local: Docker container (postgres:5432)
    # Production equivalent: AWS DMS replicating from SAP/ERP to S3
    pg_host: str = Field(default="127.0.0.1")
    pg_port: int = Field(default=5432)
    pg_database: str = Field(default="supply_chain_db")
    pg_user: str = Field(default="supply_chain_admin")
    pg_password: str = Field(default="changeme_local_only")

    # ── dbt ──────────────────────────────────────────────────────────────────
    dbt_target: str = Field(default="dev")
    dbt_threads: int = Field(default=4)

    # ── Computed Properties ───────────────────────────────────────────────────
    @property
    def pg_jdbc_url(self) -> str:
        """JDBC connection URL for Spark's JDBC writer."""
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @property
    def s3_bronze_path(self) -> str:
        """S3 path for the Bronze Delta table."""
        return f"s3a://{self.s3_bucket_name}/delta/bronze_machine_telemetry/"

    @property
    def s3_silver_path(self) -> str:
        """S3 path for the Silver Delta table."""
        return f"s3a://{self.s3_bucket_name}/delta/silver_telemetry/"

    @property
    def s3_gold_path(self) -> str:
        """S3 path for the Gold supply risk Delta table."""
        return f"s3a://{self.s3_bucket_name}/delta/gold_supply_risk/"

    @property
    def s3_checkpoint_path(self) -> str:
        """S3 path for Databricks Auto Loader checkpoints."""
        return f"s3a://{self.s3_bucket_name}/checkpoints/"

    @field_validator("pg_port", "dbt_threads", mode="before")
    @classmethod
    def coerce_int(cls, v):
        """Accept string integers from env vars."""
        return int(v)


# Singleton — import this, do not instantiate AppSettings yourself.
settings = AppSettings()
