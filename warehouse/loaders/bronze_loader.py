"""
Bronze Loader: Delta Lake (MinIO) → Postgres bronze schema.

Bridges the streaming layer output into the warehouse so dbt can read it
as a dbt source. All columns land as TEXT — strong typing happens in the
dbt `stg_transactions` staging model.

Uses Spark's native JDBC writer to prevent OOM errors on large datasets,
and performs a transactional staging table swap to guarantee zero-downtime
table replacement.

Architectural context (warehouse/loaders/):
    This is a loading/EL component, not streaming logic. It lives in the
    warehouse layer so engineers maintaining dbt models can find the source
    population script without navigating Kafka or Spark streaming code.

12-Factor III compliance: All connection strings sourced from AppSettings.
No hardcoded host addresses, ports, credentials, or JDBC URLs.
"""

import logging

import psycopg2
from pyspark.sql.functions import col

from config.settings import settings
from streaming.spark.src.config import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bronze_loader")


def _ensure_bronze_schema(pg_user: str, pg_password: str) -> None:
    """
    Create the bronze schema in Postgres if it does not exist.

    Uses psycopg2 because DDL must complete before Spark writes via JDBC.
    """
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=pg_user,
        password=pg_password,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        logger.info("bronze schema verified.")
    finally:
        conn.close()


def _swap_staging_to_target(pg_user: str, pg_password: str) -> None:
    """
    Atomic swap of bronze.transactions_staging → bronze.transactions.

    Wrapped in an explicit transaction so a failure leaves the previous
    production table untouched.
    """
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=pg_user,
        password=pg_password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute("DROP TABLE IF EXISTS bronze.transactions CASCADE;")
            cur.execute("ALTER TABLE bronze.transactions_staging RENAME TO transactions;")
            cur.execute("COMMIT;")
        logger.info("Atomic table swap complete: bronze.transactions is live.")
    except Exception:
        conn.rollback()
        logger.exception("Table swap failed; rolling back.")
        raise
    finally:
        conn.close()


def main() -> None:
    spark = create_spark_session("bronze-loader")

    # Read the full Delta Lake bronze table from MinIO / GCS / S3.
    logger.info("Reading Delta Lake from: %s", settings.bronze_output_path)
    df = spark.read.format("delta").load(settings.bronze_output_path)

    # _raw_json is kept in bronze Parquet for debugging but is not needed
    # (and is bulky) in the warehouse table.
    df = df.drop("_raw_json")

    # Cast all columns to string to match Postgres TEXT expectations for
    # the raw/bronze layer. Strong typing is deferred to dbt stg_transactions.
    for c in df.columns:
        df = df.withColumn(c, col(c).cast("string"))

    pg_user = settings.pg_user
    pg_password = settings.pg_password
    jdbc_url = settings.pg_jdbc_url
    jdbc_properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver",
    }

    _ensure_bronze_schema(pg_user, pg_password)

    logger.info("Writing to bronze.transactions_staging via JDBC → %s", jdbc_url)
    df.write.jdbc(
        url=jdbc_url,
        table="bronze.transactions_staging",
        mode="overwrite",
        properties=jdbc_properties,
    )

    _swap_staging_to_target(pg_user, pg_password)
    logger.info("Bronze load complete.")
    spark.stop()


if __name__ == "__main__":
    main()
