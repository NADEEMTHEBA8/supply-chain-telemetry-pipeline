import logging

import pytest

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark():
    """Instantiates a session-scoped local SparkSession for PySpark unit tests.

    Minimal parallelism (local[2]) avoids multi-threading contention and bypasses
    the computational overhead of initiating a JVM per test case.
    """
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in local environment")

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)

    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("SupplyChainTestingSuite")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()
