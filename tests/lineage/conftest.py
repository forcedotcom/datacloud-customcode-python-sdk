from __future__ import annotations

from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="module")
def spark():
    # Arrow and case sensitivity match the SDK's default config.yaml
    session = (
        SparkSession.builder.master("local[1]")
        .appName("lineage-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("OFF")
    yield session
    session.stop()
