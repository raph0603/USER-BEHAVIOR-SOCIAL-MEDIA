import os
import sys
from datetime import datetime
from pathlib import Path

import pytest


pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

STREAMING_PATH = ROOT / "spark" / "jobs" / "streaming"
SPARK_JOBS_PATH = ROOT / "spark" / "jobs"
sys.path.insert(0, str(STREAMING_PATH))
sys.path.insert(0, str(SPARK_JOBS_PATH))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import kafka_to_iceberg_bronze as bronze


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("BronzeTimestampCompatibilityTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_string_refresh_timestamps_align_to_existing_timestamp_schema(spark):
    target = spark.range(0).select(
        lit(None).cast("string").alias("event_id"),
        lit(None).cast("timestamp").alias("last_metrics_refresh_at"),
        lit(None).cast("timestamp").alias("next_metrics_refresh_at"),
    )
    target.createOrReplaceTempView("bronze_events_timestamp_compat")
    incoming = spark.range(1).select(
        lit("event-1").alias("event_id"),
        lit("2026-07-29T05:00:00Z").alias("last_metrics_refresh_at"),
        lit("2026-07-29T06:00:00Z").alias("next_metrics_refresh_at"),
    )

    original_table = bronze.CURRENT_TABLE
    bronze.CURRENT_TABLE = "bronze_events_timestamp_compat"
    try:
        row = bronze._align_projection_temporal_types(incoming).first()
    finally:
        bronze.CURRENT_TABLE = original_table

    assert isinstance(row["last_metrics_refresh_at"], datetime)
    assert isinstance(row["next_metrics_refresh_at"], datetime)
