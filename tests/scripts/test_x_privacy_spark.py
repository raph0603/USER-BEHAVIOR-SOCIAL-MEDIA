import json
import os
import sys
from pathlib import Path

import pytest


pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PIPELINE_PATH = ROOT / "spark" / "jobs" / "pipeline"
SPARK_JOBS_PATH = ROOT / "spark" / "jobs"
sys.path.insert(0, str(PIPELINE_PATH))
sys.path.insert(0, str(SPARK_JOBS_PATH))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from cleaning import clean_text, prepare_text_for_model
from collector_stream_pipeline import protect_event


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("XPrivacyTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_cleaner_preserves_tokens_hashtags_and_emoji(spark):
    original = (
        "Salut @alice test@example.com +33 6 12 34 56 78 "
        "192.168.1.10 #Data 🚀 https://example.com"
    )
    row = spark.range(1).select(
        clean_text(lit(original)).alias("clean"),
        prepare_text_for_model(clean_text(lit(original))).alias("model"),
    ).collect()[0]

    expected = (
        "Salut <USER> <EMAIL> <PHONE> <IP> #Data 🚀 <URL>"
    )
    assert row["clean"] == expected
    assert row["model"] == "salut <USER> <EMAIL> <PHONE> <IP> #data 🚀 <URL>"


def test_cleaning_is_idempotent(spark):
    cleaned_once = "Salut <USER> <EMAIL> <PHONE> <IP> #Data 🚀 <URL>"
    result = spark.range(1).select(clean_text(lit(cleaned_once)).alias("text")).first()

    assert result["text"] == cleaned_once


def test_x_event_protection_removes_originals_from_bronze_payloads(spark):
    raw_text = "Salut @alice test@example.com https://example.com"
    source_metadata = json.dumps({"x_account": "raph_dev"})
    raw_payload = json.dumps(
        {
            "url": "https://x.com/raph_dev/status/1999999999999999999",
            "raw_text": raw_text,
        }
    )
    frame = spark.range(1).select(
        lit("x").alias("source"),
        lit("1999999999999999999").alias("platform_event_id"),
        lit("raph_dev").alias("user_id"),
        lit("raph_dev").alias("x_account"),
        lit("https://x.com/raph_dev/status/1999999999999999999").alias("url"),
        lit(raw_text).alias("raw_text"),
        lit(raw_text).alias("title"),
        lit(None).cast("string").alias("clean_text"),
        lit(None).cast("string").alias("text_for_model"),
        lit(None).cast("string").alias("error"),
        lit(source_metadata).alias("source_specific_metadata"),
        lit(raw_payload).alias("raw_source_payload"),
    )

    row = protect_event(frame, platform="x", privacy_hash_salt="test-salt").first()
    serialized = json.dumps(row.asDict(recursive=True), ensure_ascii=False)

    assert row["clean_text"] == "Salut <USER> <EMAIL> <URL>"
    assert row["text_for_model"] == "salut <USER> <EMAIL> <URL>"
    assert row["x_account"] != "raph_dev"
    assert row["url"] == "https://x.com/i/status/1999999999999999999"
    assert "raph_dev" not in serialized
    assert "test@example.com" not in serialized
    assert "@alice" not in serialized
    assert '"x_account": "<USER>"' in row["source_specific_metadata"]
    assert "<URL>" in row["raw_source_payload"]
