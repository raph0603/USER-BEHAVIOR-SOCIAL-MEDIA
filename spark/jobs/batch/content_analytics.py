"""
Spark batch job: derive entity-level analytics tables from silver events.

The existing ``lakehouse.silver.events`` table remains the monitoring-friendly
event stream. This job materializes business entities that are easier to follow
over time: main contents, interactions, append-only engagement observations,
YouTube transcripts, content-level aggregates, and user evolution.
"""

from __future__ import annotations

import os
from typing import Any

try:
    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql.functions import (
        avg,
        col,
        concat_ws,
        coalesce,
        count,
        countDistinct,
        first,
        get_json_object,
        length,
        lit,
        lower,
        max as spark_max,
        regexp_extract,
        regexp_replace,
        row_number,
        sha2,
        size,
        split,
        sum as spark_sum,
        to_date,
        to_json,
        to_timestamp,
        trim,
        unix_timestamp,
        when,
    )
except ModuleNotFoundError as exc:
    if not (exc.name or "").startswith("pyspark"):
        raise
    DataFrame = Any  # type: ignore[misc,assignment]
    SparkSession = Any  # type: ignore[misc,assignment]
    Window = None  # type: ignore[misc,assignment]


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _build_spark(app_name: str, warehouse: str) -> SparkSession:
    minio_endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ROOT_USER", "minioadmin")
    secret_key = _env("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


CONTENT_TABLE = "lakehouse.silver.contents"
INTERACTION_TABLE = "lakehouse.silver.interactions"
SNAPSHOT_TABLE = "lakehouse.silver.engagement_snapshots"
TRANSCRIPT_TABLE = "lakehouse.silver.transcripts"
CONTENT_STATS_TABLE = "lakehouse.gold.content_stats"
USER_EVOLUTION_TABLE = "lakehouse.gold.user_evolution"


CREATE_CONTENTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.contents (
  content_id STRING,
  root_content_id STRING,
  source STRING,
  platform_content_id STRING,
  content_type STRING,
  url STRING,
  title STRING,
  text STRING,
  author_id_hash STRING,
  created_at TIMESTAMP,
  event_date DATE,
  subreddit STRING,
  subreddit_title STRING,
  subreddit_description STRING,
  subreddit_created_at STRING,
  subreddit_visibility STRING,
  subreddit_weekly_visitors BIGINT,
  subreddit_weekly_contributions BIGINT,
  subreddit_member_count BIGINT,
  x_account STRING,
  youtube_channel_id STRING,
  youtube_channel_name STRING,
  language STRING,
  conversation_id STRING,
  collection_status STRING,
  metadata_status STRING,
  transcript_status STRING,
  comments_status STRING,
  last_discovered_at TIMESTAMP,
  last_enriched_at TIMESTAMP,
  canonical_metadata STRING,
  source_specific_metadata STRING,
  raw_text STRING,
  clean_text STRING,
  text_for_model STRING,
  thumbnail_url STRING,
  event_id STRING,
  observation_id STRING,
  observed_at TIMESTAMP,
  producer_name STRING,
  producer_run_id STRING,
  payload_fingerprint STRING,
  collection_method STRING,
  api_endpoint STRING,
  provenance_json STRING,
  coverage_json STRING,
  like_count_available BOOLEAN,
  view_count_available BOOLEAN,
  comment_count_available BOOLEAN,
  reply_count_available BOOLEAN,
  retweet_count_available BOOLEAN,
  bookmark_count_available BOOLEAN,
  score_available BOOLEAN,
  follower_count_available BOOLEAN,
  subscriber_count_available BOOLEAN,
  subreddit_member_count_available BOOLEAN,
  metadata_available BOOLEAN,
  transcript_available BOOLEAN,
  comments_available BOOLEAN
)
USING iceberg
PARTITIONED BY (event_date)
"""


CREATE_INTERACTIONS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.interactions (
  interaction_id STRING,
  source STRING,
  platform_interaction_id STRING,
  parent_content_id STRING,
  root_content_id STRING,
  parent_interaction_id STRING,
  conversation_id STRING,
  interaction_type STRING,
  relation_type STRING,
  depth INT,
  position_in_thread BIGINT,
  author_id_hash STRING,
  text STRING,
  created_at TIMESTAMP,
  event_date DATE,
  score BIGINT,
  like_count BIGINT,
  reply_count BIGINT,
  collection_status STRING,
  metadata_status STRING,
  canonical_metadata STRING,
  source_specific_metadata STRING,
  raw_text STRING,
  clean_text STRING,
  text_for_model STRING,
  event_id STRING,
  observation_id STRING,
  observed_at TIMESTAMP,
  producer_name STRING,
  producer_run_id STRING,
  payload_fingerprint STRING,
  collection_method STRING,
  api_endpoint STRING,
  provenance_json STRING,
  coverage_json STRING,
  like_count_available BOOLEAN,
  view_count_available BOOLEAN,
  comment_count_available BOOLEAN,
  reply_count_available BOOLEAN,
  retweet_count_available BOOLEAN,
  bookmark_count_available BOOLEAN,
  score_available BOOLEAN,
  follower_count_available BOOLEAN,
  subscriber_count_available BOOLEAN,
  subreddit_member_count_available BOOLEAN,
  metadata_available BOOLEAN,
  transcript_available BOOLEAN,
  comments_available BOOLEAN
)
USING iceberg
PARTITIONED BY (event_date)
"""


CREATE_SNAPSHOTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.engagement_snapshots (
  event_id STRING,
  observation_id STRING,
  content_id STRING,
  source STRING,
  platform_event_id STRING,
  user_id STRING,
  url STRING,
  created_at TIMESTAMP,
  observed_at TIMESTAMP,
  snapshot_at TIMESTAMP,
  age_minutes BIGINT,
  event_date DATE,
  like_count BIGINT,
  view_count BIGINT,
  comment_count BIGINT,
  reply_count BIGINT,
  retweet_count BIGINT,
  bookmark_count BIGINT,
  score BIGINT,
  follower_count BIGINT,
  subscriber_count BIGINT,
  subreddit_member_count BIGINT,
  producer_name STRING,
  producer_run_id STRING,
  payload_fingerprint STRING,
  collection_method STRING,
  api_endpoint STRING,
  provenance_json STRING,
  coverage_json STRING,
  like_count_available BOOLEAN,
  view_count_available BOOLEAN,
  comment_count_available BOOLEAN,
  reply_count_available BOOLEAN,
  retweet_count_available BOOLEAN,
  bookmark_count_available BOOLEAN,
  score_available BOOLEAN,
  follower_count_available BOOLEAN,
  subscriber_count_available BOOLEAN,
  subreddit_member_count_available BOOLEAN,
  metadata_available BOOLEAN,
  transcript_available BOOLEAN,
  comments_available BOOLEAN,
  snapshot_date DATE
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'write.metadata.delete-after-commit.enabled' = 'false'
)
"""


CREATE_TRANSCRIPTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.transcripts (
  video_id STRING,
  content_id STRING,
  transcript_status STRING,
  transcript_lifecycle_status STRING,
  requested_language STRING,
  requested_language_code STRING,
  obtained_language STRING,
  obtained_language_code STRING,
  language STRING,
  language_code STRING,
  transcript_text STRING,
  segments_json STRING,
  duration_seconds DOUBLE,
  covered_duration_seconds DOUBLE,
  word_count BIGINT,
  segment_count BIGINT,
  available_languages_json STRING,
  has_auto_captions BOOLEAN,
  is_generated BOOLEAN,
  is_translated BOOLEAN,
  generation_type STRING,
  source_language STRING,
  source_language_code STRING,
  transcript_source STRING,
  provider STRING,
  selection_strategy STRING,
  error_code STRING,
  error_message STRING,
  attempt_count BIGINT,
  last_attempt_at TIMESTAMP,
  next_attempt_at TIMESTAMP,
  collected_at TIMESTAMP,
  recovered_at TIMESTAMP,
  content_version STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  event_date DATE
)
USING iceberg
PARTITIONED BY (event_date)
"""


CREATE_CONTENT_STATS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.gold.content_stats (
  content_id STRING,
  source STRING,
  content_type STRING,
  title STRING,
  url STRING,
  author_id_hash STRING,
  created_at TIMESTAMP,
  event_date DATE,
  interaction_count BIGINT,
  unique_interacting_users BIGINT,
  avg_interaction_length DOUBLE,
  total_score BIGINT,
  latest_view_count BIGINT,
  latest_like_count BIGINT,
  latest_comment_count BIGINT,
  latest_reply_count BIGINT,
  latest_retweet_count BIGINT,
  latest_bookmark_count BIGINT,
  latest_snapshot_at TIMESTAMP,
  latest_snapshot_observation_id STRING,
  latest_snapshot_producer_name STRING,
  latest_snapshot_producer_run_id STRING,
  latest_snapshot_collection_method STRING,
  latest_snapshot_api_endpoint STRING,
  latest_snapshot_provenance_json STRING,
  latest_snapshot_coverage_json STRING,
  latest_view_count_available BOOLEAN,
  latest_like_count_available BOOLEAN,
  latest_comment_count_available BOOLEAN,
  latest_reply_count_available BOOLEAN,
  latest_retweet_count_available BOOLEAN,
  latest_bookmark_count_available BOOLEAN,
  last_discovered_at TIMESTAMP,
  last_enriched_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
"""


CREATE_USER_EVOLUTION_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.gold.user_evolution (
  user_id_hash STRING,
  source STRING,
  event_date DATE,
  contents_created BIGINT,
  interactions_created BIGINT,
  distinct_contents_touched BIGINT,
  distinct_subreddits BIGINT,
  distinct_youtube_channels BIGINT,
  distinct_conversations BIGINT,
  avg_text_length DOUBLE,
  question_count BIGINT
)
USING iceberg
PARTITIONED BY (event_date)
"""


PROVENANCE_COLUMNS = [
    "event_id",
    "observation_id",
    "observed_at",
    "producer_name",
    "producer_run_id",
    "payload_fingerprint",
    "collection_method",
    "api_endpoint",
    "provenance_json",
    "coverage_json",
    "like_count_available",
    "view_count_available",
    "comment_count_available",
    "reply_count_available",
    "retweet_count_available",
    "bookmark_count_available",
    "score_available",
    "follower_count_available",
    "subscriber_count_available",
    "subreddit_member_count_available",
    "metadata_available",
    "transcript_available",
    "comments_available",
]

PROVENANCE_COLUMN_TYPES = {
    column: (
        "TIMESTAMP"
        if column == "observed_at"
        else "BOOLEAN"
        if column.endswith("_available")
        else "STRING"
    )
    for column in PROVENANCE_COLUMNS
}


CONTENT_COLUMNS = [
    "content_id",
    "root_content_id",
    "source",
    "platform_content_id",
    "content_type",
    "url",
    "title",
    "text",
    "author_id_hash",
    "created_at",
    "event_date",
    "subreddit",
    "subreddit_title",
    "subreddit_description",
    "subreddit_created_at",
    "subreddit_visibility",
    "subreddit_weekly_visitors",
    "subreddit_weekly_contributions",
    "subreddit_member_count",
    "x_account",
    "youtube_channel_id",
    "youtube_channel_name",
    "language",
    "conversation_id",
    "collection_status",
    "metadata_status",
    "transcript_status",
    "comments_status",
    "last_discovered_at",
    "last_enriched_at",
    "canonical_metadata",
    "source_specific_metadata",
    "raw_text",
    "clean_text",
    "text_for_model",
    "thumbnail_url",
    *PROVENANCE_COLUMNS,
]

INTERACTION_COLUMNS = [
    "interaction_id",
    "source",
    "platform_interaction_id",
    "parent_content_id",
    "root_content_id",
    "parent_interaction_id",
    "conversation_id",
    "interaction_type",
    "relation_type",
    "depth",
    "position_in_thread",
    "author_id_hash",
    "text",
    "created_at",
    "event_date",
    "score",
    "like_count",
    "reply_count",
    "collection_status",
    "metadata_status",
    "canonical_metadata",
    "source_specific_metadata",
    "raw_text",
    "clean_text",
    "text_for_model",
    *PROVENANCE_COLUMNS,
]

SNAPSHOT_COLUMNS = [
    "event_id",
    "observation_id",
    "content_id",
    "source",
    "platform_event_id",
    "user_id",
    "url",
    "created_at",
    "observed_at",
    "snapshot_at",
    "age_minutes",
    "event_date",
    "view_count",
    "like_count",
    "comment_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "score",
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
    *[
        column
        for column in PROVENANCE_COLUMNS
        if column not in {"event_id", "observation_id", "observed_at"}
    ],
    "snapshot_date",
]

TRANSCRIPT_COLUMNS = [
    "video_id",
    "content_id",
    "transcript_status",
    "transcript_lifecycle_status",
    "requested_language",
    "requested_language_code",
    "obtained_language",
    "obtained_language_code",
    "language",
    "language_code",
    "transcript_text",
    "segments_json",
    "duration_seconds",
    "covered_duration_seconds",
    "word_count",
    "segment_count",
    "available_languages_json",
    "has_auto_captions",
    "is_generated",
    "is_translated",
    "generation_type",
    "source_language",
    "source_language_code",
    "transcript_source",
    "provider",
    "selection_strategy",
    "error_code",
    "error_message",
    "attempt_count",
    "last_attempt_at",
    "next_attempt_at",
    "collected_at",
    "recovered_at",
    "content_version",
    "created_at",
    "updated_at",
    "event_date",
]

CONTENT_STATS_COLUMNS = [
    "content_id",
    "source",
    "content_type",
    "title",
    "url",
    "author_id_hash",
    "created_at",
    "event_date",
    "interaction_count",
    "unique_interacting_users",
    "avg_interaction_length",
    "total_score",
    "latest_view_count",
    "latest_like_count",
    "latest_comment_count",
    "latest_reply_count",
    "latest_retweet_count",
    "latest_bookmark_count",
    "latest_snapshot_at",
    "latest_snapshot_observation_id",
    "latest_snapshot_producer_name",
    "latest_snapshot_producer_run_id",
    "latest_snapshot_collection_method",
    "latest_snapshot_api_endpoint",
    "latest_snapshot_provenance_json",
    "latest_snapshot_coverage_json",
    "latest_view_count_available",
    "latest_like_count_available",
    "latest_comment_count_available",
    "latest_reply_count_available",
    "latest_retweet_count_available",
    "latest_bookmark_count_available",
    "last_discovered_at",
    "last_enriched_at",
]

USER_EVOLUTION_COLUMNS = [
    "user_id_hash",
    "source",
    "event_date",
    "contents_created",
    "interactions_created",
    "distinct_contents_touched",
    "distinct_subreddits",
    "distinct_youtube_channels",
    "distinct_conversations",
    "avg_text_length",
    "question_count",
]


OPTIONAL_EVENT_COLUMNS = {
    "event_type": "STRING",
    "event_id": "STRING",
    "observation_id": "STRING",
    "observed_at": "STRING",
    "platform_event_id": "STRING",
    "producer_name": "STRING",
    "producer_run_id": "STRING",
    "payload_fingerprint": "STRING",
    "collection_method": "STRING",
    "api_endpoint": "STRING",
    "provenance_json": "STRING",
    "coverage_json": "STRING",
    "metadata_refreshed_at": "TIMESTAMP",
    "metadata_collected_at": "STRING",
    "owner_channel_id": "STRING",
    "subreddit": "STRING",
    "subreddit_title": "STRING",
    "subreddit_description": "STRING",
    "subreddit_created_at": "STRING",
    "subreddit_visibility": "STRING",
    "subreddit_weekly_visitors": "BIGINT",
    "subreddit_weekly_contributions": "BIGINT",
    "x_account": "STRING",
    "youtube_channel_name": "STRING",
    "language": "STRING",
    "raw_text": "STRING",
    "clean_text": "STRING",
    "text_for_model": "STRING",
    "thumbnail_url": "STRING",
    "score": "BIGINT",
    "like_count": "BIGINT",
    "view_count": "BIGINT",
    "comment_count": "BIGINT",
    "reply_count": "BIGINT",
    "retweet_count": "BIGINT",
    "bookmark_count": "BIGINT",
    "follower_count": "BIGINT",
    "subscriber_count": "BIGINT",
    "subreddit_member_count": "BIGINT",
    "like_count_available": "BOOLEAN",
    "view_count_available": "BOOLEAN",
    "comment_count_available": "BOOLEAN",
    "reply_count_available": "BOOLEAN",
    "retweet_count_available": "BOOLEAN",
    "bookmark_count_available": "BOOLEAN",
    "score_available": "BOOLEAN",
    "follower_count_available": "BOOLEAN",
    "subscriber_count_available": "BOOLEAN",
    "subreddit_member_count_available": "BOOLEAN",
    "metadata_available": "BOOLEAN",
    "transcript_available": "BOOLEAN",
    "comments_available": "BOOLEAN",
    "parent_interaction_id": "STRING",
    "conversation_id": "STRING",
    "transcript_text": "STRING",
    "transcript_segments_json": "STRING",
    "duration_seconds": "DOUBLE",
    "has_auto_captions": "BOOLEAN",
    "published_at": "STRING",
    "collected_at": "STRING",
    "updated_at": "STRING",
    "last_attempt_at": "STRING",
    "content_id": "STRING",
    "parent_content_id": "STRING",
    "root_content_id": "STRING",
    "content_type": "STRING",
    "relation_type": "STRING",
    "depth": "INT",
    "position_in_thread": "BIGINT",
    "collection_status": "STRING",
    "metadata_status": "STRING",
    "transcript_status": "STRING",
    "transcript_lifecycle_status": "STRING",
    "comments_status": "STRING",
    "storage_status": "STRING",
    "error_code": "STRING",
    "error_message": "STRING",
    "attempt_count": "INT",
    "transcript_language": "STRING",
    "transcript_language_code": "STRING",
    "transcript_requested_language": "STRING",
    "transcript_requested_language_code": "STRING",
    "transcript_obtained_language": "STRING",
    "transcript_obtained_language_code": "STRING",
    "transcript_is_generated": "BOOLEAN",
    "transcript_is_translated": "BOOLEAN",
    "transcript_generation_type": "STRING",
    "transcript_provider": "STRING",
    "transcript_source": "STRING",
    "transcript_selection_strategy": "STRING",
    "transcript_segment_count": "BIGINT",
    "transcript_available_languages": "ARRAY<STRING>",
    "transcript_available_languages_json": "STRING",
    "transcript_covered_duration_seconds": "DOUBLE",
    "transcript_collected_at": "STRING",
    "transcript_attempt_count": "INT",
    "transcript_last_attempt_at": "STRING",
    "transcript_next_attempt_at": "STRING",
    "transcript_recovered_at": "STRING",
    "transcript_content_version": "STRING",
    "transcript_error_code": "STRING",
    "transcript_error_message": "STRING",
    "transcript_source_language": "STRING",
    "transcript_source_language_code": "STRING",
    "canonical_metadata": "STRING",
    "source_specific_metadata": "STRING",
}


def _ensure_columns(spark: SparkSession, table: str, columns: dict) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def _with_optional_event_columns(events: DataFrame) -> DataFrame:
    result = events
    for name, data_type in OPTIONAL_EVENT_COLUMNS.items():
        if name not in result.columns:
            result = result.withColumn(name, lit(None).cast(data_type))
    for metric in (
        "like_count",
        "view_count",
        "comment_count",
        "reply_count",
        "retweet_count",
        "bookmark_count",
        "score",
        "follower_count",
        "subscriber_count",
        "subreddit_member_count",
    ):
        availability = f"{metric}_available"
        result = result.withColumn(
            availability,
            coalesce(col(availability), col(metric).isNotNull()),
        )
    return result


def normalize_events(events: DataFrame) -> DataFrame:
    """Normalize canonical and historical records without inventing interactions."""

    prepared = _with_optional_event_columns(events)
    thumbnail_url = coalesce(
        col("thumbnail_url"),
        get_json_object(
            col("source_specific_metadata"),
            "$.snippet.thumbnails.default.url",
        ),
    )
    text = coalesce(col("clean_text"), col("raw_text"), col("title"))
    reddit_subreddit = regexp_extract(col("url"), r"/r/([^/]+)", 1)
    reddit_post_id = regexp_extract(col("url"), r"/comments/([^/]+)", 1)
    reddit_post_slug = regexp_extract(col("url"), r"/comments/[^/]+/([^/]+)", 1)
    reddit_post_title = when(
        (col("source") == "reddit") & (reddit_post_slug != ""),
        trim(regexp_replace(reddit_post_slug, r"[_-]+", " ")),
    )
    x_status_id = regexp_extract(col("url"), r"/status/(\d+)", 1)
    youtube_video_id = regexp_extract(col("url"), r"[?&]v=([^&]+)", 1)
    derived_platform_id = (
        when((col("source") == "reddit") & (reddit_post_id != ""), reddit_post_id)
        .when((col("source") == "x") & (x_status_id != ""), x_status_id)
        .when((col("source") == "youtube") & (youtube_video_id != ""), youtube_video_id)
    )
    event_platform_id = coalesce(
        col("platform_event_id"),
        derived_platform_id,
        col("url"),
    )
    root_platform_id = when(
        col("source") == "reddit",
        coalesce(col("conversation_id"), derived_platform_id, event_platform_id),
    ).otherwise(event_platform_id)
    derived_event_content_id = sha2(
        concat_ws(":", col("source"), event_platform_id),
        256,
    )
    derived_root_content_id = sha2(
        concat_ws(":", col("source"), root_platform_id),
        256,
    )
    event_content_id = coalesce(col("content_id"), derived_event_content_id)
    root_content_id = coalesce(col("root_content_id"), derived_root_content_id)
    immediate_parent_content_id = coalesce(
        col("parent_content_id"),
        when(col("source") == "reddit", root_content_id),
    )
    explicit_interaction = lower(coalesce(col("relation_type"), lit(""))).isin(
        "comment",
        "reply",
        "interaction",
    )
    typed_interaction = lower(coalesce(col("content_type"), lit(""))).rlike(
        "(comment|reply|interaction)$"
    )
    is_interaction = (
        explicit_interaction
        | typed_interaction
        | ((col("source") == "reddit") & col("relation_type").isNull())
    )
    interaction_id = coalesce(
        when(is_interaction, event_content_id),
        sha2(
            concat_ws(
                ":",
                col("source"),
                event_platform_id,
                coalesce(col("user_id"), lit("")),
            ),
            256,
        ),
    )
    derived_subreddit = coalesce(
        col("subreddit"),
        when((col("source") == "reddit") & (reddit_subreddit != ""), reddit_subreddit),
    )

    return (
        prepared.withColumn(
            "created_at",
            coalesce(to_timestamp(col("published_at")), col("event_ts")),
        )
        .withColumn("observed_at", to_timestamp(col("observed_at")))
        .withColumn(
            "event_observed_at",
            coalesce(
                to_timestamp(col("collected_at")),
                col("observed_at"),
                col("metadata_refreshed_at"),
                col("event_ts"),
            ),
        )
        .withColumn(
            "event_metadata_at",
            coalesce(
                to_timestamp(col("metadata_collected_at")),
                col("metadata_refreshed_at"),
                col("event_observed_at"),
            ),
        )
        .withColumn(
            "observation_id",
            coalesce(
                col("observation_id"),
                col("event_id"),
                sha2(
                    concat_ws(
                        "\u001f",
                        col("source"),
                        event_platform_id,
                        col("observed_at").cast("string"),
                    ),
                    256,
                ),
            ),
        )
        .withColumn("event_id", coalesce(col("event_id"), col("observation_id")))
        .withColumn(
            "metadata_available",
            coalesce(
                col("metadata_available"),
                lower(coalesce(col("metadata_status"), lit(""))) == "success",
            ),
        )
        .withColumn(
            "transcript_available",
            coalesce(
                col("transcript_available"),
                lower(
                    coalesce(
                        col("transcript_lifecycle_status"),
                        col("transcript_status"),
                        lit(""),
                    )
                ).isin("available", "success"),
            ),
        )
        .withColumn(
            "comments_available",
            coalesce(
                col("comments_available"),
                lower(coalesce(col("comments_status"), lit(""))) == "success",
            ),
        )
        .withColumn("event_date", to_date(col("created_at")))
        .withColumn("text", text)
        .withColumn(
            "content_title",
            when(col("source") == "reddit", reddit_post_title).otherwise(col("title")),
        )
        .withColumn(
            "content_text",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(text),
        )
        .withColumn(
            "content_raw_text",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(col("raw_text")),
        )
        .withColumn(
            "content_clean_text",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(col("clean_text")),
        )
        .withColumn(
            "content_text_for_model",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(
                col("text_for_model")
            ),
        )
        .withColumn("content_thumbnail_url", thumbnail_url)
        .withColumn("platform_content_id", root_platform_id)
        .withColumn("derived_subreddit", derived_subreddit)
        .withColumn("event_content_id", event_content_id)
        .withColumn("root_content_id", root_content_id)
        .withColumn("content_id", root_content_id)
        .withColumn("parent_content_id", immediate_parent_content_id)
        .withColumn("interaction_id", interaction_id)
        .withColumn("is_interaction", is_interaction)
        .withColumn("event_content_type", col("content_type"))
        .withColumn(
            "content_type",
            when(col("source") == "reddit", lit("reddit_post"))
            .when(col("source") == "x", lit("x_post"))
            .when(col("source") == "youtube", lit("youtube_video"))
            .otherwise(coalesce(col("content_type"), lit("unknown"))),
        )
        .withColumn(
            "interaction_type",
            when(col("source") == "reddit", lit("reddit_comment"))
            .when(
                col("source") == "youtube",
                coalesce(col("event_content_type"), lit("youtube_comment")),
            )
            .when(col("source") == "x", lit("x_reply"))
            .otherwise(coalesce(col("event_content_type"), lit("interaction"))),
        )
        .withColumn("author_id_hash", col("user_id"))
        .withColumn(
            "content_author_id_hash",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(col("user_id")),
        )
        .withColumn("youtube_channel_id", col("owner_channel_id"))
    )


def build_contents(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events)
    return (
        normalized.filter((~col("is_interaction")) | (col("source") == "reddit"))
        .groupBy("content_id")
        .agg(
            first("root_content_id", ignorenulls=True).alias("root_content_id"),
            first("source", ignorenulls=True).alias("source"),
            first("platform_content_id", ignorenulls=True).alias("platform_content_id"),
            first("content_type", ignorenulls=True).alias("content_type"),
            first("url", ignorenulls=True).alias("url"),
            first("content_title", ignorenulls=True).alias("title"),
            first("content_text", ignorenulls=True).alias("text"),
            first("content_author_id_hash", ignorenulls=True).alias("author_id_hash"),
            first("created_at", ignorenulls=True).alias("created_at"),
            first("event_date", ignorenulls=True).alias("event_date"),
            first("derived_subreddit", ignorenulls=True).alias("subreddit"),
            first("subreddit_title", ignorenulls=True).alias("subreddit_title"),
            first("subreddit_description", ignorenulls=True).alias("subreddit_description"),
            first("subreddit_created_at", ignorenulls=True).alias("subreddit_created_at"),
            first("subreddit_visibility", ignorenulls=True).alias("subreddit_visibility"),
            first("subreddit_weekly_visitors", ignorenulls=True).alias("subreddit_weekly_visitors"),
            first("subreddit_weekly_contributions", ignorenulls=True).alias(
                "subreddit_weekly_contributions"
            ),
            first("subreddit_member_count", ignorenulls=True).alias("subreddit_member_count"),
            first("x_account", ignorenulls=True).alias("x_account"),
            first("youtube_channel_id", ignorenulls=True).alias("youtube_channel_id"),
            first("youtube_channel_name", ignorenulls=True).alias("youtube_channel_name"),
            first("language", ignorenulls=True).alias("language"),
            first("conversation_id", ignorenulls=True).alias("conversation_id"),
            first("collection_status", ignorenulls=True).alias("collection_status"),
            first("metadata_status", ignorenulls=True).alias("metadata_status"),
            first("transcript_status", ignorenulls=True).alias("transcript_status"),
            first("comments_status", ignorenulls=True).alias("comments_status"),
            spark_max(
                when(
                    (col("source") == "youtube")
                    & (col("event_type") == "youtube.discovery.discovered"),
                    col("event_observed_at"),
                )
            ).alias("last_discovered_at"),
            spark_max(
                when(
                    (col("source") == "youtube")
                    & (
                        col("event_type").isin(
                            "youtube.metadata.observed",
                            "youtube.metadata.changed",
                        )
                        | (lower(coalesce(col("metadata_status"), lit(""))) == "success")
                    ),
                    col("event_metadata_at"),
                )
            ).alias("last_enriched_at"),
            first("canonical_metadata", ignorenulls=True).alias("canonical_metadata"),
            first("source_specific_metadata", ignorenulls=True).alias("source_specific_metadata"),
            first("content_raw_text", ignorenulls=True).alias("raw_text"),
            first("content_clean_text", ignorenulls=True).alias("clean_text"),
            first("content_text_for_model", ignorenulls=True).alias("text_for_model"),
            first("content_thumbnail_url", ignorenulls=True).alias("thumbnail_url"),
            *(
                first(column, ignorenulls=True).alias(column)
                for column in PROVENANCE_COLUMNS
                if column != "observed_at"
            ),
            spark_max("observed_at").alias("observed_at"),
        )
        .select(*CONTENT_COLUMNS)
    )


def build_interactions(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events)
    return (
        normalized.filter(col("is_interaction"))
        .select(
            "interaction_id",
            "source",
            col("platform_event_id").alias("platform_interaction_id"),
            "parent_content_id",
            "root_content_id",
            "parent_interaction_id",
            "conversation_id",
            "interaction_type",
            "relation_type",
            "depth",
            "position_in_thread",
            "author_id_hash",
            "text",
            "created_at",
            "event_date",
            "score",
            "like_count",
            "reply_count",
            "collection_status",
            "metadata_status",
            "canonical_metadata",
            "source_specific_metadata",
            "raw_text",
            "clean_text",
            "text_for_model",
            *PROVENANCE_COLUMNS,
        )
        .dropDuplicates(["interaction_id"])
    )


def build_snapshots(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events)
    return (
        normalized.filter(~col("is_interaction"))
        .withColumn(
            "snapshot_at",
            coalesce(col("observed_at"), col("metadata_refreshed_at"), col("created_at")),
        )
        .withColumn(
            "age_minutes",
            (
                (unix_timestamp(col("snapshot_at")) - unix_timestamp(col("created_at"))) / lit(60)
            ).cast("bigint"),
        )
        .withColumn("snapshot_date", to_date(col("snapshot_at")))
        .select(*SNAPSHOT_COLUMNS)
        .dropDuplicates(["content_id", "source", "snapshot_at"])
    )


def build_transcripts(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events).filter(
        (col("source") == "youtube") & (~col("is_interaction"))
    )
    requested_language_code = coalesce(
        col("transcript_requested_language_code"),
        when(
            lower(regexp_extract(coalesce(col("language"), lit("")), r"^([A-Za-z]+)", 1)) == "vi",
            lit("vi"),
        ).otherwise(lit("en")),
    )
    lifecycle_status = (
        when(
            col("transcript_text").isNotNull() & (length(trim(col("transcript_text"))) > 0),
            lit("available"),
        )
        .when(col("transcript_lifecycle_status").isNotNull(), col("transcript_lifecycle_status"))
        .when(lower(trim(col("transcript_status"))) == "success", lit("available"))
        .when(
            lower(trim(col("transcript_status"))).isin(
                "not_available", "not_found", "age_restricted"
            ),
            lit("unavailable"),
        )
        .when(lower(trim(col("transcript_status"))) == "disabled", lit("disabled"))
        .when(lower(trim(col("transcript_status"))) == "rate_limited", lit("rate_limited"))
        .when(lower(trim(col("transcript_status"))) == "ip_blocked", lit("blocked"))
        .when(
            lower(trim(col("transcript_status"))) == "permanent_error",
            lit("permanent_error"),
        )
        .when(lower(trim(col("transcript_status"))) == "pending", lit("pending"))
        .otherwise(lit("retryable_error"))
    )
    prepared = (
        normalized.withColumn(
            "transcript_text",
            coalesce(col("transcript_text"), lit(None).cast("string")),
        )
        .withColumn(
            "word_count",
            when(
                col("transcript_text").isNotNull(),
                size(split(trim(col("transcript_text")), r"\s+")).cast("bigint"),
            ).otherwise(lit(None).cast("bigint")),
        )
        .select(
            col("platform_event_id").alias("video_id"),
            "content_id",
            "transcript_status",
            lifecycle_status.alias("transcript_lifecycle_status"),
            coalesce(
                col("transcript_requested_language"),
                requested_language_code,
            ).alias("requested_language"),
            requested_language_code.alias("requested_language_code"),
            coalesce(
                col("transcript_obtained_language"),
                col("transcript_language"),
            ).alias("obtained_language"),
            coalesce(
                col("transcript_obtained_language_code"),
                col("transcript_language_code"),
            ).alias("obtained_language_code"),
            coalesce(col("transcript_language"), col("language")).alias("language"),
            col("transcript_language_code").alias("language_code"),
            "transcript_text",
            col("transcript_segments_json").alias("segments_json"),
            "duration_seconds",
            col("transcript_covered_duration_seconds").alias("covered_duration_seconds"),
            "word_count",
            col("transcript_segment_count").alias("segment_count"),
            coalesce(
                col("transcript_available_languages_json"),
                to_json(col("transcript_available_languages")),
            ).alias("available_languages_json"),
            "has_auto_captions",
            col("transcript_is_generated").alias("is_generated"),
            col("transcript_is_translated").alias("is_translated"),
            col("transcript_generation_type").alias("generation_type"),
            col("transcript_source_language").alias("source_language"),
            col("transcript_source_language_code").alias("source_language_code"),
            "transcript_source",
            coalesce(
                col("transcript_provider"),
                col("transcript_source"),
                lit("youtube_transcript_api"),
            ).alias("provider"),
            col("transcript_selection_strategy").alias("selection_strategy"),
            col("transcript_error_code").alias("error_code"),
            col("transcript_error_message").alias("error_message"),
            coalesce(col("transcript_attempt_count"), col("attempt_count"))
            .cast("bigint")
            .alias("attempt_count"),
            to_timestamp(coalesce(col("transcript_last_attempt_at"), col("last_attempt_at"))).alias(
                "last_attempt_at"
            ),
            to_timestamp(col("transcript_next_attempt_at")).alias("next_attempt_at"),
            to_timestamp(col("transcript_collected_at")).alias("collected_at"),
            to_timestamp(col("transcript_recovered_at")).alias("recovered_at"),
            col("transcript_content_version").alias("content_version"),
            "created_at",
            to_timestamp(col("updated_at")).alias("updated_at"),
            "event_date",
        )
    )
    priority = when(col("transcript_lifecycle_status") == "available", lit(0)).otherwise(lit(1))
    window = Window.partitionBy(
        "video_id",
        "content_id",
        "requested_language_code",
    ).orderBy(
        priority.asc(),
        col("last_attempt_at").desc_nulls_last(),
        col("updated_at").desc_nulls_last(),
    )
    return (
        prepared.withColumn("transcript_row_number", row_number().over(window))
        .filter(col("transcript_row_number") == 1)
        .drop("transcript_row_number")
    )


def build_content_stats(contents: DataFrame, interactions: DataFrame, snapshots: DataFrame):
    interaction_stats = interactions.groupBy("root_content_id").agg(
        count("*").cast("bigint").alias("interaction_count"),
        countDistinct("author_id_hash").cast("bigint").alias("unique_interacting_users"),
        avg(size(split(trim(coalesce(col("text"), lit(""))), r"\s+"))).alias(
            "avg_interaction_length"
        ),
        spark_sum("score").cast("bigint").alias("total_score"),
    )

    latest_window = Window.partitionBy("content_id").orderBy(
        col("snapshot_at").desc_nulls_last(),
        col("observation_id").desc_nulls_last(),
    )
    latest_snapshots = (
        snapshots.withColumn("_rank", row_number().over(latest_window))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    return (
        contents.join(
            interaction_stats,
            contents.content_id == interaction_stats.root_content_id,
            "left",
        )
        .join(
            latest_snapshots.select(
                "content_id",
                col("view_count").alias("latest_view_count"),
                col("like_count").alias("latest_like_count"),
                col("comment_count").alias("latest_comment_count"),
                col("reply_count").alias("latest_reply_count"),
                col("retweet_count").alias("latest_retweet_count"),
                col("bookmark_count").alias("latest_bookmark_count"),
                col("snapshot_at").alias("latest_snapshot_at"),
                col("observation_id").alias("latest_snapshot_observation_id"),
                col("producer_name").alias("latest_snapshot_producer_name"),
                col("producer_run_id").alias("latest_snapshot_producer_run_id"),
                col("collection_method").alias("latest_snapshot_collection_method"),
                col("api_endpoint").alias("latest_snapshot_api_endpoint"),
                col("provenance_json").alias("latest_snapshot_provenance_json"),
                col("coverage_json").alias("latest_snapshot_coverage_json"),
                col("view_count_available").alias("latest_view_count_available"),
                col("like_count_available").alias("latest_like_count_available"),
                col("comment_count_available").alias("latest_comment_count_available"),
                col("reply_count_available").alias("latest_reply_count_available"),
                col("retweet_count_available").alias("latest_retweet_count_available"),
                col("bookmark_count_available").alias("latest_bookmark_count_available"),
            ),
            "content_id",
            "left",
        )
        .select(*CONTENT_STATS_COLUMNS)
    )


def build_user_evolution(contents: DataFrame, interactions: DataFrame) -> DataFrame:
    content_activity = contents.select(
        col("author_id_hash").alias("user_id_hash"),
        "source",
        "event_date",
        "content_id",
        "subreddit",
        "youtube_channel_id",
        lit(None).cast("string").alias("conversation_id"),
        col("text").alias("activity_text"),
        lit(1).cast("bigint").alias("contents_created"),
        lit(0).cast("bigint").alias("interactions_created"),
    )
    interaction_activity = interactions.select(
        col("author_id_hash").alias("user_id_hash"),
        "source",
        "event_date",
        col("root_content_id").alias("content_id"),
        lit(None).cast("string").alias("subreddit"),
        lit(None).cast("string").alias("youtube_channel_id"),
        "conversation_id",
        col("text").alias("activity_text"),
        lit(0).cast("bigint").alias("contents_created"),
        lit(1).cast("bigint").alias("interactions_created"),
    )
    activity = content_activity.unionByName(interaction_activity)
    return (
        activity.groupBy("user_id_hash", "source", "event_date")
        .agg(
            spark_sum("contents_created").cast("bigint").alias("contents_created"),
            spark_sum("interactions_created").cast("bigint").alias("interactions_created"),
            countDistinct("content_id").cast("bigint").alias("distinct_contents_touched"),
            countDistinct("subreddit").cast("bigint").alias("distinct_subreddits"),
            countDistinct("youtube_channel_id").cast("bigint").alias("distinct_youtube_channels"),
            countDistinct("conversation_id").cast("bigint").alias("distinct_conversations"),
            avg(size(split(trim(coalesce(col("activity_text"), lit(""))), r"\s+"))).alias(
                "avg_text_length"
            ),
            spark_sum(
                when(lower(coalesce(col("activity_text"), lit(""))).contains("?"), 1).otherwise(0)
            )
            .cast("bigint")
            .alias("question_count"),
        )
        .select(*USER_EVOLUTION_COLUMNS)
    )


def _create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    for statement in (
        CREATE_CONTENTS_SQL,
        CREATE_INTERACTIONS_SQL,
        CREATE_SNAPSHOTS_SQL,
        CREATE_TRANSCRIPTS_SQL,
        CREATE_CONTENT_STATS_SQL,
        CREATE_USER_EVOLUTION_SQL,
    ):
        spark.sql(statement)

    _ensure_columns(
        spark,
        CONTENT_TABLE,
        {
            "root_content_id": "STRING",
            "platform_content_id": "STRING",
            "subreddit": "STRING",
            "subreddit_title": "STRING",
            "subreddit_description": "STRING",
            "subreddit_created_at": "STRING",
            "subreddit_visibility": "STRING",
            "subreddit_weekly_visitors": "BIGINT",
            "subreddit_weekly_contributions": "BIGINT",
            "subreddit_member_count": "BIGINT",
            "x_account": "STRING",
            "youtube_channel_id": "STRING",
            "youtube_channel_name": "STRING",
            "language": "STRING",
            "conversation_id": "STRING",
            "collection_status": "STRING",
            "metadata_status": "STRING",
            "transcript_status": "STRING",
            "comments_status": "STRING",
            "last_discovered_at": "TIMESTAMP",
            "last_enriched_at": "TIMESTAMP",
            "canonical_metadata": "STRING",
            "source_specific_metadata": "STRING",
            "raw_text": "STRING",
            "clean_text": "STRING",
            "text_for_model": "STRING",
            "thumbnail_url": "STRING",
            **PROVENANCE_COLUMN_TYPES,
        },
    )
    _ensure_columns(
        spark,
        INTERACTION_TABLE,
        {
            "platform_interaction_id": "STRING",
            "root_content_id": "STRING",
            "parent_interaction_id": "STRING",
            "conversation_id": "STRING",
            "relation_type": "STRING",
            "depth": "INT",
            "position_in_thread": "BIGINT",
            "score": "BIGINT",
            "like_count": "BIGINT",
            "reply_count": "BIGINT",
            "collection_status": "STRING",
            "metadata_status": "STRING",
            "canonical_metadata": "STRING",
            "source_specific_metadata": "STRING",
            "raw_text": "STRING",
            "clean_text": "STRING",
            "text_for_model": "STRING",
            **PROVENANCE_COLUMN_TYPES,
        },
    )
    _ensure_columns(
        spark,
        SNAPSHOT_TABLE,
        {
            "content_id": "STRING",
            "snapshot_at": "TIMESTAMP",
            "event_date": "DATE",
            "follower_count": "BIGINT",
            "subscriber_count": "BIGINT",
            "subreddit_member_count": "BIGINT",
            "event_id": "STRING",
            "observation_id": "STRING",
            "platform_event_id": "STRING",
            "user_id": "STRING",
            "url": "STRING",
            "created_at": "TIMESTAMP",
            "observed_at": "TIMESTAMP",
            "age_minutes": "BIGINT",
            "producer_name": "STRING",
            "producer_run_id": "STRING",
            "payload_fingerprint": "STRING",
            "collection_method": "STRING",
            "api_endpoint": "STRING",
            "provenance_json": "STRING",
            "coverage_json": "STRING",
            **{column: "BOOLEAN" for column in PROVENANCE_COLUMNS if column.endswith("_available")},
        },
    )
    _ensure_columns(
        spark,
        TRANSCRIPT_TABLE,
        {
            "transcript_status": "STRING",
            "transcript_lifecycle_status": "STRING",
            "requested_language": "STRING",
            "requested_language_code": "STRING",
            "obtained_language": "STRING",
            "obtained_language_code": "STRING",
            "language_code": "STRING",
            "segments_json": "STRING",
            "duration_seconds": "DOUBLE",
            "covered_duration_seconds": "DOUBLE",
            "segment_count": "BIGINT",
            "available_languages_json": "STRING",
            "has_auto_captions": "BOOLEAN",
            "is_generated": "BOOLEAN",
            "is_translated": "BOOLEAN",
            "generation_type": "STRING",
            "source_language": "STRING",
            "source_language_code": "STRING",
            "transcript_source": "STRING",
            "provider": "STRING",
            "selection_strategy": "STRING",
            "error_code": "STRING",
            "error_message": "STRING",
            "attempt_count": "BIGINT",
            "last_attempt_at": "TIMESTAMP",
            "next_attempt_at": "TIMESTAMP",
            "collected_at": "TIMESTAMP",
            "recovered_at": "TIMESTAMP",
            "content_version": "STRING",
            "updated_at": "TIMESTAMP",
        },
    )
    _ensure_columns(
        spark,
        CONTENT_STATS_TABLE,
        {
            "latest_snapshot_observation_id": "STRING",
            "latest_snapshot_producer_name": "STRING",
            "latest_snapshot_producer_run_id": "STRING",
            "latest_snapshot_collection_method": "STRING",
            "latest_snapshot_api_endpoint": "STRING",
            "latest_snapshot_provenance_json": "STRING",
            "latest_snapshot_coverage_json": "STRING",
            "latest_view_count_available": "BOOLEAN",
            "latest_like_count_available": "BOOLEAN",
            "latest_comment_count_available": "BOOLEAN",
            "latest_reply_count_available": "BOOLEAN",
            "latest_retweet_count_available": "BOOLEAN",
            "latest_bookmark_count_available": "BOOLEAN",
            "last_discovered_at": "TIMESTAMP",
            "last_enriched_at": "TIMESTAMP",
        },
    )
    spark.sql(
        f"""
        UPDATE {TRANSCRIPT_TABLE}
        SET transcript_lifecycle_status = COALESCE(
          transcript_lifecycle_status,
          CASE
            WHEN transcript_text IS NOT NULL AND LENGTH(TRIM(transcript_text)) > 0
              THEN 'available'
            WHEN LOWER(TRIM(transcript_status)) = 'success' THEN 'available'
            WHEN LOWER(TRIM(transcript_status)) IN (
              'not_available', 'not_found', 'age_restricted'
            ) THEN 'unavailable'
            WHEN LOWER(TRIM(transcript_status)) = 'disabled' THEN 'disabled'
            WHEN LOWER(TRIM(transcript_status)) = 'rate_limited' THEN 'rate_limited'
            WHEN LOWER(TRIM(transcript_status)) = 'ip_blocked' THEN 'blocked'
            WHEN LOWER(TRIM(transcript_status)) = 'permanent_error'
              THEN 'permanent_error'
            WHEN LOWER(TRIM(transcript_status)) = 'pending' THEN 'pending'
            ELSE 'retryable_error'
          END
        ),
        requested_language_code = COALESCE(
          requested_language_code,
          CASE
            WHEN LOWER(REPLACE(COALESCE(language, ''), '_', '-')) = 'vi'
              OR LOWER(REPLACE(COALESCE(language, ''), '_', '-')) LIKE 'vi-%'
              OR LOWER(COALESCE(language, '')) LIKE '%vietnam%'
              THEN 'vi'
            ELSE 'en'
          END
        ),
        requested_language = COALESCE(
          requested_language,
          CASE
            WHEN LOWER(REPLACE(COALESCE(language, ''), '_', '-')) = 'vi'
              OR LOWER(REPLACE(COALESCE(language, ''), '_', '-')) LIKE 'vi-%'
              OR LOWER(COALESCE(language, '')) LIKE '%vietnam%'
              THEN 'vi'
            ELSE 'en'
          END
        ),
        obtained_language = COALESCE(obtained_language, language),
        obtained_language_code = COALESCE(obtained_language_code, language_code),
        generation_type = COALESCE(
          generation_type,
          CASE
            WHEN is_generated = TRUE THEN 'automatic'
            WHEN is_generated = FALSE THEN 'manual'
          END
        ),
        provider = COALESCE(provider, transcript_source, 'youtube_transcript_api')
        WHERE transcript_lifecycle_status IS NULL
           OR requested_language_code IS NULL
           OR requested_language IS NULL
           OR provider IS NULL
        """
    )


def _merge_dataframe(
    dataframe: DataFrame,
    table: str,
    columns: list[str],
    key_columns: list[str],
    update_existing: bool = True,
) -> None:
    if dataframe.rdd.isEmpty():
        return

    view_name = table.replace(".", "_") + "_upsert"
    dataframe.select(*columns).createOrReplaceTempView(view_name)

    def assignment(column: str) -> str:
        if table != TRANSCRIPT_TABLE:
            return f"t.{column} = COALESCE(s.{column}, t.{column})"
        if column == "transcript_status":
            return (
                "t.transcript_status = CASE "
                "WHEN s.transcript_status = 'success' THEN 'success' "
                "WHEN t.transcript_status IN ('success', 'not_available', 'disabled') "
                "THEN t.transcript_status "
                "ELSE COALESCE(s.transcript_status, t.transcript_status) END"
            )
        if column == "transcript_lifecycle_status":
            return (
                "t.transcript_lifecycle_status = CASE "
                "WHEN s.transcript_lifecycle_status = 'available' THEN 'available' "
                "WHEN t.transcript_lifecycle_status IN ("
                "'available', 'unavailable', 'disabled', 'permanent_error') "
                "THEN t.transcript_lifecycle_status ELSE COALESCE("
                "s.transcript_lifecycle_status, t.transcript_lifecycle_status) END"
            )
        if column in {"error_code", "error_message"}:
            return (
                f"t.{column} = CASE "
                "WHEN s.transcript_lifecycle_status = 'available' THEN NULL "
                f"WHEN t.transcript_lifecycle_status = 'available' THEN t.{column} "
                f"ELSE COALESCE(s.{column}, t.{column}) END"
            )
        if column == "attempt_count":
            return (
                "t.attempt_count = GREATEST("
                "COALESCE(s.attempt_count, 0), COALESCE(t.attempt_count, 0))"
            )
        if column in {"last_attempt_at", "updated_at"}:
            return f"t.{column} = GREATEST(s.{column}, t.{column})"
        if column == "next_attempt_at":
            return "t.next_attempt_at = s.next_attempt_at"
        if column in {"created_at", "event_date"}:
            return f"t.{column} = COALESCE(t.{column}, s.{column})"
        return f"t.{column} = COALESCE(s.{column}, t.{column})"

    assignments = ", ".join(assignment(column) for column in columns if column not in key_columns)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{column}" for column in columns)
    predicate = " AND ".join(f"t.{column} <=> s.{column}" for column in key_columns)
    update_clause = (
        f"WHEN MATCHED THEN UPDATE SET {assignments}" if update_existing and assignments else ""
    )
    dataframe.sparkSession.sql(
        f"""
        MERGE INTO {table} AS t
        USING {view_name} AS s
        ON {predicate}
        {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )


def main() -> None:
    bucket = _env("MINIO_BUCKET", "lakehouse")
    warehouse = f"s3a://{bucket}/warehouse"
    spark = _build_spark("content-analytics", warehouse)
    spark.sparkContext.setLogLevel("WARN")

    _create_tables(spark)

    events = spark.table("lakehouse.silver.events")
    contents = build_contents(events)
    interactions = build_interactions(events)
    snapshots = build_snapshots(events)
    transcripts = build_transcripts(events)
    content_stats = build_content_stats(contents, interactions, snapshots)
    user_evolution = build_user_evolution(contents, interactions)

    _merge_dataframe(contents, CONTENT_TABLE, CONTENT_COLUMNS, ["content_id"])
    _merge_dataframe(
        interactions,
        INTERACTION_TABLE,
        INTERACTION_COLUMNS,
        ["interaction_id"],
    )
    _merge_dataframe(
        snapshots,
        SNAPSHOT_TABLE,
        SNAPSHOT_COLUMNS,
        ["observation_id"],
        update_existing=False,
    )
    _merge_dataframe(
        transcripts,
        TRANSCRIPT_TABLE,
        TRANSCRIPT_COLUMNS,
        ["video_id", "content_id", "requested_language_code"],
    )
    _merge_dataframe(
        content_stats,
        CONTENT_STATS_TABLE,
        CONTENT_STATS_COLUMNS,
        ["content_id"],
    )
    _merge_dataframe(
        user_evolution,
        USER_EVOLUTION_TABLE,
        USER_EVOLUTION_COLUMNS,
        ["user_id_hash", "source", "event_date"],
    )

    spark.stop()


if __name__ == "__main__":
    main()
