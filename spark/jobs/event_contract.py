"""Canonical event field definitions shared by Spark pipeline stages."""

from __future__ import annotations


EVENT_FIELD_TYPES = {
    "user_id": "string",
    "url": "string",
    "title": "string",
    "raw_text": "string",
    "clean_text": "string",
    "text_for_model": "string",
    "thumbnail_url": "string",
    "timestamp": "string",
    "source": "string",
    "error": "string",
    "event_id": "string",
    "observation_id": "string",
    "observed_at": "string",
    "platform_event_id": "string",
    "producer_name": "string",
    "producer_run_id": "string",
    "payload_fingerprint": "string",
    "collection_method": "string",
    "api_endpoint": "string",
    "provenance_json": "string",
    "coverage_json": "string",
    "owner_channel_id": "string",
    "subreddit": "string",
    "subreddit_title": "string",
    "subreddit_description": "string",
    "subreddit_created_at": "string",
    "subreddit_visibility": "string",
    "subreddit_weekly_visitors": "long",
    "subreddit_weekly_contributions": "long",
    "x_account": "string",
    "youtube_channel_name": "string",
    "language": "string",
    "parent_interaction_id": "string",
    "conversation_id": "string",
    "transcript_text": "string",
    "transcript_segments_json": "string",
    "duration_seconds": "double",
    "has_auto_captions": "boolean",
    "collaborator_channel_ids": "array_string",
    "like_count": "long",
    "view_count": "long",
    "comment_count": "long",
    "reply_count": "long",
    "retweet_count": "long",
    "bookmark_count": "long",
    "score": "long",
    "follower_count": "long",
    "subscriber_count": "long",
    "subreddit_member_count": "long",
    "like_count_available": "boolean",
    "view_count_available": "boolean",
    "comment_count_available": "boolean",
    "reply_count_available": "boolean",
    "retweet_count_available": "boolean",
    "bookmark_count_available": "boolean",
    "score_available": "boolean",
    "follower_count_available": "boolean",
    "subscriber_count_available": "boolean",
    "subreddit_member_count_available": "boolean",
    "metadata_available": "boolean",
    "transcript_available": "boolean",
    "comments_available": "boolean",
    "published_at": "string",
    "collected_at": "string",
    "updated_at": "string",
    "last_attempt_at": "string",
    "content_id": "string",
    "parent_content_id": "string",
    "root_content_id": "string",
    "content_type": "string",
    "relation_type": "string",
    "depth": "int",
    "position_in_thread": "long",
    "collection_status": "string",
    "metadata_status": "string",
    "transcript_status": "string",
    "transcript_lifecycle_status": "string",
    "comments_status": "string",
    "storage_status": "string",
    "error_code": "string",
    "error_message": "string",
    "attempt_count": "int",
    "collector_version": "string",
    "source_payload_version": "string",
    "metadata_collected_at": "string",
    "metadata_error_code": "string",
    "metadata_error_message": "string",
    "comments_collected_at": "string",
    "comments_error_code": "string",
    "comments_error_message": "string",
    "transcript_language": "string",
    "transcript_language_code": "string",
    "transcript_requested_language": "string",
    "transcript_requested_language_code": "string",
    "transcript_obtained_language": "string",
    "transcript_obtained_language_code": "string",
    "transcript_source_language": "string",
    "transcript_source_language_code": "string",
    "transcript_is_generated": "boolean",
    "transcript_is_translated": "boolean",
    "transcript_generation_type": "string",
    "transcript_provider": "string",
    "transcript_source": "string",
    "transcript_selection_strategy": "string",
    "transcript_segment_count": "long",
    "transcript_available_languages": "array_string",
    "transcript_available_languages_json": "string",
    "transcript_covered_duration_seconds": "double",
    "transcript_collected_at": "string",
    "transcript_attempt_count": "int",
    "transcript_last_attempt_at": "string",
    "transcript_next_attempt_at": "string",
    "transcript_recovered_at": "string",
    "transcript_content_version": "string",
    "transcript_error_code": "string",
    "transcript_error_message": "string",
    "event_type": "string",
    "event_version": "string",
    "video_id": "string",
    "channel_id": "string",
    "correlation_id": "string",
    "query_id": "string",
    "metadata_source": "string",
    "metadata_schema_version": "string",
    "yt_dlp_version": "string",
    "enrichment_status": "string",
    "next_attempt_at": "string",
    "error_class": "string",
    "metadata_hash": "string",
    "previous_metadata_hash": "string",
    "changed_fields": "array_string",
    "last_metadata_refresh_at": "string",
    "next_metadata_refresh_at": "string",
    "metadata_refresh_count": "int",
    "last_metrics_refresh_at": "string",
    "next_metrics_refresh_at": "string",
    "metrics_refresh_count": "int",
    "metrics_refresh_status": "string",
    "payload_json": "string",
    "canonical_metadata": "string",
    "source_specific_metadata": "string",
    "raw_source_payload": "string",
}

EVENT_COLUMNS = tuple(EVENT_FIELD_TYPES)
BRONZE_COLUMNS = EVENT_COLUMNS + ("metadata_refreshed_at", "event_ts")
SILVER_COLUMNS = BRONZE_COLUMNS + ("event_date",)

BRONZE_EVENT_LOG_METADATA_TYPES = {
    "kafka_topic": "string",
    "kafka_partition": "int",
    "kafka_offset": "long",
    "kafka_timestamp": "timestamp",
    "bronze_epoch_id": "long",
    "bronze_run_id": "string",
    "ingested_at": "timestamp",
}
BRONZE_EVENT_LOG_COLUMNS = BRONZE_COLUMNS + tuple(BRONZE_EVENT_LOG_METADATA_TYPES)

BRONZE_DLQ_TYPES = {
    "dlq_id": "string",
    "kafka_topic": "string",
    "kafka_partition": "int",
    "kafka_offset": "long",
    "kafka_timestamp": "timestamp",
    "category": "string",
    "payload_fingerprint": "string",
    "protected_payload": "string",
    "failed_at": "timestamp",
    "bronze_epoch_id": "long",
    "bronze_run_id": "string",
}
BRONZE_DLQ_COLUMNS = tuple(BRONZE_DLQ_TYPES)

OUTCOME_STATUS_COLUMNS = frozenset(
    {
        "collection_status",
        "metadata_status",
        "transcript_status",
        "transcript_lifecycle_status",
        "comments_status",
        "storage_status",
    }
)

ICEBERG_TYPES = {
    **{
        name: {
            "string": "STRING",
            "long": "BIGINT",
            "int": "INT",
            "double": "DOUBLE",
            "boolean": "BOOLEAN",
            "array_string": "ARRAY<STRING>",
        }[data_type]
        for name, data_type in EVENT_FIELD_TYPES.items()
    },
    "metadata_refreshed_at": "TIMESTAMP",
    "event_ts": "TIMESTAMP",
    "event_date": "DATE",
    **{
        name: {
            "string": "STRING",
            "long": "BIGINT",
            "int": "INT",
            "timestamp": "TIMESTAMP",
        }[data_type]
        for name, data_type in BRONZE_EVENT_LOG_METADATA_TYPES.items()
    },
    **{
        name: {
            "string": "STRING",
            "long": "BIGINT",
            "int": "INT",
            "timestamp": "TIMESTAMP",
        }[data_type]
        for name, data_type in BRONZE_DLQ_TYPES.items()
    },
}


def spark_struct_type(*extra_fields: tuple[str, str]):
    """Build a nullable Spark schema without importing PySpark at module import time."""

    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    factories = {
        "string": StringType,
        "long": LongType,
        "int": IntegerType,
        "double": DoubleType,
        "boolean": BooleanType,
        "array_string": lambda: ArrayType(StringType()),
        "timestamp": TimestampType,
    }
    fields = list(EVENT_FIELD_TYPES.items()) + list(extra_fields)
    return StructType(
        [StructField(name, factories[data_type](), True) for name, data_type in fields]
    )


def create_table_columns(columns: tuple[str, ...]) -> str:
    """Render a deterministic Iceberg column list for CREATE TABLE statements."""

    return ",\n          ".join(f"{column} {ICEBERG_TYPES[column]}" for column in columns)


def merge_assignment(column: str) -> str:
    """Render an idempotent update that cannot downgrade a terminal outcome."""

    if column == "transcript_lifecycle_status":
        return (
            "t.transcript_lifecycle_status = CASE "
            "WHEN s.transcript_lifecycle_status = 'available' THEN 'available' "
            "WHEN t.transcript_lifecycle_status IN ("
            "'available', 'unavailable', 'disabled', 'permanent_error') "
            "THEN t.transcript_lifecycle_status ELSE COALESCE("
            "s.transcript_lifecycle_status, t.transcript_lifecycle_status) END"
        )
    if column in OUTCOME_STATUS_COLUMNS:
        return (
            f"t.{column} = CASE "
            f"WHEN s.{column} = 'success' THEN 'success' "
            f"WHEN t.{column} IN ('success', 'not_available', 'disabled') "
            f"THEN t.{column} ELSE COALESCE(s.{column}, t.{column}) END"
        )
    if column == "attempt_count":
        return (
            "t.attempt_count = GREATEST(COALESCE(s.attempt_count, 0), COALESCE(t.attempt_count, 0))"
        )
    if column in {"last_attempt_at", "updated_at"}:
        return f"t.{column} = GREATEST(s.{column}, t.{column})"
    return f"t.{column} = COALESCE(s.{column}, t.{column})"
