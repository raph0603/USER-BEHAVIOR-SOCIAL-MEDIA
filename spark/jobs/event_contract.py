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
    "platform_event_id": "string",
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
    "transcript_source_language": "string",
    "transcript_source_language_code": "string",
    "transcript_is_generated": "boolean",
    "transcript_is_translated": "boolean",
    "transcript_source": "string",
    "transcript_selection_strategy": "string",
    "transcript_segment_count": "long",
    "transcript_available_languages": "array_string",
    "transcript_covered_duration_seconds": "double",
    "transcript_collected_at": "string",
    "transcript_error_code": "string",
    "transcript_error_message": "string",
    "canonical_metadata": "string",
    "source_specific_metadata": "string",
    "raw_source_payload": "string",
}

EVENT_COLUMNS = tuple(EVENT_FIELD_TYPES)
BRONZE_COLUMNS = EVENT_COLUMNS + ("metadata_refreshed_at", "event_ts")
SILVER_COLUMNS = BRONZE_COLUMNS + ("event_date",)

OUTCOME_STATUS_COLUMNS = frozenset(
    {
        "collection_status",
        "metadata_status",
        "transcript_status",
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

    return ",\n          ".join(
        f"{column} {ICEBERG_TYPES[column]}" for column in columns
    )


def merge_assignment(column: str) -> str:
    """Render an idempotent update that cannot downgrade a terminal outcome."""

    if column in OUTCOME_STATUS_COLUMNS:
        return (
            f"t.{column} = CASE "
            f"WHEN s.{column} = 'success' THEN 'success' "
            f"WHEN t.{column} IN ('success', 'not_available', 'disabled') "
            f"THEN t.{column} ELSE COALESCE(s.{column}, t.{column}) END"
        )
    if column == "attempt_count":
        return (
            "t.attempt_count = GREATEST("
            "COALESCE(s.attempt_count, 0), COALESCE(t.attempt_count, 0))"
        )
    if column in {"last_attempt_at", "updated_at"}:
        return f"t.{column} = GREATEST(s.{column}, t.{column})"
    return f"t.{column} = COALESCE(s.{column}, t.{column})"
