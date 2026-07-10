"""
Spark batch job: derive entity-level analytics tables from silver events.

The existing ``lakehouse.silver.events`` table remains the monitoring-friendly
event stream. This job materializes business entities that are easier to follow
over time: main contents, interactions, append-only engagement observations,
YouTube transcripts, content-level aggregates, and user evolution.
"""

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    coalesce,
    count,
    countDistinct,
    first,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    row_number,
    sha2,
    size,
    split,
    sum as spark_sum,
    to_date,
    trim,
    when,
)


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
  raw_text STRING,
  clean_text STRING,
  text_for_model STRING
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
  parent_interaction_id STRING,
  conversation_id STRING,
  interaction_type STRING,
  author_id_hash STRING,
  text STRING,
  created_at TIMESTAMP,
  event_date DATE,
  score BIGINT,
  like_count BIGINT,
  reply_count BIGINT,
  raw_text STRING,
  clean_text STRING,
  text_for_model STRING
)
USING iceberg
PARTITIONED BY (event_date)
"""


CREATE_SNAPSHOTS_SQL = """
CREATE TABLE IF NOT EXISTS lakehouse.silver.engagement_snapshots (
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
  language STRING,
  transcript_text STRING,
  segments_json STRING,
  duration_seconds DOUBLE,
  word_count BIGINT,
  has_auto_captions BOOLEAN,
  created_at TIMESTAMP,
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
  latest_snapshot_at TIMESTAMP
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


CONTENT_COLUMNS = [
    "content_id",
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
    "raw_text",
    "clean_text",
    "text_for_model",
]

INTERACTION_COLUMNS = [
    "interaction_id",
    "source",
    "platform_interaction_id",
    "parent_content_id",
    "parent_interaction_id",
    "conversation_id",
    "interaction_type",
    "author_id_hash",
    "text",
    "created_at",
    "event_date",
    "score",
    "like_count",
    "reply_count",
    "raw_text",
    "clean_text",
    "text_for_model",
]

SNAPSHOT_COLUMNS = [
    "content_id",
    "source",
    "snapshot_at",
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
    "snapshot_date",
]

TRANSCRIPT_COLUMNS = [
    "video_id",
    "content_id",
    "language",
    "transcript_text",
    "segments_json",
    "duration_seconds",
    "word_count",
    "has_auto_captions",
    "created_at",
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
    "platform_event_id": "STRING",
    "metadata_refreshed_at": "TIMESTAMP",
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
    "parent_interaction_id": "STRING",
    "conversation_id": "STRING",
    "transcript_text": "STRING",
    "transcript_segments_json": "STRING",
    "duration_seconds": "DOUBLE",
    "has_auto_captions": "BOOLEAN",
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
    return result


def normalize_events(events: DataFrame) -> DataFrame:
    prepared = _with_optional_event_columns(events)
    text = coalesce(col("clean_text"), col("title"), col("raw_text"))
    reddit_subreddit = regexp_extract(col("url"), r"/r/([^/]+)", 1)
    reddit_post_id = regexp_extract(col("url"), r"/comments/([^/]+)", 1)
    reddit_post_slug = regexp_extract(col("url"), r"/comments/[^/]+/([^/]+)", 1)
    reddit_post_title = when(
        (col("source") == "reddit") & (reddit_post_slug != ""),
        trim(regexp_replace(reddit_post_slug, r"[_-]+", " ")),
    )
    x_status_id = regexp_extract(col("url"), r"/status/(\d+)", 1)
    youtube_video_id = regexp_extract(col("url"), r"[?&]v=([^&]+)", 1)
    derived_root_id = (
        when((col("source") == "reddit") & (reddit_post_id != ""), reddit_post_id)
        .when((col("source") == "x") & (x_status_id != ""), x_status_id)
        .when((col("source") == "youtube") & (youtube_video_id != ""), youtube_video_id)
    )
    platform_content_id = coalesce(
        col("conversation_id"),
        derived_root_id,
        col("platform_event_id"),
        col("url"),
    )
    derived_subreddit = coalesce(
        col("subreddit"),
        when((col("source") == "reddit") & (reddit_subreddit != ""), reddit_subreddit),
    )
    content_id = sha2(concat_ws(":", col("source"), platform_content_id), 256)
    interaction_id = sha2(
        concat_ws(
            ":",
            col("source"),
            coalesce(col("platform_event_id"), lit("")),
            coalesce(col("user_id"), lit("")),
            coalesce(col("url"), lit("")),
            coalesce(col("event_ts").cast("string"), lit("")),
        ),
        256,
    )

    return (
        prepared.withColumn("created_at", col("event_ts"))
        .withColumn("event_date", to_date(col("event_ts")))
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
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(
                col("raw_text")
            ),
        )
        .withColumn(
            "content_clean_text",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(
                col("clean_text")
            ),
        )
        .withColumn(
            "content_text_for_model",
            when(col("source") == "reddit", lit(None).cast("string")).otherwise(
                col("text_for_model")
            ),
        )
        .withColumn("platform_content_id", platform_content_id)
        .withColumn("derived_subreddit", derived_subreddit)
        .withColumn("content_id", content_id)
        .withColumn("interaction_id", interaction_id)
        .withColumn(
            "content_type",
            when(col("source") == "reddit", lit("reddit_post"))
            .when(col("source") == "x", lit("x_post"))
            .when(col("source") == "youtube", lit("youtube_video"))
            .otherwise(lit("unknown")),
        )
        .withColumn(
            "interaction_type",
            when(col("source") == "reddit", lit("reddit_comment"))
            .when(col("source") == "x", lit("x_reply"))
            .when(col("source") == "youtube", lit("youtube_comment"))
            .otherwise(lit("interaction")),
        )
        .withColumn("author_id_hash", col("user_id"))
        .withColumn("youtube_channel_id", col("owner_channel_id"))
    )


def build_contents(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events)
    return (
        normalized.groupBy("content_id")
        .agg(
            first("source", ignorenulls=True).alias("source"),
            first("platform_content_id", ignorenulls=True).alias("platform_content_id"),
            first("content_type", ignorenulls=True).alias("content_type"),
            first("url", ignorenulls=True).alias("url"),
            first("content_title", ignorenulls=True).alias("title"),
            first("content_text", ignorenulls=True).alias("text"),
            first("author_id_hash", ignorenulls=True).alias("author_id_hash"),
            first("created_at", ignorenulls=True).alias("created_at"),
            first("event_date", ignorenulls=True).alias("event_date"),
            first("derived_subreddit", ignorenulls=True).alias("subreddit"),
            first("subreddit_title", ignorenulls=True).alias("subreddit_title"),
            first("subreddit_description", ignorenulls=True).alias(
                "subreddit_description"
            ),
            first("subreddit_created_at", ignorenulls=True).alias(
                "subreddit_created_at"
            ),
            first("subreddit_visibility", ignorenulls=True).alias(
                "subreddit_visibility"
            ),
            first("subreddit_weekly_visitors", ignorenulls=True).alias(
                "subreddit_weekly_visitors"
            ),
            first("subreddit_weekly_contributions", ignorenulls=True).alias(
                "subreddit_weekly_contributions"
            ),
            first("subreddit_member_count", ignorenulls=True).alias(
                "subreddit_member_count"
            ),
            first("x_account", ignorenulls=True).alias("x_account"),
            first("youtube_channel_id", ignorenulls=True).alias("youtube_channel_id"),
            first("youtube_channel_name", ignorenulls=True).alias(
                "youtube_channel_name"
            ),
            first("language", ignorenulls=True).alias("language"),
            first("content_raw_text", ignorenulls=True).alias("raw_text"),
            first("content_clean_text", ignorenulls=True).alias("clean_text"),
            first("content_text_for_model", ignorenulls=True).alias("text_for_model"),
        )
        .select(*CONTENT_COLUMNS)
    )


def build_interactions(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events)
    return normalized.select(
        "interaction_id",
        "source",
        col("platform_event_id").alias("platform_interaction_id"),
        col("content_id").alias("parent_content_id"),
        "parent_interaction_id",
        "conversation_id",
        "interaction_type",
        "author_id_hash",
        "text",
        "created_at",
        "event_date",
        "score",
        "like_count",
        "reply_count",
        "raw_text",
        "clean_text",
        "text_for_model",
    ).dropDuplicates(["interaction_id"])


def build_snapshots(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events)
    return (
        normalized.withColumn(
            "snapshot_at",
            coalesce(col("metadata_refreshed_at"), col("created_at")),
        )
        .withColumn("snapshot_date", to_date(col("snapshot_at")))
        .select(*SNAPSHOT_COLUMNS)
        .dropDuplicates(["content_id", "source", "snapshot_at"])
    )


def build_transcripts(events: DataFrame) -> DataFrame:
    normalized = normalize_events(events).filter(col("source") == "youtube")
    return (
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
            col("platform_content_id").alias("video_id"),
            "content_id",
            "language",
            "transcript_text",
            col("transcript_segments_json").alias("segments_json"),
            "duration_seconds",
            "word_count",
            "has_auto_captions",
            "created_at",
            "event_date",
        )
        .filter(col("transcript_text").isNotNull())
        .dropDuplicates(["video_id", "content_id"])
    )


def build_content_stats(contents: DataFrame, interactions: DataFrame, snapshots: DataFrame):
    interaction_stats = interactions.groupBy("parent_content_id").agg(
        count("*").cast("bigint").alias("interaction_count"),
        countDistinct("author_id_hash").cast("bigint").alias("unique_interacting_users"),
        avg(size(split(trim(coalesce(col("text"), lit(""))), r"\s+"))).alias(
            "avg_interaction_length"
        ),
        spark_sum("score").cast("bigint").alias("total_score"),
    )

    latest_window = Window.partitionBy("content_id").orderBy(
        col("snapshot_at").desc_nulls_last()
    )
    latest_snapshots = (
        snapshots.withColumn("_rank", row_number().over(latest_window))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    return (
        contents.join(
            interaction_stats,
            contents.content_id == interaction_stats.parent_content_id,
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
        col("parent_content_id").alias("content_id"),
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
            spark_sum("interactions_created")
            .cast("bigint")
            .alias("interactions_created"),
            countDistinct("content_id").cast("bigint").alias(
                "distinct_contents_touched"
            ),
            countDistinct("subreddit").cast("bigint").alias("distinct_subreddits"),
            countDistinct("youtube_channel_id")
            .cast("bigint")
            .alias("distinct_youtube_channels"),
            countDistinct("conversation_id")
            .cast("bigint")
            .alias("distinct_conversations"),
            avg(size(split(trim(coalesce(col("activity_text"), lit(""))), r"\s+"))).alias(
                "avg_text_length"
            ),
            spark_sum(
                when(lower(coalesce(col("activity_text"), lit(""))).contains("?"), 1)
                .otherwise(0)
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
            "raw_text": "STRING",
            "clean_text": "STRING",
            "text_for_model": "STRING",
        },
    )
    _ensure_columns(
        spark,
        INTERACTION_TABLE,
        {
            "platform_interaction_id": "STRING",
            "parent_interaction_id": "STRING",
            "conversation_id": "STRING",
            "score": "BIGINT",
            "like_count": "BIGINT",
            "reply_count": "BIGINT",
            "raw_text": "STRING",
            "clean_text": "STRING",
            "text_for_model": "STRING",
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
        },
    )
    _ensure_columns(
        spark,
        TRANSCRIPT_TABLE,
        {
            "segments_json": "STRING",
            "duration_seconds": "DOUBLE",
            "has_auto_captions": "BOOLEAN",
        },
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
    assignments = ", ".join(
        f"t.{column} = s.{column}" for column in columns if column not in key_columns
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{column}" for column in columns)
    predicate = " AND ".join(
        f"t.{column} <=> s.{column}" for column in key_columns
    )
    update_clause = (
        f"WHEN MATCHED THEN UPDATE SET {assignments}"
        if update_existing and assignments
        else ""
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
        ["content_id", "source", "snapshot_at"],
        update_existing=False,
    )
    _merge_dataframe(
        transcripts,
        TRANSCRIPT_TABLE,
        TRANSCRIPT_COLUMNS,
        ["video_id", "content_id"],
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
