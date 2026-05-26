from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
    TimestampType,
)


# YouTube
# One message = one comment (top-level or reply) on a target video.
YOUTUBE_SCHEMA = StructType([
    # --- video metadata (denormalized: repeated per comment) ---
    StructField("video_id",                   StringType(),    nullable=False),
    StructField("video_title",                StringType(),    nullable=True),
    StructField("video_published_at",         TimestampType(), nullable=True),
    StructField("video_view_count",           LongType(),      nullable=True),
    StructField("video_like_count",           LongType(),    nullable=True),
    StructField("video_duration_seconds",     LongType(),      nullable=True),

    # --- comment payload ---
    StructField("thread_id",                  StringType(),    nullable=False),
    StructField("thread_total_reply_count",   LongType(),      nullable=True),
    StructField("comment_id",                 StringType(),    nullable=False),
    StructField("is_reply",                   IntegerType(),   nullable=False),   # 0 = top-level, 1 = reply
    StructField("author_hash",                StringType(),    nullable=True),    # SHA-256 of author handle (privacy)
    StructField("text",                       StringType(),    nullable=False),   # raw comment text — the thing we clean
    StructField("comment_like_count",         LongType(),      nullable=True),
    StructField("comment_published_at",       TimestampType(), nullable=False),

    # --- crawler-side precomputed features (forwarded as-is, not recomputed) ---
    StructField("text_len_chars",             LongType(),      nullable=True),
    StructField("text_len_words",             LongType(),      nullable=True),
    StructField("has_question",               IntegerType(),   nullable=True),
    StructField("upper_ratio",                DoubleType(),    nullable=True),
    StructField("kw_price",                   IntegerType(),   nullable=True),
    StructField("kw_range",                   IntegerType(),   nullable=True),
    StructField("kw_charging",                IntegerType(),   nullable=True),
    StructField("video_age_days_at_comment",  DoubleType(),    nullable=True),
])
