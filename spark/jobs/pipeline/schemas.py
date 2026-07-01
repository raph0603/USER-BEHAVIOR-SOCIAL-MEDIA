from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType
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
    StructField("subscriber_count",           LongType(),      nullable=True),
])


# Reddit
# One message = one comment on a post in a subreddit.
REDDIT_SCHEMA = StructType([
    StructField("post_url",           StringType(),    nullable=False),
    StructField("comment_id",         StringType(),    nullable=False),   # key
    StructField("parent_id",          StringType(),    nullable=True),
    StructField("depth",              IntegerType(),   nullable=True),
    StructField("author_hash",        StringType(),    nullable=True),
    StructField("author",             StringType(),    nullable=True),    # PII — dropped before clean.posts
    StructField("comment_text",       StringType(),    nullable=False),   # raw text — the thing we clean
    StructField("created_utc",        DoubleType(),    nullable=True),    # epoch seconds
    StructField("created_iso",        TimestampType(), nullable=False),
    StructField("score",              LongType(),      nullable=True),
    StructField("comment_permalink",  StringType(),    nullable=True),
    StructField("subreddit_member_count", LongType(),  nullable=True),
])


# X (Twitter)
# One message = one tweet.
X_SCHEMA = StructType([
    StructField("page_url",        StringType(),    nullable=True),
    StructField("tweet_url",       StringType(),    nullable=True),
    StructField("status_id",       StringType(),    nullable=False),   # key
    StructField("article_index",   IntegerType(),   nullable=True),
    StructField("screen_name",     StringType(),    nullable=True),    # PII — dropped before clean.posts
    StructField("display_name",    StringType(),    nullable=True),    # PII — dropped before clean.posts
    StructField("author_hash",     StringType(),    nullable=True),
    StructField("tweet_text",      StringType(),    nullable=False),   # raw text — the thing we clean
    StructField("lang",            StringType(),    nullable=True),
    StructField("tweet_time",      StringType(),    nullable=True),
    StructField("tweet_time_iso",  TimestampType(), nullable=False),
    StructField("reply_count",     LongType(),      nullable=True),
    StructField("retweet_count",   LongType(),      nullable=True),
    StructField("like_count",      LongType(),      nullable=True),
    StructField("bookmark_count",  LongType(),      nullable=True),
    StructField("view_count",      LongType(),      nullable=True),
    StructField("is_reply",        BooleanType(),   nullable=True),
    StructField("is_pinned",       BooleanType(),   nullable=True),
    StructField("has_media",       BooleanType(),   nullable=True),
    StructField("media_count",     IntegerType(),   nullable=True),
    StructField("hashtags",        StringType(),    nullable=True),
    StructField("mentions",        StringType(),    nullable=True),
    StructField("external_links",  StringType(),    nullable=True),
    StructField("follower_count",  LongType(),      nullable=True),
    StructField("scraped_at_utc",  TimestampType(), nullable=True),
])