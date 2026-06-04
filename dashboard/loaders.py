import pandas as pd
import numpy as np


COMMON_COLUMNS = [
    "source",
    "item_id",
    "parent_id",
    "is_reply",
    "author_hash",
    "author_name",
    "text",
    "created_at",
    "scraped_at",
    "url",
    "container_url",
    "engagement",
    "engagement_2",
    "lang",
    "depth",
    "has_media",
    "media_count",
    "has_question",
    "text_len_chars",
    "text_len_words",
    "kw_price",
    "kw_range",
    "kw_charging",
]


def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def smart_read_csv(path):
    encodings = ["utf-8-sig", "utf-8", "latin1"]
    separators = [",", ";", "\t"]

    last_error = None

    for enc in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, low_memory=False)
                if df.shape[1] > 1:
                    return df
            except Exception as e:
                last_error = e

    raise ValueError(f"Impossible de lire correctement le CSV: {path} | Dernière erreur: {last_error}")


def parse_datetime_column(series):
    return pd.to_datetime(series.astype(str).str.strip(), errors="coerce", utc=True)


def load_reddit(path):
    df = smart_read_csv(path)

    print("\nREDDIT RAW COLUMNS:", list(df.columns))

    df = df.rename(columns={
        "comment_id": "item_id",
        "author": "author_name",
        "comment_text": "text",
        "created_iso": "created_at",
        "comment_permalink": "url",
        "post_url": "container_url",
        "score": "engagement",
    })

    df["source"] = "reddit"
    df["is_reply"] = pd.to_numeric(df.get("depth"), errors="coerce").fillna(0) > 0
    df["scraped_at"] = pd.NaT
    df["engagement_2"] = pd.NA
    df["lang"] = pd.NA
    df["has_media"] = pd.NA
    df["media_count"] = pd.NA
    df["has_question"] = df["text"].fillna("").astype(str).str.contains(r"\?", regex=True)
    df["text_len_chars"] = df["text"].fillna("").astype(str).str.len()
    df["text_len_words"] = df["text"].fillna("").astype(str).str.split().str.len()
    df["kw_price"] = pd.NA
    df["kw_range"] = pd.NA
    df["kw_charging"] = pd.NA

    df = ensure_columns(df, COMMON_COLUMNS)
    df = df[COMMON_COLUMNS]

    df["created_at"] = parse_datetime_column(df["created_at"])

    print("REDDIT SHAPE:", df.shape)
    print("REDDIT created_at non-null:", df["created_at"].notna().sum())
    return df


def load_x(path):
    df = smart_read_csv(path)

    print("\nX RAW COLUMNS:", list(df.columns))

    df = df.rename(columns={
        "status_id": "item_id",
        "display_name": "author_name",
        "tweet_text": "text",
        "tweet_time_iso": "created_at",
        "scraped_at_utc": "scraped_at",
        "tweet_url": "url",
        "page_url": "container_url",
        "like_count": "engagement",
        "reply_count": "engagement_2",
    })

    df["source"] = "x"
    df["parent_id"] = pd.NA
    df["depth"] = pd.NA
    df["has_question"] = df["text"].fillna("").astype(str).str.contains(r"\?", regex=True)
    df["text_len_chars"] = df["text"].fillna("").astype(str).str.len()
    df["text_len_words"] = df["text"].fillna("").astype(str).str.split().str.len()
    df["kw_price"] = pd.NA
    df["kw_range"] = pd.NA
    df["kw_charging"] = pd.NA

    df = ensure_columns(df, COMMON_COLUMNS)
    df = df[COMMON_COLUMNS]

    df["created_at"] = parse_datetime_column(df["created_at"])
    df["scraped_at"] = parse_datetime_column(df["scraped_at"])

    print("X SHAPE:", df.shape)
    print("X created_at non-null:", df["created_at"].notna().sum())
    return df


def load_youtube(path):
    df = smart_read_csv(path)

    print("\nYOUTUBE RAW COLUMNS:", list(df.columns))
    print("YOUTUBE RAW SHAPE:", df.shape)

    df = df.rename(columns={
        "comment_id": "item_id",
        "comment_published_at": "created_at",
        "comment_like_count": "engagement",
        "thread_total_reply_count": "engagement_2",
    })

    df["source"] = "youtube"
    df["parent_id"] = pd.NA
    df["author_name"] = "anonymous"
    df["scraped_at"] = pd.NaT
    df["url"] = "https://www.youtube.com/watch?v=" + df["video_id"].astype(str)
    df["container_url"] = df["url"]
    df["lang"] = pd.NA
    df["depth"] = pd.NA
    df["has_media"] = pd.NA
    df["media_count"] = pd.NA

    df = ensure_columns(df, COMMON_COLUMNS)
    df = df[COMMON_COLUMNS]

    df["created_at"] = parse_datetime_column(df["created_at"])

    print("YOUTUBE created_at non-null after parse:", df["created_at"].notna().sum())
    print(df[["source", "item_id", "created_at", "author_name", "text"]].head(5))

    return df


def load_all_data(reddit_path, x_path, youtube_path):
    reddit = load_reddit(reddit_path)
    x_df = load_x(x_path)
    youtube = load_youtube(youtube_path)

    df = pd.concat([reddit, x_df, youtube], ignore_index=True)

    if "engagement" in df.columns:
        df["engagement"] = pd.to_numeric(df["engagement"], errors="coerce")

    if "engagement_2" in df.columns:
        df["engagement_2"] = pd.to_numeric(df["engagement_2"], errors="coerce")

    if "text_len_chars" in df.columns:
        df["text_len_chars"] = pd.to_numeric(df["text_len_chars"], errors="coerce")

    if "text_len_words" in df.columns:
        df["text_len_words"] = pd.to_numeric(df["text_len_words"], errors="coerce")

    df["is_reply"] = df["is_reply"].astype("boolean")

    print("\nCOUNTS BY SOURCE")
    print(df.groupby("source").size())

    print("\nNON-NULL created_at BY SOURCE")
    print(df.groupby("source")["created_at"].apply(lambda s: s.notna().sum()))

    return df