import os
from urllib.parse import urlparse

import duckdb
import pandas as pd


DEFAULT_TABLE_PATH = "s3://lakehouse/warehouse/silver/events"
DEFAULT_MINIO_ENDPOINT = "http://localhost:9000"
ENGAGEMENT_COLUMNS = (
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
)
AUTHOR_METADATA_COLUMNS = (
    "platform_event_id",
    "metadata_refreshed_at",
    "owner_channel_id",
    "collaborator_channel_ids",
)
OPTIONAL_ENGAGEMENT_COLUMNS = (
    "comment_count",
    "reply_count",
    "retweet_count",
    "bookmark_count",
    "score",
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
)


def _select_silver_events(connection, table_path, optional_columns=None):
    optional_columns = optional_columns or []
    metadata_columns = "".join(
        f"            {column},\n" for column in optional_columns
    )
    return connection.execute(
        f"""
        SELECT
            user_id,
            url,
            title,
            event_ts,
            source,
            error,
{metadata_columns}            like_count,
            view_count,
            event_date
        FROM iceberg_scan(?, allow_moved_paths = true)
        """,
        [table_path],
    ).fetchdf()


def _endpoint_settings(endpoint_url):
    parsed = urlparse(endpoint_url)

    if parsed.scheme:
        endpoint = parsed.netloc or parsed.path
        use_ssl = parsed.scheme.lower() == "https"
    else:
        endpoint = endpoint_url
        use_ssl = False

    return endpoint.rstrip("/"), use_ssl


def get_iceberg_config():
    endpoint_url = os.getenv(
        "DASHBOARD_MINIO_ENDPOINT",
        os.getenv("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
    )

    return {
        "table_path": os.getenv(
            "DASHBOARD_ICEBERG_TABLE_PATH",
            DEFAULT_TABLE_PATH,
        ),
        "endpoint_url": endpoint_url,
        "access_key": os.getenv(
            "DASHBOARD_MINIO_ACCESS_KEY",
            os.getenv("MINIO_ROOT_USER", "minioadmin"),
        ),
        "secret_key": os.getenv(
            "DASHBOARD_MINIO_SECRET_KEY",
            os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        ),
        "region": os.getenv("DASHBOARD_MINIO_REGION", "us-east-1"),
        "unsafe_version_guessing": os.getenv(
            "DASHBOARD_ICEBERG_UNSAFE_VERSION_GUESSING",
            "true",
        ).strip().lower()
        in {"1", "true", "yes", "on"},
    }


def _connect_iceberg(config):
    endpoint, use_ssl = _endpoint_settings(config["endpoint_url"])
    table_location = urlparse(config["table_path"])
    scope = f"s3://{table_location.netloc}"
    connection = duckdb.connect()

    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute("INSTALL iceberg")
    connection.execute("LOAD iceberg")
    if config["unsafe_version_guessing"]:
        connection.execute("SET unsafe_enable_version_guessing = true")
    connection.execute(
        """
        CREATE SECRET dashboard_minio (
            TYPE s3,
            PROVIDER config,
            KEY_ID ?,
            SECRET ?,
            REGION ?,
            ENDPOINT ?,
            URL_STYLE 'path',
            USE_SSL ?,
            SCOPE ?
        )
        """,
        [
            config["access_key"],
            config["secret_key"],
            config["region"],
            endpoint,
            use_ssl,
            scope,
        ],
    )

    return connection


def load_iceberg_data(config=None):
    config = config or get_iceberg_config()
    connection = None

    try:
        connection = _connect_iceberg(config)
        optional_columns = [
            *AUTHOR_METADATA_COLUMNS,
            *OPTIONAL_ENGAGEMENT_COLUMNS,
        ]
        while True:
            try:
                df = _select_silver_events(
                    connection,
                    config["table_path"],
                    optional_columns=optional_columns,
                )
                break
            except Exception as author_metadata_exc:
                missing_columns = [
                    column
                    for column in optional_columns
                    if column in str(author_metadata_exc)
                ]
                if not missing_columns:
                    raise
                optional_columns = [
                    column
                    for column in optional_columns
                    if column not in missing_columns
                ]
    except Exception as exc:
        raise RuntimeError(
            "Unable to read the Iceberg Silver table. "
            f"Table: {config['table_path']} | "
            f"MinIO: {config['endpoint_url']} | "
            f"Error: {exc}"
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    df = df.rename(
        columns={
            "user_id": "author_hash",
            "title": "text",
            "event_ts": "created_at",
        }
    )

    df["source"] = df["source"].astype("string").str.strip().str.lower()
    df["text"] = df["text"].astype("string")
    df["url"] = df["url"].astype("string")
    df["author_hash"] = df["author_hash"].astype("string")
    df["error"] = df["error"].astype("string")
    for column in AUTHOR_METADATA_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    df["owner_channel_id"] = df["owner_channel_id"].astype("string")
    df["platform_event_id"] = df["platform_event_id"].astype("string")
    df["metadata_refreshed_at"] = pd.to_datetime(
        df["metadata_refreshed_at"],
        errors="coerce",
        utc=True,
    )
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    for column in ENGAGEMENT_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    df["text_len_chars"] = df["text"].fillna("").str.len()
    df["text_len_words"] = df["text"].fillna("").str.split().str.len()
    df["has_question"] = df["text"].fillna("").str.contains(r"\?", regex=True)

    return df
