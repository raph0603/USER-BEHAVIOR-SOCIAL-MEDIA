import os
from urllib.parse import urlparse

import duckdb
import pandas as pd


DEFAULT_TABLE_PATH = "s3://lakehouse/warehouse/silver/events"
DEFAULT_MINIO_ENDPOINT = "http://localhost:9000"


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
        df = connection.execute(
            """
            SELECT
                user_id,
                url,
                title,
                event_ts,
                source,
                error,
                event_date
            FROM iceberg_scan(?, allow_moved_paths = true)
            """,
            [config["table_path"]],
        ).fetchdf()
    except Exception as exc:
        raise RuntimeError(
            "Impossible de lire la table Iceberg Silver. "
            f"Table: {config['table_path']} | "
            f"MinIO: {config['endpoint_url']} | "
            f"Erreur: {exc}"
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
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["text_len_chars"] = df["text"].fillna("").str.len()
    df["text_len_words"] = df["text"].fillna("").str.split().str.len()
    df["has_question"] = df["text"].fillna("").str.contains(r"\?", regex=True)

    return df
