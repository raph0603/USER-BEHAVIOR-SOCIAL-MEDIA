import os
from pathlib import Path
import pandas as pd

def get_iceberg_config():
    # Return mock config
    return {
        "table_path": "s3://lakehouse/warehouse/silver/events",
        "endpoint_url": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "region": "us-east-1",
        "unsafe_version_guessing": True
    }

def load_iceberg_data(config=None):
    # Support mock failures if specified via environment variable
    if os.getenv("MOCK_LOADERS_FAILURE") == "true":
        raise RuntimeError("Simulated Iceberg data load failure")

    mock_dir_env = os.getenv("MOCK_DATA_DIR")
    if mock_dir_env:
        mock_dir = Path(mock_dir_env)
        parquet_file = mock_dir / "mock_events.parquet"
        csv_file = mock_dir / "mock_events.csv"
        
        if parquet_file.exists():
            return pd.read_parquet(parquet_file)
        elif csv_file.exists():
            df = pd.read_csv(csv_file)
            # Ensure datetimes are properly parsed
            if "created_at" in df.columns:
                df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
            if "metadata_refreshed_at" in df.columns:
                df["metadata_refreshed_at"] = pd.to_datetime(df["metadata_refreshed_at"], errors="coerce", utc=True)
            
            # Reconstruct collaborator_channel_ids if present as stringified JSON
            if "collaborator_channel_ids" in df.columns:
                import json
                def parse_list(x):
                    if pd.isna(x) or not x:
                        return None
                    if isinstance(x, (list, tuple)):
                        return list(x)
                    try:
                        parsed = json.loads(x)
                        if isinstance(parsed, list):
                            return parsed
                    except Exception:
                        pass
                    return [x]
                df["collaborator_channel_ids"] = df["collaborator_channel_ids"].apply(parse_list)
            return df

    # Return default DataFrame if no custom files are provided
    data = [
        {
            "author_hash": "youtube-hash1",
            "url": "https://www.youtube.com/watch?v=vid1",
            "text": "Cool YouTube Video",
            "created_at": pd.to_datetime("2026-06-01T12:00:00Z", utc=True),
            "source": "youtube",
            "error": None,
            "platform_event_id": "vid1",
            "metadata_refreshed_at": pd.to_datetime("2026-06-01T12:05:00Z", utc=True),
            "owner_channel_id": "owner1",
            "collaborator_channel_ids": ["collab1", "collab2"],
            "like_count": 100,
            "view_count": 1000,
            "comment_count": 5,
            "reply_count": 0,
            "retweet_count": 0,
            "bookmark_count": 0,
            "score": 0,
            "text_len_chars": 18,
            "text_len_words": 3,
            "has_question": False
        },
        {
            "author_hash": "x-hash2",
            "url": "https://twitter.com/user/status/tweet1",
            "text": "Hello World? #social",
            "created_at": pd.to_datetime("2026-06-02T15:30:00Z", utc=True),
            "source": "x",
            "error": None,
            "platform_event_id": "tweet1",
            "metadata_refreshed_at": pd.to_datetime("2026-06-02T15:35:00Z", utc=True),
            "owner_channel_id": None,
            "collaborator_channel_ids": None,
            "like_count": 50,
            "view_count": 500,
            "comment_count": 2,
            "reply_count": 0,
            "retweet_count": 0,
            "bookmark_count": 0,
            "score": 0,
            "text_len_chars": 20,
            "text_len_words": 3,
            "has_question": True
        },
        {
            "author_hash": "reddit-hash3",
            "url": "https://reddit.com/r/test/comments/post1",
            "text": "Interesting Reddit Post",
            "created_at": pd.to_datetime("2026-06-03T09:00:00Z", utc=True),
            "source": "reddit",
            "error": None,
            "platform_event_id": "post1",
            "metadata_refreshed_at": pd.to_datetime("2026-06-03T09:05:00Z", utc=True),
            "owner_channel_id": None,
            "collaborator_channel_ids": None,
            "like_count": None,
            "view_count": None,
            "comment_count": 10,
            "reply_count": 0,
            "retweet_count": 0,
            "bookmark_count": 0,
            "score": 15,
            "text_len_chars": 23,
            "text_len_words": 3,
            "has_question": False
        }
    ]
    return pd.DataFrame(data)


def load_iceberg_table(table_path, config=None, limit=None):
    dataframe = load_iceberg_data(config)
    return dataframe.head(limit) if limit is not None else dataframe
