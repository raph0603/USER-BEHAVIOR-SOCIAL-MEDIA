import html as html_lib
import json
import os
import re
from io import BytesIO
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from ai_server import AIServerError, get_ai_server_health, predict_post
from airflow_monitoring import AirflowClient
from loaders import get_iceberg_config, load_iceberg_data, load_optional_iceberg_table
from manual_import import (
    MANUAL_IMPORT_DAG_ID,
    get_manual_import_config,
    load_import_events,
    publish_events,
    summarize_events,
)
from navigation import render_navigation
from youtube_presentation import (
    build_youtube_display_rows,
    build_youtube_freshness_table,
    coverage_summary,
    format_available_metric,
    format_timestamp,
    freshness_warning,
    metric_is_available,
    provenance_summary,
    transcript_lifecycle_status,
    transcript_provenance_label,
    transcript_retry_warning,
    transcript_status_presentation,
    youtube_data_completeness,
    youtube_thumbnail_display_url,
)


ENGAGEMENT_LABELS = {
    "like_count": "Likes",
    "view_count": "Views",
    "comment_count": "Comments",
    "reply_count": "Replies",
    "retweet_count": "Retweets",
    "bookmark_count": "Bookmarks",
    "score": "Score Reddit",
    "follower_count": "Followers",
    "subscriber_count": "Subscribers",
    "subreddit_member_count": "Subreddit Members",
    "subreddit_weekly_visitors": "Weekly Visitors",
    "subreddit_weekly_contributions": "Weekly Contributions",
}
ENGAGEMENT_COLUMNS = tuple(ENGAGEMENT_LABELS)
PROFILE_ENGAGEMENT_COLUMNS = (
    "follower_count",
    "subscriber_count",
    "subreddit_member_count",
)
SUMMED_ENGAGEMENT_COLUMNS = tuple(
    column for column in ENGAGEMENT_COLUMNS if column not in PROFILE_ENGAGEMENT_COLUMNS
)
OPTIONAL_DASHBOARD_COLUMNS = (
    "platform_event_id",
    "metadata_refreshed_at",
    "owner_channel_id",
    "subreddit_title",
    "subreddit_description",
    "subreddit_created_at",
    "subreddit_visibility",
    "subreddit_weekly_visitors",
    "subreddit_weekly_contributions",
    "collaborator_channel_ids",
    "raw_text",
    "clean_text",
    "text_for_model",
)
REDDIT_COMMUNITY_COLUMNS = (
    "subreddit_title",
    "subreddit_description",
    "subreddit_created_at",
    "subreddit_visibility",
    "subreddit_member_count",
    "subreddit_weekly_visitors",
    "subreddit_weekly_contributions",
)
STATIC_REDDIT_COMMUNITY_FALLBACKS = {
    "electricvehicles": {
        "subreddit_title": "Electric Vehicle News and Discussion",
        "subreddit_description": (
            "The future of sustainable transportation is here! This is the Reddit "
            "community for EV owners and enthusiasts."
        ),
        "subreddit_created_at": "Apr 20, 2009",
        "subreddit_visibility": "public",
        "subreddit_member_count": 509000,
    },
    "teslamotors": {
        "subreddit_title": "TeslaMotors - The original and largest Tesla community!",
        "subreddit_description": "The original and largest Tesla community!",
        "subreddit_created_at": "Sep 4, 2010",
        "subreddit_visibility": "public",
        "subreddit_member_count": 124000,
    },
}

TRACKING_ROLE_ORDER = [
    "Author",
    "Owner YouTube",
    "YouTube collaborator",
]

VIDEO_LEVEL_ENGAGEMENT_COLUMNS = (
    "like_count",
    "view_count",
    "comment_count",
)
DEFAULT_BALANCING_REPORT_PATH = "/app/balancing/report.json"
MODEL_PIPELINE_TABLES = {
    "post_features": ("silver", "post_features"),
    "engagement_snapshots": ("silver", "engagement_snapshots"),
    "context_features": ("silver", "context_features"),
    "model_predictions": ("gold", "model_predictions"),
    "training_examples": ("gold", "training_examples"),
}
CONTENT_ANALYTICS_TABLES = {
    "contents": ("silver", "contents"),
    "interactions": ("silver", "interactions"),
    "engagement_snapshots": ("silver", "engagement_snapshots"),
    "transcripts": ("silver", "transcripts"),
    "content_stats": ("gold", "content_stats"),
    "user_evolution": ("gold", "user_evolution"),
}


def has_dashboard_value(value):
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return bool(str(value).strip())


def normalized_status(value, default="pending"):
    if not has_dashboard_value(value):
        return default
    return str(value).strip().lower()


def positive_env_float(name, default):
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        return float(default)
    return value if value > 0 else float(default)


def render_add_data_panel():
    with st.expander("Add data", expanded=False):
        with st.form("dashboard_import_data"):
            uploaded_files = st.file_uploader(
                "Crawled data files",
                type=["csv", "json", "jsonl", "ndjson"],
                accept_multiple_files=True,
            )
            source_label = st.selectbox(
                "Source",
                ["Auto-detect", "YouTube", "X", "Reddit"],
            )
            trigger_pipeline = st.checkbox(
                "Run lakehouse import pipeline",
                value=True,
            )
            submitted = st.form_submit_button(
                "Import files",
                type="primary",
                icon=":material/upload_file:",
                width="stretch",
            )

        if submitted:
            if not uploaded_files:
                st.error("Upload at least one crawled data file.")
                return

            selected_source = source_label.lower()
            source = "auto" if selected_source == "auto-detect" else selected_source
            imported_events = []
            try:
                for uploaded_file in uploaded_files:
                    imported_events.extend(
                        load_import_events(
                            uploaded_file.name,
                            uploaded_file.getvalue(),
                            source=source,
                        )
                    )
                published_counts = publish_events(
                    imported_events,
                    get_manual_import_config(),
                )
                message = f"Imported {len(imported_events):,} event(s): " + ", ".join(
                    f"{count:,} {source}" for source, count in published_counts.items()
                )
                if trigger_pipeline:
                    run = AirflowClient().trigger_dag(
                        MANUAL_IMPORT_DAG_ID,
                        {
                            "sources": list(published_counts),
                            "record_count": len(imported_events),
                        },
                    )
                    message += f" | Pipeline started: {run.get('dag_run_id', MANUAL_IMPORT_DAG_ID)}"
                else:
                    message += " | Pipeline not started"
                st.cache_data.clear()
                st.success(message)
            except Exception as exc:
                parsed_counts = summarize_events(imported_events)
                if parsed_counts:
                    st.warning(f"Parsed before failure: {parsed_counts}")
                st.error(f"Unable to import file data: {exc}")


def format_engagement_total(series):
    values = series.dropna()
    if values.empty:
        return "N/A"
    return f"{int(values.sum()):,}"


def latest_known_value(series):
    values = series.dropna()
    if values.empty:
        return pd.NA
    return values.iloc[-1]


def format_metric_cell(value):
    if pd.isna(value):
        return "N/A"
    return f"{int(value):,}"


def format_datetime_cell(value):
    if pd.isna(value):
        return "N/A"
    return value.strftime("%Y-%m-%d %H:%M")


def build_engagement_by_source(dataframe):
    summary_columns = [
        "Source",
        "Events",
        "Metadata rows",
        "Metadata coverage",
        "Latest metadata",
        *ENGAGEMENT_LABELS.values(),
    ]
    if dataframe.empty or "source" not in dataframe.columns:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(
            columns=["Source", *ENGAGEMENT_LABELS.values()]
        )

    source_rows = dataframe.copy()
    for column in ENGAGEMENT_COLUMNS:
        if column not in source_rows.columns:
            source_rows[column] = pd.NA
        availability_column = f"{column}_available"
        source_rows[column] = source_rows.apply(
            lambda row, metric=column, flag=availability_column: (
                row.get(metric) if metric_is_available(row.get(metric), row.get(flag)) else pd.NA
            ),
            axis=1,
        )
    if "metadata_refreshed_at" not in source_rows.columns:
        source_rows["metadata_refreshed_at"] = pd.NaT

    sort_columns = [
        column
        for column in ("created_at", "metadata_refreshed_at")
        if column in source_rows.columns
    ]
    if sort_columns:
        source_rows = source_rows.sort_values(sort_columns, na_position="first")

    grouped = source_rows.groupby("source", dropna=False)
    summed = grouped[list(SUMMED_ENGAGEMENT_COLUMNS)].sum(min_count=1)
    profile = grouped[list(PROFILE_ENGAGEMENT_COLUMNS)].agg(latest_known_value)
    metrics = pd.concat([summed, profile], axis=1)
    metrics = (
        metrics[list(ENGAGEMENT_COLUMNS)]
        .reset_index()
        .rename(columns={"source": "Source", **ENGAGEMENT_LABELS})
    )

    observed = grouped.agg(
        Events=("source", "size"),
        **{
            "Metadata rows": (
                "metadata_refreshed_at",
                lambda series: int(series.notna().sum()),
            ),
            "Latest metadata": ("metadata_refreshed_at", "max"),
        },
    )
    observed["Metadata coverage"] = (
        observed["Metadata rows"].astype(str) + "/" + observed["Events"].astype(str)
    )
    observed = observed.reset_index().rename(columns={"source": "Source"})
    raw_summary = observed.merge(metrics, on="Source", how="left")
    display_summary = raw_summary.copy()
    display_summary["Latest metadata"] = display_summary["Latest metadata"].apply(
        format_datetime_cell
    )
    for column in ENGAGEMENT_LABELS.values():
        display_summary[column] = display_summary[column].apply(format_metric_cell)

    return display_summary[summary_columns], metrics


def normalize_collaborators(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if hasattr(value, "tolist"):
        return normalize_collaborators(value.tolist())
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        value = value.strip()
        return [value] if value else []
    return []


def format_collaborators(value):
    collaborators = normalize_collaborators(value)
    if collaborators is None:
        return "N/A"
    if not collaborators:
        return "None"
    return ", ".join(collaborators)


def collaborator_count(value):
    collaborators = normalize_collaborators(value)
    return pd.NA if collaborators is None else len(collaborators)


def youtube_video_key(dataframe):
    if "platform_event_id" in dataframe.columns:
        platform_ids = dataframe["platform_event_id"].astype("string")
    else:
        platform_ids = pd.Series(pd.NA, index=dataframe.index, dtype="string")
    urls = dataframe["url"].astype("string") if "url" in dataframe else platform_ids
    return platform_ids.fillna(urls)


def deduplicate_youtube_videos(dataframe):
    if "source" not in dataframe.columns:
        return dataframe.copy()

    youtube_rows = dataframe[dataframe["source"] == "youtube"].copy()
    if youtube_rows.empty:
        return youtube_rows

    youtube_rows["_video_key"] = youtube_video_key(youtube_rows)
    sort_columns = [
        column
        for column in ("created_at", "metadata_refreshed_at")
        if column in youtube_rows.columns
    ]
    youtube_rows = (
        youtube_rows.sort_values(
            sort_columns,
            ascending=False,
            na_position="last",
        )
        if sort_columns
        else youtube_rows
    )
    return youtube_rows.drop_duplicates("_video_key").drop(columns=["_video_key"])


def build_analytics_rows(dataframe):
    if "source" not in dataframe.columns:
        return dataframe.copy()

    youtube_rows = deduplicate_youtube_videos(dataframe)
    other_rows = dataframe[dataframe["source"] != "youtube"].copy()
    return pd.concat([other_rows, youtube_rows], ignore_index=True)


def prepare_dashboard_dataframe(dataframe):
    prepared = dataframe.copy()
    for column in ENGAGEMENT_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = pd.NA
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    for column in OPTIONAL_DASHBOARD_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared["metadata_refreshed_at"] = pd.to_datetime(
        prepared["metadata_refreshed_at"],
        errors="coerce",
        utc=True,
    )
    return prepared


def build_user_tracking_rows(dataframe):
    tracking_frames = []
    analytics_rows = build_analytics_rows(dataframe)

    if "author_hash" in analytics_rows.columns:
        author_rows = analytics_rows.dropna(subset=["author_hash"]).copy()
        if not author_rows.empty:
            author_rows["tracked_identifier"] = author_rows["author_hash"].astype("string")
            author_rows["identifier_role"] = "Author"
            tracking_frames.append(author_rows)

    if {"source", "owner_channel_id"}.issubset(analytics_rows.columns):
        owner_rows = analytics_rows[
            (analytics_rows["source"] == "youtube") & analytics_rows["owner_channel_id"].notna()
        ].copy()
        if not owner_rows.empty:
            owner_rows["tracked_identifier"] = owner_rows["owner_channel_id"].astype("string")
            owner_rows["identifier_role"] = "Owner YouTube"
            tracking_frames.append(owner_rows)

    if {"source", "collaborator_channel_ids"}.issubset(analytics_rows.columns):
        collaborator_records = []
        youtube_rows = analytics_rows[analytics_rows["source"] == "youtube"]
        for _, row in youtube_rows.iterrows():
            collaborators = normalize_collaborators(row.get("collaborator_channel_ids"))
            if not collaborators:
                continue
            for collaborator in collaborators:
                collaborator_row = row.copy()
                collaborator_row["tracked_identifier"] = collaborator
                collaborator_row["identifier_role"] = "YouTube collaborator"
                collaborator_records.append(collaborator_row)

        if collaborator_records:
            tracking_frames.append(pd.DataFrame(collaborator_records))

    if not tracking_frames:
        return pd.DataFrame(columns=[*dataframe.columns, "identifier_role"])

    tracking_df = pd.concat(tracking_frames, ignore_index=True)
    tracking_df["author_hash"] = tracking_df["tracked_identifier"].astype("string")
    return tracking_df.drop(columns=["tracked_identifier"])


def format_optional_text(value):
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except (TypeError, ValueError):
        pass
    value = str(value).strip()
    if not value or value.lower() in {"none", "nan", "<na>"}:
        return "N/A"
    return value


def format_count(value):
    if pd.isna(value):
        return "N/A"
    return f"{int(value):,}"


def format_rate(value):
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.1f}%"


def normalize_export_value(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return json.dumps([str(item) for item in value], ensure_ascii=False)
    if hasattr(value, "tolist"):
        return normalize_export_value(value.tolist())
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def prepare_python_export(dataframe):
    export_df = dataframe.copy()
    for column in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[column]):
            export_df[column] = export_df[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif export_df[column].dtype == "object":
            export_df[column] = export_df[column].map(normalize_export_value)
    return export_df


def dataframe_to_parquet_bytes(dataframe):
    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    return buffer.getvalue()


def render_python_export(raw_dataframe, analytics_dataframe):
    with st.expander("Export data for Python", expanded=False):
        dataset_label = st.radio(
            "Dataset",
            options=[
                "Filtered events",
                "Analytics rows",
            ],
            horizontal=True,
        )
        selected_df = analytics_dataframe if dataset_label == "Analytics rows" else raw_dataframe
        export_df = prepare_python_export(selected_df)
        file_slug = "analytics_rows" if dataset_label == "Analytics rows" else "filtered_events"

        metric_columns = st.columns(3)
        metric_columns[0].metric("Rows", f"{len(export_df):,}")
        metric_columns[1].metric("Columns", f"{len(export_df.columns):,}")
        metric_columns[2].metric(
            "Sources",
            f"{export_df['source'].dropna().nunique():,}"
            if "source" in export_df.columns
            else "N/A",
        )

        download_columns = st.columns(3)
        download_columns[0].download_button(
            "CSV",
            data=export_df.to_csv(index=False).encode("utf-8"),
            file_name=f"{file_slug}.csv",
            mime="text/csv",
            width="stretch",
        )
        download_columns[1].download_button(
            "JSONL",
            data=export_df.to_json(
                orient="records",
                lines=True,
                force_ascii=False,
            ).encode("utf-8"),
            file_name=f"{file_slug}.jsonl",
            mime="application/x-ndjson",
            width="stretch",
        )
        try:
            parquet_data = dataframe_to_parquet_bytes(export_df)
            parquet_disabled = False
            parquet_help = None
        except ImportError:
            parquet_data = b""
            parquet_disabled = True
            parquet_help = "Install pyarrow to enable Parquet export."
        download_columns[2].download_button(
            "Parquet",
            data=parquet_data,
            file_name=f"{file_slug}.parquet",
            mime="application/vnd.apache.parquet",
            disabled=parquet_disabled,
            help=parquet_help,
            width="stretch",
        )

        st.code(
            f"""import pandas as pd

df = pd.read_parquet("{file_slug}.parquet")
df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
print(df.shape)
print(df.head())""",
            language="python",
        )


st.set_page_config(
    page_title="Social Media Lakehouse Dashboard",
    layout="wide",
)

render_navigation()

st.title("Social Media Lakehouse Dashboard")
st.caption("Monitoring cleaned events received in the Iceberg Silver table")


@st.cache_data(ttl=60, show_spinner="Reading the Iceberg Silver table...")
def get_data():
    return load_iceberg_data()


@st.cache_data(ttl=10, show_spinner=False)
def get_airflow_status():
    return AirflowClient().load_status()


@st.cache_data(ttl=30, show_spinner=False)
def get_recent_collector_runs():
    limit = int(os.getenv("DASHBOARD_COLLECTOR_RUN_HISTORY_LIMIT", "5"))
    return AirflowClient().load_recent_collector_runs(limit=limit)


@st.cache_data(ttl=60, show_spinner=False)
def get_balancing_report():
    report_path = Path(os.getenv("DASHBOARD_BALANCING_REPORT_PATH", DEFAULT_BALANCING_REPORT_PATH))
    if not report_path.is_file():
        return None
    with report_path.open(encoding="utf-8") as report_file:
        report = json.load(report_file)
    report["_report_path"] = str(report_path)
    report["_modified_at"] = pd.Timestamp(report_path.stat().st_mtime, unit="s")
    return report


@st.cache_data(ttl=60, show_spinner=False)
def get_model_pipeline_tables():
    tables = {}
    errors = {}
    for label, (namespace, table_name) in MODEL_PIPELINE_TABLES.items():
        dataframe, error = load_optional_iceberg_table(
            namespace,
            table_name,
            config,
            limit=5000,
        )
        tables[label] = dataframe
        if error:
            errors[label] = error
    return tables, errors


@st.cache_data(ttl=60, show_spinner=False)
def get_content_analytics_tables():
    tables = {}
    errors = {}
    for label, (namespace, table_name) in CONTENT_ANALYTICS_TABLES.items():
        dataframe, error = load_optional_iceberg_table(
            namespace,
            table_name,
            config,
            limit=10000,
        )
        tables[label] = dataframe
        if error:
            errors[label] = error
    return tables, errors


@st.fragment(run_every="15s")
def render_airflow_monitoring():
    st.subheader("Airflow orchestration")

    try:
        status = get_airflow_status()
    except Exception as exc:
        st.warning(f"Airflow monitoring unavailable: {exc}")
        return

    active_runs = status["active_runs"]
    next_runs = status["next_runs"]

    if active_runs:
        st.caption(f"{len(active_runs)} active job(s)")
        for run in active_runs:
            label = (
                f"{run['dag_id']} - {run['state']} - "
                f"{run['completed_tasks']}/{run['total_tasks']} tasks"
            )
            st.write(f"**{label}**")
            st.progress(
                run["progress"],
                text=f"{run['progress']} % complete",
            )
            if run["failed_tasks"]:
                st.error(f"{run['failed_tasks']} failed task(s) in this run")
    else:
        st.success("No Airflow job currently running")

    if next_runs:
        next_run = next_runs[0]
        metric_columns = st.columns(3)
        metric_columns[0].metric(
            "Next job",
            next_run["dag_id"],
        )
        metric_columns[1].metric(
            "Scheduled in",
            next_run["countdown"],
        )
        metric_columns[2].metric(
            "Scheduled time",
            next_run["next_run"].astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        )

        schedule_rows = [
            {
                "Job": run["dag_id"],
                "Next run": run["next_run"].astimezone().strftime("%Y-%m-%d %H:%M:%S"),
                "In": run["countdown"],
            }
            for run in next_runs
        ]
        st.dataframe(
            schedule_rows,
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("No next run scheduled")

    try:
        collector_runs = get_recent_collector_runs()
    except Exception as exc:
        st.warning(f"Collector run history unavailable: {exc}")
        collector_runs = []

    if collector_runs:
        st.subheader("Last collector runs")
        blocked_runs = [row for row in collector_runs if row["collector_status"] == "blocked"]
        if blocked_runs:
            st.warning(f"{len(blocked_runs)} collector block(s) found in recent runs")
        collector_rows = [
            {
                "Started": (
                    row["started_at"].astimezone().strftime("%Y-%m-%d %H:%M:%S")
                    if row["started_at"]
                    else "-"
                ),
                "DAG": row["dag_id"],
                "Source": row["source"],
                "Task": row["task_state"] or "-",
                "Collector": row["collector_status"],
                "Message": row["message"],
                "Run": row["run_id"],
            }
            for row in collector_runs
        ]
        st.dataframe(
            collector_rows,
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("No recent collector runs found")

    st.caption(
        "Auto-refresh every 15 seconds. "
        f"Last check: "
        f"{status['checked_at'].astimezone().strftime('%H:%M:%S')}"
    )


config = get_iceberg_config()

st.sidebar.header("Data")
st.sidebar.caption(f"Table: `{config['table_path']}`")
st.sidebar.caption(f"MinIO: `{config['endpoint_url']}`")

if st.sidebar.button("Refresh data", width="stretch"):
    st.cache_data.clear()
    st.rerun()

try:
    df = get_data()
except RuntimeError as exc:
    st.error(str(exc))
    st.info(
        "Start at least MinIO with "
        "`docker compose up -d minio minio-init`, then restart the dashboard."
    )
    st.stop()

if df.empty:
    st.warning("The Iceberg Silver table is accessible but contains no events.")
    st.stop()

df = prepare_dashboard_dataframe(df)

st.sidebar.header("Filters")

available_sources = sorted(df["source"].dropna().unique())
sources = st.sidebar.multiselect(
    "Sources",
    options=available_sources,
    default=available_sources,
)

df_filtered = df[df["source"].isin(sources)].copy()
df_dated = df_filtered.dropna(subset=["created_at"]).copy()

if not df_dated.empty:
    min_date = df_dated["created_at"].min().date()
    max_date = df_dated["created_at"].max().date()
    date_range = st.sidebar.date_input(
        "Period",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            df_filtered["created_at"].isna()
            | (
                (df_filtered["created_at"].dt.date >= start_date)
                & (df_filtered["created_at"].dt.date <= end_date)
            )
        ]
else:
    st.sidebar.warning("No valid date for the current selection.")

analytics_df = build_analytics_rows(df_filtered)


def render_overview_summary():
    total_records = len(analytics_df)
    latest_activity = analytics_df["created_at"].max()
    unique_identifiers = analytics_df["author_hash"].dropna().nunique()
    avg_text_words = analytics_df["text_len_words"].mean() if total_records else 0
    missing_timestamps_pct = analytics_df["created_at"].isna().mean() * 100 if total_records else 0
    pipeline_error_pct = (
        analytics_df["error"].fillna("").str.strip().ne("").mean() * 100 if total_records else 0
    )

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Events", f"{total_records:,}")
    c2.metric(
        "Last activity",
        latest_activity.strftime("%Y-%m-%d %H:%M") if pd.notna(latest_activity) else "N/A",
    )
    c3.metric("Unique identifiers", f"{unique_identifiers:,}")
    c4.metric("Average length", f"{avg_text_words:.1f} words")
    c5.metric("Missing dates", f"{missing_timestamps_pct:.1f}%")
    c6.metric("Pipeline errors", f"{pipeline_error_pct:.1f}%")


def render_engagement_metadata():
    st.subheader("Engagement metadata")
    engagement_metrics = st.columns(len(ENGAGEMENT_LABELS))
    for metric, (column, label) in zip(
        engagement_metrics,
        ENGAGEMENT_LABELS.items(),
    ):
        metric.metric(label, format_engagement_total(analytics_df[column]))

    engagement_by_source, engagement_by_source_metrics = build_engagement_by_source(analytics_df)
    st.dataframe(
        engagement_by_source,
        width="stretch",
        hide_index=True,
        column_config={
            "Events": st.column_config.NumberColumn("Events", format="%d"),
            "Metadata rows": st.column_config.NumberColumn(
                "Metadata rows",
                format="%d",
            ),
        },
    )
    st.caption(
        "N/A values are metrics unavailable for the platform or not collected "
        "yet. Follower, subscriber and subreddit member counts use the latest "
        "known value instead of a sum."
    )

    st.caption("Note: YouTube metrics are grouped by video before aggregation.")

    common_engagement_columns = [
        "like_count",
        "view_count",
        "comment_count",
        "reply_count",
    ]
    engagement_chart_data = (
        engagement_by_source_metrics.rename(
            columns={ENGAGEMENT_LABELS[column]: column for column in common_engagement_columns}
        )
        .melt(
            id_vars="Source",
            value_vars=common_engagement_columns,
            var_name="metric",
            value_name="value",
        )
        .dropna(subset=["value"])
    )
    engagement_chart_data["metric"] = engagement_chart_data["metric"].map(ENGAGEMENT_LABELS)

    if not engagement_chart_data.empty:
        fig_engagement = px.bar(
            engagement_chart_data,
            x="Source",
            y="value",
            color="metric",
            barmode="group",
            labels={"value": "Total", "metric": "Metric"},
        )
        st.plotly_chart(fig_engagement, width="stretch")


def render_youtube_authors():
    st.subheader("YouTube authors and collaborations")
    youtube_df = deduplicate_youtube_videos(df_filtered)
    if youtube_df.empty:
        st.info("No YouTube event in the current selection.")
    else:
        youtube_df["collaborator_count"] = youtube_df["collaborator_channel_ids"].apply(
            collaborator_count
        )
        youtube_df["collaborators"] = youtube_df["collaborator_channel_ids"].apply(
            format_collaborators
        )

        known_collaborators = youtube_df["collaborator_count"].dropna()
        videos_with_collaborators = (
            f"{int((known_collaborators > 0).sum()):,}" if not known_collaborators.empty else "N/A"
        )
        distinct_collaborator_ids = sorted(
            {
                collaborator
                for value in youtube_df["collaborator_channel_ids"]
                for collaborator in (normalize_collaborators(value) or [])
            }
        )

        yt_metrics = st.columns(4)
        yt_metrics[0].metric("YouTube videos", f"{len(youtube_df):,}")
        yt_metrics[1].metric(
            "Known owners",
            f"{youtube_df['owner_channel_id'].dropna().nunique():,}",
        )
        yt_metrics[2].metric(
            "With collaborators",
            videos_with_collaborators,
        )
        yt_metrics[3].metric(
            "Unique collaborators",
            (f"{len(distinct_collaborator_ids):,}" if not known_collaborators.empty else "N/A"),
        )

        youtube_author_rows = youtube_df.sort_values("created_at", ascending=False)[
            [
                "created_at",
                "text",
                "owner_channel_id",
                "collaborator_count",
                "collaborators",
                "url",
            ]
        ].head(100)
        youtube_author_rows["created_at"] = youtube_author_rows["created_at"].dt.strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        youtube_author_rows["created_at"] = youtube_author_rows["created_at"].fillna("N/A")
        youtube_author_rows["text"] = youtube_author_rows["text"].fillna("").str.slice(0, 180)
        youtube_author_rows["owner_channel_id"] = youtube_author_rows["owner_channel_id"].apply(
            format_optional_text
        )
        youtube_author_rows["collaborator_count"] = youtube_author_rows["collaborator_count"].apply(
            format_available_metric
        )
        st.dataframe(
            youtube_author_rows,
            width="stretch",
            hide_index=True,
            column_config={
                "created_at": "Timestamp",
                "text": "Title",
                "owner_channel_id": "Owner channel ID",
                "collaborator_count": st.column_config.TextColumn("Collaborators"),
                "collaborators": "Collaborator channel IDs",
                "url": st.column_config.LinkColumn("URL"),
            },
        )
        st.caption(
            "`N/A` means the YouTube page did not allow confirming the "
            "collaborator list. `None` means the video was read and no "
            "accepted collaborator was found."
        )


def render_source_activity():
    st.subheader("Events by source")
    source_counts = (
        analytics_df.groupby("source")
        .size()
        .reset_index(name="records")
        .sort_values("records", ascending=False)
    )

    if not source_counts.empty:
        fig_source = px.bar(
            source_counts,
            x="source",
            y="records",
            color="source",
            labels={"source": "Source", "records": "Events"},
        )
        st.plotly_chart(fig_source, width="stretch")
    else:
        st.info("No event for the current filters.")

    st.subheader("Activity over time")
    df_time = analytics_df.dropna(subset=["created_at"]).copy()

    if not df_time.empty:
        df_time["date"] = df_time["created_at"].dt.date
        time_counts = df_time.groupby(["date", "source"]).size().reset_index(name="records")
        fig_time = px.line(
            time_counts,
            x="date",
            y="records",
            color="source",
            markers=True,
            labels={"date": "Date", "records": "Events", "source": "Source"},
        )
        st.plotly_chart(fig_time, width="stretch")
    else:
        st.info("No valid timestamp for the current filters.")


def render_balancing_report():
    st.subheader("Dataset balanced by source")
    balancing_report = get_balancing_report()
    if not balancing_report:
        st.info(
            "No balancing report available. Run the DAG "
            "`build_balanced_comment_dataset` to generate "
            "`data/balancing/report.json`."
        )
    else:
        balance_metrics = st.columns(5)
        balance_metrics[0].metric(
            "Source rows",
            format_count(balancing_report.get("total_before")),
        )
        balance_metrics[1].metric(
            "Balanced rows",
            format_count(balancing_report.get("total_after")),
        )
        balance_metrics[2].metric(
            "Target per source",
            format_count(balancing_report.get("effective_target_per_group")),
        )
        balance_metrics[3].metric("Seed", balancing_report.get("seed", "N/A"))
        balance_metrics[4].metric(
            "Dimensions",
            ", ".join(balancing_report.get("dimensions", [])) or "N/A",
        )

        st.caption(
            "Target table: "
            f"`{balancing_report.get('output_table', 'N/A')}` | "
            f"Report: `{balancing_report.get('_report_path')}` | "
            "Last generation: "
            f"{balancing_report.get('_modified_at').strftime('%Y-%m-%d %H:%M:%S')}"
        )
        constraints = balancing_report.get("constraints") or []
        if constraints:
            st.warning("Constraints: " + " | ".join(constraints))

        before_distribution = pd.DataFrame(balancing_report.get("distribution_before", []))
        after_distribution = pd.DataFrame(balancing_report.get("distribution_after", []))
        if not before_distribution.empty and not after_distribution.empty:
            dimensions = balancing_report.get("dimensions", [])
            before_distribution["dataset"] = "Before"
            after_distribution["dataset"] = "After"
            distribution = pd.concat(
                [before_distribution, after_distribution],
                ignore_index=True,
            )
            if dimensions:
                distribution["group"] = (
                    distribution[dimensions]
                    .astype(str)
                    .agg(
                        " | ".join,
                        axis=1,
                    )
                )
            else:
                distribution["group"] = "all"
            fig_balance = px.bar(
                distribution,
                x="group",
                y="count",
                color="dataset",
                barmode="group",
                labels={
                    "group": "Source",
                    "count": "Rows",
                    "dataset": "Dataset",
                },
            )
            fig_balance.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_balance, width="stretch")

            distribution_table = distribution[[*dimensions, "dataset", "count"]].rename(
                columns={
                    "source": "Source",
                    "engagement_band": "Engagement",
                    "comment_type": "Comment type",
                    "dataset": "Dataset",
                    "count": "Rows",
                }
            )
            st.dataframe(
                distribution_table,
                width="stretch",
                hide_index=True,
                column_config={
                    "Rows": st.column_config.NumberColumn(format="%d"),
                },
            )


def render_identifier_tracking():
    st.subheader("Identifier tracking")
    user_tracking_df = build_user_tracking_rows(df_filtered)

    if user_tracking_df.empty:
        st.info("No identifier available for the current filters.")
    else:
        role_summary = (
            user_tracking_df.groupby("identifier_role")["author_hash"]
            .nunique()
            .reindex(TRACKING_ROLE_ORDER, fill_value=0)
        )
        role_metrics = st.columns(len(TRACKING_ROLE_ORDER))
        for index, role in enumerate(TRACKING_ROLE_ORDER):
            role_metrics[index].metric(role, format_count(role_summary[role]))

        available_roles = sorted(
            set(user_tracking_df["identifier_role"].dropna().unique().tolist())
            - set(TRACKING_ROLE_ORDER)
        )
        role_options = [*TRACKING_ROLE_ORDER, *available_roles]
        selected_roles = st.multiselect(
            "Identifier types to include",
            options=role_options,
            default=role_options,
        )
        user_tracking_df = user_tracking_df[
            user_tracking_df["identifier_role"].isin(selected_roles)
        ].copy()

        if user_tracking_df.empty:
            st.info("No identifier available for these types.")
        else:
            st.caption(
                "YouTube owners and collaborators are included as "
                "contribution identifiers separate from comment authors."
            )

        for column in ENGAGEMENT_COLUMNS:
            user_tracking_df[column] = pd.to_numeric(
                user_tracking_df[column],
                errors="coerce",
            )

        user_activity = (
            user_tracking_df.groupby("author_hash")
            .agg(
                events=("author_hash", "size"),
                sources=(
                    "source",
                    lambda s: ", ".join(sorted(s.dropna().unique())),
                ),
                roles=(
                    "identifier_role",
                    lambda s: ", ".join(sorted(s.dropna().unique())),
                ),
                first_activity=("created_at", "min"),
                last_activity=("created_at", "max"),
                active_days=(
                    "created_at",
                    lambda s: s.dropna().dt.date.nunique(),
                ),
                avg_text_words=("text_len_words", "mean"),
                events_with_replies=(
                    "reply_count",
                    lambda s: (s.fillna(0) > 0).sum(),
                ),
                reply_observations=("reply_count", lambda s: s.notna().sum()),
            )
            .reset_index()
        )

        for column in ENGAGEMENT_COLUMNS:
            totals = (
                user_tracking_df.groupby("author_hash")[column]
                .sum(min_count=1)
                .rename(f"total_{column}")
            )
            averages = (
                user_tracking_df.groupby("author_hash")[column].mean().rename(f"avg_{column}")
            )
            user_activity = user_activity.merge(
                totals,
                on="author_hash",
                how="left",
            ).merge(
                averages,
                on="author_hash",
                how="left",
            )

        total_columns = [f"total_{column}" for column in ENGAGEMENT_COLUMNS]
        user_activity["total_engagement"] = user_activity[total_columns].fillna(0).sum(axis=1)
        user_activity["avg_engagement_per_event"] = (
            user_activity["total_engagement"] / user_activity["events"]
        )
        user_activity["reply_rate_pct"] = (
            user_activity["events_with_replies"]
            / user_activity["reply_observations"].replace(0, pd.NA)
            * 100
        )
        user_activity["author_display"] = user_activity["author_hash"].str.slice(
            0,
            12,
        )

        ranking_options = {
            "events": "Activity",
            "total_engagement": "Engagement total",
            "avg_engagement_per_event": "Average engagement",
            "total_like_count": "Likes",
            "total_view_count": "Views",
            "total_reply_count": "Replies",
            "reply_rate_pct": "Reply rate",
        }
        ranking_metric = st.selectbox(
            "Rank identifiers by",
            options=list(ranking_options),
            format_func=ranking_options.get,
        )

        top_users = (
            user_activity.sort_values(
                [ranking_metric, "events"],
                ascending=False,
                na_position="last",
            )
            .head(25)
            .copy()
        )
        top_users_table = top_users[
            [
                "author_display",
                "roles",
                "sources",
                "events",
                "active_days",
                "reply_rate_pct",
                "total_like_count",
                "total_view_count",
                "total_reply_count",
                "total_comment_count",
                "total_engagement",
                "avg_engagement_per_event",
                "last_activity",
            ]
        ].rename(
            columns={
                "author_display": "Identifier",
                "roles": "Type",
                "sources": "Sources",
                "events": "Events",
                "active_days": "Active days",
                "reply_rate_pct": "Reply rate (%)",
                "total_like_count": "Likes",
                "total_view_count": "Views",
                "total_reply_count": "Replies",
                "total_comment_count": "Comments",
                "total_engagement": "Engagement total",
                "avg_engagement_per_event": "Average engagement",
                "last_activity": "Last activity",
            }
        )
        st.dataframe(
            top_users_table,
            width="stretch",
            hide_index=True,
            column_config={
                "Events": st.column_config.NumberColumn(format="%d"),
                "Active days": st.column_config.NumberColumn(format="%d"),
                "Reply rate (%)": st.column_config.NumberColumn(format="%.1f"),
                "Likes": st.column_config.NumberColumn(format="%d"),
                "Views": st.column_config.NumberColumn(format="%d"),
                "Replies": st.column_config.NumberColumn(format="%d"),
                "Comments": st.column_config.NumberColumn(format="%d"),
                "Engagement total": st.column_config.NumberColumn(format="%d"),
                "Average engagement": st.column_config.NumberColumn(format="%.1f"),
            },
        )

        selector_options = top_users["author_hash"].tolist()
        if selector_options:
            selector_labels = {
                row.author_hash: (
                    f"{row.author_display} - {int(row.events)} event(s) - {row.roles}"
                )
                for row in top_users.itertuples()
            }
            selected_author = st.selectbox(
                "Identifier to track",
                options=selector_options,
                format_func=selector_labels.get,
            )
            selected_user = user_activity[user_activity["author_hash"] == selected_author].iloc[0]
            selected_events = user_tracking_df[
                user_tracking_df["author_hash"] == selected_author
            ].copy()

            user_metrics = st.columns(6)
            user_metrics[0].metric("Events", format_count(selected_user.events))
            user_metrics[1].metric(
                "Active days",
                format_count(selected_user.active_days),
            )
            user_metrics[2].metric(
                "Reply rate",
                format_rate(selected_user.reply_rate_pct),
            )
            user_metrics[3].metric(
                "Likes",
                format_count(selected_user.total_like_count),
            )
            user_metrics[4].metric(
                "Views",
                format_count(selected_user.total_view_count),
            )
            user_metrics[5].metric(
                "Average engagement",
                f"{selected_user.avg_engagement_per_event:.1f}",
            )

            dated_user_events = selected_events.dropna(subset=["created_at"]).copy()
            if not dated_user_events.empty:
                dated_user_events["date"] = dated_user_events["created_at"].dt.date
                daily_user_progress = (
                    dated_user_events.groupby("date")
                    .agg(
                        events=("author_hash", "size"),
                        likes=("like_count", "sum"),
                        views=("view_count", "sum"),
                        replies=("reply_count", "sum"),
                        comments=("comment_count", "sum"),
                    )
                    .reset_index()
                    .sort_values("date")
                )
                cumulative_columns = {
                    "events": "Cumulative events",
                    "likes": "Cumulative likes",
                    "views": "Cumulative views",
                    "replies": "Cumulative replies",
                    "comments": "Cumulative comments",
                }
                for column in cumulative_columns:
                    daily_user_progress[f"cumulative_{column}"] = (
                        daily_user_progress[column].fillna(0).cumsum()
                    )
                progress_chart = daily_user_progress.melt(
                    id_vars="date",
                    value_vars=[f"cumulative_{column}" for column in cumulative_columns],
                    var_name="metric",
                    value_name="value",
                )
                progress_chart["metric"] = progress_chart["metric"].replace(
                    {f"cumulative_{column}": label for column, label in cumulative_columns.items()}
                )
                fig_user_progress = px.line(
                    progress_chart,
                    x="date",
                    y="value",
                    color="metric",
                    markers=True,
                    labels={
                        "date": "Date",
                        "value": "Cumulative total",
                        "metric": "Metric",
                    },
                )
                st.plotly_chart(fig_user_progress, width="stretch")
            else:
                st.info("No valid date for this identifier.")

            selected_recent = selected_events.sort_values(
                "created_at",
                ascending=False,
            ).head(25)
            selected_recent = selected_recent[
                [
                    "source",
                    "identifier_role",
                    "created_at",
                    "text",
                    "like_count",
                    "view_count",
                    "comment_count",
                    "reply_count",
                    "url",
                ]
            ].copy()
            selected_recent["created_at"] = selected_recent["created_at"].dt.strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            selected_recent["text"] = selected_recent["text"].fillna("").str.slice(0, 180)
            st.dataframe(
                selected_recent,
                width="stretch",
                hide_index=True,
                column_config={
                    "source": "Source",
                    "identifier_role": "Type",
                    "created_at": "Timestamp",
                    "text": "Content",
                    "like_count": st.column_config.NumberColumn(
                        "Likes",
                        format="%d",
                    ),
                    "view_count": st.column_config.NumberColumn(
                        "Views",
                        format="%d",
                    ),
                    "comment_count": st.column_config.NumberColumn(
                        "Comments",
                        format="%d",
                    ),
                    "reply_count": st.column_config.NumberColumn(
                        "Replies",
                        format="%d",
                    ),
                    "url": st.column_config.LinkColumn("URL"),
                },
            )
        st.caption(
            "Reply rate is the share of events for this "
            "identifier with at least one observed reply. Metrics "
            "still depend on what each platform exposes."
        )


def prepare_optional_table(dataframe):
    prepared = dataframe.copy()
    for column in (
        "event_ts",
        "created_at",
        "observed_at",
        "snapshot_at",
        "latest_snapshot_at",
        "last_discovered_at",
        "last_enriched_at",
        "last_attempt_at",
        "next_attempt_at",
        "collected_at",
        "updated_at",
        "recovered_at",
        "retrieved_at",
        "prediction_ts",
    ):
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(
                prepared[column],
                errors="coerce",
                utc=True,
            )
    for column in (
        "text_len_chars",
        "text_len_words",
        "has_question",
        "hashtag_count",
        "mention_count",
        "url_count",
        "emoji_count",
        "age_minutes",
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
        "subreddit_weekly_visitors",
        "subreddit_weekly_contributions",
        "recent_posts_1h",
        "top_similarity",
        "avg_similarity_top10",
        "trend_growth_1h",
        "trend_growth_24h",
        "topic_freshness_hours",
        "confidence",
        "virality_score",
        "attempt_count",
    ):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    return prepared


def render_optional_table_status(label, dataframe, error):
    if dataframe.empty:
        st.info(f"`{label}` is not available yet.")
        if error:
            with st.expander(f"{label} read details", expanded=False):
                st.caption(error)
        return False
    return True


def render_live_ai_prediction():
    st.subheader("Live virality prediction")
    try:
        health = get_ai_server_health()
        model_state = "loaded" if health.get("model_loaded") else "available (cold)"
        st.success(f"AI server connected — model {model_state}")
    except AIServerError as exc:
        st.warning(f"AI server unavailable: {exc}")

    with st.form("live_ai_prediction"):
        source = st.selectbox(
            "Source",
            options=["youtube", "x", "reddit", ""],
            format_func=lambda value: value or "unspecified",
        )
        text = st.text_area(
            "Post text",
            placeholder="Paste the social-media post to score...",
            height=140,
        )
        audience = st.number_input(
            "Known audience (optional)",
            min_value=0,
            value=0,
            step=1,
        )
        submitted = st.form_submit_button("Predict virality")

    if not submitted:
        return
    if not text.strip():
        st.error("Enter post text before requesting a prediction.")
        return

    try:
        result = predict_post(
            text.strip(),
            source,
            float(audience) if audience else None,
        )
    except AIServerError as exc:
        st.error(f"Prediction failed: {exc}")
        return

    metrics = st.columns(3)
    metrics[0].metric("Viral score", f"{float(result['viral_score']):.1%}")
    metrics[1].metric("Label", str(result["label"]))
    confidence = result.get("confidence")
    metrics[2].metric(
        "Confidence",
        f"{float(confidence):.1%}" if confidence is not None else "N/A",
    )
    st.write(str(result.get("explanation_text", "")))

    suggestions = result.get("suggestions") or []
    if suggestions:
        st.markdown("**Suggestions**")
        for suggestion in suggestions:
            st.write(f"- {suggestion}")

    factors = result.get("top_factors") or []
    if factors:
        st.dataframe(
            pd.DataFrame(factors),
            width="stretch",
            hide_index=True,
        )


def render_model_pipeline():
    render_live_ai_prediction()
    st.divider()
    st.subheader("Model pipeline")
    tables, errors = get_model_pipeline_tables()
    prepared_tables = {
        label: prepare_optional_table(dataframe) for label, dataframe in tables.items()
    }

    feature_df = prepared_tables["post_features"]
    snapshot_df = prepared_tables["engagement_snapshots"]
    context_df = prepared_tables["context_features"]
    prediction_df = prepared_tables["model_predictions"]
    training_df = prepared_tables["training_examples"]

    pipeline_metrics = st.columns(5)
    pipeline_metrics[0].metric("Post features", format_count(len(feature_df)))
    pipeline_metrics[1].metric("Engagement snapshots", format_count(len(snapshot_df)))
    pipeline_metrics[2].metric("Context rows", format_count(len(context_df)))
    pipeline_metrics[3].metric("Predictions", format_count(len(prediction_df)))
    pipeline_metrics[4].metric("Training examples", format_count(len(training_df)))

    st.caption(
        "These tables are optional lakehouse outputs from the classification "
        "pipeline. Missing tables mean the corresponding job or service has "
        "not produced data yet."
    )

    if render_optional_table_status(
        "silver.post_features",
        feature_df,
        errors.get("post_features"),
    ):
        st.subheader("Text feature layer")
        feature_metrics = st.columns(4)
        feature_metrics[0].metric(
            "Feature versions",
            format_count(feature_df["feature_version"].dropna().nunique())
            if "feature_version" in feature_df.columns
            else "N/A",
        )
        feature_metrics[1].metric(
            "Average words",
            f"{feature_df['text_len_words'].mean():.1f}"
            if "text_len_words" in feature_df.columns and feature_df["text_len_words"].notna().any()
            else "N/A",
        )
        feature_metrics[2].metric(
            "Posts with URLs",
            format_count((feature_df.get("url_count", pd.Series(dtype=float)).fillna(0) > 0).sum()),
        )
        feature_metrics[3].metric(
            "Posts with mentions",
            format_count(
                (feature_df.get("mention_count", pd.Series(dtype=float)).fillna(0) > 0).sum()
            ),
        )

        feature_columns = [
            column
            for column in (
                "source",
                "event_ts",
                "text_for_model",
                "text_len_words",
                "has_question",
                "hashtag_count",
                "mention_count",
                "url_count",
                "emoji_count",
                "feature_version",
            )
            if column in feature_df.columns
        ]
        st.dataframe(
            feature_df.sort_values(
                "event_ts",
                ascending=False,
                na_position="last",
            )[feature_columns].head(100),
            width="stretch",
            hide_index=True,
        )

    if render_optional_table_status(
        "silver.engagement_snapshots",
        snapshot_df,
        errors.get("engagement_snapshots"),
    ):
        st.subheader("Engagement snapshots")
        snapshot_metrics = st.columns(3)
        snapshot_metrics[0].metric(
            "Observed posts",
            format_count(snapshot_df["platform_event_id"].dropna().nunique())
            if "platform_event_id" in snapshot_df.columns
            else "N/A",
        )
        snapshot_metrics[1].metric(
            "Latest observation",
            snapshot_df["observed_at"].max().strftime("%Y-%m-%d %H:%M")
            if "observed_at" in snapshot_df.columns and pd.notna(snapshot_df["observed_at"].max())
            else "N/A",
        )
        snapshot_metrics[2].metric(
            "Median age",
            f"{snapshot_df['age_minutes'].median():.0f} min"
            if "age_minutes" in snapshot_df.columns and snapshot_df["age_minutes"].notna().any()
            else "N/A",
        )

        available_snapshot_metrics = [
            column
            for column in (
                "like_count",
                "view_count",
                "comment_count",
                "reply_count",
                "retweet_count",
                "bookmark_count",
                "score",
            )
            if column in snapshot_df.columns
        ]
        if available_snapshot_metrics:
            snapshot_chart = (
                snapshot_df.groupby("source")[available_snapshot_metrics]
                .sum(min_count=1)
                .reset_index()
                .melt(
                    id_vars="source",
                    var_name="metric",
                    value_name="value",
                )
                .dropna(subset=["value"])
            )
            if not snapshot_chart.empty:
                fig_snapshots = px.bar(
                    snapshot_chart,
                    x="source",
                    y="value",
                    color="metric",
                    barmode="group",
                    labels={
                        "source": "Source",
                        "value": "Observed total",
                        "metric": "Metric",
                    },
                )
                st.plotly_chart(fig_snapshots, width="stretch")

    if render_optional_table_status(
        "silver.context_features",
        context_df,
        errors.get("context_features"),
    ):
        st.subheader("Retrieval context features")
        context_columns = [
            column
            for column in (
                "source",
                "retrieved_at",
                "top_similarity",
                "avg_similarity_top10",
                "recent_posts_1h",
                "trend_growth_1h",
                "trend_growth_24h",
                "topic_freshness_hours",
                "matched_topics",
            )
            if column in context_df.columns
        ]
        st.dataframe(
            context_df.sort_values(
                "retrieved_at",
                ascending=False,
                na_position="last",
            )[context_columns].head(100),
            width="stretch",
            hide_index=True,
        )

    if render_optional_table_status(
        "gold.model_predictions",
        prediction_df,
        errors.get("model_predictions"),
    ):
        st.subheader("Gold predictions")
        prediction_metrics = st.columns(3)
        prediction_metrics[0].metric(
            "Classes",
            format_count(prediction_df["predicted_class"].dropna().nunique())
            if "predicted_class" in prediction_df.columns
            else "N/A",
        )
        prediction_metrics[1].metric(
            "Average confidence",
            f"{prediction_df['confidence'].mean():.2f}"
            if "confidence" in prediction_df.columns and prediction_df["confidence"].notna().any()
            else "N/A",
        )
        prediction_metrics[2].metric(
            "Context used",
            format_count(prediction_df["context_used"].fillna(False).sum())
            if "context_used" in prediction_df.columns
            else "N/A",
        )
        prediction_columns = [
            column
            for column in (
                "source",
                "prediction_ts",
                "model_version",
                "model_type",
                "predicted_class",
                "confidence",
                "virality_score",
                "context_used",
                "schema_version",
            )
            if column in prediction_df.columns
        ]
        st.dataframe(
            prediction_df.sort_values(
                "prediction_ts",
                ascending=False,
                na_position="last",
            )[prediction_columns].head(100),
            width="stretch",
            hide_index=True,
        )

    if render_optional_table_status(
        "gold.training_examples",
        training_df,
        errors.get("training_examples"),
    ):
        st.subheader("Gold training examples")
        training_summary = (
            training_df.groupby(["label_horizon", "label_value"])
            .size()
            .reset_index(name="examples")
            if {"label_horizon", "label_value"}.issubset(training_df.columns)
            else pd.DataFrame()
        )
        if not training_summary.empty:
            fig_training = px.bar(
                training_summary,
                x="label_horizon",
                y="examples",
                color="label_value",
                barmode="group",
                labels={
                    "label_horizon": "Label horizon",
                    "examples": "Examples",
                    "label_value": "Label",
                },
            )
            st.plotly_chart(fig_training, width="stretch")
        training_columns = [
            column
            for column in (
                "source",
                "label_horizon",
                "label_value",
                "dataset_version",
                "feature_version",
                "schema_version",
                "example_date",
                "text_for_model",
            )
            if column in training_df.columns
        ]
        st.dataframe(
            training_df[training_columns].head(100),
            width="stretch",
            hide_index=True,
        )


def filter_content_rows(contents):
    filtered = contents.copy()
    if filtered.empty:
        return filtered

    st.subheader("Filters")
    filter_columns = st.columns(3)
    if "source" in filtered.columns:
        source_options = sorted(filtered["source"].dropna().unique())
        selected_sources = filter_columns[0].multiselect(
            "Source",
            source_options,
            default=source_options,
        )
        filtered = filtered[filtered["source"].isin(selected_sources)]
    if "content_type" in filtered.columns:
        type_options = sorted(filtered["content_type"].dropna().unique())
        selected_types = filter_columns[1].multiselect(
            "Content type",
            type_options,
            default=type_options,
        )
        filtered = filtered[filtered["content_type"].isin(selected_types)]
    if "subreddit" in filtered.columns:
        subreddit_options = sorted(filtered["subreddit"].dropna().unique())
        selected_subreddits = filter_columns[2].multiselect(
            "Subreddit",
            subreddit_options,
            default=subreddit_options,
        )
        if selected_subreddits:
            filtered = filtered[filtered["subreddit"].isin(selected_subreddits)]

    text_columns = st.columns(3)
    user_filter = text_columns[0].text_input("User")
    keyword_filter = text_columns[1].text_input("Keyword")
    date_column = "created_at" if "created_at" in filtered.columns else None

    if user_filter and "author_id_hash" in filtered.columns:
        filtered = filtered[
            filtered["author_id_hash"]
            .fillna("")
            .str.contains(
                user_filter,
                case=False,
                regex=False,
            )
        ]
    if keyword_filter:
        searchable = pd.Series("", index=filtered.index, dtype="string")
        for column in ("title", "text", "url"):
            if column in filtered.columns:
                searchable = searchable.str.cat(
                    filtered[column].fillna("").astype("string"),
                    sep=" ",
                )
        filtered = filtered[searchable.str.contains(keyword_filter, case=False, regex=False)]
    if date_column and filtered[date_column].notna().any():
        min_date = filtered[date_column].min().date()
        max_date = filtered[date_column].max().date()
        selected_range = text_columns[2].date_input(
            "Date",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
            filtered = filtered[
                filtered[date_column].isna()
                | (
                    (filtered[date_column].dt.date >= start_date)
                    & (filtered[date_column].dt.date <= end_date)
                )
            ]
    return filtered


def enrich_content_rows(contents):
    enriched = contents.copy()
    if enriched.empty or "url" not in enriched.columns:
        return enriched

    if "subreddit" not in enriched.columns:
        enriched["subreddit"] = pd.NA

    reddit_mask = (
        enriched.get("source", pd.Series("", index=enriched.index))
        .astype("string")
        .str.lower()
        .eq("reddit")
    )
    missing_subreddit = enriched["subreddit"].isna() | (
        enriched["subreddit"].astype("string").str.strip() == ""
    )
    derived_subreddits = (
        enriched.loc[reddit_mask & missing_subreddit, "url"]
        .astype("string")
        .str.extract(r"/r/([^/]+)", expand=False)
    )
    enriched.loc[reddit_mask & missing_subreddit, "subreddit"] = derived_subreddits
    return enriched


def ensure_reddit_community_columns(contents):
    enriched = contents.copy()
    for column in REDDIT_COMMUNITY_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = pd.NA
    return enriched


def parse_dashboard_count(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace("\u202f", " ").replace("\xa0", " ")
    match = re.search(r"([\d][\d\s,.]*)([KMBkmb]?)", text)
    if not match:
        return None
    number_text = match.group(1).replace(" ", "")
    suffix = match.group(2).lower()
    if "," in number_text and "." in number_text:
        number_text = number_text.replace(",", "")
    elif "," in number_text:
        number_text = number_text.replace(",", ".")
    try:
        value = float(number_text)
    except ValueError:
        return None
    multiplier = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}.get(suffix, 1)
    return int(value * multiplier)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_reddit_community_from_old_html(subreddit):
    info = {column: None for column in REDDIT_COMMUNITY_COLUMNS}
    subreddit = str(subreddit or "").strip().strip("/")
    if not subreddit:
        return info
    fallback = STATIC_REDDIT_COMMUNITY_FALLBACKS.get(subreddit.lower(), {})
    try:
        response = requests.get(
            f"https://old.reddit.com/r/{subreddit}/",
            headers={
                "User-Agent": os.getenv(
                    "REDDIT_USER_AGENT",
                    "Mozilla/5.0 Chrome/124 Safari/537.36 user-behavior-lakehouse/1.0",
                )
            },
            timeout=int(os.getenv("DASHBOARD_REDDIT_LOOKUP_TIMEOUT_SECONDS", "10")),
        )
        response.raise_for_status()
    except requests.RequestException:
        info.update(fallback)
        return info

    html = response.text
    title_match = re.search(
        r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if not title_match:
        title_match = re.search(
            r"<title>(.*?)</title>",
            html,
            re.IGNORECASE | re.DOTALL,
        )
    if title_match:
        title = html_lib.unescape(title_match.group(1))
        title = re.sub(r"\s+[.:|•-]\s+r/[A-Za-z0-9_]+.*$", "", title).strip()
        info["subreddit_title"] = title or None

    description_match = re.search(
        r'<meta\s+(?:name|property)=["\'](?:description|og:description)["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if description_match:
        info["subreddit_description"] = (
            html_lib.unescape(description_match.group(1)).strip() or None
        )

    created_match = re.search(
        r"(?:created|a community for)\s+(?:on\s+)?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        html_lib.unescape(re.sub(r"<[^>]+>", " ", html)),
        re.IGNORECASE,
    )
    if created_match:
        info["subreddit_created_at"] = created_match.group(1)
    info["subreddit_visibility"] = "public"

    member_match = re.search(
        r'<span[^>]+class=["\'][^"\']*number[^"\']*["\'][^>]*>'
        r"\s*([^<]+?)\s*</span>\s*"
        r'<span[^>]+class=["\'][^"\']*word[^"\']*["\'][^>]*>'
        r"\s*(?:readers|subscribers|members|abonnes|membres)\s*</span>",
        html,
        re.IGNORECASE,
    )
    if member_match:
        info["subreddit_member_count"] = parse_dashboard_count(member_match.group(1))
    for column, value in fallback.items():
        if info.get(column) is None:
            info[column] = value
    return info


def enrich_reddit_community_from_web(contents):
    enriched = ensure_reddit_community_columns(contents)
    if enriched.empty or "subreddit" not in enriched.columns:
        return enriched

    reddit_mask = (
        enriched.get("source", pd.Series("", index=enriched.index))
        .astype("string")
        .str.lower()
        .eq("reddit")
    )
    subreddits = (
        enriched.loc[reddit_mask, "subreddit"]
        .dropna()
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
    )
    for subreddit in subreddits[: int(os.getenv("DASHBOARD_REDDIT_LOOKUP_LIMIT", "25"))]:
        subreddit_mask = reddit_mask & enriched["subreddit"].astype("string").eq(subreddit)
        missing_any = False
        for column in REDDIT_COMMUNITY_COLUMNS:
            values = enriched.loc[subreddit_mask, column]
            if values.isna().any() or values.astype("string").str.strip().eq("").any():
                missing_any = True
                break
        if not missing_any:
            continue
        info = fetch_reddit_community_from_old_html(subreddit)
        for column, value in info.items():
            if value is None:
                continue
            missing = enriched[column].isna() | enriched[column].astype("string").str.strip().eq("")
            enriched.loc[subreddit_mask & missing, column] = value
    return enriched


def enrich_reddit_community_from_snapshots(contents, snapshots):
    if contents.empty or snapshots.empty:
        return contents
    if "content_id" not in contents.columns:
        return contents
    required_columns = {"content_id", "source", "subreddit_member_count"}
    if not required_columns.issubset(snapshots.columns):
        return contents

    reddit_snapshots = snapshots[snapshots["source"] == "reddit"].copy()
    reddit_snapshots = reddit_snapshots.dropna(subset=["content_id"])
    if reddit_snapshots.empty:
        return contents
    if "snapshot_at" in reddit_snapshots.columns:
        reddit_snapshots = reddit_snapshots.sort_values("snapshot_at")

    latest_members = (
        reddit_snapshots.groupby("content_id", dropna=False)["subreddit_member_count"]
        .last()
        .rename("snapshot_subreddit_member_count")
        .reset_index()
    )
    enriched = contents.copy()
    if "subreddit_member_count" not in enriched.columns:
        enriched["subreddit_member_count"] = pd.NA
    enriched = enriched.merge(latest_members, on="content_id", how="left")
    missing_members = enriched["subreddit_member_count"].isna()
    enriched.loc[missing_members, "subreddit_member_count"] = enriched.loc[
        missing_members,
        "snapshot_subreddit_member_count",
    ]
    return enriched.drop(columns=["snapshot_subreddit_member_count"])


def content_label(row):
    title = format_optional_text(row.get("title"))
    if title == "N/A":
        title = format_optional_text(row.get("url"))
    return f"{row.get('source', 'unknown')} | {title[:90]}"


def render_content_interactions(content_row, interactions):
    content_id = content_row.get("content_id")
    if interactions.empty or "parent_content_id" not in interactions.columns:
        st.info("No interaction table available for this content.")
        return
    related = interactions[interactions["parent_content_id"] == content_id].copy()
    if related.empty:
        st.info("No comments or replies found for this content.")
        return
    columns = [
        column
        for column in (
            "created_at",
            "interaction_type",
            "author_id_hash",
            "text",
            "score",
            "like_count",
            "reply_count",
        )
        if column in related.columns
    ]
    if "created_at" in related.columns:
        related = related.sort_values("created_at", ascending=False)
    st.dataframe(related[columns].head(200), width="stretch", hide_index=True)


def render_content_analytics():
    st.subheader("Content analytics")
    tables, errors = get_content_analytics_tables()
    contents = enrich_content_rows(prepare_optional_table(tables["contents"]))
    interactions = prepare_optional_table(tables["interactions"])
    engagement_snapshots = prepare_optional_table(tables["engagement_snapshots"])
    transcripts = prepare_optional_table(tables["transcripts"])
    content_stats = prepare_optional_table(tables["content_stats"])
    user_evolution = prepare_optional_table(tables["user_evolution"])
    contents = enrich_reddit_community_from_snapshots(
        ensure_reddit_community_columns(contents),
        engagement_snapshots,
    )
    contents = enrich_reddit_community_from_web(contents)

    metric_columns = st.columns(5)
    metric_columns[0].metric("Contents", format_count(len(contents)))
    metric_columns[1].metric("Interactions", format_count(len(interactions)))
    transcript_count = len(transcripts)
    if "transcript_status" in transcripts.columns:
        transcript_count = int(
            transcripts["transcript_status"].astype("string").str.lower().eq("success").sum()
        )
    metric_columns[2].metric("Transcripts", format_count(transcript_count))
    metric_columns[3].metric("Content stats", format_count(len(content_stats)))
    metric_columns[4].metric("User days", format_count(len(user_evolution)))

    if contents.empty:
        render_optional_table_status("silver.contents", contents, errors.get("contents"))
        return

    explorer_tab, reddit_tab, x_tab, youtube_tab, users_tab = st.tabs(
        ["Content Explorer", "Reddit", "X", "YouTube", "Users"]
    )

    with explorer_tab:
        filtered_contents = filter_content_rows(contents)
        stat_columns = [
            "content_id",
            "interaction_count",
            "unique_interacting_users",
            "avg_interaction_length",
            "latest_view_count",
            "latest_like_count",
            "latest_comment_count",
            "latest_reply_count",
        ]
        available_stats = [column for column in stat_columns if column in content_stats.columns]
        display_rows = filtered_contents.copy()
        if not content_stats.empty and available_stats:
            display_rows = display_rows.merge(
                content_stats[available_stats],
                on="content_id",
                how="left",
            )
        table_columns = [
            column
            for column in (
                "source",
                "content_type",
                "created_at",
                "subreddit",
                "subreddit_title",
                "subreddit_description",
                "subreddit_member_count",
                "subreddit_weekly_visitors",
                "subreddit_weekly_contributions",
                "youtube_channel_id",
                "title",
                "interaction_count",
                "unique_interacting_users",
                "latest_view_count",
                "latest_like_count",
                "url",
            )
            if column in display_rows.columns
        ]
        st.dataframe(
            display_rows[table_columns].head(250),
            width="stretch",
            hide_index=True,
            column_config={"url": st.column_config.LinkColumn("URL")},
        )
        if not filtered_contents.empty:
            selected_index = st.selectbox(
                "Content",
                options=filtered_contents.index.tolist(),
                format_func=lambda index: content_label(filtered_contents.loc[index]),
            )
            render_content_interactions(filtered_contents.loc[selected_index], interactions)

    with reddit_tab:
        reddit_contents = contents[contents["source"] == "reddit"].copy()
        if reddit_contents.empty:
            st.info("No Reddit content available.")
        else:
            subreddit_summary = (
                reddit_contents.groupby("subreddit", dropna=False)
                .agg(
                    posts=("content_id", "count"),
                    **{column: (column, "first") for column in REDDIT_COMMUNITY_COLUMNS},
                )
                .reset_index()
                .sort_values("posts", ascending=False)
            )
            empty_community_columns = [
                column
                for column in REDDIT_COMMUNITY_COLUMNS
                if subreddit_summary[column].isna().all()
            ]
            if empty_community_columns:
                st.caption(
                    "Some Reddit community columns are empty until the Reddit "
                    "collector runs again and content analytics is refreshed."
                )
            st.dataframe(
                subreddit_summary,
                width="stretch",
                hide_index=True,
                column_config={
                    "subreddit_member_count": st.column_config.NumberColumn(
                        "Subscribers",
                        format="%d",
                    ),
                    "subreddit_weekly_visitors": st.column_config.NumberColumn(
                        "Weekly visitors",
                        format="%d",
                    ),
                    "subreddit_weekly_contributions": st.column_config.NumberColumn(
                        "Weekly contributions",
                        format="%d",
                    ),
                },
            )
            selected_index = st.selectbox(
                "Reddit post",
                options=reddit_contents.index.tolist(),
                format_func=lambda index: content_label(reddit_contents.loc[index]),
            )
            render_content_interactions(reddit_contents.loc[selected_index], interactions)

    with x_tab:
        x_contents = contents[contents["source"] == "x"].copy()
        if x_contents.empty:
            st.info("No X content available.")
        else:
            selected_index = st.selectbox(
                "X post",
                options=x_contents.index.tolist(),
                format_func=lambda index: content_label(x_contents.loc[index]),
            )
            selected = x_contents.loc[selected_index]
            render_content_interactions(selected, interactions)

    with youtube_tab:
        youtube_contents = contents[contents["source"] == "youtube"].copy()
        if youtube_contents.empty:
            st.info("No YouTube content available.")
        else:
            youtube_display = build_youtube_display_rows(
                youtube_contents,
                transcripts,
                content_stats,
                engagement_snapshots,
            )
            enrichment_stale_hours = positive_env_float(
                "DASHBOARD_YOUTUBE_ENRICHMENT_STALE_HOURS",
                168,
            )
            snapshot_stale_hours = positive_env_float(
                "DASHBOARD_YOUTUBE_SNAPSHOT_STALE_HOURS",
                24,
            )
            st.caption("YouTube videos")
            card_columns = st.columns(3)
            for position, (_, video_row) in enumerate(youtube_display.head(30).iterrows()):
                with card_columns[position % 3]:
                    with st.container(border=True):
                        thumbnail_url = youtube_thumbnail_display_url(video_row)
                        if thumbnail_url:
                            st.image(thumbnail_url, width=120)
                        else:
                            st.caption("No thumbnail")
                        st.markdown(f"**{format_optional_text(video_row.get('title'))}**")
                        channel = format_optional_text(video_row.get("youtube_channel_name"))
                        if channel != "N/A":
                            st.caption(channel)
                        complete_count, complete_total, checks = youtube_data_completeness(
                            video_row
                        )
                        st.progress(
                            complete_count / complete_total,
                            text=f"Data completeness {complete_count}/{complete_total}",
                        )
                        transcript_status = transcript_lifecycle_status(video_row)
                        st.caption(
                            " | ".join(
                                [
                                    "Views: "
                                    + format_available_metric(
                                        video_row.get("latest_view_count"),
                                        video_row.get("latest_view_count_available"),
                                    ),
                                    "Likes: "
                                    + format_available_metric(
                                        video_row.get("latest_like_count"),
                                        video_row.get("latest_like_count_available"),
                                    ),
                                    "Comments: "
                                    + format_available_metric(
                                        video_row.get("latest_comment_count"),
                                        video_row.get("latest_comment_count_available"),
                                    ),
                                    f"Transcript: {transcript_status.replace('_', ' ')}",
                                ]
                            )
                        )
                        st.caption(transcript_provenance_label(video_row))
                        st.caption(
                            " | ".join(
                                [
                                    "Discovered: "
                                    + format_timestamp(video_row.get("last_discovered_at")),
                                    "Enriched: "
                                    + format_timestamp(video_row.get("last_enriched_at")),
                                    "Snapshot: "
                                    + format_timestamp(video_row.get("latest_snapshot_at")),
                                ]
                            )
                        )
                        st.caption("Coverage: " + coverage_summary(video_row))
                        st.caption("Provenance: " + provenance_summary(video_row))
                        st.caption(
                            "Transcript attempts: "
                            + format_available_metric(
                                video_row.get("latest_transcript_attempt_count")
                            )
                            + " | Last: "
                            + format_timestamp(video_row.get("latest_transcript_last_attempt_at"))
                            + " | Next: "
                            + format_timestamp(video_row.get("latest_transcript_next_attempt_at"))
                        )
                        missing = [label for label, available in checks.items() if not available]
                        if missing:
                            st.caption("Missing: " + ", ".join(missing))
                        else:
                            st.success("Complete data available.")

                        warnings = [
                            freshness_warning(
                                "Metadata enrichment",
                                video_row.get("last_enriched_at"),
                                stale_after_hours=enrichment_stale_hours,
                            ),
                            freshness_warning(
                                "Engagement snapshot",
                                video_row.get("latest_snapshot_at"),
                                stale_after_hours=snapshot_stale_hours,
                            ),
                            transcript_retry_warning(video_row),
                        ]
                        for warning in warnings:
                            if warning:
                                st.warning(warning)

            with st.expander("YouTube freshness and coverage", expanded=True):
                freshness_table = build_youtube_freshness_table(
                    youtube_display,
                    enrichment_stale_hours=enrichment_stale_hours,
                    snapshot_stale_hours=snapshot_stale_hours,
                )
                st.dataframe(
                    freshness_table,
                    width="stretch",
                    hide_index=True,
                    column_config={"URL": st.column_config.LinkColumn("URL")},
                )

            selected_index = st.selectbox(
                "YouTube video",
                options=youtube_display.index.tolist(),
                format_func=lambda index: content_label(youtube_display.loc[index]),
            )
            selected = youtube_display.loc[selected_index]
            render_content_interactions(selected, interactions)
            if "content_id" in transcripts.columns:
                video_transcripts = transcripts[
                    transcripts["content_id"] == selected.get("content_id")
                ].copy()
            else:
                video_transcripts = pd.DataFrame()
            if video_transcripts.empty:
                st.info("Transcript collection has not been attempted for this video.")
            else:
                transcript_sort_columns = [
                    column
                    for column in ("last_attempt_at", "updated_at")
                    if column in video_transcripts.columns
                ]
                if transcript_sort_columns:
                    video_transcripts = video_transcripts.sort_values(
                        transcript_sort_columns,
                        ascending=False,
                        na_position="last",
                    )
                transcript_row = video_transcripts.iloc[0]
                status, level, message = transcript_status_presentation(transcript_row)
                st.caption(f"Transcript lifecycle: {status.replace('_', ' ')}")
                st.caption(transcript_provenance_label(transcript_row))
                if status != "available":
                    getattr(st, level)(message)
                transcript_detail_columns = [
                    column
                    for column in (
                        "requested_language_code",
                        "obtained_language_code",
                        "transcript_lifecycle_status",
                        "generation_type",
                        "is_generated",
                        "is_translated",
                        "provider",
                        "model",
                        "fallback_reason",
                        "prompt_version",
                        "generated_by_model",
                        "selection_strategy",
                        "attempt_count",
                        "last_attempt_at",
                        "next_attempt_at",
                        "error_code",
                        "content_version",
                    )
                    if column in video_transcripts.columns
                ]
                transcript_details = video_transcripts[transcript_detail_columns].copy()
                for column in (
                    "requested_language_code",
                    "obtained_language_code",
                    "generation_type",
                    "is_generated",
                    "is_translated",
                    "provider",
                    "error_code",
                    "content_version",
                ):
                    if column in transcript_details.columns:
                        transcript_details[column] = transcript_details[column].apply(
                            format_optional_text
                        )
                if "transcript_lifecycle_status" in transcript_details.columns:
                    transcript_details["transcript_lifecycle_status"] = video_transcripts.apply(
                        transcript_lifecycle_status, axis=1
                    )
                for column in ("last_attempt_at", "next_attempt_at"):
                    if column in transcript_details.columns:
                        transcript_details[column] = transcript_details[column].apply(
                            format_timestamp
                        )
                if "attempt_count" in transcript_details.columns:
                    transcript_details["attempt_count"] = transcript_details["attempt_count"].apply(
                        format_available_metric
                    )
                st.dataframe(
                    transcript_details,
                    width="stretch",
                    hide_index=True,
                )
                transcript = transcript_row.get("transcript_text")
                transcript_text = "" if pd.isna(transcript) else str(transcript)
                if transcript_text.strip():
                    keyword = st.text_input("Transcript keyword")
                    if keyword:
                        lines = [
                            line
                            for line in transcript_text.splitlines()
                            if keyword.lower() in line.lower()
                        ]
                        st.text_area("Transcript", "\n".join(lines), height=260)
                    else:
                        st.text_area("Transcript", transcript_text, height=260)

    with users_tab:
        if user_evolution.empty:
            render_optional_table_status(
                "gold.user_evolution",
                user_evolution,
                errors.get("user_evolution"),
            )
        else:
            users = sorted(user_evolution["user_id_hash"].dropna().unique())
            if not users:
                st.info("No user identifier available in user evolution yet.")
                return
            selected_user = st.selectbox("User", users)
            user_rows = user_evolution[user_evolution["user_id_hash"] == selected_user].copy()
            st.dataframe(user_rows, width="stretch", hide_index=True)
            if "event_date" in user_rows.columns and not user_rows.empty:
                chart_rows = user_rows.melt(
                    id_vars=["event_date", "source"],
                    value_vars=[
                        column
                        for column in (
                            "contents_created",
                            "interactions_created",
                            "distinct_contents_touched",
                            "question_count",
                        )
                        if column in user_rows.columns
                    ],
                    var_name="metric",
                    value_name="value",
                )
                fig_user = px.line(
                    chart_rows,
                    x="event_date",
                    y="value",
                    color="metric",
                    line_dash="source",
                    markers=True,
                )
                st.plotly_chart(fig_user, width="stretch")


def render_quality_overview():
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Unique identifiers by source")
        identifiers_by_source = (
            analytics_df.groupby("source")["author_hash"]
            .nunique()
            .reset_index(name="identifiers")
            .sort_values("identifiers", ascending=False)
        )
        fig_identifiers = px.bar(
            identifiers_by_source,
            x="source",
            y="identifiers",
            color="source",
            labels={"source": "Source", "identifiers": "Unique identifiers"},
        )
        st.plotly_chart(fig_identifiers, width="stretch")

    with col2:
        st.subheader("Average text length")
        text_length_by_source = (
            analytics_df.groupby("source")["text_len_words"]
            .mean()
            .reset_index()
            .sort_values("text_len_words", ascending=False)
        )
        fig_length = px.bar(
            text_length_by_source,
            x="source",
            y="text_len_words",
            color="source",
            labels={
                "source": "Source",
                "text_len_words": "Average length in words",
            },
        )
        st.plotly_chart(fig_length, width="stretch")

    st.subheader("Data quality by source")
    quality_by_source = (
        analytics_df.groupby("source")
        .agg(
            total_records=("source", "size"),
            missing_text_pct=("text", lambda s: s.isna().mean() * 100),
            missing_author_hash_pct=("author_hash", lambda s: s.isna().mean() * 100),
            missing_created_at_pct=("created_at", lambda s: s.isna().mean() * 100),
            missing_url_pct=("url", lambda s: s.isna().mean() * 100),
            pipeline_error_pct=(
                "error",
                lambda s: s.fillna("").str.strip().ne("").mean() * 100,
            ),
        )
        .reset_index()
        .rename(
            columns={
                "source": "Source",
                "total_records": "Events",
                "missing_text_pct": "Missing text (%)",
                "missing_author_hash_pct": "Missing identifier (%)",
                "missing_created_at_pct": "Missing timestamp (%)",
                "missing_url_pct": "Missing URL (%)",
                "pipeline_error_pct": "Pipeline error (%)",
            }
        )
    )
    st.dataframe(quality_by_source, width="stretch", hide_index=True)


def render_recent_events():
    st.subheader("Recent events")
    recent_df = analytics_df.copy()
    recent_df["text"] = recent_df["text"].fillna("").str.strip().str.slice(0, 240)
    recent_df["url"] = recent_df["url"].fillna("N/A")
    recent_df["author_hash"] = recent_df["author_hash"].fillna("N/A").str.slice(0, 12)
    recent_df = recent_df.sort_values("created_at", ascending=False)
    recent_df["created_at"] = recent_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    recent_df["created_at"] = recent_df["created_at"].fillna("N/A")
    recent_df["metadata_refreshed_at"] = recent_df["metadata_refreshed_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    recent_df["metadata_refreshed_at"] = recent_df["metadata_refreshed_at"].fillna("N/A")
    recent_df["platform_event_id"] = recent_df["platform_event_id"].apply(format_optional_text)
    recent_df["error"] = recent_df["error"].fillna("")
    recent_df["owner_channel_id"] = recent_df["owner_channel_id"].apply(format_optional_text)
    recent_df["collaborators"] = recent_df["collaborator_channel_ids"].apply(format_collaborators)
    for column in ENGAGEMENT_COLUMNS:
        availability_column = f"{column}_available"
        recent_df[column] = recent_df.apply(
            lambda row, metric=column, flag=availability_column: (
                format_available_metric(row.get(metric), row.get(flag))
            ),
            axis=1,
        )
    recent_df = recent_df[
        [
            "source",
            "created_at",
            "author_hash",
            "platform_event_id",
            "metadata_refreshed_at",
            "owner_channel_id",
            "collaborators",
            "text",
            *ENGAGEMENT_COLUMNS,
            "url",
            "error",
        ]
    ].head(100)

    st.dataframe(
        recent_df,
        width="stretch",
        hide_index=True,
        column_config={
            "source": "Source",
            "created_at": "Timestamp",
            "author_hash": "Identifier hash",
            "platform_event_id": "Platform ID",
            "metadata_refreshed_at": "Last metadata refresh",
            "owner_channel_id": "Owner channel ID",
            "collaborators": "Collaborator channel IDs",
            "text": "Cleaned content",
            **{
                column: st.column_config.TextColumn(label)
                for column, label in ENGAGEMENT_LABELS.items()
            },
            "url": st.column_config.LinkColumn("URL"),
            "error": "Error",
        },
    )


(
    overview_tab,
    engagement_tab,
    authors_tab,
    tracking_tab,
    content_tab,
    model_tab,
    quality_tab,
    events_tab,
) = st.tabs(
    [
        "Overview",
        "Engagement",
        "YouTube authors",
        "Identifier tracking",
        "Content Explorer",
        "Model pipeline",
        "Quality",
        "Events",
    ]
)

with overview_tab:
    render_overview_summary()
    render_airflow_monitoring()
    render_add_data_panel()
    render_source_activity()
    render_balancing_report()

with engagement_tab:
    render_engagement_metadata()

with authors_tab:
    render_youtube_authors()

with tracking_tab:
    render_identifier_tracking()

with content_tab:
    render_content_analytics()

with model_tab:
    render_model_pipeline()

with quality_tab:
    render_quality_overview()

with events_tab:
    render_python_export(df_filtered, analytics_df)
    render_recent_events()
