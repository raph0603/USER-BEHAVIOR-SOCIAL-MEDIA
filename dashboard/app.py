import json
import os
from io import BytesIO
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from airflow_monitoring import AirflowClient
from loaders import get_iceberg_config, load_iceberg_data
from manual_import import (
    MANUAL_IMPORT_DAG_ID,
    get_manual_import_config,
    load_import_events,
    publish_events,
    summarize_events,
)
from navigation import render_navigation


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
}
ENGAGEMENT_COLUMNS = tuple(ENGAGEMENT_LABELS)
OPTIONAL_DASHBOARD_COLUMNS = (
    "platform_event_id",
    "metadata_refreshed_at",
    "owner_channel_id",
    "collaborator_channel_ids",
)

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
                message = (
                    f"Imported {len(imported_events):,} event(s): "
                    + ", ".join(
                        f"{count:,} {source}"
                        for source, count in published_counts.items()
                    )
                )
                if trigger_pipeline:
                    run = AirflowClient().trigger_dag(
                        MANUAL_IMPORT_DAG_ID,
                        {
                            "sources": list(published_counts),
                            "record_count": len(imported_events),
                        },
                    )
                    message += (
                        " | Pipeline started: "
                        f"{run.get('dag_run_id', MANUAL_IMPORT_DAG_ID)}"
                    )
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
    youtube_rows = youtube_rows.sort_values(
        sort_columns,
        ascending=False,
        na_position="last",
    ) if sort_columns else youtube_rows
    return youtube_rows.drop_duplicates("_video_key").drop(
        columns=["_video_key"]
    )


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
            author_rows["tracked_identifier"] = author_rows[
                "author_hash"
            ].astype("string")
            author_rows["identifier_role"] = "Author"
            tracking_frames.append(author_rows)

    if {"source", "owner_channel_id"}.issubset(analytics_rows.columns):
        owner_rows = analytics_rows[
            (analytics_rows["source"] == "youtube")
            & analytics_rows["owner_channel_id"].notna()
        ].copy()
        if not owner_rows.empty:
            owner_rows["tracked_identifier"] = owner_rows[
                "owner_channel_id"
            ].astype("string")
            owner_rows["identifier_role"] = "Owner YouTube"
            tracking_frames.append(owner_rows)

    if {"source", "collaborator_channel_ids"}.issubset(
        analytics_rows.columns
    ):
        collaborator_records = []
        youtube_rows = analytics_rows[analytics_rows["source"] == "youtube"]
        for _, row in youtube_rows.iterrows():
            collaborators = normalize_collaborators(
                row.get("collaborator_channel_ids")
            )
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
    tracking_df["author_hash"] = tracking_df["tracked_identifier"].astype(
        "string"
    )
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
            export_df[column] = export_df[column].dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
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
        selected_df = (
            analytics_dataframe
            if dataset_label == "Analytics rows"
            else raw_dataframe
        )
        export_df = prepare_python_export(selected_df)
        file_slug = (
            "analytics_rows"
            if dataset_label == "Analytics rows"
            else "filtered_events"
        )

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
st.caption(
    "Monitoring cleaned events received in the Iceberg Silver table"
)


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
    report_path = Path(
        os.getenv("DASHBOARD_BALANCING_REPORT_PATH", DEFAULT_BALANCING_REPORT_PATH)
    )
    if not report_path.is_file():
        return None
    with report_path.open(encoding="utf-8") as report_file:
        report = json.load(report_file)
    report["_report_path"] = str(report_path)
    report["_modified_at"] = pd.Timestamp(report_path.stat().st_mtime, unit="s")
    return report


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
                st.error(
                    f"{run['failed_tasks']} failed task(s) in this run"
                )
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
                "Next run": run["next_run"]
                .astimezone()
                .strftime("%Y-%m-%d %H:%M:%S"),
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
        blocked_runs = [
            row for row in collector_runs
            if row["collector_status"] == "blocked"
        ]
        if blocked_runs:
            st.warning(
                f"{len(blocked_runs)} collector block(s) found in recent runs"
            )
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
    missing_timestamps_pct = (
        analytics_df["created_at"].isna().mean() * 100 if total_records else 0
    )
    pipeline_error_pct = (
        analytics_df["error"].fillna("").str.strip().ne("").mean() * 100
        if total_records
        else 0
    )

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Events", f"{total_records:,}")
    c2.metric(
        "Last activity",
        latest_activity.strftime("%Y-%m-%d %H:%M")
        if pd.notna(latest_activity)
        else "N/A",
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

    engagement_by_source = (
        analytics_df.groupby("source")[list(ENGAGEMENT_COLUMNS)]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"source": "Source", **ENGAGEMENT_LABELS})
    )
    st.dataframe(
        engagement_by_source,
        width="stretch",
        hide_index=True,
        column_config={
            label: st.column_config.NumberColumn(label, format="%d")
            for label in ENGAGEMENT_LABELS.values()
        },
    )
    st.caption(
        "N/A values are metrics unavailable for the "
        "platform. Reddit score is kept separate from likes."
    )

    st.caption(
        "Note: YouTube metrics are grouped by video before aggregation."
    )

    common_engagement_columns = [
        "like_count",
        "view_count",
        "comment_count",
        "reply_count",
    ]
    engagement_chart_data = (
        engagement_by_source.rename(
            columns={
                ENGAGEMENT_LABELS[column]: column
                for column in common_engagement_columns
            }
        )
        .melt(
            id_vars="Source",
            value_vars=common_engagement_columns,
            var_name="metric",
            value_name="value",
        )
        .dropna(subset=["value"])
    )
    engagement_chart_data["metric"] = engagement_chart_data["metric"].map(
        ENGAGEMENT_LABELS
    )

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
        youtube_df["collaborator_count"] = youtube_df[
            "collaborator_channel_ids"
        ].apply(collaborator_count)
        youtube_df["collaborators"] = youtube_df[
            "collaborator_channel_ids"
        ].apply(format_collaborators)

        known_collaborators = youtube_df["collaborator_count"].dropna()
        videos_with_collaborators = (
            int((known_collaborators > 0).sum())
            if not known_collaborators.empty
            else 0
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
            f"{videos_with_collaborators:,}",
        )
        yt_metrics[3].metric(
            "Unique collaborators",
            f"{len(distinct_collaborator_ids):,}",
        )

        youtube_author_rows = (
            youtube_df.sort_values("created_at", ascending=False)
            [
                [
                    "created_at",
                    "text",
                    "owner_channel_id",
                    "collaborator_count",
                    "collaborators",
                    "url",
                ]
            ]
            .head(100)
        )
        youtube_author_rows["created_at"] = youtube_author_rows[
            "created_at"
        ].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        youtube_author_rows["created_at"] = youtube_author_rows[
            "created_at"
        ].fillna("N/A")
        youtube_author_rows["text"] = (
            youtube_author_rows["text"].fillna("").str.slice(0, 180)
        )
        youtube_author_rows["owner_channel_id"] = youtube_author_rows[
            "owner_channel_id"
        ].apply(format_optional_text)
        st.dataframe(
            youtube_author_rows,
            width="stretch",
            hide_index=True,
            column_config={
                "created_at": "Timestamp",
                "text": "Title",
                "owner_channel_id": "Owner channel ID",
                "collaborator_count": st.column_config.NumberColumn(
                    "Collaborators",
                    format="%d",
                ),
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
        time_counts = (
            df_time.groupby(["date", "source"])
            .size()
            .reset_index(name="records")
        )
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

        before_distribution = pd.DataFrame(
            balancing_report.get("distribution_before", [])
        )
        after_distribution = pd.DataFrame(
            balancing_report.get("distribution_after", [])
        )
        if not before_distribution.empty and not after_distribution.empty:
            dimensions = balancing_report.get("dimensions", [])
            before_distribution["dataset"] = "Before"
            after_distribution["dataset"] = "After"
            distribution = pd.concat(
                [before_distribution, after_distribution],
                ignore_index=True,
            )
            if dimensions:
                distribution["group"] = distribution[dimensions].astype(str).agg(
                    " | ".join,
                    axis=1,
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

            distribution_table = distribution[
                [*dimensions, "dataset", "count"]
            ].rename(
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
                user_tracking_df.groupby("author_hash")[column]
                .mean()
                .rename(f"avg_{column}")
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
        user_activity["total_engagement"] = (
            user_activity[total_columns].fillna(0).sum(axis=1)
        )
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
                "Reply rate (%)": st.column_config.NumberColumn(
                    format="%.1f"
                ),
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
                    f"{row.author_display} - {int(row.events)} event(s) - "
                    f"{row.roles}"
                )
                for row in top_users.itertuples()
            }
            selected_author = st.selectbox(
                "Identifier to track",
                options=selector_options,
                format_func=selector_labels.get,
            )
            selected_user = user_activity[
                user_activity["author_hash"] == selected_author
            ].iloc[0]
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

            dated_user_events = selected_events.dropna(
                subset=["created_at"]
            ).copy()
            if not dated_user_events.empty:
                dated_user_events["date"] = dated_user_events[
                    "created_at"
                ].dt.date
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
                    value_vars=[
                        f"cumulative_{column}"
                        for column in cumulative_columns
                    ],
                    var_name="metric",
                    value_name="value",
                )
                progress_chart["metric"] = progress_chart["metric"].replace(
                    {
                        f"cumulative_{column}": label
                        for column, label in cumulative_columns.items()
                    }
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
            selected_recent["created_at"] = selected_recent[
                "created_at"
            ].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            selected_recent["text"] = (
                selected_recent["text"].fillna("").str.slice(0, 180)
            )
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
    recent_df["author_hash"] = (
        recent_df["author_hash"].fillna("N/A").str.slice(0, 12)
    )
    recent_df = recent_df.sort_values("created_at", ascending=False)
    recent_df["created_at"] = recent_df["created_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    recent_df["created_at"] = recent_df["created_at"].fillna("N/A")
    recent_df["metadata_refreshed_at"] = recent_df[
        "metadata_refreshed_at"
    ].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    recent_df["metadata_refreshed_at"] = recent_df[
        "metadata_refreshed_at"
    ].fillna("N/A")
    recent_df["platform_event_id"] = recent_df["platform_event_id"].apply(
        format_optional_text
    )
    recent_df["error"] = recent_df["error"].fillna("")
    recent_df["owner_channel_id"] = recent_df["owner_channel_id"].apply(
        format_optional_text
    )
    recent_df["collaborators"] = recent_df["collaborator_channel_ids"].apply(
        format_collaborators
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
                column: st.column_config.NumberColumn(label, format="%d")
                for column, label in ENGAGEMENT_LABELS.items()
            },
            "url": st.column_config.LinkColumn("URL"),
            "error": "Error",
        },
    )

overview_tab, engagement_tab, authors_tab, tracking_tab, quality_tab, events_tab = st.tabs(
    [
        "Overview",
        "Engagement",
        "YouTube authors",
        "Identifier tracking",
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

with quality_tab:
    render_quality_overview()

with events_tab:
    render_python_export(df_filtered, analytics_df)
    render_recent_events()
