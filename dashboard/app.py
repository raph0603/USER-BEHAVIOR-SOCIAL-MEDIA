import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

from loaders import load_all_data


st.set_page_config(
    page_title="Social Media Crawling Dashboard",
    layout="wide"
)

st.title("Social Media Crawling Dashboard")
st.caption("Monitoring dashboard for Reddit, X/Twitter and YouTube crawled data")


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

REDDIT_PATH = DATA_DIR / "reddit.csv"
X_PATH = DATA_DIR / "x.csv"
YOUTUBE_PATH = DATA_DIR / "youtube.csv"


@st.cache_data
def get_data():
    return load_all_data(REDDIT_PATH, X_PATH, YOUTUBE_PATH)


df = get_data()

st.sidebar.header("Filters")

sources = st.sidebar.multiselect(
    "Sources",
    options=sorted(df["source"].dropna().unique()),
    default=sorted(df["source"].dropna().unique())
)

df_filtered = df[df["source"].isin(sources)].copy()

df_dated = df_filtered.dropna(subset=["created_at"]).copy()

if not df_dated.empty:
    min_date = df_dated["created_at"].min().date()
    max_date = df_dated["created_at"].max().date()

    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date)
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            df_filtered["created_at"].isna() |
            (
                (df_filtered["created_at"].dt.date >= start_date) &
                (df_filtered["created_at"].dt.date <= end_date)
            )
        ]
else:
    st.sidebar.warning("No valid dates found in current selection.")

total_records = len(df_filtered)
latest_activity = df_filtered["created_at"].max()
reply_rate = (df_filtered["is_reply"].fillna(False).mean() * 100) if total_records > 0 else 0
avg_text_words = df_filtered["text_len_words"].dropna().mean() if total_records > 0 else 0
avg_engagement = df_filtered["engagement"].dropna().mean() if total_records > 0 else 0
missing_timestamps_pct = (df_filtered["created_at"].isna().mean() * 100) if total_records > 0 else 0

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total records", f"{total_records:,}")
c2.metric("Latest activity", str(latest_activity) if pd.notna(latest_activity) else "N/A")
c3.metric("Reply rate", f"{reply_rate:.1f}%")
c4.metric("Avg text length (words)", f"{avg_text_words:.1f}")
c5.metric("Avg engagement", f"{avg_engagement:.1f}")
c6.metric("Missing timestamps", f"{missing_timestamps_pct:.1f}%")

st.subheader("Records by source")
source_counts = (
    df_filtered.groupby("source")
    .size()
    .reset_index(name="records")
    .sort_values("records", ascending=False)
)
fig_source = px.bar(source_counts, x="source", y="records", color="source")
st.plotly_chart(fig_source, use_container_width=True)

st.subheader("Activity over time")
df_time = df_filtered.dropna(subset=["created_at"]).copy()

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
        markers=True
    )
    st.plotly_chart(fig_time, use_container_width=True)
else:
    st.info("No valid timestamps available for the current filter.")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Replies vs top-level")
    reply_counts = (
        df_filtered.assign(
            reply_type=df_filtered["is_reply"].fillna(False).map({True: "Reply", False: "Top-level"})
        )
        .groupby(["source", "reply_type"])
        .size()
        .reset_index(name="records")
    )

    fig_reply = px.bar(
        reply_counts,
        x="source",
        y="records",
        color="reply_type",
        barmode="group"
    )
    st.plotly_chart(fig_reply, use_container_width=True)

with col2:
    st.subheader("Average engagement by source")
    engagement_by_source = (
        df_filtered.groupby("source")["engagement"]
        .mean()
        .reset_index()
        .sort_values("engagement", ascending=False)
    )

    fig_eng = px.bar(
        engagement_by_source,
        x="source",
        y="engagement",
        color="source"
    )
    st.plotly_chart(fig_eng, use_container_width=True)

st.subheader("Data quality by source")

quality_by_source = (
    df_filtered.groupby("source")
    .agg(
        total_records=("item_id", "size"),
        missing_text_pct=("text", lambda s: s.isna().mean() * 100),
        missing_author_hash_pct=("author_hash", lambda s: s.isna().mean() * 100),
        missing_created_at_pct=("created_at", lambda s: s.isna().mean() * 100),
        empty_text_pct=("text", lambda s: (s.fillna("").astype(str).str.strip() == "").mean() * 100),
        avg_text_len_words=("text_len_words", "mean")
    )
    .reset_index()
    .rename(columns={
        "source": "Source",
        "total_records": "Total records",
        "missing_text_pct": "Missing text (%)",
        "missing_author_hash_pct": "Missing author hash (%)",
        "missing_created_at_pct": "Missing timestamp (%)",
        "empty_text_pct": "Empty text after cleaning (%)",
        "avg_text_len_words": "Avg text length (words)"
    })
)

st.dataframe(quality_by_source, use_container_width=True)

st.subheader("Average text length by source")

avg_len_df = (
    df_filtered.groupby("source")["text_len_words"]
    .mean()
    .reset_index()
    .sort_values("text_len_words", ascending=False)
)

if not avg_len_df.empty:
    fig_avg_len = px.bar(
        avg_len_df,
        x="source",
        y="text_len_words",
        color="source",
        labels={
            "source": "Source",
            "text_len_words": "Average text length (words)"
        }
    )

    fig_avg_len.update_layout(
        height=350,
        showlegend=False
    )

    st.plotly_chart(fig_avg_len, use_container_width=True)
else:
    st.info("No text length data available.")

st.subheader("Top records by engagement")
top_df = df_filtered.copy()

top_df["text"] = top_df["text"].fillna("").astype(str).str.strip()
top_df["author_name"] = top_df["author_name"].fillna("N/A").astype(str).str.strip()
top_df["url"] = top_df["url"].fillna("").astype(str).str.strip()
top_df["engagement"] = pd.to_numeric(top_df["engagement"], errors="coerce")

top_df = top_df[
    (top_df["text"] != "") &
    (top_df["engagement"].notna())
].copy()

top_df["created_at_display"] = top_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
top_df["created_at_display"] = top_df["created_at_display"].fillna("N/A")
top_df["text"] = top_df["text"].str.slice(0, 220)

top_df = top_df.sort_values("engagement", ascending=False)

top_cols = [
    "source",
    "created_at_display",
    "author_name",
    "text",
    "engagement",
    "url"
]

top_df = top_df[top_cols].head(20).rename(columns={
    "created_at_display": "created_at"
})

st.dataframe(top_df, use_container_width=True)

st.subheader("Sample data preview")
preview_df = df_filtered.copy()

preview_df["text"] = preview_df["text"].fillna("").astype(str).str.strip()
preview_df["author_name"] = preview_df["author_name"].fillna("N/A").astype(str).str.strip()
preview_df["url"] = preview_df["url"].fillna("").astype(str).str.strip()

preview_df = preview_df[
    (preview_df["text"] != "") &
    (
        preview_df["created_at"].notna() |
        (preview_df["url"] != "")
    )
].copy()

preview_df["created_at_display"] = preview_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
preview_df["created_at_display"] = preview_df["created_at_display"].fillna("N/A")
preview_df["engagement_display"] = preview_df["engagement"].fillna(0)

preview_df["text"] = preview_df["text"].str.slice(0, 180)
preview_df["url"] = preview_df["url"].replace("", "N/A")

preview_df = preview_df.sort_values("created_at", ascending=False)

preview_cols = [
    "source",
    "created_at_display",
    "author_name",
    "text",
    "engagement_display",
    "url"
]

preview_df = preview_df[preview_cols].head(50).rename(columns={
    "created_at_display": "created_at",
    "engagement_display": "engagement"
})

st.dataframe(preview_df, use_container_width=True)

st.subheader("YouTube content indicators")
yt = df_filtered[df_filtered["source"] == "youtube"].copy()

if len(yt) > 0:
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("% has question", f"{yt['has_question'].fillna(0).astype(float).mean() * 100:.1f}%")
    k2.metric("% kw_price", f"{yt['kw_price'].fillna(0).astype(float).mean() * 100:.1f}%")
    k3.metric("% kw_range", f"{yt['kw_range'].fillna(0).astype(float).mean() * 100:.1f}%")
    k4.metric("% kw_charging", f"{yt['kw_charging'].fillna(0).astype(float).mean() * 100:.1f}%")
else:
    st.info("No YouTube data in current filter.")