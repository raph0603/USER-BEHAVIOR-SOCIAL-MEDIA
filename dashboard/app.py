import pandas as pd
import plotly.express as px
import streamlit as st

from airflow_monitoring import AirflowClient
from loaders import ENGAGEMENT_COLUMNS, get_iceberg_config, load_iceberg_data
from navigation import render_navigation


ENGAGEMENT_LABELS = {
    "like_count": "Likes",
    "view_count": "Vues",
    "comment_count": "Commentaires",
    "reply_count": "Réponses",
    "retweet_count": "Retweets",
    "bookmark_count": "Favoris",
    "score": "Score Reddit",
}


def format_engagement_total(series):
    values = series.dropna()
    if values.empty:
        return "N/A"
    return f"{int(values.sum()):,}"


st.set_page_config(
    page_title="Social Media Lakehouse Dashboard",
    layout="wide",
)

render_navigation()

st.title("Social Media Lakehouse Dashboard")
st.caption(
    "Monitoring des événements nettoyés reçus dans la table Iceberg Silver"
)


@st.cache_data(ttl=60, show_spinner="Lecture de la table Iceberg Silver...")
def get_data():
    return load_iceberg_data()


@st.cache_data(ttl=10, show_spinner=False)
def get_airflow_status():
    return AirflowClient().load_status()


@st.fragment(run_every="15s")
def render_airflow_monitoring():
    st.subheader("Orchestration Airflow")

    try:
        status = get_airflow_status()
    except Exception as exc:
        st.warning(f"Monitoring Airflow indisponible: {exc}")
        return

    active_runs = status["active_runs"]
    next_runs = status["next_runs"]

    if active_runs:
        st.caption(f"{len(active_runs)} job(s) actif(s)")
        for run in active_runs:
            label = (
                f"{run['dag_id']} - {run['state']} - "
                f"{run['completed_tasks']}/{run['total_tasks']} tâches"
            )
            st.write(f"**{label}**")
            st.progress(
                run["progress"],
                text=f"{run['progress']} % terminé",
            )
            if run["failed_tasks"]:
                st.error(
                    f"{run['failed_tasks']} tâche(s) en échec dans ce run"
                )
    else:
        st.success("Aucun job Airflow en cours")

    if next_runs:
        next_run = next_runs[0]
        metric_columns = st.columns(3)
        metric_columns[0].metric(
            "Prochain job",
            next_run["dag_id"],
        )
        metric_columns[1].metric(
            "Planification",
            next_run["countdown"],
        )
        metric_columns[2].metric(
            "Heure prévue",
            next_run["next_run"].astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        )

        schedule_rows = [
            {
                "Job": run["dag_id"],
                "Prochaine exécution": run["next_run"]
                .astimezone()
                .strftime("%Y-%m-%d %H:%M:%S"),
                "Dans": run["countdown"],
            }
            for run in next_runs
        ]
        st.dataframe(
            schedule_rows,
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("Aucune prochaine exécution planifiée")

    st.caption(
        "Actualisation automatique toutes les 15 secondes. "
        f"Dernière vérification: "
        f"{status['checked_at'].astimezone().strftime('%H:%M:%S')}"
    )


render_airflow_monitoring()

config = get_iceberg_config()

st.sidebar.header("Données")
st.sidebar.caption(f"Table: `{config['table_path']}`")
st.sidebar.caption(f"MinIO: `{config['endpoint_url']}`")

if st.sidebar.button("Actualiser les données", width="stretch"):
    st.cache_data.clear()
    st.rerun()

try:
    df = get_data()
except RuntimeError as exc:
    st.error(str(exc))
    st.info(
        "Démarre au minimum MinIO avec "
        "`docker compose up -d minio minio-init`, puis relance le dashboard."
    )
    st.stop()

if df.empty:
    st.warning("La table Iceberg Silver est accessible mais ne contient aucun événement.")
    st.stop()

st.sidebar.header("Filtres")

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
        "Période",
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
    st.sidebar.warning("Aucune date valide pour la sélection actuelle.")

total_records = len(df_filtered)
latest_activity = df_filtered["created_at"].max()
unique_identifiers = df_filtered["author_hash"].dropna().nunique()
avg_text_words = df_filtered["text_len_words"].mean() if total_records else 0
missing_timestamps_pct = (
    df_filtered["created_at"].isna().mean() * 100 if total_records else 0
)
pipeline_error_pct = (
    df_filtered["error"].fillna("").str.strip().ne("").mean() * 100
    if total_records
    else 0
)

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Événements", f"{total_records:,}")
c2.metric(
    "Dernière activité",
    latest_activity.strftime("%Y-%m-%d %H:%M")
    if pd.notna(latest_activity)
    else "N/A",
)
c3.metric("Identifiants uniques", f"{unique_identifiers:,}")
c4.metric("Longueur moyenne", f"{avg_text_words:.1f} mots")
c5.metric("Dates manquantes", f"{missing_timestamps_pct:.1f}%")
c6.metric("Erreurs pipeline", f"{pipeline_error_pct:.1f}%")

st.subheader("Métadonnées d'engagement")
engagement_metrics = st.columns(len(ENGAGEMENT_LABELS))
for metric, (column, label) in zip(
    engagement_metrics,
    ENGAGEMENT_LABELS.items(),
):
    metric.metric(label, format_engagement_total(df_filtered[column]))

engagement_by_source = (
    df_filtered.groupby("source")[list(ENGAGEMENT_COLUMNS)]
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
    "Les valeurs N/A correspondent aux métriques non disponibles pour la "
    "plateforme. Le score Reddit est conservé séparément des likes."
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
        labels={"value": "Total", "metric": "Métrique"},
    )
    st.plotly_chart(fig_engagement, width="stretch")

st.subheader("Événements par source")
source_counts = (
    df_filtered.groupby("source")
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
        labels={"source": "Source", "records": "Événements"},
    )
    st.plotly_chart(fig_source, width="stretch")
else:
    st.info("Aucun événement pour les filtres actuels.")

st.subheader("Activité dans le temps")
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
        markers=True,
        labels={"date": "Date", "records": "Événements", "source": "Source"},
    )
    st.plotly_chart(fig_time, width="stretch")
else:
    st.info("Aucun timestamp valide pour les filtres actuels.")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Identifiants uniques par source")
    identifiers_by_source = (
        df_filtered.groupby("source")["author_hash"]
        .nunique()
        .reset_index(name="identifiers")
        .sort_values("identifiers", ascending=False)
    )
    fig_identifiers = px.bar(
        identifiers_by_source,
        x="source",
        y="identifiers",
        color="source",
        labels={"source": "Source", "identifiers": "Identifiants uniques"},
    )
    st.plotly_chart(fig_identifiers, width="stretch")

with col2:
    st.subheader("Longueur moyenne des textes")
    text_length_by_source = (
        df_filtered.groupby("source")["text_len_words"]
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
            "text_len_words": "Longueur moyenne en mots",
        },
    )
    st.plotly_chart(fig_length, width="stretch")

st.subheader("Qualité des données par source")
quality_by_source = (
    df_filtered.groupby("source")
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
            "total_records": "Événements",
            "missing_text_pct": "Texte manquant (%)",
            "missing_author_hash_pct": "Identifiant manquant (%)",
            "missing_created_at_pct": "Timestamp manquant (%)",
            "missing_url_pct": "URL manquante (%)",
            "pipeline_error_pct": "Erreur pipeline (%)",
        }
    )
)
st.dataframe(quality_by_source, width="stretch", hide_index=True)

st.subheader("Événements récents")
recent_df = df_filtered.copy()
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
recent_df["error"] = recent_df["error"].fillna("")
recent_df = recent_df[
    [
        "source",
        "created_at",
        "author_hash",
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
        "author_hash": "Identifiant hash",
        "text": "Contenu nettoyé",
        **{
            column: st.column_config.NumberColumn(label, format="%d")
            for column, label in ENGAGEMENT_LABELS.items()
        },
        "url": st.column_config.LinkColumn("URL"),
        "error": "Erreur",
    },
)
