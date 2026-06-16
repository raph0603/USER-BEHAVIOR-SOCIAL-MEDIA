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
        return "Aucun"
    return ", ".join(collaborators)


def collaborator_count(value):
    collaborators = normalize_collaborators(value)
    return pd.NA if collaborators is None else len(collaborators)


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

st.subheader("Auteurs et collaborations YouTube")
youtube_df = df_filtered[df_filtered["source"] == "youtube"].copy()
if youtube_df.empty:
    st.info("Aucun événement YouTube dans la sélection actuelle.")
else:
    youtube_unique_df = youtube_df.drop_duplicates(["url"]).copy()
    youtube_df = youtube_unique_df
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
    yt_metrics[0].metric("Vidéos YouTube", f"{len(youtube_df):,}")
    yt_metrics[1].metric(
        "Owners connus",
        f"{youtube_df['owner_channel_id'].dropna().nunique():,}",
    )
    yt_metrics[2].metric(
        "Avec collaborateurs",
        f"{videos_with_collaborators:,}",
    )
    yt_metrics[3].metric(
        "Collaborateurs uniques",
        f"{len(distinct_collaborator_ids):,}",
    )

    youtube_author_rows = (
        youtube_df.sort_values("created_at", ascending=False)
        .drop_duplicates(["url"])
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
            "text": "Titre",
            "owner_channel_id": "Owner channel ID",
            "collaborator_count": st.column_config.NumberColumn(
                "Collaborateurs",
                format="%d",
            ),
            "collaborators": "Collaborator channel IDs",
            "url": st.column_config.LinkColumn("URL"),
        },
    )
    st.caption(
        "`N/A` signifie que la page YouTube n'a pas permis de confirmer la "
        "liste. `Aucun` signifie que la vidéo a été lue et qu'aucun "
        "collaborateur accepté n'a été trouvé."
    )

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

st.subheader("Suivi par identifiant")
user_tracking_df = df_filtered.dropna(subset=["author_hash"]).copy()

if user_tracking_df.empty:
    st.info("Aucun identifiant disponible pour les filtres actuels.")
else:
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
        "events": "Activité",
        "total_engagement": "Engagement total",
        "avg_engagement_per_event": "Engagement moyen",
        "total_like_count": "Likes",
        "total_view_count": "Vues",
        "total_reply_count": "Réponses",
        "reply_rate_pct": "Taux de réponses",
    }
    ranking_metric = st.selectbox(
        "Classer les identifiants par",
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
            "author_display": "Identifiant",
            "sources": "Sources",
            "events": "Événements",
            "active_days": "Jours actifs",
            "reply_rate_pct": "Taux de réponses (%)",
            "total_like_count": "Likes",
            "total_view_count": "Vues",
            "total_reply_count": "Réponses",
            "total_comment_count": "Commentaires",
            "total_engagement": "Engagement total",
            "avg_engagement_per_event": "Engagement moyen",
            "last_activity": "Dernière activité",
        }
    )
    st.dataframe(
        top_users_table,
        width="stretch",
        hide_index=True,
        column_config={
            "Événements": st.column_config.NumberColumn(format="%d"),
            "Jours actifs": st.column_config.NumberColumn(format="%d"),
            "Taux de réponses (%)": st.column_config.NumberColumn(
                format="%.1f"
            ),
            "Likes": st.column_config.NumberColumn(format="%d"),
            "Vues": st.column_config.NumberColumn(format="%d"),
            "Réponses": st.column_config.NumberColumn(format="%d"),
            "Commentaires": st.column_config.NumberColumn(format="%d"),
            "Engagement total": st.column_config.NumberColumn(format="%d"),
            "Engagement moyen": st.column_config.NumberColumn(format="%.1f"),
        },
    )

    selector_options = top_users["author_hash"].tolist()
    if selector_options:
        selector_labels = {
            row.author_hash: (
                f"{row.author_display} - {int(row.events)} event(s) - "
                f"{row.sources}"
            )
            for row in top_users.itertuples()
        }
        selected_author = st.selectbox(
            "Identifiant à suivre",
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
        user_metrics[0].metric("Événements", format_count(selected_user.events))
        user_metrics[1].metric(
            "Jours actifs",
            format_count(selected_user.active_days),
        )
        user_metrics[2].metric(
            "Taux de réponses",
            format_rate(selected_user.reply_rate_pct),
        )
        user_metrics[3].metric(
            "Likes",
            format_count(selected_user.total_like_count),
        )
        user_metrics[4].metric(
            "Vues",
            format_count(selected_user.total_view_count),
        )
        user_metrics[5].metric(
            "Engagement moyen",
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
                "events": "Événements cumulés",
                "likes": "Likes cumulés",
                "views": "Vues cumulées",
                "replies": "Réponses cumulées",
                "comments": "Commentaires cumulés",
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
                    "value": "Total cumulé",
                    "metric": "Métrique",
                },
            )
            st.plotly_chart(fig_user_progress, width="stretch")
        else:
            st.info("Aucune date valide pour cet identifiant.")

        selected_recent = selected_events.sort_values(
            "created_at",
            ascending=False,
        ).head(25)
        selected_recent = selected_recent[
            [
                "source",
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
                "created_at": "Timestamp",
                "text": "Contenu",
                "like_count": st.column_config.NumberColumn(
                    "Likes",
                    format="%d",
                ),
                "view_count": st.column_config.NumberColumn(
                    "Vues",
                    format="%d",
                ),
                "comment_count": st.column_config.NumberColumn(
                    "Commentaires",
                    format="%d",
                ),
                "reply_count": st.column_config.NumberColumn(
                    "Réponses",
                    format="%d",
                ),
                "url": st.column_config.LinkColumn("URL"),
            },
        )
    st.caption(
        "Le taux de réponses correspond à la part des événements de cet "
        "identifiant qui ont au moins une réponse observée. Les métriques "
        "restent dépendantes de ce que chaque plateforme expose."
    )

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
        "author_hash": "Identifiant hash",
        "owner_channel_id": "Owner channel ID",
        "collaborators": "Collaborator channel IDs",
        "text": "Contenu nettoyé",
        **{
            column: st.column_config.NumberColumn(label, format="%d")
            for column, label in ENGAGEMENT_LABELS.items()
        },
        "url": st.column_config.LinkColumn("URL"),
        "error": "Erreur",
    },
)
