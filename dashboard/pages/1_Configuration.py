import streamlit as st

from airflow_monitoring import (
    AirflowClient,
    CRAWLER_VARIABLE_KEY,
    INSIGHT_VARIABLE_KEY,
)
from navigation import render_navigation
from query_builder import (
    LANGUAGES,
    X_CONTENT_FILTERS,
    YOUTUBE_ORDERS,
    build_x_query,
    build_youtube_query,
    normalize_items,
    normalize_subreddit,
)


DEFAULT_KEYWORDS = [
    "electric vehicle",
    "EV",
    "electric car",
    "Tesla",
    "EV charging",
    "battery range",
]
DEFAULT_CRAWLER_CONFIG = {
    "youtube_keywords": DEFAULT_KEYWORDS,
    "youtube_event_count": 500,
    "youtube_search_language": "en",
    "youtube_keyword_match_mode": "OR",
    "youtube_search_order": "date",
    "x_keywords": DEFAULT_KEYWORDS,
    "x_event_count": 500,
    "x_search_language": "en",
    "x_keyword_match_mode": "OR",
    "x_scroll_rounds": 5,
    "x_content_filter": "",
    "x_exclude_replies": True,
    "reddit_keywords": DEFAULT_KEYWORDS,
    "reddit_keyword_match_mode": "OR",
    "reddit_event_count": 500,
    "reddit_subreddits": ["electricvehicles", "teslamotors"],
    "reddit_comment_scan_limit": 100,
}
DEFAULT_INSIGHT_CONFIG = {
    "lookback_days": 15,
    "max_events_per_source": 500,
    "x_headless": True,
}


def label_for_value(options, value):
    return next(
        (label for label, option_value in options.items() if option_value == value),
        next(iter(options)),
    )


@st.dialog("Confirmer la suppression")
def confirm_item_deletion(state_key, index, item):
    st.write(f"Supprimer **{item}** ?")
    cancel_column, delete_column = st.columns(2)
    if cancel_column.button("Annuler", width="stretch"):
        st.rerun()
    if delete_column.button(
        "Supprimer",
        type="primary",
        icon=":material/delete:",
        width="stretch",
    ):
        current_items = st.session_state.get(state_key, [])
        if index < len(current_items) and current_items[index] == item:
            del current_items[index]
        st.rerun()


def render_item_list(state_key, title, empty_message):
    st.markdown(f"**{title}**")
    items = st.session_state[state_key]
    if not items:
        st.info(empty_message)
        return

    for index, item in enumerate(items):
        with st.container(
            horizontal=True,
            vertical_alignment="center",
            gap="small",
        ):
            st.code(item, width="stretch")
            if st.button(
                ":material/delete:",
                key=f"remove_{state_key}_{index}",
                help=f"Supprimer {item}",
                type="tertiary",
                width="content",
            ):
                confirm_item_deletion(state_key, index, item)


def render_add_form(state_key, form_key, label, button_label, normalizer=None):
    with st.form(form_key, clear_on_submit=True):
        new_item = st.text_input(label)
        submitted = st.form_submit_button(button_label)
    item = normalizer(new_item) if normalizer else new_item.strip()
    if submitted and item:
        st.session_state[state_key] = normalize_items(
            [*st.session_state[state_key], item]
        )
        st.rerun()


def config_value(config, platform_key, legacy_key, fallback):
    return config.get(platform_key, config.get(legacy_key, fallback))


st.set_page_config(page_title="Configuration des crawlers", layout="wide")
render_navigation()
st.title("Configuration des crawlers")
st.caption(
    "Chaque plateforme possède ses propres mots-clés, filtres et limites. "
    "Les recherches finales sont générées automatiquement."
)

client = AirflowClient()
try:
    crawler_config = client.get_variable(
        CRAWLER_VARIABLE_KEY,
        DEFAULT_CRAWLER_CONFIG,
    )
    insight_config = client.get_variable(
        INSIGHT_VARIABLE_KEY,
        DEFAULT_INSIGHT_CONFIG,
    )
except Exception as exc:
    st.warning(f"Configuration Airflow indisponible: {exc}")
    crawler_config = DEFAULT_CRAWLER_CONFIG.copy()
    insight_config = DEFAULT_INSIGHT_CONFIG.copy()

state_defaults = {
    "youtube_keywords": config_value(
        crawler_config,
        "youtube_keywords",
        "keywords",
        DEFAULT_KEYWORDS,
    ),
    "x_keywords": config_value(
        crawler_config,
        "x_keywords",
        "keywords",
        DEFAULT_KEYWORDS,
    ),
    "reddit_keywords": config_value(
        crawler_config,
        "reddit_keywords",
        "keywords",
        DEFAULT_KEYWORDS,
    ),
    "crawler_subreddits": crawler_config["reddit_subreddits"],
}
for state_key, values in state_defaults.items():
    if state_key not in st.session_state:
        st.session_state[state_key] = normalize_items(values)

youtube_tab, x_tab, reddit_tab, refresh_tab = st.tabs(
    ["YouTube", "X", "Reddit", "Rafraîchissement"]
)

with youtube_tab:
    st.subheader("Recherche YouTube")
    st.caption(
        "La langue privilégie les résultats correspondants. Le tri est appliqué "
        "directement par l'API YouTube."
    )
    youtube_settings, youtube_terms = st.columns([1, 2])
    with youtube_settings:
        youtube_event_count = st.number_input(
            "Nombre maximal de vidéos",
            min_value=1,
            max_value=500,
            value=int(crawler_config["youtube_event_count"]),
        )
        youtube_language_label = st.selectbox(
            "Langue privilégiée",
            options=list(LANGUAGES),
            index=list(LANGUAGES).index(
                label_for_value(
                    LANGUAGES,
                    config_value(
                        crawler_config,
                        "youtube_search_language",
                        "search_language",
                        "en",
                    ),
                )
            ),
            key="youtube_language",
        )
        youtube_match_mode = st.selectbox(
            "Correspondance",
            options=["OR", "AND"],
            index=0
            if config_value(
                crawler_config,
                "youtube_keyword_match_mode",
                "keyword_match_mode",
                "OR",
            )
            == "OR"
            else 1,
            format_func=lambda value: "Au moins un mot (OU)"
            if value == "OR"
            else "Tous les mots (ET)",
            key="youtube_match_mode",
        )
        youtube_order_label = st.selectbox(
            "Trier les résultats",
            options=list(YOUTUBE_ORDERS),
            index=list(YOUTUBE_ORDERS).index(
                label_for_value(
                    YOUTUBE_ORDERS,
                    crawler_config["youtube_search_order"],
                )
            ),
        )
    with youtube_terms:
        render_add_form(
            "youtube_keywords",
            "add_youtube_keyword",
            "Ajouter un mot-clé YouTube",
            "Ajouter",
        )
        render_item_list(
            "youtube_keywords",
            "Mots-clés YouTube",
            "Ajoutez au moins un mot-clé YouTube.",
        )

    youtube_query = build_youtube_query(
        st.session_state.youtube_keywords,
        youtube_match_mode,
    )
    st.markdown("**Aperçu envoyé à YouTube**")
    st.code(youtube_query or "Aucun mot-clé", wrap_lines=True)

with x_tab:
    st.subheader("Recherche X")
    st.caption(
        "Les opérateurs de langue, de contenu et d'exclusion sont ajoutés à la "
        "requête X générée."
    )
    x_settings, x_terms = st.columns([1, 2])
    with x_settings:
        x_event_count = st.number_input(
            "Nombre maximal de posts",
            min_value=1,
            max_value=500,
            value=int(crawler_config["x_event_count"]),
        )
        x_scroll_rounds = st.number_input(
            "Scrolls par recherche",
            min_value=1,
            max_value=50,
            value=int(crawler_config["x_scroll_rounds"]),
        )
        x_language_label = st.selectbox(
            "Langue",
            options=list(LANGUAGES),
            index=list(LANGUAGES).index(
                label_for_value(
                    LANGUAGES,
                    config_value(
                        crawler_config,
                        "x_search_language",
                        "search_language",
                        "en",
                    ),
                )
            ),
            key="x_language",
        )
        x_match_mode = st.selectbox(
            "Correspondance",
            options=["OR", "AND"],
            index=0
            if config_value(
                crawler_config,
                "x_keyword_match_mode",
                "keyword_match_mode",
                "OR",
            )
            == "OR"
            else 1,
            format_func=lambda value: "Au moins un mot (OU)"
            if value == "OR"
            else "Tous les mots (ET)",
            key="x_match_mode",
        )
        x_filter_label = st.selectbox(
            "Type de contenu",
            options=list(X_CONTENT_FILTERS),
            index=list(X_CONTENT_FILTERS).index(
                label_for_value(
                    X_CONTENT_FILTERS,
                    crawler_config["x_content_filter"],
                )
            ),
        )
        x_exclude_replies = st.checkbox(
            "Exclure les réponses",
            value=bool(crawler_config["x_exclude_replies"]),
        )
    with x_terms:
        render_add_form(
            "x_keywords",
            "add_x_keyword",
            "Ajouter un mot-clé X",
            "Ajouter",
        )
        render_item_list(
            "x_keywords",
            "Mots-clés X",
            "Ajoutez au moins un mot-clé X.",
        )

    x_language = LANGUAGES[x_language_label]
    x_query = build_x_query(
        st.session_state.x_keywords,
        x_match_mode,
        x_language,
        X_CONTENT_FILTERS[x_filter_label],
        x_exclude_replies,
    )
    st.markdown("**Aperçu envoyé à X**")
    st.code(x_query or "Aucun mot-clé", wrap_lines=True)

with reddit_tab:
    st.subheader("Collecte Reddit")
    st.caption(
        "Reddit fournit les commentaires récents par subreddit. Les mots-clés "
        "sont ensuite appliqués localement au texte des commentaires."
    )
    reddit_settings, reddit_terms = st.columns([1, 2])
    with reddit_settings:
        reddit_event_count = st.number_input(
            "Nombre maximal de commentaires",
            min_value=1,
            max_value=500,
            value=int(crawler_config["reddit_event_count"]),
        )
        reddit_scan_limit = st.number_input(
            "Commentaires inspectés par subreddit",
            min_value=1,
            max_value=100,
            value=int(crawler_config["reddit_comment_scan_limit"]),
        )
        reddit_match_mode = st.selectbox(
            "Correspondance des mots-clés",
            options=["OR", "AND"],
            index=0
            if crawler_config["reddit_keyword_match_mode"] == "OR"
            else 1,
            format_func=lambda value: "Au moins un mot (OU)"
            if value == "OR"
            else "Tous les mots (ET)",
        )
        st.info(
            "Le flux public Reddit ne propose pas de filtre de langue natif."
        )
    with reddit_terms:
        render_add_form(
            "reddit_keywords",
            "add_reddit_keyword",
            "Ajouter un mot-clé Reddit",
            "Ajouter",
        )
        render_item_list(
            "reddit_keywords",
            "Mots-clés Reddit",
            "Ajoutez au moins un mot-clé Reddit.",
        )

    st.markdown("**Subreddits surveillés**")
    render_add_form(
        "crawler_subreddits",
        "add_subreddit",
        "Ajouter un subreddit",
        "Ajouter",
        normalizer=normalize_subreddit,
    )
    render_item_list(
        "crawler_subreddits",
        "Liste des subreddits",
        "Ajoutez au moins un subreddit.",
    )
    st.markdown("**Aperçu du filtrage Reddit**")
    st.code(
        f"{reddit_match_mode}: "
        + ", ".join(st.session_state.reddit_keywords),
        wrap_lines=True,
    )

with refresh_tab:
    st.subheader("Rafraîchissement des métadonnées")
    st.caption(
        "Ces réglages concernent le DAG qui actualise les likes, vues, réponses "
        "et autres métriques des événements déjà présents dans Silver."
    )
    refresh_column, refresh_actions = st.columns([2, 1])
    with refresh_column:
        lookback_days = st.number_input(
            "Fenêtre en jours",
            min_value=1,
            max_value=365,
            value=int(insight_config["lookback_days"]),
        )
        max_events_per_source = st.number_input(
            "Limite d'événements par source",
            min_value=1,
            max_value=2000,
            value=int(insight_config["max_events_per_source"]),
        )
        refresh_x_headless = st.checkbox(
            "X en mode headless",
            value=bool(insight_config["x_headless"]),
        )
    with refresh_actions:
        save_refresh = st.button(
            "Enregistrer",
            key="save_refresh",
            width="stretch",
        )
        launch_refresh = st.button(
            "Enregistrer et lancer",
            key="launch_refresh",
            type="primary",
            width="stretch",
        )

    if save_refresh or launch_refresh:
        refresh_config = {
            "lookback_days": int(lookback_days),
            "max_events_per_source": int(max_events_per_source),
            "x_headless": refresh_x_headless,
        }
        try:
            client.save_variable(INSIGHT_VARIABLE_KEY, refresh_config)
            if launch_refresh:
                run = client.trigger_dag(
                    "refresh_recent_engagement_insights",
                    refresh_config,
                )
                st.success(
                    "Rafraîchissement lancé: "
                    f"{run.get('dag_run_id', 'nouveau run')}"
                )
            else:
                st.success("Configuration de rafraîchissement enregistrée.")
        except Exception as exc:
            st.error(f"Impossible d'enregistrer: {exc}")

st.divider()
st.subheader("Enregistrer ou lancer la collecte")
st.caption(
    "Cette action applique ensemble les réglages YouTube, X et Reddit ci-dessus."
)
pipeline_column, save_column, launch_column = st.columns([2, 1, 1])
with pipeline_column:
    dag_id = st.selectbox(
        "Pipeline",
        [
            "user_behavior_lakehouse_no_row_checks",
            "user_behavior_lakehouse",
        ],
    )
save_collection = save_column.button(
    "Enregistrer",
    key="save_collection",
    width="stretch",
)
launch_collection = launch_column.button(
    "Enregistrer et lancer",
    key="launch_collection",
    type="primary",
    width="stretch",
)

if save_collection or launch_collection:
    missing_platforms = [
        platform
        for platform, state_key in [
            ("YouTube", "youtube_keywords"),
            ("X", "x_keywords"),
            ("Reddit", "reddit_keywords"),
        ]
        if not st.session_state[state_key]
    ]
    if missing_platforms:
        st.error(
            "Ajoutez au moins un mot-clé pour: "
            + ", ".join(missing_platforms)
        )
    elif not st.session_state.crawler_subreddits:
        st.error("Ajoutez au moins un subreddit.")
    else:
        collection_config = {
            "youtube_keywords": st.session_state.youtube_keywords,
            "youtube_event_count": int(youtube_event_count),
            "youtube_search_queries": [youtube_query],
            "youtube_search_language": LANGUAGES[youtube_language_label],
            "youtube_keyword_match_mode": youtube_match_mode,
            "youtube_search_order": YOUTUBE_ORDERS[youtube_order_label],
            "x_keywords": st.session_state.x_keywords,
            "x_event_count": int(x_event_count),
            "x_search_queries": [x_query],
            "x_search_language": x_language,
            "x_keyword_match_mode": x_match_mode,
            "x_scroll_rounds": int(x_scroll_rounds),
            "x_content_filter": X_CONTENT_FILTERS[x_filter_label],
            "x_exclude_replies": x_exclude_replies,
            "reddit_keywords": st.session_state.reddit_keywords,
            "reddit_keyword_match_mode": reddit_match_mode,
            "reddit_event_count": int(reddit_event_count),
            "reddit_subreddits": st.session_state.crawler_subreddits,
            "reddit_comment_scan_limit": int(reddit_scan_limit),
        }
        try:
            client.save_variable(CRAWLER_VARIABLE_KEY, collection_config)
            if launch_collection:
                run = client.trigger_dag(dag_id, collection_config)
                st.success(
                    f"Collecte lancée: {run.get('dag_run_id', dag_id)}"
                )
            else:
                st.success("Configuration de collecte enregistrée.")
        except Exception as exc:
            st.error(f"Impossible d'enregistrer: {exc}")
