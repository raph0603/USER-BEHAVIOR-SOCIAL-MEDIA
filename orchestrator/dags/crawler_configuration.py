import json

from airflow.models import Variable


VARIABLE_KEY = "crawler_dashboard_config"
INSIGHT_VARIABLE_KEY = "insight_dashboard_config"
DEFAULT_CRAWLER_CONFIG = {
    "keywords": [
        "electric vehicle",
        "EV",
        "electric car",
        "Tesla",
        "EV charging",
        "battery range",
    ],
    "search_language": "en",
    "keyword_match_mode": "OR",
    "youtube_keywords": [
        "electric vehicle",
        "EV",
        "electric car",
        "Tesla",
        "EV charging",
        "battery range",
    ],
    "youtube_event_count": 1000,
    "youtube_search_queries": [
        '"electric vehicle" | EV | "electric car" | Tesla | '
        '"EV charging" | "battery range"'
    ],
    "youtube_search_language": "en",
    "youtube_keyword_match_mode": "OR",
    "youtube_search_order": "date",
    "x_keywords": [
        "electric vehicle",
        "EV",
        "electric car",
        "Tesla",
        "EV charging",
        "battery range",
    ],
    "x_event_count": 1000,
    "x_search_queries": [
        '(electric vehicle OR EV OR "electric car") lang:en -filter:replies',
        '(Tesla OR "EV charging" OR "battery range") lang:en -filter:replies',
    ],
    "x_search_language": "en",
    "x_keyword_match_mode": "OR",
    "x_scroll_rounds": 5,
    "x_content_filter": "",
    "x_exclude_replies": True,
    "reddit_event_count": 1000,
    "reddit_keywords": [
        "electric vehicle",
        "EV",
        "electric car",
        "Tesla",
        "EV charging",
        "battery range",
    ],
    "reddit_keyword_match_mode": "OR",
    "reddit_subreddits": ["electricvehicles", "teslamotors"],
    "reddit_comment_scan_limit": 1000,
}
DEFAULT_INSIGHT_CONFIG = {
    "lookback_days": 15,
    "max_events_per_source": 1000,
    "x_headless": True,
}


def _load_config(variable_key, defaults):
    raw_value = Variable.get(variable_key, default_var=None)
    if not raw_value:
        return defaults.copy()

    try:
        stored_config = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return defaults.copy()

    config = defaults.copy()
    config.update(
        {
            key: value
            for key, value in stored_config.items()
            if key in defaults
        }
    )
    return config


def load_crawler_config():
    return _load_config(VARIABLE_KEY, DEFAULT_CRAWLER_CONFIG)


def load_insight_config():
    return _load_config(INSIGHT_VARIABLE_KEY, DEFAULT_INSIGHT_CONFIG)
