LANGUAGES = {
    "All languages": "",
    "English": "en",
    "French": "fr",
    "Spanish": "es",
    "German": "de",
    "Italian": "it",
    "Portuguese": "pt",
}

X_CONTENT_FILTERS = {
    "All content": "",
    "With media": "filter:media",
    "With links": "filter:links",
    "Videos": "filter:videos",
    "Images": "filter:images",
}

YOUTUBE_ORDERS = {
    "Newest": "date",
    "Relevance": "relevance",
    "Most viewed": "viewCount",
    "Top rated": "rating",
}


def normalize_items(values):
    normalized = []
    for value in values:
        item = str(value).strip()
        if item and item.casefold() not in {
            existing.casefold() for existing in normalized
        }:
            normalized.append(item)
    return normalized


def quote_keyword(keyword):
    return f'"{keyword}"' if " " in keyword and not keyword.startswith('"') else keyword


def build_youtube_query(keywords, match_mode):
    terms = [quote_keyword(keyword) for keyword in normalize_items(keywords)]
    separator = " | " if match_mode == "OR" else " "
    return separator.join(terms)


def build_x_query(
    keywords,
    match_mode,
    language,
    content_filter,
    exclude_replies,
):
    terms = [quote_keyword(keyword) for keyword in normalize_items(keywords)]
    if match_mode == "OR" and len(terms) > 1:
        query = f"({' OR '.join(terms)})"
    else:
        query = " ".join(terms)

    operators = []
    if language:
        operators.append(f"lang:{language}")
    if content_filter:
        operators.append(content_filter)
    if exclude_replies:
        operators.append("-filter:replies")
    return " ".join([query, *operators]).strip()


def normalize_subreddit(value):
    return value.strip().removeprefix("r/").strip("/")
