import csv
import json
import sys

# Per-platform field typing. We detect the platform from the CSV header so the
# same column name (e.g. is_reply) can be an int for youtube (0/1) but a bool
# for x (True/False).
YOUTUBE_INT = {
    "video_view_count", "video_like_count", "video_duration_seconds",
    "thread_total_reply_count", "is_reply", "comment_like_count",
    "text_len_chars", "text_len_words", "has_question",
    "kw_price", "kw_range", "kw_charging",
}
YOUTUBE_FLOAT = {"upper_ratio", "video_age_days_at_comment"}

REDDIT_INT = {"depth", "score"}
REDDIT_FLOAT = {"created_utc"}

X_INT = {
    "article_index", "reply_count", "retweet_count", "like_count",
    "bookmark_count", "view_count", "media_count",
}
X_BOOL = {"is_reply", "is_pinned", "has_media"}

PLATFORM_TYPES = {
    "youtube": {"int": YOUTUBE_INT, "float": YOUTUBE_FLOAT, "bool": set()},
    "reddit":  {"int": REDDIT_INT,  "float": REDDIT_FLOAT,  "bool": set()},
    "x":       {"int": X_INT,       "float": set(),         "bool": X_BOOL},
}


def detect_platform(columns) -> str:
    cols = set(columns or [])
    if "tweet_text" in cols:
        return "x"
    if "comment_text" in cols:
        return "reddit"
    if "video_id" in cols and "text" in cols:
        return "youtube"
    sys.exit(f"cannot detect platform from columns: {sorted(cols)}")


def _parse_count(value: str):
    # Handle abbreviated counts from X like "1.7K", "2.3M", "1,234".
    s = str(value).strip().replace(",", "")
    if s == "":
        return None
    suffix = s[-1].upper()
    mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix)
    if mult is not None:
        s = s[:-1]
    else:
        mult = 1
    try:
        return int(float(s) * mult)
    except ValueError:
        return None


def coerce(field: str, value: str, types):
    if value is None or value == "":
        return None
    if field in types["int"]:
        return _parse_count(value)
    if field in types["float"]:
        try:
            return float(value)
        except ValueError:
            return None
    if field in types["bool"]:
        return str(value).strip().lower() in ("true", "1", "yes")
    return value  # string


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: python replay_csv_to_kafka.py <input.csv> <output.jsonl>")

    csv_path, out_path = sys.argv[1], sys.argv[2]

    with open(csv_path, encoding="utf-8-sig", newline="") as f_in, \
        open(out_path, "w", encoding="utf-8", newline="\n") as f_out:
        reader = csv.DictReader(f_in)
        platform = detect_platform(reader.fieldnames)
        types = PLATFORM_TYPES[platform]
        count = 0
        for row in reader:
            record = {field: coerce(field, raw, types) for field, raw in row.items()}
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    print(f"detected platform: {platform}")
    print(f"wrote {count} records to {out_path}")


if __name__ == "__main__":
    main()
