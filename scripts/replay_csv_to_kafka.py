import csv
import json
import sys

INT_FIELDS = {
    "video_view_count", "video_like_count", "video_duration_seconds",
    "thread_total_reply_count", "is_reply", "comment_like_count",
    "text_len_chars", "text_len_words", "has_question",
    "kw_price", "kw_range", "kw_charging",
}
FLOAT_FIELDS = {"upper_ratio", "video_age_days_at_comment"}

def coerce(field: str, value: str):
    if value is None or value == "":
        return None
    if field in INT_FIELDS:
        return int(float(value))
    if field in FLOAT_FIELDS:
        return float(value)
    return value # string

def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: python replay_csv_to_kafka.py <input.csv> <output.jsonl>")

    csv_path, out_path = sys.argv[1], sys.argv[2]
    
    with open(csv_path, encoding="utf-8-sig", newline="") as f_in, \
        open(out_path, "w", encoding="utf-8", newline="\n") as f_out:
        reader = csv.DictReader(f_in)
        count = 0
        for row in reader:
            record = {field: coerce(field, raw) for field, raw in row.items()}
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    print(f"wrote {count} records to {out_path}")

if __name__ == "__main__":
    main()