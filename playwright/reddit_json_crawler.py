import requests
import csv
import hashlib
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

INPUT_FILE = "reddit_urls.txt"
OUTPUT_CSV = f"reddit_comments_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

def hash_username(username):
    if not username:
        return "anonymous"
    return hashlib.sha256(username.encode("utf-8")).hexdigest()

def load_urls():
    try:
        # utf-8-sig strips a BOM if the file was saved by PowerShell Out-File -Encoding utf8
        with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Input file not found: {INPUT_FILE}")
        return []

def make_json_url(post_url, host=None):
    post_url = post_url.rstrip("/")
    if host:  # swap the hostname (e.g. old.reddit.com fallback) while keeping the path
        parsed = urlparse(post_url)
        post_url = urlunparse((parsed.scheme or "https", host, parsed.path, "", "", ""))
    return post_url + ".json?limit=500"

def unix_to_iso(ts):
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except:
        return ""

def extract_comment_tree(children, post_url, rows, depth=0, subreddit_member_count=None):
    for child in children:
        kind = child.get("kind")
        data = child.get("data", {})

        if kind != "t1":
            continue

        author = data.get("author", "")
        body = data.get("body", "")
        comment_id = data.get("id", "")
        parent_id = data.get("parent_id", "")
        created_utc = data.get("created_utc")
        score = data.get("score", None)
        permalink = data.get("permalink", "")

        if body:
            rows.append({
                "post_url": post_url,
                "comment_id": comment_id,
                "parent_id": parent_id,
                "depth": depth,
                "author_hash": hash_username(author),
                "author": author,
                "comment_text": body.replace("\n", " ").replace("\r", " "),
                "created_utc": created_utc,
                "created_iso": unix_to_iso(created_utc),
                "score": score,
                "comment_permalink": f"https://www.reddit.com{permalink}" if permalink else "",
                "subreddit_member_count": subreddit_member_count
            })

        replies = data.get("replies")
        if isinstance(replies, dict):
            reply_children = replies.get("data", {}).get("children", [])
            extract_comment_tree(reply_children, post_url, rows, depth=depth+1, subreddit_member_count=subreddit_member_count)

def _fetch_json(post_url):
    """Fetch the post JSON; on 403/429 (Reddit blocking www) retry via old.reddit.com."""
    for host in (None, "old.reddit.com"):
        json_url = make_json_url(post_url, host)
        try:
            response = requests.get(json_url, headers=HEADERS, timeout=30)
            if response.status_code in (403, 429):
                print(f"  HTTP {response.status_code} for {json_url} -> trying fallback")
                time.sleep(random.uniform(1.0, 2.0))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"  Error {json_url}: {e}")
            continue
    return None


def fetch_post_comments(post_url):
    payload = _fetch_json(post_url)
    if payload is None:
        print(f"JSON request failed for {post_url} (www + old.reddit both blocked)")
        return []

    if not isinstance(payload, list) or len(payload) < 2:
        print(f"Unexpected JSON format for {post_url}")
        return []

    comments_listing = payload[1]
    children = comments_listing.get("data", {}).get("children", [])

    subreddit_subscribers = None
    try:
        post_data = payload[0]["data"]["children"][0]["data"]
        if isinstance(post_data, dict):
            subreddit_subscribers = post_data.get("subreddit_subscribers")
    except (IndexError, KeyError, TypeError, AttributeError):
        pass

    rows = []
    extract_comment_tree(children, post_url, rows, depth=0, subreddit_member_count=subreddit_subscribers)
    return rows

def main():
    urls = load_urls()
    if not urls:
        return

    print(f"Scraping JSON from {len(urls)} URLs...")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = [
            "post_url",
            "comment_id",
            "parent_id",
            "depth",
            "author_hash",
            "author",
            "comment_text",
            "created_utc",
            "created_iso",
            "score",
            "comment_permalink",
            "subreddit_member_count"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        total_comments = 0

        for i, url in enumerate(urls, start=1):
            print(f"\n--- Post {i}/{len(urls)} ---")
            print(url)

            rows = fetch_post_comments(url)
            print(f"Comments fetched: {len(rows)}")

            for row in rows:
                writer.writerow(row)

            csvfile.flush()
            total_comments += len(rows)

            sleep_time = random.uniform(1.5, 4.0)
            time.sleep(sleep_time)

    print(f"\n[DONE] {total_comments} comments saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()