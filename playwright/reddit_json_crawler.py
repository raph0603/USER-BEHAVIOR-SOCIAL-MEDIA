import requests
import csv
import hashlib
import time
import random
from datetime import datetime, timezone

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
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Fichier introuvable : {INPUT_FILE}")
        return []

def make_json_url(post_url):
    post_url = post_url.rstrip("/")
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

def fetch_post_comments(post_url):
    json_url = make_json_url(post_url)

    try:
        response = requests.get(json_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except Exception as e:
        print(f"Erreur requête JSON pour {post_url} : {e}")
        return []

    if not isinstance(payload, list) or len(payload) < 2:
        print(f"Format JSON inattendu pour {post_url}")
        return []

    comments_listing = payload[1]
    children = comments_listing.get("data", {}).get("children", [])

    subreddit_subscribers = None
    try:
        subreddit_subscribers = payload[0]["data"]["children"][0]["data"].get("subreddit_subscribers")
    except (IndexError, KeyError, TypeError):
        pass

    rows = []
    extract_comment_tree(children, post_url, rows, depth=0, subreddit_member_count=subreddit_subscribers)
    return rows

def main():
    urls = load_urls()
    if not urls:
        return

    print(f"Scraping JSON de {len(urls)} URLs...")

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
            print(f"Commentaires récupérés : {len(rows)}")

            for row in rows:
                writer.writerow(row)

            csvfile.flush()
            total_comments += len(rows)

            sleep_time = random.uniform(1.5, 4.0)
            time.sleep(sleep_time)

    print(f"\n[TERMINÉ] {total_comments} commentaires sauvegardés dans {OUTPUT_CSV}")

if __name__ == "__main__":
    main()