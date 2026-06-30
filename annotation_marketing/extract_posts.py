"""
Extrait les posts originaux de YouTube, X et Reddit vers un format commun :
{post_id, platform, language, text}
Sauvegarde un fichier all_posts_raw.jsonl + un résumé de comptage par (platform, language).
"""
import json
import csv
import glob
from pathlib import Path
from collections import Counter, defaultdict

BASE = Path("/sessions/gracious-stoic-albattani/mnt/Projet_user_behavior/Codes")
OUT = Path("/sessions/gracious-stoic-albattani/mnt/outputs")
YT_CACHE = Path("/tmp/yt_extract")


def extract_youtube(lang_dir: str, language: str):
    posts = []
    folder = YT_CACHE / lang_dir / "yt_raw_json"
    for f in sorted(glob.glob(str(folder / "*.json"))):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        vid = d.get("video_id")
        snippet = (d.get("video_metadata") or {}).get("snippet") or {}
        title = (snippet.get("title") or "").strip()
        description = (snippet.get("description") or "").strip()
        if description and description != title:
            text = f"{title}\n{description}"
        else:
            text = title or description
        if not text:
            continue
        posts.append({
            "post_id": f"yt_{language}_{vid}",
            "platform": "youtube",
            "language": language,
            "text": text,
        })
    return posts


def extract_x(csv_path: str, language: str):
    posts = []
    seen_pages = set()
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("is_reply") != "False" or row.get("article_index") != "0":
                continue
            page_url = row.get("page_url")
            if page_url in seen_pages:
                continue
            seen_pages.add(page_url)
            text = (row.get("tweet_text") or "").strip()
            if not text:
                continue
            posts.append({
                "post_id": f"x_{language}_{row.get('status_id')}",
                "platform": "x",
                "language": language,
                "text": text,
            })
    return posts


def extract_reddit(jsonl_path: str, language: str):
    posts = []
    if not Path(jsonl_path).exists():
        return posts
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            title = (d.get("title") or "").strip()
            selftext = (d.get("selftext") or "").strip()
            if selftext and selftext != title:
                text = f"{title}\n{selftext}"
            else:
                text = title or selftext
            if not text:
                continue
            post_url = d.get("post_url", "")
            pid = post_url.rstrip("/").split("/")[-1] if post_url else str(hash(text))
            posts.append({
                "post_id": f"reddit_{language}_{pid}",
                "platform": "reddit",
                "language": language,
                "text": text,
            })
    return posts


def main():
    all_posts = []
    all_posts += extract_youtube("anglais", "en")
    all_posts += extract_youtube("viet", "vi")
    all_posts += extract_x(str(BASE / "webscrap_x/anglais/x_thread_parallel_20260601_183556.csv"), "en")
    all_posts += extract_x(str(BASE / "webscrap_x/viet/x_thread_parallel_20260618_050101.csv"), "vi")
    all_posts += extract_reddit(str(BASE / "webscrap_reddit/anglais/reddit_posts.jsonl"), "en")
    all_posts += extract_reddit(str(BASE / "webscrap_reddit/viet/reddit_posts.jsonl"), "vi")

    # dédoublonnage par texte exact (sécurité)
    seen_text = set()
    deduped = []
    for p in all_posts:
        key = (p["platform"], p["language"], p["text"])
        if key in seen_text:
            continue
        seen_text.add(key)
        deduped.append(p)

    out_file = OUT / "all_posts_raw.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for p in deduped:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    counts = Counter((p["platform"], p["language"]) for p in deduped)
    print("=== Comptage posts originaux disponibles (avant équilibrage) ===")
    for (platform, lang), n in sorted(counts.items()):
        print(f"  {platform:10s} {lang:3s} : {n}")
    print(f"\nTotal : {len(deduped)} posts -> {out_file}")


if __name__ == "__main__":
    main()
