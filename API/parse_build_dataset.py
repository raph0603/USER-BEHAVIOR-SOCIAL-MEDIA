from pathlib import Path
import pandas as pd
import numpy as np
import re
import json
import hashlib

INPUT_DIR = Path("yt_raw_json")
OUTPUT_CSV = Path("user_behavior_dataset_v2.csv")

# =========================
# ANONYMISATION (NIST SP 800-88 R1)
# =========================
# Le Salt (sel) rend impossible le déchiffrement inversé (Rainbow Tables)
SECRET_SALT = "EV_Project_2026_SecureKey!"

def anonymize_identity(text):
    """Applique f(P) = SHA-256(P || Salt)"""
    if not text or pd.isna(text):
        return None
    payload = f"{text}{SECRET_SALT}".encode('utf-8')
    return hashlib.sha256(payload).hexdigest()

def parse_iso8601_duration(d):
    """Convertit une durée ISO 8601 en secondes."""
    if not d: return None
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', d)
    if not m: return None
    return (int(m.group(1) or 0) * 3600 + 
            int(m.group(2) or 0) * 60 + 
            int(m.group(3) or 0))

def upper_ratio(s):
    letters = re.findall(r"[A-Za-z]", s)
    if not letters: return 0.0
    return len([ch for ch in letters if ch.isupper()]) / len(letters)

# =========================
# PARSING & FEATURE ENGINEERING
# =========================
def main():
    rows = []
    json_files = list(INPUT_DIR.glob("*.json"))
    print(f"Extraction des données depuis {len(json_files)} fichiers JSON...")

    for json_path in json_files:
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        video_id = data.get("video_id")
        meta = data.get("video_metadata", {}) or {}
        snippet = meta.get("snippet", {}) or {}
        stats = meta.get("statistics", {}) or {}
        content = meta.get("contentDetails", {}) or {}

        v_title = snippet.get("title")
        v_pub_at = snippet.get("publishedAt")
        v_views = int(stats.get("viewCount", 0)) if stats.get("viewCount") else None
        v_likes = int(stats.get("likeCount", 0)) if stats.get("likeCount") else None
        v_dur_sec = parse_iso8601_duration(content.get("duration"))

        for page in data.get("comment_threads_pages", []):
            for item in page.get("items", []):
                thread_id = item.get("id")
                thread_snip = item.get("snippet", {}) or {}
                t_total_replies = thread_snip.get("totalReplyCount", 0)

                # --- TOP LEVEL COMMENT ---
                top = thread_snip.get("topLevelComment", {}) or {}
                top_snip = top.get("snippet", {}) or {}
                text_top = top_snip.get("textOriginal") or top_snip.get("textDisplay", "")
                
                rows.append({
                    "video_id": video_id,
                    "video_title": v_title,
                    "video_published_at": v_pub_at,
                    "video_view_count": v_views,
                    "video_like_count": v_likes,
                    "video_duration_seconds": v_dur_sec,
                    
                    "thread_id": thread_id,
                    "thread_total_reply_count": t_total_replies,
                    
                    "comment_id": top.get("id"),
                    "is_reply": 0,
                    # ANONYMISATION DE L'AUTEUR ICI
                    "author_hash": anonymize_identity(top_snip.get("authorDisplayName")),
                    "text": text_top,
                    "comment_like_count": top_snip.get("likeCount", 0),
                    "comment_published_at": top_snip.get("publishedAt"),
                })

                # --- REPLIES ---
                for reply in (item.get("replies", {}) or {}).get("comments", []):
                    rep_snip = reply.get("snippet", {}) or {}
                    text_rep = rep_snip.get("textOriginal") or rep_snip.get("textDisplay", "")
                    
                    rows.append({
                        "video_id": video_id,
                        "video_title": v_title,
                        "video_published_at": v_pub_at,
                        "video_view_count": v_views,
                        "video_like_count": v_likes,
                        "video_duration_seconds": v_dur_sec,
                        
                        "thread_id": thread_id,
                        "thread_total_reply_count": t_total_replies,
                        
                        "comment_id": reply.get("id"),
                        "is_reply": 1,
                        # ANONYMISATION DE L'AUTEUR ICI
                        "author_hash": anonymize_identity(rep_snip.get("authorDisplayName")),
                        "text": text_rep,
                        "comment_like_count": rep_snip.get("likeCount", 0),
                        "comment_published_at": rep_snip.get("publishedAt"),
                    })

    # --- CRÉATION DU DATAFRAME ET CALCUL DES FEATURES ---
    print(f"Construction du DataFrame ({len(rows)} commentaires trouvés)...")
    df = pd.DataFrame(rows)
    df["text"] = df["text"].fillna("").astype(str)

    # Conversion des dates
    df["video_published_at"] = pd.to_datetime(df["video_published_at"], errors="coerce")
    df["comment_published_at"] = pd.to_datetime(df["comment_published_at"], errors="coerce")

    # NLP Features basiques
    df["text_len_chars"] = df["text"].str.len()
    df["text_len_words"] = df["text"].str.split().apply(len)
    df["has_question"] = (df["text"].str.count(r"\?") > 0).astype(int)
    df["upper_ratio"] = df["text"].apply(upper_ratio)

    # Features Mots-clés EV
    df["kw_price"] = df["text"].str.contains(r"price|expensive|cheap|cost|€|\$", case=False, regex=True).astype(int)
    df["kw_range"] = df["text"].str.contains(r"range|autonomy|km|miles|battery", case=False, regex=True).astype(int)
    df["kw_charging"] = df["text"].str.contains(r"charge|charging|charger", case=False, regex=True).astype(int)

    # Features Temporelles (Sentiment Drift)
    df["video_age_days_at_comment"] = ((df["comment_published_at"] - df["video_published_at"]).dt.total_seconds() / (24 * 3600))
    
    # Remplacement des valeurs infinies ou aberrantes
    df = df.replace([np.inf, -np.inf], np.nan)

    # Sauvegarde
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"[SUCCÈS] Dataset final sauvegardé : {OUTPUT_CSV} ({len(df)} lignes, {len(df.columns)} colonnes).")
    print("-> L'anonymisation SHA-256 a été appliquée avec succès aux auteurs.")

if __name__ == "__main__":
    main()