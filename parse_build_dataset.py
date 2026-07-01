from pathlib import Path
import pandas as pd
import numpy as np
import re
import json
import hashlib

# Configuration des répertoires et fichiers cibles
INPUT_DIR = Path("yt_raw_json")
OUTPUT_COMMENTS_CSV = Path("user_interactions_dataset.csv")
OUTPUT_VIDEOS_CSV = Path("video_context_dataset.csv")

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
    """Calcule le ratio de lettres majuscules dans une chaîne."""
    if not isinstance(s, str): return 0.0
    letters = re.findall(r"[A-Za-z]", s)
    if not letters: return 0.0
    return len([ch for ch in letters if ch.isupper()]) / len(letters)

# =========================
# PARSING & FEATURE ENGINEERING
# =========================
def main():
    videos_rows = []
    comments_rows = []
    
    json_files = list(INPUT_DIR.glob("*.json"))
    if not json_files:
        print(f"[ERREUR] Aucun fichier JSON trouvé dans le dossier '{INPUT_DIR}'.")
        return

    print(f"Extraction des données depuis {len(json_files)} fichiers JSON...")

    for json_path in json_files:
        with json_path.open("r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"[ATTENTION] Fichier corrompu ou vide ignoré : {json_path.name}")
                continue

        video_id = data.get("video_id")
        meta = data.get("video_metadata", {}) or {}
        snippet = meta.get("snippet", {}) or {}
        stats = meta.get("statistics", {}) or {}
        content = meta.get("contentDetails", {}) or {}

        # Extraction des métadonnées de la vidéo
        v_title = snippet.get("title")
        v_description = snippet.get("description")
        v_channel = snippet.get("channelTitle")
        v_pub_at = snippet.get("publishedAt")
        v_views = int(stats.get("viewCount", 0)) if stats.get("viewCount") else None
        v_likes = int(stats.get("likeCount", 0)) if stats.get("likeCount") else None
        v_dur_sec = parse_iso8601_duration(content.get("duration"))

        # --- TRAITEMENT ROBUSTE DE LA TRANSCRIPTION ---
        transcript_data = data.get("video_transcript") or data.get("transcript")
        full_transcript_text = ""
        
        if isinstance(transcript_data, list):
            # Si c'est une liste de dictionnaires [{"text": "...", "start": ...}]
            full_transcript_text = " ".join([item.get("text", "") for item in transcript_data if isinstance(item, dict)])
        elif isinstance(transcript_data, str):
            # Si c'est déjà une chaîne brute
            full_transcript_text = transcript_data
        
        # Nettoyage des espaces superflus dans la transcription
        full_transcript_text = re.sub(r'\s+', ' ', full_transcript_text).strip()

        # Sauvegarde unique par vidéo (sans duplication)
        videos_rows.append({
            "video_id": video_id,
            "video_title": v_title,
            "video_description": v_description,
            "video_channel": v_channel,
            "video_published_at": v_pub_at,
            "video_view_count": v_views,
            "video_like_count": v_likes,
            "video_duration_seconds": v_dur_sec,
            "video_transcript": full_transcript_text if full_transcript_text else None
        })

        # --- EXTRACTION DES COMMENTAIRES ET RÉPONSES ---
        for page in data.get("comment_threads_pages", []):
            for item in page.get("items", []):
                thread_id = item.get("id")
                thread_snip = item.get("snippet", {}) or {}
                t_total_replies = thread_snip.get("totalReplyCount", 0)

                # --- 1. Top Level Comment ---
                top = thread_snip.get("topLevelComment", {}) or {}
                top_snip = top.get("snippet", {}) or {}
                text_top = top_snip.get("textOriginal") or top_snip.get("textDisplay", "")
                
                comments_rows.append({
                    "video_id": video_id,  # Clé de jointure
                    "thread_id": thread_id,
                    "thread_total_reply_count": t_total_replies,
                    "comment_id": top.get("id"),
                    "is_reply": 0,
                    "author_hash": anonymize_identity(top_snip.get("authorDisplayName")),
                    "text": text_top,
                    "comment_like_count": top_snip.get("likeCount", 0),
                    "comment_published_at": top_snip.get("publishedAt"),
                    "video_published_at_tmp": v_pub_at  # Conservé temporairement pour le calcul du drift
                })

                # --- 2. Replies ---
                for reply in (item.get("replies", {}) or {}).get("comments", []):
                    rep_snip = reply.get("snippet", {}) or {}
                    text_rep = rep_snip.get("textOriginal") or rep_snip.get("textDisplay", "")
                    
                    comments_rows.append({
                        "video_id": video_id,  # Clé de jointure
                        "thread_id": thread_id,
                        "thread_total_reply_count": t_total_replies,
                        "comment_id": reply.get("id"),
                        "is_reply": 1,
                        "author_hash": anonymize_identity(rep_snip.get("authorDisplayName")),
                        "text": text_rep,
                        "comment_like_count": rep_snip.get("likeCount", 0),
                        "comment_published_at": rep_snip.get("publishedAt"),
                        "video_published_at_tmp": v_pub_at  # Conservé temporairement
                    })

    # =========================================================
    # TRAITEMENT & FEATURING DU DATASET VIDEOS (CONTEXTE)
    # =========================================================
    print(f"\nBuilding videos dataset ({len(videos_rows)} vidéos uniques)...")
    df_videos = pd.DataFrame(videos_rows)
    df_videos["video_published_at"] = pd.to_datetime(df_videos["video_published_at"], errors="coerce")
    
    # Remplacement des valeurs aberrantes/infinies
    df_videos = df_videos.replace([np.inf, -np.inf], np.nan)
    df_videos.to_csv(OUTPUT_VIDEOS_CSV, index=False, encoding="utf-8-sig")
    print(f"[SUCCÈS] Dataset Vidéos/Transcriptions sauvegardé : {OUTPUT_VIDEOS_CSV}")

    # =========================================================
    # TRAITEMENT & FEATURING DU DATASET INTERACTIONS (COMMENTS)
    # =========================================================
    print(f"Building user interactions dataset ({len(comments_rows)} commentaires trouvés)...")
    if not comments_rows:
        print("[ATTENTION] Aucun commentaire extrait des fichiers JSON.")
        return

    df_comments = pd.DataFrame(comments_rows)
    df_comments["text"] = df_comments["text"].fillna("").astype(str)

    # Conversion des formats de date
    df_comments["comment_published_at"] = pd.to_datetime(df_comments["comment_published_at"], errors="coerce")
    df_comments["video_published_at_tmp"] = pd.to_datetime(df_comments["video_published_at_tmp"], errors="coerce")

    # NLP Features basiques
    df_comments["text_len_chars"] = df_comments["text"].str.len()
    df_comments["text_len_words"] = df_comments["text"].str.split().apply(lambda x: len(x) if isinstance(x, list) else 0)
    df_comments["has_question"] = (df_comments["text"].str.count(r"\?") > 0).astype(int)
    df_comments["upper_ratio"] = df_comments["text"].apply(upper_ratio)

    # Features Mots-clés EV (Analyse de thématiques)
    df_comments["kw_price"] = df_comments["text"].str.contains(r"price|expensive|cheap|cost|€|\$", case=False, regex=True).astype(int)
    df_comments["kw_range"] = df_comments["text"].str.contains(r"range|autonomy|km|miles|battery", case=False, regex=True).astype(int)
    df_comments["kw_charging"] = df_comments["text"].str.contains(r"charge|charging|charger", case=False, regex=True).astype(int)

    # Features Temporelles (Calcul du Sentiment Drift)
    df_comments["video_age_days_at_comment"] = ((df_comments["comment_published_at"] - df_comments["video_published_at_tmp"]).dt.total_seconds() / (24 * 3600))
    
    # Suppression de la colonne temporelle temporaire pour parfaire la normalisation
    df_comments = df_comments.drop(columns=["video_published_at_tmp"])

    # Nettoyage final des valeurs infinies
    df_comments = df_comments.replace([np.inf, -np.inf], np.nan)

    # Sauvegarde finale
    df_comments.to_csv(OUTPUT_COMMENTS_CSV, index=False, encoding="utf-8-sig")
    print(f"[SUCCÈS] Dataset Interactions Utilisateurs sauvegardé : {OUTPUT_COMMENTS_CSV}")
    print(f"-> Structure finale : {len(df_comments)} lignes et {len(df_comments.columns)} colonnes.")
    print("-> L'anonymisation SHA-256 (NIST SP 800-88 R1) a été appliquée avec succès aux auteurs.")

if __name__ == "__main__":
    main()