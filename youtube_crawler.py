from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm
import json
import os
import time
from pathlib import Path
import sys
import random

# AJOUT : Importation de la bibliothèque tierce pour les sous-titres
from youtube_transcript_api import YouTubeTranscriptApi

# =========================
# CONFIGURATION
# =========================
API_KEY = os.getenv("YOUTUBE_API_KEY")
TRANSCRIPT_LANGUAGES = [
    language.strip()
    for language in os.getenv("YOUTUBE_TRANSCRIPT_LANGUAGES", "en,vi").split(",")
    if language.strip()
]
INPUT_FILE = "video_list.txt"
OUTPUT_DIR = Path("yt_raw_json")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_youtube_service():
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is required")
    return build("youtube", "v3", developerKey=API_KEY)

def load_video_ids(filepath):
    """Charge les IDs de vidéos depuis le fichier texte."""
    if not Path(filepath).exists():
        print(f"[ERREUR] Le fichier {filepath} n'existe pas.")
        sys.exit(1)
    with open(filepath, "r", encoding="utf-8") as f:
        # Nettoie les sauts de ligne et ignore les lignes vides
        return [line.strip() for line in f if line.strip()]

def fetch_video_metadata(youtube, video_id):
    """Récupère les métadonnées enrichies de la vidéo."""
    try:
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails,status,topicDetails,recordingDetails",
            id=video_id,
        )
        response = request.execute()
        items = response.get("items", [])
        return items[0] if items else None
    except HttpError as e:
        print(f"\n[ERREUR HTTP] Métadonnées pour {video_id} : {e}")
        return None

# AJOUT : Nouvelle fonction pour récupérer la transcription textuelle
def fetch_video_transcript(video_id):
    """
    Récupère la transcription de la vidéo de manière robuste.
    Gere le repli sur l'anglais/vietnamien et convertit l'objet en dictionnaire JSON.
    """
    try:
        # CORRECTION : On enlève l'argument cookies_from_browser qui causait le TypeError
        api = YouTubeTranscriptApi()
        transcript_obj = api.fetch(video_id, languages=TRANSCRIPT_LANGUAGES)
        
        if transcript_obj is None:
            return None
            
        # Sécurité si l'objet renvoyé n'est pas directement listable
        if not hasattr(transcript_obj, '__iter__'):
            for attr in ['lines', 'entries', '_transcript', 'data']:
                if hasattr(transcript_obj, attr):
                    iterable = getattr(transcript_obj, attr)
                    if hasattr(iterable, '__iter__'):
                        transcript_obj = iterable
                        break
        
        clean_transcript = []
        for item in transcript_obj:
            if isinstance(item, dict):
                clean_transcript.append(item)
            else:
                # Extraction des attributs pour l'objet FetchedTranscript
                clean_transcript.append({
                    "text": getattr(item, "text", str(item)),
                    "start": getattr(item, "start", 0.0),
                    "duration": getattr(item, "duration", 0.0)
                })
        
        return clean_transcript if clean_transcript else None

    except Exception as e:
        error_type = type(e).__name__
        print(f"\n[DEBUG TRANSCRIPT] Impossible de récupérer pour {video_id} | Erreur : {error_type}")
        return None

def fetch_all_comments_for_video(youtube, video_id, sleep_seconds=0.5):
    """Récupère tous les commentaires. Gère les commentaires désactivés et le quota."""
    all_pages = []
    next_page_token = None

    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText",
            )
            response = request.execute()
            all_pages.append(response)
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
                
            time.sleep(sleep_seconds)

        except HttpError as e:
            # Vérifier la raison de l'erreur
            error_reason = e.error_details[0]['reason'] if e.error_details else "unknown"
            
            if error_reason == "commentsDisabled":
                print(f"\n[INFO] Commentaires désactivés pour la vidéo {video_id}.")
            elif error_reason == "quotaExceeded":
                print("\n[STOP] QUOTA API ATTEINT POUR AUJOURD'HUI. Arrêt du script.")
                sys.exit(0) # Arrête tout le programme
            else:
                print(f"\n[ERREUR] Impossible de récupérer les commentaires ({video_id}) : {error_reason}")
            
            break # On sort de la boucle de cette vidéo mais on passe à la suivante

    return all_pages

def save_json(data, path: Path):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    video_ids = load_video_ids(INPUT_FILE)
    print(f"Chargement de {len(video_ids)} vidéos à traiter.")
    
    youtube = get_youtube_service()

    for video_id in tqdm(video_ids, desc="Collecte en cours"):
        out_file = OUTPUT_DIR / f"{video_id}.json"
        
        # CHECKPOINTING : Si le fichier existe déjà, on le passe
        if out_file.exists():
            continue
            
        # 1) Métadonnées
        metadata = fetch_video_metadata(youtube, video_id)
        if metadata is None:
            continue
            
        # MODIFICATION 1.5) Transcription
        transcript = fetch_video_transcript(video_id)
            
        # 2) Commentaires
        comment_pages = fetch_all_comments_for_video(youtube, video_id)
        
        # 3) Sauvegarde enrichie
        payload = {
            "video_id": video_id,
            "video_metadata": metadata,
            "video_transcript": transcript,  # AJOUT : Stockage de la transcription dans le JSON
            "comment_threads_pages": comment_pages,
        }
        time.sleep(random.uniform(3.0, 6.0))
        save_json(payload, out_file)

if __name__ == "__main__":
    main()
