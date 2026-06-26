from googleapiclient.discovery import build
import os
import time

# =========================
# CONFIGURATION
# =========================
API_KEY = os.getenv("YOUTUBE_API_KEY")
SEARCH_LANGUAGES = [
    language.strip()
    for language in os.getenv("YOUTUBE_SEARCH_LANGUAGES", "en,vi").split(",")
    if language.strip()
]
OUTPUT_FILE = "video_list.txt"

# Liste de mots-clés pour trouver des vidéos pertinentes
SEARCH_QUERIES = [
    "electric vehicle commercial",
    "EV super bowl ad",
    "electric car review 2024",
    "Tesla Model 3 Highland review",
    "Hyundai Ioniq 5 review",
    "Kia EV9 test drive",
    "BYD Seal review",
    "Rivian R1T review",
    "electric car pros and cons",
    "why I sold my electric car" # Très bon pour le sentiment négatif/drift,
    "Tesla",
    "EV review",
    "electric vehicle good idea ?",
    "electric vehicle problem",
    "đánh giá xe điện",
    "xe điện VinFast",
    "pin xe điện",
    "trạm sạc xe điện",
]

def get_youtube_service():
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is required")
    return build("youtube", "v3", developerKey=API_KEY)

def search_videos(youtube, query, language, max_results=50):
    """Recherche des vidéos par mot-clé et retourne une liste d'IDs."""
    print(f"Recherche pour : '{query}' ({language})...")
    try:
        request = youtube.search().list(
            part="id",
            q=query,
            type="video",
            relevanceLanguage=language,
            maxResults=max_results
        )
        response = request.execute()
        
        # Extraction des IDs
        video_ids = [item["id"]["videoId"] for item in response.get("items", [])]
        return video_ids
    except Exception as e:
        print(f"[ERREUR] Échec de la recherche pour '{query}': {e}")
        return []

def main():
    youtube = get_youtube_service()
    all_video_ids = set() # Le "set" permet d'éviter automatiquement les doublons

    for query in SEARCH_QUERIES:
        for language in SEARCH_LANGUAGES:
            ids = search_videos(youtube, query, language, max_results=50)
            all_video_ids.update(ids)
            time.sleep(1) # Petite pause pour l'API

    # Sauvegarde dans le fichier texte
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for vid in all_video_ids:
            f.write(f"{vid}\n")

    print(f"\n[SUCCÈS] {len(all_video_ids)} IDs de vidéos uniques ont été sauvegardés dans {OUTPUT_FILE}.")

if __name__ == "__main__":
    main()
