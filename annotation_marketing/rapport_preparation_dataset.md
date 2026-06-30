# Préparation du dataset d'annotation des rôles marketing — rapport

Date : 24/06/2026

## Objectif

Préparer un dataset équilibré (par réseau et par langue) de posts originaux issus de YouTube, X et Reddit, segmentés en unités courtes prêtes pour l'annotation des rôles rhétoriques marketing (hook, pain_point, solution, benefit, proof, social_proof, urgency, scarcity, objection_handling, cta, educational, storytelling, uncertain). L'annotation LLM elle-même (silver/gold dataset) est reportée à une phase ultérieure ; cette livraison couvre la sélection équilibrée et la segmentation.

## Pipeline

1. **Scraping Reddit manquant** (`reddit_post_scraper.py`) : le texte des posts originaux Reddit n'avait jamais été scrappé (seulement les commentaires). Script Playwright connecté en CDP à un Chrome déjà ouvert/authentifié (contournement du blocage anti-bot Akamai), avec gestion du rate-limit 429 (file d'attente + pause croissante) et reprise automatique. Résultat : 732 posts EN, 211 posts VI récupérés.
2. **Extraction commune** (`extract_posts.py`) : normalisation des 3 réseaux vers `{post_id, platform, language, text}`.
3. **Filtre qualité et équilibrage** (`build_balanced_sample.py`) : texte entre 20 et 3000 caractères (les posts > 3000 caractères, essentiellement de longs essais Reddit vietnamiens hors-sujet marketing, ont été exclus pour ne pas fausser la segmentation). Pour chaque langue, le réseau le moins fourni fixe le quota, les deux autres sont sous-échantillonnés au même nombre (seed fixe = reproductible).
4. **Segmentation** (`segment_posts.py`) : découpage en phrases + split supplémentaire sur conjonctions de coordination (et/and/mais/but) quand une phrase mélange deux fonctions.

## Résultats — posts retenus (équilibrés)

| Réseau | Langue | Posts retenus |
|---|---|---|
| Reddit | EN | 418 |
| X | EN | 418 |
| YouTube | EN | 418 |
| Reddit | VI | 135 |
| X | VI | 135 |
| YouTube | VI | 135 |

**Total : 1659 posts** (équilibre parfait par réseau au sein de chaque langue ; le goulot d'étranglement est Reddit VI à 135 posts disponibles après filtre qualité).

## Résultats — segments produits

| Réseau | Langue | Segments | Moy. segments/post |
|---|---|---|---|
| Reddit | EN | 1577 | 3.8 |
| X | EN | 1408 | 3.4 |
| YouTube | EN | 7361 | 17.6 |
| Reddit | VI | 1319 | 9.8 |
| X | VI | 901 | 6.7 |
| YouTube | VI | 1940 | 14.4 |

**Total : 14 506 segments.** Longueur des segments : médiane 48 caractères, moyenne 61.7, max 1754.

## Point d'attention pour la suite

L'équilibrage a été fait au niveau **post**, comme demandé, pas au niveau **segment**. YouTube produit nettement plus de segments par post (descriptions plus longues) que X ou Reddit — YouTube EN représente à lui seul ~51 % des segments anglais. Si l'annotation finale doit aussi être équilibrée au niveau segment (et non juste au niveau post), il faudra un sous-échantillonnage supplémentaire après l'annotation LLM, ou un plafond de segments par post avant le passage en annotation.

## Fichiers livrés (`Codes/annotation_marketing/`)

- `reddit_post_scraper.py` — scraper Reddit (titre + selftext), via CDP
- `extract_posts.py` — extraction commune des 3 réseaux
- `build_balanced_sample.py` — filtre qualité + équilibrage réseau × langue
- `segment_posts.py` — segmentation mécanique en unités courtes
- `prompt_annotation.txt` — prompt LLM prêt à l'emploi pour la phase d'annotation
- `all_posts_raw.jsonl` — 2798 posts extraits avant équilibrage (toutes langues/réseaux)
- `posts_originaux_selection.jsonl` — 1659 posts équilibrés sélectionnés
- `segments_a_annoter.jsonl` — 14 506 segments prêts pour l'annotation LLM
- `rapport_preparation_dataset.md` — ce rapport

## Prochaine étape (non faite dans cette phase)

Lancer l'annotation LLM sur `segments_a_annoter.jsonl` avec `prompt_annotation.txt`, filtrer par seuil de confiance (≥ 0.75 = silver auto, < 0.75 ou uncertain = revue humaine), puis produire `silver_dataset.jsonl` et `gold_dataset.jsonl`.
