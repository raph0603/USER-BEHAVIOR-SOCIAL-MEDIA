# Rapport — sentiment analysis à l'inférence (posts + commentaires)

Date : 26/06/2026

## 1. Ce qui a été fait

- **Lexique EN étendu** (`ev_lexicon_en.py`) : ~50 termes/expressions spécifiques aux VE notés à la main et fusionnés dans le lexique VADER (`vaderSentiment`), ex. `range_anxiety` (-2.4), `battery_fire` (-3.3), `tax_credit` (+1.9), `eco_friendly` (+2.0).
- **Moteur vietnamien from scratch** (`vi_sentiment_engine.py`) : lexique de polarité (~70 termes), négateurs (không, chẳng, chưa...), intensificateurs (rất, quá, cực kỳ...), compound score normalisé sur la même échelle -1..1 que VADER. Aucune librairie VI existante utilisée — VADER n'a pas d'équivalent vietnamien mûr et gratuit.
- **Pondération par rôle rhétorique** (posts uniquement, `silver_dataset_sentiment.jsonl` / `gold_dataset_sentiment.jsonl`) : le score est atténué quand le rôle déjà annoté indique que la négation est de la réassurance (`objection_handling`, ×0.45) ou de l'urgence commerciale (`urgency`/`scarcity`, ×0.7) ; un flag `intentional_negative_rhetoric` marque les `pain_point` sans toucher au score (négatif voulu, pas une erreur).
- **Sentiment + stance sur les 3 corpus de commentaires** (`output/reddit_sentiment.csv`, `x_sentiment.csv`, `youtube_sentiment.csv`), à partir du texte brut retrouvé dans `dashboard/data/` (la donnée n'était pas perdue, juste dans un autre dossier que les fichiers `Features/`) :

| Plateforme | Lignes traitées | EN | VI |
|---|---|---|---|
| Reddit | 108 645 | 108 645 | 0 |
| X | 16 500 | 16 491 | 8 |
| YouTube | 621 074 | 620 236 | 838 |

(le compte YouTube réel est 621 074, pas 775 250 — `wc -l` comptait aussi les retours à la ligne à l'intérieur des commentaires multi-lignes, vérifié en relisant le CSV avec pandas).

## 2. Ce qui a été bien fait

- **Vérification de cohérence contre l'existant** : les fichiers Reddit/X avaient déjà un sentiment calculé par un script de collègue (VADER nu, non retrouvé dans le repo). Comparaison des distributions :

  | | Positif | Neutre | Négatif |
  |---|---|---|---|
  | Reddit — baseline collègue | 50 672 | 28 746 | 29 227 |
  | Reddit — notre version | 50 919 | 28 356 | 29 370 |
  | X — baseline collègue | 6 801 | 6 966 | 2 732 |
  | X — notre version | 6 846 | 6 907 | 2 746 |

  Quasi-identique : ça confirme que le moteur EN reproduit bien VADER de base, et que l'extension lexicale EV a un effet réel mais mesuré (quelques centaines de lignes basculent de label), pas un effet aléatoire ou cassé.
- **Gestion honnête de la donnée manquante** : exploration complète avant de coder (rapport transmis avant action), pas de supposition silencieuse sur des fichiers manquants.
- **Contrainte "pas de LLM payant" respectée à 100 %** : tout est lexique + règles, exécuté localement.
- **Reprise sur erreur** : le traitement YouTube (775 k lignes apparentes) a été fait par lots avec checkpoint, pas de perte de données malgré les coupures du sandbox entre chaque appel.

## 3. Ce qui a été mal fait / limites honnêtes

- **Lexique vietnamien petit et non validé statistiquement** : ~70 termes vs ~7 500 pour VADER EN. Sur les commentaires YouTube en vietnamien, une bonne partie ressort en `neutral` faute de couverture lexicale (vu sur l'échantillon : plusieurs phrases clairement non-neutres scorées 0.0). C'est un vrai angle mort, pas juste une marge d'erreur.
- **Pas de tokenizer vietnamien dédié** (type VnCoreNLP) : segmentation par espaces simples, donc une partie des expressions composées du lexique peut être mal détectée si l'ordre des mots varie légèrement.
- **Détection de langue par regex de diacritiques** : fonctionne bien pour distinguer EN/VI net, mais un commentaire vietnamien sans diacritiques (orthographe informelle, "khong" au lieu de "không") sera mal classé en EN. Pas mesuré précisément combien de cas ça concerne.
- **La pondération par rôle n'est calibrée qu'avec 2 règles simples** (multiplicateurs choisis à l'intuition, pas appris sur données). C'est défendable comme première itération mais pas validé empiriquement (pas de vérité terrain "sentiment réel" pour comparer).
- **Stance basée sur des marqueurs lexicaux explicites + repli sur le sentiment général** : peu de marqueurs (~10-15 par langue/catégorie). Beaucoup de commentaires sans marqueur explicite tombent dans le repli "stance = sentiment", ce qui revient à ne pas vraiment distinguer les deux dans ces cas — c'est la plus grosse simplification du système.
- **Pas de test sur un échantillon annoté manuellement** : la validation faite ici est une comparaison à un autre VADER nu (Reddit/X) et une lecture d'échantillon à l'œil (quelques lignes), pas une mesure de précision/rappel sur un gold sentiment.

## 4. Améliorable, et comment

- **Lexique VI** : l'élargir significativement (200-300 termes) en parcourant un échantillon réel de commentaires VI non couverts, plutôt que de partir de l'intuition seule.
- **Stance** : remplacer le repli par une vraie troisième dimension — par exemple un petit classifieur de règles dédié (pas juste des mots-clés) qui regarde la structure de la phrase (question rhétorique, citation du message d'origine, etc.), sur le modèle du dispositif de vote à 3 angles déjà utilisé pour les rôles rhétoriques.
- **Validation empirique** : prendre un échantillon de 100-150 commentaires (mix EN/VI, mix plateformes), les faire annoter par moi-même (Claude, lecture directe) en aveugle du score heuristique, puis comparer — donnerait une vraie mesure d'accord plutôt qu'une comparaison indirecte.
- **Pondération par rôle** : si le temps le permet, calibrer les multiplicateurs sur les cas où confidence du rôle est élevée (≥0.85) pour vérifier que l'ajustement va dans le bon sens sur un échantillon contrôlé.
- **Détection de langue** : ajouter une liste de mots-outils vietnamiens fréquents sans diacritiques (la, va, khong, rat...) en complément des diacritiques, pour réduire les faux EN.

## 5. Fichiers livrés

- `annotation_marketing/sentiment/ev_lexicon_en.py`
- `annotation_marketing/sentiment/vi_sentiment_engine.py`
- `annotation_marketing/sentiment/sentiment_engine.py`
- `annotation_marketing/sentiment/apply_sentiment_posts.py`
- `annotation_marketing/sentiment/apply_sentiment_comments.py`
- `annotation_marketing/silver_dataset_sentiment.jsonl` (1 266 lignes)
- `annotation_marketing/gold_dataset_sentiment.jsonl` (5 610 lignes)
- `annotation_marketing/sentiment/output/reddit_sentiment.csv` (108 645 lignes)
- `annotation_marketing/sentiment/output/x_sentiment.csv` (16 499 lignes)
- `annotation_marketing/sentiment/output/youtube_sentiment.csv` (621 074 lignes)
- ce rapport (`rapport_sentiment.md`)

Tous les CSV de commentaires sont joinables aux fichiers `Features/*_comment_level.csv` / `*_tweet_level.csv` existants par `comment_id` / `status_id`.
