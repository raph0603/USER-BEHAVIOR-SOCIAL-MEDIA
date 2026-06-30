# annotation_marketing — README

Annotation des rôles rhétoriques marketing (EV) + sentiment, sur posts et
commentaires Reddit/X/YouTube (EN+VI). Lire ce fichier avant d'utiliser les
datasets — il résume les points qui ont une incidence directe sur l'usage.

## ⚠️ À savoir avant d'utiliser les données

1. **Aucun appel LLM payant, aucun humain n'a annoté ce corpus.** Le doc de
   cahier des charges (`instructions_annotation_roles_marketing.md`) prévoit
   un appel LLM avec triple annotation à températures différentes (section
   9). Cette contrainte de l'utilisateur ("pas de LLM payant") a remplacé ce
   processus par des heuristiques à base de règles/lexique (regex multilingue
   FR/EN/VI), exécutées par Claude directement, avec un dispositif de vote à
   3 angles indépendants pour simuler la corroboration inter-annotateur
   (détail complet : `rapport_qualite_annotation.md`, section 1).
   **Conséquence : il n'existe pas de fichier "appel LLM" dans ce dossier —
   c'est voulu, pas un oubli.**

2. **50.4 % des segments sont `uncertain`** (3 468 / 6 876, désormais isolés
   dans `uncertain_dataset.jsonl`). Ce taux élevé reflète la nature réelle du
   corpus (descriptions YouTube automatiques, timestamps, mentions légales,
   fragments hors-sujet), pas une sous-performance de l'annotateur — voir
   `rapport_qualite_annotation.md` section 4. **À garder en tête si vous
   filtrez/échantillonnez sur ce corpus : la moitié des lignes n'a pas de
   rôle marketing assigné.**

3. **Convention silver/gold (mise à jour) :**
   - `gold_dataset.jsonl` (286 lignes) = sous-ensemble à confiance maximale
     (≥0.95, accord 3/3 des votes indépendants) — le plus proche d'un set
     "vérifié" qu'on puisse produire sans annotateur humain. À utiliser comme
     référence/évaluation, pas comme set d'entraînement principal.
   - `silver_dataset.jsonl` (3 122 lignes) = reste des segments à rôle réel
     (confidence <0.95, ≠ `uncertain`) — set principal pour l'entraînement.
   - `uncertain_dataset.jsonl` (3 468 lignes) = segments sans rôle marketing
     assignable avec confiance suffisante. Conservé séparément pour ne pas
     perdre de données, mais à exclure de tout entraînement de classifieur de
     rôle.

## Fichiers

- `silver_dataset.jsonl`, `gold_dataset.jsonl`, `uncertain_dataset.jsonl` — splits ci-dessus.
- `*_sentiment.jsonl` — mêmes splits + sentiment (label/score) et pondération par rôle rhétorique.
- `sentiment/` — moteur de sentiment (lexique EV étendu EN, moteur de règles VI, pondération par rôle, stance commentaires) + scripts d'application sur les 3 corpus de commentaires.
- `rapport_preparation_dataset.md` — préparation amont (scraping, segmentation, échantillonnage).
- `rapport_qualite_annotation.md` — méthodologie d'annotation des rôles, écarts vs cahier des charges, statistiques détaillées.
- `rapport_sentiment.md` — méthodologie sentiment, auto-évaluation honnête (bien fait / mal fait / améliorable).
- `restructure_pipeline.py` — script de correction des tags langue + reconstruction silver/gold/uncertain (cette passe-ci).

## Limite connue sur la vérification du tag langue

46 segments tagués `language: "en"` contenaient en réalité du texte
vietnamien (diacritiques détectés) — corrigés dans cette passe. Le moteur de
vote de rôle ne lit jamais le champ `language` en entrée, donc ce bug ne peut
pas avoir biaisé l'attribution du rôle pour ces lignes — **mais** la
recompute de vérification effectuée donne un résultat différent du rôle
original sur 22 de ces 46 lignes, ce qui indique que le script de vote
disponible ici ne reproduit pas exactement la logique utilisée pour produire
les fichiers originaux (probablement une version légèrement différente/
antérieure). Par précaution, **seul le tag `language` a été corrigé ; le
`primary_role` original a été conservé tel quel**, pour ne pas introduire un
changement non vérifié. À garder en tête si une ré-annotation complète est
envisagée plus tard.
