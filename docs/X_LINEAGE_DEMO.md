# Démonstration de lineage d’un événement X

Cette démonstration collecte exactement un nouveau post X réel et produit un
dossier vérifiable retraçant son parcours complet :

```text
collecteur X → RAW → confidentialité → Bronze → Silver → Gold → JSON
```

Elle n’utilise jamais le fixture de test comme résultat final et ne reconstruit
jamais un RAW à partir de Bronze. Si le collecteur authentifié ne retourne
aucun nouvel événement, la commande échoue sans créer de faux exemple.

## Définition du RAW

Le RAW est la projection canonique Avro construite par le collecteur juste
avant sa sérialisation et sa publication dans `x.raw.events`. La capture est
opt-in et intervient avant toute règle de confidentialité. Elle conserve donc
le texte observé, l’identifiant natif, l’URL, le compte X, les compteurs et les
timestamps réellement exposés au collecteur.

Bronze ne peut pas servir à reconstruire ce RAW : avant son écriture, la
passerelle de confidentialité remplace les PII, hache les identifiants et
assainit les payloads JSON. Cette transformation est volontairement
irréversible.

`raw.json` est sensible. Il peut contenir des mentions, adresses e-mail,
numéros de téléphone, adresses IP, URL incorporées et noms de comptes. Le
dossier `artifacts/x-lineage/` est ignoré par Git, créé avec des permissions
restreintes lorsque le système les prend en charge, et ne doit être utilisé ni
par le dashboard ni pour l’entraînement. Appliquez une rétention courte et
supprimez manuellement le dossier lorsqu’il n’est plus nécessaire.

## Politique de confidentialité

La passerelle commune remplace les valeurs suivantes avant Bronze :

| Valeur observée | Valeur protégée |
|---|---|
| mention X | `<USER>` |
| adresse e-mail | `<EMAIL>` |
| téléphone | `<PHONE>` |
| adresse IP | `<IP>` |
| URL dans le texte ou un payload JSON | `<URL>` |

Le compte X est remplacé par un SHA-256 salé. L’URL canonique Bronze devient
`https://x.com/i/status/<platform_event_id>` afin de ne pas exposer le nom du
compte dans son chemin. Les hashtags, emojis, nombres ordinaires et noms
techniques restent inchangés.

Les trois champs textuels ont des responsabilités distinctes :

- `raw_text` dans `raw.json` est le texte original observé par le collecteur ;
- `raw_text` et `clean_text` après la passerelle contiennent le texte protégé
  avec les tokens stables ;
- `text_for_model` normalise la casse du langage naturel tout en restaurant
  exactement `<USER>`, `<EMAIL>`, `<PHONE>`, `<IP>` et `<URL>`.

Le nom historique `raw_text` est conservé dans le contrat Bronze pour
compatibilité, mais sa valeur Bronze est déjà protégée. Seul `raw.json`
contient le texte réellement antérieur au nettoyage.

## Transformations par couche

Bronze écrit d’abord l’événement nettoyé dans le journal immuable
`lakehouse.bronze.event_log`, puis fusionne sa projection courante dans
`lakehouse.bronze.events`. La projection aligne les chaînes ISO entrantes avec
les colonnes temporelles d’une éventuelle ancienne table Iceberg, sans modifier
les types du journal.

Silver applique l’événement de manière idempotente dans
`lakehouse.silver.events` et conserve la preuve dans `applied_events`. Le post
X est matérialisé comme contenu racine dans `contents` : profondeur `0`, parent
`null`, `root_content_id = content_id` et
`conversation_id = platform_event_id`. Aucune interaction artificielle n’est
créée. `post_features` calcule les longueurs, compteurs de tokens, ponctuation,
ratios, diversité lexicale et métriques réellement observées. Une métrique
inconnue reste `null`.

Gold recalcule `content_stats` à partir des contenus, interactions et
observations Silver, puis `user_evolution` à partir de l’auteur anonymisé et de
la date. Gold peut contenir moins de lignes que Silver : une prédiction ou un
exemple d’entraînement n’est exporté que si un traitement réel l’a produit, et
une observation d’engagement n’existe que lorsqu’une métrique source a
réellement été observée.

## Exécution sous WSL

Préparez `.env` avec une authentification X valide (`X_AUTH_TOKEN` et
éventuellement `X_CT0`, ou un profil navigateur déjà authentifié), puis lancez
depuis le chemin Linux du dépôt :

```bash
./scripts/export_x_lineage.sh \
  --max-events 1 \
  --output artifacts/x-lineage \
  --timeout 900
```

La commande :

1. construit et démarre Kafka, Schema Registry, MinIO et Spark ;
2. relève les offsets de départ afin de ne lire que le nouvel événement ;
3. lance le collecteur avec une limite stricte de `1` et capture le RAW ;
4. exécute les jobs de confidentialité, Bronze, Silver, features et Gold avec
   un identifiant `x_lineage_<timestamp>_<random>` et des checkpoints dédiés ;
5. exporte et valide toutes les lignes liées ;
6. calcule les SHA-256 et retourne un code non nul si une couche obligatoire
   manque.

Le script ne réinitialise aucune table, aucun topic et aucun checkpoint
régulier. Une nouvelle collecte peut toutefois retourner zéro ligne lorsque le
profil n’est plus authentifié, qu’un challenge X bloque le navigateur, que la
recherche ne contient aucun post inédit ou que X limite les requêtes. Ces cas
échouent explicitement.

La rétention Kafka limite la possibilité de rejouer un ancien événement, mais
n’affecte pas la définition du RAW : un ancien message Kafka n’est de toute
façon pas substitué à la capture directe demandée. La démonstration collecte un
nouvel événement si aucun RAW direct n’existe.

## Lecture du dossier exporté

Le résultat se trouve dans :

```text
artifacts/x-lineage/<platform_event_id>/
```

- `raw.json` contient l’unique événement exact avant confidentialité ;
- `clean.json` montre les valeurs avant/après et les nombres de remplacements ;
- `bronze.json`, `silver.json` et `gold.json` regroupent les lignes par table ;
- `lineage.json` décrit les transformations de chaque étape ;
- `manifest.json` contient les identifiants, runs, topics, coordonnées Kafka,
  snapshots Iceberg, checkpoints, décomptes, avertissements et erreurs ;
- `manifest.sha256` authentifie le manifeste, dont la section `sha256`
  authentifie les autres exports et journaux ;
- `logs/` conserve la sortie de chaque étape.

`status: "PASS"` signifie que toutes les tables obligatoires contiennent
exactement la ligne attendue et que les validations de confidentialité et de
racine Silver ont réussi. Les tables facultatives absentes apparaissent dans
`warnings`. Toute table obligatoire absente ou toute PII détectée après RAW
apparaît dans `errors` et force `status: "FAIL"`.
