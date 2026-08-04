# Exemple scientifique de transformation X

Ce workflow sélectionne un événement X réel conservé dans `x.raw.events`, le
classe avec les compteurs de transformations demandés, puis vérifie son parcours
complet :

```text
RAW → Clean → Bronze → Silver → Gold
```

Il distingue systématiquement `real_existing`, `real_new_collection` et
`controlled_fixture`. Une fixture ne peut pas obtenir le statut d’exemple réel.

## Fichiers produits

Le résultat est écrit dans :

```text
artifacts/x-transformation-example/<platform_event_id>/
```

- `raw.json` : événement Kafka RAW réel avant confidentialité ;
- `clean.json` : comparaison avant/après la passerelle de confidentialité ;
- `bronze.json`, `silver.json`, `gold.json` : lignes Iceberg associées ;
- `comparison.json` : comparaison structurée et transformations observées ;
- `example-table.tex` : tableau IEEE double colonne construit depuis les JSON ;
- `selection-report.md` : population inspectée, scores, choix et limites ;
- `manifest.json` : identité, snapshots, cardinalités, validations et SHA-256.

Les données réelles sont ignorées par Git. Elles peuvent contenir des
identifiants publics ou du texte sensible et doivent conserver une durée de
rétention courte.

## Outils

- `spark/jobs/maintenance/inspect_x_transformation_candidates.py` classe les
  RAW réels avec le score documenté ;
- `spark/jobs/maintenance/replay_x_raw_candidate.py` extrait et rejoue
  exactement un candidat existant dans la passerelle Clean ;
- `spark/jobs/maintenance/replay_x_lineage_event.py` matérialise le même
  événement dans Bronze et Silver de façon idempotente ;
- `scripts/finalize_x_transformation_example.py` génère les livrables
  scientifiques et refuse un manifeste `PASS` si les validations obligatoires
  échouent.

Le rapport signale explicitement les types de données absents. Une chaîne
ressemblant à une URL mais interrompue par des espaces n’est pas présentée comme
une URL effectivement anonymisée.
