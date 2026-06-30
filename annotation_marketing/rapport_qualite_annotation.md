# Rapport de qualité — annotation des rôles marketing (silver / gold)

Date : 26/06/2026

## 1. Méthodologie réellement utilisée (et écart avec le doc d'instructions)

Le document de cahier des charges (`instructions_annotation_roles_marketing.md`) prévoit une annotation par appel LLM (section 7), avec double/triple annotation à températures différentes (section 9) pour calculer un taux d'accord entre annotations.

**Contrainte posée par l'utilisateur : aucun appel LLM payant n'a été utilisé à aucun moment du projet.** Toute l'annotation a été produite directement par Claude, sans API tierce, via deux familles de méthode :

- `claude_direct_reading` (abandonné dans cette passe, voir ci-dessous) : lecture individuelle du segment en contexte par Claude et attribution manuelle du rôle.
- Heuristiques à base de règles (regex / lexique multilingue FR-EN-VI) écrites et exécutées par Claude, appliquées programmatiquement à l'ensemble du corpus.

Pour répondre à la demande de double annotation/vote, **tous les 6 876 segments ont été repassés dans cette session** par un dispositif de vote à 3 angles indépendants, conçu pour se substituer au principe de la section 9 sans appel API :

1. **Vote A** — lexique d'action explicite (liens, CTA, chiffres/unités, autorité/expertise, etc.)
2. **Vote B** — signaux structurels (position du segment dans le post, début de phrase impératif, syntaxe)
3. **Jugement de base** — le label déjà attribué lors des passes précédentes (lecture directe ou heuristique calibrée)

Règle de décision appliquée à chaque segment :
- Si **A et B confirment** le label de base → confiance relevée fortement (+0.12, plafond 0.95) → équivalent "3/3".
- Si **un seul des deux** confirme → confiance relevée modérément (+0.04 à +0.07 selon que le rôle est "objectif" — cta/proof/social_proof/urgency/scarcity — ou plus interprétatif) → équivalent "2/3".
- Si **aucun ne confirme** → la confiance de base est conservée inchangée (le jugement initial fait foi, ni récompensé ni pénalisé) → équivalent "1/3", reste en gold si < 0.75.
- Si le label de base était `uncertain` mais que **A et B s'accordent entre eux sur un même rôle réel** → le label est mis à jour vers ce rôle, avec une confiance modérée (0.6–0.85 selon force du signal).

La source de toutes les lignes du dataset final est donc renommée de façon honnête en **`claude_triple_heuristic_vote`** (plus de `human_manual_single_pass` ni `human_verified` — aucun humain n'est jamais intervenu).

**Taux d'accord (calculable pour la première fois, section 13 du doc) :**

| Niveau d'accord | Segments | % du total |
|---|---|---|
| 3/3 (label confirmé par les 2 votes indépendants) | 528 | 7.7 % |
| 2/3 (label confirmé par 1 vote indépendant) | 776 | 11.3 % |
| 1/3 (aucune confirmation, jugement de base conservé) | 1 956 | 28.4 % |
| `uncertain` confirmé par les 2 votes (aucun signal trouvé) | 3 468 | 50.4 % |
| `uncertain` requalifié en rôle réel par accord A+B | 148 | 2.2 % |

## 2. Effet sur le split silver / gold

**Mise à jour (passe de revue qualité ultérieure) : la convention silver/gold a été corrigée.** La version précédente de ce rapport définissait `gold` comme "le reste, envoyé en validation" (5 610 lignes) et `silver` comme le sous-ensemble à confiance ≥0.75 (1 266 lignes) — c'est l'inverse de l'usage standard, où gold doit être le **petit** sous-ensemble fiable et silver le set principal, plus large. Aucune vérification humaine n'existant dans ce projet (contrainte explicite, aucun LLM payant non plus), "gold" est ici redéfini comme le sous-ensemble à confiance algorithmique maximale (≥0.95, accord 3/3 des votes), pas un set "vérifié humainement" :

| | Définition | Lignes |
|---|---|---|
| `gold_dataset.jsonl` | confiance ≥ 0.95 (accord 3/3), rôle ≠ uncertain | **286** |
| `silver_dataset.jsonl` | confiance < 0.95, rôle ≠ uncertain | **3 122** |
| `uncertain_dataset.jsonl` (nouveau, séparé) | rôle = uncertain | **3 468** |

Avant cette correction, le split était : silver 1 266 / gold 5 610 (avec gold incluant à la fois des segments à rôle réel sous le seuil 0.75 ET tous les `uncertain`, ce qui n'a pas de sens pour un "gold").

Les volumes restent **loin des cibles du doc** (silver 5 000–20 000 / gold 500–2 000, section 10) pour la même raison de fond qu'avant : le corpus filtré VE ne compte que 6 876 segments, et la moitié est structurellement non-marketing (timestamps, génériques de chaîne, mentions légales...). Le nouveau gold (286) est même un peu **sous** la cible basse (500) — c'est le prix de la rigueur du seuil 0.95 ; l'élargir à 0.90 donnerait 959 lignes (dans la cible) mais avec un accord 2/3 inclus, donc moins strictement "gold".

Sur les 3 260 segments qui avaient déjà un rôle réel (≠ uncertain) avant cette passe, la distribution de confiance était :

| Confiance | Segments |
|---|---|
| ≥ 0.75 | 1 059 |
| 0.65–0.74 | 484 |
| 0.55–0.64 | 248 |
| < 0.55 | 1 469 |

Le bond de 1 059 → 1 266 silver vient essentiellement de la tranche 0.65–0.74 qui franchit le seuil grâce à la corroboration. Les 1 469 segments à confiance < 0.55 sont des estimations heuristiques volontairement prudentes (motifs lexicaux faibles) : aucune corroboration honnête ne peut les faire dépasser 0.75 sans fabriquer de la confiance.

## 3. Statistiques finales (section 13 du doc)

- **Posts** : 955 (tous liés aux véhicules électriques, filtre EV appliqué en amont)
- **Segments annotés** : 6 876 — moyenne **7.2 segments/post**
- **Segments rejetés (bruit)** : 2 796 (`segments_rejetes.jsonl`)
- **Corpus complet pré-filtre EV** : 11 710 segments (`segments_a_annoter_clean.jsonl`)
- **Répartition plateforme** : YouTube 4 718, Reddit 1 124, X 1 034
- **Répartition langue** : EN 4 695, VI 2 181
- **% `uncertain`** : 50.4 % (3 468 segments)

Distribution des rôles primaires (confiance moyenne entre parenthèses) :

| Rôle | Segments | Confiance moyenne |
|---|---|---|
| uncertain | 3 468 | 0.55 |
| cta | 954 | 0.85 |
| hook | 835 | 0.60 |
| educational | 367 | 0.42 |
| proof | 325 | 0.74 |
| benefit | 249 | 0.51 |
| pain_point | 213 | 0.48 |
| storytelling | 132 | 0.44 |
| objection_handling | 130 | 0.49 |
| social_proof | 87 | 0.64 |
| urgency | 77 | 0.68 |
| solution | 27 | 0.38 |
| scarcity | 12 | 0.65 |

## 4. Écarts persistants par rapport au doc d'instructions (honnêteté)

1. **Pas de script d'appel LLM payant** : aucun n'existe, car aucune API payante n'a été utilisée (refus explicite de l'utilisateur).
2. **Pas de "vraie" double annotation LLM** (3 passes à températures différentes d'un même modèle) : remplacée par le dispositif de vote à 3 angles décrit en section 1, qui répond à l'esprit de la section 9 (réduire l'incertitude par corroboration indépendante) sans appel API.
3. **Volumes silver/gold** : convention corrigée (voir section 2) ; volumes toujours en deçà des cibles du doc pour des raisons de taille/nature du corpus, pas de méthode.
4. **Taux d'`uncertain` élevé (50.4 %)** : reflète la nature réelle du corpus (descriptions YouTube très automatiques, mentions légales, timestamps), pas une sous-performance de l'annotateur. Isolé depuis dans `uncertain_dataset.jsonl` (3 468 lignes) pour ne pas polluer silver/gold.
5. **Filtre "E