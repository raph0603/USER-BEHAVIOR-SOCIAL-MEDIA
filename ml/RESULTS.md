# Stage-1 results — 2026-08-13

The authoritative reproducible figures are in section 0. They were generated directly
from pinned Apache Iceberg snapshots and are linked to the exact dataset and model
artifacts. Sections 1–5 retain the larger July manual-export experiment for historical
comparison only; those figures must not be cited as reproducible lakehouse results.

Regenerate the whole file's contents with:

```powershell
$env:PYTHONIOENCODING='utf-8'
& ".\ml\.venv\Scripts\python.exe" ml/run_pipeline.py `
    --lakehouse-manifest <manifest.json> --report
```

---

## 0. Official pinned-snapshot run

- Dataset version: `dataset-v2-dd7ce6e598d3ebae6cd7`
- Dataset fingerprint: `dd7ce6e598d3ebae6cd774e9c52c47d68f33e640c589b23d15afd1fc958bad5a`
- Manifest SHA-256: `c7ba10aaf98f4fb2b06394d1b337e4e0d69feacc1505b6c6c167b9e345afbfea`
- Model SHA-256: `f3f0b029aad54933d8dbcea192de35d9689c79e78149cb5235387ec943064093`
- `lakehouse.gold.training_examples` snapshot: `6550655688150864602`
- `lakehouse.silver.post_features` snapshot: `8259184521274725029`
- `lakehouse.silver.engagement_snapshots` snapshot: `4006086415507010724`

The pinned dataset contains 197 examples (156 X, 41 YouTube). Audience is deliberately
absent: the manifest records `excluded_no_prepublication_history`, Gold reports 100%
missing audience, and the model contains no `chan_*` feature. The author-grouped test
split contains 41 examples; its small size makes the confidence intervals wide.

| Group | n | ROC-AUC (95% CI) | PR-AUC (95% CI) | Brier | ECE | F1 @ 0.26 |
|---|---:|---:|---:|---:|---:|---:|
| **overall** | 41 | **0.623** [0.154, 0.950] | **0.193** [0.030, 0.697] | 0.110 | 0.207 | 0.154 |
| X | 35 | 0.803 [0.545, 0.992] | 0.244 [0.067, 1.000] | 0.101 | 0.226 | 0.190 |
| YouTube | 6 | 0.200 [0.000, 0.600] | 0.200 [0.167, 0.600] | 0.161 | 0.092 | 0.000 |

Machine-readable evidence is stored in `ml/results/stage1_evaluation.json` and
`ml/results/stage1_model_lineage.json`. Both carry the snapshot map and dataset
fingerprint; the latter also binds the serialized model by SHA-256.

---

## 1. Historical audience-enabled artifacts — non-citable

Sections 1–5 describe the July manual-export experiment. Its audience values were
observed during scraping and were not proven to precede publication. A viral post may
have increased the measured subscriber or follower count before collection. The feature
importance and performance below are therefore exposed to look-ahead bias, must not be
interpreted causally, and must not be used as evidence of deployable pre-publication
performance.

`ml/models/` is gitignored — these files exist only on a machine that has trained.

| File | Role |
|---|---|
| **`stage1_multisource.joblib`** | **the model the API loads** |
| `rhetorical_role.joblib` | 12-way marketing-role classifier over segments |
| `topic_model.joblib` | NMF, 8 topics over TF-IDF |
| `stage1_content_model.joblib` | superseded; only `serve/predict.py` still reads it |
| `stage1_fusion.joblib`, `stage1_content_embed.joblib` | dead — nothing loads them |

`stage1_multisource.joblib` is a dict:

| Key | Type | Value |
|---|---|---|
| `model` | `XGBClassifier` | 300 trees, `max_depth=3`, `lr=0.03` |
| `calibrator` | `LogisticRegression` | Platt scaling over the raw log-odds |
| `threshold` | `float` | **0.23** — picked out-of-fold, not 0.5 |
| `features` | `list` | the 52 column names, in order |
| `content_model` | `Pipeline` | TF-IDF (1-2 gram, 20k, `min_df=2`) → LogisticRegression |

---

## 2. Data

3798 labelled rows after cleaning, deduplication and dropping posts with no observed
engagement. Split 2928 / 870, grouped on `author_hash` so no author appears on both sides.

| Source | Rows | Viral | Rate | Known audience |
|---|---|---|---|---|
| youtube | 2000 | 500 | 0.250 | 1708 (85%) |
| reddit | 1079 | 285 | 0.264 | 297 (28%) |
| x | 719 | 180 | 0.250 | 362 (50%) |

> Historical result: this experiment used the retired dataset-relative label.
> It must not be compared directly with models trained under the current
> [versioned virality contract](../docs/VIRALITY_LABEL_CONTRACT.md).

For this historical run, the label was built **per source**: within a platform,
`log1p` of that platform's engagement metrics was z-scored, and the top 25% was
labelled viral. Engagement columns built the label and were never features.

---

## 3. Results

| Group | n | ROC-AUC (95% CI) | PR-AUC (95% CI) | Brier | ECE |
|---|---|---|---|---|---|
| **overall** | 870 | **0.794** [0.760, 0.826] | **0.607** [0.538, 0.669] | 0.145 | 0.017 |
| youtube | 401 | 0.886 [0.845, 0.921] | 0.762 [0.680, 0.833] | 0.104 | 0.031 |
| x | 150 | 0.749 [0.665, 0.830] | 0.523 [0.383, 0.668] | 0.176 | 0.064 |
| reddit | 319 | 0.661 [0.596, 0.723] | 0.409 [0.323, 0.510] | 0.182 | 0.030 |

`ECE = 0.017` means a reported "70%" is off by about 1.7 percentage points on average — the
probability can be shown to a user as a probability. X's interval spans 0.165 because it has
only 150 test rows; treat its number as indicative.

### Decisions at the 0.23 threshold

| Group | Precision | Recall | F1 | TP | FP | FN | TN |
|---|---|---|---|---|---|---|---|
| overall | 0.406 | 0.842 | 0.548 | 187 | 274 | 35 | 374 |
| youtube | 0.513 | 0.821 | 0.632 | 78 | 74 | 17 | 232 |
| x | 0.423 | 0.786 | 0.550 | 33 | 45 | 9 | 63 |
| reddit | 0.329 | 0.894 | 0.481 | 76 | 155 | 9 | 79 |

The threshold maximises F1 on out-of-fold training scores, which buys recall at the cost of
precision: the model catches 84% of viral posts and is wrong about 6 in 10 of the posts it
flags. That trade is a product decision, not a fixed property — raising the threshold moves
it back. Do not tune it on the table above; that is the test set.

### Exploratory role classifier

Macro-F1 **0.495** over 12 roles, 3122 silver segments. Strong on `cta` (F1 0.91) and
`proof` (0.70), weak on `objection_handling` (0.25) and `social_proof` (0.31). The labels are
automated heuristic silver with no independently human-verified gold set. The score
therefore measures agreement with held-out silver labels, not linguistic accuracy against
ground truth. Individual assignments must not be presented as validated linguistic
conclusions.

This component is retained as an **exploratory feature family**: it converts segment-level
heuristics into human-readable `role_*` dimensions that can be inspected qualitatively in
TreeSHAP. Role-derived absences are not used for prescriptive suggestions.

#### Paired role-feature ablation

The two variants below use the same pinned dataset, author-grouped split, content scores,
seed, XGBoost parameters and calibration procedure. The only controlled difference is the
presence of the 26 `role_*` columns.

| Variant | Features | PR-AUC | ROC-AUC | Brier | F1 |
|---|---:|---:|---:|---:|---:|
| With exploratory roles | 47 | 0.193 | 0.623 | 0.110 | 0.154 |
| Without roles | 21 | 0.271 | 0.658 | 0.112 | 0.250 |

For `without - with`, the paired-bootstrap PR-AUC delta is **+0.079** with 95% CI
**[-0.011, 0.368]**, and the ROC-AUC delta is **+0.035** with CI
**[-0.150, 0.250]**. Both intervals include zero: this small holdout demonstrates no
reliable predictive benefit from the role features. The point estimates favor removing
them, but are not precise enough to establish superiority. Full machine-readable evidence:
`results/stage1_role_ablation.json`.

---

## 4. Feature weights

XGBoost has no linear coefficients; the weight of a feature is its mean absolute SHAP
contribution over the test set (`pred_contribs`), which is exactly what drives the
`top_factors` shown per prediction.

### By family

| Family | Count | Influence |
|---|---|---|
| `chan_*` — channel audience | 4 | **29.6%** |
| `content_score` — TF-IDF text model | 1 | **21.1%** |
| `topic_*` — NMF topics | 8 | 20.2% |
| text / cognitive friction | 10 | 15.0% |
| `role_*` — marketing roles | 26 | 11.3% |
| `src_*` — platform one-hot | 3 | 2.9% |

The historical model assigned 29.6% of measured SHAP influence to audience. This large
value is a leakage warning, not evidence that audience has overwhelming pre-publication
predictive power: the collected count may already incorporate growth caused by the post.

In this historical SHAP analysis, the 26 role features carry 11.3% between them, the
weakest return per column in the set. SHAP measures how the model used these heuristic
signals; it does not validate the linguistic correctness of a role assignment.

### Top 20 individual features

| # | Feature | Weight | Share |
|---|---|---|---|
| 1 | `chan_log_audience` | 0.6452 | 29.56% |
| 2 | `content_score` | 0.4598 | 21.06% |
| 3 | `topic_0` | 0.1501 | 6.87% |
| 4 | `f_word` | 0.0775 | 3.55% |
| 5 | `topic_1` | 0.0774 | 3.55% |
| 6 | `char_count` | 0.0704 | 3.23% |
| 7 | `role_ratio_hook` | 0.0668 | 3.06% |
| 8 | `topic_3` | 0.0546 | 2.50% |
| 9 | `src_x` | 0.0503 | 2.30% |
| 10 | `topic_7` | 0.0480 | 2.20% |
| 11 | `topic_4` | 0.0478 | 2.19% |
| 12 | `has_question` | 0.0438 | 2.01% |
| 13 | `role_ratio_proof` | 0.0416 | 1.90% |
| 14 | `word_count` | 0.0394 | 1.80% |
| 15 | `cognitive_friction_score` | 0.0334 | 1.53% |
| 16 | `role_ratio_storytelling` | 0.0331 | 1.52% |
| 17 | `topic_2` | 0.0227 | 1.04% |
| 18 | `topic_5` | 0.0217 | 0.99% |
| 19 | `role_ratio_benefit` | 0.0201 | 0.92% |
| 20 | `topic_6` | 0.0191 | 0.87% |

These 15 strongest features carry **87.3%** of the total influence; the remaining 37 share
12.7%.

### Features contributing nothing

Exactly 0.0000 on the test set: `chan_has_audience`, `chan_audience_is_zero`,
`role_n_scarcity`, `role_ratio_scarcity`, `role_n_solution`, `role_n_social_proof`,
`role_n_urgency`, `role_ratio_pain_point`.

Two different causes, and they need different answers:

- `chan_has_audience` and `chan_audience_is_zero` are redundant now that an unknown audience
  is `NaN`: XGBoost routes missing values itself, so the flags say nothing new. They are kept
  because the export can carry a real observed zero, which they are the only way to express.
- The dead `role_*` columns cover roles the silver set barely contains (`scarcity` has 12
  samples, `solution` 27), so the classifier almost never predicts them. They will start
  carrying signal only when the annotation set grows.

---

## 5. How the numbers got here

Each step measured in isolation, on the same rows:

| Step | PR-AUC | ROC-AUC |
|---|---|---|
| starting point | 0.453 | 0.703 |
| + audience contract fixed (65 → 1042 known audiences) | 0.504 | 0.739 |
| + subscriber cache applied (453 → 1857 rows) | 0.603 | 0.793 |
| + shallower trees picked out-of-fold | 0.607 | 0.794 |

Per source, over the whole sequence: Reddit **0.476 → 0.661** (it used to predict *backwards*),
X **0.571 → 0.749**, calibration error **0.123 → 0.017**.

Hyperparameter tuning contributed almost nothing (+0.004, well inside the CI). Most of the
historical lift coincided with adding temporally invalid audience measurements and cannot
be credited to a leakage-free feature improvement.

---

## 6. What these numbers do not say

- **The July figures in sections 1–5 are historical only.** They use the manual CSV path
  and are not interchangeable with the official pinned-snapshot figures in section 0.
- **Audience is not timestamp-valid in sections 1–5.** Subscriber/follower counts were
  collected after publication and may partly be consequences of virality. Those figures
  are optimistic and the apparent feature power must not be generalized.
- **The official model excludes audience.** Reintroduction requires a frozen history and
  the invariant `reputation_observed_at <= post_published_at`. The intended inputs are
  timestamped subscriber/follower counts and author-level Reddit karma; the join selects
  only the last observation available before publication. See
  `docs/PREPUBLICATION_REPUTATION.md`.
- **Reddit's audience feature is degenerate.** `subreddit_member_count` takes 3 distinct
  values across the whole dataset, because the corpus covers 2 subreddits. Filling the
  remaining 782 Reddit rows with it would raise coverage without adding information — it
  would act as a disguised subreddit indicator. Author karma, collected at crawl time, is the
  signal worth having.
- **No Stage-2 number exists.** `build_stage2_dataset.py` and `train_stage2.py` are written
  and tested against synthetic trajectories, but `silver.engagement_snapshots` has no history
  yet. Any Stage-2 metric you see today was produced from generated data.
- **Two earlier figures are wrong and still circulate:** overall PR-AUC **0.773** and YouTube
  ROC-AUC **0.931**. Both came from filling an unknown audience with `0`, which turned the
  feature into a near-perfect stand-in for "is this YouTube?".
