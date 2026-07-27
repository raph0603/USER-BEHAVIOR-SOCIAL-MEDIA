# Stage-1 results — 2026-07-27

Every figure below comes from `ml/train/verify_answers.py`, which scores **what serving
actually returns** (calibrated probability, the threshold stored in the bundle) and reports
a 95% bootstrap confidence interval. Nothing here is quoted without one.

Regenerate the whole file's contents with:

```powershell
$env:PYTHONIOENCODING='utf-8'
& ".\ml\.venv\Scripts\python.exe" ml/run_pipeline.py `
    --manual-csv-input data/samples/newest_data_enriched.csv --allow-stale-input
& ".\ml\.venv\Scripts\python.exe" ml/train/verify_answers.py --n-boot 2000
```

---

## 1. Artifacts

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

The label is built **per source**: within a platform, `log1p` of that platform's engagement
metrics is z-scored, and the top 25% is labelled viral. Engagement columns build the label
and are never features.

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

### Role classifier

Macro-F1 **0.495** over 12 roles, 3122 silver segments. Strong on `cta` (F1 0.91) and
`proof` (0.70), weak on `objection_handling` (0.25) and `social_proof` (0.31). The labels are
LLM/heuristic silver with no human-verified gold set, so this is the least trustworthy
component and one of the clearest improvement targets.

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

Audience and content are now close (29.6% vs 21.1%). Before the audience contract was fixed,
`chan_log_audience` outweighed `content_score` **5.5×** — the model was reading the channel
and barely reading the post. It now does both.

The 26 role features carry 11.3% between them, the weakest return per column in the set.

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

Hyperparameter tuning contributed almost nothing (+0.004, well inside the CI). The gains came
from data correctness, not from modelling.

---

## 6. What these numbers do not say

- **They are not "official" by the paper's own rule.** The run above uses the manual CSV
  path, not a lakehouse dataset with a pinned Iceberg snapshot, manifest and fingerprint.
  Recompute from a frozen dataset version before putting any of this in a results table.
- **Audience is not timestamp-valid.** Subscriber counts were read today, not as of each
  post's publication date. A post that went viral has since gained subscribers, so the
  YouTube figures are optimistic. Audience size is a legitimate pre-launch feature; only the
  measurement is inflated. The honest fix is a historical snapshot at post time.
- **Reddit's audience feature is degenerate.** `subreddit_member_count` takes 3 distinct
  values across the whole dataset, because the corpus covers 2 subreddits. Filling the
  remaining 782 Reddit rows with it would raise coverage without adding information — it
  would act as a disguised subreddit indicator. Author karma, collected at crawl time, is the
  signal worth having.
- **No Stage-2 number exists**, and the first one will need a caveat. `build_stage2_dataset.py`
  and `train_stage2.py` are written and tested against synthetic trajectories, but
  `silver.engagement_snapshots` has no history yet. Worse, the task hides a trap: engagement
  counters are cumulative, so the level a post has reached by the horizon is a lower bound on
  its level at the label horizon — "big now" predicts "big later" for free. On the synthetic
  trajectories, ranking by the 6-hour view count alone beat the trained model (ROC-AUC 0.829
  vs 0.806). `train_stage2.py` prints that baseline beside every result for exactly this
  reason; a Stage-2 figure that does not clear it has learnt nothing about the curve.
- **Two earlier figures are wrong and still circulate:** overall PR-AUC **0.773** and YouTube
  ROC-AUC **0.931**. Both came from filling an unknown audience with `0`, which turned the
  feature into a near-perfect stand-in for "is this YouTube?".
