# Reddit audience ablation audit — 2026-07-30

The first local run described below is **invalid as an audience ablation** and
must not be used as a model result. The raw Reddit corpus did not contain an
audience measurement. A current subreddit member count was injected while
preparing the training input, after the outcomes were already known.

## Data

- 9,696 labelled Reddit comments after cleaning and text deduplication
- 7,926 train rows / 1,770 test rows
- 3,678 distinct authors before the split
- viral label: top engagement-score quartile within Reddit
- test viral rate: 26.27%
- genuine audience coverage in the raw input: 0 / 9,696
- injected audience coverage in the prepared input: 9,696 / 9,696
- distinct audience values: **1**
- rhetorical-role model unavailable locally; `role_*` features were skipped in both variants

The training corpus only contains `r/electricvehicles`. Its current member
count was inserted into every row during preparation. It is neither a
historical measurement nor an author-level audience feature.

## Discarded diagnostic output

These figures only demonstrate that a constant column has no mathematical
effect. They are not valid with-versus-without-audience metrics.

| Variant | ROC-AUC (95% CI) | PR-AUC (95% CI) | Brier | Precision | Recall | F1 | Accuracy |
|---|---:|---:|---:|---:|---:|---:|---:|
| with audience | 0.583 [0.556, 0.614] | 0.326 [0.294, 0.365] | 0.191 | 0.275 | 0.903 | 0.421 | 0.348 |
| without audience | 0.583 [0.556, 0.614] | 0.326 [0.294, 0.365] | 0.191 | 0.275 | 0.903 | 0.421 | 0.348 |
| delta (with - without) | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

Both variants selected a 0.22 decision threshold from author-grouped
out-of-fold training predictions. On the holdout, each produced TP=420,
FP=1,109, FN=45 and TN=196.

## Why the run is invalid

The zero delta was predetermined by preprocessing: every row received the same
value. The run says nothing about whether audience helps predict Reddit
engagement. Using a current value also creates temporal misalignment because it
was measured after the comments and their scores.

A valid follow-up needs author karma or another audience proxy captured at or
before comment time, with real variation across authors. If subreddit size is
used instead, it must be timestamped historically and the corpus must cover
multiple subreddits; even then it measures community size, not author audience.

The reusable trainer now rejects an entirely missing or constant audience
feature. Run it only on a prepared dataset containing genuine variation:

```powershell
& ".\ml\.venv\Scripts\python.exe" ml/train/train_reddit_audience_ablation.py `
    --data ml/data/train_dataset.parquet `
    --folds 5 `
    --n-boot 2000
```

The valid follow-up uses five-fold `StratifiedGroupKFold` validation. Authors
remain exclusive to one validation fold, while stratification keeps the viral
rate comparable across folds. Class imbalance is handled only in each training
partition through a fold-specific XGBoost `scale_pos_weight`; validation rows
are never duplicated, removed, or otherwise resampled. Text-model scores are
also generated out of fold with the same author grouping.

The command writes:

- `ml/models/reddit_with_audience.joblib`
- `ml/models/reddit_without_audience.joblib`
- `ml/results/reddit_audience_ablation.json`
