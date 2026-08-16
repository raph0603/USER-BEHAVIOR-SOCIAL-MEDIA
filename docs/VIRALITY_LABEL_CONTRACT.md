# Virality label contract

## Ground-truth definition

The legacy dataset schema (`dataset-v2-*`) labeled the highest-ranked 25% of
each platform inside every dataset. Those artifacts are identified as
`legacy_dataset_relative_top_quartile`; they are not rewritten or accepted as
new official inputs.

The current schema (`dataset-v3-*`) uses an absolute threshold in the existing
engagement-score space:

```text
tau_s = Q_0.75({e_i : i belongs to the pinned reference population R_s})
y_i = 1[e_i >= tau_s]
```

The quantile method is explicitly `linear`. Equality is positive. Adding,
removing, or changing unrelated evaluation rows cannot change an existing
label because the builder only applies the frozen `tau_s` values.

The engagement score itself is unchanged. Its version is
`coverage_aware_log_sum_sqrt_observed_v1`: the available, non-negative
platform counters are transformed with `log1p`, summed, and divided by the
square root of the number of observed counters. The current temporal contract
selects the earliest observation at or after `T+24h`, within the configured
tolerance (currently 24 hours by default).

## Policies

`platform_reference_quantile` is the target policy. It requires a pre-existing
contract whose reference population is independently pinned by dataset
identity and exact Iceberg snapshot IDs. The builder fails if the contract is
missing, incompatible with the engagement horizon, or lacks a platform.

No independent historical reference population exists in the repository at
this revision. For that reason, an explicitly limited
`training_reference_quantile` mode is available. It reproduces the current
author-grouped 80/20 split before labeling, estimates thresholds only from the
training partition, freezes the contract, and then applies it to both train and
holdout rows. Holdout scores never participate in threshold estimation. This
mode is suitable for transitional experiments, but it is not evidence of
temporal generalization and must not be described as a historical reference.

The reference-size guard has intentionally no library or environment default.
`--min-reference-examples-per-platform` must be chosen explicitly. The Airflow
form exposes a technical minimum of one so that the parameter is always
visible; that value is not a claim of statistical adequacy and should be
overridden deliberately.

## Contract and lineage

Contracts are written to:

```text
<export-root>/virality-contracts/<virality_contract_fingerprint>.json
```

The SHA-256 fingerprint uses canonical JSON and includes the policy, quantile,
quantile method, thresholds, engagement-score version, horizon, tolerance,
observation-selection policy, reference identity and snapshots, and relevant
eligibility rules. Generation timestamps and descriptive diagnostics are
excluded from the logical fingerprint.

The dataset manifest, exported Parquet rows, training manifest, model bundle,
and evaluation JSON all carry `virality_policy` and
`virality_contract_fingerprint`. Training and evaluation fail on a mismatch.
The model's separate decision setting is named
`classification_probability_threshold`; it is not a ground-truth engagement
threshold, and its calibration behavior is unchanged.

## Transitional build

Choose the minimum reference size explicitly:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/build_training_dataset.py \
  --dataset-version auto \
  --label-horizon-hours 24 \
  --label-tolerance-hours 24 \
  --virality-policy training_reference_quantile \
  --viral-quantile 0.75 \
  --min-reference-examples-per-platform <chosen-minimum> \
  --virality-contract-output /opt/spark/balancing/ml/virality_thresholds.json \
  --export-root /opt/spark/balancing/ml \
  --manifest-output /opt/spark/balancing/ml/runs/virality-v3.json
```

## Pinned historical build

Generate a contract only after exporting a genuinely independent scored
reference and its immutable manifest (dataset fingerprint and exact Iceberg
snapshot IDs):

```bash
python scripts/build_virality_contract.py \
  --reference-data <pinned-reference-parquet> \
  --reference-manifest <pinned-reference-manifest.json> \
  --output <virality-thresholds.json> \
  --quantile 0.75 \
  --horizon-hours 24 \
  --tolerance-hours 24 \
  --min-reference-examples-per-platform <chosen-minimum>
```

After review, apply it without recalculation:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/maintenance/build_training_dataset.py \
  --dataset-version auto \
  --label-horizon-hours 24 \
  --label-tolerance-hours 24 \
  --virality-policy platform_reference_quantile \
  --virality-contract /opt/spark/balancing/ml/virality_thresholds.json \
  --export-root /opt/spark/balancing/ml \
  --manifest-output /opt/spark/balancing/ml/runs/virality-v3.json
```

Never create a threshold for an unsupported platform from another platform's
data. A missing or insufficient platform reference is a hard failure.
