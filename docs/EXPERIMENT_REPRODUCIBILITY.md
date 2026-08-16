# Reproducible experiment identity

The official Stage-1 workflow records the complete chain that produced a reported result:

```text
pinned Silver Iceberg snapshots
  -> deterministic dataset fingerprint
  -> pinned Gold training_examples snapshot
  -> exact Git revision
  -> resolved runtime and container identity
  -> resolved training configuration
  -> persisted train/holdout composition
  -> serialized model SHA-256
  -> evaluation of that serialized model
```

No value in this chain is intended to be filled in after training. The dataset builder,
training process, and evaluator obtain identities from the resources they actually use.

## Identity layers

| Identity | Meaning | Primary evidence |
|---|---|---|
| Dataset | Source snapshots, labeling/filter contract, and exact Gold export | Lakehouse dataset manifest |
| Code | Full Git commit and initial working-tree state | `environment_manifest.json` |
| Environment | Runtime versions, installed dependencies, lock-file SHA-256, and container digest when applicable | `environment_manifest.json` |
| Training configuration | Fully resolved split, TF-IDF, logistic-regression, NMF, XGBoost, calibration, threshold, feature, labeling, and audience parameters | `training_config.json` |
| Split | Exact sorted stable IDs assigned to train and holdout | `split_manifest.json` |
| Model | SHA-256 of the serialized bundle plus its embedded compact lineage | `experiment_lineage.json` and the joblib bundle |
| Evaluation | Metrics and holdout predictions tied to the exact model SHA-256 | `evaluation.json` |

`experiment_id` is deterministic. It hashes the dataset, manifest, Git, environment,
training-configuration, and split identities; it is not a random run label.

## Fingerprint algorithms

All logical fingerprints use UTF-8 JSON with sorted keys and compact separators, followed
by SHA-256.

- `dataset_fingerprint` remains the existing hash of the training-example schema, pinned
  source snapshots, and deterministic filters.
- `manifest_sha256` hashes immutable dataset-manifest fields. `created_at`,
  `generated_at`, and the machine-specific `dataset_relative_path` are excluded.
- `environment_fingerprint` hashes code identity, runtime versions, installed dependency
  versions, the dependency-lock SHA-256, and container identity. `generated_at` is excluded.
- `training_config_fingerprint` hashes the resolved configuration used by training.
  It also contains the SHA-256 of the role and topic artifacts used to build features.
- `split_fingerprint` hashes sorted `train_content_ids` and `holdout_content_ids` plus
  the strategy, group column, stable-ID column, seed, and test size. Membership order is
  irrelevant; feature-column order remains meaningful and is preserved.
- `model_sha256` hashes the bytes of the model file that evaluation loads.
- `evaluation_fingerprint` hashes the complete logical evaluation payload except its
  generation timestamp.

The dependency lock is `ml/requirements-train.txt`; the workflow reuses it rather than
introducing another package manager.

## Official-run requirements

An official run requires all of the following:

1. Silver inputs are read by immutable Iceberg snapshot ID. If balancing is enabled, the
   balanced-events snapshot is pinned too.
2. The Gold training table is re-read at a captured immutable snapshot before export.
3. The dataset manifest contains a valid dataset fingerprint, Gold snapshot ID, and
   canonical manifest SHA-256.
4. `git rev-parse --verify HEAD` resolves a full commit and the working tree is clean at
   run start.
5. Runtime and installed dependency versions are captured from the training process.
6. Spark dataset construction records Python, Java, Spark, locked dependencies, and both
   driver and executor image digests. Container training receives its own immutable digest
   resolved from Docker metadata. A
   repository digest is preferred; a local image ID is used for an unpushed local build.
7. Training configuration and the exact split are persisted before model fitting.
8. Evaluation loads the serialized model, verifies its SHA-256 and all lineage fields,
   then selects the holdout from the persisted ID list.

A dirty tree fails by default. `--allow-dirty-nonofficial` is the explicit compatibility
escape hatch and marks the run non-official. A legacy dataset manifest similarly requires
`--allow-legacy-manifest-nonofficial`.

When training inside Docker, use the launcher below. It builds when requested, inspects
the configured image, and injects `ML_CONTAINER_IMAGE` and
`ML_CONTAINER_IMAGE_DIGEST`; neither value is hardcoded.

```bash
python ml/run_official_container.py --service ai-trainer --build -- \
  python ml/run_pipeline.py \
  --lakehouse-manifest /workspace/data/lakehouse-ml/runs/<run>.json \
  --report
```

## Generated evidence

The current default paths are:

```text
ml/results/environment_manifest.json
ml/results/training_config.json
ml/results/split_manifest.json
ml/results/experiment_lineage.json
ml/models/stage1_multisource.joblib
ml/results/evaluation.json
```

The split sidecar deliberately contains the full sorted partition membership so an audit
can reconstruct the holdout exactly. The model and evaluation carry the compact identity,
while `experiment_lineage.json` carries source snapshots, Gold snapshot, the determinism
contract, and the model SHA-256.

## Replay and verification

Keep reference copies of the dataset manifest, experiment lineage, and evaluation. Check
out the recorded `git_commit`, restore or build the recorded container environment, and
re-run with a preflight assertion:

```bash
python ml/run_official_container.py --service ai-trainer --build -- \
  python ml/run_pipeline.py \
  --lakehouse-manifest /workspace/data/lakehouse-ml/runs/<reference>.json \
  --expected-lineage /workspace/reference/experiment_lineage.json
```

`--expected-lineage` fails before fitting if the dataset, Git commit, environment,
training configuration, or split differs. Verify the resulting artefacts and compare the
logical replay outputs with:

```bash
python ml/reproducibility_cli.py verify \
  --dataset-manifest data/lakehouse-ml/runs/<reference>.json \
  --environment-manifest ml/results/environment_manifest.json \
  --training-config ml/results/training_config.json \
  --split-manifest ml/results/split_manifest.json \
  --lineage ml/results/experiment_lineage.json \
  --model ml/models/stage1_multisource.joblib \
  --evaluation ml/results/evaluation.json \
  --reference-lineage reference/experiment_lineage.json \
  --reference-evaluation reference/evaluation.json
```

The command prints one PASS/FAIL line per identity and exits nonzero on failure. Without
reference artifacts, `Replay outputs` is truthfully reported as `NOT RUN` rather than
being inferred from field presence. Persist the machine-readable and reviewable tables:

```bash
python ml/reproducibility_cli.py verify \
  --dataset-manifest data/lakehouse-ml/runs/<reference>.json \
  --reference-lineage reference/experiment_lineage.json \
  --reference-evaluation reference/evaluation.json \
  --report-json ml/results/reproducibility_check.json \
  --report-markdown ml/results/reproducibility_check.md
```

Extract the paper-ready identity values without hardcoding them:

```bash
python ml/reproducibility_cli.py report \
  --dataset-manifest data/lakehouse-ml/runs/<reference>.json \
  --lineage ml/results/experiment_lineage.json \
  --evaluation ml/results/evaluation.json \
  --output ml/results/paper_run_identity.json
```

## Determinism contract and limitations

The current workflow does not claim byte-identical joblib/XGBoost output. Those libraries
do not make serialization-byte stability a public cross-build guarantee. Every evaluation
is still bound to the exact evaluated bytes by `model_sha256`.

Replay compares:

- Silver and Gold snapshots;
- dataset, feature-schema, environment, configuration, and split fingerprints;
- ordered holdout labels and probabilities;
- evaluation metrics.

The lineage records explicit absolute tolerances for predictions and metrics. Model
SHA-256 equality is required only if a future determinism contract explicitly sets
`model_byte_identity_expected` to true.

This work does not change the top-25% definition, engagement score, target, virality
threshold protocol, or grouped train/holdout methodology.

### Corpus-fitted transformations

The main TF-IDF/logistic content model is fitted out-of-fold on the outer training rows;
the holdout vocabulary is never used to fit it. The rhetorical-role classifier is fitted
on a separate supervised silver corpus. An overlap audit found that such a corpus can
contain exact text also present in the outer holdout, so official models exclude all
`role_*` features. They remain available only to explicitly exploratory role analyses
until their fitting boundary is derived from the persisted outer split.

The current NMF topic model is an explicitly transductive, unsupervised transformation:
its TF-IDF vocabulary and topic basis are fitted on the full experiment corpus before the
outer split. This is recorded as `topic_model.fit_scope =
full_dataset_unsupervised_transductive` in the training configuration. It does not use
labels, but it does use holdout text distribution and is therefore a known methodological
limitation. Changing that fit scope belongs to the separate evaluation-protocol work; it
must not be described as inductive performance in the meantime.

### Guaranteed replay contract

With the same immutable dataset, code, environment, resolved configuration, and persisted
split, replay requires the same feature schema and compares predictions and metrics using
the absolute tolerances recorded before comparison in `determinism_contract`. The current
values are `1e-12` for probabilities and metrics. Tests cover both sides of each boundary.
The contract does not require byte-identical joblib/XGBoost serialization, so a replayed
model SHA may differ; every individual evaluation must still match the SHA of the exact
serialized file it loaded.

## Paper run-identity table

After the next successful official run, populate `IDENTITY OF THE REPORTED REPRODUCIBLE
RUN` directly from these fields; do not type substitute values:

| Paper field | Source field |
|---|---|
| Dataset version | dataset manifest: `dataset_version` |
| Dataset fingerprint | dataset manifest: `dataset_fingerprint` |
| Silver post-features snapshot | dataset manifest: `iceberg_snapshots_json` |
| Silver engagement snapshot | dataset manifest: `iceberg_snapshots_json` |
| Gold training-set snapshot | dataset manifest: `gold_snapshot_id` |
| Manifest SHA-256 | dataset manifest: `manifest_sha256` |
| Git commit | environment manifest: `code.git_commit` |
| Container image digest | environment manifest: `container.digest` |
| Environment fingerprint | environment manifest: `environment_fingerprint` |
| Training-config fingerprint | training config: `training_config_fingerprint` |
| Split fingerprint | split manifest: `split_fingerprint` |
| Model SHA-256 | experiment lineage or evaluation: `model_sha256` |
| Evaluation fingerprint | evaluation artifact: `evaluation_fingerprint` |

No reported-run values are committed by this change because no clean official run was
performed as part of it.
