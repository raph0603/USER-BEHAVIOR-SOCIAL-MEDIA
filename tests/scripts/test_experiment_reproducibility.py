from __future__ import annotations

from copy import deepcopy
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
for import_root in (ROOT, ML_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from common.reproducibility import (
    GitIdentity,
    build_split_manifest,
    canonical_json,
    capture_git_identity,
    capture_environment_manifest,
    compact_lineage,
    environment_identity,
    experiment_id,
    fingerprint,
    manifest_sha256,
    normalize_container_digest,
    require_official_git,
    split_fingerprint,
    validate_lineage_match,
    validate_split_manifest,
)
from experiment_config import resolved_training_config, validate_training_config
from reproducibility_cli import compare_replay, validate_artifacts
from evaluation_artifact import build_evaluation_artifact, validate_evaluation_inputs
import run_official_container
from train.train_viral import feature_columns


def lineage() -> dict:
    value = {
        "experiment_id": "experiment-v1-example",
        "dataset_version": "dataset-v2-1234567890abcdef1234",
        "dataset_fingerprint": "a" * 64,
        "manifest_sha256": "b" * 64,
        "git_commit": "c" * 40,
        "environment_fingerprint": "d" * 64,
        "training_config_fingerprint": "e" * 64,
        "split_fingerprint": "f" * 64,
        "model_sha256": "1" * 64,
        "silver_snapshot_ids": {"lakehouse.silver.post_features": 10},
        "gold_snapshot_id": 20,
        "determinism_contract": {
            "model_byte_identity_expected": False,
            "prediction_absolute_tolerance": 1e-9,
            "metric_absolute_tolerance": 1e-9,
        },
    }
    return value


def environment_manifest() -> dict:
    value = {
        "schema_version": "environment-v1",
        "generated_at": "2026-08-16T00:00:00Z",
        "code": {"git_commit": "c" * 40, "git_dirty": False},
        "runtime": {"python": "3.12.0", "java": None, "spark": None},
        "dependencies": {"numpy": "2.4.6"},
        "dependency_lock": {"path": "ml/requirements-train.txt", "sha256": "2" * 64},
        "container": {
            "runtime_detected": False,
            "image": None,
            "digest": None,
            "digest_available": False,
        },
    }
    value["environment_fingerprint"] = fingerprint(environment_identity(value))
    return value


def valid_artifacts() -> dict:
    dataset = {
        "schema_version": "v2",
        "source_snapshots": {
            "lakehouse.silver.engagement_snapshots": 11,
            "lakehouse.silver.post_features": 10,
        },
        "filters": {"label_horizon_hours": 24, "viral_quantile": 0.75},
        "gold_table": "lakehouse.gold.training_examples",
        "gold_snapshot_id": 20,
    }
    dataset["dataset_fingerprint"] = fingerprint(
        {
            "schema_version": dataset["schema_version"],
            "source_snapshots": dataset["source_snapshots"],
            "filters": dataset["filters"],
        }
    )
    dataset["dataset_version"] = f"dataset-v2-{dataset['dataset_fingerprint'][:20]}"
    dataset["manifest_sha256"] = manifest_sha256(dataset)
    environment = environment_manifest()
    config = resolved_training_config(
        seed=42,
        test_size=0.2,
        feature_columns=["char_count", "content_score"],
        feature_versions=["features-v1"],
        dataset_schema_version="v2",
        dataset_manifest=dataset,
        content_backend="tfidf_logistic_regression",
        scale_pos_weight=3.0,
    )
    split = build_split_manifest(
        ["a", "b"],
        ["c"],
        strategy="group_shuffle_split",
        group_column="author_hash",
        seed=42,
        test_size=0.2,
        id_column="example_id",
    )
    run_lineage = {
        "schema_version": "experiment-lineage-v1",
        "official_run": False,
        "dataset_version": dataset["dataset_version"],
        "dataset_fingerprint": dataset["dataset_fingerprint"],
        "silver_snapshot_ids": dataset["source_snapshots"],
        "gold_snapshot_id": dataset["gold_snapshot_id"],
        "manifest_sha256": dataset["manifest_sha256"],
        "git_commit": environment["code"]["git_commit"],
        "environment_fingerprint": environment["environment_fingerprint"],
        "training_config_fingerprint": config["training_config_fingerprint"],
        "split_fingerprint": split["split_fingerprint"],
        "model_sha256": "1" * 64,
        "virality_contract_fingerprint": None,
    }
    run_lineage["experiment_id"] = experiment_id(run_lineage)
    bundle = {
        "lineage": compact_lineage(run_lineage),
        "features": config["feature_schema"]["model_columns"],
    }
    evaluation = build_evaluation_artifact(
        lineage=run_lineage,
        model_sha256=run_lineage["model_sha256"],
        overall_metrics={"pr_auc": 0.5},
        source_metrics={"x": {"pr_auc": 0.5}},
        predictions=[{"content_id": "c", "source": "x", "label": 1, "probability": 0.7}],
    )
    return {
        "dataset_manifest": dataset,
        "environment_manifest": environment,
        "training_config": config,
        "split_manifest": split,
        "lineage": run_lineage,
        "bundle": bundle,
        "model_sha256": run_lineage["model_sha256"],
        "evaluation": evaluation,
    }


def test_canonical_json_and_fingerprint_ignore_mapping_order() -> None:
    first = {"b": [2, 1], "a": {"y": 2, "x": 1}}
    second = {"a": {"x": 1, "y": 2}, "b": [2, 1]}

    assert canonical_json(first) == canonical_json(second)
    assert fingerprint(first) == fingerprint(second)


def test_canonical_json_preserves_semantically_meaningful_array_order() -> None:
    assert fingerprint({"model_columns": ["a", "b"]}) != fingerprint({"model_columns": ["b", "a"]})


def test_environment_fingerprint_changes_with_dependency_version() -> None:
    first = environment_manifest()
    second = deepcopy(first)
    second["dependencies"]["numpy"] = "2.4.7"

    assert fingerprint(environment_identity(first)) != fingerprint(environment_identity(second))


def test_environment_timestamp_does_not_change_logical_identity() -> None:
    first = environment_manifest()
    second = deepcopy(first)
    second["generated_at"] = "2030-01-01T00:00:00Z"

    assert fingerprint(environment_identity(first)) == fingerprint(environment_identity(second))


def test_dataset_manifest_ignores_local_output_path_and_timestamp() -> None:
    first = {
        "dataset_version": "dataset-v2-example",
        "dataset_relative_path": "C:/local/run-a",
        "created_at": "2026-01-01T00:00:00Z",
    }
    second = {
        **first,
        "dataset_relative_path": "/tmp/run-b",
        "created_at": "2030-01-01T00:00:00Z",
    }

    assert manifest_sha256(first) == manifest_sha256(second)


def test_training_config_fingerprint_changes_with_hyperparameter() -> None:
    config = resolved_training_config(
        seed=42,
        test_size=0.2,
        feature_columns=["char_count", "content_score"],
        feature_versions=["features-v1"],
        dataset_schema_version="v2",
        dataset_manifest={"filters": {"viral_quantile": 0.75}},
        content_backend="tfidf_logistic_regression",
        auxiliary_artifacts={"topic_model.joblib": "a" * 64},
        scale_pos_weight=3.0,
    )
    validate_training_config(config)
    changed = deepcopy(config)
    changed.pop("training_config_fingerprint")
    changed["xgboost"]["max_depth"] += 1

    assert config["training_config_fingerprint"] != fingerprint(changed)


def test_split_fingerprint_is_order_independent_but_partition_sensitive() -> None:
    baseline = split_fingerprint(["c", "a"], ["d", "b"])

    assert baseline == split_fingerprint(["a", "c"], ["b", "d"])
    assert baseline != split_fingerprint(["a", "b"], ["c", "d"])


def test_split_manifest_validates_the_persisted_composition() -> None:
    manifest = build_split_manifest(
        ["a", "b"],
        ["c"],
        strategy="group_shuffle_split",
        group_column="author_hash",
        seed=42,
        test_size=0.2,
        id_column="example_id",
    )
    validate_split_manifest(manifest)
    manifest["holdout_content_ids"].append("d")

    with pytest.raises(ValueError, match="Split fingerprint"):
        validate_split_manifest(manifest)


def test_split_fingerprint_changes_with_semantic_split_contract() -> None:
    baseline = build_split_manifest(
        ["a", "b"],
        ["c"],
        strategy="group_shuffle_split",
        group_column="author_hash",
        seed=42,
        test_size=0.2,
        id_column="example_id",
    )
    changed = deepcopy(baseline)
    changed["seed"] = 43

    with pytest.raises(ValueError, match="Split fingerprint"):
        validate_split_manifest(changed)


def test_git_identity_detects_a_dirty_working_tree(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"], cwd=tmp_path, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "commit.gpgsign", "false"], cwd=tmp_path, check=True)
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "test: initialize fixture"], cwd=tmp_path, check=True)

    assert capture_git_identity(tmp_path).git_dirty is False
    tracked.write_text("dirty\n", encoding="utf-8")
    dirty = capture_git_identity(tmp_path)
    assert dirty.git_dirty is True
    with pytest.raises(RuntimeError, match="clean Git working tree"):
        require_official_git(dirty, allow_dirty_nonofficial=False)
    assert require_official_git(dirty, allow_dirty_nonofficial=True) is False


def test_container_digest_can_be_unavailable_without_being_invented() -> None:
    assert normalize_container_digest(None) is None
    assert normalize_container_digest("") is None
    with pytest.raises(ValueError, match="immutable"):
        normalize_container_digest("trainer:latest")


def test_created_container_must_match_the_resolved_image(monkeypatch) -> None:
    class Result:
        stdout = '[{"Image":"sha256:' + "a" * 64 + '"}]'

    monkeypatch.setattr(run_official_container, "_run", lambda *args, **kwargs: Result())
    run_official_container.validate_created_container("container-id", "sha256:" + "a" * 64)
    with pytest.raises(RuntimeError, match="image changed"):
        run_official_container.validate_created_container("container-id", "sha256:" + "b" * 64)


def test_environment_manifest_records_an_unavailable_container_digest(tmp_path: Path) -> None:
    lock = tmp_path / "requirements.txt"
    lock.write_text("example==1.0\n", encoding="utf-8")

    manifest = capture_environment_manifest(
        tmp_path,
        GitIdentity(git_commit="a" * 40, git_dirty=False),
        dependency_lock=lock,
        distributions=(),
        require_container_digest=False,
    )

    assert manifest["container"]["digest"] is None
    assert manifest["container"]["digest_available"] is False


def test_model_and_evaluation_receive_the_same_compact_lineage() -> None:
    expected = lineage()
    bundle = {"lineage": compact_lineage(expected)}
    evaluation = build_evaluation_artifact(
        lineage=expected,
        model_sha256=expected["model_sha256"],
        overall_metrics={"pr_auc": 0.5},
        source_metrics={"x": {"pr_auc": 0.5}},
        predictions=[{"content_id": "a", "source": "x", "label": 1, "probability": 0.7}],
    )

    assert bundle["lineage"] == evaluation["lineage"]
    assert evaluation["model_sha256"] == expected["model_sha256"]


def test_evaluation_rejects_a_different_model_or_environment() -> None:
    expected = lineage()
    environment = environment_manifest()
    expected["environment_fingerprint"] = environment["environment_fingerprint"]
    bundle = {"lineage": compact_lineage(expected)}

    with pytest.raises(ValueError, match="Serialized model"):
        validate_evaluation_inputs(
            bundle=bundle,
            lineage=expected,
            dataset_manifest=None,
            environment_manifest=environment,
            model_sha256="9" * 64,
        )

    wrong_environment = deepcopy(environment)
    wrong_environment["dependencies"]["numpy"] = "0.0.0"
    wrong_environment["environment_fingerprint"] = fingerprint(
        environment_identity(wrong_environment)
    )
    with pytest.raises(ValueError, match="Environment/model"):
        validate_evaluation_inputs(
            bundle=bundle,
            lineage=expected,
            dataset_manifest=None,
            environment_manifest=wrong_environment,
            model_sha256=expected["model_sha256"],
        )

    dataset = {
        "dataset_version": "dataset-v2-different000000000000",
        "dataset_fingerprint": "7" * 64,
        "gold_snapshot_id": 20,
    }
    dataset["manifest_sha256"] = manifest_sha256(dataset)
    with pytest.raises(ValueError, match="Dataset/model"):
        validate_evaluation_inputs(
            bundle=bundle,
            lineage=expected,
            dataset_manifest=dataset,
            environment_manifest=environment,
            model_sha256=expected["model_sha256"],
        )


def test_replay_validation_uses_explicit_prediction_and_metric_tolerances() -> None:
    reference_lineage = lineage()
    candidate_lineage = deepcopy(reference_lineage)
    reference_evaluation = {
        "predictions": [{"content_id": "a", "probability": 0.7}],
        "metrics": {"overall": {"pr_auc": 0.5}},
    }
    candidate_evaluation = {
        "predictions": [{"content_id": "a", "probability": 0.7 + 5e-10}],
        "metrics": {"overall": {"pr_auc": 0.5 + 5e-10}},
    }

    assert (
        compare_replay(
            reference_lineage,
            candidate_lineage,
            reference_evaluation,
            candidate_evaluation,
        )
        == []
    )
    candidate_lineage["split_fingerprint"] = "0" * 64
    assert "lineage.split_fingerprint differs" in compare_replay(
        reference_lineage,
        candidate_lineage,
        reference_evaluation,
        candidate_evaluation,
    )


def test_replay_fails_above_recorded_tolerances() -> None:
    reference_lineage = lineage()
    candidate_lineage = deepcopy(reference_lineage)
    reference_evaluation = {
        "predictions": [{"content_id": "a", "probability": 0.7}],
        "metrics": {"overall": {"pr_auc": 0.5}},
    }
    candidate_evaluation = {
        "predictions": [{"content_id": "a", "probability": 0.7 + 2e-9}],
        "metrics": {"overall": {"pr_auc": 0.5 + 2e-9}},
    }

    failures = compare_replay(
        reference_lineage,
        candidate_lineage,
        reference_evaluation,
        candidate_evaluation,
    )
    assert any(item.startswith("predictions") for item in failures)
    assert any(item.startswith("metrics") for item in failures)


def test_official_feature_selection_excludes_supervised_role_features() -> None:
    frame = __import__("pandas").DataFrame(
        columns=[
            "src_x",
            "topic_0",
            "role_ratio_hook",
            "chan_has_audience",
            *[
                "char_count",
                "word_count",
                "has_question",
                "is_vietnamese",
                "f_word",
                "f_sent",
                "f_clause",
                "f_info",
                "f_visual",
                "cognitive_friction_score",
            ],
        ]
    )

    selected = feature_columns(frame, include_roles=False)
    assert "role_ratio_hook" not in selected
    assert "topic_0" in selected


def test_central_verifier_executes_every_identity_check() -> None:
    artifacts = valid_artifacts()
    results = validate_artifacts(**artifacts)

    assert set(results) == {
        "Dataset fingerprint",
        "Manifest SHA-256",
        "Silver snapshots",
        "Gold snapshot",
        "Git revision",
        "Environment fingerprint",
        "Training config",
        "Feature schema",
        "Split fingerprint",
        "Model identity",
        "Evaluation metrics",
    }
    assert all(passed is True for passed, _ in results.values())


@pytest.mark.parametrize(
    ("check_name", "mutate"),
    [
        (
            "Dataset fingerprint",
            lambda value: value["dataset_manifest"].update(dataset_fingerprint="0" * 64),
        ),
        (
            "Manifest SHA-256",
            lambda value: value["dataset_manifest"].update(manifest_sha256="0" * 64),
        ),
        ("Silver snapshots", lambda value: value["lineage"].update(silver_snapshot_ids={})),
        ("Gold snapshot", lambda value: value["lineage"].update(gold_snapshot_id=21)),
        ("Git revision", lambda value: value["lineage"].update(git_commit="0" * 40)),
        (
            "Environment fingerprint",
            lambda value: value["lineage"].update(environment_fingerprint="0" * 64),
        ),
        ("Training config", lambda value: value["training_config"]["xgboost"].update(max_depth=99)),
        ("Feature schema", lambda value: value["bundle"].update(features=["different"])),
        ("Split fingerprint", lambda value: value["split_manifest"].update(seed=99)),
        ("Model identity", lambda value: value.update(model_sha256="9" * 64)),
        (
            "Evaluation metrics",
            lambda value: value["evaluation"].update(split_fingerprint="0" * 64),
        ),
    ],
)
def test_central_verifier_rejects_each_inconsistent_artifact(check_name, mutate) -> None:
    artifacts = valid_artifacts()
    mutate(artifacts)

    assert validate_artifacts(**artifacts)[check_name][0] is False
