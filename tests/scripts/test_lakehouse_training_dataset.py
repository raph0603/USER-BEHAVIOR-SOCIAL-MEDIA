import ast
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
PIPELINE_DIR = ROOT / "spark" / "jobs" / "pipeline"
sys.path.insert(0, str(PIPELINE_DIR))
sys.path.insert(0, str(ROOT / "ml"))
sys.path.insert(0, str(ROOT))

import dataset_manifest
import gold_schemas
import virality_contract
from common.reproducibility import manifest_sha256


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BUILD_DATASET = _load_module(
    "ml_build_dataset",
    ROOT / "ml" / "preprocess" / "build_dataset.py",
)
RUN_PIPELINE = _load_module("ml_run_pipeline", ROOT / "ml" / "run_pipeline.py")


def _load_train_version_validator():
    path = ROOT / "ml" / "train" / "train_viral.py"
    module = ast.parse(path.read_text(encoding="utf-8"))
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "validate_dataset_version"
    )
    namespace = {"pd": pd}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["validate_dataset_version"]


VALIDATE_DATASET_VERSION = _load_train_version_validator()


def _write_contract(root: Path):
    contract = virality_contract.build_contract(
        {"x": [1.0, 2.0]},
        policy=virality_contract.TRAINING_REFERENCE_POLICY,
        quantile=0.75,
        reference={"source_snapshots": {"features": 1, "engagement": 2}},
        horizon_hours=24,
        tolerance_hours=24,
        eligibility_filters={"min_text_chars": 3},
        min_reference_examples_per_platform=1,
    )
    path = root / "virality-contracts" / f"{contract.fingerprint}.json"
    contract.write(path)
    return contract, path


def _load_feature_columns():
    path = ROOT / "ml" / "train" / "train_viral.py"
    module = ast.parse(path.read_text(encoding="utf-8"))
    content_features = next(
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "CONTENT_FEATURES"
            for target in node.targets
        )
    )
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "feature_columns"
    )
    namespace = {"pd": pd}
    exec(
        compile(ast.Module(body=[content_features, function], type_ignores=[]), str(path), "exec"),
        namespace,
    )
    return namespace["feature_columns"]


FEATURE_COLUMNS = _load_feature_columns()


class OfficialTrainingInputTests(unittest.TestCase):
    def test_dataset_manifest_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            payload = {
                "schema_version": "v1",
                "dataset_version": "dataset-v3-0123456789abcdef0123",
                "dataset_fingerprint": "0" * 64,
                "manifest_sha256": "1" * 64,
            }
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), payload)

    def test_manifest_sha_excludes_volatile_fields(self):
        payload = {
            "schema_version": "v1",
            "dataset_version": "dataset-v3-0123456789abcdef0123",
            "dataset_fingerprint": "0" * 64,
            "created_at": "now",
            "dataset_relative_path": "one.parquet",
        }
        first = manifest_sha256(payload)
        payload["created_at"] = "later"
        payload["dataset_relative_path"] = "two.parquet"
        self.assertEqual(first, manifest_sha256(payload))

    def test_validate_dataset_version_accepts_expected_value(self):
        frame = pd.DataFrame({"dataset_version": ["dataset-v3-abc"]})
        VALIDATE_DATASET_VERSION(frame, "dataset-v3-abc")

    def test_validate_dataset_version_rejects_mismatch(self):
        frame = pd.DataFrame({"dataset_version": ["dataset-v3-abc"]})
        with self.assertRaises(ValueError):
            VALIDATE_DATASET_VERSION(frame, "dataset-v3-other")

    def test_contract_helper_writes_versioned_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            contract, path = _write_contract(Path(directory))
            self.assertTrue(path.exists())
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8"))["fingerprint"],
                contract.fingerprint,
            )

    def test_build_dataset_rejects_missing_contract_lineage_for_official_input(self):
        frame = pd.DataFrame({"source": ["x"], "viral": [1]})
        with self.assertRaises(ValueError):
            BUILD_DATASET.validate_virality_lineage(frame)

    def test_training_pipeline_requires_lakehouse_manifest_for_official_run(self):
        source = (ROOT / "ml" / "run_pipeline.py").read_text(encoding="utf-8")
        self.assertIn("Official training requires --lakehouse-manifest", source)
        self.assertIn("--manual-csv-input", source)

    def test_training_pipeline_records_environment_identity(self):
        source = (ROOT / "ml" / "run_pipeline.py").read_text(encoding="utf-8")
        self.assertIn("capture_environment_manifest", source)
        self.assertIn("require_official_git", source)
        self.assertIn("validate_dataset_build_environment", source)

    def test_training_pipeline_passes_virality_contract_identity(self):
        source = (ROOT / "ml" / "run_pipeline.py").read_text(encoding="utf-8")
        self.assertIn("--virality-contract-fingerprint", source)
        self.assertIn("--virality-policy", source)

    def test_builder_declares_expected_training_tables(self):
        source = (
            ROOT / "spark" / "jobs" / "maintenance" / "build_training_dataset.py"
        ).read_text(encoding="utf-8")
        self.assertIn("lakehouse.silver.post_features", source)
        self.assertIn("lakehouse.silver.engagement_snapshots", source)
        self.assertIn("lakehouse.gold.training_examples", source)

    def test_builder_records_snapshot_and_environment_fields(self):
        source = (
            ROOT / "spark" / "jobs" / "maintenance" / "build_training_dataset.py"
        ).read_text(encoding="utf-8")
        for field in (
            "gold_snapshot_id",
            "build_environment",
            "build_environment_fingerprint",
            "iceberg_snapshots_json",
        ):
            self.assertIn(field, source)

    def test_training_dag_builds_then_consumes_the_run_manifest(self):
        source = (ROOT / "orchestrator" / "dags" / "ai_train_pipeline.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('task_id="build_lakehouse_training_dataset"', source)
        self.assertIn("build_training_dataset.py", source)
        self.assertIn("--dataset-version", source)
        self.assertIn("--lakehouse-manifest", source)
        self.assertIn("--post-features-snapshot-id", source)
        self.assertIn("--engagement-snapshots-snapshot-id", source)
        self.assertIn("/workspace/data/lakehouse-ml/ml/runs/", source)
        self.assertIn("initialize_services >> build_lakehouse_dataset >> train_stage1", source)
        run_pipeline = (ROOT / "ml" / "run_pipeline.py").read_text(encoding="utf-8")
        self.assertLess(
            run_pipeline.index("Train role classifier"), run_pipeline.index("Build dataset")
        )
        self.assertNotIn("filtered_events.csv", source)

    def test_model_metrics_and_reports_preserve_the_snapshot_lineage(self):
        train = (ROOT / "ml" / "train" / "grouped_cv_stage1.py").read_text(encoding="utf-8")
        evaluation = (ROOT / "ml" / "train" / "evaluate.py").read_text(encoding="utf-8")
        report = (ROOT / "ml" / "report.py").read_text(encoding="utf-8")
        lineage = (ROOT / "ml" / "dataset_lineage.py").read_text(encoding="utf-8")

        self.assertIn('"dataset_lineage": dataset_lineage', train)
        self.assertIn("model_lineage_path", train)
        self.assertIn('"iceberg_snapshot_ids"', lineage)
        self.assertIn('"training_snapshot_id"', lineage)
        self.assertIn("dataset_lineage", evaluation)
        self.assertIn("pinned snapshot ID", report)

    def test_official_model_excludes_unfrozen_audience_features(self):
        frame = pd.DataFrame(
            {
                "char_count": [10],
                "word_count": [2],
                "has_question": [0],
                "is_vietnamese": [0],
                "f_word": [0.1],
                "f_sent": [0.1],
                "f_clause": [0.1],
                "f_info": [0.1],
                "f_visual": [0.1],
                "cognitive_friction_score": [0.1],
                "src_x": [1],
                "topic_0": [0.5],
                "chan_log_audience": [9.0],
                "chan_has_audience": [1],
            }
        )

        features = FEATURE_COLUMNS(frame, include_audience=False)

        self.assertIn("src_x", features)
        self.assertIn("topic_0", features)
        self.assertFalse(any(name.startswith("chan_") for name in features))

    def test_role_feature_ablation_changes_only_the_exploratory_family(self):
        frame = pd.DataFrame(
            {
                "char_count": [10],
                "word_count": [2],
                "has_question": [0],
                "is_vietnamese": [0],
                "f_word": [0.1],
                "f_sent": [0.1],
                "f_clause": [0.1],
                "f_info": [0.1],
                "f_visual": [0.1],
                "cognitive_friction_score": [0.1],
                "src_x": [1],
                "topic_0": [0.5],
                "role_ratio_hook": [0.5],
                "role_n_hook": [1],
                "chan_log_audience": [9.0],
            }
        )

        with_roles = FEATURE_COLUMNS(frame, include_audience=False, include_roles=True)
        without_roles = FEATURE_COLUMNS(frame, include_audience=False, include_roles=False)

        self.assertEqual(
            set(with_roles) - set(without_roles),
            {"role_ratio_hook", "role_n_hook"},
        )
        self.assertFalse(any(name.startswith("role_") for name in without_roles))
        self.assertFalse(any(name.startswith("chan_") for name in without_roles))

    def test_role_component_is_encoded_as_exploratory_in_artifacts(self):
        contract = (ROOT / "ml" / "role_contract.py").read_text(encoding="utf-8")
        training = (ROOT / "ml" / "train" / "grouped_cv_stage1.py").read_text(
            encoding="utf-8"
        )
        evaluation = (ROOT / "ml" / "train" / "evaluate.py").read_text(encoding="utf-8")
        serving = (ROOT / "ml" / "serve" / "explain_viral.py").read_text(encoding="utf-8")

        self.assertIn('ROLE_COMPONENT_STATUS = "exploratory"', contract)
        self.assertIn('"human_gold_validated": False', contract)
        self.assertIn('"role_feature_contract": role_feature_contract()', training)
        self.assertIn('"role_feature_contract": bundle.get', evaluation)
        self.assertIn("Exploratory role cue", serving)
        self.assertNotIn("Add a clear call to action", serving)

    def test_balancing_preserves_unknown_and_known_zero_bands(self):
        source = (ROOT / "spark" / "jobs" / "maintenance" / "build_balanced_dataset.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('lit("unknown")', source)
        self.assertIn('lit("none")', source)
        self.assertIn("engagement_observed_metrics", source)
        self.assertIn("_metric_available", source)


if __name__ == "__main__":
    unittest.main()
