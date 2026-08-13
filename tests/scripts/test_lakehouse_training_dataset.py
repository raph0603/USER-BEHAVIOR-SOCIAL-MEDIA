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

import dataset_manifest
import gold_schemas


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


class DatasetManifestTests(unittest.TestCase):
    def _identity(self, snapshots=None, filters=None):
        return dataset_manifest.DatasetIdentity(
            schema_version="v2",
            source_snapshots=snapshots
            or {
                "lakehouse.silver.post_features": 123,
                "lakehouse.silver.engagement_snapshots": 456,
            },
            filters=filters
            or {
                "label_horizon_hours": 24,
                "viral_quantile": 0.75,
            },
        )

    def test_same_inputs_have_the_same_version_regardless_of_mapping_order(self):
        first = self._identity()
        replay = self._identity(
            snapshots={
                "lakehouse.silver.engagement_snapshots": 456,
                "lakehouse.silver.post_features": 123,
            },
            filters={"viral_quantile": 0.75, "label_horizon_hours": 24},
        )

        self.assertEqual(first.fingerprint, replay.fingerprint)
        self.assertEqual(first.dataset_version, replay.dataset_version)
        self.assertRegex(first.dataset_version, r"^dataset-v2-[a-f0-9]{20}$")

    def test_snapshot_or_filter_change_creates_a_new_version(self):
        baseline = self._identity()
        new_snapshot = self._identity(
            snapshots={
                "lakehouse.silver.post_features": 124,
                "lakehouse.silver.engagement_snapshots": 456,
            }
        )
        new_filter = self._identity(filters={"label_horizon_hours": 24, "viral_quantile": 0.80})

        self.assertNotEqual(baseline.dataset_version, new_snapshot.dataset_version)
        self.assertNotEqual(baseline.dataset_version, new_filter.dataset_version)

    def test_missing_rate_preserves_an_empty_population(self):
        self.assertIsNone(dataset_manifest.missing_rate(0, 0))
        self.assertEqual(dataset_manifest.missing_rate(2, 4), 0.5)

    def test_gold_contract_contains_examples_and_manifest_lineage(self):
        for field in (
            "example_id",
            "observation_id",
            "engagement_coverage",
            "audience_count",
            "audience_available",
        ):
            self.assertIn(field, gold_schemas.TRAINING_EXAMPLES_DDL)
        for field in (
            "dataset_version",
            "schema_version",
            "period_start",
            "period_end",
            "source_tables_json",
            "iceberg_snapshots_json",
            "filters_json",
            "example_count",
            "missing_rates_json",
            "distributions_json",
            "dataset_fingerprint",
            "created_at",
        ):
            self.assertIn(field, gold_schemas.DATASET_MANIFESTS_DDL)
        self.assertEqual(gold_schemas.TRAINING_EXAMPLES_SCHEMA_VERSION, "v2")


class AudienceFeatureTests(unittest.TestCase):
    def test_reddit_community_size_is_not_treated_as_author_audience(self):
        frame = pd.DataFrame(
            {
                "source": ["reddit", "youtube", "x"],
                "subreddit_member_count": [500_000, np.nan, np.nan],
                "subscriber_count": [np.nan, 10_000, np.nan],
                "follower_count": [np.nan, np.nan, 2_000],
            }
        )

        result = BUILD_DATASET.add_channel_features(frame)

        self.assertTrue(np.isnan(result.loc[0, "chan_log_audience"]))
        self.assertEqual(result.loc[0, "chan_has_audience"], 0)
        self.assertAlmostEqual(result.loc[1, "chan_log_audience"], np.log1p(10_000))
        self.assertAlmostEqual(result.loc[2, "chan_log_audience"], np.log1p(2_000))

    def test_known_zero_and_unknown_audience_are_distinct(self):
        frame = pd.DataFrame(
            {
                "audience_count": [0, np.nan, 10],
                "audience_available": [True, False, True],
            }
        )

        result = BUILD_DATASET.add_channel_features(frame)

        self.assertEqual(result.loc[0, "chan_log_audience"], 0.0)
        self.assertEqual(result.loc[0, "chan_has_audience"], 1)
        self.assertEqual(result.loc[0, "chan_audience_is_zero"], 1)
        self.assertTrue(np.isnan(result.loc[1, "chan_log_audience"]))
        self.assertEqual(result.loc[1, "chan_has_audience"], 0)
        self.assertEqual(result.loc[1, "chan_audience_is_zero"], 0)
        self.assertAlmostEqual(result.loc[2, "chan_log_audience"], np.log1p(10))

    def test_legacy_numeric_zero_is_still_a_known_observation(self):
        result = BUILD_DATASET.add_channel_features(pd.DataFrame({"subscriber_count": [0, np.nan]}))

        self.assertEqual(result.loc[0, "chan_has_audience"], 1)
        self.assertEqual(result.loc[0, "chan_audience_is_zero"], 1)
        self.assertEqual(result.loc[1, "chan_has_audience"], 0)

    def test_a_real_count_survives_a_coverage_flag_that_denies_it(self):
        # The export merges a value from one collector with the flags of another: YouTube
        # videos.list cannot see subscriber counts and says so, while the row carries a
        # count fetched by channels.list. Discarding it cost 977 of 1042 known audiences.
        frame = pd.DataFrame(
            {
                "subscriber_count": [984000.0, np.nan],
                "subscriber_count_available": [False, False],
            }
        )

        result = BUILD_DATASET.add_channel_features(frame)

        self.assertEqual(result.loc[0, "chan_has_audience"], 1)
        self.assertAlmostEqual(result.loc[0, "chan_log_audience"], np.log1p(984000.0))
        self.assertEqual(result.loc[1, "chan_has_audience"], 0)

    def test_a_denied_zero_stays_unknown(self):
        # The flag still earns its keep: an un-observed placeholder zero must not be read
        # as an author who genuinely has no audience.
        frame = pd.DataFrame(
            {"follower_count": [0.0, 0.0], "follower_count_available": [False, True]}
        )

        result = BUILD_DATASET.add_channel_features(frame)

        self.assertEqual(result.loc[0, "chan_has_audience"], 0)
        self.assertTrue(np.isnan(result.loc[0, "chan_log_audience"]))
        self.assertEqual(result.loc[1, "chan_has_audience"], 1)
        self.assertEqual(result.loc[1, "chan_audience_is_zero"], 1)


class OfficialTrainingInputTests(unittest.TestCase):
    def _write_manifest(self, root: Path, *, relative_path: str | None = None):
        identity = dataset_manifest.DatasetIdentity(
            schema_version="v2",
            source_snapshots={
                "lakehouse.silver.post_features": 123,
                "lakehouse.silver.engagement_snapshots": 456,
            },
            filters={"label_horizon_hours": 24, "viral_quantile": 0.75},
        )
        dataset_path = root / "datasets" / identity.dataset_version
        dataset_path.mkdir(parents=True)
        manifest_path = root / "runs" / "run.json"
        manifest_path.parent.mkdir()
        payload = {
            "dataset_version": identity.dataset_version,
            "schema_version": identity.schema_version,
            "dataset_fingerprint": identity.fingerprint,
            "source_tables_json": dataset_manifest.canonical_json(
                {"tables": sorted(identity.source_snapshots)}
            ),
            "iceberg_snapshots_json": dataset_manifest.canonical_json(
                dict(identity.source_snapshots)
            ),
            "filters_json": dataset_manifest.canonical_json(dict(identity.filters)),
            "dataset_relative_path": relative_path
            or f"../datasets/{identity.dataset_version}",
            "format": "parquet",
            "official_input": True,
            "training_table": "lakehouse.gold.training_examples",
            "training_snapshot_id": 789,
            "example_count": 1,
        }
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")
        return manifest_path, dataset_path, identity, payload

    def test_builder_accepts_lossless_silver_column_names(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "silver.csv"
            pd.DataFrame(
                {
                    "user_id": ["author-1"],
                    "title": ["Example post"],
                    "event_ts": ["2026-07-30T00:00:00Z"],
                    "source": ["youtube"],
                }
            ).to_csv(path, index=False)

            result = BUILD_DATASET.load_events(path)

            self.assertEqual(result.loc[0, "author_hash"], "author-1")
            self.assertEqual(result.loc[0, "clean_text"], "Example post")
            self.assertIn("created_at", result.columns)

    def test_manifest_resolves_one_exact_relative_parquet_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path, dataset_path, identity, _ = self._write_manifest(root)

            resolved, version = RUN_PIPELINE.load_lakehouse_manifest(manifest_path)

            self.assertEqual(resolved, dataset_path.resolve())
            self.assertEqual(version, identity.dataset_version)

    def test_manifest_rejects_invalid_fingerprint_and_path_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary_root = Path(directory)
            root = temporary_root / "export"
            outside = temporary_root / "outside-dataset"
            outside.mkdir(parents=True)
            manifest_path, _, identity, payload = self._write_manifest(root)
            payload["dataset_fingerprint"] = "not-a-sha256"
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "SHA-256"):
                RUN_PIPELINE.load_lakehouse_manifest(manifest_path)

            payload["dataset_fingerprint"] = "b" * 64
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not match"):
                RUN_PIPELINE.load_lakehouse_manifest(manifest_path)

            payload["dataset_fingerprint"] = identity.fingerprint
            payload["dataset_relative_path"] = "../../outside-dataset"
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "escapes the export root"):
                RUN_PIPELINE.load_lakehouse_manifest(manifest_path)

    def test_official_labels_require_a_dataset_version(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.csv"
            pd.DataFrame(
                {
                    "source": ["youtube"],
                    "text_for_model": ["example text"],
                    "label_value": ["viral"],
                }
            ).to_csv(path, index=False)

            with self.assertRaisesRegex(ValueError, "dataset_version"):
                BUILD_DATASET.load_events(path)

    def test_model_training_revalidates_the_exact_dataset_version(self):
        expected = "dataset-v2-1234567890abcdef1234"
        frame = pd.DataFrame({"dataset_version": [expected, expected]})

        VALIDATE_DATASET_VERSION(frame, expected)
        with self.assertRaisesRegex(ValueError, "received"):
            VALIDATE_DATASET_VERSION(frame, "dataset-v2-aaaaaaaaaaaaaaaaaaaa")
        with self.assertRaisesRegex(ValueError, "must include"):
            VALIDATE_DATASET_VERSION(pd.DataFrame({"viral": [1]}), expected)

    def test_builder_pins_iceberg_snapshots_and_uses_insert_only_merges(self):
        source = (ROOT / "spark" / "jobs" / "maintenance" / "build_training_dataset.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('.option("snapshot-id", str(snapshot_id))', source)
        self.assertIn("lakehouse.gold.training_examples", source)
        self.assertIn("lakehouse.gold.dataset_manifests", source)
        self.assertGreaterEqual(source.count("WHEN NOT MATCHED THEN"), 2)
        self.assertIn("dataset_relative_path", source)
        self.assertIn("_feature_rank", source)
        self.assertIn("_snapshot_tiebreaker", source)
        self.assertIn("DATASET_BUILDER_REVISION", source)
        self.assertIn('"dataset_builder_revision"', source)
        self.assertIn('"training_snapshot_id": training_snapshot_id', source)
        self.assertIn("_read_snapshot(\n            spark,\n            TRAINING_EXAMPLES_TABLE", source)
        self.assertIn('examples.orderBy("example_id")', source)
        self.assertIn(
            "Exporting an existing dataset version requires --training-examples-snapshot-id",
            source,
        )
        self.assertIn("actual_count != expected_count", source)
        self.assertIn("DATASET_VERSION_PATTERN", source)
        self.assertNotIn("read.csv", source.lower())

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
            run_pipeline.index("Build dataset"), run_pipeline.index("Train role classifier")
        )
        self.assertNotIn("filtered_events.csv", source)

    def test_model_metrics_and_reports_preserve_the_snapshot_lineage(self):
        train = (ROOT / "ml" / "train" / "train_viral.py").read_text(encoding="utf-8")
        evaluation = (ROOT / "ml" / "train" / "evaluate.py").read_text(encoding="utf-8")
        report = (ROOT / "ml" / "report.py").read_text(encoding="utf-8")
        lineage = (ROOT / "ml" / "dataset_lineage.py").read_text(encoding="utf-8")

        self.assertIn('"dataset_lineage": dataset_lineage', train)
        self.assertIn("model_lineage_path", train)
        self.assertIn('"iceberg_snapshot_ids"', lineage)
        self.assertIn('"training_snapshot_id"', lineage)
        self.assertIn("dataset_lineage", evaluation)
        self.assertIn("pinned snapshot ID", report)

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
