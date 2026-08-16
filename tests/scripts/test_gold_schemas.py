"""
Tests for spark/jobs/pipeline/gold_schemas.py

Validates:
- ModelPredictionRow dataclass round-trips correctly
- TrainingExampleRow dataclass round-trips correctly
- Validation helpers catch invalid rows
- DDL contains all required columns
- Gold tables are not mixed with monitoring tables
"""

import sys
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE_PATH = ROOT / "spark" / "jobs" / "pipeline"
sys.path.insert(0, str(PIPELINE_PATH))

import gold_schemas as gs


class ModelPredictionRowTests(unittest.TestCase):
    def _valid_row(self, **overrides) -> gs.ModelPredictionRow:
        base = dict(
            source="x",
            platform_event_id="tweet_999",
            prediction_ts=datetime(2026, 6, 1, 12, 0, 0),
            model_version="1.0.0",
            model_type="bert-base-uncased",
            predicted_class="viral",
            confidence=0.92,
            virality_score=0.75,
            context_used=True,
        )
        base.update(overrides)
        return gs.ModelPredictionRow(**base)

    def test_valid_row_passes_validation(self):
        row = self._valid_row()
        errors = gs.validate_prediction_row(row)
        self.assertEqual(errors, [], f"Unexpected errors: {errors}")

    def test_missing_source_fails(self):
        row = self._valid_row(source="")
        errors = gs.validate_prediction_row(row)
        self.assertTrue(any("source" in e for e in errors))

    def test_invalid_confidence_fails(self):
        row = self._valid_row(confidence=1.5)
        errors = gs.validate_prediction_row(row)
        self.assertTrue(any("confidence" in e for e in errors))

    def test_zero_confidence_is_valid(self):
        row = self._valid_row(confidence=0.0)
        errors = gs.validate_prediction_row(row)
        self.assertEqual(errors, [])

    def test_to_dict_round_trip(self):
        row = self._valid_row()
        d = row.to_dict()
        restored = gs.ModelPredictionRow.from_dict(d)
        self.assertEqual(restored.source, row.source)
        self.assertEqual(restored.predicted_class, row.predicted_class)
        self.assertAlmostEqual(restored.confidence, row.confidence)
        self.assertEqual(restored.context_used, row.context_used)

    def test_schema_version_default(self):
        row = self._valid_row()
        self.assertEqual(row.schema_version, gs.PREDICTIONS_SCHEMA_VERSION)


class TrainingExampleRowTests(unittest.TestCase):
    def _valid_row(self, **overrides) -> gs.TrainingExampleRow:
        base = dict(
            source="reddit",
            platform_event_id="r_post_888",
            text_for_model="this is a great product",
            feature_version="v1",
            label_horizon="T+24h",
            label_value="viral",
            dataset_version="2026-06-01",
            split_name="train",
            virality_policy="training_reference_quantile",
            virality_contract_fingerprint="a" * 64,
        )
        base.update(overrides)
        return gs.TrainingExampleRow(**base)

    def test_valid_row_passes_validation(self):
        row = self._valid_row()
        errors = gs.validate_training_example(row)
        self.assertEqual(errors, [], f"Unexpected errors: {errors}")

    def test_invalid_label_horizon_fails(self):
        row = self._valid_row(label_horizon="T+999h")
        errors = gs.validate_training_example(row)
        self.assertTrue(any("label_horizon" in e for e in errors))

    def test_valid_label_horizons(self):
        for horizon in gs.VALID_LABEL_HORIZONS:
            row = self._valid_row(label_horizon=horizon)
            errors = gs.validate_training_example(row)
            self.assertEqual(errors, [], f"horizon {horizon!r} should be valid")

    def test_empty_text_fails(self):
        row = self._valid_row(text_for_model="")
        errors = gs.validate_training_example(row)
        self.assertTrue(any("text_for_model" in e for e in errors))

    def test_context_feature_snapshot_is_optional(self):
        row = self._valid_row(context_feature_snapshot=None)
        errors = gs.validate_training_example(row)
        self.assertEqual(errors, [])

    def test_to_dict_round_trip(self):
        row = self._valid_row()
        d = row.to_dict()
        restored = gs.TrainingExampleRow.from_dict(d)
        self.assertEqual(restored.source, row.source)
        self.assertEqual(restored.label_horizon, row.label_horizon)
        self.assertEqual(restored.label_value, row.label_value)

    def test_schema_version_default(self):
        row = self._valid_row()
        self.assertEqual(row.schema_version, gs.TRAINING_EXAMPLES_SCHEMA_VERSION)


class GoldSchemaContractTests(unittest.TestCase):
    def test_predictions_ddl_contains_required_columns(self):
        required = {
            "source",
            "platform_event_id",
            "prediction_ts",
            "model_version",
            "model_type",
            "predicted_class",
            "confidence",
            "virality_score",
            "context_used",
            "schema_version",
            "prediction_date",
        }
        for col_name in required:
            self.assertIn(col_name, gs.MODEL_PREDICTIONS_DDL, f"Missing: {col_name}")

    def test_training_examples_ddl_contains_required_columns(self):
        required = {
            "source",
            "platform_event_id",
            "text_for_model",
            "feature_version",
            "label_horizon",
            "label_value",
            "dataset_version",
            "split_name",
            "virality_policy",
            "virality_contract_fingerprint",
            "context_feature_snapshot",
            "schema_version",
            "example_date",
        }
        for col_name in required:
            self.assertIn(col_name, gs.TRAINING_EXAMPLES_DDL, f"Missing: {col_name}")

    def test_gold_tables_use_gold_namespace(self):
        self.assertIn("lakehouse.gold.model_predictions", gs.MODEL_PREDICTIONS_DDL)
        self.assertIn("lakehouse.gold.training_examples", gs.TRAINING_EXAMPLES_DDL)

    def test_gold_tables_not_in_silver_namespace(self):
        self.assertNotIn("lakehouse.silver", gs.MODEL_PREDICTIONS_DDL)
        self.assertNotIn("lakehouse.silver", gs.TRAINING_EXAMPLES_DDL)

    def test_predictions_not_mixed_with_training(self):
        """Ensure the two Gold tables are fully separate."""
        self.assertNotIn("training_examples", gs.MODEL_PREDICTIONS_DDL)
        self.assertNotIn("model_predictions", gs.TRAINING_EXAMPLES_DDL)

    def test_monitoring_columns_not_in_gold(self):
        monitoring_only = {"error", "kafka_topic", "kafka_partition", "kafka_offset"}
        for col_name in monitoring_only:
            self.assertNotIn(col_name, gs.MODEL_PREDICTIONS_COLUMNS)
            self.assertNotIn(col_name, gs.TRAINING_EXAMPLES_COLUMNS)

    def test_predictions_column_list_consistent_with_ddl(self):
        for col_name in gs.MODEL_PREDICTIONS_COLUMNS:
            self.assertIn(col_name, gs.MODEL_PREDICTIONS_DDL)

    def test_training_examples_column_list_consistent_with_ddl(self):
        for col_name in gs.TRAINING_EXAMPLES_COLUMNS:
            self.assertIn(col_name, gs.TRAINING_EXAMPLES_DDL)

    def test_gold_contract_does_not_mention_generative_models(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "gold_schemas.py").read_text(
            encoding="utf-8"
        )
        for kw in ["generative model", "generate text", "llm response", "gpt"]:
            self.assertNotIn(kw, source.lower(), f"Unexpected keyword: {kw!r}")


if __name__ == "__main__":
    unittest.main()
