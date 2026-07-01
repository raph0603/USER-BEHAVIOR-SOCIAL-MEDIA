"""
Tests for spark/jobs/pipeline/context_features.py

Validates:
- ContextFeatureRow dataclass round-trips correctly
- validate_context_feature_row catches invalid rows
- DDL contains all required columns
- CONTEXT_FEATURE_COLUMNS is consistent with the DDL
- Contract is documented as retrieval-enhanced (not generative)
"""

import sys
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

PIPELINE_PATH = ROOT / "spark" / "jobs" / "pipeline"
sys.path.insert(0, str(PIPELINE_PATH))

import context_features as cf


class ContextFeatureRowTests(unittest.TestCase):
    def _valid_row(self, **overrides) -> cf.ContextFeatureRow:
        base = dict(
            source="x",
            platform_event_id="tweet_123",
            retrieved_at=datetime(2026, 6, 1, 12, 0, 0),
            top_similarity=0.87,
            avg_similarity_top10=0.71,
            recent_posts_1h=42,
            trend_growth_1h=0.15,
            trend_growth_24h=0.03,
            topic_freshness_hours=2.5,
            matched_topics=["electric vehicles", "battery"],
        )
        base.update(overrides)
        return cf.ContextFeatureRow(**base)

    def test_valid_row_passes_validation(self):
        row = self._valid_row()
        errors = cf.validate_context_feature_row(row)
        self.assertEqual(errors, [], f"Unexpected validation errors: {errors}")

    def test_missing_source_fails_validation(self):
        row = self._valid_row(source="")
        errors = cf.validate_context_feature_row(row)
        self.assertTrue(any("source" in e for e in errors))

    def test_missing_platform_event_id_fails_validation(self):
        row = self._valid_row(platform_event_id="")
        errors = cf.validate_context_feature_row(row)
        self.assertTrue(any("platform_event_id" in e for e in errors))

    def test_invalid_retrieved_at_fails_validation(self):
        row = self._valid_row(retrieved_at="not-a-datetime")
        errors = cf.validate_context_feature_row(row)
        self.assertTrue(any("retrieved_at" in e for e in errors))

    def test_negative_recent_posts_fails_validation(self):
        row = self._valid_row(recent_posts_1h=-1)
        errors = cf.validate_context_feature_row(row)
        self.assertTrue(any("recent_posts_1h" in e for e in errors))

    def test_none_optional_fields_are_valid(self):
        row = cf.ContextFeatureRow(
            source="reddit",
            platform_event_id="r_post_456",
            retrieved_at=datetime(2026, 6, 1, 12, 0, 0),
        )
        errors = cf.validate_context_feature_row(row)
        self.assertEqual(errors, [])

    def test_to_dict_round_trip(self):
        row = self._valid_row()
        d = row.to_dict()
        restored = cf.ContextFeatureRow.from_dict(d)
        self.assertEqual(restored.source, row.source)
        self.assertEqual(restored.platform_event_id, row.platform_event_id)
        self.assertAlmostEqual(restored.top_similarity, row.top_similarity)
        self.assertEqual(restored.matched_topics, row.matched_topics)

    def test_from_dict_handles_string_datetime(self):
        d = {
            "source": "youtube",
            "platform_event_id": "yt_789",
            "retrieved_at": "2026-06-01T12:00:00",
            "matched_topics": ["ai", "llm"],
        }
        row = cf.ContextFeatureRow.from_dict(d)
        self.assertIsInstance(row.retrieved_at, datetime)
        self.assertEqual(row.matched_topics, ["ai", "llm"])

    def test_matched_topics_defaults_to_empty_list(self):
        row = cf.ContextFeatureRow(
            source="x",
            platform_event_id="p1",
            retrieved_at=datetime(2026, 6, 1),
        )
        self.assertEqual(row.matched_topics, [])


class ContextFeatureSchemaContractTests(unittest.TestCase):
    def test_ddl_contains_all_required_columns(self):
        required = {
            "source",
            "platform_event_id",
            "retrieved_at",
            "top_similarity",
            "avg_similarity_top10",
            "recent_posts_1h",
            "trend_growth_1h",
            "trend_growth_24h",
            "topic_freshness_hours",
            "matched_topics",
            "retrieval_date",
        }
        for col_name in required:
            self.assertIn(col_name, cf.CREATE_TABLE_SQL, f"Missing column in DDL: {col_name}")

    def test_context_feature_columns_consistent_with_ddl(self):
        for col_name in cf.CONTEXT_FEATURE_COLUMNS:
            self.assertIn(
                col_name,
                cf.CREATE_TABLE_SQL,
                f"Column in CONTEXT_FEATURE_COLUMNS not in DDL: {col_name}",
            )

    def test_schema_version_is_set(self):
        self.assertIsInstance(cf.SCHEMA_VERSION, str)
        self.assertTrue(len(cf.SCHEMA_VERSION) > 0)

    def test_contract_documents_retrieval_not_generation(self):
        """
        The module docstring must explicitly state that this is
        retrieval-enhanced classification, not a generative flow.
        """
        module_source = (
            Path(__file__).resolve().parents[2]
            / "spark"
            / "jobs"
            / "pipeline"
            / "context_features.py"
        )
        content = module_source.read_text(encoding="utf-8")
        # Must mention retrieval
        self.assertIn("retrieval", content.lower())
        # Must not claim to generate text responses (LLM / generative response flow)
        # Note: "not a generative model" is acceptable — we only ban generative
        # *response* patterns that would imply a text-generation flow.
        self.assertNotIn("generate text", content.lower())
        self.assertNotIn("llm response", content.lower())
        # GPT references would imply a generative approach
        self.assertNotIn("gpt", content.lower())

    def test_partitioned_by_retrieval_date(self):
        self.assertIn("retrieval_date", cf.CREATE_TABLE_SQL)
        self.assertIn("PARTITIONED BY", cf.CREATE_TABLE_SQL)


if __name__ == "__main__":
    unittest.main()
