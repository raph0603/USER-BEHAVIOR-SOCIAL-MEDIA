import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PIPELINE_DIR = ROOT / "spark" / "jobs" / "pipeline"
sys.path.insert(0, str(PIPELINE_DIR))

import quality_rules


RULES_PATH = ROOT / "spark" / "jobs" / "config" / "lakehouse_quality_rules.json"
QUALITY_JOB = ROOT / "spark" / "jobs" / "maintenance" / "lakehouse_quality.py"


class QualityConfigurationTests(unittest.TestCase):
    def test_default_rules_cover_every_required_integrity_dimension(self):
        config = quality_rules.load_quality_config(
            RULES_PATH,
            profile_name="standard",
        )

        self.assertEqual(
            {rule.kind for rule in config.rules},
            quality_rules.RULE_KINDS,
        )
        self.assertEqual(
            {rule.severity for rule in config.rules},
            {"info", "warning", "error"},
        )
        self.assertEqual(config.profile.fail_severities, frozenset({"error"}))

    def test_no_row_profile_keeps_rules_and_emits_non_fatal_anomaly(self):
        standard = quality_rules.load_quality_config(RULES_PATH, profile_name="standard")
        no_rows = quality_rules.load_quality_config(
            RULES_PATH,
            profile_name="no_row_checks",
        )

        self.assertEqual(
            [rule.rule_id for rule in standard.rules],
            [rule.rule_id for rule in no_rows.rules],
        )
        self.assertEqual(
            quality_rules.empty_outcome(no_rows.profile),
            ("anomaly", "warning"),
        )
        self.assertFalse(quality_rules.result_causes_failure("anomaly", "warning", no_rows.profile))
        self.assertEqual(
            quality_rules.empty_outcome(standard.profile),
            ("failed", "error"),
        )

        reporting_profile = quality_rules.QualityProfile(
            name="reporting",
            allow_empty=False,
            empty_severity="warning",
            fail_severities=frozenset({"error"}),
        )
        self.assertEqual(
            quality_rules.empty_outcome(reporting_profile),
            ("failed", "warning"),
        )

    def test_threshold_overrides_cannot_retarget_a_rule(self):
        config = quality_rules.load_quality_config(
            RULES_PATH,
            profile_name="standard",
            threshold_overrides={"bronze_event_log_freshness": {"max_age_minutes": 30}},
        )
        rule = next(rule for rule in config.rules if rule.rule_id == "bronze_event_log_freshness")
        self.assertEqual(rule.options["max_age_minutes"], 30.0)

        with self.assertRaisesRegex(ValueError, "non-threshold"):
            quality_rules.load_quality_config(
                RULES_PATH,
                profile_name="standard",
                threshold_overrides={
                    "bronze_event_log_freshness": {"table": "lakehouse.silver.events"}
                },
            )

    def test_invalid_severity_and_duplicate_ids_are_rejected(self):
        document = json.loads(RULES_PATH.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rules.json"
            document["rules"][0]["severity"] = "critical"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be one of"):
                quality_rules.load_quality_config(path, profile_name="standard")

            document["rules"][0]["severity"] = "error"
            document["rules"].append(dict(document["rules"][0]))
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                quality_rules.load_quality_config(path, profile_name="standard")

    def test_inline_and_file_threshold_overrides_are_equivalent(self):
        payload = {"bronze_event_log_not_empty": {"min_rows": 10}}
        inline = quality_rules.parse_threshold_overrides(json.dumps(payload))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "overrides.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            from_file = quality_rules.parse_threshold_overrides(str(path))

        self.assertEqual(inline, payload)
        self.assertEqual(from_file, payload)


class OrphanDetectionTests(unittest.TestCase):
    def test_object_schemes_are_normalized_before_comparison(self):
        objects = {
            "s3a://lakehouse/warehouse/bronze/event_log/data/a.parquet",
            "s3a://lakehouse/warehouse/bronze/event_log/data/orphan.parquet",
        }
        referenced = {
            "s3://lakehouse/warehouse/bronze/event_log/data/a.parquet",
        }

        self.assertEqual(
            quality_rules.find_orphan_files(objects, referenced),
            ["lakehouse/warehouse/bronze/event_log/data/orphan.parquet"],
        )

    def test_result_identity_is_stable_per_run_profile_and_rule(self):
        first = quality_rules.stable_result_id(
            "scheduled__2026-07-20",
            "standard",
            "bronze_event_log_schema",
        )
        replay = quality_rules.stable_result_id(
            "scheduled__2026-07-20",
            "standard",
            "bronze_event_log_schema",
        )

        self.assertEqual(first, replay)
        self.assertEqual(len(first), 64)
        self.assertNotEqual(
            first,
            quality_rules.stable_result_id(
                "scheduled__2026-07-21",
                "standard",
                "bronze_event_log_schema",
            ),
        )

    def test_quality_job_lists_references_without_deleting_objects(self):
        source = QUALITY_JOB.read_text(encoding="utf-8")

        self.assertIn("lakehouse.monitoring.data_quality_results", source)
        self.assertIn(".all_data_files", source)
        self.assertIn(".all_delete_files", source)
        self.assertIn("filesystem.listFiles(root, True)", source)
        self.assertIn('"deletion_performed": False', source)
        self.assertNotIn("filesystem.delete(", source)
        self.assertNotIn("WHEN MATCHED THEN DELETE", source)
        self.assertIn("WHEN NOT MATCHED THEN", source)
        migration = (
            ROOT / "spark" / "jobs" / "maintenance" / "migrate_pipeline_reliability.py"
        ).read_text(encoding="utf-8")
        self.assertIn("ensure_quality_results_table", migration)
        self.assertIn('"--profile"', source)
        self.assertIn('"--threshold-overrides"', source)
        self.assertIn('"--fail-on"', source)


if __name__ == "__main__":
    unittest.main()
