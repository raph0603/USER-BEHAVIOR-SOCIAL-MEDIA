import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark" / "jobs"))

from pipeline.reliability import (  # noqa: E402
    deterministic_event_id,
    fail_on_data_loss_option,
    guarded_test_fault_enabled,
    payload_fingerprint,
    protected_payload_envelope,
)


class EventIdentityTests(unittest.TestCase):
    def _event(self, **overrides):
        event = {
            "event_id": "video-1",
            "source": "youtube",
            "platform_event_id": "video-1",
            "user_id": "channel-1",
            "url": "https://youtube.test/watch?v=video-1",
            "timestamp": "2026-07-20T00:00:00+00:00",
            "collected_at": "2026-07-20T00:01:00+00:00",
            "event_type": "youtube_metadata",
            "event_version": "v1",
            "view_count": 10,
        }
        event.update(overrides)
        return event

    def test_event_identity_is_deterministic_for_the_same_payload(self):
        event = self._event()

        self.assertEqual(deterministic_event_id(event), deterministic_event_id(event))
        self.assertEqual(len(deterministic_event_id(event)), 64)

    def test_platform_identifier_is_not_mistaken_for_journal_identity(self):
        event_id = deterministic_event_id(self._event())

        self.assertNotEqual(event_id, "video-1")

    def test_distinct_observation_or_payload_gets_a_distinct_identity(self):
        first = deterministic_event_id(self._event())
        later = deterministic_event_id(
            self._event(
                collected_at="2026-07-20T00:11:00+00:00",
                view_count=11,
            )
        )

        self.assertNotEqual(first, later)

    def test_valid_precomputed_identity_is_preserved(self):
        supplied = "a" * 64

        self.assertEqual(deterministic_event_id(self._event(event_id=supplied)), supplied)

    def test_fingerprint_ignores_mapping_insertion_order(self):
        self.assertEqual(
            payload_fingerprint({"source": "x", "value": 1}),
            payload_fingerprint({"value": 1, "source": "x"}),
        )


class ProtectedDlqTests(unittest.TestCase):
    def test_protected_payload_never_contains_rejected_content(self):
        raw = '{"token":"secret-value","email":"person@example.test"}'
        envelope = protected_payload_envelope(raw)
        decoded = json.loads(envelope)

        self.assertTrue(decoded["redacted"])
        self.assertEqual(decoded["byte_length"], len(raw.encode("utf-8")))
        self.assertEqual(len(decoded["sha256"]), 64)
        self.assertNotIn("secret-value", envelope)
        self.assertNotIn("person@example.test", envelope)


class DataLossPolicyTests(unittest.TestCase):
    def test_fail_on_data_loss_defaults_to_true(self):
        self.assertEqual(fail_on_data_loss_option(None), "true")

    def test_disabling_data_loss_failure_requires_explicit_override(self):
        with self.assertRaisesRegex(ValueError, "ALLOW_KAFKA_DATA_LOSS"):
            fail_on_data_loss_option("false", allow_data_loss="false")

        self.assertEqual(
            fail_on_data_loss_option("false", allow_data_loss="true"),
            "false",
        )

    def test_invalid_boolean_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_FAIL_ON_DATA_LOSS"):
            fail_on_data_loss_option("sometimes")


class TestFaultPolicyTests(unittest.TestCase):
    def test_fault_is_disabled_by_default(self):
        self.assertFalse(
            guarded_test_fault_enabled(
                None,
                test_mode=None,
                fault_name="PIPELINE_TEST_FAIL_AFTER_BRONZE_COMMIT",
            )
        )

    def test_fault_requires_explicit_test_mode(self):
        with self.assertRaisesRegex(ValueError, "PIPELINE_TEST_MODE"):
            guarded_test_fault_enabled(
                "true",
                test_mode="false",
                fault_name="PIPELINE_TEST_FAIL_AFTER_BRONZE_COMMIT",
            )

        self.assertTrue(
            guarded_test_fault_enabled(
                "true",
                test_mode="true",
                fault_name="PIPELINE_TEST_FAIL_AFTER_BRONZE_COMMIT",
            )
        )


class PipelineStructureTests(unittest.TestCase):
    def test_bronze_journal_precedes_projection_and_handoff(self):
        source = (ROOT / "spark" / "jobs" / "streaming" / "kafka_to_iceberg_bronze.py").read_text(
            encoding="utf-8"
        )

        journal = source.index("table=EVENT_LOG_TABLE")
        projection = source.index("_merge_current_projection(committed")
        handoff = source.index("committed_after_projection.select")
        self.assertLess(journal, projection)
        self.assertLess(projection, handoff)
        fault = source.index("if fail_after_commit:", journal)
        self.assertLess(journal, fault)
        self.assertLess(fault, projection)
        self.assertIn("INGRESS_DLQ_TABLE", source)
        self.assertIn("protected_payload", source)

    def test_bronze_projection_aligns_legacy_temporal_columns(self):
        source = (ROOT / "spark" / "jobs" / "streaming" / "kafka_to_iceberg_bronze.py").read_text(
            encoding="utf-8"
        )

        align = source.index("def _align_projection_temporal_types")
        merge = source.index("def _merge_current_projection")
        create_view = source.index("projection.createOrReplaceTempView", merge)
        self.assertLess(align, merge)
        self.assertIn("_align_projection_temporal_types(_latest_projection(events))", source)
        self.assertLess(merge, create_view)
        self.assertIn("isinstance(field.dataType, TimestampType)", source)
        self.assertIn("isinstance(field.dataType, DateType)", source)

    def test_bronze_projection_combines_complementary_updates(self):
        source = (ROOT / "spark" / "jobs" / "streaming" / "kafka_to_iceberg_bronze.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("last(col(column), ignorenulls=True)", source)
        self.assertIn("Window.unboundedFollowing", source)

    def test_silver_projection_aligns_legacy_temporal_columns(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "silver_merge.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("def _align_current_temporal_types", source)
        self.assertIn(
            "_align_current_temporal_types(_latest_current_state(events))",
            source,
        )
        self.assertIn("isinstance(field.dataType, TimestampType)", source)
        self.assertIn("isinstance(field.dataType, DateType)", source)

    def test_silver_projection_combines_complementary_updates(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "silver_merge.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("last(col(column), ignorenulls=True)", source)
        self.assertIn("Window.unboundedFollowing", source)

    def test_silver_records_applied_id_only_after_state_merge(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "silver_merge.py").read_text(
            encoding="utf-8"
        )

        current = source.index("_merge_current_state(unapplied")
        applied = source.index("_record_applied_events(unapplied", current)
        self.assertLess(current, applied)

    def test_kafka_stages_have_no_unsafe_literal_default(self):
        paths = [
            ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py",
            ROOT / "spark" / "jobs" / "streaming" / "kafka_to_iceberg_bronze.py",
            ROOT / "spark" / "jobs" / "batch" / "bronze_to_silver_from_kafka.py",
        ]

        for path in paths:
            with self.subTest(path=path.name):
                source = path.read_text(encoding="utf-8")
                self.assertNotIn('.option("failOnDataLoss", "false")', source)
                self.assertIn("fail_on_data_loss_option", source)

    def test_event_id_is_additive_in_avro_and_spark_contracts(self):
        avro = json.loads((ROOT / "schemas" / "playwright_event.avsc").read_text(encoding="utf-8"))
        fields = {field["name"]: field for field in avro["fields"]}
        spark_contract = (ROOT / "spark" / "jobs" / "event_contract.py").read_text(encoding="utf-8")

        self.assertEqual(fields["event_id"]["default"], None)
        self.assertEqual(fields["payload_fingerprint"]["default"], None)
        self.assertIn('"event_id": "string"', spark_contract)
        self.assertIn('"payload_fingerprint": "string"', spark_contract)


if __name__ == "__main__":
    unittest.main()
