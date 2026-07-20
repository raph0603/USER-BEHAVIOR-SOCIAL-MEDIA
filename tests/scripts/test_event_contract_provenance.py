import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path

try:
    from fastavro import parse_schema, schemaless_reader, schemaless_writer
except ModuleNotFoundError:
    parse_schema = schemaless_reader = schemaless_writer = None

from common.event_envelope import (
    COVERAGE_FIELDS,
    ENVELOPE_FIELDS,
    enrich_event_envelope,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "canonical_event_provenance.json"


def _load_event_contract():
    path = ROOT / "spark" / "jobs" / "event_contract.py"
    spec = importlib.util.spec_from_file_location("event_contract_provenance", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _fixture_event() -> tuple[dict, dict]:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    event = enrich_event_envelope(fixture["input"], **fixture["producer"])
    return event, fixture


class EventEnvelopeTests(unittest.TestCase):
    def test_identity_provenance_and_coverage_are_deterministic(self):
        first, fixture = _fixture_event()
        second, _ = _fixture_event()

        self.assertEqual(first, second)
        self.assertEqual(len(first["event_id"]), 64)
        self.assertEqual(len(first["observation_id"]), 64)
        self.assertEqual(len(first["payload_fingerprint"]), 64)
        self.assertTrue(first["view_count_available"])
        self.assertFalse(first["like_count_available"])
        self.assertTrue(first["metadata_available"])
        self.assertFalse(first["transcript_available"])
        self.assertFalse(first["comments_available"])

        coverage = json.loads(first["coverage_json"])
        for name, expected in fixture["expected_coverage"].items():
            self.assertIs(coverage[name], expected)
        provenance = json.loads(first["provenance_json"])
        self.assertEqual(provenance["producer_name"], "youtube_metrics_worker")
        self.assertEqual(provenance["api_endpoint"], "videos.list")
        self.assertNotIn("raw_source_payload", provenance)
        self.assertNotIn("opaque", first["provenance_json"])

    def test_same_observation_can_carry_distinct_event_types(self):
        first, fixture = _fixture_event()
        changed = dict(fixture["input"])
        changed["event_type"] = "youtube.transcript.requested"
        second = enrich_event_envelope(changed, **fixture["producer"])

        self.assertEqual(first["observation_id"], second["observation_id"])
        self.assertNotEqual(first["event_id"], second["event_id"])


class ContractParityTests(unittest.TestCase):
    def setUp(self):
        self.avro = json.loads(
            (ROOT / "schemas" / "playwright_event.avsc").read_text(encoding="utf-8")
        )
        self.contract = _load_event_contract()

    @staticmethod
    def _avro_type(field: dict) -> str:
        data_type = field["type"]
        if isinstance(data_type, list):
            data_type = next(item for item in data_type if item != "null")
        if isinstance(data_type, dict):
            if data_type.get("type") == "array" and data_type.get("items") == "string":
                return "array_string"
            raise AssertionError(f"Unsupported Avro field type: {data_type}")
        return {
            "string": "string",
            "long": "long",
            "int": "int",
            "double": "double",
            "boolean": "boolean",
        }[data_type]

    def test_avro_python_and_spark_fields_have_exact_parity(self):
        avro_types = {field["name"]: self._avro_type(field) for field in self.avro["fields"]}

        self.assertEqual(avro_types, self.contract.EVENT_FIELD_TYPES)
        self.assertEqual(tuple(avro_types), self.contract.EVENT_COLUMNS)

    def test_new_fields_are_additive_and_nullable(self):
        fields = {field["name"]: field for field in self.avro["fields"]}
        for name in ENVELOPE_FIELDS:
            with self.subTest(name=name):
                self.assertIn(name, fields)
                self.assertEqual(fields[name].get("default"), None)
                self.assertIn("null", fields[name]["type"])
        self.assertTrue(set(COVERAGE_FIELDS).issubset(fields))

    @unittest.skipUnless(parse_schema is not None, "fastavro is not installed")
    def test_fixture_roundtrips_from_collector_to_lakehouse_contract(self):
        event, _ = _fixture_event()
        record = {
            field["name"]: event.get(field["name"], field.get("default"))
            for field in self.avro["fields"]
        }
        buffer = io.BytesIO()
        parsed = parse_schema(self.avro)
        schemaless_writer(buffer, parsed, record)
        buffer.seek(0)
        decoded = schemaless_reader(buffer, parsed)
        bronze = {
            name: decoded.get(name)
            for name in self.contract.BRONZE_COLUMNS
            if name not in {"metadata_refreshed_at", "event_ts"}
        }

        self.assertEqual(bronze["event_id"], event["event_id"])
        self.assertEqual(bronze["observation_id"], event["observation_id"])
        self.assertEqual(bronze["producer_run_id"], "fixture-run-1")
        self.assertEqual(bronze["view_count"], 0)
        self.assertTrue(bronze["view_count_available"])
        self.assertIsNone(bronze["like_count"])
        self.assertFalse(bronze["like_count_available"])

    def test_analytics_dashboard_and_ml_consumers_keep_the_envelope(self):
        analytics = (ROOT / "spark" / "jobs" / "batch" / "content_analytics.py").read_text(
            encoding="utf-8"
        )
        dashboard = (ROOT / "dashboard" / "loaders.py").read_text(encoding="utf-8")
        ml_builder = (ROOT / "ml" / "preprocess" / "build_dataset.py").read_text(encoding="utf-8")

        for name in ("observation_id", "provenance_json", "coverage_json"):
            self.assertIn(f'"{name}"', analytics)
            self.assertIn(f'"{name}"', dashboard)
        self.assertIn("engagement_coverage", ml_builder)
        self.assertIn("raw.notna()", ml_builder)


class YouTubePipelineEventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(ROOT / "playwright"))
        from youtube_pipeline_events import pipeline_event

        cls.pipeline_event = staticmethod(pipeline_event)

    def test_worker_event_exposes_zero_without_inventing_unknown_values(self):
        event = self.pipeline_event(
            "youtube.engagement.snapshot",
            "video-1",
            collected_at=__import__("datetime").datetime.fromisoformat("2026-07-20T01:00:00+00:00"),
            view_count=0,
            like_count=None,
            metadata_status="success",
        )

        self.assertTrue(event["view_count_available"])
        self.assertFalse(event["like_count_available"])
        self.assertEqual(event["producer_name"], "youtube_metrics_worker")
        self.assertEqual(event["api_endpoint"], "videos.list")


if __name__ == "__main__":
    unittest.main()
