import ast
import copy
import contextlib
import importlib.util
import io
import json
import os
import sqlite3
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote

import numpy as np
import pandas as pd

try:
    from fastavro import parse_schema, schemaless_reader, schemaless_writer
except ModuleNotFoundError:
    parse_schema = schemaless_reader = schemaless_writer = None

from common.collection import (
    ContentRelationship,
    OperationResult,
    canonical_content_id,
    isoformat_utc,
    is_terminal_status,
    overall_status,
    safe_json_dumps,
    utc_now,
)
from common.event_envelope import enrich_event_envelope
from common.transcripts import (
    TranscriptPayload,
    legacy_transcript_status,
    transcript_lifecycle_status,
)


ROOT = Path(__file__).resolve().parents[2]
PRODUCER_PATH = ROOT / "playwright" / "producer.py"
COLLECTOR_PIPELINE_PATH = ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py"


def _producer_functions(*names):
    module = ast.parse(PRODUCER_PATH.read_text(encoding="utf-8"))
    return [
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names
    ]


def _collector_functions(*names):
    module = ast.parse(COLLECTOR_PIPELINE_PATH.read_text(encoding="utf-8"))
    return [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]


class ProducerEnvironmentParsingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        nodes = _producer_functions("_env_json_list")
        namespace = {"json": json, "os": os}
        exec(
            compile(ast.Module(body=nodes, type_ignores=[]), str(PRODUCER_PATH), "exec"),
            namespace,
        )
        cls.env_json_list = staticmethod(namespace["_env_json_list"])

    def test_env_json_list_accepts_double_encoded_arrays(self):
        previous = os.environ.get("TEST_JSON_LIST")
        os.environ["TEST_JSON_LIST"] = json.dumps(json.dumps(["electricvehicles"]))
        try:
            self.assertEqual(
                self.env_json_list("TEST_JSON_LIST", []),
                ["electricvehicles"],
            )
        finally:
            if previous is None:
                os.environ.pop("TEST_JSON_LIST", None)
            else:
                os.environ["TEST_JSON_LIST"] = previous

    def test_env_json_list_covers_youtube_and_x_query_names(self):
        previous_youtube = os.environ.get("YOUTUBE_SEARCH_QUERIES_JSON")
        previous_x = os.environ.get("X_SEARCH_QUERIES_JSON")
        os.environ["YOUTUBE_SEARCH_QUERIES_JSON"] = json.dumps(
            json.dumps(["electric vehicle review"])
        )
        os.environ["X_SEARCH_QUERIES_JSON"] = json.dumps(
            [{"value": '(Tesla OR "EV charging") lang:en'}]
        )
        try:
            self.assertEqual(
                self.env_json_list("YOUTUBE_SEARCH_QUERIES_JSON", []),
                ["electric vehicle review"],
            )
            self.assertEqual(
                self.env_json_list("X_SEARCH_QUERIES_JSON", []),
                ['(Tesla OR "EV charging") lang:en'],
            )
        finally:
            if previous_youtube is None:
                os.environ.pop("YOUTUBE_SEARCH_QUERIES_JSON", None)
            else:
                os.environ["YOUTUBE_SEARCH_QUERIES_JSON"] = previous_youtube
            if previous_x is None:
                os.environ.pop("X_SEARCH_QUERIES_JSON", None)
            else:
                os.environ["X_SEARCH_QUERIES_JSON"] = previous_x

    def test_env_json_list_accepts_named_objects(self):
        previous = os.environ.get("TEST_JSON_LIST")
        os.environ["TEST_JSON_LIST"] = json.dumps(
            [{"subreddit": "teslamotors"}, {"keyword": "EV charging"}]
        )
        try:
            self.assertEqual(
                self.env_json_list("TEST_JSON_LIST", []),
                ["teslamotors", "EV charging"],
            )
        finally:
            if previous is None:
                os.environ.pop("TEST_JSON_LIST", None)
            else:
                os.environ["TEST_JSON_LIST"] = previous


class RedditCommentExtractionTests(unittest.TestCase):
    def test_author_timeout_keeps_the_comment_with_anonymous_author(self):
        import re
        from urllib.parse import urljoin

        function = _producer_functions("_extract_reddit_comment_event")[0]

        class FakeTimeoutError(Exception):
            pass

        class FakeLogger:
            def debug(self, *args, **kwargs):
                pass

        class FakeLocator:
            def __init__(self, value="", timeout=False):
                self.value = value
                self.timeout = timeout
                self.first = self

            def count(self):
                return 1

            def inner_text(self, timeout):
                if self.timeout:
                    raise FakeTimeoutError("author disappeared")
                return self.value

            def get_attribute(self, name):
                return None

        class FakeComment:
            def get_attribute(self, name):
                return {"data-fullname": "t1_comment-1", "data-depth": "0"}.get(name)

            def locator(self, selector):
                if selector == ".usertext-body .md":
                    return FakeLocator("Useful comment")
                if selector == "a.author":
                    return FakeLocator(timeout=True)
                return FakeLocator()

        namespace = {
            "LOGGER": FakeLogger(),
            "PlaywrightTimeoutError": FakeTimeoutError,
            "PlaywrightError": FakeTimeoutError,
            "_clean_text": lambda value: value.strip(),
            "_hash_identity": lambda author, comment_id: f"{author}-{comment_id}",
            "_extract_reddit_score": lambda comment: None,
            "re": re,
            "urljoin": urljoin,
        }
        exec(
            compile(ast.Module(body=[function], type_ignores=[]), str(PRODUCER_PATH), "exec"),
            namespace,
        )

        event = namespace["_extract_reddit_comment_event"](
            FakeComment(), "https://www.reddit.com/r/ev/comments/post-1/title/"
        )

        self.assertEqual(event["user_id"], "reddit-anonymous-comment-1")
        self.assertEqual(event["title"], "Useful comment")


class SchemaRegistryLookupTests(unittest.TestCase):
    @staticmethod
    def _lookup_with_status(status: int):
        def urlopen(url, timeout):
            raise HTTPError(url, status, "registry error", None, None)

        namespace = {
            "HTTPError": HTTPError,
            "json": json,
            "quote": quote,
            "urlopen": urlopen,
        }
        exec(
            compile(
                ast.Module(
                    body=_collector_functions("_registered_avro_schemas"),
                    type_ignores=[],
                ),
                str(COLLECTOR_PIPELINE_PATH),
                "exec",
            ),
            namespace,
        )
        return namespace["_registered_avro_schemas"]

    def test_missing_optional_subject_does_not_abort_cleaning(self):
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            schemas = self._lookup_with_status(404)(
                "http://schema-registry:8081",
                "youtube.channel.results-value",
            )

        self.assertEqual(schemas, [])
        warning = json.loads(output.getvalue())
        self.assertEqual(warning["event"], "schema_registry_subject_missing")
        self.assertEqual(warning["subject"], "youtube.channel.results-value")

    def test_non_missing_registry_errors_still_fail_cleaning(self):
        with self.assertRaises(HTTPError) as raised:
            self._lookup_with_status(503)(
                "http://schema-registry:8081",
                "youtube.channel.results-value",
            )

        self.assertEqual(raised.exception.code, 503)


@unittest.skipUnless(parse_schema is not None, "fastavro is not installed")
class AvroCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.current = json.loads(
            (ROOT / "schemas" / "playwright_event.avsc").read_text(encoding="utf-8")
        )
        self.old = copy.deepcopy(self.current)
        last_original = next(
            index
            for index, field in enumerate(self.old["fields"])
            if field["name"] == "subreddit_member_count"
        )
        self.old["fields"] = self.old["fields"][: last_original + 1]
        self.old_record = {
            "user_id": "user-1",
            "url": "https://example.test/content/1",
            "title": "A title",
            "raw_text": "A body",
            "clean_text": None,
            "text_for_model": None,
            "timestamp": "2026-01-01T00:00:00Z",
            "source": "youtube",
            "error": None,
        }
        for field in self.old["fields"]:
            self.old_record.setdefault(field["name"], field.get("default"))

    def test_new_reader_supplies_defaults_for_old_writer(self):
        buffer = io.BytesIO()
        schemaless_writer(buffer, parse_schema(self.old), self.old_record)
        buffer.seek(0)
        decoded = schemaless_reader(
            buffer,
            parse_schema(self.old),
            parse_schema(self.current),
        )

        self.assertEqual(decoded["title"], "A title")
        self.assertIsNone(decoded["metadata_status"])
        self.assertIsNone(decoded["root_content_id"])

    def test_old_reader_ignores_additive_new_fields(self):
        current_record = dict(self.old_record)
        for field in self.current["fields"]:
            current_record.setdefault(field["name"], field.get("default"))
        current_record["metadata_status"] = "success"
        current_record["content_type"] = "youtube_video"

        buffer = io.BytesIO()
        schemaless_writer(buffer, parse_schema(self.current), current_record)
        buffer.seek(0)
        decoded = schemaless_reader(
            buffer,
            parse_schema(self.current),
            parse_schema(self.old),
        )

        self.assertEqual(decoded["source"], "youtube")
        self.assertNotIn("metadata_status", decoded)

    def test_cleaner_selects_the_registered_writer_schema_id(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("_registered_avro_schemas", source)
        self.assertIn('withColumn("_schema_id"', source)
        self.assertIn("for schema_id, writer_schema", source)
        decoder = source[source.index("def _decode_confluent_avro") : source.index("def main")]
        self.assertIn('withColumn("_decoded_json", decoded_json)', decoder)
        self.assertIn('from_json(col("_decoded_json"), spark_struct_type())', decoder)
        self.assertNotIn("unionByName", decoder)
        self.assertIn("unregistered_writer_schema", source)
        self.assertIn("_decode_error", source)

    def test_cleaner_selects_writer_schemas_per_kafka_topic(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("subjects_by_topic: Mapping[str, str]", source)
        self.assertIn('col("_kafka_topic") == lit(topic)', source)
        self.assertIn('source_topic.split(",")', source)
        self.assertIn('{topic: f"{topic}-value" for topic in source_topics}', source)

    def test_cleaner_accepts_component_results_without_content_text(self):
        source = (ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('endswith(".result")', source)
        self.assertIn(".when(~component_result, invalid_reason", source)

    def test_online_cleaning_rotates_affected_platform_checkpoints(self):
        cleaner = (
            ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py"
        ).read_text(encoding="utf-8")
        factory = (ROOT / "orchestrator" / "dags" / "lakehouse_dag_factory.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('_env("CLEAN_CHECKPOINT_VERSION", "pre_bronze_v5")', cleaner)
        self.assertIn(
            'checkpoint_variable = f"{platform.upper()}_CLEAN_CHECKPOINT_VERSION"',
            factory,
        )
        self.assertEqual(factory.count('checkpoint_default="pre_bronze_v7"'), 3)
        self.assertIn(
            'CLEAN_CHECKPOINT_VERSION="${{{checkpoint_variable}:-{checkpoint_default}}}"',
            factory,
        )


class ProcessedStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class_node = _producer_functions("ProcessedState")[0]
        namespace = {
            "Path": Path,
            "datetime": datetime,
            "timezone": timezone,
            "sqlite3": sqlite3,
            "is_terminal_status": is_terminal_status,
            "STATUS_PARTIAL": "partial",
        }
        exec(
            compile(
                ast.Module(body=[class_node], type_ignores=[]),
                str(PRODUCER_PATH),
                "exec",
            ),
            namespace,
        )
        cls.ProcessedState = namespace["ProcessedState"]

    def test_incomplete_records_remain_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            state = self.ProcessedState(str(Path(directory) / "state.sqlite"))
            state.mark_many(
                "youtube",
                [
                    {
                        "event_id": "video-1",
                        "collection_status": "partial",
                        "metadata_status": "success",
                        "transcript_status": "failed",
                        "comments_status": "success",
                        "attempt_count": 1,
                    }
                ],
            )
            self.assertFalse(state.contains("youtube", "video-1"))
            self.assertEqual(state.next_attempt_count("youtube", "video-1"), 2)

            state.mark_many(
                "youtube",
                [
                    {
                        "event_id": "video-1",
                        "collection_status": "success",
                        "metadata_status": "success",
                        "transcript_status": "not_available",
                        "comments_status": "success",
                        "attempt_count": 2,
                    }
                ],
            )
            self.assertTrue(state.contains("youtube", "video-1"))
            state.close()

    def test_legacy_rows_are_migrated_as_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            connection = sqlite3.connect(database)
            connection.execute(
                """
                CREATE TABLE processed_events (
                  source TEXT NOT NULL,
                  event_id TEXT NOT NULL,
                  processed_at TEXT NOT NULL,
                  PRIMARY KEY (source, event_id)
                )
                """
            )
            connection.execute(
                "INSERT INTO processed_events VALUES (?, ?, ?)",
                ("youtube", "old-video", "2025-01-01T00:00:00Z"),
            )
            connection.commit()
            connection.close()

            state = self.ProcessedState(str(database))
            self.assertFalse(state.contains("youtube", "old-video"))
            self.assertEqual(state.next_attempt_count("youtube", "old-video"), 1)
            state.close()


class CanonicalEventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        nodes = _producer_functions("_prepare_event")
        namespace = {
            "ContentRelationship": ContentRelationship,
            "STATUS_DISABLED": "disabled",
            "STATUS_SUCCESS": "success",
            "canonical_content_id": canonical_content_id,
            "enrich_event_envelope": enrich_event_envelope,
            "isoformat_utc": isoformat_utc,
            "safe_json_dumps": safe_json_dumps,
            "transcript_lifecycle_status": transcript_lifecycle_status,
            "utc_now": utc_now,
            "_env_str": lambda name, default: default,
        }
        exec(
            compile(ast.Module(body=nodes, type_ignores=[]), str(PRODUCER_PATH), "exec"),
            namespace,
        )
        cls.prepare = staticmethod(namespace["_prepare_event"])

    def test_x_root_is_not_fabricated_as_an_interaction(self):
        event = self.prepare(
            {
                "event_id": "123",
                "platform_event_id": "123",
                "source": "x",
                "user_id": "x-user",
                "url": "https://x.com/user/status/123",
                "title": "Root post",
                "timestamp": "2026-01-01T00:00:00Z",
                "conversation_id": "123",
            }
        )

        self.assertEqual(event["relation_type"], "root")
        self.assertEqual(event["content_type"], "x_post")
        self.assertEqual(event["content_id"], event["root_content_id"])
        self.assertIsNone(event["parent_content_id"])

    def test_reddit_comment_keeps_immediate_parent_and_root(self):
        event = self.prepare(
            {
                "event_id": "comment-2",
                "platform_event_id": "comment-2",
                "source": "reddit",
                "user_id": "reddit-user",
                "url": "https://www.reddit.com/r/ev/comments/post-1/title/comment-2/",
                "title": "A reply",
                "timestamp": "2026-01-01T00:00:00Z",
                "conversation_id": "post-1",
                "parent_interaction_id": "comment-1",
                "depth": 2,
            }
        )

        self.assertEqual(event["relation_type"], "reply")
        self.assertEqual(event["root_content_id"], canonical_content_id("reddit", "post-1"))
        self.assertEqual(
            event["parent_content_id"],
            canonical_content_id("reddit", "comment-1"),
        )
        self.assertNotEqual(event["content_id"], event["root_content_id"])

    def test_reddit_detail_pass_uses_candidate_community_metadata(self):
        source = PRODUCER_PATH.read_text(encoding="utf-8")
        self.assertIn("candidate_subreddit_info", source)
        self.assertIn(
            "detail_member_count,\n                        candidate_subreddit_info", source
        )


class YouTubeMetadataMappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = {
            "_completed_at",
            "_terminal_collection_status",
            "_first_operation_error",
            "_parse_youtube_duration_seconds",
            "_preferred_youtube_transcript_languages",
            "_search_youtube_video_ids",
            "_youtube_video_event",
        }
        nodes = _producer_functions(*names)
        namespace = {
            "ContentRelationship": ContentRelationship,
            "OperationResult": OperationResult,
            "STATUS_SUCCESS": "success",
            "canonical_content_id": canonical_content_id,
            "isoformat_utc": isoformat_utc,
            "overall_status": overall_status,
            "safe_json_dumps": safe_json_dumps,
            "legacy_transcript_status": legacy_transcript_status,
            "transcript_lifecycle_status": transcript_lifecycle_status,
            "utc_now": utc_now,
            "parse_count": lambda value: int(value) if value is not None else None,
            "youtube_authors": types.SimpleNamespace(SUBSCRIBER_COUNTS={}),
            "_env_str": lambda name, default: default,
            "re": __import__("re"),
        }
        exec(
            compile(ast.Module(body=nodes, type_ignores=[]), str(PRODUCER_PATH), "exec"),
            namespace,
        )
        cls.map_video = staticmethod(namespace["_youtube_video_event"])
        cls.search_video_ids = staticmethod(namespace["_search_youtube_video_ids"])

    def test_youtube_search_skips_malformed_items(self):
        class FakeList:
            def execute(self):
                return {
                    "items": [
                        {"id": {"videoId": "video-1"}},
                        {"id": "video-2"},
                        {"id": None},
                        "malformed",
                    ]
                }

        class FakeSearch:
            def list(self, **_kwargs):
                return FakeList()

        fake_youtube = types.SimpleNamespace(search=lambda: FakeSearch())

        self.assertEqual(
            self.search_video_ids(fake_youtube, "ev", 10, "en", "date"),
            ["video-1", "video-2"],
        )

    def test_youtube_collaborator_page_collection_is_opt_in(self):
        source = PRODUCER_PATH.read_text(encoding="utf-8")

        self.assertIn(
            '_env_bool("YOUTUBE_COLLABORATOR_COLLECTION_ENABLED", False)',
            source,
        )
        self.assertIn("YouTube collaborator page collection is disabled", source)

    def test_youtube_events_are_published_incrementally_with_a_deadline(self):
        source = PRODUCER_PATH.read_text(encoding="utf-8")

        self.assertIn("youtube_processing_deadline = time.monotonic()", source)
        self.assertIn("publish(video_events)", source)
        self.assertIn('if mode != "youtube":\n            publish(events)', source)

    def test_video_metadata_and_transcript_provenance_are_preserved(self):
        now = datetime(2026, 1, 2, tzinfo=timezone.utc)
        metadata = OperationResult.success(
            {
                "snippet": {
                    "title": "Video title",
                    "description": "Video description",
                    "publishedAt": "2026-01-01T12:00:00Z",
                    "channelId": "channel-1",
                    "channelTitle": "Channel",
                    "tags": ["ev"],
                    "thumbnails": {
                        "default": {"url": "https://img.youtube.com/vi/video-1/default.jpg"}
                    },
                },
                "statistics": {
                    "viewCount": "100",
                    "likeCount": "10",
                    "commentCount": "2",
                },
                "contentDetails": {"duration": "PT1M30S", "caption": "true"},
                "status": {"privacyStatus": "public"},
                "topicDetails": {"topicCategories": ["https://example.test/ev"]},
            },
            completed_at=now,
        )
        comments = OperationResult.success([], completed_at=now)
        transcript = OperationResult.success(
            TranscriptPayload(
                video_id="video-1",
                language="English",
                language_code="en",
                is_generated=False,
                is_translated=False,
                source_language="English",
                source_language_code="en",
                source="youtube_transcript_api",
                selection_strategy="manual_preferred",
                text="Transcript body",
                segments=({"text": "Transcript body", "start": 0.0, "duration": 1.0},),
                segment_count=1,
                word_count=2,
                available_languages=({"language": "English", "language_code": "en"},),
                covered_duration_seconds=1.0,
                collected_at=now,
            ),
            completed_at=now,
        )

        event = self.map_video(
            "video-1",
            metadata,
            comments,
            transcript,
            "channel-1",
            [],
            1,
        )

        self.assertEqual(event["timestamp"], "2026-01-01T12:00:00Z")
        self.assertEqual(event["raw_text"], "Video description")
        self.assertEqual(event["user_id"], "youtube-channel-channel-1")
        self.assertEqual(event["comment_count"], 2)
        self.assertEqual(
            event["thumbnail_url"],
            "https://img.youtube.com/vi/video-1/default.jpg",
        )
        self.assertEqual(event["transcript_language_code"], "en")
        self.assertEqual(event["transcript_lifecycle_status"], "available")
        self.assertEqual(event["transcript_status"], "success")
        self.assertEqual(event["transcript_requested_language_code"], "en")
        self.assertEqual(event["transcript_generation_type"], "manual")
        self.assertEqual(len(event["transcript_content_version"]), 64)
        self.assertEqual(event["transcript_selection_strategy"], "manual_preferred")
        self.assertEqual(event["duration_seconds"], 90.0)


class PipelineHandoffTests(unittest.TestCase):
    def test_silver_handoff_occurs_after_the_bronze_merge(self):
        source = (ROOT / "spark" / "jobs" / "streaming" / "kafka_to_iceberg_bronze.py").read_text(
            encoding="utf-8"
        )
        merge_index = source.index("MERGE INTO lakehouse.bronze.events")
        handoff_index = source.index('.write.format("kafka")', merge_index)

        self.assertGreater(handoff_index, merge_index)
        self.assertNotIn("kafka_payload.writeStream", source)
        self.assertNotIn("kafka_query.awaitTermination", source)

    def test_lakehouse_health_checks_are_scoped_to_the_current_run(self):
        check = (ROOT / "tests" / "spark" / "lakehouse" / "lakehouse_check.py").read_text(
            encoding="utf-8"
        )
        dag = (ROOT / "orchestrator" / "dags" / "lakehouse_dag_factory.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("since_timestamp", check)
        self.assertIn('col("collected_at")', check)
        self.assertIn("dag_run.start_date.isoformat()", dag)

    def test_content_analytics_cannot_downgrade_terminal_transcripts(self):
        source = (ROOT / "spark" / "jobs" / "batch" / "content_analytics.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("t.transcript_status IN ('success', 'not_available', 'disabled')", source)
        self.assertIn("t.attempt_count = GREATEST", source)


class MlLabelCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = ROOT / "ml" / "preprocess" / "build_dataset.py"
        module = ast.parse(path.read_text(encoding="utf-8"))
        function = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == "add_viral_label"
        )
        namespace = {
            "ENGAGEMENT_METRICS": {
                "youtube": ["view_count", "like_count", "comment_count"],
                "x": ["like_count", "view_count"],
                "reddit": ["score", "comment_count"],
            },
            "VIRAL_QUANTILE": 0.75,
            "np": np,
            "pd": pd,
        }
        exec(
            compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"),
            namespace,
        )
        cls.add_label = staticmethod(namespace["add_viral_label"])

    def test_missing_engagement_does_not_become_a_zero_label(self):
        frame = pd.DataFrame(
            {
                "source": ["youtube", "youtube", "youtube"],
                "view_count": [None, 0, 100],
                "like_count": [None, 0, 10],
                "comment_count": [None, 0, 2],
            }
        )

        labeled = self.add_label(frame, 0.5)

        self.assertTrue(pd.isna(labeled.loc[0, "viral"]))
        self.assertEqual(labeled.loc[0, "engagement_observed_metrics"], 0)
        self.assertFalse(pd.isna(labeled.loc[1, "viral"]))
        self.assertEqual(labeled.loc[1, "engagement_coverage"], 1.0)

    def test_training_rejects_missing_or_stale_exports(self):
        path = ROOT / "ml" / "run_pipeline.py"
        spec = importlib.util.spec_from_file_location("run_pipeline_check", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as directory:
            export = Path(directory) / "filtered_events.csv"
            with self.assertRaises(FileNotFoundError):
                module.validate_training_input(
                    export,
                    max_age_hours=24,
                    allow_stale=False,
                )

            export.write_text("source,text\nyoutube,example\n", encoding="utf-8")
            old_timestamp = datetime.now(timezone.utc).timestamp() - (48 * 3600)
            os.utime(export, (old_timestamp, old_timestamp))
            with self.assertRaises(RuntimeError):
                module.validate_training_input(
                    export,
                    max_age_hours=24,
                    allow_stale=False,
                )
            module.validate_training_input(
                export,
                max_age_hours=24,
                allow_stale=True,
            )


if __name__ == "__main__":
    unittest.main()
