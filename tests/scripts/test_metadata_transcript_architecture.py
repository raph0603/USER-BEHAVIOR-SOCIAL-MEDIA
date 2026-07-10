import ast
import copy
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
from common.transcripts import TranscriptPayload


ROOT = Path(__file__).resolve().parents[2]
PRODUCER_PATH = ROOT / "playwright" / "producer.py"


def _producer_functions(*names):
    module = ast.parse(PRODUCER_PATH.read_text(encoding="utf-8"))
    return [
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names
    ]


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
        source = (
            ROOT / "spark" / "jobs" / "pipeline" / "collector_stream_pipeline.py"
        ).read_text(encoding="utf-8")

        self.assertIn("_registered_avro_schemas", source)
        self.assertIn('withColumn("_schema_id"', source)
        self.assertIn("for schema_id, writer_schema", source)
        self.assertIn("allowMissingColumns=True", source)
        self.assertIn("unregistered_writer_schema", source)
        self.assertIn("_decode_error", source)


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
            "isoformat_utc": isoformat_utc,
            "safe_json_dumps": safe_json_dumps,
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
        self.assertIn("detail_member_count,\n                        candidate_subreddit_info", source)


class YouTubeMetadataMappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = {
            "_completed_at",
            "_terminal_collection_status",
            "_first_operation_error",
            "_parse_youtube_duration_seconds",
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
        self.assertEqual(event["transcript_language_code"], "en")
        self.assertEqual(event["transcript_selection_strategy"], "manual_preferred")
        self.assertEqual(event["duration_seconds"], 90.0)


class PipelineHandoffTests(unittest.TestCase):
    def test_silver_handoff_occurs_after_the_bronze_merge(self):
        source = (
            ROOT / "spark" / "jobs" / "streaming" / "kafka_to_iceberg_bronze.py"
        ).read_text(encoding="utf-8")
        merge_index = source.index("MERGE INTO lakehouse.bronze.events")
        handoff_index = source.index('.write.format("kafka")', merge_index)

        self.assertGreater(handoff_index, merge_index)
        self.assertNotIn("kafka_payload.writeStream", source)
        self.assertNotIn("kafka_query.awaitTermination", source)

    def test_lakehouse_health_checks_are_scoped_to_the_current_run(self):
        check = (
            ROOT / "tests" / "spark" / "lakehouse" / "lakehouse_check.py"
        ).read_text(encoding="utf-8")
        dag = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse.py"
        ).read_text(encoding="utf-8")

        self.assertIn("since_timestamp", check)
        self.assertIn('col("collected_at")', check)
        self.assertIn("dag_run.start_date.isoformat()", dag)

    def test_content_analytics_cannot_downgrade_terminal_transcripts(self):
        source = (
            ROOT / "spark" / "jobs" / "batch" / "content_analytics.py"
        ).read_text(encoding="utf-8")

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
