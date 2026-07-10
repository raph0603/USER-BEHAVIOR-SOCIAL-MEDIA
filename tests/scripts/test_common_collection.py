import hashlib
import json
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from common.collection import (
    ALL_STATUSES,
    ContentRelationship,
    OperationResult,
    STATUS_DISABLED,
    STATUS_FAILED,
    STATUS_NOT_AVAILABLE,
    STATUS_PARTIAL,
    STATUS_PENDING,
    STATUS_RATE_LIMITED,
    STATUS_SUCCESS,
    canonical_content_id,
    is_retryable_status,
    is_terminal_status,
    overall_status,
    safe_json_dumps,
    sanitize_error_message,
    sanitize_json_value,
)


class CollectionStatusTests(unittest.TestCase):
    def test_status_sets_have_explicit_retry_semantics(self):
        self.assertEqual(
            ALL_STATUSES,
            {
                STATUS_PENDING,
                STATUS_SUCCESS,
                STATUS_PARTIAL,
                STATUS_NOT_AVAILABLE,
                STATUS_DISABLED,
                STATUS_RATE_LIMITED,
                STATUS_FAILED,
            },
        )
        for status in (STATUS_SUCCESS, STATUS_NOT_AVAILABLE, STATUS_DISABLED):
            with self.subTest(status=status):
                self.assertTrue(is_terminal_status(status))
                self.assertFalse(is_retryable_status(status))
        for status in (
            STATUS_PENDING,
            STATUS_PARTIAL,
            STATUS_RATE_LIMITED,
            STATUS_FAILED,
        ):
            with self.subTest(status=status):
                self.assertFalse(is_terminal_status(status))
                self.assertTrue(is_retryable_status(status))

    def test_invalid_status_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported collection status"):
            OperationResult(status="unknown")

    def test_overall_status_preserves_partial_completion(self):
        success = OperationResult.success({"value": 1})
        unavailable = OperationResult.unavailable()

        self.assertEqual(overall_status([]), STATUS_PENDING)
        self.assertEqual(overall_status([success]), STATUS_SUCCESS)
        self.assertEqual(
            overall_status([success, unavailable]),
            STATUS_PARTIAL,
        )
        self.assertEqual(
            overall_status([STATUS_RATE_LIMITED, STATUS_FAILED]),
            STATUS_FAILED,
        )

    def test_result_serialization_and_event_fields_are_stable(self):
        started = datetime(2026, 7, 10, 12, 0, tzinfo=timezone(timedelta(hours=7)))
        completed = started + timedelta(seconds=2)
        result = OperationResult.partial(
            {"items": 3},
            error_code="page_limit_reached",
            error_message="  only   three pages  ",
            attempt_count=2,
            started_at=started,
            completed_at=completed,
        )

        self.assertTrue(result.is_retryable)
        self.assertFalse(result.is_terminal)
        self.assertEqual(
            result.to_dict(),
            {
                "status": STATUS_PARTIAL,
                "payload": {"items": 3},
                "error_code": "page_limit_reached",
                "error_message": "only three pages",
                "attempt_count": 2,
                "started_at": "2026-07-10T05:00:00Z",
                "completed_at": "2026-07-10T05:00:02Z",
            },
        )
        self.assertEqual(
            result.as_event_fields("Transcript Fetch"),
            {
                "transcript_fetch_status": STATUS_PARTIAL,
                "transcript_fetch_error_code": "page_limit_reached",
                "transcript_fetch_error_message": "only three pages",
                "transcript_fetch_attempt_count": 2,
                "transcript_fetch_last_attempt_at": "2026-07-10T05:00:02Z",
            },
        )

    def test_negative_attempt_count_and_empty_component_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "cannot be negative"):
            OperationResult.failed(attempt_count=-1)
        with self.assertRaisesRegex(ValueError, "component"):
            OperationResult.success({}).as_event_fields("---")


class SafeSerializationTests(unittest.TestCase):
    def test_nested_credentials_and_free_form_tokens_are_redacted(self):
        value = {
            "api_key": "top-secret",
            "nested": {
                "refreshToken": "refresh-secret",
                "message": (
                    "Authorization: Bearer abc.def "
                    "https://example.test/path?token=visible&item=2"
                ),
            },
        }

        sanitized = sanitize_json_value(value)

        self.assertEqual(sanitized["api_key"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["refreshToken"], "[REDACTED]")
        self.assertNotIn("abc.def", sanitized["nested"]["message"])
        self.assertNotIn("token=visible", sanitized["nested"]["message"])

    def test_json_conversion_handles_cycles_dates_bytes_and_non_finite_numbers(self):
        cycle = []
        cycle.append(cycle)
        value = {
            "cycle": cycle,
            "when": datetime(2026, 1, 2, 3, 4, 5),
            "bytes": b"hello",
            "nan": float("nan"),
            "set": {3, 1, 2},
        }

        sanitized = sanitize_json_value(value)

        self.assertEqual(sanitized["cycle"], ["[CYCLE]"])
        self.assertEqual(sanitized["when"], "2026-01-02T03:04:05Z")
        self.assertEqual(sanitized["bytes"], "hello")
        self.assertIsNone(sanitized["nan"])
        self.assertEqual(sanitized["set"], [1, 2, 3])

    def test_dataclasses_are_serialized_and_unknown_objects_are_not_expanded(self):
        @dataclass
        class PublicValue:
            name: str
            password: str

        class OpaqueValue:
            private_value = "must-not-leak"

        self.assertEqual(
            sanitize_json_value(PublicValue("record", "secret")),
            {"name": "record", "password": "[REDACTED]"},
        )
        self.assertEqual(sanitize_json_value(OpaqueValue()), "<OpaqueValue>")

    def test_safe_json_is_deterministic_and_valid(self):
        serialized = safe_json_dumps({"z": 1, "a": "café"})

        self.assertEqual(serialized, '{"a":"café","z":1}')
        self.assertEqual(json.loads(serialized), {"a": "café", "z": 1})

    def test_error_messages_are_bounded_and_redacted(self):
        message = sanitize_error_message(
            "Bearer private-token https://example.test/?api_key=private",
            max_length=60,
        )

        self.assertNotIn("private-token", message)
        self.assertNotIn("api_key=private", message)
        self.assertLessEqual(len(message), 60)


class ContentRelationshipTests(unittest.TestCase):
    def test_canonical_id_and_root_relationship_are_stable(self):
        expected = hashlib.sha256(b"youtube:video-1").hexdigest()

        self.assertEqual(canonical_content_id(" YouTube ", "video-1"), expected)
        root = ContentRelationship.root(
            source="youtube",
            platform_content_id="video-1",
            content_type="video",
        )
        self.assertEqual(root.content_id, expected)
        self.assertEqual(root.root_content_id, expected)
        self.assertEqual(root.conversation_id, "video-1")
        self.assertEqual(root.relation_type, "root")
        self.assertEqual(root.depth, 0)

    def test_child_relationship_preserves_parent_and_root(self):
        child = ContentRelationship.child(
            source="reddit",
            platform_content_id="comment-2",
            parent_content_id="parent-canonical-id",
            root_content_id="root-canonical-id",
            conversation_id="thread-1",
            content_type="comment",
            relation_type="reply",
            depth=2,
            position_in_thread=4,
        )

        self.assertEqual(child.parent_content_id, "parent-canonical-id")
        self.assertEqual(child.root_content_id, "root-canonical-id")
        self.assertEqual(child.depth, 2)
        self.assertEqual(child.position_in_thread, 4)

    def test_relationship_validation_rejects_impossible_root(self):
        with self.assertRaisesRegex(ValueError, "root content"):
            ContentRelationship(
                content_id="content",
                parent_content_id="parent",
                root_content_id="content",
                conversation_id="thread",
                content_type="post",
                relation_type="root",
                depth=0,
            )


if __name__ == "__main__":
    unittest.main()
