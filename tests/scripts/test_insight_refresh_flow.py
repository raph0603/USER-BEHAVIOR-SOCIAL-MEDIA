import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class InsightRefreshDagContractTests(unittest.TestCase):
    def test_snapshots_are_connected_after_validation(self):
        source = (
            ROOT / "orchestrator" / "dags" / "refresh_recent_engagement_insights.py"
        ).read_text(encoding="utf-8")

        for task_id in (
            "export_recent_silver_targets",
            "refresh_youtube_insights",
            "validate_refresh_output",
            "append_engagement_snapshots",
            "merge_latest_engagement_values",
        ):
            self.assertIn(f'task_id="{task_id}"', source)
        self.assertIn(
            "validate_refresh_output >> [append_snapshots, apply_updates]",
            source,
        )


class InsightRefreshValidationTests(unittest.TestCase):
    def _validator(self):
        import importlib.util

        path = ROOT / "playwright" / "validate_insight_refresh.py"
        spec = importlib.util.spec_from_file_location("validate_insight_refresh", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_valid_file_is_counted(self):
        validator = self._validator()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "source": "youtube",
                        "platform_event_id": "video-1",
                        "metadata_refreshed_at": "2026-07-17T00:00:00+00:00",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            self.assertEqual(validator.validate_file(path, "youtube"), 1)

    def test_duplicate_observation_is_rejected(self):
        validator = self._validator()
        event = {
            "source": "youtube",
            "platform_event_id": "video-1",
            "metadata_refreshed_at": "2026-07-17T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "youtube.jsonl"
            path.write_text(
                json.dumps(event) + "\n" + json.dumps(event) + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "Duplicate observation"):
                validator.validate_file(path, "youtube")


if __name__ == "__main__":
    unittest.main()
