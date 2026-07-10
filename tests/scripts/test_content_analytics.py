import ast
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
BATCH_PATH = ROOT / "spark" / "jobs" / "batch"
sys.path.insert(0, str(BATCH_PATH))

import content_analytics as ca


class ContentAnalyticsContractTests(unittest.TestCase):
    def test_silver_contents_contract(self):
        for column in (
            "content_id",
            "source",
            "platform_content_id",
            "content_type",
            "author_id_hash",
            "subreddit",
            "subreddit_title",
            "subreddit_description",
            "subreddit_member_count",
            "subreddit_weekly_visitors",
            "subreddit_weekly_contributions",
            "youtube_channel_id",
            "text_for_model",
        ):
            with self.subTest(column=column):
                self.assertIn(column, ca.CONTENT_COLUMNS)
                self.assertIn(column, ca.CREATE_CONTENTS_SQL)

    def test_silver_interactions_contract(self):
        for column in (
            "interaction_id",
            "parent_content_id",
            "parent_interaction_id",
            "conversation_id",
            "interaction_type",
            "author_id_hash",
            "reply_count",
        ):
            with self.subTest(column=column):
                self.assertIn(column, ca.INTERACTION_COLUMNS)
                self.assertIn(column, ca.CREATE_INTERACTIONS_SQL)

    def test_silver_snapshots_extend_existing_table(self):
        for column in (
            "content_id",
            "snapshot_at",
            "view_count",
            "follower_count",
            "subscriber_count",
            "subreddit_member_count",
            "snapshot_date",
        ):
            with self.subTest(column=column):
                self.assertIn(column, ca.SNAPSHOT_COLUMNS)
                self.assertIn(column, ca.CREATE_SNAPSHOTS_SQL)

    def test_gold_contracts(self):
        for column in (
            "interaction_count",
            "unique_interacting_users",
            "latest_snapshot_at",
        ):
            with self.subTest(column=column):
                self.assertIn(column, ca.CONTENT_STATS_COLUMNS)
                self.assertIn(column, ca.CREATE_CONTENT_STATS_SQL)

        for column in (
            "user_id_hash",
            "contents_created",
            "interactions_created",
            "distinct_contents_touched",
            "question_count",
        ):
            with self.subTest(column=column):
                self.assertIn(column, ca.USER_EVOLUTION_COLUMNS)
                self.assertIn(column, ca.CREATE_USER_EVOLUTION_SQL)

    def test_nullable_source_columns_are_declared(self):
        for column in (
            "subreddit",
            "subreddit_title",
            "subreddit_description",
            "subreddit_created_at",
            "subreddit_visibility",
            "subreddit_weekly_visitors",
            "subreddit_weekly_contributions",
            "x_account",
            "language",
            "conversation_id",
            "parent_interaction_id",
            "transcript_text",
            "transcript_segments_json",
            "duration_seconds",
            "has_auto_captions",
        ):
            with self.subTest(column=column):
                self.assertIn(column, ca.OPTIONAL_EVENT_COLUMNS)

    def test_content_ids_use_root_conversation_before_event_id(self):
        source = (ROOT / "spark" / "jobs" / "batch" / "content_analytics.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('col("conversation_id")', source)
        self.assertIn('regexp_extract(col("url"), r"/comments/([^/]+)", 1)', source)
        self.assertIn('regexp_extract(col("url"), r"/status/(\\d+)", 1)', source)
        self.assertIn('regexp_extract(col("url"), r"[?&]v=([^&]+)", 1)', source)

    def test_reddit_contents_do_not_use_comment_text_as_post_content(self):
        source = (ROOT / "spark" / "jobs" / "batch" / "content_analytics.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('regexp_extract(col("url"), r"/r/([^/]+)", 1)', source)
        self.assertIn('"derived_subreddit"', source)
        self.assertIn('first("derived_subreddit", ignorenulls=True).alias("subreddit")', source)
        self.assertIn('regexp_extract(col("url"), r"/comments/[^/]+/([^/]+)", 1)', source)
        self.assertIn('"content_title"', source)
        self.assertIn('"content_text"', source)
        self.assertIn('when(col("source") == "reddit", lit(None).cast("string"))', source)


class ContentAnalyticsIntegrationTextTests(unittest.TestCase):
    def test_airflow_dag_runs_content_analytics(self):
        source = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse.py"
        ).read_text(encoding="utf-8")

        self.assertIn("build_content_analytics_command", source)
        self.assertIn("content_analytics.py", source)
        self.assertIn("update_content_analytics", source)

    def test_airflow_dag_backfills_youtube_transcripts(self):
        source = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse.py"
        ).read_text(encoding="utf-8")
        requirements = (ROOT / "spark" / "requirements.txt").read_text(
            encoding="utf-8"
        )

        self.assertIn("build_youtube_transcripts_command", source)
        self.assertIn("youtube_transcripts.py", source)
        self.assertIn("backfill_youtube_transcripts", source)
        self.assertIn("youtube-transcript-api==1.2.4", requirements)

    def test_dashboard_surfaces_content_explorer(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        for value in (
            "CONTENT_ANALYTICS_TABLES",
            "Content Explorer",
            "Reddit",
            "subreddit_weekly_visitors",
            "subreddit_weekly_contributions",
            "X",
            "YouTube",
            "Users",
            "enrich_content_rows",
            "enrich_reddit_community_from_snapshots",
            "engagement_snapshots",
            'str.extract(r"/r/([^/]+)"',
            "render_content_analytics()",
            "Transcript keyword",
        ):
            with self.subTest(value=value):
                self.assertIn(value, source)

    def test_dashboard_enriches_missing_reddit_member_column_from_snapshots(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
        module = ast.parse(source)
        function_node = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "enrich_reddit_community_from_snapshots"
        )
        namespace = {"pd": pd}
        exec(
            compile(
                ast.Module(body=[function_node], type_ignores=[]),
                str(ROOT / "dashboard" / "app.py"),
                "exec",
            ),
            namespace,
        )

        contents = pd.DataFrame({"content_id": ["abc"], "source": ["reddit"]})
        snapshots = pd.DataFrame(
            {
                "content_id": ["abc"],
                "source": ["reddit"],
                "subreddit_member_count": [123],
            }
        )

        result = namespace["enrich_reddit_community_from_snapshots"](
            contents,
            snapshots,
        )

        self.assertEqual(result.loc[0, "subreddit_member_count"], 123)
        self.assertNotIn("snapshot_subreddit_member_count", result.columns)


if __name__ == "__main__":
    unittest.main()
