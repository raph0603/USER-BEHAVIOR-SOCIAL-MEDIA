import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch


AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None
if AIRFLOW_AVAILABLE:
    from airflow.models import DagBag


ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_DAGS_DIR = ROOT / "orchestrator" / "dags"
DAGS_DIR = Path(
    os.getenv(
        "AIRFLOW_DAG_TEST_DIR",
        str(REPOSITORY_DAGS_DIR if REPOSITORY_DAGS_DIR.is_dir() else "/opt/airflow/dags"),
    )
)


@unittest.skipUnless(AIRFLOW_AVAILABLE, "Airflow runtime is required for DagBag validation")
class LakehouseDagBagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        environment = {
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
            "AIRFLOW_VAR_CRAWLER_DASHBOARD_CONFIG": "{}",
            "AIRFLOW_VAR_INSIGHT_DASHBOARD_CONFIG": "{}",
            "LAKEHOUSE_SCHEDULE_MINUTES": "0",
            "LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES": "60",
        }
        with (
            patch.dict(os.environ, environment),
            patch(
                "socket.create_connection",
                side_effect=AssertionError("DagBag import attempted network access"),
            ),
        ):
            cls.bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

    def test_dagbag_imports_without_errors_or_network(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertIn("user_behavior_lakehouse", self.bag.dags)
        self.assertIn("user_behavior_lakehouse_no_row_checks", self.bag.dags)

    def test_profiles_preserve_row_semantics_and_quality_checks(self):
        standard = self.bag.dags["user_behavior_lakehouse"]
        no_rows = self.bag.dags["user_behavior_lakehouse_no_row_checks"]
        self.assertIn(
            "lakehouse.bronze.events 1",
            standard.get_task("verify_bronze_rows").bash_command,
        )
        self.assertIn(
            "lakehouse.bronze.events 0",
            no_rows.get_task("verify_bronze_rows").bash_command,
        )
        self.assertIn(
            "--profile standard",
            standard.get_task("validate_lakehouse_quality").bash_command,
        )
        self.assertIn(
            "--profile no_row_checks",
            no_rows.get_task("validate_lakehouse_quality").bash_command,
        )

    def test_slow_youtube_workers_do_not_hold_the_core_lock(self):
        for dag_id in (
            "user_behavior_lakehouse",
            "user_behavior_lakehouse_no_row_checks",
        ):
            dag = self.bag.dags[dag_id]
            for task_id in (
                "process_youtube_transcript_requests",
                "process_youtube_comment_requests",
                "refresh_youtube_channel_statistics",
            ):
                downstream = {
                    task.task_id
                    for task in dag.get_task(task_id).get_flat_relatives(upstream=False)
                }
                self.assertNotIn("acquire_pipeline_lock", downstream)
                self.assertNotIn("merge_clean_events_to_bronze", downstream)
                self.assertIn("lakehouse_pipeline_complete", downstream)

    def test_reconciliation_and_quality_precede_analytics(self):
        for dag_id in (
            "user_behavior_lakehouse",
            "user_behavior_lakehouse_no_row_checks",
        ):
            dag = self.bag.dags[dag_id]
            for recovery_gate in (
                "verify_bronze_recovery_lock",
                "verify_silver_recovery_lock",
            ):
                self.assertEqual(dag.get_task(recovery_gate).trigger_rule, "all_done")
                command = dag.get_task(recovery_gate).bash_command.lower()
                self.assertIn("pipeline lock ownership check failed", command)
                self.assertIn("verified shared pipeline lock", command)
            self.assertIn(
                "verify_bronze_recovery_lock",
                dag.get_task("transmit_bronze_to_silver").upstream_task_ids,
            )
            self.assertIn(
                "verify_silver_recovery_lock",
                dag.get_task("reconcile_bronze_silver").upstream_task_ids,
            )
            self.assertTrue(
                {
                    "merge_clean_events_to_bronze",
                    "transmit_bronze_to_silver",
                }.issubset(dag.get_task("lakehouse_pipeline_complete").upstream_task_ids)
            )
            self.assertEqual(
                dag.get_task("validate_lakehouse_quality").upstream_task_ids,
                {"verify_bronze_rows", "verify_silver_rows"},
            )
            for verification_task in ("verify_bronze_rows", "verify_silver_rows"):
                self.assertIn(
                    "reconcile_bronze_silver",
                    dag.get_task(verification_task).upstream_task_ids,
                )
            downstream = {
                task.task_id
                for task in dag.get_task("validate_lakehouse_quality").get_flat_relatives(
                    upstream=False
                )
            }
            self.assertIn("update_content_analytics", downstream)

    def test_engagement_refresh_has_the_required_stage_order(self):
        dag = self.bag.dags["refresh_recent_engagement_insights"]
        expected_edges = (
            ("export_recent_silver_targets", "refresh_youtube_insights"),
            ("refresh_youtube_insights", "validate_refresh_output"),
            ("validate_refresh_output", "append_engagement_snapshots"),
            ("append_engagement_snapshots", "merge_latest_engagement_values"),
            ("merge_latest_engagement_values", "compute_youtube_velocity_and_virality"),
        )
        for upstream, downstream in expected_edges:
            with self.subTest(upstream=upstream, downstream=downstream):
                self.assertIn(upstream, dag.get_task(downstream).upstream_task_ids)


if __name__ == "__main__":
    unittest.main()
