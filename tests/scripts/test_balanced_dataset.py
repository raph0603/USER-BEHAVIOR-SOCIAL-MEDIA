import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class BalancedDatasetTests(unittest.TestCase):
    def test_balancing_sources_are_syntactically_valid(self):
        paths = [
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "build_balanced_dataset.py",
            ROOT
            / "orchestrator"
            / "dags"
            / "build_balanced_comment_dataset.py",
            ROOT
            / "orchestrator"
            / "dags"
            / "user_behavior_lakehouse.py",
            ROOT
            / "orchestrator"
            / "dags"
            / "user_behavior_lakehouse_no_row_checks.py",
        ]
        for path in paths:
            with self.subTest(path=path):
                ast.parse(path.read_text(encoding="utf-8"))

    def test_balancing_job_is_reproducible_and_reports_distributions(self):
        source = (
            ROOT
            / "spark"
            / "jobs"
            / "maintenance"
            / "build_balanced_dataset.py"
        ).read_text(encoding="utf-8")

        self.assertIn("BALANCE_SEED", source)
        self.assertIn("BALANCE_DIMENSIONS", source)
        self.assertIn('DEFAULT_DIMENSIONS = ("source",)', source)
        self.assertIn("BALANCE_TARGET_PER_GROUP", source)
        self.assertIn("sha2(", source)
        self.assertIn("row_number().over", source)
        self.assertIn("distribution_before", source)
        self.assertIn("distribution_after", source)
        self.assertIn("lakehouse.silver.balanced_events", source)

    def test_balancing_dag_runs_spark_job_with_pipeline_lock(self):
        source = (
            ROOT
            / "orchestrator"
            / "dags"
            / "build_balanced_comment_dataset.py"
        ).read_text(encoding="utf-8")

        self.assertIn('dag_id="build_balanced_comment_dataset"', source)
        self.assertIn('"source"', source)
        self.assertIn('BALANCE_DATASET_SCHEDULE_MINUTES", "1440"', source)
        self.assertIn("acquire_pipeline_lock_command", source)
        self.assertIn("release_pipeline_lock_command", source)
        self.assertIn("build_balanced_dataset.py", source)

    def test_crawl_dags_refresh_balancing_report_after_silver(self):
        for dag_name in (
            "user_behavior_lakehouse.py",
            "user_behavior_lakehouse_no_row_checks.py",
        ):
            source = (
                ROOT / "orchestrator" / "dags" / dag_name
            ).read_text(encoding="utf-8")
            with self.subTest(dag=dag_name):
                self.assertIn('task_id="update_balancing_report"', source)
                self.assertIn("build_balancing_report_command()", source)
                self.assertIn("BALANCE_DIMENSIONS", source)
                self.assertIn("build_balanced_dataset.py", source)
                self.assertIn("update_balancing_report,", source)
                self.assertIn("] >> stop_realtime_streams", source)

    def test_crawl_dags_retry_x_and_reddit_instead_of_skipping(self):
        for dag_name in (
            "user_behavior_lakehouse.py",
            "user_behavior_lakehouse_no_row_checks.py",
        ):
            source = (
                ROOT / "orchestrator" / "dags" / dag_name
            ).read_text(encoding="utf-8")
            with self.subTest(dag=dag_name):
                self.assertIn('task_id="collect_x_playwright_events"', source)
                self.assertIn("-e X_FAIL_ON_ERROR=true ", source)
                self.assertIn('task_id="collect_reddit_online_events"', source)
                self.assertNotIn("skip_on_exit_code=99", source)
                self.assertNotIn("skipping X collection", source)
                self.assertIn(
                    "trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS",
                    source,
                )


if __name__ == "__main__":
    unittest.main()
