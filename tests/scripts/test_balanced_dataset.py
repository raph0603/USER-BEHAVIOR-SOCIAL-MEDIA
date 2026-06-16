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
        self.assertIn("BALANCE_DATASET_SCHEDULE_MINUTES", source)
        self.assertIn("acquire_pipeline_lock_command", source)
        self.assertIn("release_pipeline_lock_command", source)
        self.assertIn("build_balanced_dataset.py", source)


if __name__ == "__main__":
    unittest.main()
