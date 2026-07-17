import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class LakehouseScheduleDefaultsTests(unittest.TestCase):
    def test_no_row_checks_is_the_scheduled_pipeline(self):
        standard_dag = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse.py"
        ).read_text(encoding="utf-8")
        no_row_checks_dag = (
            ROOT / "orchestrator" / "dags" / "user_behavior_lakehouse_no_row_checks.py"
        ).read_text(encoding="utf-8")
        environment = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn('os.getenv("LAKEHOUSE_SCHEDULE_MINUTES", "0")', standard_dag)
        self.assertIn('"LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES",\n        "60"', no_row_checks_dag)
        self.assertIn("LAKEHOUSE_SCHEDULE_MINUTES=0", environment)
        self.assertIn("LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES=60", environment)
        self.assertIn("LAKEHOUSE_SCHEDULE_MINUTES: ${LAKEHOUSE_SCHEDULE_MINUTES:-0}", compose)
        self.assertIn(
            "LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES: "
            "${LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES:-60}",
            compose,
        )


if __name__ == "__main__":
    unittest.main()
