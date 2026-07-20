import importlib.util
import os
import sys
import types
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = ROOT / "orchestrator" / "dags"
FACTORY_PATH = DAGS_DIR / "lakehouse_dag_factory.py"


def _load_factory_module():
    airflow = types.ModuleType("airflow")
    airflow.DAG = object
    airflow_models = types.ModuleType("airflow.models")
    airflow_models_param = types.ModuleType("airflow.models.param")
    airflow_models_param.Param = object
    airflow_operators = types.ModuleType("airflow.operators")
    airflow_operators_bash = types.ModuleType("airflow.operators.bash")
    airflow_operators_bash.BashOperator = object
    airflow_operators_empty = types.ModuleType("airflow.operators.empty")
    airflow_operators_empty.EmptyOperator = object
    airflow_utils = types.ModuleType("airflow.utils")
    airflow_trigger = types.ModuleType("airflow.utils.trigger_rule")
    airflow_trigger.TriggerRule = object
    crawler_configuration = types.ModuleType("crawler_configuration")
    crawler_configuration.load_crawler_config = lambda: {}
    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = object
    pipeline_lock = types.ModuleType("pipeline_lock")
    pipeline_lock.acquire_pipeline_lock_command = lambda: "acquire"
    pipeline_lock.release_pipeline_lock_command = lambda: "release"
    pipeline_lock.verify_pipeline_lock_command = lambda: "verify"

    modules = {
        "airflow": airflow,
        "airflow.models": airflow_models,
        "airflow.models.param": airflow_models_param,
        "airflow.operators": airflow_operators,
        "airflow.operators.bash": airflow_operators_bash,
        "airflow.operators.empty": airflow_operators_empty,
        "airflow.utils": airflow_utils,
        "airflow.utils.trigger_rule": airflow_trigger,
        "crawler_configuration": crawler_configuration,
        "pendulum": pendulum,
        "pipeline_lock": pipeline_lock,
    }
    spec = importlib.util.spec_from_file_location("lakehouse_dag_factory_test", FACTORY_PATH)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


FACTORY = _load_factory_module()


class LakehouseDagFactoryTests(unittest.TestCase):
    def test_schedule_uses_the_selected_environment_variable(self):
        with patch.dict(os.environ, {"PIPELINE_TEST_SCHEDULE": "17"}):
            self.assertEqual(
                FACTORY.schedule_interval("PIPELINE_TEST_SCHEDULE", 0),
                timedelta(minutes=17),
            )
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(FACTORY.schedule_interval("PIPELINE_TEST_SCHEDULE", 0))

    def test_quality_and_row_check_commands_are_profile_explicit(self):
        standard = FACTORY.build_quality_command("standard")
        no_rows = FACTORY.build_quality_command("no_row_checks")
        self.assertIn("--profile standard", standard)
        self.assertIn("--profile no_row_checks", no_rows)
        self.assertIn("lakehouse_quality.py", standard)
        self.assertIn("__quality", standard)
        self.assertIn(
            "lakehouse.bronze.events 0",
            FACTORY.build_row_check_command("lakehouse.bronze.events", 0),
        )
        self.assertIn(
            "lakehouse.silver.events 1",
            FACTORY.build_row_check_command("lakehouse.silver.events", 1),
        )
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            FACTORY.build_quality_command("disabled")

    def test_factory_rejects_a_profile_row_policy_mismatch(self):
        with self.assertRaisesRegex(ValueError, "Row-check policy"):
            FACTORY.build_lakehouse_dag(
                dag_id="invalid",
                schedule_environment_variable="PIPELINE_TEST_SCHEDULE",
                schedule_default_minutes=0,
                quality_profile="no_row_checks",
                require_row_checks=True,
                tags=[],
            )

    def test_wrappers_keep_both_dag_ids_through_the_factory(self):
        standard = (DAGS_DIR / "user_behavior_lakehouse.py").read_text(encoding="utf-8")
        no_rows = (DAGS_DIR / "user_behavior_lakehouse_no_row_checks.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('dag_id="user_behavior_lakehouse"', standard)
        self.assertIn('quality_profile="standard"', standard)
        self.assertIn('dag_id="user_behavior_lakehouse_no_row_checks"', no_rows)
        self.assertIn('quality_profile="no_row_checks"', no_rows)
        for wrapper in (standard, no_rows):
            self.assertIn("from airflow import DAG", wrapper)
            self.assertIn("dag: DAG = build_lakehouse_dag(", wrapper)

    def test_slow_workers_are_outside_the_locked_core_path(self):
        source = FACTORY_PATH.read_text(encoding="utf-8")
        core_dependency = """[
            run_youtube_metadata,
            run_x_collection,
            run_reddit_collection,
        ] >> acquire_pipeline_lock"""
        self.assertIn(core_dependency, source)
        self.assertNotIn(
            "run_youtube_transcripts,\n            run_youtube_comments,\n"
            "            run_youtube_channels,\n        ] >> acquire_pipeline_lock",
            source,
        )
        self.assertIn(
            "start_bronze_stream >> verify_bronze_recovery_lock >> start_silver_stream",
            source,
        )
        self.assertIn(
            "start_silver_stream >> verify_silver_recovery_lock >> reconcile_bronze_silver",
            source,
        )
        self.assertIn(
            "reconcile_bronze_silver >> [wait_bronze, wait_silver]",
            source,
        )
        self.assertIn(
            "[wait_bronze, wait_silver] >> validate_lakehouse_quality",
            source,
        )


if __name__ == "__main__":
    unittest.main()
