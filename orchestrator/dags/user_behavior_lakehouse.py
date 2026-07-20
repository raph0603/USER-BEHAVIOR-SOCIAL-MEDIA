from airflow import DAG

from lakehouse_dag_factory import build_lakehouse_dag


dag: DAG = build_lakehouse_dag(
    dag_id="user_behavior_lakehouse",
    schedule_environment_variable="LAKEHOUSE_SCHEDULE_MINUTES",
    schedule_default_minutes=0,
    quality_profile="standard",
    require_row_checks=True,
    tags=["collection", "clean", "lakehouse", "spark", "realtime"],
)
