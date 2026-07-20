from airflow import DAG

from lakehouse_dag_factory import build_lakehouse_dag


dag: DAG = build_lakehouse_dag(
    dag_id="user_behavior_lakehouse_no_row_checks",
    schedule_environment_variable="LAKEHOUSE_NO_ROW_CHECKS_SCHEDULE_MINUTES",
    schedule_default_minutes=60,
    quality_profile="no_row_checks",
    require_row_checks=False,
    tags=[
        "collection",
        "clean",
        "lakehouse",
        "spark",
        "realtime",
        "no-row-checks",
    ],
)
