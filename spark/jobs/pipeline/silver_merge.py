"""Shared replay-safe application of Bronze journal events to Silver."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    lit,
    row_number,
    sha2,
    to_date,
    to_timestamp,
)
from pyspark.storagelevel import StorageLevel

from event_contract import ICEBERG_TYPES, SILVER_COLUMNS, create_table_columns, merge_assignment


SILVER_TABLE = "lakehouse.silver.events"
APPLIED_EVENTS_TABLE = "lakehouse.silver.applied_events"
APPLIED_EVENT_COLUMNS = (
    "event_id",
    "source",
    "platform_event_id",
    "event_date",
    "payload_fingerprint",
    "applied_at",
    "silver_epoch_id",
    "silver_run_id",
)


@dataclass(frozen=True)
class ApplyResult:
    input_events: int
    already_applied: int
    newly_applied: int
    current_rows_merged: int


def _ensure_columns(spark: SparkSession, table: str, columns: dict[str, str]) -> None:
    current_columns = set(spark.table(table).columns)
    for name, data_type in columns.items():
        if name not in current_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")


def ensure_silver_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
          {create_table_columns(SILVER_COLUMNS)}
        )
        USING iceberg
        PARTITIONED BY (event_date)
        """
    )
    _ensure_columns(
        spark,
        SILVER_TABLE,
        {column: ICEBERG_TYPES[column] for column in SILVER_COLUMNS},
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {APPLIED_EVENTS_TABLE} (
          event_id STRING,
          source STRING,
          platform_event_id STRING,
          event_date DATE,
          payload_fingerprint STRING,
          applied_at TIMESTAMP,
          silver_epoch_id BIGINT,
          silver_run_id STRING
        )
        USING iceberg
        PARTITIONED BY (days(applied_at))
        """
    )
    _ensure_columns(
        spark,
        APPLIED_EVENTS_TABLE,
        {
            "event_id": "STRING",
            "source": "STRING",
            "platform_event_id": "STRING",
            "event_date": "DATE",
            "payload_fingerprint": "STRING",
            "applied_at": "TIMESTAMP",
            "silver_epoch_id": "BIGINT",
            "silver_run_id": "STRING",
        },
    )


def prepare_silver_events(events: DataFrame) -> DataFrame:
    """Normalize timestamps and retain the complete canonical Silver contract."""

    return (
        events.withColumn(
            "event_ts",
            coalesce(
                to_timestamp(col("published_at")),
                to_timestamp(col("event_ts")),
                to_timestamp(col("timestamp")),
            ),
        )
        .withColumn("event_date", to_date(col("event_ts")))
        .select(*SILVER_COLUMNS)
    )


def _latest_current_state(events: DataFrame) -> DataFrame:
    business_id = coalesce(
        col("platform_event_id"),
        sha2(concat_ws("\u001f", col("user_id"), col("url"), col("event_ts")), 256),
    )
    observed = coalesce(
        to_timestamp(col("updated_at")),
        to_timestamp(col("collected_at")),
        to_timestamp(col("timestamp")),
        col("event_ts"),
    )
    window = Window.partitionBy("source", "_business_id").orderBy(
        observed.desc_nulls_last(), col("event_id").desc()
    )
    return (
        events.withColumn("_business_id", business_id)
        .withColumn("_current_rank", row_number().over(window))
        .filter(col("_current_rank") == 1)
        .drop("_business_id", "_current_rank")
    )


def _merge_current_state(events: DataFrame, *, epoch_id: int) -> int:
    current = _latest_current_state(events).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        row_count = current.count()
        if row_count == 0:
            return 0
        view_name = f"silver_current_{abs(int(epoch_id))}"
        current.createOrReplaceTempView(view_name)
        columns = list(SILVER_COLUMNS)
        assignments = ",\n              ".join(
            merge_assignment(column)
            for column in columns
            if column not in {"source", "platform_event_id"}
        )
        rendered_columns = ", ".join(columns)
        current.sparkSession.sql(
            f"""
            MERGE INTO {SILVER_TABLE} AS t
            USING {view_name} AS s
            ON t.source = s.source
               AND (
                 (s.platform_event_id IS NOT NULL
                  AND t.platform_event_id = s.platform_event_id)
                 OR
                 (s.platform_event_id IS NULL
                  AND t.user_id = s.user_id
                  AND t.url = s.url
                  AND t.event_ts = s.event_ts)
               )
            WHEN MATCHED THEN UPDATE SET
              t.platform_event_id = COALESCE(s.platform_event_id, t.platform_event_id),
              {assignments}
            WHEN NOT MATCHED THEN
              INSERT ({rendered_columns})
              VALUES ({", ".join(f"s.{name}" for name in columns)})
            """
        )
        return row_count
    finally:
        current.unpersist()


def _record_applied_events(
    events: DataFrame,
    *,
    epoch_id: int,
    run_id: str,
) -> None:
    applied = (
        events.withColumn("applied_at", current_timestamp())
        .withColumn("silver_epoch_id", lit(epoch_id).cast("long"))
        .withColumn("silver_run_id", lit(run_id))
        .select(*APPLIED_EVENT_COLUMNS)
        .dropDuplicates(["event_id"])
    )
    view_name = f"silver_applied_{abs(int(epoch_id))}"
    applied.createOrReplaceTempView(view_name)
    rendered_columns = ", ".join(APPLIED_EVENT_COLUMNS)
    applied.sparkSession.sql(
        f"""
        MERGE INTO {APPLIED_EVENTS_TABLE} AS t
        USING {view_name} AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN
          INSERT ({rendered_columns})
          VALUES ({", ".join(f"s.{name}" for name in APPLIED_EVENT_COLUMNS)})
        """
    )


def apply_events_to_silver(
    events: DataFrame,
    *,
    epoch_id: int,
    run_id: str,
) -> ApplyResult:
    """Merge current state first, then durably record every applied event ID."""

    spark = events.sparkSession
    ensure_silver_tables(spark)
    normalized = (
        prepare_silver_events(events)
        .filter(col("event_id").isNotNull())
        .dropDuplicates(["event_id"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        input_events = normalized.count()
        if input_events == 0:
            return ApplyResult(0, 0, 0, 0)
        applied_ids = spark.table(APPLIED_EVENTS_TABLE).select("event_id")
        unapplied = normalized.join(applied_ids, ["event_id"], "left_anti").persist(
            StorageLevel.MEMORY_AND_DISK
        )
        try:
            newly_applied = unapplied.count()
            if newly_applied == 0:
                return ApplyResult(input_events, input_events, 0, 0)

            current_rows = _merge_current_state(unapplied, epoch_id=epoch_id)
            # Ordering is deliberate: a crash before this point replays the
            # idempotent state MERGE; a crash after it sees the applied marker.
            _record_applied_events(unapplied, epoch_id=epoch_id, run_id=run_id)
            return ApplyResult(
                input_events=input_events,
                already_applied=input_events - newly_applied,
                newly_applied=newly_applied,
                current_rows_merged=current_rows,
            )
        finally:
            unapplied.unpersist()
    finally:
        normalized.unpersist()
