#!/usr/bin/env python3
"""Portable data transfer CLI for the lakehouse stack."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "dashboard"
for import_path in (ROOT, DASHBOARD_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

SUPPORTED_EXPORT_FORMATS = ("csv", "jsonl", "parquet")
SUPPORTED_IMPORT_FORMATS = ("csv", "json", "jsonl", "ndjson")
# Kept as an injectable seam for callers and tests that used the old module API.
AirflowClient = None
LIST_COLUMNS = (
    "changed_fields",
    "collaborator_channel_ids",
    "transcript_available_languages",
)
DATE_COLUMNS = (
    "created_at",
    "event_ts",
    "metadata_refreshed_at",
)


def _pandas():
    try:
        return importlib.import_module("pandas")
    except ImportError as exc:
        raise RuntimeError(
            "pandas is required. Run this command in the dashboard container "
            "or install dashboard/requirements.txt."
        ) from exc


def _dashboard_module(name):
    try:
        return importlib.import_module(f"dashboard.{name}")
    except ImportError:
        return importlib.import_module(name)


def get_iceberg_config():
    return _dashboard_module("loaders").get_iceberg_config()


def load_export_data(config):
    """Read every Silver event column so a transfer is not lossy."""
    loaders = _dashboard_module("loaders")
    if hasattr(loaders, "load_iceberg_table"):
        return loaders.load_iceberg_table(config["table_path"], config=config)
    # Compatibility for older dashboard images and the isolated E2E fixture.
    return loaders.load_iceberg_data(config)


def load_iceberg_data(config):
    """Backward-compatible name for the now lossless export reader."""
    return load_export_data(config)


def load_import_events(file_name, payload, source="auto"):
    return _dashboard_module("manual_import").load_import_events(
        file_name,
        payload,
        source=source,
    )


def get_manual_import_config():
    return _dashboard_module("manual_import").get_manual_import_config()


def publish_events(events, config=None):
    return _dashboard_module("manual_import").publish_events(events, config)


def new_airflow_client(config=None):
    client_class = AirflowClient
    if client_class is None:
        client_class = _dashboard_module("airflow_monitoring").AirflowClient
    return client_class(config=config)


def _log(message):
    print(message, file=sys.stderr)


def normalize_list_value(value):
    """Normalize an array-like value to a clean string list or ``None``."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        cleaned = [
            str(item).strip()
            for item in value
            if item is not None and str(item).strip()
        ]
        return cleaned or None
    if hasattr(value, "tolist"):
        return normalize_list_value(value.tolist())

    pd = _pandas()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("[") and value.endswith("]"):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return normalize_list_value(parsed)
        except json.JSONDecodeError:
            pass
    cleaned = [item.strip() for item in re.split(r"[,;]", value) if item.strip()]
    return cleaned or None


def normalize_dataframe(dataframe, output_format):
    """Normalize portable date and array representations before export."""
    pd = _pandas()
    datetime_columns = []
    for column in dataframe.columns:
        if (
            pd.api.types.is_datetime64_any_dtype(dataframe[column])
            or column in DATE_COLUMNS
        ):
            dataframe[column] = pd.to_datetime(
                dataframe[column],
                errors="coerce",
                utc=True,
            )
            datetime_columns.append(column)

    for column in LIST_COLUMNS:
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].apply(normalize_list_value)

    if output_format == "csv":
        for column in datetime_columns:
            dataframe[column] = dataframe[column].apply(
                lambda value: value.strftime("%Y-%m-%dT%H:%M:%SZ")
                if pd.notna(value)
                else ""
            )
        for column in LIST_COLUMNS:
            if column in dataframe.columns:
                dataframe[column] = dataframe[column].apply(
                    lambda value: json.dumps(value, ensure_ascii=False)
                    if value is not None
                    else ""
                )
    return dataframe


def _event_time_column(dataframe):
    for column in ("event_ts", "created_at", "timestamp", "collected_at"):
        if column in dataframe.columns:
            return column
    return None


def filter_dataframe(
    dataframe,
    source=None,
    start_date=None,
    end_date=None,
    limit=None,
):
    """Filter exported events without assuming a dashboard-only schema."""
    pd = _pandas()
    if source:
        if "source" not in dataframe.columns:
            raise ValueError("the selected Iceberg table has no source column")
        normalized_source = dataframe["source"].astype("string").str.lower()
        dataframe = dataframe[normalized_source == source.strip().lower()]

    if start_date or end_date:
        time_column = _event_time_column(dataframe)
        if time_column is None:
            raise ValueError("the selected Iceberg table has no event time column")
        event_times = pd.to_datetime(dataframe[time_column], errors="coerce", utc=True)
        if start_date:
            dataframe = dataframe[event_times >= pd.to_datetime(start_date, utc=True)]
            event_times = event_times.loc[dataframe.index]
        if end_date:
            dataframe = dataframe[event_times <= pd.to_datetime(end_date, utc=True)]

    if limit is not None:
        dataframe = dataframe.head(limit)
    return dataframe


def _export_config(args):
    config = get_iceberg_config()
    overrides = {
        "table_path": getattr(args, "table_path", None),
        "endpoint_url": getattr(args, "minio_endpoint", None),
        "access_key": getattr(args, "minio_access_key", None),
        "secret_key": getattr(args, "minio_secret_key", None),
    }
    config.update({key: value for key, value in overrides.items() if value})
    return config


def _write_export(dataframe, output_format, output):
    if output == "-":
        if output_format == "parquet":
            dataframe.to_parquet(sys.stdout.buffer, index=False)
        elif output_format == "csv":
            dataframe.to_csv(sys.stdout, index=False, lineterminator="\n")
        else:
            dataframe.to_json(
                sys.stdout,
                orient="records",
                lines=True,
                date_format="iso",
                force_ascii=False,
            )
        return

    output_path = Path(output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "csv":
        dataframe.to_csv(
            output_path,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    elif output_format == "jsonl":
        dataframe.to_json(
            output_path,
            orient="records",
            lines=True,
            date_format="iso",
            force_ascii=False,
        )
    else:
        dataframe.to_parquet(output_path, index=False)


def run_export(args):
    """Execute the export subcommand."""
    _log("Loading all event columns from the Iceberg Silver table...")
    try:
        dataframe = load_iceberg_data(_export_config(args))
        dataframe = filter_dataframe(
            dataframe,
            source=args.source,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
        )
        dataframe = normalize_dataframe(dataframe, args.format)
        _write_export(dataframe, args.format, args.output)
    except Exception as exc:
        print(f"Error loading database data or writing export: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    destination = "standard output" if args.output == "-" else args.output
    _log(f"Exported {len(dataframe)} event(s) to {destination}.")


def _read_import_payload(args):
    if args.file == "-":
        input_format = getattr(args, "format", None)
        if not input_format:
            raise ValueError("--format is required when --file is -")
        return f"stdin.{input_format}", sys.stdin.buffer.read()

    file_path = Path(args.file).expanduser().resolve()
    if not file_path.is_file():
        raise FileNotFoundError(f"file not found: {file_path}")
    file_name = file_path.name
    input_format = getattr(args, "format", None)
    if input_format:
        file_name = f"{file_path.stem}.{input_format}"
    with open(file_path, "rb") as input_file:
        return file_name, input_file.read()


def _import_config(args):
    config = get_manual_import_config()
    if getattr(args, "kafka_bootstrap", None):
        config["bootstrap_servers"] = args.kafka_bootstrap
    return config


def _airflow_config(args):
    option_names = ("airflow_url", "airflow_username", "airflow_password")
    has_override = any(getattr(args, name, None) for name in option_names)
    has_override = has_override or getattr(args, "airflow_timeout", None) is not None
    if not has_override:
        return None
    return {
        "base_url": (
            args.airflow_url
            or os.getenv("DASHBOARD_AIRFLOW_URL", "http://localhost:8088")
        ).rstrip("/"),
        "username": args.airflow_username
        or os.getenv("DASHBOARD_AIRFLOW_USERNAME", "admin"),
        "password": args.airflow_password
        or os.getenv("DASHBOARD_AIRFLOW_PASSWORD", "admin"),
        "timeout": args.airflow_timeout
        or int(os.getenv("DASHBOARD_AIRFLOW_TIMEOUT_SECONDS", "10")),
    }


def run_import(args):
    """Execute the import subcommand."""
    try:
        file_name, payload = _read_import_payload(args)
        events = load_import_events(file_name, payload, source=args.source)
    except FileNotFoundError as exc:
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"Data import validation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    if not events:
        print("No events found to import.")
        return

    try:
        published_counts = publish_events(events, _import_config(args))
    except Exception as exc:
        print(f"Error publishing events to Kafka: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Imported {len(events)} event(s) successfully.")
    for source, count in published_counts.items():
        print(f"  {source}: {count} event(s)")

    if not args.trigger_pipeline:
        return
    try:
        client = new_airflow_client(_airflow_config(args))
        dag_id = "manual_file_import_lakehouse"
        run = client.trigger_dag(
            dag_id,
            {
                "sources": list(published_counts),
                "record_count": len(events),
            },
        )
        print(f"Pipeline started: {run.get('dag_run_id', dag_id)}")
    except Exception as exc:
        print(f"Error triggering Airflow pipeline: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


def _doctor_result(name, target, check):
    try:
        detail = check()
        return {"name": name, "target": target, "status": "ok", "detail": detail}
    except Exception as exc:
        return {
            "name": name,
            "target": target,
            "status": "error",
            "detail": str(exc),
        }


def run_doctor(args):
    """Check that this environment can reach each data service."""
    iceberg_config = _export_config(args)
    import_config = get_manual_import_config()
    if args.kafka_bootstrap:
        import_config["bootstrap_servers"] = args.kafka_bootstrap

    def check_iceberg():
        loaders = _dashboard_module("loaders")
        if hasattr(loaders, "load_iceberg_table"):
            frame = loaders.load_iceberg_table(
                iceberg_config["table_path"],
                config=iceberg_config,
                limit=1,
            )
        else:
            frame = loaders.load_iceberg_data(iceberg_config).head(1)
        return f"read {len(frame)} sample row(s)"

    def check_kafka():
        admin_module = importlib.import_module("confluent_kafka.admin")
        client = admin_module.AdminClient(
            {"bootstrap.servers": import_config["bootstrap_servers"]}
        )
        metadata = client.list_topics(timeout=args.timeout)
        return f"discovered {len(metadata.topics)} topic(s)"

    def check_airflow():
        client = new_airflow_client(_airflow_config(args))
        payload = client._get("/dags", params={"limit": 1})
        return f"API returned {len(payload.get('dags', []))} DAG(s)"

    checks = {
        "iceberg": lambda: _doctor_result(
            "iceberg",
            f"{iceberg_config['table_path']} via {iceberg_config['endpoint_url']}",
            check_iceberg,
        ),
        "kafka": lambda: _doctor_result(
            "kafka",
            import_config["bootstrap_servers"],
            check_kafka,
        ),
        "airflow": lambda: _doctor_result(
            "airflow",
            (_airflow_config(args) or {}).get(
                "base_url",
                os.getenv("DASHBOARD_AIRFLOW_URL", "http://localhost:8088"),
            ),
            check_airflow,
        ),
    }
    selected_checks = args.check or tuple(checks)
    results = [checks[name]() for name in selected_checks]

    if args.output == "json":
        print(json.dumps({"checks": results}, indent=2, ensure_ascii=False))
    else:
        for result in results:
            marker = "OK" if result["status"] == "ok" else "ERROR"
            print(f"[{marker}] {result['name']}: {result['target']}")
            print(f"  {result['detail']}")
    return 1 if any(result["status"] == "error" for result in results) else 0


def _add_iceberg_options(parser):
    parser.add_argument("--table-path", help="Override the Iceberg table URI.")
    parser.add_argument("--minio-endpoint", help="Override the MinIO/S3 endpoint.")
    parser.add_argument("--minio-access-key", help="Override the MinIO access key.")
    parser.add_argument("--minio-secret-key", help="Override the MinIO secret key.")


def _add_service_options(parser):
    parser.add_argument("--kafka-bootstrap", help="Override Kafka bootstrap servers.")
    parser.add_argument("--airflow-url", help="Override the Airflow base URL.")
    parser.add_argument("--airflow-username", help="Override the Airflow username.")
    parser.add_argument("--airflow-password", help="Override the Airflow password.")
    parser.add_argument("--airflow-timeout", type=int)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="data-transfer",
        description="Export, import and transfer lakehouse events portably.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export Silver events.")
    export_parser.add_argument(
        "--format",
        choices=SUPPORTED_EXPORT_FORMATS,
        required=True,
    )
    export_parser.add_argument(
        "--output",
        required=True,
        help="Destination path, or - for standard output.",
    )
    export_parser.add_argument("--source", choices=("youtube", "x", "reddit"))
    export_parser.add_argument("--start-date")
    export_parser.add_argument("--end-date")
    export_parser.add_argument("--limit", type=int)
    _add_iceberg_options(export_parser)

    import_parser = subparsers.add_parser("import", help="Import events to Kafka.")
    import_parser.add_argument(
        "--file",
        required=True,
        help="Input path, or - for standard input.",
    )
    import_parser.add_argument(
        "--format",
        choices=SUPPORTED_IMPORT_FORMATS,
        help="Input format override; required with --file -.",
    )
    import_parser.add_argument(
        "--source",
        choices=("youtube", "x", "reddit", "auto"),
        default="auto",
    )
    import_parser.add_argument("--trigger-pipeline", action="store_true")
    _add_service_options(import_parser)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Check Iceberg, Kafka and Airflow connectivity.",
    )
    doctor_parser.add_argument("--output", choices=("text", "json"), default="text")
    doctor_parser.add_argument("--timeout", type=int, default=10)
    doctor_parser.add_argument(
        "--check",
        action="append",
        choices=("iceberg", "kafka", "airflow"),
        help="Run only this check; repeat to select more than one.",
    )
    _add_iceberg_options(doctor_parser)
    _add_service_options(doctor_parser)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == "export":
        run_export(args)
        return 0
    if args.command == "import":
        run_import(args)
        return 0
    return run_doctor(args)


if __name__ == "__main__":
    raise SystemExit(main())
