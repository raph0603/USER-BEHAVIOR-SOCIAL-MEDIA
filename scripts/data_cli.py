#!/usr/bin/env python3
"""
CLI tool for exporting and importing data from the Lakehouse.
"""

import argparse
import json
import re
import sys
from pathlib import Path
import pandas as pd

# Add project root and dashboard directory to sys.path to allow imports
root_dir = Path(__file__).resolve().parents[1]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

dashboard_dir = root_dir / "dashboard"
if str(dashboard_dir) not in sys.path:
    sys.path.insert(0, str(dashboard_dir))

try:
    from dashboard.loaders import get_iceberg_config, load_iceberg_data
except ImportError:
    # Fallback to direct import if dashboard is already in path
    from loaders import get_iceberg_config, load_iceberg_data

try:
    from dashboard.manual_import import load_import_events, publish_events, get_manual_import_config
    from dashboard.airflow_monitoring import AirflowClient
except ImportError:
    from manual_import import load_import_events, publish_events, get_manual_import_config
    from airflow_monitoring import AirflowClient



def normalize_list_value(val):
    """
    Normalizes list/tuple values to a clean list of strings or None.
    """
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        cleaned = [str(x).strip() for x in val if x is not None and str(x).strip()]
        return cleaned if cleaned else None
    if hasattr(val, "tolist"):
        return normalize_list_value(val.tolist())
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        # Check if it is a JSON array string
        if val.startswith("[") and val.endswith("]"):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    cleaned = [str(x).strip() for x in parsed if x is not None and str(x).strip()]
                    return cleaned if cleaned else None
            except Exception:
                pass
        # Split by comma or semicolon
        parsed = re.split(r"[,;]", val)
        cleaned = [str(x).strip() for x in parsed if x is not None and str(x).strip()]
        return cleaned if cleaned else None
    return None


def normalize_dataframe(df, output_format):
    """
    Normalizes datetimes and list/tuple fields in the dataframe.
    """
    # 1. Normalize datetimes
    datetime_cols = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or col in ("created_at", "metadata_refreshed_at"):
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            datetime_cols.append(col)

    # 2. Normalize list/tuple fields
    list_cols = ["collaborator_channel_ids"]
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_list_value)

    # 3. Format based on format type
    if output_format == "csv":
        for col in datetime_cols:
            df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(x) else "")
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else "")
    elif output_format == "jsonl":
        # jsonl outputs list/tuple fields as standard JSON arrays, and datetimes as ISO-8601
        # handled by pd.to_json(orient='records', lines=True, date_format='iso')
        pass
    elif output_format == "parquet":
        # pyarrow needs list fields to be clean Python lists or None (no pd.NA or NaNs in them)
        pass

    return df


def filter_dataframe(df, source=None, start_date=None, end_date=None, limit=None):
    """
    Filters the dataframe by source, start/end dates, and limit.
    """
    if source:
        df = df[df["source"] == source.strip().lower()]

    if start_date:
        start_dt = pd.to_datetime(start_date, utc=True)
        df = df[df["created_at"] >= start_dt]

    if end_date:
        end_dt = pd.to_datetime(end_date, utc=True)
        df = df[df["created_at"] <= end_dt]

    if limit is not None:
        df = df.head(limit)

    return df


def run_export(args):
    """
    Executes the export subcommand.
    """
    print("Loading data from Iceberg database...")
    try:
        config = get_iceberg_config()
        df = load_iceberg_data(config)
    except Exception as e:
        print(f"Error loading database data: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(df)} total records. Applying filters...")
    df = filter_dataframe(
        df,
        source=args.source,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit
    )

    print(f"After filtering, {len(df)} records remain. Normalizing data...")
    df = normalize_dataframe(df, args.format)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing to {output_path} as {args.format}...")
    try:
        if args.format == "csv":
            df.to_csv(output_path, index=False, encoding="utf-8")
        elif args.format == "jsonl":
            df.to_json(output_path, orient="records", lines=True, date_format="iso")
        elif args.format == "parquet":
            df.to_parquet(output_path, index=False)
        print("Export completed successfully.")
    except Exception as e:
        print(f"Error writing export file: {e}", file=sys.stderr)
        sys.exit(1)


def run_import(args):
    """
    Executes the import subcommand.
    """
    file_path = Path(args.file)
    if not file_path.is_file():
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(file_path, "rb") as f:
            payload = f.read()
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        events = load_import_events(file_path.name, payload, source=args.source)
    except Exception as e:
        print(f"Error parsing/normalizing events: {e}", file=sys.stderr)
        sys.exit(1)

    if not events:
        print("No events found to import.")
        return

    try:
        config = get_manual_import_config()
        published_counts = publish_events(events, config)
    except Exception as e:
        print(f"Error publishing events to Kafka: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Imported {len(events)} event(s) successfully.")
    for src, count in published_counts.items():
        print(f"  {src}: {count} event(s)")

    if args.trigger_pipeline:
        print("Triggering Airflow pipeline...")
        try:
            client = AirflowClient()
            dag_id = "manual_file_import_lakehouse"
            run = client.trigger_dag(
                dag_id,
                {
                    "sources": list(published_counts.keys()),
                    "record_count": len(events),
                }
            )
            run_id = run.get("dag_run_id", dag_id)
            print(f"Pipeline started successfully: {run_id}")
        except Exception as e:
            print(f"Error triggering Airflow pipeline: {e}", file=sys.stderr)
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="CLI tool for data export and import.")
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")
    subparsers.required = True

    # Export parser
    export_parser = subparsers.add_parser("export", help="Export database events to file")
    export_parser.add_argument("--format", choices=["csv", "jsonl", "parquet"], required=True, help="Export format")
    export_parser.add_argument("--output", required=True, help="Output file path")
    export_parser.add_argument("--source", help="Filter by source platform (e.g. youtube, x, reddit)")
    export_parser.add_argument("--start-date", help="Filter events starting from this date (inclusive)")
    export_parser.add_argument("--end-date", help="Filter events up to this date (inclusive)")
    export_parser.add_argument("--limit", type=int, help="Limit number of exported records")

    # Import parser
    import_parser = subparsers.add_parser("import", help="Import events from file")
    import_parser.add_argument("--file", required=True, help="Input file path to import")
    import_parser.add_argument("--source", choices=["youtube", "x", "reddit", "auto"], default="auto", help="Source platform to assign")
    import_parser.add_argument("--trigger-pipeline", action="store_true", help="Trigger Airflow pipeline after import")

    args = parser.parse_args()

    if args.command == "export":
        run_export(args)
    elif args.command == "import":
        run_import(args)


if __name__ == "__main__":
    main()
