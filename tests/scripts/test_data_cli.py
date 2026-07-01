import argparse
import json
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import pandas as pd
import numpy as np

# Add project root to path
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.data_cli as data_cli


class DataCliTests(unittest.TestCase):
    def test_normalize_list_value(self):
        # Test None
        self.assertIsNone(data_cli.normalize_list_value(None))
        self.assertIsNone(data_cli.normalize_list_value(np.nan))
        self.assertIsNone(data_cli.normalize_list_value(pd.NA))

        # Test valid list/tuple
        self.assertEqual(data_cli.normalize_list_value(["a", "b", ""]), ["a", "b"])
        self.assertEqual(data_cli.normalize_list_value(("a", None, "b")), ["a", "b"])

        # Test string representations
        self.assertEqual(data_cli.normalize_list_value('["a", "b"]'), ["a", "b"])
        self.assertEqual(data_cli.normalize_list_value('a, b; c'), ["a", "b", "c"])
        self.assertEqual(data_cli.normalize_list_value('   '), None)

    def test_filter_dataframe(self):
        df = pd.DataFrame({
            "source": ["youtube", "x", "reddit", "youtube"],
            "created_at": pd.to_datetime([
                "2026-06-01T10:00:00Z",
                "2026-06-02T10:00:00Z",
                "2026-06-03T10:00:00Z",
                "2026-06-04T10:00:00Z"
            ], utc=True)
        })

        # Filter by source
        filtered = data_cli.filter_dataframe(df.copy(), source="youtube")
        self.assertEqual(len(filtered), 2)
        self.assertTrue((filtered["source"] == "youtube").all())

        # Filter by start date
        filtered = data_cli.filter_dataframe(df.copy(), start_date="2026-06-02T10:00:00Z")
        self.assertEqual(len(filtered), 3)

        # Filter by end date
        filtered = data_cli.filter_dataframe(df.copy(), end_date="2026-06-02T10:00:00Z")
        self.assertEqual(len(filtered), 2)

        # Filter by limit
        filtered = data_cli.filter_dataframe(df.copy(), limit=2)
        self.assertEqual(len(filtered), 2)

    def test_normalize_dataframe_csv(self):
        df = pd.DataFrame({
            "created_at": pd.to_datetime(["2026-06-01T10:00:00Z"], utc=True),
            "collaborator_channel_ids": [["c1", "c2"]]
        })

        normalized = data_cli.normalize_dataframe(df.copy(), "csv")
        self.assertEqual(normalized.iloc[0]["created_at"], "2026-06-01T10:00:00Z")
        self.assertEqual(normalized.iloc[0]["collaborator_channel_ids"], '["c1", "c2"]')

    def test_normalize_dataframe_jsonl(self):
        df = pd.DataFrame({
            "created_at": pd.to_datetime(["2026-06-01T10:00:00Z"], utc=True),
            "collaborator_channel_ids": [["c1", "c2"]]
        })

        normalized = data_cli.normalize_dataframe(df.copy(), "jsonl")
        self.assertEqual(normalized.iloc[0]["created_at"], pd.to_datetime("2026-06-01T10:00:00Z", utc=True))
        self.assertEqual(normalized.iloc[0]["collaborator_channel_ids"], ["c1", "c2"])

    @patch("scripts.data_cli.load_iceberg_data")
    @patch("scripts.data_cli.get_iceberg_config")
    def test_export_command_execution_csv(self, mock_get_config, mock_load_data):
        mock_get_config.return_value = {}
        sample_df = pd.DataFrame({
            "source": ["youtube", "x"],
            "created_at": pd.to_datetime(["2026-06-01T10:00:00Z", "2026-06-02T10:00:00Z"], utc=True),
            "collaborator_channel_ids": [["c1"], None]
        })
        mock_load_data.return_value = sample_df

        output_dir = ROOT / "tests" / "temp_output"
        output_file = output_dir / "test_export.csv"

        try:
            test_args = argparse.Namespace(
                command="export",
                format="csv",
                output=str(output_file),
                source="youtube",
                start_date=None,
                end_date=None,
                limit=None
            )
            data_cli.run_export(test_args)

            self.assertTrue(output_file.exists())
            written_df = pd.read_csv(output_file)
            self.assertEqual(len(written_df), 1)
            self.assertEqual(written_df.iloc[0]["source"], "youtube")
            self.assertEqual(written_df.iloc[0]["collaborator_channel_ids"], '["c1"]')
        finally:
            if output_file.exists():
                output_file.unlink()
            if output_dir.exists():
                output_dir.rmdir()

    @patch("scripts.data_cli.load_iceberg_data")
    @patch("scripts.data_cli.get_iceberg_config")
    def test_export_command_execution_jsonl(self, mock_get_config, mock_load_data):
        mock_get_config.return_value = {}
        sample_df = pd.DataFrame({
            "source": ["youtube", "x"],
            "created_at": pd.to_datetime(["2026-06-01T10:00:00Z", "2026-06-02T10:00:00Z"], utc=True),
            "collaborator_channel_ids": [["c1"], None]
        })
        mock_load_data.return_value = sample_df

        output_dir = ROOT / "tests" / "temp_output"
        output_file = output_dir / "test_export.jsonl"

        try:
            test_args = argparse.Namespace(
                command="export",
                format="jsonl",
                output=str(output_file),
                source=None,
                start_date=None,
                end_date=None,
                limit=None
            )
            data_cli.run_export(test_args)

            self.assertTrue(output_file.exists())
            # Read lines
            with open(output_file, "r") as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 2)
            first_obj = json.loads(lines[0])
            self.assertEqual(first_obj["source"], "youtube")
            self.assertEqual(first_obj["collaborator_channel_ids"], ["c1"])
        finally:
            if output_file.exists():
                output_file.unlink()
            if output_dir.exists():
                output_dir.rmdir()

    @patch("scripts.data_cli.load_iceberg_data")
    @patch("scripts.data_cli.get_iceberg_config")
    def test_export_command_execution_parquet(self, mock_get_config, mock_load_data):
        mock_get_config.return_value = {}
        sample_df = pd.DataFrame({
            "source": ["youtube", "x"],
            "created_at": pd.to_datetime(["2026-06-01T10:00:00Z", "2026-06-02T10:00:00Z"], utc=True),
            "collaborator_channel_ids": [["c1"], None]
        })
        mock_load_data.return_value = sample_df

        output_dir = ROOT / "tests" / "temp_output"
        output_file = output_dir / "test_export.parquet"

        try:
            test_args = argparse.Namespace(
                command="export",
                format="parquet",
                output=str(output_file),
                source=None,
                start_date=None,
                end_date=None,
                limit=None
            )
            data_cli.run_export(test_args)

            self.assertTrue(output_file.exists())
            written_df = pd.read_parquet(output_file)
            self.assertEqual(len(written_df), 2)
            self.assertEqual(written_df.iloc[0]["source"], "youtube")
            self.assertEqual(list(written_df.iloc[0]["collaborator_channel_ids"]), ["c1"])
        finally:
            if output_file.exists():
                output_file.unlink()
            if output_dir.exists():
                output_dir.rmdir()

    def test_import_file_not_found(self):
        test_args = argparse.Namespace(
            command="import",
            file="nonexistent_file_xyz.csv",
            source="auto",
            trigger_pipeline=False
        )
        with self.assertRaises(SystemExit) as cm:
            with patch("sys.stderr"):
                data_cli.run_import(test_args)
        self.assertEqual(cm.exception.code, 1)

    @patch("scripts.data_cli.load_import_events")
    @patch("scripts.data_cli.publish_events")
    @patch("scripts.data_cli.get_manual_import_config")
    @patch("scripts.data_cli.Path.is_file")
    @patch("builtins.open")
    def test_import_success(self, mock_open, mock_is_file, mock_get_config, mock_publish, mock_load):
        mock_is_file.return_value = True
        mock_get_config.return_value = {}
        mock_load.return_value = [{"source": "youtube", "url": "url1", "title": "t1", "user_id": "u1", "timestamp": "ts"}]
        mock_publish.return_value = {"youtube": 1}

        test_args = argparse.Namespace(
            command="import",
            file="test_import.csv",
            source="youtube",
            trigger_pipeline=False
        )
        with patch("sys.stdout"):
            data_cli.run_import(test_args)

        mock_load.assert_called_once_with("test_import.csv", mock_open.return_value.__enter__.return_value.read.return_value, source="youtube")
        mock_publish.assert_called_once_with(mock_load.return_value, {})

    @patch("scripts.data_cli.load_import_events")
    @patch("scripts.data_cli.publish_events")
    @patch("scripts.data_cli.get_manual_import_config")
    @patch("scripts.data_cli.Path.is_file")
    @patch("builtins.open")
    @patch("scripts.data_cli.AirflowClient")
    def test_import_trigger_pipeline(self, mock_airflow_client_class, mock_open, mock_is_file, mock_get_config, mock_publish, mock_load):
        mock_is_file.return_value = True
        mock_get_config.return_value = {}
        mock_load.return_value = [{"source": "youtube", "url": "url1", "title": "t1", "user_id": "u1", "timestamp": "ts"}]
        mock_publish.return_value = {"youtube": 1}
        
        mock_airflow_client = MagicMock()
        mock_airflow_client.trigger_dag.return_value = {"dag_run_id": "test_run_id_123"}
        mock_airflow_client_class.return_value = mock_airflow_client

        test_args = argparse.Namespace(
            command="import",
            file="test_import.csv",
            source="youtube",
            trigger_pipeline=True
        )
        with patch("sys.stdout"):
            data_cli.run_import(test_args)

        mock_airflow_client.trigger_dag.assert_called_once_with(
            "manual_file_import_lakehouse",
            {
                "sources": ["youtube"],
                "record_count": 1
            }
        )

    @patch("scripts.data_cli.load_import_events")
    @patch("scripts.data_cli.Path.is_file")
    @patch("builtins.open")
    def test_import_parse_error(self, mock_open, mock_is_file, mock_load):
        mock_is_file.return_value = True
        mock_load.side_effect = ValueError("invalid csv format")

        test_args = argparse.Namespace(
            command="import",
            file="test_import.csv",
            source="auto",
            trigger_pipeline=False
        )
        with self.assertRaises(SystemExit) as cm:
            with patch("sys.stderr"):
                data_cli.run_import(test_args)
        self.assertEqual(cm.exception.code, 1)

    @patch("scripts.data_cli.load_import_events")
    @patch("scripts.data_cli.publish_events")
    @patch("scripts.data_cli.Path.is_file")
    @patch("builtins.open")
    def test_import_publish_error(self, mock_open, mock_is_file, mock_publish, mock_load):
        mock_is_file.return_value = True
        mock_load.return_value = [{"source": "youtube", "url": "url1", "title": "t1", "user_id": "u1", "timestamp": "ts"}]
        mock_publish.side_effect = RuntimeError("Kafka connection failed")

        test_args = argparse.Namespace(
            command="import",
            file="test_import.csv",
            source="auto",
            trigger_pipeline=False
        )
        with self.assertRaises(SystemExit) as cm:
            with patch("sys.stderr"):
                data_cli.run_import(test_args)
        self.assertEqual(cm.exception.code, 1)

    @patch("scripts.data_cli.load_import_events")
    @patch("scripts.data_cli.Path.is_file")
    @patch("builtins.open")
    def test_import_empty_events(self, mock_open, mock_is_file, mock_load):
        mock_is_file.return_value = True
        mock_load.return_value = []

        test_args = argparse.Namespace(
            command="import",
            file="test_import.csv",
            source="auto",
            trigger_pipeline=False
        )
        with patch("sys.stdout") as mock_stdout:
            data_cli.run_import(test_args)
        
        mock_stdout.write.assert_any_call("No events found to import.")


if __name__ == "__main__":
    unittest.main()
