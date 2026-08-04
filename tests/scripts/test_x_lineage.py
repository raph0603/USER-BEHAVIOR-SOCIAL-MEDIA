import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.x_lineage import (
    PRIVACY_TOKENS,
    RawCaptureWriter,
    expected_clean_text,
    redaction_summary,
    sensitive_values,
)
from scripts.finalize_x_lineage import finalize


FIXTURE_PATH = ROOT / "tests" / "fixtures" / "x_lineage_event.json"


@pytest.fixture
def x_event():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_reference_privacy_policy_redacts_every_required_value(x_event):
    cleaned = expected_clean_text(x_event["raw_text"])

    assert cleaned == (
        "Salut <USER>, écris-moi à <EMAIL> ou au <PHONE> depuis <IP> "
        "🚀 #Data <URL>"
    )
    assert "#Data" in cleaned
    assert "🚀" in cleaned
    assert all(token in cleaned for token in PRIVACY_TOKENS)

    originals = sensitive_values(x_event["raw_text"])
    for values in originals.values():
        for value in values:
            assert value not in cleaned


def test_reference_redaction_summary_counts_values_after_precedence(x_event):
    assert redaction_summary(x_event["raw_text"]) == {
        "email_count": 1,
        "url_count": 1,
        "user_count": 1,
        "ip_count": 1,
        "phone_count": 1,
    }


def test_raw_capture_is_opt_in_exact_and_limited(monkeypatch, tmp_path, x_event):
    monkeypatch.delenv("X_RAW_CAPTURE_ENABLED", raising=False)
    disabled = RawCaptureWriter.from_environment(
        producer_name="collector",
        producer_run_id="run-disabled",
        kafka_topic="x.raw.events",
    )
    assert disabled.capture(x_event) is None

    monkeypatch.setenv("X_RAW_CAPTURE_ENABLED", "true")
    monkeypatch.setenv("X_RAW_CAPTURE_DIR", str(tmp_path))
    monkeypatch.setenv("X_RAW_CAPTURE_LIMIT", "1")
    writer = RawCaptureWriter.from_environment(
        producer_name="collector",
        producer_run_id="run-1",
        kafka_topic="x.raw.events",
    )

    captured = writer.capture(x_event)
    assert captured == tmp_path / "raw.json"
    envelope = json.loads(captured.read_text(encoding="utf-8"))
    assert envelope["event"] == x_event
    assert envelope["capture_metadata"]["capture_stage"] == "before_privacy_cleaning"
    assert envelope["capture_metadata"]["producer_run_id"] == "run-1"
    assert writer.capture({**x_event, "platform_event_id": "different"}) is None

    if os.name != "nt":
        assert captured.stat().st_mode & 0o777 == 0o600


def test_export_command_refuses_more_than_one_event():
    if shutil.which("bash") is None:
        pytest.skip("bash is unavailable")
    result = subprocess.run(
        ["bash", "scripts/export_x_lineage.sh", "--max-events", "2"],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 2
    assert "exactly --max-events 1" in result.stderr


def test_export_command_fails_clearly_when_raw_capture_is_missing():
    source = (ROOT / "scripts" / "export_x_lineage.sh").read_text(encoding="utf-8")

    assert "RAW capture was not produced; no synthetic fallback is allowed." in source
    assert 'if [[ ! -s "$RAW_CAPTURE" ]]' in source


def test_fixture_is_never_used_as_final_export_input():
    source = (ROOT / "scripts" / "export_x_lineage.sh").read_text(encoding="utf-8")

    assert "tests/fixtures" not in source
    assert "x_lineage_event.json" not in source


def test_exact_journal_replay_is_scoped_to_one_x_platform_event():
    source = (
        ROOT / "spark" / "jobs" / "maintenance" / "replay_x_lineage_event.py"
    ).read_text(encoding="utf-8")

    assert '(col("source") == "x")' in source
    assert 'col("platform_event_id") == args.platform_event_id' in source
    assert ".limit(2)" in source
    assert "Expected exactly one committed X journal event" in source
    assert "_merge_current_projection(event, epoch_id=0)" in source
    assert "apply_events_to_silver(" in source


def test_bundle_finalization_hashes_every_existing_export(tmp_path):
    for relative in ("raw.json", "clean.json", "logs/collector.log"):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")
    manifest = {
        "status": "PASS",
        "files_generated": [
            "raw.json",
            "clean.json",
            "manifest.json",
            "manifest.sha256",
            "logs/collector.log",
        ],
        "warnings": [],
        "errors": [],
    }
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    finalized = finalize(tmp_path)

    assert finalized["status"] == "PASS"
    assert set(finalized["sha256"]) == {
        "raw.json",
        "clean.json",
        "logs/collector.log",
    }
    assert (tmp_path / "manifest.sha256").is_file()


def test_bundle_finalization_fails_when_raw_export_is_missing(tmp_path):
    manifest = {
        "status": "PASS",
        "files_generated": ["raw.json", "manifest.json", "manifest.sha256"],
        "warnings": [],
        "errors": [],
    }
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    finalized = finalize(tmp_path)

    assert finalized["status"] == "FAIL"
    assert "missing_generated_files:raw.json" in finalized["errors"]
