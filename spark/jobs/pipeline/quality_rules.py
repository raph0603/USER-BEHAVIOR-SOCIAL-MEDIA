"""Validated configuration and deterministic helpers for lakehouse quality checks."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit


SEVERITIES = frozenset({"info", "warning", "error"})
RULE_KINDS = frozenset(
    {
        "schema",
        "unique_key",
        "partitions",
        "table_empty",
        "completeness",
        "volume_ratio",
        "freshness",
        "orphan_files",
    }
)
RULE_THRESHOLD_KEYS = {
    "schema": frozenset(),
    "unique_key": frozenset({"max_duplicates"}),
    "partitions": frozenset({"min_partitions"}),
    "table_empty": frozenset({"min_rows"}),
    "completeness": frozenset({"min_rate"}),
    "volume_ratio": frozenset({"min_ratio", "max_ratio"}),
    "freshness": frozenset({"max_age_minutes"}),
    "orphan_files": frozenset({"max_orphans", "sample_limit", "max_objects"}),
}
RULE_RESULT_KEYS = {
    "schema": frozenset({"required_columns"}),
    "unique_key": frozenset({"columns", "max_duplicates"}),
    "partitions": frozenset({"min_partitions"}),
    "table_empty": frozenset({"min_rows"}),
    "completeness": frozenset({"columns", "min_rate"}),
    "volume_ratio": frozenset({"min_ratio", "max_ratio"}),
    "freshness": frozenset({"timestamp_column", "max_age_minutes"}),
    "orphan_files": frozenset({"max_orphans", "sample_limit", "max_objects"}),
}
RULE_REQUIRED_KEYS = {
    "schema": frozenset({"table", "required_columns"}),
    "unique_key": frozenset({"table", "columns"}),
    "partitions": frozenset({"table"}),
    "table_empty": frozenset({"table"}),
    "completeness": frozenset({"table", "columns", "min_rate"}),
    "volume_ratio": frozenset({"upstream_table", "downstream_table"}),
    "freshness": frozenset({"table", "timestamp_column", "max_age_minutes"}),
    "orphan_files": frozenset({"table"}),
}
TABLE_PATTERN = re.compile(r"^lakehouse\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
COLUMN_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RULE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,127}$")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


@dataclass(frozen=True)
class QualityProfile:
    name: str
    allow_empty: bool
    empty_severity: str
    fail_severities: frozenset[str]


@dataclass(frozen=True)
class QualityRule:
    rule_id: str
    kind: str
    severity: str
    options: Mapping[str, Any]

    def threshold_options(self) -> dict[str, Any]:
        return {
            key: self.options[key]
            for key in sorted(RULE_RESULT_KEYS[self.kind])
            if key in self.options
        }


@dataclass(frozen=True)
class QualityConfig:
    schema_version: str
    profile: QualityProfile
    rules: tuple[QualityRule, ...]


def _severity(value: object, *, field: str) -> str:
    severity = str(value or "").strip().lower()
    if severity not in SEVERITIES:
        raise ValueError(f"{field} must be one of {sorted(SEVERITIES)}")
    return severity


def _table(value: object, *, field: str) -> str:
    table = str(value or "").strip()
    if not TABLE_PATTERN.fullmatch(table):
        raise ValueError(f"{field} must be a three-part lakehouse table identifier")
    return table


def _columns(value: object, *, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list")
    columns = [str(column).strip() for column in value]
    if any(not COLUMN_PATTERN.fullmatch(column) for column in columns):
        raise ValueError(f"{field} contains an invalid column identifier")
    if len(columns) != len(set(columns)):
        raise ValueError(f"{field} must not contain duplicates")
    return columns


def _non_negative_int(value: object, *, field: str, default: int) -> int:
    number = default if value is None else int(str(value))
    if number < 0:
        raise ValueError(f"{field} must be non-negative")
    return number


def _positive_number(value: object, *, field: str) -> float:
    number = float(str(value))
    if number <= 0:
        raise ValueError(f"{field} must be greater than zero")
    return number


def parse_threshold_overrides(value: str | None) -> dict[str, dict[str, Any]]:
    """Parse a JSON object supplied inline or through a JSON file path."""

    if value is None or not value.strip():
        return {}
    raw = value.strip()
    try:
        candidate = Path(raw)
        if candidate.is_file():
            raw = candidate.read_text(encoding="utf-8")
    except OSError:
        pass
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Threshold overrides must be a JSON object")
    normalized: dict[str, dict[str, Any]] = {}
    for rule_id, override in parsed.items():
        if not isinstance(override, dict):
            raise ValueError(f"Threshold override for {rule_id} must be an object")
        normalized[str(rule_id)] = dict(override)
    return normalized


def _validate_rule(raw_rule: Mapping[str, Any]) -> QualityRule:
    rule_id = str(raw_rule.get("id") or "").strip()
    if not RULE_ID_PATTERN.fullmatch(rule_id):
        raise ValueError(f"Invalid quality rule id: {rule_id!r}")
    kind = str(raw_rule.get("kind") or "").strip()
    if kind not in RULE_KINDS:
        raise ValueError(f"Rule {rule_id} has unsupported kind {kind!r}")
    severity = _severity(raw_rule.get("severity"), field=f"{rule_id}.severity")
    options = {
        key: value
        for key, value in raw_rule.items()
        if key not in {"id", "kind", "severity", "profiles"}
    }
    allowed_options = RULE_REQUIRED_KEYS[kind] | RULE_RESULT_KEYS[kind]
    unknown_options = sorted(set(options) - allowed_options)
    if unknown_options:
        raise ValueError(f"Rule {rule_id} contains unknown keys: {unknown_options}")
    missing = sorted(RULE_REQUIRED_KEYS[kind] - set(options))
    if missing:
        raise ValueError(f"Rule {rule_id} is missing required keys: {missing}")

    if "table" in options:
        options["table"] = _table(options["table"], field=f"{rule_id}.table")
    for name in ("upstream_table", "downstream_table"):
        if name in options:
            options[name] = _table(options[name], field=f"{rule_id}.{name}")
    if "columns" in options:
        options["columns"] = _columns(options["columns"], field=f"{rule_id}.columns")
    if "timestamp_column" in options:
        timestamp_column = str(options["timestamp_column"]).strip()
        if not COLUMN_PATTERN.fullmatch(timestamp_column):
            raise ValueError(f"{rule_id}.timestamp_column is invalid")
        options["timestamp_column"] = timestamp_column
    if "required_columns" in options:
        required_columns = options["required_columns"]
        if not isinstance(required_columns, dict) or not required_columns:
            raise ValueError(f"{rule_id}.required_columns must be a non-empty object")
        normalized_columns = {}
        for column, data_type in required_columns.items():
            if not COLUMN_PATTERN.fullmatch(str(column)):
                raise ValueError(f"{rule_id}.required_columns contains an invalid column")
            normalized_type = str(data_type).strip().lower()
            if not normalized_type:
                raise ValueError(f"{rule_id}.required_columns contains an empty type")
            normalized_columns[str(column)] = normalized_type
        options["required_columns"] = normalized_columns

    for name, default in (
        ("max_duplicates", 0),
        ("min_partitions", 1),
        ("min_rows", 1),
        ("max_orphans", 0),
        ("sample_limit", 20),
        ("max_objects", 1_000_000),
    ):
        if name in options or name in RULE_THRESHOLD_KEYS[kind]:
            options[name] = _non_negative_int(
                options.get(name), field=f"{rule_id}.{name}", default=default
            )
    if int(str(options.get("max_objects", 1))) < 1:
        raise ValueError(f"{rule_id}.max_objects must be greater than zero")
    if "min_rate" in options:
        rate = float(str(options["min_rate"]))
        if not 0 <= rate <= 1:
            raise ValueError(f"{rule_id}.min_rate must be in [0, 1]")
        options["min_rate"] = rate
    for name, ratio_default in (("min_ratio", 0.0), ("max_ratio", 1.0)):
        if name in options or kind == "volume_ratio":
            options[name] = float(str(options.get(name, ratio_default)))
            if options[name] < 0:
                raise ValueError(f"{rule_id}.{name} must be non-negative")
    if kind == "volume_ratio" and options["min_ratio"] > options["max_ratio"]:
        raise ValueError(f"{rule_id} min_ratio must not exceed max_ratio")
    if "max_age_minutes" in options:
        options["max_age_minutes"] = _positive_number(
            options["max_age_minutes"], field=f"{rule_id}.max_age_minutes"
        )
    return QualityRule(rule_id, kind, severity, options)


def load_quality_config(
    path: Path,
    *,
    profile_name: str,
    threshold_overrides: Mapping[str, Mapping[str, Any]] | None = None,
    fail_severities: set[str] | None = None,
) -> QualityConfig:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("Quality rules file must contain a JSON object")
    schema_version = str(document.get("schema_version") or "").strip()
    if not schema_version:
        raise ValueError("Quality rules schema_version must not be empty")
    profiles = document.get("profiles")
    if not isinstance(profiles, dict) or profile_name not in profiles:
        raise ValueError(f"Unknown quality profile: {profile_name}")
    raw_profile = profiles[profile_name]
    if not isinstance(raw_profile, dict):
        raise ValueError(f"Quality profile {profile_name} must be an object")
    if not isinstance(raw_profile.get("allow_empty", False), bool):
        raise ValueError(f"profiles.{profile_name}.allow_empty must be a boolean")
    empty_severity = _severity(
        raw_profile.get("empty_severity"), field=f"profiles.{profile_name}.empty_severity"
    )
    configured_failures = raw_profile.get("fail_severities", ["error"])
    if not isinstance(configured_failures, list):
        raise ValueError(f"profiles.{profile_name}.fail_severities must be a list")
    selected_failures = (
        {_severity(value, field="fail_severities") for value in fail_severities}
        if fail_severities is not None
        else {
            _severity(value, field=f"profiles.{profile_name}.fail_severities")
            for value in configured_failures
        }
    )
    profile = QualityProfile(
        name=profile_name,
        allow_empty=bool(raw_profile.get("allow_empty", False)),
        empty_severity=empty_severity,
        fail_severities=frozenset(selected_failures),
    )

    raw_rules = document.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ValueError("Quality rules must be a non-empty list")
    overrides = threshold_overrides or {}
    known_rule_ids = {str(rule.get("id")) for rule in raw_rules if isinstance(rule, dict)}
    unknown_overrides = sorted(set(overrides) - known_rule_ids)
    if unknown_overrides:
        raise ValueError(f"Threshold overrides reference unknown rules: {unknown_overrides}")

    selected_rules = []
    seen_ids: set[str] = set()
    for raw in raw_rules:
        if not isinstance(raw, dict):
            raise ValueError("Each quality rule must be an object")
        rule_id = str(raw.get("id") or "")
        if rule_id in seen_ids:
            raise ValueError(f"Duplicate quality rule id: {rule_id}")
        seen_ids.add(rule_id)
        enabled_profiles = raw.get("profiles")
        if enabled_profiles is not None:
            if not isinstance(enabled_profiles, list):
                raise ValueError(f"Rule {rule_id}.profiles must be a list")
            if profile_name not in {str(value) for value in enabled_profiles}:
                continue
        merged = dict(raw)
        override = dict(overrides.get(rule_id, {}))
        kind = str(merged.get("kind") or "")
        invalid_override_keys = sorted(set(override) - RULE_THRESHOLD_KEYS.get(kind, frozenset()))
        if invalid_override_keys:
            raise ValueError(
                f"Rule {rule_id} override contains non-threshold keys: {invalid_override_keys}"
            )
        merged.update(override)
        selected_rules.append(_validate_rule(merged))
    if not selected_rules:
        raise ValueError(f"Quality profile {profile_name} selected no rules")
    return QualityConfig(schema_version, profile, tuple(selected_rules))


def stable_result_id(run_id: str, profile: str, rule_id: str) -> str:
    payload = "\u001f".join((run_id, profile, rule_id))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def empty_outcome(profile: QualityProfile) -> tuple[str, str]:
    status = "anomaly" if profile.allow_empty else "failed"
    return status, profile.empty_severity


def result_causes_failure(status: str, severity: str, profile: QualityProfile) -> bool:
    return status in {"failed", "anomaly"} and severity in profile.fail_severities


def canonical_object_id(path: str) -> str:
    normalized = str(path).strip().replace("\\", "/")
    parsed = urlsplit(normalized)
    if parsed.scheme.lower() in {"s3", "s3a", "s3n"}:
        return f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    if parsed.scheme == "file":
        return parsed.path
    return normalized.rstrip("/")


def find_orphan_files(objects: set[str], referenced_files: set[str]) -> list[str]:
    normalized_objects = {canonical_object_id(path) for path in objects}
    normalized_references = {canonical_object_id(path) for path in referenced_files}
    return sorted(normalized_objects - normalized_references)
