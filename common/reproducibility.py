"""Canonical identities shared by dataset, training, and evaluation workflows."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
CONTAINER_DIGEST_PATTERN = re.compile(r"^sha256:[a-f0-9]{64}$")
VOLATILE_MANIFEST_FIELDS = {"created_at", "generated_at", "dataset_relative_path"}


def canonical_json(value: Any) -> str:
    """Serialize JSON-compatible identity data deterministically."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def fingerprint(value: Any) -> str:
    """Return the SHA-256 of a canonical JSON value."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def manifest_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return immutable dataset-manifest fields used by ``manifest_sha256``."""

    return {
        key: value
        for key, value in payload.items()
        if key not in VOLATILE_MANIFEST_FIELDS and key != "manifest_sha256"
    }


def manifest_sha256(payload: Mapping[str, Any]) -> str:
    return fingerprint(manifest_identity(payload))


@dataclass(frozen=True)
class GitIdentity:
    git_commit: str
    git_dirty: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _run(command: Sequence[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        list(command),
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.stdout.strip()


def capture_git_identity(repo_root: Path) -> GitIdentity:
    """Read the actual full commit SHA and working-tree state from Git."""

    try:
        commit = _run(("git", "rev-parse", "--verify", "HEAD"), cwd=repo_root)
        status = _run(
            ("git", "status", "--porcelain=v1", "--untracked-files=normal"),
            cwd=repo_root,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"Unable to resolve the source-code revision in {repo_root}") from exc
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise RuntimeError(f"Git returned an invalid full commit SHA: {commit!r}")
    return GitIdentity(git_commit=commit, git_dirty=bool(status))


def require_official_git(identity: GitIdentity, *, allow_dirty_nonofficial: bool) -> bool:
    """Return whether the run remains official, rejecting dirty trees by default."""

    if not identity.git_dirty:
        return True
    if allow_dirty_nonofficial:
        return False
    raise RuntimeError(
        "Official reproducible runs require a clean Git working tree. "
        "Commit or stash changes, or pass --allow-dirty-nonofficial for an explicitly "
        "non-official run."
    )


def _package_versions(distributions: Iterable[str]) -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for distribution in distributions:
        key = distribution.lower().replace("-", "_")
        try:
            versions[key] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[key] = None
    return versions


def _java_version() -> str | None:
    try:
        completed = subprocess.run(
            ["java", "-version"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    line = (completed.stderr or completed.stdout).splitlines()
    return line[0].strip() if line else None


def normalize_container_digest(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip().lower()
    if "@sha256:" in candidate:
        candidate = "sha256:" + candidate.rsplit("@sha256:", 1)[1]
    if not CONTAINER_DIGEST_PATTERN.fullmatch(candidate):
        raise ValueError("Container digest must be an immutable sha256:<64 lowercase hex> value")
    return candidate


def environment_identity(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Select logical environment fields; timestamps never affect the fingerprint."""

    identity = {
        "schema_version": manifest["schema_version"],
        "code": manifest["code"],
        "runtime": manifest["runtime"],
        "dependencies": manifest["dependencies"],
        "dependency_lock": manifest["dependency_lock"],
        "container": manifest["container"],
    }
    if "components" in manifest:
        identity["components"] = manifest["components"]
    return identity


def capture_environment_manifest(
    repo_root: Path,
    git_identity: GitIdentity,
    *,
    dependency_lock: Path,
    distributions: Iterable[str],
    container_image: str | None = None,
    container_digest: str | None = None,
    require_container_digest: bool = False,
    components: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture versions from the process that will execute training."""

    lock_path = dependency_lock.resolve()
    if not lock_path.is_file():
        raise FileNotFoundError(f"Dependency lock is missing: {lock_path}")
    digest = normalize_container_digest(container_digest or os.getenv("ML_CONTAINER_IMAGE_DIGEST"))
    image = (container_image or os.getenv("ML_CONTAINER_IMAGE") or "").strip() or None
    in_container = Path("/.dockerenv").exists()
    if require_container_digest and in_container and digest is None:
        raise RuntimeError(
            "Official container training requires ML_CONTAINER_IMAGE_DIGEST. "
            "Launch it through ml/run_official_container.py so Docker metadata is injected."
        )
    spark_version = _package_versions(("pyspark",))["pyspark"]
    manifest: dict[str, Any] = {
        "schema_version": "environment-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "code": git_identity.to_dict(),
        "runtime": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "java": _java_version(),
            "spark": spark_version,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "dependencies": _package_versions(distributions),
        "dependency_lock": {
            "path": lock_path.relative_to(repo_root.resolve()).as_posix(),
            "sha256": file_sha256(lock_path),
        },
        "container": {
            "runtime_detected": in_container,
            "image": image,
            "digest": digest,
            "digest_available": digest is not None,
        },
    }
    if components:
        manifest["components"] = dict(components)
    manifest["environment_fingerprint"] = fingerprint(environment_identity(manifest))
    return manifest


def validate_environment_manifest(manifest: Mapping[str, Any]) -> None:
    expected = str(manifest.get("environment_fingerprint") or "")
    actual = fingerprint(environment_identity(manifest))
    if not SHA256_PATTERN.fullmatch(expected) or expected != actual:
        raise ValueError("Environment manifest fingerprint does not match its canonical identity")


def split_fingerprint(
    train_ids: Iterable[str],
    holdout_ids: Iterable[str],
    *,
    strategy: str | None = None,
    group_column: str | None = None,
    seed: int | None = None,
    test_size: float | None = None,
    id_column: str | None = None,
) -> str:
    train = sorted(str(value) for value in train_ids)
    holdout = sorted(str(value) for value in holdout_ids)
    if len(train) != len(set(train)) or len(holdout) != len(set(holdout)):
        raise ValueError("Split observation identifiers must be unique within each partition")
    overlap = set(train).intersection(holdout)
    if overlap:
        raise ValueError(f"Split partitions overlap on {len(overlap)} observation identifiers")
    identity: dict[str, Any] = {
        "train_content_ids": train,
        "holdout_content_ids": holdout,
    }
    semantic_metadata = {
        "strategy": strategy,
        "group_column": group_column,
        "seed": seed,
        "test_size": test_size,
        "id_column": id_column,
    }
    if any(value is not None for value in semantic_metadata.values()):
        if any(value is None for value in semantic_metadata.values()):
            raise ValueError("Complete split metadata is required when fingerprinting its contract")
        identity.update(semantic_metadata)
    return fingerprint(identity)


def build_split_manifest(
    train_ids: Iterable[str],
    holdout_ids: Iterable[str],
    *,
    strategy: str,
    group_column: str,
    seed: int,
    test_size: float,
    id_column: str,
) -> dict[str, Any]:
    train = sorted(str(value) for value in train_ids)
    holdout = sorted(str(value) for value in holdout_ids)
    result: dict[str, Any] = {
        "schema_version": "split-v1",
        "strategy": strategy,
        "group_column": group_column,
        "id_column": id_column,
        "seed": int(seed),
        "test_size": float(test_size),
        "train_content_ids": train,
        "holdout_content_ids": holdout,
        "train_count": len(train),
        "holdout_count": len(holdout),
    }
    result["split_fingerprint"] = split_fingerprint(
        train,
        holdout,
        strategy=strategy,
        group_column=group_column,
        seed=int(seed),
        test_size=float(test_size),
        id_column=id_column,
    )
    return result


def validate_split_manifest(manifest: Mapping[str, Any]) -> None:
    expected = str(manifest.get("split_fingerprint") or "")
    actual = split_fingerprint(
        manifest.get("train_content_ids", []),
        manifest.get("holdout_content_ids", []),
        strategy=str(manifest.get("strategy") or ""),
        group_column=str(manifest.get("group_column") or ""),
        seed=int(manifest.get("seed")),
        test_size=float(manifest.get("test_size")),
        id_column=str(manifest.get("id_column") or ""),
    )
    if not SHA256_PATTERN.fullmatch(expected) or expected != actual:
        raise ValueError("Split fingerprint does not match the persisted partition")


EXPERIMENT_ID_FIELDS = (
    "dataset_version",
    "dataset_fingerprint",
    "manifest_sha256",
    "git_commit",
    "environment_fingerprint",
    "training_config_fingerprint",
    "split_fingerprint",
)


def experiment_id(lineage: Mapping[str, Any]) -> str:
    identity = {field: lineage[field] for field in EXPERIMENT_ID_FIELDS}
    return f"experiment-v1-{fingerprint(identity)[:24]}"


def compact_lineage(lineage: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "experiment_id",
        "dataset_version",
        "dataset_fingerprint",
        "git_commit",
        "environment_fingerprint",
        "training_config_fingerprint",
        "split_fingerprint",
        "manifest_sha256",
    )
    compact = {field: lineage[field] for field in fields}
    if "virality_contract_fingerprint" in lineage:
        compact["virality_contract_fingerprint"] = lineage["virality_contract_fingerprint"]
    return compact


def validate_lineage_match(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    *,
    fields: Iterable[str] = EXPERIMENT_ID_FIELDS,
    context: str,
) -> None:
    mismatches = [field for field in fields if expected.get(field) != actual.get(field)]
    if mismatches:
        raise ValueError(f"{context} lineage mismatch: {', '.join(mismatches)}")


def current_python() -> str:
    """Expose the exact executable for diagnostics without fingerprinting its host path."""

    return sys.executable
