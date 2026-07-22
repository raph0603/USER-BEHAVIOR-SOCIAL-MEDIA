#!/usr/bin/env python3
"""Create and restore portable snapshots of all Compose data volumes."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


ARCHIVE_FORMAT = "user-behavior-social-media-stack"
ARCHIVE_VERSION = 1
DEFAULT_PROJECT = "user-behavior-social-media"
DEFAULT_HELPER_IMAGE = "alpine:3.20"
ARCHIVE_FILE_PATTERN = re.compile(r"^[0-9]{4,}\.tar$")


class TransferError(RuntimeError):
    """Raised when a stack snapshot cannot be completed safely."""


def _log(message):
    print(message, file=sys.stderr)


def _docker(arguments, *, input_bytes=None, check=True):
    command = ["docker", *arguments]
    try:
        return subprocess.run(
            command,
            input=input_bytes,
            capture_output=True,
            check=check,
        )
    except FileNotFoundError as exc:
        raise TransferError("Docker CLI is not installed or not on PATH") from exc
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode("utf-8", errors="replace").strip()
        raise TransferError(
            f"Docker command failed: {' '.join(command)}: {detail}"
        ) from exc


def _docker_json(arguments):
    result = _docker(arguments)
    output = result.stdout.decode("utf-8")
    return json.loads(output) if output.strip() else []


def _project_containers(project):
    result = _docker(
        [
            "container",
            "ls",
            "--all",
            "--quiet",
            "--filter",
            f"label=com.docker.compose.project={project}",
        ]
    )
    container_ids = result.stdout.decode("utf-8").split()
    if not container_ids:
        raise TransferError(
            f"no Compose containers found for project {project!r}; "
            "run `docker compose create` first"
        )
    return _docker_json(["container", "inspect", *container_ids])


def _volume_labels(volume_name, cache):
    if volume_name not in cache:
        inspected = _docker_json(["volume", "inspect", volume_name])
        cache[volume_name] = (inspected[0].get("Labels") or {}) if inspected else {}
    return cache[volume_name]


def logical_volume_id(mount, labels, service, container_number):
    """Return a stable identifier independent of Docker's generated names."""
    compose_volume = labels.get("com.docker.compose.volume")
    if compose_volume:
        return f"compose:{compose_volume}"
    destination = mount.get("Destination") or "unknown"
    return f"service:{service}:{container_number}:{destination}"


def build_volume_inventory(containers):
    """Map every persistent project volume to a portable logical identity."""
    volume_cache = {}
    inventory = {}
    excluded_binds = []
    running_services = set()

    for container in containers:
        labels = container.get("Config", {}).get("Labels") or {}
        if str(labels.get("com.docker.compose.oneoff", "false")).lower() == "true":
            continue
        service = labels.get("com.docker.compose.service", "unknown")
        container_number = labels.get("com.docker.compose.container-number", "1")
        if container.get("State", {}).get("Running"):
            running_services.add(service)

        for mount in container.get("Mounts") or []:
            if mount.get("Type") == "bind":
                excluded_binds.append(
                    {
                        "service": service,
                        "destination": mount.get("Destination"),
                        "reason": "host bind mounts are managed outside Docker volumes",
                    }
                )
                continue
            if mount.get("Type") != "volume":
                continue
            source = mount.get("Name") or mount.get("Source")
            labels_for_volume = _volume_labels(source, volume_cache)
            logical_id = logical_volume_id(
                mount,
                labels_for_volume,
                service,
                container_number,
            )
            entry = {
                "logical_id": logical_id,
                "source_volume": source,
                "service": service,
                "container_number": str(container_number),
                "destination": mount.get("Destination"),
                "compose_volume": labels_for_volume.get(
                    "com.docker.compose.volume"
                ),
            }
            existing = inventory.get(logical_id)
            if existing and existing["source_volume"] != source:
                raise TransferError(
                    f"ambiguous target for {logical_id}: "
                    f"{existing['source_volume']} and {source}"
                )
            inventory[logical_id] = entry

    return {
        "volumes": [inventory[key] for key in sorted(inventory)],
        "running_services": sorted(running_services),
        "excluded_bind_mounts": sorted(
            excluded_binds,
            key=lambda item: (item["service"], item["destination"] or ""),
        ),
    }


def discover_volume_inventory(project):
    """Discover attached volumes and detached named Compose volumes."""
    inventory = build_volume_inventory(_project_containers(project))
    volumes_by_id = {
        volume["logical_id"]: volume for volume in inventory["volumes"]
    }
    result = _docker(
        [
            "volume",
            "ls",
            "--quiet",
            "--filter",
            f"label=com.docker.compose.project={project}",
        ]
    )
    volume_names = result.stdout.decode("utf-8").split()
    if volume_names:
        for inspected in _docker_json(["volume", "inspect", *volume_names]):
            labels = inspected.get("Labels") or {}
            compose_volume = labels.get("com.docker.compose.volume")
            if not compose_volume:
                continue
            logical_id = f"compose:{compose_volume}"
            source = inspected.get("Name")
            existing = volumes_by_id.get(logical_id)
            if existing and existing["source_volume"] != source:
                raise TransferError(
                    f"ambiguous target for {logical_id}: "
                    f"{existing['source_volume']} and {source}"
                )
            if existing:
                continue
            volumes_by_id[logical_id] = {
                "logical_id": logical_id,
                "source_volume": source,
                "service": None,
                "container_number": None,
                "destination": None,
                "compose_volume": compose_volume,
            }
    inventory["volumes"] = [
        volumes_by_id[key] for key in sorted(volumes_by_id)
    ]
    return inventory


def _ensure_helper_image(image):
    result = _docker(["image", "inspect", image], check=False)
    if result.returncode == 0:
        return
    _log(f"Pulling snapshot helper image {image}...")
    _docker(["pull", image])


def _create_staging_volume(project):
    safe_project = "".join(
        character if character.isalnum() or character in "_.-" else "-"
        for character in project
    )
    volume_name = f"{safe_project}-transfer-{uuid.uuid4().hex}"
    _docker(["volume", "create", volume_name])
    return volume_name


def _remove_staging_volume(volume_name):
    _docker(["volume", "rm", "--force", volume_name], check=False)


def _archive_volume(source, staging, archive_file, helper_image):
    _docker(
        [
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={source},dst=/source,readonly",
            "--mount",
            f"type=volume,src={staging},dst=/archive",
            "--env",
            f"ARCHIVE_FILE={archive_file}",
            helper_image,
            "sh",
            "-eu",
            "-c",
            'mkdir -p /archive/volumes && tar -cf "/archive/volumes/$ARCHIVE_FILE" -C /source .',
        ]
    )


def _write_manifest(staging, manifest, helper_image):
    payload = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    _docker(
        [
            "run",
            "--rm",
            "--interactive",
            "--mount",
            f"type=volume,src={staging},dst=/archive",
            helper_image,
            "sh",
            "-eu",
            "-c",
            "cat > /archive/manifest.json",
        ],
        input_bytes=payload,
    )


def _stream_archive(staging, output, helper_image):
    command = [
        "docker",
        "run",
        "--rm",
        "--mount",
        f"type=volume,src={staging},dst=/archive,readonly",
        helper_image,
        "tar",
        "-czf",
        "-",
        "-C",
        "/archive",
        "manifest.json",
        "volumes",
    ]
    output_stream = sys.stdout.buffer if output == "-" else None
    output_file = None
    if output_stream is None:
        output_path = Path(output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_file = output_path.open("wb")
        output_stream = output_file
    try:
        result = subprocess.run(command, stdout=output_stream, stderr=subprocess.PIPE)
    finally:
        if output_file is not None:
            output_file.close()
    if result.returncode:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise TransferError(f"unable to write snapshot archive: {detail}")


def export_snapshot(args):
    inventory = discover_volume_inventory(args.project)
    if inventory["running_services"] and not args.allow_running:
        services = ", ".join(inventory["running_services"])
        raise TransferError(
            "refusing an inconsistent live snapshot; stop the Compose stack first. "
            f"Still running: {services}. Use --allow-running only for diagnostics."
        )
    if not inventory["volumes"]:
        raise TransferError("the Compose project has no persistent volumes")

    _ensure_helper_image(args.helper_image)
    staging = _create_staging_volume(args.project)
    try:
        manifest_volumes = []
        total = len(inventory["volumes"])
        for index, volume in enumerate(inventory["volumes"], start=1):
            archive_file = f"{index:04d}.tar"
            _log(
                f"[{index}/{total}] Archiving {volume['logical_id']} "
                f"from {volume['source_volume']}..."
            )
            _archive_volume(
                volume["source_volume"],
                staging,
                archive_file,
                args.helper_image,
            )
            manifest_volumes.append({**volume, "archive_file": archive_file})

        manifest = {
            "format": ARCHIVE_FORMAT,
            "version": ARCHIVE_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_project": args.project,
            "consistent": not inventory["running_services"],
            "volumes": manifest_volumes,
            "excluded_bind_mounts": inventory["excluded_bind_mounts"],
            "notes": [
                "Environment files and host bind mounts are intentionally excluded.",
                "The archive includes all named and anonymous Docker data volumes.",
            ],
        }
        _write_manifest(staging, manifest, args.helper_image)
        _stream_archive(staging, args.output, args.helper_image)
        _log(f"Snapshot complete: {len(manifest_volumes)} volume(s) archived.")
    finally:
        _remove_staging_volume(staging)


def _extract_snapshot(staging, archive, helper_image):
    command = [
        "docker",
        "run",
        "--rm",
        "--interactive",
        "--mount",
        f"type=volume,src={staging},dst=/archive",
        helper_image,
        "tar",
        "-xzf",
        "-",
        "-C",
        "/archive",
    ]
    input_file = None
    input_stream = sys.stdin.buffer
    if archive != "-":
        archive_path = Path(archive).expanduser().resolve()
        if not archive_path.is_file():
            raise TransferError(f"snapshot file not found: {archive_path}")
        input_file = archive_path.open("rb")
        input_stream = input_file
    try:
        result = subprocess.run(command, stdin=input_stream, stderr=subprocess.PIPE)
    finally:
        if input_file is not None:
            input_file.close()
    if result.returncode:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise TransferError(f"invalid or unreadable snapshot archive: {detail}")


def _read_manifest(staging, helper_image):
    result = _docker(
        [
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={staging},dst=/archive,readonly",
            helper_image,
            "cat",
            "/archive/manifest.json",
        ]
    )
    try:
        manifest = json.loads(result.stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise TransferError("snapshot manifest is not valid JSON") from exc
    if manifest.get("format") != ARCHIVE_FORMAT:
        raise TransferError("unsupported snapshot format")
    if manifest.get("version") != ARCHIVE_VERSION:
        raise TransferError(
            f"unsupported snapshot version: {manifest.get('version')}"
        )
    volumes = manifest.get("volumes")
    if not isinstance(volumes, list) or not volumes:
        raise TransferError("snapshot manifest contains no volumes")
    logical_ids = set()
    archive_files = set()
    for volume in volumes:
        if not isinstance(volume, dict):
            raise TransferError("snapshot manifest contains an invalid volume")
        logical_id = volume.get("logical_id")
        archive_file = volume.get("archive_file")
        if not isinstance(logical_id, str) or not logical_id:
            raise TransferError("snapshot volume has no logical identifier")
        if logical_id in logical_ids:
            raise TransferError(f"duplicate snapshot volume: {logical_id}")
        if not isinstance(archive_file, str) or not ARCHIVE_FILE_PATTERN.fullmatch(
            archive_file
        ):
            raise TransferError(
                f"unsafe snapshot archive filename for {logical_id}: {archive_file!r}"
            )
        if archive_file in archive_files:
            raise TransferError(f"duplicate snapshot archive: {archive_file}")
        logical_ids.add(logical_id)
        archive_files.add(archive_file)
    return manifest


def _volume_is_empty(volume, helper_image):
    result = _docker(
        [
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={volume},dst=/target,readonly",
            helper_image,
            "sh",
            "-eu",
            "-c",
            'test -z "$(find /target -mindepth 1 -maxdepth 1 -print -quit)"',
        ],
        check=False,
    )
    return result.returncode == 0


def _restore_volume(target, staging, archive_file, helper_image, overwrite):
    if not _volume_is_empty(target, helper_image):
        if not overwrite:
            raise TransferError(
                f"target volume {target} is not empty; rerun with --overwrite "
                "only after verifying the target project and backup"
            )
        _docker(
            [
                "run",
                "--rm",
                "--mount",
                f"type=volume,src={target},dst=/target",
                helper_image,
                "sh",
                "-eu",
                "-c",
                "find /target -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +",
            ]
        )
    _docker(
        [
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={target},dst=/target",
            "--mount",
            f"type=volume,src={staging},dst=/archive,readonly",
            "--env",
            f"ARCHIVE_FILE={archive_file}",
            helper_image,
            "sh",
            "-eu",
            "-c",
            'tar -xf "/archive/volumes/$ARCHIVE_FILE" -C /target',
        ]
    )


def restore_snapshot(args):
    inventory = discover_volume_inventory(args.project)
    if inventory["running_services"]:
        services = ", ".join(inventory["running_services"])
        raise TransferError(
            "refusing to restore into a running stack; stop it first. "
            f"Still running: {services}"
        )

    _ensure_helper_image(args.helper_image)
    staging = _create_staging_volume(args.project)
    try:
        _extract_snapshot(staging, args.archive, args.helper_image)
        manifest = _read_manifest(staging, args.helper_image)
        targets = {
            volume["logical_id"]: volume for volume in inventory["volumes"]
        }
        archive_volumes = manifest.get("volumes") or []
        missing = [
            volume["logical_id"]
            for volume in archive_volumes
            if volume["logical_id"] not in targets
        ]
        if missing and not args.skip_missing:
            raise TransferError(
                "target project is missing volume mappings: " + ", ".join(missing)
            )

        restore_pairs = [
            (archived, targets[archived["logical_id"]])
            for archived in archive_volumes
            if archived["logical_id"] in targets
        ]
        if not args.overwrite:
            nonempty = [
                target["source_volume"]
                for _, target in restore_pairs
                if not _volume_is_empty(
                    target["source_volume"], args.helper_image
                )
            ]
            if nonempty:
                raise TransferError(
                    "target volumes are not empty: "
                    + ", ".join(nonempty)
                    + ". Rerun with --overwrite only after verifying the target "
                    "project and backup"
                )

        restored = 0
        total = len(archive_volumes)
        for index, archived in enumerate(archive_volumes, start=1):
            target = targets.get(archived["logical_id"])
            if target is None:
                _log(f"[{index}/{total}] Skipping {archived['logical_id']}.")
                continue
            _log(
                f"[{index}/{total}] Restoring {archived['logical_id']} "
                f"into {target['source_volume']}..."
            )
            _restore_volume(
                target["source_volume"],
                staging,
                archived["archive_file"],
                args.helper_image,
                args.overwrite,
            )
            restored += 1
        _log(f"Restore complete: {restored} volume(s) restored.")
    finally:
        _remove_staging_volume(staging)


def inspect_snapshot(args):
    _ensure_helper_image(args.helper_image)
    staging = _create_staging_volume(args.project)
    try:
        _extract_snapshot(staging, args.archive, args.helper_image)
        manifest = _read_manifest(staging, args.helper_image)
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
    finally:
        _remove_staging_volume(staging)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="stack-transfer",
        description="Snapshot and restore every persistent Compose data volume.",
    )
    parser.add_argument(
        "--project",
        default=os.getenv("COMPOSE_PROJECT_NAME", DEFAULT_PROJECT),
        help="Docker Compose project name.",
    )
    parser.add_argument(
        "--helper-image",
        default=os.getenv("STACK_TRANSFER_HELPER_IMAGE", DEFAULT_HELPER_IMAGE),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Create a full snapshot.")
    export_parser.add_argument(
        "--output",
        required=True,
        help="Archive destination, or - for standard output.",
    )
    export_parser.add_argument(
        "--allow-running",
        action="store_true",
        help="Allow a potentially inconsistent live snapshot.",
    )

    restore_parser = subparsers.add_parser("restore", help="Restore a snapshot.")
    restore_parser.add_argument(
        "--archive",
        required=True,
        help="Archive path, or - for standard input.",
    )
    restore_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete existing target volume contents before restoring.",
    )
    restore_parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip archive volumes that do not exist in the target Compose version.",
    )

    inspect_parser = subparsers.add_parser("inspect", help="Print a manifest.")
    inspect_parser.add_argument(
        "--archive",
        required=True,
        help="Archive path, or - for standard input.",
    )
    return parser


def main(argv=None):
    if shutil.which("docker") is None:
        print("Stack transfer failed: Docker CLI is not on PATH", file=sys.stderr)
        return 1
    args = build_parser().parse_args(argv)
    try:
        if args.command == "export":
            export_snapshot(args)
        elif args.command == "restore":
            restore_snapshot(args)
        else:
            inspect_snapshot(args)
    except TransferError as exc:
        print(f"Stack transfer failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
