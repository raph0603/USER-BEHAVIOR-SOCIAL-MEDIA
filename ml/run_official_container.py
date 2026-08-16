"""Launch training with Docker's immutable image identity injected into the run."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        text=True,
        capture_output=capture,
        encoding="utf-8",
        errors="replace",
    )


def resolve_image_identity(service: str, *, build: bool) -> tuple[str, str, str]:
    if build:
        _run(["docker", "compose", "build", service])
    configured = _run(
        ["docker", "compose", "config", "--images", service], capture=True
    ).stdout.splitlines()
    images = [line.strip() for line in configured if line.strip()]
    if len(images) != 1:
        raise RuntimeError(f"Expected one configured image for {service}, received {images}")
    image = images[0]
    try:
        inspected = json.loads(_run(["docker", "image", "inspect", image], capture=True).stdout)[0]
    except (subprocess.CalledProcessError, IndexError, KeyError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Unable to inspect {image}; build or pull the image before an official run"
        ) from exc
    repo_digests = inspected.get("RepoDigests") or []
    if repo_digests:
        digest = str(repo_digests[0]).rsplit("@", 1)[-1]
    else:
        digest = str(inspected.get("Id") or "")
    if not digest.startswith("sha256:"):
        raise RuntimeError(f"Docker did not expose an immutable digest for {image}")
    image_id = str(inspected.get("Id") or "")
    if not image_id.startswith("sha256:"):
        raise RuntimeError(f"Docker did not expose a local immutable image ID for {image}")
    return image, digest, image_id


def validate_created_container(container_id: str, expected_image_id: str) -> None:
    """Prove that the created one-off container uses the image inspected above."""

    inspected = json.loads(
        _run(["docker", "container", "inspect", container_id], capture=True).stdout
    )[0]
    actual_image_id = str(inspected.get("Image") or "")
    if actual_image_id != expected_image_id:
        raise RuntimeError(
            "Docker image changed between identity resolution and container creation: "
            f"expected {expected_image_id}, created {actual_image_id or '<unknown>'}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a Compose training service with its resolved image digest."
    )
    parser.add_argument("--service", default="ai-trainer")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise ValueError("A container command is required after --")
    image, digest, image_id = resolve_image_identity(args.service, build=args.build)
    container_name = f"experiment-run-{uuid4().hex[:12]}"
    container_id = ""
    try:
        # The shell gate prevents the requested training command from starting until
        # the created container's immutable image ID has been inspected and matched.
        created = _run(
            [
                "docker",
                "compose",
                "run",
                "--detach",
                "--pull",
                "never",
                "--name",
                container_name,
                "--entrypoint",
                "/bin/sh",
                "--env",
                f"ML_CONTAINER_IMAGE={image}",
                "--env",
                f"ML_CONTAINER_IMAGE_DIGEST={digest}",
                args.service,
                "-c",
                'while [ ! -e /tmp/experiment-image-verified ]; do sleep 0.05; done; exec "$@"',
                "experiment-image-gate",
                *command,
            ],
            capture=True,
        )
        container_id = created.stdout.strip()
        if not container_id:
            raise RuntimeError("Docker Compose did not return the created container ID")
        validate_created_container(container_id, image_id)
        _run(["docker", "exec", container_id, "touch", "/tmp/experiment-image-verified"])
        _run(["docker", "logs", "--follow", container_id])
        exit_code = int(_run(["docker", "wait", container_id], capture=True).stdout.strip())
        return exit_code
    finally:
        if container_id:
            subprocess.run(
                ["docker", "rm", "--force", container_id],
                cwd=PROJECT_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )


if __name__ == "__main__":
    raise SystemExit(main())
