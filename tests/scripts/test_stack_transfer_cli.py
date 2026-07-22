import json
import os
import shutil
import subprocess
import tempfile
import unittest
import uuid
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[2]

import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.stack_transfer_cli as stack_transfer


class StackTransferCliTests(unittest.TestCase):
    def test_named_compose_volume_uses_stable_logical_name(self):
        mount = {"Destination": "/data"}

        logical_id = stack_transfer.logical_volume_id(
            mount,
            {"com.docker.compose.volume": "minio-data"},
            "minio",
            "1",
        )

        self.assertEqual(logical_id, "compose:minio-data")

    def test_anonymous_volume_uses_service_number_and_destination(self):
        mount = {"Destination": "/var/lib/kafka/data"}

        logical_id = stack_transfer.logical_volume_id(
            mount,
            {},
            "kafka",
            "1",
        )

        self.assertEqual(
            logical_id,
            "service:kafka:1:/var/lib/kafka/data",
        )

    @patch("scripts.stack_transfer_cli._volume_labels")
    def test_inventory_includes_anonymous_volumes_and_excludes_binds(
        self,
        mock_volume_labels,
    ):
        mock_volume_labels.side_effect = [{"com.docker.compose.volume": "state"}, {}]
        containers = [
            {
                "Config": {
                    "Labels": {
                        "com.docker.compose.service": "collector",
                        "com.docker.compose.container-number": "1",
                    }
                },
                "State": {"Running": True},
                "Mounts": [
                    {
                        "Type": "volume",
                        "Name": "project_state",
                        "Destination": "/state",
                    },
                    {
                        "Type": "volume",
                        "Name": "anonymous-id",
                        "Destination": "/cache",
                    },
                    {
                        "Type": "bind",
                        "Source": "/var/run/docker.sock",
                        "Destination": "/var/run/docker.sock",
                    },
                ],
            },
            {
                "Config": {
                    "Labels": {
                        "com.docker.compose.oneoff": "True",
                        "com.docker.compose.service": "collector",
                    }
                },
                "State": {"Running": True},
                "Mounts": [
                    {
                        "Type": "volume",
                        "Name": "temporary",
                        "Destination": "/temporary",
                    }
                ],
            },
        ]

        result = stack_transfer.build_volume_inventory(containers)

        self.assertEqual(
            [volume["logical_id"] for volume in result["volumes"]],
            ["compose:state", "service:collector:1:/cache"],
        )
        self.assertEqual(result["running_services"], ["collector"])
        self.assertEqual(len(result["excluded_bind_mounts"]), 1)

    @patch("scripts.stack_transfer_cli._docker_json")
    @patch("scripts.stack_transfer_cli._docker")
    @patch("scripts.stack_transfer_cli._project_containers")
    @patch("scripts.stack_transfer_cli.build_volume_inventory")
    def test_discovery_adds_detached_named_compose_volume(
        self,
        mock_build_inventory,
        mock_project_containers,
        mock_docker,
        mock_docker_json,
    ):
        mock_project_containers.return_value = []
        mock_build_inventory.return_value = {
            "volumes": [],
            "running_services": [],
            "excluded_bind_mounts": [],
        }
        mock_docker.return_value = subprocess.CompletedProcess(
            [], 0, stdout=b"project_archive\n", stderr=b""
        )
        mock_docker_json.return_value = [
            {
                "Name": "project_archive",
                "Labels": {"com.docker.compose.volume": "archive"},
            }
        ]

        result = stack_transfer.discover_volume_inventory("project")

        self.assertEqual(len(result["volumes"]), 1)
        self.assertEqual(result["volumes"][0]["logical_id"], "compose:archive")
        self.assertEqual(
            result["volumes"][0]["source_volume"], "project_archive"
        )

    @patch("scripts.stack_transfer_cli._ensure_helper_image")
    @patch("scripts.stack_transfer_cli.discover_volume_inventory")
    def test_export_refuses_running_stack_before_creating_staging_volume(
        self,
        mock_inventory,
        mock_helper,
    ):
        mock_inventory.return_value = {
            "volumes": [{"logical_id": "compose:data"}],
            "running_services": ["minio"],
            "excluded_bind_mounts": [],
        }
        args = Namespace(
            project="project",
            helper_image="alpine:3.20",
            output="snapshot.tar.gz",
            allow_running=False,
        )

        with self.assertRaisesRegex(
            stack_transfer.TransferError,
            "inconsistent live snapshot",
        ):
            stack_transfer.export_snapshot(args)

        mock_helper.assert_not_called()

    @patch("scripts.stack_transfer_cli._docker")
    def test_manifest_rejects_unsafe_inner_archive_path(self, mock_docker):
        manifest = {
            "format": stack_transfer.ARCHIVE_FORMAT,
            "version": stack_transfer.ARCHIVE_VERSION,
            "volumes": [
                {
                    "logical_id": "compose:data",
                    "archive_file": "../../data.tar",
                }
            ],
        }
        mock_docker.return_value = subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(manifest).encode(), stderr=b""
        )

        with self.assertRaisesRegex(
            stack_transfer.TransferError,
            "unsafe snapshot archive filename",
        ):
            stack_transfer._read_manifest("staging", "alpine:3.20")

    def test_parser_exposes_full_export_restore_and_inspection(self):
        parser = stack_transfer.build_parser()

        export_args = parser.parse_args(["export", "--output", "backup.tar.gz"])
        restore_args = parser.parse_args(
            ["restore", "--archive", "backup.tar.gz", "--overwrite"]
        )
        inspect_args = parser.parse_args(["inspect", "--archive", "backup.tar.gz"])

        self.assertEqual(export_args.command, "export")
        self.assertEqual(restore_args.command, "restore")
        self.assertTrue(restore_args.overwrite)
        self.assertEqual(inspect_args.command, "inspect")


@unittest.skipUnless(
    os.getenv("RUN_DOCKER_INTEGRATION") == "1" and shutil.which("docker"),
    "set RUN_DOCKER_INTEGRATION=1 to exercise real Docker volumes",
)
class StackTransferDockerIntegrationTests(unittest.TestCase):
    def setUp(self):
        suffix = uuid.uuid4().hex[:8]
        self.project = f"ubsm-stack-transfer-test-{suffix}"
        self.container = f"{self.project}-service-1"
        self.source_named = f"{self.project}-state-source"
        self.target_named = f"{self.project}-state-target"
        self.source_anonymous = None
        self.target_anonymous = None
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.archive = str(
            Path(self.temporary_directory.name) / "stack-backup.tar.gz"
        )

    def tearDown(self):
        self._docker("rm", "--force", self.container, check=False)
        for volume in (
            self.source_anonymous,
            self.target_anonymous,
            self.source_named,
            self.target_named,
        ):
            if volume:
                self._docker("volume", "rm", "--force", volume, check=False)
        self.temporary_directory.cleanup()

    @staticmethod
    def _docker(*arguments, check=True):
        return subprocess.run(
            ["docker", *arguments],
            check=check,
            capture_output=True,
            text=True,
        )

    def _create_project_container(self, named_volume):
        self._docker(
            "create",
            "--name",
            self.container,
            "--label",
            f"com.docker.compose.project={self.project}",
            "--label",
            "com.docker.compose.service=kafka",
            "--label",
            "com.docker.compose.container-number=1",
            "--label",
            "com.docker.compose.oneoff=False",
            "--mount",
            f"type=volume,src={named_volume},dst=/state",
            "--mount",
            "type=volume,dst=/var/lib/kafka/data",
            stack_transfer.DEFAULT_HELPER_IMAGE,
            "sh",
        )
        inspected = json.loads(
            self._docker("container", "inspect", self.container).stdout
        )[0]
        return next(
            mount["Name"]
            for mount in inspected["Mounts"]
            if mount["Destination"] == "/var/lib/kafka/data"
        )

    def _create_named_volume(self, name):
        self._docker(
            "volume",
            "create",
            "--label",
            f"com.docker.compose.project={self.project}",
            "--label",
            "com.docker.compose.volume=state",
            name,
        )

    def _write_volume_file(self, volume, path, value):
        self._docker(
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={volume},dst=/data",
            stack_transfer.DEFAULT_HELPER_IMAGE,
            "sh",
            "-eu",
            "-c",
            f"printf %s {value} > /data/{path}",
        )

    def _read_volume_file(self, volume, path):
        return self._docker(
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={volume},dst=/data,readonly",
            stack_transfer.DEFAULT_HELPER_IMAGE,
            "cat",
            f"/data/{path}",
        ).stdout

    def test_snapshot_restores_named_and_anonymous_volumes_under_new_names(self):
        self._create_named_volume(self.source_named)
        self.source_anonymous = self._create_project_container(
            self.source_named
        )
        self._write_volume_file(
            self.source_named,
            "named.txt",
            "named-content",
        )
        self._write_volume_file(
            self.source_anonymous,
            "anonymous.txt",
            "anonymous-content",
        )

        export_result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "stack_transfer_cli.py"),
                "--project",
                self.project,
                "export",
                "--output",
                self.archive,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("2 volume(s) archived", export_result.stderr)

        self._docker("rm", "--force", self.container)
        self._docker(
            "volume",
            "rm",
            self.source_named,
            self.source_anonymous,
        )
        self.source_anonymous = None

        self._create_named_volume(self.target_named)
        self.target_anonymous = self._create_project_container(
            self.target_named
        )
        restore_result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "stack_transfer_cli.py"),
                "--project",
                self.project,
                "restore",
                "--archive",
                self.archive,
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("2 volume(s) restored", restore_result.stderr)
        self.assertEqual(
            self._read_volume_file(self.target_named, "named.txt"),
            "named-content",
        )
        self.assertEqual(
            self._read_volume_file(
                self.target_anonymous,
                "anonymous.txt",
            ),
            "anonymous-content",
        )


if __name__ == "__main__":
    unittest.main()
