import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

spec = importlib.util.spec_from_file_location(
    "fixture", Path(__file__).with_name("sqlserver_fixture.py")
)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class FixtureTests(unittest.TestCase):
    def setUp(self):
        output = patch("sys.stdout", new=io.StringIO())
        output.start()
        self.addCleanup(output.stop)

    def context(self, root, **overrides):
        value = {
            "schema_version": 2,
            "root": str(root),
            "created_files": False,
            "created_container": False,
            "started_container": False,
            "ephemeral": False,
            "container_id": "a" * 64,
            "ready": True,
        }
        value.update(overrides)
        return value

    def container(self, root, *, running=True, container_id=None):
        container_id = container_id or "a" * 64
        return {
            "Id": container_id,
            "Name": "/" + fixture.container_name(root),
            "Image": "sha256:image",
            "Config": {
                "Hostname": fixture.CONTAINER_HOSTNAME,
                "Env": [
                    "ACCEPT_EULA=Y",
                    "MSSQL_PID=EnterpriseDeveloper",
                    "MSSQL_ENABLE_HADR=1",
                    "MSSQL_MEMORY_LIMIT_MB=2048",
                    "MSSQL_SA_PASSWORD=test",
                ],
                "Labels": {
                    fixture.LABEL_MANAGED: "observe-only",
                    fixture.LABEL_ROOT: fixture.root_fingerprint(root),
                    fixture.LABEL_IMAGE: fixture.IMAGE_DIGEST,
                },
            },
            "State": {"Running": running},
            "HostConfig": {
                "Memory": fixture.CONTAINER_MEMORY,
                "MemorySwap": fixture.CONTAINER_MEMORY,
                "RestartPolicy": {"Name": "no"},
                "PortBindings": {"1433/tcp": [{"HostIp": "127.0.0.1", "HostPort": "1433"}]},
            },
            "Mounts": [
                {"Source": source, "Destination": destination, "RW": writable}
                for source, (destination, writable) in fixture.expected_mounts(root).items()
            ],
        }

    def test_runner_root_is_restricted_to_disposable_hosted_runners(self):
        env = {
            "GITHUB_ACTIONS": "true",
            "RUNNER_ENVIRONMENT": "github-hosted",
            "RUNNER_OS": "Linux",
            "RUNNER_ARCH": "X64",
            "RUNNER_TEMP": "/tmp/owned",
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(fixture.runner_root(), Path("/tmp/owned/sqlserver-observer"))
        for name, value in [
            ("GITHUB_ACTIONS", "false"),
            ("RUNNER_ENVIRONMENT", "self-hosted"),
            ("RUNNER_OS", "Windows"),
            ("RUNNER_ARCH", "ARM64"),
            ("RUNNER_TEMP", "relative"),
        ]:
            with patch.dict(os.environ, {**env, name: value}, clear=True):
                with self.assertRaises(fixture.FixtureError):
                    fixture.runner_root()

    def test_fixture_environment_uses_only_the_digest_pinned_image(self):
        env = fixture.fixture_environment(Path("/private/fixture"))
        self.assertEqual(env["SQLSERVER_TEST_IMAGE"], fixture.IMAGE)
        self.assertNotIn("SQLSERVER_TEST_PACKAGE_VERSION", env)
        self.assertNotIn("SQLSERVER_TEST_PACKAGE_SHA256", env)
        self.assertEqual(env["SQLSERVER_TEST_EULA_ACCEPTED"], "true")

    def test_artifact_checksum_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "artifact"
            archive.write_bytes(b"fixture artifact")
            fixture.verify_digest(archive, hashlib.sha256(b"fixture artifact").hexdigest())
            with self.assertRaises(fixture.FixtureError):
                fixture.verify_digest(archive, "0" * 64)

    def test_credentials_are_private_exact_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "password"
            fixture.secret_file(path, " fixture whitespace ")
            self.assertEqual(path.read_text(), " fixture whitespace ")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                fixture.secret_file(path, "replacement")

    def test_container_name_is_stable_and_fixture_specific(self):
        left = fixture.container_name(Path("/tmp/left"))
        self.assertEqual(left, fixture.container_name(Path("/tmp/left")))
        self.assertNotEqual(left, fixture.container_name(Path("/tmp/right")))
        self.assertTrue(left.startswith("kuberic-mssql-observer-"))

    def test_write_configs_points_host_tests_at_loopback_and_separate_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.write_configs(root)
            absent = json.loads((root / "absent.json").read_text())
            denied = json.loads((root / "denied.json").read_text())
            bad_tls = json.loads((root / "bad-tls.json").read_text())
            self.assertEqual(absent["host"], "localhost")
            self.assertEqual(absent["expected_server_name"], fixture.CONTAINER_HOSTNAME)
            self.assertNotEqual(absent["observer_username_file"], denied["observer_username_file"])
            self.assertNotEqual(absent["observer_password_file"], denied["observer_password_file"])
            self.assertNotEqual(absent["ca_certificate_file"], bad_tls["ca_certificate_file"])
            self.assertEqual(absent["incarnation"], denied["incarnation"])

    def test_image_metadata_requires_exact_digest_version_architecture_and_os(self):
        good = [{
            "Id": "sha256:image",
            "Architecture": "amd64",
            "Os": "linux",
            "RepoDigests": [fixture.IMAGE],
            "Config": {"Labels": {"com.microsoft.version": fixture.ENGINE_VERSION}},
        }]
        with patch.object(
            fixture, "docker", return_value=subprocess.CompletedProcess([], 0, json.dumps(good), "")
        ):
            self.assertEqual(fixture.image_id(), "sha256:image")
        for keys, value in [
            (["Architecture"], "arm64"),
            (["Os"], "windows"),
            (["RepoDigests"], ["other@sha256:" + "0" * 64]),
            (["Config", "Labels", "com.microsoft.version"], "17.0.1000.1"),
        ]:
            changed = copy.deepcopy(good)
            target = changed[0]
            for key in keys[:-1]:
                target = target[key]
            target[keys[-1]] = value
            with patch.object(
                fixture, "docker", return_value=subprocess.CompletedProcess([], 0, json.dumps(changed), "")
            ):
                with self.assertRaises(fixture.FixtureError):
                    fixture.image_id()

    def test_container_verification_requires_exact_image_labels_port_memory_and_mounts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = self.container(root)
            with patch.object(fixture, "image_id", return_value="sha256:image"):
                self.assertEqual(fixture.verify_container(root, metadata), ("a" * 64, True))
                changes = [
                    (["Config", "Labels", fixture.LABEL_ROOT], "wrong"),
                    (["Config", "Hostname"], "wrong"),
                    (["Image"], "sha256:other"),
                    (["HostConfig", "Memory"], 0),
                    (["HostConfig", "PortBindings", "1433/tcp", 0, "HostIp"], "0.0.0.0"),
                    (["State", "Running"], False),
                ]
                for keys, value in changes:
                    changed = copy.deepcopy(metadata)
                    target = changed
                    for key in keys[:-1]:
                        target = target[key]
                    target[keys[-1]] = value
                    with self.assertRaises(fixture.FixtureError):
                        fixture.verify_container(root, changed)

    def test_context_is_private_exact_and_rejects_unknown_fields_or_bad_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(root, self.context(root))
            self.assertEqual(fixture.load_context(root), self.context(root))
            changed = self.context(root)
            changed["unknown"] = True
            fixture.save_context(root, changed)
            with self.assertRaises(fixture.FixtureError):
                fixture.load_context(root)
            changed = self.context(root, container_id="invalid")
            fixture.save_context(root, changed)
            with self.assertRaises(fixture.FixtureError):
                fixture.load_context(root)

    def test_fresh_ensure_prepares_files_creates_container_initializes_logins_and_becomes_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"

            def prepare(target):
                self.assertTrue((target / "owner").is_file())

            with patch.object(fixture, "prepare_fixture_files", side_effect=prepare) as files, patch.object(
                fixture, "local_fixture", return_value=root
            ), patch.object(fixture, "docker_inspect", side_effect=[None, self.container(root)]), patch.object(
                fixture, "create_container", return_value="a" * 64
            ) as create, patch.object(fixture, "initialize_logins") as logins, patch.object(
                fixture, "wait_for_container"
            ), patch.object(
                fixture, "image_id", return_value="sha256:image"
            ):
                context = fixture.ensure_fixture(root)
            files.assert_called_once_with(root)
            create.assert_called_once_with(root)
            logins.assert_called_once_with(root)
            self.assertTrue(context["ready"])
            self.assertTrue(context["created_files"])
            self.assertTrue(context["created_container"])
            self.assertTrue(context["started_container"])

    def test_existing_running_container_is_borrowed_and_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "owner").write_text(fixture.OWNER)
            with patch.object(fixture, "local_fixture", return_value=root), patch.object(
                fixture, "docker_inspect", side_effect=[self.container(root), self.container(root)]
            ), patch.object(fixture, "verify_container", return_value=("a" * 64, True)), patch.object(
                fixture, "wait_for_container"
            ), patch.object(fixture, "start_container") as start:
                context = fixture.ensure_fixture(root)
                start.assert_not_called()
                self.assertFalse(context["created_container"])
                self.assertFalse(context["started_container"])
            with patch.object(fixture, "stop_exact_container") as stop, patch.object(
                fixture, "remove_exact_container"
            ) as remove:
                fixture.release_fixture(root)
                stop.assert_not_called()
                remove.assert_not_called()

    def test_existing_stopped_container_is_started_then_stopped_but_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "owner").write_text(fixture.OWNER)
            stopped = self.container(root, running=False)
            running = self.container(root)
            with patch.object(fixture, "local_fixture", return_value=root), patch.object(
                fixture, "docker_inspect", side_effect=[stopped, running]
            ), patch.object(fixture, "verify_container", side_effect=[("a" * 64, False), ("a" * 64, True)]), patch.object(
                fixture, "start_container"
            ) as start, patch.object(fixture, "wait_for_container"):
                context = fixture.ensure_fixture(root)
                start.assert_called_once_with(root, "a" * 64)
                self.assertTrue(context["started_container"])
            with patch.object(fixture, "stop_exact_container") as stop:
                fixture.release_fixture(root)
                stop.assert_called_once_with(root, "a" * 64)

    def test_created_container_is_removed_by_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(
                root,
                self.context(root, created_container=True, started_container=True),
            )
            with patch.object(fixture, "remove_exact_container") as remove:
                fixture.release_fixture(root)
                remove.assert_called_once_with(root, "a" * 64)
            self.assertFalse((root / "fixture-run.json").exists())

    def test_replacement_container_id_is_refused_before_stop_or_remove(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = self.container(root, container_id="b" * 64)
            with patch.object(fixture, "docker_inspect", return_value=metadata), patch.object(
                fixture, "image_id", return_value="sha256:image"
            ), patch.object(fixture, "docker") as docker:
                with self.assertRaises(fixture.FixtureError):
                    fixture.stop_exact_container(root, "a" * 64)
                docker.assert_not_called()

    def test_run_test_cases_exports_image_and_runs_cargo_on_host(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(root, self.context(root))
            completed = subprocess.CompletedProcess([], 0, "tests ran\n", "")
            with patch.object(fixture, "local_fixture", return_value=root), patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(fixture, "verify_container", return_value=("a" * 64, True)), patch.object(
                fixture, "wait_for_container"
            ), patch.object(fixture, "command", return_value=completed) as command:
                fixture.run_test_cases(root)
            args = command.call_args.args[1]
            env = command.call_args.kwargs["env"]
            self.assertEqual(args[0:3], ["cargo", "test", "--locked"])
            self.assertEqual(env["SQLSERVER_TEST_IMAGE"], fixture.IMAGE)
            self.assertNotIn("SQLSERVER_TEST_PACKAGE_VERSION", env)

    def test_interruption_kills_and_drains_the_owned_host_test_process_group(self):
        process = MagicMock()
        process.pid = 123
        process.communicate.side_effect = [KeyboardInterrupt(), ("", "")]
        manager = MagicMock()
        manager.__enter__.return_value = process
        with patch.object(fixture.subprocess, "Popen", return_value=manager), patch.object(
            fixture.os, "killpg"
        ) as kill:
            with self.assertRaises(KeyboardInterrupt):
                fixture.command("host tests", ["just", "test-live"], process_group=True)
            kill.assert_called_once_with(123, fixture.signal.SIGKILL)
            self.assertEqual(process.communicate.call_count, 2)

    def test_cli_verification_requires_exact_container_engine_and_fresh_absence(self):
        now = time.time_ns() // 1_000_000
        report = {
            "schema_version": 1,
            "fresh": True,
            "max_age_millis": 60_000,
            "source": {"expected_server_name": fixture.CONTAINER_HOSTNAME},
            "observation": {
                "status": "present",
                "observed_at_unix_millis": now,
                "value": {
                    "instance": {
                        "product_version": fixture.ENGINE_VERSION,
                        "product_major_version": 17,
                        "engine_edition": 3,
                        "edition": "Enterprise Developer Edition (64-bit)",
                        "hadr_enabled": True,
                        "host_platform": "Linux",
                        "architecture": "x86_64",
                        "server_name": fixture.CONTAINER_HOSTNAME,
                    },
                    "availability_group": {"status": "absent"},
                },
            },
        }
        with tempfile.TemporaryDirectory() as directory, patch.object(
            fixture.time, "time_ns", return_value=now * 1_000_000
        ):
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(report))
            fixture.verify_cli(path)
            for keys, value in [
                (["fresh"], False),
                (["observation", "observed_at_unix_millis"], now - 60_001),
                (["observation", "value", "instance", "product_version"], "17.0.1.1"),
                (["observation", "value", "instance", "hadr_enabled"], False),
                (["observation", "value", "availability_group", "status"], "present"),
            ]:
                changed = copy.deepcopy(report)
                target = changed
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                path.write_text(json.dumps(changed))
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_cli(path)


if __name__ == "__main__":
    unittest.main()
