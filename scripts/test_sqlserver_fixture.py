import copy
from contextlib import nullcontext
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import time
import unittest
from subprocess import CompletedProcess
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

    def test_helper_supplies_eula_acknowledgement_without_changing_caller_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            env = fixture.fixture_environment(Path("/tmp/fixture"))
            self.assertEqual(env["SQLSERVER_TEST_EULA_ACCEPTED"], "true")
            self.assertNotIn("SQLSERVER_TEST_EULA_ACCEPTED", os.environ)

    def test_provisioning_is_restricted_to_disposable_hosted_runners(self):
        env = {
            "GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
            "RUNNER_OS": "Linux", "RUNNER_ARCH": "X64", "RUNNER_TEMP": "/tmp/owned",
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(fixture.runner_root(), Path("/tmp/owned/sqlserver-observer"))
        for name, value in [
            ("GITHUB_ACTIONS", "false"), ("RUNNER_ENVIRONMENT", "self-hosted"),
            ("RUNNER_OS", "Windows"), ("RUNNER_ARCH", "ARM64"),
            ("RUNNER_TEMP", ""), ("RUNNER_TEMP", "relative"), ("RUNNER_TEMP", "/tmp\nother"),
        ]:
            with patch.dict(os.environ, {**env, name: value}, clear=True):
                with self.assertRaises(fixture.FixtureError):
                    fixture.runner_root()
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "sqlserver-observer").symlink_to("/tmp", target_is_directory=True)
            with patch.dict(os.environ, {**env, "RUNNER_TEMP": directory}, clear=True):
                with self.assertRaises(fixture.FixtureError):
                    fixture.runner_root()

    def test_artifact_checksum_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "engine.deb"
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

    def test_configs_reference_separate_principals_and_untrusted_ca(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.write_configs(root, "fixture-server")
            absent = json.loads((root / "absent.json").read_text())
            denied = json.loads((root / "denied.json").read_text())
            bad_tls = json.loads((root / "bad-tls.json").read_text())
            self.assertEqual(absent["mode"], "observe_only")
            self.assertEqual(absent["host"], "localhost")
            self.assertEqual(absent["expected_server_name"], "fixture-server")
            self.assertNotEqual(absent["observer_username_file"], denied["observer_username_file"])
            self.assertNotEqual(absent["observer_password_file"], denied["observer_password_file"])
            self.assertNotEqual(absent["ca_certificate_file"], bad_tls["ca_certificate_file"])
            self.assertEqual(absent["incarnation"], denied["incarnation"])
            self.assertNotIn("password", absent)

    def test_cleanup_requires_ownership_before_any_system_command(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(fixture, "fixture_root", return_value=Path(directory)), patch.object(
                fixture, "lifecycle_lock", return_value=nullcontext()
            ):
                with patch.object(fixture, "command") as command:
                    fixture.cleanup()
                    command.assert_not_called()

    def test_cleanup_stops_and_verifies_owned_engine_before_removing_credentials(self):
        stages = []

        def command(stage, args, **kwargs):
            stages.append(stage)
            status = 0
            if stage == "check server TLS directory":
                status = 1
            return CompletedProcess(args, status, "", "")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"
            root.mkdir()
            (root / "owner").write_text(fixture.OWNER)
            (root / "password").write_text("test credential")
            fixture.save_context(root, self.context(root, created=True, ephemeral=True))
            with patch.object(fixture, "fixture_root", return_value=root), patch.object(
                fixture, "lifecycle_lock", return_value=nullcontext()
            ), patch.object(fixture, "runner_root", return_value=root), patch.object(
                fixture, "stop_local_generation", side_effect=lambda _: stages.append("stop exact generation")
            ), patch.object(
                fixture, "command", side_effect=command
            ):
                fixture.cleanup()
            self.assertFalse(root.exists())
            self.assertEqual(stages[0], "stop exact generation")

    def test_cleanup_failure_does_not_discard_credentials_or_claim_shutdown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"
            root.mkdir()
            (root / "owner").write_text(fixture.OWNER)
            credential = root / "password"
            credential.write_text("test credential")
            fixture.save_context(root, self.context(root))
            with patch.object(fixture, "fixture_root", return_value=root), patch.object(
                fixture, "lifecycle_lock", return_value=nullcontext()
            ), patch.object(fixture, "stop_local_generation", side_effect=fixture.FixtureError("stop failed")):
                with self.assertRaises(fixture.FixtureError):
                    fixture.cleanup()
            self.assertTrue(credential.exists())

    def test_provisioning_refuses_existing_storage_before_system_commands(self):
        with patch.object(Path, "read_text", return_value='ID=ubuntu\nVERSION_ID="24.04"\n'):
            with patch.object(Path, "exists", return_value=True):
                with patch.object(fixture, "command") as command:
                    with self.assertRaises(fixture.FixtureError):
                        fixture.install_fixture(Path("/tmp/unused"), {})
                    command.assert_not_called()

    def test_provisioning_exports_only_artifact_metadata_and_config_paths(self):
        original_exists = Path.exists
        original_read = Path.read_text
        calls = []

        def exists(path):
            if str(path) in ["/opt/mssql/bin/sqlservr", "/var/opt/mssql"]:
                return False
            return original_exists(path)

        def read(path, *args, **kwargs):
            if str(path) == "/etc/os-release":
                return 'ID=ubuntu\nVERSION_ID="24.04"\n'
            return original_read(path, *args, **kwargs)

        def command(stage, args, **kwargs):
            calls.append((stage, args, kwargs))
            output = ""
            if stage == "verify installed engine":
                output = fixture.ENGINE_VERSION
            elif stage == "verified-TLS fixture administration":
                output = "fixture-server\n"
            elif stage == "verify loopback binding":
                output = "LISTEN 0 128 127.0.0.1:1433 0.0.0.0:*\n"
            return CompletedProcess(args, 0, output, "")

        def download(root, name, url, digest):
            path = root / name
            path.write_bytes(b"verified artifact")
            return path

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"
            environment = Path(directory) / "github-env"
            environment.touch()
            previous_umask = os.umask(0o077)
            try:
                with patch.dict(os.environ, {"GITHUB_ENV": str(environment)}):
                    with patch.object(Path, "exists", exists), patch.object(Path, "read_text", read):
                        with patch.object(fixture, "command", side_effect=command), patch.object(
                            fixture, "download", side_effect=download
                        ), patch.object(fixture, "certificates"), patch.object(fixture, "activate_fixture") as activate:
                            fixture.install_fixture(root, self.context(root, generation=None, ready=False))
                            self.assertEqual(activate.call_args.kwargs["env"]["ACCEPT_EULA"], "Y")
            finally:
                os.umask(previous_umask)
            exported = json.dumps(fixture.fixture_environment(root))
            self.assertIn("SQLSERVER_TEST_EULA_ACCEPTED", exported)
            self.assertIn(fixture.ENGINE_VERSION, exported)
            self.assertIn("SQLSERVER_LIVE_ABSENT_CONFIG", exported)
            self.assertIn("SQLSERVER_LIVE_DENIED_CONFIG", exported)
            self.assertIn("SQLSERVER_LIVE_BAD_TLS_CONFIG", exported)
            for prefix in ["observer", "denied"]:
                self.assertNotIn((root / f"{prefix}-password").read_text(), exported)
            for stage, args, kwargs in calls:
                if stage == "verified-TLS fixture administration":
                    self.assertNotIn("-C", args)
                    self.assertNotIn("-P", args)
                    self.assertEqual(kwargs["env"]["SSL_CERT_FILE"], str(root / "ca.crt"))
                if "stdin" in kwargs:
                    self.assertNotIn(kwargs["stdin"], args)

    def test_cli_verification_requires_exact_engine_and_fresh_absence(self):
        now = time.time_ns() // 1_000_000
        report = {
            "schema_version": 1, "fresh": True, "max_age_millis": 60_000,
            "source": {"expected_server_name": "fixture-server"},
            "observation": {
                "status": "present", "observed_at_unix_millis": now,
                "value": {
                    "instance": {
                        "product_version": fixture.ENGINE_VERSION.split("-")[0],
                        "product_major_version": 17, "engine_edition": 3,
                        "edition": "Enterprise Developer Edition (64-bit)",
                        "hadr_enabled": True, "host_platform": "Linux",
                        "architecture": "x86_64", "server_name": "fixture-server",
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
            boundary = copy.deepcopy(report)
            boundary["observation"]["observed_at_unix_millis"] = now - 60_000
            path.write_text(json.dumps(boundary))
            fixture.verify_cli(path)
            changes = [
                (["fresh"], False), (["schema_version"], 2),
                (["max_age_millis"], 60_001),
                (["observation", "observed_at_unix_millis"], now - 60_001),
                (["observation", "observed_at_unix_millis"], now + 60_000),
                (["observation", "status"], "failed"),
                (["observation", "value", "instance", "product_major_version"], 16),
                (["observation", "value", "instance", "product_version"], "17.0.1000.7"),
                (["observation", "value", "instance", "engine_edition"], 2),
                (["observation", "value", "instance", "edition"], "Standard Developer"),
                (["observation", "value", "instance", "hadr_enabled"], False),
                (["observation", "value", "instance", "host_platform"], "Windows"),
                (["observation", "value", "instance", "architecture"], "aarch64"),
                (["observation", "value", "instance", "server_name"], "another-server"),
                (["observation", "value", "availability_group", "status"], "present"),
            ]
            for keys, value in changes:
                changed = copy.deepcopy(report)
                target = changed
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                path.write_text(json.dumps(changed))
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_cli(path)
                path.write_text("not JSON")
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_cli(path)
            path.write_text(json.dumps({"fresh": True}))
            with self.assertRaises(fixture.FixtureError):
                fixture.verify_cli(path)

    def make_local_fixture(self, root):
        for name in ["ca.crt", "bad-ca.crt", "server.crt", "sqlcmd"]:
            (root / name).write_text("test artifact")
        for name, value in [
            ("observer-username", "kuberic_observer"), ("observer-password", "test password"),
            ("denied-username", "kuberic_denied"), ("denied-password", "denied password"),
        ]:
            fixture.secret_file(root / name, value)
        fixture.write_configs(root, "fixture-server")

    def test_local_fixture_validates_private_owned_files_and_exact_configs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_local_fixture(root)
            with patch.object(fixture, "verify_digest"):
                self.assertEqual(fixture.local_fixture(root), (root, "fixture-server"))
                config_path = root / "absent.json"
                config = json.loads(config_path.read_text())
                config["host"] = "unrelated.example"
                config_path.write_text(json.dumps(config))
                with self.assertRaises(fixture.FixtureError):
                    fixture.local_fixture(root)
                config_path.write_text(" " * 65_537)
                with self.assertRaisesRegex(fixture.FixtureError, "64 KiB"):
                    fixture.local_fixture(root)

    def test_local_fixture_refuses_exposed_or_symlinked_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_local_fixture(root)
            with patch.object(fixture, "verify_digest"):
                password = root / "observer-password"
                password.chmod(0o644)
                with self.assertRaises(fixture.FixtureError):
                    fixture.local_fixture(root)
                password.unlink()
                password.symlink_to(root / "denied-password")
                with self.assertRaises(fixture.FixtureError):
                    fixture.local_fixture(root)

    def test_local_service_requires_project_profile_and_certificate_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "server.crt").write_bytes(b"project certificate")
            settings = (
                "[network]\nipaddress=127.0.0.1\ntcpport=1433\nforceencryption=1\n"
                "tlsprotocols=1.2\ntlscert=/var/opt/mssql/secrets/kuberic-observer/server.crt\n"
                "tlskey=/var/opt/mssql/secrets/kuberic-observer/server.key\n"
                "[hadr]\nhadrenabled=1\n[memory]\nmemorylimitmb=2048\n"
            )
            digest = hashlib.sha256(b"project certificate").hexdigest()

            def commands(stage, args, **kwargs):
                output = {
                    "verify local engine package": fixture.ENGINE_VERSION,
                    "read local service settings": settings,
                    "verify service certificate binding": digest + "  server.crt\n",
                }.get(stage, "")
                return CompletedProcess(args, 0, output, "")

            with patch.object(fixture, "command", side_effect=commands):
                fixture.verify_local_service(root)
                settings = settings.replace("127.0.0.1", "0.0.0.0")
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_local_service(root)
                settings = settings.replace("0.0.0.0", "127.0.0.1")
                digest = "0" * 64
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_local_service(root)

    def test_loopback_binding_rejects_missing_malformed_and_exposed_listeners(self):
        for output in ["", "malformed", "LISTEN 0 128 0.0.0.0:1433 0.0.0.0:*\n"]:
            with patch.object(fixture, "command", return_value=CompletedProcess([], 0, output, "")):
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_loopback_binding()

    def test_local_lock_is_shared_across_fixture_directories_and_private(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(Path, "home", return_value=Path(directory)):
            lock = fixture.local_lock()
            self.assertEqual(lock, fixture.local_lock())
            self.assertEqual(lock.parent.stat().st_mode & 0o777, 0o700)
            lock.parent.chmod(0o755)
            with self.assertRaises(fixture.FixtureError):
                fixture.local_lock()

    def test_service_status_requires_exact_active_generation(self):
        for output in [
            "ActiveState=active\nMainPID=0\nInvocationID=\n",
            "ActiveState=activating\nMainPID=123\nInvocationID=" + "a" * 32,
            "ActiveState=active\nMainPID=123\nInvocationID=unknown\n",
        ]:
            with patch.object(fixture, "command", return_value=CompletedProcess([], 0, output, "")):
                with self.assertRaises(fixture.FixtureError):
                    fixture.service_status()

    def test_cleanup_refuses_a_replaced_local_generation(self):
        with patch.object(fixture, "service_status", return_value=("active", "b" * 32, "124")):
            with patch.object(fixture, "command") as command:
                with self.assertRaises(fixture.FixtureError):
                    fixture.stop_local_generation(("active", "a" * 32, "123"))
                command.assert_not_called()

    def exercise_local(self, root, original, *, failure=None):
        running = ("active", "a" * 32, "123")
        statuses = [original, running] if original[0] == "active" else [original, running, running]
        (root / "absent.json").touch()
        with patch.object(
            fixture, "local_fixture", return_value=(root, "fixture-server")
        ), patch.object(fixture, "local_lock", return_value=root / ".local.lock"), patch.object(
            fixture, "verify_local_service"
        ), patch.object(fixture, "verify_loopback_binding"), patch.object(
            fixture, "service_status", side_effect=statuses
        ), patch.object(fixture, "wait_for_local", side_effect=failure), patch.object(
            fixture, "stop_local_generation"
        ) as stop, patch.object(
            fixture, "command", return_value=CompletedProcess([], 0, "", "")
        ) as command:
            if failure is None:
                fixture.validate_fixture(root)
            else:
                with self.assertRaises(type(failure)):
                    fixture.validate_fixture(root)
            return stop.call_args_list, command.call_args_list

    def test_local_running_fixture_skips_start_and_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            stops, commands = self.exercise_local(Path(directory), ("active", "a" * 32, "123"))
            self.assertEqual(stops, [])
            self.assertFalse(any(call.args[0] == "start configured fixture" for call in commands))
            for call in commands:
                self.assertNotIn("install", call.args[1])
                self.assertNotIn("setup", call.args[1])
                self.assertEqual(call.kwargs["env"]["SQLSERVER_TEST_EULA_ACCEPTED"], "true")

    def test_local_stopped_fixture_is_started_and_its_generation_is_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            stops, commands = self.exercise_local(Path(directory), ("inactive", "", "0"))
            self.assertEqual(stops[0].args, (("active", "a" * 32, "123"),))
            self.assertTrue(any(call.args[0] == "start configured fixture" for call in commands))

    def test_local_failed_validation_and_interrupt_still_stop_owned_generation(self):
        for error in [fixture.FixtureError("identity mismatch"), KeyboardInterrupt()]:
            with tempfile.TemporaryDirectory() as directory:
                stops, _ = self.exercise_local(Path(directory), ("inactive", "", "0"), failure=error)
                self.assertEqual(stops[0].args, (("active", "a" * 32, "123"),))

    def test_local_failed_validation_does_not_stop_borrowed_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            stops, _ = self.exercise_local(
                Path(directory), ("active", "a" * 32, "123"), failure=fixture.FixtureError("TLS failed")
            )
            self.assertEqual(stops, [])

    def test_local_lifecycle_lock_refuses_concurrent_run_before_service_access(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(
                fixture, "local_fixture", return_value=(root, "fixture-server")
            ), patch.object(fixture, "local_lock", return_value=root / ".local.lock"), patch.object(
                fixture.fcntl, "flock", side_effect=BlockingIOError
            ), patch.object(
                fixture, "verify_local_service"
            ) as verify:
                with self.assertRaises(fixture.FixtureError):
                    fixture.validate_fixture(root)
                verify.assert_not_called()

    def context(self, root, *, generation=("active", "a" * 32, "123"), created=False, ephemeral=False, ready=True):
        return {
            "schema_version": 1, "root": str(root), "created": created,
            "ephemeral": ephemeral, "ready": ready,
            "generation": list(generation) if generation is not None else None,
        }

    def test_ready_provision_is_idempotent_and_does_not_install_start_or_test(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.context(root)
            fixture.save_context(root, context)
            with patch.object(fixture, "local_fixture", return_value=(root, "fixture-server")), patch.object(
                fixture, "verify_local_service"
            ), patch.object(fixture, "wait_for_local"), patch.object(
                fixture, "verify_loopback_binding"
            ), patch.object(fixture, "service_status", return_value=("active", "a" * 32, "123")), patch.object(
                fixture, "install_fixture"
            ) as install, patch.object(fixture, "activate_fixture") as activate, patch.object(
                fixture, "command"
            ) as command:
                self.assertEqual(fixture.ensure_fixture(root), context)
                install.assert_not_called()
                activate.assert_not_called()
                command.assert_not_called()

    def test_ready_record_rejects_replacement_before_testing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(root, self.context(root))
            with patch.object(fixture, "local_fixture", return_value=(root, "fixture-server")), patch.object(
                fixture, "verify_local_service"
            ), patch.object(fixture, "service_status", return_value=("active", "b" * 32, "124")):
                with self.assertRaises(fixture.FixtureError):
                    fixture.ensure_fixture(root)

    def test_borrowed_cleanup_preserves_service_and_all_input_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credential = root / "credential"
            credential.write_text("test credential")
            fixture.save_context(root, self.context(root, generation=None))
            with patch.object(fixture, "stop_local_generation") as stop:
                fixture.release_fixture(root)
                stop.assert_not_called()
            self.assertTrue(credential.exists())
            self.assertFalse((root / "fixture-run.json").exists())

    def test_ownership_record_rejects_wrong_root_and_incomplete_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.context(root)
            context["root"] = str(root.parent)
            fixture.save_context(root, context)
            with self.assertRaises(fixture.FixtureError):
                fixture.load_context(root)
            context = self.context(root)
            context["generation"] = ["active", "invalid", "0"]
            fixture.save_context(root, context)
            with self.assertRaises(fixture.FixtureError):
                fixture.load_context(root)

    def test_incomplete_provision_record_is_not_reinitialized(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(root, self.context(root, ready=False, generation=None))
            with patch.object(fixture, "install_fixture") as install:
                with self.assertRaises(fixture.FixtureError):
                    fixture.ensure_fixture(root)
                install.assert_not_called()

    def test_fresh_provision_uses_the_same_shared_ensure_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def install(target, context):
                self.assertEqual(target, root)
                context["created"] = True
                context["generation"] = ["active", "a" * 32, "123"]
                fixture.save_context(root, context)

            with patch.object(fixture, "install_fixture", side_effect=install) as bootstrap, patch.object(
                fixture, "local_fixture", return_value=(root, "fixture-server")
            ), patch.object(fixture, "verify_local_service"), patch.object(
                fixture, "service_status", return_value=("active", "a" * 32, "123")
            ), patch.object(fixture, "wait_for_local"), patch.object(fixture, "verify_loopback_binding"):
                context = fixture.ensure_fixture(root)
                bootstrap.assert_called_once()
                self.assertTrue(context["ready"])
                self.assertTrue(context["created"])

    def test_interruption_kills_and_drains_the_exact_owned_test_process_group(self):
        process = MagicMock()
        process.pid = 123
        process.communicate.side_effect = [KeyboardInterrupt(), ("", "")]
        manager = MagicMock()
        manager.__enter__.return_value = process
        with patch.object(fixture.subprocess, "Popen", return_value=manager), patch.object(
            fixture.os, "killpg"
        ) as kill:
            with self.assertRaises(KeyboardInterrupt):
                fixture.command("local tests", ["just", "test-live"], process_group=True)
            kill.assert_called_once_with(123, fixture.signal.SIGKILL)
            self.assertEqual(process.communicate.call_count, 2)

    def test_test_process_group_timeout_is_an_explicit_fixture_error(self):
        process = MagicMock()
        process.pid = 123
        process.communicate.side_effect = [subprocess.TimeoutExpired(["just"], 1), ("", "")]
        manager = MagicMock()
        manager.__enter__.return_value = process
        with patch.object(fixture.subprocess, "Popen", return_value=manager), patch.object(
            fixture.os, "killpg"
        ) as kill:
            with self.assertRaises(fixture.FixtureError):
                fixture.command("local tests", ["just", "test-live"], process_group=True)
            kill.assert_called_once_with(123, fixture.signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
