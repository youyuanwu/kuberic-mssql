import copy
from contextlib import nullcontext
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
    AG_NAME = fixture.AVAILABILITY_GROUP_PREFIX + "1" * 32

    def setUp(self):
        output = patch("sys.stdout", new=io.StringIO())
        output.start()
        self.addCleanup(output.stop)

    def context(self, root, **overrides):
        value = {
            "schema_version": fixture.CONTEXT_SCHEMA_VERSION,
            "root": str(root),
            "created_files": False,
            "created_container": False,
            "started_container": False,
            "ephemeral": False,
            "container_id": "a" * 64,
            "ready": True,
            "availability_group": fixture.availability_group_record(self.AG_NAME),
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
                "User": "mssql",
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

    def availability_group(self):
        return {
            "group_id": "11111111-1111-4111-8111-111111111111",
            "configuration_sequence": 4294967297,
            "profile": fixture.expected_ag_profile(),
        }

    def owned_ag_context(self, root, **overrides):
        context = self.context(root)
        context["availability_group"].update(
            group_id="11111111-1111-4111-8111-111111111111",
            profile=fixture.expected_ag_profile(),
            created=True,
            state="created",
        )
        context.update(overrides)
        return context

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
        self.assertNotIn("SQLSERVER_LIVE_AG_CONFIG", env)

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

    def test_admin_sql_keeps_passwords_out_of_process_arguments_and_errors(self):
        completed = subprocess.CompletedProcess([], 1, "", "contains-secret")
        with patch.object(fixture.subprocess, "run", return_value=completed):
            with self.assertRaisesRegex(fixture.FixtureError, "exit code 1") as raised:
                fixture.sql(
                    Path("/private"),
                    "SELECT N'private-fixture-value';",
                    "sa",
                    "private-sa-password",
                )
        self.assertNotIn("private", str(raised.exception))

    def test_availability_group_creation_is_metadata_only(self):
        with patch.object(fixture, "admin_sql") as admin:
            fixture.create_availability_group(Path("/private"), self.AG_NAME)
        command = admin.call_args.args[1]
        self.assertIn(f"CREATE AVAILABILITY GROUP [{self.AG_NAME}]", command)
        self.assertIn("CLUSTER_TYPE = EXTERNAL", command)
        self.assertEqual(command.count("ENDPOINT_URL"), 3)
        self.assertEqual(command.count("SYNCHRONOUS_COMMIT"), 3)
        self.assertEqual(command.count("FAILOVER_MODE = EXTERNAL"), 3)
        self.assertEqual(command.count("SEEDING_MODE = AUTOMATIC"), 3)
        self.assertIn("REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1", command)
        self.assertNotIn("FOR DATABASE", command)
        self.assertNotIn("CREATE ENDPOINT", command)
        self.assertNotIn("CREATE CERTIFICATE", command)
        self.assertNotIn("CREATE MASTER KEY", command)

    def test_exact_ag_profile_rejects_any_changed_field(self):
        snapshot = self.availability_group()
        self.assertTrue(fixture.validate_availability_group(snapshot))
        changed = copy.deepcopy(snapshot)
        changed["profile"]["replicas"][0]["seeding_mode"] = "MANUAL"
        self.assertFalse(fixture.validate_availability_group(changed))

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

    def test_present_config_and_environment_exist_only_after_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.write_configs(root)
            with self.assertRaises(fixture.FixtureError):
                fixture.fixture_environment(root, include_ag=True)
            fixture.write_present_config(root, self.AG_NAME)
            present = json.loads((root / "present.json").read_text())
            absent = json.loads((root / "absent.json").read_text())
            self.assertEqual(present["availability_group"], self.AG_NAME)
            self.assertEqual(present["incarnation"], absent["incarnation"])
            environment = fixture.fixture_environment(root, include_ag=True)
            self.assertEqual(environment["SQLSERVER_LIVE_AG_CONFIG"], str(root / "present.json"))

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
                self.assertEqual(metadata["Config"]["User"], "mssql")
                self.assertNotIn("/etc/machine-id", {
                    destination for destination, _ in fixture.expected_mounts(root).values()
                })
                changes = [
                    (["Config", "Labels", fixture.LABEL_ROOT], "wrong"),
                    (["Config", "Hostname"], "wrong"),
                    (["Config", "User"], "0"),
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
                changed = copy.deepcopy(metadata)
                changed["Mounts"].append(
                    {"Source": "/host/machine-id", "Destination": "/etc/machine-id", "RW": False}
                )
                with self.assertRaises(fixture.FixtureError):
                    fixture.verify_container(root, changed)

    def test_container_creation_uses_direct_docker_and_image_default_user(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            completed = subprocess.CompletedProcess([], 0, "a" * 64 + "\n", "")
            with patch.object(fixture, "command") as command:
                fixture.docker("inspect", ["inspect", "fixture"])
            self.assertEqual(command.call_args.args[1], ["docker", "inspect", "fixture"])
            with patch.object(
                fixture, "command", return_value=subprocess.CompletedProcess([], 0, "", "")
            ), patch.object(fixture, "image_id", return_value="sha256:image"), patch.object(
                fixture, "docker", return_value=completed
            ) as docker, patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ):
                fixture.create_container(root)
            args = docker.call_args.args[1]
            self.assertNotIn("--user", args)
            self.assertEqual(args[args.index("--hostname") + 1], "kuberic-mssql-observer")

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

    def test_availability_group_names_are_private_unique_and_validated(self):
        first = fixture.availability_group_record()
        second = fixture.availability_group_record()
        self.assertNotEqual(first["name"], second["name"])
        self.assertTrue(fixture.valid_availability_group_name(first["name"]))
        self.assertTrue(fixture.valid_availability_group_name(second["name"]))
        changed = copy.deepcopy(first)
        changed["name"] = fixture.LEGACY_AVAILABILITY_GROUP_NAME
        self.assertFalse(fixture.validate_availability_group_record(changed))

    def test_provision_binds_exact_ag_and_refuses_same_name_unowned_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.context(root, ready=False)
            state = {"group": None}

            def inspect(_root, name):
                self.assertEqual(name, self.AG_NAME)
                return state["group"]

            def create(_root, name):
                persisted = fixture.load_context(root)["availability_group"]
                self.assertEqual(persisted["name"], name)
                self.assertEqual(persisted["state"], "creating")
                self.assertIsNone(persisted["group_id"])
                state["group"] = self.availability_group()

            with patch.object(fixture, "inspect_availability_group", side_effect=inspect), patch.object(
                fixture,
                "create_availability_group",
                side_effect=create,
            ), patch.object(
                fixture, "write_present_config"
            ):
                fixture.provision_availability_group(root, context)
            self.assertEqual(
                context["availability_group"]["group_id"],
                "11111111-1111-4111-8111-111111111111",
            )
            self.assertEqual(
                context["availability_group"]["profile"], fixture.expected_ag_profile()
            )
            self.assertTrue(context["availability_group"]["created"])
            self.assertEqual(context["availability_group"]["state"], "created")

            conflict = self.context(root, ready=False)
            with patch.object(
                fixture, "inspect_availability_group", return_value=self.availability_group()
            ), patch.object(fixture, "create_availability_group") as create:
                with self.assertRaisesRegex(fixture.FixtureError, "same-name unowned"):
                    fixture.provision_availability_group(root, conflict)
                create.assert_not_called()

    def test_persisted_unique_name_and_exact_profile_bind_interrupted_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.context(root, ready=False)
            context["availability_group"].update(created=True, state="creating")
            fixture.save_context(root, context)
            with patch.object(
                fixture, "inspect_availability_group", return_value=self.availability_group()
            ), patch.object(
                fixture, "create_availability_group"
            ) as create, patch.object(
                fixture, "write_present_config"
            ):
                fixture.provision_availability_group(root, context)
            create.assert_not_called()
            self.assertEqual(
                context["availability_group"]["group_id"],
                self.availability_group()["group_id"],
            )
            self.assertEqual(context["availability_group"]["state"], "created")

    def test_owned_ag_identity_or_profile_mismatch_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.owned_ag_context(root)
            changed = self.availability_group()
            changed["group_id"] = "22222222-2222-4222-8222-222222222222"
            with patch.object(fixture, "inspect_availability_group", return_value=changed):
                with self.assertRaisesRegex(fixture.FixtureError, "absent or mismatched"):
                    fixture.provision_availability_group(root, context)

    def test_pre_dispatch_interruption_cleans_to_reloadable_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.context(root, ready=False)
            context["availability_group"].update(created=True, state="creating")
            fixture.save_context(root, context)
            with patch.object(fixture, "inspect_availability_group", return_value=None):
                fixture.cleanup_availability_group(root, context)
            reloaded = fixture.load_context(root)
            self.assertEqual(reloaded["availability_group"]["state"], "cleaned")
            self.assertIsNone(reloaded["availability_group"]["group_id"])
            self.assertIsNone(reloaded["availability_group"]["profile"])

    def test_invalid_cleaned_availability_group_record_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.context(root, ready=False)
            context["availability_group"].update(
                created=True,
                state="cleaned",
                profile=fixture.expected_ag_profile(),
            )
            fixture.save_context(root, context)
            with self.assertRaisesRegex(fixture.FixtureError, "incompatible"):
                fixture.load_context(root)

    def test_ag_cleanup_is_retryable_and_retains_interrupted_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.owned_ag_context(root)
            fixture.save_context(root, context)
            with patch.object(
                fixture, "inspect_availability_group", return_value=self.availability_group()
            ), patch.object(
                fixture,
                "drop_availability_group",
                side_effect=fixture.FixtureError("interrupted AG cleanup"),
            ):
                with self.assertRaisesRegex(fixture.FixtureError, "interrupted"):
                    fixture.cleanup_availability_group(root, context)
            retained = fixture.load_context(root)
            self.assertEqual(retained["availability_group"]["state"], "cleaning")
            with patch.object(fixture, "inspect_availability_group", return_value=None):
                with self.assertRaisesRegex(fixture.FixtureError, "absent owned"):
                    fixture.cleanup_availability_group(root, retained)
            self.assertEqual(retained["availability_group"]["state"], "cleaning")

    def test_destructive_drop_revalidates_name_id_and_profile_in_one_batch(self):
        with patch.object(fixture, "admin_sql") as admin:
            fixture.drop_availability_group(
                Path("/private"),
                self.AG_NAME,
                "11111111-1111-4111-8111-111111111111",
            )
        sql = admin.call_args.args[1]
        self.assertIn(
            f"ag.name COLLATE Latin1_General_100_BIN2 = N'{self.AG_NAME}'", sql
        )
        self.assertIn("ag.group_id = @expected_group_id", sql)
        self.assertIn("required_synchronized_secondaries_to_commit = 1", sql)
        self.assertIn("COUNT(*) FROM sys.availability_replicas", sql)
        self.assertIn(f"DROP AVAILABILITY GROUP [{self.AG_NAME}]", sql)

    def test_replacement_at_destructive_execution_is_retained_for_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self.owned_ag_context(root)
            fixture.save_context(root, context)
            with patch.object(
                fixture, "inspect_availability_group", return_value=self.availability_group()
            ), patch.object(
                fixture,
                "drop_availability_group",
                side_effect=fixture.FixtureError(
                    "fixture availability-group identity or profile changed"
                ),
            ):
                with self.assertRaisesRegex(fixture.FixtureError, "identity or profile changed"):
                    fixture.cleanup_availability_group(root, context)
            retained = fixture.load_context(root)["availability_group"]
            self.assertEqual(retained["state"], "cleaning")
            self.assertEqual(
                retained["group_id"], "11111111-1111-4111-8111-111111111111"
            )

    def test_legacy_context_is_upgraded_without_claiming_an_ag(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = self.context(root)
            legacy["schema_version"] = 2
            legacy.pop("availability_group")
            fixture.save_context(root, legacy)
            loaded = fixture.load_context(root)
            self.assertEqual(loaded["schema_version"], fixture.CONTEXT_SCHEMA_VERSION)
            self.assertTrue(
                fixture.valid_availability_group_name(loaded["availability_group"]["name"])
            )
            self.assertEqual(fixture.load_context(root), loaded)
            changed = self.context(root, container_id="invalid")
            fixture.save_context(root, changed)
            with self.assertRaises(fixture.FixtureError):
                fixture.load_context(root)

    def test_fixed_name_schema_is_replaced_only_when_no_active_ag_is_claimed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = self.context(root)
            legacy["schema_version"] = 4
            legacy["availability_group"] = {
                "name": fixture.LEGACY_AVAILABILITY_GROUP_NAME,
                "group_id": None,
                "profile": None,
                "created": False,
                "state": "not_started",
            }
            fixture.save_context(root, legacy)
            migrated = fixture.load_context(root)
            self.assertEqual(migrated["schema_version"], fixture.CONTEXT_SCHEMA_VERSION)
            self.assertTrue(
                fixture.valid_availability_group_name(
                    migrated["availability_group"]["name"]
                )
            )
            self.assertEqual(fixture.load_context(root), migrated)

            active = copy.deepcopy(legacy)
            active["availability_group"].update(
                group_id="11111111-1111-4111-8111-111111111111",
                profile=fixture.expected_ag_profile(),
                created=True,
                state="created",
            )
            fixture.save_context(root, active)
            with self.assertRaisesRegex(fixture.FixtureError, "must be cleaned"):
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
            ), patch.object(
                fixture, "provision_availability_group"
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
            ), patch.object(fixture, "start_container") as start, patch.object(
                fixture, "provision_availability_group"
            ):
                context = fixture.ensure_fixture(root)
                start.assert_not_called()
                self.assertFalse(context["created_container"])
                self.assertFalse(context["started_container"])
            with patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ), patch.object(
                fixture, "cleanup_availability_group"
            ), patch.object(fixture, "stop_exact_container") as stop, patch.object(
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
                with patch.object(fixture, "provision_availability_group"):
                    context = fixture.ensure_fixture(root)
                start.assert_called_once_with(root, "a" * 64)
                self.assertTrue(context["started_container"])
            with patch.object(
                fixture, "docker_inspect", return_value=running
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ), patch.object(
                fixture, "cleanup_availability_group"
            ), patch.object(fixture, "stop_exact_container") as stop:
                fixture.release_fixture(root)
                stop.assert_called_once_with(root, "a" * 64)

    def test_created_container_is_removed_by_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(
                root,
                self.context(root, created_container=True, started_container=True),
            )
            with patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ), patch.object(fixture, "remove_exact_container") as remove:
                fixture.release_fixture(root)
                remove.assert_called_once_with(root, "a" * 64)
            self.assertFalse((root / "fixture-run.json").exists())

    def test_exact_created_container_removal_satisfies_ag_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(
                root,
                self.owned_ag_context(
                    root, created_container=True, started_container=True
                ),
            )
            with patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ), patch.object(
                fixture,
                "cleanup_availability_group",
                side_effect=fixture.FixtureError("SQL unavailable"),
            ) as cleanup, patch.object(
                fixture, "remove_exact_container"
            ) as remove:
                fixture.release_fixture(root)
            cleanup.assert_not_called()
            remove.assert_called_once_with(root, "a" * 64)
            self.assertFalse((root / "fixture-run.json").exists())

    def test_interrupted_created_container_cleanup_retains_ownership_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(
                root,
                self.owned_ag_context(
                    root, created_container=True, started_container=True
                ),
            )
            with patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ), patch.object(
                fixture,
                "remove_exact_container",
                side_effect=fixture.FixtureError("interrupted container removal"),
            ):
                with self.assertRaisesRegex(fixture.FixtureError, "interrupted"):
                    fixture.release_fixture(root)
            retained = fixture.load_context(root)
            self.assertEqual(retained["container_id"], "a" * 64)
            self.assertEqual(retained["availability_group"]["state"], "created")

    def test_borrowed_container_cleanup_failure_retains_actionable_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture.save_context(root, self.owned_ag_context(root))
            with patch.object(
                fixture, "docker_inspect", return_value=self.container(root)
            ), patch.object(
                fixture, "verify_container", return_value=("a" * 64, True)
            ), patch.object(
                fixture,
                "cleanup_availability_group",
                side_effect=fixture.FixtureError("interrupted AG cleanup"),
            ), patch.object(fixture, "remove_exact_container") as remove, patch.object(
                fixture, "stop_exact_container"
            ) as stop:
                with self.assertRaisesRegex(fixture.FixtureError, "interrupted"):
                    fixture.release_fixture(root)
            self.assertTrue((root / "fixture-run.json").is_file())
            remove.assert_not_called()
            stop.assert_not_called()

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
            ), patch.object(fixture, "cargo_binary", return_value="/custom/cargo"), patch.object(
                fixture, "command", return_value=completed
            ) as command:
                fixture.run_test_cases(root)
            args = command.call_args.args[1]
            env = command.call_args.kwargs["env"]
            self.assertEqual(args[0], "/custom/cargo")
            self.assertEqual(args[1:3], ["test", "--locked"])
            self.assertEqual(env["SQLSERVER_TEST_IMAGE"], fixture.IMAGE)
            self.assertNotIn("SQLSERVER_LIVE_AG_CONFIG", env)
            self.assertNotIn("SQLSERVER_TEST_PACKAGE_VERSION", env)

    def test_live_command_selection_combines_feature_targets_once(self):
        with patch.object(fixture, "cargo_binary", return_value="/custom/cargo"):
            shared = fixture.live_test_command(include_ag=True, include_kuberic=True)
            self.assertEqual(shared.count("test"), 1)
            self.assertIn("--all-features", shared)
            self.assertIn("--tests", shared)
            self.assertNotIn("--test", shared)
            self.assertNotIn("--skip", shared)
            ordinary = fixture.live_test_command(include_ag=False, include_kuberic=False)
            self.assertNotIn("--all-features", ordinary)
            self.assertIn(
                "live_kuberic_progress_matches_fresh_direct_observation", ordinary
            )
            self.assertIn("live_present_availability_group", ordinary)

    def test_cargo_resolution_honors_configuration_path_and_fallback(self):
        with patch.dict(os.environ, {"CARGO": "custom-cargo"}, clear=True), patch.object(
            fixture.shutil, "which", return_value="/tools/custom-cargo"
        ):
            self.assertEqual(fixture.cargo_binary(), "/tools/custom-cargo")
        with patch.dict(os.environ, {}, clear=True), patch.object(
            fixture.shutil, "which", return_value="/usr/bin/cargo"
        ):
            self.assertEqual(fixture.cargo_binary(), "/usr/bin/cargo")
        fallback = Path.home() / ".cargo" / "bin" / "cargo"
        with patch.dict(os.environ, {}, clear=True), patch.object(
            fixture.shutil, "which", return_value=None
        ), patch.object(
            fixture.Path, "is_file", return_value=True
        ), patch.object(
            fixture.os, "access", return_value=True
        ):
            self.assertEqual(fixture.cargo_binary(), str(fallback))

    def test_validate_uses_one_shared_live_command_and_prebuilt_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            completed = subprocess.CompletedProcess([], 0, "{}", "")
            with patch.object(fixture, "lifecycle_lock", return_value=nullcontext()), patch.object(
                fixture, "ensure_fixture"
            ), patch.object(
                fixture,
                "fixture_environment",
                return_value={"SQLSERVER_LIVE_ABSENT_CONFIG": str(root / "absent.json")},
            ), patch.object(
                fixture.Path, "is_file", return_value=True
            ), patch.object(
                fixture, "command", return_value=completed
            ) as command, patch.object(
                fixture, "verify_cli"
            ) as verify, patch.object(
                fixture, "release_fixture"
            ):
                fixture.validate_fixture(root)
            commands = [call.args[1] for call in command.call_args_list]
            self.assertEqual(commands[0], ["just", "test-live-shared", str(root)])
            self.assertEqual(
                commands[1],
                [
                    "target/debug/sqlserver-observer",
                    "--config",
                    str(root / "absent.json"),
                ],
            )
            verify.assert_called_once_with(root / "observation.json")

    def test_direct_validate_builds_observer_when_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            completed = subprocess.CompletedProcess([], 0, "{}", "")
            with patch.object(fixture, "lifecycle_lock", return_value=nullcontext()), patch.object(
                fixture, "ensure_fixture"
            ), patch.object(
                fixture,
                "fixture_environment",
                return_value={"SQLSERVER_LIVE_ABSENT_CONFIG": str(root / "absent.json")},
            ), patch.object(
                fixture.Path, "is_file", return_value=False
            ), patch.object(
                fixture, "cargo_binary", return_value="/custom/cargo"
            ), patch.object(
                fixture, "command", return_value=completed
            ) as command, patch.object(
                fixture, "verify_cli"
            ), patch.object(
                fixture, "release_fixture"
            ):
                fixture.validate_fixture(root)
            commands = [call.args[1] for call in command.call_args_list]
            self.assertEqual(commands[0], ["just", "test-live-shared", str(root)])
            self.assertEqual(
                commands[1],
                [
                    "/custom/cargo",
                    "build",
                    "--locked",
                    "--bin",
                    "sqlserver-observer",
                ],
            )
            self.assertEqual(
                commands[2],
                [
                    "target/debug/sqlserver-observer",
                    "--config",
                    str(root / "absent.json"),
                ],
            )

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
