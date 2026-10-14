#!/usr/bin/env python3
"""Run every test target and opt-in case, retaining outcomes without fail-fast."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / "target/full-validation-results"
FIXTURES = ROOT / "target/full-validation-fixtures"
RESULTS.mkdir(parents=True, exist_ok=True)
FIXTURES.mkdir(mode=0o700, parents=True, exist_ok=True)
if FIXTURES.is_symlink() or FIXTURES.stat().st_uid != os.geteuid():
    raise RuntimeError("fixture parent must be a real directory owned by the runner")
FIXTURES.chmod(0o700)
ENV = {
    **os.environ,
    "PATH": f"{Path.home()}/.local/bin:{Path.home()}/.cargo/bin:{os.environ['PATH']}",
    "CARGO_BUILD_JOBS": os.environ.get("CARGO_BUILD_JOBS", "1"),
}
for name in [
    "SQLSERVER_TEST_EULA_ACCEPTED", "KUBERIC_MSSQL_CONFIGURATION_FAULT_FILTER",
    "KUBERIC_MSSQL_SEED_FAULT_FILTER", "KUBERIC_MSSQL_CONFLICT_FILTER",
]:
    ENV.pop(name, None)
REPORT = {
    "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    "platform": sys.platform,
    "started_at_unix": time.time(),
    "results": [],
    "prior_run_cases": [],
    "resume_after": os.environ.get("FULL_VALIDATION_RESUME_AFTER", ""),
}


def save():
    (RESULTS / "summary.json").write_text(json.dumps(REPORT, indent=2) + "\n")


def execute(name, command, environment=None):
    print(f"=== {name} ===", flush=True)
    result = subprocess.run(command, cwd=ROOT, env=environment or ENV, capture_output=True, text=True)
    output = result.stdout + result.stderr
    (RESULTS / f"{name}.txt").write_text(output)
    skipped = "SKIPPED:" in output
    summaries = [
        {"passed": int(passed), "failed": int(failed), "ignored": int(ignored)}
        for passed, failed, ignored in re.findall(
            r"test result: .*?\. (\d+) passed; (\d+) failed; (\d+) ignored;", output
        )
    ]
    REPORT["results"].append({
        "name": name, "command": command, "exit_code": result.returncode,
        "success": result.returncode == 0 and not skipped,
        "self_reported_skip": skipped, "test_summaries": summaries,
    })
    save()
    if result.returncode or skipped:
        print(output[-8000:], flush=True)
    print(f"exit={result.returncode}, self_reported_skip={skipped}", flush=True)
    return result


def verify_no_fixture_resources(name):
    for kind in ["container", "network", "volume"]:
        result = execute(
            f"verify-{name}-{kind}",
            ["docker", kind, "ls", "-q", "--filter", "label=io.kuberic.mssql.fixture"]
            + (["--all"] if kind == "container" else []),
        )
        if result.returncode or result.stdout.strip():
            REPORT["results"][-1]["success"] = False
            save()
            raise RuntimeError(f"owned {kind} resources remain; refusing further fixtures")


def main():
    if sys.platform != "linux" or os.geteuid() == 0:
        raise RuntimeError("full validation requires a non-root Linux runner")
    execute("format", ["cargo", "fmt", "--all", "--", "--check"])
    execute("clippy", ["cargo", "clippy", "--locked", "--workspace", "--all-targets",
                       "--all-features", "--", "-D", "warnings"])
    execute("server-free", ["cargo", "test", "--locked", "--workspace", "--all-features",
                            "--no-fail-fast", "--", "--test-threads=1"])
    build = execute("build-tests", ["cargo", "test", "--locked", "-p", "kuberic-mssql-tests",
                                    "--all-features", "--no-run", "--message-format=json"])
    if build.returncode:
        raise RuntimeError("cannot execute live binaries because their build failed")
    executables = {}
    for line in build.stdout.splitlines():
        artifact = json.loads(line)
        if artifact.get("reason") == "compiler-artifact" and artifact.get("executable"):
            executables[artifact["target"]["name"]] = artifact["executable"]

    resume = REPORT["resume_after"]
    reached_resume = not resume
    for target in ["live_one_replica", "live_three_replica", "three_replica_lifecycle"]:
        executable = executables[target]
        listed = subprocess.check_output(
            [executable, "--ignored", "--list", "--format=terse"], cwd=ROOT, env=ENV, text=True
        )
        names = [line.removesuffix(": test") for line in listed.splitlines() if line.endswith(": test")]
        if target == "three_replica_lifecycle":
            capability = execute("acl-capability", ["sudo", "-n", "setcap", "cap_chown+ep", executable])
            if capability.returncode:
                raise RuntimeError("cannot validate ACL deletion without the requested capability")
        for index, name in enumerate(names):
            if not reached_resume:
                REPORT["prior_run_cases"].append(name)
                if name == resume:
                    reached_resume = True
                save()
                continue
            fixture = FIXTURES / f"{target}-{index}"
            environment = {
                **ENV,
                "SQLSERVER_ONE_REPLICA_ROOT": str(fixture),
                "KUBERIC_MSSQL_THREE_REPLICA_ROOT": str(fixture),
            }
            execute(f"{target}-{name}", [executable, name, "--ignored", "--exact",
                                        "--nocapture", "--test-threads=1"], environment)
            if target != "three_replica_lifecycle":
                family = "one" if target == "live_one_replica" else "three"
                cleanup_roots = []
                if fixture.exists():
                    for directory, children, files in os.walk(fixture, followlinks=False):
                        children[:] = [
                            child for child in children
                            if not (Path(directory) / child).is_symlink()
                        ]
                        if "ownership.json" in files:
                            journal = Path(directory) / "ownership.json"
                            if journal.is_symlink():
                                raise RuntimeError("refusing symlinked ownership journal")
                            cleanup_roots.append(Path(directory))
                if not cleanup_roots and not fixture.exists():
                    cleanup_roots.append(fixture)
                for ordinal, owned_root in enumerate(sorted(cleanup_roots)):
                    cleanup = execute(
                        f"cleanup-{target}-{index}-{ordinal}",
                        [executables[f"mssql-{family}-replica-fixture"], "cleanup", "--root",
                         str(owned_root)],
                    )
                    if cleanup.returncode:
                        raise RuntimeError("owned cleanup failed; refusing further SQL fixtures")
                verify_no_fixture_resources(f"{target}-{index}")

    if not reached_resume:
        raise RuntimeError(f"resume test was not in the live inventory: {resume}")

    execute("live-ag-convergence", [
        executables["mssql-ag-convergence-fixture"],
        str(FIXTURES / "ag-convergence"), executables["live_ag_convergence"],
    ])
    execute("cleanup-ag-convergence", [
        executables["mssql-three-replica-fixture"], "cleanup", "--root",
        str(FIXTURES / "ag-convergence"),
    ])
    verify_no_fixture_resources("ag-convergence")
    REPORT["finished_at_unix"] = time.time()
    save()
    return 0 if all(result["success"] for result in REPORT["results"]) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        REPORT["runner_error"] = str(error)
        save()
        print(f"full validation runner: {error}", file=sys.stderr)
        sys.exit(1)
