#!/usr/bin/env python3
"""Prove exact-once active coverage and explicit blocked licensed accounting."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from dataclasses import dataclass

import live_coverage


PARTITIONS = 4
LIVE_BINARIES = {
    "kuberic-mssql-tests::live_one_replica",
    "kuberic-mssql-tests::live_three_replica",
    "kuberic-mssql-tests::live_lease_fence",
}
COMPANIONS = (
    "sqlserver-observer-test",
    "kuberic-mssql-runtime-test",
    "kuberic-release-intake",
    "mssql-one-replica-fixture",
    "mssql-three-replica-fixture",
)


class ValidationError(ValueError):
    pass


@dataclass(frozen=True, order=True)
class TestId:
    package: str
    binary: str
    name: str


@dataclass(frozen=True)
class ListedTest:
    identity: TestId
    ignored: bool
    matches: bool


EXCLUDED = {
    TestId(
        "kuberic-mssql-tests",
        "kuberic-mssql-tests::three_replica_lifecycle",
        "real_acl_allows_host_recursive_delete_of_uid_10001_nested_files",
    )
}


def listed_tests(document):
    rows = []
    seen = set()
    try:
        for suite in document["rust-suites"].values():
            for name, case in suite["testcases"].items():
                identity = TestId(suite["package-name"], suite["binary-id"], name)
                status = case["filter-match"]["status"]
                if not all(isinstance(value, str) and value for value in (
                    identity.package, identity.binary, identity.name
                )) or not isinstance(case["ignored"], bool) or status not in ("matches", "mismatch"):
                    raise ValidationError("invalid nextest test identity or filter metadata")
                if identity in seen:
                    raise ValidationError(f"duplicate test identity: {identity}")
                seen.add(identity)
                rows.append(ListedTest(identity, case["ignored"], status == "matches"))
    except (KeyError, TypeError, AttributeError) as error:
        raise ValidationError(f"malformed nextest listing: {error}") from error
    return rows


def classify(rows, blocked=None):
    if blocked is None:
        blocked = {
            TestId("kuberic-mssql-tests", live_coverage.BINARY, name)
            for name in live_coverage.BLOCKED
        }
    ordinary, live, excluded, blocked_seen = set(), set(), set(), set()
    for row in rows:
        if row.identity in blocked:
            if not row.ignored or row.identity.binary not in LIVE_BINARIES:
                raise ValidationError(f"blocked case is not an ignored licensed test: {row.identity}")
            blocked_seen.add(row.identity)
        elif not row.ignored:
            ordinary.add(row.identity)
        elif row.identity.binary in LIVE_BINARIES:
            live.add(row.identity)
        elif row.identity in EXCLUDED:
            excluded.add(row.identity)
        else:
            raise ValidationError(f"unclassified ignored test: {row.identity}")
    if excluded != EXCLUDED:
        raise ValidationError("the privileged ACL exclusion inventory changed")
    require_equal(blocked_seen, blocked, "blocked licensed inventory is stale")
    if not ordinary or not live:
        raise ValidationError("ordinary and live inventories must both be nonempty")
    return ordinary, live


def matching(rows):
    return {row.identity for row in rows if row.matches}


def require_equal(actual, expected, label):
    if actual != expected:
        raise ValidationError(
            f"{label}: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )


def validate_partition_sets(expected, partitions, label, allow_empty=False):
    if len(partitions) != PARTITIONS:
        raise ValidationError(f"{label}: expected exactly {PARTITIONS} partitions")
    union = set()
    for index, partition in enumerate(partitions, 1):
        if not partition and not allow_empty:
            raise ValidationError(f"{label}: partition {index} is empty")
        if union & partition:
            raise ValidationError(f"{label}: partition {index} overlaps an earlier partition")
        union.update(partition)
    require_equal(union, expected, label)


def run_json(command):
    return json.loads(subprocess.check_output(command, text=True))


class ArchiveListing:
    def __init__(self, archive, workspace, extract):
        subprocess.run(
            [
                "cargo", "nextest", "list", "--archive-file", str(archive),
                "--extract-to", str(extract), "--workspace-remap", str(workspace),
                "--profile", "default", "--ignore-default-filter",
                "--list-type", "binaries-only", "--message-format", "json",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        target = extract / "target"
        for name in COMPANIONS:
            path = target / "debug" / name
            if not path.is_file() or not os.access(path, os.X_OK):
                raise ValidationError(f"missing executable archive companion: {path}")
        self.command = [
            "cargo", "nextest", "list",
            "--cargo-metadata", str(target / "nextest/cargo-metadata.json"),
            "--binaries-metadata", str(target / "nextest/binaries-metadata.json"),
            "--target-dir-remap", str(target),
            "--workspace-remap", str(workspace),
            "--message-format", "json",
        ]

    def tests(self, profile, ignored="default", partition=None, complete=False):
        command = self.command + ["--profile", profile, "--run-ignored", ignored]
        if profile == "live":
            command += ["-E", live_coverage.live_filter()]
        if partition is not None:
            command += ["--partition", partition]
        if complete:
            command += ["--ignore-default-filter"]
        return listed_tests(run_json(command))


def validate(source):
    ordinary, live = classify(source.tests("default", ignored="all", complete=True))
    for profile, mode, ignored, expected in [
        ("default", "slice", "default", ordinary),
        ("live", "hash", "only", live),
    ]:
        require_equal(matching(source.tests(profile, ignored=ignored)), expected, profile)
        partitions = [
            matching(source.tests(profile, ignored=ignored, partition=f"{mode}:{i}/{PARTITIONS}"))
            for i in range(1, PARTITIONS + 1)
        ]
        validate_partition_sets(expected, partitions, profile, allow_empty=profile == "live")
        print(f"{profile}: {len(expected)} tests, {mode} partitions {[len(p) for p in partitions]}")
    live_coverage.report()
    print("Exact-once active archive coverage verified; blocked cases accounted; privileged ACL test excluded.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-file", required=True, type=Path)
    args = parser.parse_args()
    workspace = Path(__file__).resolve().parents[3]
    archive = args.archive_file.resolve(strict=True)
    scratch = workspace / "target/nextest"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="validate-archive-", dir=scratch) as directory:
        validate(ArchiveListing(archive, workspace, Path(directory)))


if __name__ == "__main__":
    try:
        main()
    except (ValidationError, OSError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"archive coverage validation failed: {error}", file=sys.stderr)
        sys.exit(1)
