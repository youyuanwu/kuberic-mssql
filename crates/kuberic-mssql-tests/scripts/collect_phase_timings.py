#!/usr/bin/env python3
"""Publish only bounded, validated live-test phase records."""

import json
import os
import re
import stat
import sys

SCENARIOS = {
    "lease-expiry", "lease-active-restart", "sigint-recovery", "sigterm-recovery",
    "sigkill-recovery", "restart-checkpoint", "target-sql-restart",
    "accepted-reply-lost",
}
ATTEMPTS = {"main", "interrupted-child", "fault-child", "retry-child"}
OUTCOMES = {"running", "succeeded", "failed", "incomplete", "panicked"}
PHASES = {
    "fixture-launch", "preflight", "assets", "container-creation",
    "container-start", "member-one", "member-two", "member-three",
    "peer-preparation", "public-assembly", "host-stabilization", "reports",
    "build", "seed-retry", "native-convergence", "marker", "sql-restart",
    "fresh-sql", "callback-rejection", "host-drain", "cleanup",
    "interrupted-launch", "separate-cleanup", "recovery-retry", "fault-journey",
    "lease-formation", "lease-baseline", "lease-renewal", "original-expiry-wait",
    "extension-write", "renewed-expiry-wait",
}
IDENTITY = re.compile(r"[1-9][0-9]{0,9}-[0-9]{1,20}")
MAX_BYTES = 131072


def require(condition):
    if not condition:
        raise ValueError("invalid timing report")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def validate(report, identity, failed):
    require(type(report) is dict and set(report) == {
        "version", "scenario", "attempt", "process", "outcome", "phases",
    })
    require(type(report["version"]) is int and report["version"] == 1)
    require(report["scenario"] in SCENARIOS and report["attempt"] in ATTEMPTS)
    require(type(report["process"]) is int
            and str(report["process"]) == identity.split("-")[0])
    require(report["outcome"] in OUTCOMES)
    require(type(report["phases"]) is list and len(report["phases"]) <= 256)
    if not failed and report["attempt"] in {"main", "retry-child"}:
        require(report["outcome"] == "succeeded")
    for phase in report["phases"]:
        require(type(phase) is dict
                and set(phase) == {"phase", "outcome", "elapsed_ms"})
        require(phase["phase"] in PHASES and phase["outcome"] in OUTCOMES)
        require(type(phase["elapsed_ms"]) is int
                and 0 <= phase["elapsed_ms"] <= 604800000)
        if report["outcome"] == "succeeded":
            require(phase["outcome"] != "running")
    return report


def read_private(directory_fd, name):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    with os.fdopen(fd, "rb") as source:
        metadata = os.fstat(source.fileno())
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.geteuid()
                and stat.S_IMODE(metadata.st_mode) == 0o600
                and metadata.st_size <= MAX_BYTES)
        content = source.read(MAX_BYTES + 1)
        require(len(content) <= MAX_BYTES)
        return content


def collect(directory, failed=False):
    require(os.path.isabs(directory) and os.path.realpath(directory) == directory)
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    reports = {}
    required = set()
    try:
        metadata = os.fstat(fd)
        require(metadata.st_uid == os.geteuid()
                and stat.S_IMODE(metadata.st_mode) == 0o700)
        entries = os.listdir(fd)
        require(len(entries) <= 768)
        for name in sorted(entries):
            content = read_private(fd, name)
            if name.startswith(".phase-tmp-"):
                require(len(name) <= 64)
                continue
            if name.startswith(".required-"):
                identity = name[len(".required-"):]
                require(IDENTITY.fullmatch(identity) and not content)
                required.add(identity)
            else:
                require(name.endswith(".json"))
                identity = name[:-5]
                require(IDENTITY.fullmatch(identity))
                report = json.loads(content, object_pairs_hook=unique_object)
                reports[identity] = validate(report, identity, failed)
        require(set(reports) == required and len(reports) <= 256)
    finally:
        os.close(fd)
    require(sum(len(report["phases"]) for report in reports.values()) <= 8192)
    lines = []
    for identity, report in sorted(reports.items()):
        prefix = (f"MSSQL_PHASE scenario={report['scenario']} "
                  f"attempt={report['attempt']} process={report['process']}")
        session_outcome = report["outcome"]
        if session_outcome == "running":
            session_outcome = "incomplete"
        lines.append(f"{prefix} session={session_outcome}")
        for phase in report["phases"]:
            outcome = phase["outcome"]
            if outcome == "running":
                outcome = "incomplete"
            lines.append(f"{prefix} phase={phase['phase']} "
                         f"outcome={outcome} elapsed_ms={phase['elapsed_ms']}")
    return lines


def main():
    if len(sys.argv) not in (2, 3) or (len(sys.argv) == 3 and sys.argv[2] != "--failed"):
        print("usage: collect_phase_timings.py <directory> [--failed]", file=sys.stderr)
        return 2
    try:
        lines = collect(sys.argv[1], len(sys.argv) == 3)
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        print("MSSQL_PHASE_REPORT_ERROR: timing report validation failed", file=sys.stderr)
        return 1
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
