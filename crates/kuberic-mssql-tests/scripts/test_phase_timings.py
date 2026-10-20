import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from collect_phase_timings import collect


class PhaseTimingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.report = {
            "version": 1, "scenario": "lease-expiry", "attempt": "main",
            "process": 123, "outcome": "succeeded",
            "phases": [{"phase": "preflight", "outcome": "succeeded", "elapsed_ms": 15}],
        }

    def write(self):
        for name, content in (
            (".required-123-0", b""),
            ("123-0.json", json.dumps(self.report).encode()),
        ):
            path = self.root / name
            path.write_bytes(content)
            path.chmod(0o600)

    def test_success_and_interrupted_child_are_distinguished(self):
        self.write()
        self.assertIn("session=succeeded", collect(str(self.root))[0])
        self.report.update(attempt="interrupted-child", outcome="running")
        self.report["phases"][0]["outcome"] = "running"
        self.write()
        lines = collect(str(self.root))
        self.assertIn("session=incomplete", lines[0])
        self.assertIn("outcome=incomplete", lines[1])

    def test_unfinished_main_requires_failed_runner(self):
        self.report["outcome"] = "incomplete"
        self.report["phases"][0]["outcome"] = "incomplete"
        self.write()
        with self.assertRaises(ValueError):
            collect(str(self.root))
        self.assertIn("session=incomplete", collect(str(self.root), failed=True)[0])

    def test_missing_report_symlink_and_fifo_are_refused(self):
        self.write()
        path = self.root / "123-0.json"
        path.unlink()
        with self.assertRaises(ValueError):
            collect(str(self.root))
        path.symlink_to(self.root / ".required-123-0")
        with self.assertRaises(OSError):
            collect(str(self.root))
        path.unlink()
        os.mkfifo(path, 0o600)
        with self.assertRaises(ValueError):
            collect(str(self.root))

    def test_partial_snapshot_does_not_replace_committed_report(self):
        self.write()
        partial = self.root / ".phase-tmp-interrupted"
        partial.write_bytes(b'{"private-content":')
        partial.chmod(0o600)
        lines = collect(str(self.root))
        self.assertEqual(len(lines), 2)
        self.assertNotIn("private-content", "\n".join(lines))

    def test_malformed_records_are_not_echoed_or_accepted(self):
        for change in (
            {"scenario": "PRIVATE_SENTINEL"},
            {"version": True},
            {"phases": self.report["phases"] * 257},
            {"extra": "PRIVATE_SENTINEL"},
        ):
            with self.subTest(change=tuple(change)):
                original = self.report.copy()
                self.report.update(change)
                self.write()
                result = subprocess.run(
                    [sys.executable, str(Path(__file__).with_name("collect_phase_timings.py")),
                     str(self.root)],
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("PRIVATE_SENTINEL", result.stderr)
                self.report = original

    def test_duplicate_fields_and_truncation_are_refused(self):
        self.write()
        path = self.root / "123-0.json"
        for data in (b'{"version":1,"version":1}', b'{"version":'):
            path.write_bytes(data)
            with self.assertRaises(ValueError):
                collect(str(self.root))


if __name__ == "__main__":
    unittest.main()
