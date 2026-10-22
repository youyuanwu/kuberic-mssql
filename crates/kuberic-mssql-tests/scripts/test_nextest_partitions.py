import copy
import subprocess
import sys
import unittest
from unittest.mock import patch

import validate_nextest_partitions as validator
import live_coverage


def identity(name, binary="ordinary"):
    return validator.TestId("kuberic-mssql-tests", binary, name)


class PartitionCoverageTests(unittest.TestCase):
    def test_classification_keeps_ordinary_helpers_in_live_binaries(self):
        live_binary = "kuberic-mssql-tests::live_three_replica"
        ordinary = identity("child", live_binary)
        live = identity("fault", live_binary)
        excluded = next(iter(validator.EXCLUDED))
        self.assertEqual(
            validator.classify([
                validator.ListedTest(ordinary, False, True),
                validator.ListedTest(live, True, True),
                validator.ListedTest(excluded, True, True),
            ], blocked=set()),
            ({ordinary}, {live}),
        )

    def test_unknown_ignored_test_is_not_silently_omitted(self):
        with self.assertRaisesRegex(validator.ValidationError, "unclassified ignored"):
            validator.classify([validator.ListedTest(identity("unknown"), True, True)], blocked=set())

    def test_blocked_coverage_is_accounted_not_active(self):
        ordinary = identity("ordinary")
        live = identity("active", live_coverage.BINARY)
        blocked = identity("blocked", live_coverage.BINARY)
        rows = [
            validator.ListedTest(ordinary, False, True),
            validator.ListedTest(live, True, True),
            validator.ListedTest(blocked, True, True),
            validator.ListedTest(next(iter(validator.EXCLUDED)), True, True),
        ]
        self.assertEqual(validator.classify(rows, blocked={blocked}), ({ordinary}, {live}))
        with self.assertRaisesRegex(validator.ValidationError, "blocked licensed inventory is stale"):
            validator.classify(rows[:-2] + rows[-1:], blocked={blocked})
        rows[2] = validator.ListedTest(blocked, False, True)
        with self.assertRaisesRegex(validator.ValidationError, "not an ignored licensed test"):
            validator.classify(rows, blocked={blocked})

    def test_empty_live_shard_requires_explicit_permission_and_complete_union(self):
        case = identity("active", live_coverage.BINARY)
        partitions = [{case}, set(), set(), set()]
        validator.validate_partition_sets({case}, partitions, "live", allow_empty=True)
        with self.assertRaisesRegex(validator.ValidationError, "empty"):
            validator.validate_partition_sets({case}, partitions, "ordinary")
        with self.assertRaisesRegex(validator.ValidationError, "missing="):
            validator.validate_partition_sets({case}, [set()] * 4, "live", allow_empty=True)

    def test_blocked_manual_recipe_fails_with_explicit_coverage_limit(self):
        result = subprocess.run(
            [sys.executable, live_coverage.__file__, "--require-public"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("not executed or qualified", result.stdout)
        self.assertIn("no fixture was launched", result.stderr)

    def test_exact_disjoint_partitions(self):
        cases = [identity(str(i)) for i in range(4)]
        validator.validate_partition_sets(set(cases), [{case} for case in cases], "tier")

    def test_empty_overlap_missing_and_extra_partitions_fail(self):
        cases = [identity(str(i)) for i in range(5)]
        expected = set(cases[:4])
        for partitions in [
            [{cases[0]}, {cases[1]}, {cases[2]}, set()],
            [{cases[0]}, {cases[1]}, {cases[2]}, {cases[0], cases[3]}],
            [{cases[0]}, {cases[1]}, {cases[2]}],
            [{cases[0]}, {cases[1]}, {cases[2]}, {cases[4]}],
            [{cases[0]}, {cases[1]}, {cases[2]}, {cases[3], cases[4]}],
        ]:
            with self.subTest(partitions=partitions):
                with self.assertRaises(validator.ValidationError):
                    validator.validate_partition_sets(expected, partitions, "tier")

    def test_duplicate_and_malformed_listing_fail(self):
        suite = {
            "package-name": "pkg",
            "binary-id": "binary",
            "testcases": {"case": {"ignored": False, "filter-match": {"status": "matches"}}},
        }
        for document in [
            {},
            {"rust-suites": {"one": suite, "two": copy.deepcopy(suite)}},
            {"rust-suites": {"one": {**suite, "testcases": {"case": {"ignored": "false"}}}}},
        ]:
            with self.subTest(document=document):
                with self.assertRaises(validator.ValidationError):
                    validator.listed_tests(document)

    def test_live_matching_does_not_drop_ignored_tests(self):
        case = identity("ignored-sql", "kuberic-mssql-tests::live_three_replica")
        self.assertEqual(validator.matching([validator.ListedTest(case, True, True)]), {case})

    def test_failed_listing_and_invalid_json_propagate(self):
        with patch("subprocess.check_output", side_effect=subprocess.CalledProcessError(2, ["cargo"])):
            with self.assertRaises(subprocess.CalledProcessError):
                validator.run_json(["cargo"])
        with patch("subprocess.check_output", return_value="not json"):
            with self.assertRaises(ValueError):
                validator.run_json(["cargo"])


if __name__ == "__main__":
    unittest.main()
