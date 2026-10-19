import copy
import subprocess
import unittest
from unittest.mock import patch

import validate_nextest_partitions as validator


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
            ]),
            ({ordinary}, {live}),
        )

    def test_unknown_ignored_test_is_not_silently_omitted(self):
        with self.assertRaisesRegex(validator.ValidationError, "unclassified ignored"):
            validator.classify([validator.ListedTest(identity("unknown"), True, True)])

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
