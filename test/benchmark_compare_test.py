#!/usr/bin/env python3

import importlib.util
import pathlib
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("benchmark_compare.py")
SPEC = importlib.util.spec_from_file_location("benchmark_compare", MODULE_PATH)
benchmark_compare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark_compare)


def result(name, baseline, candidate, section="reads"):
    return benchmark_compare.Result(
        suite="int",
        section=section,
        test=name,
        baseline_us=baseline,
        candidate_us=candidate,
    )


class BenchmarkCompareTest(unittest.TestCase):
    def analyze(self, results):
        return benchmark_compare.analyze(results, 1.25, 1.15, 5000)

    def test_passes_small_absolute_regression(self):
        analysis = self.analyze([result("tiny", 1000, 1400)])
        self.assertFalse(analysis["failed"])

    def test_fails_large_individual_regression(self):
        analysis = self.analyze(
            [
                result("slow", 100_000, 130_000),
                result("steady", 1_000_000, 1_000_000),
            ]
        )
        self.assertTrue(analysis["failed"])
        self.assertIn(
            ("int", "reads", "slow"),
            analysis["individual_failures"],
        )

    def test_fails_aggregate_regression(self):
        analysis = self.analyze(
            [
                result("one", 100_000, 114_000),
                result("two", 100_000, 118_000),
            ]
        )
        self.assertTrue(analysis["failed"])
        self.assertIn(("int", "reads"), analysis["section_failures"])

    def test_exact_thresholds_pass(self):
        individual = self.analyze(
            [
                result("boundary", 100_000, 125_000),
                result("offset", 100_000, 105_000),
            ]
        )
        aggregate = self.analyze(
            [
                result("one", 100_000, 115_000),
                result("two", 100_000, 115_000),
            ]
        )
        self.assertFalse(individual["failed"])
        self.assertFalse(aggregate["failed"])

    def test_invalid_timing_always_fails(self):
        analysis = self.analyze([result("crash", 100_000, -1)])
        self.assertTrue(analysis["failed"])

    def test_formats_faster_candidate_delta(self):
        self.assertEqual(benchmark_compare.format_delta(-1500), "-1.50ms")

    def test_suite_specific_individual_gate(self):
        analysis = benchmark_compare.analyze(
            [
                result("startup_bound", 60_000, 85_000),
                result("steady", 1_000_000, 1_000_000),
            ],
            1.25,
            1.15,
            5000,
            {"int": (1.50, 25_000)},
        )
        self.assertFalse(analysis["failed"])

    def test_parses_result_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "result.tsv"
            path.write_text("reads\tpoint\t100\t110\n", encoding="utf-8")
            parsed = benchmark_compare.parse_input(f"text={path}")
        self.assertEqual(
            parsed,
            [
                benchmark_compare.Result(
                    suite="text",
                    section="reads",
                    test="point",
                    baseline_us=100,
                    candidate_us=110,
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
