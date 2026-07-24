#!/usr/bin/env python3

import contextlib
import importlib.util
import io
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

    def test_parses_three_consecutive_regressions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "attempts.tsv"
            path.write_text(
                "attempt\tstatus\n"
                "1\tregression\n"
                "2\tregression\n"
                "3\tregression\n",
                encoding="utf-8",
            )
            suite, statuses = benchmark_compare.parse_attempt_history(
                f"int={path}"
            )
        self.assertEqual(suite, "int")
        self.assertEqual(
            statuses,
            ["regression", "regression", "regression"],
        )

    def test_rejects_unconfirmed_final_regression(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "attempts.tsv"
            path.write_text(
                "attempt\tstatus\n1\tregression\n2\tregression\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "requires three consecutive failures",
            ):
                benchmark_compare.parse_attempt_history(f"int={path}")

    def test_report_describes_transient_retry(self):
        results = [result("point", 100_000, 101_000)]
        analysis = self.analyze(results)
        report = benchmark_compare.render(
            results,
            analysis,
            "base",
            "candidate",
            1.25,
            1.15,
            5000,
            attempt_histories={"int": ["regression", "pass"]},
        )
        self.assertIn(
            "int passed attempt 2 after 1 transient gate failure",
            report,
        )

    def test_main_accepts_attempt_history(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            timings = directory / "result.tsv"
            attempts = directory / "attempts.tsv"
            report = directory / "report.md"
            timings.write_text(
                "reads\tpoint\t100000\t101000\n",
                encoding="utf-8",
            )
            attempts.write_text(
                "attempt\tstatus\n1\tregression\n2\tpass\n",
                encoding="utf-8",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                rc = benchmark_compare.main(
                    [
                        "--input",
                        f"int={timings}",
                        "--attempt-history",
                        f"int={attempts}",
                        "--baseline-ref",
                        "base",
                        "--candidate-ref",
                        "candidate",
                        "--expected-baseline-ref",
                        "base",
                        "--expected-candidate-ref",
                        "candidate",
                        "--output",
                        str(report),
                    ]
                )
            output = report.read_text(encoding="utf-8")
        self.assertEqual(rc, 0)
        self.assertIn("int passed attempt 2", output)

    def test_rejects_regression_without_three_failed_attempts(self):
        results = [result("point", 100_000, 140_000)]
        analysis = self.analyze(results)
        with self.assertRaisesRegex(
            ValueError,
            "lacks three-failure confirmation",
        ):
            benchmark_compare.validate_retry_confirmation(
                results,
                analysis,
                {"int": ["pass"]},
            )

    def test_accepts_regression_after_three_failed_attempts(self):
        results = [result("point", 100_000, 140_000)]
        analysis = self.analyze(results)
        benchmark_compare.validate_retry_confirmation(
            results,
            analysis,
            {"int": ["regression", "regression", "regression"]},
        )

    def test_rejects_swapped_revision_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            timings = directory / "result.tsv"
            report = directory / "report.md"
            timings.write_text(
                "reads\tpoint\t100\t110\n", encoding="utf-8"
            )
            with contextlib.redirect_stdout(io.StringIO()):
                rc = benchmark_compare.main(
                    [
                        "--input",
                        f"int={timings}",
                        "--baseline-ref",
                        "candidate-sha",
                        "--candidate-ref",
                        "baseline-sha",
                        "--expected-baseline-ref",
                        "baseline-sha",
                        "--expected-candidate-ref",
                        "candidate-sha",
                        "--output",
                        str(report),
                    ]
                )
            output = report.read_text(encoding="utf-8")
        self.assertEqual(rc, 1)
        self.assertIn("revision provenance", output)
        self.assertIn("| Baseline | `candidate-sha` | `baseline-sha` |", output)


if __name__ == "__main__":
    unittest.main()
