#!/usr/bin/env python3

import contextlib
import importlib.util
import io
import json
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


def attempt_history(suite, attempts):
    return benchmark_compare.AttemptHistory(
        suite,
        tuple(frozenset(gates) for gates in attempts),
    )


def history_text(suite, attempts):
    lines = [f"suite\t{suite}", "attempt\tstatus\tfailed_gates"]
    for number, gates in enumerate(attempts, 1):
        status = "regression" if gates else "pass"
        encoded = json.dumps(
            [list(gate) for gate in sorted(gates)],
            separators=(",", ":"),
        )
        lines.append(f"{number}\t{status}\t{encoded}")
    return "\n".join(lines) + "\n"


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
                history_text(
                    "int",
                    [
                        {("individual", "int", "reads", "point")},
                        {("individual", "int", "reads", "point")},
                        {("individual", "int", "reads", "point")},
                    ],
                ),
                encoding="utf-8",
            )
            suite, history = benchmark_compare.parse_attempt_history(
                f"int={path}"
            )
        self.assertEqual(suite, "int")
        self.assertEqual(
            history.confirmed_failures,
            {("individual", "int", "reads", "point")},
        )

    def test_rotating_failures_have_no_confirmation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "attempts.tsv"
            path.write_text(
                history_text(
                    "int",
                    [
                        {("individual", "int", "reads", "one")},
                        {("individual", "int", "reads", "two")},
                    ],
                ),
                encoding="utf-8",
            )
            _suite, history = benchmark_compare.parse_attempt_history(
                f"int={path}"
            )
        self.assertEqual(history.confirmed_failures, frozenset())

    def test_confirmation_is_three_way_gate_intersection(self):
        gate_a = ("individual", "int", "reads", "one")
        gate_b = ("individual", "int", "reads", "two")
        gate_c = ("individual", "int", "reads", "three")
        history = attempt_history(
            "int",
            [
                {gate_a, gate_b},
                {gate_b, gate_c},
                {gate_a, gate_b, gate_c},
            ],
        )
        self.assertEqual(history.confirmed_failures, {gate_b})

    def test_rejects_two_attempt_history_with_live_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "attempts.tsv"
            gate = ("individual", "int", "reads", "point")
            path.write_text(
                history_text("int", [{gate}, {gate}]),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "possible regression requires three attempts",
            ):
                benchmark_compare.parse_attempt_history(f"int={path}")

    def test_rejects_history_with_mismatched_embedded_suite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "int-attempts.tsv"
            path.write_text(
                history_text("int", [set()]),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "history suite int does not match requested suite vc",
            ):
                benchmark_compare.parse_attempt_history(f"vc={path}")

    def test_main_rejects_ito_mismatched_history_reproduction(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            timings = directory / "vc.tsv"
            attempts = directory / "int-attempts.tsv"
            report = directory / "report.md"
            timings.write_text(
                "vc\tstatus\t100000\t101000\n",
                encoding="utf-8",
            )
            attempts.write_text(
                history_text("int", [set()]),
                encoding="utf-8",
            )
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    benchmark_compare.main(
                        [
                            "--input",
                            f"vc={timings}",
                            "--attempt-history",
                            f"vc={attempts}",
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
        self.assertEqual(raised.exception.code, 2)

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
            attempt_histories={
                "int": attempt_history(
                    "int",
                    [
                        {("individual", "int", "reads", "point")},
                        set(),
                    ],
                )
            },
        )
        self.assertIn(
            "int cleared after 2 attempts; no gate failed every time",
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
                history_text(
                    "int",
                    [
                        {("individual", "int", "reads", "point")},
                        set(),
                    ],
                ),
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
        self.assertIn("int cleared after 2 attempts", output)

    def test_filters_regression_without_exact_confirmation(self):
        results = [result("point", 100_000, 140_000)]
        analysis = self.analyze(results)
        confirmed = benchmark_compare.apply_retry_confirmation(
            results,
            analysis,
            {"int": attempt_history("int", [set()])},
        )
        self.assertFalse(confirmed["failed"])
        self.assertEqual(confirmed["individual_failures"], set())

    def test_accepts_same_gate_after_three_failed_attempts(self):
        results = [result("point", 100_000, 140_000)]
        analysis = self.analyze(results)
        gate = ("individual", "int", "reads", "point")
        confirmed = benchmark_compare.apply_retry_confirmation(
            results,
            analysis,
            {"int": attempt_history("int", [{gate}, {gate}, {gate}])},
        )
        self.assertTrue(confirmed["failed"])
        self.assertEqual(confirmed["confirmed_failure_ids"], {gate})

    def test_rejects_confirmed_gate_absent_from_final_results(self):
        results = [result("point", 100_000, 101_000)]
        analysis = self.analyze(results)
        gate = ("individual", "int", "reads", "point")
        with self.assertRaisesRegex(
            ValueError,
            "confirmed gates do not match final benchmark results",
        ):
            benchmark_compare.apply_retry_confirmation(
                results,
                analysis,
                {"int": attempt_history("int", [{gate}, {gate}, {gate}])},
            )

    def test_rejects_direct_history_with_mismatched_suite(self):
        results = [result("point", 100_000, 101_000)]
        analysis = self.analyze(results)
        with self.assertRaisesRegex(
            ValueError,
            "history suite vc does not match requested suite int",
        ):
            benchmark_compare.apply_retry_confirmation(
                results,
                analysis,
                {"int": attempt_history("vc", [set()])},
            )

    def test_report_lists_confirmed_failed_gate_separately_from_overall(self):
        results = [
            result("point", 100_000, 140_000),
            result("other", 100_000, 140_000),
            result("steady", 1_000_000, 1_000_000),
        ]
        raw = self.analyze(results)
        gates = {
            ("individual", "int", "reads", "point"),
            ("individual", "int", "reads", "other"),
        }
        analysis = benchmark_compare.apply_retry_confirmation(
            results,
            raw,
            {"int": attempt_history("int", [gates, gates, gates])},
        )
        report = benchmark_compare.render(
            results,
            analysis,
            "base",
            "candidate",
            1.25,
            1.15,
            5000,
            attempt_histories={
                "int": attempt_history("int", [gates, gates, gates])
            },
        )
        self.assertIn("**Overall ratio:** 1.067x", report)
        self.assertIn("**Gate result:** **FAIL**", report)
        self.assertIn(
            "individual `int / reads / point`",
            report,
        )
        self.assertIn(
            "individual `int / reads / other`",
            report,
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
