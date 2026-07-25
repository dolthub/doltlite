#!/usr/bin/env python3

import importlib.util
import pathlib
import tempfile
import types
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("benchmark_retry.py")
SPEC = importlib.util.spec_from_file_location("benchmark_retry", MODULE_PATH)
benchmark_retry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark_retry)


class FakeRunner:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def __call__(self, command, env, stdout, check):
        del command, check
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, int):
            return types.SimpleNamespace(returncode=outcome)

        if isinstance(outcome, tuple):
            baseline, candidate = outcome
            rows = [("reads", "point", baseline, candidate)]
        else:
            rows = outcome
        pathlib.Path(env["BENCH_RESULTS_OUTPUT"]).write_text(
            "".join(
                f"{section}\t{test}\t{baseline}\t{candidate}\n"
                for section, test, baseline, candidate in rows
            ),
            encoding="utf-8",
        )
        pathlib.Path(env["BENCH_SAMPLES_OUTPUT"]).write_text(
            "section\ttest\trun\tbaseline_us\tcandidate_us\n"
            + "".join(
                f"{section}\t{test}\t1\t{baseline}\t{candidate}\n"
                for section, test, baseline, candidate in rows
            ),
            encoding="utf-8",
        )
        stdout.write("benchmark output\n")
        return types.SimpleNamespace(returncode=0)


class BenchmarkRetryTest(unittest.TestCase):
    def run_retry(self, directory, outcomes):
        runner = FakeRunner(outcomes)
        rc = benchmark_retry.run_with_retries(
            "int",
            directory,
            ["fake-benchmark"],
            3,
            1.25,
            1.15,
            5000,
            5000,
            runner,
        )
        return rc, runner

    def test_first_pass_uses_one_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            rc, runner = self.run_retry(directory, [(100_000, 101_000)])
            history = pathlib.Path(directory, "int-attempts.tsv").read_text()
        self.assertEqual(rc, 0)
        self.assertEqual(runner.calls, 1)
        self.assertEqual(
            history,
            "suite\tint\n"
            "attempt\tstatus\tfailed_gates\n"
            "1\tpass\t[]\n",
        )

    def test_regression_then_pass_promotes_second_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            rc, runner = self.run_retry(
                directory,
                [(100_000, 140_000), (100_000, 101_000)],
            )
            history = (directory / "int-attempts.tsv").read_text()
            result = (directory / "int.tsv").read_text()
        self.assertEqual(rc, 0)
        self.assertEqual(runner.calls, 2)
        self.assertIn("1\tregression\t", history)
        self.assertIn("2\tpass\t[]\n", history)
        self.assertEqual(result, "reads\tpoint\t100000\t101000\n")

    def test_three_regressions_promote_final_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            rc, runner = self.run_retry(
                directory,
                [
                    (100_000, 130_000),
                    (100_000, 140_000),
                    (100_000, 150_000),
                ],
            )
            history = (directory / "int-attempts.tsv").read_text()
            result = (directory / "int.tsv").read_text()
            _suite, parsed = (
                benchmark_retry.benchmark_compare.parse_attempt_history(
                    f"int={directory / 'int-attempts.tsv'}"
                )
            )
        self.assertEqual(rc, 0)
        self.assertEqual(runner.calls, 3)
        self.assertIn(
            ("individual", "int", "reads", "point"),
            parsed.confirmed_failures,
        )
        self.assertEqual(result, "reads\tpoint\t100000\t150000\n")

    def test_rotating_individual_failures_stop_without_confirmation(self):
        steady = ("reads", "steady", 1_000_000, 1_000_000)
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            rc, runner = self.run_retry(
                directory,
                [
                    [
                        ("reads", "one", 100_000, 140_000),
                        ("reads", "two", 100_000, 100_000),
                        steady,
                    ],
                    [
                        ("reads", "one", 100_000, 100_000),
                        ("reads", "two", 100_000, 140_000),
                        steady,
                    ],
                ],
            )
            _suite, history = (
                benchmark_retry.benchmark_compare.parse_attempt_history(
                    f"int={directory / 'int-attempts.tsv'}"
                )
            )
        self.assertEqual(rc, 0)
        self.assertEqual(runner.calls, 2)
        self.assertEqual(history.confirmed_failures, frozenset())

    def test_command_error_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            rc, runner = self.run_retry(directory, [7])
            history = (directory / "int-attempts.tsv").read_text()
        self.assertEqual(rc, 7)
        self.assertEqual(runner.calls, 1)
        self.assertEqual(
            history,
            "suite\tint\n"
            "attempt\tstatus\tfailed_gates\n"
            "1\tcommand_error\t[]\n",
        )
        self.assertFalse((directory / "int.tsv").exists())

    def test_missing_output_is_not_retried(self):
        class MissingOutputRunner:
            calls = 0

            def __call__(self, command, env, stdout, check):
                del command, env, check
                self.calls += 1
                stdout.write("incomplete output\n")
                return types.SimpleNamespace(returncode=0)

        with tempfile.TemporaryDirectory() as directory:
            runner = MissingOutputRunner()
            rc = benchmark_retry.run_with_retries(
                "int",
                directory,
                ["fake-benchmark"],
                3,
                1.25,
                1.15,
                5000,
                5000,
                runner,
            )
        self.assertEqual(rc, 2)
        self.assertEqual(runner.calls, 1)


if __name__ == "__main__":
    unittest.main()
