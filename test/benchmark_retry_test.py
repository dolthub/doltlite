#!/usr/bin/env python3

import contextlib
import importlib.util
import io
import pathlib
import shutil
import tempfile
import types
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("benchmark_retry.py")
SPEC = importlib.util.spec_from_file_location("benchmark_retry", MODULE_PATH)
benchmark_retry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark_retry)

PRODUCER_ID = "producer-current"


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
            PRODUCER_ID,
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
            f"producer_id\t{PRODUCER_ID}\n"
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
        self.assertEqual(
            result,
            "# suite\tint\n"
            f"# producer_id\t{PRODUCER_ID}\n"
            "reads\tpoint\t100000\t101000\n",
        )

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
        self.assertEqual(
            result,
            "# suite\tint\n"
            f"# producer_id\t{PRODUCER_ID}\n"
            "reads\tpoint\t100000\t150000\n",
        )

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
            f"producer_id\t{PRODUCER_ID}\n"
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

    def test_promotion_copy_failure_leaves_no_canonical_files(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            attempt = directory / "attempt"
            results = directory / "results"
            attempt.mkdir()
            results.mkdir()
            (attempt / "int.tsv").write_text(
                "reads\tpoint\t100\t101\n",
                encoding="utf-8",
            )
            (attempt / "int-samples.tsv").write_text(
                "section\ttest\trun\tbaseline_us\tcandidate_us\n"
                "reads\tpoint\t1\t100\t101\n",
                encoding="utf-8",
            )
            (attempt / "int.md").write_text(
                "benchmark report\n",
                encoding="utf-8",
            )

            def fail_on_markdown(source, destination):
                if pathlib.Path(source).suffix == ".md":
                    raise FileNotFoundError("injected markdown copy failure")
                return shutil.copy2(source, destination)

            with self.assertRaisesRegex(
                FileNotFoundError,
                "injected markdown copy failure",
            ):
                benchmark_retry.promote_attempt(
                    attempt,
                    results,
                    "int",
                    PRODUCER_ID,
                    copy_function=fail_on_markdown,
                )

            self.assertEqual(list(results.iterdir()), [])
            self.assertTrue((attempt / "int.tsv").is_file())
            self.assertTrue((attempt / "int-samples.tsv").is_file())
            self.assertTrue((attempt / "int.md").is_file())

    def test_live_log_identifies_attempts_gates_and_elapsed_time(self):
        with tempfile.TemporaryDirectory() as directory:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                rc, runner = self.run_retry(
                    directory,
                    [(100_000, 140_000), (100_000, 101_000)],
                )
            log = output.getvalue()
        self.assertEqual(rc, 0)
        self.assertEqual(runner.calls, 2)
        self.assertIn("int: starting attempt 1/3", log)
        self.assertIn("int: starting attempt 2/3", log)
        self.assertIn("failed gates: individual `int / reads / point`", log)
        self.assertIn("s cumulative)", log)
        self.assertIn("no exact gate failed on every attempt", log)


if __name__ == "__main__":
    unittest.main()
