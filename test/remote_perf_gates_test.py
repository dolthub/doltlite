import contextlib
import copy
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import benchmark_retry
import benchmark_compare
import remote_perf as perf
from remote_perf_test import saved_result


class Measurements:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def __call__(self, command, env, stdout, check):
        result = copy.deepcopy(self.results[self.calls])
        self.calls += 1
        result["configuration"]["gate_mode"] = "relative" if "baseline" in result["binaries"] else "absolute"
        directory = Path(env["BENCH_RESULTS_OUTPUT"]).parent
        perf.save_results(result, directory)
        if result["status"] != "passed":
            return types.SimpleNamespace(returncode=1)
        with mock.patch.dict(os.environ, env):
            perf.write_benchmark_results(result)
        stdout.write(perf.report(result))
        return types.SimpleNamespace(returncode=0)


def measured(pairs=None, absolute=False, runs=5):
    result = saved_result(("candidate",) if absolute else ("baseline", "candidate"), runs)
    pairs = pairs or {}
    for sample in result["samples"]:
        pair = pairs.get(sample["case"], (100_000, 1000 if absolute else 100_000))
        value = pair[0 if sample["arm"] == "baseline" else 1]
        sample["wall_us"] = value[sample["trial"]-1] if isinstance(value, list) else value
    return result


class RemotePerfGatesTest(unittest.TestCase):
    def run_gate(self, directory, results):
        runner = Measurements(results)
        relative = "baseline" in results[0]["binaries"]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc = benchmark_retry.run_with_retries(
                "remote", directory, ["measure"], 3,
                perf.RELATIVE_RATIO if relative else 1.0,
                perf.AGGREGATE_RATIO if relative else 1000,
                perf.MIN_DELTA_US if relative else 0,
                perf.MIN_DELTA_US if relative else 0,
                command_runner=runner, producer_id="current", fail_confirmed=True,
                extra_artifacts=("results.json", "samples.tsv", "report.md"))
        return rc, runner

    def test_retry_defaults_match_relative_policy(self):
        with mock.patch.object(benchmark_retry, "run_with_retries", return_value=0) as run:
            benchmark_retry.main(["--suite", "remote", "--results-dir", "unused", "--", "measure"])
        self.assertEqual(run.call_args.args[3:8],
                         (3, perf.RELATIVE_RATIO, perf.AGGREGATE_RATIO, perf.MIN_DELTA_US, perf.MIN_DELTA_US))

    def test_single_outliers_and_copy_control_do_not_gate(self):
        result = measured({"push_noop": (200, [200, 200, 150_000, 200, 200]),
                           "copy_fsync": (1000, 10_000_000)})
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [result])
            loaded = perf.read_gated_results(Path(temporary) / "results.json", "relative")
        self.assertEqual((rc, runner.calls), (0, 1))
        self.assertTrue(perf.gates_pass(loaded))
        self.assertEqual(perf.gate_status(loaded, "copy_fsync"), "CONTROL")

    def test_absolute_floor_ignores_tiny_noop_regressions(self):
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [measured({"push_noop": (200, 5000)})])
        self.assertEqual((rc, runner.calls), (0, 1))

    def test_confirmed_individual_regression_fails_after_three_attempts(self):
        result = measured({"push_initial": (1_000_000, 1_300_000)})
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [result]*3)
            loaded = perf.read_gated_results(Path(temporary) / "results.json")
        self.assertEqual((rc, runner.calls), (1, 3))
        self.assertFalse(perf.gates_pass(loaded))
        self.assertEqual(perf.gate_status(loaded, "push_initial"), "FAIL")
        results = [benchmark_compare.Result("vc", "vc", "status", 1000, 1000)]
        analysis = benchmark_compare.analyze(results, 1.25, 1.15, 5000)
        report = benchmark_compare.render(results, analysis, "base", "new", 1.25, 1.15, 5000,
                                          remote=loaded)
        self.assertIn("**Gate result:** **FAIL**", report)
        self.assertIn("individual `remote / file_remote / push_initial`", report)
        self.assertNotIn("**Confirmed failed gates:** none", report)
        details = report.split("<summary>vc details</summary>", 1)[1].split("</details>", 1)[0]
        self.assertIn("| file_remote | `push_initial` |", details)
        self.assertNotIn("MEASURED", details)
        self.assertIn("| FAIL |", details)

    def test_sustained_noop_regression_above_floor_is_gated(self):
        result = measured({"push_noop": (200, 6000)})
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [result]*3)
            loaded = perf.read_gated_results(Path(temporary) / "results.json")
        self.assertEqual((rc, runner.calls), (1, 3))
        self.assertEqual(perf.gate_status(loaded, "push_noop"), "FAIL")

    def test_broad_aggregate_regression_is_confirmed(self):
        result = measured({case: (100_000, 116_000) for case in perf.CEILINGS_US})
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [result]*3)
            loaded = perf.read_gated_results(Path(temporary) / "results.json")
        self.assertEqual((rc, runner.calls), (1, 3))
        self.assertFalse(perf.gates_pass(loaded))

    def test_changing_individual_failures_are_transient(self):
        baseline = {case: (1_000_000, 1_000_000) for case in perf.CEILINGS_US}
        first = measured({**baseline, "push_initial": (1_000_000, 1_300_000)})
        second = measured({**baseline, "clone_initial": (1_000_000, 1_300_000)})
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [first, second])
            loaded = perf.read_gated_results(Path(temporary) / "results.json")
            self.assertEqual(loaded["samples"], second["samples"])
        self.assertEqual((rc, runner.calls), (0, 2))
        self.assertEqual(perf.gate_status(loaded, "clone_initial"), "TRANSIENT")

    def test_nightly_ceiling_boundaries_and_confirmation(self):
        for case, ceiling in perf.CEILINGS_US.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                result = measured({case: (0, ceiling)}, absolute=True)
                rc, runner = self.run_gate(temporary, [result])
                self.assertEqual((rc, runner.calls), (0, 1))
                result = measured({case: (0, ceiling+1)}, absolute=True)
                rc, runner = self.run_gate(temporary, [result]*3)
                loaded = perf.read_gated_results(Path(temporary) / "results.json", "absolute")
                self.assertEqual((rc, runner.calls), (1, 3))
                self.assertEqual(perf.gate_status(loaded, case), "FAIL")

    def test_verification_errors_are_not_retried(self):
        result = measured()
        result.update(status="failed", error="database mismatch", samples=[])
        with tempfile.TemporaryDirectory() as temporary:
            rc, runner = self.run_gate(temporary, [result])
            attempt = Path(temporary) / "attempts/attempt-1/results.json"
            self.assertEqual(json.loads(attempt.read_text())["status"], "failed")
            self.assertFalse((Path(temporary) / "results.json").exists())
        self.assertEqual((rc, runner.calls), (1, 1))

    def test_incomplete_or_stale_gate_artifacts_fail_closed(self):
        for corrupt in ("raw", "producer", "missing"):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                self.run_gate(directory, [measured()])
                if corrupt == "raw":
                    path = directory / "results.json"
                    result = json.loads(path.read_text())
                    for sample in result["samples"]:
                        if sample["case"] == "push_initial" and sample["arm"] == "candidate":
                            sample["wall_us"] += 1_000_000
                    path.write_text(json.dumps(result))
                if corrupt == "producer":
                    path = directory / "remote-attempts.tsv"
                    path.write_text(path.read_text().replace("current", "stale"))
                if corrupt == "missing":
                    (directory / "remote-attempts.tsv").unlink()
                with self.assertRaises((OSError, ValueError)):
                    perf.read_gated_results(directory / "results.json")


if __name__ == "__main__":
    unittest.main()
