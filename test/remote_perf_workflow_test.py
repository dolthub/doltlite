import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest

from remote_perf_test import saved_result


ROOT = Path(__file__).resolve().parents[1]


def jobs(name):
    text = (ROOT / ".github/workflows" / name).read_text().split("jobs:\n", 1)[1]
    return dict(re.findall(r"^  ([a-z-]+):\n(.*?)(?=^  [a-z-]+:|\Z)",
                           text, re.MULTILINE | re.DOTALL))


def step(job, name):
    found = re.search(rf"^    - name: {re.escape(name)}\n(.*?)(?=^    - |\Z)",
                      job, re.MULTILINE | re.DOTALL)
    assert found, name
    run = re.search(r"^      run: \|\n((?:        [^\n]*\n|\n)+)",
                    found[1], re.MULTILINE)
    assert run, name
    return textwrap.dedent(run[1])


class RemotePerfWorkflowTest(unittest.TestCase):
    def test_remote_measurements_belong_to_both_vc_jobs(self):
        pr = jobs("benchmark.yml")
        nightly = jobs("nightly-performance.yml")
        self.assertIn("test/remote_perf.py", pr["vc-relative"])
        self.assertIn("test/remote_perf.py", nightly["version-control"])
        self.assertIn("REMOTE_PERF_RUNS: ${{ github.event.inputs.remote-runs || '5' }}",
                      nightly["version-control"])
        self.assertNotIn("test/remote_perf.py", pr["sysbench-relative"])
        self.assertNotIn("test/remote_perf.py", nightly["sysbench"])
        self.assertIn("name: benchmark-result-vc", pr["vc-relative"])
        self.assertIn("name: nightly-performance-vc", nightly["version-control"])
        self.assertIn("remote/report.md", pr["vc-relative"])
        self.assertIn("--remote-results benchmark-results/remote/results.json", pr["relative-report"])
        self.assertIn("--remote-results-dir", nightly["report"])
        initialize = step(nightly["version-control"], "Run absolute version-control benchmark")
        self.assertLess(initialize.index('mkdir -p "$results/remote"'),
                        initialize.index("benchmark_retry.py"))
        self.assertLess(initialize.index('echo 1 > "$results/remote.status"'),
                        initialize.index("benchmark_retry.py"))
        self.assertFalse((ROOT / ".github/workflows/remote-performance.yml").exists())

    def run_measurement(self, root, nightly, mode):
        name = "nightly-performance.yml" if nightly else "benchmark.yml"
        job = jobs(name)["version-control" if nightly else "vc-relative"]
        title = "Run 100 MB file-remote benchmark" if nightly else "Measure 100 MB file remotes against PR base"
        binary_dir = root / "bin"
        binary_dir.mkdir()
        stub = binary_dir / "python3"
        stub.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
from pathlib import Path
with open(os.environ["COMMAND_LOG"], "a") as stream:
    stream.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[1].endswith("remote_perf_test.py"):
    raise SystemExit(0)
if os.environ["REMOTE_MODE"] == "crash":
    raise SystemExit(42)
import remote_perf
result = json.loads(Path(os.environ["FAKE_RESULT"]).read_text())
if os.environ["REMOTE_MODE"] == "failure":
    result.update(status="failed", error="database mismatch", samples=[])
output = Path(sys.argv[sys.argv.index("--output-dir") + 1])
remote_perf.save_results(result, output)
print(remote_perf.report(result))
raise SystemExit(0 if result["status"] == "passed" else 1)
''')
        stub.chmod(0o755)
        fixture = root / "fixture.json"
        fixture.write_text(json.dumps(saved_result(
            ("candidate",) if nightly else ("baseline", "candidate"))))
        env = dict(os.environ, PATH=f"{binary_dir}:{os.environ['PATH']}",
                   PYTHONPATH=str(ROOT / "test"), GITHUB_WORKSPACE=str(ROOT),
                   RUNNER_TEMP=str(root / "runner"), GITHUB_STEP_SUMMARY=str(root / "summary"),
                   GITHUB_OUTPUT=str(root / "outputs"), REMOTE_PERF_RUNS="5",
                   COMMAND_LOG=str(root / "commands"), FAKE_RESULT=str(fixture), REMOTE_MODE=mode)
        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step(job, title)],
                                cwd=root, env=env, capture_output=True, text=True, timeout=20)
        command = json.loads((root / "commands").read_text().splitlines()[-1])
        self.assertEqual(command[command.index("--size-mb")+1], "100")
        self.assertEqual(command[command.index("--runs")+1], "5")
        self.assertEqual("--baseline" in command, not nightly)
        self.assertEqual(command[command.index("--candidate")+1],
                         str(ROOT / ("build-nightly" if nightly else "build") / "doltlite"))
        if not nightly:
            self.assertEqual(command[command.index("--baseline")+1],
                             str(ROOT / "benchmark-baseline/doltlite"))
        return result

    def test_pr_measurement_preserves_verification_failures(self):
        for mode in ("success", "failure", "crash"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                result = self.run_measurement(root, False, mode)
                self.assertEqual(result.returncode, {"success": 0, "failure": 1, "crash": 42}[mode], result)
                output = root / "runner/benchmark-results/remote"
                if mode != "crash":
                    self.assertTrue((output / "samples.tsv").exists())
                    self.assertTrue((output / "results.json").exists())
                if mode == "success":
                    self.assertIn("File remote performance", (root / "summary").read_text())

    def test_nightly_saves_failure_status_before_enforcement(self):
        for mode in ("success", "failure", "crash"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                result = self.run_measurement(root, True, mode)
                self.assertEqual(result.returncode, 0, result)
                code = {"success": 0, "failure": 1, "crash": 42}[mode]
                self.assertEqual((root / "runner/nightly-results/remote.status").read_text(), f"{code}\n")
                self.assertIn(f"bench_rc={code}", (root / "outputs").read_text())
                self.assertTrue((root / "runner/nightly-results/remote.stderr").exists())

    def test_nightly_enforces_remote_and_vc_failures(self):
        code = step(jobs("nightly-performance.yml")["version-control"],
                    "Enforce version-control results")
        for vc, remote in (("0", "0"), ("1", "0"), ("0", "2"), ("", "0"), ("0", "")):
            script = code.replace("${{ steps.benchmark.outputs.bench_rc }}", vc)
            script = script.replace("${{ steps.remote.outputs.bench_rc }}", remote)
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True)
            self.assertEqual(result.returncode == 0, vc == remote == "0")

    def test_pr_report_cannot_hide_incomplete_remote_measurements(self):
        code = step(jobs("benchmark.yml")["relative-report"], "Generate relative performance report")
        code = code.replace("${{ github.event.pull_request.base.sha }}", "base")
        code = code.replace("git rev-parse HEAD", "printf candidate")
        code = code.replace("${{ steps.measurements.outcome }}", "success")
        code = code.replace("${{ needs.hotspots.result }}", "success")
        for mode in ("success", "failure", "missing", "unverified", "job_failure"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / "test").symlink_to(ROOT / "test", target_is_directory=True)
                directory = root / "benchmark-results/remote"
                directory.mkdir(parents=True)
                for suite in ("int", "textpk", "blobpk", "compositepk", "vc"):
                    (directory.parent / f"{suite}.tsv").write_text(
                        f"# suite\t{suite}\n# producer_id\tworkflow\nreads\tpoint\t1000\t1000\n")
                    status = "command_error" if mode == "job_failure" and suite == "vc" else "pass"
                    (directory.parent / f"{suite}-attempts.tsv").write_text(
                        f"suite\t{suite}\nproducer_id\tworkflow\nattempt\tstatus\tfailed_gates\n1\t{status}\t[]\n")
                for arm, value in (("baseline", "base"), ("candidate", "candidate")):
                    (root / f"benchmark-{arm}-sha").write_text(value)
                result = saved_result(("baseline", "candidate"))
                if mode == "failure":
                    result.update(status="failed", error="database mismatch", samples=[])
                if mode == "unverified":
                    result["samples"][0]["verified"] = False
                if mode != "missing":
                    (directory / "results.json").write_text(json.dumps(result))
                env = dict(os.environ, RUNNER_TEMP=str(root),
                           GITHUB_STEP_SUMMARY=str(root / "summary"), GITHUB_OUTPUT=str(root / "output"))
                completed = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", code],
                                           cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(completed.returncode, 0, completed)
                self.assertEqual((root / "output").read_text().splitlines()[-1],
                                 f"report_rc={0 if mode == 'success' else 1}")
                report = (directory.parent / "summary.md").read_text()
                if mode == "success":
                    details = report.split("<summary>vc details</summary>", 1)[1].split("</details>", 1)[0]
                    self.assertIn("| file_remote | `push_initial` |", details)
                    self.assertNotIn("## File remote performance", report)
                else:
                    self.assertIn("**FAILED:**", report)

    def test_republish_old_artifacts_does_not_require_remote_results(self):
        code = step(jobs("nightly-performance.yml")["report"], "Compose nightly report")
        code = code[code.index("remote_args=("):code.index("for suite in $suites;")]
        for source, present, expect_remote in (("", False, True), ("123", False, False), ("123", True, True)):
            with self.subTest(source=source, present=present), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                if present:
                    (root / "nightly-results/remote").mkdir(parents=True)
                script = 'suites=vc\n' + code + 'printf "%s\\n" "${remote_args[*]}" "$suites"\n'
                completed = subprocess.run(["bash", "-e", "-c", script], cwd=root,
                                           env=dict(os.environ, SOURCE_RUN_ID=source),
                                           capture_output=True, text=True, check=True)
                self.assertEqual("--remote-results-dir" in completed.stdout, expect_remote)
                self.assertEqual(completed.stdout.endswith("vc remote\n"), expect_remote)


if __name__ == "__main__":
    unittest.main()
