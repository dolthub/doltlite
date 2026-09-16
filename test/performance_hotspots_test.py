#!/usr/bin/env python3

import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch

import benchmark_compare
import performance_hotspots as hotspots


class HotspotTests(unittest.TestCase):
    def setUp(self):
        self.cases = [("scan_first", "SELECT 42;", "42"),
                      ("scan_repeat", "SELECT 42;", "42")]
        self.output = ("BEGIN scan_first\n42\n"
                       "Run Time: real 0.120000 user 0.100000 sys 0.020000\n"
                       "END scan_first\nBEGIN scan_repeat\n42\n"
                       "Run Time: real 0.080000 user 0.070000 sys 0.010000\n"
                       "END scan_repeat\n")

    def test_internal_timings(self):
        self.assertEqual(hotspots.parse_session(self.output, self.cases),
                         {"scan_first": 120000, "scan_repeat": 80000})

    def test_bad_or_missing_measurements_fail(self):
        for output in ("", self.output.replace("42", "41"),
                       self.output.replace("0.120000", "0.000000"),
                       self.output.replace("END scan_repeat", ""),
                       self.output + "Error: ignored failure\n",
                       self.output.replace("Run Time: real 0.080000 user 0.070000 sys 0.010000\n", ""),
                       self.output.replace("END scan_first", "Run Time: real 0.01 user 0.01 sys 0.00\nEND scan_first")):
            with self.subTest(output=output), self.assertRaises(ValueError):
                hotspots.parse_session(output, self.cases)

    def test_invalid_times_fail(self):
        for value in ("0", "-1", "nan", "inf", "oops"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                hotspots.positive_us(value)

    def test_fixture_must_exceed_cache(self):
        for options in (("--rows", "100"), ("--runs", "0"), ("--cache-kib", "0")):
            with self.subTest(options=options), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    hotspots.main(["--baseline", "unused", "--candidate", "unused",
                                   "--stock", "unused", *options])
                self.assertEqual(caught.exception.code, 2)

    def test_sql_failure_is_not_a_timing(self):
        with self.assertRaises(RuntimeError):
            hotspots.run(["sh", "-c", "echo 'partial output'; exit 1"])
        with self.assertRaises(RuntimeError):
            hotspots.run(["sh", "-c", "echo 'error' >&2"])

    def test_checkpoint_samples_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "samples.tsv"
            header = "run\tbelow_seconds\tcheckpoint_seconds\tpost_seconds\n"
            with patch.object(hotspots, "run", return_value="validated"):
                for body in (header, header + "1\t0.01\t0\t0.01\n",
                             header + "2\t0.01\t0.02\t0.01\n"):
                    output.write_text(body)
                    with self.assertRaises(ValueError):
                        hotspots.measure_checkpoint(Path("engine"), output)
                output.write_text(header + "1\t0.01\t0.02\t0.011\n")
                self.assertEqual(hotspots.measure_checkpoint(Path("engine"), output),
                                 {"append_below": 10000, "append_checkpoint": 20000,
                                  "append_post": 11000})

    def test_medians_raw_samples_and_stock_report(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            samples = {
                "baseline": [{"scan_first": n, "append_checkpoint": n}
                             for n in (100000, 900000, 120000)],
                "candidate": [{"scan_first": n, "append_checkpoint": n}
                              for n in (240000, 200000, 900000)],
                "stock": [{"scan_first": n} for n in (50000, 45000, 55000)],
            }
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, 262144, 65536, {"candidate": 300000000}, result, raw)
            self.assertEqual(result.read_text(),
                             "queries\tscan_first\t120000\t240000\n"
                             "checkpoint\tappend_checkpoint\t120000\t240000\n")
            self.assertIn("240.000 | 2.00× | 50.000 | 4.80×", report.getvalue())
            self.assertIn("checkpoint\tappend_checkpoint\t3\t120000\t900000\t\n", raw.read_text())
            parsed, _metadata = benchmark_compare.parse_input_artifact(f"hotspots={result}")
            analysis = benchmark_compare.analyze(parsed, 1.5, 1.25, 10000)
            self.assertTrue(analysis["individual_failures"])
            self.assertTrue(analysis["section_failures"])
            self.assertIn(("hotspots", "checkpoint", "append_checkpoint"),
                          analysis["individual_failures"])

    def test_hotspot_failure_reaches_existing_gate(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        report = workflow.split("    - name: Generate relative performance report\n", 1)[1]
        report = textwrap.dedent(report.split("      run: |\n", 1)[1].split("\n    - name:", 1)[0])
        report = report.replace("${{ github.event.pull_request.base.sha }}", "base")
        report = report.replace("git rev-parse HEAD", "printf candidate")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "benchmark-results").mkdir()
            (root / "bin").mkdir()
            stub = root / "bin/python3"
            stub.write_text('#!/bin/sh\nwhile [ "$1" != --output ]; do shift; done\n'
                            'printf "Standard benchmarks passed\\n" > "$2"\n')
            stub.chmod(0o755)
            for suite in ("int", "textpk", "blobpk", "compositepk", "vc"):
                for suffix in (".tsv", "-attempts.tsv"):
                    (root / f"benchmark-results/{suite}{suffix}").write_text("fixture\n")
            for arm, value in (("baseline", "base"), ("candidate", "candidate")):
                (root / f"benchmark-{arm}-sha").write_text(value)
            for status in ("success", "failure", "skipped", "cancelled"):
                with self.subTest(status=status):
                    env = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}",
                               RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary"),
                               GITHUB_OUTPUT=str(root / "output"))
                    subprocess.run(["bash", "-e", "-o", "pipefail", "-c",
                                    report.replace("${{ needs.hotspots.result }}", status)],
                                   cwd=root, env=env, check=True, capture_output=True)
                    self.assertEqual((root / "output").read_text().splitlines()[-1],
                                     f"report_rc={0 if status == 'success' else 1}")

    def test_hotspot_comment_summary(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        step = workflow.split("    - name: Publish hotspot measurements\n", 1)[1]
        script = textwrap.dedent(step.split("      run: |\n", 1)[1].split("\n    - name:", 1)[0])
        for key, value in (("github.server_url", "https://github.com"),
                           ("github.repository", "dolthub/doltlite"),
                           ("github.run_id", "123")):
            script = script.replace("${{ " + key + " }}", value)
        for status, measured in (("success", True), ("failure", True), ("failure", False)):
            with self.subTest(status=status, measured=measured), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                results = root / "hotspot-results"
                if measured:
                    results.mkdir()
                    (results / "hotspots.md").write_text("## Performance hotspots\n\nMeasured table\n")
                env = dict(os.environ, RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary"))
                subprocess.run(["bash", "-e", "-o", "pipefail", "-c",
                                script.replace("${{ steps.hotspots.outcome }}", status)],
                               env=env, check=True, capture_output=True)
                body = (results / "summary.md").read_text()
                self.assertEqual(body, (root / "summary").read_text())
                self.assertIn("<!-- benchmark:hotspots -->", body)
                self.assertIn(f"**Gate:** {status}", body)
                self.assertIn("https://github.com/dolthub/doltlite/actions/runs/123", body)
                self.assertIn("Measured table" if measured else "No complete hotspot measurements", body)

    def test_hotspot_comment_creates_then_updates(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        step = workflow.split("    - name: Comment PR with performance hotspots\n", 1)[1]
        script = textwrap.dedent(step.split("        script: |\n", 1)[1].split("\n    - name:", 1)[0])
        harness = r"""
const assert = require('node:assert/strict');
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const publish = new AsyncFunction('github', 'context', 'require', 'process', process.argv[1]);
let body = '<!-- benchmark:hotspots -->\nfirst results';
let exists = true;
const calls = [];
const comments = [{id: 1, body: '<!-- benchmark:relative -->\nstandard results'}];
const issues = {
  listComments: 'list',
  createComment: async args => { calls.push(['create', args]); comments.push({id: 2, body: args.body}); },
  updateComment: async args => { calls.push(['update', args]); },
};
const github = {rest: {issues}, paginate: async (method, args) => {
  assert.equal(method, 'list');
  assert.equal(args.issue_number, 10);
  return comments;
}};
const context = {repo: {owner: 'dolthub', repo: 'doltlite'}, issue: {number: 10}};
const fakeRequire = name => {
  assert.equal(name, 'fs');
  return {existsSync: () => exists, readFileSync: () => body};
};
async function main() {
  const invoke = () => publish(github, context, fakeRequire, {env: {RUNNER_TEMP: '/tmp'}});
  await invoke();
  body = '<!-- benchmark:hotspots -->\nupdated results';
  await invoke();
  assert.deepEqual(calls.map(call => call[0]), ['create', 'update']);
  assert.equal(calls[0][1].issue_number, 10);
  assert.equal(calls[1][1].comment_id, 2);
  assert.equal(calls[1][1].body, body);
  assert.equal(comments[0].body, '<!-- benchmark:relative -->\nstandard results');
  context.issue.number = undefined;
  await invoke();
  context.issue.number = 10;
  exists = false;
  await invoke();
  assert.equal(calls.length, 2);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
"""
        subprocess.run(["node", "-e", harness, script], check=True, capture_output=True)


if __name__ == "__main__":
    unittest.main()
