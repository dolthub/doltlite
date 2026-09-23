#!/usr/bin/env python3

from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import performance_hotspot_fuzzer as fuzzer


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.profile = fuzzer.Profile(64, 32, "integer", 16, False, 4096, 8, 10, 3, 12, 8)

    def test_seed_and_profile_are_independently_reproducible(self):
        first = fuzzer.profile_for(20260922, 3)
        fuzzer.profile_for(100, 99)
        self.assertEqual(first, fuzzer.profile_for(20260922, 3))
        self.assertNotEqual(first, fuzzer.profile_for(20260923, 3))
        self.assertEqual(fuzzer.fixture_sql(first), fuzzer.fixture_sql(fuzzer.profile_for(20260922, 3)))

    def test_large_payload_profiles_have_bounded_fixture_size(self):
        for seed in range(10):
            for index, width in ((6, 4096), (7, 16384)):
                p = fuzzer.profile_for(seed, index)
                self.assertEqual(p.payload, width)
                self.assertLessEqual(p.payload*p.rows, 256*1024*1024)
                self.assertLessEqual(p.start+p.width, p.rows)

    def test_all_generated_workloads_execute_and_writes_roll_back(self):
        for key in ("integer", "text"):
            for skew in (False, True):
                p = replace(self.profile, key=key, skew=skew)
                with self.subTest(key=key, skew=skew), sqlite3.connect(":memory:") as db:
                    db.executescript(fuzzer.fixture_sql(p))
                    original = list(db.iterdump())
                    self.assertEqual(db.execute("SELECT count(*),sum(length(payload)) FROM t").fetchone(), (64, 2048))
                    for case in fuzzer.cases_for(p):
                        db.execute("EXPLAIN QUERY PLAN " + case.sql).fetchall()
                        if case.verify:
                            db.execute("BEGIN")
                            db.execute(case.sql)
                            result = db.execute(case.verify).fetchall()
                            db.rollback()
                        else:
                            result = db.execute(case.sql).fetchall()
                        self.assertEqual(len(result), 1, case.name)
                        self.assertEqual(list(db.iterdump()), original, case.name)

    def test_no_autocommit_write_workloads(self):
        for case in fuzzer.cases_for(self.profile):
            for timed in (False, True):
                sql = fuzzer.unit_sql(case, timed)
                if case.verify:
                    self.assertTrue(sql.startswith("BEGIN;\n"))
                    self.assertTrue(sql.endswith("ROLLBACK;\n"))
                    if timed:
                        self.assertLess(sql.index("BEGIN;"), sql.index(".timer on"))
                        self.assertLess(sql.index(".timer off"), sql.index("ROLLBACK;"))
                self.assertNotIn("COMMIT", sql)

    def test_parse_checks_every_result_and_timer(self):
        output = "WARM\n42\nMEASURE\n42\nRun Time: real 0.01 user 0.01 sys 0.0\n42\nRun Time: real 0.02 user 0.01 sys 0.0\nEND\n"
        self.assertEqual(fuzzer.parse_measurement(output, 2), {"ms": 30, "result": "42"})
        for bad in (output.replace("MEASURE\n42", "MEASURE\n43"),
                    output.replace("real 0.01", "real nan"),
                    output.replace("real 0.01", "real -1"),
                    output.replace("Run Time: real 0.02 user 0.01 sys 0.0\n", ""),
                    output.replace("WARM\n42", "WARM\n41"), output+"extra\n"):
            with self.subTest(output=bad), self.assertRaises(ValueError):
                fuzzer.parse_measurement(bad, 2)

    def test_confirmation_requires_every_pair_and_a_timing_floor(self):
        pairs = [{"sqlite_ms": 20, "doltlite_ms": 60} for _ in range(5)]
        self.assertTrue(fuzzer.classify(pairs, 3, 20, 5))
        self.assertFalse(fuzzer.classify(pairs[:4], 3, 20, 5))
        for bad in ({"sqlite_ms": 20, "doltlite_ms": 59.99},
                    {"sqlite_ms": 0.01, "doltlite_ms": 1},
                    {"sqlite_ms": 0, "doltlite_ms": 100}):
            self.assertFalse(fuzzer.classify(pairs[:4]+[bad], 3, 20, 5))

    def test_screen_is_not_a_confirmation_sample(self):
        class FakeRunner:
            def __init__(self):
                self.calls = []

            def measure(self, binary, db, profile, case, repeats):
                self.calls.append(binary)
                return {"ms": 30 if binary == "sqlite" else 100, "result": "42"}

        runner = FakeRunner()
        binaries = {"sqlite": "sqlite", "doltlite": "doltlite"}
        result = fuzzer.measure_case(runner, binaries, binaries, self.profile,
                                     fuzzer.cases_for(self.profile)[0], 5, 3, 20)
        self.assertTrue(result["confirmed"])
        self.assertEqual(len(result["pairs"]), 5)
        self.assertEqual(len(runner.calls), 14)
        self.assertEqual(runner.calls[2:6], ["sqlite", "doltlite", "doltlite", "sqlite"])

    def test_extreme_slowdowns_do_not_multiply_into_long_batches(self):
        runner = unittest.mock.Mock()
        runner.measure.side_effect = lambda binary, db, p, case, repeats: {
            "ms": (0.01 if binary == "s" else 2000)*repeats, "result": "42"}
        result = fuzzer.measure_case(runner, {"sqlite": "s", "doltlite": "d"},
                                     {"sqlite": "s", "doltlite": "d"}, self.profile,
                                     fuzzer.cases_for(self.profile)[0], 5, 3, 20)
        self.assertEqual(result["repeats"], 1)
        self.assertTrue(result["confirmed"])

    def test_mismatches_are_errors_even_for_fast_cases(self):
        runner = unittest.mock.Mock()
        runner.measure.side_effect = [{"ms": 20, "result": "1"},
                                      {"ms": 1, "result": "2"}]
        with self.assertRaisesRegex(ValueError, "mismatch"):
            fuzzer.measure_case(runner, {"sqlite": "s", "doltlite": "d"},
                                {"sqlite": "s", "doltlite": "d"}, self.profile,
                                fuzzer.cases_for(self.profile)[0], 5, 3, 20)

    def test_budget_and_subprocess_errors_cannot_be_hotspots(self):
        runner = fuzzer.Runner(10, 1)
        with patch.object(fuzzer.subprocess, "run", side_effect=subprocess.TimeoutExpired("engine", 1)):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                runner.run(["engine"])
        runner.case_deadline = 0
        with self.assertRaises(fuzzer.CaseTimeout):
            runner.run(["engine"])
        runner.deadline = 0
        with self.assertRaises(fuzzer.BudgetExpired):
            runner.run(["engine"])
        runner = fuzzer.Runner(10, 1)
        for code, stderr in ((1, ""), (0, "SQL error")):
            with patch.object(fuzzer.subprocess, "run", return_value=subprocess.CompletedProcess("engine", code, "", stderr)):
                with self.assertRaisesRegex(RuntimeError, "failed"):
                    runner.run(["engine"])

    def test_engine_probes_accept_binary_headers_without_stderr(self):
        root = Path(__file__).resolve().parent
        runner = fuzzer.Runner(30, 5)
        with tempfile.TemporaryDirectory() as tmp:
            binaries = {}
            for kind, header in (("doltlite", b"CTLD\x0c\x00\x00\x00" + b"\x00"*8),
                                 ("sqlite", b"SQLite format 3\x00")):
                binary = Path(tmp)/kind
                binary.write_text(
                    "#!/usr/bin/env python3\n"
                    "import pathlib, sys\n"
                    "if sys.argv[2].startswith('CREATE TABLE'):\n"
                    f"    pathlib.Path(sys.argv[1]).write_bytes({header!r})\n"
                    "elif sys.argv[2] == 'SELECT doltlite_engine();':\n"
                    "    print('prolly')\n"
                    "elif sys.argv[2] == 'SELECT sqlite_version();':\n"
                    "    print('3.54.0')\n")
                binary.chmod(0o755)
                binaries[kind] = str(binary)
            engine_probe = ["bash", str(root/"lib/assert_doltlite_engine.sh")]
            stock_probe = ["bash", str(root/"assert_stock_reference.sh")]
            self.assertIn("OK:", runner.run(engine_probe + [binaries["doltlite"]]))
            self.assertIn("OK:", runner.run(stock_probe + [binaries["sqlite"], binaries["doltlite"]]))
            for probe, binary, message in (
                (engine_probe, binaries["sqlite"], "writes an SQLite database header"),
                (stock_probe, binaries["doltlite"], "does not write an SQLite database header"),
            ):
                with self.subTest(probe=probe):
                    result = subprocess.run(probe + [binary], text=True, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(message, result.stdout)
                    self.assertEqual(result.stderr, "")

    def test_partial_report_and_errors_are_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = {"seed": 1, "runs": 5, "threshold": 3, "min_ms": 20,
                      "profiles_completed": 0, "status": "budget exhausted (partial search)",
                      "cases": [{"id": "p000/scan", "error": "result mismatch"},
                                {"id": "p000/slow", "timeout": "60s timeout", "reproducer": "p000/slow.json"}]}
            fuzzer.save_report(Path(tmp), report)
            self.assertEqual(json.loads((Path(tmp)/"results.json").read_text()), report)
            summary = (Path(tmp)/"summary.md").read_text()
            self.assertIn("partial search", summary)
            self.assertIn("result mismatch", summary)
            self.assertIn("Timed out (unconfirmed)", summary)
            self.assertIn("No confirmed hotspots in the completed cases", summary)

    def test_main_preserves_timeout_reproducer_without_confirming_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/"results"
            with patch.object(fuzzer.shutil, "copyfile"), patch.object(fuzzer.Runner, "run", return_value="ok"), \
                 patch.object(fuzzer, "binary_info", return_value={}), \
                 patch.object(fuzzer, "profile_for", return_value=self.profile), \
                 patch.object(fuzzer, "measure_case", side_effect=fuzzer.CaseTimeout("timed out")):
                rc = fuzzer.main(["--doltlite", "unused", "--sqlite", "unused", "--output", str(output),
                                  "--profiles", "1", "--case", "scan_payload"])
            self.assertEqual(rc, 0)
            report = json.loads((output/"results.json").read_text())
            self.assertEqual(report["cases"][0]["timeout"], "timed out")
            self.assertFalse(report["cases"][0].get("confirmed"))
            repro = json.loads((output/"p000/scan_payload.json").read_text())
            self.assertEqual(repro["case"]["name"], "scan_payload")
            self.assertTrue((output/"p000/setup.sql").is_file())
            self.assertTrue((output/"p000/scan_payload.sql").is_file())

    def test_nightly_is_independent_and_preserves_reproducers(self):
        root = Path(__file__).resolve().parents[1]
        workflow = (root/".github/workflows/nightly-hotspots.yml").read_text()
        self.assertIn("schedule:", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("build_stock_reference.sh --shell-only", workflow)
        self.assertNotIn("sysbench", workflow)
        self.assertNotIn("PROLLY_CHECK=1", workflow)
        self.assertIn("--seed", workflow)
        self.assertIn("--runs 5", workflow)
        self.assertIn("--seconds 1800", workflow)
        self.assertIn("if: always()", workflow)
        self.assertIn("actions/upload-artifact@v4", workflow)
        self.assertIn("nightly-hotspots", (root/".github/workflows/nightly-heartbeat.yml").read_text())


if __name__ == "__main__":
    unittest.main()
