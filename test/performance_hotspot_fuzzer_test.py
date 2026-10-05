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
import performance_hotspot_search as search


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.profile = fuzzer.Profile(64, 32, "integer", 16, False, 4096, 8, 10, 3, 12, 8)

    def test_seed_and_profile_are_independently_reproducible(self):
        first = fuzzer.profile_for(20260922, 3)
        fuzzer.profile_for(100, 99)
        self.assertEqual(first, fuzzer.profile_for(20260922, 3))
        self.assertNotEqual(first, fuzzer.profile_for(20260923, 3))
        self.assertEqual(fuzzer.fixture_sql(first), fuzzer.fixture_sql(fuzzer.profile_for(20260922, 3)))
        self.assertEqual({fuzzer.profile_for(20260922, i).memory for i in range(32)}, {False, True})

    def test_memory_measurements_recreate_fixture_before_timing(self):
        p = replace(self.profile, memory=True)
        case = fuzzer.Case('update_text', "UPDATE t SET tag='new';", 'SELECT count(*) FROM t;')
        setup = fuzzer.fixture_sql(p)
        runner = fuzzer.Runner(30, 5)
        output = 'WARM\n64\nMEASURE\n64\nRun Time: real 0.01 user 0.01 sys 0.0\nEND\n'
        with patch.object(runner, 'run', return_value=output) as run:
            for _ in range(2):
                result = runner.measure('engine', 'unused.db', p, case, 1, setup)
                self.assertEqual(result['ms'], 10)
            for call in run.call_args_list:
                self.assertEqual(call.args[0], ['engine', ':memory:'])
                sql = call.args[1]
                self.assertEqual(sql.count(setup), 1)
                self.assertLess(sql.index(setup), sql.index('.print WARM'))
                self.assertLess(sql.index(setup), sql.index('.timer on'))
                self.assertEqual(sql.count('ROLLBACK;'), 2)
            with self.assertRaisesRegex(ValueError, 'fixture SQL'):
                runner.measure('engine', 'unused.db', p, case, 1)

    def test_generated_profiles_are_never_wide(self):
        for seed in range(10):
            for index in range(16):
                p = fuzzer.profile_for(seed, index)
                self.assertLess(p.payload, search.WIDE_PAYLOAD)
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

    def test_queries_under_the_sqlite_floor_need_three_times_the_floor(self):
        class FakeRunner:
            def __init__(self, sqlite_ms, doltlite_ms):
                self.ms = {"sqlite": sqlite_ms, "doltlite": doltlite_ms}

            def measure(self, binary, db, profile, case, repeats):
                return {"ms": self.ms[binary]*repeats, "result": "42"}

        binaries = {"sqlite": "sqlite", "doltlite": "doltlite"}
        case = fuzzer.cases_for(self.profile)[0]
        for sqlite_ms, doltlite_ms, confirmed in ((1, 29, False), (1, 31, True),
                                                  (5, 29, False), (12, 37, True),
                                                  (12, 35, False)):
            with self.subTest(sqlite_ms=sqlite_ms, doltlite_ms=doltlite_ms):
                result = fuzzer.measure_case(FakeRunner(sqlite_ms, doltlite_ms), binaries,
                                             binaries, self.profile, case, 5, 3, 20,
                                             min_query_ms=10)
                self.assertEqual(result["confirmed"], confirmed)
                self.assertEqual(bool(result["pairs"]), confirmed or doltlite_ms >= 24)

    def test_case_deadline_grows_to_fit_confirmation_and_is_capped(self):
        class Deadline:
            timeout = 60
        runner = Deadline()
        with patch.object(fuzzer.time, "monotonic", return_value=1000.0):
            runner.case_deadline = 1060.0
            fuzzer.extend_case_deadline(runner, 5, 1, 4.0, 26.0, 1900.0, 13000.0)
            self.assertAlmostEqual(runner.case_deadline, 1000.0 + 1.5 * 6 * 30.0)
            runner.case_deadline = 1060.0
            fuzzer.extend_case_deadline(runner, 5, 1, 40.0, 60.0, 1900.0, 13000.0)
            self.assertEqual(runner.case_deadline, 1000.0 + 10 * 60)
            runner.case_deadline = 1060.0
            fuzzer.extend_case_deadline(runner, 5, 157, 0.8, 0.3, 0.3, 1.2)
            self.assertAlmostEqual(runner.case_deadline, 1060.0)
            runner.case_deadline = None
            fuzzer.extend_case_deadline(runner, 5, 1, 40.0, 60.0, 1.0, 1.0)
            self.assertIsNone(runner.case_deadline)

    def test_timeouts_carry_the_phase_and_timings_measured_so_far(self):
        class TimingOut:
            def __init__(self, fail_at):
                self.fail_at = fail_at
                self.calls = 0

            def measure(self, binary, db, profile, case, repeats):
                self.calls += 1
                if self.calls == self.fail_at:
                    raise fuzzer.CaseTimeout("command timed out")
                return {"ms": 100.0 if binary == "sqlite" else 700.0, "result": "42"}

        binaries = {"sqlite": "sqlite", "doltlite": "doltlite"}
        case = fuzzer.cases_for(self.profile)[0]
        expected = {2: {"timeout_phase": "pilot", "sqlite_ms": 100.0},
                    7: {"timeout_phase": "confirmation", "sqlite_ms": 100.0,
                        "doltlite_ms": 700.0, "pairs_completed": 1}}
        for fail_at, evidence in expected.items():
            with self.subTest(fail_at=fail_at), self.assertRaises(fuzzer.CaseTimeout) as caught:
                fuzzer.measure_case(TimingOut(fail_at), binaries, binaries, self.profile, case, 5, 3, 20)
            self.assertEqual(caught.exception.evidence, evidence)
        line = fuzzer.timeout_evidence(dict(expected[7], timeout="x"))
        self.assertEqual(line, "Ran out during confirmation; SQLite 100.0 ms, DoltLite 700.0 ms (7.0x), "
                               "confirmation pairs done: 1. ")
        self.assertEqual(fuzzer.timeout_evidence({"timeout": "x"}), "")

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

    def run_pending_check(self, context, plain_ratio):
        calls = []
        def measure(runner, binaries, databases, p, case, runs, threshold, min_ms, setup=None, min_query_ms=0):
            calls.append(case.prepare)
            control = len(calls) > 1
            pairs = [{"doltlite_ms": 600.0, "sqlite_ms": 100.0}] * 5
            return {"pairs": pairs, "result": "1", "repeats": 1, "doltlite_ms": 600.0, "sqlite_ms": 100.0,
                    "ratio": plain_ratio if control else 6.0, "confirmed": not control}
        profile = replace(self.profile, cache_kib=fuzzer.CACHED_CHECK_KIB)
        case = fuzzer.Case("generated_0", "INSERT INTO t SELECT * FROM t WHERE 1;", "SELECT 1;",
                           "UPDATE t SET v=v+1;" if context != "plain" else "",
                           {"context": context, "operator": "upsert_update"})
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/"results"
            with patch.object(fuzzer.shutil, "copyfile"), patch.object(fuzzer.Runner, "run", return_value="ok"), \
                 patch.object(fuzzer, "binary_info", return_value={}), \
                 patch.object(fuzzer, "probe_counter_scale", return_value=None), \
                 patch.object(fuzzer.Search, "specs", return_value=iter([(0, profile, [case], "", "fresh")])), \
                 patch.object(fuzzer, "measure_case", side_effect=measure):
                rc = fuzzer.main(["--doltlite", "unused", "--sqlite", "unused", "--output", str(output),
                                  "--search"])
            self.assertEqual(rc, 0)
            record = json.loads((output/"results.json").read_text())["cases"][0]
            return record, calls, (output/"summary.md").read_text()

    def test_gap_that_closes_without_the_prior_write_is_the_pending_edit_map(self):
        record, calls, summary = self.run_pending_check("after_update", 0.3)
        self.assertEqual(calls, ["UPDATE t SET v=v+1;", ""])
        self.assertFalse(record["confirmed"])
        self.assertTrue(record["pending_edits"])
        self.assertEqual(record["plain_ratio"], 0.3)
        self.assertIn("### Known: pending edit map (#3418)", summary)
        self.assertIn("6.00× after the write, 0.30× without it", summary)
        self.assertNotIn("Screened at", summary)

    def test_gap_that_persists_without_the_prior_write_stays_confirmed(self):
        record, calls, summary = self.run_pending_check("after_delete", 5.0)
        self.assertEqual(len(calls), 2)
        self.assertTrue(record["confirmed"])
        self.assertNotIn("pending_edits", record)
        self.assertNotIn("Known: pending edit map", summary)

    def test_contexts_without_pending_edits_skip_the_control(self):
        record, calls, _summary = self.run_pending_check("plain", 0.3)
        self.assertEqual(calls, [""])
        self.assertTrue(record["confirmed"])
        self.assertNotIn("plain_ratio", record)

    def test_counter_scale_flags_quadratic_single_scan(self):
        plan = "QUERY PLAN\n`--SCAN t\n"
        self.assertFalse(fuzzer.counter_scale_superlinear(99, 2000, plan))
        self.assertFalse(fuzzer.counter_scale_superlinear(100, 533, plan))
        self.assertFalse(fuzzer.counter_scale_superlinear(100, 1600, "SCAN t\nSCAN u\n"))
        self.assertTrue(fuzzer.counter_scale_superlinear(100, 1600, plan))

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

    def test_large_unconfirmed_ratios_are_listed_for_review(self):
        def case(name, ratio, **extra):
            return dict({"id": f"p000/{name}", "ratio": ratio, "doltlite_ms": 8.5,
                         "sqlite_ms": 8.5 / (ratio or 1), "reproducer": f"p000/{name}.json",
                         "pairs": [], "confirmed": False}, **extra)
        cases = [case("floor_105", 105.2), case("floor_24", 24.8), case("small", 9.9),
                 case("cached", 40.0, uncached_reads=True, cached_ratio=1.2),
                 case("slow", 50.0, timeout="t", timeout_phase="confirmation"),
                 case("broken", 50.0, error="e"), case("none", None)]
        with tempfile.TemporaryDirectory() as tmp:
            fuzzer.save_report(Path(tmp), {"seed": 1, "runs": 5, "threshold": 3, "min_ms": 20,
                                           "profiles_completed": 1, "status": "complete",
                                           "cases": cases})
            summary = (Path(tmp)/"summary.md").read_text()
        section = summary.split("### Screened at 10× or more, not confirmed (not filed)\n", 1)[1]
        listed = [line for line in section.splitlines() if line.startswith("- `")]
        self.assertEqual(listed, [
            "- `p000/floor_105`: 105.2×, DoltLite 8.500 ms, SQLite 0.081 ms. `p000/floor_105.json`",
            "- `p000/floor_24`: 24.8×, DoltLite 8.500 ms, SQLite 0.343 ms. `p000/floor_24.json`"])

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

    def test_setup_timeout_skips_only_that_profile(self):
        setups = []
        def run(runner, command, sql=None, setup=False):
            if setup:
                setups.append(command)
                if len(setups)==1:
                    raise fuzzer.CaseTimeout("command timed out: setup")
            return "ok"
        def measure(*args, **kwargs):
            return {"pairs": [], "ratio": 1.0, "confirmed": False, "result": "1", "repeats": 1,
                    "doltlite_ms": 1.0, "sqlite_ms": 1.0}
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/"results"
            with patch.object(fuzzer.shutil, "copyfile"), \
                 patch.object(fuzzer.Runner, "run", autospec=True, side_effect=run), \
                 patch.object(fuzzer, "binary_info", return_value={}), \
                 patch.object(fuzzer, "profile_for", return_value=self.profile), \
                 patch.object(fuzzer, "measure_case", side_effect=measure):
                rc = fuzzer.main(["--doltlite", "unused", "--sqlite", "unused", "--output", str(output),
                                  "--profiles", "2", "--case", "scan_payload"])
            self.assertEqual(rc, 0)
            report = json.loads((output/"results.json").read_text())
            ids = [case["id"] for case in report["cases"]]
            self.assertEqual(ids, ["p000/setup", "p001/scan_payload"])
            self.assertEqual(len(setups), 3)
            self.assertEqual(report["cases"][0]["timeout"], "command timed out: setup")
            self.assertEqual(report["cases"][0]["reproducer"], "p000/setup.sql")
            self.assertTrue((output/"p000/setup.sql").is_file())
            self.assertEqual(report["profiles_completed"], 1)
            self.assertEqual(report["status"], "complete")
            self.assertIn("p000/setup", (output/"summary.md").read_text())

    def test_setup_failure_other_than_timeout_still_fails_the_search(self):
        def run(runner, command, sql=None, setup=False):
            if setup:
                raise RuntimeError("command failed: setup")
            return "ok"
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/"results"
            with patch.object(fuzzer.Runner, "run", autospec=True, side_effect=run), \
                 patch.object(fuzzer, "binary_info", return_value={}), \
                 patch.object(fuzzer, "profile_for", return_value=self.profile):
                rc = fuzzer.main(["--doltlite", "unused", "--sqlite", "unused", "--output", str(output),
                                  "--profiles", "2", "--case", "scan_payload"])
            self.assertEqual(rc, 1)
            report = json.loads((output/"results.json").read_text())
            self.assertEqual(report["status"], "error")

    def test_retired_seeds_confirm_against_their_own_baseline(self):
        import performance_hotspot_seeds as seeds
        retired = fuzzer.Case("generated_1", "SELECT 1;")
        fresh = fuzzer.Case("generated_2", "SELECT 2;")
        calls = []
        def measure(runner, binaries, databases, p, case, runs, threshold, min_ms, setup=None, min_query_ms=0):
            calls.append((case.name, threshold, min_query_ms))
            return {"pairs": [], "ratio": 1.0, "confirmed": False, "result": "1", "repeats": 1,
                    "doltlite_ms": 1.0, "sqlite_ms": 1.0}
        specs = [(0, self.profile, [retired], "", "retired"), (1, self.profile, [fresh], "", "fresh")]
        with tempfile.TemporaryDirectory() as tmp:
            baselines = Path(tmp)/"baselines.json"
            baselines.write_text(json.dumps({seeds.seed_key(self.profile, retired): 2.4}))
            output = Path(tmp)/"results"
            with patch.object(fuzzer.shutil, "copyfile"), patch.object(fuzzer.Runner, "run", return_value="ok"), \
                 patch.object(fuzzer, "binary_info", return_value={}), \
                 patch.object(fuzzer, "probe_counter_scale", return_value=None), \
                 patch.object(fuzzer.Search, "specs", return_value=iter(specs)), \
                 patch.object(seeds, "BASELINES", baselines), \
                 patch.object(fuzzer, "measure_case", side_effect=measure):
                rc = fuzzer.main(["--doltlite", "unused", "--sqlite", "unused", "--output", str(output),
                                  "--search", "--nightly-seeds"])
            self.assertEqual(rc, 0)
            report = json.loads((output/"results.json").read_text())
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "generated_1")
        self.assertAlmostEqual(calls[0][1], 3.6)
        self.assertEqual(calls[0][2], 0)
        self.assertEqual(calls[1], ("generated_2", 3.0, 10))
        self.assertEqual([case["origin"] for case in report["cases"]], ["retired", "fresh"])
        self.assertAlmostEqual(report["cases"][0]["threshold"], 3.6)
        self.assertNotIn("threshold", report["cases"][1])

    def run_cached_check(self, cached_ratio):
        calls = []
        def measure(runner, binaries, databases, p, case, runs, threshold, min_ms, setup=None, min_query_ms=0):
            calls.append(p.cache_kib)
            cached = p.cache_kib == fuzzer.CACHED_CHECK_KIB
            pairs = [{"doltlite_ms": 920.0, "sqlite_ms": 2.7}] * 5
            return {"pairs": pairs, "result": "1", "repeats": 23, "doltlite_ms": 40.0, "sqlite_ms": 0.12,
                    "ratio": cached_ratio if cached else 343.0, "confirmed": not cached}
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/"results"
            with patch.object(fuzzer.shutil, "copyfile"), patch.object(fuzzer.Runner, "run", return_value="ok"), \
                 patch.object(fuzzer, "binary_info", return_value={}), \
                 patch.object(fuzzer, "probe_counter_scale", return_value=None), \
                 patch.object(fuzzer, "profile_for", return_value=replace(self.profile, cache_kib=16384)), \
                 patch.object(fuzzer, "measure_case", side_effect=measure):
                rc = fuzzer.main(["--doltlite", "unused", "--sqlite", "unused", "--output", str(output),
                                  "--profiles", "1", "--case", "scan_payload"])
            self.assertEqual(rc, 0)
            self.assertEqual(calls, [16384, fuzzer.CACHED_CHECK_KIB])
            return json.loads((output/"results.json").read_text())["cases"][0], (output/"summary.md").read_text()

    def test_gap_that_persists_with_a_cached_table_stays_confirmed(self):
        record, summary = self.run_cached_check(300.0)
        self.assertTrue(record["confirmed"])
        self.assertNotIn("uncached_reads", record)
        self.assertEqual(record["cached_ratio"], 300.0)
        self.assertNotIn("Known: uncached reads", summary)

    def test_gap_that_closes_with_a_cached_table_is_uncached_reads(self):
        record, summary = self.run_cached_check(1.4)
        self.assertFalse(record["confirmed"])
        self.assertTrue(record["uncached_reads"])
        self.assertIn("Known: uncached reads", summary)

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
