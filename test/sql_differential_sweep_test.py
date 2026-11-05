#!/usr/bin/env python3

import importlib.util
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


HERE = pathlib.Path(__file__).resolve().parent
FUZZER = HERE / "sql_differential_fuzzer.py"
SWEEP = HERE / "sql_differential_sweep.py"

SPEC = importlib.util.spec_from_file_location("sql_differential_fuzzer", FUZZER)
fuzz = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fuzz)

SWEEP_SPEC = importlib.util.spec_from_file_location("sql_differential_sweep", SWEEP)
sweep = importlib.util.module_from_spec(SWEEP_SPEC)
SWEEP_SPEC.loader.exec_module(sweep)


def write_stub(path, body):
    path.write_text("#!%s\n%s" % (sys.executable, body))
    path.chmod(0o755)


class ParseGroupsTest(unittest.TestCase):
    def test_default_flags(self):
        groups, unknown, rotate = fuzz.parse_groups(
            ["--include-large-ints", "--include-desc"])
        self.assertEqual(groups, ["large-ints", "desc"])
        self.assertEqual(unknown, [])
        self.assertEqual(rotate, 0)

    def test_all(self):
        groups, unknown, rotate = fuzz.parse_groups(["--all"])
        self.assertEqual(groups, fuzz.GROUPS)
        self.assertEqual(unknown, [])
        self.assertEqual(rotate, 0)

    def test_unknown(self):
        groups, unknown, rotate = fuzz.parse_groups(["--include-nope"])
        self.assertEqual(groups, [])
        self.assertEqual(unknown, ["--include-nope"])
        self.assertEqual(rotate, 0)

    def test_rotate_flag(self):
        groups, unknown, rotate = fuzz.parse_groups(
            ["--include-large-ints", "--include-desc", "--include-rowid",
             "--rotate"])
        self.assertEqual(groups, ["large-ints", "desc", "rowid"])
        self.assertEqual(unknown, [])
        self.assertEqual(rotate, 1)

    def test_rotation_covers_every_other_group(self):
        seen = set()
        for seed in range(len(fuzz.ROTATE_GROUPS) * 2):
            seen.update(fuzz.extra_groups(seed, 1))
        self.assertEqual(seen, set(fuzz.ROTATE_GROUPS))
        self.assertNotIn("rowid", seen)
        self.assertNotIn("large-ints", seen)

    def test_rowid_scripts_exercise_allocation(self):
        blob = "\n".join(fuzz.Gen(s, ["rowid"]).run() for s in range(40))
        for needle in ("AUTOINCREMENT", "last_insert_rowid", "sqlite_sequence",
                       "ROLLBACK TO", "INSERT INTO rida(v) SELECT",
                       "INSERT INTO t(a, b)"):
            self.assertIn(needle, blob)

    def test_pr_default_selects_rowid_and_rotation(self):
        text = (HERE / "sql_differential_test.sh").read_text()
        self.assertIn("for g in large-ints desc rowid; do", text)
        self.assertIn('GENFLAGS="$GENFLAGS --rotate"', text)
        self.assertIn('GENFLAGS="$GENFLAGS --bulk=$BULK_N --bulk-every=$BULK_EVERY"',
                      text)

    def test_bulk_flags(self):
        groups, unknown, rotate, bulk, every = fuzz.parse_workload(
            ["--include-desc", "--bulk=600", "--bulk-every=100"])
        self.assertEqual(groups, ["desc"])
        self.assertEqual(unknown, [])
        self.assertEqual(rotate, 0)
        self.assertEqual(bulk, 600)
        self.assertEqual(every, 100)
        self.assertEqual(fuzz.bulk_for(100, bulk, every), 600)
        self.assertEqual(fuzz.bulk_for(101, bulk, every), 0)

    def test_bulk_spaced_form_and_unknown(self):
        groups, unknown, rotate, bulk, every = fuzz.parse_workload(
            ["--bulk", "40", "--bulk-every", "5", "--nope"])
        self.assertEqual(bulk, 40)
        self.assertEqual(every, 5)
        self.assertEqual(unknown, ["--nope"])
        self.assertEqual(groups, [])
        self.assertEqual(rotate, 0)

    def test_every_script_pauses_a_scan(self):
        sql = fuzz.Gen(7, []).run()
        self.assertIn(".scan-pause 2", sql)
        self.assertIn(".scan-resume", sql)
        self.assertIn("SAVEPOINT sps;", sql)
        self.assertNotIn("generate_series", sql)
        self.assertNotIn(fuzz.REOPEN_MARK, sql)

    def test_bulk_load_varies_keys_and_reopens(self):
        saw = set()
        for seed in range(80):
            sql = fuzz.Gen(seed, ["desc"], bulk=5).run()
            self.assertIn("generate_series(100000,", sql)
            self.assertIn("hex(zeroblob(160))", sql)
            self.assertIn(fuzz.REOPEN_MARK, sql)
            if "k BLOB" in sql:
                saw.add("blob")
                self.assertIn("printf('k%08d', value)", sql)
                self.assertNotIn("k%%08d", sql)
            if "k TEXT" in sql and "generate_series" in sql:
                self.assertIn("printf('k%08d', value)", sql)
                self.assertNotIn("k%%08d", sql)
            if "PRIMARY KEY(k DESC)" in sql or "PRIMARY KEY(k DESC, j)" in sql:
                saw.add("desc")
            if "COLLATE NOCASE" in sql:
                saw.add("nocase")
            if "j TEXT" in sql:
                saw.add("composite")
            if "k TEXT" in sql:
                saw.add("text")
        self.assertEqual(saw, {"blob", "desc", "nocase", "composite", "text"})

    def test_shell_pauses_one_statement(self):
        text = (HERE.parent / "src" / "shell.c.in").read_text()
        self.assertIn('cli_strcmp(azArg[0], "scan-pause")', text)
        self.assertIn('cli_strcmp(azArg[0], "scan-resume")', text)
        self.assertIn("pScanPause", text)

    def test_prolly_node_counter(self):
        def node(level, flags):
            return bytes([0x44, 0x4F, 0x4E, 0x50, level, 1, 0, flags])
        blob = node(0, 1) + b"xxxx" + node(1, 1 | 4) + node(0, 2)
        blob += bytes([0x44, 0x4F, 0x4E, 0x50, 0, 1, 0, 0])  # bad key flag
        leaves, internals = fuzz.count_prolly_nodes(blob)
        self.assertEqual(leaves, 2)
        self.assertEqual(internals, 1)


class GeneratorParityTest(unittest.TestCase):
    def test_cli_matches_inprocess(self):
        groups = ["large-ints", "desc"]
        sql = fuzz.Gen(7, groups).run()
        out = subprocess.check_output(
            [sys.executable, str(FUZZER), "7",
             "--include-large-ints", "--include-desc"],
            text=True)
        self.assertEqual(out, sql + "\n")

    def test_cli_bulk_respects_every(self):
        sql = fuzz.Gen(100, ["desc"], 0, fuzz.bulk_for(100, 10, 100)).run()
        out = subprocess.check_output(
            [sys.executable, str(FUZZER), "100", "--include-desc",
             "--bulk=10", "--bulk-every=100"],
            text=True)
        self.assertEqual(out, sql + "\n")
        skipped = subprocess.check_output(
            [sys.executable, str(FUZZER), "7", "--bulk=10", "--bulk-every=100"],
            text=True)
        self.assertNotIn("generate_series", skipped)


class UniqueUpdateOrderTest(unittest.TestCase):
    def update_sql(self, unique=True, without_rowid=False):
        gen = fuzz.Gen(0, ["expr"])
        gen.shape = "composite_pk"
        gen.has_unique_b = unique
        gen.without_rowid = without_rowid
        with mock.patch.object(gen.r, "choice", return_value="b"), \
                mock.patch.object(gen.r, "random", return_value=0.0), \
                mock.patch.object(gen, "pred", return_value="1"):
            gen.update()
        return gen.out[-1]

    def database(self, rows, without_rowid, unique=True):
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.execute("CREATE TABLE t(k INTEGER,j TEXT,a,b TEXT,PRIMARY KEY(k,j))"
                   + (" WITHOUT ROWID" if without_rowid else ""))
        if unique:
            db.execute("CREATE UNIQUE INDEX i_u ON t(b,k)")
        db.executemany("INSERT INTO t VALUES(?,?,?,?)", rows)
        return db

    def rows(self, db):
        return db.execute("SELECT k,j,a,b FROM t ORDER BY k,j").fetchall()

    def test_unique_conflicts_are_compared_in_both_table_orders(self):
        rows = [(1, "z", 0, "X'00'"), (1, "a", 0, b"\x00")]
        for without_rowid in (False, True):
            for values in (rows, list(reversed(rows))):
                with self.subTest(without_rowid=without_rowid, values=values):
                    db = self.database(values, without_rowid)
                    before = self.rows(db)
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute(self.update_sql())
                    self.assertEqual(self.rows(db), before)

    def test_reported_seed_preserves_unique_checks(self):
        sql = fuzz.Gen(5614757853, fuzz.GROUPS).run()
        update = next(line for line in sql.splitlines()
                      if line.startswith("UPDATE t SET b = coalesce(quote(b),'x')"))
        rows = [(1, "z", 0, "X'00'"), (1, "a", 0, b"\x00")]
        for without_rowid in (False, True):
            for values in (rows, list(reversed(rows))):
                with self.subTest(without_rowid=without_rowid, values=values):
                    db = self.database(values, without_rowid)
                    before = self.rows(db)
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute(update)
                    self.assertEqual(self.rows(db), before)

    def test_nonconflicting_update_visits_one_logical_row(self):
        rows = [(1, "z", 0, b"\x00"), (1, "a", 0, "X'00'")]
        for without_rowid in (False, True):
            for values in (rows, list(reversed(rows))):
                with self.subTest(without_rowid=without_rowid, values=values):
                    db = self.database(values, without_rowid)
                    db.execute(self.update_sql())
                    self.assertEqual(db.execute("SELECT changes()").fetchone(), (1,))
                    self.assertEqual(self.rows(db),
                                     [(1, "a", 0, "'X''00'''"), (1, "z", 0, b"\x00")])

    def test_bulk_updates_remain_covered(self):
        rows = [(1, "z", 0, b"\x00"), (1, "a", 0, "X'00'")]
        for unique, without_rowid in ((False, False), (False, True), (True, True)):
            with self.subTest(unique=unique, without_rowid=without_rowid):
                db = self.database(rows, without_rowid, unique)
                db.execute(self.update_sql(unique, without_rowid))
                self.assertEqual(db.execute("SELECT changes()").fetchone(), (2,))


class ReducerTest(unittest.TestCase):
    def test_error_delta_keeps_counts_and_direction(self):
        error = b"UNIQUE constraint failed: t.b, t.k"
        left = b"Error near line 20: " + error + b"\n"
        right = b"Error near line 3: " + error + b"\n"
        self.assertEqual(sweep.error_delta(left, right), {})
        self.assertEqual(sweep.error_delta(left * 3, right), {error: 2})
        self.assertEqual(sweep.error_delta(left, right * 2), {error: -1})

    def test_keeps_the_original_error_instead_of_a_rowid_difference(self):
        def oracle(_dl, _sq, _dd, _sd, sql):
            if "UNIQUE_ERROR" in sql:
                output = ("Error near line %d: UNIQUE constraint failed: t.b, t.k\n"
                          % len(sql.splitlines())).encode()
                return False, 1, 0, output, b""
            if "ROWID" in sql:
                return False, 0, 0, b"3261486686776371965\n", b"2\n"
            return True, 0, 0, b"", b""

        with mock.patch.object(sweep, "agree", side_effect=oracle):
            reduced = sweep.shrink_sql("dl", "sq", "d.db", "s.db", "UNIQUE_ERROR\nROWID\n")
        self.assertEqual(reduced, "UNIQUE_ERROR\n")

    def test_keeps_only_the_lines_that_fail(self):
        lines = ["noise-a", "NEED_A", "noise-b", "NEED_B", "noise-c"]

        def fails(got):
            text = "\n".join(got)
            return "NEED_A" in text and "NEED_B" in text

        self.assertEqual(sweep.ddmin(lines, fails), ["NEED_A", "NEED_B"])

    def test_empty_failure_is_not_reduced(self):
        lines = ["CREATE TABLE t(a);", "SELECT 1;"]
        self.assertEqual(sweep.ddmin(lines, lambda _candidate: True), lines)

    def test_reopen_section_is_the_trailing_reads(self):
        sql = fuzz.Gen(100, ["desc"], bulk=8).run()
        exec_sql, reopen_sql = sweep.split_phases(sql)
        self.assertIn("generate_series", exec_sql)
        self.assertIn(".scan-pause", exec_sql)
        self.assertNotIn(fuzz.REOPEN_MARK, exec_sql)
        self.assertIn("integrity_check", reopen_sql)
        self.assertNotIn("CREATE TABLE", reopen_sql)
        self.assertNotIn(".scan-pause", reopen_sql)


class SweepHarnessTest(unittest.TestCase):
    def run_sweep(self, first, last, dl, sq, extra_env=None, flags=None):
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        cmd = [sys.executable, "-u", str(SWEEP), str(dl), str(sq),
               str(first), str(last)]
        if flags:
            cmd.extend(flags)
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, env=env)

    def test_agreeing_stubs_pass_with_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            stub = tmp / "engine"
            write_stub(stub, "import sys\nsys.stdin.read()\nsys.stdout.write('ok\\n')\n")
            proc = self.run_sweep(1, 3, stub, stub)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("Results: 3 passed, 0 failed out of 3 seeds", proc.stdout)
            self.assertIn("... 1/3 (seed 1)", proc.stdout)
            self.assertIn("... 3/3 (seed 3)", proc.stdout)

    def test_time_budget_stops_between_seeds_and_reports_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            stub = tmp / "engine"
            write_stub(stub, "import sys, time\nsys.stdin.read()\ntime.sleep(0.2)\n"
                             "sys.stdout.write('ok\\n')\n")
            proc = self.run_sweep(10, 1000, stub, stub,
                                  extra_env={"DOLTLITE_DIFF_SECONDS": "1"})
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertRegex(proc.stdout, r"Time budget of 1s reached: covered seeds 10\.\.(\d+), "
                                          r"\d+ of 991; \d+\.\.1000 not run")
            covered = int(proc.stdout.split("covered seeds 10..")[1].split(",")[0])
            self.assertLess(covered, 1000)
            self.assertIn("Results: %d passed, 0 failed out of %d seeds" % (
                covered - 9, covered - 9), proc.stdout)

    def test_time_budget_still_fails_on_a_covered_divergence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            dl = tmp / "dl"
            sq = tmp / "sq"
            write_stub(dl, "import sys, time\nsys.stdin.read()\ntime.sleep(0.2)\n"
                           "sys.stdout.write('dl\\n')\n")
            write_stub(sq, "import sys\nsys.stdin.read()\nsys.stdout.write('sq\\n')\n")
            proc = self.run_sweep(1, 1000, dl, sq,
                                  extra_env={"DOLTLITE_DIFF_SECONDS": "1"})
            self.assertEqual(proc.returncode, 1, proc.stderr)
            self.assertIn("Time budget of 1s reached", proc.stdout)
            self.assertIn("FAIL: seed 1", proc.stdout)

    def test_divergence_saves_script(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            dl = tmp / "dl"
            sq = tmp / "sq"
            write_stub(dl, "import sys\nsys.stdin.read()\nsys.stdout.write('dl\\n')\n")
            write_stub(sq, "import sys\nsys.stdin.read()\nsys.stdout.write('sq\\n')\n")
            save = tmp / "save"
            save.mkdir()
            proc = self.run_sweep(4, 4, dl, sq, extra_env={
                "DOLTLITE_DIFF_SAVE_DIR": str(save),
            })
            self.assertEqual(proc.returncode, 1, proc.stderr)
            self.assertIn("FAIL: seed 4", proc.stdout)
            self.assertIn("Failing seeds: 4", proc.stdout)
            saved = save / "seed_4.sql"
            self.assertTrue(saved.is_file())
            self.assertIn("CREATE TABLE", saved.read_text())

    def test_matching_crashes_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            stub = tmp / "engine"
            write_stub(stub,
                       "import sys\nsys.stdin.read()\nsys.stdout.write('1\\n')\n"
                       "sys.exit(134)\n")
            proc = self.run_sweep(1, 1, stub, stub)
            self.assertEqual(proc.returncode, 1, proc.stdout)
            self.assertIn("FAIL: seed 1", proc.stdout)
            self.assertIn("Results: 0 passed, 1 failed out of 1 seeds",
                          proc.stdout)

    @unittest.skipUnless(os.name == "posix", "signal return codes are POSIX")
    def test_matching_signal_crashes_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            stub = tmp / "engine"
            write_stub(stub,
                       "import os, signal, sys\n"
                       "sys.stdin.read()\n"
                       "sys.stdout.write('1\\n')\n"
                       "sys.stdout.flush()\n"
                       "os.kill(os.getpid(), signal.SIGABRT)\n")
            proc = self.run_sweep(1, 1, stub, stub)
            self.assertEqual(proc.returncode, 1, proc.stdout)
            self.assertIn("FAIL: seed 1", proc.stdout)
            self.assertIn("Results: 0 passed, 1 failed out of 1 seeds",
                          proc.stdout)

    def test_bulk_seed_reopens(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            log = tmp / "calls"
            stub = tmp / "engine"
            write_stub(stub,
                       "import os, sys\n"
                       "sys.stdin.read()\n"
                       "open(os.environ['CALLLOG'], 'a').write('x')\n"
                       "sys.stdout.write('ok\\n')\n")
            proc = self.run_sweep(1, 1, stub, stub, extra_env={
                "CALLLOG": str(log),
            }, flags=["--bulk=4", "--bulk-every=1"])
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            # exec + reopen, on each engine.
            self.assertEqual(log.read_text(), "xxxx")


if __name__ == "__main__":
    unittest.main()
