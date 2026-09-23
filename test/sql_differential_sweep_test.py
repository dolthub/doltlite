#!/usr/bin/env python3

import importlib.util
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest


HERE = pathlib.Path(__file__).resolve().parent
FUZZER = HERE / "sql_differential_fuzzer.py"
SWEEP = HERE / "sql_differential_sweep.py"

SPEC = importlib.util.spec_from_file_location("sql_differential_fuzzer", FUZZER)
fuzz = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fuzz)


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


class GeneratorParityTest(unittest.TestCase):
    def test_cli_matches_inprocess(self):
        groups = ["large-ints", "desc"]
        sql = fuzz.Gen(7, groups).run()
        out = subprocess.check_output(
            [sys.executable, str(FUZZER), "7",
             "--include-large-ints", "--include-desc"],
            text=True)
        self.assertEqual(out, sql + "\n")


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


if __name__ == "__main__":
    unittest.main()
