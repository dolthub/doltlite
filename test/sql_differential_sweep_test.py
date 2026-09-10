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
        groups, unknown = fuzz.parse_groups(
            ["--include-large-ints", "--include-desc"])
        self.assertEqual(groups, ["large-ints", "desc"])
        self.assertEqual(unknown, [])

    def test_all(self):
        groups, unknown = fuzz.parse_groups(["--all"])
        self.assertEqual(groups, fuzz.GROUPS)
        self.assertEqual(unknown, [])

    def test_unknown(self):
        groups, unknown = fuzz.parse_groups(["--include-nope"])
        self.assertEqual(groups, [])
        self.assertEqual(unknown, ["--include-nope"])


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


if __name__ == "__main__":
    unittest.main()
