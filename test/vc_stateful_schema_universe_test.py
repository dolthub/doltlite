#!/usr/bin/env python3
"""The stateful VC fuzzer must generate more than one table shape.

Fail-before: the fuzzer only created INTEGER PRIMARY KEY tables and
ADD COLUMN ... TEXT. After: several PK shapes, generated columns,
DROP/RENAME COLUMN, and DEFAULT live in the same run.
"""
import os
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent
FUZZER = ROOT / "vc_stateful_fuzzer.py"

NEEDLES = (
    "WITHOUT ROWID",
    "GENERATED ALWAYS AS",
    "DROP COLUMN",
    "RENAME COLUMN",
    "NOT NULL DEFAULT",
    "PRIMARY KEY(id DESC)",
    "CREATE TABLE t_text",
    "CREATE TABLE t_comp",
    "CREATE TABLE t_wor",
    "CREATE TABLE t_desc",
    "CREATE TABLE t_gen",
)


def check(name, cond, errors):
    if cond:
        return 1, 0
    errors.append("FAIL: %s" % name)
    return 0, 1


def main():
    src = FUZZER.read_text()
    errors = []
    passed = failed = 0
    for needle in NEEDLES:
        p, f = check("source_has_%s" % needle.replace(" ", "_"), needle in src, errors)
        passed += p
        failed += f

    shapes = ("t_int", "t_text", "t_comp", "t_wor", "t_desc", "t_gen")
    p, f = check(
        "multiple_shape_tables",
        all(("CREATE TABLE %s" % n) in src for n in shapes),
        errors,
    )
    passed += p
    failed += f

    doltlite = sys.argv[1] if len(sys.argv) > 1 else ""
    if doltlite:
        doltlite = os.path.abspath(doltlite)
        tmp = tempfile.mkdtemp(prefix="doltlite-shape-")
        db = os.path.abspath(os.path.join(tmp, "s.db"))
        env = os.environ.copy()
        env["DOLTLITE_VC_STATEFUL_SECONDS"] = "0"
        # setup-check: create the repo and list shape tables, then exit.
        r = subprocess.run(
            [sys.executable, str(FUZZER), doltlite, "--setup-check", db],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
        out = (r.stdout or "") + "\n" + (r.stderr or "")
        p, f = check("setup_check_runs", r.returncode == 0, errors)
        passed += p
        failed += f
        if r.returncode != 0:
            errors.append("setup_check stdout:\n%s" % (r.stdout or ""))
            errors.append("setup_check stderr:\n%s" % (r.stderr or ""))
        for name in shapes:
            p, f = check("setup_has_%s" % name, name in out, errors)
            passed += p
            failed += f
        p, f = check("generated_self_check", "generated_ok" in out, errors)
        passed += p
        failed += f

    print("vc_stateful_schema_universe_test: %d passed, %d failed" % (passed, failed))
    if errors:
        print("\n".join(errors))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
