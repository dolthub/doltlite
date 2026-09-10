#!/usr/bin/env python3
"""Run generated SQL through doltlite and stock SQLite.

Usage: sql_differential_sweep.py DOLTLITE SQLITE FIRST LAST [--include-<group>]... [--all]
"""

import difflib
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import time

PROGRESS_EVERY = 250

MODULE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "sql_differential_fuzzer.py")
SPEC = importlib.util.spec_from_file_location("sql_differential_fuzzer",
                                              MODULE_PATH)
fuzz = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fuzz)


def unlink_db(path):
    for suffix in ("", "-lock", "-wal", "-shm", "-journal"):
        try:
            os.remove(path + suffix)
        except OSError:
            pass


def run_engine(binary, db, sql):
    proc = subprocess.run(
        [binary, db],
        input=sql.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc.returncode, proc.stdout


def maybe_progress(done, total, seed, t0):
    if done != 1 and done != total and done % PROGRESS_EVERY != 0:
        return
    elapsed = time.monotonic() - t0
    rate = done / elapsed if elapsed > 0 else 0.0
    sys.stdout.write("  ... %d/%d (seed %d)  %.1fs  %.1f/s\n" % (
        done, total, seed, elapsed, rate))
    sys.stdout.flush()


def save_failing(sql, seed):
    dest = os.environ.get("DOLTLITE_DIFF_SAVE_DIR")
    if not dest:
        return
    try:
        path = os.path.join(dest, "seed_%d.sql" % seed)
        with open(path, "w") as fh:
            fh.write(sql)
            if not sql.endswith("\n"):
                fh.write("\n")
    except OSError:
        pass


def show_diff(out_dl, out_sq):
    left = out_dl.decode("utf-8", "replace").splitlines()
    right = out_sq.decode("utf-8", "replace").splitlines()
    diff = difflib.unified_diff(left, right, lineterm="")
    for i, line in enumerate(diff):
        if i >= 20:
            break
        sys.stdout.write("    %s\n" % line)


def sweep(doltlite, sqlite3, first, last, groups):
    total = last - first + 1
    work = tempfile.mkdtemp()
    dl_db = os.path.join(work, "dl.db")
    sq_db = os.path.join(work, "sq.db")
    pass_n = 0
    fail_n = 0
    errored = 0
    failed_seeds = []
    t0 = time.monotonic()
    try:
        for i, seed in enumerate(range(first, last + 1), 1):
            try:
                sql = fuzz.Gen(seed, groups).run()
            except Exception as exc:
                sys.stdout.write("  ERROR: generator failed for seed %d\n" % seed)
                sys.stdout.write("    %s\n" % exc)
                fail_n += 1
                failed_seeds.append(seed)
                maybe_progress(i, total, seed, t0)
                continue

            unlink_db(dl_db)
            unlink_db(sq_db)
            rc_dl, out_dl = run_engine(doltlite, dl_db, sql)
            rc_sq, out_sq = run_engine(sqlite3, sq_db, sql)

            if rc_dl == rc_sq and out_dl == out_sq:
                pass_n += 1
                if b"Error" in out_dl or b"error" in out_dl:
                    errored += 1
                maybe_progress(i, total, seed, t0)
                continue

            fail_n += 1
            failed_seeds.append(seed)
            save_failing(sql, seed)
            if fail_n <= 5:
                sys.stdout.write(
                    "  FAIL: seed %d (doltlite rc=%d, stock rc=%d)\n" % (
                        seed, rc_dl, rc_sq))
                show_diff(out_dl, out_sq)
            elif fail_n == 6:
                sys.stdout.write(
                    "  ... further diffs omitted; every failing seed is listed below and\n"
                    "      its script saved to the artifact directory\n")
            maybe_progress(i, total, seed, t0)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    sys.stdout.write("\n")
    sys.stdout.write("Results: %d passed, %d failed out of %d seeds\n" % (
        pass_n, fail_n, pass_n + fail_n))
    if errored:
        sys.stdout.write(
            "          (%d of the passing seeds had a statement both engines\n"
            "           rejected, and they agreed on the rejection)\n" % errored)
    if fail_n:
        flags = []
        if set(groups) == set(fuzz.GROUPS):
            flags = ["--all"]
        else:
            flags = ["--include-%s" % g for g in groups]
        sys.stdout.write("Failing seeds:%s\n" % "".join(" %d" % s for s in failed_seeds))
        sys.stdout.write("Reproduce with: python3 test/sql_differential_fuzzer.py <seed>%s\n" %
                         ("".join(" %s" % f for f in flags)))
        sys.stdout.flush()
        return 1
    sys.stdout.flush()
    return 0


def main():
    if len(sys.argv) < 5:
        sys.stderr.write(
            "usage: %s DOLTLITE SQLITE FIRST LAST [--include-<group>]... [--all]\n"
            "groups: %s\n" % (sys.argv[0], " ".join(fuzz.GROUPS)))
        return 2
    doltlite, sqlite3 = sys.argv[1], sys.argv[2]
    try:
        first = int(sys.argv[3])
        last = int(sys.argv[4])
    except ValueError:
        sys.stderr.write("seeds must be integers\n")
        return 2
    if last < first:
        sys.stderr.write("last seed is before first seed\n")
        return 2
    groups, unknown = fuzz.parse_groups(sys.argv[5:])
    if unknown:
        sys.stderr.write("unknown flag(s): %s\n" % " ".join(unknown))
        return 2
    for binary in (doltlite, sqlite3):
        if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
            sys.stderr.write("ERROR: not executable: %s\n" % binary)
            return 1
    return sweep(doltlite, sqlite3, first, last, groups)


if __name__ == "__main__":
    sys.exit(main())
