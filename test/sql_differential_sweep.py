#!/usr/bin/env python3
"""Run generated SQL through doltlite and stock SQLite.

A script may contain a `-- @@REOPEN@@` line. The sweep runs everything above
that line, closes the process (the database stays on disk), then runs the
rest in a new process. On a mismatch it delta-debugs the statement list
before saving the script.

Usage: sql_differential_sweep.py DOLTLITE SQLITE FIRST LAST [--include-<group>]... [--all]
       [--rotate] [--bulk N] [--bulk-every K]
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


def is_clean_status(rc):
    # Python uses a negative returncode for a POSIX signal death (SIGABRT is
    # -6). Treat those as crashes, same as shell-style 128+ codes.
    return 0 <= rc < 128


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


def split_phases(sql):
    """SQL before the reopen mark, and the reads that run after a fresh open."""
    exec_lines = []
    reopen_lines = []
    mode = "exec"
    for line in sql.split("\n"):
        if line == fuzz.REOPEN_MARK:
            mode = "reopen"
            continue
        if mode == "exec":
            exec_lines.append(line)
        else:
            reopen_lines.append(line)

    def join(lines):
        while lines and lines[-1] == "":
            lines = lines[:-1]
        if not lines:
            return ""
        return "\n".join(lines) + "\n"

    return join(exec_lines), join(reopen_lines)


def pack_phases(phases):
    """One comparable blob. A signal or abort rc replaces a later clean rc."""
    chunks = []
    crash = None
    for rc, out in phases:
        if crash is None and not is_clean_status(rc):
            crash = rc
        chunks.append(("RC %d\n" % rc).encode() + out)
    rc_out = crash if crash is not None else (phases[-1][0] if phases else 0)
    return rc_out, b"\n".join(chunks)


def run_phases(binary, db, exec_sql, reopen_sql):
    phases = [run_engine(binary, db, exec_sql)]
    if reopen_sql and is_clean_status(phases[0][0]):
        phases.append(run_engine(binary, db, reopen_sql))
    return pack_phases(phases)


def agree(doltlite, sqlite3, dl_db, sq_db, sql):
    exec_sql, reopen_sql = split_phases(sql)
    unlink_db(dl_db)
    unlink_db(sq_db)
    rc_dl, out_dl = run_phases(doltlite, dl_db, exec_sql, reopen_sql)
    rc_sq, out_sq = run_phases(sqlite3, sq_db, exec_sql, reopen_sql)
    ok = rc_dl == rc_sq and is_clean_status(rc_dl) and out_dl == out_sq
    return ok, rc_dl, rc_sq, out_dl, out_sq


def ddmin(parts, test, limit=48):
    """Smallest subset of parts that still fails.

    test(candidate) is true when that subset still mismatches. An empty
    script that already mismatches is not a statement we can drop, so the
    original list is returned. Past the probe limit, untested subsets are
    rejected and the last known failure is kept.
    """
    probes = [0]

    def check(candidate):
        if probes[0] >= limit:
            return False
        probes[0] += 1
        return test(candidate)

    if not parts:
        return []
    if check([]):
        return list(parts)
    current = list(parts)
    n = 2
    while len(current) >= 2:
        if probes[0] >= limit:
            break
        chunk = (len(current) + n - 1) // n
        pieces = [current[i:i + chunk] for i in range(0, len(current), chunk)]
        reduced = None
        for i in range(len(pieces)):
            trial = [line for j, piece in enumerate(pieces) if j != i
                     for line in piece]
            if check(trial):
                reduced = trial
                break
        if reduced is None:
            for piece in pieces:
                if check(piece):
                    reduced = list(piece)
                    break
        if reduced is None:
            if n >= len(current):
                break
            n = min(len(current), n * 2)
            continue
        current = reduced
        n = max(2, n - 1)
    return current


def shrink_sql(doltlite, sqlite3, dl_db, sq_db, sql):
    lines = sql.split("\n")
    while lines and lines[-1] == "":
        lines.pop()

    def test(candidate):
        body = "\n".join(candidate)
        if body:
            body += "\n"
        ok, _, _, _, _ = agree(doltlite, sqlite3, dl_db, sq_db, body)
        return not ok

    shrunk = ddmin(lines, test)
    body = "\n".join(shrunk)
    if body and not body.endswith("\n"):
        body += "\n"
    return body


def reproduce_flags(groups, rotate, bulk, every):
    if set(groups) == set(fuzz.GROUPS):
        flags = ["--all"]
    else:
        flags = ["--include-%s" % g for g in groups]
        if rotate:
            flags.append("--rotate" if rotate == 1 else "--rotate=%d" % rotate)
    if bulk > 0:
        flags.append("--bulk=%d" % bulk)
        flags.append("--bulk-every=%d" % every)
    return flags


def sweep(doltlite, sqlite3, first, last, groups, rotate=0, bulk=0, every=1):
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
                sql = fuzz.Gen(seed, groups, rotate,
                               fuzz.bulk_for(seed, bulk, every)).run()
            except Exception as exc:
                sys.stdout.write("  ERROR: generator failed for seed %d\n" % seed)
                sys.stdout.write("    %s\n" % exc)
                fail_n += 1
                failed_seeds.append(seed)
                maybe_progress(i, total, seed, t0)
                continue

            ok, rc_dl, rc_sq, out_dl, out_sq = agree(
                doltlite, sqlite3, dl_db, sq_db, sql)

            if ok:
                pass_n += 1
                if b"Error" in out_dl or b"error" in out_dl:
                    errored += 1
                maybe_progress(i, total, seed, t0)
                continue

            fail_n += 1
            failed_seeds.append(seed)
            n_before = len([ln for ln in sql.splitlines() if ln.strip()])
            reduced = shrink_sql(doltlite, sqlite3, dl_db, sq_db, sql)
            n_after = len([ln for ln in reduced.splitlines() if ln.strip()])
            save_failing(reduced, seed)
            _, rc_dl, rc_sq, out_dl, out_sq = agree(
                doltlite, sqlite3, dl_db, sq_db, reduced)
            if fail_n <= 5:
                sys.stdout.write(
                    "  FAIL: seed %d (doltlite rc=%d, stock rc=%d)\n" % (
                        seed, rc_dl, rc_sq))
                if n_after < n_before:
                    sys.stdout.write(
                        "  reduced %d statements to %d\n" % (n_before, n_after))
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
        flags = reproduce_flags(groups, rotate, bulk, every)
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
            "       [--rotate] [--bulk N] [--bulk-every K]\n"
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
    groups, unknown, rotate, bulk, every = fuzz.parse_workload(sys.argv[5:])
    if unknown:
        sys.stderr.write("unknown flag(s): %s\n" % " ".join(unknown))
        return 2
    for binary in (doltlite, sqlite3):
        if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
            sys.stderr.write("ERROR: not executable: %s\n" % binary)
            return 1
    return sweep(doltlite, sqlite3, first, last, groups, rotate, bulk, every)


if __name__ == "__main__":
    sys.exit(main())
