#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import statistics
import shutil
import sys
import tempfile

from performance_hotspots import parse_session, run, sql


TEST_DIR = Path(__file__).resolve().parent
NAME = "savepoint_rollback"
SAVEPOINT_ROWS = 5000
SAVEPOINT_OPERATIONS = 1000
SAVEPOINT_ROLLBACK_EVERY = 4
SAVEPOINT_CACHE_KIB = 65536


def workload(rows, operations, rollback_every):
    values = [i % 100 + 1 for i in range(1, rows + 1)]
    statements = ["BEGIN;", "UPDATE t SET k=k+1;"]
    for i in range(operations):
        row_id = 1 + i * 137 % rows
        statements.extend(["SAVEPOINT item;",
                           f"UPDATE t SET k=k+1 WHERE id={row_id};"])
        if rollback_every and i % rollback_every == 0:
            statements.append("ROLLBACK TO item;")
        else:
            values[row_id - 1] += 1
        statements.append("RELEASE item;")
    statements.append("COMMIT;")
    expected = ",".join(f"{i}:{value}" for i, value in enumerate(values, 1)) + "|ok"
    return " ".join(statements), expected


def prepare(binary, db, rows, cache_kib):
    sql(binary, db, f"""PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA cache_size=-{cache_kib};
CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER NOT NULL);
BEGIN;
WITH RECURSIVE c(i) AS (
  VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows}
) INSERT INTO t SELECT i,i%100 FROM c;
COMMIT;""")
    if db.stat().st_size >= cache_kib * 1024 // 4:
        raise ValueError(f"fixture must use less than one quarter of the cache: {db}")


def measure(binary, fixture, work, batch, expected, cache_kib):
    shutil.copyfile(fixture, work)
    statements = [".headers off", ".mode list", ".output /dev/null",
                  "PRAGMA mmap_size=0;", "PRAGMA synchronous=FULL;",
                  f"PRAGMA cache_size=-{cache_kib};", "SELECT sum(k) FROM t;",
                  ".output stdout", f".print BEGIN {NAME}", ".timer on",
                  batch, ".timer off",
                  "SELECT (SELECT group_concat(id||':'||k,',') FROM "
                  "(SELECT id,k FROM t ORDER BY id)) || '|' || "
                  "(SELECT group_concat(integrity_check) FROM pragma_integrity_check);",
                  f".print END {NAME}"]
    measured = parse_session(sql(binary, work, "\n".join(statements)),
                             [(NAME, batch, expected)])[NAME]
    if work.stat().st_size >= cache_kib * 1024 // 4:
        raise ValueError(f"result must use less than one quarter of the cache: {work}")
    return measured


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compare savepoint rollbacks on a small table with pending writes.")
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--stock", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=SAVEPOINT_ROWS)
    parser.add_argument("--operations", type=int, default=SAVEPOINT_OPERATIONS)
    parser.add_argument("--rollback-every", type=int, default=SAVEPOINT_ROLLBACK_EVERY,
                        help="roll back every Nth update; 0 releases every savepoint")
    parser.add_argument("--runs", type=int, default=9)
    parser.add_argument("--cache-kib", type=int, default=SAVEPOINT_CACHE_KIB)
    parser.add_argument("--output", type=Path, help="write configuration and raw samples as JSON")
    args = parser.parse_args(argv)
    if min(args.rows, args.operations, args.runs, args.cache_kib) < 1 or args.rollback_every < 0:
        parser.error("rows, operations, runs and cache must be positive; rollback-every must be nonnegative")
    binaries = {"doltlite": args.candidate.resolve(), "sqlite": args.stock.resolve()}
    run(["bash", str(TEST_DIR / "lib/assert_doltlite_engine.sh"), str(binaries["doltlite"])])
    run(["bash", str(TEST_DIR / "assert_stock_reference.sh"),
         str(binaries["sqlite"]), str(binaries["doltlite"])])
    batch, expected = workload(args.rows, args.operations, args.rollback_every)
    samples = []
    sizes = {}
    with tempfile.TemporaryDirectory(prefix="doltlite-savepoint-perf-") as directory:
        root = Path(directory)
        fixtures = {arm: root / f"{arm}.db" for arm in binaries}
        for arm, binary in binaries.items():
            prepare(binary, fixtures[arm], args.rows, args.cache_kib)
            sizes[arm] = fixtures[arm].stat().st_size
        for trial in range(args.runs):
            order = list(binaries) if trial % 2 == 0 else list(reversed(binaries))
            sample = {"trial": trial + 1}
            for arm in order:
                work = root / f"{arm}-{trial}.db"
                sample[arm + "_us"] = measure(binaries[arm], fixtures[arm], work,
                                              batch, expected, args.cache_kib)
            samples.append(sample)
    medians = {arm: statistics.median(sample[arm + "_us"] for sample in samples)
               for arm in binaries}
    rollbacks = ((args.operations - 1) // args.rollback_every + 1
                 if args.rollback_every else 0)
    print(f"{args.rows} rows, {args.operations} savepoint updates, {rollbacks} rollbacks, "
          f"one transaction; cache {args.cache_kib} KiB.")
    print(f"Fixture bytes: DoltLite {sizes['doltlite']}, SQLite {sizes['sqlite']}.")
    print("Timing includes the initial bulk update, savepoints and commit; "
          "excludes fixture setup, database open, warmup and verification.")
    print("\n| Workload | DoltLite ms | SQLite ms | DoltLite/SQLite |")
    print("|---|---:|---:|---:|")
    print(f"| {NAME} | {medians['doltlite']/1000:.3f} | {medians['sqlite']/1000:.3f} | "
          f"{medians['doltlite']/medians['sqlite']:.2f}× |")
    if args.output:
        args.output.write_text(json.dumps({
            "rows": args.rows, "operations": args.operations, "rollbacks": rollbacks,
            "cache_kib": args.cache_kib, "fixture_bytes": sizes,
            "binaries": {arm: str(binary) for arm, binary in binaries.items()},
            "samples": samples,
        }, indent=2) + "\n")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
