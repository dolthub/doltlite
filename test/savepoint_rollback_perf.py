#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import statistics
import sys
import tempfile

from performance_hotspots import (SAVEPOINT_NAME as NAME, SAVEPOINT_ROWS,
                                  SAVEPOINT_OPERATIONS, SAVEPOINT_ROLLBACK_EVERY,
                                  SAVEPOINT_CACHE_KIB, savepoint_workload as workload,
                                  prepare_savepoints as prepare, measure_savepoints as measure,
                                  run)


TEST_DIR = Path(__file__).resolve().parent


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
