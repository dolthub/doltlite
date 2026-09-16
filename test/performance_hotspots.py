#!/usr/bin/env python3

import argparse
import csv
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import tempfile


TEST_DIR = Path(__file__).resolve().parent
PAYLOAD_BYTES = 1024
TIMER = re.compile(r"Run Time: real ([0-9.]+) user [0-9.]+ sys [0-9.]+")


def run(command, **kwargs):
    result = subprocess.run(command, text=True, capture_output=True,
                            timeout=600, **kwargs)
    if result.returncode or result.stderr.strip():
        raise RuntimeError(f"{command[0]} failed ({result.returncode}):\n"
                           f"{result.stdout}\n{result.stderr}")
    return result.stdout


def sql(binary, db, statements):
    return run([str(binary), str(db)], input=".bail on\n" + statements + "\n")


def positive_us(seconds):
    value = float(seconds)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"invalid timing: {seconds}")
    return max(1, round(value * 1_000_000))


def parse_session(output, workloads):
    lines = output.splitlines()
    times = {}
    for name, _query, expected in workloads:
        if not lines or lines.pop(0) != f"BEGIN {name}":
            raise ValueError(f"missing start marker for {name}: {output}")
        values = []
        measured = []
        while lines and lines[0] != f"END {name}":
            line = lines.pop(0)
            match = TIMER.fullmatch(line)
            if match:
                measured.append(positive_us(match[1]))
            else:
                values.append(line)
        if not lines or len(measured) != 1 or values != [expected]:
            raise ValueError(f"invalid timing or result for {name}: {output}")
        lines.pop(0)
        times[name] = measured[0]
    if lines:
        raise ValueError(f"unexpected session output: {lines}")
    return times


def workloads(rows):
    scan = "SELECT sum(length(payload)),sum(id) FROM t NOT INDEXED;"
    expected = f"{rows * PAYLOAD_BYTES}|{rows * (rows + 1) // 2}"
    points = f"""WITH RECURSIVE c(i) AS (
      VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<10000
    ) SELECT sum((SELECT length(payload) FROM t
                  WHERE id=1+(c.i*2654435761)%{rows})) FROM c;"""
    return [("scan_first", scan, expected),
            ("scan_repeat", scan, expected),
            ("point_10000", points, str(10000 * PAYLOAD_BYTES))]


def measure_queries(binary, db, rows, cache_kib):
    cases = workloads(rows)
    statements = [".headers off", ".mode list", ".output /dev/null",
                  "PRAGMA mmap_size=0;", ".output stdout",
                  f"PRAGMA cache_size=-{cache_kib};",
                  "SELECT name FROM sqlite_schema WHERE 0;"]
    for name, query, _expected in cases:
        statements.extend([f".print BEGIN {name}", ".timer on", query,
                           ".timer off", f".print END {name}"])
    return parse_session(sql(binary, db, "\n".join(statements)), cases)


def prepare(binary, db, rows):
    statements = ["CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);"]
    for first in range(1, rows + 1, 4096):
        last = min(first + 4095, rows)
        statements.append(f"""WITH RECURSIVE c(i) AS (
          VALUES({first}) UNION ALL SELECT i+1 FROM c WHERE i<{last}
        ) INSERT INTO t SELECT i,randomblob({PAYLOAD_BYTES}) FROM c;""")
    sql(binary, db, "\n".join(statements))
    if db.stat().st_size < rows * PAYLOAD_BYTES:
        raise ValueError(f"fixture unexpectedly smaller than its payload: {db}")


def measure_checkpoint(binary, output):
    env = dict(os.environ, DOLTLITE_CHECKPOINT_PERF_TRIALS="1",
               DOLTLITE_CHECKPOINT_PERF_THRESHOLD_BYTES="67108864",
               DOLTLITE_CHECKPOINT_PERF_PAYLOAD_BYTES="2097152",
               DOLTLITE_CHECKPOINT_PERF_MAX_APPENDS="128",
               DOLTLITE_CHECKPOINT_PERF_SAMPLES_OUTPUT=str(output))
    log = run(["bash", str(TEST_DIR / "doltlite_checkpoint_perf.sh"), str(binary)],
              env=env)
    output.with_suffix(".log").write_text(log)
    with output.open() as source:
        records = list(csv.DictReader(source, delimiter="\t"))
    if len(records) != 1 or records[0]["run"] != "1":
        raise ValueError(f"invalid checkpoint samples: {records}")
    return {f"append_{name}": positive_us(records[0][f"{name}_seconds"])
            for name in ("below", "checkpoint", "post")}


def write_results(samples, rows, cache_kib, sizes, result_path, sample_path):
    names = list(samples["candidate"][0])
    medians = {arm: {name: statistics.median(sample[name] for sample in runs)
                     for name in runs[0]} for arm, runs in samples.items()}
    result_path.parent.mkdir(parents=True, exist_ok=True)
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    with result_path.open("w") as output, sample_path.open("w") as raw:
        raw.write("section\ttest\trun\tbaseline_us\tcandidate_us\tstock_us\n")
        for name in names:
            section = "checkpoint" if name.startswith("append_") else "queries"
            output.write(f"{section}\t{name}\t{medians['baseline'][name]:.0f}\t"
                         f"{medians['candidate'][name]:.0f}\n")
            for i, candidate in enumerate(samples["candidate"]):
                stock = samples["stock"][i].get(name, "")
                raw.write(f"{section}\t{name}\t{i+1}\t"
                          f"{samples['baseline'][i][name]}\t{candidate[name]}\t{stock}\n")
    print("## Performance hotspots")
    print(f"\n{rows:,} rows × {PAYLOAD_BYTES} payload bytes; "
          f"{cache_kib:,} KiB cache per connection.")
    print("Fixture bytes: " + ", ".join(f"{arm}={size:,}" for arm, size in sizes.items()))
    print("\nPR-base gates: 1.5× per workload and 1.25× per section/suite, "
          "with a 10 ms minimum regression and confirmation across three attempts. "
          "Stock ratios expose standing gaps and are reported separately.")
    print("\n| Workload | PR base ms | Candidate ms | Candidate/base | Stock ms | Candidate/stock |")
    print("|---|---:|---:|---:|---:|---:|")
    for name in names:
        base, candidate = medians["baseline"][name], medians["candidate"][name]
        stock = medians["stock"].get(name)
        stock_cells = f"{stock/1000:.3f} | {candidate/stock:.2f}×" if stock else "— | —"
        print(f"| {name} | {base/1000:.3f} | {candidate/1000:.3f} | "
              f"{candidate/base:.2f}× | {stock_cells} |")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Measure engine hotspots separately from sysbench")
    for arm in ("baseline", "candidate", "stock"):
        parser.add_argument(f"--{arm}", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=262144)
    parser.add_argument("--cache-kib", type=int, default=65536)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args(argv)
    if args.runs < 1 or args.cache_kib < 1 or args.rows * PAYLOAD_BYTES < 4 * args.cache_kib * 1024:
        parser.error("require positive runs/cache and a payload at least four times the cache")
    binaries = {arm: getattr(args, arm).resolve() for arm in ("baseline", "candidate", "stock")}
    run(["bash", str(TEST_DIR / "assert_stock_reference.sh"),
         str(binaries["stock"]), str(binaries["candidate"])])
    samples = {arm: [] for arm in binaries}
    with tempfile.TemporaryDirectory(prefix="doltlite-hotspots-") as directory:
        root = Path(directory)
        databases = {arm: root / f"{arm}.db" for arm in binaries}
        for arm, binary in binaries.items():
            print(f"Preparing {arm} hotspot fixture", file=sys.stderr, flush=True)
            prepare(binary, databases[arm], args.rows)
        sizes = {arm: db.stat().st_size for arm, db in databases.items()}
        for trial in range(args.runs):
            order = ("baseline", "candidate", "stock") if trial % 2 == 0 else ("stock", "candidate", "baseline")
            for arm in order:
                print(f"Hotspots trial {trial+1}/{args.runs}: {arm}", file=sys.stderr, flush=True)
                measured = measure_queries(binaries[arm], databases[arm], args.rows, args.cache_kib)
                if arm != "stock":
                    measured.update(measure_checkpoint(binaries[arm], root / f"{arm}-{trial}.tsv"))
                samples[arm].append(measured)
        write_results(samples, args.rows, args.cache_kib, sizes,
                      Path(os.environ.get("BENCH_RESULTS_OUTPUT", "hotspots.tsv")),
                      Path(os.environ.get("BENCH_SAMPLES_OUTPUT", "hotspots-samples.tsv")))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
        sys.exit(str(exc))
