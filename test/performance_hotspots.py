#!/usr/bin/env python3

import argparse
import math
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import subprocess
import sys
import tempfile


TEST_DIR = Path(__file__).resolve().parent
PAYLOAD_BYTES = 1024
TIMER = re.compile(r"Run Time: real ([0-9.]+) user [0-9.]+ sys [0-9.]+")
ADD_COLUMN_DEFAULT = 7
# Report sections, in the order they print. A workload belongs to the section
# whose key prefixes its name; everything else is a query.
SECTIONS = (("queries", "Large Table Scans"),
            ("add_column", "Add Column With Default"))


def section_of(name):
    return "add_column" if name.startswith("add_column") else "queries"


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
        if isinstance(expected, str):
            expected = [expected]
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
        if not lines or len(measured) != len(expected) or values != expected:
            raise ValueError(f"invalid timing or result for {name}: {output}")
        lines.pop(0)
        times[name] = sum(measured)
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
    return measure_cases(binary, db, workloads(rows), cache_kib)


def measure_cases(binary, db, cases, cache_kib):
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


def index_fixture(path, rows):
    customers = min(2048, rows)
    expected = [[0, 0, 0, 0, 0] for _ in range(customers)]
    rng = random.Random(20260916)
    with path.open("w") as output:
        output.write(".bail on\nCREATE TABLE orders("
                     "id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, "
                     "amount_cents INTEGER NOT NULL, description TEXT NOT NULL);\n")
        for first in range(1, rows + 1, 1024):
            values = []
            for row_id in range(first, min(first + 1024, rows + 1)):
                customer = (row_id - 1) % customers
                amount = row_id * 37 % 100000
                description = rng.randbytes(PAYLOAD_BYTES // 2).hex()
                values.append(f"({row_id},{customer},{amount},'{description}')")
                totals = expected[customer]
                totals[0] += 1
                totals[1] += amount
                totals[2] += len(description)
                totals[3] += ord(description[0]) + ord(description[-1])
                totals[4] += row_id
            output.write("INSERT INTO orders VALUES\n" + ",\n".join(values) + ";\n")
        output.write("CREATE INDEX orders_customer ON orders(customer_id);\nANALYZE;\n")
    cases = []
    for name, columns, fields in (
        ("index_scan_row_fetch", "count(*),sum(amount_cents),sum(length(description)),"
         "sum(unicode(substr(description,1,1))+unicode(substr(description,-1,1)))", (0, 1, 2, 3)),
        ("index_scan", "count(*),sum(id)", (0, 4)),
    ):
        queries, results = [], []
        for i in range(1000):
            customer = i * 137 % customers
            queries.append(f"SELECT {columns} FROM orders WHERE customer_id={customer};")
            results.append("|".join(str(expected[customer][field]) for field in fields))
        cases.append((name, "\n".join(queries), results))
    return cases


def prepare_index_queries(binary, db, fixture, rows, cases):
    with fixture.open() as source:
        output = run([str(binary), str(db)], stdin=source)
    if output.strip():
        raise ValueError(f"unexpected index fixture output: {output}")
    check = sql(binary, db, "SELECT count(*),sum(length(description)) FROM orders;\n"
                "PRAGMA integrity_check;")
    if check != f"{rows}|{rows * PAYLOAD_BYTES}\nok\n":
        raise ValueError(f"invalid index fixture: {check}")
    for name, query, _expected in cases:
        plan = sql(binary, db, "EXPLAIN QUERY PLAN " + query.splitlines()[0])
        index = "COVERING INDEX" if name == "index_scan" else "INDEX"
        if f"SEARCH orders USING {index} orders_customer (customer_id=?)" not in plan:
            raise ValueError(f"unexpected {name} plan: {plan}")


def measure_index_queries(binary, db, cases, cache_kib):
    measured = {}
    for case in cases:
        measured.update(measure_cases(binary, db, [case], cache_kib))
    return measured


def add_column_fixture(binary, db, rows):
    """A plain table the ALTER can run against. Stock records a non-NULL
    default in the schema; doltlite writes it into every row, so the cost
    scales with the table and the fixture only needs to be big enough for
    that to show."""
    sql(binary, db, f"""CREATE TABLE ac(id INTEGER PRIMARY KEY, k INTEGER NOT NULL,
                                  s TEXT NOT NULL, d REAL NOT NULL);
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows})
INSERT INTO ac SELECT i,(i*7919)%1000,'str_'||((i*31)%50000),i*0.5 FROM c;""")
    check = sql(binary, db, "SELECT count(*) FROM ac;")
    if check != f"{rows}\n":
        raise ValueError(f"invalid add-column fixture: {check}")


def measure_add_column(binary, fixture, work, rows, cache_kib):
    """ALTER mutates the table, so every trial starts from a fresh copy. The
    verification query runs after the timer stops so only the ALTER counts."""
    for stale in (work, work.with_name(work.stem + ".db-lock")):
        if stale.exists():
            stale.unlink()
    shutil.copy(fixture, work)
    name = "add_column_default"
    expected = f"{rows}|{rows * ADD_COLUMN_DEFAULT}"
    statements = [".headers off", ".mode list", ".output /dev/null",
                  "PRAGMA mmap_size=0;", ".output stdout",
                  f"PRAGMA cache_size=-{cache_kib};",
                  "SELECT name FROM sqlite_schema WHERE 0;",
                  f".print BEGIN {name}", ".timer on",
                  f"ALTER TABLE ac ADD COLUMN z INTEGER NOT NULL DEFAULT {ADD_COLUMN_DEFAULT};",
                  ".timer off", "SELECT count(*),sum(z) FROM ac;", f".print END {name}"]
    return parse_session(sql(binary, work, "\n".join(statements)),
                         [(name, None, expected)])


def write_results(samples, result_path, sample_path):
    names = list(samples["candidate"][0])
    medians = {arm: {name: statistics.median(sample[name] for sample in runs)
                     for name in runs[0]} for arm, runs in samples.items()}
    result_path.parent.mkdir(parents=True, exist_ok=True)
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    with result_path.open("w") as output, sample_path.open("w") as raw:
        raw.write("section\ttest\trun\tbaseline_us\tcandidate_us\tstock_us\n")
        for name in names:
            section = section_of(name)
            output.write(f"{section}\t{name}\t{medians['baseline'][name]:.0f}\t"
                         f"{medians['candidate'][name]:.0f}\n")
            for i, candidate in enumerate(samples["candidate"]):
                stock = samples["stock"][i][name]
                raw.write(f"{section}\t{name}\t{i+1}\t"
                          f"{samples['baseline'][i][name]}\t{candidate[name]}\t{stock}\n")
    print("## Performance hotspots")
    print("\nPR-base gates: 1.5× per workload and 1.25× per section/suite, "
          "with a 10 ms minimum regression and confirmation across three attempts. "
          "Stock ratios expose standing gaps and are reported separately.")
    for section, title in SECTIONS:
        section_names = [name for name in names if section_of(name) == section]
        if not section_names:
            continue
        print(f"\n### {title}")
        print("\n| Workload | PR base ms | Candidate ms | Candidate/base | Stock ms | Candidate/stock |")
        print("|---|---:|---:|---:|---:|---:|")
        for name in section_names:
            base, candidate = medians["baseline"][name], medians["candidate"][name]
            stock = medians["stock"][name]
            print(f"| {name} | {base/1000:.3f} | {candidate/1000:.3f} | "
                  f"{candidate/base:.2f}× | {stock/1000:.3f} | {candidate/stock:.2f}× |")


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
        index_databases = {arm: root / f"{arm}-index.db" for arm in binaries}
        add_column_databases = {arm: root / f"{arm}-add-column.db" for arm in binaries}
        fixture = root / "index-fixture.sql"
        index_cases = index_fixture(fixture, args.rows)
        for arm, binary in binaries.items():
            print(f"Preparing {arm} hotspot fixture", file=sys.stderr, flush=True)
            prepare(binary, databases[arm], args.rows)
            prepare_index_queries(binary, index_databases[arm], fixture, args.rows, index_cases)
            add_column_fixture(binary, add_column_databases[arm], args.rows)
        for trial in range(args.runs):
            order = ("baseline", "candidate", "stock") if trial % 2 == 0 else ("stock", "candidate", "baseline")
            for arm in order:
                print(f"Hotspots trial {trial+1}/{args.runs}: {arm}", file=sys.stderr, flush=True)
                measured = measure_queries(binaries[arm], databases[arm], args.rows, args.cache_kib)
                measured.update(measure_index_queries(binaries[arm], index_databases[arm],
                                                      index_cases, args.cache_kib))
                measured.update(measure_add_column(binaries[arm], add_column_databases[arm],
                                                   root / f"{arm}-add-column-run.db",
                                                   args.rows, args.cache_kib))
                samples[arm].append(measured)
        write_results(samples,
                      Path(os.environ.get("BENCH_RESULTS_OUTPUT", "hotspots.tsv")),
                      Path(os.environ.get("BENCH_SAMPLES_OUTPUT", "hotspots-samples.tsv")))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
        sys.exit(str(exc))
