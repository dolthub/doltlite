#!/usr/bin/env python3

import argparse
import hashlib
import json
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
# The pending-map merge gap opens up with row count: 1.5x the stock update at
# 262k rows, 2.7x at 1M. The larger table gets a cache that still holds it.
INDEX_EDIT_ROWS = 1048576
INDEX_EDIT_CACHE_KIB = 131072
WIDE_ROWS = 8192
WIDE_CACHE_KIB = 4096
WIDE_PAYLOADS = (256, 1024, 2048, 4096, 16384)
WIDE_LOOKUPS = 4096
WIDE_NAME = re.compile(r"wide_p([0-9]+)_([a-z_]+)")
RETAINED_SECTIONS = (("narrow_rows", "Narrow Rows"),
                     ("zero_row_updates", "Zero Row Updates"),
                     ("small_cache", "Small Cache"),
                     ("in_transaction_mutations", "In Transaction with Mutations"),
                     ("bulk_writes", "Bulk Writes"),
                     ("after_deletes", "After Deletes"),
                     ("integer_keys", "Integer Keys"))
SECTIONS = (("queries", "Large Table Scans"),
            ("add_column", "Add Column With Default"),
            ("index_edits", "Large Index Edits"),
            ("wide_tradeoffs", "Wide Row Trade-offs"),
            *RETAINED_SECTIONS,
            ("retained", "Retained Findings"))


def section_of(name):
    if name.startswith("retained_"):
        workload = name.split("_", 2)[2]
        for category, _title in RETAINED_SECTIONS:
            if workload.startswith(category + "_"):
                return category
        return "retained"
    if name.startswith("add_column"):
        return "add_column"
    if name.startswith("index_edit"):
        return "index_edits"
    if WIDE_NAME.fullmatch(name):
        return "wide_tradeoffs"
    return "queries"


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
            ("point_10000", points, str(10000 * PAYLOAD_BYTES)),
            ("scan_after_points", scan, expected)]


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


def scan_setup(rows):
    statements = ["CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);"]
    for first in range(1, rows + 1, 4096):
        last = min(first + 4095, rows)
        statements.append(f"""WITH RECURSIVE c(i) AS (
          VALUES({first}) UNION ALL SELECT i+1 FROM c WHERE i<{last}
        ) INSERT INTO t SELECT i,randomblob({PAYLOAD_BYTES}) FROM c;""")
    return "\n".join(statements)


def prepare(binary, db, rows):
    sql(binary, db, scan_setup(rows))
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
    queries = index_queries(rows)
    results = ["|".join(str(expected[i * 137 % customers][field]) for field in (0, 1, 2, 3))
               for i in range(1000)]
    return [("index_scan_row_fetch", "\n".join(queries), results)]


def index_queries(rows):
    customers = min(2048, rows)
    columns = ("count(*),sum(amount_cents),sum(length(description)),"
               "sum(unicode(substr(description,1,1))+unicode(substr(description,-1,1)))")
    return [f"SELECT {columns} FROM orders WHERE customer_id={i * 137 % customers};"
            for i in range(1000)]


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
        if "SEARCH orders USING INDEX orders_customer (customer_id=?)" not in plan:
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


def index_edit_setup(rows):
    return f"""CREATE TABLE ie(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, s TEXT NOT NULL);
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows})
INSERT INTO ie SELECT i,(i*7919)%1000,'s'||i FROM c;
CREATE INDEX ie_k ON ie(k);
ANALYZE;"""


def index_edit_fixture(binary, db, rows):
    sql(binary, db, index_edit_setup(rows))
    check = sql(binary, db, "SELECT count(*) FROM ie;")
    if check != f"{rows}\n":
        raise ValueError(f"invalid index-edit fixture: {check}")
    return {"index_edit_update": str(rows // 2)}


def measure_index_edits(binary, fixture, work, cache_kib, expected):
    """Half the indexed values move inside one transaction. Each trial starts
    from a fresh copy and rolls back, so the fixture is never consumed."""
    for stale in (work, work.with_name(work.stem + ".db-lock")):
        if stale.exists():
            stale.unlink()
    shutil.copy(fixture, work)
    cases = [("index_edit_update", None, expected["index_edit_update"])]
    statements = [".headers off", ".mode list", ".output /dev/null",
                  "PRAGMA mmap_size=0;",
                  f"PRAGMA cache_size=-{cache_kib};",
                  "SELECT count(*) FROM ie WHERE id>0;",
                  "SELECT count(*) FROM ie INDEXED BY ie_k WHERE k>=0;",
                  ".output stdout",
                  "SELECT name FROM sqlite_schema WHERE 0;",
                  "BEGIN;",
                  ".print BEGIN index_edit_update", ".timer on",
                  "UPDATE ie SET k=k+1 WHERE id%2=0;",
                  ".timer off", "SELECT changes();", ".print END index_edit_update",
                  "ROLLBACK;"]
    return parse_session(sql(binary, work, "\n".join(statements)), cases)


def wide_setup(rows=WIDE_ROWS, payloads=WIDE_PAYLOADS):
    statements = []
    for payload in payloads:
        statements.append(f"""CREATE TABLE w{payload}(id INTEGER PRIMARY KEY, grp INTEGER NOT NULL,
  v INTEGER NOT NULL, k INTEGER NOT NULL, payload BLOB NOT NULL);
BEGIN;
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows})
INSERT INTO w{payload} SELECT i,i%64,(i*7919)%100000,i*3,
  CAST(printf('%0{payload}d',i) AS BLOB) FROM c;
COMMIT;
CREATE INDEX w{payload}_gv ON w{payload}(grp,v);""")
    return "\n".join(statements) + "\nANALYZE;\n"


def wide_workloads(payload, rows=WIDE_ROWS, lookups=WIDE_LOOKUPS):
    """Each operation touches either only the small columns or the blob, so
    the section shows what wide rows cost reads that never need the blob
    against reads that do, across payload sizes on both sides of the
    wide-leaf threshold."""
    table = f"w{payload}"
    ids = [1 + (i * 2654435761) % rows for i in range(1, lookups + 1)]
    v = {i: (i * 7919) % 100000 for i in range(1, rows + 1)}
    ordered = sorted(range(1, rows + 1), key=lambda i: (i % 64, v[i]))
    ranks, seen = {}, {}
    for i in ordered:
        seen[i % 64] = ranks[i] = seen.get(i % 64, 0) + 1
    points = (f"WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c "
              f"WHERE i<{lookups}) SELECT sum((SELECT {{}} FROM {table} "
              f"WHERE id=1+(c.i*2654435761)%{rows})) FROM c;")
    tail = "CAST(substr(payload,-3) AS INTEGER)"
    return [
        ("scan_small", f"SELECT sum(v) FROM {table} NOT INDEXED;", str(sum(v.values()))),
        ("scan_blob", f"SELECT sum({tail}) FROM {table} NOT INDEXED;",
         str(sum(i % 1000 for i in range(1, rows + 1)))),
        ("point_small", points.format("v"), str(sum(v[i] for i in ids))),
        ("point_blob", points.format(tail), str(sum(i % 1000 for i in ids))),
        ("index_fetch_small", f"SELECT sum(k) FROM (SELECT k FROM {table} INDEXED BY "
         f"{table}_gv ORDER BY grp,v LIMIT {rows // 4});",
         str(sum(i * 3 for i in ordered[:rows // 4]))),
        ("window_small", f"SELECT sum(r*v) FROM (SELECT v,row_number() OVER "
         f"(PARTITION BY grp ORDER BY v) r FROM {table} NOT INDEXED);",
         str(sum(ranks[i] * v[i] for i in range(1, rows + 1)))),
        ("update_small", f"UPDATE {table} SET v=v+1 WHERE id%8=0;", str(rows // 8)),
    ]


def wide_fixture(binary, db):
    sql(binary, db, wide_setup())
    check = sql(binary, db, "\n".join(
        f"SELECT count(*),sum(length(payload)) FROM w{p};" for p in WIDE_PAYLOADS))
    expected = "".join(f"{WIDE_ROWS}|{WIDE_ROWS * p}\n" for p in WIDE_PAYLOADS)
    if check != expected:
        raise ValueError(f"invalid wide-row fixture: {check}")


def measure_wide(binary, db):
    """One fresh connection per workload, warmed by an untimed run of the
    same statement, so each timing is the steady state of that access
    pattern alone under a cache far smaller than the wide tables."""
    measured = {}
    for payload in WIDE_PAYLOADS:
        for op, query, expected in wide_workloads(payload):
            name = f"wide_p{payload}_{op}"
            write = op.startswith("update")
            statements = [".headers off", ".mode list", ".output /dev/null",
                          "PRAGMA mmap_size=0;", f"PRAGMA cache_size=-{WIDE_CACHE_KIB};",
                          *(["BEGIN;", query, "ROLLBACK;"] if write else [query]),
                          ".output stdout", "SELECT name FROM sqlite_schema WHERE 0;",
                          *(["BEGIN;"] if write else []),
                          f".print BEGIN {name}", ".timer on", query, ".timer off",
                          *(["SELECT changes();"] if write else []),
                          f".print END {name}", *(["ROLLBACK;"] if write else [])]
            measured.update(parse_session(sql(binary, db, "\n".join(statements)),
                                          [(name, None, expected)]))
    return measured


def prepare_retained(binaries, root, corpus=None):
    from performance_hotspot_fuzzer import Profile, prologue
    directory = corpus if corpus is not None else TEST_DIR / 'performance-hotspot-corpus'
    retained = []
    names = set()
    fixtures = {}
    for path in sorted(directory.glob('*.json')):
        bundle = json.loads(path.read_text())
        if (type(bundle['issue']) is not int or bundle['issue'] <= 0
                or not re.fullmatch(r'[0-9a-f]{24}', bundle['fingerprint'])
                or type(bundle['repeats']) is not int or not 1 <= bundle['repeats'] <= 1024):
            raise ValueError(f'invalid retained hotspot: {path}')
        category = bundle.get('category', 'retained')
        if category != 'retained' and category not in dict(RETAINED_SECTIONS):
            raise ValueError(f'invalid retained hotspot category: {path}')
        suffix = bundle['fingerprint']
        if category != 'retained':
            if not re.fullmatch(r'[a-z][a-z0-9_]*', bundle['case']['name']):
                raise ValueError(f'invalid retained hotspot name: {path}')
            suffix = f"{category}_{bundle['case']['name']}"
        name = f"retained_{bundle['issue']}_{suffix}_x{bundle['repeats']}"
        if name in names:
            raise ValueError(f'duplicate retained hotspot: {name}')
        names.add(name)
        profile = Profile(**bundle['profile'])
        setup = prologue(profile.cache_kib)+bundle['setup_sql']
        fixture = hashlib.sha256((str(profile.memory)+setup).encode()).hexdigest()
        if fixture not in fixtures:
            databases = {arm: ':memory:' if profile.memory else root / f'{arm}-retained-{fixture}.db' for arm in binaries}
            for arm, binary in binaries.items():
                if not profile.memory:
                    sql(binary, databases[arm], setup)
            fixtures[fixture] = databases
        databases = fixtures[fixture]
        retained.append((name, bundle, databases))
    return retained


def measure_retained(binary, arm, retained):
    from performance_hotspot_fuzzer import Case, Profile, parse_measurement, session_sql
    measured = {}
    for name, bundle, databases in retained:
        repeats = bundle['repeats']
        output = sql(binary, databases[arm], session_sql(Profile(**bundle['profile']), Case(**bundle['case']), repeats, bundle['setup_sql']))
        result = parse_measurement(output, repeats)
        if result['result'] != bundle['expected']:
            raise ValueError(f'{name}: retained hotspot result mismatch')
        measured[name] = max(1, round(result['ms']*1000))
    return measured


def print_wide_matrix(names, medians):
    cells = {}
    for name in names:
        payload, op = WIDE_NAME.fullmatch(name).groups()
        cells.setdefault(op, {})[int(payload)] = medians["candidate"][name] / medians["stock"][name]
    payloads = sorted({p for row in cells.values() for p in row})
    print(f"\n{WIDE_ROWS:,} rows per table, {WIDE_CACHE_KIB // 1024} MiB cache, warm. "
          "`*_small` reads only the small columns; `*_blob` reads the payload. "
          "Wide values are never cached and are read from disk when fetched until they move "
          "out of band: [#3325](https://github.com/dolthub/doltlite/issues/3325). "
          "Workloads are named `wide_p<payload bytes>_<operation>`.")
    print("\nCandidate/stock by payload bytes:\n")
    print("| Operation | " + " | ".join(f"{p:,} B" for p in payloads) + " |")
    print("|---|" + "---:|" * len(payloads))
    for op, row in cells.items():
        print(f"| {op} | " + " | ".join(f"{row[p]:.2f}×" if p in row else "" for p in payloads) + " |")


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
    print("\nPR-base gates: 1.25× per workload and 1.15× per section/suite, "
          "with a 10 ms minimum regression and confirmation across three attempts. "
          "Stock ratios expose standing gaps and are reported separately.")
    if any(name.startswith("retained_") for name in names):
        print("\nRetained workloads report fixed batches; xN in a workload name is the number of repetitions.")
    for section, title in SECTIONS:
        section_names = [name for name in names if section_of(name) == section]
        if not section_names:
            continue
        print(f"\n### {title}")
        if section == 'in_transaction_mutations':
            print("\nEach timed statement follows an untimed mutation in the same transaction; rollback is untimed.")
        elif section == 'add_column':
            print("\n[#3233](https://github.com/dolthub/doltlite/issues/3233): deferred to the storage-format upgrade.")
        elif section == 'wide_tradeoffs':
            print_wide_matrix(section_names, medians)
        print("\n| Workload | PR base ms | Candidate ms | Candidate/base | Stock ms | Candidate/stock |")
        print("|---|---:|---:|---:|---:|---:|")
        for name in section_names:
            base, candidate = medians["baseline"][name], medians["candidate"][name]
            stock = medians["stock"][name]
            label = name
            if name.startswith('retained_'):
                issue, description = name.split('_', 2)[1:]
                category = section_of(name)
                if category != 'retained':
                    description = description[len(category)+1:]
                label = f'[{description}](https://github.com/dolthub/doltlite/issues/{issue})'
            print(f"| {label} | {base/1000:.3f} | {candidate/1000:.3f} | "
                  f"{candidate/base:.2f}× | {stock/1000:.3f} | {candidate/stock:.2f}× |")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Measure engine hotspots separately from sysbench")
    for arm in ("baseline", "candidate", "stock"):
        parser.add_argument(f"--{arm}", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=262144)
    parser.add_argument("--cache-kib", type=int, default=65536)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args(argv)
    if args.runs < 1 or args.cache_kib < 1 or args.rows < 1:
        parser.error("require positive rows, runs, and cache")
    binaries = {arm: getattr(args, arm).resolve() for arm in ("baseline", "candidate", "stock")}
    run(["bash", str(TEST_DIR / "assert_stock_reference.sh"),
         str(binaries["stock"]), str(binaries["candidate"])])
    samples = {arm: [] for arm in binaries}
    with tempfile.TemporaryDirectory(prefix="doltlite-hotspots-") as directory:
        root = Path(directory)
        add_column_databases = {arm: root / f"{arm}-add-column.db" for arm in binaries}
        wide_databases = {arm: root / f"{arm}-wide.db" for arm in binaries}
        for arm, binary in binaries.items():
            print(f"Preparing {arm} hotspot fixture", file=sys.stderr, flush=True)
            add_column_fixture(binary, add_column_databases[arm], args.rows)
            wide_fixture(binary, wide_databases[arm])
        retained = prepare_retained(binaries, root)
        for trial in range(args.runs):
            order = ("baseline", "candidate", "stock") if trial % 2 == 0 else ("stock", "candidate", "baseline")
            for arm in order:
                print(f"Hotspots trial {trial+1}/{args.runs}: {arm}", file=sys.stderr, flush=True)
                measured = measure_add_column(binaries[arm], add_column_databases[arm],
                                              root / f"{arm}-add-column-run.db",
                                              args.rows, args.cache_kib)
                measured.update(measure_wide(binaries[arm], wide_databases[arm]))
                measured.update(measure_retained(binaries[arm], arm, retained))
                samples[arm].append(measured)
        write_results(samples,
                      Path(os.environ.get("BENCH_RESULTS_OUTPUT", "hotspots.tsv")),
                      Path(os.environ.get("BENCH_SAMPLES_OUTPUT", "hotspots-samples.tsv")))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
        sys.exit(str(exc))
