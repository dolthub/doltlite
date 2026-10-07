#!/usr/bin/env python3
"""Compare deterministic engine counters on reduced hotspot workloads.

A baseline built before dolt_engine_stats existed is a determinism check of
the candidate only. Once both binaries expose the table, a counter that grows
by more than 5 percent and at least 8 fails the run, unless the pull request's
title or description carries a reviewed exception for it:

    perf-counter-exception: <workload> <counter> <reason>
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

SEED_DIR = Path(__file__).resolve().parent / "performance-hotspot-seeds"
STAT_LINE = re.compile(r"^([a-z_]+)=(\d+)$")
CHUNK = re.compile(
    r"WITH RECURSIVE c\(i\) AS \(VALUES\((\d+)\) UNION ALL "
    r"SELECT i\+1 FROM c WHERE i<(\d+)\) INSERT INTO \w+ .*?;",
    re.S)
RATIO = 1.05
ABS_FLOOR = 8
ROWS = 256
EXCEPTION = re.compile(
    r"^[ \t>*-]*perf-counter-exception:[ \t]*([A-Za-z0-9_]+)[ \t]+([a-z_]+)[ \t]+(\S[^\r\n]*?)[ \t]*$",
    re.M)
HTML_COMMENT = re.compile(r"<!--.*?(?:-->|\Z)", re.S)
FENCE = re.compile(r"^[ \t>]*(`{3,}|~{3,})")
INDENTED_CODE = re.compile(r"^(?: {4,}|\t)perf-counter-exception:")


def approval_text(text):
    """The text a reader sees as prose: no HTML comments, fenced blocks, or
    indented code lines, where the marker is only shown, not given."""
    lines = []
    fence = None
    for line in HTML_COMMENT.sub("", text or "").splitlines():
        m = FENCE.match(line)
        if fence:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence):
                fence = None
            continue
        if m:
            fence = m.group(1)
            continue
        if not INDENTED_CODE.match(line):
            lines.append(line)
    return "\n".join(lines)


def parse_exceptions(text):
    return {(workload, counter): reason
            for workload, counter, reason in EXCEPTION.findall(approval_text(text))}


def compare_counters(name, base, cand, exceptions, used):
    """Print each changed counter and return the ones that fail the gate."""
    worse = []
    for key in sorted(set(base) | set(cand)):
        b = base.get(key, 0)
        c = cand.get(key, 0)
        if b == c:
            continue
        flag = "diff"
        if counter_regressed(b, c):
            if (name, key) in exceptions:
                used.add((name, key))
                flag = f"ALLOWED (PR exception: {exceptions[(name, key)]})"
            else:
                flag = "REGRESS"
                worse.append(key)
        ratio = "inf" if b == 0 else f"{c / b:.3f}"
        print(f"{name} {key}: base={b} cand={c} ratio={ratio} {flag}")
    return worse


def counter_regressed(base, cand):
    if base == 0 and cand <= ABS_FLOOR:
        return False
    if cand - base < ABS_FLOOR:
        return False
    if base == 0:
        return True
    return cand / base > RATIO


def shrink_setup(sql, rows=ROWS):
    def repl(match):
        start = int(match.group(1))
        end = int(match.group(2))
        if start > rows:
            return ""
        if end > rows:
            return match.group(0).replace(f"WHERE i<{end}", f"WHERE i<{rows}", 1)
        return match.group(0)
    return CHUNK.sub(repl, sql)


def insert_rows(table, select, rows=ROWS):
    return (f"WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL "
            f"SELECT i+1 FROM c WHERE i<{rows}) "
            f"INSERT INTO {table} SELECT {select} FROM c;")


def hotspot_workloads():
    rows = ROWS
    scan_table = (
        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);\n"
        + insert_rows("t", "i, printf('%064d', i)", rows))
    yield "hotspot_scan", scan_table + """
SELECT sum(length(payload)),sum(id) FROM t NOT INDEXED;
SELECT sum(length(payload)),sum(id) FROM t NOT INDEXED;"""
    yield "hotspot_point", scan_table + f"""
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<32)
SELECT sum((SELECT length(payload) FROM t
            WHERE id=1+(c.i*2654435761)%{rows})) FROM c;"""
    yield "hotspot_index_fetch", f"""
CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL,
                    amount_cents INTEGER NOT NULL, description TEXT NOT NULL);
{insert_rows("orders", "i,(i-1)%32,i*37%100000,printf('%032x',i)", rows)}
CREATE INDEX orders_customer ON orders(customer_id);
SELECT count(*),sum(amount_cents),sum(length(description))
  FROM orders WHERE customer_id=3;
SELECT count(*),sum(amount_cents) FROM orders WHERE customer_id=17;"""
    yield "hotspot_index_edit", f"""
CREATE TABLE ie(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, s TEXT NOT NULL);
{insert_rows("ie", "i,(i*7919)%1000,'s'||i", rows)}
CREATE INDEX ie_k ON ie(k);
UPDATE ie SET k=k+1 WHERE id%2=0;
SELECT changes(),sum(k) FROM ie;"""
    yield "hotspot_wide", f"""
CREATE TABLE w(id INTEGER PRIMARY KEY, grp INTEGER NOT NULL,
               v INTEGER NOT NULL, payload BLOB NOT NULL);
{insert_rows("w", "i,i%16,(i*7919)%100000,CAST(printf('%0128d',i) AS BLOB)", 64)}
CREATE INDEX w_gv ON w(grp,v);
SELECT sum(v) FROM w;
SELECT sum(length(payload)) FROM w WHERE id=7;"""
    yield "hotspot_uncached", f"""
PRAGMA cache_size=-32;
CREATE TABLE u(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);
{insert_rows("u", "i, CAST(printf('%0256d',i) AS BLOB)", 64)}
SELECT sum(length(payload)) FROM u NOT INDEXED;
SELECT length(payload) FROM u WHERE id=11;"""
    yield "hotspot_group", f"""
CREATE TABLE g(id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, v INTEGER NOT NULL);
{insert_rows("g", "i,i%16,(i*7919)%100000", rows)}
SELECT count(*),sum(s) FROM (SELECT grp,sum(v) s FROM g GROUP BY grp);"""
    yield "hotspot_add_column", f"""
CREATE TABLE ac(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
{insert_rows("ac", "i,i", rows)}
ALTER TABLE ac ADD COLUMN z INTEGER NOT NULL DEFAULT 7;
SELECT count(*),sum(z) FROM ac;"""
    yield "hotspot_text_key", f"""
CREATE TABLE tk(id TEXT PRIMARY KEY, v INTEGER NOT NULL, payload BLOB NOT NULL);
{insert_rows("tk", "printf('%016x',i),i,CAST(printf('%032d',i) AS BLOB)", rows)}
SELECT v FROM tk WHERE id=printf('%016x', 10);
SELECT sum(length(payload)) FROM tk NOT INDEXED;"""
    yield "hotspot_pending", f"""
CREATE TABLE p(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
{insert_rows("p", "i,i", rows)}
BEGIN;
UPDATE p SET v=v+1 WHERE id%4=0;
SELECT sum(v) FROM p;
ROLLBACK;"""


def sysbench_workloads():
    setup = f"""
CREATE TABLE sbtest1(id INTEGER PRIMARY KEY, k INTEGER, c TEXT, pad TEXT);
CREATE INDEX k_1 ON sbtest1(k);
{insert_rows("sbtest1", "i, i%16, printf('c-%d', i), printf('p-%d', i)", 64)}
CREATE TABLE sbtest2(id INTEGER PRIMARY KEY, k INTEGER);
INSERT INTO sbtest2 SELECT id, k FROM sbtest1;
CREATE TABLE sbtest_types(id INTEGER PRIMARY KEY, ival INTEGER, rval REAL, tval TEXT);
{insert_rows("sbtest_types", "i, i, i*1.5, printf('t-%d', i)", 64)}
"""
    statements = {
        "sysbench_point_select": "SELECT c FROM sbtest1 WHERE id=7;",
        "sysbench_range_select": "SELECT c FROM sbtest1 WHERE id BETWEEN 10 AND 20;",
        "sysbench_sum_range": "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 10 AND 30;",
        "sysbench_order_range": "SELECT c FROM sbtest1 WHERE id BETWEEN 10 AND 20 ORDER BY c;",
        "sysbench_distinct_range": "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN 10 AND 20;",
        "sysbench_index_scan": "SELECT count(k) FROM sbtest1 WHERE k BETWEEN 1 AND 8;",
        "sysbench_update_index": "UPDATE sbtest1 SET k=k+1 WHERE id=3;",
        "sysbench_update_non_index": "UPDATE sbtest1 SET c='x' WHERE id=4;",
        "sysbench_delete_insert": "DELETE FROM sbtest1 WHERE id=5; INSERT INTO sbtest1 VALUES(5,1,'c','p');",
        "sysbench_groupby": "SELECT k, count(*) FROM sbtest1 GROUP BY k;",
        "sysbench_table_scan": "SELECT count(*) FROM sbtest1 WHERE c LIKE '%c%';",
        "sysbench_types_scan": "SELECT count(*) FROM sbtest_types WHERE tval LIKE '%t%';",
        "sysbench_join": "SELECT count(*) FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE a.id BETWEEN 1 AND 8;",
        "sysbench_read_only": """
SELECT c FROM sbtest1 WHERE id=9;
SELECT c FROM sbtest1 WHERE id BETWEEN 4 AND 12;
SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 4 AND 12;
SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN 4 AND 12 ORDER BY c;""",
        "sysbench_read_write": """
BEGIN;
SELECT c FROM sbtest1 WHERE id=6;
UPDATE sbtest1 SET k=k+1 WHERE id=6;
UPDATE sbtest1 SET c='rw' WHERE id=8;
DELETE FROM sbtest1 WHERE id=12;
INSERT INTO sbtest1 VALUES(12,2,'c','p');
COMMIT;""",
    }
    for name, sql in statements.items():
        yield name, setup + sql


def retired_workloads():
    if not SEED_DIR.is_dir():
        return
    for path in sorted(SEED_DIR.glob("*.json")):
        bundle = json.loads(path.read_text())
        case = bundle["case"]
        parts = [shrink_setup(bundle["setup_sql"])]
        if case.get("prepare"):
            parts.append(case["prepare"])
        parts.append(case["sql"])
        if case.get("verify"):
            parts.append(case["verify"])
        yield "retired_" + path.stem, "\n".join(parts)


def workloads():
    yield from hotspot_workloads()
    yield from sysbench_workloads()
    yield from retired_workloads()


def session(work):
    return (
        ".bail on\n.headers off\n.mode list\n.nullvalue NULL\n"
        ".output /dev/null\n"
        + work.strip() + "\n"
        ".headers off\n.mode list\n.output stdout\n"
        "SELECT name||'='||value FROM dolt_engine_stats ORDER BY name;\n"
    )


def run_sql(binary, sql, timeout):
    return subprocess.run([str(binary), ":memory:"], input=sql, text=True,
                          capture_output=True, timeout=timeout)


def stats_available(binary, timeout):
    result = run_sql(binary, ".bail on\n.headers off\n.mode list\n"
                     "SELECT count(*) FROM dolt_engine_stats;\n", timeout)
    if result.returncode == 0 and result.stdout.strip().isdigit():
        return True
    text = (result.stderr + "\n" + result.stdout).lower()
    if "no such table" in text:
        return False
    raise RuntimeError(
        f"{binary} could not read dolt_engine_stats ({result.returncode}):\n"
        f"{result.stdout[-800:]}\n{result.stderr[-800:]}")


def read_counters(binary, work, timeout):
    result = run_sql(binary, session(work), timeout)
    if result.returncode != 0:
        raise RuntimeError(
            f"{binary} failed ({result.returncode}):\n"
            f"{result.stdout[-1500:]}\n{result.stderr[-1500:]}")
    values = {}
    for line in result.stdout.splitlines():
        match = STAT_LINE.fullmatch(line.strip())
        if not match:
            raise RuntimeError(f"unexpected counter line from {binary}: {line!r}")
        values[match.group(1)] = int(match.group(2))
    if not values:
        raise RuntimeError(f"{binary} returned no engine counters")
    return values


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--exceptions", type=Path,
                        help="text of the pull request's title and description")
    args = parser.parse_args(argv)
    exceptions = {}
    if args.exceptions and args.exceptions.is_file():
        exceptions = parse_exceptions(args.exceptions.read_text())
    used = set()
    if not stats_available(args.candidate, args.timeout):
        print("candidate has no dolt_engine_stats", file=sys.stderr)
        return 1
    compare = stats_available(args.baseline, args.timeout)
    if not compare:
        print("baseline has no dolt_engine_stats; checking candidate determinism only")
    failures = []
    count = 0
    for name, work in workloads():
        count += 1
        first = read_counters(args.candidate, work, args.timeout)
        second = read_counters(args.candidate, work, args.timeout)
        if first != second:
            failures.append(f"{name}: candidate counters are not deterministic")
            print(f"{name}: candidate run1={first} run2={second}")
            continue
        if not compare:
            print(f"{name}: deterministic")
            continue
        base = read_counters(args.baseline, work, args.timeout)
        worse = compare_counters(name, base, first, exceptions, used)
        if worse:
            failures.append(f"{name}: {', '.join(worse)}")
    for workload, counter in sorted(set(exceptions) - used):
        print(f"unused exception: {workload} {counter} (no regression to allow)")
    print(f"engine counters: {count} workloads, failures {len(failures)}")
    if failures:
        for item in failures:
            print(item, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
