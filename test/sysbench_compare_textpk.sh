#!/bin/bash
set -e

DOLTLITE=${DOLTLITE:-./doltlite}
SQLITE3=${SQLITE3:-./sqlite3}
BENCH_TIMER_SQLITE=${BENCH_TIMER_SQLITE:-./bench_timer_sqlite}
BENCH_TIMER_DOLTLITE=${BENCH_TIMER_DOLTLITE:-./bench_timer_doltlite}
SQLITE_AUTOCOMMIT_PRAGMAS=${SQLITE_AUTOCOMMIT_PRAGMAS:-"PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"}
SQLITE_FILE_CACHE_PRAGMA=${SQLITE_FILE_CACHE_PRAGMA:-"PRAGMA cache_size=16384;"}
ROWS=${BENCH_ROWS:-100000}
BENCH_MAX_MULTIPLIER=${BENCH_MAX_MULTIPLIER:-2.4}
BENCH_AVG_MAX_MULTIPLIER=${BENCH_AVG_MAX_MULTIPLIER:-1.95}
BENCH_AC_WRITE_MAX_MULTIPLIER=${BENCH_AC_WRITE_MAX_MULTIPLIER:-10}
BENCH_AC_WRITE_AVG_MAX_MULTIPLIER=${BENCH_AC_WRITE_AVG_MAX_MULTIPLIER:-5}
BENCH_AC_WRITE_RUNS=${BENCH_AC_WRITE_RUNS:-9}
SEED=42
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

python3 << PYEOF
import random, string, os

random.seed($SEED)
R = $ROWS
d = '$TMPDIR'

def rint(a, b):
    if b < a:
        b = a
    return random.randint(a, b)

def rstr(n):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

def hk(i):
    return f"{i:032x}"

def write_prepare(f):
    f.write("CREATE TABLE sbtest1(id TEXT PRIMARY KEY, k INTEGER NOT NULL DEFAULT 0, c TEXT NOT NULL DEFAULT '', pad TEXT NOT NULL DEFAULT '');\n")
    f.write("CREATE INDEX k_idx ON sbtest1(k);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest1 VALUES('{hk(i)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def write_prepare_join(f):
    f.write("CREATE TABLE sbtest2(id TEXT PRIMARY KEY, k INTEGER NOT NULL DEFAULT 0, c TEXT NOT NULL DEFAULT '', pad TEXT NOT NULL DEFAULT '');\n")
    f.write("CREATE INDEX k_idx2 ON sbtest2(k);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest2 VALUES('{hk(i)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def write_prepare_types(f):
    f.write("CREATE TABLE sbtest_types(id TEXT PRIMARY KEY, ival INTEGER, rval REAL, tval TEXT);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest_types VALUES('{hk(i)}',{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")
    f.write("COMMIT;\n")


def stable_seed(name):
    h = 0
    for ch in name:
        h = ((h * 131) + ord(ch)) % 10000
    return $SEED + h

def make_test(name, prepare_fn, workload_fn):
    random.seed($SEED)
    with open(f'{d}/{name}.sql', 'w') as f:
        prepare_fn(f)
        f.write(".print BENCH_START\n")
        random.seed(stable_seed(name))
        workload_fn(f)
        f.write(".print BENCH_END\n")

def prep_main(f):
    write_prepare(f)

def prep_with_join(f):
    write_prepare(f)
    write_prepare_join(f)

def prep_with_types(f):
    write_prepare(f)
    write_prepare_types(f)


def w_bulk_insert(f):
    f.write("CREATE TABLE sbtest_bulk(id TEXT PRIMARY KEY, k INTEGER, c TEXT, pad TEXT);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest_bulk VALUES('{hk(i)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_point_select(f):
    for _ in range(10000):
        f.write(f"SELECT c FROM sbtest1 WHERE id='{hk(rint(1,R))}';\n")

def w_range_select(f):
    for _ in range(1000):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")

def w_sum_range(f):
    for _ in range(1000):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")

def w_order_range(f):
    for _ in range(100):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}' ORDER BY c;\n")

def w_distinct_range(f):
    for _ in range(100):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}' ORDER BY c;\n")

def w_index_scan(f):
    for _ in range(1000):
        f.write(f"SELECT id, c FROM sbtest1 WHERE k={rint(1,R)};\n")

def w_update_index(f):
    f.write("BEGIN;\n")
    for _ in range(10000):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE id='{hk(rint(1,R))}';\n")
    f.write("COMMIT;\n")

def w_update_non_index(f):
    f.write("BEGIN;\n")
    for _ in range(10000):
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id='{hk(rint(1,R))}';\n")
    f.write("COMMIT;\n")

def w_delete_insert(f):
    f.write("BEGIN;\n")
    for _ in range(5000):
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES('{hk(id)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_oltp_insert(f):
    f.write("BEGIN;\n")
    for i in range(R+1, R+5001):
        f.write(f"INSERT INTO sbtest1 VALUES('{hk(i)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_write_only(f):
    f.write("BEGIN;\n")
    for _ in range(1000):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE id='{hk(rint(1,R))}';\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id='{hk(rint(1,R))}';\n")
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES('{hk(id)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_select_random_points(f):
    for _ in range(1000):
        pts=','.join(f"'{hk(rint(1,R))}'" for _ in range(10))
        f.write(f"SELECT id,k,c,pad FROM sbtest1 WHERE id IN ({pts});\n")

def w_select_random_ranges(f):
    for _ in range(1000):
        s=rint(1,max(R-10,1))
        f.write(f"SELECT count(k) FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+9)}';\n")

def w_covering_index_scan(f):
    for _ in range(1000):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT count(k) FROM sbtest1 WHERE k BETWEEN {s} AND {s+99};\n")

def w_groupby_scan(f):
    for _ in range(100):
        s=rint(1,max(R-1000,1))
        f.write(f"SELECT k, count(*) FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+999)}' GROUP BY k ORDER BY k;\n")

def w_index_join(f):
    for _ in range(500):
        s=rint(1,max(R-10,1))
        f.write(f"SELECT a.id, b.id FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE a.id BETWEEN '{hk(s)}' AND '{hk(s+9)}';\n")

def w_index_join_scan(f):
    for _ in range(100):
        s=rint(1,max(R-50,1))
        f.write(f"SELECT count(*) FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE b.id BETWEEN '{hk(s)}' AND '{hk(s+49)}';\n")

def w_types_delete_insert(f):
    f.write("BEGIN;\n")
    for _ in range(5000):
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest_types WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest_types VALUES('{hk(id)}',{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")
    f.write("COMMIT;\n")

def w_types_table_scan(f):
    for _ in range(100):
        f.write(f"SELECT count(*) FROM sbtest_types WHERE tval LIKE '%{rstr(3)}%';\n")

def w_table_scan(f):
    for _ in range(100):
        f.write("SELECT count(*) FROM sbtest1 WHERE c LIKE '%abc%';\n")

def w_read_only(f):
    for _ in range(1000):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE id='{hk(rint(1,R))}';\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}' ORDER BY c;\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}' ORDER BY c;\n")

def w_read_write(f):
    f.write("BEGIN;\n")
    for _ in range(1000):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE id='{hk(rint(1,R))}';\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE id='{hk(rint(1,R))}';\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id='{hk(rint(1,R))}';\n")
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES('{hk(id)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

make_test("oltp_bulk_insert",    prep_main, w_bulk_insert)
make_test("oltp_point_select",   prep_main, w_point_select)
make_test("oltp_range_select",   prep_main, w_range_select)
make_test("oltp_sum_range",      prep_main, w_sum_range)
make_test("oltp_order_range",    prep_main, w_order_range)
make_test("oltp_distinct_range", prep_main, w_distinct_range)
make_test("oltp_index_scan",     prep_main, w_index_scan)
make_test("oltp_update_index",   prep_main, w_update_index)
make_test("oltp_update_non_index", prep_main, w_update_non_index)
make_test("oltp_delete_insert",  prep_main, w_delete_insert)
make_test("oltp_insert",         prep_main, w_oltp_insert)
make_test("oltp_write_only",     prep_main, w_write_only)
make_test("select_random_points", prep_main, w_select_random_points)
make_test("select_random_ranges", prep_main, w_select_random_ranges)
make_test("covering_index_scan", prep_main, w_covering_index_scan)
make_test("groupby_scan",        prep_main, w_groupby_scan)
make_test("index_join",          prep_with_join, w_index_join)
make_test("index_join_scan",     prep_with_join, w_index_join_scan)
make_test("types_delete_insert", prep_with_types, w_types_delete_insert)
make_test("types_table_scan",    prep_with_types, w_types_table_scan)
make_test("table_scan",          prep_main, w_table_scan)
make_test("oltp_read_only",      prep_main, w_read_only)
make_test("oltp_read_write",     prep_main, w_read_write)

AC = 200  # statements per autocommit test

def w_bulk_insert_autocommit(f):
    f.write("CREATE TABLE sbtest_ac_bulk(id TEXT PRIMARY KEY, k INTEGER, c TEXT, pad TEXT);\n")
    for i in range(1, AC+1):
        f.write(f"INSERT INTO sbtest_ac_bulk VALUES('{hk(i)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_oltp_insert_autocommit(f):
    for i in range(R+1, R+AC+1):
        f.write(f"INSERT INTO sbtest1 VALUES('{hk(i)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_update_index_autocommit(f):
    for _ in range(AC):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE id='{hk(rint(1,R))}';\n")

def w_update_non_index_autocommit(f):
    for _ in range(AC):
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id='{hk(rint(1,R))}';\n")

def w_delete_insert_autocommit(f):
    for _ in range(AC // 2):
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest1 WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES('{hk(id)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_write_only_autocommit(f):
    for _ in range(AC // 4):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE id='{hk(rint(1,R))}';\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id='{hk(rint(1,R))}';\n")
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest1 WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES('{hk(id)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_types_delete_insert_autocommit(f):
    for _ in range(AC // 2):
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest_types WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest_types VALUES('{hk(id)}',{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")

def w_read_write_autocommit(f):
    iters = AC // 4
    for _ in range(iters):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE id='{hk(rint(1,R))}';\n")
        s = rint(1, max(R-100, 1))
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")
        s = rint(1, max(R-100, 1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN '{hk(s)}' AND '{hk(s+99)}';\n")
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE id='{hk(rint(1,R))}';\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id='{hk(rint(1,R))}';\n")
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest1 WHERE id='{hk(id)}';\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES('{hk(id)}',{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

make_test("oltp_bulk_insert_ac",      prep_main, w_bulk_insert_autocommit)
make_test("oltp_insert_ac",           prep_main, w_oltp_insert_autocommit)
make_test("oltp_update_index_ac",     prep_main, w_update_index_autocommit)
make_test("oltp_update_non_index_ac", prep_main, w_update_non_index_autocommit)
make_test("oltp_delete_insert_ac",    prep_main, w_delete_insert_autocommit)
make_test("oltp_write_only_ac",       prep_main, w_write_only_autocommit)
make_test("types_delete_insert_ac",   prep_with_types, w_types_delete_insert_autocommit)
make_test("oltp_read_write_ac",       prep_main, w_read_write_autocommit)
PYEOF

READ_TESTS="oltp_point_select oltp_range_select oltp_sum_range oltp_order_range oltp_distinct_range oltp_index_scan select_random_points select_random_ranges covering_index_scan groupby_scan index_join index_join_scan types_table_scan table_scan oltp_read_only"
WRITE_TESTS="oltp_bulk_insert oltp_insert oltp_update_index oltp_update_non_index oltp_delete_insert oltp_write_only types_delete_insert oltp_read_write"
WRITE_TESTS_AC="oltp_bulk_insert_ac oltp_insert_ac oltp_update_index_ac oltp_update_non_index_ac oltp_delete_insert_ac oltp_write_only_ac types_delete_insert_ac oltp_read_write_ac"

source "$(dirname "$0")/lib/sysbench_benchmark.sh"

echo "<!-- benchmark:textpk -->"
echo "## Sysbench-Style Benchmark (TEXT PK): $BENCH_CANDIDATE_LABEL vs $BENCH_BASELINE_LABEL"
echo ""
echo "_Companion to the classic Sysbench-Style Benchmark. Every workload here"
echo "runs against tables with a 32-char hex \`TEXT PRIMARY KEY\` (UUID-shaped)._"
benchmark_gate_note
echo ""
echo "### In-Memory"
echo ""
echo "#### Reads"
echo ""
run_section "mem_reads" "$READ_TESTS" ":memory:" ":memory:"
echo ""
echo "#### Writes"
echo ""
run_section "mem_writes" "$WRITE_TESTS" ":memory:" ":memory:"
echo ""
echo "### File-Backed"
echo ""
echo "#### Reads"
echo ""
run_section "file_reads" "$READ_TESTS" "$TMPDIR/bench_file" "$TMPDIR/bench_file"
echo ""
echo "#### Writes"
echo ""
run_section "file_writes" "$WRITE_TESTS" "$TMPDIR/bench_file" "$TMPDIR/bench_file"

echo ""
echo "### File-Backed (autocommit)"
echo ""
echo "_Each statement runs as its own transaction — exposes per-commit_"
echo "_fixed costs that the wrapped-in-BEGIN/COMMIT tests amortize away._"
benchmark_autocommit_note
echo ""
echo "#### Reads"
echo ""
echo "_Reads have no commit cost; these are the same SQL files as the_"
echo "_File-Backed Reads section, included here for symmetry and to_"
echo "_catch any per-statement overhead doltlite pays on the read path._"
echo ""
SQLITE_BENCH_PRAGMAS="$SQLITE_AUTOCOMMIT_PRAGMAS" run_section "ac_reads" "$READ_TESTS" "$TMPDIR/bench_file" "$TMPDIR/bench_file"
echo ""
echo "#### Writes"
echo ""
SQLITE_BENCH_PRAGMAS="$SQLITE_AUTOCOMMIT_PRAGMAS" run_section "ac_writes" "$WRITE_TESTS_AC" "$TMPDIR/bench_file" "$TMPDIR/bench_file"

echo ""
echo "_${ROWS} rows, $(bench_runs_summary), workload-only timing via host monotonic clock when available._"

benchmark_finish
