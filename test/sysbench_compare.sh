#!/bin/bash
#
# Sysbench-style OLTP benchmark: doltlite vs stock SQLite
#
# Uses a single CLI invocation per test to avoid multi-connection issues.
# Each test gets its own database with identical pre-populated data.
#
set -e

DOLTLITE=${DOLTLITE:-./doltlite}
SQLITE3=${SQLITE3:-./sqlite3}
ROWS=${BENCH_ROWS:-10000}
SEED=42
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

# ============================================================
# Generate SQL files: each test = prepare + timing markers + workload
# ============================================================
python3 << PYEOF
import random, string, os

random.seed($SEED)
R = $ROWS
d = '$TMPDIR'

def rstr(n):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

# Common schema + data
def write_prepare(f):
    f.write("CREATE TABLE sbtest1(id INTEGER PRIMARY KEY, k INTEGER NOT NULL DEFAULT 0, c TEXT NOT NULL DEFAULT '', pad TEXT NOT NULL DEFAULT '');\n")
    f.write("CREATE INDEX k_idx ON sbtest1(k);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest1 VALUES({i},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def write_prepare_join(f):
    f.write("CREATE TABLE sbtest2(id INTEGER PRIMARY KEY, k INTEGER NOT NULL DEFAULT 0, c TEXT NOT NULL DEFAULT '', pad TEXT NOT NULL DEFAULT '');\n")
    f.write("CREATE INDEX k_idx2 ON sbtest2(k);\n")
    f.write("BEGIN;\n")
    for i in range(1, min(R,1000)+1):
        f.write(f"INSERT INTO sbtest2 VALUES({i},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def write_prepare_types(f):
    f.write("CREATE TABLE sbtest_types(id INTEGER PRIMARY KEY, ival INTEGER, rval REAL, tval TEXT);\n")
    f.write("BEGIN;\n")
    for i in range(1, 1001):
        f.write(f"INSERT INTO sbtest_types VALUES({i},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")
    f.write("COMMIT;\n")

# Each test file: prepare + ".print BENCH_START" + workload + ".print BENCH_END"
# The runner times between START and END markers

def make_test(name, prepare_fn, workload_fn):
    random.seed($SEED)  # Reset for deterministic prepare
    with open(f'{d}/{name}.sql', 'w') as f:
        prepare_fn(f)
        f.write(".print BENCH_START\n")
        random.seed($SEED + hash(name) % 10000)  # Unique workload seed
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

# --- Tests ---

def w_bulk_insert(f):
    f.write("CREATE TABLE sbtest_bulk(id INTEGER PRIMARY KEY, k INTEGER, c TEXT, pad TEXT);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest_bulk VALUES({i},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_point_select(f):
    for _ in range(10000):
        f.write(f"SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n")

def w_range_select(f):
    for _ in range(1000):
        s=random.randint(1,R-100)
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n")

def w_sum_range(f):
    for _ in range(1000):
        s=random.randint(1,R-100)
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n")

def w_order_range(f):
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n")

def w_distinct_range(f):
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f"SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n")

def w_index_scan(f):
    for _ in range(1000):
        f.write(f"SELECT id, c FROM sbtest1 WHERE k={random.randint(1,R)};\n")

def w_update_index(f):
    f.write("BEGIN;\n")
    for _ in range(10000):
        f.write(f"UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n")
    f.write("COMMIT;\n")

def w_update_non_index(f):
    f.write("BEGIN;\n")
    for _ in range(10000):
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id={random.randint(1,R)};\n")
    f.write("COMMIT;\n")

def w_delete_insert(f):
    f.write("BEGIN;\n")
    for _ in range(5000):
        id=random.randint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE id={id};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_oltp_insert(f):
    f.write("BEGIN;\n")
    for i in range(R+1, R+5001):
        f.write(f"INSERT INTO sbtest1 VALUES({i},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_write_only(f):
    f.write("BEGIN;\n")
    for _ in range(1000):
        f.write(f"UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id={random.randint(1,R)};\n")
        id=random.randint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE id={id};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_select_random_points(f):
    for _ in range(1000):
        pts=','.join(str(random.randint(1,R)) for _ in range(10))
        f.write(f"SELECT id,k,c,pad FROM sbtest1 WHERE id IN ({pts});\n")

def w_select_random_ranges(f):
    for _ in range(1000):
        s=random.randint(1,R-10)
        f.write(f"SELECT count(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+9};\n")

def w_covering_index_scan(f):
    for _ in range(1000):
        s=random.randint(1,R-100)
        f.write(f"SELECT count(k) FROM sbtest1 WHERE k BETWEEN {s} AND {s+99};\n")

def w_groupby_scan(f):
    for _ in range(100):
        s=random.randint(1,R-1000)
        f.write(f"SELECT k, count(*) FROM sbtest1 WHERE id BETWEEN {s} AND {s+999} GROUP BY k ORDER BY k;\n")

def w_index_join(f):
    for _ in range(500):
        s=random.randint(1,R-10)
        f.write(f"SELECT a.id, b.id FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE a.id BETWEEN {s} AND {s+9};\n")

def w_index_join_scan(f):
    for _ in range(100):
        s=random.randint(1,950)
        f.write(f"SELECT count(*) FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE b.id BETWEEN {s} AND {s+49};\n")

def w_types_delete_insert(f):
    f.write("BEGIN;\n")
    for _ in range(5000):
        id=random.randint(1,1000)
        f.write(f"DELETE FROM sbtest_types WHERE id={id};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest_types VALUES({id},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")
    f.write("COMMIT;\n")

def w_types_table_scan(f):
    for _ in range(100):
        f.write(f"SELECT count(*) FROM sbtest_types WHERE tval LIKE '%{rstr(3)}%';\n")

def w_table_scan(f):
    f.write("SELECT count(*) FROM sbtest1 WHERE c LIKE '%abc%';\n")

def w_read_only(f):
    for _ in range(1000):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n")
        s=random.randint(1,R-100)
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n")
        s=random.randint(1,R-100)
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n")
        s=random.randint(1,R-100)
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n")
        s=random.randint(1,R-100)
        f.write(f"SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n")

def w_read_write(f):
    f.write("BEGIN;\n")
    for _ in range(1000):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n")
        s=random.randint(1,R-100)
        f.write(f"SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n")
        s=random.randint(1,R-100)
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n")
        f.write(f"UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE id={random.randint(1,R)};\n")
        id=random.randint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE id={id};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

# Generate all test SQL files
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
PYEOF

# ============================================================
# Run each test: single CLI invocation, SQL timestamps for timing
# ============================================================
run_bench() {
  local engine="$1" binary="$2" sql_file="$3" db_template="$4"
  # For file-backed, use a unique temp file per invocation
  local db="$db_template"
  if [ "$db" != ":memory:" ]; then
    db="/tmp/bench_${engine}_${RANDOM}_$$.db"
    rm -f "$db"
  fi
  local output
  output=$(sed \
    -e "s/\.print BENCH_START/SELECT 'TS_START:' || (julianday('now')*86400000);/" \
    -e "s/\.print BENCH_END/SELECT 'TS_END:' || (julianday('now')*86400000);/" \
    "$sql_file" | "$binary" "$db" 2>&1)
  if [ "$db" != ":memory:" ]; then rm -f "$db"; fi
  # Extract timestamps and compute delta
  echo "$output" | python3 -c "
import sys, re
start = end = None
for line in sys.stdin:
    m = re.search(r'TS_START:(\d+)', line)
    if m: start = int(m.group(1))
    m = re.search(r'TS_END:(\d+)', line)
    if m: end = int(m.group(1))
if start is not None and end is not None:
    print(end - start)
else:
    print(-1)
"
}

READ_TESTS="oltp_point_select oltp_range_select oltp_sum_range oltp_order_range oltp_distinct_range oltp_index_scan select_random_points select_random_ranges covering_index_scan groupby_scan index_join index_join_scan types_table_scan table_scan oltp_read_only"
WRITE_TESTS="oltp_bulk_insert oltp_insert oltp_update_index oltp_update_non_index oltp_delete_insert oltp_write_only types_delete_insert oltp_read_write"

# ============================================================
# Output markdown table
# ============================================================
run_section() {
  local tests="$1" db_sq="$2" db_dl="$3"
  echo "| Test | SQLite (ms) | Doltlite (ms) | Multiplier |"
  echo "|------|-------------|---------------|------------|"
  for t in $tests; do
  s=$(run_bench sqlite "$SQLITE3" "$TMPDIR/$t.sql" "$db_sq")
  d=$(run_bench doltlite "$DOLTLITE" "$TMPDIR/$t.sql" "$db_dl")
  s_display="$s"
  d_display="$d"
  if [ "$s" -eq -1 ] 2>/dev/null; then s_display="crash"; fi
  if [ "$d" -eq -1 ] 2>/dev/null; then d_display="crash"; fi
  if [ "$s" -gt 0 ] 2>/dev/null && [ "$d" -ge 0 ] 2>/dev/null; then
    ratio=$(python3 -c "print(f'{$d/$s:.2f}')")
  else
    ratio="--"
  fi
  echo "| $t | $s_display | $d_display | ${ratio} |"
  done
}

echo "## Sysbench-Style Benchmark: Doltlite vs SQLite"
echo ""
echo "### In-Memory"
echo ""
echo "#### Reads"
echo ""
run_section "$READ_TESTS" ":memory:" ":memory:"
echo ""
echo "#### Writes"
echo ""
run_section "$WRITE_TESTS" ":memory:" ":memory:"
echo ""
echo "### File-Backed"
echo ""
echo "#### Reads"
echo ""
run_section "$READ_TESTS" "/tmp/bench_file" "/tmp/bench_file"
echo ""
echo "#### Writes"
echo ""
run_section "$WRITE_TESTS" "/tmp/bench_file" "/tmp/bench_file"

echo ""
echo "_${ROWS} rows, single CLI invocation per test, workload-only timing via SQL timestamps._"
