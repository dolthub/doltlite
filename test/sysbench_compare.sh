#!/bin/bash
#
# Sysbench-style OLTP benchmark: doltlite vs stock SQLite
#
# Mimics sysbench workloads: bulk insert, point select, range select,
# update index, update non-index, delete/insert, read-only, read-write.
#
# Output format: markdown table (for CI PR comments)
# Set BENCH_FORMAT=terminal for human-readable terminal output.
#
set -e

DOLTLITE=${DOLTLITE:-./doltlite}
SQLITE3=${SQLITE3:-./sqlite3}
ROWS=${BENCH_ROWS:-10000}
SEED=42

DB_DL=/tmp/bench_doltlite_$$.db
DB_SQ=/tmp/bench_sqlite3_$$.db

cleanup() { rm -f "$DB_DL" "$DB_SQ"; }
trap cleanup EXIT

# Timing helper: returns milliseconds
time_ms() {
  local start=$(python3 -c "import time; print(int(time.time()*1000))")
  eval "$@" > /dev/null 2>&1
  local end=$(python3 -c "import time; print(int(time.time()*1000))")
  echo $((end - start))
}

# Output helpers
if [ "${BENCH_FORMAT}" = "terminal" ]; then
  header() { printf "%-40s %10s %10s %10s\n" "Test" "SQLite" "Doltlite" "Ratio"; printf "%-40s %10s %10s %10s\n" "----" "------" "--------" "-----"; }
  row() { printf "%-40s %8dms %8dms %10s\n" "$1" "$2" "$3" "$4"; }
  footer() { echo ""; echo "Rows: $ROWS | Ratio = doltlite/sqlite (1.00x = same speed)"; }
else
  header() {
    echo "## Sysbench-Style Benchmark: doltlite vs stock SQLite"
    echo ""
    echo "| Test | SQLite (ms) | Doltlite (ms) | Ratio |"
    echo "|------|-------------|---------------|-------|"
  }
  row() { echo "| $1 | $2 | $3 | $4 |"; }
  footer() {
    echo ""
    echo "_${ROWS} rows per table. Ratio = doltlite/sqlite (1.00x = parity)._"
  }
fi

bench() {
  local name="$1" sql_file="$2"
  local t_sq t_dl ratio
  t_sq=$(time_ms "cat '$sql_file' | $SQLITE3 '$DB_SQ'")
  t_dl=$(time_ms "cat '$sql_file' | $DOLTLITE '$DB_DL'")
  ratio=$(python3 -c "print(f'{$t_dl/$t_sq:.2f}x' if $t_sq>0 else 'N/A')")
  row "$name" "$t_sq" "$t_dl" "$ratio"
}

# ============================================================
# Generate all SQL workloads upfront (deterministic via seed)
# ============================================================
TMPDIR=$(mktemp -d)

python3 -c "
import random, string, os
random.seed($SEED)
R=$ROWS
d='$TMPDIR'

def rstr(n):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

# Schema + bulk insert
with open(f'{d}/prepare.sql','w') as f:
    f.write('''CREATE TABLE sbtest1(
  id INTEGER PRIMARY KEY,
  k INTEGER NOT NULL DEFAULT 0,
  c TEXT NOT NULL DEFAULT '',
  pad TEXT NOT NULL DEFAULT ''
);
CREATE INDEX k_idx ON sbtest1(k);
BEGIN;
''')
    for i in range(1, R+1):
        f.write(f\"INSERT INTO sbtest1 VALUES({i},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# Point selects
with open(f'{d}/point_select.sql','w') as f:
    for _ in range(1000):
        f.write(f'SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n')

# Range selects
with open(f'{d}/range_select.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')

# Sum range
with open(f'{d}/sum_range.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')

# Order range
with open(f'{d}/order_range.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')

# Distinct range
with open(f'{d}/distinct_range.sql','w') as f:
    for _ in range(100):
        s=random.randint(1,R-100)
        f.write(f'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')

# Index scan
with open(f'{d}/index_scan.sql','w') as f:
    for _ in range(100):
        f.write(f'SELECT id, c FROM sbtest1 WHERE k={random.randint(1,R)};\n')

# Update index
with open(f'{d}/update_index.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(1000):
        f.write(f'UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n')
    f.write('COMMIT;\n')

# Update non-index
with open(f'{d}/update_nonindex.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(1000):
        f.write(f\"UPDATE sbtest1 SET c='{rstr(120)}' WHERE id={random.randint(1,R)};\n\")
    f.write('COMMIT;\n')

# Delete + insert
with open(f'{d}/delete_insert.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(500):
        id=random.randint(1,R)
        f.write(f'DELETE FROM sbtest1 WHERE id={id};\n')
        f.write(f\"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# Read-only mixed
with open(f'{d}/read_only.sql','w') as f:
    for _ in range(100):
        for _ in range(10):
            f.write(f'SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99} ORDER BY c;\n')

# Read-write mixed
with open(f'{d}/read_write.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(100):
        for _ in range(10):
            f.write(f'SELECT c FROM sbtest1 WHERE id={random.randint(1,R)};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT c FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        s=random.randint(1,R-100)
        f.write(f'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+99};\n')
        f.write(f'UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n')
        f.write(f\"UPDATE sbtest1 SET c='{rstr(120)}' WHERE id={random.randint(1,R)};\n\")
        id=random.randint(1,R)
        f.write(f'DELETE FROM sbtest1 WHERE id={id};\n')
        f.write(f\"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# Table scan
with open(f'{d}/table_scan.sql','w') as f:
    f.write(\"SELECT COUNT(*) FROM sbtest1 WHERE c LIKE '%abc%';\n\")

# --- Additional Dolt benchmarks ---

# oltp_insert: single-row inserts (no batch/transaction)
with open(f'{d}/oltp_insert.sql','w') as f:
    f.write('BEGIN;\n')
    for i in range(R+1, R+501):
        f.write(f\"INSERT INTO sbtest1 VALUES({i},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# oltp_write_only: pure write mix (update index + update non-index + delete/insert)
with open(f'{d}/write_only.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(100):
        f.write(f'UPDATE sbtest1 SET k={random.randint(1,R)} WHERE id={random.randint(1,R)};\n')
        f.write(f\"UPDATE sbtest1 SET c='{rstr(120)}' WHERE id={random.randint(1,R)};\n\")
        id=random.randint(1,R)
        f.write(f'DELETE FROM sbtest1 WHERE id={id};\n')
        f.write(f\"INSERT OR REPLACE INTO sbtest1 VALUES({id},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

# select_random_points: 1000 random IN-list point lookups
with open(f'{d}/select_random_points.sql','w') as f:
    for _ in range(100):
        pts = ','.join(str(random.randint(1,R)) for _ in range(10))
        f.write(f'SELECT id, k, c, pad FROM sbtest1 WHERE id IN ({pts});\n')

# select_random_ranges: 100 random range lookups
with open(f'{d}/select_random_ranges.sql','w') as f:
    for _ in range(100):
        s = random.randint(1, R-10)
        f.write(f'SELECT count(k) FROM sbtest1 WHERE id BETWEEN {s} AND {s+9};\n')

# covering_index_scan: queries satisfied entirely by the k index
with open(f'{d}/covering_index_scan.sql','w') as f:
    for _ in range(100):
        s = random.randint(1, R-100)
        f.write(f'SELECT count(k) FROM sbtest1 WHERE k BETWEEN {s} AND {s+99};\n')

# groupby_scan: GROUP BY on indexed column
with open(f'{d}/groupby_scan.sql','w') as f:
    for _ in range(20):
        s = random.randint(1, R-1000)
        f.write(f'SELECT k, count(*) FROM sbtest1 WHERE id BETWEEN {s} AND {s+999} GROUP BY k ORDER BY k;\n')

# index_join: join two tables on index
# First create a second table in the prepare step
with open(f'{d}/prepare_join.sql','w') as f:
    f.write('''CREATE TABLE IF NOT EXISTS sbtest2(
  id INTEGER PRIMARY KEY,
  k INTEGER NOT NULL DEFAULT 0,
  c TEXT NOT NULL DEFAULT '',
  pad TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS k_idx2 ON sbtest2(k);
BEGIN;
''')
    for i in range(1, min(R, 1000)+1):
        f.write(f\"INSERT INTO sbtest2 VALUES({i},{random.randint(1,R)},'{rstr(120)}','{rstr(60)}');\n\")
    f.write('COMMIT;\n')

with open(f'{d}/index_join.sql','w') as f:
    for _ in range(50):
        s = random.randint(1, min(R, 1000)-10)
        f.write(f'SELECT a.id, b.id FROM sbtest1 a JOIN sbtest2 b ON a.k = b.k WHERE a.id BETWEEN {s} AND {s+9};\n')

# index_join_scan: join with full scan on one side
with open(f'{d}/index_join_scan.sql','w') as f:
    for _ in range(10):
        s = random.randint(1, min(R, 1000)-50)
        f.write(f'SELECT count(*) FROM sbtest1 a JOIN sbtest2 b ON a.k = b.k WHERE b.id BETWEEN {s} AND {s+49};\n')

# types_delete_insert: typed columns (INT, REAL, TEXT) delete+reinsert
with open(f'{d}/prepare_types.sql','w') as f:
    f.write('''CREATE TABLE IF NOT EXISTS sbtest_types(
  id INTEGER PRIMARY KEY,
  ival INTEGER,
  rval REAL,
  tval TEXT,
  bval BLOB
);
BEGIN;
''')
    for i in range(1, 1001):
        f.write(f\"INSERT INTO sbtest_types VALUES({i},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}',X'{rstr(20)}');\n\")
    f.write('COMMIT;\n')

with open(f'{d}/types_delete_insert.sql','w') as f:
    f.write('BEGIN;\n')
    for _ in range(500):
        id = random.randint(1, 1000)
        f.write(f'DELETE FROM sbtest_types WHERE id={id};\n')
        f.write(f\"INSERT OR REPLACE INTO sbtest_types VALUES({id},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}',X'{rstr(20)}');\n\")
    f.write('COMMIT;\n')

# types_table_scan: full scan on typed table
with open(f'{d}/types_table_scan.sql','w') as f:
    for _ in range(10):
        f.write(f\"SELECT count(*) FROM sbtest_types WHERE tval LIKE '%{rstr(3)}%';\n\")
"

# ============================================================
# Run benchmarks
# ============================================================
header

# Prepare both databases
t_sq=$(time_ms "cat '$TMPDIR/prepare.sql' | $SQLITE3 '$DB_SQ'")
t_dl=$(time_ms "cat '$TMPDIR/prepare.sql' | $DOLTLITE '$DB_DL'")
ratio=$(python3 -c "print(f'{$t_dl/$t_sq:.2f}x' if $t_sq>0 else 'N/A')")
row "oltp_bulk_insert ($ROWS rows)" "$t_sq" "$t_dl" "$ratio"

# Prepare join and types tables
time_ms "cat '$TMPDIR/prepare_join.sql' | $SQLITE3 '$DB_SQ'" > /dev/null
time_ms "cat '$TMPDIR/prepare_join.sql' | $DOLTLITE '$DB_DL'" > /dev/null
time_ms "cat '$TMPDIR/prepare_types.sql' | $SQLITE3 '$DB_SQ'" > /dev/null
time_ms "cat '$TMPDIR/prepare_types.sql' | $DOLTLITE '$DB_DL'" > /dev/null

bench "oltp_point_select (1000 queries)" "$TMPDIR/point_select.sql"
bench "oltp_range_select (100 x 100 rows)" "$TMPDIR/range_select.sql"
bench "oltp_sum_range (100 queries)" "$TMPDIR/sum_range.sql"
bench "oltp_order_range (100 queries)" "$TMPDIR/order_range.sql"
bench "oltp_distinct_range (100 queries)" "$TMPDIR/distinct_range.sql"
bench "oltp_index_scan (100 queries)" "$TMPDIR/index_scan.sql"
bench "oltp_update_index (1000 updates)" "$TMPDIR/update_index.sql"
bench "oltp_update_non_index (1000 updates)" "$TMPDIR/update_nonindex.sql"
bench "oltp_delete_insert (500 pairs)" "$TMPDIR/delete_insert.sql"
bench "oltp_read_only (100 txns)" "$TMPDIR/read_only.sql"
bench "oltp_read_write (100 txns)" "$TMPDIR/read_write.sql"
bench "table_scan (full scan LIKE)" "$TMPDIR/table_scan.sql"
bench "oltp_insert (500 inserts)" "$TMPDIR/oltp_insert.sql"
bench "oltp_write_only (100 txns)" "$TMPDIR/write_only.sql"
bench "select_random_points (100 x 10-IN)" "$TMPDIR/select_random_points.sql"
bench "select_random_ranges (100 ranges)" "$TMPDIR/select_random_ranges.sql"
bench "covering_index_scan (100 queries)" "$TMPDIR/covering_index_scan.sql"
bench "groupby_scan (20 queries)" "$TMPDIR/groupby_scan.sql"
bench "index_join (50 joins)" "$TMPDIR/index_join.sql"
bench "index_join_scan (10 joins)" "$TMPDIR/index_join_scan.sql"
bench "types_delete_insert (500 pairs)" "$TMPDIR/types_delete_insert.sql"
bench "types_table_scan (10 scans)" "$TMPDIR/types_table_scan.sql"

footer

rm -rf "$TMPDIR"
