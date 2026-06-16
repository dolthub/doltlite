#!/bin/bash
set -e

DOLTLITE=${DOLTLITE:-./doltlite}
SQLITE3=${SQLITE3:-./sqlite3}
BENCH_TIMER_SQLITE=${BENCH_TIMER_SQLITE:-./bench_timer_sqlite}
BENCH_TIMER_DOLTLITE=${BENCH_TIMER_DOLTLITE:-./bench_timer_doltlite}
SQLITE_AUTOCOMMIT_PRAGMAS=${SQLITE_AUTOCOMMIT_PRAGMAS:-"PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"}
SQLITE_FILE_CACHE_PRAGMA=${SQLITE_FILE_CACHE_PRAGMA:-"PRAGMA cache_size=16384;"}
ROWS=${BENCH_ROWS:-100000}
BENCH_MAX_MULTIPLIER=${BENCH_MAX_MULTIPLIER:-2.5}
BENCH_AVG_MAX_MULTIPLIER=${BENCH_AVG_MAX_MULTIPLIER:-2}
BENCH_AC_WRITE_MAX_MULTIPLIER=${BENCH_AC_WRITE_MAX_MULTIPLIER:-6}
BENCH_AC_WRITE_AVG_MAX_MULTIPLIER=${BENCH_AC_WRITE_AVG_MAX_MULTIPLIER:-5}
BENCH_AC_WRITE_RUNS=${BENCH_AC_WRITE_RUNS:-9}
SEED=42
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

fmt_us() {
  python3 - "$1" <<'PYEOF'
import sys
print(f"{int(sys.argv[1]):,}")
PYEOF
}

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

B = 10000
def parts(i):
    return (i // B, i % B)
def kw(i):
    a, b = parts(i)
    return f"a={a} AND b={b}"
def kvals(i):
    a, b = parts(i)
    return f"{a},{b}"
def ktuple(i):
    a, b = parts(i)
    return f"({a},{b})"
def kbetween(s, e):
    sa, sb = parts(s)
    ea, eb = parts(e)
    return f"(a,b) BETWEEN ({sa},{sb}) AND ({ea},{eb})"

def write_prepare(f):
    f.write("CREATE TABLE sbtest1(a INTEGER NOT NULL, b INTEGER NOT NULL, k INTEGER NOT NULL DEFAULT 0, c TEXT NOT NULL DEFAULT '', pad TEXT NOT NULL DEFAULT '', PRIMARY KEY(a,b)) WITHOUT ROWID;\n")
    f.write("CREATE INDEX k_idx ON sbtest1(k);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest1 VALUES({kvals(i)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def write_prepare_join(f):
    f.write("CREATE TABLE sbtest2(a INTEGER NOT NULL, b INTEGER NOT NULL, k INTEGER NOT NULL DEFAULT 0, c TEXT NOT NULL DEFAULT '', pad TEXT NOT NULL DEFAULT '', PRIMARY KEY(a,b)) WITHOUT ROWID;\n")
    f.write("CREATE INDEX k_idx2 ON sbtest2(k);\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest2 VALUES({kvals(i)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def write_prepare_types(f):
    f.write("CREATE TABLE sbtest_types(a INTEGER NOT NULL, b INTEGER NOT NULL, ival INTEGER, rval REAL, tval TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest_types VALUES({kvals(i)},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")
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
    f.write("CREATE TABLE sbtest_bulk(a INTEGER, b INTEGER, k INTEGER, c TEXT, pad TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;\n")
    f.write("BEGIN;\n")
    for i in range(1, R+1):
        f.write(f"INSERT INTO sbtest_bulk VALUES({kvals(i)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_point_select(f):
    for _ in range(10000):
        f.write(f"SELECT c FROM sbtest1 WHERE {kw(rint(1,R))};\n")

def w_range_select(f):
    for _ in range(1000):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE {kbetween(s, s+99)};\n")

def w_sum_range(f):
    for _ in range(1000):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE {kbetween(s, s+99)};\n")

def w_order_range(f):
    for _ in range(100):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE {kbetween(s, s+99)} ORDER BY c;\n")

def w_distinct_range(f):
    for _ in range(100):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT DISTINCT c FROM sbtest1 WHERE {kbetween(s, s+99)} ORDER BY c;\n")

def w_index_scan(f):
    for _ in range(1000):
        f.write(f"SELECT a, b, c FROM sbtest1 WHERE k={rint(1,R)};\n")

def w_update_index(f):
    f.write("BEGIN;\n")
    for _ in range(10000):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE {kw(rint(1,R))};\n")
    f.write("COMMIT;\n")

def w_update_non_index(f):
    f.write("BEGIN;\n")
    for _ in range(10000):
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE {kw(rint(1,R))};\n")
    f.write("COMMIT;\n")

def w_delete_insert(f):
    f.write("BEGIN;\n")
    for _ in range(5000):
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({kvals(id)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_oltp_insert(f):
    f.write("BEGIN;\n")
    for i in range(R+1, R+5001):
        f.write(f"INSERT INTO sbtest1 VALUES({kvals(i)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_write_only(f):
    f.write("BEGIN;\n")
    for _ in range(1000):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE {kw(rint(1,R))};\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE {kw(rint(1,R))};\n")
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({kvals(id)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
    f.write("COMMIT;\n")

def w_select_random_points(f):
    for _ in range(1000):
        pts=','.join(ktuple(rint(1,R)) for _ in range(10))
        f.write(f"SELECT a,b,k,c,pad FROM sbtest1 WHERE (a,b) IN ({pts});\n")

def w_select_random_ranges(f):
    for _ in range(1000):
        s=rint(1,max(R-10,1))
        f.write(f"SELECT count(k) FROM sbtest1 WHERE {kbetween(s, s+9)};\n")

def w_covering_index_scan(f):
    for _ in range(1000):
        s=rint(1,max(R-100,1))
        f.write(f"SELECT count(k) FROM sbtest1 WHERE k BETWEEN {s} AND {s+99};\n")

def w_groupby_scan(f):
    for _ in range(100):
        s=rint(1,max(R-1000,1))
        f.write(f"SELECT k, count(*) FROM sbtest1 WHERE {kbetween(s, s+999)} GROUP BY k ORDER BY k;\n")

def w_index_join(f):
    for _ in range(500):
        s=rint(1,max(R-10,1))
        sa,sb=parts(s); ea,eb=parts(s+9)
        f.write(f"SELECT a.a, a.b, b.a, b.b FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE (a.a,a.b) BETWEEN ({sa},{sb}) AND ({ea},{eb});\n")

def w_index_join_scan(f):
    for _ in range(100):
        s=rint(1,max(R-50,1))
        sa,sb=parts(s); ea,eb=parts(s+49)
        f.write(f"SELECT count(*) FROM sbtest1 a JOIN sbtest2 b ON a.k=b.k WHERE (b.a,b.b) BETWEEN ({sa},{sb}) AND ({ea},{eb});\n")

def w_types_delete_insert(f):
    f.write("BEGIN;\n")
    for _ in range(5000):
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest_types WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest_types VALUES({kvals(id)},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")
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
            f.write(f"SELECT c FROM sbtest1 WHERE {kw(rint(1,R))};\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE {kbetween(s, s+99)};\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE {kbetween(s, s+99)};\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE {kbetween(s, s+99)} ORDER BY c;\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT DISTINCT c FROM sbtest1 WHERE {kbetween(s, s+99)} ORDER BY c;\n")

def w_read_write(f):
    f.write("BEGIN;\n")
    for _ in range(1000):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE {kw(rint(1,R))};\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT c FROM sbtest1 WHERE {kbetween(s, s+99)};\n")
        s=rint(1,max(R-100,1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE {kbetween(s, s+99)};\n")
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE {kw(rint(1,R))};\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE {kw(rint(1,R))};\n")
        id=rint(1,R)
        f.write(f"DELETE FROM sbtest1 WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({kvals(id)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")
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
    f.write("CREATE TABLE sbtest_ac_bulk(a INTEGER, b INTEGER, k INTEGER, c TEXT, pad TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;\n")
    for i in range(1, AC+1):
        f.write(f"INSERT INTO sbtest_ac_bulk VALUES({kvals(i)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_oltp_insert_autocommit(f):
    for i in range(R+1, R+AC+1):
        f.write(f"INSERT INTO sbtest1 VALUES({kvals(i)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_update_index_autocommit(f):
    for _ in range(AC):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE {kw(rint(1,R))};\n")

def w_update_non_index_autocommit(f):
    for _ in range(AC):
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE {kw(rint(1,R))};\n")

def w_delete_insert_autocommit(f):
    for _ in range(AC // 2):
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest1 WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({kvals(id)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_write_only_autocommit(f):
    for _ in range(AC // 4):
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE {kw(rint(1,R))};\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE {kw(rint(1,R))};\n")
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest1 WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({kvals(id)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

def w_types_delete_insert_autocommit(f):
    for _ in range(AC // 2):
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest_types WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest_types VALUES({kvals(id)},{random.randint(-1000000,1000000)},{random.uniform(-1e6,1e6)},'{rstr(50)}');\n")

def w_read_write_autocommit(f):
    iters = AC // 4
    for _ in range(iters):
        for _ in range(10):
            f.write(f"SELECT c FROM sbtest1 WHERE {kw(rint(1,R))};\n")
        s = rint(1, max(R-100, 1))
        f.write(f"SELECT c FROM sbtest1 WHERE {kbetween(s, s+99)};\n")
        s = rint(1, max(R-100, 1))
        f.write(f"SELECT SUM(k) FROM sbtest1 WHERE {kbetween(s, s+99)};\n")
        f.write(f"UPDATE sbtest1 SET k={rint(1,R)} WHERE {kw(rint(1,R))};\n")
        f.write(f"UPDATE sbtest1 SET c='{rstr(60)}' WHERE {kw(rint(1,R))};\n")
        id = rint(1, R)
        f.write(f"DELETE FROM sbtest1 WHERE {kw(id)};\n")
        f.write(f"INSERT OR REPLACE INTO sbtest1 VALUES({kvals(id)},{rint(1,R)},'{rstr(60)}','{rstr(30)}');\n")

make_test("oltp_bulk_insert_ac",      prep_main, w_bulk_insert_autocommit)
make_test("oltp_insert_ac",           prep_main, w_oltp_insert_autocommit)
make_test("oltp_update_index_ac",     prep_main, w_update_index_autocommit)
make_test("oltp_update_non_index_ac", prep_main, w_update_non_index_autocommit)
make_test("oltp_delete_insert_ac",    prep_main, w_delete_insert_autocommit)
make_test("oltp_write_only_ac",       prep_main, w_write_only_autocommit)
make_test("types_delete_insert_ac",   prep_with_types, w_types_delete_insert_autocommit)
make_test("oltp_read_write_ac",       prep_main, w_read_write_autocommit)
PYEOF

run_bench() {
  local engine="$1" binary="$2" sql_file="$3" db_template="$4"
  local db="$db_template"
  if [ "$db" != ":memory:" ]; then
    db="/tmp/bench_${engine}_${RANDOM}_$$.db"
    rm -f "$db"
  fi
  local bench_sql_file="$sql_file"
  local sqlite_pragmas="${SQLITE_BENCH_PRAGMAS:-}"
  if [ "$engine" = "sqlite" ] && [ "$db_template" != ":memory:" ]; then
    sqlite_pragmas="$SQLITE_FILE_CACHE_PRAGMA $sqlite_pragmas"
  fi
  if [ "$engine" = "sqlite" ] && [ -n "$sqlite_pragmas" ]; then
    bench_sql_file="$TMPDIR/pragma_${engine}_${RANDOM}_$$.sql"
    printf "%s\n" "$sqlite_pragmas" > "$bench_sql_file"
    cat "$sql_file" >> "$bench_sql_file"
  fi
  local timer=""
  if [ "$engine" = "sqlite" ] && [ -x "$BENCH_TIMER_SQLITE" ]; then
    timer="$BENCH_TIMER_SQLITE"
  elif [ "$engine" = "doltlite" ] && [ -x "$BENCH_TIMER_DOLTLITE" ]; then
    timer="$BENCH_TIMER_DOLTLITE"
  fi
  if [ -n "$timer" ]; then
    "$timer" "$db" "$bench_sql_file"
    if [ "$db" != ":memory:" ]; then rm -f "$db"; fi
    return
  fi
  local output
  output=$(sed \
    -e "s/\.print BENCH_START/SELECT 'TS_START:' || CAST((julianday('now')*86400000000) AS INTEGER);/" \
    -e "s/\.print BENCH_END/SELECT 'TS_END:' || CAST((julianday('now')*86400000000) AS INTEGER);/" \
    "$bench_sql_file" | "$binary" "$db" 2>&1)
  if [ "$db" != ":memory:" ]; then rm -f "$db"; fi
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

bench_runs_for_test() {
  case "$1" in
    *_ac) echo "$BENCH_AC_WRITE_RUNS" ;;
    *) echo "${BENCH_RUNS:-5}" ;;
  esac
}

bench_runs_summary() {
  local base_runs="${BENCH_RUNS:-5}"
  if [ "$BENCH_AC_WRITE_RUNS" = "$base_runs" ]; then
    echo "median of ${base_runs} invocations per test"
  else
    echo "median of ${base_runs} invocations per test; autocommit writes use ${BENCH_AC_WRITE_RUNS}"
  fi
}

median_us() {
  python3 - "$@" <<'PYEOF'
import sys
vals = sorted(int(v) for v in sys.argv[1:] if int(v) >= 0)
if not vals:
    print(-1)
else:
    print(vals[len(vals)//2])
PYEOF
}

run_bench_stable() {
  local test_name="$1" engine="$2" binary="$3" sql_file="$4" db_template="$5"
  local runs
  local i
  local sample
  local samples=""
  runs=$(bench_runs_for_test "$test_name")
  for ((i=0; i<runs; i++)); do
    sample=$(run_bench "$engine" "$binary" "$sql_file" "$db_template")
    if [ -z "$samples" ]; then
      samples="$sample"
    else
      samples="$samples $sample"
    fi
  done
  median_us $samples
}

READ_TESTS="oltp_point_select oltp_range_select oltp_sum_range oltp_order_range oltp_distinct_range oltp_index_scan select_random_points select_random_ranges covering_index_scan groupby_scan index_join index_join_scan types_table_scan table_scan oltp_read_only"
WRITE_TESTS="oltp_bulk_insert oltp_insert oltp_update_index oltp_update_non_index oltp_delete_insert oltp_write_only types_delete_insert oltp_read_write"
WRITE_TESTS_AC="oltp_bulk_insert_ac oltp_insert_ac oltp_update_index_ac oltp_update_non_index_ac oltp_delete_insert_ac oltp_write_only_ac types_delete_insert_ac oltp_read_write_ac"

BENCH_RESULTS_FILE="$TMPDIR/bench_results.tsv"
: > "$BENCH_RESULTS_FILE"

run_section() {
  local section="$1" tests="$2" db_sq="$3" db_dl="$4"
  local ratio_sum=0
  local ratio_count=0
  local avg_ratio="--"
  echo "| Test | SQLite (us) | Doltlite (us) | Multiplier |"
  echo "|------|------------:|--------------:|-----------:|"
  for t in $tests; do
  s=$(run_bench_stable "$t" sqlite "$SQLITE3" "$TMPDIR/$t.sql" "$db_sq")
  d=$(run_bench_stable "$t" doltlite "$DOLTLITE" "$TMPDIR/$t.sql" "$db_dl")
  s_display="$s"
  d_display="$d"
  if [ "$s" -eq -1 ] 2>/dev/null; then s_display="crash"; fi
  if [ "$d" -eq -1 ] 2>/dev/null; then d_display="crash"; fi
  if [ "$s" -ge 0 ] 2>/dev/null; then s_display=$(fmt_us "$s"); fi
  if [ "$d" -ge 0 ] 2>/dev/null; then d_display=$(fmt_us "$d"); fi
  if [ "$s" -gt 0 ] 2>/dev/null && [ "$d" -ge 0 ] 2>/dev/null; then
    ratio=$(python3 -c "print(f'{$d/$s:.2f}')")
    ratio_sum=$(python3 -c "print($ratio_sum + ($d/$s))")
    ratio_count=$((ratio_count + 1))
  else
    ratio="--"
  fi
  printf '%s\t%s\t%s\t%s\n' "$section" "$t" "$s" "$d" >> "$BENCH_RESULTS_FILE"
  echo "| $t | $s_display | $d_display | ${ratio} |"
  done
  if [ "$ratio_count" -gt 0 ]; then
    avg_ratio=$(python3 -c "print(f'{($ratio_sum/$ratio_count):.2f}')")
  fi
  echo "| Average |  |  | ${avg_ratio} |"
}

echo "<!-- benchmark:compositepk -->"
echo "## Sysbench-Style Benchmark (composite PK): Doltlite vs SQLite"
echo ""
echo "_Companion to the classic Sysbench-Style Benchmark. Every workload here"
echo "runs against tables with a 2-column INTEGER \`PRIMARY KEY(a, b) WITHOUT ROWID\`._"
echo "_Individual ratios gated at ${BENCH_MAX_MULTIPLIER}×; section averages gated at ${BENCH_AVG_MAX_MULTIPLIER}×. Autocommit writes use ${BENCH_AC_WRITE_MAX_MULTIPLIER}× / ${BENCH_AC_WRITE_AVG_MAX_MULTIPLIER}×._"
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
echo "_SQLite uses WAL mode with synchronous=FULL in this section so_"
echo "_the comparison uses SQLite's durable WAL autocommit path._"
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

check_ceiling() {
  local section="$1" tests="$2" max="$3"
  local failed=0
  for t in $tests; do
    local line
    line=$(awk -F '\t' -v section="$section" -v test="$t" \
      '$1==section && $2==test {print $3 "\t" $4; exit}' \
      "$BENCH_RESULTS_FILE")
    s="${line%%$'\t'*}"
    d="${line#*$'\t'}"
    if [ "$s" -gt 0 ] 2>/dev/null && [ "$d" -ge 0 ] 2>/dev/null; then
      over=$(python3 -c "r=$d/$s; print(1 if r>$max else 0)")
      if [ "$over" = "1" ]; then
        ratio=$(python3 -c "print(f'{$d/$s:.2f}')")
        echo "FAIL: $section/$t = ${ratio}x (ceiling: ${max}x)" >&2
        failed=1
      fi
    fi
  done
  return $failed
}

check_average_ceiling() {
  local section="$1" tests="$2" max="$3"
  local ratio
  ratio=$(python3 - "$BENCH_RESULTS_FILE" "$section" "$tests" <<'PYEOF'
import sys
path, section, tests = sys.argv[1], sys.argv[2], sys.argv[3].split()
wanted = set(tests)
ratios = []
with open(path) as f:
    for line in f:
        cols = line.rstrip("\n").split("\t")
        if len(cols) < 4 or cols[0] != section or cols[1] not in wanted:
            continue
        s, d = int(cols[2]), int(cols[3])
        if s > 0 and d >= 0:
            ratios.append(d / s)
if ratios:
    print(f"{sum(ratios) / len(ratios):.2f}")
else:
    print("")
PYEOF
)
  if [ -n "$ratio" ]; then
    over=$(python3 -c "r=$ratio; print(1 if r>$max else 0)")
    if [ "$over" = "1" ]; then
      echo "FAIL: $section average = ${ratio}x (ceiling: ${max}x)" >&2
      return 1
    fi
  fi
  return 0
}

echo ""
echo "### Performance Ceiling Check (${BENCH_MAX_MULTIPLIER}x individual, ${BENCH_AVG_MAX_MULTIPLIER}x average; autocommit writes: ${BENCH_AC_WRITE_MAX_MULTIPLIER}x / ${BENCH_AC_WRITE_AVG_MAX_MULTIPLIER}x)"
echo ""

ceiling_ok=0
check_ceiling "mem_reads"   "$READ_TESTS"     "$BENCH_MAX_MULTIPLIER" || ceiling_ok=1
check_ceiling "mem_writes"  "$WRITE_TESTS"    "$BENCH_MAX_MULTIPLIER" || ceiling_ok=1
check_ceiling "file_reads"  "$READ_TESTS"     "$BENCH_MAX_MULTIPLIER" || ceiling_ok=1
check_ceiling "file_writes" "$WRITE_TESTS"    "$BENCH_MAX_MULTIPLIER" || ceiling_ok=1
check_ceiling "ac_reads"    "$READ_TESTS"     "$BENCH_MAX_MULTIPLIER" || ceiling_ok=1
check_ceiling "ac_writes"   "$WRITE_TESTS_AC" "$BENCH_AC_WRITE_MAX_MULTIPLIER" || ceiling_ok=1
check_average_ceiling "mem_reads"   "$READ_TESTS"     "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
check_average_ceiling "mem_writes"  "$WRITE_TESTS"    "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
check_average_ceiling "file_reads"  "$READ_TESTS"     "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
check_average_ceiling "file_writes" "$WRITE_TESTS"    "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
check_average_ceiling "ac_reads"    "$READ_TESTS"     "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
check_average_ceiling "ac_writes"   "$WRITE_TESTS_AC" "$BENCH_AC_WRITE_AVG_MAX_MULTIPLIER" || ceiling_ok=1

if [ "$ceiling_ok" = "0" ]; then
  echo "All tests within ceilings."
else
  echo ""
  echo "**FAILED**: One or more tests exceeded their ceiling."
  exit 1
fi
