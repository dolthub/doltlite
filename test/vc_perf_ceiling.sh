#!/bin/bash
set -euo pipefail

DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
if [ ! -x "$DOLTLITE" ] && [ -x ./sqlite3 ]; then
  DOLTLITE=./sqlite3
fi
if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found: $DOLTLITE" >&2
  exit 1
fi

RUNS=${VC_PERF_RUNS:-3}
TABLES=${VC_PERF_TABLES:-800}
ROWS_PER_TABLE=${VC_PERF_ROWS_PER_TABLE:-125}
BRANCHES=${VC_PERF_BRANCHES:-300}
CHECKOUT_TABLES=${VC_PERF_CHECKOUT_TABLES:-400}
CHECKOUT_ROWS_PER_TABLE=${VC_PERF_CHECKOUT_ROWS_PER_TABLE:-250}
MERGE_ROWS=${VC_PERF_MERGE_ROWS:-100000}
MERGE_CHANGE_ROWS=${VC_PERF_MERGE_CHANGE_ROWS:-2000}
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

ms_now() {
  python3 - <<'PYEOF'
import time
print(int(time.time() * 1000))
PYEOF
}

median_ms() {
  python3 - "$@" <<'PYEOF'
import sys
vals = sorted(int(v) for v in sys.argv[1:])
print(vals[len(vals)//2])
PYEOF
}

run_sql() {
  local db="$1"
  local sql="$2"
  printf '%s\n' "$sql" | "$DOLTLITE" "$db" >/dev/null
}

run_sql_file() {
  local db="$1"
  local file="$2"
  "$DOLTLITE" "$db" < "$file" >/dev/null
}

cte() {
  echo "WITH RECURSIVE c(i) AS (SELECT $1 UNION ALL SELECT i+1 FROM c WHERE i<$2)"
}

write_many_tables_sql() {
  local file="$1"
  local n="$2"
  local rows="$3"
  {
    echo "PRAGMA journal_mode=OFF;"
    echo "PRAGMA synchronous=OFF;"
    for ((i=1; i<=n; i++)); do
      printf -v table "t%04d" "$i"
      echo "CREATE TABLE $table(id INTEGER PRIMARY KEY, v TEXT);"
      echo "INSERT INTO $table(id,v) $(cte 1 "$rows") SELECT i, 'v_' || i FROM c;"
    done
    echo "SELECT dolt_commit('-A','-m','base');"
  } > "$file"
}

write_modify_tables_sql() {
  local file="$1"
  local first="$2"
  local last="$3"
  local schema_change="${4:-0}"
  {
    for ((i=first; i<=last; i++)); do
      printf -v table "t%04d" "$i"
      echo "UPDATE $table SET v='changed_' || id WHERE id=1;"
      if [ "$schema_change" = "1" ]; then
        echo "ALTER TABLE $table ADD COLUMN extra_$i INTEGER;"
      fi
    done
  } > "$file"
}

make_many_tables_db() {
  local db="$1"
  local n="$2"
  local rows="$3"
  local sql="$TMPDIR/many_${n}_${rows}.sql"
  write_many_tables_sql "$sql" "$n" "$rows"
  run_sql_file "$db" "$sql"
}

make_branch_db() {
  local db="$1"
  {
    echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
    echo "INSERT INTO t VALUES(1,'base');"
    echo "SELECT dolt_commit('-A','-m','base');"
    for ((i=1; i<=BRANCHES; i++)); do
      echo "SELECT dolt_branch('b$i');"
    done
  } > "$TMPDIR/branches.sql"
  run_sql_file "$db" "$TMPDIR/branches.sql"
}

make_merge_data_db() {
  local db="$1"
  run_sql "$db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(id,v) $(cte 1 "$MERGE_ROWS") SELECT i, 'base_' || i FROM c;
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_' || id WHERE id BETWEEN 1 AND $MERGE_CHANGE_ROWS;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_' || id
  WHERE id BETWEEN $((MERGE_CHANGE_ROWS + 1)) AND $((MERGE_CHANGE_ROWS * 2));
SELECT dolt_commit('-A','-m','main');"
}

make_merge_schema_db() {
  local db="$1"
  run_sql "$db" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a');
INSERT INTO b VALUES(1,'b');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_commit('-A','-m','feat schema');
SELECT dolt_checkout('main');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_commit('-A','-m','main schema');"
}

make_merge_conflict_db() {
  local db="$1"
  run_sql "$db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(id,v) $(cte 1 "$MERGE_ROWS") SELECT i, 'base_' || i FROM c;
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_' || id WHERE id BETWEEN 1 AND $MERGE_CHANGE_ROWS;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_' || id WHERE id BETWEEN 1 AND $MERGE_CHANGE_ROWS;
SELECT dolt_commit('-A','-m','main');"
}

prepare_fixtures() {
  echo "Preparing version-control benchmark fixtures..." >&2

  MANY_CLEAN="$TMPDIR/many_clean.db"
  make_many_tables_db "$MANY_CLEAN" "$TABLES" "$ROWS_PER_TABLE"

  MANY_DATA="$TMPDIR/many_data.db"
  cp "$MANY_CLEAN" "$MANY_DATA"
  write_modify_tables_sql "$TMPDIR/dirty_data.sql" 1 40 0
  run_sql_file "$MANY_DATA" "$TMPDIR/dirty_data.sql"

  MANY_SCHEMA="$TMPDIR/many_schema.db"
  cp "$MANY_CLEAN" "$MANY_SCHEMA"
  write_modify_tables_sql "$TMPDIR/dirty_schema.sql" 1 20 1
  run_sql_file "$MANY_SCHEMA" "$TMPDIR/dirty_schema.sql"

  BRANCH_DB="$TMPDIR/branches.db"
  make_branch_db "$BRANCH_DB"

  CHECKOUT_DB="$TMPDIR/checkout.db"
  make_many_tables_db "$CHECKOUT_DB" "$CHECKOUT_TABLES" "$CHECKOUT_ROWS_PER_TABLE"
  run_sql "$CHECKOUT_DB" "SELECT dolt_branch('feat');"

  MERGE_DATA_DB="$TMPDIR/merge_data.db"
  make_merge_data_db "$MERGE_DATA_DB"

  MERGE_SCHEMA_DB="$TMPDIR/merge_schema.db"
  make_merge_schema_db "$MERGE_SCHEMA_DB"

  MERGE_CONFLICT_DB="$TMPDIR/merge_conflict.db"
  make_merge_conflict_db "$MERGE_CONFLICT_DB"
}

failures=0
rows=""

bench_sql() {
  local name="$1"
  local seed="$2"
  local sql="$3"
  local ceiling="$4"
  local allow_error="${5:-0}"
  local vals=()
  local db start end elapsed out err

  for ((r=1; r<=RUNS; r++)); do
    db="$TMPDIR/${name}_${r}.db"
    cp "$seed" "$db"
    out="$TMPDIR/${name}_${r}.out"
    err="$TMPDIR/${name}_${r}.err"
    start=$(ms_now)
    if ! printf '%s\n' "$sql" | "$DOLTLITE" "$db" >"$out" 2>"$err"; then
      if [ "$allow_error" = "1" ]; then
        :
      else
        echo "Benchmark $name failed on run $r:" >&2
        cat "$err" >&2
        return 1
      fi
    fi
    end=$(ms_now)
    elapsed=$((end-start))
    vals+=("$elapsed")
  done

  local median
  median=$(median_ms "${vals[@]}")
  local status="PASS"
  if [ "$median" -gt "$ceiling" ]; then
    status="FAIL"
    failures=$((failures+1))
  fi
  rows="${rows}| $name | $median | $ceiling | $status |\n"
}

prepare_fixtures

bench_sql "status_clean_many_tables" "$MANY_CLEAN" \
  "SELECT count(*) FROM dolt_status;" 200
bench_sql "status_dirty_many_tables" "$MANY_DATA" \
  "SELECT count(*) FROM dolt_status;" 200
bench_sql "diff_regular_working_one_table" "$MANY_DATA" \
  "SELECT count(*) FROM dolt_diff_t0001 WHERE to_commit='WORKING';" 150
bench_sql "diff_regular_working_many_tables" "$MANY_DATA" \
  "SELECT count(*) FROM dolt_diff WHERE commit_hash='WORKING' AND data_change=1;" 200
bench_sql "diff_stat_working_many_tables" "$MANY_DATA" \
  "SELECT count(*), coalesce(sum(data_change),0) FROM dolt_diff WHERE commit_hash='WORKING';" 200
bench_sql "diff_schema_working_many_tables" "$MANY_SCHEMA" \
  "SELECT count(*) FROM dolt_diff WHERE commit_hash='WORKING' AND schema_change=1;" 200
bench_sql "branch_list_many_branches" "$BRANCH_DB" \
  "SELECT count(*) FROM dolt_branches;" 100
bench_sql "branch_create_delete" "$BRANCH_DB" \
  "SELECT dolt_branch('tmp_perf'); SELECT dolt_branch('-D','tmp_perf');" 100
bench_sql "checkout_branch_clean" "$CHECKOUT_DB" \
  "SELECT dolt_checkout('feat'); SELECT dolt_checkout('main');" 200
bench_sql "merge_data_no_conflicts" "$MERGE_DATA_DB" \
  "SELECT dolt_merge('feat');" 150
bench_sql "merge_schema_no_conflicts" "$MERGE_SCHEMA_DB" \
  "SELECT dolt_merge('feat');" 100
bench_sql "merge_data_conflicts" "$MERGE_CONFLICT_DB" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT count(*) FROM dolt_conflicts_t; ROLLBACK;" 250 1
bench_sql "merge_data_conflicts_with_resolve" "$MERGE_CONFLICT_DB" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT count(*) FROM dolt_conflicts; ROLLBACK;" 250 1

cat <<EOF
<!-- benchmark:vc-perf -->
## Version-Control Performance Ceilings

Runs: median of $RUNS executions per benchmark, excluding fixture setup. The
working-set fixtures use $TABLES committed tables with $ROWS_PER_TABLE rows each
($((TABLES * ROWS_PER_TABLE)) rows total); data-dirty cases update 1 row in 40
tables, and schema-dirty cases update 1 row plus add a column in 20 tables.
Branch tests use $BRANCHES branches, checkout uses a clean $CHECKOUT_TABLES-table
branch switch over $((CHECKOUT_TABLES * CHECKOUT_ROWS_PER_TABLE)) rows, and merge
tests use $MERGE_ROWS-row tables with $MERGE_CHANGE_ROWS changed or conflicting
rows per side.

| Benchmark | Median ms | Ceiling ms | Result |
|---|---:|---:|---|
$(printf "%b" "$rows")
EOF

if [ "$failures" -ne 0 ]; then
  echo "$failures version-control benchmark(s) exceeded their ceiling." >&2
  exit 1
fi
