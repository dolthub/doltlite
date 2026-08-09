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
VC_PERF_BASELINE="${VC_PERF_BASELINE:-}"
if [ -n "$VC_PERF_BASELINE" ] && [ ! -x "$VC_PERF_BASELINE" ]; then
  echo "baseline doltlite binary not found: $VC_PERF_BASELINE" >&2
  exit 1
fi
FIXTURE_DOLTLITE="${VC_PERF_BASELINE:-$DOLTLITE}"
VC_PERF_BASELINE_LABEL="${VC_PERF_BASELINE_LABEL:-PR base}"
VC_PERF_CANDIDATE_LABEL="${VC_PERF_CANDIDATE_LABEL:-PR candidate}"

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
RESULTS_FILE="$TMPDIR/vc_results.tsv"
SAMPLES_FILE="$TMPDIR/vc_samples.tsv"
: > "$RESULTS_FILE"
printf 'section\ttest\trun\tbaseline_us\tcandidate_us\n' > "$SAMPLES_FILE"

us_now() {
  python3 - <<'PYEOF'
import time
print(time.monotonic_ns() // 1000)
PYEOF
}

median_us() {
  python3 - "$@" <<'PYEOF'
import sys
vals = sorted(int(v) for v in sys.argv[1:])
print(vals[len(vals)//2])
PYEOF
}

run_sql() {
  local db="$1"
  local sql="$2"
  printf '%s\n' "$sql" | "$FIXTURE_DOLTLITE" "$db" >/dev/null
}

run_sql_file() {
  local db="$1"
  local file="$2"
  "$FIXTURE_DOLTLITE" "$db" < "$file" >/dev/null
}

remove_sample_db() {
  local db="$1"
  rm -f "$db" "$db-lock" "$db-wal" "$db-shm" "$db-journal"
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
  write_modify_tables_sql "$TMPDIR/dirty_data.sql" 1 $((TABLES < 40 ? TABLES : 40)) 0
  run_sql_file "$MANY_DATA" "$TMPDIR/dirty_data.sql"

  MANY_SCHEMA="$TMPDIR/many_schema.db"
  cp "$MANY_CLEAN" "$MANY_SCHEMA"
  write_modify_tables_sql "$TMPDIR/dirty_schema.sql" 1 $((TABLES < 20 ? TABLES : 20)) 1
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

time_sql() {
  local binary="$1"
  local db="$2"
  local sql="$3"
  local out="$4"
  local err="$5"
  local allow_error="$6"
  local start end rc
  start=$(us_now)
  set +e
  printf '%s\n' "$sql" | "$binary" "$db" >"$out" 2>"$err"
  rc=$?
  set -e
  end=$(us_now)
  if [ "$rc" -ne 0 ] && [ "$allow_error" != "1" ]; then
    cat "$err" >&2
    return "$rc"
  fi
  echo $((end-start))
}

bench_sql() {
  local name="$1"
  local seed="$2"
  local sql="$3"
  local ceiling="$4"
  local allow_error="${5:-0}"
  local candidate_vals=()
  local baseline_vals=()
  local candidate_db baseline_db candidate_us baseline_us out err

  for ((r=1; r<=RUNS; r++)); do
    candidate_db="$TMPDIR/${name}_candidate_${r}.db"
    cp "$seed" "$candidate_db"
    out="$TMPDIR/${name}_candidate_${r}.out"
    err="$TMPDIR/${name}_candidate_${r}.err"

    if [ -n "$VC_PERF_BASELINE" ]; then
      baseline_db="$TMPDIR/${name}_baseline_${r}.db"
      cp "$seed" "$baseline_db"
      if [ $((r % 2)) -eq 1 ]; then
        if ! baseline_us=$(time_sql "$VC_PERF_BASELINE" "$baseline_db" \
            "$sql" "$TMPDIR/${name}_baseline_${r}.out" \
            "$TMPDIR/${name}_baseline_${r}.err" "$allow_error"); then
          echo "Baseline benchmark $name failed on run $r" >&2
          return 1
        fi
        if ! candidate_us=$(time_sql "$DOLTLITE" "$candidate_db" \
            "$sql" "$out" "$err" "$allow_error"); then
          echo "Candidate benchmark $name failed on run $r" >&2
          return 1
        fi
      else
        if ! candidate_us=$(time_sql "$DOLTLITE" "$candidate_db" \
            "$sql" "$out" "$err" "$allow_error"); then
          echo "Candidate benchmark $name failed on run $r" >&2
          return 1
        fi
        if ! baseline_us=$(time_sql "$VC_PERF_BASELINE" "$baseline_db" \
            "$sql" "$TMPDIR/${name}_baseline_${r}.out" \
            "$TMPDIR/${name}_baseline_${r}.err" "$allow_error"); then
          echo "Baseline benchmark $name failed on run $r" >&2
          return 1
        fi
      fi
      baseline_vals+=("$baseline_us")
      printf 'vc\t%s\t%d\t%s\t%s\n' \
        "$name" "$r" "$baseline_us" "$candidate_us" >> "$SAMPLES_FILE"
    else
      if ! candidate_us=$(time_sql "$DOLTLITE" "$candidate_db" \
          "$sql" "$out" "$err" "$allow_error"); then
        echo "Benchmark $name failed on run $r" >&2
        return 1
      fi
      printf 'vc\t%s\t%d\t0\t%s\n' \
        "$name" "$r" "$candidate_us" >> "$SAMPLES_FILE"
    fi
    candidate_vals+=("$candidate_us")
    remove_sample_db "$candidate_db"
    rm -f "$out" "$err"
    if [ -n "$VC_PERF_BASELINE" ]; then
      remove_sample_db "$baseline_db"
      rm -f "$TMPDIR/${name}_baseline_${r}.out" \
        "$TMPDIR/${name}_baseline_${r}.err"
    fi
  done

  local candidate_median baseline_median ratio status
  candidate_median=$(median_us "${candidate_vals[@]}")
  if [ -n "$VC_PERF_BASELINE" ]; then
    baseline_median=$(median_us "${baseline_vals[@]}")
    ratio=$(python3 -c \
      "print(f'{$candidate_median/$baseline_median:.3f}')")
    rows="${rows}| $name | $(python3 -c "print(f'{$baseline_median/1000:.2f}')") | $(python3 -c "print(f'{$candidate_median/1000:.2f}')") | ${ratio}x |\n"
    printf 'vc\t%s\t%s\t%s\n' \
      "$name" "$baseline_median" "$candidate_median" >> "$RESULTS_FILE"
  else
    local eff_ceiling_us
    eff_ceiling_us=$(python3 -c "print(int($ceiling * 1000 * $IO_SCALE))")
    status="PASS"
    if [ "$candidate_median" -gt "$eff_ceiling_us" ]; then
      status="FAIL"
      failures=$((failures+1))
    fi
    rows="${rows}| $name | $(python3 -c "print(f'{$candidate_median/1000:.2f}')") | $(python3 -c "print(f'{$eff_ceiling_us/1000:.0f}')") | $status |\n"
    printf 'vc\t%s\t%d\t%s\n' \
      "$name" "$eff_ceiling_us" "$candidate_median" >> "$RESULTS_FILE"
  fi
}

# Absolute ceilings are meaningless on a runner whose disk is degraded: the
# write-path benchmarks are fsync-bound, and shared-runner fsync latency has
# been observed to swing several-fold while CPU-bound reads on the same run
# got faster. Measure fsync cost with an engine-independent probe on the
# benchmark disk and scale the ceilings when it exceeds the reference,
# leaving them exact on a healthy runner. The probe is reported so the
# reference can be retuned from nightly history; healthy GitHub runners
# measure ~120us, so the reference only engages on real degradation.
VC_PERF_IO_REF_US=${VC_PERF_IO_REF_US:-1000}
VC_PERF_IO_SCALE_MAX=${VC_PERF_IO_SCALE_MAX:-5}
IO_PROBE_US=$(python3 - "$TMPDIR" <<'PYEOF'
import os, sys, time
path = os.path.join(sys.argv[1], "io_probe.bin")

def measure():
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.pwrite(fd, b"\0" * 65536, 0)
        os.fsync(fd)
        vals = []
        for i in range(15):
            t0 = time.monotonic_ns()
            os.pwrite(fd, os.urandom(8192), (i % 8) * 8192)
            os.fsync(fd)
            vals.append((time.monotonic_ns() - t0) // 1000)
        vals.sort()
        return vals[len(vals) // 2]
    finally:
        os.close(fd)
        try:
            os.unlink(path)
        except OSError:
            pass

# The probe is an assist, not a gate: a transient IO error must not fail
# the benchmark, so retry briefly and fall back to unscaled ceilings (0).
result = 0
for attempt in range(3):
    try:
        result = measure()
        break
    except OSError:
        time.sleep(0.2)
print(result)
PYEOF
)
if [ "$IO_PROBE_US" -gt 0 ]; then
  IO_SCALE=$(python3 -c "print(f'{min(max(1.0, $IO_PROBE_US/$VC_PERF_IO_REF_US), $VC_PERF_IO_SCALE_MAX):.2f}')")
  IO_PROBE_NOTE="${IO_PROBE_US}us per 8KiB write+fsync"
else
  IO_SCALE=1.00
  IO_PROBE_NOTE="unavailable (probe IO error; ceilings unscaled)"
fi

prepare_fixtures

bench_sql "status_clean_many_tables" "$MANY_CLEAN" \
  "SELECT count(*) FROM dolt_status;" 130
bench_sql "status_dirty_many_tables" "$MANY_DATA" \
  "SELECT count(*) FROM dolt_status;" 130
bench_sql "diff_regular_working_one_table" "$MANY_DATA" \
  "SELECT count(*) FROM dolt_diff_t0001 WHERE to_commit='WORKING';" 120
bench_sql "diff_regular_working_many_tables" "$MANY_DATA" \
  "SELECT count(*) FROM dolt_diff WHERE commit_hash='WORKING' AND data_change=1;" 140
bench_sql "diff_stat_working_many_tables" "$MANY_DATA" \
  "SELECT count(*), coalesce(sum(data_change),0) FROM dolt_diff WHERE commit_hash='WORKING';" 140
bench_sql "diff_schema_working_many_tables" "$MANY_SCHEMA" \
  "SELECT count(*) FROM dolt_diff WHERE commit_hash='WORKING' AND schema_change=1;" 140
bench_sql "branch_list_many_branches" "$BRANCH_DB" \
  "SELECT count(*) FROM dolt_branches;" 35
bench_sql "branch_create_delete" "$BRANCH_DB" \
  "SELECT dolt_branch('tmp_perf'); SELECT dolt_branch('-D','tmp_perf');" 40
bench_sql "checkout_branch_clean" "$CHECKOUT_DB" \
  "SELECT dolt_checkout('feat'); SELECT dolt_checkout('main');" 150
bench_sql "merge_data_no_conflicts" "$MERGE_DATA_DB" \
  "SELECT dolt_merge('feat');" 50
bench_sql "merge_schema_no_conflicts" "$MERGE_SCHEMA_DB" \
  "SELECT dolt_merge('feat');" 35
bench_sql "merge_data_conflicts" "$MERGE_CONFLICT_DB" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT count(*) FROM dolt_conflicts_t; ROLLBACK;" 180 1
bench_sql "merge_data_conflicts_with_resolve" "$MERGE_CONFLICT_DB" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT count(*) FROM dolt_conflicts; ROLLBACK;" 180 1

if [ -n "${VC_PERF_RESULTS_OUTPUT:-}" ]; then
  mkdir -p "$(dirname "$VC_PERF_RESULTS_OUTPUT")"
  cp "$RESULTS_FILE" "$VC_PERF_RESULTS_OUTPUT"
fi
if [ -n "${VC_PERF_SAMPLES_OUTPUT:-}" ]; then
  mkdir -p "$(dirname "$VC_PERF_SAMPLES_OUTPUT")"
  cp "$SAMPLES_FILE" "$VC_PERF_SAMPLES_OUTPUT"
fi

if [ -n "$VC_PERF_BASELINE" ]; then
  cat <<EOF
<!-- benchmark:vc-perf -->
## Version-Control Performance: $VC_PERF_CANDIDATE_LABEL vs $VC_PERF_BASELINE_LABEL

Runs: median of $RUNS paired executions per benchmark, excluding fixture setup.
Execution order alternates between baseline and candidate on each repetition.
IO probe: ${IO_PROBE_NOTE}.

| Benchmark | $VC_PERF_BASELINE_LABEL median ms | $VC_PERF_CANDIDATE_LABEL median ms | Ratio |
|---|---:|---:|---:|
$(printf "%b" "$rows")
EOF
else
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

IO probe: ${IO_PROBE_NOTE} (reference ${VC_PERF_IO_REF_US}us);
ceilings scaled by ${IO_SCALE}x.

| Benchmark | Median ms | Ceiling ms | Result |
|---|---:|---:|---|
$(printf "%b" "$rows")
EOF
fi

if [ -z "$VC_PERF_BASELINE" ] && [ "$failures" -ne 0 ]; then
  echo "$failures version-control benchmark(s) exceeded their ceiling." >&2
  exit 1
fi
