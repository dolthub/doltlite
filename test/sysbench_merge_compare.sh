#!/bin/bash
#
# Merge-time benchmark: fast-merge tree-walker vs row-by-row.
#
# For each scenario, build a base table, branch, mutate both sides,
# then time the dolt_merge() call twice — once with the fast path
# (default) and once with DOLTLITE_FORCE_ROW_MERGE=1 to force the
# row-by-row path. Report the speedup factor and emit a markdown
# results table for the PR comment.
#
# Scenarios:
#   1. Non-overlapping PK ranges      (fast merge's best case)
#   2. Fully overlapping              (worst case; predicate-eligible
#                                      but every leaf diverges →
#                                      walker falls back internally)
#   3. 50% overlapping                (partial win)
#   4. Opt-out: secondary index       (predicate excludes; fast path
#                                      shouldn't even try)
#   5. Opt-out: FK CASCADE            (predicate excludes)
#
# Ceilings (worst-case checks; fast path must not regress these):
#   - Scenario 2 (full overlap):   fast ≤ 1.20× row time
#   - Scenarios 4, 5 (opt-out):    fast ≤ 1.10× row time
#
# Speedup expectations (informational, not gated — variance on shared
# CI runners is high):
#   - Scenario 1: significant speedup expected; the larger BENCH_ROWS,
#                 the more dramatic
#   - Scenario 3: smaller speedup expected
#
# Tunables via env:
#   BENCH_ROWS    rows in the base table (default 50000 locally,
#                 lower on shared CI runners)
#   BENCH_RUNS    runs per scenario per path (default 3; bench output
#                 reports the median)

set -u
DOLTLITE="${DOLTLITE:-./doltlite}"
ROWS="${BENCH_ROWS:-50000}"
RUNS="${BENCH_RUNS:-3}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Ceiling defaults; can be overridden by env for stricter local runs.
CEILING_OVERLAP="${BENCH_CEILING_OVERLAP:-1.20}"
CEILING_OPTOUT="${BENCH_CEILING_OPTOUT:-1.10}"

# fmt N microseconds with thousands separators.
fmt_us() {
  python3 -c 'import sys;print(f"{int(sys.argv[1]):,}")' "$1"
}

# Median of a list of ints, via python.
median_us() {
  python3 -c 'import sys, statistics; print(int(statistics.median(int(x) for x in sys.argv[1:])))' "$@"
}

# Run dolt_merge once on a fresh DB built from the given setup SQL.
# $1 = setup SQL (everything BEFORE dolt_merge — schema, init commit,
#       branches, mutations on both sides, dolt_checkout main)
# $2 = "fast" or "row" — controls DOLTLITE_FORCE_ROW_MERGE
# Echoes the merge time in microseconds.
time_one_merge() {
  local setup="$1" mode="$2"
  local db="$TMPDIR/bench_$$.db"; rm -f "$db"

  # Build the base + branched state without timing.
  echo "$setup" | "$DOLTLITE" "$db" > /dev/null 2>&1

  # Time just the merge.
  local env_pfx=""
  if [ "$mode" = "row" ]; then env_pfx="DOLTLITE_FORCE_ROW_MERGE=1"; fi
  local t0 t1
  t0=$(python3 -c 'import time;print(int(time.time()*1_000_000))')
  echo "SELECT dolt_merge('feat');" | env $env_pfx "$DOLTLITE" "$db" > /dev/null 2>&1
  t1=$(python3 -c 'import time;print(int(time.time()*1_000_000))')
  rm -f "$db"
  echo $((t1 - t0))
}

# Run a scenario $RUNS times in each mode, return median pair.
# Echoes "<fast_us> <row_us>".
run_scenario() {
  local setup="$1"
  local fast_times=() row_times=() i fast_med row_med
  for i in $(seq 1 "$RUNS"); do
    fast_times+=("$(time_one_merge "$setup" fast)")
    row_times+=("$(time_one_merge "$setup" row)")
  done
  fast_med=$(median_us "${fast_times[@]}")
  row_med=$(median_us "${row_times[@]}")
  echo "$fast_med $row_med"
}

# ============================================================
# Scenario builders. Each emits a setup SQL block ending with
# dolt_checkout('main') so the caller can append dolt_merge.
# ============================================================

# Non-overlapping small diffs against a large base.
#   base: R rows
#   left: updates IDs [1..K]  (K small relative to R)
#   right: updates IDs [R-K..R]
# Most of the tree is unchanged on both sides — the headline win
# case for fast merge. With BENCH_DIFF_K small relative to BENCH_ROWS,
# the boundary keys above the affected leaves stay aligned, so the
# walker can splice the unchanged interior subtrees wholesale.
build_nonoverlap() {
  local R="$1"
  local K="${BENCH_DIFF_K:-20}"
  local hi_start=$((R - K))
  cat <<EOF
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$R)
INSERT INTO t SELECT n, 'init_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_'||id WHERE id<=$K;
SELECT dolt_commit('-A','-m','main_left');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_'||id WHERE id>=$hi_start;
SELECT dolt_commit('-A','-m','feat_right');
SELECT dolt_checkout('main');
EOF
}

# Fully overlapping: both sides update every row, on different columns.
# Walker falls back internally (every leaf diverges); should be ~equal
# to row path.
build_overlap_full() {
  local R="$1"
  cat <<EOF
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w TEXT);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$R)
INSERT INTO t SELECT n, 'a_'||n, 'b_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_'||id;
SELECT dolt_commit('-A','-m','main_full');
SELECT dolt_checkout('feat');
UPDATE t SET w='feat_'||id;
SELECT dolt_commit('-A','-m','feat_full');
SELECT dolt_checkout('main');
EOF
}

# 50% overlap: left edits [1, 0.6R], right edits [0.4R, R].
build_overlap_partial() {
  local R="$1"
  local lo_end=$((R * 6 / 10))
  local hi_start=$((R * 4 / 10))
  cat <<EOF
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$R)
INSERT INTO t SELECT n, 'init_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_'||id WHERE id<=$lo_end;
SELECT dolt_commit('-A','-m','main_lo');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_'||id WHERE id>=$hi_start;
SELECT dolt_commit('-A','-m','feat_hi');
SELECT dolt_checkout('main');
EOF
}

# Opt-out: secondary index. Predicate excludes → fast path doesn't
# even try. Time should be ~equal to row path (predicate cost is
# negligible). Same diff shape as the non-overlap scenario so the
# comparison isolates predicate overhead.
build_optout_index() {
  local R="$1"
  local K="${BENCH_DIFF_K:-20}"
  local hi_start=$((R - K))
  cat <<EOF
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX i_v ON t(v);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$R)
INSERT INTO t SELECT n, 'init_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_'||id WHERE id<=$K;
SELECT dolt_commit('-A','-m','main_left');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_'||id WHERE id>=$hi_start;
SELECT dolt_commit('-A','-m','feat_right');
SELECT dolt_checkout('main');
EOF
}

# Opt-out: FK with ON DELETE CASCADE.
build_optout_fk() {
  local R="$1"
  local K="${BENCH_DIFF_K:-20}"
  local hi_start=$((R - K))
  cat <<EOF
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  v TEXT,
  FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE
);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$R)
INSERT INTO parent SELECT n FROM seq;
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$R)
INSERT INTO t SELECT n, n, 'init_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_'||id WHERE id<=$K;
SELECT dolt_commit('-A','-m','main_left');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_'||id WHERE id>=$hi_start;
SELECT dolt_commit('-A','-m','feat_right');
SELECT dolt_checkout('main');
EOF
}

# Compute speedup factor as fast/row (smaller = faster fast path).
ratio() {
  python3 -c 'import sys; r=int(sys.argv[2]); print(f"{int(sys.argv[1])/r:.2f}" if r>0 else "inf")' "$1" "$2"
}

# Format speedup as Nx (row/fast).
speedup_x() {
  python3 -c 'import sys; f=int(sys.argv[1]); print(f"{int(sys.argv[2])/f:.2f}" if f>0 else "inf")' "$1" "$2"
}

# Compare against ceiling: fast/row <= ceiling. Echo "OK" or "OVER".
check_ceiling() {
  local fast="$1" row="$2" ceiling="$3"
  python3 -c 'import sys; f,r,c=int(sys.argv[1]),int(sys.argv[2]),float(sys.argv[3]); print("OK" if r==0 or f/r<=c else "OVER")' "$fast" "$row" "$ceiling"
}

# Header
echo "## Merge Benchmark: Fast Path vs Row-by-Row"
echo "<!-- benchmark:merge -->"
echo ""
echo "Rows per scenario: \`$ROWS\`   |   Runs per cell: \`$RUNS\` (median)"
echo ""
echo "| Scenario | Fast (μs) | Row (μs) | Fast / Row | Speedup | Ceiling | Status |"
echo "|---|---:|---:|---:|---:|---:|:---:|"

bench_rc=0

scenario_row() {
  local label="$1" setup="$2" ceiling="$3"
  local times fast_med row_med ratio_v sx status
  times=$(run_scenario "$setup")
  fast_med=$(echo "$times" | awk '{print $1}')
  row_med=$(echo "$times" | awk '{print $2}')
  ratio_v=$(ratio "$fast_med" "$row_med")
  sx=$(speedup_x "$fast_med" "$row_med")
  status=$(check_ceiling "$fast_med" "$row_med" "$ceiling")
  if [ "$status" = "OVER" ]; then bench_rc=1; fi
  printf "| %s | %s | %s | %s | %sx | %s | %s |\n" \
    "$label" "$(fmt_us "$fast_med")" "$(fmt_us "$row_med")" "$ratio_v" "$sx" "$ceiling" "$status"
}

# Run the scenarios.
scenario_row "Non-overlapping PK ranges" "$(build_nonoverlap "$ROWS")"        "10.00"
scenario_row "50% overlap"               "$(build_overlap_partial "$ROWS")"   "10.00"
scenario_row "Full overlap"              "$(build_overlap_full "$ROWS")"      "$CEILING_OVERLAP"
scenario_row "Opt-out: secondary index"  "$(build_optout_index "$ROWS")"      "$CEILING_OPTOUT"
scenario_row "Opt-out: FK CASCADE"       "$(build_optout_fk "$ROWS")"         "$CEILING_OPTOUT"

echo ""
echo "_Ceilings of 10× on speedup scenarios are reporting-only; only the no-regression ceilings (full-overlap and opt-out) are gated. Variance on shared runners is too high to assert speedup floors._"

exit $bench_rc
