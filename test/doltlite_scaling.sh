#!/bin/bash
# Scaling-curve gates: asserts RATIOS between sizes/depths rather than
# wall-clock, so runner speed cancels out. Catches superlinear regressions
# in per-commit cost vs history depth and per-op cost vs table size.
#
# IMPORTANT: perf is meaningless on a DOLTLITE_PROLLY_CHECK build (it adds a
# full-tree walk to every commit). CI builds a plain binary for this suite.
set -uo pipefail

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found: $DOLTLITE" >&2
  exit 1
fi
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

pass=0
fail=0

check_ratio() {
  local desc="$1" num="$2" den="$3" max="$4"
  # floor the denominator at 1ms so a fast run can't divide by ~zero
  local den_floored=$(( den > 0 ? den : 1 ))
  local ratio=$(( (num * 10) / den_floored ))
  if [ "$ratio" -le $(( max * 10 )) ]; then
    echo "  PASS: $desc (${num}ms / ${den_floored}ms = $((ratio/10)).$((ratio%10))x <= ${max}x)"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc (${num}ms / ${den_floored}ms = $((ratio/10)).$((ratio%10))x > ${max}x)"
    fail=$((fail+1))
  fi
}

check_max() {
  local desc="$1" val="$2" max="$3" unit="$4"
  if [ "$val" -le "$max" ]; then
    echo "  PASS: $desc (${val}${unit} <= ${max}${unit})"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc (${val}${unit} > ${max}${unit})"
    fail=$((fail+1))
  fi
}

ms_now() {
  python3 -c 'import time; print(int(time.time()*1000))'
}

# run_ms <db> <sql...>: run statements in one session, echo elapsed ms
run_ms() {
  local db="$1"; shift
  local t0 t1
  t0=$(ms_now)
  "$DOLTLITE" "$db" "$@" > /dev/null 2>&1
  t1=$(ms_now)
  echo $(( t1 - t0 ))
}

# min-of-3 timing to shed scheduler noise
run_ms_min3() {
  local best=999999999 t
  for _ in 1 2 3; do
    t=$(run_ms "$@")
    if [ "$t" -lt "$best" ]; then best=$t; fi
  done
  echo "$best"
}

commit_block() {
  local db="$1" start="$2" count="$3"
  local stmts="" k
  for (( k=0; k<count; k++ )); do
    stmts+="UPDATE t SET v=v+1 WHERE id=$(( (start + k) % 10000 + 1 ));"
    stmts+="SELECT dolt_commit('-am','c$((start + k))');"
  done
  run_ms "$db" "$stmts"
}

seed_rows() {
  local db="$1" total="$2" cols="$3"
  local i=1 end
  while [ "$i" -le "$total" ]; do
    end=$(( i + 99999 )); [ "$end" -gt "$total" ] && end=$total
    "$DOLTLITE" "$db" "WITH RECURSIVE c(x) AS (VALUES($i) UNION ALL SELECT x+1 FROM c WHERE x<$end) INSERT INTO t SELECT $cols FROM c;" > /dev/null 2>&1
    i=$(( end + 1 ))
  done
}

echo "══════════════════════════════════════"
echo "  Segment A: per-commit cost vs history depth"
echo "══════════════════════════════════════"
DBA="$TMPDIR/depth"
"$DOLTLITE" "$DBA" "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);" > /dev/null 2>&1
seed_rows "$DBA" 10000 "x, 0"
"$DOLTLITE" "$DBA" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');" > /dev/null 2>&1

BLOCK=200
block1=$(commit_block "$DBA" 0 "$BLOCK")
echo "  depth 1-200:      ${block1}ms ($((block1 / BLOCK))ms/commit)"
for b in 1 2 3 4; do
  commit_block "$DBA" $(( b * BLOCK )) "$BLOCK" > /dev/null
done
block6=$(commit_block "$DBA" $(( 5 * BLOCK )) "$BLOCK")
echo "  depth 1001-1200:  ${block6}ms ($((block6 / BLOCK))ms/commit)"

# Per-commit cost is currently O(WAL since gc), so this ratio runs ~6-11x
# depending on the machine's fsync-vs-replay balance. The gate is a backstop
# against anything worse; tighten to ~3x once the incremental-refresh fix
# lands.
check_ratio "per-commit growth over 1000 commits" "$block6" "$block1" 20

gc_ms=$(run_ms "$DBA" "SELECT dolt_gc();")
echo "  dolt_gc: ${gc_ms}ms"
postgc=$(commit_block "$DBA" 1200 50)
postgc_scaled=$(( postgc * BLOCK / 50 ))
echo "  post-gc:          ${postgc}ms for 50 ($((postgc / 50))ms/commit)"
check_ratio "post-gc commit cost vs shallow-history cost" "$postgc_scaled" "$block1" 3

echo ""
echo "══════════════════════════════════════"
echo "  Segment B: per-op cost vs table size (post-gc)"
echo "══════════════════════════════════════"
declare -a SIZES=(100000 1000000)
declare -a T_OPEN T_LOOKUP T_COMMIT
for idx in 0 1; do
  n=${SIZES[$idx]}
  db="$TMPDIR/size$n"
  "$DOLTLITE" "$db" "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER, pad TEXT);" > /dev/null 2>&1
  seed_rows "$db" "$n" "x, 'row_'||x, x%1000, printf('%032d', x)"
  "$DOLTLITE" "$db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed'); SELECT dolt_gc();" > /dev/null 2>&1

  T_OPEN[$idx]=$(run_ms_min3 "$db" "SELECT 1;")

  lookups=""
  for (( k=0; k<400; k++ )); do
    lookups+="SELECT val FROM t WHERE id=$(( (k * 997) % n + 1 ));"
  done
  T_LOOKUP[$idx]=$(run_ms_min3 "$db" "$lookups")

  lo=$(( n / 2 ))
  T_COMMIT[$idx]=$(run_ms_min3 "$db" \
    "UPDATE t SET val=val+1 WHERE id BETWEEN $lo AND $(( lo + 499 )); SELECT dolt_commit('-am','delta');")

  size_bytes=$(wc -c < "$db" | tr -d ' ')
  echo "  N=$n: open=${T_OPEN[$idx]}ms lookups400=${T_LOOKUP[$idx]}ms upd500+commit=${T_COMMIT[$idx]}ms file=$(( size_bytes / 1000000 ))MB"
  if [ "$n" = "1000000" ]; then
    check_max "structural sharing: bytes/row at 1M" $(( size_bytes / n )) 150 "B"
  fi
done

# 10x more rows; ops should be ~O(log n) once the store is compacted
check_ratio "open cost, 1M vs 100k rows" "${T_OPEN[1]}" "${T_OPEN[0]}" 8
check_ratio "400 point lookups, 1M vs 100k rows" "${T_LOOKUP[1]}" "${T_LOOKUP[0]}" 8
check_ratio "500-row commit, 1M vs 100k rows" "${T_COMMIT[1]}" "${T_COMMIT[0]}" 8

echo ""
echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
