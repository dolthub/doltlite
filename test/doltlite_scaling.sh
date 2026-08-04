#!/bin/bash
# Scaling-curve gates: asserts RATIOS between sizes/depths rather than
# wall-clock, so runner speed cancels out. Catches superlinear regressions
# in per-commit cost vs history depth, per-op cost vs table size, and
# per-byte cost vs blob size.
#
# IMPORTANT: perf is meaningless on a DOLTLITE_PROLLY_CHECK build (it adds a
# full-tree walk to every commit). CI builds a plain binary for this suite.
set -uo pipefail

median5() {
  local values=("$@") i j t
  for (( i=1; i<5; i++ )); do
    t="${values[$i]}"
    j=$i
    while [ "$j" -gt 0 ] && [ "${values[$((j-1))]}" -gt "$t" ]; do
      values[$j]="${values[$((j-1))]}"
      j=$((j-1))
    done
    values[$j]="$t"
  done
  echo "${values[2]}"
}

if [ "${1:-}" = "--self-test" ]; then
  failures=0
  for case in \
    "1000 100 1000 1000 1000 1000" \
    "100 100 100 100 1000 1000" \
    "500 1000 100 500 750 500" \
    "3 5 4 3 2 1"; do
    read -r expected a b c d e <<< "$case"
    actual=$(median5 "$a" "$b" "$c" "$d" "$e")
    if [ "$actual" != "$expected" ]; then
      echo "FAIL: median5 $a $b $c $d $e (expected $expected, got $actual)"
      failures=$((failures+1))
    fi
  done
  if [ "$failures" -eq 0 ]; then
    echo "PASS: scaling sample policy"
  fi
  exit "$failures"
fi

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

check_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc (expected |$expected| got |$actual|)"
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

# Median-of-five tolerates two scheduler outliers in either direction.
run_ms_median5() {
  local samples=() t
  for _ in 1 2 3 4 5; do
    t=$(run_ms "$@")
    samples+=("$t")
  done
  median5 "${samples[@]}"
}

query() {
  local db="$1"; shift
  "$DOLTLITE" "$db" "$@" 2>/dev/null
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

# Seed in ONE session: per-batch reopens would each replay the growing WAL,
# turning large seeds quadratic and drowning the signal this suite gates.
# The SQL goes over stdin — at 100M rows the statement list exceeds Linux's
# 128KB single-argument limit and execve would fail before doltlite runs.
seed_rows() {
  local db="$1" total="$2" cols="$3"
  local i=1 end
  {
    while [ "$i" -le "$total" ]; do
      end=$(( i + 99999 )); [ "$end" -gt "$total" ] && end=$total
      printf 'WITH RECURSIVE c(x) AS (VALUES(%d) UNION ALL SELECT x+1 FROM c WHERE x<%d) INSERT INTO t SELECT %s FROM c;\n' "$i" "$end" "$cols"
      i=$(( end + 1 ))
    done
  } | "$DOLTLITE" "$db" > /dev/null 2>&1
}

# Per-commit cost is depth-independent now that the head re-confirm proves
# the store unchanged from the tail root record instead of replaying the
# WAL; nominal growth is ~3x from residual per-session bookkeeping. Shared
# CI hosts still spike a single 200-commit block (seen 8.0x vs a hard 6x
# cut), so the gate sits at SESSION_OP_GATE headroom and the deep sample
# is median-of-five. Ops issued through fresh sessions still pay an open-time
# WAL replay that only a user-driven gc folds away.
COMMIT_GROWTH_GATE=8
SESSION_OP_GATE=8

echo "══════════════════════════════════════"
echo "  Segment A: cost vs history depth"
echo "══════════════════════════════════════"
DBA="$TMPDIR/depth"
"$DOLTLITE" "$DBA" "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); CREATE TABLE s(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO s VALUES(1,0),(2,0),(3,0),(4,0),(5,0),(6,0);" > /dev/null 2>&1
seed_rows "$DBA" 10000 "x, 0"
"$DOLTLITE" "$DBA" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed'); SELECT dolt_branch('anchor');" > /dev/null 2>&1

# Side branches off the seed commit for merge timing, on disjoint rows so none
# of them conflict. Three per depth because a branch can only be merged once and
# one sample of a merge is mostly runner jitter.
sides_sql=""
row=0
for side in side1a side1b side1c side2a side2b side2c; do
  row=$((row + 1))
  sides_sql="$sides_sql SELECT dolt_branch('$side'); SELECT dolt_checkout('$side'); UPDATE s SET v=1 WHERE id=$row; SELECT dolt_commit('-am','$side'); SELECT dolt_checkout('main');"
done
"$DOLTLITE" "$DBA" "$sides_sql" > /dev/null 2>&1

median3() {
  printf '%s\n%s\n%s\n' "$1" "$2" "$3" | sort -n | awk 'NR==2{print; exit}'
}

depth_ops() {
  # echoes "log_ms checkout_ms merge_ms" for the current depth
  local prefix="$1"
  local lg co m1 m2 m3 mg
  lg=$(run_ms_median5 "$DBA" "SELECT count(*) FROM dolt_log;")
  # Both checkouts in one session per sample. run_ms starts a fresh session on
  # whatever branch the db is left on, so measuring them separately made every
  # sample after the first a no-op: cheap and stable, but no longer a checkout.
  co=$(run_ms_median5 "$DBA" \
       "SELECT dolt_checkout('anchor'); SELECT dolt_checkout('main');")
  # One branch per sample, since merging the same one twice is a no-op.
  m1=$(run_ms "$DBA" "SELECT dolt_merge('${prefix}a');")
  m2=$(run_ms "$DBA" "SELECT dolt_merge('${prefix}b');")
  m3=$(run_ms "$DBA" "SELECT dolt_merge('${prefix}c');")
  mg=$(median3 "$m1" "$m2" "$m3")
  echo "$lg $co $mg"
}

BLOCK=200
block1=$(commit_block "$DBA" 0 "$BLOCK")
read -r log1 co1 mg1 <<< "$(depth_ops side1)"
echo "  depth 200:   ${block1}ms/block ($((block1 / BLOCK))ms/commit) log=${log1}ms checkout=${co1}ms merge=${mg1}ms"
for b in 1 2 3 4; do
  commit_block "$DBA" $(( b * BLOCK )) "$BLOCK" > /dev/null
done
# Median of five deep blocks tolerates two scheduler spikes without allowing
# two fast outliers to hide three slow samples.
deep1=$(commit_block "$DBA" $(( 5 * BLOCK )) "$BLOCK")
deep2=$(commit_block "$DBA" $(( 6 * BLOCK )) "$BLOCK")
deep3=$(commit_block "$DBA" $(( 7 * BLOCK )) "$BLOCK")
deep4=$(commit_block "$DBA" $(( 8 * BLOCK )) "$BLOCK")
deep5=$(commit_block "$DBA" $(( 9 * BLOCK )) "$BLOCK")
deep=$(median5 "$deep1" "$deep2" "$deep3" "$deep4" "$deep5")
read -r log_deep co_deep mg_deep <<< "$(depth_ops side2)"
echo "  depth ~2000: ${deep}ms/block ($((deep / BLOCK))ms/commit) log=${log_deep}ms checkout=${co_deep}ms merge=${mg_deep}ms [samples ${deep1}ms, ${deep2}ms, ${deep3}ms, ${deep4}ms, ${deep5}ms; median ${deep}ms]"

check_ratio "per-commit growth, depth ~2000 vs 200" "$deep" "$block1" "$COMMIT_GROWTH_GATE"
check_ratio "dolt_log full walk, depth ~2000 vs 200" "$log_deep" "$log1" "$SESSION_OP_GATE"
check_ratio "checkout old commit, depth ~2000 vs 200" "$co_deep" "$co1" "$SESSION_OP_GATE"
check_ratio "merge across divergence, depth ~2000 vs 200" "$mg_deep" "$mg1" "$SESSION_OP_GATE"
check_eq "merged rows visible" "6|1" "$(query "$DBA" "SELECT count(*), max(v) FROM s WHERE v=1;")"

gc_ms=$(run_ms "$DBA" "SELECT dolt_gc();")
echo "  dolt_gc: ${gc_ms}ms"
# Median of five tolerates two cold samples without accepting two fast outliers.
postgc1=$(commit_block "$DBA" 2000 50)
postgc2=$(commit_block "$DBA" 2050 50)
postgc3=$(commit_block "$DBA" 2100 50)
postgc4=$(commit_block "$DBA" 2150 50)
postgc5=$(commit_block "$DBA" 2200 50)
postgc=$(median5 "$postgc1" "$postgc2" "$postgc3" "$postgc4" "$postgc5")
postgc_scaled=$(( postgc * BLOCK / 50 ))
echo "  post-gc:     ${postgc}ms for 50 ($((postgc / 50))ms/commit) [samples ${postgc1}ms, ${postgc2}ms, ${postgc3}ms, ${postgc4}ms, ${postgc5}ms; median ${postgc}ms]"
# Align with COMMIT_GROWTH_GATE headroom rather than a brittle 3x wall-clock cut.
check_ratio "post-gc commit cost vs shallow-history cost" "$postgc_scaled" "$block1" "$COMMIT_GROWTH_GATE"
# every one of the 2250 single-row commits must have landed exactly once
# (2000 depth build + five 50-commit post-gc samples)
check_eq "commit increments all applied" "2250" "$(query "$DBA" "SELECT sum(v) FROM t;")"

echo ""
echo "══════════════════════════════════════"
echo "  Segment B: per-op cost vs table size (post-gc)"
echo "══════════════════════════════════════"
declare -a SIZES=(100000 1000000 10000000 100000000)
declare -a T_OPEN T_LOOKUP T_COMMIT T_SCAN T_GC T_DIFF
for idx in 0 1 2 3; do
  n=${SIZES[$idx]}
  db="$TMPDIR/size$n"
  "$DOLTLITE" "$db" "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER, pad TEXT);" > /dev/null 2>&1
  seed_rows "$db" "$n" "x, 'row_'||x, x%1000, printf('%032d', x)"
  "$DOLTLITE" "$db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');" > /dev/null 2>&1
  T_GC[$idx]=$(run_ms "$db" "SELECT dolt_gc();")

  T_OPEN[$idx]=$(run_ms_median5 "$db" "SELECT 1;")

  lookups=""
  for (( k=0; k<400; k++ )); do
    lookups+="SELECT val FROM t WHERE id=$(( (k * 997) % n + 1 ));"
  done
  T_LOOKUP[$idx]=$(run_ms_median5 "$db" "$lookups")

  lo=$(( n / 2 ))
  T_COMMIT[$idx]=$(run_ms_median5 "$db" \
    "UPDATE t SET val=val+1 WHERE id BETWEEN $lo AND $(( lo + 499 )); SELECT dolt_commit('-am','delta');")

  T_SCAN[$idx]=$(run_ms_median5 "$db" "SELECT count(*) FROM t WHERE val >= 0;")

  # Correctness at scale, one full pass: row count, key-set integrity
  # (sum of ids has a closed form), value integrity (seed val=id%1000 plus
  # the five median-of-5 delta commits above), and per-row content (name and
  # pad are pure functions of id, so any lost/duplicated/corrupted row or
  # chunk-boundary bug shows up).
  sum_id=$(( n * (n + 1) / 2 ))
  sum_val=$(( n / 1000 * 499500 + 2500 ))
  check_eq "content verify at N=$n" "$n|$sum_id|$sum_val|0" \
    "$(query "$db" "SELECT count(*)||'|'||sum(id)||'|'||sum(val)||'|'||sum(name <> 'row_'||id OR pad <> printf('%032d',id)) FROM t;")"

  # History read-back: the newest commit's diff must be exactly the 500
  # modified rows, with old values one increment behind new values.
  head_hash=$(query "$db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
  t0=$(ms_now)
  diff_out=$(query "$db" "SELECT count(*) FROM dolt_diff_t WHERE to_commit='$head_hash' AND diff_type='modified' AND to_val=from_val+1;")
  T_DIFF[$idx]=$(( $(ms_now) - t0 ))
  check_eq "history diff of newest commit at N=$n" "500" "$diff_out"

  # branch isolation: a delete branch sees its deletion; main does not
  "$DOLTLITE" "$db" "SELECT dolt_checkout('-b','wipe'); DELETE FROM t WHERE id<=1000; SELECT dolt_commit('-am','wipe'); SELECT dolt_checkout('main');" > /dev/null 2>&1
  wipe_seen=$(query "$db" "SELECT dolt_checkout('wipe'); SELECT count(*) FROM t WHERE id<=1000;" | tail -1)
  main_seen=$(query "$db" "SELECT dolt_checkout('main'); SELECT count(*) FROM t WHERE id<=1000;" | tail -1)
  check_eq "branch isolation at N=$n" "0|1000" "$wipe_seen|$main_seen"

  size_bytes=$(wc -c < "$db" | tr -d ' ')
  echo "  N=$n: open=${T_OPEN[$idx]}ms lookups400=${T_LOOKUP[$idx]}ms upd500+commit=${T_COMMIT[$idx]}ms scan=${T_SCAN[$idx]}ms gc=${T_GC[$idx]}ms diff=${T_DIFF[$idx]}ms file=$(( size_bytes / 1000000 ))MB"
  if [ "$n" -ge 1000000 ]; then
    check_max "structural sharing: bytes/row at N=$n" $(( size_bytes / n )) 150 "B"
  fi
  rm -f "$db"
done

# 10x more rows per step; point ops should be ~O(log n), scans/gc ~O(n).
# The diff-vtab commit filter currently enumerates ~O(n); the gate keeps it
# from getting worse and tightens if pushdown improves.
for step in 1 2 3; do
  lo=${SIZES[$((step-1))]}; hi=${SIZES[$step]}
  check_ratio "open cost, ${hi} vs ${lo} rows" "${T_OPEN[$step]}" "${T_OPEN[$((step-1))]}" 8
  check_ratio "400 point lookups, ${hi} vs ${lo} rows" "${T_LOOKUP[$step]}" "${T_LOOKUP[$((step-1))]}" 8
  check_ratio "500-row commit, ${hi} vs ${lo} rows" "${T_COMMIT[$step]}" "${T_COMMIT[$((step-1))]}" 8
  check_ratio "full scan, ${hi} vs ${lo} rows" "${T_SCAN[$step]}" "${T_SCAN[$((step-1))]}" 20
  check_ratio "dolt_gc, ${hi} vs ${lo} rows" "${T_GC[$step]}" "${T_GC[$((step-1))]}" 20
  check_ratio "newest-commit diff, ${hi} vs ${lo} rows" "${T_DIFF[$step]}" "${T_DIFF[$((step-1))]}" 20
done

echo ""
echo "══════════════════════════════════════"
echo "  Segment C: cost vs blob size"
echo "══════════════════════════════════════"
DBC="$TMPDIR/blob"
"$DOLTLITE" "$DBC" "CREATE TABLE b(id INTEGER PRIMARY KEY, data BLOB); SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');" > /dev/null 2>&1
b_small=$(run_ms "$DBC" "INSERT INTO b VALUES(1, randomblob(1048576)); SELECT dolt_commit('-am','small');")
b_big=$(run_ms "$DBC" "INSERT INTO b VALUES(2, randomblob(16777216)); SELECT dolt_commit('-am','big');")
r_small=$(run_ms_median5 "$DBC" "SELECT length(data) FROM b WHERE id=1;")
r_big=$(run_ms_median5 "$DBC" "SELECT length(data) FROM b WHERE id=2;")
echo "  insert+commit: 1MB=${b_small}ms 16MB=${b_big}ms; read: 1MB=${r_small}ms 16MB=${r_big}ms"
check_eq "blob roundtrip lengths" "1048576|16777216" "$(query "$DBC" "SELECT group_concat(length(data),'|') FROM b ORDER BY id;")"
# 16x the bytes; ~linear expected, gate at 3x headroom over linear
check_ratio "blob insert+commit, 16MB vs 1MB" "$b_big" "$b_small" 48
check_ratio "blob readback, 16MB vs 1MB" "$r_big" "$r_small" 48

echo ""
echo ""
echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
