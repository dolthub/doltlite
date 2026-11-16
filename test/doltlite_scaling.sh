#!/bin/bash
# Ratio gates (not wall-clock). Meaningless on DOLTLITE_PROLLY_CHECK (full-tree walk per commit).
set -euo pipefail

median_n() {
  printf '%s\n' "$@" | sort -n | awk -v n="$#" 'NR==int((n+1)/2){print; exit}'
}

SAMPLES=7

ms_now() {
  python3 -c 'import time; print(int(time.time()*1000))'
}

# Gates compare engine counters, not time: wall-clock on a hosted runner
# follows its CPU model and its throttled disk, while the counters are exact.
# "nodes" is tree nodes fetched plus chunks read and written; "bytes" is
# bytes verified on read plus bytes hashed on write.
WORK_SQL="SELECT 'scale-work '
  ||sum(CASE WHEN name IN ('cache_miss','chunk_read','chunk_write') THEN value ELSE 0 END)||' '
  ||sum(CASE WHEN name IN ('verify_bytes','hash_bytes') THEN value ELSE 0 END)
  FROM dolt_engine_stats;"

# Prints "<wall ms> <nodes> <bytes>" for one session running the SQL.
run_counted() {
  local db="$1" sql="$2" t0 t1 out work
  t0=$(ms_now)
  out=$("$DOLTLITE" "$db" "$sql $WORK_SQL") || return $?
  t1=$(ms_now)
  work=$(printf '%s\n' "$out" | sed -n 's/^scale-work \([0-9][0-9]* [0-9][0-9]*\)$/\1/p' | tail -1)
  if [ -z "$work" ]; then
    echo "no engine counters after: ${sql:0:60}" >&2
    return 1
  fi
  echo "$(( t1 - t0 )) $work"
}

# Median of each column over "<wall> <nodes> <bytes>" samples.
median_work() {
  local walls=() nodes=() bytes=() sample w n b
  for sample in "$@"; do
    read -r w n b <<< "$sample"
    walls+=("$w"); nodes+=("$n"); bytes+=("$b")
  done
  echo "$(median_n "${walls[@]}") $(median_n "${nodes[@]}") $(median_n "${bytes[@]}")"
}

# Odd SAMPLES: median drops (SAMPLES-1)/2 outliers each side.
run_counted_median() {
  local samples=() t i
  for (( i=0; i<SAMPLES; i++ )); do
    t=$(run_counted "$@") || return $?
    samples+=("$t")
  done
  median_work "${samples[@]}"
}

wall_of() { echo "${1%% *}"; }

if [ "${1:-}" = "--self-test" ]; then
  failures=0
  for case in \
    "1000 100 1000 1000 1000 1000 1000 1000" \
    "100 100 100 100 100 1000 1000 1000" \
    "500 1000 100 500 400 750 500 600" \
    "4 7 1 5 3 2 6 4"; do
    read -r expected rest <<< "$case"
    # shellcheck disable=SC2086
    actual=$(median_n $rest)
    if [ "$actual" != "$expected" ]; then
      echo "FAIL: median_n $rest (expected $expected, got $actual)"
      failures=$((failures+1))
    fi
  done
  scaling_failure() { echo "injected database failure" >&2; return 17; }
  DOLTLITE=scaling_failure
  for timer in run_counted run_counted_median; do
    if actual=$("$timer" :memory: "SELECT 1;" 2>&1); then
      rc=0
    else
      rc=$?
    fi
    if [ "$rc" -ne 17 ] || [ "$actual" != "injected database failure" ]; then
      echo "FAIL: $timer concealed a database error ($rc: $actual)"
      failures=$((failures+1))
    fi
  done
  python3 "$(dirname "$0")/doltlite_scaling_test.py" "$0" || failures=$((failures+1))
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
  local desc="$1" num="$2" den="$3" max="$4" unit="${5-ms}"
  # floor den at 1ms
  local den_floored=$(( den > 0 ? den : 1 ))
  local ratio=$(( (num * 10) / den_floored ))
  if [ "$ratio" -le $(( max * 10 )) ]; then
    echo "  PASS: $desc (${num}${unit} / ${den_floored}${unit} = $((ratio/10)).$((ratio%10))x <= ${max}x)"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc (${num}${unit} / ${den_floored}${unit} = $((ratio/10)).$((ratio%10))x > ${max}x)"
    fail=$((fail+1))
  fi
}

check_work() {
  local desc="$1" num="$2" den="$3" max="$4" nw nn nb dw dn db
  read -r nw nn nb <<< "$num"
  read -r dw dn db <<< "$den"
  check_ratio "$desc: nodes" "$nn" "$dn" "$max" ""
  check_ratio "$desc: bytes" "$nb" "$db" "$max" ""
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

query() {
  local db="$1"; shift
  "$DOLTLITE" "$db" "$@"
}

commit_block() {
  local db="$1" start="$2" count="$3"
  local stmts="" k
  for (( k=0; k<count; k++ )); do
    stmts+="UPDATE t SET v=v+1 WHERE id=$(( (start + k) % 10000 + 1 ));"
    stmts+="SELECT dolt_commit('-am','c$((start + k))');"
  done
  run_counted "$db" "$stmts"
}

# One session + stdin: reopen replays WAL (quadratic); argv hits 128KB.
seed_rows() {
  local db="$1" total="$2" cols="$3"
  local i=1 end
  {
    while [ "$i" -le "$total" ]; do
      end=$(( i + 99999 )); [ "$end" -gt "$total" ] && end=$total
      printf 'WITH RECURSIVE c(x) AS (VALUES(%d) UNION ALL SELECT x+1 FROM c WHERE x<%d) INSERT INTO t SELECT %s FROM c;\n' "$i" "$end" "$cols"
      i=$(( end + 1 ))
    done
  } | "$DOLTLITE" "$db" > /dev/null
}

# Work that must not follow history depth or table size, beyond one more tree level.
FLAT_WORK_GATE=2
# 10x the rows: that much work plus tree overhead.
LINEAR_WORK_GATE=12
# 12x the history, for what walks it: the log, and a merge whose base is the
# root commit.
DEPTH_WORK_GATE=16
BLOB_WORK_GATE=20

echo "══════════════════════════════════════"
echo "  Segment A: cost vs history depth"
echo "══════════════════════════════════════"
DBA="$TMPDIR/depth"
"$DOLTLITE" "$DBA" "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); CREATE TABLE s(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO s VALUES(1,0),(2,0),(3,0),(4,0),(5,0),(6,0);" > /dev/null
seed_rows "$DBA" 10000 "x, 0"
"$DOLTLITE" "$DBA" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed'); SELECT dolt_branch('anchor');" > /dev/null

# Disjoint side branches; three per depth (merge once, jitter).
sides_sql=""
row=0
for side in side1a side1b side1c side2a side2b side2c; do
  row=$((row + 1))
  sides_sql="$sides_sql SELECT dolt_branch('$side'); SELECT dolt_checkout('$side'); UPDATE s SET v=1 WHERE id=$row; SELECT dolt_commit('-am','$side'); SELECT dolt_checkout('main');"
done
"$DOLTLITE" "$DBA" "$sides_sql" > /dev/null

depth_ops() {
  local prefix="$1"
  local lg co m1 m2 m3
  lg=$(run_counted_median "$DBA" "SELECT count(*) FROM dolt_log;") || return $?
  # Both checkouts in one session: a leftover branch made later samples a no-op.
  co=$(run_counted_median "$DBA" \
       "SELECT dolt_checkout('anchor'); SELECT dolt_checkout('main');") || return $?
  # One branch per sample: merging twice is a no-op.
  m1=$(run_counted "$DBA" "SELECT dolt_merge('${prefix}a');") || return $?
  m2=$(run_counted "$DBA" "SELECT dolt_merge('${prefix}b');") || return $?
  m3=$(run_counted "$DBA" "SELECT dolt_merge('${prefix}c');") || return $?
  echo "$lg|$co|$(median_work "$m1" "$m2" "$m3")"
}

BLOCK=200
block1=$(commit_block "$DBA" 0 "$BLOCK")
depth_result=$(depth_ops side1)
IFS='|' read -r log1 co1 mg1 <<< "$depth_result"
echo "  depth 200:   $(wall_of "$block1")ms/block log=$(wall_of "$log1")ms checkout=$(wall_of "$co1")ms merge=$(wall_of "$mg1")ms"
for b in 1 2 3 4; do
  commit_block "$DBA" $(( b * BLOCK )) "$BLOCK" > /dev/null
done
deep_samples=()
deep_start=$(( 5 * BLOCK ))
for (( i=0; i<SAMPLES; i++ )); do
  sample=$(commit_block "$DBA" $(( deep_start + i * BLOCK )) "$BLOCK")
  deep_samples+=("$sample")
done
deep=$(median_work "${deep_samples[@]}")
depth_result=$(depth_ops side2)
IFS='|' read -r log_deep co_deep mg_deep <<< "$depth_result"
echo "  depth ~$(( deep_start + SAMPLES * BLOCK )): $(wall_of "$deep")ms/block log=$(wall_of "$log_deep")ms checkout=$(wall_of "$co_deep")ms merge=$(wall_of "$mg_deep")ms"

check_work "per-commit growth, deep vs 200" "$deep" "$block1" "$FLAT_WORK_GATE"
check_work "dolt_log full walk, deep vs 200" "$log_deep" "$log1" "$DEPTH_WORK_GATE"
check_work "checkout old commit, deep vs 200" "$co_deep" "$co1" "$FLAT_WORK_GATE"
check_work "merge across divergence, deep vs 200" "$mg_deep" "$mg1" "$DEPTH_WORK_GATE"
check_eq "merged rows visible" "6|1" "$(query "$DBA" "SELECT count(*), max(v) FROM s WHERE v=1;")"

gc_work=$(run_counted "$DBA" "SELECT dolt_gc();")
echo "  dolt_gc: $(wall_of "$gc_work")ms"
postgc_samples=()
postgc_start=$(( deep_start + SAMPLES * BLOCK ))
for (( i=0; i<SAMPLES; i++ )); do
  sample=$(commit_block "$DBA" $(( postgc_start + i * 50 )) 50)
  postgc_samples+=("$sample")
done
postgc=$(median_work "${postgc_samples[@]}")
read -r pw pn pb <<< "$postgc"
echo "  post-gc:     ${pw}ms for 50 commits"
check_work "post-gc commit cost vs shallow-history cost" \
  "$pw $(( pn * BLOCK / 50 )) $(( pb * BLOCK / 50 ))" "$block1" "$FLAT_WORK_GATE"
expect_commits=$(( BLOCK + 4 * BLOCK + SAMPLES * BLOCK + SAMPLES * 50 ))
check_eq "commit increments all applied" "$expect_commits" "$(query "$DBA" "SELECT sum(v) FROM t;")"

echo ""
echo "══════════════════════════════════════"
echo "  Segment B: per-op cost vs table size (post-gc)"
echo "══════════════════════════════════════"
declare -a SIZES=(100000 1000000 10000000 100000000)
declare -a W_OPEN W_LOOKUP W_COMMIT W_SCAN W_GC W_DIFF
for idx in 0 1 2 3; do
  n=${SIZES[$idx]}
  db="$TMPDIR/size$n"
  "$DOLTLITE" "$db" "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER, pad TEXT);" > /dev/null
  seed_rows "$db" "$n" "x, 'row_'||x, x%1000, printf('%032d', x)"
  "$DOLTLITE" "$db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');" > /dev/null
  W_GC[$idx]=$(run_counted "$db" "SELECT dolt_gc();")

  W_OPEN[$idx]=$(run_counted_median "$db" "SELECT 1;")

  lookups=""
  for (( k=0; k<400; k++ )); do
    lookups+="SELECT val FROM t WHERE id=$(( (k * 997) % n + 1 ));"
  done
  W_LOOKUP[$idx]=$(run_counted_median "$db" "$lookups")

  lo=$(( n / 2 ))
  W_COMMIT[$idx]=$(run_counted_median "$db" \
    "UPDATE t SET val=val+1 WHERE id BETWEEN $lo AND $(( lo + 499 )); SELECT dolt_commit('-am','delta');")

  W_SCAN[$idx]=$(run_counted_median "$db" "SELECT count(*) FROM t WHERE val >= 0;")

  # Closed-form id/val plus name/pad from id.
  sum_id=$(( n * (n + 1) / 2 ))
  sum_val=$(( n / 1000 * 499500 + SAMPLES * 500 ))
  check_eq "content verify at N=$n" "$n|$sum_id|$sum_val|0" \
    "$(query "$db" "SELECT count(*)||'|'||sum(id)||'|'||sum(val)||'|'||sum(name <> 'row_'||id OR pad <> printf('%032d',id)) FROM t;")"

  # Newest diff is the 500 modified rows, old=new-1.
  head_hash=$(query "$db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
  diff_out=$("$DOLTLITE" "$db" "SELECT count(*) FROM dolt_diff_t WHERE to_commit='$head_hash' AND diff_type='modified' AND to_val=from_val+1;")
  W_DIFF[$idx]=$(run_counted "$db" "SELECT count(*) FROM dolt_diff_t WHERE to_commit='$head_hash' AND diff_type='modified' AND to_val=from_val+1;")
  check_eq "history diff of newest commit at N=$n" "500" "$diff_out"

  "$DOLTLITE" "$db" "SELECT dolt_checkout('-b','wipe'); DELETE FROM t WHERE id<=1000; SELECT dolt_commit('-am','wipe'); SELECT dolt_checkout('main');" > /dev/null
  wipe_seen=$(query "$db" "SELECT dolt_checkout('wipe'); SELECT count(*) FROM t WHERE id<=1000;" | tail -1)
  main_seen=$(query "$db" "SELECT dolt_checkout('main'); SELECT count(*) FROM t WHERE id<=1000;" | tail -1)
  check_eq "branch isolation at N=$n" "0|1000" "$wipe_seen|$main_seen"

  size_bytes=$(wc -c < "$db" | tr -d ' ')
  echo "  N=$n: open=$(wall_of "${W_OPEN[$idx]}")ms lookups400=$(wall_of "${W_LOOKUP[$idx]}")ms upd500+commit=$(wall_of "${W_COMMIT[$idx]}")ms scan=$(wall_of "${W_SCAN[$idx]}")ms gc=$(wall_of "${W_GC[$idx]}")ms diff=$(wall_of "${W_DIFF[$idx]}")ms file=$(( size_bytes / 1000000 ))MB"
  if [ "$n" -ge 1000000 ]; then
    check_max "structural sharing: bytes/row at N=$n" $(( size_bytes / n )) 150 "B"
  fi
  rm -f "$db"
done

# 10x rows/step; point ~O(log n), scan/gc ~O(n), newest diff ~O(change).
for step in 1 2 3; do
  lo=${SIZES[$((step-1))]}; hi=${SIZES[$step]}
  check_work "open, ${hi} vs ${lo} rows" "${W_OPEN[$step]}" "${W_OPEN[$((step-1))]}" "$FLAT_WORK_GATE"
  check_work "400 point lookups, ${hi} vs ${lo} rows" "${W_LOOKUP[$step]}" "${W_LOOKUP[$((step-1))]}" "$FLAT_WORK_GATE"
  check_work "500-row commit, ${hi} vs ${lo} rows" "${W_COMMIT[$step]}" "${W_COMMIT[$((step-1))]}" "$FLAT_WORK_GATE"
  check_work "full scan, ${hi} vs ${lo} rows" "${W_SCAN[$step]}" "${W_SCAN[$((step-1))]}" "$LINEAR_WORK_GATE"
  check_work "dolt_gc, ${hi} vs ${lo} rows" "${W_GC[$step]}" "${W_GC[$((step-1))]}" "$LINEAR_WORK_GATE"
  check_work "newest-commit diff, ${hi} vs ${lo} rows" "${W_DIFF[$step]}" "${W_DIFF[$((step-1))]}" "$FLAT_WORK_GATE"
done

echo ""
echo "══════════════════════════════════════"
echo "  Segment C: cost vs blob size"
echo "══════════════════════════════════════"
DBC="$TMPDIR/blob"
"$DOLTLITE" "$DBC" "CREATE TABLE b(id INTEGER PRIMARY KEY, data BLOB); SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');" > /dev/null
b_small=$(run_counted "$DBC" "INSERT INTO b VALUES(1, randomblob(1048576)); SELECT dolt_commit('-am','small');")
b_big=$(run_counted "$DBC" "INSERT INTO b VALUES(2, randomblob(16777216)); SELECT dolt_commit('-am','big');")
r_small=$(run_counted_median "$DBC" "SELECT length(data) FROM b WHERE id=1;")
r_big=$(run_counted_median "$DBC" "SELECT length(data) FROM b WHERE id=2;")
echo "  insert+commit: 1MB=$(wall_of "$b_small")ms 16MB=$(wall_of "$b_big")ms; read: 1MB=$(wall_of "$r_small")ms 16MB=$(wall_of "$r_big")ms"
check_eq "blob roundtrip lengths" "1048576|16777216" "$(query "$DBC" "SELECT group_concat(length(data),'|') FROM b ORDER BY id;")"
# 16x the bytes.
check_work "blob insert+commit, 16MB vs 1MB" "$b_big" "$b_small" "$BLOB_WORK_GATE"
check_work "blob readback, 16MB vs 1MB" "$r_big" "$r_small" "$BLOB_WORK_GATE"

echo ""
echo ""
echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
