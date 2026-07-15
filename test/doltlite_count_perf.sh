#!/bin/bash
DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

echo "=== Doltlite COUNT(*) Performance Tests ==="
echo ""

if [ ! -x "$DOLTLITE" ]; then
  echo "ERROR: $DOLTLITE not found or not executable"
  exit 1
fi

time_count_us() {
  local db="$1"
  local out
  out=$($DOLTLITE "$db" 2>&1 <<EOF
.timer ON
SELECT count(*) FROM t;
EOF
)
  local secs
  secs=$(echo "$out" | awk '/Run Time/ { for(i=1;i<=NF;i++) if($i=="real") { print $(i+1); exit } }')
  if [ -z "$secs" ]; then
    echo 0
    return
  fi
  python3 -c "print(int(float('$secs') * 1000000))"
}

build_db() {
  local size="$1"
  local db="$2"
  rm -f "$db"
  python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('BEGIN;')
for i in range($size):
    print(f'INSERT INTO t VALUES({i}, \"row_{i}\");')
print('COMMIT;')
print(\"SELECT dolt_commit('-A','-m','init');\")
" | $DOLTLITE "$db" > /dev/null 2>&1
}

assert_count() {
  local name="$1" db="$2" expected="$3"
  local got
  got=$(echo "SELECT count(*) FROM t;" | $DOLTLITE "$db" 2>&1)
  if [ "$got" = "$expected" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — count=$got"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name expected=$expected got=$got"
    echo "  FAIL: $name — expected=$expected got=$got"
  fi
}

assert_sql() {
  local name="$1" db="$2" sql="$3" expected="$4"
  local got
  got=$(echo "$sql" | $DOLTLITE "$db" 2>&1)
  if [ "$got" = "$expected" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — result=$got"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name expected=$expected got=$got"
    echo "  FAIL: $name — expected=$expected got=$got"
  fi
}

assert_bounded() {
  local name="$1" actual_us="$2" max_us="$3"
  if [ "$actual_us" -le "$max_us" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — ${actual_us}us <= ${max_us}us"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name ${actual_us}us > ${max_us}us"
    echo "  FAIL: $name — ${actual_us}us > ${max_us}us"
  fi
}

assert_ratio_bounded() {
  local name="$1" small_us="$2" large_us="$3" max_ratio="$4"
  if [ "$small_us" -le 0 ]; then small_us=1; fi
  local ratio
  ratio=$((large_us * 100 / small_us))
  local limit=$((max_ratio * 100))
  if [ "$ratio" -le "$limit" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — ${small_us}us -> ${large_us}us (${ratio}%/${limit}%)"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name ${small_us}us -> ${large_us}us (${ratio}% > ${limit}%)"
    echo "  FAIL: $name — ${small_us}us -> ${large_us}us (${ratio}%/${limit}%)"
  fi
}

DB_1K="/tmp/count_perf_1k_$$.db"
DB_10K="/tmp/count_perf_10k_$$.db"
DB_100K="/tmp/count_perf_100k_$$.db"
DB_1M="/tmp/count_perf_1m_$$.db"

echo "Building test databases..."
build_db 1000 "$DB_1K";       echo "  1K built"
build_db 10000 "$DB_10K";     echo "  10K built"
build_db 100000 "$DB_100K";   echo "  100K built"
build_db 1000000 "$DB_1M";    echo "  1M built"

echo ""
echo "--- Correctness ---"
assert_count "count_1k"    "$DB_1K"   1000
assert_count "count_10k"   "$DB_10K"  10000
assert_count "count_100k"  "$DB_100K" 100000
assert_count "count_1m"    "$DB_1M"   1000000
assert_sql "count_not_null_rowid_range" "$DB_1K" \
  "CREATE TABLE u(id INTEGER PRIMARY KEY, k INTEGER NOT NULL);
   INSERT INTO u VALUES(1,1),(3,3),(7,7),(10,10);
   SELECT count(k) FROM u WHERE id BETWEEN 2 AND 8;" 2
assert_sql "count_rowid_range_empty" "$DB_1K" \
  "SELECT count(*) FROM t WHERE id BETWEEN 2000 AND 2010;" 0
assert_sql "count_index_range_duplicates_and_affinity" "$DB_1K" \
  "CREATE TABLE v(id INTEGER PRIMARY KEY, k INTEGER NOT NULL);
   CREATE INDEX v_k_idx ON v(k);
   INSERT INTO v VALUES(1,10),(2,10),(3,15),(4,20),(5,30);
   SELECT count(k) FROM v WHERE k BETWEEN '10' AND '20';" 4
assert_sql "count_index_range_null_bound" "$DB_1K" \
  "SELECT count(k) FROM v WHERE k BETWEEN NULL AND 20;" 0
assert_sql "count_index_range_row_dependent_bounds" "$DB_1K" \
  "SELECT count(k) FROM v WHERE k BETWEEN id AND id+100;" 5
assert_sql "count_index_range_nullable_count_arg" "$DB_1K" \
  "CREATE TABLE w(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, v INTEGER);
   CREATE INDEX w_k_idx ON w(k);
   INSERT INTO w VALUES(1,10,NULL),(2,10,1),(3,15,NULL),(4,20,2);
   SELECT count(v) FROM w WHERE k BETWEEN 10 AND 20;" 2

echo ""
echo "--- Performance ---"
T_1K=$(time_count_us "$DB_1K");     echo "  1K:    ${T_1K}us"
T_10K=$(time_count_us "$DB_10K");   echo "  10K:   ${T_10K}us"
T_100K=$(time_count_us "$DB_100K"); echo "  100K:  ${T_100K}us"
T_1M=$(time_count_us "$DB_1M");     echo "  1M:    ${T_1M}us"

echo ""
echo "--- Assertions ---"
assert_bounded "count_1m_under_5ms" "$T_1M" 5000
assert_ratio_bounded "count_1k_to_1m_under_20x" "$T_1K" "$T_1M" 20

echo ""
echo "--- After INSERT and re-count ---"
echo "INSERT INTO t VALUES(99999999, 'extra');" | $DOLTLITE "$DB_1M" > /dev/null 2>&1
assert_count "count_1m_after_insert" "$DB_1M" 1000001

echo ""
echo "--- After DELETE and re-count ---"
echo "DELETE FROM t WHERE id=99999999;" | $DOLTLITE "$DB_1M" > /dev/null 2>&1
assert_count "count_1m_after_delete" "$DB_1M" 1000000

echo ""
echo "--- Round-trip across commit and reopen ---"
echo "INSERT INTO t VALUES(99999999, 'extra2');
SELECT dolt_commit('-A','-m','add row');" | $DOLTLITE "$DB_1M" > /dev/null 2>&1
assert_count "count_1m_after_commit_reopen" "$DB_1M" 1000001

rm -f "$DB_1K" "$DB_10K" "$DB_100K" "$DB_1M"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
