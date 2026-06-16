#!/bin/bash

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== ARM-correctness (S11 + S12) ==="
echo ""

DB=/tmp/test_arm_gc_$$.db; db_rm "$DB"

{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  for i in $(seq 1 50); do
    echo "INSERT INTO t VALUES($i, 'row_$i');"
    echo "SELECT dolt_commit('-A','-m','c$i');"
  done
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "s11_gc_after_reopen_no_crash" \
  "SELECT dolt_gc();" \
  "chunks removed" "$DB"

run_test "s11_data_intact_after_gc" \
  "SELECT count(*) FROM t;" \
  "50" "$DB"

run_test "s11_history_intact_after_gc" \
  "SELECT count(*) FROM dolt_log;" \
  "51" "$DB"

run_test_match "s11_gc_again_after_reopen" \
  "SELECT dolt_gc();" \
  "chunks removed" "$DB"

run_test "s11_data_still_intact" \
  "SELECT v FROM t WHERE id=25;" \
  "row_25" "$DB"

db_rm "$DB"

DB=/tmp/test_arm_churn_$$.db; db_rm "$DB"

{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  printf "INSERT INTO t VALUES "
  for i in $(seq 1 5000); do
    if [ $i -gt 1 ]; then printf ", "; fi
    printf "(%d, 'row_%d_with_some_payload_to_make_leaves_bigger')" "$i" "$i"
  done
  echo ";"
  echo "SELECT dolt_commit('-A','-m','seed');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "s12_read_under_churn" \
  "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 5000;" \
  "5000" "$DB"

run_test "s12_read_specific_values_no_corruption" \
  "SELECT v FROM t WHERE id=1
UNION ALL SELECT v FROM t WHERE id=2500
UNION ALL SELECT v FROM t WHERE id=5000;" \
  "row_1_with_some_payload_to_make_leaves_bigger
row_2500_with_some_payload_to_make_leaves_bigger
row_5000_with_some_payload_to_make_leaves_bigger" "$DB"

run_test "s12_aggregate_consistent" \
  "SELECT sum(id), count(*) FROM t;" \
  "12502500|5000" "$DB"

db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
