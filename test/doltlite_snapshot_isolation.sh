#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $want\n  got:      $got"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Snapshot isolation contract (P6) ==="
echo ""

DB=/tmp/test_p6_repeat_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,10);" \
  | $DOLTLITE "$DB" > /dev/null 2>&1

OUTFILE=/tmp/test_p6_conn1_$$.out
(
  echo "BEGIN;"
  echo "SELECT 'first:'||v FROM t WHERE id=1;"
  sleep 2
  echo "SELECT 'second:'||v FROM t WHERE id=1;"
  echo "COMMIT;"
) | $DOLTLITE "$DB" > "$OUTFILE" 2>&1 &
PID=$!

sleep 0.3
echo "UPDATE t SET v=99 WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
wait $PID

first=$(grep '^first:' "$OUTFILE" | head -1)
second=$(grep '^second:' "$OUTFILE" | head -1)

run_test "begin_repeatable_first_select" "$first" "first:10"
run_test "begin_repeatable_second_select" "$second" "second:10"
post=$($DOLTLITE "$DB" "SELECT 'post:'||v FROM t WHERE id=1;" 2>/dev/null)
run_test "begin_repeatable_post_visible" "$post" "post:99"
rm -f "$OUTFILE"
db_rm "$DB"

DB=/tmp/test_p6_auto_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,10);" \
  | $DOLTLITE "$DB" > /dev/null 2>&1

OUTFILE=/tmp/test_p6_auto_conn1_$$.out
(
  echo "SELECT 'first:'||v FROM t WHERE id=1;"
  sleep 1
  echo "SELECT 'second:'||v FROM t WHERE id=1;"
) | $DOLTLITE "$DB" > "$OUTFILE" 2>&1 &
PID=$!

sleep 0.3
echo "UPDATE t SET v=42 WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
wait $PID

first=$(grep '^first:' "$OUTFILE" | head -1)
second=$(grep '^second:' "$OUTFILE" | head -1)
run_test "autocommit_first_select" "$first" "first:10"
run_test "autocommit_second_sees_update" "$second" "second:42"
rm -f "$OUTFILE"
db_rm "$DB"

DB=/tmp/test_p6_subq_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);" \
  | $DOLTLITE "$DB" > /dev/null 2>&1

out=$($DOLTLITE "$DB" "SELECT
  (SELECT v FROM t WHERE id=1) || ':' ||
  (SELECT v FROM t WHERE id=2) || ':' ||
  (SELECT sum(v) FROM t);" 2>&1)
run_test "compound_query_consistent" "$out" "10:20:60"
db_rm "$DB"

DB=/tmp/test_p6_rollback_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

out=$(echo "BEGIN;
SELECT v FROM t WHERE id=1;
UPDATE t SET v=99 WHERE id=1;
SELECT v FROM t WHERE id=1;
ROLLBACK;
SELECT v FROM t WHERE id=1;" | $DOLTLITE "$DB" 2>&1 | tr '\n' '|')
run_test "rollback_to_pre_update" "$out" "10|99|10|"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
