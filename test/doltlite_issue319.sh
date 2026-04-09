#!/bin/bash
#
# Regression test for GitHub issue #319
#
# Updating rows that exist only in the current transaction's deferred
# MutMap must advance through those rows exactly once instead of looping.
#
set -euo pipefail

DOLTLITE=./doltlite
PASS=0
FAIL=0
ERRORS=""

run_test() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="$4"
  local result
  result=$(printf "%s\n" "$sql" | perl -e 'alarm(10); exec @ARGV' "$DOLTLITE" "$db" 2>&1)
  if [ "$result" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"
  fi
}

echo "=== Issue #319 Regression Tests ==="
echo ""

DB1=/tmp/test_issue319_$$.db
rm -f "$DB1"
run_test "issue319_update_rows_inserted_in_txn" \
"CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
BEGIN;
INSERT INTO t1 VALUES(1,100),(2,200);
UPDATE t1 SET val = val + 50;
SELECT id, val FROM t1 ORDER BY id;
COMMIT;" \
"1|150
2|250" "$DB1"

DB2=/tmp/test_issue319_cross_table_repro_$$.db
rm -f "$DB2"
run_test "issue319_cross_table_repro" \
"CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT, data TEXT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(ref);
BEGIN;
INSERT INTO t1 VALUES(1,100),(2,200);
INSERT INTO t2 VALUES(1,1,'a'),(2,2,'b');
UPDATE t1 SET val = val + 50;
SELECT id, val FROM t1 ORDER BY id;
SELECT id, ref, data FROM t2 ORDER BY id;
COMMIT;" \
"1|150
2|250
1|1|a
2|2|b" "$DB2"

rm -f "$DB1" "$DB2"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
echo "All tests passed!"
