#!/bin/bash
#
# Regression tests for WITHOUT ROWID mutation edge cases.
# These intentionally assert Doltlite behavior directly instead of using
# the SQLite oracle, because some covered cases differ from stock SQLite.
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

echo "=== WITHOUT ROWID Regression Tests ==="
echo ""

DB325=/tmp/test_wr_regression_325_$$.db
rm -f "$DB325"
run_test "delete_in_index_scan" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
DELETE FROM t WHERE val IN (10, 50);
SELECT val FROM t ORDER BY val;" \
"20
30
40" "$DB325"

DB326=/tmp/test_wr_regression_326_$$.db
rm -f "$DB326"
run_test "savepoint_sequence_release" \
"CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SAVEPOINT sp;
UPDATE t SET v = 'x' WHERE k = 2;
DELETE FROM t WHERE k = 3;
INSERT INTO t VALUES(4,'d');
RELEASE sp;
SELECT * FROM t ORDER BY k;" \
"1|a
2|x
4|d" "$DB326"

DB327=/tmp/test_wr_regression_327_$$.db
rm -f "$DB327"
run_test "multi_delete_secondary_indexes" \
"CREATE TABLE t(k INT PRIMARY KEY, a INT, b TEXT, c REAL) WITHOUT ROWID;
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES(1,10,'x',1.1),(2,20,'y',2.2),(3,30,'z',3.3),(4,40,'w',4.4);
DELETE FROM t WHERE k IN (2, 4);
SELECT * FROM t ORDER BY k;" \
"1|10|x|1.1
3|30|z|3.3" "$DB327"

rm -f "$DB325" "$DB326" "$DB327"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
echo "All tests passed!"
