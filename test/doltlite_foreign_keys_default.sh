#!/bin/bash
set -u

DOLTLITE="${1:-./doltlite}"
PASS=0
FAIL=0
ERRORS=""

run_test() {
  local name="$1" sql="$2" expected="$3" db="$4"
  local got
  got=$(printf "%s\n" "$sql" | "$DOLTLITE" "$db" 2>&1)
  if [ "$got" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $got"
  fi
}

run_test_match() {
  local name="$1" sql="$2" pattern="$3" db="$4"
  local got
  got=$(printf "%s\n" "$sql" | "$DOLTLITE" "$db" 2>&1)
  if printf "%s\n" "$got" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern:  $pattern\n  got:      $got"
  fi
}

echo "=== Doltlite Default Foreign Key Tests ==="

DB1=/tmp/doltlite_fk_default_$$.db; rm -f "$DB1"
run_test "foreign_keys_default_on" "PRAGMA foreign_keys;" "1" "$DB1"

DB2=/tmp/doltlite_fk_pk_$$.db; rm -f "$DB2"
run_test_match "fk_pk_parent_default_rejected" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO child VALUES (1,999);
" "FOREIGN KEY constraint failed" "$DB2"
run_test "fk_pk_parent_no_rows" "SELECT count(*) FROM child;" "0" "$DB2"

DB3=/tmp/doltlite_fk_unique_$$.db; rm -f "$DB3"
run_test_match "fk_unique_parent_default_rejected" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, u INTEGER UNIQUE);
CREATE TABLE child(id INTEGER PRIMARY KEY, u INTEGER REFERENCES parent(u));
INSERT INTO child VALUES (1,999);
" "FOREIGN KEY constraint failed" "$DB3"
run_test "fk_unique_parent_no_rows" "SELECT count(*) FROM child;" "0" "$DB3"

rm -f "$DB1" "$DB2" "$DB3"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -ne 0 ]; then
  printf "%b\n" "$ERRORS"
  exit 1
fi
