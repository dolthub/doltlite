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

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Schema-cookie prepared-statement invalidation ==="
echo ""

DB=/tmp/test_cookie_alter_$$.db; db_rm "$DB"
run_test "alter_add_column" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
ALTER TABLE t ADD COLUMN w TEXT DEFAULT 'd';
SELECT id||':'||v||':'||w FROM t;" \
  "1:a:d" "$DB"
db_rm "$DB"

DB=/tmp/test_cookie_drop_$$.db; db_rm "$DB"
out=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
DROP TABLE t;
SELECT count(*) FROM t;" | $DOLTLITE "$DB" 2>&1)
if echo "$out" | grep -q "no such table"; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: drop_table_rejected\n  got: $out"
fi
db_rm "$DB"

DB=/tmp/test_cookie_determ_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);" | $DOLTLITE "$DB" > /dev/null 2>&1

c1=$($DOLTLITE "$DB" "PRAGMA schema_version;" 2>/dev/null)
c2=$($DOLTLITE "$DB" "PRAGMA schema_version;" 2>/dev/null)
if [ -n "$c1" ] && [ "$c1" = "$c2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: cookie_deterministic_across_reopens\n  c1=$c1\n  c2=$c2"
fi
db_rm "$DB"

DB1=/tmp/test_cookie_diff1_$$.db; db_rm "$DB1"
DB2=/tmp/test_cookie_diff2_$$.db; db_rm "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);"           | $DOLTLITE "$DB1" > /dev/null 2>&1
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"   | $DOLTLITE "$DB2" > /dev/null 2>&1

c1=$($DOLTLITE "$DB1" "PRAGMA schema_version;" 2>/dev/null)
c2=$($DOLTLITE "$DB2" "PRAGMA schema_version;" 2>/dev/null)
if [ -n "$c1" ] && [ -n "$c2" ] && [ "$c1" != "$c2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: cookie_diff_for_diff_schema\n  c1=$c1\n  c2=$c2"
fi
db_rm "$DB1"; db_rm "$DB2"

DB=/tmp/test_cookie_bump_$$.db; db_rm "$DB"
out=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT 'A:' || (SELECT schema_version FROM pragma_schema_version);
ALTER TABLE t ADD COLUMN v TEXT;
SELECT 'B:' || (SELECT schema_version FROM pragma_schema_version);" | $DOLTLITE "$DB" 2>&1)
a=$(echo "$out" | grep '^A:' | sed 's/A://')
b=$(echo "$out" | grep '^B:' | sed 's/B://')
if [ -n "$a" ] && [ -n "$b" ] && [ "$a" != "$b" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: schema_change_bumps_cookie\n  a=$a\n  b=$b\n  out=$out"
fi
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
