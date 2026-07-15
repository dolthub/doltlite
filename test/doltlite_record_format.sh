#!/bin/bash

DOLTLITE="${1:-./doltlite}"
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

run_test_contains() {
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

echo "=== Record-format hardening (P10/P11/P12) ==="
echo ""

DB=/tmp/test_recfmt_v55_$$.db; db_rm "$DB"
run_test "p10_2pow55_roundtrip" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, 36028797018963968);
SELECT n FROM t WHERE id=1;" \
  "36028797018963968" "$DB"
db_rm "$DB"

DB=/tmp/test_recfmt_v63_$$.db; db_rm "$DB"
run_test "p10_near_max_int_roundtrip" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, 9223372036854775000);
SELECT n FROM t WHERE id=1;" \
  "9223372036854775000" "$DB"
db_rm "$DB"

DB=/tmp/test_recfmt_vneg_$$.db; db_rm "$DB"
run_test "p10_negative_roundtrip" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, -9223372036854775000);
SELECT n FROM t WHERE id=1;" \
  "-9223372036854775000" "$DB"
db_rm "$DB"

DB=/tmp/test_recfmt_wide_$$.db; db_rm "$DB"
{
  printf "CREATE TABLE wide(id INTEGER PRIMARY KEY"
  for i in $(seq 1 500); do printf ", c%d INTEGER" "$i"; done
  printf ");\n"

  printf "INSERT INTO wide(id"
  for i in $(seq 1 500); do printf ", c%d" "$i"; done
  printf ") VALUES(1"
  for i in $(seq 1 500); do printf ", %d" "$i"; done
  printf ");\n"

  printf "SELECT c1, c250, c500 FROM wide WHERE id=1;\n"
} > /tmp/test_recfmt_wide_$$.sql

out=$($DOLTLITE "$DB" < /tmp/test_recfmt_wide_$$.sql 2>&1)
rm -f /tmp/test_recfmt_wide_$$.sql
if [ "$out" = "1|250|500" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: p12_500_columns_roundtrip\n  got: $out"
fi
db_rm "$DB"

DB=/tmp/test_recfmt_wide1500_$$.db; db_rm "$DB"
{
  printf "CREATE TABLE wide(id INTEGER PRIMARY KEY"
  for i in $(seq 1 1500); do printf ", c%d INTEGER" "$i"; done
  printf ");\n"

  printf "INSERT INTO wide(id, c1500) VALUES(1, 42);\n"
  printf "SELECT c1500 FROM wide WHERE id=1;\n"
} > /tmp/test_recfmt_wide1500_$$.sql

out=$($DOLTLITE "$DB" < /tmp/test_recfmt_wide1500_$$.sql 2>&1)
rm -f /tmp/test_recfmt_wide1500_$$.sql
if [ "$out" = "42" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: p12_1500_columns_roundtrip\n  got: $out"
fi
db_rm "$DB"

DB=/tmp/test_recfmt_smoke_$$.db; db_rm "$DB"
run_test "p11_normal_record_decode" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB, c REAL, d INTEGER);
INSERT INTO t VALUES(1, 'hello', x'01020304', 3.14, 42);
SELECT a, hex(b), c, d FROM t;" \
  "hello|01020304|3.14|42" "$DB"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
