#!/bin/bash
# Cover the three deep-review record-format hardening items:
#
# P10 — varint 9th byte: doltlite's dlReadVarint must accept SQLite-
#       writer-encoded values >= 2^55, where the 9th byte carries a
#       full 8 bits (not 7).
# P11 — serial types 10 and 11 are SQLite-reserved/internal. A record
#       carrying them must surface as SQLITE_CORRUPT, not silently
#       coerce to NULL.
# P12 — wide tables: SQLite's default SQLITE_MAX_COLUMN is 2000.
#       doltlite must accept tables up to that, not the prior 256.

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

# ----------------------------------------------------------------
# P10 — large integer keys / values requiring 9-byte varint
# ----------------------------------------------------------------
# Value 2^55 is the boundary where SQLite's writer flips to 9-byte
# form. Roundtripping it tests that doltlite's reader decodes the
# 9th byte at full 8-bit width.
DB=/tmp/test_recfmt_v55_$$.db; db_rm "$DB"
run_test "p10_2pow55_roundtrip" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, 36028797018963968);
SELECT n FROM t WHERE id=1;" \
  "36028797018963968" "$DB"
db_rm "$DB"

# Larger: just under 2^63.
DB=/tmp/test_recfmt_v63_$$.db; db_rm "$DB"
run_test "p10_near_max_int_roundtrip" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, 9223372036854775000);
SELECT n FROM t WHERE id=1;" \
  "9223372036854775000" "$DB"
db_rm "$DB"

# Negative values use the same 9-byte form for their unsigned wrap.
DB=/tmp/test_recfmt_vneg_$$.db; db_rm "$DB"
run_test "p10_negative_roundtrip" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, -9223372036854775000);
SELECT n FROM t WHERE id=1;" \
  "-9223372036854775000" "$DB"
db_rm "$DB"

# ----------------------------------------------------------------
# P12 — wide tables (>256 columns, up to SQLITE_MAX_COLUMN)
# ----------------------------------------------------------------
# Build a 500-column CREATE statement programmatically and round-trip.
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

# 1500 columns — still under SQLITE_MAX_COLUMN=2000.
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

# ----------------------------------------------------------------
# P11 — serial types 10/11 are not directly producible from SQL
# (SQLite's writer never emits them). We assert by SQL parity that
# normal records still decode correctly; the reject path is
# exercised by the regression C tests if a corrupted blob shows up.
# Smoke: ordinary records still work.
# ----------------------------------------------------------------
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
