#!/bin/bash

DOLTLITE=./doltlite
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
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

run_test_match() {
  local n="$1" got="$2" pat="$3"
  if echo "$got" | grep -qE "$pat"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $pat\n  got:     $got"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA journal_mode (doltlite-format) ==="
echo ""

DB=/tmp/test_jm_dl_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

run_test "jm_dl_read" \
  "$($DOLTLITE "$DB" "PRAGMA journal_mode;" 2>&1)" "wal"

run_test "jm_dl_set_wal" \
  "$($DOLTLITE "$DB" "PRAGMA journal_mode = WAL;" 2>&1)" "wal"

# journal_mode is inapplicable to the chunk store, so a mode request is
# silently ignored and the fixed mode ("wal") is reported -- SQLite's
# convention for inapplicable journal modes, matching auto_vacuum.
for mode in DELETE TRUNCATE PERSIST MEMORY OFF; do
  out=$($DOLTLITE "$DB" "PRAGMA journal_mode = $mode;" 2>&1)
  run_test "jm_dl_set_${mode}_noop" "$out" "wal"
done

db_rm "$DB"

if [ -x "$SQLITE3" ]; then
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_jm_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  run_test "jm_stock_read" \
    "$($DOLTLITE "$DB" "PRAGMA journal_mode;" 2>&1)" "delete"

  out=$($DOLTLITE "$DB" "PRAGMA journal_mode = DELETE;" 2>&1)
  if echo "$out" | grep -qi "doltlite-format"; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: jm_stock_no_doltlite_msg_leaked\n  got: $out"
  else
    PASS=$((PASS+1))
  fi

  db_rm "$DB"
fi

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
