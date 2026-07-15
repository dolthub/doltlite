#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
PASS=0; FAIL=0; ERRORS=""

run_test_match() {
  local n="$1" got="$2" pat="$3"
  if echo "$got" | grep -qE "$pat"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $pat\n  got:     $got"
  fi
}

run_test_eq() {
  local n="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $want\n  got:      $got"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA wal_checkpoint (doltlite-format) ==="
echo ""

DB=/tmp/test_wc_dl_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

run_test_eq "wc_dl_default" \
  "$($DOLTLITE "$DB" "PRAGMA wal_checkpoint;" 2>&1)" "0|0|0"

run_test_eq "wc_dl_passive_explicit" \
  "$($DOLTLITE "$DB" "PRAGMA wal_checkpoint(PASSIVE);" 2>&1)" "0|0|0"

for mode in FULL RESTART TRUNCATE; do
  out=$($DOLTLITE "$DB" "PRAGMA wal_checkpoint($mode);" 2>&1)
  run_test_match "wc_dl_${mode}_rejected" "$out" \
    "wal_checkpoint mode is not configurable on doltlite-format"
done

db_rm "$DB"

if [ -x "$SQLITE3" ]; then
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_wc_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  out=$($DOLTLITE "$DB" "PRAGMA wal_checkpoint(FULL);" 2>&1)
  if echo "$out" | grep -qi "doltlite-format"; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: wc_stock_no_doltlite_msg_leaked\n  got: $out"
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
