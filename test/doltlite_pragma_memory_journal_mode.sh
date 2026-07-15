#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

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

echo "=== PRAGMA journal_mode on :memory: ==="
echo ""

run_test_eq "mem_journal_mode_is_memory" \
  "$($DOLTLITE ":memory:" "PRAGMA journal_mode;" 2>&1)" "memory"

run_test_eq "mem_set_memory_idempotent" \
  "$($DOLTLITE ":memory:" "PRAGMA journal_mode = MEMORY;" 2>&1)" "memory"

DB=/tmp/test_mwjm_file_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1
run_test_eq "file_journal_mode_is_wal" \
  "$($DOLTLITE "$DB" "PRAGMA journal_mode;" 2>&1)" "wal"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
