#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" got="$2" want="$3"
  got=$(printf '%s' "$got" | tr -d '\r')
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $want\n  got:      $got"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA auto_vacuum / incremental_vacuum (doltlite-format) ==="
echo ""

DB=/tmp/test_av_dl_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1),(2),(3);" > /dev/null 2>&1

run_test "av_dl_read" \
  "$($DOLTLITE "$DB" "PRAGMA auto_vacuum;" 2>&1)" "0"

out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 0;" 2>&1)
run_test "av_dl_set_none_ok" "$out" ""

out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 1; PRAGMA auto_vacuum;" 2>&1)
run_test "av_dl_set_full_noop" "$out" "0"

out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 2; PRAGMA auto_vacuum;" 2>&1)
run_test "av_dl_set_incr_noop" "$out" "0"

out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = FULL; PRAGMA auto_vacuum = incremental; PRAGMA auto_vacuum;" 2>&1)
run_test "av_dl_set_named_noop" "$out" "0"

out=$($DOLTLITE "$DB" "PRAGMA incremental_vacuum;" 2>&1)
run_test "av_dl_incr_noop" "$out" ""
out=$($DOLTLITE "$DB" "PRAGMA incremental_vacuum(4);" 2>&1)
run_test "av_dl_incr_n_noop" "$out" ""

out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum; SELECT count(*) FROM t; PRAGMA integrity_check;" 2>&1)
run_test "av_dl_data_intact" "$out" "0
3
ok"

db_rm "$DB"

if [ -x "$SQLITE3" ]; then
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_av_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "PRAGMA auto_vacuum = 1; CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  run_test "av_stock_read" \
    "$($DOLTLITE "$DB" "PRAGMA auto_vacuum;" 2>&1)" "1"

  out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 2; PRAGMA auto_vacuum;" 2>&1)
  run_test "av_stock_set_applies" "$out" "2"

  db_rm "$DB"
fi

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
