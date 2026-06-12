#!/bin/bash
# P7 (auto_vacuum slice). Doltlite has no page layout, no allocator,
# and no concept of vacuum at the storage level. Stock SQLite's
# `PRAGMA auto_vacuum = FULL|INCR` and `PRAGMA incremental_vacuum`
# both presume a paged store with a free-page list. On doltlite-format
# databases these are accepted as silent no-ops, matching stock
# SQLite's behavior on a database that cannot change auto_vacuum mode:
#
#   - PRAGMA auto_vacuum;          -> reads back 0 (NONE).
#   - PRAGMA auto_vacuum = <any>;  -> silently OK, never applied;
#                                     read-back stays 0.
#   - PRAGMA incremental_vacuum;   -> silent no-op, no output rows.
#   - Data is unaffected by any of the above.
#
# Stock-SQLite-format files opened via the orig route keep the
# upstream behavior.

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

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA auto_vacuum / incremental_vacuum (doltlite-format) ==="
echo ""

# Doltlite-format database (created by doltlite).
DB=/tmp/test_av_dl_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1),(2),(3);" > /dev/null 2>&1

# Read is always supported, returns 0.
run_test "av_dl_read" \
  "$($DOLTLITE "$DB" "PRAGMA auto_vacuum;" 2>&1)" "0"

# Set to NONE: silently OK.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 0;" 2>&1)
run_test "av_dl_set_none_ok" "$out" ""

# Set to FULL: accepted as a no-op, read-back stays 0.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 1; PRAGMA auto_vacuum;" 2>&1)
run_test "av_dl_set_full_noop" "$out" "0"

# Set to INCR: accepted as a no-op, read-back stays 0.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 2; PRAGMA auto_vacuum;" 2>&1)
run_test "av_dl_set_incr_noop" "$out" "0"

# Named forms behave the same.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = FULL; PRAGMA auto_vacuum = incremental; PRAGMA auto_vacuum;" 2>&1)
run_test "av_dl_set_named_noop" "$out" "0"

# Incremental vacuum: silent no-op, no output rows.
out=$($DOLTLITE "$DB" "PRAGMA incremental_vacuum;" 2>&1)
run_test "av_dl_incr_noop" "$out" ""
out=$($DOLTLITE "$DB" "PRAGMA incremental_vacuum(4);" 2>&1)
run_test "av_dl_incr_n_noop" "$out" ""

# No-op setting never sticks across reopen and data is unaffected.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum; SELECT count(*) FROM t; PRAGMA integrity_check;" 2>&1)
run_test "av_dl_data_intact" "$out" "0
3
ok"

db_rm "$DB"

# Stock-SQLite-format database (created by stock sqlite3 binary).
# Doltlite routes this through the orig engine; the upstream
# auto_vacuum / incremental_vacuum semantics apply.
if [ -x "$SQLITE3" ]; then
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_av_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "PRAGMA auto_vacuum = 1; CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  # On a stock file, doltlite's PRAGMA path delegates to stock SQLite.
  # Reading should return 1 (since the file was created with FULL).
  run_test "av_stock_read" \
    "$($DOLTLITE "$DB" "PRAGMA auto_vacuum;" 2>&1)" "1"

  # The stock route keeps upstream set semantics: auto_vacuum can be
  # reconfigured on a stock-format file and reads back the new value.
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
