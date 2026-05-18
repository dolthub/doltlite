#!/bin/bash
# P7 (auto_vacuum slice). Doltlite has no page layout, no allocator,
# and no concept of vacuum at the storage level. Stock SQLite's
# `PRAGMA auto_vacuum = FULL|INCR` and `PRAGMA incremental_vacuum`
# both presume a paged store with a free-page list. doltlite used
# to silently no-op these, which made `auto_vacuum=1` appear to
# succeed and lulled users into expecting space reclamation that
# never happened.
#
# Contract after this PR:
#
#   - PRAGMA auto_vacuum;          -> reads back 0 (NONE) — read is
#                                     a query of the catalog-level
#                                     flag and is supported.
#   - PRAGMA auto_vacuum = 0;      -> silently OK (it's already NONE).
#   - PRAGMA auto_vacuum = 1|2;    -> ERROR with clear message naming
#                                     doltlite-format incompatibility.
#   - PRAGMA incremental_vacuum;   -> ERROR at runtime.
#
# Stock-SQLite-format files opened via the orig route keep the
# upstream behavior (the prolly-route reject doesn't fire).

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

echo "=== PRAGMA auto_vacuum / incremental_vacuum (doltlite-format) ==="
echo ""

# Doltlite-format database (created by doltlite).
DB=/tmp/test_av_dl_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

# Read is always supported, returns 0.
run_test "av_dl_read" \
  "$($DOLTLITE "$DB" "PRAGMA auto_vacuum;" 2>&1)" "0"

# Set to NONE: silently OK.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 0;" 2>&1)
run_test "av_dl_set_none_ok" "$out" ""

# Set to FULL: clear error.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 1;" 2>&1)
run_test_match "av_dl_set_full_rejected" "$out" "auto_vacuum is not supported"

# Set to INCR: clear error.
out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 2;" 2>&1)
run_test_match "av_dl_set_incr_rejected" "$out" "auto_vacuum is not supported"

# Incremental vacuum: runtime error.
out=$($DOLTLITE "$DB" "PRAGMA incremental_vacuum;" 2>&1)
run_test_match "av_dl_incr_rejected" "$out" "error|SQL logic"

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

  # Stock SQLite returns SQLITE_READONLY for changing the mode
  # of an existing page-size-fixed db. doltlite shouldn't surface
  # that as the doltlite-specific error.
  out=$($DOLTLITE "$DB" "PRAGMA auto_vacuum = 0;" 2>&1)
  if echo "$out" | grep -qi "auto_vacuum is not supported"; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: av_stock_no_doltlite_msg_leaked\n  doltlite error msg leaked into stock-format route\n  got: $out"
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
