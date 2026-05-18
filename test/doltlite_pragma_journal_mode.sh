#!/bin/bash
# P7 (journal_mode slice). Doltlite has no SQLite-style journal;
# the chunk store has its own append-only WAL that's an implementation
# detail of the prolly btree. `PRAGMA journal_mode = X` used to be
# cosmetic — the value was recorded but did nothing. Reads always
# returned WAL.
#
# Contract after this PR:
#
#   - PRAGMA journal_mode;            -> reads back WAL.
#   - PRAGMA journal_mode = WAL;      -> idempotent OK (already WAL).
#   - PRAGMA journal_mode = X;        -> error for X in
#                                        {DELETE,TRUNCATE,PERSIST,MEMORY},
#                                        on doltlite-format DBs.
#   - PRAGMA journal_mode = OFF;      -> coerced to QUERY by SQLite's
#                                        defensive-mode handler before
#                                        our check; reads back WAL.
#   - Stock-SQLite-format DBs:          unchanged; upstream semantics.

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

# Idempotent: already WAL.
run_test "jm_dl_set_wal" \
  "$($DOLTLITE "$DB" "PRAGMA journal_mode = WAL;" 2>&1)" "wal"

# Real mode change attempts: error.
for mode in DELETE TRUNCATE PERSIST MEMORY; do
  out=$($DOLTLITE "$DB" "PRAGMA journal_mode = $mode;" 2>&1)
  run_test_match "jm_dl_set_${mode}_rejected" "$out" \
    "journal_mode is not configurable on doltlite-format"
done

# OFF is coerced to QUERY by SQLite's defensive-mode handling
# before the prolly check runs. Returns the current mode.
out=$($DOLTLITE "$DB" "PRAGMA journal_mode = OFF;" 2>&1)
run_test "jm_dl_set_off_defensive" "$out" "wal"

db_rm "$DB"

if [ -x "$SQLITE3" ]; then
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_jm_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  # Stock SQLite delete-mode database. Doltlite routes through
  # the orig engine; the upstream journal_mode behavior applies.
  run_test "jm_stock_read" \
    "$($DOLTLITE "$DB" "PRAGMA journal_mode;" 2>&1)" "delete"

  # Stock allows mode change via the upstream path; the doltlite
  # error must not leak into this code path.
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
