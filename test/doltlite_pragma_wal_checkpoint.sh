#!/bin/bash
# P7 (wal_checkpoint slice). The SQLite checkpoint mode arg
# (PASSIVE / FULL / RESTART / TRUNCATE) describes blocking behavior
# and post-checkpoint WAL state. Doltlite's GC is closest to
# TRUNCATE — it blocks all readers and writers via the chunk-store
# lock, sweeps unreachable chunks, and atomically rewrites the file
# so the WAL section is compacted into the index.
#
# But aliasing TRUNCATE to GC silently is misleading: a user asking
# for PASSIVE explicitly does not want a stop-the-world operation.
# So this PR rejects every non-default mode and keeps the default
# (PASSIVE) as the existing no-op.
#
# Contract after this PR (doltlite-format DBs):
#   - PRAGMA wal_checkpoint;             -> default; current no-op,
#                                            returns "0|0|0".
#   - PRAGMA wal_checkpoint(PASSIVE);    -> same; explicit PASSIVE
#                                            is allowed because it
#                                            matches the default.
#   - PRAGMA wal_checkpoint(FULL|RESTART|TRUNCATE|NOOP)
#                                        -> ERROR: "use the default
#                                            (PASSIVE) form".
#   - Users who want a full sweep should call SELECT dolt_gc().
#
# Stock-SQLite-format DBs via the orig route keep upstream
# semantics unchanged.

DOLTLITE=./doltlite
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

# Default form is allowed.
run_test_eq "wc_dl_default" \
  "$($DOLTLITE "$DB" "PRAGMA wal_checkpoint;" 2>&1)" "0|0|0"

# Explicit PASSIVE is allowed.
run_test_eq "wc_dl_passive_explicit" \
  "$($DOLTLITE "$DB" "PRAGMA wal_checkpoint(PASSIVE);" 2>&1)" "0|0|0"

# Other modes reject.
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

  # Stock-format files reach the orig route; the doltlite check
  # does not fire. PRAGMA wal_checkpoint(FULL) should not surface
  # the doltlite-specific message.
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
