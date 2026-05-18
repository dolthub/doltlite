#!/bin/bash
# P7 (final slice). PagerShim used to initialize journalMode to WAL
# unconditionally and shimPagerOpenWal promoted to WAL on every
# call. As a result, `PRAGMA journal_mode` on a `:memory:` database
# reported "wal" even though there's no WAL file (and can't be one
# for memdbs).
#
# After this PR:
#   - File-backed doltlite DBs report "wal" (unchanged).
#   - `:memory:` DBs report "memory" — matches stock SQLite.
#   - Setting journal_mode = MEMORY on a memdb is idempotent OK.
#   - Setting journal_mode = WAL on a memdb is rejected by SQLite's
#     existing OP_JournalMode WAL-transition check (empty filename
#     coerces eNew to eOld silently); the doltlite reject doesn't
#     fire because eNew ends up equal to eOld.
#
# This closes out the P7 cluster.

DOLTLITE=./doltlite
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

# :memory: reports memory, not wal.
run_test_eq "mem_journal_mode_is_memory" \
  "$($DOLTLITE ":memory:" "PRAGMA journal_mode;" 2>&1)" "memory"

# Idempotent set to current.
run_test_eq "mem_set_memory_idempotent" \
  "$($DOLTLITE ":memory:" "PRAGMA journal_mode = MEMORY;" 2>&1)" "memory"

# File-backed DB still reports wal (regression guard).
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
