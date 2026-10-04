#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"
dltest_init_queries

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
PASS=0; FAIL=0; ERRORS=""



db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA journal_mode (doltlite-format) ==="
echo ""

DB=/tmp/test_jm_dl_$$.db; db_rm "$DB"
dltest_query "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

dltest_assert_equal "jm_dl_read" \
  "$(dltest_query "$DB" "PRAGMA journal_mode;" 2>&1)" "wal"

dltest_assert_equal "jm_dl_set_wal" \
  "$(dltest_query "$DB" "PRAGMA journal_mode = WAL;" 2>&1)" "wal"

# Chunk store ignores journal_mode and reports wal.
for mode in DELETE TRUNCATE PERSIST MEMORY OFF; do
  out=$(dltest_query "$DB" "PRAGMA journal_mode = $mode;" 2>&1)
  dltest_assert_equal "jm_dl_set_${mode}_noop" "$out" "wal"
done

db_rm "$DB"

if [ -x "$SQLITE3" ]; then
  # shellcheck source=lib/require_stock_sqlite3.sh
  source "$(dirname "$0")/lib/require_stock_sqlite3.sh"
  if ! require_stock_sqlite3 "$SQLITE3"; then
    exit 1
  fi
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_jm_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  dltest_assert_equal "jm_stock_read" \
    "$(dltest_query "$DB" "PRAGMA journal_mode;" 2>&1)" "delete"

  out=$(dltest_query "$DB" "PRAGMA journal_mode = DELETE;" 2>&1)
  if echo "$out" | grep -qi "doltlite-format"; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: jm_stock_no_doltlite_msg_leaked\n  got: $out"
  else
    PASS=$((PASS+1))
  fi

  db_rm "$DB"
fi

dltest_finish
