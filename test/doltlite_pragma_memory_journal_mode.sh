#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"
dltest_init_queries

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""


db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA journal_mode on :memory: ==="
echo ""

dltest_assert_equal "mem_journal_mode_is_memory" \
  "$(dltest_query ":memory:" "PRAGMA journal_mode;" 2>&1)" "memory"

dltest_assert_equal "mem_set_memory_idempotent" \
  "$(dltest_query ":memory:" "PRAGMA journal_mode = MEMORY;" 2>&1)" "memory"

DB=/tmp/test_mwjm_file_$$.db; db_rm "$DB"
dltest_query "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1
dltest_assert_equal "file_journal_mode_is_wal" \
  "$(dltest_query "$DB" "PRAGMA journal_mode;" 2>&1)" "wal"
db_rm "$DB"

dltest_finish
