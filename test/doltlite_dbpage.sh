#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== sqlite_dbpage divergence contract ==="
echo ""

DB=/tmp/test_dbpage_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "page1_size" "SELECT length(data) FROM sqlite_dbpage WHERE pgno=1;" "4096" "$DB"

run_test "page1_magic" "SELECT substr(cast(data AS TEXT), 1, 15) FROM sqlite_dbpage WHERE pgno=1;" "SQLite format 3" "$DB"

run_test_match "page2_rejected" "SELECT length(data) FROM sqlite_dbpage WHERE pgno=2;" \
  "doltlite: sqlite_dbpage only supports pgno=1" "$DB"

run_test_match "page99_rejected" "SELECT length(data) FROM sqlite_dbpage WHERE pgno=99;" \
  "doltlite: sqlite_dbpage only supports pgno=1" "$DB"

run_test "full_scan_count" "SELECT count(*) FROM sqlite_dbpage;" "1" "$DB"

run_test "default_schema" \
  "SELECT quote(schema) FROM sqlite_dbpage WHERE pgno=1;" "'main'" "$DB"

run_test "main_schema" \
  "SELECT count(*) FROM sqlite_dbpage WHERE pgno=1 AND schema='main';" "1" "$DB"

run_test "main_schema_argument" \
  "SELECT count(*) FROM sqlite_dbpage('main') WHERE pgno=1;" "1" "$DB"

run_test "main_schema_case_insensitive" \
  "SELECT count(*) FROM sqlite_dbpage('MAIN') WHERE pgno=1;" "1" "$DB"

run_test "unknown_schema" \
  "SELECT count(*) FROM sqlite_dbpage WHERE pgno=1 AND schema='nosuch';" "0" "$DB"

run_test "unknown_schema_argument" \
  "SELECT count(*) FROM sqlite_dbpage('nosuch') WHERE pgno=1;" "0" "$DB"

run_test "null_schema" \
  "SELECT count(*) FROM sqlite_dbpage(NULL) WHERE pgno=1;" "0" "$DB"

run_test "attached_schema_unsupported" \
  "ATTACH ':memory:' AS aux;
SELECT count(*) FROM sqlite_dbpage('aux') WHERE pgno=1;" "0" "$DB"

run_test "unknown_schema_precedes_page_check" \
  "SELECT count(*) FROM sqlite_dbpage('nosuch') WHERE pgno=99;" "0" "$DB"

db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
