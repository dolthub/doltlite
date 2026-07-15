#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test_int_ge() {
  local n="$1" got="$2" floor="$3"
  if [ -n "$got" ] && [ "$got" -ge "$floor" ] 2>/dev/null; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: >= $floor\n  got:      $got"
  fi
}

run_test_int_in() {
  local n="$1" got="$2" lo="$3" hi="$4"
  if [ -n "$got" ] && [ "$got" -ge "$lo" ] 2>/dev/null && [ "$got" -le "$hi" ] 2>/dev/null; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: in [$lo, $hi]\n  got:      $got"
  fi
}

run_test_grows() {
  local n="$1" before="$2" after="$3"
  if [ -n "$before" ] && [ -n "$after" ] && [ "$after" -gt "$before" ] 2>/dev/null; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  before: $before, after: $after"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA page_count (chunk count) ==="
echo ""

DB=/tmp/test_pc_empty_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "SELECT 1;" > /dev/null 2>&1
empty_pc=$($DOLTLITE "$DB" "PRAGMA page_count;" 2>&1 | tr -d '\n')
run_test_int_in "pc_empty_db_small_positive" "$empty_pc" 1 100
db_rm "$DB"

DB=/tmp/test_pc_small_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "SELECT 1;" > /dev/null 2>&1
pre=$($DOLTLITE "$DB" "PRAGMA page_count;" 2>&1 | tr -d '\n')
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');" > /dev/null 2>&1
post=$($DOLTLITE "$DB" "PRAGMA page_count;" 2>&1 | tr -d '\n')
run_test_grows "pc_grows_with_data" "$pre" "$post"
db_rm "$DB"

DB=/tmp/test_pc_large_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','seed');" > /dev/null 2>&1
small=$($DOLTLITE "$DB" "PRAGMA page_count;" 2>&1 | tr -d '\n')

{
  echo "BEGIN;"
  for i in $(seq 2 1000); do
    echo "INSERT INTO t VALUES($i,'row_$i');"
  done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-A','-m','bulk');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

big=$($DOLTLITE "$DB" "PRAGMA page_count;" 2>&1 | tr -d '\n')
run_test_grows "pc_grows_with_bulk_insert" "$small" "$big"

run_test_int_in "pc_not_fabricated_value" "$big" 1 200
db_rm "$DB"

DB=/tmp/test_pc_gc_rootpages_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "PRAGMA foreign_keys=ON;
CREATE TABLE a(
  id TEXT PRIMARY KEY,
  x TEXT NOT NULL
);
CREATE TABLE b(
  id TEXT PRIMARY KEY,
  a_id TEXT NOT NULL REFERENCES a(id),
  y TEXT NOT NULL
);
CREATE TABLE c(
  a_id TEXT NOT NULL REFERENCES a(id),
  b_id TEXT NOT NULL REFERENCES b(id),
  y TEXT NOT NULL,
  z INTEGER NOT NULL CHECK(z >= 0),
  PRIMARY KEY(a_id,b_id)
) WITHOUT ROWID;
CREATE INDEX idx_b_a ON b(a_id);
CREATE INDEX idx_c_y ON c(y);
SELECT dolt_gc();" > /dev/null 2>&1

pc=$($DOLTLITE "$DB" "PRAGMA page_count;" 2>&1 | tr -d '\n')
max_root=$($DOLTLITE "$DB" "SELECT max(rootpage) FROM sqlite_master;" 2>&1 | tr -d '\n')
run_test_int_ge "pc_gc_covers_schema_rootpages" "$pc" "$max_root"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
