#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== dolt_hashof WORKING/STAGED refs ==="
echo ""

DB=:memory:

dirty() {
  printf "%s" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','c');
INSERT INTO t VALUES(2,2);
$1"
}

run_test_lastline "hashof_db_working_equals_noarg" \
  "$(dirty "SELECT dolt_hashof_db('WORKING') = dolt_hashof_db();")" \
  "1" \
  "$DB"

run_test_lastline "hashof_db_empty_equals_working" \
  "$(dirty "SELECT dolt_hashof_db('') = dolt_hashof_db('WORKING');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_db_working_case_insensitive" \
  "$(dirty "SELECT dolt_hashof_db('working') = dolt_hashof_db('WORKING');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_db_staged_equals_head_before_add" \
  "$(dirty "SELECT dolt_hashof_db('STAGED') = dolt_hashof_db('HEAD');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_db_working_differs_from_head_when_dirty" \
  "$(dirty "SELECT dolt_hashof_db('WORKING') != dolt_hashof_db('HEAD');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_table_working_equals_noarg" \
  "$(dirty "SELECT dolt_hashof_table('t','WORKING') = dolt_hashof_table('t');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_table_working_differs_from_head" \
  "$(dirty "SELECT dolt_hashof_table('t','WORKING') != dolt_hashof_table('t','HEAD');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_catalog_working_equals_noarg" \
  "$(dirty "SELECT dolt_hashof_catalog('WORKING') = dolt_hashof_catalog();")" \
  "1" \
  "$DB"

run_test_lastline "hashof_db_staged_equals_working_after_add" \
  "$(dirty "SELECT dolt_add('t');
SELECT dolt_hashof_db('STAGED') = dolt_hashof_db('WORKING');")" \
  "1" \
  "$DB"

run_test_lastline "hashof_db_staged_differs_from_head_after_add" \
  "$(dirty "SELECT dolt_add('t');
SELECT dolt_hashof_db('STAGED') != dolt_hashof_db('HEAD');")" \
  "1" \
  "$DB"

run_test_match "hashof_working_still_errors" \
  "$(dirty "SELECT dolt_hashof('WORKING');")" \
  "invalid ref spec" \
  "$DB"

run_test_match "hashof_staged_still_errors" \
  "$(dirty "SELECT dolt_hashof('STAGED');")" \
  "invalid ref spec" \
  "$DB"

DBI=/tmp/test_hashof_index_$$.db; rm -f "$DBI"
$DOLTLITE "$DBI" > /dev/null 2>&1 <<'SQL'
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, grp INTEGER NOT NULL, name TEXT NOT NULL);
CREATE INDEX child_grp ON child(grp);
CREATE UNIQUE INDEX child_with_parent ON child(grp, parent_id, name) WHERE parent_id IS NOT NULL;
INSERT INTO child VALUES(1, NULL, 1, 'a'), (2, 3, 1, 'b');
SELECT dolt_commit('-Am','seed');
SQL

run_test_match "hashof_index_is_hex" \
  "SELECT dolt_hashof_index('child_grp');" \
  "^[0-9a-f]{40}$" "$DBI"
run_test "hashof_index_is_stable" \
  "SELECT dolt_hashof_index('child_grp') = dolt_hashof_index('child_grp');" \
  "1" "$DBI"
run_test "hashof_index_distinguishes_indexes" \
  "SELECT dolt_hashof_index('child_grp') <> dolt_hashof_index('child_with_parent');" \
  "1" "$DBI"
run_test "hashof_index_accepts_a_ref" \
  "SELECT dolt_hashof_index('child_grp') = dolt_hashof_index('child_grp','HEAD');" \
  "1" "$DBI"
run_test_match "hashof_index_unknown_name_errors" \
  "SELECT dolt_hashof_index('nope');" \
  "index not found" "$DBI"
run_test "hashof_index_null_is_null" \
  "SELECT coalesce(dolt_hashof_index(NULL),'NULL');" \
  "NULL" "$DBI"
HASHES_BEFORE_REINDEX=$($DOLTLITE "$DBI" \
  "SELECT dolt_hashof_index('child_grp') || dolt_hashof_table('child') || dolt_hashof_db();" \
  2>/dev/null)
run_test "reindex_changes_no_hash" \
  "REINDEX; SELECT dolt_hashof_index('child_grp') || dolt_hashof_table('child') || dolt_hashof_db();" \
  "$HASHES_BEFORE_REINDEX" "$DBI"
run_test "reindex_leaves_status_clean" \
  "REINDEX; SELECT count(*) FROM dolt_status;" \
  "0" "$DBI"

echo "INSERT INTO child VALUES(9, 3, 1, 'z');" | $DOLTLITE "$DBI" > /dev/null 2>&1
run_test "uncommitted_index_write_moves_index_hash" \
  "SELECT dolt_hashof_index('child_with_parent') <> dolt_hashof_index('child_with_parent','HEAD');" \
  "1" "$DBI"
run_test "uncommitted_index_write_moves_table_hash" \
  "SELECT dolt_hashof_table('child') <> dolt_hashof_table('child','HEAD');" \
  "1" "$DBI"

DBJ=/tmp/test_hashof_index2_$$.db; rm -f "$DBJ"
$DOLTLITE "$DBJ" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','seed');
SQL
run_test "table_without_indexes_keeps_root_schema_hash" \
  "SELECT dolt_hashof_table('t') = dolt_hashof_table('t','HEAD');" \
  "1" "$DBJ"

rm -f "$DBI" "$DBJ"

dltest_finish
