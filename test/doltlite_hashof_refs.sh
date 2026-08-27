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

dltest_finish
