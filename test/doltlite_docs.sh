#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_docs Tests ==="

DB=/tmp/test_docs_$$.db
rm -f "$DB"

run_test "fresh_select_shows_default_agent" \
  "SELECT doc_name, length(doc_text) > 500 FROM dolt_docs;" \
  "AGENT.md|1" "$DB"

run_test "fresh_no_status_row" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

run_test "fresh_not_in_sqlite_master" \
  "SELECT count(*) FROM sqlite_master WHERE name='dolt_docs';" \
  "0" "$DB"

run_test "insert_materializes_and_seeds_agent" \
  "INSERT INTO dolt_docs VALUES('README.md','# hello');
   SELECT doc_name FROM dolt_docs ORDER BY doc_name;" \
  "AGENT.md
README.md" "$DB"

run_test "status_new_table" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "dolt_docs|0|new table" "$DB"

run_test "visible_in_sqlite_master" \
  "SELECT count(*) FROM sqlite_master WHERE name='dolt_docs' AND type='table';" \
  "1" "$DB"

run_test "commit_docs" \
  "SELECT dolt_add('-A') IS NOT NULL;
   SELECT dolt_commit('-m','add docs') IS NOT NULL;
   SELECT count(*) FROM dolt_status;" \
  "1
1
0" "$DB"

run_test "update_and_replace" \
  "UPDATE dolt_docs SET doc_text='v2' WHERE doc_name='README.md';
   REPLACE INTO dolt_docs VALUES('LICENSE.md','MIT');
   SELECT doc_name, doc_text FROM dolt_docs WHERE doc_name != 'AGENT.md' ORDER BY doc_name;" \
  "LICENSE.md|MIT
README.md|v2" "$DB"

run_test_match "dup_pk_rejected" \
  "INSERT INTO dolt_docs VALUES('README.md','again');" \
  "UNIQUE constraint failed: dolt_docs.doc_name" "$DB"

run_test_match "null_doc_text_rejected" \
  "INSERT INTO dolt_docs VALUES('x', NULL);" \
  "NOT NULL constraint failed: dolt_docs.doc_text" "$DB"

run_test "insert_or_ignore_dup" \
  "INSERT OR IGNORE INTO dolt_docs VALUES('README.md','ignored');
   SELECT doc_text FROM dolt_docs WHERE doc_name='README.md';" \
  "v2" "$DB"

run_test "drop_falls_back_to_module" \
  "DROP TABLE dolt_docs;
   SELECT doc_name FROM dolt_docs;" \
  "AGENT.md" "$DB"

rm -f "$DB"
DB=/tmp/test_docs2_$$.db
rm -f "$DB"

run_test "zero_row_write_materializes" \
  "DELETE FROM dolt_docs WHERE doc_name='nope';
   SELECT table_name, status FROM dolt_status;
   SELECT doc_name FROM dolt_docs;" \
  "dolt_docs|new table
AGENT.md" "$DB"

rm -f "$DB"
DB=/tmp/test_docs3_$$.db
rm -f "$DB"

run_test "rollback_undoes_materialization" \
  "BEGIN;
   INSERT INTO dolt_docs VALUES('a','1');
   ROLLBACK;
   SELECT doc_name FROM dolt_docs;
   SELECT count(*) FROM sqlite_master WHERE name='dolt_docs';" \
  "AGENT.md
0" "$DB"

run_test "txn_commit_keeps_docs" \
  "BEGIN;
   INSERT INTO dolt_docs VALUES('a','1');
   COMMIT;
   SELECT doc_name FROM dolt_docs ORDER BY doc_name;" \
  "AGENT.md
a" "$DB"

run_test_match "mid_statement_failure_atomic" \
  "DELETE FROM dolt_docs;
   INSERT INTO dolt_docs VALUES('b','1'),('b','2');" \
  "UNIQUE constraint failed" "$DB"

run_test "mid_statement_failure_left_no_rows" \
  "SELECT count(*) FROM dolt_docs WHERE doc_name='b';" \
  "0" "$DB"

rm -f "$DB"
DB=/tmp/test_docs4_$$.db
rm -f "$DB"

run_test_match "drop_without_table_rejected" \
  "DROP TABLE dolt_docs;" \
  "may not be dropped" "$DB"

run_test_match "drop_if_exists_without_table_rejected" \
  "DROP TABLE IF EXISTS dolt_docs;" \
  "may not be dropped" "$DB"

run_test "case_insensitive_names" \
  "INSERT INTO DOLT_DOCS VALUES('x','y');
   SELECT count(*) FROM Dolt_Docs;" \
  "2" "$DB"

run_test "conflict_inspect_and_resolve" \
  "DELETE FROM dolt_docs;
   INSERT INTO dolt_docs VALUES('README.md','base');
   SELECT dolt_commit('-A','-m','base') IS NOT NULL;
   SELECT dolt_branch('b2') IS NOT NULL;
   UPDATE dolt_docs SET doc_text='main edit';
   SELECT dolt_commit('-A','-m','m') IS NOT NULL;
   SELECT dolt_checkout('b2') IS NOT NULL;
   UPDATE dolt_docs SET doc_text='b2 edit';
   SELECT dolt_commit('-A','-m','b') IS NOT NULL;
   SELECT dolt_checkout('main') IS NOT NULL;
   BEGIN;
   SELECT dolt_merge('b2') IS NOT NULL;
   SELECT base_doc_text, our_doc_text, their_doc_text FROM dolt_conflicts_dolt_docs;
   SELECT dolt_conflicts_resolve('--ours','dolt_docs') IS NOT NULL;
   SELECT doc_text FROM dolt_docs;
   COMMIT;" \
  "1
1
1
1
1
1
Error near line 12: Merge has 1 conflict(s). Resolve and then commit with dolt_commit.
base|main edit|b2 edit
1
main edit" "$DB"

rm -f "$DB"
DB=/tmp/test_docs5_$$.db
rm -f "$DB"

run_test "agent_insert_over_seed_first_write" \
  "INSERT INTO dolt_docs VALUES('AGENT.md','mine');
   SELECT doc_name, doc_text FROM dolt_docs;" \
  "AGENT.md|mine" "$DB"

run_test_match "agent_second_insert_rejected" \
  "INSERT INTO dolt_docs VALUES('AGENT.md','other');" \
  "UNIQUE constraint failed" "$DB"

rm -f "$DB"
DB=/tmp/test_docs6_$$.db
rm -f "$DB"

run_test "agent_update_pre_materialization_sticks" \
  "UPDATE dolt_docs SET doc_text='edited' WHERE doc_name='AGENT.md';
   SELECT doc_name, doc_text FROM dolt_docs;" \
  "AGENT.md|edited" "$DB"

rm -f "$DB"
DB=/tmp/test_docs7_$$.db
rm -f "$DB"

run_test "agent_delete_sticks" \
  "DELETE FROM dolt_docs WHERE doc_name='AGENT.md';
   SELECT count(*) FROM dolt_docs;" \
  "0" "$DB"

rm -f "$DB"
DB=/tmp/test_docs8_$$.db
rm -f "$DB"

run_test "agent_default_committable" \
  "INSERT INTO dolt_docs VALUES('README.md','hi');
   SELECT dolt_commit('-A','-m','docs') IS NOT NULL;
   SELECT count(*) FROM dolt_docs WHERE doc_name='AGENT.md';" \
  "1
1" "$DB"

rm -f "$DB"

dltest_finish
