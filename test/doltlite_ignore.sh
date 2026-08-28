#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_ignore Tests ==="

DB=/tmp/test_ignore_$$.db
rm -f "$DB"

run_test "fresh_select_empty" \
  "SELECT count(*) FROM dolt_ignore;" \
  "0" "$DB"

run_test "fresh_not_materialized" \
  "SELECT count(*) FROM sqlite_master WHERE name='dolt_ignore';
   SELECT count(*) FROM dolt_status;" \
  "0
0" "$DB"

run_test "fresh_schema" \
  "SELECT name, type, \"notnull\", pk FROM pragma_table_info('dolt_ignore');" \
  "pattern|TEXT|1|1
ignored|TINYINT|1|0" "$DB"

run_test "insert_materializes" \
  "INSERT INTO dolt_ignore VALUES('tmp_*', 1);
   SELECT * FROM dolt_ignore;" \
  "tmp_*|1" "$DB"

run_test "status_new_table" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "dolt_ignore|0|new table" "$DB"

run_test "visible_in_sqlite_master" \
  "SELECT count(*) FROM sqlite_master WHERE name='dolt_ignore' AND type='table';" \
  "1" "$DB"

run_test_match "dup_pk_rejected" \
  "INSERT INTO dolt_ignore VALUES('tmp_*', 0);" \
  "UNIQUE constraint failed: dolt_ignore.pattern" "$DB"

run_test_match "null_pattern_rejected" \
  "INSERT INTO dolt_ignore VALUES(NULL, 1);" \
  "NOT NULL constraint failed: dolt_ignore.pattern" "$DB"

run_test "insert_or_ignore_dup" \
  "INSERT OR IGNORE INTO dolt_ignore VALUES('tmp_*', 0);
   SELECT ignored FROM dolt_ignore WHERE pattern='tmp_*';" \
  "1" "$DB"

run_test "update_replace_delete" \
  "INSERT INTO dolt_ignore VALUES('keep_me', 0);
   UPDATE dolt_ignore SET ignored=1 WHERE pattern='keep_me';
   REPLACE INTO dolt_ignore VALUES('tmp_*', 0);
   DELETE FROM dolt_ignore WHERE pattern='keep_me';
   SELECT * FROM dolt_ignore;" \
  "tmp_*|0" "$DB"

run_test "add_respects_ignore" \
  "UPDATE dolt_ignore SET ignored=1 WHERE pattern='tmp_*';
   CREATE TABLE tmp_secret(id INTEGER PRIMARY KEY);
   CREATE TABLE keep(id INTEGER PRIMARY KEY);
   INSERT INTO keep VALUES(1);
   SELECT dolt_add('-A');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
dolt_ignore|1|new table
keep|1|new table" "$DB"

run_test "commit_ignore" \
  "SELECT dolt_commit('-m','ignore tmp') IS NOT NULL;
   SELECT count(*) FROM dolt_status;" \
  "1
0" "$DB"

run_test "drop_falls_back_to_module" \
  "DROP TABLE dolt_ignore;
   SELECT count(*) FROM dolt_ignore;
   SELECT count(*) FROM sqlite_master WHERE name='dolt_ignore';" \
  "0
0" "$DB"

DB2=/tmp/test_ignore_case_$$.db
rm -f "$DB2"

run_test "patterns_are_case_sensitive" \
  "INSERT INTO dolt_ignore VALUES('TMP_*', 1);
   CREATE TABLE tmp_a(id INTEGER PRIMARY KEY);
   SELECT dolt_add('-A');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
dolt_ignore|1|new table
tmp_a|1|new table" "$DB2"

DB4=/tmp/test_ignore_utf8_$$.db
rm -f "$DB4"

run_test "question_mark_matches_utf8_character" \
  "INSERT INTO dolt_ignore VALUES('tmp_?', 1);
   CREATE TABLE \"tmp_é\"(id INTEGER PRIMARY KEY);
   CREATE TABLE tmp_xy(id INTEGER PRIMARY KEY);
   SELECT dolt_add('-A');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
dolt_ignore|1|new table
tmp_xy|1|new table" "$DB4"

DB3=/tmp/test_ignore_specificity_$$.db
rm -f "$DB3"

run_test_match "incomparable_patterns_conflict" \
  "INSERT INTO dolt_ignore VALUES('a*bc', 1),('ab*', 0);
   CREATE TABLE abxbc(id INTEGER PRIMARY KEY);
   SELECT dolt_add('-A');" \
  "matches conflicting patterns" "$DB3"

DB5=/tmp/test_ignore_force_$$.db
rm -f "$DB5"

run_test "force_add_named_ignored" \
  "INSERT INTO dolt_ignore VALUES('tmp_*', 1);
   CREATE TABLE tmp_secret(id INTEGER PRIMARY KEY);
   CREATE TABLE keep(id INTEGER PRIMARY KEY);
   SELECT dolt_add('-f', 'tmp_secret');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
dolt_ignore|0|new table
keep|0|new table
tmp_secret|1|new table" "$DB5"

run_test "force_added_table_commits" \
  "SELECT dolt_add('dolt_ignore', 'keep');
   SELECT dolt_commit('-m','forced') IS NOT NULL;
   SELECT count(*) FROM dolt_status;
   INSERT INTO tmp_secret VALUES(1);
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
1
0
tmp_secret|0|modified" "$DB5"

DB6=/tmp/test_ignore_force_all_$$.db
rm -f "$DB6"

run_test "force_add_all_stages_ignored" \
  "INSERT INTO dolt_ignore VALUES('tmp_*', 1);
   CREATE TABLE tmp_secret(id INTEGER PRIMARY KEY);
   CREATE TABLE keep(id INTEGER PRIMARY KEY);
   SELECT dolt_add('-A', '-f');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
dolt_ignore|1|new table
keep|1|new table
tmp_secret|1|new table" "$DB6"

DB7=/tmp/test_ignore_force_dot_$$.db
rm -f "$DB7"

run_test "force_add_dot_stages_ignored" \
  "INSERT INTO dolt_ignore VALUES('tmp_*', 1);
   CREATE TABLE tmp_secret(id INTEGER PRIMARY KEY);
   SELECT dolt_add('--force', '.');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "0
dolt_ignore|1|new table
tmp_secret|1|new table" "$DB7"

run_test_match "force_requires_name" \
  "SELECT dolt_add('-f');" \
  "requires table name" "$DB7"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7"

dltest_finish
