#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_tests Tests ==="

DB=/tmp/test_dolt_tests_$$.db
rm -f "$DB"

run_test "fresh_select_empty" \
  "SELECT count(*) FROM dolt_tests;" \
  "0" "$DB"

run_test "fresh_not_materialized" \
  "SELECT count(*) FROM sqlite_master WHERE name='dolt_tests';
   SELECT count(*) FROM dolt_status;" \
  "0
0" "$DB"

run_test "fresh_schema" \
  "SELECT name, type, \"notnull\", pk FROM pragma_table_info('dolt_tests');" \
  "test_name|TEXT|1|1
test_group|TEXT|0|0
test_query|TEXT|1|0
assertion_type|TEXT|1|0
assertion_comparator|TEXT|1|0
assertion_value|TEXT|0|0" "$DB"

run_test "insert_materializes" \
  "INSERT INTO dolt_tests VALUES('rows','group_a','SELECT 1','expected_rows','==','1');
   SELECT * FROM dolt_tests;" \
  "rows|group_a|SELECT 1|expected_rows|==|1" "$DB"

run_test "status_new_table" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "dolt_tests|0|new table" "$DB"

run_test_match "duplicate_name_rejected" \
  "INSERT INTO dolt_tests VALUES('rows','x','SELECT 1','expected_rows','==','1');" \
  "UNIQUE constraint failed: dolt_tests.test_name" "$DB"

run_test_match "null_query_rejected" \
  "INSERT INTO dolt_tests VALUES('null_query',NULL,NULL,'expected_rows','==','1');" \
  "NOT NULL constraint failed: dolt_tests.test_query" "$DB"

run_test_match "invalid_assertion_rejected" \
  "INSERT INTO dolt_tests VALUES('bad_type',NULL,'SELECT 1','row_count','==','1');" \
  "CHECK constraint failed" "$DB"

run_test_match "invalid_comparator_rejected" \
  "INSERT INTO dolt_tests VALUES('bad_cmp',NULL,'SELECT 1','expected_rows','=','1');" \
  "CHECK constraint failed" "$DB"

run_test "nullable_group_and_value" \
  "INSERT INTO dolt_tests(test_name,test_query,assertion_type,assertion_comparator)
   VALUES('null_value','SELECT NULL','expected_single_value','==');
   SELECT test_group IS NULL, assertion_value IS NULL FROM dolt_tests WHERE test_name='null_value';" \
  "1|1" "$DB"

run_test "update_replace_delete" \
  "UPDATE dolt_tests SET test_group='updated' WHERE test_name='rows';
   REPLACE INTO dolt_tests VALUES('rows','replaced','SELECT 1,2','expected_columns','==','2');
   DELETE FROM dolt_tests WHERE test_name='null_value';
   SELECT * FROM dolt_tests;" \
  "rows|replaced|SELECT 1,2|expected_columns|==|2" "$DB"

run_test "commit_tests" \
  "SELECT dolt_commit('-A','-m','add tests') IS NOT NULL;
   SELECT count(*) FROM dolt_status;" \
  "1
0" "$DB"

run_test "runner_pass_and_fail" \
  "CREATE TABLE fixture(i INT PRIMARY KEY, s TEXT);
   INSERT INTO fixture VALUES(1,'one'),(2,'two');
   INSERT INTO dolt_tests VALUES
     ('a_rows_pass','group_a','SELECT * FROM fixture','expected_rows','==','2'),
     ('b_rows_fail','group_a','SELECT * FROM fixture','expected_rows','==','3'),
     ('c_cols_pass','group_b','SELECT i,s FROM fixture LIMIT 1','expected_columns','==','2'),
     ('d_value_pass','group_b','SELECT count(*) FROM fixture','expected_single_value','>=','2');
   SELECT test_name, status, message FROM dolt_test_run() ORDER BY test_name;" \
  "a_rows_pass|PASS|
b_rows_fail|FAIL|Assertion failed: expected_rows equal to 3, got 2
c_cols_pass|PASS|
d_value_pass|PASS|
rows|PASS|" "$DB"

run_test "runner_name_group_wildcard" \
  "SELECT test_name FROM dolt_test_run('a_rows_pass');
   SELECT test_name FROM dolt_test_run('group_b') ORDER BY test_name;
   SELECT count(*) FROM dolt_test_run('*');" \
  "a_rows_pass
c_cols_pass
d_value_pass
5" "$DB"

run_test "runner_multiple_arguments" \
  "SELECT test_name FROM dolt_test_run('a_rows_pass','group_b');" \
  "a_rows_pass
c_cols_pass
d_value_pass" "$DB"

run_test "all_comparators" \
  "INSERT INTO dolt_tests VALUES
     ('cmp_eq','cmp','SELECT count(*) FROM fixture','expected_single_value','==','2'),
     ('cmp_ne','cmp','SELECT count(*) FROM fixture','expected_single_value','!=','3'),
     ('cmp_lt','cmp','SELECT count(*) FROM fixture','expected_single_value','<','3'),
     ('cmp_le','cmp','SELECT count(*) FROM fixture','expected_single_value','<=','2'),
     ('cmp_gt','cmp','SELECT count(*) FROM fixture','expected_single_value','>','1'),
     ('cmp_ge','cmp','SELECT count(*) FROM fixture','expected_single_value','>=','2');
   SELECT count(*) FROM dolt_test_run('cmp') WHERE status='PASS';" \
  "6" "$DB"

run_test "null_assertion" \
  "INSERT INTO dolt_tests(test_name,test_group,test_query,assertion_type,assertion_comparator)
   VALUES('null_pass','nulls','SELECT NULL','expected_single_value','=='),
         ('null_fail','nulls','SELECT NULL','expected_single_value','!=');
   SELECT test_name, status, message FROM dolt_test_run('nulls');" \
  "null_fail|FAIL|Assertion failed: expected_single_value not equal to NULL, got NULL
null_pass|PASS|" "$DB"

run_test "runner_validation_results" \
  "INSERT INTO dolt_tests VALUES
     ('multi_stmt','validation','SELECT 1; SELECT 2','expected_rows','==','1'),
     ('write_stmt','validation','CREATE TABLE nope(x INT)','expected_rows','==','1'),
     ('command_stmt','validation','SELECT dolt_branch(''side_effect'')','expected_rows','==','1'),
     ('pragma_read','validation','PRAGMA table_info(fixture)','expected_rows','>=','1'),
     ('pragma_tail','validation','SELECT 1; PRAGMA query_only=ON','expected_rows','==','1'),
     ('pragma_write','validation','PRAGMA query_only=ON','expected_rows','==','0'),
     ('recursive','validation','SELECT * FROM dolt_test_run()','expected_rows','==','1'),
     ('zero_rows','validation','SELECT i FROM fixture WHERE 0','expected_single_value','==','1'),
     ('many_cols','validation','SELECT i,s FROM fixture LIMIT 1','expected_single_value','==','1'),
     ('many_rows','validation','SELECT i FROM fixture','expected_single_value','==','1');
   SELECT test_name, status, message FROM dolt_test_run('validation');
   SELECT count(*) FROM dolt_branches WHERE name='side_effect';
   PRAGMA query_only;
   CREATE TABLE pragma_guard(id INT);
   SELECT count(*) FROM sqlite_master WHERE name='pragma_guard';" \
  "command_stmt|FAIL|Cannot execute write queries
many_cols|FAIL|expected_single_value expects exactly one cell. Received multiple columns
many_rows|FAIL|expected_single_value expects exactly one cell. Received multiple rows
multi_stmt|FAIL|Can only run exactly one query
pragma_read|FAIL|Cannot execute PRAGMA queries
pragma_tail|FAIL|Cannot execute PRAGMA queries
pragma_write|FAIL|Cannot execute PRAGMA queries
recursive|FAIL|Cannot call dolt_test_run in dolt_tests
write_stmt|FAIL|Cannot execute write queries
zero_rows|FAIL|expected_single_value expects exactly one cell. Received 0 rows
0
0
1" "$DB"

run_test "runner_query_error" \
  "INSERT INTO dolt_tests VALUES
     ('query_error','errors','SELECT * FROM absent','expected_rows','==','0');
   SELECT test_name, status, message FROM dolt_test_run('errors');" \
  "query_error|FAIL|query error: table not found: absent" "$DB"

run_test_match "runner_missing_argument" \
  "SELECT * FROM dolt_test_run('missing');" \
  "could not find tests for argument: missing" "$DB"

run_test_match "runner_nonliteral_argument" \
  "SELECT * FROM dolt_test_run(upper('errors'));" \
  "dolt_test_run requires literal arguments" "$DB"

run_test "drop_materialized_table" \
  "DROP TABLE dolt_tests;
   SELECT count(*) FROM dolt_tests;
   SELECT count(*) FROM sqlite_master WHERE name='dolt_tests';" \
  "0
0" "$DB"

rm -f "$DB"
DB=/tmp/test_dolt_tests_zero_$$.db
rm -f "$DB"

run_test "zero_row_write_materializes" \
  "DELETE FROM dolt_tests WHERE test_name='missing';
   SELECT table_name, status FROM dolt_status;
   SELECT count(*) FROM sqlite_master WHERE name='dolt_tests';" \
  "dolt_tests|new table
1" "$DB"

rm -f "$DB"
DB=/tmp/test_dolt_tests_txn_$$.db
rm -f "$DB"

run_test "rollback_undoes_materialization" \
  "BEGIN;
   INSERT INTO dolt_tests VALUES('t',NULL,'SELECT 1','expected_rows','==','1');
   ROLLBACK;
   SELECT count(*) FROM dolt_tests;
   SELECT count(*) FROM sqlite_master WHERE name='dolt_tests';" \
  "0
0" "$DB"

run_test_match "runner_without_tests_errors" \
  "SELECT * FROM dolt_test_run();" \
  "could not find tests for argument: *" "$DB"

rm -f "$DB"
DB=/tmp/test_dolt_tests_schema_$$.db
rm -f "$DB"

run_test "hand_created_exact_schema" \
  "CREATE TABLE dolt_tests(
     test_name TEXT NOT NULL,
     test_group TEXT,
     test_query TEXT NOT NULL,
     assertion_type TEXT NOT NULL,
     assertion_comparator TEXT NOT NULL,
     assertion_value TEXT,
     PRIMARY KEY(test_name),
     CONSTRAINT assertion_type_check CHECK(assertion_type IN
       ('expected_rows','expected_columns','expected_single_value')),
     CONSTRAINT assertion_comparator_check CHECK(assertion_comparator IN
       ('==','!=','<','>','<=','>='))
   );
   INSERT INTO dolt_tests VALUES('hand',NULL,'SELECT 1','expected_rows','==','1');
   SELECT test_name, status FROM dolt_test_run();" \
  "hand|PASS" "$DB"

rm -f "$DB"
DB=/tmp/test_dolt_tests_bad_schema_$$.db
rm -f "$DB"

run_test_match "hand_created_wrong_check_rejected" \
  "CREATE TABLE dolt_tests(
     test_name TEXT NOT NULL,
     test_group TEXT,
     test_query TEXT NOT NULL,
     assertion_type TEXT NOT NULL,
     assertion_comparator TEXT NOT NULL,
     assertion_value TEXT,
     PRIMARY KEY(test_name),
     CONSTRAINT assertion_type_check CHECK(assertion_type IN
       ('bad','expected_columns','expected_single_value')),
     CONSTRAINT assertion_comparator_check CHECK(assertion_comparator IN
       ('==','!=','<','>','<=','>='))
   );" \
  "wrong expressions" "$DB"

rm -f "$DB"
DB=/tmp/test_dolt_tests_branch_$$.db
rm -f "$DB"

run_test "branch_isolation" \
  "INSERT INTO dolt_tests VALUES('base',NULL,'SELECT 1','expected_rows','==','1');
   SELECT dolt_commit('-A','-m','base') IS NOT NULL;
   SELECT dolt_checkout('-b','feature') IS NOT NULL;
   INSERT INTO dolt_tests VALUES('feature',NULL,'SELECT 1','expected_rows','==','1');
   SELECT dolt_checkout('main') IS NOT NULL;
   SELECT group_concat(test_name, ',') FROM dolt_tests;" \
  "1
1
1
base" "$DB"

run_test "merge_tests" \
  "SELECT dolt_checkout('feature') IS NOT NULL;
   SELECT dolt_commit('-A','-m','feature test') IS NOT NULL;
   SELECT dolt_checkout('main') IS NOT NULL;
   SELECT dolt_merge('feature') IS NOT NULL;
   SELECT group_concat(test_name, ',') FROM dolt_tests;" \
  "1
1
1
1
base,feature" "$DB"

rm -f "$DB"

dltest_finish
