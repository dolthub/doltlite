#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Diff Across ALTER TABLE Tests ==="
echo ""

DB1=/tmp/test_diff_alter1_$$.db; rm -f "$DB1"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'alice'),(2,'bob'),(3,'carol');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB1" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN age INTEGER;" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "alter_add_col_no_data_change" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "0" "$DB1"

echo "UPDATE t SET age=30 WHERE id=1;" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "alter_only_updated_row_in_diff" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "1" "$DB1"

run_test "alter_updated_row_is_modify" \
  "SELECT diff_type || '|' || coalesce(to_id, from_id) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "modified|1" "$DB1"

echo "SELECT dolt_commit('-A','-m','add age column and update alice');" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "alter_cross_commit_count" \
  "SELECT coalesce(sum(rows_added + rows_deleted + rows_modified), 0) FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "1" "$DB1"

run_test "alter_cross_commit_type" \
  "SELECT rows_modified FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "1" "$DB1"

DB2=/tmp/test_diff_alter2_$$.db; rm -f "$DB2"

echo "CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO items VALUES(1,'hat'),(2,'coat');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1

echo "ALTER TABLE items ADD COLUMN price REAL;
ALTER TABLE items ADD COLUMN qty INTEGER;" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "multi_add_col_no_change" \
  "SELECT count(*) FROM dolt_diff_items WHERE to_commit='WORKING';" \
  "0" "$DB2"

echo "UPDATE items SET price=9.99, qty=5 WHERE id=2;" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "multi_add_col_one_update" \
  "SELECT count(*) FROM dolt_diff_items WHERE to_commit='WORKING';" \
  "1" "$DB2"

run_test "multi_add_col_correct_row" \
  "SELECT coalesce(to_id, from_id) FROM dolt_diff_items WHERE to_commit='WORKING';" \
  "2" "$DB2"

echo "SELECT dolt_commit('-A','-m','update coat price');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "diff_table_across_alter_count" \
  "SELECT count(*) FROM dolt_diff_items WHERE diff_type='modified';" \
  "1" "$DB2"

DB3=/tmp/test_diff_alter3_$$.db; rm -f "$DB3"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN w TEXT;
INSERT INTO t VALUES(2,'y','z');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "insert_after_alter_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "1" "$DB3"

run_test "insert_after_alter_type" \
  "SELECT diff_type || '|' || coalesce(to_id, from_id) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "added|2" "$DB3"

DB4=/tmp/test_diff_alter4_$$.db; rm -f "$DB4"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN extra TEXT;
DELETE FROM t WHERE id=1;" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "delete_after_alter_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "1" "$DB4"

run_test "delete_after_alter_type" \
  "SELECT diff_type || '|' || coalesce(to_id, from_id) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "removed|1" "$DB4"

DB5=/tmp/test_diff_alter5_$$.db; rm -f "$DB5"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'alice'),(2,'bob');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN age INTEGER;
UPDATE t SET age=30 WHERE id=1;
SELECT dolt_commit('-A','-m','add age on main');" | $DOLTLITE "$DB5" > /dev/null 2>&1

echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "UPDATE t SET name='BOB' WHERE id=2;
SELECT dolt_commit('-A','-m','update bob on feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test_match "merge_after_alter_no_crash" \
  "SELECT dolt_merge('feature');" \
  "." "$DB5"

run_test_match "diff_after_merge_works" \
  "SELECT coalesce(sum(rows_added + rows_deleted + rows_modified), 0) FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "^[0-9]+$" "$DB5"

DB6=/tmp/test_diff_alter6_$$.db; rm -f "$DB6"

echo "CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a,b));
INSERT INTO t VALUES('x',1),('y',2);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
INSERT INTO t VALUES('m',9);
SELECT dolt_commit('-A','-m','main2');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN c TEXT;
UPDATE t SET c='z' WHERE a='x';
SELECT dolt_commit('-A','-m','feat2');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test "pk_covering_add_col_diff_row" \
  "SELECT dolt_checkout('feat'); SELECT from_a || '|' || from_b || '|' || coalesce(from_c,'NULL') || ' -> ' || to_a || '|' || to_b || '|' || to_c || ' ' || diff_type FROM dolt_diff_t('HEAD~1','HEAD');" \
  "0
x|1|NULL -> x|1|z modified" "$DB6"

run_test "pk_covering_add_col_diff_stat" \
  "SELECT dolt_checkout('feat'); SELECT rows_modified FROM dolt_diff_stat('HEAD~1','HEAD','t');" \
  "0
1" "$DB6"

run_test "pk_covering_add_col_diff_summary" \
  "SELECT dolt_checkout('feat'); SELECT diff_type || '|' || data_change FROM dolt_diff_summary('HEAD~1','HEAD');" \
  "0
modified|1" "$DB6"

run_test_match "pk_covering_add_col_patch_emits_update" \
  "SELECT dolt_checkout('feat'); SELECT statement FROM dolt_patch('HEAD~1','HEAD');" \
  "^UPDATE \"t\" SET \"c\"='z' WHERE \"a\"='x' AND \"b\"=1;$" "$DB6"

run_test "pk_covering_add_col_merge" \
  "SELECT length(dolt_merge('feat')); SELECT a || '|' || b || '|' || coalesce(c,'NULL') FROM t ORDER BY a;" \
  "40
m|9|NULL
x|1|z
y|2|NULL" "$DB6"

DB7=/tmp/test_diff_alter7_$$.db; rm -f "$DB7"

echo "CREATE TABLE t(a TEXT PRIMARY KEY) WITHOUT ROWID;
INSERT INTO t VALUES('x'),('y');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
INSERT INTO t VALUES('m');
SELECT dolt_commit('-A','-m','main2');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN c INT DEFAULT 7;
SELECT dolt_commit('-A','-m','feat2');" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test "pk_covering_add_col_default_cherry_pick" \
  "SELECT length(dolt_cherry_pick('feat')); SELECT a || '|' || c FROM t ORDER BY a;" \
  "40
m|7
x|7
y|7" "$DB7"

rm -f "$DB1" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7"

dltest_finish
