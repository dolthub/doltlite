#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Staging Workflow Tests ==="
echo ""

DB=/tmp/test_staging_$$.db
rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO users VALUES(1,'Alice'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "status_clean_after_commit" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

echo "INSERT INTO users VALUES(2,'Bob');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_unstaged_modify" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "users|0|modified" "$DB"

echo "SELECT dolt_add('users');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_after_add" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "users|1|modified" "$DB"

run_test_match "commit_staged" \
  "SELECT dolt_commit('-m','Add Bob');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "status_clean_after_staged_commit" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

run_test "log_two_commits" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB"

echo "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT); INSERT INTO orders VALUES(1,'hat');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_new_table" \
  "SELECT table_name, staged, status FROM dolt_status WHERE table_name='orders';" \
  "orders|0|new table" "$DB"

echo "SELECT dolt_add('orders');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_orders_staged" \
  "SELECT table_name, staged FROM dolt_status WHERE table_name='orders';" \
  "orders|1" "$DB"

run_test_match "commit_orders" \
  "SELECT dolt_commit('-m','Add orders');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "log_three_commits" \
  "SELECT count(*) FROM dolt_log;" \
  "4" "$DB"

DB2=/tmp/test_staging2_$$.db
rm -f "$DB2"
echo "CREATE TABLE t1(x); INSERT INTO t1 VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO t1 VALUES(2); CREATE TABLE t2(y); INSERT INTO t2 VALUES(10);" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "status_two_changes" \
  "SELECT count(*) FROM dolt_status;" \
  "2" "$DB2"

echo "SELECT dolt_add('-A');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "add_all_stages_both" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "2" "$DB2"

run_test_match "commit_all_staged" \
  "SELECT dolt_commit('-m','both');" \
  "^[0-9a-f]{40}$" "$DB2"

run_test "clean_after_add_all_commit" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB2"

DB3=/tmp/test_staging3_$$.db
rm -f "$DB3"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test_match "commit_dash_a" \
  "SELECT dolt_commit('-A','-m','shortcut');" \
  "^[0-9a-f]{40}$" "$DB3"

run_test "clean_after_dash_a" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB3"

run_test "log_after_dash_a" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB3"

DB4=/tmp/test_staging4_$$.db
rm -f "$DB4"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test_error_match "commit_without_add_fails" \
  "SELECT dolt_commit('-m','should fail');" \
  "nothing to commit" "$DB4"

run_test "data_survives_failed_commit" \
  "SELECT count(*) FROM t;" \
  "2" "$DB4"

DB5=/tmp/test_staging5_$$.db
rm -f "$DB5"
echo "CREATE TABLE a(x); CREATE TABLE b(y); INSERT INTO a VALUES(1); INSERT INTO b VALUES(2); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO a VALUES(10); INSERT INTO b VALUES(20);" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "two_unstaged_changes" \
  "SELECT count(*) FROM dolt_status WHERE staged=0;" \
  "2" "$DB5"

echo "SELECT dolt_add('a');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "one_staged_one_unstaged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB5"

run_test "b_still_unstaged" \
  "SELECT table_name FROM dolt_status WHERE staged=0;" \
  "b" "$DB5"

run_test_match "commit_only_a" \
  "SELECT dolt_commit('-m','only a');" \
  "^[0-9a-f]{40}$" "$DB5"

run_test "b_still_unstaged_after_commit" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "b|0|modified" "$DB5"

DB6=/tmp/test_staging6_$$.db
rm -f "$DB6"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_add('t');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test "staged_persists" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB6"

DB6B=/tmp/test_staging6b_$$.db
rm -f "$DB6B"
echo "CREATE TABLE keep_t(x); CREATE TABLE drop_t(y); INSERT INTO keep_t VALUES(1); INSERT INTO drop_t VALUES(2); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6B" > /dev/null 2>&1
echo "DROP TABLE drop_t;" | $DOLTLITE "$DB6B" > /dev/null 2>&1

run_test "dropped_table_unstaged" \
  "SELECT table_name, staged, status FROM dolt_status WHERE table_name='drop_t';" \
  "drop_t|0|deleted" "$DB6B"

echo "SELECT dolt_add('drop_t');" | $DOLTLITE "$DB6B" > /dev/null 2>&1
run_test "dropped_table_staged" \
  "SELECT table_name, staged, status FROM dolt_status WHERE table_name='drop_t';" \
  "drop_t|1|deleted" "$DB6B"

run_test_match "commit_dropped_table" \
  "SELECT dolt_commit('-m','drop drop_t');" \
  "^[0-9a-f]{40}$" "$DB6B"

run_test "dropped_table_gone_after_commit" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='drop_t';" \
  "0" "$DB6B"

DB7=/tmp/test_staging7_$$.db
rm -f "$DB7"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB7" > /dev/null 2>&1

echo "SELECT dolt_add('.');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test "add_dot_stages_all" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB7"

DB9=/tmp/test_staging_9_$$.db
rm -f "$DB9"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE membership(team_id INTEGER, employee_id INTEGER, PRIMARY KEY(team_id, employee_id));
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB9" > /dev/null 2>&1

run_test "status_clean_composite_pk_schema" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB9"

DB10=/tmp/test_staging10_$$.db
rm -f "$DB10"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY); CREATE TABLE u(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); INSERT INTO u VALUES(1);" | $DOLTLITE "$DB10" > /dev/null 2>&1

run_test "add_dot_mixed_with_table" \
  "SELECT dolt_add('t', '.');" \
  "Error near line 1: error: the table(s) . do not exist" "$DB10"

run_test "add_dot_mixed_stages_nothing" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "0" "$DB10"

run_test "add_dot_twice" \
  "SELECT dolt_add('.', '.');" \
  "Error near line 1: error: the table(s) ., . do not exist" "$DB10"

DB11=/tmp/test_staging11_$$.db
rm -f "$DB11"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1);" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "SELECT dolt_add('-A', '.');" | $DOLTLITE "$DB11" > /dev/null 2>&1
run_test "add_all_flag_with_dot_stages" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB11"

# An AUTOINCREMENT table cannot take an insert without sqlite_sequence.
# Staging the first one by name takes sqlite_sequence with it.
DBA=/tmp/test_staging_autoinc_$$.db
rm -f "$DBA"
run_test "autoinc_named_add_stages_sqlite_sequence" \
  "CREATE TABLE plain(id INTEGER PRIMARY KEY, v);
   CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, a INT);
   INSERT INTO t(a) VALUES(1),(2);
   SELECT dolt_add('t');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name, staged;" \
  "0
plain|0|new table
sqlite_sequence|1|new table
t|1|new table" "$DBA"
run_test "autoinc_named_add_commit_is_insertable_on_new_branch" \
  "SELECT dolt_commit('-m','c1') IS NOT NULL;
   SELECT dolt_checkout('-b','b2');
   INSERT INTO t(a) VALUES(3);
   SELECT count(*), max(id)>2 FROM t;
   PRAGMA integrity_check;" \
  "1
0
3|1
ok" "$DBA"
run_test "autoinc_named_add_insertable_from_fresh_connection" \
  "INSERT INTO t(a) VALUES(4); SELECT count(*) FROM t;" \
  "4" "$DBA/b2"
run_test "autoinc_second_table_leaves_staged_sequence_alone" \
  "CREATE TABLE t2(id INTEGER PRIMARY KEY AUTOINCREMENT, a INT);
   INSERT INTO t2(a) VALUES(1);
   SELECT dolt_add('t2');
   SELECT table_name, staged, status FROM dolt_status ORDER BY table_name, staged;
   SELECT dolt_commit('-m','c2') IS NOT NULL;
   SELECT dolt_checkout('-b','b3');
   INSERT INTO t2(a) VALUES(2);
   SELECT count(*) FROM t2;" \
  "0
sqlite_sequence|0|modified
t|0|modified
t2|1|new table
1
0
2" "$DBA/b2"
run_test "plain_named_add_does_not_stage_sqlite_sequence" \
  "INSERT INTO t(a) VALUES(9);
   SELECT dolt_add('plain');
   SELECT table_name, staged FROM dolt_status WHERE table_name='sqlite_sequence';" \
  "0
sqlite_sequence|0" "$DBA"
rm -f "$DBA"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB6B" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11"

dltest_finish
