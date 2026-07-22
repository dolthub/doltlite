#!/bin/bash
DLTEST_MATCH_FLAGS=i
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Behavior Tests ==="
echo ""

echo "--- Conflict checkout tests ---"

DB=/tmp/test_bhv1_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main_val';
SELECT dolt_commit('-A','-m','main edit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat_val';
SELECT dolt_commit('-A','-m','feat edit');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "checkout_blocked_conflict" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_checkout('feature'); ROLLBACK;" \
  "unresolved merge conflicts" "$DB"

run_test_match "checkout_create_blocked_conflict" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_checkout('-b','blocked_branch_tx'); ROLLBACK;" \
  "unresolved merge conflicts" "$DB"

TX_OUT=$({
cat <<'SQL'
BEGIN;
SELECT dolt_merge('feature');
SELECT dolt_checkout('-b','blocked_branch_tx');
SELECT 'CNT|' || count(*) FROM dolt_branches WHERE name='blocked_branch_tx';
ROLLBACK;
SQL
} | $DOLTLITE "$DB" 2>&1 | grep '^CNT|')
if [ "$TX_OUT" = "CNT|0" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: checkout_create_no_branch_on_conflict\n  expected: CNT|0\n  got:      $TX_OUT"
fi

run_test "checkout_after_rollback" \
  "SELECT dolt_checkout('feature'); SELECT active_branch();" \
  "0
feature" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

rm -f "$DB"

DB=/tmp/test_bhv3_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main2';
SELECT dolt_commit('-A','-m','main2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat2';
SELECT dolt_commit('-A','-m','feat2');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "checkout_after_rolled_back_merge" \
  "SELECT dolt_checkout('feature'); SELECT active_branch();" \
  "0
feature" "$DB"

rm -f "$DB"

echo "--- Schema merge tests ---"

DB=/tmp/test_bhv4_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN x TEXT;
UPDATE t SET x='mx';
SELECT dolt_commit('-A','-m','add x');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN y INTEGER;
UPDATE t SET y=42;
SELECT dolt_commit('-A','-m','add y');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_merge_both_add_col" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

run_test_match "schema_merge_has_x_col" \
  "SELECT x FROM t WHERE id=1;" \
  "mx" "$DB"

run_test "schema_merge_has_y_col" \
  "SELECT y FROM t WHERE id=1;" \
  "42" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv4b_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN x TEXT;
UPDATE t SET x='x1' WHERE id=1;
UPDATE t SET x='x2' WHERE id=2;
UPDATE t SET x='x3' WHERE id=3;
SELECT dolt_commit('-A','-m','add x');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN y INTEGER;
UPDATE t SET y=10 WHERE id=1;
UPDATE t SET y=20 WHERE id=2;
UPDATE t SET y=30 WHERE id=3;
SELECT dolt_commit('-A','-m','add y');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_data_migrate_row1" \
  "SELECT x, y FROM t WHERE id=1;" \
  "x1|10" "$DB"

run_test "schema_data_migrate_row2" \
  "SELECT x, y FROM t WHERE id=2;" \
  "x2|20" "$DB"

run_test "schema_data_migrate_row3" \
  "SELECT x, y FROM t WHERE id=3;" \
  "x3|30" "$DB"

run_test "schema_data_migrate_orig" \
  "SELECT v FROM t WHERE id=1;" \
  "a" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv4c_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN x TEXT;
UPDATE t SET x='val_x' WHERE id=1;
INSERT INTO t VALUES(2,'main_new','new_x');
SELECT dolt_commit('-A','-m','add x with new row');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN y INTEGER;
UPDATE t SET y=7 WHERE id=1;
INSERT INTO t VALUES(3,'b1_new',77);
SELECT dolt_commit('-A','-m','add y with new row');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_data_migrate_both_cols" \
  "SELECT x, y FROM t WHERE id=1;" \
  "val_x|7" "$DB"

run_test "schema_data_migrate_main_row" \
  "SELECT x, typeof(y) FROM t WHERE id=2;" \
  "new_x|null" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv5_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='hi';
SELECT dolt_commit('-A','-m','add extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','data only');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_one_side_ok" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv6_$$.db; rm -f "$DB"
echo "CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "UPDATE t1 SET v='A';
SELECT dolt_commit('-A','-m','t1 change');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t2 SET v='X';
SELECT dolt_commit('-A','-m','t2 change');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "diff_tables_merge_ok" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

run_test "diff_tables_t1" "SELECT v FROM t1;" "A" "$DB"
run_test "diff_tables_t2" "SELECT v FROM t2;" "X" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv7s_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN z TEXT;
UPDATE t SET z='main_z';
SELECT dolt_commit('-A','-m','add z on main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN z TEXT;
UPDATE t SET z='b1_z';
SELECT dolt_commit('-A','-m','add z on b1');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_merge_same_col_same_def" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]|conflict" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv8s_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN w TEXT;
SELECT dolt_commit('-A','-m','add w TEXT');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN w INTEGER;
SELECT dolt_commit('-A','-m','add w INTEGER');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_merge_same_col_diff_type" \
  "SELECT dolt_merge('b1');" \
  "Merge conflict detected.*dolt_schema_conflicts" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv9s_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, extra TEXT);
INSERT INTO t VALUES(1,'a','e');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN newcol TEXT;
SELECT dolt_commit('-A','-m','add newcol');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='b';
SELECT dolt_commit('-A','-m','data change');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_add_col_other_data" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

rm -f "$DB"

echo "--- dolt_at working state tests ---"

DB=/tmp/test_bhv7_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'committed');
SELECT dolt_commit('-A','-m','feat committed');
INSERT INTO t VALUES(3,'uncommitted');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "at_working_uncommitted_count" \
  "SELECT count(*) FROM dolt_at_t('feature');" \
  "3" "$DB"

run_test "at_working_uncommitted_row" \
  "SELECT v FROM dolt_at_t('feature') WHERE id=3;" \
  "uncommitted" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv8_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat commit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "at_committed_count" \
  "SELECT count(*) FROM dolt_at_t('feature');" \
  "2" "$DB"

run_test "at_committed_row" \
  "SELECT v FROM dolt_at_t('feature') WHERE id=2;" \
  "feat_row" "$DB"

rm -f "$DB"

DB=/tmp/test_bhv9_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'committed');
SELECT dolt_commit('-A','-m','feat committed');
INSERT INTO t VALUES(3,'uncommitted');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

FEAT_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0;" | $DOLTLITE "$DB" 2>&1)

echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
FEAT_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB/feature" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "at_commit_hash_count" \
  "SELECT count(*) FROM dolt_at_t('$FEAT_HASH');" \
  "2" "$DB"

rm -f "$DB"

dltest_finish
