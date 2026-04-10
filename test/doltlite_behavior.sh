#!/bin/bash
#
# Tests for: conflict-checkout blocking, schema merge errors, dolt_at working state.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qiE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Behavior Tests ==="
echo ""

# ============================================================
# Conflict Checkout Tests
# ============================================================

echo "--- Conflict checkout tests ---"

# Test 1: Checkout blocked during unresolved merge conflicts
DB=/tmp/test_bhv1_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main_val';
SELECT dolt_commit('-A','-m','main edit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat_val';
SELECT dolt_commit('-A','-m','feat edit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Now we have unresolved conflicts; checkout should fail
run_test_match "checkout_blocked_conflict" \
  "SELECT dolt_checkout('feature');" \
  "conflict|merge" "$DB"

run_test_match "checkout_create_blocked_conflict" \
  "SELECT dolt_checkout('-b','blocked_branch');" \
  "conflict|merge" "$DB"

run_test "checkout_create_no_branch_on_conflict" \
  "SELECT count(*) FROM dolt_branches WHERE name='blocked_branch';" \
  "0" "$DB"

# Test 2: Resolve conflicts, then checkout should succeed
echo "SELECT dolt_conflicts_resolve('--ours','t');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_commit('-A','-m','resolved');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "checkout_after_resolve" \
  "SELECT dolt_checkout('feature'); SELECT active_branch();" \
  "0
feature" "$DB"

# Switch back for cleanup
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

rm -f "$DB"

# Test 3: Abort merge, then checkout should succeed
DB=/tmp/test_bhv3_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main2';
SELECT dolt_commit('-A','-m','main2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat2';
SELECT dolt_commit('-A','-m','feat2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Abort the merge
echo "SELECT dolt_merge('--abort');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Checkout should now succeed
run_test "checkout_after_abort" \
  "SELECT dolt_checkout('feature'); SELECT active_branch();" \
  "0
feature" "$DB"

rm -f "$DB"

# ============================================================
# Schema Merge Tests
# ============================================================

echo "--- Schema merge tests ---"

# Test 4: Both branches add different columns to same table -> merge succeeds
DB=/tmp/test_bhv4_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds column 'x'
echo "ALTER TABLE t ADD COLUMN x TEXT;
UPDATE t SET x='mx';
SELECT dolt_commit('-A','-m','add x');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 adds column 'y'
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN y INTEGER;
UPDATE t SET y=42;
SELECT dolt_commit('-A','-m','add y');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Back to main, merge should succeed with schema merge
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_merge_both_add_col" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

# Verify both columns are present after merge
run_test_match "schema_merge_has_x_col" \
  "SELECT x FROM t WHERE id=1;" \
  "mx" "$DB"

run_test "schema_merge_has_y_col" \
  "SELECT y FROM t WHERE id=1;" \
  "42" "$DB"

rm -f "$DB"

# Test 4b: Schema merge row data migration - multiple rows with both columns
DB=/tmp/test_bhv4b_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds column 'x' with data for all rows
echo "ALTER TABLE t ADD COLUMN x TEXT;
UPDATE t SET x='x1' WHERE id=1;
UPDATE t SET x='x2' WHERE id=2;
UPDATE t SET x='x3' WHERE id=3;
SELECT dolt_commit('-A','-m','add x');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 adds column 'y' with data for all rows
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN y INTEGER;
UPDATE t SET y=10 WHERE id=1;
UPDATE t SET y=20 WHERE id=2;
UPDATE t SET y=30 WHERE id=3;
SELECT dolt_commit('-A','-m','add y');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify all rows have correct values for BOTH added columns
run_test "schema_data_migrate_row1" \
  "SELECT x, y FROM t WHERE id=1;" \
  "x1|10" "$DB"

run_test "schema_data_migrate_row2" \
  "SELECT x, y FROM t WHERE id=2;" \
  "x2|20" "$DB"

run_test "schema_data_migrate_row3" \
  "SELECT x, y FROM t WHERE id=3;" \
  "x3|30" "$DB"

# Verify original columns preserved
run_test "schema_data_migrate_orig" \
  "SELECT v FROM t WHERE id=1;" \
  "a" "$DB"

rm -f "$DB"

# Test 4c: Schema merge with new rows added on branch with different column
DB=/tmp/test_bhv4c_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds column 'x' and inserts a new row
echo "ALTER TABLE t ADD COLUMN x TEXT;
UPDATE t SET x='val_x' WHERE id=1;
INSERT INTO t VALUES(2,'main_new','new_x');
SELECT dolt_commit('-A','-m','add x with new row');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 adds column 'y' and inserts a different new row
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN y INTEGER;
UPDATE t SET y=7 WHERE id=1;
INSERT INTO t VALUES(3,'b1_new',77);
SELECT dolt_commit('-A','-m','add y with new row');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_merge('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Row 1 should have both x and y values
run_test "schema_data_migrate_both_cols" \
  "SELECT x, y FROM t WHERE id=1;" \
  "val_x|7" "$DB"

# Row 2 (added on main) should have x but NULL y
run_test "schema_data_migrate_main_row" \
  "SELECT x, typeof(y) FROM t WHERE id=2;" \
  "new_x|null" "$DB"

rm -f "$DB"

# Test 5: One branch adds column, other doesn't modify schema -> should succeed
DB=/tmp/test_bhv5_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds a column
echo "ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='hi';
SELECT dolt_commit('-A','-m','add extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 only changes data
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','data only');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_one_side_ok" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

rm -f "$DB"

# Test 6: Both branches modify different tables -> should succeed
DB=/tmp/test_bhv6_$$.db; rm -f "$DB"
echo "CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main changes t1
echo "UPDATE t1 SET v='A';
SELECT dolt_commit('-A','-m','t1 change');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 changes t2
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t2 SET v='X';
SELECT dolt_commit('-A','-m','t2 change');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "diff_tables_merge_ok" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

run_test "diff_tables_t1" "SELECT v FROM t1;" "A" "$DB"
run_test "diff_tables_t2" "SELECT v FROM t2;" "X" "$DB"

rm -f "$DB"

# Test 7: Both branches add same column with same definition -> merge succeeds
DB=/tmp/test_bhv7s_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds column 'z' TEXT
echo "ALTER TABLE t ADD COLUMN z TEXT;
UPDATE t SET z='main_z';
SELECT dolt_commit('-A','-m','add z on main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 adds same column 'z' TEXT
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN z TEXT;
UPDATE t SET z='b1_z';
SELECT dolt_commit('-A','-m','add z on b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
# Merge should succeed (same column added on both) — row conflict may occur
# since both modified the same row with different z values, but schema merge OK
run_test_match "schema_merge_same_col_same_def" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]|conflict" "$DB"

rm -f "$DB"

# Test 8: Both branches add same column with different types -> schema conflict
DB=/tmp/test_bhv8s_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds column 'w' as TEXT
echo "ALTER TABLE t ADD COLUMN w TEXT;
SELECT dolt_commit('-A','-m','add w TEXT');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 adds column 'w' as INTEGER
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN w INTEGER;
SELECT dolt_commit('-A','-m','add w INTEGER');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_merge_same_col_diff_type" \
  "SELECT dolt_merge('b1');" \
  "schema conflict" "$DB"

rm -f "$DB"

# Test 9: One branch adds column, other drops different column -> merge succeeds
DB=/tmp/test_bhv9s_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, extra TEXT);
INSERT INTO t VALUES(1,'a','e');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds a new column
echo "ALTER TABLE t ADD COLUMN newcol TEXT;
SELECT dolt_commit('-A','-m','add newcol');" | $DOLTLITE "$DB" > /dev/null 2>&1

# b1 only changes data (can't drop columns easily in SQLite, so just data change)
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='b';
SELECT dolt_commit('-A','-m','data change');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "schema_add_col_other_data" \
  "SELECT dolt_merge('b1');" \
  "^[0-9a-f]" "$DB"

rm -f "$DB"

# ============================================================
# dolt_at Working State Tests
# ============================================================

echo "--- dolt_at working state tests ---"

# Test 7: Uncommitted data on feature visible via dolt_at from main
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

# From main, dolt_at_t('feature') should see 3 rows (including uncommitted)
run_test "at_working_uncommitted_count" \
  "SELECT count(*) FROM dolt_at_t('feature');" \
  "3" "$DB"

# Verify the uncommitted row is there
run_test "at_working_uncommitted_row" \
  "SELECT v FROM dolt_at_t('feature') WHERE id=3;" \
  "uncommitted" "$DB"

rm -f "$DB"

# Test 8: Committed data visible via dolt_at
DB=/tmp/test_bhv8_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat commit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# From main, dolt_at_t('feature') should see 2 rows
run_test "at_committed_count" \
  "SELECT count(*) FROM dolt_at_t('feature');" \
  "2" "$DB"

run_test "at_committed_row" \
  "SELECT v FROM dolt_at_t('feature') WHERE id=2;" \
  "feat_row" "$DB"

rm -f "$DB"

# Test 9: dolt_at with commit hash should show that commit's state (not working)
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

# Get the commit hash for 'feature' branch
FEAT_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0;" | $DOLTLITE "$DB" 2>&1)

# By commit hash, should see only committed state (2 rows, not 3)
# But first we need the feature branch commit hash, not main's
# Switch to feature, get the hash, switch back
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
FEAT_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "at_commit_hash_count" \
  "SELECT count(*) FROM dolt_at_t('$FEAT_HASH');" \
  "2" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
