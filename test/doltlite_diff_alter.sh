#!/bin/bash
# Tests for dolt_diff across ALTER TABLE ADD COLUMN schema changes.
# Verifies that rows not modified after ADD COLUMN are NOT reported
# as changed, and rows actually modified ARE reported correctly.
DOLTLITE=./doltlite
PASS=0
FAIL=0
ERRORS=""

run_test() {
  local name="$1" sql="$2" expected="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if [ "$result" = "$expected" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"; fi
}

run_test_match() {
  local name="$1" sql="$2" pattern="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if echo "$result" | grep -qE "$pattern"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"; fi
}

echo "=== Doltlite Diff Across ALTER TABLE Tests ==="
echo ""

# ---------------------------------------------------------------
# Test 1: ALTER TABLE ADD COLUMN, no row updates => empty diff
# ---------------------------------------------------------------
DB1=/tmp/test_diff_alter1_$$.db; rm -f "$DB1"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'alice'),(2,'bob'),(3,'carol');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB1" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN age INTEGER;" | $DOLTLITE "$DB1" > /dev/null 2>&1

# Working diff should be empty -- only schema changed, no data
run_test "alter_add_col_no_data_change" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "0" "$DB1"

# ---------------------------------------------------------------
# Test 2: ALTER TABLE ADD COLUMN + update some rows => only
#         updated rows show up in diff
# ---------------------------------------------------------------
echo "UPDATE t SET age=30 WHERE id=1;" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "alter_only_updated_row_in_diff" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "1" "$DB1"

run_test "alter_updated_row_is_modify" \
  "SELECT diff_type, rowid_val FROM dolt_diff('t');" \
  "modified|1" "$DB1"

# ---------------------------------------------------------------
# Test 3: Commit after ALTER + update, then cross-commit diff
#         should show only the one modified row
# ---------------------------------------------------------------
echo "SELECT dolt_commit('-A','-m','add age column and update alice');" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "alter_cross_commit_count" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB1"

run_test "alter_cross_commit_type" \
  "SELECT diff_type FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "modified" "$DB1"

# ---------------------------------------------------------------
# Test 4: Multiple ADD COLUMNs, untouched rows stay equal
# ---------------------------------------------------------------
DB2=/tmp/test_diff_alter2_$$.db; rm -f "$DB2"

echo "CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO items VALUES(1,'hat'),(2,'coat');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1

echo "ALTER TABLE items ADD COLUMN price REAL;
ALTER TABLE items ADD COLUMN qty INTEGER;" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "multi_add_col_no_change" \
  "SELECT count(*) FROM dolt_diff('items');" \
  "0" "$DB2"

echo "UPDATE items SET price=9.99, qty=5 WHERE id=2;" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "multi_add_col_one_update" \
  "SELECT count(*) FROM dolt_diff('items');" \
  "1" "$DB2"

run_test "multi_add_col_correct_row" \
  "SELECT rowid_val FROM dolt_diff('items');" \
  "2" "$DB2"

# ---------------------------------------------------------------
# Test 5: dolt_diff_<table> also works across ALTER TABLE
# ---------------------------------------------------------------
echo "SELECT dolt_commit('-A','-m','update coat price');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "diff_table_across_alter_count" \
  "SELECT count(*) FROM dolt_diff_items WHERE diff_type='modified';" \
  "1" "$DB2"

# ---------------------------------------------------------------
# Test 6: INSERT after ALTER TABLE => shows as 'added', not spurious
# ---------------------------------------------------------------
DB3=/tmp/test_diff_alter3_$$.db; rm -f "$DB3"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN w TEXT;
INSERT INTO t VALUES(2,'y','z');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "insert_after_alter_count" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "1" "$DB3"

run_test "insert_after_alter_type" \
  "SELECT diff_type, rowid_val FROM dolt_diff('t');" \
  "added|2" "$DB3"

# ---------------------------------------------------------------
# Test 7: DELETE after ALTER TABLE => shows as 'removed'
# ---------------------------------------------------------------
DB4=/tmp/test_diff_alter4_$$.db; rm -f "$DB4"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN extra TEXT;
DELETE FROM t WHERE id=1;" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "delete_after_alter_count" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "1" "$DB4"

run_test "delete_after_alter_type" \
  "SELECT diff_type, rowid_val FROM dolt_diff('t');" \
  "removed|1" "$DB4"

# ---------------------------------------------------------------
# Test 8: Merge after ALTER TABLE (regression test for segfault)
# ---------------------------------------------------------------
DB5=/tmp/test_diff_alter5_$$.db; rm -f "$DB5"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'alice'),(2,'bob');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1

# Main branch: add column and update
echo "ALTER TABLE t ADD COLUMN age INTEGER;
UPDATE t SET age=30 WHERE id=1;
SELECT dolt_commit('-A','-m','add age on main');" | $DOLTLITE "$DB5" > /dev/null 2>&1

# Feature branch: modify a different row (no schema change)
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "UPDATE t SET name='BOB' WHERE id=2;
SELECT dolt_commit('-A','-m','update bob on feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB5" > /dev/null 2>&1

# Merge should not segfault
run_test_match "merge_after_alter_no_crash" \
  "SELECT dolt_merge('feature');" \
  "." "$DB5"

# After merge, diff between merge commit and its parent should work
run_test_match "diff_after_merge_works" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9]+$" "$DB5"

# Cleanup
rm -f "$DB1" "$DB2" "$DB3" "$DB4" "$DB5"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
