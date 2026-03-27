#!/bin/bash
DOLTLITE=./doltlite
PASS=0
FAIL=0
ERRORS=""

run_test() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="$4"
  local result
  result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if [ "$result" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"
  fi
}

run_test_match() {
  local name="$1"
  local sql="$2"
  local pattern="$3"
  local db="$4"
  local result
  result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if echo "$result" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"
  fi
}

echo "=== Doltlite Staging Workflow Tests ==="
echo ""

# --- Setup: create a database with initial commit ---
DB=/tmp/test_staging_$$.db
rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO users VALUES(1,'Alice'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# --- dolt_status: clean after commit ---
run_test "status_clean_after_commit" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

# --- dolt_status: detect unstaged insert ---
echo "INSERT INTO users VALUES(2,'Bob');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_unstaged_modify" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "users|0|modified" "$DB"

# --- dolt_add: stage the change ---
echo "SELECT dolt_add('users');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_after_add" \
  "SELECT table_name, staged, status FROM dolt_status;" \
  "users|1|modified" "$DB"

# --- dolt_commit: commit staged ---
run_test_match "commit_staged" \
  "SELECT dolt_commit('-m','Add Bob');" \
  "^[0-9a-f]{40}$" "$DB"

# --- Status clean after commit ---
run_test "status_clean_after_staged_commit" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

# --- Log has 2 commits ---
run_test "log_two_commits" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB"

# --- New table shows as 'new table' ---
echo "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT); INSERT INTO orders VALUES(1,'hat');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_new_table" \
  "SELECT table_name, staged, status FROM dolt_status WHERE table_name='orders';" \
  "orders|0|new table" "$DB"

# --- Stage only orders, users untouched ---
echo "SELECT dolt_add('orders');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_orders_staged" \
  "SELECT table_name, staged FROM dolt_status WHERE table_name='orders';" \
  "orders|1" "$DB"

# --- Commit orders ---
run_test_match "commit_orders" \
  "SELECT dolt_commit('-m','Add orders');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "log_three_commits" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB"

# --- dolt_add -A stages everything ---
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

# --- dolt_commit -A shortcut (stage+commit in one call) ---
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
  "2" "$DB3"

# --- Commit without staging fails ---
DB4=/tmp/test_staging4_$$.db
rm -f "$DB4"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test_match "commit_without_add_fails" \
  "SELECT dolt_commit('-m','should fail');" \
  "nothing to commit" "$DB4"

# --- After failed commit, data is still there ---
run_test "data_survives_failed_commit" \
  "SELECT count(*) FROM t;" \
  "2" "$DB4"

# --- Partial staging: modify two tables, stage one ---
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

# --- Persistence: staged state survives reopen ---
DB6=/tmp/test_staging6_$$.db
rm -f "$DB6"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_add('t');" | $DOLTLITE "$DB6" > /dev/null 2>&1

# Reopen and check staged state
run_test "staged_persists" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB6"

# --- dolt_add dot (.) stages everything ---
DB7=/tmp/test_staging7_$$.db
rm -f "$DB7"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB7" > /dev/null 2>&1

echo "SELECT dolt_add('.');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test "add_dot_stages_all" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB7"

# --- dolt_add('-a') lowercase flag ---
DB8=/tmp/test_staging_8_$$.db
rm -f "$DB8"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB8" > /dev/null 2>&1

echo "SELECT dolt_add('-a');" | $DOLTLITE "$DB8" > /dev/null 2>&1
run_test "add_lowercase_a_stages_all" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB8"

# --- Cleanup ---
rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
