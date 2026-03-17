#!/bin/bash
DOLTLITE=./doltlite
PASS=0
FAIL=0
ERRORS=""

run_test() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="${4:-:memory:}"
  local result
  result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  local exit_code=$?
  if [ $exit_code -eq 137 ] || [ $exit_code -eq 139 ]; then
    result="CRASH (exit $exit_code)"
  fi
  if [ "$result" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"
  fi
}

# Match a pattern (for commit hashes which are nondeterministic)
run_test_match() {
  local name="$1"
  local sql="$2"
  local pattern="$3"
  local db="${4:-:memory:}"
  local result
  result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if echo "$result" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"
  fi
}

echo "=== Doltlite Commit & Log Tests ==="
echo ""

DB=/tmp/test_dolt_commit_$$.db
rm -f "$DB"

# --- dolt_commit basics ---

run_test_match "commit_returns_hash" \
  "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A', '-m', 'init');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "commit_requires_message" \
  "SELECT dolt_commit();" \
  "Error near line 1: dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')" "$DB"

# --- dolt_log basics ---

run_test_match "log_shows_commit" \
  "SELECT message FROM dolt_log;" \
  "init" "$DB"

run_test_match "log_has_committer" \
  "SELECT committer FROM dolt_log;" \
  "doltlite" "$DB"

run_test_match "log_has_hash" \
  "SELECT commit_hash FROM dolt_log;" \
  "^[0-9a-f]{40}$" "$DB"

# --- Multiple commits ---

run_test_match "second_commit" \
  "INSERT INTO t VALUES(2); SELECT dolt_commit('-A', '-m', 'add row 2');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "log_count_two" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB"

run_test_match "log_order" \
  "SELECT message FROM dolt_log;" \
  "add row 2" "$DB"

# --- Author flag ---

run_test_match "commit_with_author" \
  "INSERT INTO t VALUES(3); SELECT dolt_commit('-A', '-m', 'add 3', '--author', 'Alice <alice@test.com>');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "author_name" \
  "SELECT committer FROM dolt_log LIMIT 1;" \
  "Alice" "$DB"

run_test "author_email" \
  "SELECT email FROM dolt_log LIMIT 1;" \
  "alice@test.com" "$DB"

run_test "log_count_three" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB"

# --- Data persists across reopen ---

DB2=/tmp/test_dolt_persist_$$.db
rm -f "$DB2"

echo "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES(1,'hat'),(2,'coat'); SELECT dolt_commit('-A', '-m', 'create items');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "persist_data" \
  "SELECT name FROM items ORDER BY id;" \
  "hat
coat" "$DB2"

run_test "persist_log" \
  "SELECT message FROM dolt_log;" \
  "create items" "$DB2"

# --- Commit after schema change ---

run_test_match "commit_after_alter" \
  "ALTER TABLE items ADD COLUMN price REAL DEFAULT 0; SELECT dolt_commit('-A', '-m', 'add price column');" \
  "^[0-9a-f]{40}$" "$DB2"

run_test "log_after_alter" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB2"

# --- Empty log before first commit ---

run_test "empty_log" \
  "SELECT count(*) FROM dolt_log;" \
  "0" ":memory:"

# --- Commit with no changes (should fail) ---

DB3=/tmp/test_dolt_nochange_$$.db
rm -f "$DB3"

echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A', '-m', 'first');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "commit_no_changes" \
  "SELECT dolt_commit('-m', 'no changes');" \
  "Error near line 1: nothing to commit, working tree clean (use dolt_add to stage changes)" "$DB3"

run_test "one_commit_after_no_change" \
  "SELECT count(*) FROM dolt_log;" \
  "1" "$DB3"

# --- Multiple tables ---

DB4=/tmp/test_dolt_multi_$$.db
rm -f "$DB4"

run_test_match "multi_table_commit" \
  "CREATE TABLE a(x); CREATE TABLE b(y); INSERT INTO a VALUES(1); INSERT INTO b VALUES(2); SELECT dolt_commit('-A', '-m', 'two tables');" \
  "^[0-9a-f]{40}$" "$DB4"

run_test "multi_table_data_a" \
  "SELECT * FROM a;" \
  "1" "$DB4"

run_test "multi_table_data_b" \
  "SELECT * FROM b;" \
  "2" "$DB4"

# --- dolt_log columns ---

run_test "log_column_count" \
  "SELECT count(*) FROM pragma_table_info('dolt_log');" \
  "5" ":memory:"

# Verify we can SELECT specific columns
run_test_match "log_select_columns" \
  "SELECT commit_hash, committer, email, date, message FROM dolt_log LIMIT 1;" \
  "." "$DB"

# --- Cleanup ---

rm -f "$DB" "$DB2" "$DB3" "$DB4"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
