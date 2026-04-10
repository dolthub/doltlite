#!/bin/bash
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

echo "=== Doltlite Diff Tests ==="
echo ""

DB=/tmp/test_diff_$$.db; rm -f "$DB"

# Setup: create table, commit, make changes
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# No changes = empty diff
run_test "empty_diff" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "0" "$DB"

# Insert a row
echo "INSERT INTO t VALUES(4,'d');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "diff_add" \
  "SELECT diff_type, rowid_val FROM dolt_diff('t');" \
  "added|4" "$DB"

# Delete a row
echo "DELETE FROM t WHERE id=2;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "diff_add_and_delete" \
  "SELECT diff_type FROM dolt_diff('t') ORDER BY rowid_val;" \
  "removed" "$DB"

# Update a row
echo "UPDATE t SET val='A' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "diff_modify" \
  "SELECT diff_type FROM dolt_diff('t') WHERE rowid_val=1;" \
  "modified" "$DB"

# Count all changes
run_test "diff_count_changes" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "3" "$DB"

# Commit, then diff should be empty
echo "SELECT dolt_commit('-A','-m','changes');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "diff_empty_after_commit" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "0" "$DB"

# Diff between two commits
run_test "diff_between_commits" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

# Diff types between commits
run_test_match "diff_types_between_commits" \
  "SELECT diff_type FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1)) ORDER BY rowid_val;" \
  "modified" "$DB"

# Diff nonexistent table = empty
run_test "diff_no_such_table" \
  "SELECT count(*) FROM dolt_diff('nonexistent');" \
  "0" "$DB"

# Bad ref should surface an error, not an empty diff
run_test_match "diff_bad_ref_errors" \
  "SELECT count(*) FROM dolt_diff('t', 'definitely_not_a_ref', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "Error" "$DB"

# Diff with only adds (new table after commit)
DB2=/tmp/test_diff2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(x); SELECT dolt_commit('-A','-m','empty');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO t VALUES(1),(2),(3);" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "diff_all_adds" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "3" "$DB2"

run_test "diff_all_adds_type" \
  "SELECT DISTINCT diff_type FROM dolt_diff('t');" \
  "added" "$DB2"

# Diff with only deletes
echo "SELECT dolt_commit('-A','-m','added');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "DELETE FROM t;" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "diff_all_deletes" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "3" "$DB2"

run_test "diff_all_deletes_type" \
  "SELECT DISTINCT diff_type FROM dolt_diff('t');" \
  "removed" "$DB2"

# --- Diff with branch names as refs ---
DB3=/tmp/test_diff3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'c'); UPDATE t SET val='A' WHERE id=1; SELECT dolt_commit('-A','-m','feat changes');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "diff_branch_names" \
  "SELECT count(*) FROM dolt_diff('t', 'main', 'feat');" \
  "2" "$DB3"

run_test_match "diff_branch_names_types" \
  "SELECT diff_type FROM dolt_diff('t', 'main', 'feat') ORDER BY rowid_val;" \
  "modified" "$DB3"

run_test_match "diff_branch_names_added" \
  "SELECT diff_type FROM dolt_diff('t', 'main', 'feat') ORDER BY rowid_val;" \
  "added" "$DB3"

# Branch name result matches hex hash result
run_test "diff_branch_matches_hash" \
  "SELECT (SELECT group_concat(diff_type||rowid_val) FROM dolt_diff('t', 'main', 'feat'))
       = (SELECT group_concat(diff_type||rowid_val) FROM dolt_diff('t',
            (SELECT hash FROM dolt_branches WHERE name='main'),
            (SELECT hash FROM dolt_branches WHERE name='feat')));" \
  "1" "$DB3"

# Diff with one branch name and one hash
run_test "diff_mixed_ref_types" \
  "SELECT count(*) FROM dolt_diff('t', 'main',
    (SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "2" "$DB3"

# Diff from branch to working state (only from_commit, no to_commit)
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4,'d');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test "diff_branch_to_working" \
  "SELECT count(*) FROM dolt_diff('t');" \
  "1" "$DB3"

# Tag as ref
DB4=/tmp/test_diff4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','v1');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_tag('v1');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','v2');" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "diff_tag_ref" \
  "SELECT count(*) FROM dolt_diff('t', 'v1', 'main');" \
  "1" "$DB4"

run_test "diff_tag_type" \
  "SELECT diff_type FROM dolt_diff('t', 'v1', 'main');" \
  "added" "$DB4"

# Cleanup
rm -f "$DB" "$DB2" "$DB3" "$DB4"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
