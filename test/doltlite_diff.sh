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

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_working_diff" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "0" "$DB"

echo "INSERT INTO t VALUES(4,'d');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "working_add" \
  "SELECT diff_type || '|' || coalesce(to_id, from_id) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "added|4" "$DB"

echo "DELETE FROM t WHERE id=2;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "working_add_and_delete" \
  "SELECT group_concat(diff_type, ',') FROM (SELECT diff_type FROM dolt_diff_t WHERE to_commit='WORKING' ORDER BY coalesce(to_id, from_id));" \
  "removed" "$DB"

echo "UPDATE t SET val='A' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "working_modify" \
  "SELECT diff_type FROM dolt_diff_t WHERE to_commit='WORKING' AND coalesce(to_id, from_id)=1;" \
  "modified" "$DB"

run_test "working_change_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "3" "$DB"

echo "SELECT dolt_commit('-A','-m','changes');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "working_empty_after_commit" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "0" "$DB"

run_test "diff_stat_between_commits" \
  "SELECT coalesce(sum(rows_added + rows_deleted + rows_modified), 0) FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "3" "$DB"

run_test "diff_summary_between_commits" \
  "SELECT data_change || '|' || schema_change FROM dolt_diff_summary((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "1|0" "$DB"

run_test "diff_no_such_table" \
  "SELECT count(*) FROM dolt_diff WHERE table_name='nonexistent';" \
  "0" "$DB"

run_test_match "diff_bad_ref_errors" \
  "SELECT count(*) FROM dolt_diff_stat('definitely_not_a_ref', (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "Error" "$DB"

DB2=/tmp/test_diff2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(x); SELECT dolt_commit('-A','-m','empty');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO t VALUES(1),(2),(3);" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "diff_all_adds" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "3" "$DB2"

run_test "diff_all_adds_type" \
  "SELECT DISTINCT diff_type FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "added" "$DB2"

echo "SELECT dolt_commit('-A','-m','added');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "DELETE FROM t;" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "diff_all_deletes" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "3" "$DB2"

run_test "diff_all_deletes_type" \
  "SELECT DISTINCT diff_type FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "removed" "$DB2"

DB3=/tmp/test_diff3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'c'); UPDATE t SET val='A' WHERE id=1; SELECT dolt_commit('-A','-m','feat changes');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "diff_branch_names" \
  "SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat('main', 'feat', 't');" \
  "1|1" "$DB3"

run_test "diff_branch_matches_hash" \
  "SELECT (SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat('main', 'feat', 't')) = (SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat((SELECT hash FROM dolt_branches WHERE name='main'), (SELECT hash FROM dolt_branches WHERE name='feat'), 't'));" \
  "1" "$DB3"

run_test "diff_mixed_ref_types" \
  "SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat('main', (SELECT hash FROM dolt_branches WHERE name='feat'), 't');" \
  "1|1" "$DB3"

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4,'d');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test "diff_branch_to_working" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "1" "$DB3"

DB4=/tmp/test_diff4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','v1');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_tag('v1');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','v2');" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "diff_tag_ref" \
  "SELECT rows_added FROM dolt_diff_stat('v1', 'main', 't');" \
  "1" "$DB4"

run_test "diff_tag_type" \
  "SELECT diff_type FROM dolt_diff_summary('v1', 'main', 't');" \
  "modified" "$DB4"

rm -f "$DB" "$DB2" "$DB3" "$DB4"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
