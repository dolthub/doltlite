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
echo "INSERT INTO t VALUES(3,'c'); UPDATE t SET val='A' WHERE id=1; SELECT dolt_commit('-A','-m','feat changes');" | $DOLTLITE "$DB3/feat" > /dev/null 2>&1

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
echo "INSERT INTO t VALUES(4,'d');" | $DOLTLITE "$DB3/feat" > /dev/null 2>&1
run_test "diff_branch_to_working" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "1" "$DB3/feat"

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

DB5=/tmp/test_diff5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat changes'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,'main'); SELECT dolt_commit('-A','-m','main changes'); SELECT dolt_merge('feat');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "diff_first_parent_ref" \
  "SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat('HEAD^1', 'HEAD', 't');" \
  "1|0" "$DB5"

run_test "diff_second_parent_ref" \
  "SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat('HEAD^2', 'HEAD', 't');" \
  "1|0" "$DB5"

DB6=/tmp/test_diff6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','c1'); INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','c2'); SELECT dolt_tag('v1','HEAD~1'); SELECT dolt_branch('from_tag','v1');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test "diff_branch_created_from_tag" \
  "SELECT rows_added || '|' || rows_modified FROM dolt_diff_stat('from_tag', 'main', 't');" \
  "1|0" "$DB6"

DB7=/tmp/test_diff7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','c1'); ALTER TABLE t RENAME TO u; CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); INSERT INTO t VALUES(7,'z'); SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test "diff_rename_recreate_family_count" \
  "SELECT count(*) FROM dolt_diff_stat('HEAD~1', 'HEAD');" \
  "2" "$DB7"

DB8=/tmp/test_diff8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_checkout('-b','feat'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'feat'); SELECT dolt_commit('-A','-m','feat_add_u'); SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); SELECT dolt_merge('feat');" | $DOLTLITE "$DB8" > /dev/null 2>&1

run_test "diff_merge_replay_stat" \
  "SELECT rows_added || '|' || rows_modified || '|' || rows_deleted FROM dolt_diff_stat('HEAD^1', 'HEAD', 'u');" \
  "1|0|0" "$DB8"
run_test "diff_merge_replay_summary" \
  "SELECT coalesce(from_table_name,'') || '|' || coalesce(to_table_name,'') || '|' || diff_type || '|' || data_change || '|' || schema_change FROM dolt_diff_summary('HEAD^1', 'HEAD', 'u');" \
  "|u|added|1|1" "$DB8"

DB9=/tmp/test_diff9_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_checkout('-b','feat'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'feat'); SELECT dolt_commit('-A','-m','feat_add_u'); SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB9" > /dev/null 2>&1

run_test "diff_cherrypick_replay_stat" \
  "SELECT rows_added || '|' || rows_modified || '|' || rows_deleted FROM dolt_diff_stat('HEAD~1', 'HEAD', 'u');" \
  "1|0|0" "$DB9"
run_test "diff_cherrypick_replay_summary" \
  "SELECT coalesce(from_table_name,'') || '|' || coalesce(to_table_name,'') || '|' || diff_type || '|' || data_change || '|' || schema_change FROM dolt_diff_summary('HEAD~1', 'HEAD', 'u');" \
  "|u|added|1|1" "$DB9"

DB10=/tmp/test_diff10_$$.db; rm -f "$DB10"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_checkout('-b','feat'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'feat'); SELECT dolt_commit('-A','-m','feat_add_u'); SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); SELECT dolt_checkout('feat'); SELECT dolt_rebase('main');" | $DOLTLITE "$DB10" > /dev/null 2>&1

run_test "diff_rebase_replay_stat" \
  "SELECT rows_added || '|' || rows_modified || '|' || rows_deleted FROM dolt_diff_stat('main', 'feat', 'u');" \
  "1|0|0" "$DB10/feat"
run_test "diff_rebase_replay_summary" \
  "SELECT coalesce(from_table_name,'') || '|' || coalesce(to_table_name,'') || '|' || diff_type || '|' || data_change || '|' || schema_change FROM dolt_diff_summary('main', 'feat', 'u');" \
  "|u|added|1|1" "$DB10/feat"

DB11=/tmp/test_diff11_$$.db; rm -f "$DB11"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'later'); SELECT dolt_commit('-A','-m','add_u'); SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main_check' LIMIT 1));" | $DOLTLITE "$DB11" > /dev/null 2>&1

run_test "diff_revert_schema_only_replay_stat_count" \
  "SELECT count(*) FROM dolt_diff_stat('HEAD~1', 'HEAD');" \
  "1" "$DB11"
run_test "diff_revert_schema_only_replay_stat_row" \
  "SELECT table_name || '|' || rows_added || '|' || rows_modified || '|' || rows_deleted FROM dolt_diff_stat('HEAD~1', 'HEAD');" \
  "t|0|0|0" "$DB11"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
