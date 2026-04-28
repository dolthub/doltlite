#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }

echo "=== Doltlite dolt_log Tests ==="
echo ""

DB1=/tmp/test_log1_$$.db; rm -f "$DB1"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); INSERT INTO t VALUES(2,'c2'); SELECT dolt_commit('-A','-m','c2'); SELECT dolt_tag('v1','HEAD~1'); SELECT dolt_branch('from_tag','v1');" | $DOLTLITE "$DB1" > /dev/null 2>&1
run_test "log_from_tag_branch_top_message_after_reopen" "SELECT message FROM dolt_log LIMIT 1;" "c1" "$DB1/from_tag"
run_test "log_from_tag_branch_count_after_reopen" "SELECT count(*) FROM dolt_log;" "2" "$DB1/from_tag"

DB2=/tmp/test_log2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat1'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,'main'); SELECT dolt_commit('-A','-m','main2'); SELECT dolt_merge('feat'); SELECT dolt_branch('from_p1','HEAD^1'); SELECT dolt_branch('from_p2','HEAD^2');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "log_from_first_parent_branch_top_message_after_reopen" "SELECT message FROM dolt_log LIMIT 1;" "main2" "$DB2/from_p1"
run_test "log_from_second_parent_branch_top_message_after_reopen" "SELECT message FROM dolt_log LIMIT 1;" "feat1" "$DB2/from_p2"
run_test "log_from_second_parent_branch_count_after_reopen" "SELECT count(*) FROM dolt_log;" "3" "$DB2/from_p2"

DB3=/tmp/test_log3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); INSERT INTO t VALUES(2,'c2'); SELECT dolt_commit('-A','-m','c2'); SELECT dolt_tag('v1');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test "tag_does_not_change_log_count_after_reopen" "SELECT count(*) FROM dolt_log;" "3" "$DB3"
run_test "tag_does_not_change_log_top_message_after_reopen" "SELECT message FROM dolt_log LIMIT 1;" "c2" "$DB3"

rm -f "$DB1" "$DB2" "$DB3"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
