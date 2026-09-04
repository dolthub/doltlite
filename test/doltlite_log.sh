#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

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

DB4=/tmp/test_log4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-Am','base'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2); SELECT dolt_commit('-Am','feat-only'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test "hash_filter_excludes_unreachable_commit" "SELECT count(*) FROM dolt_log WHERE commit_hash=dolt_hashof('feat');" "0" "$DB4"
run_test "hash_filter_matches_unpushed_predicate" "SELECT count(*) FROM dolt_log WHERE (commit_hash||'')=dolt_hashof('feat');" "0" "$DB4"
run_test "hash_filter_includes_reachable_commit" "SELECT count(*) FROM dolt_log WHERE commit_hash=dolt_hashof('main');" "1" "$DB4"
run_test "hash_filter_honors_explicit_revision" "SELECT count(*) FROM dolt_log('feat') WHERE commit_hash=dolt_hashof('feat');" "1" "$DB4"

DB5=/tmp/test_log5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','c1','--date','2020-01-01T00:00:00');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-Am','feat1','--date','2020-01-02T00:00:00');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_commit('-Am','main1','--date','2020-01-03T00:00:00');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(4);
SELECT dolt_commit('-Am','feat2','--date','2020-01-04T00:00:00');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB5" > /dev/null 2>&1
run_test "log_merge_orders_by_height_then_date" \
  "SELECT group_concat(message,'|') FROM (SELECT message FROM dolt_log);" \
  "Merge branch 'feat' into main|feat2|main1|feat1|c1|Initialize data repository" "$DB5"
run_test "log_merge_order_limit_takes_newest" \
  "SELECT group_concat(message,'|') FROM (SELECT message FROM dolt_log LIMIT 3);" \
  "Merge branch 'feat' into main|feat2|main1" "$DB5"

DB6=/tmp/test_log6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','c1','--date','2020-01-01T00:00:00');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-Am','main1','--date','2020-01-02T00:00:00');
INSERT INTO t VALUES(3);
SELECT dolt_commit('-Am','main2','--date','2020-01-03T00:00:00');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(9);
SELECT dolt_commit('-Am','feat_dated_2030','--date','2030-06-01T00:00:00');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test "log_height_outranks_a_later_date" \
  "SELECT group_concat(message,'|') FROM (SELECT message FROM dolt_log);" \
  "Merge branch 'feat' into main|main2|feat_dated_2030|main1|c1|Initialize data repository" "$DB6"

rm -f "$DB1" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6"
dltest_finish
