#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

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

run_test "diff_summary_no_such_table" \
  "SELECT count(*) FROM dolt_diff_summary('main','main','nonexistent');" \
  "0" "$DB"

run_test "diff_no_such_table" \
  "SELECT count(*) FROM dolt_diff WHERE table_name='nonexistent';" \
  "0" "$DB"

# Match Dolt: dolt_diff_stat errors when the filter names a table on neither side.
run_test_match "diff_stat_no_such_table" \
  "SELECT count(*) FROM dolt_diff_stat('main','main','nonexistent');" \
  "table not found: nonexistent" "$DB"

run_test_match "diff_bad_ref_errors" \
  "SELECT count(*) FROM dolt_diff_stat('definitely_not_a_ref', (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "dolt_diff_stat: ref not found: definitely_not_a_ref" "$DB"

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
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_checkout('-b','feat'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'feat'); SELECT dolt_commit('-A','-m','feat_add_u'); SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB9" > /dev/null 2>&1

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

DB12=/tmp/test_diff12_$$.db; rm -f "$DB12"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_checkout('-b','feat'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'feat'); SELECT dolt_commit('-A','-m','feat_add_u'); SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); SELECT dolt_merge('feat');" | $DOLTLITE "$DB12" > /dev/null 2>&1

run_test "diff_summary_merge_replay_count" \
  "SELECT count(*) FROM dolt_diff;" \
  "4" "$DB12"
run_test "diff_summary_merge_replay_merge_row" \
  "SELECT table_name || '|' || data_change || '|' || schema_change FROM dolt_diff WHERE message=\"Merge branch 'feat' into main\";" \
  "u|1|1" "$DB12"

DB13=/tmp/test_diff13_$$.db; rm -f "$DB13"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_commit('-A','-m','main_check'); CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'later'); SELECT dolt_commit('-A','-m','add_u'); SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main_check' LIMIT 1));" | $DOLTLITE "$DB13" > /dev/null 2>&1

run_test "diff_summary_revert_replay_count" \
  "SELECT count(*) FROM dolt_diff;" \
  "4" "$DB13"
run_test "diff_summary_revert_schema_only_row" \
  "SELECT table_name || '|' || data_change || '|' || schema_change FROM dolt_diff WHERE message='main_check';" \
  "t|0|1" "$DB13"

DB14=/tmp/test_diff14_$$.db; rm -f "$DB14"
echo "CREATE TABLE r(id INTEGER PRIMARY KEY, v REAL);
INSERT INTO r VALUES(1, 3.14);
SELECT dolt_commit('-A','-m','c1');
UPDATE r SET v = 3.14 WHERE id=1;" | $DOLTLITE "$DB14" > /dev/null 2>&1

run_test "diff_real_same_value_no_diff" \
  "SELECT count(*) FROM dolt_diff_r WHERE to_commit='WORKING';" \
  "0" "$DB14"

DB15=/tmp/test_diff15_$$.db; rm -f "$DB15"
echo "CREATE TABLE r(id INTEGER PRIMARY KEY, v REAL);
INSERT INTO r VALUES(1, 0.0);
SELECT dolt_commit('-A','-m','c1');
UPDATE r SET v = -0.0 WHERE id=1;" | $DOLTLITE "$DB15" > /dev/null 2>&1

run_test "diff_real_pos_neg_zero_no_diff" \
  "SELECT count(*) FROM dolt_diff_r WHERE to_commit='WORKING';" \
  "0" "$DB15"

DB16=/tmp/test_diff16_$$.db; rm -f "$DB16"
echo "CREATE TABLE r(id INTEGER PRIMARY KEY, v REAL);
INSERT INTO r VALUES(1, 1.5);
SELECT dolt_commit('-A','-m','c1');
UPDATE r SET v = 2.5 WHERE id=1;" | $DOLTLITE "$DB16" > /dev/null 2>&1

run_test "diff_real_distinct_values_modified" \
  "SELECT count(*) FROM dolt_diff_r WHERE to_commit='WORKING' AND diff_type='modified';" \
  "1" "$DB16"

DB17=/tmp/test_diff17_$$.db; rm -f "$DB17"
echo "CREATE TABLE r(id INTEGER PRIMARY KEY, v REAL);
INSERT INTO r VALUES(1, 0.0);
SELECT dolt_commit('-A','-m','c1');
UPDATE r SET v = -0.0 WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB17" > /dev/null 2>&1

run_test "diff_stat_real_pos_neg_zero_no_diff" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD~1', 'HEAD', 'r');" \
  "0" "$DB17"

DB18=/tmp/test_diff18_$$.db; rm -f "$DB18"
echo "CREATE TABLE baseline(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','baseline');
CREATE TABLE empty_t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','create_empty');
DROP TABLE empty_t;
SELECT dolt_commit('-A','-m','drop_empty');" | $DOLTLITE "$DB18" > /dev/null 2>&1

run_test "diff_empty_table_history" \
  "SELECT group_concat(message || '|' || data_change || '|' || schema_change, ',') FROM (SELECT message, data_change, schema_change FROM dolt_diff WHERE table_name || '' = 'empty_t' ORDER BY message);" \
  "create_empty|0|1,drop_empty|0|1" "$DB18"

run_test "diff_empty_table_history_filtered" \
  "SELECT group_concat(message || '|' || data_change || '|' || schema_change, ',') FROM (SELECT message, data_change, schema_change FROM dolt_diff WHERE table_name = 'empty_t' ORDER BY message);" \
  "create_empty|0|1,drop_empty|0|1" "$DB18"

echo "CREATE TABLE empty_working(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB18" > /dev/null 2>&1
run_test "diff_empty_table_working" \
  "SELECT data_change || '|' || schema_change FROM dolt_diff WHERE commit_hash='WORKING' AND table_name='empty_working';" \
  "0|1" "$DB18"

DB19=/tmp/test_diff19_$$.db; rm -f "$DB19"
$DOLTLITE "$DB19" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT);
INSERT INTO t VALUES(1,1);
CREATE VIEW v1 AS SELECT id FROM t;
SELECT dolt_commit('-Am','c1');
CREATE TABLE newt(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','c2 table only');
CREATE INDEX tx ON t(x);
SELECT dolt_commit('-Am','c3 index only');
CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;
SELECT dolt_commit('-Am','c4 trigger');
DROP VIEW v1;
CREATE VIEW v1 AS SELECT id, x FROM t;
SELECT dolt_commit('-Am','c5 view changed');
SQL
run_test "diff_table_added_does_not_list_untouched_view" \
  "SELECT group_concat(DISTINCT table_name) FROM dolt_diff WHERE commit_hash=dolt_hashof('HEAD~3');" \
  "newt" "$DB19"
run_test "diff_index_added_does_not_list_untouched_view" \
  "SELECT group_concat(DISTINCT table_name) FROM dolt_diff WHERE commit_hash=dolt_hashof('HEAD~2');" \
  "t" "$DB19"
run_test "diff_trigger_added_lists_dolt_schemas" \
  "SELECT group_concat(DISTINCT table_name) FROM dolt_diff WHERE commit_hash=dolt_hashof('HEAD~1');" \
  "dolt_schemas" "$DB19"
run_test "diff_view_changed_lists_dolt_schemas" \
  "SELECT group_concat(DISTINCT table_name) FROM dolt_diff WHERE commit_hash=dolt_hashof('HEAD');" \
  "dolt_schemas" "$DB19"
echo "CREATE TABLE later(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB19" > /dev/null 2>&1
run_test "status_table_added_does_not_list_untouched_view" \
  "SELECT group_concat(table_name||'|'||staged||'|'||status) FROM dolt_status;" \
  "later|0|new table" "$DB19"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11" "$DB12" "$DB13" "$DB14" "$DB15" "$DB16" "$DB17" "$DB18" "$DB19"

dltest_finish
