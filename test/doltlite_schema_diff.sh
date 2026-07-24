#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Schema Diff Tests ==="
echo ""

DB=/tmp/test_sd_add_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'b');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "add_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "1" "$DB"
run_test "add_to_name" "SELECT to_table_name FROM dolt_schema_diff('v1','v2');" "t2" "$DB"
run_test "add_from_name_empty" "SELECT length(from_table_name) FROM dolt_schema_diff('v1','v2');" "0" "$DB"
run_test_match "add_to_sql" \
  "SELECT to_create_statement FROM dolt_schema_diff('v1','v2');" "CREATE TABLE t2" "$DB"
run_test "add_from_sql_empty" \
  "SELECT length(from_create_statement) FROM dolt_schema_diff('v1','v2');" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_multi_$$.db; rm -f "$DB"
echo "SELECT dolt_commit('-A','-m','empty');
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','add 3 tables');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_count" \
  "SELECT count(*) FROM dolt_schema_diff((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1),(SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_none_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "none_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_badref_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "bad_to_ref_errors" \
  "SELECT count(*) FROM dolt_schema_diff((SELECT commit_hash FROM dolt_log LIMIT 1),'definitely_not_a_ref');" \
  "Error" "$DB"
run_test_match "bad_single_arg_errors" \
  "SELECT count(*) FROM dolt_schema_diff('definitely_not_a_ref');" \
  "Error" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-A','-m','feat add table');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_count" "SELECT count(*) FROM dolt_schema_diff('main','feat');" "1" "$DB"
run_test "branch_to_name" "SELECT to_table_name FROM dolt_schema_diff('main','feat');" "feat_tbl" "$DB"
run_test "branch_from_name_empty" "SELECT length(from_table_name) FROM dolt_schema_diff('main','feat');" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_reverse_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE TABLE extra(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reverse_fwd_to_name" "SELECT to_table_name FROM dolt_schema_diff('v1','v2');" "extra" "$DB"
run_test "reverse_fwd_from_empty" "SELECT length(from_table_name) FROM dolt_schema_diff('v1','v2');" "0" "$DB"

run_test "reverse_rev_from_name" "SELECT from_table_name FROM dolt_schema_diff('v2','v1');" "extra" "$DB"
run_test "reverse_rev_to_empty" "SELECT length(to_table_name) FROM dolt_schema_diff('v2','v1');" "0" "$DB"

run_test_match "reverse_from" \
  "SELECT from_create_statement FROM dolt_schema_diff('v2','v1');" "CREATE TABLE" "$DB"
run_test "reverse_to_empty_sql" \
  "SELECT length(to_create_statement) FROM dolt_schema_diff('v2','v1');" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_same_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('v1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "same_empty" "SELECT count(*) FROM dolt_schema_diff('v1','v1');" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_range_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "range_count" "SELECT count(*) FROM dolt_schema_diff('HEAD~1..HEAD');" "1" "$DB"
run_test "range_to_name" "SELECT to_table_name FROM dolt_schema_diff('HEAD~1..HEAD');" "u" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('before');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, x TEXT);
INSERT INTO feat_tbl VALUES(1,'a');
SELECT dolt_commit('-A','-m','feat table');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-A','-m','main work');
SELECT dolt_merge('feat');
SELECT dolt_tag('after');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "merge_added" \
  "SELECT to_table_name FROM dolt_schema_diff('before','after') WHERE from_table_name='';" \
  "feat_tbl" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_index_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE INDEX idx_name ON t(name);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "index_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "^[1-9]" "$DB"
run_test_match "index_name" \
  "SELECT to_table_name FROM dolt_schema_diff('v1','v2') LIMIT 1;" "idx_name" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_view_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE VIEW v AS SELECT * FROM t;
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "view_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "^[1-9]" "$DB"
run_test_match "view_name" \
  "SELECT to_table_name FROM dolt_schema_diff('v1','v2') WHERE to_table_name='v';" "v" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_rename_filter_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c1');
ALTER TABLE t RENAME TO t2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rename_filter_new_name" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD','t2');" "1" "$DB"
run_test "rename_filter_old_name" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD','t');" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE TABLE t2(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "persist_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_stmt_$$.db; rm -f "$DB"
echo "SELECT dolt_commit('-A','-m','empty');
SELECT dolt_tag('v1');
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);
SELECT dolt_commit('-A','-m','add users');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "stmt_content" \
  "SELECT to_create_statement FROM dolt_schema_diff('v1','v2');" \
  "CREATE TABLE users" "$DB"
run_test_match "stmt_cols" \
  "SELECT to_create_statement FROM dolt_schema_diff('v1','v2');" \
  "name TEXT" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_branch_from_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
SELECT dolt_branch('tagfeat','v1');
SELECT dolt_checkout('tagfeat');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-A','-m','tagfeat add col');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_from_tag_count" \
  "SELECT count(*) FROM dolt_schema_diff('v1','tagfeat');" "1" "$DB"
run_test "branch_from_tag_name" \
  "SELECT to_table_name FROM dolt_schema_diff('v1','tagfeat');" "t" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_parents_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-A','-m','feat add u');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_commit('-A','-m','main data');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "first_parent_to_merge_count" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD^1','HEAD');" "1" "$DB"
run_test "first_parent_to_merge_name" \
  "SELECT to_table_name FROM dolt_schema_diff('HEAD^1','HEAD');" "u" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_second_parent_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat data');
SELECT dolt_checkout('main');
CREATE TABLE m(id INTEGER PRIMARY KEY, y TEXT);
SELECT dolt_commit('-A','-m','main add m');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "second_parent_to_merge_count" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD^2','HEAD');" "1" "$DB"
run_test "second_parent_to_merge_name" \
  "SELECT to_table_name FROM dolt_schema_diff('HEAD^2','HEAD');" "m" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_drop_recreate_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, vv TEXT, extra INT);
INSERT INTO t VALUES(1,'recreated',7);
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "drop_recreate_same_name_count" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD');" "1" "$DB"
run_test_match "drop_recreate_from_stmt" \
  "SELECT from_create_statement FROM dolt_schema_diff('HEAD~1','HEAD');" \
  "CREATE TABLE t.*v TEXT" "$DB"
run_test_match "drop_recreate_to_stmt" \
  "SELECT to_create_statement FROM dolt_schema_diff('HEAD~1','HEAD');" \
  "CREATE TABLE t.*vv TEXT.*extra INT" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_merge_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES(1,'x');
SELECT dolt_commit('-A','-m','feat add u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main check');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_replay_u_count" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD^1','HEAD','u');" "1" "$DB"
run_test_match "merge_replay_u_to_stmt" \
  "SELECT to_create_statement FROM dolt_schema_diff('HEAD^1','HEAD','u');" \
  "CREATE TABLE u.*w TEXT" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_cherrypick_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES(1,'x');
SELECT dolt_commit('-A','-m','feat add u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main check');
SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cherrypick_replay_u_count" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD','u');" "1" "$DB"
run_test_match "cherrypick_replay_u_to_stmt" \
  "SELECT to_create_statement FROM dolt_schema_diff('HEAD~1','HEAD','u');" \
  "CREATE TABLE u.*w TEXT" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_revert_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main check');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES(1,'x');
SELECT dolt_commit('-A','-m','add u');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main check' LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "revert_replay_t_count" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD','t');" "1" "$DB"
run_test_match "revert_replay_from_stmt" \
  "SELECT from_create_statement FROM dolt_schema_diff('HEAD~1','HEAD','t');" \
  "CHECK ?\\(v > 0\\)" "$DB"
run_test_match "revert_replay_to_stmt" \
  "SELECT to_create_statement FROM dolt_schema_diff('HEAD~1','HEAD','t');" \
  "CREATE TABLE t.*v INT\\)" "$DB"

rm -f "$DB"

DB=/tmp/test_sd_rebase_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES(1,'x');
SELECT dolt_commit('-A','-m','feat add u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rebase_replay_u_count" \
  "SELECT count(*) FROM dolt_schema_diff('main','feat','u');" "1" "$DB"
run_test_match "rebase_replay_u_to_stmt" \
  "SELECT to_create_statement FROM dolt_schema_diff('main','feat','u');" \
  "CREATE TABLE u.*w TEXT" "$DB"

rm -f "$DB"

# WORKING / STAGED pseudo-refs must resolve like the other dolt_* surfaces.
# Before the catalog-aware resolver fix, schema_diff only accepted commit refs,
# so HEAD/WORKING (and HEAD..WORKING, HEAD/STAGED) errored with
# "to_ref 'WORKING' could not be resolved".
DB=/tmp/test_sd_working_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "CREATE TABLE w(id INTEGER PRIMARY KEY, x TEXT);" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "working_count" "SELECT count(*) FROM dolt_schema_diff('HEAD','WORKING');" "1" "$DB"
run_test "working_to_name" "SELECT to_table_name FROM dolt_schema_diff('HEAD','WORKING');" "w" "$DB"
run_test "working_from_empty" "SELECT length(from_table_name) FROM dolt_schema_diff('HEAD','WORKING');" "0" "$DB"
run_test "working_range_count" "SELECT count(*) FROM dolt_schema_diff('HEAD..WORKING');" "1" "$DB"
run_test "working_self_empty" "SELECT count(*) FROM dolt_schema_diff('WORKING','WORKING');" "0" "$DB"

echo "SELECT dolt_add('-A');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "staged_count" "SELECT count(*) FROM dolt_schema_diff('HEAD','STAGED');" "1" "$DB"
run_test "staged_to_name" "SELECT to_table_name FROM dolt_schema_diff('HEAD','STAGED');" "w" "$DB"

rm -f "$DB"

dltest_finish
