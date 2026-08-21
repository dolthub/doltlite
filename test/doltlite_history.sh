#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_history_<table> Tests ==="
echo ""

DB=/tmp/test_hist_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Alice');
INSERT INTO t VALUES(2,'Bob');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "basic_count" "SELECT count(*) FROM dolt_history_t;" "2" "$DB"
run_test_match "basic_hash" "SELECT commit_hash FROM dolt_history_t LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
run_test_match "basic_date" "SELECT commit_date FROM dolt_history_t LIMIT 1;" "^[0-9]{4}-" "$DB"
run_test "basic_committer" "SELECT committer FROM dolt_history_t LIMIT 1;" "doltlite" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_count" "SELECT count(*) FROM dolt_history_t;" "6" "$DB"

run_test "multi_row1" "SELECT count(*) FROM dolt_history_t WHERE id=1;" "3" "$DB"

run_test "multi_row2" "SELECT count(*) FROM dolt_history_t WHERE id=2;" "2" "$DB"

run_test "multi_row3" "SELECT count(*) FROM dolt_history_t WHERE id=3;" "1" "$DB"

run_test "multi_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "3" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='v2' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='v3' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "update_count" "SELECT count(*) FROM dolt_history_t WHERE id=1;" "3" "$DB"

run_test "update_distinct" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "3" "$DB"

run_test_match "update_type" "SELECT typeof(v) FROM dolt_history_t LIMIT 1;" "text" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "persist_count" "SELECT count(*) FROM dolt_history_t;" "3" "$DB"
run_test "persist_row1" "SELECT count(*) FROM dolt_history_t WHERE id=1;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_tables_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO orders VALUES(2,'Gadget');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tables_users" "SELECT count(*) FROM dolt_history_users;" "1" "$DB"
run_test "tables_orders" "SELECT count(*) FROM dolt_history_orders;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_author_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init','--author','TestUser <test@test.com>');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "author_name" "SELECT committer FROM dolt_history_t LIMIT 1;" "TestUser" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "merge_count" "SELECT count(*) FROM dolt_history_t;" "^[6-9]" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_ref_branches_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'c2');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v1','HEAD~1');
SELECT dolt_branch('from_tag','v1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "history_from_tag_branch_reopen_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "1" "$DB/from_tag"

rm -f "$DB"

DB=/tmp/test_hist_parent_branches_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main2');
SELECT dolt_merge('feat');
SELECT dolt_branch('from_p1','HEAD^1');
SELECT dolt_branch('from_p2','HEAD^2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "history_from_first_parent_branch_reopen_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "2" "$DB/from_p1"
run_test "history_from_second_parent_branch_reopen_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "2" "$DB/from_p2"

HASH=$(echo "SELECT dolt_hashof('HEAD^2');" | $DOLTLITE "$DB" 2>&1)
run_test "history_filter_second_parent_hash" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t WHERE commit_hash='$HASH';" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_noncurrent_$$.db; rm -f "$DB"
echo "SELECT dolt_checkout('-b','feature');
CREATE TABLE feature_only(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO feature_only VALUES(1,'one');
SELECT dolt_commit('-A','-m','feature one');
INSERT INTO feature_only VALUES(2,'two');
SELECT dolt_commit('-A','-m','feature two');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "noncurrent_default_starts_at_head" "SELECT count(*) FROM dolt_history_feature_only;" "0" "$DB"
run_test "noncurrent_start_ref" "SELECT count(*) FROM dolt_history_feature_only('feature');" "3" "$DB"
run_test "noncurrent_exact_commit" "SELECT count(*) FROM dolt_history_feature_only WHERE commit_hash=dolt_hashof('feature');" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_join_order_$$.db; rm -f "$DB"
echo "CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO items VALUES(1,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_checkout('-b','feature');
INSERT INTO items VALUES(2,'feature-transient');
SELECT dolt_commit('-A','-m','feature first');
DELETE FROM items WHERE id=2;
INSERT INTO items VALUES(3,'feature-head');
SELECT dolt_commit('-A','-m','feature head');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

ANCESTRY="WITH RECURSIVE ancestry(commit_hash) AS (
  SELECT dolt_hashof('feature')
  UNION ALL
  SELECT parents.parent_hash
  FROM ancestry
  JOIN dolt_commit_ancestors parents
    ON parents.commit_hash=ancestry.commit_hash
   AND parents.parent_index=0
  WHERE parents.parent_hash IS NOT NULL
)"

run_test "history_join_noncurrent_uses_session_head" \
  "$ANCESTRY SELECT count(*) FROM ancestry JOIN dolt_history_items history ON history.commit_hash=ancestry.commit_hash;" \
  "1" "$DB"
run_test "history_cross_join_noncurrent_uses_session_head" \
  "$ANCESTRY SELECT count(*) FROM ancestry CROSS JOIN dolt_history_items history WHERE history.commit_hash=ancestry.commit_hash;" \
  "1" "$DB"
run_test "history_join_explicit_ref" \
  "$ANCESTRY SELECT count(*) FROM ancestry JOIN dolt_history_items('feature') history ON history.commit_hash=ancestry.commit_hash;" \
  "5" "$DB"
run_test "history_cross_join_explicit_ref" \
  "$ANCESTRY SELECT count(*) FROM ancestry CROSS JOIN dolt_history_items('feature') history WHERE history.commit_hash=ancestry.commit_hash;" \
  "5" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_merge_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1,'feat');
SELECT dolt_commit('-A','-m','feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main_check');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "history_merge_replay_u_rows" "SELECT count(*) FROM dolt_history_u;" "2" "$DB"
run_test "history_merge_replay_u_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_u;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_cherrypick_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1,'feat');
SELECT dolt_commit('-A','-m','feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main_check');
SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "history_cherrypick_replay_u_rows" "SELECT count(*) FROM dolt_history_u;" "1" "$DB"
run_test "history_cherrypick_replay_u_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_u;" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_revert_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main_check');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1,'later');
SELECT dolt_commit('-A','-m','add_u');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main_check' LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "history_revert_replay_u_rows" "SELECT count(*) FROM dolt_history_u;" "2" "$DB"
run_test "history_revert_replay_u_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_u;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_rebase_replay_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1,'feat');
SELECT dolt_commit('-A','-m','feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-A','-m','main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "history_rebase_replay_u_rows" "SELECT count(*) FROM dolt_history_u;" "1" "$DB/feat"
run_test "history_rebase_replay_u_distinct_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_u;" "1" "$DB/feat"

rm -f "$DB"

DB=/tmp/test_hist_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_count" "SELECT count(*) FROM dolt_history_t;" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_long_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 5); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

run_test "long_count" "SELECT count(*) FROM dolt_history_t;" "6" "$DB"
run_test "long_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "6" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_late_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "late_t2" "SELECT count(*) FROM dolt_history_t2;" "1" "$DB"

run_test "late_t" "SELECT count(*) FROM dolt_history_t;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_quoted_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE "odd""name"(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO "odd""name" VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
EOF

run_test "quoted_count" 'SELECT count(*) FROM "dolt_history_odd""name";' "1" "$DB"
run_test "quoted_value" 'SELECT v FROM "dolt_history_odd""name";' "a" "$DB"

rm -f "$DB"

DB=/tmp/test_hist_filter_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "filter_row1" "SELECT count(*) FROM dolt_history_t WHERE id=1;" "2" "$DB"
run_test "filter_row3" "SELECT count(*) FROM dolt_history_t WHERE id=3;" "1" "$DB"

run_test_match "filter_commit" \
  "SELECT count(*) FROM dolt_history_t WHERE commit_hash=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "^3$" "$DB"

rm -f "$DB"

# Historical root with a different key shape used to return raw sort-key bytes and drop PK filters.
DB=/tmp/test_hist_keyshape_$$.db; rm -f "$DB"
echo "CREATE TABLE s(a TEXT PRIMARY KEY, b TEXT) WITHOUT ROWID;
INSERT INTO s VALUES('x','one'),('y','two');
SELECT dolt_commit('-Am','v1');
DROP TABLE s;
CREATE TABLE s(a INTEGER PRIMARY KEY, b TEXT);
INSERT INTO s VALUES(1,'one'),(2,'two'),(7,'seven');
SELECT dolt_commit('-Am','v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "keyshape_history_eq_filters" \
  "SELECT a, b FROM dolt_history_s WHERE a=7;" \
  "7|seven" "$DB"
run_test "keyshape_history_upper_bound" \
  "SELECT count(*) FROM dolt_history_s WHERE a<'z';" \
  "2" "$DB"
run_test "keyshape_history_values_render" \
  "SELECT a FROM dolt_history_s ORDER BY a;" \
  "1
2
7
x
y" "$DB"

rm -f "$DB"


DBH=/tmp/test_hist_namemap_$$.db; rm -f "$DBH"
echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'BEE','CEE');
SELECT dolt_commit('-Am','base');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-Am','drop_b');" | $DOLTLITE "$DBH" > /dev/null 2>&1

run_test "hist_namemap_old_commit" \
  "SELECT group_concat(a || '=' || c, ',') FROM (SELECT a, c FROM dolt_history_t ORDER BY commit_date);" \
  "1=CEE,1=CEE" "$DBH"

rm -f "$DBH"

dltest_finish