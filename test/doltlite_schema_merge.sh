#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite ALTER TABLE Merge Tests ==="
echo ""

DB=/tmp/test_alter_merge1_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='new' WHERE id=1;
INSERT INTO t VALUES(2,'b','hello');
SELECT dolt_commit('-A','-m','add extra column');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "alter_merge_pre_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "0" "$DB"

run_test_match "alter_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"

run_test "alter_merge_post_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
run_test "alter_merge_row1" "SELECT extra FROM t WHERE id=1;" "new" "$DB"
run_test "alter_merge_row2" "SELECT extra FROM t WHERE id=2;" "hello" "$DB"
run_test "alter_merge_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_merge1b_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN "odd""name" TEXT;
UPDATE t SET "odd""name"='quoted' WHERE id=1;
SELECT dolt_commit('-A','-m','add quoted column');
SELECT dolt_checkout('main');
EOF

run_test_match "alter_merge_quoted_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "alter_merge_quoted_val" 'SELECT "odd""name" FROM t WHERE id=1;' "quoted" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_merge1c_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
ALTER TABLE t ADD COLUMN "main""col" TEXT;
UPDATE t SET "main""col"='left' WHERE id=1;
SELECT dolt_commit('-A','-m','main quoted add');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN "feat""col" TEXT;
UPDATE t SET "feat""col"='right' WHERE id=1;
SELECT dolt_commit('-A','-m','feat quoted add');
SELECT dolt_checkout('main');
EOF

run_test_match "alter_merge_escaped_quoted_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "alter_merge_escaped_main_val" 'SELECT "main""col" FROM t WHERE id=1;' "left" "$DB"
run_test "alter_merge_escaped_feat_val" 'SELECT "feat""col" FROM t WHERE id=1;' "right" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_merge2_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='main_val';
SELECT dolt_commit('-A','-m','main adds extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='feat_val';
SELECT dolt_commit('-A','-m','feat adds extra');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "alter_same_col_merge" "SELECT dolt_merge('feat');" "conflict|merge failed|Error" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_merge3_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN col_main TEXT;
UPDATE t SET col_main='m';
SELECT dolt_commit('-A','-m','main adds col_main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN col_feat TEXT;
UPDATE t SET col_feat='f';
SELECT dolt_commit('-A','-m','feat adds col_feat');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_diff_cols_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"

run_test_match "schema_diff_cols_has_col_main" \
  "SELECT col_main FROM t WHERE id=1;" \
  "m" "$DB"

run_test_match "schema_diff_cols_has_col_feat" \
  "SELECT typeof(col_feat) FROM t WHERE id=1;" \
  "null|text" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_merge4_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE keep(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO keep VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "DROP TABLE t;
SELECT dolt_commit('-A','-m','drop t');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','insert into t');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "drop_vs_modify_merge" "SELECT dolt_merge('feat');" "conflict|merge failed|Error" "$DB"

run_test "drop_vs_modify_keep" "SELECT w FROM keep WHERE id=1;" "x" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_cp_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='cp' WHERE id=1;
SELECT dolt_commit('-A','-m','alter add extra');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_alter_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "cp_alter_col_exists" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
run_test "cp_alter_val" "SELECT extra FROM t WHERE id=1;" "cp" "$DB"
run_test_match "cp_alter_msg" "SELECT message FROM dolt_log LIMIT 1;" "^alter add extra$" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_revert_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='rev' WHERE id=1;
SELECT dolt_commit('-A','-m','alter add extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "revert_alter_pre" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"

run_test_match "revert_alter_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "revert_alter_col_gone" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "0" "$DB"
run_test "revert_alter_data" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test_match "revert_alter_msg" "SELECT message FROM dolt_log LIMIT 1;" "Revert" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_cp_disjoint_table_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE base(id INTEGER PRIMARY KEY, v INT);
INSERT INTO base VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(k INTEGER PRIMARY KEY, w TEXT);
INSERT INTO feat_tbl VALUES(1,'x');
SELECT dolt_commit('-A','-m','feat adds table');
SELECT dolt_checkout('main');
CREATE TABLE base_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO base_new SELECT * FROM base;
DROP TABLE base;
ALTER TABLE base_new RENAME TO base;
SELECT dolt_commit('-A','-m','main adds check');
EOF

run_test_match "cp_disjoint_table_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_disjoint_table_exists" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='feat_tbl';" \
  "1" "$DB"
run_test "cp_disjoint_table_rows" "SELECT count(*) FROM feat_tbl;" "1" "$DB"
run_test "cp_disjoint_table_reopen" "SELECT count(*) FROM feat_tbl;" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_cp_disjoint_idx_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES(1,10);
INSERT INTO b VALUES(1,20);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_commit('-A','-m','feat idx');
SELECT dolt_checkout('main');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_commit('-A','-m','main idx');
EOF

run_test_match "cp_disjoint_idx_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_disjoint_idx_a" \
  "SELECT count(*) FROM pragma_index_list('a') WHERE name='idx_a_v';" \
  "1" "$DB"
run_test "cp_disjoint_idx_b" \
  "SELECT count(*) FROM pragma_index_list('b') WHERE name='idx_b_v';" \
  "1" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_revert_disjoint_table_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE base(id INTEGER PRIMARY KEY, v INT);
INSERT INTO base VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
CREATE TABLE feat_tbl(k INTEGER PRIMARY KEY, w TEXT);
INSERT INTO feat_tbl VALUES(1,'x');
SELECT dolt_commit('-A','-m','add table');
CREATE TABLE base_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO base_new SELECT * FROM base;
DROP TABLE base;
ALTER TABLE base_new RENAME TO base;
SELECT dolt_commit('-A','-m','add check');
EOF

run_test_match "revert_disjoint_table_hash" \
  "SELECT dolt_revert('HEAD~1');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "revert_disjoint_table_gone" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='feat_tbl';" \
  "0" "$DB"
run_test "revert_disjoint_table_base" "SELECT count(*) FROM base;" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_revert_disjoint_idx_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES(1,10);
INSERT INTO b VALUES(1,20);
SELECT dolt_commit('-A','-m','init');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_commit('-A','-m','add idx b');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_commit('-A','-m','add idx a');
EOF

run_test_match "revert_disjoint_idx_hash" \
  "SELECT dolt_revert('HEAD~1');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "revert_disjoint_idx_a" \
  "SELECT count(*) FROM pragma_index_list('a') WHERE name='idx_a_v';" \
  "1" "$DB"
run_test "revert_disjoint_idx_b" \
  "SELECT count(*) FROM pragma_index_list('b') WHERE name='idx_b_v';" \
  "0" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_diff_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('before');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='x' WHERE id=1;
INSERT INTO t VALUES(2,'b','y');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('after');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "diff_across_alter" \
  "SELECT diff_type FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1) LIMIT 1;" \
  "modified|added" "$DB"

run_test_match "diff_across_alter_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "^[1-9]" "$DB"

run_test_match "history_across_alter" \
  "SELECT count(*) FROM dolt_history_t;" \
  "^[2-9]" "$DB"

run_test "history_commits" \
  "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" \
  "2" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_at_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'old');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='new' WHERE id=1;
INSERT INTO t VALUES(2,'two','ext');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "at_before_alter_count" \
  "SELECT count(*) FROM dolt_at_t('v1');" \
  "1" "$DB"

run_test "at_after_alter_count" \
  "SELECT count(*) FROM dolt_at_t('v2');" \
  "2" "$DB"

run_test_match "at_after_alter_extra" \
  "SELECT extra FROM dolt_at_t('v2') WHERE id=1;" \
  "new" "$DB"

rm -f "$DB"

DB=/tmp/test_alter_sd_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_diff_alter_count" \
  "SELECT count(*) FROM dolt_schema_diff('v1','v2');" \
  "^[1-9]" "$DB"

run_test_match "schema_diff_alter_table" \
  "SELECT to_table_name FROM dolt_schema_diff('v1','v2') WHERE to_table_name='t';" \
  "^t$" "$DB"

run_test_match "schema_diff_alter_to_stmt" \
  "SELECT to_create_statement FROM dolt_schema_diff('v1','v2') WHERE to_table_name='t';" \
  "extra" "$DB"

run_test_match "schema_diff_alter_from_stmt" \
  "SELECT from_create_statement FROM dolt_schema_diff('v1','v2') WHERE to_table_name='t';" \
  "v TEXT" "$DB"

rm -f "$DB"

DB=/tmp/test_schema_merge10_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN x INTEGER;
UPDATE t SET x=10 WHERE id=1;
UPDATE t SET x=20 WHERE id=2;
SELECT dolt_commit('-A','-m','main adds x');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN y TEXT;
UPDATE t SET y='hello' WHERE id=1;
UPDATE t SET y='world' WHERE id=2;
SELECT dolt_commit('-A','-m','feat adds y');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_diff_cols_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"
run_test "schema_merge_diff_cols_x1" "SELECT x FROM t WHERE id=1;" "10" "$DB"
run_test "schema_merge_diff_cols_x2" "SELECT x FROM t WHERE id=2;" "20" "$DB"
run_test "schema_merge_diff_cols_y1" "SELECT typeof(y) FROM t WHERE id=1;" "text" "$DB"
run_test "schema_merge_diff_cols_count" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='x' OR name='y';" \
  "2" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_merge11_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-A','-m','main adds extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-A','-m','feat adds extra');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_same_col_converge" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"
run_test "schema_merge_same_col_exists" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_merge12_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t ADD COLUMN extra INTEGER;
SELECT dolt_commit('-A','-m','main adds extra INTEGER');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-A','-m','feat adds extra TEXT');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_same_col_diff_type" "SELECT dolt_merge('feat');" "schema conflict|conflict|Error" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_merge13_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(3,'c');
UPDATE t SET v='a2' WHERE id=1;
SELECT dolt_commit('-A','-m','main changes data');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='e1' WHERE id=1;
UPDATE t SET extra='e2' WHERE id=2;
SELECT dolt_commit('-A','-m','feat adds extra col');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_onesided_col_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"
run_test "schema_merge_onesided_col_exists" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
run_test "schema_merge_onesided_row_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "schema_merge_onesided_data_main" "SELECT v FROM t WHERE id=1;" "a2" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_merge14_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, extra TEXT);
INSERT INTO t VALUES(1,'a','e1');
INSERT INTO t VALUES(2,'b','e2');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'c','e3');
SELECT dolt_commit('-A','-m','feat adds data');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_ff_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]|Fast-forward" "$DB"
run_test "schema_merge_ff_count" "SELECT count(*) FROM t;" "3" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_merge15_$$.db; rm -f "$DB"
echo "CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "ALTER TABLE t1 ADD COLUMN col_main TEXT;
UPDATE t1 SET col_main='m1' WHERE id=1;
SELECT dolt_commit('-A','-m','main adds col to t1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
ALTER TABLE t2 ADD COLUMN col_feat TEXT;
UPDATE t2 SET col_feat='f1' WHERE id=1;
SELECT dolt_commit('-A','-m','feat adds col to t2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_multi_table_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"
run_test "schema_merge_multi_t1_col" \
  "SELECT count(*) FROM pragma_table_info('t1') WHERE name='col_main';" \
  "1" "$DB"
run_test "schema_merge_multi_t2_col" \
  "SELECT count(*) FROM pragma_table_info('t2') WHERE name='col_feat';" \
  "1" "$DB"
run_test "schema_merge_multi_t1_data" "SELECT col_main FROM t1 WHERE id=1;" "m1" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_merge16_$$.db; rm -f "$DB"

INSERTS=""
for i in $(seq 1 50); do
  INSERTS="${INSERTS}INSERT INTO t VALUES($i,'row$i');"
done

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
${INSERTS}
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

UPDATES=""
for i in $(seq 1 50); do
  UPDATES="${UPDATES}UPDATE t SET score=$((i*10)) WHERE id=$i;"
done

echo "ALTER TABLE t ADD COLUMN score INTEGER;
${UPDATES}
INSERT INTO t VALUES(51,'row51',510);
SELECT dolt_commit('-A','-m','main adds score + row51');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
INSERT INTO t VALUES(52,'row52');
SELECT dolt_commit('-A','-m','feat adds row52');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_many_rows_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"
run_test "schema_merge_many_rows_count" "SELECT count(*) FROM t;" "52" "$DB"
run_test "schema_merge_many_rows_score1" "SELECT score FROM t WHERE id=1;" "10" "$DB"
run_test "schema_merge_many_rows_score25" "SELECT score FROM t WHERE id=25;" "250" "$DB"
run_test "schema_merge_many_rows_score50" "SELECT score FROM t WHERE id=50;" "500" "$DB"
run_test "schema_merge_many_rows_row51" "SELECT v||':'||score FROM t WHERE id=51;" "row51:510" "$DB"
run_test "schema_merge_many_rows_row52_exists" "SELECT v FROM t WHERE id=52;" "row52" "$DB"
run_test "schema_merge_many_rows_score_col" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='score';" \
  "1" "$DB"
rm -f "$DB"

# Both branches ADD a disjoint column while theirs also edits a shared column
# and deletes a row. The row-level changes must survive the schema merge --
# taking ours wholesale and backfilling only the added columns silently drops
# theirs' edit to id=5 and deletion of id=9.
DB=/tmp/test_dual_addcol_rows_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES(1,'one'),(5,'five'),(9,'nine');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='five-theirs' WHERE id=5;
DELETE FROM t WHERE id=9;
ALTER TABLE t ADD COLUMN c TEXT;
UPDATE t SET c='cval' WHERE id=1;
SELECT dolt_commit('-Am','feat_change');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN b TEXT;
SELECT dolt_commit('-Am','main_addcol');
EOF
run_test_match "dual_addcol_rows_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "dual_addcol_rows_theirs_edit_kept" "SELECT a FROM t WHERE id=5;" "five-theirs" "$DB"
run_test "dual_addcol_rows_theirs_delete_kept" "SELECT count(*) FROM t WHERE id=9;" "0" "$DB"
run_test "dual_addcol_rows_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "dual_addcol_rows_both_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name IN ('b','c');" "2" "$DB"
run_test "dual_addcol_rows_theirs_newcol_val" "SELECT c FROM t WHERE id=1;" "cval" "$DB"
run_test "dual_addcol_rows_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

dltest_finish
