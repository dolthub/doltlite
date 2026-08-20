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

# Dual ADD COLUMN on a table whose shared columns use mixed-case spellings in
# CREATE TABLE. normalizeTheirsToMergedLayout must match those names
# case-insensitively (same contract as schema findColumn) so theirs' edits to
# shared columns are not treated as a new ordinal.
DB=/tmp/test_schema_merge_dual_add_case_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, Name TEXT, Score INT);
INSERT INTO t VALUES(1, 'alice', 10), (2, 'bob', 20);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
ALTER TABLE t ADD COLUMN FeatNote TEXT;
UPDATE t SET Name = 'ALICE', Score = 11, FeatNote = 'f1' WHERE id = 1;
DELETE FROM t WHERE id = 2;
SELECT dolt_commit('-Am', 'feat dual-add side');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN MainNote TEXT;
UPDATE t SET MainNote = 'm1' WHERE id = 1;
SELECT dolt_commit('-Am', 'main dual-add side');
EOF

run_test_match "schema_merge_dual_add_case_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"
run_test "schema_merge_dual_add_case_name" \
  "SELECT Name FROM t WHERE id=1;" "ALICE" "$DB"
run_test "schema_merge_dual_add_case_score" \
  "SELECT Score FROM t WHERE id=1;" "11" "$DB"
run_test "schema_merge_dual_add_case_featnote" \
  "SELECT FeatNote FROM t WHERE id=1;" "f1" "$DB"
run_test "schema_merge_dual_add_case_mainnote" \
  "SELECT MainNote FROM t WHERE id=1;" "m1" "$DB"
run_test "schema_merge_dual_add_case_deleted" \
  "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test "schema_merge_dual_add_case_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name IN ('FeatNote','MainNote');" \
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
run_test "schema_merge_autocommit_schema_conflicts_empty" \
  "SELECT count(*) FROM dolt_schema_conflicts;" "0" "$DB"
run_test "schema_merge_autocommit_conflicts_empty" \
  "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
rm -f "$DB"

DB=/tmp/test_schema_conflicts_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-Am','feat schema');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN extra INTEGER;
SELECT dolt_commit('-Am','main schema');
BEGIN;
SELECT dolt_merge('feat');
COMMIT;
EOF

# Conflicts are never committed, so the merge and every inspection of it have to
# share one session; sc() re-runs the merge and discards it afterwards.
sc() { printf "BEGIN;\nSELECT dolt_merge('feat');\n%s\nROLLBACK;\n" "$1"; }

run_test "schema_conflicts_columns" \
  "SELECT group_concat(name, '|') FROM (SELECT name FROM pragma_table_info('dolt_schema_conflicts') ORDER BY cid);" \
  "table_name|base_schema|our_schema|their_schema|description" "$DB"
run_test_lastline "schema_conflicts_summary" \
  "$(sc "SELECT \"table\" || '|' || num_conflicts FROM dolt_conflicts;")" \
  "t|0" "$DB"
run_test_lastline "schema_conflicts_status" \
  "$(sc "SELECT table_name || '|' || staged || '|' || status FROM dolt_status WHERE status='schema conflict';")" \
  "t|0|schema conflict" "$DB"
run_test_lastline "schema_conflicts_row" \
  "$(sc "SELECT table_name || '|' || (base_schema LIKE 'CREATE TABLE t%') || '|' || (our_schema LIKE '%extra INTEGER%') || '|' || (their_schema LIKE '%extra TEXT%') || '|' || description FROM dolt_schema_conflicts;")" \
  "t|1|1|1|both branches add column 'extra' with different definitions" "$DB"
run_test_match "schema_conflicts_resolve_refused" \
  "$(sc "SELECT dolt_conflicts_resolve('--ours','t');")" \
  "Unable to automatically resolve schema conflicts|Error" "$DB"
run_test_match "schema_conflicts_commit_refused" \
  "$(sc "SELECT dolt_commit('-Am','must fail');")" \
  "unresolved schema conflicts|Error" "$DB"

# The COMMIT in the setup above was refused, so nothing conflicted reached disk.
run_test "schema_conflicts_not_persisted" \
  "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts);" \
  "0|0" "$DB"
run_test_match "schema_conflicts_abort" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_merge('--abort');" "^[0-9]+$" "$DB"
run_test "schema_conflicts_abort_clears" \
  "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" \
  "0|0|0" "$DB"
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

DB=/tmp/test_schema_tokenizer_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t_default(id INTEGER PRIMARY KEY, v TEXT DEFAULT ')');
CREATE TABLE t_check(id INTEGER PRIMARY KEY, v TEXT CHECK(v <> ')' AND v <> ','));
CREATE TABLE t_generated(id INTEGER PRIMARY KEY, v TEXT, g TEXT AS (v || '),(') VIRTUAL);
CREATE TABLE t_identifier(id INTEGER PRIMARY KEY, "odd,)name" TEXT);
CREATE TABLE t_comment(
  id INTEGER PRIMARY KEY,
  /* ), punctuation */ v TEXT
);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t_default ADD COLUMN feat_col TEXT;
ALTER TABLE t_check ADD COLUMN feat_col TEXT;
ALTER TABLE t_generated ADD COLUMN feat_col TEXT;
ALTER TABLE t_identifier ADD COLUMN feat_col TEXT;
ALTER TABLE t_comment ADD COLUMN feat_col TEXT;
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
ALTER TABLE t_default ADD COLUMN main_col TEXT;
ALTER TABLE t_check ADD COLUMN main_col TEXT;
ALTER TABLE t_generated ADD COLUMN main_col TEXT;
ALTER TABLE t_identifier ADD COLUMN main_col TEXT;
ALTER TABLE t_comment ADD COLUMN main_col TEXT;
SELECT dolt_commit('-Am','main');
EOF
run_test_match "schema_tokenizer_merge_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "schema_tokenizer_default" \
  "SELECT count(*) FROM pragma_table_xinfo('t_default') WHERE name IN ('main_col','feat_col');" \
  "2" "$DB"
run_test "schema_tokenizer_default_value" \
  "INSERT INTO t_default(id) VALUES(1); SELECT v FROM t_default;" \
  ")" "$DB"
run_test "schema_tokenizer_check" \
  "SELECT count(*) FROM pragma_table_xinfo('t_check') WHERE name IN ('main_col','feat_col');" \
  "2" "$DB"
run_test_match "schema_tokenizer_check_value" \
  "INSERT INTO t_check(id,v) VALUES(1,')');" \
  "CHECK constraint failed" "$DB"
run_test "schema_tokenizer_generated" \
  "SELECT count(*) FROM pragma_table_xinfo('t_generated') WHERE name IN ('main_col','feat_col');" \
  "2" "$DB"
run_test "schema_tokenizer_generated_value" \
  "INSERT INTO t_generated(id,v) VALUES(1,'x'); SELECT g FROM t_generated;" \
  "x),(" "$DB"
run_test "schema_tokenizer_identifier" \
  "SELECT count(*) FROM pragma_table_xinfo('t_identifier') WHERE name IN ('main_col','feat_col');" \
  "2" "$DB"
run_test "schema_tokenizer_identifier_name" \
  "SELECT count(*) FROM pragma_table_xinfo('t_identifier') WHERE name='odd,)name';" \
  "1" "$DB"
run_test "schema_tokenizer_comment" \
  "SELECT count(*) FROM pragma_table_xinfo('t_comment') WHERE name IN ('main_col','feat_col');" \
  "2" "$DB"
rm -f "$DB"

# Only one side changes the columns, so the other side and the ancestor still
# store the dropped column. Every value right of it must not shift left, and a
# cell neither side touched must not read as a conflict. Values verified
# against Dolt 2.2.2.
DB=/tmp/test_drop_col_merge_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'a1','b1','c1'),(2,'a2','b2','c2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'a3','b3','c3');
SELECT dolt_commit('-A','-m','insert on feat');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on main');
EOF
run_test_match "drop_col_merge_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_col_merge_incoming_row" "SELECT a||'|'||c FROM t WHERE k=3;" \
  "a3|c3" "$DB"
run_test "drop_col_merge_existing_row" "SELECT a||'|'||c FROM t WHERE k=1;" \
  "a1|c1" "$DB"
run_test "drop_col_merge_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='b';" "0" "$DB"
run_test "drop_col_merge_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# The mirror direction: the side that dropped the column is the one merged in,
# so it is our own rows that have to move into the merged layout.
DB=/tmp/test_drop_col_merge_rev_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'a1','b1','c1'),(2,'a2','b2','c2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'a3','b3','c3');
SELECT dolt_commit('-A','-m','insert on main');
EOF
run_test_match "drop_col_merge_rev_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_col_merge_rev_our_row" "SELECT a||'|'||c FROM t WHERE k=3;" \
  "a3|c3" "$DB"
run_test "drop_col_merge_rev_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='b';" "0" "$DB"
run_test "drop_col_merge_rev_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# A row both sides hold, changed only by them: the ancestor has to be read at
# merged positions or the untouched cell looks like a competing edit.
DB=/tmp/test_drop_col_merge_mod_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'a1','b1','c1'),(2,'a2','b2','c2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET c='c2new' WHERE k=2;
SELECT dolt_commit('-A','-m','modify shared row');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on main');
EOF
run_test_match "drop_col_merge_mod_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_col_merge_mod_row" "SELECT a||'|'||c FROM t WHERE k=2;" \
  "a2|c2new" "$DB"
run_test "drop_col_merge_mod_untouched" "SELECT a||'|'||c FROM t WHERE k=1;" \
  "a1|c1" "$DB"
run_test "drop_col_merge_mod_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
rm -f "$DB"

# Both sides change columns and one of them drops: the ancestor no longer
# lines up with either side, so it too is read at merged positions.
DB=/tmp/test_drop_col_merge_dual_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'a1','b1','c1'),(2,'a2','b2','c2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN d TEXT;
UPDATE t SET c='c2new' WHERE k=2;
SELECT dolt_commit('-A','-m','add d and modify on feat');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on main');
EOF
run_test_match "drop_col_merge_dual_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_col_merge_dual_modified" "SELECT a||'|'||c FROM t WHERE k=2;" \
  "a2|c2new" "$DB"
run_test "drop_col_merge_dual_untouched" "SELECT a||'|'||c FROM t WHERE k=1;" \
  "a1|c1" "$DB"
run_test "drop_col_merge_dual_added_col" \
  "SELECT count(*) FROM t WHERE d IS NOT NULL;" "0" "$DB"
run_test "drop_col_merge_dual_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
rm -f "$DB"

# The same pairing with the sides swapped. Only a rename on their side was
# recognized, so ours went unseen and the merge stopped as incompatible.
# Values verified against Dolt 2.2.2, which merges this cleanly.
DB=/tmp/test_rename_ours_drop_theirs_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INT);
INSERT INTO t VALUES(1,'a1','b1','c1',101),(2,'a2','b2','c2',202);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
INSERT INTO t(k,a,c,d) VALUES(3,'a3','c3',303);
SELECT dolt_commit('-A','-m','drop b and insert on feat');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME COLUMN c TO renamed_c;
SELECT dolt_commit('-A','-m','rename c on main');
EOF
run_test_match "rename_ours_drop_theirs_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rename_ours_drop_theirs_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" \
  "k,a,renamed_c,d" "$DB"
run_test "rename_ours_drop_theirs_base_row" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=1;" "a1|c1|101" "$DB"
run_test "rename_ours_drop_theirs_incoming_row" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=3;" "a3|c3|303" "$DB"
run_test "rename_ours_drop_theirs_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "rename_ours_drop_theirs_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# Dropping the trailing column leaves the incoming value with nowhere to go,
# the one shape where no surviving column later claims its slot.
DB=/tmp/test_drop_last_col_merge_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'a1','b1','c1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'a2','b2','c2');
SELECT dolt_commit('-A','-m','insert on feat');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN c;
SELECT dolt_commit('-A','-m','drop trailing column');
EOF
run_test_match "drop_last_col_merge_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_last_col_merge_incoming_row" "SELECT a||'|'||b FROM t WHERE k=2;" \
  "a2|b2" "$DB"
run_test "drop_last_col_merge_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='c';" "0" "$DB"
run_test "drop_last_col_merge_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# A rename makes the merge adopt the other side's schema whole, which would
# also bring back the column this side deleted and read our rows at the
# adopted positions. Values verified against Dolt 2.2.2.
DB=/tmp/test_drop_vs_rename_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INT);
INSERT INTO t VALUES(1,'a1','b1','c1',101);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN c TO renamed_c;
INSERT INTO t VALUES(2,'a2','b2','c2',202);
SELECT dolt_commit('-A','-m','rename c on feat');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on main');
EOF
run_test_match "drop_vs_rename_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_vs_rename_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" \
  "k,a,renamed_c,d" "$DB"
run_test "drop_vs_rename_base_row" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=1;" "a1|c1|101" "$DB"
run_test "drop_vs_rename_incoming_row" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=2;" "a2|c2|202" "$DB"
run_test "drop_vs_rename_conflicts" "SELECT count(*) FROM dolt_conflicts;" \
  "0" "$DB"
run_test "drop_vs_rename_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# The same pairing where their side also edits a row both sides hold.
DB=/tmp/test_drop_vs_rename_edit_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INT);
INSERT INTO t VALUES(1,'a1','b1','c1',101),(2,'a2','b2','c2',202);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN c TO renamed_c;
UPDATE t SET d=999 WHERE k=2;
SELECT dolt_commit('-A','-m','rename and edit on feat');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on main');
EOF
run_test_match "drop_vs_rename_edit_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_vs_rename_edit_row" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=2;" "a2|c2|999" "$DB"
run_test "drop_vs_rename_edit_untouched" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=1;" "a1|c1|101" "$DB"
run_test "drop_vs_rename_edit_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
rm -f "$DB"

# A rename rewrites no rows, so their root can be identical while the layout
# has still moved. Nothing about whether a side wrote rows may decide whether
# our rows are moved onto the adopted layout.
DB=/tmp/test_drop_vs_rename_norows_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INT);
INSERT INTO t VALUES(1,'a1','b1','c1',505);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN c TO renamed_c;
SELECT dolt_commit('-A','-m','rename only, no rows written');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on main');
EOF
run_test_match "drop_vs_rename_norows_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "drop_vs_rename_norows_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" \
  "k,a,renamed_c,d" "$DB"
run_test "drop_vs_rename_norows_row" \
  "SELECT a||'|'||renamed_c||'|'||d FROM t WHERE k=1;" "a1|c1|505" "$DB"
run_test "drop_vs_rename_norows_types" \
  "SELECT typeof(renamed_c)||'|'||typeof(d) FROM t WHERE k=1;" \
  "text|integer" "$DB"
run_test "drop_vs_rename_norows_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# A rename with no deletion opposite it must still keep every column.
DB=/tmp/test_rename_no_drop_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INT);
INSERT INTO t VALUES(1,'a1','b1','c1',101);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN c TO renamed_c;
INSERT INTO t VALUES(2,'a2','b2','c2',202);
SELECT dolt_commit('-A','-m','rename c on feat');
SELECT dolt_checkout('main');
UPDATE t SET a='a1x' WHERE k=1;
SELECT dolt_commit('-A','-m','edit on main');
EOF
run_test_match "rename_no_drop_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rename_no_drop_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" \
  "k,a,b,renamed_c,d" "$DB"
run_test "rename_no_drop_rows" \
  "SELECT group_concat(k||':'||a||':'||renamed_c) FROM t;" \
  "1:a1x:c1,2:a2:c2" "$DB"
rm -f "$DB"

# Their schema supplies whatever the merged rows do not carry, and an object
# this side deleted is exactly that -- so it came back, and a resurrected
# trigger fires on the next write. Verified against Dolt 2.2.2, which keeps
# the deletion.
DB=/tmp/test_merge_dropped_objects_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
CREATE VIEW v AS SELECT a,b FROM t;
CREATE TRIGGER tg AFTER INSERT ON t BEGIN UPDATE t SET b='TRIG'; END;
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'y');
SELECT dolt_commit('-A','-m','insert on feat');
SELECT dolt_checkout('main');
DROP VIEW v;
DROP TRIGGER tg;
SELECT dolt_commit('-A','-m','drop view and trigger');
EOF
run_test_match "merge_keeps_drop_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_keeps_view_dropped" \
  "SELECT count(*) FROM sqlite_master WHERE name='v';" "0" "$DB"
run_test "merge_keeps_trigger_dropped" \
  "SELECT count(*) FROM sqlite_master WHERE name='tg';" "0" "$DB"
# A trigger that came back would rewrite this row on the way in.
run_test "merge_dropped_trigger_stays_silent" \
  "INSERT INTO t VALUES(3,'kept'); SELECT b FROM t WHERE a=3;" "kept" "$DB"
rm -f "$DB"

# Nothing was deleted, so both objects have to survive the same merge.
DB=/tmp/test_merge_keeps_objects_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
CREATE VIEW v AS SELECT a,b FROM t;
CREATE TRIGGER tg AFTER INSERT ON t BEGIN UPDATE t SET b='TRIG'; END;
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'y');
SELECT dolt_commit('-A','-m','insert on feat');
SELECT dolt_checkout('main');
UPDATE t SET b='z' WHERE a=1;
SELECT dolt_commit('-A','-m','edit on main');
EOF
run_test_match "merge_undeleted_objects_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_keeps_undeleted_objects" \
  "SELECT group_concat(type||':'||name) FROM sqlite_master
   WHERE name IN ('v','tg') ORDER BY name;" "view:v,trigger:tg" "$DB"
rm -f "$DB"

# An index over a column the other branch dropped cannot be carried into the
# merge: the merged table has no such column, so the catalog it would build
# cannot be loaded, and the merge used to fail "database disk image is
# malformed" with no way past it in either direction. Dolt drops the index
# along with the column.
for dir in ours theirs; do
  DB=/tmp/test_merge_idx_dropcol_${dir}_$$.db; rm -f "$DB"
  if [ "$dir" = ours ]; then
    OURS="ALTER TABLE t DROP COLUMN b;"; THEIRS="CREATE INDEX ix ON t(b);"
  else
    OURS="CREATE INDEX ix ON t(b);"; THEIRS="ALTER TABLE t DROP COLUMN b;"
  fi
  cat <<EOF | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1'),(2,'a2','b2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
$OURS
SELECT dolt_commit('-A','-m','main side');
SELECT dolt_checkout('feat');
$THEIRS
SELECT dolt_commit('-A','-m','feat side');
SELECT dolt_checkout('main');
EOF
  run_test_match "merge_index_over_dropped_column_${dir}_hash" \
    "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
  run_test "merge_index_over_dropped_column_${dir}_objects" \
    "SELECT group_concat(type||':'||name) FROM sqlite_master ORDER BY name;" \
    "table:t" "$DB"
  run_test "merge_index_over_dropped_column_${dir}_rows" \
    "SELECT group_concat(k||':'||a) FROM t ORDER BY k;" "1:a1,2:a2" "$DB"
  run_test "merge_index_over_dropped_column_${dir}_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB"
  rm -f "$DB"
done

# A composite index goes the same way when any of its columns is dropped, and a
# unique one too: the constraint cannot outlive the column it constrains.
DB=/tmp/test_merge_idx_dropcol_composite_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
CREATE INDEX ix ON t(a,b);
CREATE UNIQUE INDEX ux ON t(b);
SELECT dolt_commit('-A','-m','indexes on main');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b on feat');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_composite_index_over_dropped_column_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_composite_index_over_dropped_column_objects" \
  "SELECT group_concat(type||':'||name) FROM sqlite_master ORDER BY name;" \
  "table:t" "$DB"
rm -f "$DB"

# Expression indexes name columns inside function calls (nested parens), not
# as a plain list. Dropping that column used to leave the index in the catalog
# and fail the merge with "database disk image is malformed".
for dir in ours theirs; do
  DB=/tmp/test_merge_idx_dropcol_expr_${dir}_$$.db; rm -f "$DB"
  if [ "$dir" = ours ]; then
    OURS="ALTER TABLE t DROP COLUMN b;"; THEIRS="CREATE INDEX ix ON t(abs(length(b)));"
  else
    OURS="CREATE INDEX ix ON t(abs(length(b)));"; THEIRS="ALTER TABLE t DROP COLUMN b;"
  fi
  cat <<EOF | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1'),(2,'a2','b2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
$OURS
SELECT dolt_commit('-A','-m','main side');
SELECT dolt_checkout('feat');
$THEIRS
SELECT dolt_commit('-A','-m','feat side');
SELECT dolt_checkout('main');
EOF
  run_test_match "merge_expr_index_over_dropped_column_${dir}_hash" \
    "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
  run_test "merge_expr_index_over_dropped_column_${dir}_objects" \
    "SELECT group_concat(type||':'||name) FROM sqlite_master ORDER BY name;" \
    "table:t" "$DB"
  run_test "merge_expr_index_over_dropped_column_${dir}_rows" \
    "SELECT group_concat(k||':'||a) FROM t ORDER BY k;" "1:a1,2:a2" "$DB"
  run_test "merge_expr_index_over_dropped_column_${dir}_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB"
  rm -f "$DB"
done

# An expression index over a surviving column is not dropped just because a
# different column disappeared, including when the expression calls abs().
DB=/tmp/test_merge_idx_expr_survives_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
CREATE INDEX ix ON t(abs(length(a)));
SELECT dolt_commit('-A','-m','index on a');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_expr_index_surviving_column_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_expr_index_surviving_column_kept" \
  "SELECT group_concat(name) FROM sqlite_master WHERE type='index';" "ix" "$DB"
rm -f "$DB"

# COLLATE names the collation, not a column. A dropped column whose name
# happens to be nocase must not take an index on (a COLLATE nocase) with it.
DB=/tmp/test_merge_idx_collate_survives_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, nocase TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','n1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
CREATE INDEX ix ON t(a COLLATE nocase);
SELECT dolt_commit('-A','-m','index on a');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN nocase;
SELECT dolt_commit('-A','-m','drop nocase');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_collate_index_survives_dropped_collation_name_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_collate_index_survives_dropped_collation_name_kept" \
  "SELECT group_concat(name) FROM sqlite_master WHERE type='index';" "ix" "$DB"
run_test "merge_collate_index_survives_dropped_collation_name_integrity" \
  "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# An index over a quoted identifier with an embedded quote still tracks the
# real column. Dropping "odd""name" has to drop the index; dropping some other
# column must not.
for dir in ours theirs; do
  DB=/tmp/test_merge_idx_dropcol_quoted_${dir}_$$.db; rm -f "$DB"
  if [ "$dir" = ours ]; then
    OURS='ALTER TABLE t DROP COLUMN "odd""name";'
    THEIRS='CREATE INDEX ix ON t("odd""name");'
  else
    OURS='CREATE INDEX ix ON t("odd""name");'
    THEIRS='ALTER TABLE t DROP COLUMN "odd""name";'
  fi
  cat <<EOF | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, "odd""name" TEXT, b TEXT);
INSERT INTO t VALUES(1,'o1','b1'),(2,'o2','b2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
$OURS
SELECT dolt_commit('-A','-m','main side');
SELECT dolt_checkout('feat');
$THEIRS
SELECT dolt_commit('-A','-m','feat side');
SELECT dolt_checkout('main');
EOF
  run_test_match "merge_quoted_index_over_dropped_column_${dir}_hash" \
    "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
  run_test "merge_quoted_index_over_dropped_column_${dir}_objects" \
    "SELECT group_concat(type||':'||name) FROM sqlite_master ORDER BY name;" \
    "table:t" "$DB"
  run_test "merge_quoted_index_over_dropped_column_${dir}_rows" \
    "SELECT group_concat(k||':'||b) FROM t ORDER BY k;" "1:b1,2:b2" "$DB"
  run_test "merge_quoted_index_over_dropped_column_${dir}_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB"
  rm -f "$DB"
done

DB=/tmp/test_merge_idx_quoted_survives_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, "odd""name" TEXT, b TEXT);
INSERT INTO t VALUES(1,'o1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
CREATE INDEX ix ON t("odd""name");
SELECT dolt_commit('-A','-m','index on quoted');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_quoted_index_surviving_column_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_quoted_index_surviving_column_kept" \
  "SELECT group_concat(name) FROM sqlite_master WHERE type='index';" "ix" "$DB"
rm -f "$DB"

# A partial index's WHERE clause names columns too. Dropping one of those
# used to leave the index in the catalog and fail the merge as malformed.
for dir in ours theirs; do
  DB=/tmp/test_merge_idx_dropcol_where_${dir}_$$.db; rm -f "$DB"
  if [ "$dir" = ours ]; then
    OURS="ALTER TABLE t DROP COLUMN b;"
    THEIRS="CREATE INDEX ix ON t(a) WHERE b = 'keep';"
  else
    OURS="CREATE INDEX ix ON t(a) WHERE b = 'keep';"
    THEIRS="ALTER TABLE t DROP COLUMN b;"
  fi
  cat <<EOF | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','keep'),(2,'a2','drop');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
$OURS
SELECT dolt_commit('-A','-m','main side');
SELECT dolt_checkout('feat');
$THEIRS
SELECT dolt_commit('-A','-m','feat side');
SELECT dolt_checkout('main');
EOF
  run_test_match "merge_partial_index_over_dropped_column_${dir}_hash" \
    "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
  run_test "merge_partial_index_over_dropped_column_${dir}_objects" \
    "SELECT group_concat(type||':'||name) FROM sqlite_master ORDER BY name;" \
    "table:t" "$DB"
  run_test "merge_partial_index_over_dropped_column_${dir}_rows" \
    "SELECT group_concat(k||':'||a) FROM t ORDER BY k;" "1:a1,2:a2" "$DB"
  run_test "merge_partial_index_over_dropped_column_${dir}_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB"
  rm -f "$DB"
done

# The WHERE literal is not a column. Dropping some other column keeps a
# partial index whose predicate only names surviving columns.
DB=/tmp/test_merge_idx_where_survives_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'keep','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
CREATE INDEX ix ON t(a) WHERE a = 'keep';
SELECT dolt_commit('-A','-m','partial on a');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','drop b');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_partial_index_surviving_predicate_hash" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_partial_index_surviving_predicate_kept" \
  "SELECT group_concat(name) FROM sqlite_master WHERE type='index';" "ix" "$DB"
rm -f "$DB"

# An index whose columns all survive must be adopted exactly as before: both
# branches indexing different surviving columns keeps both indexes, and an
# index over a column their side added comes across with it.
DB=/tmp/test_merge_idx_survives_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
CREATE INDEX ix2 ON t(a);
SELECT dolt_commit('-A','-m','index on main');
SELECT dolt_checkout('feat');
CREATE INDEX ix ON t(b);
SELECT dolt_commit('-A','-m','index on feat');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_both_indexes_survive_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_both_indexes_survive" \
  "SELECT group_concat(name) FROM sqlite_master WHERE type='index' ORDER BY name;" \
  "ix,ix2" "$DB"
rm -f "$DB"

DB=/tmp/test_merge_idx_added_col_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES(1,'a1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'a2');
SELECT dolt_commit('-A','-m','row on main');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN d TEXT;
CREATE INDEX ix ON t(d);
SELECT dolt_commit('-A','-m','add and index d on feat');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_index_over_added_column_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_index_over_added_column_kept" \
  "SELECT group_concat(name) FROM sqlite_master WHERE type='index';" "ix" "$DB"
rm -f "$DB"

# Each branch dropping a different column is an ordinary merge -- Dolt takes
# both drops -- but with no action recorded for their deletion the two
# sqlite_master rows used to conflict and the merge was refused. Assert types
# as well as values: a layout that survives reopen with a value in the wrong
# column shows up as a type mismatch, not a missing row.
DB=/tmp/test_merge_dual_drop_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
ALTER TABLE t DROP COLUMN a;
SELECT dolt_commit('-A','-m','main drops a');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-A','-m','feat drops b');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_dual_drop_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_dual_drop_columns" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" "k,n" "$DB"
run_test "merge_dual_drop_values" \
  "SELECT group_concat(k||':'||n) FROM (SELECT k,n FROM t ORDER BY k);" \
  "1:11,2:22" "$DB"
run_test "merge_dual_drop_types" \
  "SELECT DISTINCT typeof(n) FROM t;" "integer" "$DB"
run_test "merge_dual_drop_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# Our addition beside their deletion: the merged table carries both changes.
DB=/tmp/test_merge_add_vs_drop_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a1',11);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'dd';
SELECT dolt_commit('-A','-m','main adds d');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN a;
SELECT dolt_commit('-A','-m','feat drops a');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_add_vs_drop_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_add_vs_drop_columns" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" "k,n,d" "$DB"
run_test "merge_add_vs_drop_values" \
  "SELECT k||':'||n||':'||coalesce(d,'<null>') FROM t;" "1:11:dd" "$DB"
run_test "merge_add_vs_drop_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# Each branch renaming a different column. The adopted schema keeps the other
# side's column under its ancestor name, so the rename has to be carried across
# as a rename: replaying it as an addition kept the old column and left the new
# one empty, which read as NULL through the name the branch actually uses.
DB=/tmp/test_merge_dual_rename_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
ALTER TABLE t RENAME COLUMN a TO a2;
SELECT dolt_commit('-A','-m','main renames a');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN b TO b2;
SELECT dolt_commit('-A','-m','feat renames b');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_dual_rename_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_dual_rename_columns" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" "k,a2,b2,n" "$DB"
run_test "merge_dual_rename_values" \
  "SELECT group_concat(k||':'||a2||':'||b2||':'||n) FROM (SELECT * FROM t ORDER BY k);" \
  "1:a1:b1:11,2:a2:b2:22" "$DB"
run_test "merge_dual_rename_types" \
  "SELECT DISTINCT typeof(a2)||','||typeof(n) FROM t;" "text,integer" "$DB"
run_test "merge_dual_rename_integrity" "PRAGMA integrity_check;" "ok" "$DB"
# SQLite keeps the quoting the rename used, and a column spelled "a2" here but
# a2 by hand reads as a different definition -- which would turn a later merge
# of two identical schemas into a conflict.
run_test "merge_dual_rename_quoting_matches_plain_rename" \
  "SELECT sql FROM sqlite_master WHERE name='t';" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, a2 TEXT, b2 TEXT, n INTEGER)" "$DB"
rm -f "$DB"

# A merged rename must still merge against a branch that made the same rename
# by hand.
DB=/tmp/test_merge_rename_then_merge_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_branch('other');
ALTER TABLE t RENAME COLUMN a TO a2;
SELECT dolt_commit('-A','-m','main renames a');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN b TO b2;
SELECT dolt_commit('-A','-m','feat renames b');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('other');
ALTER TABLE t RENAME COLUMN a TO a2;
INSERT INTO t VALUES(2,'a2v','b2v');
SELECT dolt_commit('-A','-m','other renames a by hand');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_after_merged_rename_hash" "SELECT dolt_merge('other');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_after_merged_rename_rows" \
  "SELECT group_concat(k||':'||a2) FROM (SELECT k,a2 FROM t ORDER BY k);" \
  "1:a1,2:a2v" "$DB"
rm -f "$DB"

# A renamed-to name that cannot be written bare still has to be quoted.
DB=/tmp/test_merge_dual_rename_quoted_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
ALTER TABLE t RENAME COLUMN a TO "select";
SELECT dolt_commit('-A','-m','main renames a to a keyword');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN b TO "my col";
SELECT dolt_commit('-A','-m','feat renames b with a space');
SELECT dolt_checkout('main');
EOF
run_test_match "merge_dual_rename_quoted_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_dual_rename_quoted_values" \
  "SELECT \"select\" || '/' || \"my col\" FROM t;" "a1/b1" "$DB"
run_test "merge_dual_rename_quoted_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# An index one side adds over a column the other side renames. The indexed
# column still exists under the new name, but nothing retargets the index to
# it, and a merged catalog naming a column its table does not have cannot be
# loaded -- the merge used to report the database as corrupt. Refuse it with a
# message naming the index and column instead, leaving the branch untouched.
#
# Dolt merges these and keeps the index on the renamed column, so this is a
# deliberate divergence, asserted as such in vc_oracle_schema_merge_test.sh.
# Retargeting the index would close it (issue #2302).
for dir in ours theirs; do
  DB=/tmp/test_merge_ix_over_rename_${dir}_$$.db; rm -f "$DB"
  if [ "$dir" = ours ]; then
    MAIN="CREATE INDEX ix ON t(b);"; FEAT="ALTER TABLE t RENAME COLUMN b TO b2;"
    WANTCOLS="k,a,b"
  else
    MAIN="ALTER TABLE t RENAME COLUMN b TO b2;"; FEAT="CREATE INDEX ix ON t(b);"
    WANTCOLS="k,a,b2"
  fi
  cat <<EOF | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1'),(2,'a2','b2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
$FEAT
SELECT dolt_commit('-A','-m','feat side');
SELECT dolt_checkout('main');
$MAIN
SELECT dolt_commit('-A','-m','main side');
EOF
  run_test_match "merge_index_over_renamed_column_${dir}_refused" \
    "SELECT dolt_merge('feat');" \
    "cannot merge: index 'ix' covers column 'b' of table 't'" "$DB"
  run_test "merge_index_over_renamed_column_${dir}_schema_intact" \
    "SELECT group_concat(name) FROM pragma_table_info('t');" "$WANTCOLS" "$DB"
  run_test "merge_index_over_renamed_column_${dir}_rows_intact" \
    "SELECT count(*) FROM t;" "2" "$DB"
  run_test "merge_index_over_renamed_column_${dir}_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB"
  run_test "merge_index_over_renamed_column_${dir}_clean_status" \
    "SELECT count(*) FROM dolt_status;" "0" "$DB"
  rm -f "$DB"
done

# A unique index gets the same refusal: dropping it silently would drop the
# constraint with it.
DB=/tmp/test_merge_ux_over_rename_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN b TO b2;
SELECT dolt_commit('-A','-m','feat renames');
SELECT dolt_checkout('main');
CREATE UNIQUE INDEX ux ON t(b);
SELECT dolt_commit('-A','-m','main indexes');
EOF
run_test_match "merge_unique_index_over_renamed_column_refused" \
  "SELECT dolt_merge('feat');" "cannot merge: index 'ux' covers column 'b'" "$DB"
rm -f "$DB"

# An index over a column nobody renamed still merges, and so does an index over
# a column the other side dropped -- that one drops with the column (#2289).
DB=/tmp/test_merge_ix_unaffected_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN b TO b2;
SELECT dolt_commit('-A','-m','feat renames b');
SELECT dolt_checkout('main');
CREATE INDEX ix ON t(a);
SELECT dolt_commit('-A','-m','main indexes a');
EOF
run_test_match "merge_index_over_other_column_still_merges" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_index_over_other_column_result" \
  "SELECT group_concat(name) FROM pragma_table_info('t');" "k,a,b2" "$DB"
rm -f "$DB"

# An index of theirs over a column we dropped is not adopted, and the master row
# for it must not be written either: the row lands at a number one of our own
# indexes already occupies, so ours is displaced by an index that cannot resolve
# against the merged table, and the catalog then does not load at all. Only
# reachable when the table carries an index the drop does not touch, which is
# why the no-other-index case merged correctly all along.
for extra in "CREATE INDEX ix0 ON t(a);" "CREATE INDEX ix0 ON t(n);"; do
  DB=/tmp/test_merge_idx_row_$$.db; rm -f "$DB"
  cat <<EOF | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER);
$extra
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX ix ON t(b);
SELECT dolt_commit('-Am','feat indexes b');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-Am','main drops b');
EOF
  run_test_match "merge_index_row_over_dropped_column_hash" \
    "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
  run_test "merge_index_row_over_dropped_column_keeps_our_index" \
    "SELECT group_concat(name) FROM sqlite_master WHERE type='index';" "ix0" "$DB"
  run_test "merge_index_row_over_dropped_column_columns" \
    "SELECT group_concat(name) FROM pragma_table_info('t');" "k,a,n" "$DB"
  run_test "merge_index_row_over_dropped_column_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB"
  rm -f "$DB"
done

# An index of theirs over a column that survives is still adopted beside ours.
DB=/tmp/test_merge_idx_row_survives_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER);
CREATE INDEX ix0 ON t(a);
INSERT INTO t VALUES(1,'a1','b1',11);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX ix ON t(n);
SELECT dolt_commit('-Am','feat indexes n');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-Am','main drops b');
EOF
run_test_match "merge_index_row_unaffected_hash" "SELECT dolt_merge('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "merge_index_row_unaffected_keeps_both" \
  "SELECT group_concat(name) FROM (SELECT name FROM sqlite_master WHERE type='index' ORDER BY name);" \
  "ix,ix0" "$DB"
rm -f "$DB"

dltest_finish
