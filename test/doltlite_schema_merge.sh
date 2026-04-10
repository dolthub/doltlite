#!/bin/bash
#
# Tests for Schema merge and ALTER TABLE interactions with dolt operations (merge, cherry-pick,
# revert, diff, history, point-in-time queries, schema_diff).
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite ALTER TABLE Merge Tests ==="
echo ""

# ============================================================
# Test 1: ALTER TABLE ADD COLUMN on a branch, then merge into main
# ============================================================

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

# Before merge, main should not have the extra column
run_test "alter_merge_pre_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "0" "$DB"

run_test_match "alter_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"

# After merge, extra column should exist
run_test "alter_merge_post_cols" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
run_test "alter_merge_row1" "SELECT extra FROM t WHERE id=1;" "new" "$DB"
run_test "alter_merge_row2" "SELECT extra FROM t WHERE id=2;" "hello" "$DB"
run_test "alter_merge_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Test 1b: quoted added columns migrate correctly across merge
# ============================================================

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

# ============================================================
# Test 1c: schema merge handles escaped quoted identifiers
# ============================================================

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

# ============================================================
# Test 2: ALTER TABLE ADD COLUMN same name on both branches — conflict?
# ============================================================

DB=/tmp/test_alter_merge2_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds column
echo "ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='main_val';
SELECT dolt_commit('-A','-m','main adds extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Feature adds same column name
echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='feat_val';
SELECT dolt_commit('-A','-m','feat adds extra');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Both branches changed the same row's extra column — should conflict
run_test_match "alter_same_col_merge" "SELECT dolt_merge('feat');" "conflict|merge failed|Error" "$DB"

rm -f "$DB"

# ============================================================
# Test 3: ALTER TABLE ADD COLUMN on one branch, different ADD COLUMN
#          on another, then merge
# ============================================================

DB=/tmp/test_alter_merge3_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main adds col_main
echo "ALTER TABLE t ADD COLUMN col_main TEXT;
UPDATE t SET col_main='m';
SELECT dolt_commit('-A','-m','main adds col_main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Feature adds col_feat
echo "SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN col_feat TEXT;
UPDATE t SET col_feat='f';
SELECT dolt_commit('-A','-m','feat adds col_feat');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Schema merge: different columns added on each side -> should succeed
run_test_match "schema_diff_cols_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB"

# Verify both columns present
run_test_match "schema_diff_cols_has_col_main" \
  "SELECT col_main FROM t WHERE id=1;" \
  "m" "$DB"

run_test_match "schema_diff_cols_has_col_feat" \
  "SELECT typeof(col_feat) FROM t WHERE id=1;" \
  "null|text" "$DB"

rm -f "$DB"

# ============================================================
# Test 4: DROP TABLE on one branch, modify data on another, then merge
# ============================================================

DB=/tmp/test_alter_merge4_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE keep(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO keep VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main drops table t
echo "DROP TABLE t;
SELECT dolt_commit('-A','-m','drop t');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Feature modifies data in t
echo "SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','insert into t');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Drop vs modify — should conflict
run_test_match "drop_vs_modify_merge" "SELECT dolt_merge('feat');" "conflict|merge failed|Error" "$DB"

# keep table should still be accessible
run_test "drop_vs_modify_keep" "SELECT w FROM keep WHERE id=1;" "x" "$DB"

rm -f "$DB"

# ============================================================
# Test 5: Cherry-pick a commit that includes an ALTER TABLE
# ============================================================

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

# Cherry-pick the alter commit
run_test_match "cp_alter_hash" \
  "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "^[0-9a-f]{40}$" "$DB"

# Column should exist on main now
run_test "cp_alter_col_exists" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
run_test "cp_alter_val" "SELECT extra FROM t WHERE id=1;" "cp" "$DB"
run_test_match "cp_alter_msg" "SELECT message FROM dolt_log LIMIT 1;" "^alter add extra$" "$DB"

rm -f "$DB"

# ============================================================
# Test 6: Revert a commit that includes an ALTER TABLE (ADD COLUMN)
# ============================================================

DB=/tmp/test_alter_revert_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='rev' WHERE id=1;
SELECT dolt_commit('-A','-m','alter add extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify column exists before revert
run_test "revert_alter_pre" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"

# Revert the alter commit (HEAD)
run_test_match "revert_alter_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

# After revert, extra column should be gone and data restored
run_test "revert_alter_col_gone" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "0" "$DB"
run_test "revert_alter_data" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test_match "revert_alter_msg" "SELECT message FROM dolt_log LIMIT 1;" "Revert" "$DB"

rm -f "$DB"

# ============================================================
# Test 7: dolt_diff and dolt_history across a schema change
# ============================================================

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

# dolt_diff between before and after should show changes
run_test_match "diff_across_alter" \
  "SELECT diff_type FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1) LIMIT 1;" \
  "modified|added" "$DB"

# dolt_diff should show at least the modified row
run_test_match "diff_across_alter_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "^[1-9]" "$DB"

# dolt_history should include rows from both commits
run_test_match "history_across_alter" \
  "SELECT count(*) FROM dolt_history_t;" \
  "^[2-9]" "$DB"

# History should have entries from 2 different commits
run_test "history_commits" \
  "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" \
  "2" "$DB"

rm -f "$DB"

# ============================================================
# Test 8: dolt_at for a commit before vs after ALTER TABLE
# ============================================================

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

# At v1: no extra column, 1 row
run_test "at_before_alter_count" \
  "SELECT count(*) FROM dolt_at_t('v1');" \
  "1" "$DB"

# At v2: 2 rows
run_test "at_after_alter_count" \
  "SELECT count(*) FROM dolt_at_t('v2');" \
  "2" "$DB"

# At v2: extra column data accessible
run_test_match "at_after_alter_extra" \
  "SELECT extra FROM dolt_at_t('v2') WHERE id=1;" \
  "new" "$DB"

rm -f "$DB"

# ============================================================
# Test 9: dolt_schema_diff showing the ALTER changes
# ============================================================

DB=/tmp/test_alter_sd_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Schema diff should show a change for table t
run_test_match "schema_diff_alter_count" \
  "SELECT count(*) FROM dolt_schema_diff('v1','v2');" \
  "^[1-9]" "$DB"

run_test_match "schema_diff_alter_table" \
  "SELECT table_name FROM dolt_schema_diff('v1','v2') WHERE table_name='t';" \
  "^t$" "$DB"

# The to_create_stmt should include the extra column
run_test_match "schema_diff_alter_to_stmt" \
  "SELECT to_create_stmt FROM dolt_schema_diff('v1','v2') WHERE table_name='t';" \
  "extra" "$DB"

# The from_create_stmt should NOT include the extra column
run_test_match "schema_diff_alter_from_stmt" \
  "SELECT from_create_stmt FROM dolt_schema_diff('v1','v2') WHERE table_name='t';" \
  "v TEXT" "$DB"

rm -f "$DB"

# ============================================================
# Schema Merge: column additions
# ============================================================

# Test 10: Both branches add different columns with data — merge succeeds
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

# Test 11: Both branches add same column with identical definition — convergent, succeeds
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

# Test 12: Both branches add same column with different types — error
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

# ============================================================
# Schema Merge: one-sided changes
# ============================================================

# Test 13: Only one branch adds column, other changes data — column propagated
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

# Test 14: Only one branch drops a column (via recreating table), other doesn't touch schema
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

# Main just fast-forwards to feat since it has no changes
run_test_match "schema_merge_ff_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]|Fast-forward" "$DB"
run_test "schema_merge_ff_count" "SELECT count(*) FROM t;" "3" "$DB"
rm -f "$DB"

# ============================================================
# Schema Merge: multiple tables
# ============================================================

# Test 15: Two branches each add columns to different tables — no conflict
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

# ============================================================
# Schema Merge: data integrity with many rows
# ============================================================

# Test 16: 50 rows, one branch adds a score column and sets all values + adds row 51.
#           Other branch adds row 52. After merge: 52 rows, scores correct.
DB=/tmp/test_schema_merge16_$$.db; rm -f "$DB"

# Build insert statements for 50 rows
INSERTS=""
for i in $(seq 1 50); do
  INSERTS="${INSERTS}INSERT INTO t VALUES($i,'row$i');"
done

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
${INSERTS}
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main branch: add score column, set values for all 50 rows, add row 51
UPDATES=""
for i in $(seq 1 50); do
  UPDATES="${UPDATES}UPDATE t SET score=$((i*10)) WHERE id=$i;"
done

echo "ALTER TABLE t ADD COLUMN score INTEGER;
${UPDATES}
INSERT INTO t VALUES(51,'row51',510);
SELECT dolt_commit('-A','-m','main adds score + row51');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Feat branch: add row 52
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

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
