#!/bin/bash
#
# Tests for ALTER TABLE interactions with dolt operations (merge, cherry-pick,
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

# This is a schema conflict — different columns added on each side
run_test_match "alter_diff_cols_merge" "SELECT dolt_merge('feat');" "conflict" "$DB"

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
run_test_match "cp_alter_msg" "SELECT message FROM dolt_log LIMIT 1;" "Cherry-pick: alter add extra" "$DB"

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
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
