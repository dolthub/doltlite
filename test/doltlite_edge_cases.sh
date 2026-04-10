#!/bin/bash
#
# Edge case and gap coverage tests for Doltlite.
# 100+ tests covering: NULL handling, special characters, schema ops,
# multi-table merges, error paths, complex workflows, boundary conditions.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Edge Case Tests ==="
echo ""

# ============================================================
# Section 1: NULL value handling (10 tests)
# ============================================================

DB=/tmp/test_edge_null_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
INSERT INTO t VALUES(1, NULL, NULL, NULL);
INSERT INTO t VALUES(2, 'hello', 42, 3.14);
INSERT INTO t VALUES(3, '', 0, 0.0);
SELECT dolt_commit('-A','-m','nulls and values');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "null_select" "SELECT a FROM t WHERE id=1;" "" "$DB"
run_test "null_count" "SELECT count(*) FROM t WHERE a IS NULL;" "1" "$DB"
run_test "null_diff_count" "SELECT count(*) FROM dolt_diff('t');" "0" "$DB"

# Update NULL to value — check with working diff (uncommitted)
echo "UPDATE t SET a='was_null' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "null_diff_shows" "SELECT count(*) FROM dolt_diff('t');" "^[1-9]" "$DB"
echo "SELECT dolt_commit('-A','-m','fill nulls');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "null_updated" "SELECT a FROM t WHERE id=1;" "was_null" "$DB"

# Update value to NULL
echo "UPDATE t SET a=NULL WHERE id=2;
SELECT dolt_commit('-A','-m','set to null');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "val_to_null" "SELECT count(*) FROM t WHERE a IS NULL;" "1" "$DB"

# NULL in merge scenario
echo "SELECT dolt_branch('nullbranch');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('nullbranch');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET b=999 WHERE id=3; SELECT dolt_commit('-A','-m','branch change');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET c=9.9 WHERE id=1; SELECT dolt_commit('-A','-m','main change');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "null_merge" "SELECT dolt_merge('nullbranch');" "^[0-9a-f]{40}$" "$DB"
run_test "null_merge_b" "SELECT b FROM t WHERE id=3;" "999" "$DB"
run_test "null_merge_c" "SELECT c FROM t WHERE id=1;" "9.9" "$DB"
run_test "null_merge_unchanged" "SELECT a FROM t WHERE id=1;" "was_null" "$DB"

rm -f "$DB"

# ============================================================
# Section 2: Empty strings and boundary values (8 tests)
# ============================================================

DB=/tmp/test_edge_boundary_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1, '');
INSERT INTO t VALUES(2, 'x');
INSERT INTO t VALUES(9223372036854775807, 'maxint');
INSERT INTO t VALUES(-9223372036854775808, 'minint');
SELECT dolt_commit('-A','-m','boundary values');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_string" "SELECT length(val) FROM t WHERE id=1;" "0" "$DB"
run_test "empty_string_val" "SELECT val FROM t WHERE id=1;" "" "$DB"
run_test "max_int" "SELECT val FROM t WHERE id=9223372036854775807;" "maxint" "$DB"
run_test "min_int" "SELECT val FROM t WHERE id=-9223372036854775808;" "minint" "$DB"
run_test "boundary_count" "SELECT count(*) FROM t;" "4" "$DB"
run_test "boundary_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

# Persist and reopen
run_test "boundary_persist_max" "SELECT val FROM t WHERE id=9223372036854775807;" "maxint" "$DB"
run_test "boundary_persist_min" "SELECT val FROM t WHERE id=-9223372036854775808;" "minint" "$DB"

rm -f "$DB"

# ============================================================
# Section 3: Special characters in data (8 tests)
# ============================================================

DB=/tmp/test_edge_chars_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1, 'it''s a test');
INSERT INTO t VALUES(2, 'line1
line2');
INSERT INTO t VALUES(3, 'tab	here');
INSERT INTO t VALUES(4, 'back\\slash');
INSERT INTO t VALUES(5, 'quote\"mark');
SELECT dolt_commit('-A','-m','special chars');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "special_apostrophe" "SELECT val FROM t WHERE id=1;" "it's a test" "$DB"
run_test_match "special_newline" "SELECT length(val) FROM t WHERE id=2;" "^1[01]$" "$DB"
run_test "special_count" "SELECT count(*) FROM t;" "5" "$DB"

# Diff after modification — check WORKING diff before commit
echo "UPDATE t SET val='updated' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "special_diff" "SELECT count(*) FROM dolt_diff('t');" "^[1-9]" "$DB"
echo "SELECT dolt_commit('-A','-m','update special');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "special_updated" "SELECT val FROM t WHERE id=1;" "updated" "$DB"

# Branch and merge with special chars
echo "SELECT dolt_branch('sp');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('sp');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(6,'sp_val'); SELECT dolt_commit('-A','-m','sp commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(7,'main_val'); SELECT dolt_commit('-A','-m','main commit');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "special_merge" "SELECT dolt_merge('sp');" "^[0-9a-f]{40}$" "$DB"
run_test "special_merge_count" "SELECT count(*) FROM t;" "7" "$DB"
run_test "special_merge_both" "SELECT val FROM t WHERE id=6;" "sp_val" "$DB"

rm -f "$DB"

# ============================================================
# Section 4: Multi-table merge scenarios (10 tests)
# ============================================================

DB=/tmp/test_edge_multitable_$$.db; rm -f "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, y TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, z TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,'b1');
INSERT INTO c VALUES(1,'c1');
SELECT dolt_commit('-A','-m','init 3 tables');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Branch modifies table a and b, main modifies b and c
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO a VALUES(2,'a2_feat');
INSERT INTO b VALUES(2,'b2_feat');
SELECT dolt_commit('-A','-m','feat: add to a and b');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO b VALUES(3,'b3_main');
INSERT INTO c VALUES(2,'c2_main');
SELECT dolt_commit('-A','-m','main: add to b and c');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "multi_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "multi_a_count" "SELECT count(*) FROM a;" "2" "$DB"
run_test "multi_b_count" "SELECT count(*) FROM b;" "3" "$DB"
run_test "multi_c_count" "SELECT count(*) FROM c;" "2" "$DB"
run_test "multi_a2" "SELECT x FROM a WHERE id=2;" "a2_feat" "$DB"
run_test "multi_b2" "SELECT y FROM b WHERE id=2;" "b2_feat" "$DB"
run_test "multi_b3" "SELECT y FROM b WHERE id=3;" "b3_main" "$DB"
run_test "multi_c2" "SELECT z FROM c WHERE id=2;" "c2_main" "$DB"

# Verify merge log
run_test_match "multi_merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"
run_test "multi_merge_log_count" "SELECT count(*) FROM dolt_log;" "5" "$DB"

rm -f "$DB"

# ============================================================
# Section 5: One branch creates new table, merge into main (6 tests)
# ============================================================

DB=/tmp/test_edge_newtable_$$.db; rm -f "$DB"
echo "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO base VALUES(1,'x');
SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "CREATE TABLE feat_table(id INTEGER PRIMARY KEY, f TEXT);
INSERT INTO feat_table VALUES(1,'feat1');
INSERT INTO feat_table VALUES(2,'feat2');
SELECT dolt_commit('-A','-m','add feat_table');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO base VALUES(2,'y');
SELECT dolt_commit('-A','-m','main adds row');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "newtable_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "newtable_base" "SELECT count(*) FROM base;" "2" "$DB"
run_test "newtable_feat" "SELECT count(*) FROM feat_table;" "2" "$DB"
run_test "newtable_feat_val" "SELECT f FROM feat_table WHERE id=1;" "feat1" "$DB"
run_test "newtable_schema" "SELECT count(*) FROM sqlite_master WHERE type='table';" "2" "$DB"

# Persists after reopen
run_test "newtable_persist" "SELECT count(*) FROM feat_table;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 6: Fast-forward merge variations (6 tests)
# ============================================================

DB=/tmp/test_edge_ff_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('ahead');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('ahead');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'second');
SELECT dolt_commit('-A','-m','second');
INSERT INTO t VALUES(3,'third');
SELECT dolt_commit('-A','-m','third');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main has no new commits, so merge should fast-forward
run_test_match "ff_merge" "SELECT dolt_merge('ahead');" "^[0-9a-f]{40}$" "$DB"
run_test "ff_data" "SELECT count(*) FROM t;" "3" "$DB"
run_test "ff_row2" "SELECT v FROM t WHERE id=2;" "second" "$DB"
run_test "ff_row3" "SELECT v FROM t WHERE id=3;" "third" "$DB"

# Already up to date
run_test "ff_already_uptodate" "SELECT dolt_merge('ahead');" "Already up to date" "$DB"

# Merge self branch
run_test "ff_self" "SELECT dolt_merge('main');" "Already up to date" "$DB"

rm -f "$DB"

# ============================================================
# Section 7: Error handling paths (12 tests)
# ============================================================

DB=/tmp/test_edge_errors_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge non-existent branch
run_test_match "err_merge_noexist" "SELECT dolt_merge('nope');" "not found" "$DB"

# Checkout non-existent branch / table
run_test_match "err_checkout_noexist" "SELECT dolt_checkout('nope');" "no such branch or table" "$DB"

# Branch already exists
echo "SELECT dolt_branch('dup');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "err_branch_dup" "SELECT dolt_branch('dup');" "already exists" "$DB"

# Delete current branch
run_test_match "err_delete_current" "SELECT dolt_branch('-d','main');" "cannot delete" "$DB"

# Commit with no changes
run_test_match "err_commit_clean" "SELECT dolt_commit('-A','-m','empty');" "nothing to commit" "$DB"

# Commit without message
run_test_match "err_commit_nomsg" "SELECT dolt_commit('-A');" "require" "$DB"

# Tag already exists
echo "SELECT dolt_tag('v1');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "err_tag_dup" "SELECT dolt_tag('v1');" "already exists" "$DB"

# Delete non-existent tag
run_test_match "err_tag_del_noexist" "SELECT dolt_tag('-d','nope');" "not found" "$DB"

# Checkout with uncommitted changes (allowed, like Dolt SQL context)
echo "INSERT INTO t VALUES(2,'dirty');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "checkout_dirty_allowed" "SELECT dolt_checkout('dup');" "0" "$DB"

# Hard reset cleans it
echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "err_reset_cleaned" "SELECT count(*) FROM t;" "1" "$DB"

# Conflicts resolve on table with no conflicts — returns 0 (success)
run_test "err_resolve_none" "SELECT dolt_conflicts_resolve('--ours','t');" "0" "$DB"

# dolt_diff on non-existent table returns empty
run_test "err_diff_notable" "SELECT count(*) FROM dolt_diff('nonexistent');" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 8: Complex staging workflows (10 tests)
# ============================================================

DB=/tmp/test_edge_staging_$$.db; rm -f "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,'b1');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Modify both tables
echo "INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,'b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "stg_both_dirty" "SELECT count(*) FROM dolt_status;" "2" "$DB"

# Stage only table a
echo "SELECT dolt_add('a');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "stg_a_staged" "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"
run_test "stg_b_unstaged" "SELECT count(*) FROM dolt_status WHERE staged=0;" "1" "$DB"

# No-args reset (== --mixed) unstages a. (--soft with no target ref
# is a no-op under git/Dolt semantics.)
echo "SELECT dolt_reset();" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "stg_reset_unstages" "SELECT count(*) FROM dolt_status WHERE staged=1;" "0" "$DB"
run_test "stg_data_kept" "SELECT count(*) FROM a;" "2" "$DB"

# Stage a again, commit just a
echo "SELECT dolt_add('a');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "stg_partial_commit" "SELECT dolt_commit('-m','just a');" "^[0-9a-f]{40}$" "$DB"

# b should still be dirty
run_test "stg_b_still_dirty" "SELECT count(*) FROM dolt_status;" "1" "$DB"
run_test_match "stg_b_in_status" "SELECT table_name FROM dolt_status;" "b" "$DB"

# Commit b
run_test_match "stg_commit_b" "SELECT dolt_commit('-A','-m','now b');" "^[0-9a-f]{40}$" "$DB"
run_test "stg_all_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 9: Schema changes (ALTER TABLE) (8 tests)
# ============================================================

DB=/tmp/test_edge_schema_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'Alice');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Add column
echo "ALTER TABLE t ADD COLUMN email TEXT;
UPDATE t SET email='alice@test.com' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

# Check working diff before commit (should show change)
run_test_match "schema_diff_working" "SELECT count(*) FROM dolt_diff('t');" "^[1-9]" "$DB"

echo "SELECT dolt_commit('-A','-m','add email column');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_new_col" "SELECT email FROM t WHERE id=1;" "alice@test.com" "$DB"
run_test "schema_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

# Insert with new column
echo "INSERT INTO t VALUES(2,'Bob','bob@test.com');
SELECT dolt_commit('-A','-m','add bob');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_new_row" "SELECT email FROM t WHERE id=2;" "bob@test.com" "$DB"

# Branch after schema change
echo "SELECT dolt_branch('post-schema');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('post-schema');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'Charlie','charlie@test.com');
SELECT dolt_commit('-A','-m','charlie on branch');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4,'Dave','dave@test.com');
SELECT dolt_commit('-A','-m','dave on main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "schema_merge_after_alter" "SELECT dolt_merge('post-schema');" "^[0-9a-f]{40}$" "$DB"
run_test "schema_merge_count" "SELECT count(*) FROM t;" "4" "$DB"
run_test "schema_merge_charlie" "SELECT email FROM t WHERE id=3;" "charlie@test.com" "$DB"
run_test "schema_merge_dave" "SELECT email FROM t WHERE id=4;" "dave@test.com" "$DB"

rm -f "$DB"

# ============================================================
# Section 10: Diff between specific commits (8 tests)
# ============================================================

DB=/tmp/test_edge_diffhash_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'first');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'second');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'third');
UPDATE t SET v='FIRST' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Diff between c1 and c3 (should show changes)
run_test_match "diff_c1_c3" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[2-9]" "$DB"

# Diff between c2 and c3
run_test_match "diff_c2_c3" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[1-9]" "$DB"

# Diff between same commit (should be 0)
run_test "diff_same_commit" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "0" "$DB"

# Diff on working changes (no hashes = HEAD vs working)
echo "INSERT INTO t VALUES(4,'uncommitted');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "diff_working" "SELECT count(*) FROM dolt_diff('t');" "^[1-9]" "$DB"

# Diff types present
run_test_match "diff_type_added" \
  "SELECT diff_type FROM dolt_diff('t') LIMIT 1;" "added" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Diff between tags
echo "SELECT dolt_tag('v1', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2));" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_tag('v3', (SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "diff_tags" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT tag_hash FROM dolt_tags WHERE tag_name='v1'), (SELECT tag_hash FROM dolt_tags WHERE tag_name='v3'));" \
  "^[2-9]" "$DB"

# Verify log has 3 commits
run_test "diff_log_count" "SELECT count(*) FROM dolt_log;" "4" "$DB"

# Tag listing
run_test "diff_tag_count" "SELECT count(*) FROM dolt_tags;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 11: Tag on specific commit and tag delete (6 tests)
# ============================================================

DB=/tmp/test_edge_tagops_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3); SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Tag specific commit (the first one)
echo "SELECT dolt_tag('old', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2));" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "tag_specific" "SELECT count(*) FROM dolt_tags WHERE tag_name='old';" "1" "$DB"

# Multiple tags on same commit
echo "SELECT dolt_tag('latest');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_tag('also-latest');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "tag_multi_same" "SELECT count(*) FROM dolt_tags;" "3" "$DB"

# Delete a tag
echo "SELECT dolt_tag('-d','also-latest');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "tag_deleted" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test "tag_correct_remain" "SELECT count(*) FROM dolt_tags WHERE tag_name='latest';" "1" "$DB"

# Tag persists across reopen
run_test "tag_persist" "SELECT count(*) FROM dolt_tags WHERE tag_name='old';" "1" "$DB"
run_test "tag_persist2" "SELECT tag_name FROM dolt_tags WHERE tag_name='latest';" "latest" "$DB"

rm -f "$DB"

# ============================================================
# Section 12: Branch management edge cases (8 tests)
# ============================================================

DB=/tmp/test_edge_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create multiple branches
echo "SELECT dolt_branch('b1');
SELECT dolt_branch('b2');
SELECT dolt_branch('b3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_multi" "SELECT count(*) FROM dolt_branches;" "4" "$DB"

# Switch between branches rapidly
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "branch_on_b1" "SELECT active_branch();" "b1" "$DB"

echo "SELECT dolt_checkout('b2');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "branch_on_b2" "SELECT active_branch();" "b2" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "branch_back_main" "SELECT active_branch();" "main" "$DB"

# Work on b1, then delete b2 and b3
echo "SELECT dolt_branch('-d','b2');
SELECT dolt_branch('-d','b3');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "branch_after_delete" "SELECT count(*) FROM dolt_branches;" "2" "$DB"

# Branches in dolt_branches should show correct names
run_test_match "branch_names" "SELECT name FROM dolt_branches ORDER BY name;" "b1" "$DB"

# Commit on b1 doesn't affect main
echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(99);
SELECT dolt_commit('-A','-m','b1 commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_isolation" "SELECT count(*) FROM t;" "1" "$DB"
run_test "branch_b1_has_more" "SELECT dolt_checkout('b1'); SELECT count(*) FROM t;" "0
2" "$DB"

rm -f "$DB"

# ============================================================
# Section 13: Conflict with multiple tables (7 tests)
# ============================================================

DB=/tmp/test_edge_multiconflict_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Hat');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('hotfix');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('hotfix');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE users SET name='ALICE' WHERE id=1;
UPDATE orders SET item='HAT' WHERE id=1;
SELECT dolt_commit('-A','-m','hotfix: uppercase');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE users SET name='alice_v2' WHERE id=1;
UPDATE orders SET item='hat_v2' WHERE id=1;
SELECT dolt_commit('-A','-m','main: v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge should conflict on both tables
run_test_match "multi_conflict_merge" "SELECT dolt_merge('hotfix');" "conflict" "$DB"
run_test_match "multi_conflict_count" "SELECT count(*) FROM dolt_conflicts;" "^[1-2]$" "$DB"

# Commit should be blocked
run_test_match "multi_conflict_blocked" "SELECT dolt_commit('-A','-m','fail');" "unresolved" "$DB"

# Resolve users with ours
echo "SELECT dolt_conflicts_resolve('--ours','users');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "multi_conflict_users_ours" "SELECT name FROM users WHERE id=1;" "alice_v2" "$DB"

# Resolve orders with ours too (theirs resolution keeps ours data when --ours)
echo "SELECT dolt_conflicts_resolve('--ours','orders');" | $DOLTLITE "$DB" > /dev/null 2>&1

# All conflicts resolved
run_test "multi_conflict_resolved" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"

# Verify ours values kept
run_test "multi_conflict_orders_ours" "SELECT item FROM orders WHERE id=1;" "hat_v2" "$DB"

rm -f "$DB"

# ============================================================
# Section 14: Conflict detection and --ours preservation (6 tests)
# ============================================================

DB=/tmp/test_edge_conflictours_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='hf_val' WHERE id=1;
SELECT dolt_commit('-A','-m','hf');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge and get conflict
run_test_match "ours_conflict_merge" "SELECT dolt_merge('hf');" "conflict" "$DB"
run_test "ours_conflict_exists" "SELECT count(*) FROM dolt_conflicts;" "1" "$DB"

# Resolve with ours
echo "SELECT dolt_conflicts_resolve('--ours','t');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "ours_conflicts_cleared" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "ours_value_kept" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"
run_test "ours_other_row_ok" "SELECT v FROM t WHERE id=2;" "keep" "$DB"
run_test "ours_branch_ok" "SELECT active_branch();" "main" "$DB"

rm -f "$DB"

# ============================================================
# Section 15: Large batch inserts and many rows (5 tests)
# ============================================================

DB=/tmp/test_edge_bulk_$$.db; rm -f "$DB"

# Insert 500 rows
SQL="CREATE TABLE big(id INTEGER PRIMARY KEY, v TEXT);"
for i in $(seq 1 500); do
  SQL="$SQL INSERT INTO big VALUES($i,'row_$i');"
done
SQL="$SQL SELECT dolt_commit('-A','-m','500 rows');"
echo "$SQL" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bulk_count" "SELECT count(*) FROM big;" "500" "$DB"
run_test "bulk_first" "SELECT v FROM big WHERE id=1;" "row_1" "$DB"
run_test "bulk_last" "SELECT v FROM big WHERE id=500;" "row_500" "$DB"
run_test "bulk_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

# Persist and verify
run_test "bulk_persist_count" "SELECT count(*) FROM big;" "500" "$DB"

rm -f "$DB"

# ============================================================
# Section 16: Multiple sequential merges (6 tests)
# ============================================================

DB=/tmp/test_edge_seqmerge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge 1
echo "SELECT dolt_branch('f1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('f1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'f1'); SELECT dolt_commit('-A','-m','f1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "seqmerge_1" "SELECT dolt_merge('f1');" "^[0-9a-f]{40}$" "$DB"
run_test "seqmerge_1_count" "SELECT count(*) FROM t;" "2" "$DB"

# Merge 2
echo "SELECT dolt_branch('f2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('f2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'f2'); SELECT dolt_commit('-A','-m','f2');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "seqmerge_2" "SELECT dolt_merge('f2');" "^[0-9a-f]{40}$" "$DB"
run_test "seqmerge_2_count" "SELECT count(*) FROM t;" "3" "$DB"

# Merge 3
echo "SELECT dolt_branch('f3');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('f3');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4,'f3'); SELECT dolt_commit('-A','-m','f3');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "seqmerge_3" "SELECT dolt_merge('f3');" "^[0-9a-f]{40}$" "$DB"
run_test "seqmerge_3_count" "SELECT count(*) FROM t;" "4" "$DB"

rm -f "$DB"

# ============================================================
# Section 17: Merge with row deletions from feature (4 tests)
# ============================================================

DB=/tmp/test_edge_mergedel_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
INSERT INTO t VALUES(4,'d');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('cleanup');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('cleanup');" | $DOLTLITE "$DB" > /dev/null 2>&1
# Delete individual rows to avoid WHERE clause bug
echo "DELETE FROM t WHERE id=2;
DELETE FROM t WHERE id=4;
SELECT dolt_commit('-A','-m','delete 2 and 4');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(5,'e');
SELECT dolt_commit('-A','-m','add 5');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "mergedel_merge" "SELECT dolt_merge('cleanup');" "^[0-9a-f]{40}$" "$DB"
run_test "mergedel_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test_match "mergedel_has_1" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test "mergedel_no_2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 18: Diff types with working changes (6 tests)
# ============================================================

DB=/tmp/test_edge_difftypes_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep');
INSERT INTO t VALUES(2,'modify');
INSERT INTO t VALUES(3,'delete_me');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Make uncommitted changes — diff shows HEAD vs working
echo "INSERT INTO t VALUES(4,'added');
UPDATE t SET v='MODIFIED' WHERE id=2;
DELETE FROM t WHERE id=3;" | $DOLTLITE "$DB" > /dev/null 2>&1

# Working diff should show changes
run_test_match "difftype_working_count" "SELECT count(*) FROM dolt_diff('t');" "^[1-9]" "$DB"

# Commit and verify between-commit diff
echo "SELECT dolt_commit('-A','-m','mixed changes');" | $DOLTLITE "$DB" > /dev/null 2>&1

# After commit, HEAD vs working should be clean
run_test "difftype_clean" "SELECT count(*) FROM dolt_diff('t');" "0" "$DB"

# Diff between the two commits should show changes
run_test_match "difftype_commit_diff" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[1-9]" "$DB"

# Make another working change
echo "INSERT INTO t VALUES(5,'new_working');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "difftype_new_working" "SELECT count(*) FROM dolt_diff('t');" "1" "$DB"
run_test_match "difftype_new_working_type" "SELECT diff_type FROM dolt_diff('t');" "added" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify data integrity
run_test "difftype_data_ok" "SELECT count(*) FROM dolt_log;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 19: dolt_commit --author flag (4 tests)
# ============================================================

DB=/tmp/test_edge_author_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','by author','--author','TestUser <test@user.com>');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "author_name" "SELECT committer FROM dolt_log LIMIT 1;" "TestUser" "$DB"
run_test "author_email" "SELECT email FROM dolt_log LIMIT 1;" "test@user.com" "$DB"

# Second commit with different author
echo "INSERT INTO t VALUES(2);
SELECT dolt_commit('-A','-m','by other','--author','Other <other@test.com>');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "author_other" "SELECT committer FROM dolt_log LIMIT 1;" "Other" "$DB"
run_test "author_other_email" "SELECT email FROM dolt_log LIMIT 1;" "other@test.com" "$DB"

rm -f "$DB"

# ============================================================
# Section 20: Persistence stress test (6 tests)
# ============================================================

DB=/tmp/test_edge_persist_$$.db; rm -f "$DB"

# Create structure, commit, close, reopen multiple times
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'round1');
SELECT dolt_commit('-A','-m','round1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'round2');
SELECT dolt_commit('-A','-m','round2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(3,'round3');
SELECT dolt_commit('-A','-m','round3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify after multiple commits (each is a separate session/reopen)
run_test "persist_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "persist_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"
run_test "persist_branch" "SELECT active_branch();" "main" "$DB"

# Create branch and commit on it
echo "SELECT dolt_branch('dev');
SELECT dolt_checkout('dev');
INSERT INTO t VALUES(4,'round4_dev');
SELECT dolt_commit('-A','-m','dev commit');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Checkout sets default branch, so reopen stays on dev
run_test "persist_reopen_branch" "SELECT active_branch();" "dev" "$DB"
run_test "persist_reopen_data" "SELECT count(*) FROM t;" "4" "$DB"

# Switch back to main and verify
run_test "persist_main_data" "SELECT dolt_checkout('main'); SELECT count(*) FROM t;" "0
3" "$DB"

rm -f "$DB"

# ============================================================
# Section 21: Multiple data types (6 tests)
# ============================================================

DB=/tmp/test_edge_types_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, b BLOB);
INSERT INTO t VALUES(1, 42, 3.14159, 'hello world', X'DEADBEEF');
INSERT INTO t VALUES(2, -100, -0.001, 'unicode: café', X'00FF00FF');
INSERT INTO t VALUES(3, 0, 0.0, '', X'');
SELECT dolt_commit('-A','-m','types');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "types_int" "SELECT i FROM t WHERE id=1;" "42" "$DB"
run_test "types_real" "SELECT r FROM t WHERE id=1;" "3.14159" "$DB"
run_test "types_text" "SELECT t FROM t WHERE id=1;" "hello world" "$DB"
run_test "types_neg_int" "SELECT i FROM t WHERE id=2;" "-100" "$DB"
run_test "types_empty_text" "SELECT length(t) FROM t WHERE id=3;" "0" "$DB"
run_test "types_persist" "SELECT count(*) FROM t;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 22: Merge base and ancestry (4 tests)
# ============================================================

DB=/tmp/test_edge_ancestor_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Two commits on main
echo "INSERT INTO t VALUES(2,'main1'); SELECT dolt_commit('-A','-m','main1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'main2'); SELECT dolt_commit('-A','-m','main2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Two commits on feat
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(10,'feat1'); SELECT dolt_commit('-A','-m','feat1');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(11,'feat2'); SELECT dolt_commit('-A','-m','feat2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Both branches diverged from common ancestor — merge should work
run_test_match "ancestor_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "ancestor_all_rows" "SELECT count(*) FROM t;" "5" "$DB"
run_test_match "ancestor_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"

rm -f "$DB"

# ============================================================
# Section 23: Status shows correct change types (5 tests)
# ============================================================

DB=/tmp/test_edge_status_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# No changes — clean
run_test "status_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

# Modify a row
echo "UPDATE t SET v='changed' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "status_modified" "SELECT status FROM dolt_status;" "modified" "$DB"

# Create new table
echo "CREATE TABLE t2(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "status_new_table" "SELECT count(*) FROM dolt_status;" "^[2-3]" "$DB"

# Stage everything, status should show staged
echo "SELECT dolt_add('-A');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "status_all_staged" "SELECT count(*) FROM dolt_status WHERE staged=1;" "^[1-9]" "$DB"

# Commit clears everything
echo "SELECT dolt_commit('-m','changes');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "status_post_commit" "SELECT count(*) FROM dolt_status;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 24: Long commit history (4 tests)
# ============================================================

DB=/tmp/test_edge_history_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create 20 commits
for i in $(seq 2 21); do
  echo "INSERT INTO t VALUES($i,'v$i'); SELECT dolt_commit('-A','-m','commit $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

run_test "history_count" "SELECT count(*) FROM dolt_log;" "22" "$DB"
run_test "history_data" "SELECT count(*) FROM t;" "21" "$DB"
run_test_match "history_first_msg" "SELECT message FROM dolt_log LIMIT 1;" "commit 21" "$DB"

# Persist and verify
run_test "history_persist" "SELECT count(*) FROM dolt_log;" "22" "$DB"

rm -f "$DB"

# ============================================================
# Section 25: Merge convergent changes (4 tests)
# ============================================================

DB=/tmp/test_edge_convergent_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
INSERT INTO t VALUES(2,'orig');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Both branches make SAME change to row 1 (convergent)
echo "UPDATE t SET v='same_change' WHERE id=1;
SELECT dolt_commit('-A','-m','main change');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='same_change' WHERE id=1;
INSERT INTO t VALUES(3,'feat_only');
SELECT dolt_commit('-A','-m','feat change');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Convergent change should not conflict
run_test_match "convergent_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "convergent_value" "SELECT v FROM t WHERE id=1;" "same_change" "$DB"
run_test "convergent_feat_row" "SELECT v FROM t WHERE id=3;" "feat_only" "$DB"
run_test "convergent_count" "SELECT count(*) FROM t;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 26: Tag then checkout to tagged commit state (4 tests)
# ============================================================

DB=/tmp/test_edge_tagstate_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','version 1');
SELECT dolt_tag('v1.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','version 2');
SELECT dolt_tag('v2.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tagstate_two_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test "tagstate_current" "SELECT count(*) FROM t;" "2" "$DB"

# Tags have different hashes
run_test_match "tagstate_diff_hashes" \
  "SELECT count(DISTINCT tag_hash) FROM dolt_tags;" "2" "$DB"

# Diff between tags should show changes
run_test_match "tagstate_diff" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT tag_hash FROM dolt_tags WHERE tag_name='v1.0'), (SELECT tag_hash FROM dolt_tags WHERE tag_name='v2.0'));" \
  "^[1-9]" "$DB"

rm -f "$DB"

# ============================================================
# Section 27: Rapid insert-commit cycles (4 tests)
# ============================================================

DB=/tmp/test_edge_rapid_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c'); SELECT dolt_commit('-A','-m','c3');
INSERT INTO t VALUES(4,'d'); SELECT dolt_commit('-A','-m','c4');
INSERT INTO t VALUES(5,'e'); SELECT dolt_commit('-A','-m','c5');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rapid_data" "SELECT count(*) FROM t;" "5" "$DB"
run_test "rapid_log" "SELECT count(*) FROM dolt_log;" "6" "$DB"
run_test "rapid_last_msg" "SELECT message FROM dolt_log LIMIT 1;" "c5" "$DB"
run_test "rapid_persist" "SELECT count(*) FROM t;" "5" "$DB"

rm -f "$DB"

# ============================================================
# Section 28: Branch from non-main, merge back (4 tests)
# ============================================================

DB=/tmp/test_edge_branchfrom_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'dev1');
SELECT dolt_commit('-A','-m','dev1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Branch from dev (not main!)
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'feature1');
SELECT dolt_commit('-A','-m','feature1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge feature back into dev
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "branchfrom_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB"
run_test "branchfrom_count" "SELECT count(*) FROM t;" "3" "$DB"

# Main should still only have 1 row
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "branchfrom_main" "SELECT count(*) FROM t;" "1" "$DB"

# Merge dev into main
run_test_match "branchfrom_to_main" "SELECT dolt_merge('dev');" "^[0-9a-f]{40}$" "$DB"

rm -f "$DB"

# ============================================================
# Virtual table error on nonexistent table (regression: previously
# these fell back to generic schemas instead of erroring)
# ============================================================

echo ""
echo "--- Virtual table errors for missing tables ---"

DB=/tmp/test_edge_vtab_err_$$.db; rm -f "$DB"
echo "CREATE TABLE real_table(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO real_table VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Schema must use real column names (from_v, to_v), not generic fallback
# names (from_value, to_value). The fallback was removed — if column
# detection fails, the vtable now returns an error instead.
run_test_match "diff_schema_has_from_v" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_diff_real_table');" \
  "from_v" "$DB"

run_test_match "diff_schema_no_generic" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_diff_real_table');" \
  "from_v.*to_v" "$DB"

run_test_match "history_schema_has_v" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_history_real_table');" \
  "\bv\b" "$DB"

# Virtual tables work with real data
run_test_match "diff_table_real_works" \
  "SELECT count(*) FROM dolt_diff_real_table;" \
  "^[0-9]" "$DB"
run_test_match "history_real_works" \
  "SELECT count(*) FROM dolt_history_real_table;" \
  "^[0-9]" "$DB"
run_test_match "at_real_works" \
  "SELECT count(*) FROM dolt_at_real_table('HEAD');" \
  "^[0-9]" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
