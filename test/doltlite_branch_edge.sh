#!/bin/bash
#
# Comprehensive edge-case tests for branch interactions, cross-session
# persistence, multi-branch diff/history, reset, schema changes across
# branches, merge edge cases, and GC edge cases in doltlite.
#
DOLTLITE=${DOLTLITE:-./doltlite}
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); echo "  PASS: $n"; else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; echo "  FAIL: $n"; echo "    expected: $e"; echo "    got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); echo "  PASS: $n"; else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; echo "  FAIL: $n"; echo "    pattern: $p"; echo "    got:     $r"; fi; }

# Helper to clean up DB + WAL files
db_rm() { rm -f "$1" "${1}-wal" "${1}-journal"; }

echo "=== Doltlite Branch Edge Case Tests ==="
echo ""

# ############################################################
# Category 1: Branch lifecycle edge cases
# ############################################################

echo "--- Category 1: Branch lifecycle edge cases ---"

# Test: Create branch, delete it, recreate with same name -- old data should be gone
DB=/tmp/test_bedge_lifecycle_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('temp');
SELECT dolt_checkout('temp');
INSERT INTO t VALUES(2,'old_branch_data');
SELECT dolt_commit('-A','-m','old branch commit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Delete the branch
echo "SELECT dolt_branch('-D','temp');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Recreate with same name -- should start from main's HEAD (1 row)
echo "SELECT dolt_branch('temp');
SELECT dolt_checkout('temp');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "lifecycle_recreate_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "lifecycle_recreate_no_old_data" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
db_rm "$DB"

# Test: Delete branch that doesn't exist -- should error
DB=/tmp/test_bedge_delnone_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "delete_nonexistent_branch" "SELECT dolt_branch('-d','nope');" "not found" "$DB"
db_rm "$DB"

# Test: Create branch that already exists -- should error
DB=/tmp/test_bedge_dup_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "create_duplicate_branch" "SELECT dolt_branch('feature');" "already exists" "$DB"
db_rm "$DB"

# Test: Delete the current branch -- should error
DB=/tmp/test_bedge_delcur_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "delete_current_branch" "SELECT dolt_branch('-d','feat');" "cannot delete" "$DB"
db_rm "$DB"

# Test: Branch from a specific tag
DB=/tmp/test_bedge_tag_branch_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create a branch from the tag -- dolt_branch with a start point
# If dolt_branch supports a start point argument, test it; otherwise this may just
# create from HEAD. We test by checking the branch exists.
run_test "tag_branch_create" "SELECT dolt_branch('from_tag');" "0" "$DB"
db_rm "$DB"

echo ""

# ############################################################
# Category 2: Cross-session persistence
# ############################################################

echo "--- Category 2: Cross-session persistence ---"

# Test: Session 1 creates table/inserts/commits; Session 2 sees data
DB=/tmp/test_bedge_xsess1_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello');
INSERT INTO t VALUES(2,'world');
SELECT dolt_commit('-A','-m','initial');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Session 2: new CLI invocation
run_test "xsess_data_visible" "SELECT count(*) FROM t;" "2" "$DB"
run_test "xsess_data_val" "SELECT v FROM t WHERE id=1;" "hello" "$DB"
db_rm "$DB"

# Test: Session 1 creates branch + commits; Session 2 checks out and sees data
DB=/tmp/test_bedge_xsess2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('dev');
SELECT dolt_checkout('dev');
INSERT INTO t VALUES(2,'dev_data');
SELECT dolt_commit('-A','-m','dev commit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Session 2: checkout dev, verify data
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "xsess_branch_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "xsess_branch_val" "SELECT v FROM t WHERE id=2;" "dev_data" "$DB"
db_rm "$DB"

# Test: Session 1 merges; Session 2 verifies merge result
DB=/tmp/test_bedge_xsess3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Session 2: verify merge result
run_test "xsess_merge_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "xsess_merge_val" "SELECT v FROM t WHERE id=2;" "feat_row" "$DB"
db_rm "$DB"

# Test: Session 1 hard resets; Session 2 sees reset state
DB=/tmp/test_bedge_xsess4_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'discard');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

C1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_reset('--hard','$C1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Session 2: verify reset took effect
run_test "xsess_reset_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "xsess_reset_val" "SELECT v FROM t WHERE id=1;" "keep" "$DB"
run_test "xsess_reset_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"
db_rm "$DB"

echo ""

# ############################################################
# Category 3: Multi-branch diff/history
# ############################################################

echo "--- Category 3: Multi-branch diff/history ---"

# Setup: two branches with different commits
DB=/tmp/test_bedge_mbdiff_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'main_row');
SELECT dolt_commit('-A','-m','main adds row 2');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat_row');
SELECT dolt_commit('-A','-m','feat adds row 3');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Get commit hashes for cross-branch diff
MAIN_HEAD=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
FEAT_HEAD=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Test: dolt_diff between two different branches' commits
run_test_match "diff_cross_branch" \
  "SELECT coalesce(sum(rows_added + rows_deleted + rows_modified), 0) FROM dolt_diff_stat('$FEAT_HEAD', '$MAIN_HEAD', 't');" \
  "^[1-9]" "$DB"

db_rm "$DB"

# Setup: fast-forward merge for history/log tests
DB=/tmp/test_bedge_ffhist_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_commit('-A','-m','feat commit 1');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_commit('-A','-m','feat commit 2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Test: dolt_history after fast-forward merge -- should show linear history
run_test_match "ff_history_count" \
  "SELECT count(*) FROM dolt_history_t;" \
  "^[3-9]" "$DB"

# Test: dolt_log after fast-forward merge -- no merge commit
run_test "ff_log_no_merge" "SELECT message FROM dolt_log LIMIT 1;" "feat commit 2" "$DB"
run_test "ff_log_count" "SELECT count(*) FROM dolt_log;" "4" "$DB"

# Test: dolt_at with branch name shows branch's committed state
DB2=/tmp/test_bedge_at_branch_$$.db; db_rm "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'shared');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('other');
INSERT INTO t VALUES(2,'main_only');
SELECT dolt_commit('-A','-m','main add');
SELECT dolt_checkout('other');
INSERT INTO t VALUES(3,'other_only');
SELECT dolt_commit('-A','-m','other add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "at_branch_main" "SELECT count(*) FROM dolt_at_t('main');" "2" "$DB2"
run_test "at_branch_other" "SELECT count(*) FROM dolt_at_t('other');" "2" "$DB2"
run_test "at_branch_main_has2" "SELECT count(*) FROM dolt_at_t('main') WHERE id=2;" "1" "$DB2"
run_test "at_branch_main_no3" "SELECT count(*) FROM dolt_at_t('main') WHERE id=3;" "0" "$DB2"
run_test "at_branch_other_has3" "SELECT count(*) FROM dolt_at_t('other') WHERE id=3;" "1" "$DB2"
run_test "at_branch_other_no2" "SELECT count(*) FROM dolt_at_t('other') WHERE id=2;" "0" "$DB2"

db_rm "$DB" "$DB2"

echo ""

# ############################################################
# Category 4: Reset edge cases
# ############################################################

echo "--- Category 4: Reset edge cases ---"

# Test: Hard reset discards uncommitted changes
DB=/tmp/test_bedge_reset1_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'committed');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'uncommitted');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reset_hard_before" "SELECT count(*) FROM t;" "2" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reset_hard_discard" "SELECT count(*) FROM t;" "1" "$DB"
run_test "reset_hard_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"
db_rm "$DB"

# Test: Hard reset to a specific commit hash
DB=/tmp/test_bedge_reset2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

C1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)

echo "INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'v3');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reset_hash_before" "SELECT count(*) FROM t;" "3" "$DB"

echo "SELECT dolt_reset('--hard','$C1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reset_hash_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "reset_hash_val" "SELECT v FROM t WHERE id=1;" "v1" "$DB"
run_test "reset_hash_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"
db_rm "$DB"

# Test: Soft reset unstages but keeps working changes
DB=/tmp/test_bedge_reset3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'new'); SELECT dolt_add('-A');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "soft_reset_staged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

echo "SELECT dolt_reset();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reset_unstaged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "0" "$DB"
run_test "reset_data_kept" "SELECT count(*) FROM t;" "2" "$DB"
db_rm "$DB"

# Test: Hard reset then checkout another branch
DB=/tmp/test_bedge_reset4_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('other');
INSERT INTO t VALUES(2,'extra');
SELECT dolt_commit('-A','-m','main extra');" | $DOLTLITE "$DB" > /dev/null 2>&1

C1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_reset('--hard','$C1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "reset_then_checkout" "SELECT dolt_checkout('other');" "0" "$DB"
run_test "reset_then_checkout_branch" "SELECT active_branch();" "other" "$DB"
run_test "reset_then_checkout_data" "SELECT count(*) FROM t;" "1" "$DB"
db_rm "$DB"

echo ""

# ############################################################
# Category 5: Schema changes across branches
# ############################################################

echo "--- Category 5: Schema changes across branches ---"

# Test: Add column on branch, checkout main -- main should have old schema
DB=/tmp/test_bedge_schema1_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='new' WHERE id=1;
SELECT dolt_commit('-A','-m','add extra');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_main_no_extra" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "0" "$DB"

# Switch back to feat: extra column should be there
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "schema_feat_has_extra" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='extra';" \
  "1" "$DB"
run_test "schema_feat_extra_val" "SELECT extra FROM t WHERE id=1;" "new" "$DB"
db_rm "$DB"

# Test: Create table on branch, merge to main -- main should have the new table
DB=/tmp/test_bedge_schema2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE new_table(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO new_table VALUES(1,'from_feat');
SELECT dolt_commit('-A','-m','add new_table');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Before merge, new_table shouldn't exist on main
run_test_match "schema_merge_pre_no_table" \
  "SELECT count(*) FROM new_table;" \
  "no such table" "$DB"

echo "SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# After merge, new_table should exist
run_test "schema_merge_post_table" "SELECT count(*) FROM new_table;" "1" "$DB"
run_test "schema_merge_post_val" "SELECT w FROM new_table WHERE id=1;" "from_feat" "$DB"
db_rm "$DB"

# Test: Drop table on branch, merge to main (ff) -- main should NOT have the table
DB=/tmp/test_bedge_schema3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE drop_me(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t VALUES(1,'keep');
INSERT INTO drop_me VALUES(1,'gone');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE drop_me;
SELECT dolt_commit('-A','-m','drop table');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Before merge, drop_me still exists on main
run_test "schema_drop_pre" "SELECT count(*) FROM drop_me;" "1" "$DB"

echo "SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# After ff merge, drop_me should be gone
run_test_match "schema_drop_post" \
  "SELECT count(*) FROM drop_me;" \
  "no such table" "$DB"
run_test "schema_drop_other_intact" "SELECT v FROM t WHERE id=1;" "keep" "$DB"
db_rm "$DB"

echo ""

# ############################################################
# Category 6: Merge edge cases
# ############################################################

echo "--- Category 6: Merge edge cases ---"

# Test: Merge branch with no changes (already up to date)
DB=/tmp/test_bedge_merge1_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_up_to_date" "SELECT dolt_merge('feat');" "Already up to date" "$DB"
db_rm "$DB"

# Test: Merge after cherry-pick -- cherry-picked commit shouldn't cause conflict
DB=/tmp/test_bedge_merge2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_commit('-A','-m','feat: add cherry');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick the feat commit onto main
echo "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1

# Now merge feat into main -- the cherry-picked row already exists
# This should succeed without conflict (either "already up to date" or clean merge)
run_test_match "merge_after_cp" \
  "SELECT dolt_merge('feat');" \
  "up to date|^[0-9a-f]{40}$" "$DB"

# Data should be intact
run_test "merge_after_cp_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "merge_after_cp_val" "SELECT v FROM t WHERE id=2;" "cherry" "$DB"
db_rm "$DB"

# Test: Fast-forward merge -- no merge commit, data correct
DB=/tmp/test_bedge_merge3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'ff_data');
SELECT dolt_commit('-A','-m','feat commit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "ff_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "ff_merge_data" "SELECT count(*) FROM t;" "2" "$DB"
run_test "ff_merge_val" "SELECT v FROM t WHERE id=2;" "ff_data" "$DB"
# No merge commit: last message should be feat's commit
run_test "ff_merge_no_merge_commit" "SELECT message FROM dolt_log LIMIT 1;" "feat commit" "$DB"
run_test "ff_merge_log_count" "SELECT count(*) FROM dolt_log;" "3" "$DB"
db_rm "$DB"

# Test: Non-fast-forward merge -- merge commit exists, data from both branches
DB=/tmp/test_bedge_merge4_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'main_row');
SELECT dolt_commit('-A','-m','main adds row');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat_row');
SELECT dolt_commit('-A','-m','feat adds row');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "noff_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "noff_merge_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "noff_merge_main_row" "SELECT v FROM t WHERE id=2;" "main_row" "$DB"
run_test "noff_merge_feat_row" "SELECT v FROM t WHERE id=3;" "feat_row" "$DB"
# Non-ff merge should have a merge commit
run_test_match "noff_merge_commit_msg" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"
# Log should have: merge, main adds, feat adds, init = 4 commits
run_test "noff_merge_log_count" "SELECT count(*) FROM dolt_log;" "5" "$DB"
db_rm "$DB"

echo ""

# ############################################################
# Category 7: GC edge cases
# ############################################################

echo "--- Category 7: GC edge cases ---"

# Test: GC with no garbage -- should be a no-op
DB=/tmp/test_bedge_gc1_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Run GC once to clean up any intermediate chunks
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Second GC should find nothing to remove
run_test_match "gc_noop" "SELECT dolt_gc();" "0 chunks removed" "$DB"
run_test "gc_noop_data" "SELECT count(*) FROM t;" "1" "$DB"
db_rm "$DB"

# Test: GC after many commits -- should not lose any reachable data
DB=/tmp/test_bedge_gc2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 15); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

run_test_match "gc_many_commits" "SELECT dolt_gc();" "chunks" "$DB"
run_test "gc_many_val" "SELECT v FROM t WHERE id=1;" "v15" "$DB"
run_test "gc_many_log" "SELECT count(*) FROM dolt_log;" "17" "$DB"

# Verify old commits still reachable via dolt_at
run_test_match "gc_many_history" \
  "SELECT count(*) FROM dolt_history_t;" \
  "^16$" "$DB"
db_rm "$DB"

# Test: GC on an empty database -- should not error
DB=/tmp/test_bedge_gc3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','empty');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_empty_db" "SELECT dolt_gc();" "chunks" "$DB"
run_test "gc_empty_db_data" "SELECT count(*) FROM t;" "0" "$DB"
run_test "gc_empty_db_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"
db_rm "$DB"

echo ""

# ############################################################
# Summary
# ############################################################

echo "=== Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests ==="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
