#!/bin/bash
#
# Tests for dolt_cherry_pick and dolt_revert.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Cherry-Pick & Revert Tests ==="
echo ""

# ============================================================
# Cherry-pick: basic functionality
# ============================================================

DB=/tmp/test_cp_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','add feat_row');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick the feat commit onto main
run_test_match "cp_basic_hash" \
  "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_basic_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_basic_val" "SELECT v FROM t WHERE id=2;" "feat_row" "$DB"
run_test_match "cp_basic_msg" "SELECT message FROM dolt_log LIMIT 1;" "^add feat_row$" "$DB"
run_test "cp_basic_branch" "SELECT active_branch();" "main" "$DB"
run_test "cp_basic_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: pick from middle of history
# ============================================================

DB=/tmp/test_cp_middle_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(10,'feat1');
SELECT dolt_commit('-A','-m','feat commit 1');
INSERT INTO t VALUES(11,'feat2');
SELECT dolt_commit('-A','-m','feat commit 2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Get the first feat commit hash (the one that added row 10)
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
CP_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick only the first feat commit
run_test_match "cp_middle_pick" \
  "SELECT dolt_cherry_pick('$CP_HASH');" \
  "^[0-9a-f]{40}$" "$DB"

# Should only have row 10, not row 11
run_test "cp_middle_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_middle_has10" "SELECT v FROM t WHERE id=10;" "feat1" "$DB"
run_test "cp_middle_no11" "SELECT count(*) FROM t WHERE id=11;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: with conflict
# ============================================================

DB=/tmp/test_cp_conflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE id=1;
SELECT dolt_commit('-A','-m','feat modifies row 1');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main modifies row 1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_conflict_msg" \
  "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "conflict" "$DB"

run_test_match "cp_conflict_count" "SELECT count(*) FROM dolt_conflicts;" "^[1-9]" "$DB"
run_test_match "cp_conflict_blocked" "SELECT dolt_commit('-A','-m','fail');" "unresolved" "$DB"

# Resolve with ours
echo "SELECT dolt_conflicts_resolve('--ours','t');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "cp_conflict_resolved" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "cp_conflict_ours" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: non-conflicting changes
# ============================================================

DB=/tmp/test_cp_noconflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','feat adds row 3');
SELECT dolt_checkout('main');
UPDATE t SET v='A' WHERE id=1;
SELECT dolt_commit('-A','-m','main updates row 1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick should cleanly add row 3 without conflicting with row 1 change
run_test_match "cp_noc_hash" \
  "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "cp_noc_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_noc_row1" "SELECT v FROM t WHERE id=1;" "A" "$DB"
run_test "cp_noc_row3" "SELECT v FROM t WHERE id=3;" "c" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: error cases
# ============================================================

DB=/tmp/test_cp_errors_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# No arguments
run_test_match "cp_err_noarg" "SELECT dolt_cherry_pick();" "usage" "$DB"

# Invalid hash
run_test_match "cp_err_badhash" "SELECT dolt_cherry_pick('not_a_hash');" "invalid" "$DB"

# Cannot cherry-pick the root (seed) commit: it has no parent to replay
run_test_match "cp_err_initial" \
  "SELECT dolt_cherry_pick((SELECT commit_hash FROM dolt_log WHERE message='Initialize data repository'));" \
  "initial commit" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: with file persistence
# ============================================================

DB=/tmp/test_cp_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_data');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify data persists after reopen
run_test "cp_persist_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_persist_val" "SELECT v FROM t WHERE id=2;" "feat_data" "$DB"
run_test_match "cp_persist_log" "SELECT message FROM dolt_log LIMIT 1;" "^feat add$" "$DB"

rm -f "$DB"

# ============================================================
# Revert: basic functionality
# ============================================================

DB=/tmp/test_rv_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'added');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_before_count" "SELECT count(*) FROM t;" "2" "$DB"

# Revert the last commit (add row 2)
run_test_match "rv_basic_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "rv_basic_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "rv_basic_val" "SELECT v FROM t WHERE id=1;" "init" "$DB"
run_test "rv_basic_no2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test_match "rv_basic_msg" "SELECT message FROM dolt_log LIMIT 1;" "^Revert \"add row 2\"$" "$DB"
run_test "rv_basic_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

# ============================================================
# Revert: undo an UPDATE
# ============================================================

DB=/tmp/test_rv_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='changed' WHERE id=1;
SELECT dolt_commit('-A','-m','update row 1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_upd_before" "SELECT v FROM t WHERE id=1;" "changed" "$DB"

run_test_match "rv_upd_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

# Row 1 should be reverted to 'original'
run_test "rv_upd_reverted" "SELECT v FROM t WHERE id=1;" "original" "$DB"
# Row 2 should be unchanged
run_test "rv_upd_other" "SELECT v FROM t WHERE id=2;" "keep" "$DB"
run_test "rv_upd_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Revert: revert a middle commit
# ============================================================

DB=/tmp/test_rv_middle_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'second');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'third');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Revert c2 (added row 2) while keeping c3 (row 3)
run_test_match "rv_mid_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "^[0-9a-f]{40}$" "$DB"

# Row 2 should be gone, row 3 should remain
run_test "rv_mid_no2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test "rv_mid_has3" "SELECT v FROM t WHERE id=3;" "third" "$DB"
run_test "rv_mid_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Revert: with conflict
# ============================================================

DB=/tmp/test_rv_conflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='v2' WHERE id=1;
SELECT dolt_commit('-A','-m','update to v2');
UPDATE t SET v='v3' WHERE id=1;
SELECT dolt_commit('-A','-m','update to v3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Revert the c2 commit (orig->v2). Since c3 also modified row 1,
# this should conflict.
run_test_match "rv_conf_msg" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "conflict" "$DB"

run_test_match "rv_conf_count" "SELECT count(*) FROM dolt_conflicts;" "^[1-9]" "$DB"

# Resolve with ours (keep v3)
echo "SELECT dolt_conflicts_resolve('--ours','t');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "rv_conf_ours" "SELECT v FROM t WHERE id=1;" "v3" "$DB"

rm -f "$DB"

# ============================================================
# Revert: error cases
# ============================================================

DB=/tmp/test_rv_errors_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_noarg_noop" "SELECT dolt_revert();" "0" "$DB"
run_test_match "rv_err_badhash" "SELECT dolt_revert('bad');" "invalid" "$DB"
run_test_match "rv_err_initial" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='Initialize data repository'));" \
  "initial commit" "$DB"

rm -f "$DB"

# ============================================================
# Revert: persistence
# ============================================================

DB=/tmp/test_rv_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'to_revert');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify after reopen
run_test "rv_persist_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test_match "rv_persist_log" "SELECT message FROM dolt_log LIMIT 1;" "Revert" "$DB"
run_test "rv_persist_log_count" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick + Revert combined workflow
# ============================================================

DB=/tmp/test_cp_rv_combo_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick feat onto main
echo "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "combo_after_cp" "SELECT count(*) FROM t;" "2" "$DB"

# Now revert the cherry-pick
run_test_match "combo_revert" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "combo_after_rv" "SELECT count(*) FROM t;" "1" "$DB"
run_test "combo_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: multiple cherry-picks
# ============================================================

DB=/tmp/test_cp_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(10,'cp1');
SELECT dolt_commit('-A','-m','feat1');
INSERT INTO t VALUES(11,'cp2');
SELECT dolt_commit('-A','-m','feat2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick first feat commit
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_cherry_pick('$HASH1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cp_multi_first" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_multi_has10" "SELECT v FROM t WHERE id=10;" "cp1" "$DB"

# Cherry-pick second feat commit
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH2=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_cherry_pick('$HASH2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cp_multi_both" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_multi_has11" "SELECT v FROM t WHERE id=11;" "cp2" "$DB"
run_test "cp_multi_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

# ============================================================
# Revert: revert then revert the revert
# ============================================================

DB=/tmp/test_rv_double_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'added');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Revert the add
echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "rv_double_after1" "SELECT count(*) FROM t;" "1" "$DB"

# Revert the revert — should bring row 2 back
run_test_match "rv_double_revert2" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_double_after2" "SELECT count(*) FROM t;" "2" "$DB"
run_test "rv_double_val" "SELECT v FROM t WHERE id=2;" "added" "$DB"
run_test "rv_double_log" "SELECT count(*) FROM dolt_log;" "5" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: pick onto branch with diverged history
# ============================================================

DB=/tmp/test_cp_diverged_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(10,'feat_row');
SELECT dolt_commit('-A','-m','feat commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(20,'main_row');
SELECT dolt_commit('-A','-m','main commit');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick feat commit onto main (which already has its own changes)
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_div_hash" "SELECT dolt_cherry_pick('$HASH');" "^[0-9a-f]{40}$" "$DB"
run_test "cp_div_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_div_has10" "SELECT v FROM t WHERE id=10;" "feat_row" "$DB"
run_test "cp_div_has20" "SELECT v FROM t WHERE id=20;" "main_row" "$DB"

rm -f "$DB"

# ============================================================
# Revert: multi-row commit
# ============================================================

DB=/tmp/test_rv_multirow_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'a');
INSERT INTO t VALUES(3,'b');
INSERT INTO t VALUES(4,'c');
SELECT dolt_commit('-A','-m','add 3 rows');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_multi_before" "SELECT count(*) FROM t;" "4" "$DB"

run_test_match "rv_multi_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "rv_multi_after" "SELECT count(*) FROM t;" "1" "$DB"
run_test "rv_multi_only_init" "SELECT v FROM t WHERE id=1;" "init" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick: new table from another branch
# ============================================================

DB=/tmp/test_cp_newtable_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'new_table');
SELECT dolt_commit('-A','-m','feat: add t2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_newtbl_hash" \
  "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "cp_newtbl_t" "SELECT count(*) FROM t;" "1" "$DB"
run_test "cp_newtbl_t2" "SELECT count(*) FROM t2;" "1" "$DB"
run_test "cp_newtbl_val" "SELECT w FROM t2 WHERE id=1;" "new_table" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
