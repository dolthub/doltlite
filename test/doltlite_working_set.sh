#!/bin/bash
#
# Per-branch WorkingSet tests.
# Verifies that staged state and merge state are independent per branch.
#
DOLTLITE=${DOLTLITE:-./doltlite}
PASS=0; FAIL=0; ERRORS=""
run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi
}
run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi
}

echo "=== Per-Branch WorkingSet Tests ==="
echo ""

# ============================================================
# Section 1: Staged state is independent per branch
# ============================================================

DB=/tmp/test_ws_staged_$$.db; rm -f "$DB"

# Setup: create table, commit, create feature branch
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Commit different changes on each branch, then verify staged state is per-branch
# Main: modify and commit
echo "UPDATE t SET val='A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main edit');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "main_committed" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

# Checkout to feature and make a different commit
echo "SELECT dolt_checkout('feature');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature add');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1

run_test "feature_committed" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

# Now stage but DON'T commit on feature
echo "INSERT INTO t VALUES(4,'d');
SELECT dolt_add('t');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1

run_test "feature_has_staged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB/feature"

# Commit on feature so we can switch
echo "SELECT dolt_commit('-m','feature staged');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1

# Switch back to main — main should be clean
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "main_clean_after_switch" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

# Switch back to feature
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Feature was committed clean — should be clean
run_test "feature_clean_after_switch" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB/feature"

# Verify data: feature should have rows 1-4
run_test "feature_data_count" \
  "SELECT count(*) FROM t;" "4" "$DB/feature"

rm -f "$DB"

# ============================================================
# Section 2: Merge state is independent per branch
# ============================================================

DB=/tmp/test_ws_merge_$$.db; rm -f "$DB"

# Setup: two branches with conflicting changes
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
SELECT dolt_branch('feature');
UPDATE t SET val='main_change' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main edit');
SELECT dolt_checkout('feature');
UPDATE t SET val='feature_change' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature edit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge feature into main — conflict state is available in-session and can be aborted
run_test_match "main_has_conflicts" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'CF|' || count(*) FROM dolt_conflicts; ROLLBACK;" "CF\\|1" "$DB"

run_test_match "main_clean_after_abort" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_merge('--abort'); SELECT 'ST|' || count(*) FROM dolt_status; ROLLBACK;" "ST\\|0" "$DB"

run_test_match "main_val_after_abort" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_merge('--abort'); SELECT 'VAL|' || val FROM t WHERE id=1; ROLLBACK;" "VAL\\|main_change" "$DB"

rm -f "$DB"

# ============================================================
# Section 3: WorkingSet persists across sessions
# ============================================================

DB=/tmp/test_ws_persist_$$.db; rm -f "$DB"

# Session 1: create table, commit, stage a change
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
UPDATE t SET val='staged_val' WHERE id=1;
SELECT dolt_add('t');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Session 2: reopen and verify staged state persists
run_test "ws_persist_staged_count" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

run_test "ws_persist_staged_status" \
  "SELECT status FROM dolt_status WHERE staged=1;" "modified" "$DB"

rm -f "$DB"

# ============================================================
# Section 4: WorkingSet survives GC
# ============================================================

DB=/tmp/test_ws_gc_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
UPDATE t SET val='staged' WHERE id=1;
SELECT dolt_add('t');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Run GC
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Staged state should survive GC
run_test "ws_gc_staged_survives" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

run_test "ws_gc_data_ok" \
  "SELECT val FROM t WHERE id=1;" "staged" "$DB"

rm -f "$DB"

# ============================================================
# Section 5: Hard reset clears staged state
# ============================================================

DB=/tmp/test_ws_reset_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
UPDATE t SET val='changed' WHERE id=1;
SELECT dolt_add('t');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "pre_reset_staged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "post_reset_clean" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

run_test "post_reset_val" \
  "SELECT val FROM t WHERE id=1;" "a" "$DB"

rm -f "$DB"

# ============================================================
# Section 6: Multiple branches with independent staged state (file-backed)
# ============================================================

DB=/tmp/test_ws_multi_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Commit different things on each branch
echo "SELECT dolt_checkout('b1');
UPDATE t SET val='b1_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1 edit');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('b2');
INSERT INTO t VALUES(2,'b2_new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2 add');" | $DOLTLITE "$DB/b2" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify each branch has its own committed data
run_test "multi_main_count" \
  "SELECT count(*) FROM t;" "1" "$DB"

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_b1_val" \
  "SELECT val FROM t WHERE id=1;" "b1_val" "$DB/b1"

run_test "multi_b1_count" \
  "SELECT count(*) FROM t;" "1" "$DB/b1"

echo "SELECT dolt_checkout('b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_b2_count" \
  "SELECT count(*) FROM t;" "2" "$DB/b2"

run_test "multi_b2_new_row" \
  "SELECT val FROM t WHERE id=2;" "b2_new" "$DB/b2"

rm -f "$DB"

# ============================================================
# Results
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ -n "$ERRORS" ]; then echo -e "$ERRORS"; fi
if [ $FAIL -gt 0 ]; then exit 1; fi
