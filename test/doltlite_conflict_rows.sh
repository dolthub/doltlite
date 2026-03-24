#!/bin/bash
#
# Tests for per-row conflict resolution via dolt_conflicts_<tablename>.
# Uses DELETE FROM dolt_conflicts_<table> WHERE base_rowid=N to resolve
# individual conflict rows.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Per-Row Conflict Resolution Tests ==="
echo ""

# Helper: create a merge conflict on row 1 of table t
setup_conflict() {
  local DB="$1"
  rm -f "$DB"
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf_val' WHERE id=1;
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1
}

# ============================================================
# View individual conflict rows
# ============================================================

DB=/tmp/test_cfrow_view_$$.db
setup_conflict "$DB"

run_test "view_count" "SELECT count(*) FROM dolt_conflicts_t;" "1" "$DB"
run_test "view_base_rowid" "SELECT base_rowid FROM dolt_conflicts_t;" "1" "$DB"
run_test "view_our_rowid" "SELECT our_rowid FROM dolt_conflicts_t;" "1" "$DB"
run_test "view_their_rowid" "SELECT their_rowid FROM dolt_conflicts_t;" "1" "$DB"
# Values are now decoded as text (pipe-separated fields)
run_test_match "view_base_val" "SELECT typeof(base_value) FROM dolt_conflicts_t;" "text|null" "$DB"
run_test_match "view_their_val" "SELECT typeof(their_value) FROM dolt_conflicts_t;" "text" "$DB"

rm -f "$DB"

# ============================================================
# DELETE resolves individual conflict (keeps ours)
# ============================================================

DB=/tmp/test_cfrow_del_$$.db
setup_conflict "$DB"

echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

# After deleting all conflicts, the conflict table module is removed
run_test "del_summary_cleared" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "del_ours_kept" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"
run_test "del_other_ok" "SELECT v FROM t WHERE id=2;" "keep" "$DB"
# Merge commit was already created; no new commit needed after resolving
run_test "del_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

rm -f "$DB"

# ============================================================
# --ours resolution clears per-table (not all)
# ============================================================

DB=/tmp/test_cfrow_ours_$$.db
setup_conflict "$DB"

echo "SELECT dolt_conflicts_resolve('--ours','t');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ours_cleared" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "ours_val" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"

rm -f "$DB"

# ============================================================
# Conflict table not present when no conflicts
# ============================================================

DB=/tmp/test_cfrow_noconf_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "noconf_table" "SELECT count(*) FROM dolt_conflicts_t;" "no such table" "$DB"
run_test "noconf_summary" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Conflict table persists across reopen
# ============================================================

DB=/tmp/test_cfrow_persist_$$.db
setup_conflict "$DB"

# Reopen — conflicts visible
run_test "persist_summary" "SELECT count(*) FROM dolt_conflicts;" "1" "$DB"
run_test "persist_rows" "SELECT count(*) FROM dolt_conflicts_t;" "1" "$DB"
run_test "persist_rowid" "SELECT base_rowid FROM dolt_conflicts_t;" "1" "$DB"

# Delete via new session
echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify cleared after another reopen
run_test "persist_after_del" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"

rm -f "$DB"

# ============================================================
# DELETE with no matching row is a no-op
# ============================================================

DB=/tmp/test_cfrow_noop_$$.db
setup_conflict "$DB"

echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=999;" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "noop_still_there" "SELECT count(*) FROM dolt_conflicts_t;" "1" "$DB"
run_test "noop_rowid" "SELECT base_rowid FROM dolt_conflicts_t;" "1" "$DB"

rm -f "$DB"

# ============================================================
# Cherry-pick conflict with per-row resolution
# ============================================================

DB=/tmp/test_cfrow_cp_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cp_has_conflict" "SELECT count(*) FROM dolt_conflicts_t;" "1" "$DB"

echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "cp_resolved" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "cp_val" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"

rm -f "$DB"

# ============================================================
# Conflict on non-trivial rowid
# ============================================================

DB=/tmp/test_cfrow_bigid_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1000,'orig');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf_val' WHERE id=1000;
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1000;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bigid_rowid" "SELECT base_rowid FROM dolt_conflicts_t;" "1000" "$DB"

echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1000;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "bigid_cleared" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "bigid_val" "SELECT v FROM t WHERE id=1000;" "main_val" "$DB"

rm -f "$DB"

# ============================================================
# Resolve, then make new commit, then verify log
# ============================================================

DB=/tmp/test_cfrow_fullflow_$$.db
setup_conflict "$DB"

echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge commit already exists; conflicts just needed to be resolved
run_test "flow_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"
run_test "flow_val" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"
run_test "flow_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test_match "flow_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
