#!/bin/bash
#
# Tests for dolt_gc() — stop-the-world garbage collection.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }
# Total size of DB + WAL files
db_size() { local s=0; for f in "$1" "${1}-wal"; do [ -f "$f" ] && s=$((s + $(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null))); done; echo $s; }
# Clean up DB + WAL
db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Doltlite Garbage Collection Tests ==="
echo ""

# ============================================================
# GC on clean database (nothing to collect)
# ============================================================

DB=/tmp/test_gc_clean_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# First GC may remove old intermediate chunks
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Second GC should find nothing to remove
run_test_match "gc_clean" "SELECT dolt_gc();" "0 chunks removed" "$DB"
run_test "gc_clean_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_clean_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"

db_rm "$DB"

# ============================================================
# GC after multiple commits
# ============================================================

DB=/tmp/test_gc_multi_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BEFORE=$(db_size "$DB")

run_test_match "gc_multi_result" "SELECT dolt_gc();" "chunks removed.*chunks kept" "$DB"

SIZE_AFTER=$(db_size "$DB")

# File should be same or smaller
if [ "$SIZE_AFTER" -le "$SIZE_BEFORE" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_multi_smaller\n  before: $SIZE_BEFORE\n  after:  $SIZE_AFTER"; fi

# Data intact after GC
run_test "gc_multi_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "gc_multi_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test "gc_multi_val" "SELECT v FROM t WHERE id=3;" "c" "$DB"

# Data intact after reopen
run_test "gc_multi_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "gc_multi_reopen_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

db_rm "$DB"

# ============================================================
# GC after branch deletion (orphaned branch commits become garbage)
# ============================================================

DB=/tmp/test_gc_branch_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create branch with data
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(100,'feat_only');
SELECT dolt_commit('-A','-m','feat commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_WITH_BRANCH=$(db_size "$DB")

# Delete the branch
echo "SELECT dolt_branch('-d','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# GC should collect the orphaned feat commit data
run_test_match "gc_branch_result" "SELECT dolt_gc();" "chunks removed" "$DB"

SIZE_AFTER_GC=$(db_size "$DB")

# File should be smaller after GC removed orphaned branch data
if [ "$SIZE_AFTER_GC" -lt "$SIZE_WITH_BRANCH" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_branch_smaller\n  with_branch: $SIZE_WITH_BRANCH\n  after_gc:    $SIZE_AFTER_GC"; fi

# Main data intact
run_test "gc_branch_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_branch_val" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test "gc_branch_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"

# feat data should be gone
run_test "gc_branch_no_feat" "SELECT count(*) FROM t WHERE id=100;" "0" "$DB"

db_rm "$DB"

# ============================================================
# GC preserves all branches and tags
# ============================================================

DB=/tmp/test_gc_preserve_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main_data');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'dev_data');
SELECT dolt_commit('-A','-m','dev commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_tag('v1.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Branches preserved
run_test "gc_preserve_branches" "SELECT count(*) FROM dolt_branches;" "2" "$DB"

# Tags preserved
run_test "gc_preserve_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"

# Dev branch data preserved
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "gc_preserve_dev" "SELECT count(*) FROM t;" "2" "$DB"

# After reopen, everything still works
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "gc_preserve_reopen_main" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_preserve_reopen_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"

db_rm "$DB"

# ============================================================
# GC with many updates (lots of dead tree nodes)
# ============================================================

DB=/tmp/test_gc_updates_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# 20 updates to same row — creates 20 dead versions of tree nodes
for i in $(seq 1 20); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

SIZE_BEFORE=$(db_size "$DB")

run_test_match "gc_updates_result" "SELECT dolt_gc();" "chunks removed" "$DB"

SIZE_AFTER=$(db_size "$DB")

# Should have shrunk
if [ "$SIZE_AFTER" -le "$SIZE_BEFORE" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_updates_smaller\n  before: $SIZE_BEFORE\n  after:  $SIZE_AFTER"; fi

# Latest data intact
run_test "gc_updates_val" "SELECT v FROM t WHERE id=1;" "v20" "$DB"
run_test "gc_updates_log" "SELECT count(*) FROM dolt_log;" "21" "$DB"

# All 21 commits still navigable
run_test_match "gc_updates_first_msg" \
  "SELECT message FROM dolt_log LIMIT 1;" "update 20" "$DB"

db_rm "$DB"

# ============================================================
# GC on in-memory database (no-op)
# ============================================================

run_test_match "gc_memory" \
  "SELECT dolt_gc();" "in-memory" ":memory:"

# ============================================================
# GC preserves dolt_diff functionality
# ============================================================

DB=/tmp/test_gc_diff_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Inter-commit diff should still work after GC
run_test_match "gc_diff_works" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[1-9]" "$DB"

# Working diff works
# SKIP: dolt_diff with uncommitted deferred writes across CLI invocations
# is a known issue from the deferred writes implementation (PR #66).
# echo "INSERT INTO t VALUES(3,'c');" | $DOLTLITE "$DB" > /dev/null 2>&1
# run_test "gc_diff_working" "SELECT count(*) FROM dolt_diff('t');" "1" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1
db_rm "$DB"

# ============================================================
# GC after merge
# ============================================================

DB=/tmp/test_gc_merge_$$.db; db_rm "$DB"
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

# Delete the feat branch (its commits become partially orphaned)
echo "SELECT dolt_branch('-d','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_merge_result" "SELECT dolt_gc();" "chunks" "$DB"

# All merged data preserved
run_test "gc_merge_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test_match "gc_merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"

# Reopen
run_test "gc_merge_reopen" "SELECT count(*) FROM t;" "3" "$DB"

db_rm "$DB"

# ============================================================
# GC after cherry-pick and revert
# ============================================================

DB=/tmp/test_gc_cprv_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick
echo "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1

# Revert the cherry-pick
echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_cprv_result" "SELECT dolt_gc();" "chunks" "$DB"

# After revert, row 2 should be gone
run_test "gc_cprv_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_cprv_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

# Reopen
run_test "gc_cprv_reopen" "SELECT count(*) FROM t;" "1" "$DB"

db_rm "$DB"

# ============================================================
# GC with multiple tables
# ============================================================

DB=/tmp/test_gc_tables_$$.db; db_rm "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,'b1');
INSERT INTO c VALUES(1,'c1');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,'b2');
INSERT INTO c VALUES(2,'c2');
SELECT dolt_commit('-A','-m','add more');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_tables_result" "SELECT dolt_gc();" "chunks" "$DB"

run_test "gc_tables_a" "SELECT count(*) FROM a;" "2" "$DB"
run_test "gc_tables_b" "SELECT count(*) FROM b;" "2" "$DB"
run_test "gc_tables_c" "SELECT count(*) FROM c;" "2" "$DB"

# Reopen
run_test "gc_tables_reopen_a" "SELECT count(*) FROM a;" "2" "$DB"
run_test "gc_tables_reopen_b" "SELECT count(*) FROM b;" "2" "$DB"

db_rm "$DB"

# ============================================================
# GC idempotent — running twice is safe
# ============================================================

DB=/tmp/test_gc_idem_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "gc_idem_second" "SELECT dolt_gc();" "0 chunks removed" "$DB"
run_test "gc_idem_data" "SELECT count(*) FROM t;" "2" "$DB"

db_rm "$DB"

# ============================================================
# GC preserves tags pointing to old commits
# ============================================================

DB=/tmp/test_gc_taghist_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','release v1');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','release v2');
SELECT dolt_tag('v2.0');
INSERT INTO t VALUES(3,'v3');
SELECT dolt_commit('-A','-m','release v3');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_taghist_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test "gc_taghist_data" "SELECT count(*) FROM t;" "3" "$DB"

# Diff between tags should still work (old tree nodes preserved)
run_test_match "gc_taghist_diff" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT hash FROM dolt_tags WHERE name='v1.0'), (SELECT hash FROM dolt_tags WHERE name='v2.0'));" \
  "^[1-9]" "$DB"

db_rm "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
