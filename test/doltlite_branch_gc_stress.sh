#!/bin/bash
#
# Stress tests for many branches + garbage collection.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(30);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(30);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

file_size() {
  local s=0
  for f in "$1" "${1}-wal"; do
    if [ -f "$f" ]; then
      local fs=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null)
      s=$((s + fs))
    fi
  done
  echo $s
}
db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Doltlite Branch & GC Stress Tests ==="
echo ""

# ============================================================
# Setup: create DB with table and initial commit
# ============================================================

DB=/tmp/test_branch_gc_stress_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, branch_name TEXT, val INTEGER);
INSERT INTO t VALUES(0,'main',0);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# ============================================================
# 1. Create 100 branches, each with a unique commit
# ============================================================

echo "  Creating 100 branches..."
for i in $(seq 1 100); do
  echo "SELECT dolt_branch('b$i');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "SELECT dolt_checkout('b$i');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "INSERT INTO t VALUES($i,'b$i',$i);
SELECT dolt_commit('-A','-m','commit on b$i');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
done
echo "  Done creating branches."

# ============================================================
# 2. Verify 101 branches (100 + main)
# ============================================================

run_test "many_branches_count" "SELECT count(*) FROM dolt_branches;" "101" "$DB"

# ============================================================
# 3. Checkout branch #50, verify correct data
# ============================================================

echo "SELECT dolt_checkout('b50');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "checkout_b50_data" "SELECT val FROM t WHERE id=50;" "50" "$DB"
run_test "checkout_b50_name" "SELECT branch_name FROM t WHERE id=50;" "b50" "$DB"
# b50 should have 2 rows: the init row and its own row
run_test "checkout_b50_count" "SELECT count(*) FROM t;" "2" "$DB"
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# ============================================================
# 4. Merge 5 branches (b1-b5) into main, verify accumulated data
# ============================================================

for i in $(seq 1 5); do
  echo "SELECT dolt_merge('b$i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

# main should now have: init row + 5 merged rows = 6
run_test "merge_5_count" "SELECT count(*) FROM t;" "6" "$DB"
run_test "merge_5_has_b1" "SELECT val FROM t WHERE id=1;" "1" "$DB"
run_test "merge_5_has_b5" "SELECT val FROM t WHERE id=5;" "5" "$DB"

# ============================================================
# 5. Delete 90 branches (b6-b95), keep b96-b100 unmerged
# ============================================================

echo "  Deleting 90 branches..."
for i in $(seq 6 95); do
  echo "SELECT dolt_branch('-d','b$i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done
echo "  Done deleting branches."

# Should have: main + b1..b5 (merged) + b96..b100 (unmerged) = 11
run_test "after_delete_count" "SELECT count(*) FROM dolt_branches;" "11" "$DB"

# ============================================================
# 6. GC after deleting 90 branches — should reclaim space
# ============================================================

SIZE_BEFORE_GC=$(file_size "$DB")

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_GC=$(file_size "$DB")

if [ "$SIZE_AFTER_GC" -lt "$SIZE_BEFORE_GC" ]; then
  PASS=$((PASS+1))
  echo "  PASS: gc_reclaims_space — before=$SIZE_BEFORE_GC after=$SIZE_AFTER_GC"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: gc_reclaims_space\n  before: $SIZE_BEFORE_GC\n  after:  $SIZE_AFTER_GC"
  echo "  FAIL: gc_reclaims_space — before=$SIZE_BEFORE_GC after=$SIZE_AFTER_GC"
fi

# ============================================================
# 7. After GC, verify surviving branches are intact
# ============================================================

run_test "post_gc_branch_count" "SELECT count(*) FROM dolt_branches;" "11" "$DB"

# Check main data
run_test "post_gc_main_count" "SELECT count(*) FROM t;" "6" "$DB"
run_test "post_gc_main_b3" "SELECT val FROM t WHERE id=3;" "3" "$DB"

# Check an unmerged surviving branch
echo "SELECT dolt_checkout('b98');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "post_gc_b98_data" "SELECT val FROM t WHERE id=98;" "98" "$DB"
run_test "post_gc_b98_count" "SELECT count(*) FROM t;" "2" "$DB"
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# ============================================================
# 8. After GC, verify dolt_log on main still works
# ============================================================

run_test_match "post_gc_log" "SELECT count(*) FROM dolt_log;" "^[1-9]" "$DB"

# ============================================================
# 9. After GC, verify dolt_at works for surviving branches
# ============================================================

run_test "post_gc_at_b99" \
  "SELECT count(*) FROM dolt_at_t('b99');" "2" "$DB"
run_test "post_gc_at_b99_val" \
  "SELECT val FROM dolt_at_t('b99') WHERE id=99;" "99" "$DB"

db_rm "$DB"

# ============================================================
# 10. Create branch, add 1000 rows, delete branch, GC — reclaim chunks
# ============================================================

DB=/tmp/test_gc_1000rows_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT);
INSERT INTO t VALUES(0,'base');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('bulk');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('bulk');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Insert 1000 rows in batches
echo "  Inserting 1000 rows on bulk branch..."
for batch in $(seq 0 9); do
  SQL=""
  for j in $(seq 0 99); do
    id=$((batch * 100 + j + 1))
    SQL="${SQL}INSERT INTO t VALUES($id,'data_$id');"
  done
  echo "$SQL" | $DOLTLITE "$DB" > /dev/null 2>&1
done
echo "SELECT dolt_commit('-A','-m','1000 rows');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_WITH_BULK=$(file_size "$DB")

echo "SELECT dolt_branch('-d','bulk');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_BULK_GC=$(file_size "$DB")

if [ "$SIZE_AFTER_BULK_GC" -lt "$SIZE_WITH_BULK" ]; then
  PASS=$((PASS+1))
  echo "  PASS: gc_1000rows — before=$SIZE_WITH_BULK after=$SIZE_AFTER_BULK_GC"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: gc_1000rows\n  before: $SIZE_WITH_BULK\n  after:  $SIZE_AFTER_BULK_GC"
  echo "  FAIL: gc_1000rows — before=$SIZE_WITH_BULK after=$SIZE_AFTER_BULK_GC"
fi

# Main data still intact
run_test "gc_1000rows_main" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_1000rows_val" "SELECT data FROM t WHERE id=0;" "base" "$DB"

db_rm "$DB"

# ============================================================
# 11. Structural sharing: 2 branches from same commit — minimal size increase
# ============================================================

DB=/tmp/test_struct_share_$$.db; db_rm "$DB"

# Create a table with enough data to make size meaningful
SQL="CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT);"
for i in $(seq 1 200); do
  SQL="${SQL}INSERT INTO t VALUES($i,'row_data_padding_for_size_$i');"
done
SQL="${SQL}SELECT dolt_commit('-A','-m','init');"
echo "$SQL" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BASE=$(file_size "$DB")

echo "SELECT dolt_branch('share_a');
SELECT dolt_branch('share_b');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_WITH_BRANCHES=$(file_size "$DB")

# Two branches from same commit should add very little
BRANCH_OVERHEAD=$((SIZE_WITH_BRANCHES - SIZE_BASE))
# Allow up to 10% overhead for branch metadata
LIMIT=$((SIZE_BASE / 10))
if [ "$LIMIT" -lt 4096 ]; then LIMIT=4096; fi

if [ "$BRANCH_OVERHEAD" -lt "$LIMIT" ]; then
  PASS=$((PASS+1))
  echo "  PASS: struct_share_branches — overhead=$BRANCH_OVERHEAD limit=$LIMIT"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: struct_share_branches\n  overhead: $BRANCH_OVERHEAD\n  limit:    $LIMIT"
  echo "  FAIL: struct_share_branches — overhead=$BRANCH_OVERHEAD limit=$LIMIT"
fi

# ============================================================
# 12. Modify 1 row on each branch — small size increase
# ============================================================

echo "SELECT dolt_checkout('share_a');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET data='modified_a' WHERE id=1;
SELECT dolt_commit('-A','-m','mod a');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('share_b');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET data='modified_b' WHERE id=100;
SELECT dolt_commit('-A','-m','mod b');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_MODS=$(file_size "$DB")
MOD_INCREASE=$((SIZE_AFTER_MODS - SIZE_WITH_BRANCHES))

# Modifying 1 row per branch should add much less than the total data size
if [ "$MOD_INCREASE" -lt "$SIZE_BASE" ]; then
  PASS=$((PASS+1))
  echo "  PASS: struct_share_mods — increase=$MOD_INCREASE base=$SIZE_BASE"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: struct_share_mods\n  increase: $MOD_INCREASE\n  base:     $SIZE_BASE"
  echo "  FAIL: struct_share_mods — increase=$MOD_INCREASE base=$SIZE_BASE"
fi

db_rm "$DB"

# ============================================================
# 13. GC on database with no unreachable chunks — no-op
# ============================================================

DB=/tmp/test_gc_noop_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# First GC to clean any intermediate chunks
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Second GC should be a no-op
run_test_match "gc_noop" "SELECT dolt_gc();" "0 chunks removed" "$DB"

SIZE_BEFORE_NOOP=$(file_size "$DB")
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1
SIZE_AFTER_NOOP=$(file_size "$DB")

if [ "$SIZE_AFTER_NOOP" -eq "$SIZE_BEFORE_NOOP" ]; then
  PASS=$((PASS+1))
  echo "  PASS: gc_noop_size — unchanged at $SIZE_AFTER_NOOP"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: gc_noop_size\n  before: $SIZE_BEFORE_NOOP\n  after:  $SIZE_AFTER_NOOP"
fi

db_rm "$DB"

# ============================================================
# 14. GC immediately after first commit — should keep everything
# ============================================================

DB=/tmp/test_gc_first_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello');
SELECT dolt_commit('-A','-m','first');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_first_commit" "SELECT dolt_gc();" "chunks" "$DB"
run_test "gc_first_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_first_val" "SELECT v FROM t WHERE id=1;" "hello" "$DB"
run_test "gc_first_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"

db_rm "$DB"

# ============================================================
# 15. Create and delete same branch name repeatedly
# ============================================================

DB=/tmp/test_gc_recycle_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for cycle in $(seq 1 10); do
  echo "SELECT dolt_branch('recycled');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "SELECT dolt_checkout('recycled');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "INSERT INTO t VALUES($((cycle + 100)),'cycle_$cycle');
SELECT dolt_commit('-A','-m','cycle $cycle');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "SELECT dolt_branch('-d','recycled');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

# After 10 create/delete cycles, only main should remain
run_test "recycle_branch_count" "SELECT count(*) FROM dolt_branches;" "1" "$DB"

# Main data should be unaffected
run_test "recycle_main_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "recycle_main_val" "SELECT v FROM t WHERE id=1;" "init" "$DB"

# GC should clean up all orphaned cycle commits
run_test_match "recycle_gc" "SELECT dolt_gc();" "chunks removed" "$DB"

# Data still intact after GC
run_test "recycle_post_gc_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "recycle_post_gc_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"

# Create the branch one more time to prove the name is reusable after GC
echo "SELECT dolt_branch('recycled');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "recycle_reuse_after_gc" "SELECT count(*) FROM dolt_branches;" "2" "$DB"

db_rm "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
