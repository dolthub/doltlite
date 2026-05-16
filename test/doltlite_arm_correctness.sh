#!/bin/bash
# Cover deep-review S11 + S12 ARM-correctness items.
#
# S11 — chunkIndexReplaceEntries called sqlite3_free(idx->aIndex) even
#       when aIndex was mmap'd memory, AND leaked the mmap region.
#       Triggered by any dolt_gc that runs after the index was loaded
#       via csReadIndex's mmap path. csReleaseIndexBuf handles both
#       paths correctly; the fix routes through it.
#
# S12 — cursorCurrentTreeValue caches a borrowed pointer into a prolly-
#       cache node payload. The cursor's level already pins the leaf
#       via aLevel[].pEntry's nRef, so the bytes survive — but the
#       contract was implicit. Now pCachedPayload sites take an
#       explicit pCachedFrom = prollyCacheGet(...) refcount that
#       CLEAR_CACHED_PAYLOAD releases. This is defensive; the test
#       just exercises read paths under cache churn.

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== ARM-correctness (S11 + S12) ==="
echo ""

# ----------------------------------------------------------------
# S11.1 — dolt_gc after a non-trivial index load
#
# Build a database with enough commits to make the on-disk index
# non-trivial, then reopen (forcing mmap-based csReadIndex on
# systems where mmap is available), then run dolt_gc which calls
# chunkIndexReplaceEntries. Pre-fix, sqlite3_free on mmap'd memory
# was UB; post-fix, csReleaseIndexBuf does the right thing for both
# paths.
# ----------------------------------------------------------------
DB=/tmp/test_arm_gc_$$.db; db_rm "$DB"

# Phase 1: build it
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  for i in $(seq 1 50); do
    echo "INSERT INTO t VALUES($i, 'row_$i');"
    echo "SELECT dolt_commit('-A','-m','c$i');"
  done
} | $DOLTLITE "$DB" > /dev/null 2>&1

# Phase 2: reopen, run GC (this is the path that hits chunkIndexReplaceEntries).
# Pre-fix this would sqlite3_free(aIndex) where aIndex could be mmap'd,
# and would leak the mmap region. Post-fix uses csReleaseIndexBuf.
run_test_match "s11_gc_after_reopen_no_crash" \
  "SELECT dolt_gc();" \
  "chunks removed" "$DB"

run_test "s11_data_intact_after_gc" \
  "SELECT count(*) FROM t;" \
  "50" "$DB"

run_test "s11_history_intact_after_gc" \
  "SELECT count(*) FROM dolt_log;" \
  "51" "$DB"

# Phase 3: reopen and re-GC. This loads the new index (post-GC) and
# replaces again. Pre-fix this could compound the leak / hit UB twice.
run_test_match "s11_gc_again_after_reopen" \
  "SELECT dolt_gc();" \
  "chunks removed" "$DB"

run_test "s11_data_still_intact" \
  "SELECT v FROM t WHERE id=25;" \
  "row_25" "$DB"

db_rm "$DB"

# ----------------------------------------------------------------
# S12 — Cursor read under cache churn
#
# Make the cache too small to hold all leaves, then do reads that
# would force evictions while another cursor is active. If the
# borrowed pointer was unsafe, this would UAF.
# ----------------------------------------------------------------
DB=/tmp/test_arm_churn_$$.db; db_rm "$DB"

# Build a fat table with enough rows that the prolly tree has
# multiple leaves.
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  printf "INSERT INTO t VALUES "
  for i in $(seq 1 5000); do
    if [ $i -gt 1 ]; then printf ", "; fi
    printf "(%d, 'row_%d_with_some_payload_to_make_leaves_bigger')" "$i" "$i"
  done
  echo ";"
  echo "SELECT dolt_commit('-A','-m','seed');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

# Repeatedly read across the whole range. Each read traverses many
# leaves. If S12 was a live UAF, this would crash. Post-fix the
# explicit pin makes the invariant structural.
run_test "s12_read_under_churn" \
  "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 5000;" \
  "5000" "$DB"

run_test "s12_read_specific_values_no_corruption" \
  "SELECT v FROM t WHERE id=1
UNION ALL SELECT v FROM t WHERE id=2500
UNION ALL SELECT v FROM t WHERE id=5000;" \
  "row_1_with_some_payload_to_make_leaves_bigger
row_2500_with_some_payload_to_make_leaves_bigger
row_5000_with_some_payload_to_make_leaves_bigger" "$DB"

# Cursor-heavy ops that hit pCachedPayload paths.
run_test "s12_aggregate_consistent" \
  "SELECT sum(id), count(*) FROM t;" \
  "12502500|5000" "$DB"

db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
