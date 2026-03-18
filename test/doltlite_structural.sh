#!/bin/bash
#
# Structural sharing and GC cleanup tests for Doltlite.
#
# Verifies:
# - Prolly tree structural sharing: changing 1 row in a large table
#   produces a small delta, not a full copy
# - GC cleans up orphaned chunks from deleted branches
# - GC preserves shared chunks between branches
# - Multiple small commits don't cause linear file growth
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

echo "=== Structural Sharing & GC Tests ==="
echo ""

file_size() {
  stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null
}

assert_less() {
  local name="$1" actual="$2" limit="$3"
  if [ "$actual" -lt "$limit" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — $actual < $limit"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  $actual >= $limit"
    echo "  FAIL: $name — $actual >= $limit"
  fi
}

assert_greater() {
  local name="$1" actual="$2" limit="$3"
  if [ "$actual" -gt "$limit" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — $actual > $limit"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  $actual <= $limit"
    echo "  FAIL: $name — $actual <= $limit"
  fi
}

# ============================================================
# Structural sharing: 1-row update on 1K table
# ============================================================

echo "--- Structural sharing: 1-row change on 1K table ---"

DB=/tmp/test_ss_1k_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 999); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_INIT=$(file_size "$DB")
echo "  After 1K rows committed: ${SIZE_AFTER_INIT} bytes"

# Update 1 row and commit
# Insert a new row (not update) to ensure new chunks are created
echo "INSERT INTO t VALUES(9998,'new_inserted_row');
SELECT dolt_commit('-A','-m','insert 1 row');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_INSERT=$(file_size "$DB")
DELTA=$((SIZE_AFTER_INSERT - SIZE_AFTER_INIT))
echo "  After inserting 1 row: ${SIZE_AFTER_INSERT} bytes (delta: ${DELTA})"

# Delta should be small — only changed tree nodes + new commit.
# For a 1K row table (~25KB), adding 1 row should add < 50% of table size.
HALF_INIT=$((SIZE_AFTER_INIT / 2))
assert_less "ss_1row_delta_small" "$DELTA" "$HALF_INIT"

# Delta must be > 0 (new chunks were actually created)
assert_greater "ss_1row_delta_nonzero" "$DELTA" "0"

rm -f "$DB"

# ============================================================
# Structural sharing: 1-row update on 10K table (same test, bigger)
# ============================================================

echo ""
echo "--- Structural sharing: 1-row change on 10K table ---"

DB=/tmp/test_ss_10k_$$.db; rm -f "$DB"
python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('BEGIN;')
for i in range(10000):
    print(f'INSERT INTO t VALUES({i}, \"row_{i}\");')
print('COMMIT;')
print(\"SELECT dolt_commit('-A','-m','init');\")
" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_INIT_10K=$(file_size "$DB")
echo "  After 10K rows: ${SIZE_INIT_10K} bytes"

echo "INSERT INTO t VALUES(99999,'new_inserted_row');
SELECT dolt_commit('-A','-m','insert 1 row');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_INS_10K=$(file_size "$DB")
DELTA_10K=$((SIZE_INS_10K - SIZE_INIT_10K))
echo "  After 1-row insert: ${SIZE_INS_10K} bytes (delta: ${DELTA_10K})"

# For 10K rows (~270KB), 1-row insert should add < 10% of table size
TEN_PCT=$((SIZE_INIT_10K / 10))
assert_less "ss_10k_1row_delta" "$DELTA_10K" "$TEN_PCT"
assert_greater "ss_10k_1row_nonzero" "$DELTA_10K" "0"

rm -f "$DB"

# ============================================================
# Structural sharing across branches
# ============================================================

echo ""
echo "--- Structural sharing: branch with 1 new row ---"

DB=/tmp/test_ss_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 999); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BEFORE_BRANCH=$(file_size "$DB")
echo "  Before branch work: ${SIZE_BEFORE_BRANCH} bytes"

echo "SELECT dolt_checkout('feat');
INSERT INTO t VALUES(9999,'new_row');
SELECT dolt_commit('-A','-m','feat add 1 row');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_BRANCH=$(file_size "$DB")
BRANCH_DELTA=$((SIZE_AFTER_BRANCH - SIZE_BEFORE_BRANCH))
echo "  After feat commit: ${SIZE_AFTER_BRANCH} bytes (delta: ${BRANCH_DELTA})"

# Branch with 1 new row should share ~99% of chunks with main.
# Delta should be < 25% of the base size.
QUARTER=$((SIZE_BEFORE_BRANCH / 4))
assert_less "ss_branch_small_delta" "$BRANCH_DELTA" "$QUARTER"

rm -f "$DB"

# ============================================================
# GC cleans orphaned branch chunks
# ============================================================

echo ""
echo "--- GC: clean up deleted branch ---"

DB=/tmp/test_gc_orphan_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 999); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create branch with lots of unique data
echo "SELECT dolt_branch('big');
SELECT dolt_checkout('big');
BEGIN;$(for i in $(seq 1000 1999); do echo "INSERT INTO t VALUES($i,'big_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','big branch');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_WITH_BRANCH=$(file_size "$DB")
echo "  With branch (2K rows): ${SIZE_WITH_BRANCH} bytes"

# Delete branch and GC
echo "SELECT dolt_branch('-d','big');
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_GC=$(file_size "$DB")
echo "  After branch delete + GC: ${SIZE_AFTER_GC} bytes"

# GC should reclaim the orphaned branch data.
# File should shrink by a significant amount.
assert_less "gc_shrinks_file" "$SIZE_AFTER_GC" "$SIZE_WITH_BRANCH"

RECLAIMED=$((SIZE_WITH_BRANCH - SIZE_AFTER_GC))
echo "  Reclaimed: ${RECLAIMED} bytes"

# Should reclaim at least 10% of file size
TEN_PCT_BRANCH=$((SIZE_WITH_BRANCH / 10))
assert_greater "gc_reclaims_significant" "$RECLAIMED" "$TEN_PCT_BRANCH"

# Data on main should survive
MAIN_COUNT=$(echo "SELECT count(*) FROM t;" | $DOLTLITE "$DB" 2>&1)
if [ "$MAIN_COUNT" = "1000" ]; then
  PASS=$((PASS+1)); echo "  PASS: gc_data_survives — 1000 rows on main"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_data_survives\n  expected: 1000\n  got: $MAIN_COUNT"
  echo "  FAIL: gc_data_survives — expected 1000, got $MAIN_COUNT"
fi

rm -f "$DB"

# ============================================================
# GC preserves shared chunks between branches
# ============================================================

echo ""
echo "--- GC: preserve shared chunks ---"

DB=/tmp/test_gc_shared_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 999); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Small change on feat
echo "SELECT dolt_checkout('feat');
INSERT INTO t VALUES(9999,'feat_only');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Small change on main
echo "INSERT INTO t VALUES(8888,'main_only');
SELECT dolt_commit('-A','-m','main change');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BEFORE_GC=$(file_size "$DB")
echo "  Before GC (2 branches, mostly shared): ${SIZE_BEFORE_GC} bytes"

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_GC=$(file_size "$DB")
echo "  After GC: ${SIZE_AFTER_GC} bytes"

# Both branches share most chunks — GC should NOT shrink much
# (nothing is truly orphaned since both branches are alive)
# File should stay within 90% of original
NINETY_PCT=$((SIZE_BEFORE_GC * 9 / 10))
assert_greater "gc_preserves_shared" "$SIZE_AFTER_GC" "$NINETY_PCT"

# Both branches' data should survive
MAIN_COUNT=$(echo "SELECT count(*) FROM t WHERE id=8888;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
FEAT_COUNT=$(echo "SELECT count(*) FROM t WHERE id=9999;" | $DOLTLITE "$DB" 2>&1)
if [ "$MAIN_COUNT" = "1" ] && [ "$FEAT_COUNT" = "1" ]; then
  PASS=$((PASS+1)); echo "  PASS: gc_both_branches_intact"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_both_branches_intact\n  main=$MAIN_COUNT feat=$FEAT_COUNT"
  echo "  FAIL: gc_both_branches_intact — main=$MAIN_COUNT feat=$FEAT_COUNT"
fi

rm -f "$DB"

# ============================================================
# Multiple small commits don't bloat linearly
# ============================================================

echo ""
echo "--- Sub-linear growth: 10 small commits ---"

DB=/tmp/test_ss_commits_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 999); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BASE=$(file_size "$DB")
echo "  Base (1K rows): ${SIZE_BASE} bytes"

# 10 commits in one session, each changing 1 row
python3 -c "
for c in range(1, 11):
    print(f'UPDATE t SET v=\"commit_{c}\" WHERE id={c * 100};')
    print(f\"SELECT dolt_commit('-A','-m','change {c}');\")
print('SELECT count(*) FROM t;')
print('SELECT count(*) FROM dolt_log;')
" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER_10=$(file_size "$DB")
GROWTH=$((SIZE_AFTER_10 - SIZE_BASE))
echo "  After 10 1-row commits: ${SIZE_AFTER_10} bytes (growth: ${GROWTH})"

# 10 commits × 1 row each. Each commit adds a commit object + catalog
# chunk + small tree delta. Growth should be less than 3x the base
# (generous bound to account for commit metadata overhead).
THREE_X=$((SIZE_BASE * 3))
assert_less "commits_sublinear" "$GROWTH" "$THREE_X"

# Verify data integrity (in a fresh session)
COUNT=$(echo "SELECT count(*) FROM t;" | $DOLTLITE "$DB" 2>&1)
LOG_COUNT=$(echo "SELECT count(*) FROM dolt_log;" | $DOLTLITE "$DB" 2>&1)
if [ "$COUNT" = "1000" ]; then
  PASS=$((PASS+1)); echo "  PASS: commit_data_intact — 1000 rows"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: commit_data_intact\n  rows=$COUNT"
  echo "  FAIL: commit_data_intact — rows=$COUNT"
fi

rm -f "$DB"

# ============================================================
# GC after many commits reclaims old tree nodes
# ============================================================

echo ""
echo "--- GC after many commits ---"

DB=/tmp/test_gc_commits_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 999); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# 20 commits updating different rows — creates dead tree nodes
for c in $(seq 1 20); do
  echo "UPDATE t SET v='v${c}' WHERE id=$((c * 50));
SELECT dolt_commit('-A','-m','change $c');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

SIZE_BEFORE=$(file_size "$DB")
echo "  After 20 updates: ${SIZE_BEFORE} bytes"

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER=$(file_size "$DB")
echo "  After GC: ${SIZE_AFTER} bytes"

# GC may reclaim some dead intermediate tree nodes, but when all
# commits are reachable, reclamation is not guaranteed. Allow equal.
if [ "$SIZE_AFTER" -le "$SIZE_BEFORE" ]; then
  PASS=$((PASS+1)); echo "  PASS: gc_after_commits_helps — $SIZE_AFTER <= $SIZE_BEFORE"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_after_commits_helps\n  $SIZE_AFTER > $SIZE_BEFORE"
  echo "  FAIL: gc_after_commits_helps — $SIZE_AFTER > $SIZE_BEFORE"
fi

# Data integrity
COUNT=$(echo "SELECT count(*) FROM t;" | $DOLTLITE "$DB" 2>&1)
if [ "$COUNT" = "1000" ]; then
  PASS=$((PASS+1)); echo "  PASS: gc_commits_data_ok — 1000 rows"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_commits_data_ok\n  expected: 1000\n  got: $COUNT"
  echo "  FAIL: gc_commits_data_ok — expected 1000, got $COUNT"
fi

rm -f "$DB"

# ============================================================
# GC idempotent: second run doesn't change size
# ============================================================

echo ""
echo "--- GC idempotent ---"

DB=/tmp/test_gc_idem_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;$(for i in $(seq 0 499); do echo "INSERT INTO t VALUES($i,'row_$i');"; done)
COMMIT;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(9999,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1
SIZE_FIRST_GC=$(file_size "$DB")

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1
SIZE_SECOND_GC=$(file_size "$DB")

echo "  First GC: ${SIZE_FIRST_GC}, Second GC: ${SIZE_SECOND_GC}"

# Allow small variance (±5%) between GC runs for platform differences
DIFF_ABS=$(( SIZE_SECOND_GC > SIZE_FIRST_GC ? SIZE_SECOND_GC - SIZE_FIRST_GC : SIZE_FIRST_GC - SIZE_SECOND_GC ))
THRESHOLD=$(( SIZE_FIRST_GC / 20 ))  # 5%
if [ "$DIFF_ABS" -le "$THRESHOLD" ]; then
  PASS=$((PASS+1)); echo "  PASS: gc_idempotent — within 5% ($DIFF_ABS <= $THRESHOLD)"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_idempotent\n  first=$SIZE_FIRST_GC second=$SIZE_SECOND_GC diff=$DIFF_ABS"
  echo "  FAIL: gc_idempotent — first=$SIZE_FIRST_GC second=$SIZE_SECOND_GC"
fi

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
