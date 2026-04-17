#!/bin/bash
#
# Large merge tests.
#
# Verifies merge correctness at scale: 10K-100K row merges with
# various conflict patterns. Tests both the streaming three-way
# diff and the merge application path under memory pressure.
#
# Usage: bash test/large_merge_test.sh [path/to/doltlite] [--quick]
#

set -u

DOLTLITE="${1:-./doltlite}"
QUICK=0
if [ "${2:-}" = "--quick" ]; then QUICK=1; fi
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

echo "=== Large Merge Tests ==="

# Helper: generate N INSERT statements for a table t(id PK, v TEXT)
gen_inserts() {
  local start=$1 end=$2 prefix=$3
  local i
  for i in $(seq $start $end); do
    echo "INSERT INTO t VALUES($i,'${prefix}_$i');"
  done
}

# Helper: generate N UPDATE statements
gen_updates() {
  local start=$1 end=$2 new_prefix=$3
  local i
  for i in $(seq $start $end); do
    echo "UPDATE t SET v='${new_prefix}_$i' WHERE id=$i;"
  done
}

# ── Scenario 1: Large non-overlapping merge (10K rows each side) ──
echo ""
echo "--- Scenario 1: Non-overlapping merge (10K rows each side) ---"

DB="$TMPROOT/s1.db"
rm -f "$DB"

# Base: rows 1-1000
"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 1000 base
  echo "SELECT dolt_commit('-Am','base');"
)" >/dev/null 2>&1

# Branch feat: add rows 1001-11000
"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_inserts 1001 11000 feat
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat adds 10K');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

# Main: add rows 11001-21000
"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  gen_inserts 11001 21000 main
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main adds 10K');"
)" >/dev/null 2>&1

# Merge
RESULT=$("$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" 2>&1)
COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
if [ "$COUNT" = "21000" ]; then
  pass_name "s1_non_overlapping_10k_count"
else
  fail_name "s1_non_overlapping_10k_count"
  echo "    expected 21000, got $COUNT"
fi

# Verify data from both sides survived
V1=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=5000;" 2>/dev/null)
V2=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=15000;" 2>/dev/null)
if [ "$V1" = "feat_5000" ] && [ "$V2" = "main_15000" ]; then
  pass_name "s1_non_overlapping_10k_values"
else
  fail_name "s1_non_overlapping_10k_values"
  echo "    v1=$V1 v2=$V2"
fi

CONFLICTS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
if [ "$CONFLICTS" = "0" ]; then
  pass_name "s1_no_conflicts"
else
  fail_name "s1_no_conflicts"
  echo "    conflicts=$CONFLICTS"
fi

echo ""

# ── Scenario 2: Large overlapping updates (both sides modify same rows) ──
echo "--- Scenario 2: Overlapping updates (5K rows modified by both sides) ---"

DB="$TMPROOT/s2.db"
rm -f "$DB"

"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 5000 base
  echo "SELECT dolt_commit('-Am','base 5K rows');"
)" >/dev/null 2>&1

# feat: update odd rows
"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  for i in $(seq 1 2 5000); do
    echo "UPDATE t SET v='feat_$i' WHERE id=$i;"
  done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat updates odd rows');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

# main: update even rows
"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  for i in $(seq 2 2 5000); do
    echo "UPDATE t SET v='main_$i' WHERE id=$i;"
  done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main updates even rows');"
)" >/dev/null 2>&1

# Merge - should succeed, no overlap
RESULT=$("$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" 2>&1)
COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
if [ "$COUNT" = "5000" ]; then
  pass_name "s2_overlapping_5k_count"
else
  fail_name "s2_overlapping_5k_count"
  echo "    expected 5000, got $COUNT"
fi

V_ODD=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=2999;" 2>/dev/null)
V_EVEN=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=3000;" 2>/dev/null)
if [ "$V_ODD" = "feat_2999" ] && [ "$V_EVEN" = "main_3000" ]; then
  pass_name "s2_interleaved_values"
else
  fail_name "s2_interleaved_values"
  echo "    odd=$V_ODD even=$V_EVEN"
fi

CONFLICTS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
if [ "$CONFLICTS" = "0" ]; then
  pass_name "s2_no_conflicts"
else
  fail_name "s2_no_conflicts"
  echo "    conflicts=$CONFLICTS"
fi

echo ""

# ── Scenario 3: Massive conflict merge (both sides update same rows differently) ──
echo "--- Scenario 3: Conflict merge (1000 rows conflicting) ---"

DB="$TMPROOT/s3.db"
rm -f "$DB"

"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 2000 base
  echo "SELECT dolt_commit('-Am','base 2K');"
)" >/dev/null 2>&1

# feat: update rows 501-1500
"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_updates 501 1500 feat
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat updates 1K');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

# main: update rows 1001-2000 (overlap: 1001-1500 = 500 rows conflicting)
"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  gen_updates 1001 2000 main
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main updates 1K');"
)" >/dev/null 2>&1

# Merge - should produce conflicts for rows 1001-1500
"$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
CONFLICTS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
if [ "$CONFLICTS" = "1" ]; then
  pass_name "s3_has_conflicts"
else
  fail_name "s3_has_conflicts"
  echo "    expected 1 (one table), got $CONFLICTS"
fi

# Non-conflicting rows should be merged correctly
V500=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=500;" 2>/dev/null)
V600=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=600;" 2>/dev/null)
V1800=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=1800;" 2>/dev/null)
if [ "$V500" = "base_500" ] && [ "$V600" = "feat_600" ] && [ "$V1800" = "main_1800" ]; then
  pass_name "s3_non_conflicting_values_correct"
else
  fail_name "s3_non_conflicting_values_correct"
  echo "    v500=$V500 v600=$V600 v1800=$V1800"
fi

echo ""

# ── Scenario 4: Large delete vs modify (one side deletes, other modifies) ──
echo "--- Scenario 4: Large delete vs modify (500 rows) ---"

DB="$TMPROOT/s4.db"
rm -f "$DB"

"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 2000 base
  echo "SELECT dolt_commit('-Am','base');"
)" >/dev/null 2>&1

# feat: delete rows 501-1000
"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  for i in $(seq 501 1000); do
    echo "DELETE FROM t WHERE id=$i;"
  done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat deletes 500');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

# main: modify rows 501-1000
"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  gen_updates 501 1000 main_mod
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main modifies same 500');"
)" >/dev/null 2>&1

# Merge - should produce delete-modify conflicts
"$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
CONFLICTS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
if [ "$CONFLICTS" = "1" ]; then
  pass_name "s4_delete_modify_conflicts"
else
  fail_name "s4_delete_modify_conflicts"
  echo "    conflicts=$CONFLICTS"
fi

# Rows outside the conflict zone should be untouched
V1=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=1;" 2>/dev/null)
V2000=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=2000;" 2>/dev/null)
if [ "$V1" = "base_1" ] && [ "$V2000" = "base_2000" ]; then
  pass_name "s4_non_overlapping_rows_intact"
else
  fail_name "s4_non_overlapping_rows_intact"
  echo "    v1=$V1 v2000=$V2000"
fi

echo ""

# ── Scenario 5: Convergent merge (both sides make same changes) ──
echo "--- Scenario 5: Convergent merge (both sides same 1K updates) ---"

DB="$TMPROOT/s5.db"
rm -f "$DB"

"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 2000 base
  echo "SELECT dolt_commit('-Am','base');"
)" >/dev/null 2>&1

# Both sides make the exact same change
"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_updates 501 1500 converged
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat converged updates');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  gen_updates 501 1500 converged
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main converged updates');"
)" >/dev/null 2>&1

# Merge - should succeed with zero conflicts
"$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
CONFLICTS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
V1000=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=1000;" 2>/dev/null)
if [ "$CONFLICTS" = "0" ] && [ "$V1000" = "converged_1000" ]; then
  pass_name "s5_convergent_merge_clean"
else
  fail_name "s5_convergent_merge_clean"
  echo "    conflicts=$CONFLICTS v1000=$V1000"
fi

echo ""

# ── Scenario 6: Multi-table merge at scale ──
echo "--- Scenario 6: Multi-table merge (3 tables, 5K rows each) ---"

DB="$TMPROOT/s6.db"
rm -f "$DB"

"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);"
  echo "CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);"
  echo "CREATE TABLE t3(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 1000 base | sed 's/INTO t/INTO t1/'
  gen_inserts 1 1000 base | sed 's/INTO t/INTO t2/'
  gen_inserts 1 1000 base | sed 's/INTO t/INTO t3/'
  echo "SELECT dolt_commit('-Am','base 3 tables');"
)" >/dev/null 2>&1

# feat: add 5K rows to each table
"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_inserts 1001 6000 feat | sed 's/INTO t/INTO t1/'
  gen_inserts 1001 6000 feat | sed 's/INTO t/INTO t2/'
  gen_inserts 1001 6000 feat | sed 's/INTO t/INTO t3/'
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat adds 15K total');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

# main: add 5K different rows to each table
"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  gen_inserts 6001 11000 main | sed 's/INTO t/INTO t1/'
  gen_inserts 6001 11000 main | sed 's/INTO t/INTO t2/'
  gen_inserts 6001 11000 main | sed 's/INTO t/INTO t3/'
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main adds 15K total');"
)" >/dev/null 2>&1

"$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
C1=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t1;" 2>/dev/null)
C2=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t2;" 2>/dev/null)
C3=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t3;" 2>/dev/null)
if [ "$C1" = "11000" ] && [ "$C2" = "11000" ] && [ "$C3" = "11000" ]; then
  pass_name "s6_multi_table_merge_counts"
else
  fail_name "s6_multi_table_merge_counts"
  echo "    t1=$C1 t2=$C2 t3=$C3"
fi

echo ""

# ── Scenario 6b: Repeated merge abort/retry across multiple tables ──
echo "--- Scenario 6b: Repeated merge abort/retry across multiple tables ---"

DB="$TMPROOT/s6b.db"
rm -f "$DB"

"$DOLTLITE" "$DB" "$(
  echo "CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);"
  echo "CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);"
  echo "CREATE TABLE t3(id INTEGER PRIMARY KEY, v TEXT);"
  gen_inserts 1 1000 base | sed 's/INTO t/INTO t1/'
  gen_inserts 1 1000 base | sed 's/INTO t/INTO t2/'
  gen_inserts 1 1000 base | sed 's/INTO t/INTO t3/'
  echo "SELECT dolt_commit('-Am','base');"
)" >/dev/null 2>&1

"$DOLTLITE" "$DB" "$(
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_updates 201 500 feat | sed 's/UPDATE t/UPDATE t1/'
  gen_updates 201 500 feat | sed 's/UPDATE t/UPDATE t2/'
  gen_updates 201 500 feat | sed 's/UPDATE t/UPDATE t3/'
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat conflicts');"
  echo "SELECT dolt_checkout('main');"
)" >/dev/null 2>&1

"$DOLTLITE" "$DB" "$(
  echo "BEGIN;"
  gen_updates 201 500 main | sed 's/UPDATE t/UPDATE t1/'
  gen_updates 201 500 main | sed 's/UPDATE t/UPDATE t2/'
  gen_updates 201 500 main | sed 's/UPDATE t/UPDATE t3/'
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main conflicts');"
)" >/dev/null 2>&1

for attempt in 1 2 3; do
  "$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
  CONFLICTS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
  V1=$("$DOLTLITE" "$DB" "SELECT v FROM t1 WHERE id=250;" 2>/dev/null)
  V2=$("$DOLTLITE" "$DB" "SELECT v FROM t2 WHERE id=250;" 2>/dev/null)
  V3=$("$DOLTLITE" "$DB" "SELECT v FROM t3 WHERE id=250;" 2>/dev/null)
  if [ "$CONFLICTS" = "3" ] && [ "$V1" = "main_250" ] && [ "$V2" = "main_250" ] && [ "$V3" = "main_250" ]; then
    pass_name "s6b_attempt_${attempt}_merge_conflicts"
  else
    fail_name "s6b_attempt_${attempt}_merge_conflicts"
    echo "    conflicts=$CONFLICTS t1=$V1 t2=$V2 t3=$V3"
  fi

  ABORT=$("$DOLTLITE" "$DB" "SELECT dolt_merge('--abort');" 2>/dev/null)
  CONFLICTS_AFTER=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;" 2>/dev/null)
  V1_AFTER=$("$DOLTLITE" "$DB" "SELECT v FROM t1 WHERE id=250;" 2>/dev/null)
  V2_AFTER=$("$DOLTLITE" "$DB" "SELECT v FROM t2 WHERE id=250;" 2>/dev/null)
  V3_AFTER=$("$DOLTLITE" "$DB" "SELECT v FROM t3 WHERE id=250;" 2>/dev/null)
  if [ "$ABORT" = "0" ] && [ "$CONFLICTS_AFTER" = "0" ] \
     && [ "$V1_AFTER" = "main_250" ] && [ "$V2_AFTER" = "main_250" ] && [ "$V3_AFTER" = "main_250" ]; then
    pass_name "s6b_attempt_${attempt}_abort_restores"
  else
    fail_name "s6b_attempt_${attempt}_abort_restores"
    echo "    abort=$ABORT conflicts_after=$CONFLICTS_AFTER t1=$V1_AFTER t2=$V2_AFTER t3=$V3_AFTER"
  fi
done

echo ""

if [ "$QUICK" = "1" ]; then
  echo "(skipping 50K+ row scenarios in --quick mode)"
  echo ""
  echo "======================================="
  echo "Results: $pass passed, $fail failed"
  echo "======================================="
  if [ $fail -gt 0 ]; then
    echo "Failed:$FAILED_NAMES"
    exit 1
  fi
  exit 0
fi

# ── Scenario 7: 50K row non-overlapping merge ──
echo "--- Scenario 7: 50K rows each side, non-overlapping ---"

DB="$TMPROOT/s7.db"
rm -f "$DB"

# Use pipe to avoid shell argument length limits
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  echo "INSERT INTO t VALUES(0,'anchor');"
  echo "SELECT dolt_commit('-Am','anchor');"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1

{
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_inserts 1 50000 feat
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat 50K');"
  echo "SELECT dolt_checkout('main');"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1

{
  echo "BEGIN;"
  gen_inserts 50001 100000 main
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main 50K');"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1

"$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
if [ "$COUNT" = "100001" ]; then
  pass_name "s7_50k_each_side_count"
else
  fail_name "s7_50k_each_side_count"
  echo "    expected 100001, got $COUNT"
fi

V25K=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=25000;" 2>/dev/null)
V75K=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=75000;" 2>/dev/null)
if [ "$V25K" = "feat_25000" ] && [ "$V75K" = "main_75000" ]; then
  pass_name "s7_50k_spot_check_values"
else
  fail_name "s7_50k_spot_check_values"
  echo "    v25k=$V25K v75k=$V75K"
fi

echo ""

# ── Scenario 8: Large merge with secondary index ──
echo "--- Scenario 8: 10K merge with secondary index ---"

DB="$TMPROOT/s8.db"
rm -f "$DB"

# Helper for 3-column inserts: (id, k, v)
gen_inserts_3col() {
  local start=$1 end=$2 prefix=$3
  local i
  for i in $(seq $start $end); do
    echo "INSERT INTO t VALUES($i,$i,'${prefix}_$i');"
  done
}

{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);"
  echo "CREATE INDEX idx_k ON t(k);"
  gen_inserts_3col 1 1000 base
  echo "SELECT dolt_commit('-Am','base with index');"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1

{
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  gen_inserts_3col 1001 11000 feat
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat 10K indexed');"
  echo "SELECT dolt_checkout('main');"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1

{
  echo "BEGIN;"
  gen_inserts_3col 11001 21000 main
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main 10K indexed');"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1

# Note: secondary index merge currently only merges the main table
# rows, not the index entries from the other branch. The index is
# rebuilt from the merged main table. This means the main-table row
# count reflects the merge, but the index merge is a known gap.
"$DOLTLITE" "$DB" "SELECT dolt_merge('feat');" >/dev/null 2>&1
COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
if [ "$COUNT" = "21000" ]; then
  pass_name "s8_indexed_merge_count"
else
  # Pre-existing: index merge doesn't propagate feat-side rows.
  # Track separately from the streaming diff change.
  fail_name "s8_indexed_merge_count (known issue)"
  echo "    count=$COUNT (expected 21000)"
fi

echo ""

echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
