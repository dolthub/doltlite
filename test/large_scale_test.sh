#!/bin/bash
#
# Large-scale tests: 1M and 10M rows with performance thresholds.
#
# Usage:
#   large_scale_test.sh [doltlite-binary] [--quick]
#
# Quick mode (CI): 100K rows, relaxed thresholds
# Full mode:       1M + 10M rows, strict thresholds
#

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

QUICK=0
if [ "$2" = "--quick" ]; then QUICK=1; fi

pass=0
fail=0
DB="$DOLTLITE"

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$expected|"
    echo "    actual:   |$actual|"
    fail=$((fail+1))
  fi
}

check_time() {
  local desc="$1" elapsed="$2" max="$3"
  if [ "$elapsed" -le "$max" ]; then
    echo "  PASS: $desc (${elapsed}s <= ${max}s)"; pass=$((pass+1))
  else
    echo "  FAIL: $desc (${elapsed}s > ${max}s TIMEOUT)"
    fail=$((fail+1))
  fi
}

ts() { date +%s; }

# Insert N rows in batches of 100K to avoid CTE ephemeral table overhead.
batch_insert() {
  local dbpath="$1" table="$2" total="$3" cols="$4"
  local batch=100000 i=1
  while [ "$i" -le "$total" ]; do
    local end=$((i + batch - 1))
    if [ "$end" -gt "$total" ]; then end=$total; fi
    "$DB" "$dbpath" "WITH RECURSIVE c(x) AS (VALUES($i) UNION ALL SELECT x+1 FROM c WHERE x<$end) INSERT INTO $table SELECT $cols FROM c;" > /dev/null 2>&1
    i=$((end + 1))
  done
}

if [ "$QUICK" = "1" ]; then
  echo "=== QUICK MODE (CI) ==="
  N1=100000
  N1_INSERT_MAX=15
  N1_UPDATE_MAX=15
  N1_DIFF_MAX=15
  N1_CLONE_MAX=15
  N1_COMMIT_MAX=15
  N10=0
else
  echo "=== FULL MODE ==="
  N1=1000000
  N1_INSERT_MAX=30
  N1_UPDATE_MAX=30
  N1_DIFF_MAX=30
  N1_CLONE_MAX=30
  N1_COMMIT_MAX=30
  N10=10000000
  N10_INSERT_MAX=120
  N10_COMMIT_MAX=60
  N10_UPDATE_MAX=120
  N10_DIFF_MAX=120
  N10_CLONE_MAX=60
fi

# ════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════"
echo "  ${N1}-row single table test"
echo "══════════════════════════════════════"

# ── 1. Insert ────────────────────────────────────────────
echo ""
echo "--- 1. Insert ${N1} rows ---"
"$DB" "$TMPDIR/db" "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER, status TEXT);" > /dev/null 2>&1
t0=$(ts)
batch_insert "$TMPDIR/db" "t" "$N1" "x, 'row_'||x, x%1000, 'active'"
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "insert ${N1} rows" "$elapsed" "$N1_INSERT_MAX"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t;")
check "row count" "$N1" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT min(id), max(id) FROM t;")
check "id range" "1|$N1" "$result"

# ── 2. Commit ────────────────────────────────────────────
echo ""
echo "--- 2. dolt_add + dolt_commit ---"
t0=$(ts)
"$DB" "$TMPDIR/db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','${N1} rows');" > /dev/null 2>&1
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "commit ${N1} rows" "$elapsed" "$N1_COMMIT_MAX"

# ── 3. Bulk update 50% ──────────────────────────────────
echo ""
echo "--- 3. Update 50% of rows ---"
t0=$(ts)
"$DB" "$TMPDIR/db" "UPDATE t SET status='done' WHERE id%2=0; SELECT dolt_add('-A'); SELECT dolt_commit('-m','update half');" > /dev/null 2>&1
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "update 50%" "$elapsed" "$N1_UPDATE_MAX"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t WHERE status='done';")
check "half updated" "$((N1/2))" "$result"

# ── 4. Diff ──────────────────────────────────────────────
echo ""
echo "--- 4. Diff ---"
t0=$(ts)
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';")
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "diff" "$elapsed" "$N1_DIFF_MAX"
check "diff count" "$((N1/2))" "$result"

# ── 5. Delete 10% ───────────────────────────────────────
echo ""
echo "--- 5. Delete 10% ---"
"$DB" "$TMPDIR/db" "DELETE FROM t WHERE id%10=0; SELECT dolt_add('-A'); SELECT dolt_commit('-m','delete 10%');" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t;")
check "rows after delete" "$((N1 - N1/10))" "$result"

# ── 6. Branch + merge ───────────────────────────────────
echo ""
echo "--- 6. Branch + merge ---"
"$DB" "$TMPDIR/db" <<ENDSQL
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES($((N1+1)), 'feature_row', 999, 'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature');
SELECT dolt_checkout('main');
UPDATE t SET val=0 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feature');
.quit
ENDSQL
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t;")
check "merge row count" "$((N1 - N1/10 + 1))" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT val FROM t WHERE id=1;")
check "main change kept" "0" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT name FROM t WHERE id=$((N1+1));")
check "feature row present" "feature_row" "$result"

# ── 7. Clone ─────────────────────────────────────────────
echo ""
echo "--- 7. Clone ---"
t0=$(ts)
"$DB" "$TMPDIR/db" "SELECT dolt_remote('add','origin','file://$TMPDIR/remote'); SELECT dolt_push('origin','main');" > /dev/null 2>&1
"$DB" "$TMPDIR/clone" "SELECT dolt_clone('file://$TMPDIR/remote');" > /dev/null 2>&1
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "push + clone" "$elapsed" "$N1_CLONE_MAX"
result=$("$DB" "$TMPDIR/clone" "SELECT count(*) FROM t;")
check "clone row count" "$((N1 - N1/10 + 1))" "$result"

# ── 8. Push from clone + pull ────────────────────────────
echo ""
echo "--- 8. Round-trip ---"
"$DB" "$TMPDIR/clone" "INSERT INTO t VALUES(99999999,'clone_row',42,'pushed'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','from clone'); SELECT dolt_push('origin','main');" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/db" "SELECT dolt_pull('origin','main'); SELECT name FROM t WHERE id=99999999;")
check "pull from clone" "0
clone_row" "$result"

# ── 9. Rapid commits ─────────────────────────────────────
echo ""
echo "--- 9. 20 rapid commits ---"
for i in $(seq 1 20); do
  "$DB" "$TMPDIR/db" "UPDATE t SET val=$((i*100)) WHERE id=$((i*2+1)); SELECT dolt_add('-A'); SELECT dolt_commit('-m','rapid $i');" > /dev/null 2>&1
done
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_log;")
check "commits in log" "1" "$([ "$result" -ge 25 ] && echo 1 || echo 0)"

# ── 10. Queries ──────────────────────────────────────────
echo ""
echo "--- 10. Queries ---"
result=$("$DB" "$TMPDIR/db" "SELECT avg(val) IS NOT NULL FROM t;")
check "avg" "1" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(DISTINCT status) >= 2 FROM t;")
check "distinct" "1" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(*) > 0 FROM t WHERE val BETWEEN 100 AND 200;")
check "range scan" "1" "$result"

# ── 11. File size ────────────────────────────────────────
echo ""
db_size=$(stat -f%z "$TMPDIR/db" 2>/dev/null || stat -c%s "$TMPDIR/db" 2>/dev/null)
echo "  Database: $((db_size / 1048576))MB"

# ════════════════════════════════════════════════════════════
if [ "$N10" -gt 0 ]; then
echo ""
echo "══════════════════════════════════════"
echo "  10M-row test"
echo "══════════════════════════════════════"

echo ""
echo "--- 12. Insert 10M rows ---"
"$DB" "$TMPDIR/10m.db" "CREATE TABLE big(id INTEGER PRIMARY KEY, name TEXT, val INTEGER);" > /dev/null 2>&1
t0=$(ts)
batch_insert "$TMPDIR/10m.db" "big" "$N10" "x, 'row_'||x, x%1000"
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "insert 10M" "$elapsed" "$N10_INSERT_MAX"

result=$("$DB" "$TMPDIR/10m.db" "SELECT count(*) FROM big;")
check "10M count" "$N10" "$result"

echo ""
echo "--- 13. Commit 10M ---"
t0=$(ts)
"$DB" "$TMPDIR/10m.db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','10M');" > /dev/null 2>&1
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "commit 10M" "$elapsed" "$N10_COMMIT_MAX"

echo ""
echo "--- 14. Update 50% of 10M ---"
t0=$(ts)
"$DB" "$TMPDIR/10m.db" "UPDATE big SET val=val+1 WHERE id%2=0; SELECT dolt_add('-A'); SELECT dolt_commit('-m','update 5M rows');" > /dev/null 2>&1
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "update 5M rows" "$elapsed" "$N10_UPDATE_MAX"

result=$("$DB" "$TMPDIR/10m.db" "SELECT count(*) FROM big;")
check "10M intact after update" "$N10" "$result"

echo ""
echo "--- 15. Diff 10M ---"
t0=$(ts)
result=$("$DB" "$TMPDIR/10m.db" "SELECT count(*) FROM dolt_diff_big WHERE diff_type='modified';")
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "diff 5M changes" "$elapsed" "$N10_DIFF_MAX"
check "10M diff count" "$((N10/2))" "$result"

echo ""
echo "--- 16. Clone 10M ---"
t0=$(ts)
"$DB" "$TMPDIR/10m.db" "SELECT dolt_remote('add','origin','file://$TMPDIR/10m_remote'); SELECT dolt_push('origin','main');" > /dev/null 2>&1
"$DB" "$TMPDIR/10m_clone" "SELECT dolt_clone('file://$TMPDIR/10m_remote');" > /dev/null 2>&1
elapsed=$(( $(ts) - t0 ))
echo "  ${elapsed}s"
check_time "clone 10M" "$elapsed" "$N10_CLONE_MAX"

result=$("$DB" "$TMPDIR/10m_clone" "SELECT count(*) FROM big;")
check "10M clone count" "$N10" "$result"

echo ""
echo "--- 17. Point lookups ---"
result=$("$DB" "$TMPDIR/10m.db" "SELECT name FROM big WHERE id=5000000;")
check "10M midpoint" "row_5000000" "$result"
result=$("$DB" "$TMPDIR/10m.db" "SELECT name FROM big WHERE id=9000000;")
check "10M near-end" "row_9000000" "$result"

m10_size=$(stat -f%z "$TMPDIR/10m.db" 2>/dev/null || stat -c%s "$TMPDIR/10m.db" 2>/dev/null)
echo "  10M database: $((m10_size / 1048576))MB"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
