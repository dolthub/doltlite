#!/bin/bash
#
# Large-scale test: 100K rows, multi-commit, branch, merge, diff, clone.
#
# Usage: large_scale_test.sh [doltlite-binary] [--quick]
#

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

N=100000
QUICK=0
if [ "$2" = "--quick" ]; then N=10000; QUICK=1; fi

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

ts() { date +%s; }

echo "=== ${N} row large-scale test ==="

# ── 1. Load ──────────────────────────────────────────────
echo ""
echo "--- 1. Insert ${N} rows and commit ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<ENDSQL
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER, status TEXT);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, 'row_'||x, x%1000, 'active' FROM c;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','load ${N} rows');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t;")
check "row count" "$N" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT min(id), max(id) FROM t;")
check "id range" "1|$N" "$result"

# ── 2. Bulk update ───────────────────────────────────────
echo ""
echo "--- 2. Bulk update 50% of rows ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<'ENDSQL'
UPDATE t SET status='done' WHERE id % 2 = 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','mark even rows done');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t WHERE status='done';")
check "half rows updated" "$((N/2))" "$result"

# ── 3. Diff ──────────────────────────────────────────────
echo ""
echo "--- 3. Diff between commits ---"
t0=$(ts)
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';")
check "diff shows $((N/2)) modified" "$((N/2))" "$result"
echo "  ($(( $(ts) - t0 ))s)"

# ── 4. Delete 10% ────────────────────────────────────────
echo ""
echo "--- 4. Delete 10% of rows ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<'ENDSQL'
DELETE FROM t WHERE id % 10 = 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','delete every 10th row');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t;")
check "rows after delete" "$((N - N/10))" "$result"

# ── 5. Branch + merge ────────────────────────────────────
echo ""
echo "--- 5. Branch, diverge, merge ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<ENDSQL
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(${N}+1, 'feature_row', 999, 'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature: add row');
SELECT dolt_checkout('main');
UPDATE t SET val=0 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main: update row 1');
SELECT dolt_merge('feature');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t;")
check "merge row count" "$((N - N/10 + 1))" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT val FROM t WHERE id=1;")
check "main change kept" "0" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT name FROM t WHERE id=$((N+1));")
check "feature row merged" "feature_row" "$result"

# ── 6. Clone ─────────────────────────────────────────────
echo ""
echo "--- 6. Clone via filesystem remote ---"
t0=$(ts)
"$DB" "$TMPDIR/db" "SELECT dolt_remote('add','origin','file://$TMPDIR/remote'); SELECT dolt_push('origin','main');" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/clone" "SELECT dolt_clone('file://$TMPDIR/remote');")
check "clone ok" "0" "$result"
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/clone" "SELECT count(*) FROM t;")
check "clone row count" "$((N - N/10 + 1))" "$result"

result=$("$DB" "$TMPDIR/clone" "SELECT count(*) FROM dolt_log;")
check "clone has history" "1" "$([ "$result" -ge 5 ] && echo 1 || echo 0)"

# ── 7. Push from clone + pull ────────────────────────────
echo ""
echo "--- 7. Round-trip push/pull ---"
t0=$(ts)
"$DB" "$TMPDIR/clone" <<'ENDSQL'
INSERT INTO t VALUES(999999, 'clone_row', 42, 'pushed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','from clone');
SELECT dolt_push('origin','main');
.quit
ENDSQL

result=$("$DB" "$TMPDIR/db" "SELECT dolt_pull('origin','main'); SELECT name FROM t WHERE id=999999;")
check "pull gets clone data" "0
clone_row" "$result"
echo "  ($(( $(ts) - t0 ))s)"

# ── 8. 20 rapid commits ─────────────────────────────────
echo ""
echo "--- 8. 20 rapid commits ---"
t0=$(ts)
for i in $(seq 1 20); do
  "$DB" "$TMPDIR/db" "UPDATE t SET val=$((i*100)) WHERE id=$((i*2+1)); SELECT dolt_add('-A'); SELECT dolt_commit('-m','rapid $i');" > /dev/null 2>&1
done
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_log;")
check "20+ commits in log" "1" "$([ "$result" -ge 25 ] && echo 1 || echo 0)"

# ── 9. Aggregate queries on final state ──────────────────
echo ""
echo "--- 9. Aggregate queries ---"
t0=$(ts)
result=$("$DB" "$TMPDIR/db" "SELECT avg(val) IS NOT NULL FROM t;")
check "avg query" "1" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT count(DISTINCT status) FROM t;")
check "distinct statuses" "1" "$([ "$result" -ge 2 ] && echo 1 || echo 0)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM t WHERE val BETWEEN 100 AND 200;")
check "range scan" "1" "$([ "$result" -gt 0 ] && echo 1 || echo 0)"
echo "  ($(( $(ts) - t0 ))s)"

# ── 10. File size ────────────────────────────────────────
echo ""
db_size=$(stat -f%z "$TMPDIR/db" 2>/dev/null || stat -c%s "$TMPDIR/db" 2>/dev/null)
db_mb=$((db_size / 1048576))
echo "  Database: ${db_mb}MB ($((db_size/1024))KB)"
check "file exists" "1" "$([ "$db_size" -gt 0 ] && echo 1 || echo 0)"

# ── 11. Million-row test (skip in quick mode) ────────────
if [ "$QUICK" = "0" ]; then
echo ""
echo "--- 11. 1M row insert + commit ---"
t0=$(ts)
"$DB" "$TMPDIR/1m.db" "CREATE TABLE big(id INTEGER PRIMARY KEY, name TEXT, val INTEGER);" > /dev/null 2>&1
"$DB" "$TMPDIR/1m.db" "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1000000) INSERT INTO big SELECT x,'row_'||x,x%1000 FROM c; SELECT dolt_add('-A'); SELECT dolt_commit('-m','1M rows');" > /dev/null 2>&1
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/1m.db" "SELECT count(*) FROM big;")
check "1M rows inserted" "1000000" "$result"

result=$("$DB" "$TMPDIR/1m.db" "SELECT name FROM big WHERE id=500000;")
check "1M midpoint lookup" "row_500000" "$result"

sz=$("$DB" "$TMPDIR/1m.db" ".dbinfo" 2>/dev/null | head -1)
m1_size=$(stat -f%z "$TMPDIR/1m.db" 2>/dev/null || stat -c%s "$TMPDIR/1m.db" 2>/dev/null)
echo "  1M database: $((m1_size / 1048576))MB"

echo ""
echo "--- 12. 10M rows (batched) ---"
t0=$(ts)
"$DB" "$TMPDIR/10m.db" "CREATE TABLE big(id INTEGER PRIMARY KEY, name TEXT, val INTEGER);" > /dev/null 2>&1
for batch in $(seq 0 9); do
  start=$((batch * 1000000 + 1))
  end=$((start + 999999))
  "$DB" "$TMPDIR/10m.db" "WITH RECURSIVE c(x) AS (VALUES($start) UNION ALL SELECT x+1 FROM c WHERE x<$end) INSERT INTO big SELECT x,'row_'||x,x%1000 FROM c;" > /dev/null 2>&1
done
"$DB" "$TMPDIR/10m.db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','10M rows');" > /dev/null 2>&1
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/10m.db" "SELECT count(*) FROM big;")
check "10M rows inserted" "10000000" "$result"

result=$("$DB" "$TMPDIR/10m.db" "SELECT name FROM big WHERE id=5000000;")
check "10M midpoint lookup" "row_5000000" "$result"

m10_size=$(stat -f%z "$TMPDIR/10m.db" 2>/dev/null || stat -c%s "$TMPDIR/10m.db" 2>/dev/null)
echo "  10M database: $((m10_size / 1048576))MB"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
