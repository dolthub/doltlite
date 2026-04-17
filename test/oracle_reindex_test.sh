#!/bin/bash
#
# REINDEX tests for doltlite.
#
# Verifies that REINDEX correctly rebuilds secondary indexes
# across various contexts: after commit, with dirty working set,
# with multiple indexes, NULLs, WITHOUT ROWID tables, compound
# PKs, after merge, after checkout, and after reopen.
#
# Usage: bash test/oracle_reindex_test.sh <doltlite>
#

set -u
DOLTLITE="${1:?usage: $0 <doltlite>}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

dl() { "$DOLTLITE" "$1" "$2" 2>/dev/null; }

echo "=== REINDEX Tests ==="

# ── 1: Basic REINDEX after commit ─────────────────────────
echo ""
echo "--- 1: Basic REINDEX after commit ---"
DB="$TMPROOT/1.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10),(2,20),(3,30); SELECT dolt_commit('-Am','init'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "1_count" || fail_name "1_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "1_idx" || fail_name "1_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "1_integrity" || fail_name "1_integrity"

# ── 2: REINDEX with dirty working set ────────────────────
echo ""
echo "--- 2: REINDEX with dirty working set ---"
DB="$TMPROOT/2.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','init'); INSERT INTO t VALUES(2,20); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "2_count" || fail_name "2_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "2_idx" || fail_name "2_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "2_integrity" || fail_name "2_integrity"

# ── 3: REINDEX with multiple indexes ─────────────────────
echo ""
echo "--- 3: Multiple indexes ---"
DB="$TMPROOT/3.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT); CREATE INDEX idx_a ON t(a); CREATE INDEX idx_b ON t(b); CREATE INDEX idx_ab ON t(a,b); INSERT INTO t VALUES(1,10,'x'),(2,20,'y'),(3,10,'x'); SELECT dolt_commit('-Am','init'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "3_count" || fail_name "3_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE a=10;")" = "2" ] && pass_name "3_idx_a" || fail_name "3_idx_a"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE b='x';")" = "2" ] && pass_name "3_idx_b" || fail_name "3_idx_b"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE a=10 AND b='x';")" = "2" ] && pass_name "3_idx_ab" || fail_name "3_idx_ab"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "3_integrity" || fail_name "3_integrity"

# ── 4: REINDEX with NULLs ────────────────────────────────
echo ""
echo "--- 4: NULLs in indexed columns ---"
DB="$TMPROOT/4.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,NULL),(2,NULL),(3,10); SELECT dolt_commit('-Am','init'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "4_count" || fail_name "4_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k IS NULL;")" = "2" ] && pass_name "4_null" || fail_name "4_null"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "4_integrity" || fail_name "4_integrity"

# ── 5: WITHOUT ROWID table (TEXT PK) ─────────────────────
echo ""
echo "--- 5: WITHOUT ROWID table ---"
DB="$TMPROOT/5.db"
dl "$DB" "CREATE TABLE t(a TEXT PRIMARY KEY, b INT); CREATE INDEX idx ON t(b); INSERT INTO t VALUES('x',10),('y',20),('z',10); SELECT dolt_commit('-Am','init'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "5_count" || fail_name "5_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE b=10;")" = "2" ] && pass_name "5_idx" || fail_name "5_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "5_integrity" || fail_name "5_integrity"

# ── 6: Compound PK ───────────────────────────────────────
echo ""
echo "--- 6: Compound PK ---"
DB="$TMPROOT/6.db"
dl "$DB" "CREATE TABLE t(a INT, b INT, c TEXT, PRIMARY KEY(a,b)); CREATE INDEX idx ON t(c); INSERT INTO t VALUES(1,1,'alpha'),(1,2,'beta'),(2,1,'alpha'); SELECT dolt_commit('-Am','init'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "6_count" || fail_name "6_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE c='alpha';")" = "2" ] && pass_name "6_idx" || fail_name "6_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "6_integrity" || fail_name "6_integrity"

# ── 7: REINDEX after merge ───────────────────────────────
echo ""
echo "--- 7: REINDEX after merge ---"
DB="$TMPROOT/7.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
dl "$DB" "REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "7_count" || fail_name "7_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "7_idx" || fail_name "7_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "7_integrity" || fail_name "7_integrity"

# ── 8: REINDEX after checkout ────────────────────────────
echo ""
echo "--- 8: REINDEX after checkout ---"
DB="$TMPROOT/8.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10),(2,20); SELECT dolt_commit('-Am','init'); SELECT dolt_branch('other'); SELECT dolt_checkout('other'); INSERT INTO t VALUES(3,30); SELECT dolt_commit('-Am','other'); SELECT dolt_checkout('main'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "8_count" || fail_name "8_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "8_integrity" || fail_name "8_integrity"

# ── 9: REINDEX after reopen ──────────────────────────────
echo ""
echo "--- 9: REINDEX after reopen ---"
DB="$TMPROOT/9.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10),(2,20),(3,30); SELECT dolt_commit('-Am','init');" >/dev/null
dl "$DB" "REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "9_count" || fail_name "9_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "9_idx" || fail_name "9_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "9_integrity" || fail_name "9_integrity"

# ── 10: REINDEX after DELETE ─────────────────────────────
echo ""
echo "--- 10: REINDEX after DELETE ---"
DB="$TMPROOT/10.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50); SELECT dolt_commit('-Am','init'); DELETE FROM t WHERE id IN (2,4); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "10_count" || fail_name "10_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k>0;")" = "3" ] && pass_name "10_idx" || fail_name "10_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "10_integrity" || fail_name "10_integrity"

# ── 11: REINDEX after UPDATE on indexed column ───────────
echo ""
echo "--- 11: REINDEX after UPDATE ---"
DB="$TMPROOT/11.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10),(2,20),(3,30); SELECT dolt_commit('-Am','init'); UPDATE t SET k=99 WHERE id=2; REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=99;")" = "1" ] && pass_name "11_updated" || fail_name "11_updated"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "0" ] && pass_name "11_old_gone" || fail_name "11_old_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "11_integrity" || fail_name "11_integrity"

# ── 12: REINDEX with UNIQUE index ────────────────────────
echo ""
echo "--- 12: UNIQUE index ---"
DB="$TMPROOT/12.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT UNIQUE); INSERT INTO t VALUES(1,10),(2,20),(3,30); SELECT dolt_commit('-Am','init'); REINDEX;" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "12_count" || fail_name "12_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "12_integrity" || fail_name "12_integrity"

# ── 13: REINDEX preserves data through commit cycle ──────
echo ""
echo "--- 13: REINDEX + commit + reopen ---"
DB="$TMPROOT/13.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10),(2,20); SELECT dolt_commit('-Am','init'); INSERT INTO t VALUES(3,30); REINDEX; SELECT dolt_commit('-Am','after_reindex');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "13_count" || fail_name "13_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=30;")" = "1" ] && pass_name "13_idx" || fail_name "13_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "13_integrity" || fail_name "13_integrity"

# ── 14: Large table REINDEX ──────────────────────────────
echo ""
echo "--- 14: Large table (1000 rows) ---"
DB="$TMPROOT/14.db"
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT);"
  echo "CREATE INDEX idx ON t(k);"
  echo "BEGIN;"
  for i in $(seq 1 1000); do echo "INSERT INTO t VALUES($i,$i,'row_$i');"; done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','init');"
  echo "REINDEX;"
} | "$DOLTLITE" "$DB" >/dev/null 2>&1
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "1000" ] && pass_name "14_count" || fail_name "14_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=500;")" = "1" ] && pass_name "14_idx" || fail_name "14_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "14_integrity" || fail_name "14_integrity"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
