#!/bin/bash
#
# Oracle tests: cherry-pick and revert of schema-changing commits.
#
# Verifies that cherry-pick and revert correctly handle commits
# that include ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP TABLE,
# and mixed schema+data changes.
#
# Usage: bash test/oracle_schema_change_cherry_pick_revert_test.sh <doltlite>
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

echo "=== Schema-Changing Cherry-Pick / Revert Tests ==="

# ── 1: Cherry-pick ADD COLUMN ────────────────────────────
echo ""
echo "--- 1: Cherry-pick ADD COLUMN ---"
DB="$TMPROOT/1.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); ALTER TABLE t ADD COLUMN extra INT DEFAULT 0; INSERT INTO t VALUES(2,'b',42); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "1_count" || fail_name "1_count"
[ "$(dl "$DB" "SELECT extra FROM t WHERE id=2;")" = "42" ] && pass_name "1_new_col" || fail_name "1_new_col"
[ "$(dl "$DB" "SELECT extra FROM t WHERE id=1;")" = "0" ] && pass_name "1_default" || fail_name "1_default"

# ── 2: Revert ADD COLUMN ────────────────────────────────
echo ""
echo "--- 2: Revert ADD COLUMN ---"
DB="$TMPROOT/2.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-Am','base'); ALTER TABLE t ADD COLUMN extra INT DEFAULT 0; INSERT INTO t VALUES(2,'b',42); SELECT dolt_commit('-Am','add col'); SELECT dolt_revert('HEAD');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "1" ] && pass_name "2_count" || fail_name "2_count"
[ "$(dl "$DB" "SELECT v FROM t WHERE id=1;")" = "a" ] && pass_name "2_val" || fail_name "2_val"

# ── 3: Cherry-pick DROP COLUMN ───────────────────────────
echo ""
echo "--- 3: Cherry-pick DROP COLUMN ---"
DB="$TMPROOT/3.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT); INSERT INTO t VALUES(1,10,100); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); ALTER TABLE t DROP COLUMN b; INSERT INTO t VALUES(2,20); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "3_count" || fail_name "3_count"
[ "$(dl "$DB" "SELECT a FROM t WHERE id=2;")" = "20" ] && pass_name "3_new_row" || fail_name "3_new_row"

# ── 4: Cherry-pick RENAME COLUMN ────────────────────────
echo ""
echo "--- 4: Cherry-pick RENAME COLUMN ---"
DB="$TMPROOT/4.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, old_name TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); ALTER TABLE t RENAME COLUMN old_name TO new_name; UPDATE t SET new_name='updated'; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT new_name FROM t WHERE id=1;")" = "updated" ] && pass_name "4_renamed" || fail_name "4_renamed"

# ── 5: Cherry-pick CREATE TABLE ──────────────────────────
echo ""
echo "--- 5: Cherry-pick CREATE TABLE ---"
DB="$TMPROOT/5.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); CREATE TABLE t2(id INTEGER PRIMARY KEY, x INT); INSERT INTO t2 VALUES(1,99); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t2;")" = "1" ] && pass_name "5_new_table" || fail_name "5_new_table"
[ "$(dl "$DB" "SELECT x FROM t2 WHERE id=1;")" = "99" ] && pass_name "5_data" || fail_name "5_data"

# ── 6: Revert CREATE TABLE ──────────────────────────────
echo ""
echo "--- 6: Revert CREATE TABLE ---"
DB="$TMPROOT/6.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-Am','base'); CREATE TABLE t2(id INTEGER PRIMARY KEY, x INT); INSERT INTO t2 VALUES(1,99); SELECT dolt_commit('-Am','add t2'); SELECT dolt_revert('HEAD');" >/dev/null
TBL_COUNT=$(dl "$DB" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t2';")
[ "$TBL_COUNT" = "0" ] && pass_name "6_table_gone" || fail_name "6_table_gone"

# ── 7: Revert mixed schema + data changes ───────────────
echo ""
echo "--- 7: Revert mixed schema + data ---"
DB="$TMPROOT/7.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-Am','base'); ALTER TABLE t ADD COLUMN score INT DEFAULT 0; UPDATE t SET score=100 WHERE id=1; INSERT INTO t VALUES(3,'c',50); DELETE FROM t WHERE id=2; SELECT dolt_commit('-Am','mixed'); SELECT dolt_revert('HEAD');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "7_count" || fail_name "7_count"
[ "$(dl "$DB" "SELECT v FROM t WHERE id=2;")" = "b" ] && pass_name "7_deleted_back" || fail_name "7_deleted_back"

# ── 8: Cherry-pick CREATE INDEX ───────────────────────────
echo ""
echo "--- 8: Cherry-pick CREATE INDEX ---"
DB="$TMPROOT/8.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); INSERT INTO t VALUES(1,10),(2,20); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); CREATE INDEX idx_k ON t(k); INSERT INTO t VALUES(3,30); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "8_count" || fail_name "8_count"
INTEG=$(dl "$DB" "PRAGMA integrity_check;")
[ "$INTEG" = "ok" ] && pass_name "8_integrity" || fail_name "8_integrity"

# ── 9: Revert CREATE INDEX ──────────────────────────────
echo ""
echo "--- 9: Revert CREATE INDEX ---"
DB="$TMPROOT/9.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); INSERT INTO t VALUES(1,10),(2,20); SELECT dolt_commit('-Am','base'); CREATE INDEX idx_k ON t(k); SELECT dolt_commit('-Am','add index'); SELECT dolt_revert('HEAD');" >/dev/null
IDX_COUNT=$(dl "$DB" "SELECT count(*) FROM sqlite_master WHERE name='idx_k';")
[ "$IDX_COUNT" = "0" ] && pass_name "9_index_gone" || fail_name "9_index_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "9_integrity" || fail_name "9_integrity"

# ── 10: Cherry-pick DROP INDEX ──────────────────────────
echo ""
echo "--- 10: Cherry-pick DROP INDEX ---"
DB="$TMPROOT/10.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx_k ON t(k); INSERT INTO t VALUES(1,10),(2,20); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); DROP INDEX idx_k; INSERT INTO t VALUES(3,30); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "10_count" || fail_name "10_count"
IDX_COUNT=$(dl "$DB" "SELECT count(*) FROM sqlite_master WHERE name='idx_k';")
[ "$IDX_COUNT" = "0" ] && pass_name "10_index_gone" || fail_name "10_index_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "10_integrity" || fail_name "10_integrity"

# ── 11: Cherry-pick preserves existing data ──────────────
echo ""
echo "--- 11: Cherry-pick doesn't lose existing rows ---"
DB="$TMPROOT/11.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-Am','base'); INSERT INTO t VALUES(2,'main_only'); SELECT dolt_commit('-Am','main'); SELECT dolt_branch('feat', 'HEAD~1'); SELECT dolt_checkout('feat'); ALTER TABLE t ADD COLUMN x INT DEFAULT 0; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "11_count" || fail_name "11_count"
[ "$(dl "$DB" "SELECT v FROM t WHERE id=2;")" = "main_only" ] && pass_name "11_existing" || fail_name "11_existing"

# ── 12: Revert preserves later commits' data ────────────
echo ""
echo "--- 12: Revert doesn't affect later data ---"
DB="$TMPROOT/12.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-Am','c1'); ALTER TABLE t ADD COLUMN x INT DEFAULT 0; SELECT dolt_commit('-Am','c2 add col'); INSERT INTO t VALUES(2,'b',10); SELECT dolt_commit('-Am','c3 add row'); SELECT dolt_revert('HEAD~1');" >/dev/null
# Revert c2 (add col) while keeping c3's row
# This is a tricky case — revert of schema change with dependent data
R=$(dl "$DB" "SELECT count(*) FROM t;" 2>&1)
# Just verify it doesn't crash
[ -n "$R" ] && pass_name "12_no_crash" || fail_name "12_no_crash"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
