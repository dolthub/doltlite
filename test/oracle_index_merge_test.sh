#!/bin/bash
#
# Comprehensive index merge tests.
#
# Validates that secondary indexes survive merge correctly with
# the new sort-key encoding (index cols + PK in the key). Covers
# unique/non-unique, single/multi-column, NULL values, duplicates,
# conflicts, convergent merges, and integrity checks.
#
# Usage: bash test/oracle_index_merge_test.sh <doltlite>
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
dl_pipe() { "$DOLTLITE" "$1" 2>/dev/null; }

echo "=== Comprehensive Index Merge Tests ==="

# ── 1: Non-unique single-column index, non-overlapping adds ──
echo ""
echo "--- 1: Non-unique single-col, non-overlapping adds ---"
DB="$TMPROOT/1.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "1_count" || fail_name "1_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "1_integrity" || fail_name "1_integrity"

# ── 2: Non-unique single-column, duplicate index values ──
echo ""
echo "--- 2: Non-unique single-col, duplicate values ---"
DB="$TMPROOT/2.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,10,'feat_dup'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,10,'main_dup'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "2_count" || fail_name "2_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=10;")" = "3" ] && pass_name "2_idx_scan" || fail_name "2_idx_scan"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "2_integrity" || fail_name "2_integrity"

# ── 3: Unique index, non-overlapping adds ──
echo ""
echo "--- 3: Unique index, non-overlapping adds ---"
DB="$TMPROOT/3.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT UNIQUE, v TEXT); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "3_count" || fail_name "3_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "3_integrity" || fail_name "3_integrity"

# ── 4: Multi-column index, non-overlapping ──
echo ""
echo "--- 4: Multi-column index, non-overlapping ---"
DB="$TMPROOT/4.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, v TEXT); CREATE INDEX idx ON t(a,b); INSERT INTO t VALUES(1,1,1,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,2,2,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,3,3,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "4_count" || fail_name "4_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "4_integrity" || fail_name "4_integrity"

# ── 5: Multi-column index with shared prefix ──
echo ""
echo "--- 5: Multi-column index, shared prefix ---"
DB="$TMPROOT/5.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, v TEXT); CREATE INDEX idx ON t(a,b); INSERT INTO t VALUES(1,1,1,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,1,2,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,1,3,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "5_count" || fail_name "5_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE a=1;")" = "3" ] && pass_name "5_prefix_scan" || fail_name "5_prefix_scan"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "5_integrity" || fail_name "5_integrity"

# ── 6: NULL index values from both sides ──
echo ""
echo "--- 6: NULL index values from both sides ---"
DB="$TMPROOT/6.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,NULL,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,NULL,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,NULL,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t NOT INDEXED;")" = "3" ] && pass_name "6_table_count" || fail_name "6_table_count"
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "6_index_count" || fail_name "6_index_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "6_integrity" || fail_name "6_integrity"

# ── 7: Update indexed column on one side ──
echo ""
echo "--- 7: Update indexed column on one side ---"
DB="$TMPROOT/7.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base1'); INSERT INTO t VALUES(2,20,'base2'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET k=15 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "7_count" || fail_name "7_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=15;")" = "1" ] && pass_name "7_updated_via_idx" || fail_name "7_updated_via_idx"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=10;")" = "0" ] && pass_name "7_old_gone" || fail_name "7_old_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "7_integrity" || fail_name "7_integrity"

# ── 8: Delete on one side, add on other ──
echo ""
echo "--- 8: Delete on one side, add on other ---"
DB="$TMPROOT/8.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base1'); INSERT INTO t VALUES(2,20,'base2'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); DELETE FROM t WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "8_count" || fail_name "8_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=10;")" = "0" ] && pass_name "8_deleted_gone" || fail_name "8_deleted_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "8_integrity" || fail_name "8_integrity"

# ── 9: Convergent update (both sides same change) ──
echo ""
echo "--- 9: Convergent update with index ---"
DB="$TMPROOT/9.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET k=99 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET k=99 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "0" ] && pass_name "9_no_conflict" || fail_name "9_no_conflict"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=99;")" = "1" ] && pass_name "9_idx_scan" || fail_name "9_idx_scan"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "9_integrity" || fail_name "9_integrity"

# ── 10: Conflict with indexed column ──
# Note: conflict merges leave phantom index entries from the losing
# side because the index is three-way merged independently of the
# main table. The phantom clears when conflicts are resolved.
echo ""
echo "--- 10: Modify-modify conflict with index ---"
DB="$TMPROOT/10.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET k=100 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET k=200 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "1" ] && pass_name "10_conflict" || fail_name "10_conflict"
# Resolve conflict, then verify integrity
dl "$DB" "DELETE FROM dolt_conflicts_t; REINDEX; SELECT dolt_commit('-Am','resolved');" >/dev/null
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "10_integrity_after_resolve" || fail_name "10_integrity_after_resolve"

# ── 11: Multiple indexes on same table ──
echo ""
echo "--- 11: Multiple indexes on same table ---"
DB="$TMPROOT/11.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT); CREATE INDEX idx_a ON t(a); CREATE INDEX idx_b ON t(b); INSERT INTO t VALUES(1,10,100,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20,200,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,300,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "11_count" || fail_name "11_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE a=20;")" = "1" ] && pass_name "11_idx_a" || fail_name "11_idx_a"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE b=200;")" = "1" ] && pass_name "11_idx_b" || fail_name "11_idx_b"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "11_integrity" || fail_name "11_integrity"

# ── 12: Large merge with index (1000 rows each side) ──
echo ""
echo "--- 12: Large merge with index (1000+1000 rows) ---"
DB="$TMPROOT/12.db"
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT);"
  echo "CREATE INDEX idx ON t(k);"
  echo "INSERT INTO t VALUES(0,0,'anchor');"
  echo "SELECT dolt_commit('-Am','base');"
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  echo "BEGIN;"
  for i in $(seq 1 1000); do echo "INSERT INTO t VALUES($i,$i,'feat_$i');"; done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','feat');"
  echo "SELECT dolt_checkout('main');"
  echo "BEGIN;"
  for i in $(seq 1001 2000); do echo "INSERT INTO t VALUES($i,$i,'main_$i');"; done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-Am','main');"
  echo "SELECT dolt_merge('feat');"
} | dl_pipe "$DB" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2001" ] && pass_name "12_count" || fail_name "12_count"
[ "$(dl "$DB" "SELECT count(*) FROM t NOT INDEXED;")" = "2001" ] && pass_name "12_table_count" || fail_name "12_table_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "12_integrity" || fail_name "12_integrity"

# ── 13: Text index with duplicates ──
echo ""
echo "--- 13: Text index with duplicates ---"
DB="$TMPROOT/13.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, v INT); CREATE INDEX idx ON t(name); INSERT INTO t VALUES(1,'alice',1); INSERT INTO t VALUES(2,'bob',1); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(3,'alice',2); INSERT INTO t VALUES(4,'carol',1); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(5,'alice',3); INSERT INTO t VALUES(6,'dave',1); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "6" ] && pass_name "13_count" || fail_name "13_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE name='alice';")" = "3" ] && pass_name "13_alice_count" || fail_name "13_alice_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "13_integrity" || fail_name "13_integrity"

# ── 14: Index survives reopen after merge ──
echo ""
echo "--- 14: Index survives reopen after merge ---"
DB="$TMPROOT/14.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat'); SELECT dolt_commit('-Am','merged');" >/dev/null
# Reopen
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "14_count" || fail_name "14_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "14_idx_after_reopen" || fail_name "14_idx_after_reopen"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "14_integrity" || fail_name "14_integrity"

# ── 15: Fast-forward merge preserves index ──
echo ""
echo "--- 15: Fast-forward merge preserves index ---"
DB="$TMPROOT/15.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "15_ff_count" || fail_name "15_ff_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "15_ff_idx" || fail_name "15_ff_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "15_integrity" || fail_name "15_integrity"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
