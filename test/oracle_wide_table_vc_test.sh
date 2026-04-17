#!/bin/bash
#
# Oracle tests: wide tables (50-200 columns) through VC operations.
#
# Exercises the MAX_RECORD_FIELDS=256 limit and field-level merge
# on tables wider than the old 64-column limit.
#
# Usage: bash test/oracle_wide_table_vc_test.sh <doltlite>
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

# Helpers to generate column defs and values
gen_cols() { local n=$1; for i in $(seq 1 $n); do echo -n "c$i INT"; [ $i -lt $n ] && echo -n ", "; done; }
gen_vals() { local n=$1; for i in $(seq 1 $n); do echo -n "$i"; [ $i -lt $n ] && echo -n ","; done; }

echo "=== Wide Table + Version Control Tests ==="

# ── 1: 100-column commit + reopen ────────────────────────
echo ""
echo "--- 1: 100-column commit + reopen ---"
DB="$TMPROOT/1.db"
COLS=$(gen_cols 100); VALS=$(gen_vals 100)
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS); INSERT INTO t VALUES(1, $VALS); SELECT dolt_commit('-Am','init');" >/dev/null
R1=$(dl "$DB" "SELECT c1 FROM t WHERE id=1;")
R65=$(dl "$DB" "SELECT c65 FROM t WHERE id=1;")
R100=$(dl "$DB" "SELECT c100 FROM t WHERE id=1;")
[ "$R1" = "1" ] && pass_name "1_c1" || fail_name "1_c1; got $R1"
[ "$R65" = "65" ] && pass_name "1_c65" || fail_name "1_c65; got $R65"
[ "$R100" = "100" ] && pass_name "1_c100" || fail_name "1_c100; got $R100"

# ── 2: 100-column non-overlapping merge ──────────────────
echo ""
echo "--- 2: 100-column non-overlapping merge ---"
DB="$TMPROOT/2.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS); INSERT INTO t VALUES(1, $VALS); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c25=999 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET c75=888 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT c25 FROM t WHERE id=1;")" = "999" ] && pass_name "2_feat_col" || fail_name "2_feat_col"
[ "$(dl "$DB" "SELECT c75 FROM t WHERE id=1;")" = "888" ] && pass_name "2_main_col" || fail_name "2_main_col"
[ "$(dl "$DB" "SELECT c50 FROM t WHERE id=1;")" = "50" ] && pass_name "2_unchanged" || fail_name "2_unchanged"
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "0" ] && pass_name "2_no_conflicts" || fail_name "2_no_conflicts"

# ── 3: 100-column conflict merge ─────────────────────────
echo ""
echo "--- 3: 100-column conflict (same column both sides) ---"
DB="$TMPROOT/3.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS); INSERT INTO t VALUES(1, $VALS); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c50=1000 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET c50=2000 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "1" ] && pass_name "3_conflict" || fail_name "3_conflict"
[ "$(dl "$DB" "SELECT c50 FROM t WHERE id=1;")" = "2000" ] && pass_name "3_ours_wins" || fail_name "3_ours_wins"

# ── 4: 200-column merge (near limit) ────────────────────
echo ""
echo "--- 4: 200-column non-overlapping merge ---"
DB="$TMPROOT/4.db"
COLS200=$(gen_cols 200); VALS200=$(gen_vals 200)
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS200); INSERT INTO t VALUES(1, $VALS200); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c1=111, c100=222, c200=333 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET c50=444, c150=555 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT c1 FROM t WHERE id=1;")" = "111" ] && pass_name "4_c1" || fail_name "4_c1"
[ "$(dl "$DB" "SELECT c50 FROM t WHERE id=1;")" = "444" ] && pass_name "4_c50" || fail_name "4_c50"
[ "$(dl "$DB" "SELECT c100 FROM t WHERE id=1;")" = "222" ] && pass_name "4_c100" || fail_name "4_c100"
[ "$(dl "$DB" "SELECT c150 FROM t WHERE id=1;")" = "555" ] && pass_name "4_c150" || fail_name "4_c150"
[ "$(dl "$DB" "SELECT c200 FROM t WHERE id=1;")" = "333" ] && pass_name "4_c200" || fail_name "4_c200"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "4_integrity" || fail_name "4_integrity"

# ── 5: Wide table cherry-pick ────────────────────────────
echo ""
echo "--- 5: 80-column cherry-pick ---"
DB="$TMPROOT/5.db"
COLS80=$(gen_cols 80); VALS80=$(gen_vals 80)
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS80); INSERT INTO t VALUES(1, $VALS80); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c1=111, c40=222, c80=333 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_cherry_pick(dolt_hashof('feat'));" >/dev/null
[ "$(dl "$DB" "SELECT c1 FROM t WHERE id=1;")" = "111" ] && pass_name "5_c1" || fail_name "5_c1"
[ "$(dl "$DB" "SELECT c40 FROM t WHERE id=1;")" = "222" ] && pass_name "5_c40" || fail_name "5_c40"
[ "$(dl "$DB" "SELECT c80 FROM t WHERE id=1;")" = "333" ] && pass_name "5_c80" || fail_name "5_c80"

# ── 6: Wide table revert ────────────────────────────────
echo ""
echo "--- 6: 80-column revert ---"
DB="$TMPROOT/6.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS80); INSERT INTO t VALUES(1, $VALS80); SELECT dolt_commit('-Am','base'); UPDATE t SET c1=999, c80=888 WHERE id=1; INSERT INTO t(id, c1) VALUES(2, 42); SELECT dolt_commit('-Am','changes'); SELECT dolt_revert('HEAD');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "1" ] && pass_name "6_count" || fail_name "6_count"
[ "$(dl "$DB" "SELECT c1 FROM t WHERE id=1;")" = "1" ] && pass_name "6_c1_reverted" || fail_name "6_c1_reverted"
[ "$(dl "$DB" "SELECT c80 FROM t WHERE id=1;")" = "80" ] && pass_name "6_c80_reverted" || fail_name "6_c80_reverted"

# ── 7: Wide table with index through merge ───────────────
echo ""
echo "--- 7: 80-column table with index ---"
DB="$TMPROOT/7.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS80); CREATE INDEX idx ON t(c40); INSERT INTO t VALUES(1, $VALS80); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c40=999 WHERE id=1; INSERT INTO t(id, c1, c40) VALUES(2, 10, 500); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t(id, c1, c40) VALUES(3, 20, 600); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "7_count" || fail_name "7_count"
[ "$(dl "$DB" "SELECT c40 FROM t WHERE id=1;")" = "999" ] && pass_name "7_updated" || fail_name "7_updated"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "7_integrity" || fail_name "7_integrity"

# ── 8: Wide table merge with multiple rows ───────────────
echo ""
echo "--- 8: 100-column, multiple rows, both sides add ---"
DB="$TMPROOT/8.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS); INSERT INTO t VALUES(1, $VALS); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t(id, c1, c50, c100) VALUES(2, 10, 20, 30); INSERT INTO t(id, c1, c50, c100) VALUES(3, 40, 50, 60); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t(id, c1, c50, c100) VALUES(4, 70, 80, 90); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "4" ] && pass_name "8_count" || fail_name "8_count"
[ "$(dl "$DB" "SELECT c50 FROM t WHERE id=2;")" = "20" ] && pass_name "8_feat_row" || fail_name "8_feat_row"
[ "$(dl "$DB" "SELECT c50 FROM t WHERE id=4;")" = "80" ] && pass_name "8_main_row" || fail_name "8_main_row"
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "0" ] && pass_name "8_no_conflicts" || fail_name "8_no_conflicts"

# ── 9: Convergent merge on wide table ────────────────────
echo ""
echo "--- 9: 100-column convergent merge ---"
DB="$TMPROOT/9.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS); INSERT INTO t VALUES(1, $VALS); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c99=777 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET c99=777 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "0" ] && pass_name "9_no_conflict" || fail_name "9_no_conflict"
[ "$(dl "$DB" "SELECT c99 FROM t WHERE id=1;")" = "777" ] && pass_name "9_converged" || fail_name "9_converged"

# ── 10: Reopen after wide merge ──────────────────────────
echo ""
echo "--- 10: Reopen after 100-column merge ---"
DB="$TMPROOT/10.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, $COLS); INSERT INTO t VALUES(1, $VALS); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET c1=111 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET c100=999 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat'); SELECT dolt_commit('-Am','merged');" >/dev/null
[ "$(dl "$DB" "SELECT c1 FROM t WHERE id=1;")" = "111" ] && pass_name "10_c1" || fail_name "10_c1"
[ "$(dl "$DB" "SELECT c100 FROM t WHERE id=1;")" = "999" ] && pass_name "10_c100" || fail_name "10_c100"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "10_integrity" || fail_name "10_integrity"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
