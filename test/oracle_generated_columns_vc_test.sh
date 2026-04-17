#!/bin/bash
#
# Oracle tests: generated columns through version control operations.
#
# Verifies that STORED and VIRTUAL generated columns survive commit,
# reopen, merge, cherry-pick, revert, checkout, and conflict resolution.
#
# Usage: bash test/oracle_generated_columns_vc_test.sh <doltlite>
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

dl() {
  local db="$1"; shift
  "$DOLTLITE" "$db" "$@" 2>/dev/null
}

echo "=== Generated Columns + Version Control Tests ==="

# ── A: STORED generated column survives commit + reopen ────
echo ""
echo "--- A: STORED generated column commit + reopen ---"

DB="$TMPROOT/a.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*2) STORED);
INSERT INTO t(id,x) VALUES(1,10),(2,20),(3,30);
SELECT dolt_commit('-Am','init');
SQL
)" >/dev/null

R1=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
R2=$(dl "$DB" "SELECT y FROM t WHERE id=2;")
R3=$(dl "$DB" "SELECT y FROM t WHERE id=3;")
[ "$R1" = "20" ] && pass_name "a_stored_reopen_r1" || fail_name "a_stored_reopen_r1; got $R1"
[ "$R2" = "40" ] && pass_name "a_stored_reopen_r2" || fail_name "a_stored_reopen_r2; got $R2"
[ "$R3" = "60" ] && pass_name "a_stored_reopen_r3" || fail_name "a_stored_reopen_r3; got $R3"

# ── B: VIRTUAL generated column survives commit + reopen ──
echo ""
echo "--- B: VIRTUAL generated column commit + reopen ---"

DB="$TMPROOT/b.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x+100) VIRTUAL);
INSERT INTO t(id,x) VALUES(1,5),(2,15);
SELECT dolt_commit('-Am','init');
SQL
)" >/dev/null

R1=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
R2=$(dl "$DB" "SELECT y FROM t WHERE id=2;")
[ "$R1" = "105" ] && pass_name "b_virtual_reopen_r1" || fail_name "b_virtual_reopen_r1; got $R1"
[ "$R2" = "115" ] && pass_name "b_virtual_reopen_r2" || fail_name "b_virtual_reopen_r2; got $R2"

# ── C: Non-overlapping merge with STORED generated column ──
echo ""
echo "--- C: Non-overlapping merge with STORED generated column ---"

DB="$TMPROOT/c.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*2) STORED);
INSERT INTO t(id,x) VALUES(1,10),(2,20);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET x=15 WHERE id=1;
SELECT dolt_commit('-Am','feat: x=15');
SELECT dolt_checkout('main');
UPDATE t SET x=25 WHERE id=2;
SELECT dolt_commit('-Am','main: x=25');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

R1=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
R2=$(dl "$DB" "SELECT y FROM t WHERE id=2;")
[ "$R1" = "30" ] && pass_name "c_merge_stored_r1" || fail_name "c_merge_stored_r1; got $R1"
[ "$R2" = "50" ] && pass_name "c_merge_stored_r2" || fail_name "c_merge_stored_r2; got $R2"

# ── D: Non-overlapping merge with VIRTUAL generated column ──
echo ""
echo "--- D: Non-overlapping merge with VIRTUAL generated column ---"

DB="$TMPROOT/d.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*3) VIRTUAL);
INSERT INTO t(id,x) VALUES(1,10),(2,20);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t(id,x) VALUES(3,30);
SELECT dolt_commit('-Am','feat: add row');
SELECT dolt_checkout('main');
UPDATE t SET x=25 WHERE id=2;
SELECT dolt_commit('-Am','main: update');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

R2=$(dl "$DB" "SELECT y FROM t WHERE id=2;")
R3=$(dl "$DB" "SELECT y FROM t WHERE id=3;")
CNT=$(dl "$DB" "SELECT count(*) FROM t;")
[ "$R2" = "75" ] && pass_name "d_merge_virtual_r2" || fail_name "d_merge_virtual_r2; got $R2"
[ "$R3" = "90" ] && pass_name "d_merge_virtual_r3" || fail_name "d_merge_virtual_r3; got $R3"
[ "$CNT" = "3" ] && pass_name "d_merge_virtual_count" || fail_name "d_merge_virtual_count; got $CNT"

# ── E: Conflict on base column of generated col ────────────
echo ""
echo "--- E: Conflict on base column of generated col ---"

DB="$TMPROOT/e.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*2) STORED);
INSERT INTO t(id,x) VALUES(1,10);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET x=100 WHERE id=1;
SELECT dolt_commit('-Am','feat: x=100');
SELECT dolt_checkout('main');
UPDATE t SET x=200 WHERE id=1;
SELECT dolt_commit('-Am','main: x=200');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CONFLICTS=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
XVAL=$(dl "$DB" "SELECT x FROM t WHERE id=1;")
YVAL=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
[ "$CONFLICTS" = "1" ] && pass_name "e_conflict_detected" || fail_name "e_conflict_detected; got $CONFLICTS"
# Ours wins in working set; y should match
[ "$YVAL" = "$((XVAL*2))" ] && pass_name "e_generated_consistent" || fail_name "e_generated_consistent; x=$XVAL y=$YVAL"

# ── F: Cherry-pick with generated column ───────────────────
echo ""
echo "--- F: Cherry-pick with generated column ---"

DB="$TMPROOT/f.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*2) STORED);
INSERT INTO t(id,x) VALUES(1,10);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t(id,x) VALUES(2,20);
UPDATE t SET x=15 WHERE id=1;
SELECT dolt_commit('-Am','feat changes');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick(dolt_hashof('feat'));
SQL
)" >/dev/null

R1=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
R2=$(dl "$DB" "SELECT y FROM t WHERE id=2;")
[ "$R1" = "30" ] && pass_name "f_cherry_pick_r1" || fail_name "f_cherry_pick_r1; got $R1"
[ "$R2" = "40" ] && pass_name "f_cherry_pick_r2" || fail_name "f_cherry_pick_r2; got $R2"

# ── G: Revert with generated column ───────────────────────
echo ""
echo "--- G: Revert with generated column ---"

DB="$TMPROOT/g.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*2) STORED);
INSERT INTO t(id,x) VALUES(1,10);
SELECT dolt_commit('-Am','base');
UPDATE t SET x=99 WHERE id=1;
INSERT INTO t(id,x) VALUES(2,50);
SELECT dolt_commit('-Am','changes');
SELECT dolt_revert('HEAD');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
R1=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
[ "$CNT" = "1" ] && pass_name "g_revert_count" || fail_name "g_revert_count; got $CNT"
[ "$R1" = "20" ] && pass_name "g_revert_r1_restored" || fail_name "g_revert_r1_restored; got $R1"

# ── H: Generated column expression preserved across checkout ──
echo ""
echo "--- H: Expression preserved across checkout ---"

DB="$TMPROOT/h.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*3) STORED);
INSERT INTO t(id,x) VALUES(1,10);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
INSERT INTO t(id,x) VALUES(2,20);
SELECT dolt_commit('-Am','other');
SELECT dolt_checkout('main');
INSERT INTO t(id,x) VALUES(3,30);
SELECT dolt_commit('-Am','main');
SQL
)" >/dev/null

# Check main branch
R3=$(dl "$DB" "SELECT y FROM t WHERE id=3;")
[ "$R3" = "90" ] && pass_name "h_main_expression" || fail_name "h_main_expression; got $R3"

# Switch to other and check
dl "$DB" "SELECT dolt_checkout('other');" >/dev/null
R2=$(dl "$DB" "SELECT y FROM t WHERE id=2;")
[ "$R2" = "60" ] && pass_name "h_other_expression" || fail_name "h_other_expression; got $R2"

# ── I: Convergent merge (both sides same update) ──────────
echo ""
echo "--- I: Convergent merge with generated column ---"

DB="$TMPROOT/i.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x*2) STORED);
INSERT INTO t(id,x) VALUES(1,10);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET x=50 WHERE id=1;
SELECT dolt_commit('-Am','feat: x=50');
SELECT dolt_checkout('main');
UPDATE t SET x=50 WHERE id=1;
SELECT dolt_commit('-Am','main: x=50');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CONFLICTS=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
YVAL=$(dl "$DB" "SELECT y FROM t WHERE id=1;")
[ "$CONFLICTS" = "0" ] && pass_name "i_convergent_no_conflict" || fail_name "i_convergent_no_conflict; got $CONFLICTS"
[ "$YVAL" = "100" ] && pass_name "i_convergent_value" || fail_name "i_convergent_value; got $YVAL"

# ── J: Multiple generated columns ─────────────────────────
echo ""
echo "--- J: Multiple generated columns through merge ---"

DB="$TMPROOT/j.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  a INT,
  b INT,
  sum_ab INT GENERATED ALWAYS AS (a+b) STORED,
  prod_ab INT GENERATED ALWAYS AS (a*b) STORED
);
INSERT INTO t(id,a,b) VALUES(1,3,4),(2,5,6);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET a=10 WHERE id=1;
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=20 WHERE id=2;
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

SUM1=$(dl "$DB" "SELECT sum_ab FROM t WHERE id=1;")
PROD1=$(dl "$DB" "SELECT prod_ab FROM t WHERE id=1;")
SUM2=$(dl "$DB" "SELECT sum_ab FROM t WHERE id=2;")
PROD2=$(dl "$DB" "SELECT prod_ab FROM t WHERE id=2;")
[ "$SUM1" = "14" ] && pass_name "j_multi_sum1" || fail_name "j_multi_sum1; got $SUM1"
[ "$PROD1" = "40" ] && pass_name "j_multi_prod1" || fail_name "j_multi_prod1; got $PROD1"
[ "$SUM2" = "25" ] && pass_name "j_multi_sum2" || fail_name "j_multi_sum2; got $SUM2"
[ "$PROD2" = "100" ] && pass_name "j_multi_prod2" || fail_name "j_multi_prod2; got $PROD2"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
