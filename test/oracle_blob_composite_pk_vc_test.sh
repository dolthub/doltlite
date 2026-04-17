#!/bin/bash
#
# Oracle tests: BLOB in composite PRIMARY KEY through version control.
#
# Verifies that tables with BLOB columns in composite PKs survive
# commit, reopen, merge (clean, conflict, convergent, delete-modify),
# cherry-pick, revert, and byte-order edge cases.
#
# Usage: bash test/oracle_blob_composite_pk_vc_test.sh <doltlite>
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

echo "=== BLOB Composite PK + Version Control Tests ==="

# ── A: Commit + reopen ────────────────────────────────────
echo ""
echo "--- A: BLOB composite PK commit + reopen ---"

DB="$TMPROOT/a.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'DEADBEEF', 1, 'r1');
INSERT INTO t VALUES(X'CAFEBABE', 2, 'r2');
INSERT INTO t VALUES(X'DEADBEEF', 3, 'r3');
SELECT dolt_commit('-Am','init');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
R1=$(dl "$DB" "SELECT v FROM t WHERE a=X'DEADBEEF' AND b=1;")
R2=$(dl "$DB" "SELECT v FROM t WHERE a=X'CAFEBABE' AND b=2;")
R3=$(dl "$DB" "SELECT v FROM t WHERE a=X'DEADBEEF' AND b=3;")
[ "$CNT" = "3" ] && pass_name "a_count" || fail_name "a_count; got $CNT"
[ "$R1" = "r1" ] && pass_name "a_r1" || fail_name "a_r1; got $R1"
[ "$R2" = "r2" ] && pass_name "a_r2" || fail_name "a_r2; got $R2"
[ "$R3" = "r3" ] && pass_name "a_r3" || fail_name "a_r3; got $R3"

# ── B: Non-overlapping merge ──────────────────────────────
echo ""
echo "--- B: Non-overlapping merge ---"

DB="$TMPROOT/b.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'AA', 1, 'base1');
INSERT INTO t VALUES(X'BB', 1, 'base2');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(X'AA', 2, 'feat_aa2');
INSERT INTO t VALUES(X'CC', 1, 'feat_cc1');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(X'BB', 2, 'main_bb2');
INSERT INTO t VALUES(X'DD', 1, 'main_dd1');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
FAA2=$(dl "$DB" "SELECT v FROM t WHERE a=X'AA' AND b=2;")
MBB2=$(dl "$DB" "SELECT v FROM t WHERE a=X'BB' AND b=2;")
FCC1=$(dl "$DB" "SELECT v FROM t WHERE a=X'CC' AND b=1;")
MDD1=$(dl "$DB" "SELECT v FROM t WHERE a=X'DD' AND b=1;")
CONF=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
[ "$CNT" = "6" ] && pass_name "b_count" || fail_name "b_count; got $CNT"
[ "$FAA2" = "feat_aa2" ] && pass_name "b_feat_row" || fail_name "b_feat_row; got $FAA2"
[ "$MBB2" = "main_bb2" ] && pass_name "b_main_row" || fail_name "b_main_row; got $MBB2"
[ "$FCC1" = "feat_cc1" ] && pass_name "b_feat_new" || fail_name "b_feat_new; got $FCC1"
[ "$MDD1" = "main_dd1" ] && pass_name "b_main_new" || fail_name "b_main_new; got $MDD1"
[ "$CONF" = "0" ] && pass_name "b_no_conflicts" || fail_name "b_no_conflicts; got $CONF"

# ── C: Modify-modify conflict ─────────────────────────────
echo ""
echo "--- C: Modify-modify conflict ---"

DB="$TMPROOT/c.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'DEADBEEF', 1, 'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE a=X'DEADBEEF';
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE a=X'DEADBEEF';
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CONF=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
VAL=$(dl "$DB" "SELECT v FROM t WHERE a=X'DEADBEEF';")
[ "$CONF" = "1" ] && pass_name "c_conflict" || fail_name "c_conflict; got $CONF"
[ "$VAL" = "main_val" ] && pass_name "c_ours_wins" || fail_name "c_ours_wins; got $VAL"

# ── D: Delete vs modify conflict ──────────────────────────
echo ""
echo "--- D: Delete vs modify conflict ---"

DB="$TMPROOT/d.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'AABB', 1, 'target');
INSERT INTO t VALUES(X'CCDD', 1, 'bystander');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DELETE FROM t WHERE a=X'AABB';
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='modified' WHERE a=X'AABB';
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CONF=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
KEEP=$(dl "$DB" "SELECT v FROM t WHERE a=X'CCDD';")
[ "$CONF" = "1" ] && pass_name "d_dm_conflict" || fail_name "d_dm_conflict; got $CONF"
[ "$KEEP" = "bystander" ] && pass_name "d_bystander_ok" || fail_name "d_bystander_ok; got $KEEP"

# ── E: Convergent merge ───────────────────────────────────
echo ""
echo "--- E: Convergent merge ---"

DB="$TMPROOT/e.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'FF00', 1, 'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='same' WHERE a=X'FF00';
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='same' WHERE a=X'FF00';
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CONF=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
VAL=$(dl "$DB" "SELECT v FROM t WHERE a=X'FF00';")
[ "$CONF" = "0" ] && pass_name "e_convergent_clean" || fail_name "e_convergent_clean; got $CONF"
[ "$VAL" = "same" ] && pass_name "e_convergent_val" || fail_name "e_convergent_val; got $VAL"

# ── F: Cherry-pick ────────────────────────────────────────
echo ""
echo "--- F: Cherry-pick ---"

DB="$TMPROOT/f.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'AA', 1, 'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(X'BB', 2, 'feat');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick(dolt_hashof('feat'));
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
VAL=$(dl "$DB" "SELECT v FROM t WHERE a=X'BB' AND b=2;")
[ "$CNT" = "2" ] && pass_name "f_cp_count" || fail_name "f_cp_count; got $CNT"
[ "$VAL" = "feat" ] && pass_name "f_cp_val" || fail_name "f_cp_val; got $VAL"

# ── G: Revert ─────────────────────────────────────────────
echo ""
echo "--- G: Revert ---"

DB="$TMPROOT/g.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'AA', 1, 'original');
SELECT dolt_commit('-Am','base');
UPDATE t SET v='changed' WHERE a=X'AA';
INSERT INTO t VALUES(X'BB', 1, 'added');
SELECT dolt_commit('-Am','changes');
SELECT dolt_revert('HEAD');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
VAL=$(dl "$DB" "SELECT v FROM t WHERE a=X'AA';")
[ "$CNT" = "1" ] && pass_name "g_revert_count" || fail_name "g_revert_count; got $CNT"
[ "$VAL" = "original" ] && pass_name "g_revert_val" || fail_name "g_revert_val; got $VAL"

# ── H: Byte-order edge cases ──────────────────────────────
echo ""
echo "--- H: Byte-order edge cases through merge ---"

DB="$TMPROOT/h.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a BLOB, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(X'00', 1, 'zero');
INSERT INTO t VALUES(X'FF', 1, 'ff');
INSERT INTO t VALUES(X'0000', 1, 'dbl_zero');
INSERT INTO t VALUES(X'00FF', 1, 'zero_ff');
INSERT INTO t VALUES(X'FF00', 1, 'ff_zero');
INSERT INTO t VALUES(X'', 1, 'empty');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(X'0001', 1, 'feat_01');
INSERT INTO t VALUES(X'7F', 1, 'feat_7f');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(X'FFFE', 1, 'main_fffe');
INSERT INTO t VALUES(X'80', 1, 'main_80');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
F01=$(dl "$DB" "SELECT v FROM t WHERE a=X'0001';")
F7F=$(dl "$DB" "SELECT v FROM t WHERE a=X'7F';")
MFE=$(dl "$DB" "SELECT v FROM t WHERE a=X'FFFE';")
M80=$(dl "$DB" "SELECT v FROM t WHERE a=X'80';")
CONF=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
[ "$CNT" = "10" ] && pass_name "h_edge_count" || fail_name "h_edge_count; got $CNT"
[ "$F01" = "feat_01" ] && pass_name "h_feat_01" || fail_name "h_feat_01; got $F01"
[ "$F7F" = "feat_7f" ] && pass_name "h_feat_7f" || fail_name "h_feat_7f; got $F7F"
[ "$MFE" = "main_fffe" ] && pass_name "h_main_fffe" || fail_name "h_main_fffe; got $MFE"
[ "$M80" = "main_80" ] && pass_name "h_main_80" || fail_name "h_main_80; got $M80"
[ "$CONF" = "0" ] && pass_name "h_no_conflicts" || fail_name "h_no_conflicts; got $CONF"

# ── I: BLOB + INT mixed PK ordering ───────────────────────
echo ""
echo "--- I: INT-first composite PK with BLOB ---"

DB="$TMPROOT/i.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(seq INT, data BLOB, v TEXT, PRIMARY KEY(seq, data));
INSERT INTO t VALUES(1, X'AA', 'base1');
INSERT INTO t VALUES(1, X'BB', 'base2');
INSERT INTO t VALUES(2, X'AA', 'base3');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(1, X'CC', 'feat');
INSERT INTO t VALUES(3, X'AA', 'feat3');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, X'BB', 'main');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
FCC=$(dl "$DB" "SELECT v FROM t WHERE seq=1 AND data=X'CC';")
F3A=$(dl "$DB" "SELECT v FROM t WHERE seq=3 AND data=X'AA';")
MBB=$(dl "$DB" "SELECT v FROM t WHERE seq=2 AND data=X'BB';")
CONF=$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")
[ "$CNT" = "6" ] && pass_name "i_count" || fail_name "i_count; got $CNT"
[ "$FCC" = "feat" ] && pass_name "i_feat_row" || fail_name "i_feat_row; got $FCC"
[ "$F3A" = "feat3" ] && pass_name "i_feat3" || fail_name "i_feat3; got $F3A"
[ "$MBB" = "main" ] && pass_name "i_main_row" || fail_name "i_main_row; got $MBB"
[ "$CONF" = "0" ] && pass_name "i_no_conflicts" || fail_name "i_no_conflicts; got $CONF"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
