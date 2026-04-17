#!/bin/bash
#
# Oracle tests: NULL values in indexed columns through merge.
#
# Verifies that tables with NULLs in non-PK indexed columns
# survive merge correctly. Non-unique indexes can have multiple
# rows with the same sort key (e.g. two rows with a=NULL, b=NULL)
# and the merge must preserve all of them.
#
# Usage: bash test/oracle_null_index_merge_test.sh <doltlite>
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

echo "=== NULL Index Merge Tests ==="

# ── A: Non-unique index with duplicate NULL keys through merge ──
echo ""
echo "--- A: Duplicate NULL index keys through merge ---"

DB="$TMPROOT/a.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1, 10, NULL, 'base1');
INSERT INTO t VALUES(2, 10, 20,   'base2');
INSERT INTO t VALUES(3, NULL, 30, 'base3');
INSERT INTO t VALUES(4, NULL, NULL,'base4');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET c='feat_mod' WHERE id=1;
INSERT INTO t VALUES(5, NULL, NULL, 'feat_nn');
INSERT INTO t VALUES(6, 10, NULL, 'feat_n2');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET c='main_mod' WHERE id=3;
INSERT INTO t VALUES(7, NULL, 50, 'main_n50');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

TABLE_CNT=$(dl "$DB" "SELECT count(*) FROM t NOT INDEXED;")
INDEX_CNT=$(dl "$DB" "SELECT count(*) FROM t INDEXED BY idx_ab WHERE a IS NOT NULL OR a IS NULL;")
INTEGRITY=$(dl "$DB" "PRAGMA integrity_check;")

[ "$TABLE_CNT" = "7" ] && pass_name "a_table_count" || fail_name "a_table_count; got $TABLE_CNT"
[ "$INDEX_CNT" = "7" ] && pass_name "a_index_count" || fail_name "a_index_count; got $INDEX_CNT"
[ "$INTEGRITY" = "ok" ] && pass_name "a_integrity" || fail_name "a_integrity; got $INTEGRITY"

# ── B: Simple case: no NULL collisions ────────────────────
echo ""
echo "--- B: Index merge without NULL collisions ---"

DB="$TMPROOT/b.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1, 10, 20, 'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2, 30, 40, 'feat');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3, 50, 60, 'main');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

CNT=$(dl "$DB" "SELECT count(*) FROM t;")
INTEGRITY=$(dl "$DB" "PRAGMA integrity_check;")
[ "$CNT" = "3" ] && pass_name "b_count" || fail_name "b_count; got $CNT"
[ "$INTEGRITY" = "ok" ] && pass_name "b_integrity" || fail_name "b_integrity; got $INTEGRITY"

# ── C: Single NULL column in index ────────────────────────
echo ""
echo "--- C: Single NULL column in index through merge ---"

DB="$TMPROOT/c.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT);
CREATE INDEX idx_k ON t(k);
INSERT INTO t VALUES(1, NULL, 'base1');
INSERT INTO t VALUES(2, 10,   'base2');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3, NULL, 'feat_null');
INSERT INTO t VALUES(4, 20,   'feat_20');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5, NULL, 'main_null');
INSERT INTO t VALUES(6, 30,   'main_30');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

TABLE_CNT=$(dl "$DB" "SELECT count(*) FROM t NOT INDEXED;")
INDEX_CNT=$(dl "$DB" "SELECT count(*) FROM t;")
INTEGRITY=$(dl "$DB" "PRAGMA integrity_check;")
[ "$TABLE_CNT" = "6" ] && pass_name "c_table_count" || fail_name "c_table_count; got $TABLE_CNT"
[ "$INDEX_CNT" = "6" ] && pass_name "c_index_count" || fail_name "c_index_count; got $INDEX_CNT"
[ "$INTEGRITY" = "ok" ] && pass_name "c_integrity" || fail_name "c_integrity; got $INTEGRITY"

# ── D: All NULLs from both sides ──────────────────────────
echo ""
echo "--- D: All-NULL index values from both branches ---"

DB="$TMPROOT/d.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT);
CREATE INDEX idx_k ON t(k);
INSERT INTO t VALUES(1, NULL, 'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2, NULL, 'feat');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3, NULL, 'main');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
SQL
)" >/dev/null

TABLE_CNT=$(dl "$DB" "SELECT count(*) FROM t NOT INDEXED;")
INDEX_CNT=$(dl "$DB" "SELECT count(*) FROM t;")
INTEGRITY=$(dl "$DB" "PRAGMA integrity_check;")
[ "$TABLE_CNT" = "3" ] && pass_name "d_table_count" || fail_name "d_table_count; got $TABLE_CNT"
[ "$INDEX_CNT" = "3" ] && pass_name "d_index_count" || fail_name "d_index_count; got $INDEX_CNT"
[ "$INTEGRITY" = "ok" ] && pass_name "d_integrity" || fail_name "d_integrity; got $INTEGRITY"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
