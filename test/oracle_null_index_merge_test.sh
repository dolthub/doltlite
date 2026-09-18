#!/bin/bash

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
echo "--- E: Cell merge sets the trailing indexed column to NULL ---"

# Both sides edit row 2 in different columns; theirs NULLs the indexed
# trailing column. The merged record stops short of that column, and the
# index key must still carry the NULL rather than end at the rowid.
for OP in merge cherry_pick; do
  DB="$TMPROOT/e_$OP.db"
  dl "$DB" "$(cat <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, c INT);
CREATE INDEX ix0 ON t(c);
INSERT INTO t VALUES(1,1,1),(2,2,2),(3,3,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
UPDATE t SET c=NULL WHERE id=2;
SELECT dolt_commit('-am','c null');
SELECT dolt_checkout('main');
UPDATE t SET a=73 WHERE id=2;
SELECT dolt_commit('-am','a 73');
SELECT dolt_$OP('b');
SQL
)" >/dev/null

  SCAN=$(dl "$DB" "SELECT group_concat(id||'|'||a||'|'||coalesce(c,'NULL'),';') FROM (SELECT * FROM t NOT INDEXED ORDER BY id);")
  IDX=$(dl "$DB" "SELECT group_concat(id||'|'||a||'|'||coalesce(c,'NULL'),';') FROM (SELECT * FROM t INDEXED BY ix0 ORDER BY id);")
  NULLS=$(dl "$DB" "SELECT count(*) FROM t WHERE c IS NULL;")
  INTEGRITY=$(dl "$DB" "PRAGMA integrity_check;")
  [ "$SCAN" = "1|1|1;2|73|NULL;3|3|3" ] && pass_name "e_${OP}_scan" || fail_name "e_${OP}_scan; got $SCAN"
  [ "$IDX" = "$SCAN" ] && pass_name "e_${OP}_index_matches_scan" || fail_name "e_${OP}_index_matches_scan; got $IDX"
  [ "$NULLS" = "1" ] && pass_name "e_${OP}_null_lookup" || fail_name "e_${OP}_null_lookup; got $NULLS"
  [ "$INTEGRITY" = "ok" ] && pass_name "e_${OP}_integrity" || fail_name "e_${OP}_integrity; got $INTEGRITY"
done

# Trailing INTEGER PRIMARY KEY column: the trimmed record stops before the
# IPK slot, and the key must still end with the rowid.
DB="$TMPROOT/e_ipk_tail.db"
dl "$DB" "$(cat <<'SQL'
CREATE TABLE t(a INT, c INT, id INTEGER PRIMARY KEY);
CREATE INDEX ix0 ON t(c);
INSERT INTO t VALUES(1,1,1),(2,2,2),(3,3,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
UPDATE t SET c=NULL WHERE id=2;
SELECT dolt_commit('-am','c null');
SELECT dolt_checkout('main');
UPDATE t SET a=73 WHERE id=2;
SELECT dolt_commit('-am','a 73');
SELECT dolt_merge('b');
SQL
)" >/dev/null
IDX=$(dl "$DB" "SELECT group_concat(id||'|'||a||'|'||coalesce(c,'NULL'),';') FROM (SELECT * FROM t INDEXED BY ix0 ORDER BY id);")
INTEGRITY=$(dl "$DB" "PRAGMA integrity_check;")
[ "$IDX" = "1|1|1;2|73|NULL;3|3|3" ] && pass_name "e_ipk_tail_index" || fail_name "e_ipk_tail_index; got $IDX"
[ "$INTEGRITY" = "ok" ] && pass_name "e_ipk_tail_integrity" || fail_name "e_ipk_tail_integrity; got $INTEGRITY"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
echo "__SUITE_COMPLETE__"
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
