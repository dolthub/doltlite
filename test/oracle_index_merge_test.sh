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

dl() { "$DOLTLITE" "$1" "$2" 2>/dev/null; }
dl_pipe() { "$DOLTLITE" "$1" 2>/dev/null; }

echo "=== Comprehensive Index Merge Tests ==="

echo ""
echo "--- 1: Non-unique single-col, non-overlapping adds ---"
DB="$TMPROOT/1.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "1_count" || fail_name "1_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "1_integrity" || fail_name "1_integrity"

echo ""
echo "--- 2: Non-unique single-col, duplicate values ---"
DB="$TMPROOT/2.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,10,'feat_dup'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,10,'main_dup'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "2_count" || fail_name "2_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=10;")" = "3" ] && pass_name "2_idx_scan" || fail_name "2_idx_scan"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "2_integrity" || fail_name "2_integrity"

echo ""
echo "--- 3: Unique index, non-overlapping adds ---"
DB="$TMPROOT/3.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT UNIQUE, v TEXT); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "3_count" || fail_name "3_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "3_integrity" || fail_name "3_integrity"

echo ""
echo "--- 4: Multi-column index, non-overlapping ---"
DB="$TMPROOT/4.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, v TEXT); CREATE INDEX idx ON t(a,b); INSERT INTO t VALUES(1,1,1,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,2,2,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,3,3,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "4_count" || fail_name "4_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "4_integrity" || fail_name "4_integrity"

echo ""
echo "--- 5: Multi-column index, shared prefix ---"
DB="$TMPROOT/5.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, v TEXT); CREATE INDEX idx ON t(a,b); INSERT INTO t VALUES(1,1,1,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,1,2,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,1,3,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "5_count" || fail_name "5_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE a=1;")" = "3" ] && pass_name "5_prefix_scan" || fail_name "5_prefix_scan"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "5_integrity" || fail_name "5_integrity"

echo ""
echo "--- 6: NULL index values from both sides ---"
DB="$TMPROOT/6.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,NULL,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,NULL,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,NULL,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t NOT INDEXED;")" = "3" ] && pass_name "6_table_count" || fail_name "6_table_count"
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "6_index_count" || fail_name "6_index_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "6_integrity" || fail_name "6_integrity"

echo ""
echo "--- 7: Update indexed column on one side ---"
DB="$TMPROOT/7.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base1'); INSERT INTO t VALUES(2,20,'base2'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET k=15 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "7_count" || fail_name "7_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=15;")" = "1" ] && pass_name "7_updated_via_idx" || fail_name "7_updated_via_idx"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=10;")" = "0" ] && pass_name "7_old_gone" || fail_name "7_old_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "7_integrity" || fail_name "7_integrity"

echo ""
echo "--- 8: Delete on one side, add on other ---"
DB="$TMPROOT/8.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base1'); INSERT INTO t VALUES(2,20,'base2'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); DELETE FROM t WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "8_count" || fail_name "8_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=10;")" = "0" ] && pass_name "8_deleted_gone" || fail_name "8_deleted_gone"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "8_integrity" || fail_name "8_integrity"

echo ""
echo "--- 9: Convergent update with index ---"
DB="$TMPROOT/9.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET k=99 WHERE id=1; SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); UPDATE t SET k=99 WHERE id=1; SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" = "0" ] && pass_name "9_no_conflict" || fail_name "9_no_conflict"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=99;")" = "1" ] && pass_name "9_idx_scan" || fail_name "9_idx_scan"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "9_integrity" || fail_name "9_integrity"

echo ""
echo "--- 10: Modify-modify conflict with index ---"
DB="$TMPROOT/10.db"
CONF=$(
  {
    cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT);
CREATE INDEX idx ON t(k);
INSERT INTO t VALUES(1,10,'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET k=100 WHERE id=1;
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET k=200 WHERE id=1;
SELECT dolt_commit('-Am','main');
BEGIN;
SELECT dolt_merge('feat');
SELECT 'CONF', count(*) FROM dolt_conflicts;
DELETE FROM dolt_conflicts_t;
REINDEX;
SELECT dolt_commit('-Am','resolved');
SQL
  } | dl_pipe "$DB" | awk -F'|' '$1=="CONF"{print $2}'
)
[ "$CONF" = "1" ] && pass_name "10_conflict" || fail_name "10_conflict"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "10_integrity_after_resolve" || fail_name "10_integrity_after_resolve"

echo ""
echo "--- 11: Multiple indexes on same table ---"
DB="$TMPROOT/11.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT); CREATE INDEX idx_a ON t(a); CREATE INDEX idx_b ON t(b); INSERT INTO t VALUES(1,10,100,'base'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20,200,'feat'); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30,300,'main'); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "11_count" || fail_name "11_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE a=20;")" = "1" ] && pass_name "11_idx_a" || fail_name "11_idx_a"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE b=200;")" = "1" ] && pass_name "11_idx_b" || fail_name "11_idx_b"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "11_integrity" || fail_name "11_integrity"

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

echo ""
echo "--- 13: Text index with duplicates ---"
DB="$TMPROOT/13.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, v INT); CREATE INDEX idx ON t(name); INSERT INTO t VALUES(1,'alice',1); INSERT INTO t VALUES(2,'bob',1); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(3,'alice',2); INSERT INTO t VALUES(4,'carol',1); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(5,'alice',3); INSERT INTO t VALUES(6,'dave',1); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "6" ] && pass_name "13_count" || fail_name "13_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE name='alice';")" = "3" ] && pass_name "13_alice_count" || fail_name "13_alice_count"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "13_integrity" || fail_name "13_integrity"

echo ""
echo "--- 14: Index survives reopen after merge ---"
DB="$TMPROOT/14.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,30); SELECT dolt_commit('-Am','main'); SELECT dolt_merge('feat'); SELECT dolt_commit('-Am','merged');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "3" ] && pass_name "14_count" || fail_name "14_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "14_idx_after_reopen" || fail_name "14_idx_after_reopen"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "14_integrity" || fail_name "14_integrity"

echo ""
echo "--- 15: Fast-forward merge preserves index ---"
DB="$TMPROOT/15.db"
dl "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT); CREATE INDEX idx ON t(k); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20); SELECT dolt_commit('-Am','feat'); SELECT dolt_checkout('main'); SELECT dolt_merge('feat');" >/dev/null
[ "$(dl "$DB" "SELECT count(*) FROM t;")" = "2" ] && pass_name "15_ff_count" || fail_name "15_ff_count"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE k=20;")" = "1" ] && pass_name "15_ff_idx" || fail_name "15_ff_idx"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "15_integrity" || fail_name "15_integrity"

# Their side drops a column that precedes the indexed one and edits rows, so
# incoming records are in the shortened layout while our index columns are
# numbered for the old one.
echo ""
echo "--- 16: DROP COLUMN before the indexed column on their side, merge ---"
for OP in merge cherry_pick; do
  DB="$TMPROOT/16_$OP.db"
  dl_pipe "$DB" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, n INT);
CREATE INDEX ix ON t(n);
INSERT INTO t VALUES(1,1,1,1),(2,2,2,2),(3,3,3,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
ALTER TABLE t DROP COLUMN a;
UPDATE t SET n=30 WHERE id=3;
SELECT dolt_commit('-am','drop a, edit row 3');
SELECT dolt_checkout('main');
UPDATE t SET n=20 WHERE id=2;
SELECT dolt_commit('-am','edit row 2');
SELECT dolt_$OP('b');
SQL
  SCAN=$(dl "$DB" "SELECT group_concat(id||'|'||b||'|'||n,';') FROM (SELECT * FROM t NOT INDEXED ORDER BY id);")
  IDX=$(dl "$DB" "SELECT group_concat(id||'|'||b||'|'||n,';') FROM (SELECT * FROM t INDEXED BY ix ORDER BY id);")
  [ "$SCAN" = "1|1|1;2|2|20;3|3|30" ] && pass_name "16_${OP}_scan" || fail_name "16_${OP}_scan; got $SCAN"
  [ "$IDX" = "$SCAN" ] && pass_name "16_${OP}_index_matches_scan" || fail_name "16_${OP}_index_matches_scan; got $IDX"
  [ "$(dl "$DB" "SELECT count(*) FROM t WHERE n=30;")" = "1" ] && pass_name "16_${OP}_lookup" || fail_name "16_${OP}_lookup"
  [ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "16_${OP}_integrity" || fail_name "16_${OP}_integrity"
done

echo ""
echo "--- 17: DROP COLUMN plus DELETE on their side ---"
DB="$TMPROOT/17.db"
dl_pipe "$DB" <<'SQL' >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, n INT);
CREATE INDEX ix ON t(n);
INSERT INTO t VALUES(1,1,1,1),(2,2,2,2),(3,3,3,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
ALTER TABLE t DROP COLUMN a;
DELETE FROM t WHERE id=3;
INSERT INTO t VALUES(4,4,40);
SELECT dolt_commit('-am','drop a, delete 3, add 4');
SELECT dolt_checkout('main');
UPDATE t SET n=20 WHERE id=2;
SELECT dolt_commit('-am','edit row 2');
SELECT dolt_merge('b');
SQL
SCAN=$(dl "$DB" "SELECT group_concat(id||'|'||b||'|'||n,';') FROM (SELECT * FROM t NOT INDEXED ORDER BY id);")
IDX=$(dl "$DB" "SELECT group_concat(id||'|'||b||'|'||n,';') FROM (SELECT * FROM t INDEXED BY ix ORDER BY id);")
[ "$SCAN" = "1|1|1;2|2|20;4|4|40" ] && pass_name "17_scan" || fail_name "17_scan; got $SCAN"
[ "$IDX" = "$SCAN" ] && pass_name "17_index_matches_scan" || fail_name "17_index_matches_scan; got $IDX"
[ "$(dl "$DB" "SELECT count(*) FROM t WHERE n=40;")" = "1" ] && pass_name "17_lookup" || fail_name "17_lookup"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "17_integrity" || fail_name "17_integrity"

echo ""
echo "--- 18: DROP COLUMN on their side, WITHOUT ROWID composite PK ---"
DB="$TMPROOT/18.db"
dl_pipe "$DB" <<'SQL' >/dev/null
CREATE TABLE t(k1 INT, k2 TEXT, a INT, n INT, PRIMARY KEY(k1,k2)) WITHOUT ROWID;
CREATE INDEX ix ON t(n);
INSERT INTO t VALUES(1,'x',1,1),(2,'x',2,2),(3,'x',3,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
ALTER TABLE t DROP COLUMN a;
UPDATE t SET n=30 WHERE k1=3;
SELECT dolt_commit('-am','drop a, edit row 3');
SELECT dolt_checkout('main');
UPDATE t SET n=20 WHERE k1=2;
SELECT dolt_commit('-am','edit row 2');
SELECT dolt_merge('b');
SQL
SCAN=$(dl "$DB" "SELECT group_concat(k1||'|'||n,';') FROM (SELECT * FROM t NOT INDEXED ORDER BY k1);")
IDX=$(dl "$DB" "SELECT group_concat(k1||'|'||n,';') FROM (SELECT * FROM t INDEXED BY ix ORDER BY k1);")
[ "$SCAN" = "1|1;2|20;3|30" ] && pass_name "18_scan" || fail_name "18_scan; got $SCAN"
[ "$IDX" = "$SCAN" ] && pass_name "18_index_matches_scan" || fail_name "18_index_matches_scan; got $IDX"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "18_integrity" || fail_name "18_integrity"

echo ""
echo "--- 19: DROP COLUMN on their side, UNIQUE and partial indexes ---"
DB="$TMPROOT/19.db"
dl_pipe "$DB" <<'SQL' >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, n INT, flag INT);
CREATE UNIQUE INDEX ux ON t(n);
CREATE INDEX px ON t(flag) WHERE flag>0;
INSERT INTO t VALUES(1,1,1,0),(2,2,2,1),(3,3,3,0);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
ALTER TABLE t DROP COLUMN a;
UPDATE t SET n=30, flag=1 WHERE id=3;
SELECT dolt_commit('-am','drop a, edit row 3');
SELECT dolt_checkout('main');
UPDATE t SET n=20 WHERE id=2;
SELECT dolt_commit('-am','edit row 2');
SELECT dolt_merge('b');
SQL
[ "$(dl "$DB" "SELECT count(*) FROM t INDEXED BY ux WHERE n=30;")" = "1" ] && pass_name "19_unique_lookup" || fail_name "19_unique_lookup"
[ "$(dl "$DB" "SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY px WHERE flag>0 ORDER BY id);")" = "2,3" ] && pass_name "19_partial_rows" || fail_name "19_partial_rows"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "19_integrity" || fail_name "19_integrity"

# SQLite refuses to drop an indexed column, so the only way an index can
# outlive its column is our side indexing a column their side drops. The
# merge must drop that index and keep the others consistent.
echo ""
echo "--- 20: Our index on a column their side dropped ---"
DB="$TMPROOT/20.db"
dl_pipe "$DB" <<'SQL' >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, n INT);
CREATE INDEX ix_n ON t(n);
INSERT INTO t VALUES(1,1,1),(2,2,2),(3,3,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
ALTER TABLE t DROP COLUMN a;
UPDATE t SET n=30 WHERE id=3;
SELECT dolt_commit('-am','drop a, edit row 3');
SELECT dolt_checkout('main');
CREATE INDEX ix_a ON t(a);
UPDATE t SET n=20 WHERE id=2;
SELECT dolt_commit('-am','index a, edit row 2');
SELECT dolt_merge('b');
SQL
[ "$(dl "$DB" "SELECT group_concat(name) FROM sqlite_master WHERE type='index' ORDER BY name;")" = "ix_n" ] && pass_name "20_dropped_column_index_gone" || fail_name "20_dropped_column_index_gone"
[ "$(dl "$DB" "SELECT group_concat(name) FROM pragma_table_info('t');")" = "id,n" ] && pass_name "20_columns" || fail_name "20_columns"
SCAN=$(dl "$DB" "SELECT group_concat(id||'|'||n,';') FROM (SELECT * FROM t NOT INDEXED ORDER BY id);")
IDX=$(dl "$DB" "SELECT group_concat(id||'|'||n,';') FROM (SELECT * FROM t INDEXED BY ix_n ORDER BY id);")
[ "$SCAN" = "1|1;2|20;3|30" ] && pass_name "20_scan" || fail_name "20_scan; got $SCAN"
[ "$IDX" = "$SCAN" ] && pass_name "20_index_matches_scan" || fail_name "20_index_matches_scan; got $IDX"
[ "$(dl "$DB" "PRAGMA integrity_check;")" = "ok" ] && pass_name "20_integrity" || fail_name "20_integrity"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
echo "__SUITE_COMPLETE__"
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
