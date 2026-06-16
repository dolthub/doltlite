#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
PASS=0; FAIL=0; SKIP=0
ERRORS=""
VC_UNAVAILABLE="dolt version-control features are not available on stock SQLite databases"

want_eq() {
  local name="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    want: $(printf %q "$want")\n    got:  $(printf %q "$got")"
  fi
}

skip() {
  local name="$1" reason="$2"
  SKIP=$((SKIP+1))
  echo "  SKIP: $name — $reason"
}

sq_last() { printf '%s\n' "$1" | $SQLITE3 "$2" 2>&1 | tail -1; }
dl_last() { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1 | tail -1; }
dl_all()  { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1; }

want_vc_unavailable() {
  local name="$1" sql="$2" db="$3"
  want_eq "$name" "$(dl_all "$sql" "$db" | grep -c "$VC_UNAVAILABLE")" "1"
}

seed_stock() {
  rm -f "$1"
  $SQLITE3 "$1" "$2"
}

echo "=== doltlite opens stock-SQLite file ==="

if [ ! -x "$SQLITE3" ]; then
  echo "SKIP: stock sqlite3 binary not found at $SQLITE3"
  echo ""
  echo "Results: 0 passed, 0 failed, 1 skipped"
  exit 0
fi

echo ""
echo "--- reads ---"

DB=$TMP/r1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');"
want_eq "R1_select_star_count" \
  "$(dl_last "SELECT count(*) FROM t;" "$DB")" "3"

want_eq "R1b_select_by_pk" \
  "$(dl_last "SELECT v FROM t WHERE id=2;" "$DB")" "b"

want_eq "R2_join" \
  "$(dl_last "WITH u(id,w) AS (VALUES(1,'x'),(2,'y')) SELECT t.v||u.w FROM t JOIN u USING(id) WHERE t.id=1;" "$DB")" "ax"

want_eq "R3_aggregate" \
  "$(dl_last "SELECT group_concat(v,',') FROM (SELECT v FROM t ORDER BY id);" "$DB")" "a,b,c"

DB=$TMP/r4.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX idx_v ON t(v); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');"
want_eq "R4_select_via_pre_existing_index" \
  "$(dl_last "SELECT id FROM t WHERE v='b';" "$DB")" "2"

want_eq "R5_sqlite_master_visible" \
  "$(dl_last "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_v';" "$DB")" "1"

DB=$TMP/r6.db
seed_stock "$DB" "CREATE TABLE accounts(username VARCHAR NOT NULL PRIMARY KEY, email TEXT, balance INTEGER, status TEXT); INSERT INTO accounts VALUES('alice','alice@example.com',100,'active'),('bob','bob@example.com',250,'frozen'),('carol','carol@example.com',50,'active');"
want_eq "R6_non_integer_pk_reads_payload_columns" \
  "$(dl_last "SELECT group_concat(username||':'||email||':'||balance||':'||status, ',') FROM accounts ORDER BY username;" "$DB")" \
  "alice:alice@example.com:100:active,bob:bob@example.com:250:frozen,carol:carol@example.com:50:active"

want_eq "R7_non_integer_pk_payload_predicate" \
  "$(dl_last "SELECT username FROM accounts WHERE balance >= 200 AND status='frozen';" "$DB")" \
  "bob"

want_eq "R8_text_pk_rowid_still_available" \
  "$(dl_last "SELECT count(*) FROM accounts WHERE rowid IS NOT NULL;" "$DB")" \
  "3"

want_eq "R9_text_pk_column_types_preserved" \
  "$(dl_last "SELECT group_concat(typeof(email)||':'||typeof(balance)||':'||typeof(status), ',') FROM (SELECT * FROM accounts ORDER BY username);" "$DB")" \
  "text:integer:text,text:integer:text,text:integer:text"

DB=$TMP/r10.db
seed_stock "$DB" "CREATE TABLE orders(region TEXT NOT NULL, order_id INTEGER NOT NULL, item TEXT, qty INTEGER, PRIMARY KEY(region, order_id)); INSERT INTO orders VALUES('east',1,'pen',2),('east',2,'pad',3),('west',1,'bag',4);"
want_eq "R10_composite_pk_reads_payload_columns" \
  "$(dl_last "SELECT group_concat(region||':'||order_id||':'||item||':'||qty, ',') FROM (SELECT * FROM orders ORDER BY region, order_id);" "$DB")" \
  "east:1:pen:2,east:2:pad:3,west:1:bag:4"

want_eq "R11_composite_pk_rowid_still_available" \
  "$(dl_last "SELECT count(*) FROM orders WHERE rowid IS NOT NULL;" "$DB")" \
  "3"

DB=$TMP/r12.db
seed_stock "$DB" "CREATE TABLE blobs(k BLOB NOT NULL PRIMARY KEY, label TEXT, n INTEGER); INSERT INTO blobs VALUES(x'01','one',1),(x'0203','two-three',23);"
want_eq "R12_blob_pk_reads_payload_columns" \
  "$(dl_last "SELECT group_concat(hex(k)||':'||label||':'||n, ',') FROM (SELECT * FROM blobs ORDER BY k);" "$DB")" \
  "01:one:1,0203:two-three:23"

echo ""
echo "--- autocommit writes ---"

DB=$TMP/w1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
want_eq "W1_autocommit_insert" \
  "$(dl_last "INSERT INTO t VALUES(2,'b'); SELECT v FROM t WHERE id=2;" "$DB")" "b"

want_eq "W2_autocommit_update" \
  "$(dl_last "UPDATE t SET v='Z' WHERE id=1; SELECT v FROM t WHERE id=1;" "$DB")" "Z"

want_eq "W3_autocommit_delete" \
  "$(dl_last "DELETE FROM t WHERE id=1; SELECT count(*) FROM t WHERE id=1;" "$DB")" "0"

want_eq "W4_bulk_insert" \
  "$(dl_last "INSERT INTO t SELECT n+10, hex(n) FROM (WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<50) SELECT n FROM seq); SELECT count(*) FROM t WHERE id>10;" "$DB")" "50"

DB=$TMP/w5.db
seed_stock "$DB" "CREATE TABLE accounts(username TEXT NOT NULL PRIMARY KEY, email TEXT, balance INTEGER, status TEXT); INSERT INTO accounts VALUES('alice','alice@example.com',100,'active'),('bob','bob@example.com',250,'frozen');"
want_eq "W5_text_pk_insert_update_delete_payload" \
  "$(dl_last "INSERT INTO accounts VALUES('carol','carol@example.com',50,'active'); UPDATE accounts SET balance=balance+5, status='active' WHERE username='bob'; DELETE FROM accounts WHERE username='alice'; SELECT group_concat(username||':'||email||':'||balance||':'||status, ',') FROM (SELECT * FROM accounts ORDER BY username);" "$DB")" \
  "bob:bob@example.com:255:active,carol:carol@example.com:50:active"

want_eq "W6_text_pk_writes_visible_to_stock" \
  "$(sq_last "SELECT group_concat(username||':'||balance, ',') FROM (SELECT * FROM accounts ORDER BY username);" "$DB")" \
  "bob:255,carol:50"

DB=$TMP/w7.db
seed_stock "$DB" "CREATE TABLE orders(region TEXT NOT NULL, order_id INTEGER NOT NULL, item TEXT, qty INTEGER, PRIMARY KEY(region, order_id)); INSERT INTO orders VALUES('east',1,'pen',2);"
want_eq "W7_composite_pk_insert_update_payload" \
  "$(dl_last "INSERT INTO orders VALUES('west',1,'bag',4); UPDATE orders SET qty=5 WHERE region='east' AND order_id=1; SELECT group_concat(region||':'||order_id||':'||item||':'||qty, ',') FROM (SELECT * FROM orders ORDER BY region, order_id);" "$DB")" \
  "east:1:pen:5,west:1:bag:4"

echo ""
echo "--- DDL ---"

DB=$TMP/d1.db
seed_stock "$DB" "CREATE TABLE existing(id INTEGER PRIMARY KEY);"
want_eq "D1_create_new_table" \
  "$(dl_last "CREATE TABLE new_t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO new_t VALUES(1,'x'); SELECT v FROM new_t;" "$DB")" "x"

want_eq "D2_drop_table" \
  "$(dl_last "CREATE TABLE tmp(id INTEGER); DROP TABLE tmp; SELECT count(*) FROM sqlite_master WHERE name='tmp';" "$DB")" "0"

want_eq "D3_alter_table_add_column" \
  "$(dl_last "ALTER TABLE existing ADD COLUMN v TEXT; INSERT INTO existing(id,v) VALUES(7,'seven'); SELECT v FROM existing WHERE id=7;" "$DB")" "seven"

DB=$TMP/d4.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');"
want_eq "D4_create_index_on_populated_table" \
  "$(dl_last "CREATE INDEX idx_v ON t(v); SELECT id FROM t WHERE v='b';" "$DB")" "2"

DB=$TMP/d5.db
seed_stock "$DB" "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);"
want_eq "D5_create_index_on_empty_table" \
  "$(dl_last "CREATE INDEX idx_v ON u(v); SELECT name FROM sqlite_master WHERE type='index';" "$DB")" "idx_v"

DB=$TMP/d6.db
seed_stock "$DB" "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX idx_v ON u(v); INSERT INTO u VALUES(1,'a'),(2,'b');"
want_eq "D6_drop_pre_existing_index" \
  "$(dl_last "DROP INDEX idx_v; SELECT count(*) FROM sqlite_master WHERE type='index';" "$DB")" "0"

DB=$TMP/d7.db
seed_stock "$DB" "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX idx_v ON u(v); INSERT INTO u VALUES(1,'a'),(2,'b');"
want_eq "D7_reindex_populated_table" \
  "$(dl_last "REINDEX; SELECT id FROM u WHERE v='b';" "$DB")" "2"

DB=$TMP/d8.db
seed_stock "$DB" "CREATE TABLE existing(id INTEGER PRIMARY KEY);"
want_eq "D8_create_text_pk_table_on_stock_file_keeps_rowid" \
  "$(dl_last "CREATE TABLE created(k TEXT NOT NULL PRIMARY KEY, v TEXT, n INTEGER); INSERT INTO created VALUES('a','alpha',1),('b','beta',2); SELECT count(*)||':'||group_concat(k||':'||v||':'||n, ',') FROM (SELECT * FROM created WHERE rowid IS NOT NULL ORDER BY k);" "$DB")" \
  "2:a:alpha:1,b:beta:2"

want_eq "D9_created_text_pk_table_readable_by_stock" \
  "$(sq_last "SELECT group_concat(k||':'||v||':'||n, ',') FROM (SELECT * FROM created ORDER BY k);" "$DB")" \
  "a:alpha:1,b:beta:2"

echo ""
echo "--- constraints ---"

DB=$TMP/c1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, u INTEGER UNIQUE); INSERT INTO t VALUES(1,10);"
want_eq "C1_unique_constraint_enforced" \
  "$(dl_all "INSERT INTO t VALUES(2,10);" "$DB" | grep -ci 'unique')" "1"

DB=$TMP/c2.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL); INSERT INTO t VALUES(1,5);"
want_eq "C2_not_null_constraint_enforced" \
  "$(dl_all "INSERT INTO t(id,v) VALUES(2,NULL);" "$DB" | grep -ci 'not null')" "1"

DB=$TMP/c3.db
seed_stock "$DB" "PRAGMA foreign_keys=1; CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES p(id)); INSERT INTO p VALUES(1); INSERT INTO c VALUES(1,1);"
want_eq "C3_fk_constraint_enforced" \
  "$(dl_all "PRAGMA foreign_keys=1; INSERT INTO c VALUES(2,99);" "$DB" | grep -ci 'foreign key')" "1"

DB=$TMP/c4.db
seed_stock "$DB" "CREATE TABLE accounts(username TEXT NOT NULL PRIMARY KEY, email TEXT); INSERT INTO accounts VALUES('alice','alice@example.com');"
want_eq "C4_text_pk_uniqueness_enforced" \
  "$(dl_all "INSERT INTO accounts VALUES('alice','dup@example.com');" "$DB" | grep -ci 'unique')" "1"

DB=$TMP/c5.db
seed_stock "$DB" "CREATE TABLE orders(region TEXT NOT NULL, order_id INTEGER NOT NULL, item TEXT, PRIMARY KEY(region, order_id)); INSERT INTO orders VALUES('east',1,'pen');"
want_eq "C5_composite_pk_uniqueness_enforced" \
  "$(dl_all "INSERT INTO orders VALUES('east',1,'pad');" "$DB" | grep -ci 'unique')" "1"

echo ""
echo "--- transactions ---"

DB=$TMP/t1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
want_eq "T1_begin_commit" \
  "$(dl_last "BEGIN; INSERT INTO t VALUES(2,'b'); COMMIT; SELECT count(*) FROM t;" "$DB")" "2"

DB=$TMP/t2.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
want_eq "T2_begin_rollback" \
  "$(dl_last "BEGIN; INSERT INTO t VALUES(2,'b'); ROLLBACK; SELECT count(*) FROM t;" "$DB")" "1"

DB=$TMP/t3.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
want_eq "T3_savepoint_release" \
  "$(dl_last "BEGIN; SAVEPOINT s; INSERT INTO t VALUES(2,'b'); RELEASE s; COMMIT; SELECT count(*) FROM t;" "$DB")" "2"

DB=$TMP/t4.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
want_eq "T4_savepoint_rollback_to" \
  "$(dl_last "BEGIN; SAVEPOINT s; INSERT INTO t VALUES(2,'b'); ROLLBACK TO s; COMMIT; SELECT count(*) FROM t;" "$DB")" "1"

echo ""
echo "--- VC features on non-doltlite file ---"

DB=$TMP/v1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);"
want_eq "V1_doltlite_engine_reports_orig" \
  "$(dl_last "SELECT doltlite_engine();" "$DB")" "orig"

want_vc_unavailable "V2_dolt_add_unavailable_on_stock_format" \
  "SELECT dolt_add('-A');" "$DB"

want_vc_unavailable "V3_dolt_commit_unavailable_on_stock_format" \
  "SELECT dolt_commit('-m','stock');" "$DB"

want_vc_unavailable "V4_dolt_branch_unavailable_on_stock_format" \
  "SELECT dolt_branch('feat');" "$DB"

want_vc_unavailable "V5_dolt_checkout_unavailable_on_stock_format" \
  "SELECT dolt_checkout('feat');" "$DB"

want_vc_unavailable "V6_dolt_merge_unavailable_on_stock_format" \
  "SELECT dolt_merge('feat');" "$DB"

want_vc_unavailable "V7_dolt_reset_unavailable_on_stock_format" \
  "SELECT dolt_reset('--hard');" "$DB"

want_vc_unavailable "V8_dolt_log_unavailable_on_stock_format" \
  "SELECT count(*) FROM dolt_log;" "$DB"

want_vc_unavailable "V9_dolt_status_unavailable_on_stock_format" \
  "SELECT count(*) FROM dolt_status;" "$DB"

echo ""
echo "--- durability ---"

DB=$TMP/p1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
dl_all "INSERT INTO t VALUES(2,'b'); UPDATE t SET v='Z' WHERE id=1;" "$DB" >/dev/null
want_eq "P1_doltlite_writes_durable_reopen_with_doltlite" \
  "$(dl_last "SELECT v||'/'||(SELECT count(*) FROM t) FROM t WHERE id=1;" "$DB")" "Z/2"

want_eq "P2_doltlite_writes_visible_in_stock_sqlite3" \
  "$(sq_last "SELECT v||'/'||(SELECT count(*) FROM t) FROM t WHERE id=1;" "$DB")" "Z/2"

DB=$TMP/p3.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
dl_all "INSERT INTO t VALUES(2,'b'); UPDATE t SET v='Z' WHERE id=1;" "$DB" >/dev/null
$SQLITE3 "$DB" "INSERT INTO t VALUES(3,'c');"
want_eq "P3_stock_writes_after_doltlite_visible_in_doltlite" \
  "$(dl_last "SELECT count(*) FROM t;" "$DB")" "3"

echo ""
echo "============================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "============================="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
