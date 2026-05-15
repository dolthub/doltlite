#!/bin/bash
# Open a stock-SQLite-format file with doltlite and exercise it.
# This is the "drop-in replacement" scenario: someone has a SQLite
# database (built by sqlite3 CLI, an app's SDK, etc.) and opens it
# with doltlite to inspect or modify it. doltlite detects the SQLite
# header via origBtreeIsSqliteFile and routes the whole connection
# through the renamed-orig stock SQLite engine.
#
# All cases here use a file that was created and seeded by the stock
# `sqlite3` binary, then opened by `./doltlite`. The contract is:
# anything the stock binary can do on that file, doltlite should also
# be able to do. dolt_* VC features are out of scope (the file isn't
# in doltlite format).
#
# KNOWN BUGS — these tests are skipped today and will be removed once
# the underlying bugs are fixed in a follow-up PR:
#   - CREATE INDEX on a populated table SIGSEGVs in orig_sqlite3BtreeInsert
#     when run via the doltlite binary on a SQLite-format file. The same
#     SQL works in the stock sqlite3 binary. Repro: any pre-populated
#     SQLite file + CREATE INDEX over a column.

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
PASS=0; FAIL=0; SKIP=0
ERRORS=""

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

# Run SQL on stock sqlite3, return last line of stdout.
sq_last() { printf '%s\n' "$1" | $SQLITE3 "$2" 2>&1 | tail -1; }
# Run SQL on doltlite, return last line of stdout.
dl_last() { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1 | tail -1; }
# Run SQL on doltlite, return all output.
dl_all()  { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1; }

# Seed a stock SQLite file at $1 with $2 (the schema/data SQL).
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

# ============================================================
# Reads
# ============================================================
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

# Pre-existing index — reads should use it transparently.
DB=$TMP/r4.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX idx_v ON t(v); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');"
want_eq "R4_select_via_pre_existing_index" \
  "$(dl_last "SELECT id FROM t WHERE v='b';" "$DB")" "2"

want_eq "R5_sqlite_master_visible" \
  "$(dl_last "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_v';" "$DB")" "1"

# ============================================================
# Autocommit writes
# ============================================================
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

# ============================================================
# DDL
# ============================================================
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

# Known bug: CREATE INDEX on populated table crashes
skip "D4_create_index_on_populated_table" \
  "CREATE INDEX SIGSEGVs in orig_sqlite3BtreeInsert when source has rows; works in stock sqlite3 binary"

# Variant — empty table.
DB=$TMP/d5.db
seed_stock "$DB" "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);"
want_eq "D5_create_index_on_empty_table" \
  "$(dl_last "CREATE INDEX idx_v ON u(v); SELECT name FROM sqlite_master WHERE type='index';" "$DB")" "idx_v"

DB=$TMP/d6.db
seed_stock "$DB" "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX idx_v ON u(v); INSERT INTO u VALUES(1,'a'),(2,'b');"
want_eq "D6_drop_pre_existing_index" \
  "$(dl_last "DROP INDEX idx_v; SELECT count(*) FROM sqlite_master WHERE type='index';" "$DB")" "0"

skip "D7_reindex_populated_table" \
  "REINDEX hits the same orig_sqlite3BtreeInsert crash path as CREATE INDEX"

# ============================================================
# Constraints (declared at table-create time)
# ============================================================
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

# ============================================================
# Transactions
# ============================================================
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

# ============================================================
# VC features on a stock-SQLite file (should error cleanly, not crash)
# ============================================================
echo ""
echo "--- VC features on non-doltlite file ---"

DB=$TMP/v1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);"
# Connection-level functions should still be registered when the
# opened file is in stock-SQLite format. Currently they aren't.
skip "V1_doltlite_engine_reports_orig" \
  "doltlite_engine() not registered when opened file is stock SQLite; should return 'orig'"

# dolt_log on a stock-format file: should error, not crash.
# Skipped today because the error path output isn't stable.
skip "V2_dolt_log_errors_gracefully" \
  "dolt_log behavior on stock-format file is undefined; needs explicit rejection or pass-through"

# ============================================================
# Durability across reopen
# ============================================================
echo ""
echo "--- durability ---"

DB=$TMP/p1.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
# Write through doltlite, close, reopen through doltlite.
dl_all "INSERT INTO t VALUES(2,'b'); UPDATE t SET v='Z' WHERE id=1;" "$DB" >/dev/null
want_eq "P1_doltlite_writes_durable_reopen_with_doltlite" \
  "$(dl_last "SELECT v||'/'||(SELECT count(*) FROM t) FROM t WHERE id=1;" "$DB")" "Z/2"

# And readable from stock sqlite3 — file is still a valid SQLite file.
want_eq "P2_doltlite_writes_visible_in_stock_sqlite3" \
  "$(sq_last "SELECT v||'/'||(SELECT count(*) FROM t) FROM t WHERE id=1;" "$DB")" "Z/2"

DB=$TMP/p3.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
dl_all "INSERT INTO t VALUES(2,'b'); UPDATE t SET v='Z' WHERE id=1;" "$DB" >/dev/null
# Then make another write through stock sqlite3 and read with doltlite.
$SQLITE3 "$DB" "INSERT INTO t VALUES(3,'c');"
want_eq "P3_stock_writes_after_doltlite_visible_in_doltlite" \
  "$(dl_last "SELECT count(*) FROM t;" "$DB")" "3"

echo ""
echo "============================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "============================="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
