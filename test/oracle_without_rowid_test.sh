#!/bin/bash
#
# Oracle test: WITHOUT ROWID tables against stock SQLite.
#
# doltlite keys the prolly btree on the user PK for every table —
# not on sqlite's auto-allocated rowid — so a WITHOUT ROWID
# declaration is closer to a no-op here than in stock sqlite. That
# makes this oracle especially pointed at:
#
#   - the few SQL-level behaviors that WITHOUT ROWID genuinely
#     changes (no rowid column; INTEGER PRIMARY KEY is NOT a rowid
#     alias; AUTOINCREMENT is illegal; PK columns become NOT NULL
#     effectively via "PRIMARY KEY implies NOT NULL" in WITHOUT
#     ROWID — in plain tables it still allows NULL),
#   - composite PK shapes (WITHOUT ROWID was invented to support
#     these efficiently; the prolly btree key encoder has to match),
#   - TEXT / BLOB PKs with non-default collations,
#   - PK updates, where stock sqlite has to delete + re-insert the
#     row record while doltlite's prolly mutate does the same work
#     via a different code path,
#   - interactions with generated columns, triggers, FKs, and
#     savepoints that we've previously oracled on rowid tables.
#
# Oracle target is stock sqlite3 built from the same source tree so
# both engines parse identical SQL and the only variable is the
# storage layer.
#
# Usage: bash oracle_without_rowid_test.sh [doltlite] [sqlite3]
#

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  tr -d '\r' \
    | sed -e 's/[[:space:]]\{1,\}/ /g' -e 's/^ //' -e 's/ $//' \
          -e 's/^Runtime error /Error /' \
          -e 's/^Error: in prepare, / /' \
          -e 's/ ([0-9]*)$//'
}

oracle() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/sq"

  local dl_out
  dl_out=$(printf '%s\n' "$sql" | "$DOLTLITE" "$dir/dl/db" 2>&1 | normalize)

  local sq_out
  sq_out=$(printf '%s\n' "$sql" | "$SQLITE3" "$dir/sq/db" 2>&1 | normalize)

  if [ "$dl_out" = "$sq_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    sqlite3:";  echo "$sq_out" | sed 's/^/      /'
  fi
}

echo "=== Oracle Tests: WITHOUT ROWID ==="
echo ""

# ─── Basic round-trip ──────────────────────────────────────────────
echo "--- basic round-trip ---"

# Simple single-column PK.
oracle "int_pk_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a'), (2, 'b'), (3, 'c');
SELECT id, v FROM t ORDER BY id;
"

# TEXT PK.
oracle "text_pk_basic" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('apple', 1), ('banana', 2), ('cherry', 3);
SELECT k, v FROM t ORDER BY k;
"

# BLOB PK.
oracle "blob_pk_basic" "
CREATE TABLE t(k BLOB PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(x'01', 1), (x'02', 2), (x'0f', 3);
SELECT hex(k), v FROM t ORDER BY k;
"

# Ordering relies on the PK comparator — reverse-ordered INSERT,
# natural SELECT order.
oracle "int_pk_sorted_on_scan" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(3,30),(1,10),(2,20);
SELECT id, v FROM t;
"

# ─── rowid column absence ──────────────────────────────────────────
echo "--- rowid column absence ---"

# Referring to rowid in a WITHOUT ROWID table is an error in stock
# sqlite (rowid is not a column). Both engines should reject it.
oracle "select_rowid_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 10);
SELECT rowid FROM t;
"

oracle "select_oid_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 10);
SELECT oid FROM t;
"

# SELECT * must NOT include a rowid column for a WITHOUT ROWID table
# (it doesn't include one for plain tables either, but pin it here).
oracle "select_star_no_rowid_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT * FROM t ORDER BY id;
"

# ─── INTEGER PRIMARY KEY is NOT a rowid alias ──────────────────────
echo "--- integer PK semantics ---"

# In a WITHOUT ROWID table, INTEGER PRIMARY KEY is a regular
# integer-typed column. It is still UNIQUE (PK), but it does NOT
# alias rowid, which means INSERT does not auto-allocate a value,
# and AUTOINCREMENT is not allowed.
oracle "int_pk_without_rowid_no_autoalloc" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t(v) VALUES('a');
SELECT id, v FROM t;
"

# AUTOINCREMENT is illegal on WITHOUT ROWID.
oracle "autoincrement_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT) WITHOUT ROWID;
SELECT 1;
"

# ─── PRIMARY KEY implies NOT NULL ──────────────────────────────────
echo "--- PK implies NOT NULL ---"

# A quirk of stock sqlite: in a plain (rowid) table, a non-INTEGER
# PRIMARY KEY column allows NULL values. In a WITHOUT ROWID table
# the PK is strictly NOT NULL. Both engines should match.
oracle "text_pk_null_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(NULL, 1);
SELECT count(*) FROM t;
"

oracle "composite_pk_null_half_rejected" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, NULL, 10);
SELECT count(*) FROM t;
"

# ─── Composite PK ──────────────────────────────────────────────────
echo "--- composite PK ---"

oracle "composite_int_pk_round_trip" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, 1, 'a'),(1, 2, 'b'),(2, 1, 'c'),(2, 2, 'd');
SELECT a, b, v FROM t ORDER BY a, b;
"

oracle "composite_text_int_pk" "
CREATE TABLE t(region TEXT, code INT, v TEXT, PRIMARY KEY(region, code)) WITHOUT ROWID;
INSERT INTO t VALUES
  ('us', 1, 'a'), ('us', 2, 'b'),
  ('eu', 1, 'c'), ('eu', 2, 'd');
SELECT region, code, v FROM t ORDER BY region, code;
"

oracle "composite_pk_range_scan" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1,1,11),(1,2,12),(2,1,21),(2,2,22),(3,1,31);
SELECT a, b, v FROM t WHERE a = 2 ORDER BY b;
"

# Duplicate composite PK must be rejected.
oracle "composite_pk_dup_rejected" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, 1, 'a');
INSERT INTO t VALUES(1, 1, 'b');
SELECT a, b, v FROM t;
"

# ─── PK with collation ─────────────────────────────────────────────
echo "--- PK collation ---"

# NOCASE collation on a TEXT PK: 'Alice' and 'alice' collide.
oracle "text_pk_nocase" "
CREATE TABLE t(k TEXT PRIMARY KEY COLLATE NOCASE, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('Alice', 1);
INSERT INTO t VALUES('alice', 2);
SELECT k, v FROM t ORDER BY k;
"

# RTRIM collation.
oracle "text_pk_rtrim" "
CREATE TABLE t(k TEXT PRIMARY KEY COLLATE RTRIM, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('abc', 1);
INSERT INTO t VALUES('abc ', 2);
SELECT k, v FROM t;
"

# ─── UPDATE the PK ─────────────────────────────────────────────────
echo "--- UPDATE PK ---"

# UPDATE that changes the PK: row is effectively deleted + inserted
# under the new key. In the prolly btree this flows through the
# mutmap as an INSERT of the new key and a DELETE of the old.
oracle "update_pk_same_row" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a'),(2, 'b');
UPDATE t SET k = 99 WHERE k = 1;
SELECT k, v FROM t ORDER BY k;
"

# UPDATE composite PK half.
oracle "update_composite_pk_half" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, 1, 'a'),(1, 2, 'b'),(2, 1, 'c');
UPDATE t SET a = 10 WHERE a = 1 AND b = 1;
SELECT a, b, v FROM t ORDER BY a, b;
"

# UPDATE that would collide with an existing PK is rejected.
oracle "update_pk_to_existing_collides" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a'),(2, 'b');
UPDATE t SET k = 2 WHERE k = 1;
SELECT k, v FROM t ORDER BY k;
"

# ─── REPLACE / UPSERT ──────────────────────────────────────────────
echo "--- REPLACE / UPSERT ---"

oracle "replace_into_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a');
REPLACE INTO t VALUES(1, 'b');
SELECT k, v FROM t;
"

oracle "upsert_do_update_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(k) DO UPDATE SET v = excluded.v;
SELECT k, v FROM t;
"

oracle "upsert_do_nothing_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(k) DO NOTHING;
SELECT k, v FROM t;
"

# Composite UPSERT target.
oracle "composite_upsert_do_update" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, 1, 10);
INSERT INTO t VALUES(1, 1, 999)
  ON CONFLICT(a, b) DO UPDATE SET v = excluded.v;
SELECT a, b, v FROM t;
"

# ─── Secondary indexes ─────────────────────────────────────────────
echo "--- secondary indexes ---"

oracle "secondary_index_on_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, tag TEXT, v INT) WITHOUT ROWID;
CREATE INDEX idx_tag ON t(tag);
INSERT INTO t VALUES(1, 'a', 10),(2, 'b', 20),(3, 'a', 30);
SELECT k, v FROM t WHERE tag = 'a' ORDER BY k;
"

oracle "unique_secondary_index_on_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, u INT, v INT) WITHOUT ROWID;
CREATE UNIQUE INDEX idx_u ON t(u);
INSERT INTO t VALUES(1, 100, 'a');
INSERT INTO t VALUES(2, 100, 'b');
SELECT k, u FROM t ORDER BY k;
"

# ─── Aggregates, joins, ORDER BY ───────────────────────────────────
echo "--- aggregates / joins ---"

oracle "aggregate_over_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40);
SELECT count(*), sum(v), min(v), max(v) FROM t;
"

oracle "join_two_without_rowid_tables" "
CREATE TABLE a(k INT PRIMARY KEY, x INT) WITHOUT ROWID;
CREATE TABLE b(k INT PRIMARY KEY, y INT) WITHOUT ROWID;
INSERT INTO a VALUES(1, 10), (2, 20);
INSERT INTO b VALUES(1, 100), (2, 200);
SELECT a.k, a.x, b.y FROM a JOIN b USING (k) ORDER BY a.k;
"

# Join between a rowid table and a WITHOUT ROWID table.
oracle "join_mixed_rowid_and_without" "
CREATE TABLE a(k INT PRIMARY KEY, x INT);
CREATE TABLE b(k INT PRIMARY KEY, y INT) WITHOUT ROWID;
INSERT INTO a VALUES(1, 10),(2, 20);
INSERT INTO b VALUES(1, 100),(2, 200);
SELECT a.k, a.x, b.y FROM a JOIN b USING (k) ORDER BY a.k;
"

# ─── FK interactions ──────────────────────────────────────────────
echo "--- FK interactions ---"

# FK from a WITHOUT ROWID child to a WITHOUT ROWID parent.
oracle "fk_without_rowid_parent_and_child" "
PRAGMA foreign_keys = ON;
CREATE TABLE p(k INT PRIMARY KEY, name TEXT) WITHOUT ROWID;
CREATE TABLE c(id INT PRIMARY KEY,
  pk INT REFERENCES p(k) ON DELETE CASCADE) WITHOUT ROWID;
INSERT INTO p VALUES(1, 'a'), (2, 'b');
INSERT INTO c VALUES(10, 1), (11, 1), (12, 2);
DELETE FROM p WHERE k = 1;
SELECT k FROM p ORDER BY k;
SELECT id, pk FROM c ORDER BY id;
"

# Composite-PK parent with FK from a plain child.
oracle "fk_to_composite_pk_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE p(region TEXT, code INT, PRIMARY KEY(region, code)) WITHOUT ROWID;
CREATE TABLE c(id INT PRIMARY KEY, region TEXT, code INT,
  FOREIGN KEY(region, code) REFERENCES p(region, code) ON DELETE CASCADE);
INSERT INTO p VALUES('us', 1), ('us', 2);
INSERT INTO c VALUES(10, 'us', 1), (11, 'us', 1), (12, 'us', 2);
DELETE FROM p WHERE code = 1;
SELECT region, code FROM p;
SELECT id FROM c ORDER BY id;
"

# ─── Triggers ─────────────────────────────────────────────────────
echo "--- triggers ---"

oracle "trigger_before_insert_on_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, k INT, v INT);
CREATE TRIGGER bi BEFORE INSERT ON t BEGIN
  INSERT INTO log(k, v) VALUES(new.k, new.v);
END;
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT k, v FROM t ORDER BY k;
SELECT k, v FROM log ORDER BY id;
"

oracle "trigger_after_update_on_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, old_v INT, new_v INT);
CREATE TRIGGER au AFTER UPDATE ON t BEGIN
  INSERT INTO log(old_v, new_v) VALUES(old.v, new.v);
END;
INSERT INTO t VALUES(1, 10);
UPDATE t SET v = 99 WHERE k = 1;
SELECT old_v, new_v FROM log;
"

# ─── Generated columns ────────────────────────────────────────────
echo "--- generated columns ---"

oracle "stored_generated_on_without_rowid" "
CREATE TABLE t(
  k INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED
) WITHOUT ROWID;
INSERT INTO t(k, a) VALUES(1, 5), (2, 10);
SELECT k, a, doubled FROM t ORDER BY k;
"

oracle "virtual_generated_on_without_rowid" "
CREATE TABLE t(
  k INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL
) WITHOUT ROWID;
INSERT INTO t(k, a) VALUES(1, 5);
SELECT k, a, doubled FROM t;
"

# ─── Savepoint interaction ────────────────────────────────────────
echo "--- savepoint ---"

oracle "rollback_to_savepoint_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a');
SAVEPOINT s;
INSERT INTO t VALUES(2, 'b');
UPDATE t SET v = 'z' WHERE k = 1;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT k, v FROM t ORDER BY k;
"

# Nested savepoint: inner rollback should keep outer inserts.
oracle "nested_savepoint_without_rowid" "
CREATE TABLE t(k INT PRIMARY KEY, v INT) WITHOUT ROWID;
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT k, v FROM t ORDER BY k;
"

# ─── Bulk ─────────────────────────────────────────────────────────
echo "--- bulk ---"

make_inserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES($i, 'row-$i');"
  done
}

oracle "bulk_100_rows_int_pk" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
$(make_inserts 100)
SELECT count(*) FROM t;
SELECT k, v FROM t WHERE k = 50;
SELECT k, v FROM t WHERE k BETWEEN 95 AND 100 ORDER BY k;
"

make_composite_inserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES(1, $i, 'even-$i');"
    echo "INSERT INTO t VALUES(2, $i, 'odd-$i');"
  done
}

oracle "bulk_composite_pk" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;
$(make_composite_inserts 50)
SELECT count(*) FROM t;
SELECT v FROM t WHERE a = 2 AND b = 25;
SELECT count(*) FROM t WHERE a = 1;
"

# ─── Final report ───────────────────────────────────────────────────

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
