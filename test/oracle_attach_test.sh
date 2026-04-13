#!/bin/bash
#
# Oracle test: ATTACH DATABASE and cross-engine queries against
# stock SQLite.
#
# doltlite's main database is a prolly btree while any ATTACH-ed
# database is opened by the original SQLite btree implementation
# (dispatched via pOrigBtree). Every SQLite API that walks all
# databases — savepoint ops, commit, schema-change flags, cursor
# tripping — has to correctly dispatch between the two engines for
# every attached db index. The last round of savepoint + ALTER
# RENAME work uncovered one such mis-dispatch (trip-all-cursors).
# This oracle is designed to surface more.
#
# Every scenario pre-seeds an in-memory aux db via `ATTACH DATABASE
# ':memory:' AS aux;` inside the same input, so both engines see
# the same attached state. The main db is prolly for doltlite and
# stock for sqlite3; the aux db is stock in both engines.
#
# Usage: bash oracle_attach_test.sh [doltlite] [sqlite3]
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

echo "=== Oracle Tests: ATTACH + cross-engine ==="
echo ""

# ─── Basic ATTACH + DETACH ─────────────────────────────────────────
echo "--- basic ATTACH/DETACH ---"

oracle "attach_create_read" "
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.t(id INT PRIMARY KEY, v TEXT);
INSERT INTO aux.t VALUES(1, 'a'),(2, 'b');
SELECT id, v FROM aux.t ORDER BY id;
"

oracle "attach_detach_forgets" "
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.t(id INT PRIMARY KEY);
INSERT INTO aux.t VALUES(1);
DETACH DATABASE aux;
SELECT 'still-ok';
"

# Query sqlite_master through an attached db.
oracle "attach_schema_list" "
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY);
CREATE TABLE aux.v(id INT PRIMARY KEY);
SELECT name FROM aux.sqlite_master WHERE type='table' ORDER BY name;
"

# DETACH rejects an in-use db (stock SQLite error; both engines
# should match — this tests that the attached btree is still alive
# enough to be identified).
oracle "detach_nonexistent_fails" "
DETACH DATABASE nosuch;
"

# ─── Same table name in main and attached ──────────────────────────
echo "--- name collision ---"

oracle "main_and_aux_same_name" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main-1');
INSERT INTO aux.t VALUES(1, 'aux-1');
SELECT 'main', id, v FROM main.t;
SELECT 'aux',  id, v FROM aux.t;
"

# Unqualified name resolves to main.
oracle "unqualified_resolves_main" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
INSERT INTO aux.t VALUES(2, 'aux');
SELECT id, v FROM t;
"

# ─── Cross-engine SELECT ───────────────────────────────────────────
echo "--- cross-engine SELECT ---"

# Simple SELECT joining main (prolly for doltlite) with aux (stock).
oracle "join_main_prolly_with_aux_stock" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main-1'),(2, 'main-2');
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, tag TEXT);
INSERT INTO aux.u VALUES(1, 'one'),(2, 'two');
SELECT t.id, t.v, u.tag FROM t JOIN aux.u u USING (id) ORDER BY t.id;
"

# Subquery that pulls from aux.
oracle "subquery_from_aux" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a'),(2, 'b'),(3, 'c');
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.keep(id INT);
INSERT INTO aux.keep VALUES(1),(3);
SELECT id, v FROM t WHERE id IN (SELECT id FROM aux.keep) ORDER BY id;
"

# UNION across engines.
oracle "union_main_and_aux" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v TEXT);
INSERT INTO aux.u VALUES(2, 'b');
SELECT id, v FROM t UNION ALL SELECT id, v FROM aux.u ORDER BY id;
"

# ─── Cross-engine INSERT / UPDATE / DELETE ─────────────────────────
echo "--- cross-engine DML ---"

# INSERT INTO aux SELECT FROM main. This is where the most
# interesting cross-engine interaction happens — the reader cursor
# is on the prolly tree, the writer cursor is on the stock btree.
oracle "insert_into_aux_select_from_main" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v INT);
INSERT INTO aux.u SELECT id, v * 2 FROM t;
SELECT id, v FROM aux.u ORDER BY id;
"

# Reverse direction: read from aux, write to main (the normal
# direction is most interesting, but pin this too).
oracle "insert_into_main_select_from_aux" "
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v INT);
INSERT INTO aux.u VALUES(1, 10),(2, 20);
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t SELECT id, v + 100 FROM aux.u;
SELECT id, v FROM t ORDER BY id;
"

# UPDATE main from a value pulled from aux.
oracle "update_main_with_aux_value" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 0);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.src(id INT PRIMARY KEY, v INT);
INSERT INTO aux.src VALUES(1, 42);
UPDATE t SET v = (SELECT v FROM aux.src WHERE id = 1) WHERE id = 1;
SELECT id, v FROM t;
"

# DELETE from main using aux as a filter.
oracle "delete_from_main_by_aux_filter" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1),(2, 2),(3, 3);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.drop_these(id INT);
INSERT INTO aux.drop_these VALUES(1),(3);
DELETE FROM t WHERE id IN (SELECT id FROM aux.drop_these);
SELECT id, v FROM t ORDER BY id;
"

# ─── Savepoint spanning engines ────────────────────────────────────
echo "--- savepoint spanning engines ---"

# SAVEPOINT followed by writes on both main and aux, then
# ROLLBACK TO. Both engines must revert both sides.
oracle "savepoint_rollback_spans_engines" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
INSERT INTO aux.u VALUES(10, 100);
SAVEPOINT s;
INSERT INTO t VALUES(2, 2);
INSERT INTO aux.u VALUES(20, 200);
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT 'main', id, v FROM t ORDER BY id;
SELECT 'aux',  id, v FROM aux.u ORDER BY id;
"

# RELEASE keeps both sides.
oracle "savepoint_release_keeps_both" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v INT);
SAVEPOINT s;
INSERT INTO t VALUES(1, 1);
INSERT INTO aux.u VALUES(10, 100);
RELEASE SAVEPOINT s;
SELECT 'main', id FROM t;
SELECT 'aux',  id FROM aux.u;
"

# BEGIN / COMMIT that touches both sides — verifies that atomic
# commit coordinates across engines.
oracle "txn_commit_spans_engines" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 1);
INSERT INTO aux.u VALUES(10, 100);
COMMIT;
SELECT 'main', id FROM t;
SELECT 'aux',  id FROM aux.u;
"

oracle "txn_rollback_spans_engines" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 1);
INSERT INTO aux.u VALUES(10, 100);
ROLLBACK;
SELECT count(*) FROM t;
SELECT count(*) FROM aux.u;
"

# ─── Triggers that touch both engines ──────────────────────────────
echo "--- cross-engine triggers ---"

# AFTER INSERT trigger on a main-db table that mirrors into an
# attached table. Stock SQLite has a restriction: triggers cannot
# reference tables in attached databases in their body (temp
# triggers can, but regular triggers cannot). Pin whichever
# behavior both engines produce so we catch drift.
oracle "trigger_references_aux_rejected" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.log(id INT PRIMARY KEY, v INT);
CREATE TRIGGER bi AFTER INSERT ON t BEGIN
  INSERT INTO aux.log VALUES(new.id, new.v);
END;
INSERT INTO t VALUES(1, 10);
SELECT 'main', id, v FROM t;
SELECT 'aux-log', id, v FROM aux.log;
"

# TEMP trigger is allowed to reference any db.
oracle "temp_trigger_references_aux_ok" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.log(id INT PRIMARY KEY, v INT);
CREATE TEMP TRIGGER bi AFTER INSERT ON t BEGIN
  INSERT INTO aux.log VALUES(new.id, new.v);
END;
INSERT INTO t VALUES(1, 10);
SELECT 'main', id, v FROM t;
SELECT 'aux-log', id, v FROM aux.log;
"

# ─── Schema introspection across engines ───────────────────────────
echo "--- schema introspection ---"

oracle "pragma_database_list_after_attach" "
ATTACH DATABASE ':memory:' AS aux;
SELECT name FROM pragma_database_list ORDER BY seq;
"

oracle "pragma_table_info_across_dbs" "
CREATE TABLE t(a INT, b TEXT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.t(a INT, c TEXT, d REAL);
SELECT 'main', name, type FROM pragma_table_info('t')
  WHERE schema='main' OR schema IS NULL ORDER BY cid;
SELECT 'aux',  name, type FROM pragma_table_info('t', 'aux') ORDER BY cid;
"

# ─── Large ATTACH workload ─────────────────────────────────────────
echo "--- bulk cross-engine ---"

make_cross_inserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES($i, 'v-$i');"
    echo "INSERT INTO aux.u VALUES($i, 'aux-$i');"
  done
}

oracle "bulk_50_rows_across_engines" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.u(id INT PRIMARY KEY, v TEXT);
$(make_cross_inserts 50)
SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM aux.u);
SELECT t.v, u.v FROM t JOIN aux.u u USING (id) WHERE t.id = 25;
"

# ─── Final report ───────────────────────────────────────────────────

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
