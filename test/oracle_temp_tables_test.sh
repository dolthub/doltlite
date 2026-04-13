#!/bin/bash
#
# Oracle test: CREATE TEMP TABLE / CREATE TEMP TRIGGER / TEMP
# indices against stock SQLite.
#
# In both engines the TEMP database is a stock-SQLite btree opened
# behind pOrigBtree, sitting alongside the main prolly database.
# Temp tables exercise the subset of cross-engine paths where the
# "other" db is always the same implicit `temp` — most real bugs
# would be caught by the ATTACH oracle, but TEMP has its own
# parser-level path (CREATE TEMP ... without an explicit schema
# qualifier) and interacts with triggers, savepoints, and cursor
# walks that make it worth pinning separately.
#
# Usage: bash oracle_temp_tables_test.sh [doltlite] [sqlite3]
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

echo "=== Oracle Tests: CREATE TEMP TABLE / TRIGGER ==="
echo ""

# ─── Basic round-trip ─────────────────────────────────────────────
echo "--- basic ---"

oracle "temp_table_create_and_query" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a'),(2, 'b');
SELECT id, v FROM t ORDER BY id;
"

# Explicit schema qualifier.
oracle "temp_qualified_name" "
CREATE TABLE temp.t(id INT PRIMARY KEY, v TEXT);
INSERT INTO temp.t VALUES(1, 'a');
SELECT id, v FROM temp.t;
"

# CREATE TEMPORARY (long form) works the same as CREATE TEMP.
oracle "temporary_long_form" "
CREATE TEMPORARY TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
SELECT id, v FROM t;
"

# Temp table is listed in sqlite_temp_master (one of the shadow
# schemas). Both engines should expose it.
oracle "temp_table_in_sqlite_temp_master" "
CREATE TEMP TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT name FROM sqlite_temp_master WHERE type='table' AND name='t';
"

# ─── Name collision with main ─────────────────────────────────────
echo "--- name collision with main ---"

# A temp table shadows a main table with the same name: unqualified
# reference picks the TEMP one.
oracle "temp_shadows_main_unqualified" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'temp');
SELECT id, v FROM t;
"

# Explicit qualifiers disambiguate.
oracle "temp_vs_main_qualified" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'temp');
SELECT 'main', v FROM main.t;
SELECT 'temp', v FROM temp.t;
"

# DROPping the temp table un-shadows the main table.
oracle "drop_temp_unshadows_main" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'temp');
DROP TABLE t;
SELECT id, v FROM t;
"

# ─── Cross-engine JOIN ────────────────────────────────────────────
echo "--- cross-engine JOIN ---"

# JOIN between a prolly table in main and a temp table (stock).
oracle "join_main_with_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
CREATE TEMP TABLE keep(id INT PRIMARY KEY);
INSERT INTO keep VALUES(1),(3);
SELECT t.id, t.v FROM t JOIN keep USING (id) ORDER BY t.id;
"

# Subquery from temp feeding a main-db DELETE.
oracle "delete_from_main_by_temp_filter" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
CREATE TEMP TABLE drop_ids(id INT);
INSERT INTO drop_ids VALUES(2);
DELETE FROM t WHERE id IN (SELECT id FROM drop_ids);
SELECT id, v FROM t ORDER BY id;
"

# INSERT INTO main SELECT FROM temp.
oracle "insert_into_main_from_temp" "
CREATE TEMP TABLE src(id INT PRIMARY KEY, v INT);
INSERT INTO src VALUES(1,10),(2,20);
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t SELECT id, v * 100 FROM src;
SELECT id, v FROM t ORDER BY id;
"

# INSERT INTO temp SELECT FROM main.
oracle "insert_into_temp_from_main" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1),(2,2);
CREATE TEMP TABLE dst(id INT PRIMARY KEY, v INT);
INSERT INTO dst SELECT id, v * 10 FROM t;
SELECT id, v FROM dst ORDER BY id;
"

# ─── Savepoint spanning main + temp ───────────────────────────────
echo "--- savepoint spanning main + temp ---"

oracle "savepoint_rollback_spans_main_and_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TEMP TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
INSERT INTO u VALUES(10, 100);
SAVEPOINT s;
INSERT INTO t VALUES(2, 2);
INSERT INTO u VALUES(20, 200);
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT 'main', id FROM t ORDER BY id;
SELECT 'temp', id FROM u ORDER BY id;
"

oracle "txn_rollback_spans_main_and_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TEMP TABLE u(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 1);
INSERT INTO u VALUES(10, 100);
ROLLBACK;
SELECT count(*) FROM t;
SELECT count(*) FROM u;
"

oracle "txn_commit_spans_main_and_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TEMP TABLE u(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 1);
INSERT INTO u VALUES(10, 100);
COMMIT;
SELECT count(*) FROM t;
SELECT count(*) FROM u;
"

# ─── TEMP triggers ────────────────────────────────────────────────
echo "--- TEMP triggers ---"

# TEMP trigger on a main table, body writes to another main table.
oracle "temp_trigger_on_main_writes_main" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TEMP TRIGGER bi AFTER INSERT ON t BEGIN
  INSERT INTO log(what) VALUES('ins:' || new.id);
END;
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT what FROM log ORDER BY id;
"

# TEMP trigger on a main table, body writes to a TEMP table.
# This is the path where the trigger body spans engines: read
# from prolly, write to stock.
oracle "temp_trigger_writes_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TEMP TABLE tlog(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TEMP TRIGGER bi AFTER INSERT ON t BEGIN
  INSERT INTO tlog(what) VALUES('ins:' || new.id);
END;
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT what FROM tlog ORDER BY id;
"

# TEMP trigger that fires on UPDATE and reads old.
oracle "temp_trigger_on_update_reads_old" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TEMP TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT);
CREATE TEMP TRIGGER au AFTER UPDATE ON t BEGIN
  INSERT INTO log(msg) VALUES(old.v || '->' || new.v);
END;
INSERT INTO t VALUES(1, 10);
UPDATE t SET v = 99 WHERE id = 1;
SELECT msg FROM log;
"

# ─── Indexes on TEMP tables ───────────────────────────────────────
echo "--- indexes on TEMP tables ---"

oracle "temp_table_index_seek" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, tag TEXT);
CREATE INDEX idx_tag ON t(tag);
INSERT INTO t VALUES(1, 'a'),(2, 'b'),(3, 'a'),(4, 'c');
SELECT id FROM t WHERE tag = 'a' ORDER BY id;
"

oracle "temp_table_unique_index_rejects_dup" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, u INT);
CREATE UNIQUE INDEX idx_u ON t(u);
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(2, 100);
SELECT id, u FROM t ORDER BY id;
"

# ─── Constraints on TEMP tables ───────────────────────────────────
echo "--- TEMP constraints ---"

oracle "temp_check_constraint" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, -1);
SELECT id, v FROM t ORDER BY id;
"

oracle "temp_not_null_constraint" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t VALUES(1, NULL);
SELECT count(*) FROM t;
"

# ─── Bulk ─────────────────────────────────────────────────────────
echo "--- bulk ---"

make_inserts() {
  local n="$1" tbl="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO $tbl VALUES($i, 'v-$i');"
  done
}

oracle "bulk_50_rows_temp" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
$(make_inserts 50 t)
SELECT count(*) FROM t;
SELECT v FROM t WHERE id = 25;
"

# Bulk cross-engine: 50 rows into main and temp via a single
# statement (copy temp to main).
oracle "bulk_50_copy_temp_to_main" "
CREATE TEMP TABLE src(id INT PRIMARY KEY, v TEXT);
$(make_inserts 50 src)
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t SELECT id, v FROM src;
SELECT count(*) FROM t;
SELECT v FROM t WHERE id = 25;
"

# ─── Final report ───────────────────────────────────────────────────

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
