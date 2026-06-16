#!/bin/bash

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

echo "--- basic ---"

oracle "temp_table_create_and_query" "
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a'),(2, 'b');
SELECT id, v FROM t ORDER BY id;
"

oracle "temp_qualified_name" "
CREATE TABLE temp.t(id INT PRIMARY KEY, v TEXT);
INSERT INTO temp.t VALUES(1, 'a');
SELECT id, v FROM temp.t;
"

oracle "temporary_long_form" "
CREATE TEMPORARY TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
SELECT id, v FROM t;
"

oracle "temp_table_in_sqlite_temp_master" "
CREATE TEMP TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT name FROM sqlite_temp_master WHERE type='table' AND name='t';
"

echo "--- name collision with main ---"

oracle "temp_shadows_main_unqualified" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'temp');
SELECT id, v FROM t;
"

oracle "temp_vs_main_qualified" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'temp');
SELECT 'main', v FROM main.t;
SELECT 'temp', v FROM temp.t;
"

oracle "drop_temp_unshadows_main" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'main');
CREATE TEMP TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'temp');
DROP TABLE t;
SELECT id, v FROM t;
"

echo "--- cross-engine JOIN ---"

oracle "join_main_with_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
CREATE TEMP TABLE keep(id INT PRIMARY KEY);
INSERT INTO keep VALUES(1),(3);
SELECT t.id, t.v FROM t JOIN keep USING (id) ORDER BY t.id;
"

oracle "delete_from_main_by_temp_filter" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
CREATE TEMP TABLE drop_ids(id INT);
INSERT INTO drop_ids VALUES(2);
DELETE FROM t WHERE id IN (SELECT id FROM drop_ids);
SELECT id, v FROM t ORDER BY id;
"

oracle "insert_into_main_from_temp" "
CREATE TEMP TABLE src(id INT PRIMARY KEY, v INT);
INSERT INTO src VALUES(1,10),(2,20);
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t SELECT id, v * 100 FROM src;
SELECT id, v FROM t ORDER BY id;
"

oracle "insert_into_temp_from_main" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1),(2,2);
CREATE TEMP TABLE dst(id INT PRIMARY KEY, v INT);
INSERT INTO dst SELECT id, v * 10 FROM t;
SELECT id, v FROM dst ORDER BY id;
"

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

echo "--- TEMP triggers ---"

oracle "temp_trigger_on_main_writes_main" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TEMP TRIGGER bi AFTER INSERT ON t BEGIN
  INSERT INTO log(what) VALUES('ins:' || new.id);
END;
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT what FROM log ORDER BY id;
"

oracle "temp_trigger_writes_temp" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TEMP TABLE tlog(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TEMP TRIGGER bi AFTER INSERT ON t BEGIN
  INSERT INTO tlog(what) VALUES('ins:' || new.id);
END;
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT what FROM tlog ORDER BY id;
"

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

oracle "bulk_50_copy_temp_to_main" "
CREATE TEMP TABLE src(id INT PRIMARY KEY, v TEXT);
$(make_inserts 50 src)
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t SELECT id, v FROM src;
SELECT count(*) FROM t;
SELECT v FROM t WHERE id = 25;
"


echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
