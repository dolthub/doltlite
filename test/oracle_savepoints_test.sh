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

echo "=== Oracle Tests: savepoints + rollbacks ==="
echo ""

echo "--- basic transactions ---"

oracle "txn_commit_visible" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_all" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
BEGIN;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_update" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
BEGIN;
UPDATE t SET v = v * 10;
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_delete" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
BEGIN;
DELETE FROM t WHERE id >= 2;
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_multi_table" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, v INT);
INSERT INTO a VALUES(1, 10);
INSERT INTO b VALUES(1, 100);
BEGIN;
INSERT INTO a VALUES(2, 20);
INSERT INTO b VALUES(2, 200);
UPDATE a SET v = 999 WHERE id = 1;
DELETE FROM b WHERE id = 1;
ROLLBACK;
SELECT 'a', id, v FROM a ORDER BY id;
SELECT 'b', id, v FROM b ORDER BY id;
"

echo "--- single savepoint ---"

oracle "savepoint_release_keeps_changes" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_to_reverts" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_then_reuse" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t VALUES(3, 30);
INSERT INTO t VALUES(4, 40);
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_update_restores" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
SAVEPOINT s1;
UPDATE t SET v = v + 1000;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_delete_restores" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
SAVEPOINT s1;
DELETE FROM t WHERE id >= 2;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_mixed_dml" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
SAVEPOINT s1;
INSERT INTO t VALUES(4, 40);
UPDATE t SET v = v + 100 WHERE id = 2;
DELETE FROM t WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

echo "--- nested savepoints ---"

oracle "nested_release_inner_only" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
RELEASE SAVEPOINT s2;
INSERT INTO t VALUES(3, 30);
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "nested_rollback_inner_only" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "nested_rollback_outer_discards_inner" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
RELEASE SAVEPOINT s2;
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "three_deep_rollback_to_middle" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
SAVEPOINT s3;
INSERT INTO t VALUES(3, 30);
INSERT INTO t VALUES(4, 40);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_same_name_shadowing" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, v FROM t ORDER BY id;
"

echo "--- multi-table savepoints ---"

oracle "savepoint_rollback_multi_table" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, v INT);
INSERT INTO a VALUES(1, 10);
INSERT INTO b VALUES(1, 100);
SAVEPOINT s1;
INSERT INTO a VALUES(2, 20);
INSERT INTO b VALUES(2, 200);
UPDATE a SET v = 999 WHERE id = 1;
DELETE FROM b WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT 'a', id, v FROM a ORDER BY id;
SELECT 'b', id, v FROM b ORDER BY id;
"

oracle "savepoint_three_tables_inner_rollback" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, v INT);
CREATE TABLE c(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO a VALUES(1, 1);
INSERT INTO b VALUES(1, 11);
SAVEPOINT s2;
INSERT INTO c VALUES(1, 111);
UPDATE a SET v = 999 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT 'a', id, v FROM a ORDER BY id;
SELECT 'b', id, v FROM b ORDER BY id;
SELECT 'c', id, v FROM c ORDER BY id;
"


echo "--- in-place mutation transitions ---"

oracle "rollback_undoes_insert_then_delete" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
DELETE FROM t WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

oracle "rollback_undoes_insert_delete_insert" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 999);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

oracle "rollback_undoes_delete_then_resurrect" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
DELETE FROM t WHERE id = 1;
SAVEPOINT s2;
INSERT INTO t VALUES(1, 999);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
SELECT 'after_rollback', count(*) FROM t;
RELEASE SAVEPOINT s1;
SELECT 'after_outer_release', id, v FROM t;
"

oracle "rollback_undoes_chained_updates" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
UPDATE t SET v = 100 WHERE id = 1;
UPDATE t SET v = 200 WHERE id = 1;
UPDATE t SET v = 300 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

oracle "rollback_undoes_upsert_overwrite" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 999) ON CONFLICT(id) DO UPDATE SET v = 999;
INSERT INTO t VALUES(2, 20) ON CONFLICT(id) DO UPDATE SET v = excluded.v;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "rollback_undoes_upsert_on_pending_insert" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(1, 999) ON CONFLICT(id) DO UPDATE SET v = 999;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

oracle "rollback_undoes_replace_into" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
REPLACE INTO t VALUES(1, 999);
REPLACE INTO t VALUES(2, 22);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "rollback_drops_insert_then_update_in_same_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
UPDATE t SET v = 999 WHERE id = 2;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "rollback_undoes_trigger_update_on_pending" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE other(id INT PRIMARY KEY, v INT);
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN
  UPDATE other SET v = v + 1000 WHERE id = new.id;
END;
SAVEPOINT s1;
INSERT INTO t     VALUES(1, 10);
INSERT INTO other VALUES(1, 100);
SAVEPOINT s2;
UPDATE t SET v = 99 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT 't', id, v FROM t ORDER BY id;
SELECT 'o', id, v FROM other ORDER BY id;
"

oracle "deep_nesting_rollback_to_outer" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
SAVEPOINT s1;
UPDATE t SET v = 10 WHERE id = 1;
SAVEPOINT s2;
UPDATE t SET v = 100 WHERE id = 1;
SAVEPOINT s3;
UPDATE t SET v = 1000 WHERE id = 1;
SAVEPOINT s4;
UPDATE t SET v = 10000 WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

oracle "nested_inserts_and_updates_rollback" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 1);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 2);
UPDATE t SET v = 11 WHERE id = 1;
SAVEPOINT s3;
INSERT INTO t VALUES(3, 3);
UPDATE t SET v = 22 WHERE id = 2;
UPDATE t SET v = 111 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"


oracle "rollback_undoes_alter_rename" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
ALTER TABLE t RENAME TO renamed_t;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
SELECT id, v FROM t;
"

oracle "rollback_undoes_alter_rename_column" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
ALTER TABLE t RENAME COLUMN v TO val;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT sql FROM sqlite_master WHERE type='table' AND name='t';
SELECT id, v FROM t;
"

oracle "rollback_undoes_create_index" "
CREATE TABLE t(id INT PRIMARY KEY, v INT, tag TEXT);
INSERT INTO t VALUES(1, 10, 'a'),(2, 20, 'b');
SAVEPOINT s1;
CREATE INDEX idx_t_tag ON t(tag);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t_tag';
SELECT id, v, tag FROM t ORDER BY id;
"

echo "--- savepoint + schema ---"

oracle "savepoint_rollback_create_table" "
CREATE TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(1);
SAVEPOINT s1;
CREATE TABLE u(id INT PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1, 'x');
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
SELECT id FROM t ORDER BY id;
"

oracle "savepoint_rollback_drop_table" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 10),(2, 20);
INSERT INTO u VALUES(1, 'a'),(2, 'b');
SAVEPOINT s1;
DROP TABLE u;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
SELECT id, v FROM u ORDER BY id;
"

oracle "savepoint_rollback_add_column" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20);
SAVEPOINT s1;
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra = 'filled' WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT sql FROM sqlite_master WHERE type='table' AND name='t';
SELECT id, v FROM t ORDER BY id;
"

echo "--- savepoint + triggers ---"

oracle "savepoint_rollback_reverts_trigger_writes" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT, v INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(new.id, new.v);
END;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT 't', id, v FROM t ORDER BY id;
SELECT 'log', id, v FROM log ORDER BY id;
"

oracle "trigger_raise_rollback_through_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TRIGGER no_neg BEFORE INSERT ON t WHEN new.v < 0 BEGIN
  SELECT RAISE(ROLLBACK, 'no neg');
END;
BEGIN;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, -1);
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

echo "--- bulk ---"

make_bulk_insert() {
  local n="$1" tbl="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO $tbl VALUES($i, 'row_$i');"
  done
}

BULK_100="$(make_bulk_insert 100 t)"
BULK_1K="$(make_bulk_insert 1000 t)"

oracle "savepoint_rollback_100_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0, 'seed');
SAVEPOINT s1;
$BULK_100
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_1000_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0, 'seed');
SAVEPOINT s1;
$BULK_1K
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT id FROM t;
"

oracle "savepoint_release_1000_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
SAVEPOINT s1;
$BULK_1K
RELEASE SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT id FROM t WHERE id IN (1, 250, 500, 750, 1000) ORDER BY id;
"

echo "--- savepoint inside BEGIN ---"

oracle "begin_savepoint_release_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
RELEASE SAVEPOINT s1;
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

oracle "begin_savepoint_rollback_to_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

oracle "begin_rollback_discards_everything" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
BEGIN;
INSERT INTO t VALUES(2, 20);
SAVEPOINT s1;
INSERT INTO t VALUES(3, 30);
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO t VALUES(4, 40);
RELEASE SAVEPOINT s2;
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
