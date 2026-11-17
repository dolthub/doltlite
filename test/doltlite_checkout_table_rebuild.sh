#!/bin/bash
# A table checkout that rebuilds the table's schema leaves other tables, the
# table's triggers and its AUTOINCREMENT counter alone, as Dolt does.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Table checkout rebuilds only the table ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_checkout_rebuild_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

seed() {  # seed <db> <fk action>
  "$DOLTLITE" "$1" "
PRAGMA foreign_keys=ON;
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v INT);
CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES t(id) $2);
CREATE TABLE log(n INT);
CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id); END;
INSERT INTO t(v) VALUES(1),(2),(3); DELETE FROM t WHERE id=3;
INSERT INTO c VALUES(10,1),(20,2);
DELETE FROM log;
SELECT dolt_add('.'); SELECT dolt_commit('-m','c1');" >/dev/null
}

for action in "ON DELETE CASCADE" "ON DELETE SET NULL" ""; do
  tag=$(echo "${action:-none}" | tr ' ' '_' | tr 'A-Z' 'a-z')
  DB="$ROOT/fk_$tag.db"
  seed "$DB" "$action"
  run_test "checkout_after_add_column_keeps_children_$tag" "
PRAGMA foreign_keys=ON;
ALTER TABLE t ADD COLUMN w INT;
SELECT dolt_checkout('t');
SELECT id, pid FROM c ORDER BY id;
SELECT table_name FROM dolt_status ORDER BY 1;
" "0
10|1
20|2" "$DB"
done

DB="$ROOT/trigger.db"
seed "$DB" "ON DELETE CASCADE"
run_test "checkout_after_add_column_keeps_trigger" "
ALTER TABLE t ADD COLUMN w INT;
SELECT dolt_checkout('t');
SELECT name FROM sqlite_schema WHERE type='trigger';
INSERT INTO t(v) VALUES(9);
SELECT n FROM log;
" "0
tr
4" "$DB"

DB="$ROOT/autoinc.db"
seed "$DB" "ON DELETE CASCADE"
run_test "checkout_after_add_column_keeps_autoinc_counter" "
ALTER TABLE t ADD COLUMN w INT;
SELECT dolt_checkout('t');
SELECT count(*) FROM dolt_status;
INSERT INTO t(v) VALUES(9);
SELECT max(id) FROM t;
" "0
0
4" "$DB"

DB="$ROOT/branch.db"
seed "$DB" "ON DELETE CASCADE"
"$DOLTLITE" "$DB" "SELECT dolt_checkout('-b','b1'); ALTER TABLE t ADD COLUMN w INT;
SELECT dolt_commit('-am','b1 widens t'); SELECT dolt_checkout('main');" >/dev/null
run_test "checkout_table_from_branch_keeps_children_and_trigger" "
PRAGMA foreign_keys=ON;
SELECT dolt_checkout('b1','t');
SELECT count(*) FROM c;
SELECT name FROM sqlite_schema WHERE type='trigger';
SELECT group_concat(name) FROM pragma_table_info('t');
" "0
2
tr
id,v,w" "$DB"

DB="$ROOT/renamed_index.db"
"$DOLTLITE" "$DB" "CREATE TABLE t(id INT PRIMARY KEY, a INT); CREATE INDEX ta ON t(a);
INSERT INTO t VALUES(1,1); SELECT dolt_commit('-Am','c1');
ALTER TABLE t RENAME TO t2;" >/dev/null
run_test_error_match "checkout_table_names_index_owner_after_rename" \
  "SELECT dolt_checkout('t');" \
  "cannot check out table t: its index ta is now an index of table t2; drop that index and retry" "$DB"
run_test "checkout_table_refused_leaves_rename_in_place" "
SELECT group_concat(name) FROM sqlite_schema WHERE type='table';
SELECT tbl_name FROM sqlite_schema WHERE name='ta';
" "t2
t2" "$DB"
run_test "checkout_table_after_dropping_clashing_index" "
DROP INDEX ta;
SELECT dolt_checkout('t');
SELECT tbl_name FROM sqlite_schema WHERE name='ta';
SELECT table_name || '|' || staged || '|' || status FROM dolt_status;
SELECT (SELECT count(*) FROM t) || '|' || (SELECT count(*) FROM t2);
" "0
t
t2|0|new table
1|1" "$DB"

for mode in single multiple; do
  DB="$ROOT/staged_drop_$mode.db"
  "$DOLTLITE" "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);
CREATE INDEX tv ON t(v);
CREATE TABLE u(id INTEGER PRIMARY KEY,v TEXT);
INSERT INTO t VALUES(1,'base');
INSERT INTO u VALUES(1,'base');
SELECT dolt_commit('-Am','base');
DROP TABLE t;
UPDATE u SET v='staged';
SELECT dolt_add('.');
UPDATE u SET v='working';" >/dev/null
  checkout="'t'"
  uvalue=working
  if [ "$mode" = multiple ]; then checkout="'t','u'"; uvalue=staged; fi
  staged=$("$DOLTLITE" "$DB" "SELECT dolt_hashof_db('STAGED');")
  run_test "checkout_staged_drop_$mode" "
SELECT dolt_checkout($checkout);
SELECT id,v FROM t;
SELECT id FROM t INDEXED BY tv WHERE v='base';
SELECT v FROM u;
SELECT table_name,staged,status FROM dolt_status WHERE table_name='t' ORDER BY staged;
SELECT dolt_hashof_db('STAGED')='$staged';
" "0
1|base
1
$uvalue
t|0|new table
t|1|deleted
1" "$DB"
done

dltest_finish
