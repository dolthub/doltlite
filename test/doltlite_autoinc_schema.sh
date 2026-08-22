#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite AUTOINCREMENT is per schema, not per table name ==="
echo ""

DB=/tmp/test_doltlite_autoinc_schema_$$.db
OTHER=/tmp/test_doltlite_autoinc_schema_other_$$.db
rm -f "$DB" "$OTHER"
trap 'rm -f "$DB" "$OTHER"' EXIT

run_test "temp_same_name_starts_at_1" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
INSERT INTO t DEFAULT VALUES;
CREATE TEMP TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO temp.t DEFAULT VALUES;
SELECT group_concat(id) FROM main.t;
SELECT id FROM temp.t;
" "1,2
1" "$DB"

rm -f "$DB"
run_test "rename_temp_does_not_steal_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
CREATE TEMP TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO temp.u DEFAULT VALUES;
ALTER TABLE temp.u RENAME TO t;
INSERT INTO temp.t DEFAULT VALUES;
INSERT INTO main.t DEFAULT VALUES;
SELECT group_concat(id) FROM main.t;
SELECT group_concat(id) FROM temp.t;
" "1,2
1,2" "$DB"

rm -f "$DB"
run_test "drop_temp_does_not_reset_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
INSERT INTO t DEFAULT VALUES;
CREATE TEMP TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO temp.t DEFAULT VALUES;
DROP TABLE temp.t;
INSERT INTO main.t DEFAULT VALUES;
SELECT group_concat(id) FROM main.t;
" "1,2,3" "$DB"

rm -f "$DB"
run_test "drop_recreate_main_starts_at_1" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
INSERT INTO t DEFAULT VALUES;
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
SELECT id FROM t;
" "1" "$DB"

rm -f "$DB"
run_test "rename_main_continues_sequence" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
ALTER TABLE t RENAME TO u;
INSERT INTO u DEFAULT VALUES;
SELECT group_concat(id) FROM u;
" "1,2" "$DB"

rm -f "$DB" "$OTHER"
printf '%s\n' "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);" \
  | "$DOLTLITE" "$OTHER" >/dev/null
run_test "attached_same_name_starts_at_1" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t DEFAULT VALUES;
INSERT INTO t DEFAULT VALUES;
ATTACH '$OTHER' AS o;
INSERT INTO o.t DEFAULT VALUES;
SELECT group_concat(id) FROM main.t;
SELECT id FROM o.t;
" "1,2
1" "$DB"

dltest_finish
