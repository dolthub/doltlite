#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_schemas Scoped Operations ==="
echo ""

# Views and triggers have no catalog entry of their own. dolt_status reports
# them under the single name dolt_schemas, so the table-scoped commands have
# to accept that name and act on the whole set.

BASE="CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE lg(x INT);
CREATE VIEW v AS SELECT id FROM t;
CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES(NEW.id); END;
SELECT dolt_commit('-Am','base');"

seed() { # $1=db  $2=mutation
  rm -f "$1"
  printf '%s\n%s\n' "$BASE" "$2" | $DOLTLITE "$1" > /dev/null 2>&1
}

# --- checkout reverts the whole set, whatever the change was ---
for case_name in added_view dropped_view modified_trigger dropped_trigger unchanged; do
  case $case_name in
    added_view)       MUT="CREATE VIEW v2 AS SELECT 1;" ;;
    dropped_view)     MUT="DROP VIEW v;" ;;
    modified_trigger) MUT="DROP TRIGGER tr; CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES(99); END;" ;;
    dropped_trigger)  MUT="DROP TRIGGER tr;" ;;
    unchanged)        MUT="" ;;
  esac
  DBC=/tmp/test_schemas_co_${case_name}_$$.db
  seed "$DBC" "$MUT"
  echo "SELECT dolt_checkout('dolt_schemas');" | $DOLTLITE "$DBC" > /dev/null 2>&1
  run_test "checkout_restores_${case_name}" \
    "SELECT group_concat(type||':'||name ORDER BY type||':'||name) FROM sqlite_master WHERE type IN ('view','trigger');" \
    "trigger:tr,view:v" "$DBC"
  run_test "checkout_clears_status_${case_name}" \
    "SELECT count(*) FROM dolt_status;" \
    "0" "$DBC"
  rm -f "$DBC"
done

# --- add stages only the view/trigger set, not pending row changes ---
DB2=/tmp/test_schemas_add_$$.db
seed "$DB2" "DROP TRIGGER tr;
CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES(NEW.id*100); END;
INSERT INTO t VALUES(1);"
echo "SELECT dolt_add('dolt_schemas');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "add_stages_schemas" \
  "SELECT group_concat(table_name||'|'||staged) FROM (SELECT table_name,staged FROM dolt_status WHERE table_name='dolt_schemas');" \
  "dolt_schemas|1" "$DB2"
run_test "add_leaves_row_changes_unstaged" \
  "SELECT count(*) FROM dolt_status WHERE table_name='t' AND staged=1;" \
  "0" "$DB2"

echo "SELECT dolt_commit('-m','schemas only');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "commit_lands_staged_trigger" \
  "SELECT sql FROM sqlite_master WHERE name='tr';" \
  "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES(NEW.id*100); END" "$DB2"
run_test "commit_left_row_change_pending" \
  "SELECT count(*) FROM dolt_status WHERE table_name='t';" \
  "1" "$DB2"
rm -f "$DB2"

# --- reset unstages it again ---
DB3=/tmp/test_schemas_reset_$$.db
seed "$DB3" "DROP TRIGGER tr;
CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES(NEW.id*100); END;"
echo "SELECT dolt_add('dolt_schemas');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test "reset_precondition_staged" \
  "SELECT staged FROM dolt_status WHERE table_name='dolt_schemas';" \
  "1" "$DB3"
echo "SELECT dolt_reset('dolt_schemas');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test "reset_unstages_schemas" \
  "SELECT staged FROM dolt_status WHERE table_name='dolt_schemas';" \
  "0" "$DB3"
run_test "reset_keeps_working_copy" \
  "SELECT sql FROM sqlite_master WHERE name='tr';" \
  "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES(NEW.id*100); END" "$DB3"
rm -f "$DB3"

# A real table by that name cannot exist: the dolt_ prefix is reserved, so
# the name is unambiguous.
DB4=/tmp/test_schemas_reserved_$$.db
rm -f "$DB4"
run_test "dolt_schemas_name_is_reserved_for_tables" \
  "CREATE TABLE dolt_schemas(a INT);" \
  "Parse error near line 1: table names beginning with dolt_ are reserved for internal use" "$DB4"
rm -f "$DB4"

# A checkout that fails must leave the live schema exactly as it was: the
# schema pass rewrites objects name by name, so an unknown name has to be
# rejected before any of it runs.
DB5=/tmp/test_schemas_atomic_$$.db
seed "$DB5" "CREATE VIEW v2 AS SELECT 2;"
run_test "failed_checkout_names_the_missing_one" \
  "SELECT dolt_checkout('dolt_schemas','nosuchtable');" \
  "Error near line 1: no such branch or table: nosuchtable" "$DB5"
run_test "failed_checkout_keeps_working_schema" \
  "SELECT group_concat(type||':'||name ORDER BY type||':'||name) FROM sqlite_master WHERE type IN ('view','trigger');" \
  "trigger:tr,view:v,view:v2" "$DB5"
run_test "failed_checkout_keeps_status" \
  "SELECT count(*) FROM dolt_status;" \
  "1" "$DB5"
rm -f "$DB5"

# Same guarantee for an ordinary table, whose schema pass drops and recreates
# it the same way.
DB6=/tmp/test_schemas_atomic_table_$$.db
rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
SELECT dolt_commit('-Am','base');
ALTER TABLE t ADD COLUMN b INT;" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test "failed_table_checkout_keeps_added_column" \
  "SELECT dolt_checkout('t','nosuchtable');
   SELECT group_concat(name) FROM pragma_table_info('t');" \
  "Error near line 1: no such branch or table: nosuchtable
id,a,b" "$DB6"
rm -f "$DB6"

# Tables, views and indexes share one namespace. When a name moves from one
# kind of object to another, staging or unstaging only one side would commit
# two objects with that name, a schema that cannot be loaded.
collide() { # $1=case  $2=setup  $3=one-sided command  $4=refusal
  local db=/tmp/test_schemas_collide_$1_$$.db
  rm -f "$db"
  printf '%s\n' "$2" | $DOLTLITE "$db" > /dev/null 2>&1
  run_test "name_collision_${1}_refused" "$3" "Error near line 1: $4" "$db"
  run_test "name_collision_${1}_staged_together" \
    "SELECT dolt_add('.');
     SELECT dolt_commit('-m','both') IS NOT NULL;
     SELECT dolt_checkout('-b','b2');
     SELECT count(*)>0 FROM sqlite_master;
     PRAGMA integrity_check;" \
    "0
1
0
1
ok" "$db"
  run_test "name_collision_${1}_reopen" \
    "SELECT dolt_reset('--hard'); SELECT count(*) FROM dolt_status;" \
    "0
0" "$db"
  rm -f "$db"
}
TX="CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
CREATE TABLE x(id INTEGER PRIMARY KEY, a INT);
SELECT dolt_commit('-Am','c0');"
TV="CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
CREATE VIEW vv AS SELECT 1;
SELECT dolt_commit('-Am','c0');"
collide view_over_table "$TX
DROP TABLE x; CREATE VIEW x AS SELECT a FROM t;" \
  "SELECT dolt_add('dolt_schemas');" \
  "cannot stage view 'x': name conflicts with table 'x' in staged schema; stage both changes together"
collide index_over_view "$TV
DROP VIEW vv; CREATE INDEX vv ON t(a);" \
  "SELECT dolt_add('t');" \
  "cannot stage index 'vv': name conflicts with view 'vv' in staged schema; stage both changes together"
collide table_over_view "$TV
DROP VIEW vv; CREATE TABLE vv(q);" \
  "SELECT dolt_add('vv');" \
  "cannot stage table 'vv': name conflicts with view 'vv' in staged schema; stage both changes together"
collide view_over_renamed_table "$TX
ALTER TABLE x RENAME TO y; CREATE VIEW x AS SELECT a FROM t;" \
  "SELECT dolt_add('dolt_schemas');" \
  "cannot stage view 'x': name conflicts with table 'x' in staged schema; stage both changes together"
collide unstage_table_side "$TX
DROP TABLE x; CREATE VIEW x AS SELECT a FROM t; SELECT dolt_add('.');" \
  "SELECT dolt_reset('x');" \
  "cannot stage table 'x': name conflicts with view 'x' in staged schema; stage both changes together"
collide unstage_view_side "$TV
DROP VIEW vv; CREATE TABLE vv(q); SELECT dolt_add('.');" \
  "SELECT dolt_reset('dolt_schemas');" \
  "cannot stage view 'vv': name conflicts with table 'vv' in staged schema; stage both changes together"

dltest_finish
