#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Clean Tests ==="

DB=/tmp/test_clean_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'tracked'); SELECT dolt_commit('-Am','base'); CREATE TABLE u(id INTEGER PRIMARY KEY); CREATE TABLE v(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB" >/dev/null 2>&1

run_test "clean_named" "SELECT dolt_clean('u');" "0" "$DB"
run_test "clean_named_drops_target" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='u';" "0" "$DB"
run_test "clean_named_keeps_other_untracked" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='v';" "1" "$DB"
run_test "clean_named_keeps_tracked" "SELECT v FROM t;" "tracked" "$DB"
run_test "clean_all" "SELECT dolt_clean();" "0" "$DB"
run_test "clean_all_drops_untracked" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='v';" "0" "$DB"

echo "CREATE TABLE sqlitex(id INTEGER PRIMARY KEY); CREATE TABLE doltx(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB" >/dev/null 2>&1
run_test "clean_prefix_names" "SELECT dolt_clean();" "0" "$DB"
run_test "clean_prefix_names_dropped" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('sqlitex','doltx');" "0" "$DB"

echo "CREATE TABLE dry_run(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB" >/dev/null 2>&1
run_test "clean_dry_run" "SELECT dolt_clean('--dry-run');" "0" "$DB"
run_test "clean_dry_run_keeps_table" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dry_run';" "1" "$DB"
run_test "clean_named_dry_run" "SELECT dolt_clean('dry_run','--dry-run');" "0" "$DB"
run_test "clean_named_dry_run_keeps_table" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dry_run';" "1" "$DB"

run_test_match "clean_unknown" "SELECT dolt_clean('missing');" "table not found" "$DB"
run_test_match "clean_unknown_is_atomic" "SELECT dolt_clean('dry_run','missing');" "table not found" "$DB"
run_test "clean_unknown_keeps_valid_target" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dry_run';" "1" "$DB"
run_test_match "clean_null_rejected" "SELECT dolt_clean(NULL);" "table not found" "$DB"
run_test "clean_null_keeps_untracked" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dry_run';" "1" "$DB"
run_test_match "clean_unknown_option" "SELECT dolt_clean('--unknown');" "unknown option" "$DB"

echo "CREATE TABLE staged_new(id INTEGER PRIMARY KEY); SELECT dolt_add('staged_new'); CREATE TABLE unstaged_new(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB" >/dev/null 2>&1
run_test "clean_keeps_staged_new" "SELECT dolt_clean();" "0" "$DB"
run_test "clean_staged_new_exists" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='staged_new';" "1" "$DB"
run_test "clean_unstaged_new_gone" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='unstaged_new';" "0" "$DB"
run_test "clean_staged_state_unchanged" "SELECT staged || ':' || status FROM dolt_status WHERE table_name='staged_new';" "1:new table" "$DB"

echo "INSERT INTO t VALUES(2,'working');" | $DOLTLITE "$DB" >/dev/null 2>&1
run_test "clean_named_tracked" "SELECT dolt_clean('T');" "0" "$DB"
run_test "clean_named_tracked_keeps_changes" "SELECT group_concat(v, ',') FROM t ORDER BY id;" "tracked,working" "$DB"

DB2=/tmp/test_clean_rename_$$.db; rm -f "$DB2"
echo "CREATE TABLE old_name(id INTEGER PRIMARY KEY); INSERT INTO old_name VALUES(1); SELECT dolt_commit('-Am','base'); ALTER TABLE old_name RENAME TO new_name;" | $DOLTLITE "$DB2" >/dev/null 2>&1
run_test "clean_unstaged_rename" "SELECT dolt_clean();" "0" "$DB2"
run_test "clean_unstaged_rename_drops_new_name" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='new_name';" "0" "$DB2"
run_test "clean_unstaged_rename_reports_old_deleted" "SELECT table_name || ':' || status FROM dolt_status;" "old_name:deleted" "$DB2"

DB3=/tmp/test_clean_vtab_$$.db; rm -f "$DB3"
echo "CREATE VIRTUAL TABLE docs USING fts5(body); INSERT INTO docs VALUES('x');" | $DOLTLITE "$DB3" >/dev/null 2>&1
run_test "clean_untracked_vtab" "SELECT dolt_clean();" "0" "$DB3"
run_test "clean_untracked_vtab_shadows" "SELECT count(*) FROM sqlite_master WHERE name='docs' OR name GLOB 'docs_*';" "0" "$DB3"
echo "CREATE VIRTUAL TABLE tracked_docs USING fts5(body); INSERT INTO tracked_docs VALUES('x'); SELECT dolt_add('tracked_docs');" | $DOLTLITE "$DB3" >/dev/null 2>&1
run_test "clean_keeps_staged_vtab" "SELECT dolt_clean();" "0" "$DB3"
run_test "clean_keeps_staged_vtab_shadows" "SELECT count(*) FROM sqlite_master WHERE name='tracked_docs' OR name GLOB 'tracked_docs_*';" "6" "$DB3"

DB4=/tmp/test_clean_fresh_$$.db; rm -f "$DB4"
echo "CREATE TABLE fresh(id INTEGER PRIMARY KEY);" | $DOLTLITE "$DB4" >/dev/null 2>&1
run_test "clean_fresh_repo" "SELECT dolt_clean();" "0" "$DB4"
run_test "clean_fresh_repo_drops_table" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='fresh';" "0" "$DB4"

DB5=/tmp/test_clean_fk_$$.db; rm -f "$DB5"
echo "PRAGMA foreign_keys=ON; CREATE TABLE z_parent(id INTEGER PRIMARY KEY); CREATE TABLE a_child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES z_parent(id)); INSERT INTO z_parent VALUES(1); INSERT INTO a_child VALUES(1,1); SELECT dolt_clean();" | $DOLTLITE "$DB5" >/dev/null 2>&1
run_test "clean_untracked_fk_pair" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('a_child','z_parent');" "0" "$DB5"

DB6=/tmp/test_clean_fk_blocked_$$.db; rm -f "$DB6"
echo "PRAGMA foreign_keys=ON; CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id)); SELECT dolt_commit('-Am','base'); ALTER TABLE parent RENAME TO renamed_parent;" | $DOLTLITE "$DB6" >/dev/null 2>&1
run_test "clean_referenced_table_dry_run" "PRAGMA foreign_keys=ON; SELECT dolt_clean('--dry-run');" "0" "$DB6"
run_test "clean_referenced_table_dry_run_keeps_parent" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='renamed_parent';" "1" "$DB6"
run_test "clean_referenced_table" "PRAGMA foreign_keys=ON; SELECT dolt_clean();" "0" "$DB6"
run_test "clean_referenced_table_dropped" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='renamed_parent';" "0" "$DB6"
run_test "clean_referenced_table_reports_old_deleted" "SELECT table_name || ':' || status FROM dolt_status WHERE table_name='parent';" "parent:deleted" "$DB6"

DB7=/tmp/test_clean_tracked_fk_$$.db; rm -f "$DB7"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id)); SELECT dolt_commit('-Am','base');" | $DOLTLITE "$DB7" >/dev/null 2>&1
run_test "clean_with_only_tracked_fk_tables" "SELECT dolt_clean();" "0" "$DB7"
run_test "clean_with_only_tracked_fk_tables_keeps_both" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('child','parent');" "2" "$DB7"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7"
dltest_finish
