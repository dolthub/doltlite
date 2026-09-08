#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite reserved dolt_ prefix Tests ==="

DB=/tmp/test_reserved_$$.db
rm -f "$DB"

run_test "setup_repo" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES(1,'a');
   SELECT dolt_commit('-Am','init') IS NOT NULL;" \
  "1" "$DB"

run_test "system_log_still_readable" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB"

run_test_match "create_table_dolt_log_reserved" \
  "CREATE TABLE dolt_log(x);" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_table_dolt_status_case_reserved" \
  "CREATE TABLE DOLT_STATUS(x);" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_view_dolt_branches_reserved" \
  "CREATE VIEW dolt_branches AS SELECT 1 AS x;" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_vtable_dolt_conflicts_reserved" \
  "CREATE VIRTUAL TABLE dolt_conflicts USING fts5(a);" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_index_dolt_name_reserved" \
  "CREATE INDEX dolt_idx ON t(v);" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_trigger_dolt_name_reserved" \
  "CREATE TRIGGER dolt_trg AFTER INSERT ON t BEGIN SELECT 1; END;" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "rename_to_dolt_status_reserved" \
  "ALTER TABLE t RENAME TO dolt_status;" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test "rename_refused_keeps_system_status" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

run_test "log_unshadowed_after_refused_create" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB"

run_test_match "create_view_dolt_ignore_reserved" \
  "CREATE VIEW dolt_ignore AS SELECT 1 AS x;" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_vtable_dolt_docs_reserved" \
  "CREATE VIRTUAL TABLE dolt_docs USING fts5(a);" \
  "table names beginning with dolt_ are reserved for internal use" "$DB"

run_test_match "create_dolt_ignore_extra_col_shape" \
  "CREATE TABLE dolt_ignore(
      pattern TEXT NOT NULL,
      ignored TINYINT NOT NULL,
      extra INT,
      PRIMARY KEY(pattern));" \
  "dolt_ignore must have exactly two columns" "$DB"

run_test "create_dolt_ignore_shape_ok" \
  "CREATE TABLE dolt_ignore(
      pattern TEXT NOT NULL,
      ignored TINYINT NOT NULL,
      PRIMARY KEY(pattern));
   INSERT INTO dolt_ignore VALUES('tmp_*', 1);
   SELECT * FROM dolt_ignore;" \
  "tmp_*|1" "$DB"

run_test_match "add_column_dolt_ignore_refused" \
  "ALTER TABLE dolt_ignore ADD COLUMN extra INT;" \
  "table dolt_ignore may not be altered" "$DB"

run_test "add_column_refused_status_still_works" \
  "SELECT table_name, status FROM dolt_status WHERE table_name='dolt_ignore';" \
  "dolt_ignore|new table" "$DB"

run_test_match "rename_column_dolt_ignore_refused" \
  "ALTER TABLE dolt_ignore RENAME COLUMN ignored TO flagged;" \
  "table dolt_ignore may not be altered" "$DB"

run_test_match "drop_column_dolt_ignore_refused" \
  "ALTER TABLE dolt_ignore DROP COLUMN ignored;" \
  "table dolt_ignore may not be altered" "$DB"

run_test "drop_ignore_recovers_empty_module" \
  "DROP TABLE dolt_ignore;
   SELECT count(*) FROM dolt_ignore;" \
  "0" "$DB"

run_test "create_dolt_docs_shape_ok" \
  "CREATE TABLE dolt_docs(
      doc_name TEXT NOT NULL,
      doc_text TEXT NOT NULL,
      PRIMARY KEY(doc_name));
   INSERT INTO dolt_docs VALUES('README.md','hi');
   SELECT doc_name FROM dolt_docs;" \
  "README.md" "$DB"

run_test_match "add_column_dolt_docs_refused" \
  "ALTER TABLE dolt_docs ADD COLUMN extra INT;" \
  "may not be altered" "$DB"

run_test "drop_docs" \
  "DROP TABLE dolt_docs;
   SELECT count(*) FROM dolt_docs WHERE doc_name='AGENT.md';" \
  "1" "$DB"

run_test "lazy_ignore_materialize_still_works" \
  "INSERT INTO dolt_ignore VALUES('x_*', 1);
   SELECT pattern FROM dolt_ignore;" \
  "x_*" "$DB"

run_test "create_dolt_rebase_still_allowed" \
  "CREATE TABLE dolt_rebase(id INTEGER PRIMARY KEY);
   INSERT INTO dolt_rebase VALUES(1);
   SELECT id FROM dolt_rebase;" \
  "1" "$DB"

run_test_match "add_column_dolt_rebase_refused" \
  "ALTER TABLE dolt_rebase ADD COLUMN extra INT;" \
  "table dolt_rebase may not be altered" "$DB"

rm -f "$DB"
dltest_finish
