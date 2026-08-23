#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite clustered PK is NOT NULL like WITHOUT ROWID ==="
echo ""

DB=/tmp/test_doltlite_pk_clustered_notnull_$$.db
DB2=/tmp/test_doltlite_pk_clustered_notnull2_$$.db
rm -f "$DB" "$DB2"
trap 'rm -f "$DB" "$DB2"' EXIT

run_test "text_pk_pragma_notnull" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
SELECT name, \"notnull\", pk FROM pragma_table_info('t') ORDER BY cid;
" "k|1|1
v|0|0" "$DB"

rm -f "$DB"
run_test_match "text_pk_null_insert" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES(NULL, 1);
" "NOT NULL constraint failed: t.k" "$DB"

rm -f "$DB"
run_test "text_pk_insert_or_ignore_nulls" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT OR IGNORE INTO t VALUES(NULL, 1);
INSERT OR IGNORE INTO t VALUES(NULL, 2);
SELECT count(*) FROM t;
" "0" "$DB"

rm -f "$DB" "$DB2"
auto_pragma=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
SELECT name, \"notnull\", pk FROM pragma_table_info('t') ORDER BY cid;
" "$DB")
wr_pragma=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
SELECT name, \"notnull\", pk FROM pragma_table_info('t') ORDER BY cid;
" "$DB2")
if [ "$auto_pragma" = "$wr_pragma" ]; then
  dltest_pass
else
  dltest_fail "text_pk_pragma_matches_without_rowid" \
    "  auto: $auto_pragma
  wr:   $wr_pragma"
fi

rm -f "$DB" "$DB2"
auto_err=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES(NULL, 1);
" "$DB" | sed 's/near line [0-9]*: //')
wr_err=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(NULL, 1);
" "$DB2" | sed 's/near line [0-9]*: //')
if [ "$auto_err" = "$wr_err" ]; then
  dltest_pass
else
  dltest_fail "text_pk_error_matches_without_rowid" \
    "  auto: $auto_err
  wr:   $wr_err"
fi

rm -f "$DB"
run_test_match "int_pk_null_rejected" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t(v) VALUES(1);
" "NOT NULL constraint failed: t.id" "$DB"

rm -f "$DB"
run_test_match "integer_pk_desc_null_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY DESC, v INT);
INSERT INTO t(v) VALUES(1);
" "NOT NULL constraint failed: t.id" "$DB"

rm -f "$DB"
run_test "composite_pk_pragma_notnull" "
CREATE TABLE t(a INT, b INT, PRIMARY KEY(a, b));
SELECT name, \"notnull\", pk FROM pragma_table_info('t') ORDER BY cid;
" "a|1|1
b|1|2" "$DB"

rm -f "$DB"
run_test_match "composite_pk_null_rejected" "
CREATE TABLE t(a INT, b INT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1, NULL);
" "NOT NULL constraint failed: t.b" "$DB"

rm -f "$DB"
run_test "temp_text_pk_allows_null" "
CREATE TEMP TABLE t(k TEXT PRIMARY KEY, v INT);
SELECT name, \"notnull\" FROM pragma_table_info('t') WHERE name='k';
INSERT INTO t VALUES(NULL, 1);
SELECT quote(k), v FROM t;
" "k|0
NULL|1" "$DB"

rm -f "$DB"
run_test "integer_pk_is_rowid_alias" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT name, \"notnull\", pk FROM pragma_table_info('t') WHERE name='id';
INSERT INTO t(v) VALUES(1);
SELECT quote(id), v FROM t;
" "id|0|1
1|1" "$DB"

rm -f "$DB"
run_test "integer_table_pk_is_rowid_alias" "
CREATE TABLE t(id INTEGER, v INT, PRIMARY KEY(id));
SELECT name, \"notnull\", pk FROM pragma_table_info('t') WHERE name='id';
INSERT INTO t(v) VALUES(1);
SELECT quote(id), typeof(id), v FROM t;
" "id|0|1
1|integer|1" "$DB"

rm -f "$DB"
run_test_match "explicit_without_rowid_still_rejects" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(NULL, 1);
" "NOT NULL constraint failed: t.k" "$DB"

rm -f "$DB"
run_test "unique_non_pk_allows_nulls" "
CREATE TABLE t(k TEXT UNIQUE, v INT);
INSERT INTO t VALUES(NULL, 1);
INSERT INTO t VALUES(NULL, 2);
SELECT count(*) FROM t;
" "2" "$DB"

rm -f "$DB"
run_test "text_pk_notnull_setup" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
SELECT name, \"notnull\" FROM pragma_table_info('t') WHERE name='k';
" "k|1" "$DB"
run_test "text_pk_notnull_survives_reopen" "
SELECT name, \"notnull\" FROM pragma_table_info('t') WHERE name='k';
" "k|1" "$DB"
run_test_match "text_pk_null_insert_after_reopen" "
INSERT INTO t VALUES(NULL, 1);
" "NOT NULL constraint failed: t.k" "$DB"

rm -f "$DB"
run_test_match "text_pk_update_null_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a', 1);
UPDATE t SET k = NULL;
SELECT quote(k), v FROM t;
" "NOT NULL constraint failed: t.k" "$DB"

dltest_finish
