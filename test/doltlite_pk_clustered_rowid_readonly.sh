#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite clustered PK rowid is read-only ==="
echo ""

DB=/tmp/test_doltlite_pk_clustered_rowid_readonly_$$.db
DB2=/tmp/test_doltlite_pk_clustered_rowid_readonly2_$$.db
rm -f "$DB" "$DB2"
trap 'rm -f "$DB" "$DB2"' EXIT

run_test "text_pk_select_rowid" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a', 1);
SELECT last_insert_rowid() = rowid FROM t;
SELECT v FROM t WHERE rowid = last_insert_rowid();
" "1
1" "$DB"

rm -f "$DB"
run_test_match "text_pk_insert_rowid_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t(rowid, k, v) VALUES(99, 'a', 1);
" "has no column named rowid" "$DB"

rm -f "$DB"
run_test_match "text_pk_update_rowid_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a', 1);
UPDATE t SET rowid = 5;
" "no such column: rowid" "$DB"

rm -f "$DB"
run_test_match "text_pk_insert_oid_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t(oid, k, v) VALUES(99, 'a', 1);
" "has no column named oid" "$DB"

rm -f "$DB"
run_test_match "text_pk_insert_underscore_rowid_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t(_rowid_, k, v) VALUES(99, 'a', 1);
" "has no column named _rowid_" "$DB"

rm -f "$DB" "$DB2"
auto_ins=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t(rowid, k, v) VALUES(99, 'a', 1);
" "$DB" | sed 's/near line [0-9]*: //')
wr_ins=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t(rowid, k, v) VALUES(99, 'a', 1);
" "$DB2" | sed 's/near line [0-9]*: //')
if [ "$auto_ins" = "$wr_ins" ]; then
  dltest_pass
else
  dltest_fail "text_pk_insert_error_matches_without_rowid" \
    "  auto: $auto_ins
  wr:   $wr_ins"
fi

rm -f "$DB" "$DB2"
auto_upd=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a', 1);
UPDATE t SET rowid = 5;
" "$DB" | sed 's/near line [0-9]*: //')
wr_upd=$(dltest_run_sql "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('a', 1);
UPDATE t SET rowid = 5;
" "$DB2" | sed 's/near line [0-9]*: //')
if [ "$auto_upd" = "$wr_upd" ]; then
  dltest_pass
else
  dltest_fail "text_pk_update_error_matches_without_rowid" \
    "  auto: $auto_upd
  wr:   $wr_upd"
fi

rm -f "$DB"
run_test_match "int_pk_insert_rowid_rejected" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t(rowid, v) VALUES(99, 1);
" "has no column named rowid" "$DB"

rm -f "$DB"
run_test_match "composite_pk_insert_rowid_rejected" "
CREATE TABLE t(a INT, b INT, PRIMARY KEY(a, b));
INSERT INTO t(rowid, a, b) VALUES(99, 1, 2);
" "has no column named rowid" "$DB"

rm -f "$DB"
run_test "temp_text_pk_insert_rowid" "
CREATE TEMP TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t(rowid, k, v) VALUES(99, 'a', 1);
SELECT rowid, k, v FROM t;
" "99|a|1" "$DB"

rm -f "$DB"
run_test "integer_pk_insert_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t(rowid, v) VALUES(5, 1);
SELECT id, v FROM t;
" "5|1" "$DB"

rm -f "$DB"
run_test "integer_pk_update_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
UPDATE t SET rowid = 9;
SELECT id, v FROM t;
" "9|1" "$DB"

rm -f "$DB"
run_test "keyless_insert_rowid" "
CREATE TABLE t(v INT);
INSERT INTO t(rowid, v) VALUES(7, 1);
SELECT rowid, v FROM t;
" "7|1" "$DB"

rm -f "$DB"
run_test_match "explicit_without_rowid_insert_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t(rowid, k, v) VALUES(99, 'a', 1);
" "has no column named rowid" "$DB"

rm -f "$DB"
run_test "text_pk_rowid_setup" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a', 1);
SELECT typeof(rowid), v FROM t;
" "integer|1" "$DB"
run_test "text_pk_select_rowid_survives_reopen" "
SELECT typeof(rowid), v FROM t;
" "integer|1" "$DB"
run_test_match "text_pk_insert_rowid_rejected_after_reopen" "
INSERT INTO t(rowid, k, v) VALUES(99, 'b', 2);
" "has no column named rowid" "$DB"

dltest_finish
