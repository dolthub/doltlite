#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite clustered-PK rowid identity ==="
echo ""

DB=/tmp/test_doltlite_rowid_identity_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

run_test "int_pk_rowid_is_the_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'a');
SELECT last_insert_rowid();
SELECT rowid FROM t;
SELECT v FROM t WHERE rowid = last_insert_rowid();
" "10
10
a" "$DB"

rm -f "$DB"
run_test "text_pk_rowid_matches_last_insert" "
CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES('a','one');
SELECT last_insert_rowid() = rowid FROM t;
SELECT v FROM t WHERE rowid = last_insert_rowid();
" "1
one" "$DB"

rm -f "$DB"
run_test "text_pk_rowids_are_distinct" "
CREATE TABLE t(k TEXT PRIMARY KEY);
INSERT INTO t VALUES('a');
INSERT INTO t VALUES('b');
SELECT count(DISTINCT rowid) FROM t;
" "2" "$DB"

rm -f "$DB"
run_test_match "explicit_without_rowid_hides_rowid" "
CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES('a','one');
SELECT rowid FROM t;
" "no such column: rowid" "$DB"

rm -f "$DB"
run_test "integer_pk_unchanged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(v) VALUES('a');
INSERT INTO t(v) VALUES('b');
SELECT last_insert_rowid();
SELECT group_concat(rowid) FROM t;
" "2
1,2" "$DB"

rm -f "$DB"
run_test "keyless_rowid_unchanged" "
CREATE TABLE t(v TEXT);
INSERT INTO t VALUES('a');
SELECT last_insert_rowid();
SELECT rowid FROM t;
" "1
1" "$DB"

rm -f "$DB"
run_test "int_pk_zero_and_negative" "
CREATE TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT last_insert_rowid();
INSERT INTO t VALUES(-5);
SELECT last_insert_rowid();
SELECT group_concat(rowid) FROM t;
SELECT id FROM t WHERE rowid = 0;
" "0
-5
-5,0
0" "$DB"

rm -f "$DB"
run_test "int_pk_eq_still_finds_row" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'a');
SELECT v FROM t WHERE id=10;
" "a" "$DB"

rm -f "$DB"
run_test "composite_pk_rowid_is_not_first_col" "
CREATE TABLE t(id INT, k TEXT, PRIMARY KEY(id, k));
INSERT INTO t VALUES(10,'a');
SELECT last_insert_rowid() = 10;
SELECT last_insert_rowid() = rowid FROM t;
SELECT k FROM t WHERE rowid = last_insert_rowid();
" "0
1
a" "$DB"

rm -f "$DB"
run_test "text_pk_rowid_stable_setup" "
CREATE TABLE t(k TEXT PRIMARY KEY);
INSERT INTO t VALUES('a');
SELECT last_insert_rowid() = rowid FROM t;
" "1" "$DB"
TEXT_ROWID=$(dltest_run_sql "SELECT rowid FROM t;" "$DB")
if ! [[ "$TEXT_ROWID" =~ ^[0-9]+$ ]]; then
  dltest_fail "text_pk_rowid_stable_reopen" "  expected a numeric rowid, got: $TEXT_ROWID"
else
  run_test "text_pk_rowid_stable_reopen" "SELECT rowid FROM t;" "$TEXT_ROWID" "$DB"
fi

rm -f "$DB"
run_test "text_pk_rowid_via_secondary_index" "
CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES('a','one');
SELECT last_insert_rowid() = rowid FROM t WHERE v='one';
SELECT v FROM t WHERE rowid = last_insert_rowid();
" "1
one" "$DB"

dltest_finish
