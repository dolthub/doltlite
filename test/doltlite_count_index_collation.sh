#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== COUNT(*) BETWEEN honors collation and INDEXED BY ==="
echo ""

DB=/tmp/test_doltlite_count_index_collation_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

run_test "nocase_index_binary_column" "
CREATE TABLE t(k TEXT);
CREATE INDEX t_nocase ON t(k COLLATE NOCASE);
INSERT INTO t VALUES('A');
SELECT count(*) FROM t WHERE k BETWEEN 'a' AND 'z';
SELECT count(*) FROM (SELECT k FROM t WHERE k BETWEEN 'a' AND 'z');
SELECT count(*) FROM t NOT INDEXED WHERE k BETWEEN 'a' AND 'z';
" "0
0
0" "$DB"

rm -f "$DB"
run_test "indexed_by_binary_skips_nocase" "
CREATE TABLE t(k TEXT);
CREATE INDEX t_nocase ON t(k COLLATE NOCASE);
CREATE INDEX t_bin ON t(k);
INSERT INTO t VALUES('A');
SELECT count(*) FROM t INDEXED BY t_bin WHERE k BETWEEN 'a' AND 'z';
" "0" "$DB"

rm -f "$DB"
run_test "binary_index_on_nocase_column" "
CREATE TABLE t(k TEXT COLLATE NOCASE);
CREATE INDEX t_bin ON t(k COLLATE BINARY);
INSERT INTO t VALUES('A'),('b');
SELECT count(*) FROM t WHERE k BETWEEN 'a' AND 'z';
" "2" "$DB"

rm -f "$DB"
run_test "integer_between_still_counts" "
CREATE TABLE t(v INT);
CREATE INDEX t_v ON t(v);
INSERT INTO t VALUES(1),(2),(3),(10);
SELECT count(*) FROM t WHERE v BETWEEN 1 AND 3;
" "3" "$DB"

rm -f "$DB"
run_test_match "integer_between_uses_covering_index" "
CREATE TABLE t(v INT);
CREATE INDEX t_v ON t(v);
INSERT INTO t VALUES(1),(2),(3);
EXPLAIN QUERY PLAN SELECT count(*) FROM t WHERE v BETWEEN 1 AND 3;
" "SEARCH t USING COVERING INDEX t_v" "$DB"

rm -f "$DB"
run_test "numeric_bounds_on_text_or_untyped_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, z);
CREATE INDEX tx ON t(x); CREATE INDEX tz ON t(z);
INSERT INTO t VALUES(1,'10','10'),(2,'9',9),(3,'5',5.0),(4,'20','20');
CREATE TABLE k(a TEXT PRIMARY KEY); INSERT INTO k VALUES('-3'),('0'),('4'),('7');
SELECT count(*) FROM t WHERE x BETWEEN CAST(5 AS INTEGER) AND CAST(20 AS INTEGER);
SELECT count(*) FROM t WHERE z BETWEEN CAST(5 AS INTEGER) AND CAST(20 AS INTEGER);
SELECT count(*) FROM k WHERE a BETWEEN CAST(-5 AS INTEGER) AND CAST(5 AS INTEGER);
SELECT count(*) FROM t WHERE x BETWEEN '10' AND '5';
" "4
4
3
3" "$DB"

rm -f "$DB"
run_test "nocase_keys_with_nul" "
CREATE TABLE u(id INTEGER PRIMARY KEY, n TEXT COLLATE NOCASE); CREATE INDEX un ON u(n);
INSERT INTO u VALUES(1,'a'||char(0)||'z'),(2,'A'||char(0)||'b'),(3,'a'),(4,'b');
SELECT count(*) FROM u WHERE n BETWEEN 'a'||char(0)||'a' AND 'a'||char(0)||'c';
SELECT count(*) FROM u WHERE n BETWEEN 'A' AND 'b';
" "2
4" "$DB"

rm -f "$DB"
run_test_match "text_bounds_on_text_index_use_covering_index" "
CREATE TABLE t(x TEXT);
CREATE INDEX tx ON t(x);
INSERT INTO t VALUES('a'),('b');
EXPLAIN QUERY PLAN SELECT count(*) FROM t WHERE x BETWEEN 'a' AND 'b';
" "SEARCH t USING COVERING INDEX tx" "$DB"

dltest_finish
