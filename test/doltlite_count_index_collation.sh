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

dltest_finish
