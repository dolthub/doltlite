#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== Count index range: extended-numeric BETWEEN ==="
echo ""

# CountIndexRange used a byte-successor upper bound; 18-byte ints beyond ±2^53 share a 9-byte IEEE prefix.
# lo=-9007199254740995 eq=-9007199254740994 (9-byte) ext=-9007199254740993 (18-byte prefix of eq)

DB=/tmp/test_count_numeric_range_$$.db
db_rm "$DB"

run_test "numeric_pk_between_lo_eq" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY);
   INSERT INTO t VALUES(-9007199254740995);
   INSERT INTO t VALUES(-9007199254740994);
   INSERT INTO t VALUES(-9007199254740993);
   SELECT count(*) FROM t WHERE k BETWEEN -9007199254740995 AND -9007199254740994;" \
  "2" \
  "$DB"

db_rm "$DB"
run_test "numeric_pk_between_eq_eq" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY);
   INSERT INTO t VALUES(-9007199254740995);
   INSERT INTO t VALUES(-9007199254740994);
   INSERT INTO t VALUES(-9007199254740993);
   SELECT count(*) FROM t WHERE k BETWEEN -9007199254740994 AND -9007199254740994;" \
  "1" \
  "$DB"

db_rm "$DB"
run_test "numeric_pk_between_matches_scan" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY);
   INSERT INTO t VALUES(-9007199254740995);
   INSERT INTO t VALUES(-9007199254740994);
   INSERT INTO t VALUES(-9007199254740993);
   SELECT count(*) FROM t WHERE k BETWEEN -9007199254740994 AND -9007199254740994;
   SELECT count(*) FROM (SELECT k FROM t WHERE k BETWEEN -9007199254740994 AND -9007199254740994);" \
  "1
1" \
  "$DB"

db_rm "$DB"
run_test "without_rowid_between_eq_eq" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY) WITHOUT ROWID;
   INSERT INTO t VALUES(-9007199254740995);
   INSERT INTO t VALUES(-9007199254740994);
   INSERT INTO t VALUES(-9007199254740993);
   SELECT count(*) FROM t WHERE k BETWEEN -9007199254740994 AND -9007199254740994;" \
  "1" \
  "$DB"

db_rm "$DB"
run_test "secondary_index_between_eq_eq" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER NOT NULL);
   CREATE INDEX t_k ON t(k);
   INSERT INTO t VALUES(1,-9007199254740995);
   INSERT INTO t VALUES(2,-9007199254740994);
   INSERT INTO t VALUES(3,-9007199254740993);
   SELECT count(*) FROM t WHERE k BETWEEN -9007199254740994 AND -9007199254740994;" \
  "1" \
  "$DB"

db_rm "$DB"
run_test "composite_pk_between_keeps_twins" \
  "CREATE TABLE t(k INTEGER, j TEXT, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740994, 'A');
   INSERT INTO t VALUES(-9007199254740994, 'B');
   INSERT INTO t VALUES(-9007199254740993, 'z');
   SELECT count(*) FROM t WHERE k BETWEEN -9007199254740994 AND -9007199254740994;" \
  "2" \
  "$DB"

db_rm "$DB"
run_test "positive_numeric_pk_between_eq_eq" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY);
   INSERT INTO t VALUES(9007199254740993);
   INSERT INTO t VALUES(9007199254740994);
   INSERT INTO t VALUES(9007199254740995);
   SELECT count(*) FROM t WHERE k BETWEEN 9007199254740994 AND 9007199254740994;" \
  "1" \
  "$DB"

echo ""
echo "=== Count index range: DESC column after the range column ==="
echo ""

db_rm "$DB"
run_test "index_desc_neighbor_between_keeps_upper_bound" \
  "CREATE TABLE cd(a, b);
   INSERT INTO cd VALUES(1,1),(2,2),(3,3),(NULL,4),(5,NULL),(1.5,6),('x',7);
   CREATE INDEX cd_ab ON cd(a, b DESC);
   SELECT count(*) FROM cd WHERE a BETWEEN 1 AND 3;
   SELECT count(*) FROM cd WHERE a BETWEEN 3 AND 3;
   SELECT count(*) FROM cd WHERE a BETWEEN 2 AND 3;
   SELECT count(*) FROM cd WHERE a BETWEEN 1 AND 1.5;" \
  "4
1
2
2" \
  "$DB"

db_rm "$DB"
run_test "index_desc_neighbor_between_matches_scan" \
  "CREATE TABLE cd(a, b);
   INSERT INTO cd VALUES(1,1),(2,2),(3,3),(NULL,4),(5,NULL),(1.5,6),('x',7);
   CREATE INDEX cd_ab ON cd(a, b DESC);
   SELECT count(*) FROM cd WHERE a BETWEEN 1 AND 3;
   SELECT count(*) FROM (SELECT a FROM cd WHERE a BETWEEN 1 AND 3);
   SELECT count(*) FROM cd WHERE a >= 1 AND a <= 3;" \
  "4
4
4" \
  "$DB"

db_rm "$DB"
run_test "index_asc_neighbor_between_control" \
  "CREATE TABLE cd(a, b);
   INSERT INTO cd VALUES(1,1),(2,2),(3,3),(NULL,4),(5,NULL),(1.5,6),('x',7);
   CREATE INDEX cd_ab ON cd(a, b);
   SELECT count(*) FROM cd WHERE a BETWEEN 1 AND 3;
   SELECT count(*) FROM cd WHERE a BETWEEN 3 AND 3;" \
  "4
1" \
  "$DB"

db_rm "$DB"
run_test "without_rowid_pk_desc_neighbor_between" \
  "CREATE TABLE w(k1, k2, PRIMARY KEY(k1, k2 DESC)) WITHOUT ROWID;
   INSERT INTO w VALUES(1,'a'),(1,'b'),(2,'a'),(3,'a'),(4,'a');
   SELECT count(*) FROM w WHERE k1 BETWEEN 1 AND 3;
   SELECT count(*) FROM w WHERE k1 BETWEEN 3 AND 3;" \
  "4
1" \
  "$DB"

db_rm "$DB"
run_test "text_index_desc_neighbor_between" \
  "CREATE TABLE tx(a TEXT, b);
   INSERT INTO tx VALUES('a',1),('b',1),('c',1),('c',2),('d',1);
   CREATE INDEX tx_ab ON tx(a, b DESC);
   SELECT count(*) FROM tx WHERE a BETWEEN 'a' AND 'c';" \
  "4" \
  "$DB"

db_rm "$DB"

dltest_finish
