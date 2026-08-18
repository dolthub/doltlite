#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== Count index range: extended-numeric BETWEEN ==="
echo ""

# COUNT(*) WHERE k BETWEEN lo AND hi uses OP_CountIndexRange. The upper
# exclusive bound used to be a raw byte successor of the encoded sort
# key. An integer beyond ±2^53 is 18 bytes whose first 9 are the IEEE
# neighbor, so that successor sat above the neighbor and counted it.
#
#   lo  = -9007199254740995
#   eq  = -9007199254740994   (9-byte exact)
#   ext = -9007199254740993   (18-byte, same 9-byte prefix as eq)

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

dltest_finish
