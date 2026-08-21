#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== Prefix-seek contract: bounds, tree+pending, extended numeric ==="
echo ""

# Prefix seek may land res<0; SeekGT/SeekLE walk matches only if eqSeen is set.
# An 18-byte neighbour sharing the first 9 bytes used to clear eqSeen.
# lo=-9007199254740995 eq=-9007199254740994 (9-byte) ext=-9007199254740993 (18-byte prefix of eq)

DB=/tmp/test_seek_prefix_contract_$$.db
db_rm "$DB"

PROBE="
SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k > -9007199254740994 ORDER BY k, j);
SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k >= -9007199254740994 ORDER BY k, j);
SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k < -9007199254740994 ORDER BY k, j);
SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k <= -9007199254740994 ORDER BY k, j);
SELECT coalesce(quote(min(k)),'N') FROM t WHERE k > -9007199254740994;
SELECT coalesce(quote(max(k)),'N') FROM t WHERE k <= -9007199254740994;"

BOUNDS="-9007199254740993/'z'
-9007199254740994/'A'|-9007199254740994/'B'|-9007199254740993/'z'
-9007199254740995/'lo'
-9007199254740995/'lo'|-9007199254740994/'A'|-9007199254740994/'B'
-9007199254740993
-9007199254740994"

run_test "composite_pk_pending_twin" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740995, 'lo', 0);
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994, 'B', 3);
   $PROBE" \
  "$BOUNDS" \
  "$DB"

db_rm "$DB"

run_test "without_rowid_pending_twin" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j)) WITHOUT ROWID;
   INSERT INTO t VALUES(-9007199254740995, 'lo', 0);
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994, 'B', 3);
   $PROBE" \
  "$BOUNDS" \
  "$DB"

db_rm "$DB"

run_test "desc_pk_pending_twin" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k DESC, j));
   INSERT INTO t VALUES(-9007199254740995, 'lo', 0);
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994, 'B', 3);
   $PROBE" \
  "$BOUNDS" \
  "$DB"

db_rm "$DB"

# ext pending, eq committed; the 18-byte key used to be invisible to a 9-byte bound.
run_test "pending_extended_neighbour" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740995, 'lo', 0);
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   $PROBE" \
  "-9007199254740993/'z'
-9007199254740994/'A'|-9007199254740993/'z'
-9007199254740995/'lo'
-9007199254740995/'lo'|-9007199254740994/'A'
-9007199254740993
-9007199254740994" \
  "$DB"

db_rm "$DB"

run_test "tombstone_exact_pending_twin" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740995, 'lo', 0);
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   DELETE FROM t WHERE k=-9007199254740994 AND j='A';
   INSERT INTO t VALUES(-9007199254740994, 'B', 3);
   $PROBE" \
  "-9007199254740993/'z'
-9007199254740994/'B'|-9007199254740993/'z'
-9007199254740995/'lo'
-9007199254740995/'lo'|-9007199254740994/'B'
-9007199254740993
-9007199254740994" \
  "$DB"

db_rm "$DB"

run_test "numeric_pk_pending_above" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, j TEXT);
   INSERT INTO t VALUES(-9007199254740995, 'lo');
   INSERT INTO t VALUES(-9007199254740994, 'A');
   INSERT INTO t VALUES(-9007199254740993, 'z');
   BEGIN;
   INSERT INTO t VALUES(-9007199254740992, 'hi');
   SELECT coalesce(group_concat(quote(k),'|'),'none') FROM (SELECT k FROM t WHERE k > -9007199254740994 ORDER BY k);
   SELECT coalesce(group_concat(quote(k),'|'),'none') FROM (SELECT k FROM t WHERE k >= -9007199254740994 ORDER BY k);
   SELECT coalesce(group_concat(quote(k),'|'),'none') FROM (SELECT k FROM t WHERE k < -9007199254740994 ORDER BY k);
   SELECT coalesce(group_concat(quote(k),'|'),'none') FROM (SELECT k FROM t WHERE k <= -9007199254740994 ORDER BY k);" \
  "-9007199254740993|-9007199254740992
-9007199254740994|-9007199254740993|-9007199254740992
-9007199254740995
-9007199254740995|-9007199254740994" \
  "$DB"

db_rm "$DB"

run_test "positive_extended_pending_twin" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(9007199254740993, 'z', 2);
   INSERT INTO t VALUES(9007199254740994, 'A', 1);
   INSERT INTO t VALUES(9007199254740995, 'hi', 0);
   BEGIN;
   INSERT INTO t VALUES(9007199254740994, 'B', 3);
   SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k > 9007199254740994 ORDER BY k, j);
   SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k >= 9007199254740994 ORDER BY k, j);
   SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k < 9007199254740994 ORDER BY k, j);
   SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT quote(k)||'/'||quote(j) AS q FROM t WHERE k <= 9007199254740994 ORDER BY k, j);" \
  "9007199254740995/'hi'
9007199254740994/'A'|9007199254740994/'B'|9007199254740995/'hi'
9007199254740993/'z'
9007199254740993/'z'|9007199254740994/'A'|9007199254740994/'B'" \
  "$DB"

db_rm "$DB"

run_test "range_update_skips_bound_twins" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740995, 'lo', 0);
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994, 'B', 3);
   UPDATE t SET a=99 WHERE k > -9007199254740994 AND k < -9007199254740992;
   SELECT changes();
   SELECT coalesce(group_concat(quote(k)||'/'||quote(j)||'='||quote(a),'|'),'none') FROM (SELECT k,j,a FROM t ORDER BY k,j);" \
  "1
-9007199254740995/'lo'=0|-9007199254740994/'A'=1|-9007199254740994/'B'=3|-9007199254740993/'z'=99" \
  "$DB"

db_rm "$DB"

# Enough pending twins that they cannot all sit in one leaf with ext.
run_test "many_pending_twins_still_exclusive" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994, 't00', 1);
   INSERT INTO t VALUES(-9007199254740994, 't01', 1);
   INSERT INTO t VALUES(-9007199254740994, 't02', 1);
   INSERT INTO t VALUES(-9007199254740994, 't03', 1);
   INSERT INTO t VALUES(-9007199254740994, 't04', 1);
   INSERT INTO t VALUES(-9007199254740994, 't05', 1);
   INSERT INTO t VALUES(-9007199254740994, 't06', 1);
   INSERT INTO t VALUES(-9007199254740994, 't07', 1);
   INSERT INTO t VALUES(-9007199254740994, 't08', 1);
   INSERT INTO t VALUES(-9007199254740994, 't09', 1);
   INSERT INTO t VALUES(-9007199254740994, 't10', 1);
   INSERT INTO t VALUES(-9007199254740994, 't11', 1);
   INSERT INTO t VALUES(-9007199254740994, 't12', 1);
   INSERT INTO t VALUES(-9007199254740994, 't13', 1);
   INSERT INTO t VALUES(-9007199254740994, 't14', 1);
   INSERT INTO t VALUES(-9007199254740994, 't15', 1);
   INSERT INTO t VALUES(-9007199254740994, 't16', 1);
   INSERT INTO t VALUES(-9007199254740994, 't17', 1);
   INSERT INTO t VALUES(-9007199254740994, 't18', 1);
   INSERT INTO t VALUES(-9007199254740994, 't19', 1);
   SELECT count(*) FROM t WHERE k > -9007199254740994;
   SELECT count(*) FROM t WHERE k = -9007199254740994;
   SELECT count(*) FROM t WHERE k <= -9007199254740994;
   SELECT coalesce(quote(min(k)),'N') FROM t WHERE k > -9007199254740994;" \
  "1
20
20
-9007199254740993" \
  "$DB"

db_rm "$DB"

dltest_finish
