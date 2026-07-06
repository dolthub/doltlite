#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== In-transaction seek visibility over mut-map edits ==="
echo ""

DB=/tmp/test_txn_seek_visibility_$$.db; db_rm "$DB"

run_test "intkey_range_seek_skips_deleted_finds_insert" \
  "CREATE TABLE d(k INTEGER PRIMARY KEY, v);
   INSERT INTO d VALUES (5, 'old');
   BEGIN;
   DELETE FROM d WHERE k = 5;
   INSERT INTO d VALUES (3, 'new');
   SELECT k FROM d WHERE k >= 0 ORDER BY k;
   SELECT k FROM d WHERE k <= 4 ORDER BY k DESC;
   SELECT max(k) FROM d WHERE k <= 4;
   ROLLBACK;" \
  "3
3
3" \
  "$DB"

db_rm "$DB"

run_test "intkey_truncate_reinsert_range_seeks" \
  "CREATE TABLE d(k INTEGER PRIMARY KEY, v);
   INSERT INTO d VALUES (5, 'old');
   BEGIN;
   DELETE FROM d;
   INSERT INTO d VALUES (3, 'new');
   SELECT k FROM d WHERE k >= 0 ORDER BY k;
   SELECT k FROM d WHERE k <= 4 ORDER BY k DESC;
   SELECT max(k) FROM d WHERE k BETWEEN 0 AND 10;
   SELECT max(k) FROM d WHERE k <= 4;
   ROLLBACK;" \
  "3
3
3
3" \
  "$DB"

db_rm "$DB"

run_test "intkey_uncommitted_insert_visible_to_range_seeks" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v);
   INSERT INTO t VALUES (10, 'old');
   BEGIN;
   INSERT INTO t VALUES (7, 'new');
   SELECT min(k) FROM t WHERE k >= 5;
   SELECT k FROM t WHERE k >= 5 ORDER BY k;
   SELECT max(k) FROM t WHERE k <= 8;
   ROLLBACK;" \
  "7
7
10
7" \
  "$DB"

db_rm "$DB"

run_test "intkey_seek_above_deleted_key" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v);
   INSERT INTO t VALUES (5, 'a'), (7, 'b');
   BEGIN;
   DELETE FROM t WHERE k = 5;
   SELECT min(k) FROM t WHERE k >= 5;
   ROLLBACK;" \
  "7" \
  "$DB"

db_rm "$DB"

run_test "composite_pk_prefix_max_over_masked_rows" \
  "CREATE TABLE e(a INTEGER, b INTEGER, PRIMARY KEY(a, b)) WITHOUT ROWID;
   INSERT INTO e VALUES (5, 1);
   BEGIN;
   DELETE FROM e;
   INSERT INTO e VALUES (3, 1);
   SELECT max(a) FROM e WHERE a BETWEEN 0 AND 10;
   SELECT max(b) FROM e WHERE a = 3;
   ROLLBACK;" \
  "3
1" \
  "$DB"

db_rm "$DB"

run_test "composite_pk_segdir_shape_max_reads" \
  "CREATE TABLE d(level INTEGER, idx INTEGER, val, PRIMARY KEY(level, idx));
   INSERT INTO d VALUES (1,0,'old');
   INSERT INTO d SELECT 0, value, 'old' FROM generate_series(0,15);
   INSERT INTO d VALUES (1025,0,'old');
   INSERT INTO d SELECT 1024, value, 'old' FROM generate_series(0,15);
   BEGIN;
   DELETE FROM d;
   INSERT INTO d SELECT 0, value, 'new' FROM generate_series(0,15);
   SELECT coalesce((SELECT max(idx) FROM d WHERE level = 1) + 1, 0);
   SELECT max(level) FROM d WHERE level BETWEEN 0 AND 1023;
   INSERT INTO d SELECT 1024, value, 'new' FROM generate_series(0,15);
   SELECT max(level) FROM d WHERE level BETWEEN 1024 AND 2047;
   SELECT coalesce((SELECT max(idx) FROM d WHERE level = 1025) + 1, 0);
   ROLLBACK;" \
  "0
0
1024
0" \
  "$DB"

db_rm "$DB"

run_test "ranged_delete_spares_rows_outside_range" \
  "CREATE TABLE d(a INTEGER, b INTEGER, c, PRIMARY KEY(a, b));
   INSERT INTO d VALUES (0,0,'x'),(0,1,'x'),(2,0,'x'),(2,1,'x');
   BEGIN;
   DELETE FROM d;
   INSERT INTO d VALUES (0,0,'x'),(0,1,'x'),(2,0,'x'),(2,1,'x');
   DELETE FROM d WHERE a = 0;
   INSERT INTO d VALUES (1, 0, 'x');
   DELETE FROM d WHERE a = 2;
   SELECT a, b FROM d;
   ROLLBACK;" \
  "1|0" \
  "$DB"

db_rm "$DB"

run_test "no_phantom_row_after_all_rows_deleted" \
  "CREATE TABLE d(k INTEGER PRIMARY KEY, v);
   INSERT INTO d VALUES (5, 'x');
   BEGIN;
   DELETE FROM d WHERE k = 5;
   SELECT count(*) FROM d WHERE k >= 0;
   SELECT coalesce(max(k), 'none') FROM d WHERE k >= 0;
   ROLLBACK;" \
  "0
none" \
  "$DB"

db_rm "$DB"

run_test "prefix_seek_sees_updated_row_value" \
  "CREATE TABLE d(a INTEGER, b INTEGER, v, PRIMARY KEY(a,b));
   INSERT INTO d VALUES (1,0,'old'),(1,1,'old2'),(2,0,'oldx');
   BEGIN;
   UPDATE d SET v='new' WHERE a=1 AND b=0;
   SELECT b, v FROM d WHERE a=1;
   ROLLBACK;" \
  "0|new
1|old2" \
  "$DB"

db_rm "$DB"

run_test "fts4_langid_rebuild_preserves_partitions" \
  "CREATE VIRTUAL TABLE t2 USING fts4(x, languageid=l);
   INSERT INTO t2(docid, x, l) SELECT value, 'w' || value || ' common', value % 2 FROM generate_series(0,39);
   INSERT INTO t2(t2) VALUES('rebuild');
   SELECT count(*) FROM t2 WHERE t2 MATCH 'common' AND l = 0;
   SELECT count(*) FROM t2 WHERE t2 MATCH 'common' AND l = 1;" \
  "20
20" \
  "$DB"

db_rm "$DB"

dltest_finish
