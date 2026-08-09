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

# A deferred merged seek that lands on a row present in both the tree and
# the pending mut map must serve the pending value: the mut-map entry
# shadows the committed row.
run_test "deferred_seek_lands_on_shadowed_row_reads_pending_value" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (12,'old'),(27,'far');
   BEGIN;
   UPDATE t SET v='new' WHERE id=12;
   SELECT v FROM t WHERE id>5 AND id<20;
   SELECT v FROM t WHERE id<20 ORDER BY id DESC LIMIT 1;
   ROLLBACK;" \
  "new
new" \
  "$DB"

db_rm "$DB"

run_test "second_range_update_sees_first_updates_value" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
   INSERT INTO t VALUES (12,120),(27,270);
   BEGIN;
   UPDATE t SET v=v+1 WHERE id>=6 AND id<=15;
   UPDATE t SET v=v+1 WHERE id>=10 AND id<=17;
   COMMIT;
   SELECT id||'='||v FROM t;" \
  "12=122
27=270" \
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

# A join of three or more tables emits OP_IfEmpty for the third and later
# loops, which breaks out of the whole join when the table is empty. Answering
# that from the persisted root alone dropped rows written earlier in the same
# transaction, because they are still in the pending map.
DB=/tmp/test_txn_ifempty_$$.db; db_rm "$DB"

run_test "join_sees_table_populated_only_in_this_txn" \
  "CREATE TABLE t1(a INTEGER PRIMARY KEY);
   CREATE TABLE t2(a INTEGER PRIMARY KEY);
   CREATE TABLE t3(a INTEGER PRIMARY KEY);
   INSERT INTO t1 VALUES (1);
   INSERT INTO t2 VALUES (1);
   BEGIN;
   INSERT INTO t3 VALUES (1);
   SELECT count(*) FROM t1, t2, t3;
   SELECT count(*) FROM t3, t1, t2;
   COMMIT;
   SELECT count(*) FROM t1, t2, t3;" \
  "1
1
1" \
  "$DB"

db_rm "$DB"

# The optimization must still fire for a table that really is empty, and a
# table emptied inside the transaction must still join to nothing.
DB=/tmp/test_txn_ifempty_neg_$$.db; db_rm "$DB"

run_test "join_still_empty_for_empty_and_emptied_tables" \
  "CREATE TABLE t1(a INTEGER PRIMARY KEY);
   CREATE TABLE t2(a INTEGER PRIMARY KEY);
   CREATE TABLE e(a INTEGER PRIMARY KEY);
   CREATE TABLE d(a INTEGER PRIMARY KEY);
   INSERT INTO t1 VALUES (1);
   INSERT INTO t2 VALUES (1);
   INSERT INTO d VALUES (1);
   SELECT count(*) FROM t1, t2, e;
   BEGIN;
   DELETE FROM d;
   SELECT count(*) FROM t1, t2, d;
   INSERT INTO e VALUES (9);
   SELECT count(*) FROM t1, t2, e;
   ROLLBACK;
   SELECT count(*) FROM t1, t2, e;" \
  "0
0
1
0" \
  "$DB"

db_rm "$DB"

# A one-pass DELETE/UPDATE scan steps off each row it just wrote. When the
# departed row was mut-map-sourced, the step must resume from that row's key:
# the tree cursor is parked at the next committed row, and a step seeded from
# it skips every pending row in between.
DB=/tmp/test_txn_onepass_resume_$$.db; db_rm "$DB"

run_test "onepass_delete_covers_pending_rows" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (10,'ten'),(20,'twenty');
   BEGIN;
   INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
   DELETE FROM t WHERE id < 3;
   COMMIT;
   SELECT group_concat(id) FROM t;" \
  "3,4,5,10,20" \
  "$DB"

db_rm "$DB"

run_test "onepass_update_covers_pending_rows" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (10,'ten'),(20,'twenty');
   BEGIN;
   INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');
   UPDATE t SET v='UPD' WHERE id < 4;
   COMMIT;
   SELECT group_concat(id||'='||v) FROM t;" \
  "1=UPD,2=UPD,3=UPD,10=ten,20=twenty" \
  "$DB"

db_rm "$DB"

# The step lands merged: pending rows interleave committed ones, and a
# pending update shadowing a committed row must survive as the merged value.
run_test "onepass_update_resume_lands_merged" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
   INSERT INTO t VALUES (13,130),(27,270);
   BEGIN;
   INSERT INTO t VALUES (10,1000);
   UPDATE t SET v=v+1 WHERE id BETWEEN 7 AND 14;
   COMMIT;
   SELECT group_concat(id||'='||v) FROM t;" \
  "10=1001,13=131,27=270" \
  "$DB"

db_rm "$DB"

# A scan whose moveto lands on a pending row defers the tree-side seek. When
# a deferred write then deactivates the merge state, the resume re-seeds both
# sides past the departed key; a surviving deferral would re-seek the tree
# back to that key on the following step, skipping pending rows or
# resurrecting delete-masked ones.
DB=/tmp/test_txn_onepass_deferred_$$.db; db_rm "$DB"

run_test "onepass_update_from_deferred_seek_landing" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (5,'a'),(8,'skip'),(13,'a'),(27,'a');
   BEGIN;
   INSERT INTO t VALUES (10,'a');
   UPDATE t SET v='a2' WHERE id=5;
   UPDATE t SET v=v||'x' WHERE id>=5 AND v<>'skip';
   COMMIT;
   SELECT group_concat(id||'='||v) FROM t;" \
  "5=a2x,8=skip,10=ax,13=ax,27=ax" \
  "$DB"

db_rm "$DB"

run_test "onepass_delete_from_deferred_seek_landing" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (5,'a'),(8,'skip'),(13,'a'),(27,'a');
   BEGIN;
   INSERT INTO t VALUES (10,'a');
   UPDATE t SET v='a2' WHERE id=5;
   DELETE FROM t WHERE id>=5 AND v<>'skip';
   COMMIT;
   SELECT group_concat(id||'='||v) FROM t;" \
  "8=skip" \
  "$DB"

db_rm "$DB"

run_test "onepass_interleaved_writes_no_resurrected_delete" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (12,'a'),(21,'a'),(27,'b');
   BEGIN;
   INSERT OR REPLACE INTO t VALUES (53,'z');
   DELETE FROM t WHERE id>=26 AND id<=42 AND v<>'skip';
   INSERT OR REPLACE INTO t VALUES (28,'skip');
   INSERT OR REPLACE INTO t VALUES (40,'a');
   INSERT OR REPLACE INTO t VALUES (31,'skip');
   INSERT OR REPLACE INTO t VALUES (23,'b');
   SELECT count(*), coalesce(sum(id),0) FROM t WHERE id>=25;
   DELETE FROM t WHERE id=21;
   UPDATE t SET v=v||'F' WHERE id>=23 AND v<>'skip';
   COMMIT;
   SELECT id||'='||v FROM t ORDER BY id;
   SELECT count(*) FROM t;" \
  "4|152
12=a
23=bF
28=skip
31=skip
40=aF
53=zF
6" \
  "$DB"

db_rm "$DB"

run_test "onepass_interleaved_writes_covers_late_insert" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (21,'skip'),(22,'skip'),(23,'skip'),(31,'skip'),
                        (35,'a'),(40,'skip'),(50,'a'),(52,'b'),(56,'skip');
   BEGIN;
   UPDATE t SET v=v||'u' WHERE id=14;
   INSERT OR REPLACE INTO t VALUES (25,'z');
   INSERT OR REPLACE INTO t VALUES (6,'skip');
   INSERT OR REPLACE INTO t VALUES (1,'z');
   DELETE FROM t WHERE id=60;
   SELECT count(*), coalesce(sum(id),0) FROM t WHERE id>=58;
   INSERT OR REPLACE INTO t VALUES (19,'a');
   UPDATE t SET v=v||'F' WHERE id>=19 AND v<>'skip';
   COMMIT;
   SELECT id||'='||v FROM t ORDER BY id;
   SELECT count(*) FROM t;" \
  "0|0
1=z
6=skip
19=aF
21=skip
22=skip
23=skip
25=zF
31=skip
35=aF
40=skip
50=aF
52=bF
56=skip
13" \
  "$DB"

db_rm "$DB"

dltest_finish
