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

# Range bounds cannot exact-key the pending map; equality can.
run_test "blobkey_range_seek_sees_pending_row_above_tree" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID;
   INSERT INTO t VALUES(5,'c');
   BEGIN;
   INSERT INTO t VALUES(20,'p');
   SELECT coalesce(group_concat(k),'none') FROM t WHERE k > 10;
   SELECT coalesce(group_concat(k),'none') FROM t WHERE k >= 20;
   SELECT coalesce(group_concat(k),'none') FROM t WHERE k > 1;
   SELECT count(*) FROM t WHERE k = 20;
   UPDATE t SET v='u' WHERE k > 10;
   SELECT coalesce(group_concat(k||'='||v),'none') FROM (SELECT k,v FROM t ORDER BY k);
   ROLLBACK;" \
  "20
20
5,20
1
5=c,20=u" \
  "$DB"

db_rm "$DB"

# No committed rows, so the range has nothing to fall back on.
run_test "blobkey_range_seek_sees_pending_only_table" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, v TEXT) WITHOUT ROWID;
   BEGIN;
   INSERT INTO t VALUES(-18,'p');
   SELECT count(*) FROM t WHERE k > -19 AND k < -2;
   UPDATE t SET v='u' WHERE k > -19;
   SELECT k||'='||v FROM t;
   DELETE FROM t WHERE k > -19 AND k < -2;
   SELECT count(*) FROM t;
   COMMIT;" \
  "1
-18=u
0" \
  "$DB"

db_rm "$DB"

# >= uses positive default_rc; treating that as direction used to miss pending landings.
run_test "ge_bound_sees_pending_row" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, a, b TEXT) WITHOUT ROWID;
   BEGIN;
   INSERT INTO t VALUES(-14, 'x', 'y');
   SELECT count(*) FROM t WHERE k >= -18 AND k <= 33;
   SELECT count(*) FROM t WHERE k >= -14;
   UPDATE t SET a = 10 WHERE k >= -18;
   SELECT quote(a) FROM t;
   DELETE FROM t WHERE k >= -18;
   SELECT count(*) FROM t;
   COMMIT;" \
  "1
1
10
0" \
  "$DB"

db_rm "$DB"

# GLOB/LIKE compile to >= / < on a secondary index.
run_test "prefix_match_sees_pending_row" \
  "CREATE TABLE t(k INTEGER, a, b TEXT);
   CREATE INDEX i_b ON t(b, a);
   BEGIN;
   INSERT INTO t VALUES(-3, NULL, 'aXb');
   SELECT count(*) FROM t WHERE b GLOB 'a*';
   SELECT count(*) FROM t WHERE b >= 'a' AND b < 'b';
   SELECT count(*) FROM t WHERE b LIKE 'a%';
   COMMIT;" \
  "1
1
1" \
  "$DB"

db_rm "$DB"

# Equality must not take the next pending row above a missing key.
run_test "equality_seek_does_not_match_greater_pending_row" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, a, b TEXT);
   BEGIN;
   INSERT INTO t VALUES(26, 42.693, 'z');
   UPDATE t SET b = 'zz' WHERE k = 20;
   DELETE FROM t WHERE k = 20;
   SELECT count(*) FROM t WHERE k = 20;
   SELECT quote(k)||'/'||quote(b) FROM t;
   COMMIT;" \
  "0
26/'z'" \
  "$DB"

db_rm "$DB"

run_test "composite_pk_range_seek_sees_pending_row" \
  "CREATE TABLE t(a INTEGER, b TEXT, v TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
   INSERT INTO t VALUES(1,'x','c');
   BEGIN;
   INSERT INTO t VALUES(9,'y','p');
   SELECT coalesce(group_concat(a),'none') FROM t WHERE a > 4;
   SELECT count(*) FROM t WHERE a = 9 AND b = 'y';
   DELETE FROM t WHERE a > 4;
   SELECT coalesce(group_concat(a),'none') FROM t;
   COMMIT;" \
  "9
1
1" \
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

# Pending mut-map entry shadows the committed row.
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

# OP_IfEmpty must see pending inserts, not only the persisted root.
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

# Still empty if emptied in-transaction.
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

# After a mut-map-sourced write, resume from that key, not the tree cursor.
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

# Pending update shadowing a committed row must survive as the merged value.
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

# Pending-only row must not advance the tree cursor; it has to sort between two committed rows.
run_test "onepass_update_blobkey_covers_committed_row_after_pending" \
  "CREATE TABLE t(k INTEGER, j TEXT, v TEXT, PRIMARY KEY(k, j)) WITHOUT ROWID;
   INSERT INTO t VALUES (1,'a','x'),(3,'c','x');
   BEGIN;
   INSERT INTO t VALUES (2,'b','x');
   UPDATE t SET v='u' WHERE k >= 1;
   COMMIT;
   SELECT group_concat(k||j||'='||v, ',') FROM (SELECT k,j,v FROM t ORDER BY k);" \
  "1a=u,2b=u,3c=u" \
  "$DB"

db_rm "$DB"

run_test "onepass_delete_blobkey_covers_committed_row_after_pending" \
  "CREATE TABLE t(k INTEGER, j TEXT, v TEXT, PRIMARY KEY(k, j)) WITHOUT ROWID;
   INSERT INTO t VALUES (1,'a','x'),(3,'c','x');
   BEGIN;
   INSERT INTO t VALUES (2,'b','x');
   DELETE FROM t WHERE k >= 1;
   COMMIT;
   SELECT count(*) FROM t;" \
  "0" \
  "$DB"

db_rm "$DB"

run_test "onepass_update_blobkey_interleaved_rows" \
  "CREATE TABLE t(k INTEGER, j TEXT, v TEXT, PRIMARY KEY(k, j)) WITHOUT ROWID;
   INSERT INTO t VALUES (10,'a','x'),(30,'c','x'),(50,'e','x'),(70,'g','x');
   BEGIN;
   INSERT INTO t VALUES (20,'b','x'),(40,'d','x'),(60,'f','x');
   UPDATE t SET v='u' WHERE k >= 10;
   COMMIT;
   SELECT count(*) FROM t WHERE v='u';" \
  "7" \
  "$DB"

db_rm "$DB"

# Resume must re-seed both sides; a leftover deferral re-seeks the departed key.
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

# Seek past last lands via BtreeLast (backward); a forward step must not walk back into range.
DB=/tmp/test_txn_index_seek_past_end_$$.db; db_rm "$DB"

run_test "index_seek_past_end_after_pending_update" \
  "CREATE TABLE t(k INTEGER UNIQUE, a, b TEXT);
   CREATE INDEX i_b ON t(b, a);
   INSERT INTO t VALUES(-5, -25.110, x'0001');
   INSERT INTO t VALUES(3, 33, '');
   BEGIN;
   UPDATE t SET b = 'a' WHERE b > 'AB';
   SELECT count(*) FROM t WHERE b > 'zz';
   SELECT count(*) FROM t WHERE b > x'ffff';
   SELECT count(*) FROM t WHERE b >= x'0001';
   SELECT group_concat(quote(b), '|') FROM (SELECT b FROM t ORDER BY b, k);
   COMMIT;" \
  "0
0
0
''|'a'" \
  "$DB"

db_rm "$DB"

run_test "index_delete_past_end_spares_all_rows" \
  "CREATE TABLE t(k INTEGER UNIQUE, a, b TEXT);
   CREATE INDEX i_b ON t(b, a);
   INSERT INTO t VALUES(-5, -25.110, x'0001');
   INSERT INTO t VALUES(3, 33, '');
   BEGIN;
   UPDATE t SET b = 'a' WHERE b > 'AB';
   DELETE FROM t WHERE b > 'zz';
   COMMIT;
   SELECT group_concat(quote(b), '|') FROM (SELECT b FROM t ORDER BY b, k);" \
  "''|'a'" \
  "$DB"

db_rm "$DB"

# Deleted last row: backward last-landing with no mut-map side used to unmask the delete.
DB=/tmp/test_txn_masked_last_row_$$.db; db_rm "$DB"

run_test "range_scan_masks_deleted_last_row" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, a) WITHOUT ROWID;
   INSERT INTO t VALUES(-17,-33.132);
   INSERT INTO t VALUES(-38,-44.638);
   BEGIN;
   DELETE FROM t WHERE k = -17;
   SELECT count(*) FROM t WHERE k > -20 AND k < -3;
   SELECT count(*) FROM t WHERE k >= -20;
   SELECT coalesce(group_concat(k), 'none') FROM t WHERE k > -20;
   SELECT count(*) FROM t;
   COMMIT;" \
  "0
0
none
1" \
  "$DB"

db_rm "$DB"

run_test "range_scan_masks_deleted_last_row_text_pk" \
  "CREATE TABLE t(k TEXT PRIMARY KEY, a) WITHOUT ROWID;
   INSERT INTO t VALUES('m',1);
   INSERT INTO t VALUES('a',2);
   BEGIN;
   DELETE FROM t WHERE k = 'm';
   SELECT count(*) FROM t WHERE k > 'b' AND k < 'z';
   SELECT coalesce(group_concat(k), 'none') FROM t WHERE k > 'b';
   SELECT coalesce(group_concat(k), 'none') FROM t WHERE k < 'z' ORDER BY k DESC;
   COMMIT;" \
  "0
none
a" \
  "$DB"

db_rm "$DB"

# Phantom last-row served to UPDATE surfaces as OP_IdxDelete index corruption.
run_test "update_over_deleted_last_row_is_not_corruption" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, a) WITHOUT ROWID;
   CREATE INDEX i_a ON t(a);
   INSERT INTO t VALUES(-17,-33.132);
   INSERT INTO t VALUES(-38,-44.638);
   BEGIN;
   DELETE FROM t WHERE k = -17;
   UPDATE t SET a = 27 WHERE k > -20 AND k < -3;
   COMMIT;
   SELECT count(*), coalesce(group_concat(k||'/'||a),'none') FROM t;" \
  "1|-38/-44.638" \
  "$DB"

db_rm "$DB"

# Prefix walk to BtreeLast: SeekGT must not serve a pending row below that landing.
DB=/tmp/test_txn_atlast_forward_$$.db; db_rm "$DB"

run_test "seek_gt_after_last_landing_finds_nothing" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, b TEXT);
   CREATE INDEX i_b ON t(b);
   INSERT INTO t VALUES(1, 'm');
   BEGIN;
   INSERT INTO t VALUES(2, 'a');
   SELECT coalesce(group_concat(k), 'none') FROM t WHERE b > 'm';
   SELECT coalesce(group_concat(k), 'none') FROM t WHERE b > 'a';
   SELECT coalesce(group_concat(b), 'none') FROM (SELECT b FROM t ORDER BY b DESC);
   SELECT quote(max(b)), quote(min(b)) FROM t;
   COMMIT;" \
  "none
1
m,a
'm'|'a'" \
  "$DB"

db_rm "$DB"

run_test "index_seek_past_end_steps_backward" \
  "CREATE TABLE t(k INTEGER UNIQUE, a, b TEXT);
   CREATE INDEX i_b ON t(b, a);
   INSERT INTO t VALUES(-5, -25.110, x'0001');
   INSERT INTO t VALUES(3, 33, '');
   BEGIN;
   UPDATE t SET b = 'a' WHERE b > 'AB';
   SELECT quote(max(b)) FROM t WHERE b < x'ffff';
   SELECT group_concat(quote(b), '|') FROM (SELECT b FROM t WHERE b <= 'a' ORDER BY b DESC);
   COMMIT;" \
  "'a'
'a'|''" \
  "$DB"

db_rm "$DB"

# Keys no double represents exactly share a prefix with the neighbour; that landing used to drop pending rows.
DB=/tmp/test_txn_extended_numeric_$$.db; db_rm "$DB"

run_test "range_update_reaches_pending_extended_numeric_key" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740993, 'z');
   UPDATE t SET b = '' WHERE k > -9007199254740994 AND k < 9007199254740994;
   SELECT changes();
   SELECT quote(k) || '/' || quote(b) FROM t;
   COMMIT;
   SELECT quote(k) || '/' || quote(b) FROM t;" \
  "1
-9007199254740993/''
-9007199254740993/''" \
  "$DB"

db_rm "$DB"

run_test "bounded_seek_sees_pending_extended_numeric_keys" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   INSERT INTO t VALUES(500, 'c');
   BEGIN;
   INSERT INTO t VALUES(-9007199254740993, 'n');
   INSERT INTO t VALUES(9007199254740993, 'p');
   SELECT count(*) FROM t WHERE k > -9007199254740994;
   SELECT count(*) FROM t WHERE k >= -9007199254740994;
   SELECT count(*) FROM t WHERE k > 9007199254740992;
   SELECT group_concat(quote(k), '|') FROM (SELECT k FROM t WHERE k > -9007199254740994 ORDER BY k);
   COMMIT;" \
  "3
3
1
-9007199254740993|500|9007199254740993" \
  "$DB"

db_rm "$DB"

# Landing is above, not equality, or UPDATE rewrites a neighbour.
run_test "equality_seek_near_extended_numeric_key_hits_right_row" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740993, 'n');
   INSERT INTO t VALUES(-9007199254740994, 'base');
   UPDATE t SET b = 'hit' WHERE k = -9007199254740994;
   SELECT changes();
   SELECT group_concat(quote(k) || '/' || quote(b), '|') FROM (SELECT k, b FROM t ORDER BY k);
   COMMIT;" \
  "1
-9007199254740994/'hit'|-9007199254740993/'n'" \
  "$DB"

db_rm "$DB"

# Walk past a pending prefix-equal tombstone; the in-between row still comes back.
run_test "tombstone_sharing_extended_prefix_does_not_hide_row" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   INSERT INTO t VALUES(-9007199254740993,'ext'),(-9007199254740992,'mid');
   BEGIN;
   DELETE FROM t WHERE k = -9007199254740993;
   SELECT coalesce(group_concat(quote(k)),'none') FROM t WHERE k > -9007199254740994;
   SELECT count(*) FROM t WHERE k = -9007199254740993;
   COMMIT;
   SELECT coalesce(group_concat(quote(k)),'none') FROM t;" \
  "-9007199254740992
0
-9007199254740992" \
  "$DB"

db_rm "$DB"

# Committed prefix-equal + extended-numeric neighbour cleared eqSeen; SeekGT stepped onto a pending twin.
run_test "gt_excludes_pending_twin_of_extended_bound" \
  "CREATE TABLE t(k INTEGER, j TEXT, a, PRIMARY KEY(k, j));
   INSERT INTO t VALUES(-9007199254740994, 'A', 1);
   INSERT INTO t VALUES(-9007199254740993, 'z', 2);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994, 'B', 3);
   SELECT coalesce(group_concat(quote(k)||'/'||quote(j), '|'),'none') FROM (SELECT k, j FROM t WHERE k > -9007199254740994 ORDER BY k, j);
   SELECT coalesce(group_concat(quote(k)||'/'||quote(j), '|'),'none') FROM (SELECT k, j FROM t WHERE k >= -9007199254740994 ORDER BY k, j);
   COMMIT;" \
  "-9007199254740993/'z'
-9007199254740994/'A'|-9007199254740994/'B'|-9007199254740993/'z'" \
  "$DB"

db_rm "$DB"

run_test "adjacent_extended_keys_respect_both_bounds" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740994,'lo'),(-9007199254740993,'m'),(-9007199254740992,'hi');
   SELECT group_concat(quote(k)) FROM (SELECT k FROM t WHERE k > -9007199254740995 ORDER BY k);
   SELECT group_concat(quote(k)) FROM (SELECT k FROM t WHERE k > -9007199254740994 ORDER BY k);
   UPDATE t SET b='u' WHERE k > -9007199254740994 AND k < -9007199254740992;
   SELECT changes();
   SELECT group_concat(quote(k)||'='||quote(b)) FROM (SELECT k,b FROM t ORDER BY k);
   COMMIT;" \
  "-9007199254740994,-9007199254740993,-9007199254740992
-9007199254740993,-9007199254740992
1
-9007199254740994='lo',-9007199254740993='u',-9007199254740992='hi'" \
  "$DB"

db_rm "$DB"

# Delete+reinsert: live row is a pending insert behind a tombstone at the same key.
run_test "reinserted_extended_key_is_reachable_by_range" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   INSERT INTO t VALUES(-9007199254740993,'old');
   BEGIN;
   DELETE FROM t WHERE k = -9007199254740993;
   INSERT INTO t VALUES(-9007199254740993,'new');
   SELECT coalesce(group_concat(quote(k)||'='||quote(b)),'none') FROM t WHERE k > -9007199254740994;
   UPDATE t SET b='upd' WHERE k > -9007199254740994;
   SELECT changes();
   COMMIT;
   SELECT quote(b) FROM t;" \
  "-9007199254740993='new'
1
'upd'" \
  "$DB"

db_rm "$DB"

run_test "backward_scan_reaches_pending_extended_keys" \
  "CREATE TABLE t(k NUMERIC PRIMARY KEY, b TEXT);
   BEGIN;
   INSERT INTO t VALUES(-9007199254740993,'n'),(9007199254740993,'p');
   SELECT group_concat(quote(k)) FROM (SELECT k FROM t WHERE k < 9007199254740994 ORDER BY k DESC);
   SELECT quote(max(k)), quote(min(k)) FROM t;
   COMMIT;" \
  "9007199254740993,-9007199254740993
9007199254740993|-9007199254740993" \
  "$DB"

db_rm "$DB"

# Backward step from a below-all landing must move down, not keep a pending row above the cursor.
DB=/tmp/test_txn_backward_landing_$$.db; db_rm "$DB"

run_test "backward_seek_below_all_rows_finds_nothing" \
  "CREATE TABLE t(k TEXT PRIMARY KEY, a, b TEXT) WITHOUT ROWID;
   CREATE INDEX i_a ON t(a);
   INSERT INTO t VALUES('AB', -47.436, 'x');
   BEGIN;
   INSERT INTO t VALUES('ab', 5, 'z');
   SELECT coalesce(quote(max(a)),'NULL') FROM t WHERE a <= -1e308;
   SELECT coalesce(group_concat(quote(a)),'none') FROM (SELECT a FROM t WHERE a <= -1e308 ORDER BY a DESC);
   SELECT count(*) FROM t WHERE a <= -1e308;
   COMMIT;" \
  "NULL
none
0" \
  "$DB"

db_rm "$DB"

# DESC NULLS FIRST cannot walk an index backwards; it re-seeks after the NULL region.
run_test "desc_nulls_first_does_not_repeat_pending_row" \
  "CREATE TABLE t(k TEXT PRIMARY KEY, a, b TEXT) WITHOUT ROWID;
   CREATE INDEX i_a ON t(a);
   INSERT INTO t VALUES('AB', -47.436, 'x');
   BEGIN;
   INSERT INTO t VALUES('ab', 5, 'z');
   SELECT group_concat(quote(a),'|') FROM (SELECT a FROM t ORDER BY a DESC NULLS FIRST, quote(k));
   SELECT group_concat(quote(a),'|') FROM (SELECT a FROM t ORDER BY a DESC);
   SELECT count(*) FROM t;
   COMMIT;" \
  "5|-47.436
5|-47.436
2" \
  "$DB"

db_rm "$DB"

# Prefix seek parks both merge sides for forward travel; a backward step that trusts them skips rows underneath.
DB=/tmp/test_txn_reverse_$$.db; db_rm "$DB"

run_test "reversal_serves_pending_null_row" \
  "CREATE TABLE t(k TEXT PRIMARY KEY, a, b TEXT) WITHOUT ROWID;
   CREATE INDEX i_a ON t(a);
   INSERT INTO t VALUES('AB', -47.436, 'x');
   BEGIN;
   INSERT INTO t VALUES('zz', NULL, 'q');
   SELECT group_concat(coalesce(quote(a),'N'),'|') FROM (SELECT a FROM t ORDER BY a DESC NULLS FIRST);
   SELECT count(*) FROM t;
   COMMIT;" \
  "NULL|-47.436
2" \
  "$DB"

db_rm "$DB"

run_test "reversal_serves_committed_null_row" \
  "CREATE TABLE t(k TEXT PRIMARY KEY COLLATE RTRIM, a, b TEXT COLLATE RTRIM);
   CREATE INDEX i_a ON t(a);
   INSERT OR REPLACE INTO t(k, a, b) VALUES('', NULL, x'00');
   BEGIN;
   INSERT OR IGNORE INTO t(k, a, b) VALUES(x'0001', -19, 'zz');
   SELECT coalesce(group_concat(q,'|'),'none') FROM (SELECT coalesce(quote(a), 'N') AS q FROM t ORDER BY a DESC NULLS FIRST, coalesce(quote(k), 'N'));
   SELECT count(*) FROM t;
   COMMIT;" \
  "NULL|-19
2" \
  "$DB"

db_rm "$DB"

run_test "reversal_across_null_region_keeps_order" \
  "CREATE TABLE t(k TEXT PRIMARY KEY, a, b TEXT) WITHOUT ROWID;
   CREATE INDEX i_a ON t(a);
   INSERT INTO t VALUES('AB', -47.436, 'x');
   INSERT INTO t VALUES('zz', NULL, 'q');
   BEGIN;
   INSERT INTO t VALUES('ab', 5, 'z');
   SELECT group_concat(coalesce(quote(a),'N'),'|') FROM (SELECT a FROM t ORDER BY a DESC NULLS FIRST);
   SELECT group_concat(coalesce(quote(a),'N'),'|') FROM (SELECT a FROM t ORDER BY a ASC NULLS LAST);
   SELECT count(*) FROM t;
   COMMIT;" \
  "NULL|5|-47.436
-47.436|5|NULL
3" \
  "$DB"

db_rm "$DB"

dltest_finish
