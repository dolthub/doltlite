#!/bin/bash
DLTEST_TIMEOUT=60
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Index Tests at Scale ==="
echo ""

DB1=/tmp/test_idx1_$$.db; rm -f "$DB1"
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tid TEXT NOT NULL, seq INTEGER NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(tid, seq)
);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO events(tid, seq, payload)
  SELECT 'thread-' || (x/200), x%200, hex(randomblob(100)) FROM c;
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "2col_10k_total" \
  "SELECT count(*) FROM events;" \
  "10000" "$DB1"

run_test "2col_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB1"

run_test "2col_10k_point_all" \
  "CREATE TEMP TABLE lookups AS SELECT tid, seq FROM events;
SELECT count(*) FROM lookups l
  WHERE EXISTS(SELECT 1 FROM events e WHERE e.tid=l.tid AND e.seq=l.seq);" \
  "10000" "$DB1"

run_test "2col_10k_prefix" \
  "SELECT count(*) FROM events WHERE tid='thread-25';" \
  "200" "$DB1"

run_test "2col_10k_max" \
  "SELECT MAX(seq) FROM events WHERE tid='thread-25';" \
  "199" "$DB1"

run_test "2col_10k_orderby_desc" \
  "SELECT seq FROM events WHERE tid='thread-0' ORDER BY seq DESC LIMIT 3;" \
  "199
198
197" "$DB1"

DB2=/tmp/test_idx2_$$.db; rm -f "$DB2"
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(aggregate_kind, stream_id, stream_version)
);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO events(aggregate_kind, stream_id, stream_version, payload)
  SELECT 'thread', 'stream-' || (x/200), x%200, hex(randomblob(100)) FROM c;
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "3col_10k_total" \
  "SELECT count(*) FROM events;" \
  "10000" "$DB2"

run_test "3col_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB2"

run_test "3col_10k_1prefix" \
  "SELECT count(*) FROM events WHERE aggregate_kind='thread';" \
  "10000" "$DB2"

run_test "3col_10k_2prefix" \
  "SELECT count(*) FROM events WHERE aggregate_kind='thread' AND stream_id='stream-25';" \
  "200" "$DB2"

run_test "3col_10k_exact" \
  "SELECT count(*) FROM events WHERE aggregate_kind='thread' AND stream_id='stream-25' AND stream_version=100;" \
  "1" "$DB2"

run_test "3col_10k_max" \
  "SELECT MAX(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='stream-25';" \
  "199" "$DB2"

run_test "3col_10k_point_all" \
  "CREATE TEMP TABLE lookups AS SELECT aggregate_kind, stream_id, stream_version FROM events;
SELECT count(*) FROM lookups l
  WHERE EXISTS(SELECT 1 FROM events e
    WHERE e.aggregate_kind=l.aggregate_kind
      AND e.stream_id=l.stream_id
      AND e.stream_version=l.stream_version);" \
  "10000" "$DB2"

DB3=/tmp/test_idx3_$$.db; rm -f "$DB3"
echo "CREATE TABLE log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  region TEXT NOT NULL,
  service TEXT NOT NULL,
  ts INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  msg TEXT NOT NULL,
  UNIQUE(region, service, ts, seq)
);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO log(region, service, ts, seq, msg)
  SELECT 'us-east-' || (x%3), 'svc-' || (x%7), 1000+(x/21), x%21,
    hex(randomblob(80)) FROM c;
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "4col_10k_total" \
  "SELECT count(*) FROM log;" \
  "10000" "$DB3"

run_test "4col_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB3"

run_test "4col_10k_2prefix" \
  "SELECT count(*) FROM log WHERE region='us-east-0' AND service='svc-0';" \
  "477" "$DB3"

run_test "4col_10k_3prefix" \
  "SELECT count(*) FROM log WHERE region='us-east-0' AND service='svc-0' AND ts=1023;" \
  "1" "$DB3"

DB4=/tmp/test_idx4_$$.db; rm -f "$DB4"
echo "CREATE TABLE orders(
  id INTEGER PRIMARY KEY,
  customer TEXT NOT NULL,
  status TEXT NOT NULL,
  amount REAL NOT NULL,
  notes TEXT NOT NULL
);
CREATE INDEX idx_cust_status ON orders(customer, status);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000)
INSERT INTO orders SELECT x, 'cust-' || (x%50),
  CASE WHEN x%3=0 THEN 'shipped' WHEN x%3=1 THEN 'pending' ELSE 'returned' END,
  x*1.5, hex(randomblob(80)) FROM c;
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "secondary_10k_total" \
  "SELECT count(*) FROM orders;" \
  "10000" "$DB4"

run_test "secondary_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB4"

run_test "secondary_10k_prefix" \
  "SELECT count(*) FROM orders WHERE customer='cust-25';" \
  "200" "$DB4"

run_test "secondary_10k_exact" \
  "SELECT count(*) FROM orders WHERE customer='cust-25' AND status='shipped';" \
  "67" "$DB4"

DB5=/tmp/test_idx5_$$.db; rm -f "$DB5"
echo "CREATE TABLE kv(
  ns TEXT NOT NULL,
  key TEXT NOT NULL,
  val TEXT NOT NULL,
  PRIMARY KEY(ns, key)
) WITHOUT ROWID;
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO kv SELECT 'ns-' || (x%20), 'key-' || printf('%05d',x),
  hex(randomblob(80)) FROM c;
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "wor_10k_total" \
  "SELECT count(*) FROM kv;" \
  "10000" "$DB5"

run_test "wor_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB5"

run_test "wor_10k_prefix" \
  "SELECT count(*) FROM kv WHERE ns='ns-10';" \
  "500" "$DB5"

run_test "wor_10k_exact" \
  "SELECT val IS NOT NULL FROM kv WHERE ns='ns-10' AND key='key-00010';" \
  "1" "$DB5"

DB6=/tmp/test_idx6_$$.db; rm -f "$DB6"
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tid TEXT NOT NULL, seq INTEGER NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(tid, seq)
);" | $DOLTLITE "$DB6" > /dev/null 2>&1

for batch in $(seq 0 9); do
  echo "WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<999)
INSERT INTO events(tid, seq, payload)
  SELECT 'thread-' || ((c.x + $batch*1000)/200), (c.x + $batch*1000)%200,
    hex(randomblob(100)) FROM c;
SELECT dolt_commit('-am', 'batch $batch');" | $DOLTLITE "$DB6" > /dev/null 2>&1
done

run_test "batched_10k_total" \
  "SELECT count(*) FROM events;" \
  "10000" "$DB6"

run_test "batched_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB6"

run_test "batched_10k_point_all" \
  "CREATE TEMP TABLE lookups AS SELECT tid, seq FROM events;
SELECT count(*) FROM lookups l
  WHERE EXISTS(SELECT 1 FROM events e WHERE e.tid=l.tid AND e.seq=l.seq);" \
  "10000" "$DB6"

DB7=/tmp/test_idx7_$$.db; rm -f "$DB7"
echo "CREATE TABLE mixed(
  id INTEGER PRIMARY KEY,
  tag TEXT NOT NULL,
  seq INTEGER NOT NULL,
  score REAL NOT NULL,
  data TEXT NOT NULL,
  UNIQUE(tag, seq, score)
);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO mixed SELECT x, 'tag-' || (x%10), x/10, (x%1000)*0.01,
  hex(randomblob(80)) FROM c;
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test "mixed_10k_total" \
  "SELECT count(*) FROM mixed;" \
  "10000" "$DB7"

run_test "mixed_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB7"

run_test "mixed_10k_2prefix" \
  "SELECT count(*) FROM mixed WHERE tag='tag-5' AND seq=500;" \
  "1" "$DB7"

DB8=/tmp/test_idx8_$$.db; rm -f "$DB8"
echo "CREATE TABLE block_groups (
  id BLOB PRIMARY KEY NOT NULL,
  collection_name TEXT NOT NULL,
  sample_name TEXT NOT NULL,
  name TEXT NOT NULL,
  created_on INTEGER NOT NULL,
  parent_block_group_id BLOB,
  is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1))
) STRICT;
CREATE UNIQUE INDEX block_group_uidx
ON block_groups(collection_name, sample_name, name,
  IFNULL(hex(parent_block_group_id), ''));
INSERT INTO block_groups VALUES (
  X'1111111100000000000000000000000000000000000000000000000000000001',
  'simple', 'simple', 'm123', 1000, NULL, 1
);
SELECT dolt_commit('-am','load');" | $DOLTLITE "$DB8" > /dev/null 2>&1

run_test "txn_composite_prefix_seek_excludes_stale_tree_row" \
  "BEGIN;
INSERT INTO block_groups VALUES (
  X'6666666600000000000000000000000000000000000000000000000000000001',
  'simple', 'unknown', 'm123', 1001,
  X'1111111100000000000000000000000000000000000000000000000000000001',
  1
);
SELECT count(*) FROM block_groups
 WHERE collection_name='simple'
   AND sample_name='unknown'
   AND name='m123';
SELECT sample_name FROM block_groups
 WHERE collection_name='simple'
   AND sample_name='unknown'
   AND name='m123';
END;" \
  "1
unknown" "$DB8"

rm -f "$DB1" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8"

dltest_finish
