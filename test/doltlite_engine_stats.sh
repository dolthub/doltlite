#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite engine counter Tests ==="

DB=/tmp/test_engine_stats_$$.db
rm -f "$DB"

run_test "counters_move" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT (SELECT value FROM dolt_engine_stats WHERE name='hash_bytes')>0,
       (SELECT value FROM dolt_engine_stats WHERE name='chunk_write')>0,
       (SELECT value FROM dolt_engine_stats WHERE name='pending_insert')>0;
.open $DB
SELECT v FROM t WHERE id=1;
SELECT (SELECT value FROM dolt_engine_stats WHERE name='cache_miss')>0,
       (SELECT value FROM dolt_engine_stats WHERE name='seek')>0;" \
  "1|1|1
a
1|1" "$DB"

run_test "reset_clears" \
  "SELECT sum(value)>0 FROM dolt_engine_stats WHERE reset=1;
   SELECT sum(value) FROM dolt_engine_stats;" \
  "1
0" "$DB"

run_test_match "shell_stats_page_cache_hits" \
  "INSERT INTO t VALUES(3,'c');
SELECT v FROM t WHERE id=3;
.stats" \
  "Page cache hits: +[1-9]" "$DB"

run_test_match "shell_stats_page_cache_misses" \
  "INSERT INTO t VALUES(4,'d');
SELECT v FROM t WHERE id=4;
.stats" \
  "Page cache misses: +[1-9]" "$DB"

rm -f "$DB"
dltest_finish
