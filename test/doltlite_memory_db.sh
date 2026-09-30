#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite :memory: routing tests ==="
echo ""

run_test "memory_engine_is_prolly" "SELECT doltlite_engine();" "prolly" ":memory:"

run_test_lastline "memory_supports_dolt_commit" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init'); SELECT count(*) FROM dolt_log WHERE message='init';" \
  "1" ":memory:"

run_test_lastline "memory_supports_dolt_branch" \
  "SELECT dolt_branch('feat'); SELECT count(*) FROM dolt_branches WHERE name IN ('main','feat');" \
  "2" ":memory:"

run_test_lastline "memory_supports_dolt_checkout_isolates_branches" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','feat-row'); SELECT dolt_checkout('main'); SELECT count(*) FROM t;" \
  "1" ":memory:"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(99);" | $DOLTLITE :memory: > /dev/null 2>&1
R=$(echo "SELECT count(*) FROM t;" | $DOLTLITE :memory: 2>&1)
if echo "$R" | grep -q "no such table"; then
  dltest_pass
else
  dltest_fail "memory_opens_are_independent" "  expected: 'no such table'\n  got:      $R"
fi

SCRATCH=/tmp/test_memory_no_disk_$$
mkdir -p $SCRATCH
ORIG=$PWD
cd $SCRATCH
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" \
  | $ORIG/doltlite :memory: > /dev/null 2>&1
ARTIFACTS=$(ls -A $SCRATCH 2>&1)
cd $ORIG
if [ -z "$ARTIFACTS" ]; then
  dltest_pass
else
  dltest_fail "memory_creates_no_disk_artifacts" "  expected: empty scratch dir\n  got:      $ARTIFACTS"
fi
rm -rf $SCRATCH

run_test_lastline "attached_memory_table_independent" \
  "ATTACH ':memory:' AS aux; CREATE TABLE aux.t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO aux.t VALUES(1,'a'); SELECT count(*) FROM aux.t;" \
  "1" ":memory:"

run_test_lastline "main_and_attached_isolated" \
  "ATTACH ':memory:' AS aux; CREATE TABLE main.t(id INTEGER); INSERT INTO main.t VALUES(1); SELECT count(*) FROM aux.sqlite_master WHERE name='t';" \
  "0" ":memory:"

BORROW_SETUP="PRAGMA cache_size=-128;
CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, p BLOB);
CREATE INDEX t_gv ON t(g,v);
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<20000)
INSERT INTO t SELECT i,i%97,i,zeroblob(200) FROM c;"
for i in $(seq 20001 20200); do
  BORROW_SETUP="$BORROW_SETUP
INSERT INTO t VALUES($i,$i%97,$i,zeroblob(100));"
done

run_test_lastline "memory_small_cache_reads_after_many_commits" \
  "$BORROW_SETUP
SELECT count(*),sum(v),sum(length(p)) FROM t;
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<5000)
SELECT sum((SELECT v FROM t WHERE id=1+(c.i*7919)%20200)) FROM c;
SELECT count(*),sum(v) FROM t WHERE g=7;" \
  "209|2109855" ":memory:"

run_test_lastline "memory_small_cache_rollback_keeps_committed_rows" \
  "$BORROW_SETUP
BEGIN; UPDATE t SET v=v+1; DELETE FROM t WHERE id%3=0; SELECT sum(v) FROM t; ROLLBACK;
SELECT count(*),sum(v),sum(length(p)) FROM t;" \
  "20200|204030100|4020000" ":memory:"

RESTORE_SRC=/tmp/test_memory_restore_src_$$.db
rm -f "$RESTORE_SRC"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, p BLOB);
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30000)
INSERT INTO t SELECT i,i%50,i,zeroblob(64) FROM c;" | $DOLTLITE "$RESTORE_SRC" > /dev/null 2>&1
run_test_lastline "memory_restore_replaces_store_under_cached_nodes" \
  "$BORROW_SETUP
SELECT count(*),sum(v) FROM t;
.restore $RESTORE_SRC
SELECT count(*),sum(v),sum(length(p)) FROM t;
INSERT INTO t SELECT id+100000,g,v,p FROM t WHERE id<=3000;
SELECT count(*),sum(v),sum(length(p)) FROM t;" \
  "33000|454516500|2112000" ":memory:"
rm -f "$RESTORE_SRC" "$(dirname "$RESTORE_SRC")/.$(basename "$RESTORE_SRC")-lock"

dltest_finish
