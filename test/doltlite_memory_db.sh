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

dltest_finish
