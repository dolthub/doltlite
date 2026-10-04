#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"
dltest_init_queries

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""



db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Merge / push corner cases (F3 / F7) ==="
echo ""

DB=/tmp/test_t4_nn_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT NOT NULL, v INTEGER);
INSERT INTO t VALUES(1, 'a', 10);
INSERT INTO t VALUES(2, 'b', 20);
SELECT dolt_commit('-A','-m','init');" | dltest_query "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');
UPDATE t SET v=11 WHERE id=1;
SELECT dolt_commit('-A','-m','main_change');" | dltest_query "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
UPDATE t SET v=22 WHERE id=2;
SELECT dolt_commit('-A','-m','feat_change');
SELECT dolt_checkout('main');" | dltest_query "$DB" > /dev/null 2>&1

dltest_assert_match "f3_merge_with_notnull_succeeds" \
  "$(dltest_query "$DB" "SELECT dolt_merge('feat');" 2>&1)" "^[0-9a-f]{40}$"
dltest_assert_equal "f3_main_change_visible" \
  "$(dltest_query "$DB" "SELECT v FROM t WHERE id=1;" 2>&1)" "11"
dltest_assert_equal "f3_feat_change_visible" \
  "$(dltest_query "$DB" "SELECT v FROM t WHERE id=2;" 2>&1)" "22"
db_rm "$DB"

dltest_finish
