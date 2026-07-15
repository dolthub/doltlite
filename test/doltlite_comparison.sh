#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Tier 2 comparison fixes (E2/E3/E4/E6/E7) ==="
echo ""

DB=/tmp/test_cmp_e4_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1, 0);
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v = 0 WHERE id = 1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e4_identical_value_no_modified_rows" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD^','HEAD','t');" \
  "0" "$DB"

run_test "e4_value_intact" \
  "SELECT v FROM t WHERE id=1;" "0" "$DB"
db_rm "$DB"

DB=/tmp/test_cmp_e6_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e6_forward_scan" \
  "SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id);" "a,b,c" "$DB"
run_test "e6_reverse_scan" \
  "SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id DESC);" "c,b,a" "$DB"
run_test "e6_reverse_with_filter" \
  "SELECT group_concat(v) FROM (SELECT v FROM t WHERE id > 0 ORDER BY id DESC);" \
  "c,b,a" "$DB"
db_rm "$DB"

DB=/tmp/test_cmp_e2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1, 5);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
UPDATE t SET v = 10 WHERE id = 1;
UPDATE t SET v = 5 WHERE id = 1;
SELECT dolt_commit('-A','-m','main_roundtrip');
SELECT dolt_checkout('feat');
UPDATE t SET v = 7 WHERE id = 1;
UPDATE t SET v = 5 WHERE id = 1;
SELECT dolt_commit('-A','-m','feat_roundtrip');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e2_roundtrip_merge_no_conflict" \
  "SELECT v FROM t WHERE id = 1;" "5" "$DB"
run_test "e2_no_conflict_rows" \
  "SELECT count(*) FROM dolt_status WHERE status LIKE '%conflict%';" "0" "$DB"
db_rm "$DB"

DB=/tmp/test_cmp_e7_$$.db; db_rm "$DB"
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  printf "INSERT INTO t VALUES "
  for i in $(seq 1 2000); do
    if [ $i -gt 1 ]; then printf ", "; fi
    printf "(%d, 'row_%d_with_payload_to_force_multilevel_tree')" "$i" "$i"
  done
  echo ";"
  echo "SELECT dolt_commit('-A','-m','base');"
  echo "UPDATE t SET v = 'modified' WHERE id = 1000;"
  echo "SELECT dolt_commit('-A','-m','one_row_changed');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e7_single_row_diff" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
run_test "e7_no_phantom_rows" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD^','HEAD','t');" "0" "$DB"
db_rm "$DB"

DB=/tmp/test_cmp_e3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='B' WHERE id=2;
DELETE FROM t WHERE id=3;
INSERT INTO t VALUES(4,'d');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e3_modified_rows" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
run_test "e3_added_rows" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
run_test "e3_deleted_rows" \
  "SELECT rows_deleted FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
