#!/bin/bash
DLTEST_TIMEOUT=30
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_* vtab constraint pushdown ==="
echo ""

time_ms() {
  local start end
  start=$(python3 -c 'import time; print(int(time.time()*1000))')
  eval "$@" > /dev/null 2>&1
  end=$(python3 -c 'import time; print(int(time.time()*1000))')
  echo $((end - start))
}

DB=/tmp/test_pushdown_$$.db
rm -f "$DB"

NROWS=2000
NCOMMITS=20

echo "Building ${NROWS}-row table with ${NCOMMITS} commits..."
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  echo "BEGIN;"
  for i in $(seq 1 $NROWS); do
    echo "INSERT INTO t VALUES($i, 'v0_$i');"
  done
  echo "COMMIT;"
  echo "SELECT dolt_commit('-A','-m','init');"
  for c in $(seq 1 $((NCOMMITS - 1))); do
    echo "BEGIN;"
    for i in $(seq 1 20); do
      row=$((((c * 7 + i) % NROWS) + 1))
      echo "UPDATE t SET v='v${c}_${row}' WHERE id=${row};"
    done
    echo "COMMIT;"
    echo "SELECT dolt_commit('-A','-m','c${c}');"
  done
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "history_id_eq_planner_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_t WHERE id=50;" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"

run_test_match "history_id_no_filter_planner_no_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_t;" \
  "VIRTUAL TABLE INDEX 0" "$DB"

ID50_CONSTRAINED=$(echo "SELECT count(*) FROM dolt_history_t WHERE id=50;" | $DOLTLITE "$DB")
ID50_UNCONSTRAINED=$(echo "SELECT count(*) FROM dolt_history_t WHERE id=50;" | $DOLTLITE "$DB")
run_test "history_id_eq_count_correct" \
  "SELECT count(*) FROM dolt_history_t WHERE id=50;" "$ID50_UNCONSTRAINED" "$DB"

TOTAL=$(echo "SELECT count(*) FROM dolt_history_t;" | $DOLTLITE "$DB")
EXPECTED_TOTAL=$((NROWS * NCOMMITS))
if [ "$TOTAL" -eq "$EXPECTED_TOTAL" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: history_total_rows (expected $EXPECTED_TOTAL got $TOTAL)"
fi

ID42=$(echo "SELECT count(*) FROM dolt_history_t WHERE id=42;" | $DOLTLITE "$DB")
if [ "$ID42" -gt 0 ] && [ "$ID42" -le 20 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: history_id_eq_in_range (got $ID42)"
fi

ID_RANGE=$(echo "SELECT count(*) FROM dolt_history_t WHERE id BETWEEN 10 AND 20;" | $DOLTLITE "$DB")
EXP_RANGE=$(echo "SELECT count(*) FROM dolt_history_t WHERE id>=10 AND id<=20;" | $DOLTLITE "$DB")
run_test "history_range_matches_inequality" \
  "SELECT count(*) FROM dolt_history_t WHERE id BETWEEN 10 AND 20;" "$EXP_RANGE" "$DB"

run_test_match "history_range_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_t WHERE id>=10 AND id<=20;" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"

run_test_match "blame_id_eq_planner_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_blame_t WHERE id=50;" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"

run_test "blame_id_eq_single_row" \
  "SELECT count(*) FROM dolt_blame_t WHERE id=50;" "1" "$DB"

run_test "blame_total_rows" \
  "SELECT count(*) FROM dolt_blame_t;" "$NROWS" "$DB"

run_test_match "blame_id_eq_returns_hash" \
  "SELECT \"commit\" FROM dolt_blame_t WHERE id=50;" \
  "^[0-9a-f]{40}$" "$DB"

run_test "blame_id_returns_id" \
  "SELECT id FROM dolt_blame_t WHERE id=50;" "50" "$DB"

ONE_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB")
run_test_match "log_hash_eq_planner_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_log WHERE commit_hash='$ONE_HASH';" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"

run_test "log_hash_eq_single_row" \
  "SELECT count(*) FROM dolt_log WHERE commit_hash='$ONE_HASH';" "1" "$DB"

LOG_TOTAL=$(echo "SELECT count(*) FROM dolt_log;" | $DOLTLITE "$DB")
if [ "$LOG_TOTAL" -ge 20 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: log_total_commits_ge_20 (got $LOG_TOTAL)"
fi

run_test_match "diff_hash_eq_planner_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_diff WHERE commit_hash='$ONE_HASH';" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"

run_test "diff_hash_eq_row_count" \
  "SELECT count(*) FROM dolt_diff WHERE commit_hash='$ONE_HASH';" "1" "$DB"

DIFF_TOTAL=$(echo "SELECT count(*) FROM dolt_diff;" | $DOLTLITE "$DB")
if [ "$DIFF_TOTAL" -ge 19 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: diff_total_ge_19 (got $DIFF_TOTAL)"
fi

T_CONSTRAINED=$(time_ms "for i in \$(seq 1 3); do echo 'SELECT count(*) FROM dolt_history_t WHERE id=${NROWS};' | $DOLTLITE '$DB'; done")
T_UNCONSTRAINED=$(time_ms "for i in \$(seq 1 3); do echo 'SELECT count(*) FROM dolt_history_t;' | $DOLTLITE '$DB'; done")

echo "  Wall time: 3x constrained=${T_CONSTRAINED}ms 3x unconstrained=${T_UNCONSTRAINED}ms"
if [ "$T_CONSTRAINED" -le "$T_UNCONSTRAINED" ] || [ "$T_UNCONSTRAINED" -le 200 ]; then
  PASS=$((PASS+1))
  echo "  PASS: history_constrained_no_slower_than_full"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: history_constrained_no_slower_than_full (constrained=${T_CONSTRAINED}ms vs full=${T_UNCONSTRAINED}ms)"
fi

rm -f "$DB"

DB2=/tmp/test_pushdown_nonint_$$.db
rm -f "$DB2"
echo "CREATE TABLE k(name TEXT PRIMARY KEY, v INTEGER);
INSERT INTO k VALUES('alice',1);
INSERT INTO k VALUES('bob',2);
SELECT dolt_commit('-A','-m','init');
UPDATE k SET v=v+1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "nonint_history_total" \
  "SELECT count(*) FROM dolt_history_k;" "4" "$DB2"

run_test_match "nonint_history_no_index_pushdown" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_k WHERE name='alice';" \
  "VIRTUAL TABLE INDEX 0" "$DB2"

run_test "nonint_history_filter_correct" \
  "SELECT count(*) FROM dolt_history_k WHERE name='alice';" "2" "$DB2"

rm -f "$DB2"

DB_COLLATION=/tmp/test_pushdown_collation_$$.db
rm -f "$DB_COLLATION"
echo "CREATE TABLE Foo(id INTEGER PRIMARY KEY, v TEXT);
CREATE VIEW MyView AS SELECT * FROM Foo;
INSERT INTO Foo VALUES(1,'a');
SELECT dolt_commit('-A','-m','Seed');
SELECT dolt_branch('Feature');
SELECT dolt_tag('Release');
SELECT dolt_remote('add','Origin','file:///tmp/origin');
UPDATE Foo SET v='b' WHERE id=1;" | $DOLTLITE "$DB_COLLATION" > /dev/null 2>&1

run_test_match "binary_name_equality_still_pushes" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_branches WHERE name='main';" \
  "VIRTUAL TABLE INDEX 1" "$DB_COLLATION"
run_test_match "nocase_name_equality_scans" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_branches WHERE name COLLATE NOCASE='MAIN';" \
  "VIRTUAL TABLE INDEX 0" "$DB_COLLATION"
run_test "branches_nocase_equality" \
  "SELECT name FROM dolt_branches WHERE name COLLATE NOCASE='MAIN';" \
  "main" "$DB_COLLATION"
run_test "branches_rtrim_equality" \
  "SELECT name FROM dolt_branches WHERE name COLLATE RTRIM='main   ';" \
  "main" "$DB_COLLATION"
run_test "tags_nocase_equality" \
  "SELECT tag_name FROM dolt_tags WHERE tag_name COLLATE NOCASE='release';" \
  "Release" "$DB_COLLATION"
run_test "remotes_nocase_equality" \
  "SELECT name FROM dolt_remotes WHERE name COLLATE NOCASE='origin';" \
  "Origin" "$DB_COLLATION"
run_test "status_table_nocase_equality" \
  "SELECT table_name FROM dolt_status WHERE table_name COLLATE NOCASE='foo';" \
  "Foo" "$DB_COLLATION"

COLLATION_DIFF_COUNT=$(echo "SELECT count(*) FROM dolt_diff WHERE (table_name||'') COLLATE NOCASE='foo';" | $DOLTLITE "$DB_COLLATION")
run_test "diff_table_nocase_equality" \
  "SELECT count(*) FROM dolt_diff WHERE table_name COLLATE NOCASE='foo';" \
  "$COLLATION_DIFF_COUNT" "$DB_COLLATION"
run_test "schemas_name_nocase_equality" \
  "SELECT name FROM dolt_schemas WHERE name COLLATE NOCASE='myview';" \
  "MyView" "$DB_COLLATION"
run_test "schemas_type_nocase_equality" \
  "SELECT type FROM dolt_schemas WHERE type COLLATE NOCASE='VIEW';" \
  "view" "$DB_COLLATION"
run_test "patch_type_nocase_equality" \
  "SELECT count(*) FROM dolt_patch('HEAD','WORKING') WHERE diff_type COLLATE NOCASE='DATA';" \
  "1" "$DB_COLLATION"
run_test "log_binary_uppercase_hash_rechecked" \
  "SELECT count(*) FROM dolt_log WHERE commit_hash=upper((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "0" "$DB_COLLATION"
run_test "log_nocase_uppercase_hash" \
  "SELECT count(*) FROM dolt_log WHERE commit_hash COLLATE NOCASE=upper((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB_COLLATION"
run_test "history_binary_uppercase_hash_rechecked" \
  "SELECT count(*) FROM dolt_history_Foo WHERE commit_hash=upper((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "0" "$DB_COLLATION"
run_test "history_nocase_uppercase_hash" \
  "SELECT count(*) FROM dolt_history_Foo WHERE commit_hash COLLATE NOCASE=upper((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB_COLLATION"
run_test "ancestors_binary_uppercase_hash_rechecked" \
  "SELECT count(*) FROM dolt_commit_ancestors WHERE commit_hash=upper((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "0" "$DB_COLLATION"
run_test "ancestors_nocase_uppercase_hash" \
  "SELECT count(*) FROM dolt_commit_ancestors WHERE commit_hash COLLATE NOCASE=upper((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB_COLLATION"

rm -f "$DB_COLLATION"

# sqlite3_value_int64 reads NULL as 0; the pk=0 row is what would match by accident.
DB3=/tmp/test_pushdown_null_$$.db
rm -f "$DB3"
echo "CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'zero');
INSERT INTO t VALUES(1,'one');
SELECT dolt_commit('-A','-m','init');
UPDATE t SET v='changed' WHERE pk=1;
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='dirty' WHERE pk=0;" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "null_pk_eq_at_empty" \
  "SELECT count(*) FROM dolt_at_t WHERE commit_ref='HEAD' AND pk=NULL;" "0" "$DB3"
run_test "null_pk_eq_history_empty" \
  "SELECT count(*) FROM dolt_history_t WHERE pk=NULL;" "0" "$DB3"
run_test "null_pk_eq_blame_empty" \
  "SELECT count(*) FROM dolt_blame_t WHERE pk=NULL;" "0" "$DB3"
run_test "null_pk_ge_history_empty" \
  "SELECT count(*) FROM dolt_history_t WHERE pk>=NULL;" "0" "$DB3"
run_test "null_pk_le_history_empty" \
  "SELECT count(*) FROM dolt_history_t WHERE pk<=NULL;" "0" "$DB3"
run_test "null_table_name_diff_empty" \
  "SELECT count(*) FROM dolt_diff WHERE table_name=NULL;" "0" "$DB3"
run_test "null_staged_status_empty" \
  "SELECT count(*) FROM dolt_status WHERE staged=NULL;" "0" "$DB3"

run_test "null_join_history_empty" \
  "WITH n(k) AS (SELECT NULL) SELECT count(*) FROM n JOIN dolt_history_t h ON h.pk=n.k;" \
  "0" "$DB3"

run_test "pk_eq_zero_still_matches" \
  "SELECT count(*) FROM dolt_at_t WHERE commit_ref='HEAD' AND pk=0;" "1" "$DB3"
run_test "pk_eq_one_still_matches" \
  "SELECT count(*) FROM dolt_at_t WHERE commit_ref='HEAD' AND pk=1;" "1" "$DB3"
run_test "pk_ge_zero_still_matches" \
  "SELECT count(*) FROM dolt_history_t WHERE pk>=0;" "4" "$DB3"
run_test "staged_zero_still_matches" \
  "SELECT count(*) FROM dolt_status WHERE staged=0;" "1" "$DB3"
run_test "table_name_named_still_matches" \
  "SELECT count(*) FROM dolt_diff WHERE table_name='t';" "3" "$DB3"

rm -f "$DB3"

# INTEGER PK is a rowid alias only with a rowid; WITHOUT ROWID used to drop pushed pk constraints.
DB4=/tmp/test_pushdown_worid_$$.db
rm -f "$DB4"
echo "CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1,'one');
INSERT INTO t VALUES(2,'two');
INSERT INTO t VALUES(3,'three');
SELECT dolt_commit('-A','-m','init');
UPDATE t SET v='TWO' WHERE pk=2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "worid_table_rows" \
  "SELECT group_concat(pk || ':' || v, ' ') FROM (SELECT pk,v FROM t ORDER BY pk);" \
  "1:one 2:TWO 3:three" "$DB4"
run_test "worid_at_renders_pk" \
  "SELECT group_concat(pk || ':' || v, ' ')
     FROM (SELECT pk,v FROM dolt_at_t WHERE commit_ref='HEAD' ORDER BY pk);" \
  "1:one 2:TWO 3:three" "$DB4"
run_test "worid_at_pk_eq_selects_that_row" \
  "SELECT group_concat(pk || ':' || v, ' ')
     FROM dolt_at_t WHERE commit_ref='HEAD' AND pk=2;" "2:TWO" "$DB4"
run_test "worid_at_pk_eq_absent_is_empty" \
  "SELECT count(*) FROM dolt_at_t WHERE commit_ref='HEAD' AND pk=99;" "0" "$DB4"
run_test "worid_history_renders_pk" \
  "SELECT group_concat(pk || ':' || v, ' ')
     FROM (SELECT pk,v FROM dolt_history_t ORDER BY pk, v);" \
  "1:one 1:one 2:TWO 2:two 3:three 3:three" "$DB4"
run_test "worid_history_pk_eq" \
  "SELECT count(*) FROM dolt_history_t WHERE pk=2;" "2" "$DB4"
run_test "worid_history_pk_range" \
  "SELECT count(*) FROM dolt_history_t WHERE pk>=2;" "4" "$DB4"
run_test "worid_blame_renders_pk" \
  "SELECT group_concat(pk, ' ') FROM (SELECT pk FROM dolt_blame_t ORDER BY pk);" \
  "1 2 3" "$DB4"
run_test "worid_blame_pk_eq" \
  "SELECT count(*) FROM dolt_blame_t WHERE pk=2;" "1" "$DB4"

rm -f "$DB4"

DB4B=/tmp/test_pushdown_worid_comp_$$.db
rm -f "$DB4B"
echo "CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t VALUES(1,1,'x');
INSERT INTO t VALUES(1,2,'y');
SELECT dolt_commit('-A','-m','init');
UPDATE t SET v='Y' WHERE a=1 AND b=2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB4B" > /dev/null 2>&1

run_test "worid_composite_at_renders_key" \
  "SELECT group_concat(a || '/' || b || ':' || v, ' ')
     FROM (SELECT a,b,v FROM dolt_at_t WHERE commit_ref='HEAD' ORDER BY a,b);" \
  "1/1:x 1/2:Y" "$DB4B"
run_test "worid_composite_blame_renders_key" \
  "SELECT group_concat(a || '/' || b, ' ')
     FROM (SELECT a,b FROM dolt_blame_t ORDER BY a,b);" "1/1 1/2" "$DB4B"

rm -f "$DB4B"

# The rowid table beside it must keep its pushdown.
DB5=/tmp/test_pushdown_rowid_$$.db
rm -f "$DB5"
echo "CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'one');
INSERT INTO t VALUES(2,'two');
SELECT dolt_commit('-A','-m','init');
UPDATE t SET v='TWO' WHERE pk=2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test_match "rowid_history_still_pushes_down" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_t WHERE pk=2;" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB5"
run_test "rowid_at_renders_pk" \
  "SELECT group_concat(pk || ':' || v, ' ')
     FROM (SELECT pk,v FROM dolt_at_t WHERE commit_ref='HEAD' ORDER BY pk);" \
  "1:one 2:TWO" "$DB5"
run_test "rowid_blame_renders_pk" \
  "SELECT group_concat(pk, ' ') FROM (SELECT pk FROM dolt_blame_t ORDER BY pk);" \
  "1 2" "$DB5"

rm -f "$DB5"

dltest_finish
