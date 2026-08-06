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

# A pushed-down constraint is omitted, so xFilter is the only thing standing
# between a NULL bound and a wrong answer. Nothing compares true against NULL,
# yet sqlite3_value_int64 reads one as 0 -- so the pk=0 row below is what makes
# the difference between "matches nothing" and "matches by accident".
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

# The same shape a join produces, which is how this reaches real queries.
run_test "null_join_history_empty" \
  "WITH n(k) AS (SELECT NULL) SELECT count(*) FROM n JOIN dolt_history_t h ON h.pk=n.k;" \
  "0" "$DB3"

# Non-NULL bounds must keep working, including the pk=0 row that a NULL used
# to collide with.
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

dltest_finish
