#!/bin/bash
DOLTLITE="${1:-./build/doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r
  r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r
  r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

echo "=== Doltlite Rebase Tests ==="
echo ""

DB=/tmp/test_rebase_$$.db; rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
INSERT INTO t VALUES (3, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');
INSERT INTO t VALUES (4, 4);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'oops' WHERE commit_message = 'f1';
SQL

run_test_match "invalid_plan_continue_errors" \
  "SELECT dolt_rebase('--continue');" \
  "first non-drop action must be pick or reword" \
  "$DB"
run_test "invalid_plan_branch_preserved" \
  "SELECT active_branch();" \
  "dolt_rebase_feat" \
  "$DB"
run_test "invalid_plan_table_preserved" \
  "SELECT count(*) FROM dolt_rebase;" \
  "3" \
  "$DB"
run_test "invalid_plan_abort_works" \
  "SELECT dolt_rebase('--abort');" \
  "Interactive rebase aborted" \
  "$DB"
run_test "invalid_plan_abort_restores_branch" \
  "SELECT active_branch();" \
  "feat" \
  "$DB"

DB2=/tmp/test_rebase2_$$.db; rm -f "$DB2"
cat <<'SQL' | "$DOLTLITE" "$DB2" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
CREATE TABLE dolt_rebase(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SQL

run_test_match "start_failure_errors" \
  "SELECT dolt_rebase('-i', 'main');" \
  "failed to create dolt_rebase table|table dolt_rebase already exists|already exists" \
  "$DB2"
run_test "start_failure_restores_branch" \
  "SELECT active_branch();" \
  "feat" \
  "$DB2"
run_test "start_failure_temp_branch_removed" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" \
  "$DB2"

rm -f "$DB" "$DB2"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
