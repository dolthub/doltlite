#!/bin/bash
DOLTLITE="${1:-./doltlite}"
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
SQL

# The plan lives in the session that started the interactive rebase, so the
# edit and --continue must run together (as a user does interactively).
run_test_match "invalid_plan_continue_errors" \
  "SELECT dolt_rebase('-i', 'main');
   UPDATE dolt_rebase SET action = 'oops' WHERE commit_message = 'f1';
   SELECT dolt_rebase('--continue');" \
  "rebase failed|no rebase in progress" \
  "$DB"
run_test "invalid_plan_branch_preserved" \
  "SELECT active_branch();" \
  "main" \
  "$DB"
run_test "invalid_plan_table_preserved" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase';" \
  "0" \
  "$DB"
run_test "invalid_plan_abort_works" \
  "SELECT dolt_rebase('--abort');" \
  "Error near line 1: no rebase in progress" \
  "$DB"
run_test "invalid_plan_abort_restores_branch" \
  "SELECT active_branch();" \
  "main" \
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
  "didn't identify any commits!" \
  "$DB2"
run_test "start_failure_restores_branch" \
  "SELECT active_branch();" \
  "main" \
  "$DB2"
run_test "start_failure_temp_branch_removed" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" \
  "$DB2"

# An interactive rebase mirrors its working set onto the branch a reopen would
# land on, and clears that branch when it finishes. Rebase only refuses to
# start over a dirty *current* branch, so uncommitted work on the other branch
# used to be destroyed. The two controls establish that neither a plain
# checkout roundtrip nor a non-interactive rebase ever did this.
seed_dirty_main() {
  rm -f "$1"
  cat <<'SQL' | "$DOLTLITE" "$1" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'f1');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'m1');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(99,'row99');
SQL
}

DB3=/tmp/test_rebase_dirty_$$.db
seed_dirty_main "$DB3"
run_test "interactive_rebase_keeps_other_branch_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   SELECT dolt_rebase('--continue');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "0
interactive rebase started on branch dolt_rebase_feat; adjust the rebase plan in the dolt_rebase table, then continue rebasing by calling dolt_rebase('--continue')
Successfully rebased and updated refs/heads/feat
0
1" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "interactive_rebase_abort_keeps_other_branch_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   SELECT dolt_rebase('--abort');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "^1$" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "checkout_roundtrip_keeps_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "^1$" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "noninteractive_rebase_keeps_other_branch_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('main');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "^1$" \
  "$DB3"

rm -f "$DB" "$DB2" "$DB3"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
