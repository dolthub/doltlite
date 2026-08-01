#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

check_eq() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $actual"
  fi
}

check_match() {
  local name="$1" pattern="$2" actual="$3"
  if echo "$actual" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $actual"
  fi
}

setup_db() {
  local db="$1"
  rm -f "$db"
  cat <<'SQL' | "$DOLTLITE" "$db" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat' WHERE id=1;
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_tag('v1');
SQL
}

echo "=== Doltlite dolt_connect_branch Tests ==="
echo ""

DB=/tmp/test_connect_branch_$$.db
setup_db "$DB"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); SELECT active_branch(); SELECT v FROM t WHERE id=1; SELECT count(*) FROM t;")
check_eq "basic_switch_active_and_data" $'0\nfeat\nfeat\n2' "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); SELECT dolt_connect_branch('main'); SELECT active_branch(); SELECT v FROM t WHERE id=1; SELECT count(*) FROM t;")
check_eq "switch_back_to_main" $'0\n0\nmain\nmain\n1' "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('main'); SELECT active_branch();")
check_eq "connect_to_current_branch_noop" $'0\nmain' "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); SELECT active_branch();" 2>&1)
check_eq "connect_does_not_change_default_open_returns" $'0\nfeat' "$res"

res=$("$DOLTLITE" "$DB" "SELECT active_branch(); SELECT v FROM t WHERE id=1;")
check_eq "connect_does_not_change_default_open_state" $'main\nmain' "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('nope');" 2>&1 || true)
check_match "connect_nonexistent_branch_errors" "branch not found" "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('');" 2>&1 || true)
check_match "connect_empty_string_errors" "branch name required" "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch(NULL);" 2>&1 || true)
check_match "connect_null_errors" "branch name required" "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch();" 2>&1 || true)
check_match "connect_zero_args_errors" "wrong number of arguments" "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('main','extra');" 2>&1 || true)
check_match "connect_two_args_errors" "wrong number of arguments" "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('v1');" 2>&1 || true)
check_match "connect_to_tag_rejected" "branch not found" "$res"

res=$("$DOLTLITE" "$DB" "BEGIN; SELECT dolt_connect_branch('feat'); ROLLBACK; SELECT active_branch(); SELECT v FROM t WHERE id=1;")
check_eq "connect_inside_txn_then_rollback_keeps_feat" $'0\nfeat\nfeat' "$res"

setup_db "$DB"
res=$("$DOLTLITE" "$DB" "SAVEPOINT sp; SELECT dolt_connect_branch('feat'); ROLLBACK TO sp; SELECT active_branch(); SELECT v FROM t WHERE id=1;")
check_eq "connect_inside_savepoint_then_rollback_keeps_feat" $'0\nfeat\nfeat' "$res"

setup_db "$DB"
res=$("$DOLTLITE" "$DB" "INSERT INTO t VALUES(99,'dirty'); SELECT dolt_connect_branch('feat'); SELECT count(*) FROM t WHERE id=99; SELECT v FROM t WHERE id=1;")
check_eq "connect_with_uncommitted_writes_switches" $'0\n0\nfeat' "$res"
res=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=99;")
check_eq "connect_with_uncommitted_writes_preserves_origin" "dirty" "$res"

setup_db "$DB"
res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); INSERT INTO t VALUES(3,'feat3'); SELECT dolt_commit('-A','-m','c2');" 2>&1)
check_eq "commit_after_connect_rc_line" "0" "$(echo "$res" | sed -n 1p)"
check_match "commit_after_connect_hash_line" "^[0-9a-f]{40}$" "$(echo "$res" | sed -n 2p)"
res_feat=$("$DOLTLITE" "$DB/feat" "SELECT count(*) FROM t;")
check_eq "commit_after_connect_persists_on_feat" "3" "$res_feat"
res_main=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check_eq "commit_after_connect_leaves_main_unchanged" "1" "$res_main"

setup_db "$DB"
res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); SELECT dolt_branch('-d','feat');" 2>&1)
check_match "connect_then_delete_current_branch_errors" "cannot delete" "$res"

setup_db "$DB"
"$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); INSERT INTO t VALUES(3,'unstaged');" >/dev/null
res=$("$DOLTLITE" "$DB" "SELECT dolt_connect_branch('feat'); SELECT staged FROM dolt_status WHERE table_name='t';")
check_eq "reconnect_preserves_unstaged_state" $'0\n0' "$res"

rm -f "$DB"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  printf "%b\n" "$ERRORS"
  exit 1
fi
