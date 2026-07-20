#!/bin/bash

set -u
DOLTLITE="${1:?usage: $0 <doltlite> [dolt]}"
DOLT="${2:-}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

expect_line() {
  local name="$1" expected="$2" output="$3"
  if printf '%s\n' "$output" | grep -Fqx -- "$expected"; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    missing line: $expected"
    echo "    output: $output"
  fi
}

expect_contains() {
  local name="$1" expected="$2" output="$3"
  if printf '%s\n' "$output" | grep -Fq -- "$expected"; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    missing text: $expected"
    echo "    output: $output"
  fi
}

dl_query() {
  local db="$1"; shift
  "$DOLTLITE" "$db" "$@" 2>/dev/null
}

dolt_query() {
  local dir="$1"; shift
  cd "$dir" && "$DOLT" sql -c -q "$@" -r csv 2>/dev/null | tail -1
  cd - >/dev/null
}

echo "=== Transaction + dolt_commit Oracle Tests ==="

echo ""
echo "--- BEGIN + dolt_commit + ROLLBACK ---"

DB="$TMPROOT/a.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
BEGIN;
INSERT INTO t VALUES(2,'in_txn');
SELECT dolt_commit('-A','-m','c1');
ROLLBACK;
SQL
)" >/dev/null
DL_A=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_A_DIR="$TMPROOT/dolt_a"
  mkdir -p "$DOLT_A_DIR"
  (cd "$DOLT_A_DIR" && "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1)
  DOLT_A=$(cd "$DOLT_A_DIR" && "$DOLT" sql -c -r csv <<'SQL' 2>/dev/null | grep '^[0-9][0-9]*$' | tail -1
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
BEGIN;
INSERT INTO t VALUES(2,'in_txn');
CALL dolt_commit('-A','-m','c1');
ROLLBACK;
SELECT count(*) AS c FROM t;
SQL
)

  vc_oracle_assert_match "begin_commit_rollback_matches_dolt" "$DL_A" "$DOLT_A"
fi

if [ "$DL_A" = "2" ]; then
  pass_name "begin_commit_rollback_keeps_row"
else
  fail_name "begin_commit_rollback_keeps_row"
  echo "    expected 2, got $DL_A"
fi

echo ""
echo "--- SAVEPOINT + dolt_commit + ROLLBACK TO ---"

DB="$TMPROOT/b.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'in_sp');
SELECT dolt_commit('-A','-m','c1');
ROLLBACK TO sp1;
SQL
)" >/dev/null
DL_B=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_B_DIR="$TMPROOT/dolt_b"
  mkdir -p "$DOLT_B_DIR"
  (cd "$DOLT_B_DIR" && "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1)
  DOLT_B=$(cd "$DOLT_B_DIR" && "$DOLT" sql -c -r csv <<'SQL' 2>/dev/null | grep '^[0-9][0-9]*$' | tail -1
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'in_sp');
CALL dolt_commit('-A','-m','c1');
ROLLBACK TO sp1;
SELECT count(*) AS c FROM t;
SQL
)

  vc_oracle_assert_match "savepoint_commit_rollback_to_matches_dolt" "$DL_B" "$DOLT_B"
fi

if [ "$DL_B" = "2" ]; then
  pass_name "savepoint_commit_rollback_to_keeps_row"
else
  fail_name "savepoint_commit_rollback_to_keeps_row"
  echo "    expected 2, got $DL_B"
fi

echo ""
echo "--- No transaction + dolt_commit (baseline) ---"

DB="$TMPROOT/c.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
SQL
)" >/dev/null
DL_C=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_C" = "2" ]; then
  pass_name "no_txn_commit_works"
else
  fail_name "no_txn_commit_works"
  echo "    expected 2, got $DL_C"
fi

echo ""
echo "--- BEGIN + multiple inserts + dolt_commit + ROLLBACK ---"

DB="$TMPROOT/d.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c1');
ROLLBACK;
SQL
)" >/dev/null
DL_D=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_D" = "3" ]; then
  pass_name "begin_multi_insert_commit_rollback"
else
  fail_name "begin_multi_insert_commit_rollback"
  echo "    expected 3, got $DL_D"
fi

echo ""
echo "--- Nested savepoints + dolt_commit ---"

DB="$TMPROOT/e.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SAVEPOINT outer;
INSERT INTO t VALUES(1,'a');
SAVEPOINT inner;
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
ROLLBACK TO inner;
ROLLBACK TO outer;
SQL
)" >/dev/null
DL_E=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_E" = "2" ]; then
  pass_name "nested_savepoint_commit_keeps_all"
else
  fail_name "nested_savepoint_commit_keeps_all"
  echo "    expected 2, got $DL_E"
fi

echo ""
echo "--- dolt_commit without open txn ---"

DB="$TMPROOT/f.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
SQL
)" >/dev/null
DL_F=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_F" = "2" ]; then
  pass_name "commit_without_txn_works"
else
  fail_name "commit_without_txn_works"
  echo "    expected 2, got $DL_F"
fi

echo ""
echo "--- Reopen after BEGIN + dolt_commit (persistence) ---"

DB="$TMPROOT/g.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1,'persisted');
SELECT dolt_commit('-A','-m','c1');
SQL
)" >/dev/null

DL_G=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_G" = "1" ]; then
  pass_name "reopen_after_begin_commit_persists"
else
  fail_name "reopen_after_begin_commit_persists"
  echo "    expected 1, got $DL_G"
fi

echo ""
echo "--- BEGIN + bad dolt_commit option ---"

DB="$TMPROOT/h.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','c1');
BEGIN;
INSERT INTO t VALUES(2,'dirty');
SELECT dolt_commit('--bogus');
SQL
)" >/dev/null
DL_H=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_H_DIR="$TMPROOT/dolt_h"
  mkdir -p "$DOLT_H_DIR" && cd "$DOLT_H_DIR" && dolt init >/dev/null 2>&1
  dolt sql 2>/dev/null <<'SQL'
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
CALL dolt_commit('-Am','c1');
BEGIN;
INSERT INTO t VALUES(2,'dirty');
CALL dolt_commit('--bogus');
SQL
  DOLT_H=$(dolt_query "$DOLT_H_DIR" "SELECT count(*) FROM t")
  cd - >/dev/null

  vc_oracle_assert_match "begin_bad_commit_option_matches_dolt" "$DL_H" "$DOLT_H"
fi

if [ "$DL_H" = "1" ]; then
  pass_name "begin_bad_commit_option_rolls_back_on_reopen"
else
  fail_name "begin_bad_commit_option_rolls_back_on_reopen"
  echo "    expected 1, got $DL_H"
fi

echo ""
echo "--- Nested savepoint + bad dolt_commit option ---"

DB="$TMPROOT/i.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','c1');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'dirty');
SELECT dolt_commit('--bogus');
SQL
)" >/dev/null
DL_I=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_I_DIR="$TMPROOT/dolt_i"
  mkdir -p "$DOLT_I_DIR" && cd "$DOLT_I_DIR" && dolt init >/dev/null 2>&1
  dolt sql 2>/dev/null <<'SQL'
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
CALL dolt_commit('-Am','c1');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'dirty');
CALL dolt_commit('--bogus');
SQL
  DOLT_I=$(dolt_query "$DOLT_I_DIR" "SELECT count(*) FROM t")
  cd - >/dev/null

  vc_oracle_assert_match "nested_savepoint_bad_commit_option_matches_dolt" "$DL_I" "$DOLT_I"
fi

if [ "$DL_I" = "1" ]; then
  pass_name "nested_savepoint_bad_commit_option_rolls_back_on_reopen"
else
  fail_name "nested_savepoint_bad_commit_option_rolls_back_on_reopen"
  echo "    expected 1, got $DL_I"
fi

echo ""
echo "--- ROLLBACK TO nested savepoint before dolt_commit ---"

DB="$TMPROOT/j.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
BEGIN;
INSERT INTO t VALUES(2,'outer');
SAVEPOINT inner;
INSERT INTO t VALUES(3,'inner');
ROLLBACK TO inner;
RELEASE inner;
SELECT dolt_commit('-A','-m','outer-only');
ROLLBACK;
SQL
)" >/dev/null
DL_J=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_J_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")

if [ "$DL_J" = "1:base,2:outer" ]; then
  pass_name "nested_savepoint_rollback_before_commit_keeps_only_unrolled_rows"
else
  fail_name "nested_savepoint_rollback_before_commit_keeps_only_unrolled_rows"
  echo "    expected 1:base,2:outer, got $DL_J"
fi

if [ "$DL_J_STATUS" = "0" ]; then
  pass_name "nested_savepoint_rollback_before_commit_leaves_clean_status"
else
  fail_name "nested_savepoint_rollback_before_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_J_STATUS"
fi

echo ""
echo "--- DDL in nested savepoint + dolt_commit ---"

DB="$TMPROOT/k.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
BEGIN;
SAVEPOINT ddl_sp;
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(10,'nested-ddl');
SELECT dolt_commit('-A','-m','ddl');
ROLLBACK TO ddl_sp;
RELEASE ddl_sp;
ROLLBACK;
SQL
)" >/dev/null
DL_K=$(dl_query "$DB" "SELECT (SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='u') || ':' || (SELECT count(*) FROM u);")
DL_K_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")
DL_K_SIG=$(dl_query "$DB" "SELECT count(*) || ':' || sum(id) FROM u;")

if [ -n "$DOLT" ]; then
  DOLT_K_DIR="$TMPROOT/dolt_k"
  mkdir -p "$DOLT_K_DIR"
  (cd "$DOLT_K_DIR" && "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1)
  (cd "$DOLT_K_DIR" && "$DOLT" sql -c -r csv <<'SQL' >/dev/null 2>/dev/null
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
CALL dolt_commit('-A','-m','base');
BEGIN;
SAVEPOINT ddl_sp;
CREATE TABLE u(id INT PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(10,'nested-ddl');
CALL dolt_commit('-A','-m','ddl');
ROLLBACK TO ddl_sp;
RELEASE ddl_sp;
ROLLBACK;
SQL
)
  DOLT_K_SIG=$(dolt_query "$DOLT_K_DIR" "SELECT concat(count(*), ':', sum(id)) FROM u;")

  if [ "$DL_K_SIG" = "$DOLT_K_SIG" ]; then
    pass_name "nested_savepoint_ddl_commit_matches_dolt"
  else
    fail_name "nested_savepoint_ddl_commit_matches_dolt"
    echo "    doltlite=$DL_K_SIG dolt=$DOLT_K_SIG"
  fi
fi

if [ "$DL_K" = "1:1" ]; then
  pass_name "nested_savepoint_ddl_commit_persists_schema_and_rows"
else
  fail_name "nested_savepoint_ddl_commit_persists_schema_and_rows"
  echo "    expected 1:1, got $DL_K"
fi

if [ "$DL_K_STATUS" = "0" ]; then
  pass_name "nested_savepoint_ddl_commit_leaves_clean_status"
else
  fail_name "nested_savepoint_ddl_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_K_STATUS"
fi

echo ""
echo "--- Reuse connection after nested savepoint + dolt_commit ---"

DB="$TMPROOT/l.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
SAVEPOINT outer_sp;
INSERT INTO t VALUES(1,'outer');
SAVEPOINT inner_sp;
INSERT INTO t VALUES(2,'inner');
SELECT dolt_commit('-A','-m','nested');
BEGIN;
INSERT INTO t VALUES(3,'rolled-back-after');
ROLLBACK;
BEGIN;
INSERT INTO t VALUES(4,'committed-after');
COMMIT;
SELECT dolt_commit('-A','-m','after');
SQL
)" >/dev/null
DL_L=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_L_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")
DL_L_SIG=$(dl_query "$DB" "SELECT count(*) || ':' || sum(id) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_L_DIR="$TMPROOT/dolt_l"
  mkdir -p "$DOLT_L_DIR"
  (cd "$DOLT_L_DIR" && "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1)
  (cd "$DOLT_L_DIR" && "$DOLT" sql -c -r csv <<'SQL' >/dev/null 2>/dev/null
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
BEGIN;
SAVEPOINT outer_sp;
INSERT INTO t VALUES(1,'outer');
SAVEPOINT inner_sp;
INSERT INTO t VALUES(2,'inner');
CALL dolt_commit('-A','-m','nested');
BEGIN;
INSERT INTO t VALUES(3,'rolled-back-after');
ROLLBACK;
BEGIN;
INSERT INTO t VALUES(4,'committed-after');
COMMIT;
CALL dolt_commit('-A','-m','after');
SQL
)
  DOLT_L_SIG=$(dolt_query "$DOLT_L_DIR" "SELECT concat(count(*), ':', sum(id)) FROM t;")

  if [ "$DL_L_SIG" = "$DOLT_L_SIG" ]; then
    pass_name "nested_savepoint_commit_reuse_matches_dolt"
  else
    fail_name "nested_savepoint_commit_reuse_matches_dolt"
    echo "    doltlite=$DL_L_SIG dolt=$DOLT_L_SIG"
  fi
fi

if [ "$DL_L" = "1:outer,2:inner,4:committed-after" ]; then
  pass_name "nested_savepoint_commit_allows_later_transactions"
else
  fail_name "nested_savepoint_commit_allows_later_transactions"
  echo "    expected 1:outer,2:inner,4:committed-after, got $DL_L"
fi

if [ "$DL_L_STATUS" = "0" ]; then
  pass_name "nested_savepoint_commit_later_transactions_leave_clean_status"
else
  fail_name "nested_savepoint_commit_later_transactions_leave_clean_status"
  echo "    expected clean status, got $DL_L_STATUS"
fi

echo ""
echo "--- BEGIN + BEGIN + COMMIT + dolt_commit ---"

DB="$TMPROOT/m.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1,'in-outer');
BEGIN;
INSERT INTO t VALUES(2,'after-bad-begin');
COMMIT;
SELECT dolt_commit('-A','-m','after-sql-commit');
ROLLBACK;
SQL
)" >/dev/null
DL_M=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_M_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")

if [ "$DL_M" = "1:in-outer,2:after-bad-begin" ]; then
  pass_name "begin_begin_commit_then_dolt_commit_keeps_sql_commit"
else
  fail_name "begin_begin_commit_then_dolt_commit_keeps_sql_commit"
  echo "    expected 1:in-outer,2:after-bad-begin, got $DL_M"
fi

if [ "$DL_M_STATUS" = "0" ]; then
  pass_name "begin_begin_commit_then_dolt_commit_leaves_clean_status"
else
  fail_name "begin_begin_commit_then_dolt_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_M_STATUS"
fi

echo ""
echo "--- BEGIN + BEGIN + dolt_commit + COMMIT ---"

DB="$TMPROOT/n.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1,'in-outer');
BEGIN;
INSERT INTO t VALUES(2,'after-bad-begin');
SELECT dolt_commit('-A','-m','during-sql-transaction');
COMMIT;
ROLLBACK;
SQL
)" >/dev/null
DL_N=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_N_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")

if [ "$DL_N" = "1:in-outer,2:after-bad-begin" ]; then
  pass_name "begin_begin_dolt_commit_then_commit_keeps_dolt_commit"
else
  fail_name "begin_begin_dolt_commit_then_commit_keeps_dolt_commit"
  echo "    expected 1:in-outer,2:after-bad-begin, got $DL_N"
fi

if [ "$DL_N_STATUS" = "0" ]; then
  pass_name "begin_begin_dolt_commit_then_commit_leaves_clean_status"
else
  fail_name "begin_begin_dolt_commit_then_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_N_STATUS"
fi

echo ""
echo "--- BEGIN IMMEDIATE + BEGIN + COMMIT + dolt_commit ---"

DB="$TMPROOT/o.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN IMMEDIATE;
INSERT INTO t VALUES(1,'in-immediate');
BEGIN;
INSERT INTO t VALUES(2,'after-bad-begin');
COMMIT;
SELECT dolt_commit('-A','-m','after-sql-commit');
ROLLBACK;
SQL
)" >/dev/null
DL_O=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_O_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")

if [ "$DL_O" = "1:in-immediate,2:after-bad-begin" ]; then
  pass_name "begin_immediate_begin_commit_then_dolt_commit_keeps_sql_commit"
else
  fail_name "begin_immediate_begin_commit_then_dolt_commit_keeps_sql_commit"
  echo "    expected 1:in-immediate,2:after-bad-begin, got $DL_O"
fi

if [ "$DL_O_STATUS" = "0" ]; then
  pass_name "begin_immediate_begin_commit_then_dolt_commit_leaves_clean_status"
else
  fail_name "begin_immediate_begin_commit_then_dolt_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_O_STATUS"
fi

echo ""
echo "--- BEGIN IMMEDIATE + BEGIN + dolt_commit + COMMIT ---"

DB="$TMPROOT/p.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN IMMEDIATE;
INSERT INTO t VALUES(1,'in-immediate');
BEGIN;
INSERT INTO t VALUES(2,'after-bad-begin');
SELECT dolt_commit('-A','-m','during-immediate-transaction');
COMMIT;
ROLLBACK;
SQL
)" >/dev/null
DL_P=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_P_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")

if [ "$DL_P" = "1:in-immediate,2:after-bad-begin" ]; then
  pass_name "begin_immediate_begin_dolt_commit_then_commit_keeps_dolt_commit"
else
  fail_name "begin_immediate_begin_dolt_commit_then_commit_keeps_dolt_commit"
  echo "    expected 1:in-immediate,2:after-bad-begin, got $DL_P"
fi

if [ "$DL_P_STATUS" = "0" ]; then
  pass_name "begin_immediate_begin_dolt_commit_then_commit_leaves_clean_status"
else
  fail_name "begin_immediate_begin_dolt_commit_then_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_P_STATUS"
fi

echo ""
echo "--- BEGIN IMMEDIATE + nested savepoint + dolt_commit ---"

DB="$TMPROOT/q.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN IMMEDIATE;
INSERT INTO t VALUES(1,'in-immediate');
SAVEPOINT inner;
INSERT INTO t VALUES(2,'in-savepoint');
SELECT dolt_commit('-A','-m','immediate-savepoint');
ROLLBACK TO inner;
COMMIT;
ROLLBACK;
SQL
)" >/dev/null
DL_Q=$(dl_query "$DB" "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);")
DL_Q_STATUS=$(dl_query "$DB" "SELECT count(*) FROM dolt_status;")

if [ "$DL_Q" = "1:in-immediate,2:in-savepoint" ]; then
  pass_name "begin_immediate_nested_savepoint_dolt_commit_keeps_all"
else
  fail_name "begin_immediate_nested_savepoint_dolt_commit_keeps_all"
  echo "    expected 1:in-immediate,2:in-savepoint, got $DL_Q"
fi

if [ "$DL_Q_STATUS" = "0" ]; then
  pass_name "begin_immediate_nested_savepoint_dolt_commit_leaves_clean_status"
else
  fail_name "begin_immediate_nested_savepoint_dolt_commit_leaves_clean_status"
  echo "    expected clean status, got $DL_Q_STATUS"
fi

echo ""
echo "--- Deferred FK failure under BEGIN ---"

DB="$TMPROOT/r.db"
rm -f "$DB"
DL_R_OUT=$("$DOLTLITE" "$DB" 2>&1 <<'SQL'
.bail off
PRAGMA foreign_keys=ON;
CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED
);
SELECT dolt_commit('-A','-m','schema');
BEGIN;
INSERT INTO c VALUES(1,99);
SELECT dolt_commit('-A','-m','invalid fk');
SELECT 'inside|' || count(*) FROM c;
ROLLBACK;
SELECT 'working|' || count(*) FROM c;
SELECT 'head|' || count(*) FROM dolt_at_c('HEAD');
SELECT 'badcommits|' || count(*) FROM dolt_log WHERE message='invalid fk';
SQL
)
expect_contains "deferred_fk_begin_reports_commit_error" \
  "FOREIGN KEY constraint failed" "$DL_R_OUT"
expect_line "deferred_fk_begin_keeps_transaction_open" "inside|1" "$DL_R_OUT"
expect_line "deferred_fk_begin_rollback_clears_working" "working|0" "$DL_R_OUT"
expect_line "deferred_fk_begin_does_not_publish_head" "head|0" "$DL_R_OUT"
expect_line "deferred_fk_begin_creates_no_commit" "badcommits|0" "$DL_R_OUT"

DL_R_REOPEN=$(dl_query "$DB" "
SELECT 'working|' || count(*) FROM c;
SELECT 'head|' || count(*) FROM dolt_at_c('HEAD');
SELECT dolt_reset('--hard');
SELECT 'fk|' || count(*) FROM pragma_foreign_key_check;")
expect_line "deferred_fk_begin_reopen_working_clean" "working|0" "$DL_R_REOPEN"
expect_line "deferred_fk_begin_reopen_head_clean" "head|0" "$DL_R_REOPEN"
expect_line "deferred_fk_begin_hard_reset_has_no_orphan" "fk|0" "$DL_R_REOPEN"

echo ""
echo "--- Deferred FK failure under top-level SAVEPOINT ---"

DB="$TMPROOT/s.db"
rm -f "$DB"
DL_S_OUT=$("$DOLTLITE" "$DB" 2>&1 <<'SQL'
.bail off
PRAGMA foreign_keys=ON;
CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED
);
SELECT dolt_commit('-A','-m','schema');
SAVEPOINT outer;
INSERT INTO c VALUES(1,99);
SELECT dolt_commit('-A','-m','invalid top savepoint');
SELECT 'inside|' || count(*) FROM c;
ROLLBACK TO outer;
RELEASE outer;
SELECT 'working|' || count(*) FROM c;
SELECT 'head|' || count(*) FROM dolt_at_c('HEAD');
SELECT 'badcommits|' || count(*) FROM dolt_log
  WHERE message='invalid top savepoint';
SQL
)
expect_contains "deferred_fk_top_savepoint_reports_commit_error" \
  "FOREIGN KEY constraint failed" "$DL_S_OUT"
expect_line "deferred_fk_top_savepoint_remains_rollbackable" "inside|1" "$DL_S_OUT"
expect_line "deferred_fk_top_savepoint_rollback_clears_working" "working|0" "$DL_S_OUT"
expect_line "deferred_fk_top_savepoint_does_not_publish_head" "head|0" "$DL_S_OUT"
expect_line "deferred_fk_top_savepoint_creates_no_commit" "badcommits|0" "$DL_S_OUT"

echo ""
echo "--- Deferred FK failure under nested SAVEPOINT ---"

DB="$TMPROOT/t.db"
rm -f "$DB"
DL_T_OUT=$("$DOLTLITE" "$DB" 2>&1 <<'SQL'
.bail off
PRAGMA foreign_keys=ON;
CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED
);
SELECT dolt_commit('-A','-m','schema');
BEGIN;
SAVEPOINT inner;
INSERT INTO c VALUES(1,99);
SELECT dolt_commit('-A','-m','invalid nested savepoint');
SELECT 'inside|' || count(*) FROM c;
ROLLBACK TO inner;
RELEASE inner;
COMMIT;
SELECT 'working|' || count(*) FROM c;
SELECT 'head|' || count(*) FROM dolt_at_c('HEAD');
SQL
)
expect_contains "deferred_fk_nested_savepoint_reports_commit_error" \
  "FOREIGN KEY constraint failed" "$DL_T_OUT"
expect_line "deferred_fk_nested_savepoint_remains_rollbackable" "inside|1" "$DL_T_OUT"
expect_line "deferred_fk_nested_savepoint_rollback_clears_working" "working|0" "$DL_T_OUT"
expect_line "deferred_fk_nested_savepoint_does_not_publish_head" "head|0" "$DL_T_OUT"

echo ""
echo "--- Repair deferred FK after failed dolt_commit ---"

DB="$TMPROOT/u.db"
rm -f "$DB"
DL_U_OUT=$("$DOLTLITE" "$DB" 2>&1 <<'SQL'
.bail off
PRAGMA foreign_keys=ON;
CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED
);
SELECT dolt_commit('-A','-m','schema');
BEGIN;
INSERT INTO c VALUES(1,99);
SELECT dolt_commit('-A','-m','invalid before repair');
INSERT INTO p VALUES(99);
SELECT 'retry_hash|' || length(dolt_commit('-A','-m','repaired'));
SELECT 'parent|' || count(*) FROM p;
SELECT 'child|' || count(*) FROM c;
SELECT 'fk|' || count(*) FROM pragma_foreign_key_check;
SELECT 'repaired_commits|' || count(*) FROM dolt_log WHERE message='repaired';
SELECT 'invalid_commits|' || count(*) FROM dolt_log
  WHERE message='invalid before repair';
SQL
)
expect_contains "deferred_fk_repair_first_commit_reports_error" \
  "FOREIGN KEY constraint failed" "$DL_U_OUT"
expect_line "deferred_fk_repair_retry_commits" "retry_hash|40" "$DL_U_OUT"
expect_line "deferred_fk_repair_keeps_parent" "parent|1" "$DL_U_OUT"
expect_line "deferred_fk_repair_keeps_child" "child|1" "$DL_U_OUT"
expect_line "deferred_fk_repair_is_valid" "fk|0" "$DL_U_OUT"
expect_line "deferred_fk_repair_creates_one_commit" "repaired_commits|1" "$DL_U_OUT"
expect_line "deferred_fk_repair_does_not_create_failed_commit" \
  "invalid_commits|0" "$DL_U_OUT"

DL_U_REOPEN=$(dl_query "$DB" "
PRAGMA foreign_keys=ON;
SELECT 'parent|' || count(*) FROM p;
SELECT 'child|' || count(*) FROM c;
SELECT 'head|' || count(*) FROM dolt_at_c('HEAD');
SELECT 'fk|' || count(*) FROM pragma_foreign_key_check;")
expect_line "deferred_fk_repair_reopen_parent" "parent|1" "$DL_U_REOPEN"
expect_line "deferred_fk_repair_reopen_child" "child|1" "$DL_U_REOPEN"
expect_line "deferred_fk_repair_reopen_head" "head|1" "$DL_U_REOPEN"
expect_line "deferred_fk_repair_reopen_valid" "fk|0" "$DL_U_REOPEN"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
