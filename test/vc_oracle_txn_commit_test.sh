#!/bin/bash










set -u
DOLTLITE="${1:?usage: $0 <doltlite> [dolt]}"
DOLT="${2:-}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
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

  if [ "$DL_A" = "$DOLT_A" ]; then
    pass_name "begin_commit_rollback_matches_dolt"
  else
    fail_name "begin_commit_rollback_matches_dolt"
    echo "    doltlite=$DL_A dolt=$DOLT_A"
  fi
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

  if [ "$DL_B" = "$DOLT_B" ]; then
    pass_name "savepoint_commit_rollback_to_matches_dolt"
  else
    fail_name "savepoint_commit_rollback_to_matches_dolt"
    echo "    doltlite=$DL_B dolt=$DOLT_B"
  fi
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

  if [ "$DL_H" = "$DOLT_H" ]; then
    pass_name "begin_bad_commit_option_matches_dolt"
  else
    fail_name "begin_bad_commit_option_matches_dolt"
    echo "    doltlite=$DL_H dolt=$DOLT_H"
  fi
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

  if [ "$DL_I" = "$DOLT_I" ]; then
    pass_name "nested_savepoint_bad_commit_option_matches_dolt"
  else
    fail_name "nested_savepoint_bad_commit_option_matches_dolt"
    echo "    doltlite=$DL_I dolt=$DOLT_I"
  fi
fi

if [ "$DL_I" = "1" ]; then
  pass_name "nested_savepoint_bad_commit_option_rolls_back_on_reopen"
else
  fail_name "nested_savepoint_bad_commit_option_rolls_back_on_reopen"
  echo "    expected 1, got $DL_I"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
