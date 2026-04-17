#!/bin/bash
#
# Oracle tests: SQL transaction interaction with dolt_commit.
#
# Verifies that dolt_commit() implicitly commits any open SQL
# transaction, matching Dolt's behavior. After dolt_commit(),
# ROLLBACK is a no-op and all pending DML is durable.
#
# Usage: bash test/vc_oracle_txn_commit_test.sh <doltlite> [dolt]
#

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
  cd "$dir" && dolt sql -q "$@" -r csv 2>/dev/null | tail -1
  cd - >/dev/null
}

echo "=== Transaction + dolt_commit Oracle Tests ==="

# ── Test A: BEGIN + dolt_commit + ROLLBACK ──────────────────
echo ""
echo "--- BEGIN + dolt_commit + ROLLBACK ---"

DB="$TMPROOT/a.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
BEGIN;
INSERT INTO t VALUES(2,'in_txn');
SELECT dolt_commit('-Am','c1');
ROLLBACK;
SQL
)" >/dev/null
DL_A=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_A_DIR="$TMPROOT/dolt_a"
  mkdir -p "$DOLT_A_DIR" && cd "$DOLT_A_DIR" && dolt init >/dev/null 2>&1
  dolt sql 2>/dev/null <<'SQL'
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
BEGIN;
INSERT INTO t VALUES(2,'in_txn');
CALL dolt_commit('-Am','c1');
ROLLBACK;
SQL
  DOLT_A=$(dolt_query "$DOLT_A_DIR" "SELECT count(*) FROM t")
  cd - >/dev/null

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

# ── Test B: SAVEPOINT + dolt_commit + ROLLBACK TO ──────────
echo ""
echo "--- SAVEPOINT + dolt_commit + ROLLBACK TO ---"

DB="$TMPROOT/b.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'in_sp');
SELECT dolt_commit('-Am','c1');
ROLLBACK TO sp1;
SQL
)" >/dev/null
DL_B=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ -n "$DOLT" ]; then
  DOLT_B_DIR="$TMPROOT/dolt_b"
  mkdir -p "$DOLT_B_DIR" && cd "$DOLT_B_DIR" && dolt init >/dev/null 2>&1
  dolt sql 2>/dev/null <<'SQL'
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'in_sp');
CALL dolt_commit('-Am','c1');
ROLLBACK TO sp1;
SQL
  DOLT_B=$(dolt_query "$DOLT_B_DIR" "SELECT count(*) FROM t")
  cd - >/dev/null

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

# ── Test C: No transaction + dolt_commit (baseline) ────────
echo ""
echo "--- No transaction + dolt_commit (baseline) ---"

DB="$TMPROOT/c.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-Am','c1');
SQL
)" >/dev/null
DL_C=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_C" = "2" ]; then
  pass_name "no_txn_commit_works"
else
  fail_name "no_txn_commit_works"
  echo "    expected 2, got $DL_C"
fi

# ── Test D: BEGIN + multiple inserts + dolt_commit + ROLLBACK ──
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
SELECT dolt_commit('-Am','c1');
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

# ── Test E: Nested savepoints + dolt_commit ────────────────
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
SELECT dolt_commit('-Am','c1');
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

# ── Test F: dolt_commit without open txn is fine ───────────
echo ""
echo "--- dolt_commit without open txn ---"

DB="$TMPROOT/f.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-Am','c2');
SQL
)" >/dev/null
DL_F=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_F" = "2" ]; then
  pass_name "commit_without_txn_works"
else
  fail_name "commit_without_txn_works"
  echo "    expected 2, got $DL_F"
fi

# ── Test G: Reopen after BEGIN + dolt_commit + crash ───────
echo ""
echo "--- Reopen after BEGIN + dolt_commit (persistence) ---"

DB="$TMPROOT/g.db"
rm -f "$DB"
dl_query "$DB" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1,'persisted');
SELECT dolt_commit('-Am','c1');
SQL
)" >/dev/null

# Reopen — the committed row must be there
DL_G=$(dl_query "$DB" "SELECT count(*) FROM t;")

if [ "$DL_G" = "1" ]; then
  pass_name "reopen_after_begin_commit_persists"
else
  fail_name "reopen_after_begin_commit_persists"
  echo "    expected 1, got $DL_G"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
