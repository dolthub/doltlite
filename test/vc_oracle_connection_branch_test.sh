#!/bin/bash
#
# Oracle test: connection-time branch selection.
#
# DoltLite selects a branch from the database path using either:
#   db.sqlite@branch
#   db.sqlite/branch
# and defaults to main when no branch is specified.
#
# Dolt exposes the same branch-selection semantics as a global CLI flag:
#   dolt --branch <branch> sql ...
#
# This oracle compares the resulting active_branch() and visible table
# contents across both engines.
#

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

run_dl() {
  local dbspec="$1" query="$2"
  printf ".headers off\n.mode list\n%s\n" "$query" \
    | "$DOLTLITE" "$dbspec"
}

run_dt() {
  local repo="$1" branch="$2" query="$3"
  local parent repo_name
  parent=$(dirname "$repo")
  repo_name=$(basename "$repo")
  ( cd "$parent" && "$DOLT" --use-db "$repo_name/$branch" sql -r csv -q "$query" ) \
    | tail -n +2 | tr -d '"' | tr -d '\r'
}

setup_pair() {
  local dir="$1"
  mkdir -p "$dir/dl" "$dir/dt"

  cat >"$dir/setup_dl.sql" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
UPDATE t SET v='side' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'side');
SELECT dolt_checkout('main');
SQL
  "$DOLTLITE" "$dir/dl/db.sqlite" <"$dir/setup_dl.sql" >/dev/null 2>"$dir/dl.err"

  cat >"$dir/setup_dt.sql" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
CALL dolt_commit('-Am', 'init');
CALL dolt_branch('side');
CALL dolt_checkout('side');
UPDATE t SET v='side' WHERE id=1;
CALL dolt_commit('-Am', 'side');
CALL dolt_checkout('main');
SQL
  (
    cd "$dir/dt" &&
    "$DOLT" init >/dev/null &&
    "$DOLT" config --local --add user.name "CI" >/dev/null &&
    "$DOLT" config --local --add user.email "ci@example.com" >/dev/null &&
    "$DOLT" sql <"$dir/setup_dt.sql" >/dev/null 2>"$dir/dt.err"
  )
}

oracle() {
  local name="$1" dbspec="$2" branch="$3"
  local dir="$TMPROOT/$name"
  local q="SELECT active_branch() AS value UNION ALL SELECT v FROM t WHERE id=1;"
  local dl_out dt_out

  setup_pair "$dir"

  dl_out=$(run_dl "$dbspec" "$q" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$branch" "$q" 2>>"$dir/dt.err")

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Oracle Tests: Connection Branch Selection ==="
echo ""

oracle "default_open_uses_main" "$TMPROOT/default_open_uses_main/dl/db.sqlite" "main"
oracle "at_branch_selects_branch" "$TMPROOT/at_branch_selects_branch/dl/db.sqlite@side" "side"
oracle "slash_branch_selects_branch" "$TMPROOT/slash_branch_selects_branch/dl/db.sqlite/side" "side"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
