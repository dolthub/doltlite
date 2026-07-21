#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

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
  ( cd "$parent" && printf "%s\n" "$query" | "$DOLT" --use-db "$repo_name/$branch" sql -r csv -c ) \
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
    "$DOLT" config --global --add user.name "CI" >/dev/null &&
    "$DOLT" config --global --add user.email "ci@example.com" >/dev/null &&
    "$DOLT" init >/dev/null &&
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

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_with_query() {
  local name="$1" dbspec="$2" branch="$3" query="$4"
  local dir="$TMPROOT/$name"
  local dl_out dt_out

  setup_pair "$dir"

  dl_out=$(run_dl "$dbspec" "$query" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$branch" "$query" 2>>"$dir/dt.err")

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_with_mutation() {
  local name="$1" setup_extra="$2" dbspec="$3" branch="$4" query="$5"
  local dir="$TMPROOT/$name"
  local dl_out dt_out

  setup_pair "$dir"
  if [ -n "$setup_extra" ]; then
    run_dl "$dir/dl/db.sqlite" "$setup_extra" >/dev/null 2>>"$dir/dl.err"
    (
      cd "$dir/dt" || exit 1
      printf "%s\n" "$(printf "%s" "$setup_extra" | sed "s/SELECT dolt_/CALL dolt_/g")" | "$DOLT" sql -c >/dev/null 2>>"$dir/dt.err"
    )
  fi

  dl_out=$(run_dl "$dbspec" "$query" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$branch" "$query" 2>>"$dir/dt.err")

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_error() {
  local name="$1" setup_extra="$2" dbspec="$3" branch="$4" query="$5"
  local dir="$TMPROOT/$name"
  local dl_rc dt_rc

  setup_pair "$dir"
  if [ -n "$setup_extra" ]; then
    run_dl "$dir/dl/db.sqlite" "$setup_extra" >/dev/null 2>>"$dir/dl.err"
    (
      cd "$dir/dt" || exit 1
      printf "%s\n" "$(printf "%s" "$setup_extra" | sed "s/SELECT dolt_/CALL dolt_/g")" | "$DOLT" sql -c >/dev/null 2>>"$dir/dt.err"
    )
  fi

  run_dl "$dbspec" "$query" >/dev/null 2>>"$dir/dl.err"
  dl_rc=$?
  run_dt "$dir/dt" "$branch" "$query" >/dev/null 2>>"$dir/dt.err"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

echo "=== Oracle Tests: Connection Branch Selection ==="
echo ""

oracle "default_open_uses_main" "$TMPROOT/default_open_uses_main/dl/db.sqlite" "main"
oracle "at_branch_selects_branch" "$TMPROOT/at_branch_selects_branch/dl/db.sqlite@side" "side"
oracle "slash_branch_selects_branch" "$TMPROOT/slash_branch_selects_branch/dl/db.sqlite/side" "side"

oracle_with_mutation "renamed_branch_open_selects_branch" \
  "SELECT dolt_branch('-m','side','renamed');" \
  "$TMPROOT/renamed_branch_open_selects_branch/dl/db.sqlite@renamed" \
  "renamed" \
  "SELECT active_branch() AS value UNION ALL SELECT v FROM t WHERE id=1;"

oracle_with_mutation "copied_branch_open_selects_branch" \
  "SELECT dolt_branch('-c','side','copy');" \
  "$TMPROOT/copied_branch_open_selects_branch/dl/db.sqlite/copy" \
  "copy" \
  "SELECT active_branch() AS value UNION ALL SELECT v FROM t WHERE id=1;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
