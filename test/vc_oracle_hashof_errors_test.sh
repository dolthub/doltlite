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

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

dolt_repo_setup() {
  local repo="$1" sql="$2"
  mkdir -p "$repo"
  (
    cd "$repo" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    printf "%s\n" "$sql" | "$DOLT" sql >/dev/null 2>"$repo/.setup.err"
  )
}

oracle_both_error() {
  local name="$1" query="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl"

  printf "%s\n%s\n" "$SEED" "$query" \
    | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  local dl_rc=$?

  local dolt_seed
  dolt_seed=$(vc_oracle_translate_for_dolt "$SEED")
  dolt_repo_setup "$dir/dt" "$dolt_seed"
  (cd "$dir/dt" && "$DOLT" sql -q "$query") >"$dir/dt.out" 2>"$dir/dt.err"
  local dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error; dl_rc=$dl_rc dt_rc=$dt_rc)"
    echo "    doltlite out: $(cat "$dir/dl.out" 2>/dev/null)"
    echo "    doltlite err: $(cat "$dir/dl.err" 2>/dev/null)"
    echo "    dolt out:     $(cat "$dir/dt.out" 2>/dev/null)"
    echo "    dolt err:     $(cat "$dir/dt.err" 2>/dev/null)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_hashof* error parity ==="
echo ""

echo "--- invalid refs and tables must error (not silently NULL) ---"

oracle_both_error "hashof_bogus"            "SELECT dolt_hashof('not_a_real_branch');"
oracle_both_error "hashof_empty_string"     "SELECT dolt_hashof('');"
oracle_both_error "hashof_table_missing"    "SELECT dolt_hashof_table('not_a_real_table');"
oracle_both_error "hashof_table_empty"      "SELECT dolt_hashof_table('');"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
