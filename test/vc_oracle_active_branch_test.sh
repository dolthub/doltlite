#!/bin/bash
#
# Version-control oracle test: active_branch()
#
# Compares active_branch() output from doltlite and Dolt across
# branch operations: default branch, checkout, branch creation,
# and checkout back.
#
# Usage: bash vc_oracle_active_branch_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\nSELECT active_branch();\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | tr -d '\r' | tail -1)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf 'SELECT active_branch();\n'
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tail -1 | tr -d '"\r')

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite: '$dl_out'"
    echo "    dolt:     '$dt_out'"
  fi
}

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "=== Version Control Oracle Tests: active_branch() ==="
echo ""

echo "--- default branch ---"

oracle "default_is_main" "
$SEED
"

echo "--- after checkout ---"

oracle "checkout_feature" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
"

echo "--- back to main ---"

oracle "checkout_back_to_main" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
"

echo "--- checkout -b ---"

oracle "checkout_create_branch" "
$SEED
SELECT dolt_checkout('-b', 'new_branch');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
