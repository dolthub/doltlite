#!/bin/bash
#
# Version-control oracle test: dolt_checkout
#
# Runs identical checkout scenarios against doltlite and Dolt and compares
# the resulting (active_branch, table contents, dolt_status) post-state.
#
# Checkout has three overlapping behaviors that the oracle has to verify:
#   1. Branch switch: HEAD/active_branch moves and the visible data swaps
#      to the target branch's working set.
#   2. Branch create-and-switch (`-b`): same as above plus a new branch
#      ref is created at the current commit.
#   3. Per-table checkout: when the first arg is not a branch, treat the
#      args as table names and revert those tables in the working set
#      back to the staged catalog (or HEAD if nothing is staged). This
#      is the `git checkout -- file` analogue.
#
# Comparing only the active branch would miss revert-table bugs; comparing
# only the table contents would miss bugs that switch HEAD without moving
# the data; comparing only dolt_status would miss the branch ref. So the
# oracle compares all three concatenated.
#
# Usage: bash vc_oracle_checkout_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize_branch() {
  tr -d '\r' | awk -F'\t' 'NF >= 2 && $1 == "B" { print }'
}

normalize_rows() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "R" { print }' \
    | sort
}

normalize_status() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 4 && $1 == "S" { print }' \
    | sort -t$'\t' -k2,2 -k3,3 -k4,4
}

# $1=name, $2=setup SQL using SELECT dolt_*() form. The harness rewrites
# to CALL dolt_*() for Dolt. The setup must leave the table named `t`
# in some final state — we always SELECT from it for comparison.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_branch dl_rows dl_status
  dl_branch=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'B' || char(9) || active_branch();\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_branch)
  dl_rows=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'R' || char(9) || id || char(9) || v FROM t ORDER BY id;\n" "$setup" \
            | "$DOLTLITE" "$dir/dl/db.r" 2>>"$dir/dl.err" \
            | grep -v '^[0-9]*$' \
            | grep -v '^[0-9a-f]\{40\}$' \
            | normalize_rows)
  dl_status=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S' || char(9) || table_name || char(9) || staged || char(9) || status FROM dolt_status;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_status)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  # Run setup + comparison queries in a SINGLE dolt sql invocation —
  # CALL dolt_checkout is session-scoped in the dolt CLI, so a second
  # `dolt sql` invocation would lose the branch switch and start back
  # on main. Same goes for the working set.
  local dt_branch dt_rows dt_status
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "SELECT concat('B', char(9), active_branch());"
      echo "SELECT concat('R', char(9), id, char(9), v) FROM t ORDER BY id;"
      echo "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_branch=$(tr -d '"' < "$dir/dt.raw" | normalize_branch)
  dt_rows=$(tr -d '"' < "$dir/dt.raw" | normalize_rows)
  dt_status=$(tr -d '"' < "$dir/dt.raw" | normalize_status)

  local dl_combined dt_combined
  dl_combined="$dl_branch"$'\n'"$dl_rows"$'\n'"$dl_status"
  dt_combined="$dt_branch"$'\n'"$dt_rows"$'\n'"$dt_status"

  if [ "$dl_combined" = "$dt_combined" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite branch:"; echo "$dl_branch" | sed 's/^/      /'
    echo "    dolt branch:";     echo "$dt_branch" | sed 's/^/      /'
    echo "    doltlite rows:";   echo "$dl_rows"   | sed 's/^/      /'
    echo "    dolt rows:";       echo "$dt_rows"   | sed 's/^/      /'
    echo "    doltlite status:"; echo "$dl_status" | sed 's/^/      /'
    echo "    dolt status:";     echo "$dt_status" | sed 's/^/      /'
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_err=0
  echo "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then dl_err=1; fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_err=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
  )
  if grep -qiE 'error|fail' "$dir/dt.out" "$dir/dt.err" 2>/dev/null; then dt_err=1; fi

  if [ "$dl_err" = 1 ] && [ "$dt_err" = 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite errored: $([ $dl_err = 1 ] && echo yes || echo NO)"
    echo "    dolt errored:     $([ $dt_err = 1 ] && echo yes || echo NO)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_checkout ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- branch switch ---"

oracle "switch_to_existing" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
"

oracle "switch_to_main_noop" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('main');
"

oracle "switch_then_back" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
"

oracle "switch_sees_branch_data" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 'feature_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
"

oracle "main_unchanged_after_branch_commit" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 'feature_only');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
"

echo "--- create-and-switch (-b) ---"

oracle "dash_b_creates_and_switches" "
$SEED
SELECT dolt_checkout('-b', 'newfeat');
"

oracle "dash_b_then_commit" "
$SEED
SELECT dolt_checkout('-b', 'newfeat');
INSERT INTO t VALUES (2, 'b_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

oracle "dash_b_then_switch_back_to_main" "
$SEED
SELECT dolt_checkout('-b', 'newfeat');
INSERT INTO t VALUES (2, 'b_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('main');
"

echo "--- per-table checkout ---"

oracle "revert_single_table_working" "
$SEED
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_checkout('t');
"

oracle "revert_table_with_uncommitted_insert" "
$SEED
INSERT INTO t VALUES (2, 'uncommitted');
SELECT dolt_checkout('t');
"

oracle "revert_table_with_delete" "
$SEED
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
DELETE FROM t WHERE id=2;
SELECT dolt_checkout('t');
"

echo "--- error paths ---"

oracle_error "checkout_nonexistent" "
$SEED
SELECT dolt_checkout('nope');
"

oracle_error "dash_b_existing_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('-b', 'feature');
"

oracle_error "no_args" "
$SEED
SELECT dolt_checkout();
"

oracle_error "dash_b_no_name" "
$SEED
SELECT dolt_checkout('-b');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
