#!/bin/bash
#
# Version-control oracle test: dolt_branch (the function, not the vtable)
#
# Runs identical dolt_branch scenarios against doltlite and Dolt and
# compares the resulting dolt_branches post-state. Covers create, delete,
# force-delete (-D), copy (-c), move/rename (-m), force-create (-f), and
# creating at a start point. Also covers the error paths where both
# engines should reject an invalid call.
#
# Usage: bash vc_oracle_branch_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Replace each distinct hash with H1, H2, ... in first-appearance order.
normalize() {
  tr -d '\r' | awk -F'\t' '
    {
      h = $2
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $2 = seen[h]
      print $1 "\t" $2 "\t" $3
    }
  '
}

# Compare post-state (name, hash, dirty) across all branches.
# Committer/email/date/message are excluded for the same reason as the
# branches vtable oracle — process-derived values.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT name || char(9) || hash || char(9) || dirty FROM dolt_branches ORDER BY name'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(name, char(9), hash, char(9), dirty) FROM dolt_branches ORDER BY name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" \
           | tr -d '"' \
           | sed -E 's/\ttrue$/\t1/; s/\tfalse$/\t0/' \
           | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

echo "=== Version Control Oracle Tests: dolt_branch ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- create ---"

oracle "create_simple" "
$SEED
SELECT dolt_branch('feature');
"

oracle "create_at_start_point" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_branch('historical', (SELECT commit_hash FROM dolt_log WHERE message='c1'));
"

echo "--- delete ---"

oracle "delete_existing" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-d', 'feature');
"

oracle "delete_force" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-D', 'feature');
"

oracle_error "delete_unmerged_requires_force" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d', 'feature');
"

oracle "delete_unmerged_force" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
SELECT dolt_branch('-D', 'feature');
"

echo "--- copy ---"

oracle "copy_from_main" "
$SEED
SELECT dolt_branch('-c', 'main', 'copy');
"

oracle "copy_long_flag" "
$SEED
SELECT dolt_branch('--copy', 'main', 'copy');
"

echo "--- move / rename ---"

oracle "move_non_current" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-m', 'feature', 'renamed');
"

oracle "move_current_branch" "
$SEED
SELECT dolt_branch('-m', 'main', 'trunk');
"

oracle "move_long_flag" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('--move', 'feature', 'other');
"

echo "--- force create ---"

oracle "force_create_overwrites" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_branch('-f', 'feature');
"

echo "--- error paths ---"

oracle_error "delete_nonexistent" "
$SEED
SELECT dolt_branch('-d', 'nope');
"

oracle_error "force_delete_nonexistent" "
$SEED
SELECT dolt_branch('-D', 'nope');
"

oracle_error "copy_source_missing" "
$SEED
SELECT dolt_branch('-c', 'nope', 'dest');
"

oracle_error "move_source_missing" "
$SEED
SELECT dolt_branch('-m', 'nope', 'dest');
"

oracle_error "create_duplicate" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('feature');
"

oracle_error "create_empty_name" "
$SEED
SELECT dolt_branch('');
"

oracle_error "copy_empty_source" "
$SEED
SELECT dolt_branch('-c', '', 'dest');
"

oracle_error "copy_empty_dest" "
$SEED
SELECT dolt_branch('-c', 'main', '');
"

oracle_error "move_empty_source" "
$SEED
SELECT dolt_branch('-m', '', 'dest');
"

oracle_error "move_empty_dest" "
$SEED
SELECT dolt_branch('-m', 'main', '');
"

oracle_error "no_args" "
SELECT dolt_branch();
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
