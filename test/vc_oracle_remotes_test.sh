#!/bin/bash
#
# Version-control oracle test: dolt_remotes
#
# Runs identical remote-management scenarios against doltlite and Dolt and
# compares the normalized dolt_remotes output. Every column's value is
# fully determined by the user's input — name and url come directly from
# the dolt_remote('add', ...) call, fetch_specs is derived from the name
# using the standard refspec template, and params is always {} — so all
# four columns are included in the comparison.
#
# Error scenarios are checked with oracle_error: both engines must fail
# but the specific error text is allowed to differ.
#
# Usage: bash vc_oracle_remotes_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() { tr -d '\r'; }

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT name || char(9) || url || char(9) || fetch_specs || char(9) || params FROM dolt_remotes ORDER BY name'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(name, char(9), url, char(9), fetch_specs, char(9), params) FROM dolt_remotes ORDER BY name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  # Dolt wraps the whole concatenated value in quotes because it contains
  # commas in the JSON. Strip the outer quotes and un-escape internal
  # double quotes ("" → ") before comparing.
  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" \
           | sed -E 's/^"(.*)"$/\1/' \
           | sed 's/""/"/g' \
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

echo "=== Version Control Oracle Tests: dolt_remotes ==="
echo ""

echo "--- baseline ---"

oracle "no_remotes_on_fresh_repo" "
SELECT 1;
"

echo "--- add ---"

oracle "add_single_remote" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
"

oracle "add_two_remotes" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('add', 'upstream', 'file:///tmp/oracle_upstream');
"

oracle "add_remote_with_non_standard_name" "
SELECT dolt_remote('add', 'backup-1', 'file:///tmp/oracle_backup');
"

# Dolt rewrites http:// URLs to git+http:// on add as a scheme qualifier
# signaling "git over HTTP". doltlite uses http:// for its own HTTP remote
# protocol, which is not git-over-HTTP, so matching that rewrite would
# break doltlite's actual HTTP remote behavior. file:// URLs have the
# same meaning on both engines, so that's what the add/remove scenarios
# exercise.

echo "--- remove ---"

oracle "remove_only_remote" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('remove', 'origin');
"

oracle "remove_one_keep_others" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('add', 'upstream', 'file:///tmp/oracle_upstream');
SELECT dolt_remote('remove', 'origin');
"

oracle "add_remove_add_same_name" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('remove', 'origin');
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_new');
"

echo "--- error paths ---"

oracle_error "add_duplicate_remote" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_other');
"

oracle_error "remove_nonexistent_remote" "
SELECT dolt_remote('remove', 'nonexistent');
"

oracle_error "unknown_action" "
SELECT dolt_remote('whatever', 'origin', 'file:///tmp/oracle_origin');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
