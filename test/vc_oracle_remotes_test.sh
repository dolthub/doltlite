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
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

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
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
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

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
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

oracle_savepoint_remote_poststate() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_v dl_remotes dt_v dt_remotes

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_v=$(printf ".headers off\n.mode list\nSELECT v FROM t WHERE id=1;\n" \
         | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")
  dl_remotes=$(printf ".headers off\n.mode list\nSELECT coalesce(group_concat(name, ','), '') FROM dolt_remotes;\n" \
               | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_v=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "SELECT v FROM t WHERE id=1;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')
  dt_remotes=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "SELECT coalesce(group_concat(name, ','), '') FROM dolt_remotes;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ] && [ "$dl_v" = "$dt_v" ] && [ "$dl_remotes" = "$dt_remotes" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/v/remotes:"; { echo "$dl_rc"; echo "$dl_v"; echo "$dl_remotes"; } | sed 's/^/      /'
    echo "    dolt rc/v/remotes:"; { echo "$dt_rc"; echo "$dt_v"; echo "$dt_remotes"; } | sed 's/^/      /'
  fi
}

oracle_savepoint_clone_poststate() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_clone_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_post dt_post

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_post=$(printf ".headers off\n.mode list\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_post=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"\r')

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ] && [ "$dl_post" = "$dt_post" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/post:"; { echo "$dl_rc"; echo "$dl_post"; } | sed 's/^/      /'
    echo "    dolt rc/post:"; { echo "$dt_rc"; echo "$dt_post"; } | sed 's/^/      /'
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

echo "--- savepoint parity ---"

oracle_savepoint_remote_poststate "remote_add_inside_savepoint_releases_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "remote_remove_missing_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_remote('remove', 'missing');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "push_missing_remote_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_push('missing', 'main');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "fetch_missing_remote_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_fetch('missing');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "pull_missing_remote_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_pull('missing', 'main');
ROLLBACK TO sp1;
"

oracle_savepoint_clone_poststate "clone_bad_url_inside_savepoint_invalidates" "
SAVEPOINT sp1;
SELECT dolt_clone('bogus://remote');
ROLLBACK TO sp1;
" "SELECT active_branch();"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
