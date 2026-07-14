#!/bin/bash
#
# Oracle test: pushing only a non-main branch into a fresh remote, then
# consuming that remote, must land the consumer on the pushed branch -- the
# way `dolt clone` of a foo1-only remote checks out foo1. doltlite inheriting
# the source's "main" default made its push target open on a ref-less branch
# (the unborn-branch state behind #1609). Ground truth: real Dolt.
#
# doltlite consumes its own push target by opening the file directly (its
# push writes a usable database, not a bare repo); Dolt consumes the bare
# remote by cloning. Both should report the pushed branch as active with the
# pushed row visible.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Compare a doltlite scalar (from opening the pushed target) against a dolt
# scalar (from cloning the pushed remote).
oracle_flow() {
  local name="$1" branch="$2" dl_query="$3" dt_query="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir"

  # doltlite: build a source, push $branch into a fresh target file, open it.
  local dl_src="$dir/src.db" dl_tgt="$dir/tgt.db"
  printf '%s\n' "
    CREATE TABLE example(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
    INSERT INTO example VALUES (1, 'pushed from $branch');
    SELECT dolt_commit('-A','-m','feature commit');
    SELECT dolt_branch('$branch');
    SELECT dolt_remote('add','origin','file://$dl_tgt');
    SELECT dolt_push('origin','$branch');
  " | "$DOLTLITE" "$dl_src" >"$dir/dl_push.out" 2>"$dir/dl_push.err"
  local dl_out
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_query" \
           | "$DOLTLITE" "$dl_tgt" 2>"$dir/dl.err" | tr -d '\r' | grep '^R|')

  # dolt: build a source, push $branch into a bare file remote, clone it.
  local dt_out
  (
    cd "$dir" || exit 1
    mkdir dsrc drem
    cd dsrc || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "CREATE TABLE example(id INT PRIMARY KEY, value TEXT NOT NULL);"
      echo "INSERT INTO example VALUES (1, 'pushed from $branch');"
      echo "CALL dolt_add('-A'); CALL dolt_commit('-m','feature commit');"
      echo "CALL dolt_branch('$branch');"
    } | "$DOLT" sql >/dev/null 2>"$dir/dt_setup.err"
    "$DOLT" remote add origin "file://$dir/drem" >/dev/null 2>&1
    "$DOLT" push origin "$branch" >/dev/null 2>"$dir/dt_push.err"
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dir/drem" dclone >/dev/null 2>"$dir/dt_clone.err"
    cd dclone || exit 1
    "$DOLT" sql -r csv 2>"$dir/dt.err" <<SQL
$dt_query
SQL
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|')

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Test: push default branch ==="
echo ""

oracle_flow "active_branch_foo1" "foo1" \
  "SELECT 'R|' || active_branch();" \
  "SELECT CONCAT('R|', active_branch());"

oracle_flow "pushed_row_visible" "foo1" \
  "SELECT 'R|' || id || '|' || value FROM example ORDER BY id;" \
  "SELECT CONCAT('R|', id, '|', value) FROM example ORDER BY id;"

oracle_flow "branch_listed" "foo1" \
  "SELECT 'R|' || name FROM dolt_branches ORDER BY name;" \
  "SELECT CONCAT('R|', name) FROM dolt_branches ORDER BY name;"

# A differently-named branch, to prove it is not hardcoded to a value.
oracle_flow "active_branch_release" "release-2" \
  "SELECT 'R|' || active_branch();" \
  "SELECT CONCAT('R|', active_branch());"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
