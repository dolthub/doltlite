#!/bin/bash
#
# Oracle: push a non-main branch into a fresh remote, then consume it, and
# require the consumer to match real Dolt across the version-control surface
# -- active branch, table contents, branch list, log, status, history, and
# post-consume mutation (commit, branch).
#
# doltlite consumes its own push target by opening the file directly (its
# push writes a usable database, not a bare repo); Dolt consumes the bare
# remote by cloning. After the push-default-branch fix both land on the
# pushed branch, so every downstream VC read should agree. Commit hashes and
# wall-clock dates legitimately differ and are never compared.
#
# Each branch is consumed ONCE into a pristine doltlite target file and a
# pristine dolt clone; read scenarios query those directly, and mutation
# scenarios run against forcecopy'd copies so they stay independent.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Build a source whose feature branch $2 carries the given history, push only
# that branch into a fresh remote, and materialize two consumers:
#   $CONSUME_DL  -- doltlite target file (push writes it directly)
#   $CONSUME_DT  -- dolt clone directory
# $3 is the source body run before branching (main-side); $4 the feature-side.
consume_branch() {
  local key="$1" branch="$2"
  local base="$TMPROOT/$key"
  mkdir -p "$base"

  CONSUME_DL="$base/tgt.db"
  CONSUME_DT="$base/clone"

  # doltlite: source -> push $branch -> target file.
  printf '%s\n' "
    CREATE TABLE example(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
    INSERT INTO example VALUES (1, 'one');
    SELECT dolt_commit('-A','-m','c1 base');
    SELECT dolt_checkout('-b','$branch');
    INSERT INTO example VALUES (2, 'two');
    SELECT dolt_commit('-A','-m','c2 on $branch');
    UPDATE example SET value='ONE' WHERE id=1;
    SELECT dolt_commit('-A','-m','c3 on $branch');
    SELECT dolt_checkout('-b','bar');
    INSERT INTO example VALUES (9, 'unpushed');
    SELECT dolt_commit('-A','-m','c4 on bar');
    SELECT dolt_checkout('$branch');
    SELECT dolt_remote('add','origin','file://$CONSUME_DL');
    SELECT dolt_push('origin','$branch');
  " | "$DOLTLITE" "$base/src.db" >"$base/dl_push.out" 2>"$base/dl_push.err"

  # dolt: source -> push $branch to a bare remote -> clone.
  (
    mkdir -p "$base/dsrc" "$base/drem"
    cd "$base/dsrc" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    "$DOLT" sql -c >/dev/null 2>"$base/dt_setup.err" <<SQL
CREATE TABLE example(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO example VALUES (1, 'one');
CALL dolt_commit('-A','-m','c1 base');
CALL dolt_checkout('-b','$branch');
INSERT INTO example VALUES (2, 'two');
CALL dolt_commit('-A','-m','c2 on $branch');
UPDATE example SET value='ONE' WHERE id=1;
CALL dolt_commit('-A','-m','c3 on $branch');
CALL dolt_checkout('-b','bar');
INSERT INTO example VALUES (9, 'unpushed');
CALL dolt_commit('-A','-m','c4 on bar');
CALL dolt_checkout('$branch');
SQL
    "$DOLT" remote add origin "file://$base/drem" >/dev/null 2>&1
    "$DOLT" push origin "$branch" >/dev/null 2>"$base/dt_push.err"
    cd "$base" || exit 1
    "$DOLT" clone "file://$base/drem" clone >/dev/null 2>"$base/dt_clone.err"
  )
}

# Query the prebuilt consumers and compare. $mut (optional) is applied to a
# fresh copy of each consumer first.
compare() {
  local name="$1" mut="$2" dl_query="$3" dt_query="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir"

  local dl_db="$CONSUME_DL" dt_repo="$CONSUME_DT"
  if [ -n "$mut" ]; then
    dl_db="$dir/tgt.db"; dt_repo="$dir/clone"
    forcecopy_db "$CONSUME_DL" "$dl_db"
    cp -R "$CONSUME_DT" "$dt_repo"
    printf '%s\n' "$mut" | "$DOLTLITE" "$dl_db" >/dev/null 2>"$dir/dl_mut.err"
    ( cd "$dt_repo" && printf '%s\n' "$(vc_oracle_translate_for_dolt "$mut")" \
        | "$DOLT" sql -c >/dev/null 2>"$dir/dt_mut.err" )
  fi

  local dl_out dt_out
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_query" \
           | "$DOLTLITE" "$dl_db" 2>"$dir/dl.err" | tr -d '\r' | grep '^R|' | sort)
  dt_out=$( ( cd "$dt_repo" && printf '%s\n' "$dt_query" \
               | "$DOLT" sql -r csv 2>"$dir/dt.err" ) | tr -d '"\r' | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

forcecopy_db() { rm -f "$2"; cp "$1" "$2"; }

echo "=== Version Control Oracle Test: push default branch + consume parity ==="
echo ""

echo "--- consume foo1 ---"
consume_branch foo1key foo1

compare "active_branch" "" \
  "SELECT 'R|' || active_branch();" \
  "SELECT CONCAT('R|', active_branch());"

compare "branch_list" "" \
  "SELECT 'R|' || name FROM dolt_branches;" \
  "SELECT CONCAT('R|', name) FROM dolt_branches;"

compare "table_contents" "" \
  "SELECT 'R|' || id || '|' || value FROM example;" \
  "SELECT CONCAT('R|', id, '|', value) FROM example;"

compare "log_messages" "" \
  "SELECT 'R|' || message FROM dolt_log;" \
  "SELECT CONCAT('R|', message) FROM dolt_log;"

compare "log_count" "" \
  "SELECT 'R|' || count(*) FROM dolt_log;" \
  "SELECT CONCAT('R|', count(*)) FROM dolt_log;"

compare "history_rows" "" \
  "SELECT 'R|' || id || '|' || value FROM dolt_history_example;" \
  "SELECT CONCAT('R|', id, '|', value) FROM dolt_history_example;"

compare "status_clean" "" \
  "SELECT 'R|' || count(*) FROM dolt_status;" \
  "SELECT CONCAT('R|', count(*)) FROM dolt_status;"

MUT_COMMIT="
INSERT INTO example VALUES (3, 'three');
SELECT dolt_commit('-A','-m','c5 post-consume');
"
compare "post_commit_contents" "$MUT_COMMIT" \
  "SELECT 'R|' || id || '|' || value FROM example;" \
  "SELECT CONCAT('R|', id, '|', value) FROM example;"

compare "post_commit_log" "$MUT_COMMIT" \
  "SELECT 'R|' || message FROM dolt_log;" \
  "SELECT CONCAT('R|', message) FROM dolt_log;"

compare "post_commit_status" "$MUT_COMMIT" \
  "SELECT 'R|' || count(*) FROM dolt_status;" \
  "SELECT CONCAT('R|', count(*)) FROM dolt_status;"

MUT_BRANCH="
SELECT dolt_checkout('-b','local-work');
INSERT INTO example VALUES (4, 'four');
SELECT dolt_commit('-A','-m','c5 on local-work');
"
compare "post_branch_active" "$MUT_BRANCH" \
  "SELECT 'R|' || active_branch();" \
  "SELECT CONCAT('R|', active_branch());"

compare "post_branch_list" "$MUT_BRANCH" \
  "SELECT 'R|' || name FROM dolt_branches;" \
  "SELECT CONCAT('R|', name) FROM dolt_branches;"

compare "post_branch_contents" "$MUT_BRANCH" \
  "SELECT 'R|' || id || '|' || value FROM example;" \
  "SELECT CONCAT('R|', id, '|', value) FROM example;"

echo "--- consume a differently-named branch (not hardcoded) ---"
consume_branch relkey release-2

compare "named_active" "" \
  "SELECT 'R|' || active_branch();" \
  "SELECT CONCAT('R|', active_branch());"

compare "named_contents" "" \
  "SELECT 'R|' || id || '|' || value FROM example;" \
  "SELECT CONCAT('R|', id, '|', value) FROM example;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
