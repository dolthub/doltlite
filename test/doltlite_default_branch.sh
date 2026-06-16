#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0
fail=0
FAILED_NAMES=""

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected: $expected"
    echo "    actual:   $actual"
  fi
}

DB="$TMPROOT/db"

"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');
EOF
out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch();")
check "init_default_is_main" "main" "$out"

out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch('nope');" 2>&1)
case "$out" in *"branch 'nope' not found"*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES setter_rejects_unknown_branch"
     echo "  FAIL: setter_rejects_unknown_branch"; echo "    got: $out" ;;
esac

"$DOLTLITE" "$DB" "SELECT dolt_branch('feat');" >/dev/null
"$DOLTLITE" "$DB" "SELECT dolt_checkout('feat');" >/dev/null
out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch();")
check "checkout_does_not_change_default" "main" "$out"

"$DOLTLITE" "$DB" "SELECT dolt_default_branch('feat');" >/dev/null
out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch();")
check "setter_changes_default" "feat" "$out"

out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch();")
check "default_survives_reopen" "feat" "$out"

"$DOLTLITE" "$DB" "SELECT dolt_branch('-m','feat','trunk');" >/dev/null
out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch();")
check "rename_of_default_follows" "trunk" "$out"

"$DOLTLITE" "$DB" "SELECT dolt_branch('side');" >/dev/null
"$DOLTLITE" "$DB" "SELECT dolt_branch('-m','side','renamed_side');" >/dev/null
out=$("$DOLTLITE" "$DB" "SELECT dolt_default_branch();")
check "rename_of_non_default_unchanged" "trunk" "$out"

err=$("$DOLTLITE" "$DB" <<'EOF' 2>&1
SELECT dolt_checkout('main');
SELECT dolt_branch('-d','trunk');
EOF
)
case "$err" in *"cannot delete the default branch"*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES delete_default_rejected"
     echo "  FAIL: delete_default_rejected"; echo "    got: $err" ;;
esac

"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
SELECT dolt_checkout('renamed_side');
SELECT dolt_branch('-d','main');
EOF
out=$("$DOLTLITE" "$DB" "SELECT name FROM dolt_branches ORDER BY name;")
case "$out" in *"main"*) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES delete_non_default_main"
     echo "  FAIL: delete_non_default_main — main still present"
     echo "    got: $out" ;;
  *) pass=$((pass+1)) ;;
esac

"$DOLTLITE" "$DB" "SELECT dolt_branch('main');" >/dev/null
"$DOLTLITE" "$DB" "SELECT dolt_branch('-m','main','master');" >/dev/null
out=$("$DOLTLITE" "$DB" "SELECT name FROM dolt_branches ORDER BY name;")
case "$out" in *"master"*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES rename_non_default_main"
     echo "  FAIL: rename_non_default_main"; echo "    got: $out" ;;
esac

REMOTE="$TMPROOT/remote.db"
SRC="$TMPROOT/src.db"
CLONE="$TMPROOT/clone.db"
"$DOLTLITE" "$SRC" <<EOF >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('-m','main','trunk');
SELECT dolt_remote('add','origin','file://$REMOTE');
SELECT dolt_push('origin','trunk');
EOF
"$DOLTLITE" "$CLONE" "SELECT dolt_clone('file://$REMOTE');" >/dev/null 2>&1
out=$("$DOLTLITE" "$CLONE" "SELECT dolt_default_branch();")
check "clone_propagates_default" "trunk" "$out"
out=$("$DOLTLITE" "$CLONE" "SELECT active_branch();")
check "clone_session_starts_on_default" "trunk" "$out"

echo
echo "doltlite_default_branch: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
