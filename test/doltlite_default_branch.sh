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

# Guard tests on a fresh database
GDB="$TMPROOT/guards.db"
"$DOLTLITE" "$GDB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE log(b TEXT);
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
EOF

# 1. Write-statement guard: 1-arg rejected, 0-arg allowed
err=$("$DOLTLITE" "$GDB" "INSERT INTO t SELECT dolt_default_branch('feat');" 2>&1)
case "$err" in *"write statement"*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES write_stmt_setter_rejected"
     echo "  FAIL: write_stmt_setter_rejected"; echo "    got: $err" ;;
esac
out=$("$DOLTLITE" "$GDB" "SELECT dolt_default_branch();")
check "write_stmt_default_unchanged" "main" "$out"

"$DOLTLITE" "$GDB" "INSERT INTO log SELECT dolt_default_branch();" >/dev/null
out=$("$DOLTLITE" "$GDB" "SELECT b FROM log;")
check "write_stmt_getter_allowed" "main" "$out"

# 2. Read-only connection guard: 1-arg rejected, 0-arg allowed
err=$("$DOLTLITE" -readonly "$GDB" "SELECT dolt_default_branch('feat');" 2>&1)
case "$err" in *[Rr]eadonly*|*[Rr]ead-only*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES readonly_setter_rejected"
     echo "  FAIL: readonly_setter_rejected"; echo "    got: $err" ;;
esac
out=$("$DOLTLITE" -readonly "$GDB" "SELECT dolt_default_branch();")
check "readonly_getter_allowed" "main" "$out"
out=$("$DOLTLITE" "$GDB" "SELECT dolt_default_branch();")
check "readonly_default_unchanged" "main" "$out"

# 3. Detached HEAD guard: 1-arg rejected, 0-arg allowed
HEAD_HASH=$("$DOLTLITE" "$GDB" "SELECT commit_hash FROM dolt_log LIMIT 1;")
err=$("$DOLTLITE" "$GDB/$HEAD_HASH" "SELECT dolt_default_branch('feat');" 2>&1)
case "$err" in *"detached head"*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES detached_setter_rejected"
     echo "  FAIL: detached_setter_rejected"; echo "    got: $err" ;;
esac
out=$("$DOLTLITE" "$GDB/$HEAD_HASH" "SELECT dolt_default_branch();")
check "detached_getter_allowed" "main" "$out"
out=$("$DOLTLITE" "$GDB" "SELECT dolt_default_branch();")
check "detached_default_unchanged" "main" "$out"

# 4. Transaction sealing inside BEGIN: seals enclosing txn (matching dolt_tag / dolt_branch)
out=$("$DOLTLITE" "$GDB" <<'EOF'
BEGIN;
INSERT INTO t VALUES(99);
SELECT dolt_default_branch('feat');
ROLLBACK;
SELECT COUNT(*) FROM t WHERE id=99;
EOF
)
check "txn_seal_commits_enclosing_writes" "0
1" "$out"
out=$("$DOLTLITE" "$GDB" "SELECT dolt_default_branch();")
check "txn_seal_updates_default" "feat" "$out"
out=$("$DOLTLITE" "$GDB" <<'EOF'
SELECT dolt_checkout('main');
SELECT COUNT(*) FROM t WHERE id=99;
EOF
)
check "txn_seal_persists_on_main_across_reopen" "0
1" "$out"

# 5. Deferred FK precheck inside BEGIN: refuses before mutating refs
err=$("$DOLTLITE" "$GDB" <<'EOF' 2>&1
PRAGMA foreign_keys = ON;
BEGIN;
INSERT INTO child VALUES(1, 999);
SELECT dolt_default_branch('main');
ROLLBACK;
EOF
)
case "$err" in *"FOREIGN KEY"*|*"constraint"*) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES deferred_fk_precheck_rejected"
     echo "  FAIL: deferred_fk_precheck_rejected"; echo "    got: $err" ;;
esac
out=$("$DOLTLITE" "$GDB" "SELECT dolt_default_branch();")
check "deferred_fk_precheck_default_unchanged" "feat" "$out"

echo
echo "doltlite_default_branch: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
echo "__SUITE_COMPLETE__"
