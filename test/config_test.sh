#!/bin/bash
#
# Tests for dolt_config() — committer name and email configuration.
#

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

pass=0
fail=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$(echo "$expected" | head -5)|"
    echo "    actual:   |$(echo "$actual" | head -5)|"
    fail=$((fail+1))
  fi
}

DB="$DOLTLITE"

# ============================================================
echo "=== 1. Default author ==="
# ============================================================
"$DB" "$TMPDIR/t1.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','default commit');
.quit
SQL
result=$("$DB" "$TMPDIR/t1.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "default name is doltlite" "doltlite" "$result"

result=$("$DB" "$TMPDIR/t1.db" "SELECT email FROM dolt_log LIMIT 1;")
check "default email is empty" "" "$result"

# ============================================================
echo "=== 2. Config get defaults ==="
# ============================================================
result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('user.name');")
check "default user.name" "doltlite" "$result"

result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('user.email');")
check "default user.email" "" "$result"

# ============================================================
echo "=== 3. Config set + commit ==="
# ============================================================
"$DB" "$TMPDIR/t1.db" <<'SQL'
SELECT dolt_config('user.name', 'Alice Smith');
SELECT dolt_config('user.email', 'alice@example.com');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','configured commit');
.quit
SQL
result=$("$DB" "$TMPDIR/t1.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "commit uses configured name" "Alice Smith" "$result"

result=$("$DB" "$TMPDIR/t1.db" "SELECT email FROM dolt_log LIMIT 1;")
check "commit uses configured email" "alice@example.com" "$result"

# ============================================================
echo "=== 4. --author overrides config ==="
# ============================================================
"$DB" "$TMPDIR/t1.db" <<'SQL'
SELECT dolt_config('user.name', 'Should Not Appear');
SELECT dolt_config('user.email', 'no@no.com');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','override','--author','Bot <bot@ci.com>');
.quit
SQL
result=$("$DB" "$TMPDIR/t1.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "--author overrides name" "Bot" "$result"

result=$("$DB" "$TMPDIR/t1.db" "SELECT email FROM dolt_log LIMIT 1;")
check "--author overrides email" "bot@ci.com" "$result"

# ============================================================
echo "=== 5. Config is per-session (not persisted) ==="
# ============================================================
# Set config in one session
"$DB" "$TMPDIR/t1.db" "SELECT dolt_config('user.name', 'Ephemeral');" > /dev/null
# New session should have default
result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('user.name');")
check "config not persisted across sessions" "doltlite" "$result"

# ============================================================
echo "=== 6. Config get after set (same session) ==="
# ============================================================
result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('user.name','NewName'); SELECT dolt_config('user.name');")
check "get returns set value" "0
NewName" "$result"

result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('user.email','new@mail'); SELECT dolt_config('user.email');")
check "email get returns set value" "0
new@mail" "$result"

# ============================================================
echo "=== 7. Merge commit uses config ==="
# ============================================================
"$DB" "$TMPDIR/t7.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
SELECT dolt_config('user.name', 'Merge Author');
SELECT dolt_config('user.email', 'merge@test.com');
SELECT dolt_merge('feat');
.quit
SQL
result=$("$DB" "$TMPDIR/t7.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "merge commit uses config name" "Merge Author" "$result"

result=$("$DB" "$TMPDIR/t7.db" "SELECT email FROM dolt_log LIMIT 1;")
check "merge commit uses config email" "merge@test.com" "$result"

# ============================================================
echo "=== 8. Cherry-pick uses config ==="
# ============================================================
# Get the commit hash from the src branch before switching
cp_hash=$("$DB" "$TMPDIR/t8.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('src');
SELECT dolt_checkout('src');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to cherry-pick');
SELECT commit_hash FROM dolt_log LIMIT 1;
SQL
)
cp_hash=$(echo "$cp_hash" | tail -1)
"$DB" "$TMPDIR/t8.db" "SELECT dolt_checkout('main'); SELECT dolt_config('user.name','Cherry Picker'); SELECT dolt_config('user.email','cp@test.com'); SELECT dolt_cherry_pick('$cp_hash');" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/t8.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "cherry-pick uses config name" "Cherry Picker" "$result"

result=$("$DB" "$TMPDIR/t8.db" "SELECT email FROM dolt_log LIMIT 1;")
check "cherry-pick uses config email" "cp@test.com" "$result"

# ============================================================
echo "=== 9. Revert uses config ==="
# ============================================================
"$DB" "$TMPDIR/t9.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to revert');
SELECT dolt_config('user.name', 'Reverter');
SELECT dolt_config('user.email', 'revert@test.com');
SELECT dolt_revert(commit_hash) FROM dolt_log WHERE message='to revert';
.quit
SQL
result=$("$DB" "$TMPDIR/t9.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "revert uses config name" "Reverter" "$result"

result=$("$DB" "$TMPDIR/t9.db" "SELECT email FROM dolt_log LIMIT 1;")
check "revert uses config email" "revert@test.com" "$result"

# ============================================================
echo "=== 10. Error: unknown config key ==="
# ============================================================
result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('bad.key');" 2>&1)
check "unknown key errors" "1" "$(echo "$result" | grep -c 'unknown config key')"

result=$("$DB" "$TMPDIR/t1.db" "SELECT dolt_config('bad.key','val');" 2>&1)
check "unknown key set errors" "1" "$(echo "$result" | grep -c 'unknown config key')"

# ============================================================
echo "=== 11. Config with special characters ==="
# ============================================================
"$DB" "$TMPDIR/t11.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_config('user.name', 'O''Brien');
SELECT dolt_config('user.email', 'user+tag@example.com');
SELECT dolt_commit('-m','special chars');
.quit
SQL
result=$("$DB" "$TMPDIR/t11.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "name with apostrophe" "O'Brien" "$result"

result=$("$DB" "$TMPDIR/t11.db" "SELECT email FROM dolt_log LIMIT 1;")
check "email with plus" "user+tag@example.com" "$result"

# ============================================================
echo "=== 12. Multiple commits in one session ==="
# ============================================================
"$DB" "$TMPDIR/t12.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_config('user.name', 'Batch Author');
SELECT dolt_config('user.email', 'batch@test.com');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','first');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','third');
.quit
SQL
result=$("$DB" "$TMPDIR/t12.db" "SELECT count(DISTINCT committer) FROM dolt_log;")
check "all 3 commits same author" "1" "$result"

result=$("$DB" "$TMPDIR/t12.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "batch author name correct" "Batch Author" "$result"

# ============================================================
echo "=== 13. Config change mid-session ==="
# ============================================================
"$DB" "$TMPDIR/t13.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_config('user.name', 'Author A');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','by A');
SELECT dolt_config('user.name', 'Author B');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','by B');
.quit
SQL
result=$("$DB" "$TMPDIR/t13.db" "SELECT committer FROM dolt_log WHERE message='by A';")
check "first commit by A" "Author A" "$result"

result=$("$DB" "$TMPDIR/t13.db" "SELECT committer FROM dolt_log WHERE message='by B';")
check "second commit by B" "Author B" "$result"

# ============================================================
echo "=== 14. -A flag with config ==="
# ============================================================
"$DB" "$TMPDIR/t14.db" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_config('user.name', 'Quick Author');
SELECT dolt_config('user.email', 'quick@test.com');
SELECT dolt_commit('-A', '-m', 'auto-stage commit');
.quit
SQL
result=$("$DB" "$TMPDIR/t14.db" "SELECT committer FROM dolt_log LIMIT 1;")
check "-A commit uses config" "Quick Author" "$result"

# ============================================================
echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
