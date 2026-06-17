#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

normalize_output() {
  if [ "${DLTEST_STRIP_CR:-0}" = "1" ]; then
    tr -d '\r'
  else
    cat
  fi
}

check_eq() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $actual"
  fi
}

check_match() {
  local name="$1" pattern="$2" actual="$3"
  if echo "$actual" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $actual"
  fi
}

echo "=== Doltlite Open Branch Tests ==="
echo ""

DB=/tmp/test_open_branch_$$.db
rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
UPDATE t SET v='side' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','side');
SELECT dolt_checkout('main');
SQL

res=$("$DOLTLITE" "$DB" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "default_uses_main" $'main\nmain' "$res"

res=$("$DOLTLITE" "$DB@side" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "at_selects_side" $'side\nside' "$res"

res=$("$DOLTLITE" "$DB/side" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "slash_selects_side" $'side\nside' "$res"

res=$("$DOLTLITE" "$DB" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "branch_open_does_not_change_default" $'main\nmain' "$res"

res=$("$DOLTLITE" "$DB@missing" "SELECT active_branch();" 2>&1 | normalize_output || true)
check_match "missing_branch_errors" "unable to select branch|branch.*not found|SQLITE_NOTFOUND" "$res"

cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
SELECT dolt_branch('-m','side','renamed');
SELECT dolt_branch('-c','renamed','copy');
SQL

res=$("$DOLTLITE" "$DB@renamed" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "renamed_branch_open" $'renamed\nside' "$res"

res=$("$DOLTLITE" "$DB/copy" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "copied_branch_open" $'copy\nside' "$res"

res=$("$DOLTLITE" "$DB@renamed" "INSERT INTO t VALUES(2,'more'); SELECT dolt_commit('-A','-m','more'); SELECT active_branch(); SELECT count(*) FROM t;" | normalize_output)
check_match "commit_on_open_branch_path_hash" "^[0-9a-f]{40}$" "$(echo "$res" | sed -n '1p')"
check_eq "commit_on_open_branch_path_state" $'renamed\n2' "$(echo "$res" | tail -n +2)"

cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
SELECT dolt_branch('-D','copy');
SQL

res=$("$DOLTLITE" "$DB@copy" "SELECT active_branch();" 2>&1 | normalize_output || true)
check_match "deleted_branch_open_errors" "unable to select branch|branch.*not found|SQLITE_NOTFOUND" "$res"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  printf "%b\n" "$ERRORS"
  exit 1
fi
