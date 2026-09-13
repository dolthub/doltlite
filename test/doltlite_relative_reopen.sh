#!/bin/bash
# Reopen a relative-path database. Assert builds abort if the stock-SQLite
# probe passes that relative name to xOpen (unixOpen requires an absolute
# MAIN_DB path). Stay in cwd: Windows wrappers look up doltlite next to $0.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== relative-path reopen ==="
echo ""

DB="dl_rel_reopen_$$.db"
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

if ! "$DOLTLITE" "$DB" "CREATE TABLE q(id INTEGER PRIMARY KEY);" >/dev/null 2>&1; then
  dltest_fail "relative_create" "  CREATE TABLE on $DB failed"
else
  dltest_pass
fi

out=$("$DOLTLITE" "$DB" "INSERT INTO q VALUES(2);" 2>&1) || true
if echo "$out" | grep -qiE 'Assertion failed|zPath'; then
  dltest_fail "relative_reopen_insert" "  assert/relative xOpen: $out"
elif echo "$out" | grep -qiE 'Error|unable to open'; then
  dltest_fail "relative_reopen_insert" "  insert failed: $out"
else
  dltest_pass
fi

got=$("$DOLTLITE" "$DB" "SELECT count(*) FROM q;" 2>&1 | tail -1)
if [ "$got" = "1" ]; then
  dltest_pass
else
  dltest_fail "relative_reopen_count" "  expected 1, got: $got"
fi

abs=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$DB")
got=$("$DOLTLITE" "$abs" "INSERT INTO q VALUES(3); SELECT count(*) FROM q;" 2>&1 | tail -1)
if [ "$got" = "2" ]; then
  dltest_pass
else
  dltest_fail "absolute_reopen_count" "  expected 2, got: $got"
fi

dltest_finish

