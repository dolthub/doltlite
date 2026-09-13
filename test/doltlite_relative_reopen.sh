#!/bin/bash
# Stay in cwd: Windows wrappers look up doltlite next to $0.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== relative-path reopen ==="
echo ""

DB="dl_rel_reopen_$$.db"
rm -f "$DB"
trap 'rm -f "$DB" ".$DB-lock"' EXIT

run_test "relative_create" \
  "CREATE TABLE q(id INTEGER PRIMARY KEY);" "" "$DB"
run_test "relative_reopen_insert" \
  "INSERT INTO q VALUES(2);" "" "$DB"
run_test "relative_reopen_count" \
  "SELECT count(*) FROM q;" "1" "$DB"

abs=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$DB")
run_test "absolute_reopen_count" \
  "INSERT INTO q VALUES(3); SELECT count(*) FROM q;" "2" "$abs"

dltest_finish
