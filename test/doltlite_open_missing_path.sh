#!/bin/bash
# Missing-parent open must CANTOPEN at sqlite3_open, not succeed as a phantom connection.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite open missing-path (CANTOPEN) ==="
echo ""

MISSING_PARENT="/tmp/doltlite_missing_parent_$$/child.db"
rm -rf "$(dirname "$MISSING_PARENT")"

out=$( "$DOLTLITE" "$MISSING_PARENT" "SELECT 1;" 2>&1 ) || true
if echo "$out" | grep -qiE 'unable to open|Error'; then
  dltest_pass
  echo "  PASS: cli_missing_parent_fails"
else
  dltest_fail "cli_missing_parent_fails" "  expected open error, got: $out"
fi
if [ ! -e "$(dirname "$MISSING_PARENT")" ]; then
  dltest_pass
  echo "  PASS: cli_missing_parent_no_dir_created"
else
  dltest_fail "cli_missing_parent_no_dir_created" "  parent dir was created"
fi

OK_DB="/tmp/doltlite_open_ok_$$.db"
rm -f "$OK_DB"
run_test "valid_path_create_and_query" \
  "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT count(*) FROM t;" \
  "1" "$OK_DB"
rm -f "$OK_DB"

# ATTACH under a missing parent must fail, not error later on write.
run_test_match "attach_missing_parent_fails" \
  "ATTACH '$MISSING_PARENT' AS aux;" \
  "unable to open" ":memory:"

dltest_finish
