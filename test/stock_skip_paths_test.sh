#!/bin/bash
#
# A suite that cannot find the stock sqlite3 has not run. run_guarded_suite.sh
# reads a trailing SKIP as "never ran" and the completion sentinel as "ran to
# the end"; a suite that prints both is counted as a pass it did not earn.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOLTLITE="${1:-./doltlite}"
MISSING="$(mktemp -d)/no-such-sqlite3"
pass=0; fail=0

check() {
  local suite="$1" out rc last
  shift
  out=$(SQLITE3="$MISSING" bash "$SCRIPT_DIR/run_guarded_suite.sh" "$SCRIPT_DIR/$suite" "$@" 2>&1); rc=$?
  last=$(printf '%s\n' "$out" | grep -v '^[[:space:]]*$' | tail -1)
  if [ "$rc" -eq 0 ] && printf '%s' "$last" | grep -qE '^SKIP[: ]' \
     && ! printf '%s\n' "$out" | grep -qx '__SUITE_COMPLETE__'; then
    echo "PASS: $suite ends on its SKIP line"; pass=$((pass+1))
  else
    echo "FAIL: $suite (rc=$rc) claimed more than a skip without stock sqlite3:"
    printf '%s\n' "$out" | sed 's/^/    /'
    fail=$((fail+1))
  fi
}

check engine_floor_test.sh "$DOLTLITE" "$MISSING"
check doltlite_attach_sqlite.sh "$DOLTLITE"
check doltlite_open_sqlite_file.sh "$DOLTLITE"
check doltlite_sqlite_file_oracle.sh "$DOLTLITE"

rm -rf "$(dirname "$MISSING")"
echo ""
echo "Results: $pass passed, $fail failed"
echo "__SUITE_COMPLETE__"
[ "$fail" -eq 0 ]
