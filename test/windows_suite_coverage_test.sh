#!/usr/bin/env bash
# Windows ships the same binary as Unix. Suites that are not Unix-only
# (nofollow, fork C tests) must stay on doltlite_windows_suites so a
# rebase or history regression cannot land on master untested there.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=lib/doltlite_suite_manifest.sh
source "$root/test/lib/doltlite_suite_manifest.sh"

PASS=0
FAIL=0
ERRORS=""

windows=$(doltlite_windows_suites)

require_on_windows() {
  local s="$1"
  if grep -Fqx "$s" <<<"$windows"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: windows_missing_$s"
  fi
}

forbid_on_windows() {
  local s="$1"
  if grep -Fqx "$s" <<<"$windows"; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: windows_has_unix_only_$s"
  else
    PASS=$((PASS+1))
  fi
}

require_on_windows doltlite_rebase.sh
require_on_windows doltlite_rebase_schema.sh
require_on_windows doltlite_history.sh
require_on_windows doltlite_at.sh
require_on_windows doltlite_clustered_pk_pushdown.sh
require_on_windows doltlite_snapshot_isolation.sh
require_on_windows doltlite_savepoint.sh
require_on_windows doltlite_txn_seek_visibility.sh
require_on_windows doltlite_regression_test_c.sh
require_on_windows doltlite_gc.sh

forbid_on_windows doltlite_open_nofollow.sh

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  echo -e "$ERRORS"
  echo "__SUITE_COMPLETE__"
  exit 1
fi
echo "__SUITE_COMPLETE__"
