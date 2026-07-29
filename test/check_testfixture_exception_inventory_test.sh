#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKER="$SCRIPT_DIR/check_testfixture_exception_inventory.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

DIVERGENCES="$TMP_DIR/divergences"
CRASHES="$TMP_DIR/crashes"
INVENTORY="$TMP_DIR/inventory"

printf '%s\n' \
  'alpha alpha-1' \
  'beta beta-1 @linux # platform case' > "$DIVERGENCES"
printf '%s\n' \
  'gamma # exits before the summary' > "$CRASHES"

run_check() {
  bash "$CHECKER" "$INVENTORY" "$DIVERGENCES" "$CRASHES" >/dev/null 2>&1
}

expect_failure() {
  local label="$1"
  if run_check; then
    echo "ERROR: checker accepted $label"
    exit 1
  fi
}

printf '%s\n' \
  'alpha intentional -' \
  'beta unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma harness -' > "$INVENTORY"
run_check

printf '%s\n' \
  'alpha intentional -' \
  'gamma harness -' > "$INVENTORY"
expect_failure "a missing disposition"

printf '%s\n' \
  'alpha intentional -' \
  'beta unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma harness -' \
  'stale intentional -' > "$INVENTORY"
expect_failure "a stale disposition"

printf '%s\n' \
  'alpha intentional -' \
  'alpha harness -' \
  'beta unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma harness -' > "$INVENTORY"
expect_failure "a duplicate disposition"

printf '%s\n' \
  'alpha intentional -' \
  'beta unsupported -' \
  'gamma harness -' > "$INVENTORY"
expect_failure "an unsupported surface without an issue"

printf '%s\n' \
  'alpha intentional -' \
  'beta engine-gap https://example.com/42' \
  'gamma harness -' > "$INVENTORY"
expect_failure "a non-DoltLite issue URL"

printf '%s\n' \
  'alpha unknown -' \
  'beta unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma harness -' > "$INVENTORY"
expect_failure "an unknown category"

echo "OK: testfixture exception inventory checker self-test"
