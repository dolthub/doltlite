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

RATCHET="$TMP_DIR/ratchet"
RATCHET_SEED="$TMP_DIR/ratchet-seed"
printf '%s\n' 'gates 3' 'intentional 1' 'unsupported 1' 'harness 1' \
  'engine-gap 0' > "$RATCHET_SEED"

# Baseline matching the 3-gate fixture below, so ratchet cases can move it
# without disturbing the format cases.
write_ratchet() {
  printf '%s\n' \
    "gates ${1:-3}" \
    "intentional ${2:-1}" \
    "unsupported ${3:-1}" \
    "harness ${4:-1}" \
    "engine-gap ${5:-0}" > "$RATCHET"
}
write_ratchet

run_check() {
  bash "$CHECKER" "$INVENTORY" "$DIVERGENCES" "$CRASHES" "$RATCHET" \
    >/dev/null 2>&1
}

expect_failure() {
  local label="$1"
  if run_check; then
    echo "ERROR: checker accepted $label"
    exit 1
  fi
}

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
run_check

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "a missing disposition"

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' \
  'stale stale-1 intentional -' > "$INVENTORY"
expect_failure "a stale disposition"

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'alpha alpha-1 harness -' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "a duplicate disposition"

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'beta beta-1 unsupported -' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "an unsupported surface without an issue"

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'beta beta-1 engine-gap https://example.com/42' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "a non-DoltLite issue URL"

printf '%s\n' \
  'alpha alpha-1 unknown -' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "an unknown category"

printf '%s\n' \
  'alpha alpha-1 intentional https://github.com/dolthub/doltlite/issues/42' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness https://github.com/dolthub/doltlite/issues/42' > "$INVENTORY"
run_check

printf '%s\n' \
  'alpha alpha-1 intentional https://example.com/42' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "a non-DoltLite rationale URL on an intentional surface"

# Gates are keyed individually, so one assertion in a file can be dispositioned
# differently from its neighbours -- the whole point of moving off suite keys.
printf '%s\n' \
  'alpha alpha-1' \
  'alpha alpha-2' \
  'beta beta-1 @linux # platform case' > "$DIVERGENCES"
printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'alpha alpha-2 engine-gap https://github.com/dolthub/doltlite/issues/42' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
write_ratchet 4 1 1 1 1
run_check

printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
expect_failure "one gate of a suite left without a disposition"

# Back to the 3-gate fixture for the ratchet cases.
printf '%s\n' \
  'alpha alpha-1' \
  'beta beta-1 @linux # platform case' > "$DIVERGENCES"
printf '%s\n' \
  'alpha alpha-1 intentional -' \
  'beta beta-1 unsupported https://github.com/dolthub/doltlite/issues/42' \
  'gamma * harness -' > "$INVENTORY"
write_ratchet
run_check

write_ratchet 2
expect_failure "more gates than the ratchet allows"

write_ratchet 4
expect_failure "fewer gates than the ratchet records"

printf '%s\n' 'gates 3' 'intentional 1' 'unsupported 1' > "$RATCHET"
expect_failure "a ratchet baseline missing a category"

: > "$RATCHET"
expect_failure "an empty ratchet baseline"

printf '%s\n' 'gates lots' 'intentional 1' 'unsupported 1' 'harness 1' \
  'engine-gap 0' > "$RATCHET"
expect_failure "a non-numeric ratchet baseline"

# A name listed twice used to yield a multiline value that failed every numeric
# comparison without recording a failure, so the ratchet reported holding while
# enforcing nothing. Each duplicated name is covered because the guard is in the
# shared lookup, and a per-name typo would slip past a single case.
for dup in gates intentional unsupported harness engine-gap; do
  {
    printf '%s\n' 'gates 3' 'intentional 1' 'unsupported 1' 'harness 1' \
      'engine-gap 0'
    grep -E "^$dup " "$RATCHET_SEED"
  } > "$RATCHET"
  expect_failure "a ratchet baseline listing '$dup' twice"
done

echo "OK: testfixture exception inventory checker self-test"
