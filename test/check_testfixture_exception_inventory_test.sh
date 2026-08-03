#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKER="$SCRIPT_DIR/check_testfixture_exception_inventory.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

DIVERGENCES="$TMP_DIR/divergences"
CRASHES="$TMP_DIR/crashes"

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

# One intentional divergence, one unsupported divergence carrying a platform
# qualifier, one harness crash row.
write_fixture() {
  printf '%s\n' \
    'alpha alpha-1 class=intentional' \
    'beta beta-1 @linux class=unsupported issue=42  # platform case' \
    > "$DIVERGENCES"
  printf '%s\n' \
    'gamma class=harness  # exits before the summary' > "$CRASHES"
}
write_fixture

run_check() {
  bash "$CHECKER" "$DIVERGENCES" "$CRASHES" "$RATCHET" >/dev/null 2>&1
}

expect_failure() {
  local label="$1"
  if run_check; then
    echo "ERROR: checker accepted $label"
    exit 1
  fi
}

run_check

# A gate without a disposition is a malformed line rather than a set difference
# against a second list. There is no "stale disposition" case left to test: a
# disposition cannot outlive its gate once it is a field on the gate.
printf '%s\n' \
  'alpha alpha-1' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "a gate with no class="
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=intentional class=harness' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "two class= fields on one gate"
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=intentional' \
  'alpha alpha-1 class=harness' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "a duplicated gate"
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=intentional' \
  'beta beta-1 @linux class=unsupported' > "$DIVERGENCES"
expect_failure "an unsupported surface without an issue"
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=intentional' \
  'beta beta-1 @linux class=engine-gap issue=https://example.com/42' \
  > "$DIVERGENCES"
expect_failure "an issue= that is not a DoltLite issue number"
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=intentional' \
  'beta beta-1 @linux class=unsupported issue=42 issue=43' > "$DIVERGENCES"
expect_failure "two issue= fields on one gate"
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=unknown' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "an unknown category"
write_fixture

printf '%s\n' \
  'alpha alpha-1 class=intentional bogus=1' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "an unrecognized field"
write_fixture

printf '%s\n' \
  'alpha alpha-1 @nosuchplatform class=intentional' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "an unknown qualifier"
write_fixture

# A crash row is keyed by suite alone, so its disposition sits in field 2.
printf '%s\n' 'gamma  # no class' > "$CRASHES"
expect_failure "a crash row with no class="
write_fixture

# intentional and harness may cite the issue documenting the limitation.
printf '%s\n' \
  'alpha alpha-1 class=intentional issue=42' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
printf '%s\n' 'gamma class=harness issue=42' > "$CRASHES"
run_check
write_fixture

# Gates are keyed individually, so one assertion in a file can be dispositioned
# differently from its neighbours -- the whole point of keying per gate.
printf '%s\n' \
  'alpha alpha-1 class=intentional' \
  'alpha alpha-2 class=engine-gap issue=42' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
write_ratchet 4 1 1 1 1
run_check
write_fixture
write_ratchet

# Counted patterns retain their represented assertion count in the ratchet.
printf '%s\n' \
  'alpha alpha-1.transient.*{2} class=intentional' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
write_ratchet 4 2 1 1 0
run_check

printf '%s\n' \
  'alpha alpha-1.transient.*{0} class=intentional' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "a zero-count pattern"

printf '%s\n' \
  'alpha alpha-1.transient.*{2} class=intentional' \
  'alpha alpha-1.transient.7 class=intentional' \
  'beta beta-1 @linux class=unsupported issue=42' > "$DIVERGENCES"
expect_failure "overlapping exact and counted gates"

# Back to the 3-gate fixture for the ratchet cases.
write_fixture
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

# ratchet_for() ignores anything it does not recognize, so a malformed line would
# otherwise sit in the file looking enforced.
printf '%s\n' 'gates 3' 'stray' 'intentional 1' 'unsupported 1' 'harness 1' \
  'engine-gap 0' > "$RATCHET"
expect_failure "a stray one-field ratchet line"

printf '%s\n' 'gates 3 extra' 'intentional 1' 'unsupported 1' 'harness 1' \
  'engine-gap 0' > "$RATCHET"
expect_failure "a ratchet line with a trailing extra field"

printf '%s\n' 'gates 3' 'bogusname 1' 'intentional 1' 'unsupported 1' \
  'harness 1' 'engine-gap 0' > "$RATCHET"
expect_failure "an unknown ratchet name"

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
