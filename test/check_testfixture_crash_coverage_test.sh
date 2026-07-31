#!/usr/bin/env bash
#
# Self-test for the crash-coverage checker, whose job is to prove that a
# crash-listed file excluded from every regression bucket still has deterministic
# replacement coverage. The case anchor is the part worth testing: a mapping
# naming a replacement that no longer covers the behavior has to fail rather than
# pass on the strength of the file's name.
#
# Cases assert on the reported reason, not just a non-zero exit. Checking the
# exit alone hid a bug where an unresolvable anchor tripped set -e through a
# command substitution, so every negative case "passed" without the checker ever
# reaching its own diagnostic.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKER="$SCRIPT_DIR/check_testfixture_crash_coverage.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

CRASHES="$TMP_DIR/crashes"
COVERAGE="$TMP_DIR/coverage"
BUCKETS="$TMP_DIR/buckets"
TERMINATIONS="$TMP_DIR/terminations"
mkdir -p "$BUCKETS"
touch "$TERMINATIONS"

# Replacements are resolved against the real test/ tree, so the fixtures name
# real replacements and real cases. That also means this fails if one of those
# cases is renamed away, which is the point.
REPLACEMENT=doltlite_recover_savepointfault
if [ ! -f "$SCRIPT_DIR/$REPLACEMENT.test" ]; then
  echo "SKIP: $REPLACEMENT.test is not present"
  exit 0
fi

printf '%s\n' 'malloc # excluded, needs replacement coverage' > "$CRASHES"
printf '%s\n' 'somethingelse' "$REPLACEMENT" > "$BUCKETS/bucket.txt"

run_check() {
  bash "$CHECKER" "$CRASHES" "$COVERAGE" "$BUCKETS" "$TERMINATIONS" 2>&1
}

expect_ok() {
  local label="$1" out
  if ! out=$(run_check); then
    echo "ERROR: checker rejected $label"
    printf '%s\n' "$out" | sed 's/^/    /'
    exit 1
  fi
}

# Fails, and for the stated reason rather than incidentally.
expect_reason() {
  local label="$1" want="$2" out
  if out=$(run_check); then
    echo "ERROR: checker accepted $label"
    exit 1
  fi
  if ! printf '%s\n' "$out" | grep -q "$want"; then
    echo "ERROR: $label failed for the wrong reason (wanted /$want/)"
    printf '%s\n' "$out" | sed 's/^/    /'
    exit 1
  fi
}

printf '%s\n' "malloc testfixture $REPLACEMENT 1 # real case" > "$COVERAGE"
expect_ok "a mapping naming a real case"

printf '%s\n' "malloc testfixture $REPLACEMENT # no case field" > "$COVERAGE"
expect_reason "a mapping with no case anchor" "expected 4 fields"

printf '%s\n' "malloc testfixture $REPLACEMENT 99 # no such case" > "$COVERAGE"
expect_reason "a case absent from the replacement" "does not exist"

# An anchor matching several candidates points nowhere in particular. "Crash"
# appears in most of the scenario banners in crash_injection_test.sh.
printf '%s\n' 'malloc workflow-script crash_injection_test.sh Crash # ambiguous' \
  > "$COVERAGE"
expect_reason "an ambiguous case anchor" "is ambiguous"

printf '%s\n' 'malloc testfixture doltlite_recover_nonexistent 1 # gone' \
  > "$COVERAGE"
expect_reason "a replacement file that does not exist" "missing testfixture"

printf '%s\n' "malloc unknown-kind $REPLACEMENT 1 # bad kind" > "$COVERAGE"
expect_reason "an unknown coverage kind" "unknown coverage kind"

printf '%s\n' \
  "malloc testfixture $REPLACEMENT 1 # dup" \
  "malloc testfixture $REPLACEMENT 1 # dup" > "$COVERAGE"
expect_reason "a duplicate mapping" "duplicate coverage mappings"

: > "$COVERAGE"
expect_reason "an excluded crash test with no coverage" \
  "without replacement coverage"

printf '%s\n' 'malloc' > "$BUCKETS/bucket.txt"
: > "$COVERAGE"
expect_reason "a bucketed crash test with no termination contract" \
  "without termination contracts"

printf '%s\n' 'malloc tcl-error malloc-1.0 0000000000000000000000000000000000000000000000000000000000000000' \
  > "$TERMINATIONS"
expect_ok "a bucketed crash test with a termination contract"

printf '%s\n' 'notacrash tcl-error notacrash-1.0 0000000000000000000000000000000000000000000000000000000000000000' \
  > "$TERMINATIONS"
expect_reason "a termination contract outside the crash inventory" \
  "outside the crash inventory"

: > "$TERMINATIONS"
printf '%s\n' 'somethingelse' "$REPLACEMENT" > "$BUCKETS/bucket.txt"

printf '%s\n' \
  "malloc testfixture $REPLACEMENT 1 # real" \
  "notacrash testfixture $REPLACEMENT 2 # stale" > "$COVERAGE"
expect_reason "a mapping for a test that is not an unbucketed crash" \
  "not unbucketed crashes"

# A replacement that exists but is not gated proves nothing.
printf '%s\n' 'somethingelse' > "$BUCKETS/bucket.txt"
printf '%s\n' "malloc testfixture $REPLACEMENT 1 # ungated" > "$COVERAGE"
expect_reason "a replacement outside every regression bucket" \
  "not in a regression bucket"

echo "OK: testfixture crash coverage checker self-test"
