#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNNER="$SCRIPT_DIR/run_testfixture.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir "$TMP_DIR/build"
touch "$TMP_DIR/terminations"

cat > "$TMP_DIR/build/testfixture" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  *two.test)
    names="two-1.transient.41 two-1.transient.99"
    ;;
  *shifted.test)
    names="shifted-1.transient.52 shifted-1.transient.104"
    ;;
  *three.test)
    names="three-1.transient.41 three-1.transient.99 three-1.transient.120"
    ;;
  *one.test)
    names="one-1.transient.41"
    ;;
  *extra.test)
    names="extra-1.transient.41 extra-1.transient.99 extra-2.1"
    ;;
  *tcl_abort.test)
    echo "tcl_abort-1.0... Ok"
    echo './testfixture: stable failure' >&2
    exit 1
    ;;
  *signal.test)
    echo "signal-1.0... Ok"
    kill -SEGV $$
    ;;
  *timeout.test)
    echo "timeout-1.0... Ok"
    sleep 5
    ;;
  *sanitizer.test)
    echo "sanitizer-1.0... Ok"
    echo 'ERROR: AddressSanitizer: heap-use-after-free' >&2
    echo './testfixture: stable failure' >&2
    exit 1
    ;;
  *clean_exit.test)
    exit 0
    ;;
  *summary_exit.test)
    echo '0 errors out of 1 tests'
    exit 2
    ;;
esac
count=$(wc -w <<< "$names" | tr -d ' ')
echo "$count errors out of 10 tests"
echo "!Failures on these tests: $names"
EOF
chmod +x "$TMP_DIR/build/testfixture"

run_case() {
  local timeout="${2:-10}"
  (
    cd "$TMP_DIR/build"
    DIVERGENCE_FILE="$TMP_DIR/divergences" \
      TERMINATION_FILE="$TMP_DIR/terminations" \
      bash "$RUNNER" self-test "$timeout" "$1"
  ) >"$TMP_DIR/run.out" 2>&1
}

expect_pass() {
  if ! run_case "$1"; then
    echo "ERROR: runner rejected $2"
    cat "$TMP_DIR/run.out"
    exit 1
  fi
}

expect_failure() {
  if run_case "$1"; then
    echo "ERROR: runner accepted $2"
    cat "$TMP_DIR/run.out"
    exit 1
  fi
}

printf '%s\n' \
  'two two-1.transient.41' \
  'two two-1.transient.99' > "$TMP_DIR/divergences"
expect_pass two "exact gates"

printf '%s\n' 'shifted shifted-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_pass shifted "a counted index shift"

printf '%s\n' 'three three-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_failure three "a counted pattern with an added failure"

printf '%s\n' 'one one-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_failure one "a counted pattern with a resolved failure"

printf '%s\n' 'extra extra-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_failure extra "an unrelated failure beside a satisfied pattern"

printf '%s\n' \
  'two two-1.transient.*{2}' \
  'two two-1.transient.41' > "$TMP_DIR/divergences"
expect_failure two "overlapping exact and counted gates"

printf '%s\n' \
  'shifted shifted-1.transient.41' \
  'shifted shifted-1.transient.99' > "$TMP_DIR/divergences"
expect_failure shifted "shifted exact gates"

# An unstable gate is enforced in neither direction: the assertion may fail (it
# is gated) or pass (not reported stale). one.test fails only one of the two
# gated names, which without the marker is a stale entry.
printf '%s\n' \
  'one one-1.transient.41' \
  'one one-1.transient.99' > "$TMP_DIR/divergences"
expect_failure one "a gate whose assertion stopped failing"

printf '%s\n' \
  'one one-1.transient.41' \
  'one one-1.transient.99 unstable class=harness issue=42' > "$TMP_DIR/divergences"
expect_pass one "an unstable gate whose assertion passed this run"

printf '%s\n' \
  'one one-1.transient.41 unstable class=harness issue=42' > "$TMP_DIR/divergences"
expect_pass one "an unstable gate whose assertion failed this run"

# The marker suppresses staleness, not unexpected failures: an ungated failure
# beside an unstable gate is still a red.
printf '%s\n' \
  'extra extra-1.transient.41 unstable class=harness issue=42' \
  'extra extra-1.transient.99' > "$TMP_DIR/divergences"
expect_failure extra "an ungated failure beside an unstable gate"

stable_hash=$(printf '%s' 'stable failure' | {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum
  else
    shasum -a 256
  fi
} | awk '{print $1}')
empty_hash=0000000000000000000000000000000000000000000000000000000000000000
: > "$TMP_DIR/divergences"

printf '%s\n' "tcl_abort tcl-error tcl_abort-1.0 $stable_hash" > "$TMP_DIR/terminations"
expect_pass tcl_abort "an exact Tcl termination contract"

printf '%s\n' "tcl_abort tcl-error tcl_abort-0.9 $stable_hash" > "$TMP_DIR/terminations"
expect_failure tcl_abort "a changed last completed test"

printf '%s\n' "tcl_abort tcl-error tcl_abort-1.0 $empty_hash" > "$TMP_DIR/terminations"
expect_failure tcl_abort "a changed Tcl diagnostic"

printf '%s\n' 'signal signal-11 signal-1.0 -' > "$TMP_DIR/terminations"
expect_pass signal "an exact signal termination contract"

printf '%s\n' "signal tcl-error signal-1.0 $stable_hash" > "$TMP_DIR/terminations"
expect_failure signal "a signal classified as a Tcl error"

printf '%s\n' "timeout tcl-error timeout-1.0 $stable_hash" > "$TMP_DIR/terminations"
if run_case timeout 1; then
  echo "ERROR: runner accepted a timeout"
  exit 1
fi

printf '%s\n' "sanitizer tcl-error sanitizer-1.0 $stable_hash" > "$TMP_DIR/terminations"
expect_failure sanitizer "a sanitizer finding"

printf '%s\n' 'clean_exit clean-exit - -' > "$TMP_DIR/terminations"
expect_pass clean_exit "an exact clean early exit"

printf '%s\n' 'two clean-exit - -' > "$TMP_DIR/terminations"
expect_failure two "a stale termination contract"

: > "$TMP_DIR/terminations"
expect_failure summary_exit "a nonstandard exit after the summary"

printf '%s\n' 'timeout timeout timeout-1.0 -' > "$TMP_DIR/terminations"
expect_failure timeout "a contract that permits timeouts"

echo "OK: testfixture runner self-test"
