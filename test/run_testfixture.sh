#!/bin/bash

set -uo pipefail

LABEL="${1:?Usage: run_testfixture.sh <label> <timeout_secs> test1 test2 ...}"
TIMEOUT="${2:?Missing timeout}"
shift 2

ulimit -Sn 1024 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIVERGENCE_FILE="${DIVERGENCE_FILE:-$SCRIPT_DIR/known_testfixture_divergences.txt}"
CRASH_FILE="${CRASH_FILE:-$SCRIPT_DIR/known_testfixture_crashes.txt}"
HOST_OS="$(uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]')"
TEST_VARIANT="${TESTFIXTURE_VARIANT:-default}"
case "$HOST_OS" in
  linux*) HOST_OS=linux ;;
  darwin*) HOST_OS=darwin ;;
  msys*|mingw*|cygwin*) HOST_OS=windows ;;
esac

if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD="gtimeout"
else
  TIMEOUT_CMD=""
fi
run_with_timeout() {
  if [ -n "$TIMEOUT_CMD" ]; then
    "$TIMEOUT_CMD" "$1" "${@:2}"
  else
    shift
    "$@"
  fi
}

expected_for() {
  local file="$1"
  [ -f "$DIVERGENCE_FILE" ] || return 0
  awk -v f="$file" -v host_os="$HOST_OS" -v test_variant="$TEST_VARIANT" '
    {
      sub(/#.*/, "")
      gsub(/^[ \t]+|[ \t]+$/, "")
      if ($0 == "") next
      for (i = 3; i <= NF; i++) {
        if ($i !~ /^@/) continue
        qualifier = substr($i, 2)
        if (qualifier == "coverage") {
          if (test_variant != "coverage") next
        } else if (qualifier == "no-coverage") {
          if (test_variant == "coverage") next
        } else if (qualifier != host_os) {
          next
        }
      }
      if ($1 == f) print $2
    }
  ' "$DIVERGENCE_FILE"
}

is_crash_expected() {
  local file="$1"
  [ -f "$CRASH_FILE" ] || return 1
  awk -v f="$file" '
    BEGIN { found = 0 }
    {
      sub(/#.*/, "")
      gsub(/^[ \t]+|[ \t]+$/, "")
      if ($0 == "") next
      if ($1 == f) { found = 1; exit }
    }
    END { exit found ? 0 : 1 }
  ' "$CRASH_FILE"
}

is_in_set() {
  local needle="$1"
  local haystack="$2"
  grep -Fxq -- "$needle" <<< "$haystack"
}

count_lines() {
  if [ -z "$1" ]; then
    echo 0
  else
    printf '%s\n' "$1" | grep -c .
  fi
}

total_pass=0
total_fail_known=0
total_fail_unexpected=0
total_unused=0
total_unexpected_crashes=0
total_unexpected_clean=0
unexpected_failure_lines=""
unused_lines=""

for test in "$@"; do
  expected="$(expected_for "$test")"
  out=$(run_with_timeout "$TIMEOUT" ./testfixture ../test/${test}.test 2>&1) || true

  done_line=$(echo "$out" | grep "errors out of" | head -1)
  fail_line=$(echo "$out" | grep "^!Failures on these tests:" | head -1)

  if [ -z "$done_line" ]; then
    if is_crash_expected "$test"; then
      echo "CRASH (expected): $test"
    else
      echo "CRASH (unexpected): $test"
      total_unexpected_crashes=$((total_unexpected_crashes + 1))
      unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test: did not produce summary line"
    fi
    continue
  fi

  if is_crash_expected "$test" && [ -z "$expected" ]; then
    echo "FIXED CRASH: $test (was on crash list, now produces summary — remove from $CRASH_FILE)"
    total_unexpected_clean=$((total_unexpected_clean + 1))
    unused_lines="$unused_lines"$'\n'"  $test (crash list)"
  fi

  actual=""
  if [ -n "$fail_line" ]; then
    actual=$(echo "$fail_line" | sed 's/^!Failures on these tests://' | tr ' ' '\n' | grep -v '^$' || true)
  fi

  unexpected=""
  if [ -n "$actual" ]; then
    while IFS= read -r name; do
      [ -z "$name" ] && continue
      if ! is_in_set "$name" "$expected"; then
        unexpected="$unexpected"$'\n'"$name"
      fi
    done <<< "$actual"
  fi
  unexpected="$(echo "$unexpected" | grep -v '^$' || true)"

  fixed=""
  if [ -n "$expected" ]; then
    while IFS= read -r name; do
      [ -z "$name" ] && continue
      if ! is_in_set "$name" "$actual"; then
        fixed="$fixed"$'\n'"$name"
      fi
    done <<< "$expected"
  fi
  fixed="$(echo "$fixed" | grep -v '^$' || true)"

  n_expected_total=$(count_lines "$expected")
  n_actual_total=$(count_lines "$actual")
  n_unexpected=$(count_lines "$unexpected")
  n_fixed=$(count_lines "$fixed")

  total_run=$(echo "$done_line" | awk '{print $5}')
  total_pass=$((total_pass + total_run - n_actual_total))
  total_fail_known=$((total_fail_known + n_actual_total - n_unexpected))
  total_fail_unexpected=$((total_fail_unexpected + n_unexpected))
  total_unused=$((total_unused + n_fixed))

  if [ "$n_unexpected" -eq 0 ] && [ "$n_fixed" -eq 0 ]; then
    if [ "$n_expected_total" -gt 0 ]; then
      echo "OK: $test ($total_run tests, $n_expected_total known divergences)"
    else
      echo "OK: $test ($total_run tests)"
    fi
  else
    if [ "$n_unexpected" -gt 0 ]; then
      echo "FAIL: $test — unexpected failures:"
      echo "$unexpected" | sed 's/^/    /'
      echo "  --- last testfixture output ---"
      echo "$out" | tail -n 40 | sed 's/^/  | /'
      echo "  --- end testfixture output ---"
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test $name"
      done <<< "$unexpected"
    fi
    if [ "$n_fixed" -gt 0 ]; then
      echo "FIXED: $test — these entries no longer fail and should be removed from $DIVERGENCE_FILE:"
      echo "$fixed" | sed 's/^/    /'
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        unused_lines="$unused_lines"$'\n'"  $test $name"
      done <<< "$fixed"
    fi
  fi
done

echo
echo "=== $LABEL ==="
echo "  passing tests:                     $total_pass"
echo "  known divergences (still failing): $total_fail_known"
if [ "$total_fail_unexpected" -gt 0 ] || [ "$total_unexpected_crashes" -gt 0 ]; then
  echo "  unexpected failures:               $total_fail_unexpected"
  echo "  unexpected crashes:                $total_unexpected_crashes"
  echo "  unexpected failure list:$unexpected_failure_lines"
  echo "::error::$LABEL: $((total_fail_unexpected + total_unexpected_crashes)) unexpected failure(s)"
  exit 1
fi
if [ "$total_unused" -gt 0 ] || [ "$total_unexpected_clean" -gt 0 ]; then
  echo "  fixed entries (remove from list):  $((total_unused + total_unexpected_clean))"
  echo "  fixed entry list:$unused_lines"
  echo "::error::$LABEL: $((total_unused + total_unexpected_clean)) entry/entries should be removed from divergence/crash list"
  exit 1
fi
exit 0
