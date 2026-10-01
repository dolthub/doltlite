#!/bin/bash

DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
PASS="${PASS:-0}"
FAIL="${FAIL:-0}"
ERRORS="${ERRORS:-}"
DLTEST_TIMEOUT="${DLTEST_TIMEOUT:-10}"
DLTEST_STRIP_CR="${DLTEST_STRIP_CR:-0}"
DLTEST_MATCH_FLAGS="${DLTEST_MATCH_FLAGS:-}"

# A suite that stops early still prints whatever tallies it reached, so the
# wrapper cannot tell a finished run from a truncated one. This line is the
# proof, and only the real end of a suite emits it.
dltest_mark_complete() {
  echo "__SUITE_COMPLETE__"
}

dltest_run_sql() {
  local sql="$1"
  local db="$2"
  # macOS /bin/bash 3.2 + set -u treats empty "${arr[@]}" as unbound.
  if [ "${3:-}" = "bail" ]; then
    if [ "$DLTEST_STRIP_CR" = "1" ]; then
      ( set -o pipefail
        echo "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
          "$DOLTLITE" -bail "$db" 2>&1 | tr -d '\r' )
    else
      echo "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
        "$DOLTLITE" -bail "$db" 2>&1
    fi
  else
    if [ "$DLTEST_STRIP_CR" = "1" ]; then
      ( set -o pipefail
        echo "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
          "$DOLTLITE" "$db" 2>&1 | tr -d '\r' )
    else
      echo "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
        "$DOLTLITE" "$db" 2>&1
    fi
  fi
}

# -bail so a missing dolt_* function is a non-zero status, not a continued script.
dltest_engine() {
  local db="$1"
  local sql="$2"
  printf '%s\n' "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
    "$DOLTLITE" -bail "$db" 2>&1
}

dltest_require() {
  local name="$1"
  local db="$2"
  local sql="$3"
  local out rc
  out=$(dltest_engine "$db" "$sql")
  rc=$?
  if [ "$rc" -ne 0 ]; then
    dltest_fail "$name" "  engine rc=$rc\n  $out"
    return 1
  fi
  return 0
}

# Same contract as dltest_require, without the per-call alarm. A setup that
# commits thousands of rows can outlive DLTEST_TIMEOUT.
dltest_require_slow() {
  local name="$1"
  local db="$2"
  local sql="$3"
  local out rc
  out=$(printf '%s\n' "$sql" | "$DOLTLITE" -bail "$db" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    dltest_fail "$name" "  engine rc=$rc\n  $out"
    return 1
  fi
  return 0
}

dltest_pass() {
  PASS=$((PASS+1))
}

dltest_fail() {
  local name="$1"
  local msg="$2"
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: $name\n$msg"
}

# An error status can be the expected outcome; a signal never is.
dltest_check_signal() {
  local name="$1" rc="$2" result="$3"
  if [ "$rc" -ge 128 ]; then
    dltest_fail "$name" "  engine died of a signal (rc=$rc)\n  got: $result"
    return 1
  fi
  return 0
}

dltest_expected_error() {
  case "$1" in
    Error*|*"Error near"*|*"Parse error"*) return 0 ;;
    *) return 1 ;;
  esac
}

run_test() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="$4"
  local result rc bail=""
  if ! dltest_expected_error "$expected"; then
    bail=bail
  fi
  result=$(dltest_run_sql "$sql" "$db" $bail)
  rc=$?
  if [ -n "$bail" ]; then
    if [ "$result" = "$expected" ] && [ "$rc" -eq 0 ]; then
      dltest_pass
    else
      dltest_fail "$name" "  engine rc=$rc\n  expected: $expected\n  got:      $result"
    fi
  elif ! dltest_check_signal "$name" "$rc" "$result"; then
    :
  elif [ "$result" = "$expected" ]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected: $expected\n  got:      $result"
  fi
}

run_test_lastline() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="$4"
  local out result rc
  out=$(dltest_run_sql "$sql" "$db")
  rc=$?
  result=$(printf '%s\n' "$out" | tail -1)
  if ! dltest_check_signal "$name" "$rc" "$out"; then
    :
  elif [ "$result" = "$expected" ]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected: $expected\n  got:      $result"
  fi
}

run_test_match() {
  local name="$1"
  local sql="$2"
  local pattern="$3"
  local db="$4"
  local result rc
  result=$(dltest_run_sql "$sql" "$db")
  rc=$?
  if ! dltest_check_signal "$name" "$rc" "$result"; then
    :
  elif echo "$result" | grep -qE${DLTEST_MATCH_FLAGS} -- "$pattern"; then
    dltest_pass
  else
    dltest_fail "$name" "  pattern: $pattern\n  got:     $result"
  fi
}

dltest_finish() {
  echo ""
  echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
  if [ "$FAIL" -gt 0 ]; then
    echo -e "$ERRORS"
    dltest_mark_complete
    exit 1
  fi
  dltest_mark_complete
}

if [ "${DLTEST_SKIP_ENGINE_FLOOR:-0}" != "1" ]; then
  _dltest_floor="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/assert_doltlite_engine.sh"
  if ! bash "$_dltest_floor" "$DOLTLITE"; then
    exit 1
  fi
  unset _dltest_floor
fi
