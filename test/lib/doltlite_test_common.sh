#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/doltlite_integrity_common.sh"

DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
_dltest_integrity_real="$DOLTLITE"
if [ "${DOLTLITE##*/}" = dltest_engine_guard.pl ]; then
  _dltest_integrity_real="$DLTEST_REAL_DOLTLITE"
fi
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

dltest_run_sql() (
  local sql="$1"
  local db="$2"
  if [ -n "${4:-}" ]; then exec 3>"$4"; else exec 3>&1; fi
  # macOS /bin/bash 3.2 + set -u treats empty "${arr[@]}" as unbound.
  if [ "${3:-}" = "bail" ]; then
    if [ "$DLTEST_STRIP_CR" = "1" ]; then
      ( set -o pipefail
        echo "$sql" | DLTEST_REAL_DOLTLITE="$_dltest_integrity_real" perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
          "$_dltest_integrity_guard" -bail "$db" 2>&3 | tr -d '\r' )
    else
      echo "$sql" | DLTEST_REAL_DOLTLITE="$_dltest_integrity_real" perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
        "$_dltest_integrity_guard" -bail "$db" 2>&3
    fi
  else
    if [ "$DLTEST_STRIP_CR" = "1" ]; then
      ( set -o pipefail
        echo "$sql" | DLTEST_REAL_DOLTLITE="$_dltest_integrity_real" perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
          "$_dltest_integrity_guard" "$db" 2>&3 | tr -d '\r' )
    else
      echo "$sql" | DLTEST_REAL_DOLTLITE="$_dltest_integrity_real" perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
        "$_dltest_integrity_guard" "$db" 2>&3
    fi
  fi
)

# -bail so a missing dolt_* function is a non-zero status, not a continued script.
dltest_engine() {
  local db="$1"
  local sql="$2"
  printf '%s\n' "$sql" | DLTEST_REAL_DOLTLITE="$_dltest_integrity_real" perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" \
    "$_dltest_integrity_guard" -bail "$db" 2>&1
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
  out=$(printf '%s\n' "$sql" | dltest_checked_engine -bail "$db" 2>&1)
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
  if [ "$rc" -eq 125 ]; then
    dltest_fail "$name" "  engine integrity check failed\n  got: $result"
    return 1
  fi
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
  local db="${4:-:memory:}"
  local result rc=0 bail=""
  if ! dltest_expected_error "$expected"; then
    bail=bail
  fi
  result=$(dltest_run_sql "$sql" "$db" "$bail") || rc=$?
  if [ -n "$bail" ]; then
    if [ "$result" = "$expected" ] && [ "$rc" -eq 0 ]; then
      dltest_pass
    else
      dltest_fail "$name" "  engine rc=$rc\n  expected: $expected\n  got:      $result"
    fi
  elif ! dltest_check_signal "$name" "$rc" "$result"; then
    :
  elif [ "$rc" -ne 0 ] && [ "$result" = "$expected" ]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected: $expected\n  got:      $result"
  fi
}

run_test_lastline() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="${4:-:memory:}"
  local out result rc=0
  out=$(dltest_run_sql "$sql" "$db" bail) || rc=$?
  result=$(printf '%s\n' "$out" | tail -1)
  if ! dltest_check_signal "$name" "$rc" "$out"; then
    :
  elif [ "$rc" -eq 0 ] && [ "$result" = "$expected" ]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected: $expected\n  got:      $result"
  fi
}

dltest_match() {
  local mode="$1" name="$2" sql="$3" pattern="$4" db="${5:-:memory:}"
  local result rc=0 err bail="" valid=1 expected_error="${6:-}"
  err=$(mktemp) || return 1
  if [ "$mode" = success ]; then bail=bail; fi
  result=$(dltest_run_sql "$sql" "$db" "$bail" "$err") || rc=$?
  if [ "$mode" = error-output ] || [ "$mode" = error-lastline ]; then
    printf '%s\n' "$sql" | python3 "$(dirname "${BASH_SOURCE[0]}")/vc_oracle_refusals.py" \
      expected-errors "$err" "$expected_error" || valid=0
  fi
  if [ "$mode" = error-lastline ]; then result=$(printf '%s\n' "$result" | tail -1); fi
  if [ "$mode" = error ]; then
    local errors
    errors=$(cat "$err")
    if [ "$DLTEST_STRIP_CR" = 1 ]; then errors=$(printf '%s' "$errors" | tr -d '\r'); fi
    result="$result"$'\n'"$errors"
  fi
  if ! dltest_check_signal "$name" "$rc" "$result"; then
    :
  elif [ -z "$pattern" ]; then
    dltest_fail "$name" "  empty match pattern"
  elif { [ "$mode" = success ] && [ "$rc" -eq 0 ]; } \
    || { [ "$mode" != success ] && [ "$rc" -gt 0 ] && [ "$valid" -eq 1 ]; }; then
    if { [ "$mode" = error-lastline ] && [ "$result" = "$pattern" ]; } \
      || { [ "$mode" != error-lastline ] && printf '%s\n' "$result" | grep -E${DLTEST_MATCH_FLAGS} -- "$pattern" >/dev/null; }; then
      dltest_pass
    else
      dltest_fail "$name" "  engine rc=$rc\n  pattern: $pattern\n  got: $result\n  stderr: $(cat "$err")"
    fi
  else
    dltest_fail "$name" "  engine rc=$rc\n  pattern: $pattern\n  got: $result\n  stderr: $(cat "$err")"
  fi
  rm -f "$err"
}

run_test_match() {
  dltest_match success "$@"
}

run_test_error_match() {
  dltest_match error "$@"
}

run_test_error_output_match() {
  dltest_match error-output "$@"
}

run_test_error_lastline() {
  dltest_match error-lastline "$@"
}

dltest_init_queries() {
  DLTEST_QUERY_FAILURES=$(mktemp)
}

dltest_query() {
  local rc=0
  dltest_checked_engine -bail "$@" || rc=$?
  if [ "$rc" -ne 0 ]; then
    printf 'engine rc=%s: %s\n' "$rc" "$*" >> "${DLTEST_QUERY_FAILURES:?}"
  fi
  return "$rc"
}

dltest_assert_equal() {
  local name="$1" result="$2" expected="$3"
  if [ "$DLTEST_STRIP_CR" = 1 ]; then result=$(printf '%s' "$result" | tr -d '\r'); fi
  if [ "$result" = "$expected" ]; then dltest_pass
  else dltest_fail "$name" "  expected: $expected\n  got: $result"; fi
}

dltest_assert_match() {
  local name="$1" result="$2" pattern="$3"
  if [ -n "$pattern" ] && printf '%s\n' "$result" | grep -E -- "$pattern" >/dev/null; then dltest_pass
  else dltest_fail "$name" "  pattern: $pattern\n  got: $result"; fi
}

dltest_assert_int_ge() {
  local name="$1" result="$2" floor="$3"
  if [ -n "$result" ] && [ "$result" -ge "$floor" ] 2>/dev/null; then dltest_pass
  else dltest_fail "$name" "  expected: >= $floor\n  got: $result"; fi
}

dltest_assert_int_in() {
  local name="$1" result="$2" lo="$3" hi="$4"
  if [ -n "$result" ] && [ "$result" -ge "$lo" ] && [ "$result" -le "$hi" ] 2>/dev/null; then dltest_pass
  else dltest_fail "$name" "  expected: in [$lo, $hi]\n  got: $result"; fi
}

dltest_assert_grows() {
  local name="$1" before="$2" after="$3"
  if [ -n "$before" ] && [ -n "$after" ] && [ "$after" -gt "$before" ] 2>/dev/null; then dltest_pass
  else dltest_fail "$name" "  before: $before, after: $after"; fi
}

dltest_finish() {
  if [ -n "${DLTEST_QUERY_FAILURES:-}" ]; then
    if [ -s "$DLTEST_QUERY_FAILURES" ]; then
      dltest_fail queries "$(cat "$DLTEST_QUERY_FAILURES")"
    fi
    rm -f "$DLTEST_QUERY_FAILURES"
  fi
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
