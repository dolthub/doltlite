#!/bin/bash

DOLTLITE="${DOLTLITE:-./doltlite}"
PASS="${PASS:-0}"
FAIL="${FAIL:-0}"
ERRORS="${ERRORS:-}"
DLTEST_TIMEOUT="${DLTEST_TIMEOUT:-10}"
DLTEST_STRIP_CR="${DLTEST_STRIP_CR:-0}"
DLTEST_MATCH_FLAGS="${DLTEST_MATCH_FLAGS:-}"

dltest_run_sql() {
  local sql="$1"
  local db="$2"
  if [ "$DLTEST_STRIP_CR" = "1" ]; then
    echo "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" "$DOLTLITE" "$db" 2>&1 | tr -d '\r'
  else
    echo "$sql" | perl -e "alarm($DLTEST_TIMEOUT);exec @ARGV" "$DOLTLITE" "$db" 2>&1
  fi
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

run_test() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="$4"
  local result
  result=$(dltest_run_sql "$sql" "$db")
  if [ "$result" = "$expected" ]; then
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
  local result
  result=$(dltest_run_sql "$sql" "$db" | tail -1)
  if [ "$result" = "$expected" ]; then
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
  local result
  result=$(dltest_run_sql "$sql" "$db")
  if echo "$result" | grep -qE${DLTEST_MATCH_FLAGS} -- "$pattern"; then
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
    exit 1
  fi
}
