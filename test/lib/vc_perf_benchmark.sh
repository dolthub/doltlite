#!/usr/bin/env bash

vc_perf_conflict_sql() {
  printf '%s\n' \
    '.bail off' '.headers off' '.mode list' \
    'BEGIN;' \
    "SELECT dolt_merge('feat');" \
    "SELECT 'VC_PERF_CONFLICTS|' || count(*) FROM dolt_conflicts_t;"
  if [ "$1" = resolved ]; then
    printf '%s\n' \
      "SELECT 'VC_PERF_RESOLVE|' || dolt_conflicts_resolve('--ours','t');" \
      "SELECT 'VC_PERF_RESOLVED|' || (SELECT count(*) FROM dolt_conflicts_t) || '|' || (SELECT count(*) FROM dolt_conflicts);"
  fi
  printf '%s\n' 'ROLLBACK;' "SELECT 'VC_PERF_DONE';"
}

vc_perf_validate_conflict() {
  local rc="$1" out="$2" err="$3" expectation="$4"
  local expected actual errors pattern
  expected="VC_PERF_CONFLICTS|$MERGE_CHANGE_ROWS"
  case "$expectation" in
    conflicts) ;;
    resolved) expected="$expected"$'\nVC_PERF_RESOLVE|0\nVC_PERF_RESOLVED|0|0' ;;
    *) echo "Unknown conflict benchmark expectation: $expectation" >&2; return 1 ;;
  esac
  expected="$expected"$'\nVC_PERF_DONE'
  actual=$(tr -d '\r' < "$out")
  errors=$(tr -d '\r' < "$err")
  pattern="^(Runtime error|Error) near line [0-9]+: Merge has $MERGE_CHANGE_ROWS conflict\\(s\\)\\. Resolve and then commit with dolt_commit\\.( \\(1\\))?$"
  if [ "$rc" -ne 1 ] || [ "$actual" != "$expected" ] \
      || [ "$(printf '%s\n' "$errors" | wc -l | tr -d ' ')" != 1 ] \
      || ! printf '%s\n' "$errors" | grep -Eq "$pattern"; then
    echo "Invalid $expectation benchmark sample (exit $rc)" >&2
    echo "Expected output:" >&2
    printf '%s\n' "$expected" >&2
    echo "Actual output:" >&2
    cat "$out" >&2
    echo "Errors:" >&2
    cat "$err" >&2
    return 1
  fi
}

time_sql() {
  local binary="$1" db="$2" sql="$3" out="$4" err="$5" expectation="$6"
  local start end rc=0
  start=$(us_now)
  printf '%s\n' "$sql" | "$binary" "$db" >"$out" 2>"$err" || rc=$?
  end=$(us_now)
  if [ "$expectation" != 0 ]; then
    vc_perf_validate_conflict "$rc" "$out" "$err" "$expectation" || return 1
  elif [ "$rc" -ne 0 ]; then
    cat "$err" >&2
    return "$rc"
  fi
  echo $((end-start))
}
