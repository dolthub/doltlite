#!/bin/bash

vc_oracle_translate_for_dolt() {
  printf '%s\n' "$1" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g'
}

vc_oracle_init_repo() {
  "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
}

vc_oracle_run_doltlite_script() {
  local db="$1"
  local out="$2"
  local err="$3"
  local sql="$4"
  printf '%s\n' "$sql" | "$DOLTLITE" "$db" >"$out" 2>"$err"
}

vc_oracle_run_dolt_script() {
  local repo="$1"
  local out="$2"
  local err="$3"
  local sql="$4"
  shift 4
  (
    cd "$repo" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$sql" | "$DOLT" sql -c "$@" >"$out" 2>"$err"
  )
}

vc_oracle_run_dolt_script_for_error() {
  local repo="$1"
  local out="$2"
  local err="$3"
  local sql="$4"
  shift 4
  (
    cd "$repo" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$sql" | "$DOLT" sql "$@" >"$out" 2>"$err"
  )
}

vc_oracle_tail_csv_body() {
  tail -n +2 "$1" | tr -d '"'
}

# vc_oracle_assert_match <name> <dl_out> <dt_out>
#
# Standard equality assertion for oracle tests. Compares the doltlite and Dolt
# outputs and updates the caller's pass/fail/FAILED_NAMES counters.
#
# Guards against the vacuous-pass bug where both filtered outputs are empty
# (typically because the schema/function/vtable broke on both sides) and the
# inline `[ "$dl_out" = "$dt_out" ]` check returns true. If both are empty,
# the assertion fails with a "both sides empty" message. Callers that
# legitimately expect empty output on both sides should use
# vc_oracle_assert_match_allow_empty instead.
#
# Expects the caller to have declared `pass`, `fail`, and `FAILED_NAMES` as
# locals or globals; these are read/written via dynamic scope.
vc_oracle_assert_match() {
  local name="$1" dl_out="$2" dt_out="$3"
  if [ -z "$dl_out" ] && [ -z "$dt_out" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both sides empty — schema/function/vtable likely broke)"
    return 1
  fi
  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
    return 0
  fi
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES $name"
  echo "  FAIL: $name"
  echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
  echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  return 1
}

# vc_oracle_assert_match_allow_empty <name> <dl_out> <dt_out>
#
# Same as vc_oracle_assert_match but permits both sides to be empty. Use this
# only when "no rows match" is the intended pass condition for the test (e.g.
# a DELETE that empties a table, or a SELECT filtered to no rows).
vc_oracle_assert_match_allow_empty() {
  local name="$1" dl_out="$2" dt_out="$3"
  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
    return 0
  fi
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES $name"
  echo "  FAIL: $name"
  echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
  echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  return 1
}
