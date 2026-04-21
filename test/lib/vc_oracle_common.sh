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

# Run a piped SQL script against Dolt with -c (continue on error).
# Use this for normal oracle tests where the script should run to
# completion even if an intermediate statement errors (matching
# doltlite's shell behavior, which always continues past errors).
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

# Run a piped SQL script against Dolt WITHOUT -c. Use this for error-
# path oracle tests that verify both engines reject an operation —
# without -c, Dolt returns a non-zero exit code on the first error,
# which the test harness checks to confirm the operation was refused.
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
