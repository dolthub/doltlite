#!/bin/bash

# True only for a clean, handled non-zero exit. A status >=128 is 128+signal
# (139 = SIGSEGV, 134 = SIGABRT, 136 = SIGFPE, ...): a crash, which must never
# be scored the same as Dolt's orderly error rejection in an oracle_error case.
vc_oracle_is_clean_error() {
  [ "$1" -ne 0 ] && [ "$1" -lt 128 ]
}

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
