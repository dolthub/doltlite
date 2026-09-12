#!/bin/bash

# True only for a handled non-zero exit. Status >=128 is a crash, not an orderly error.
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

# A suite whose assertions are all "this must error" passes against an engine
# that errors at everything. Prove both sides can run a known-good script
# before believing any of their failures.
vc_oracle_require_working_engines() {
  local dir="$1"
  local probe_sql dl_rc dt_rc dt_sql ok=1
  probe_sql="CREATE TABLE oracle_probe(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO oracle_probe VALUES(1,'probe');
SELECT dolt_commit('-A','-m','probe');
SELECT count(*) FROM oracle_probe;"

  mkdir -p "$dir/dl" "$dir/dt"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" \
    "$probe_sql"
  dl_rc=$?
  if [ "$dl_rc" -ne 0 ] || ! grep -qx '1' "$dir/dl.out"; then
    echo "  FAIL: engine_probe (doltlite cannot run a known-good script; rc=$dl_rc)"
    sed 's/^/      /' "$dir/dl.err" 2>/dev/null | head -5
    ok=0
  fi

  dt_sql=$(vc_oracle_translate_for_dolt "$probe_sql")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$dt_sql"
  dt_rc=$?
  if [ "$dt_rc" -ne 0 ]; then
    echo "  FAIL: engine_probe (dolt cannot run a known-good script; rc=$dt_rc)"
    sed 's/^/      /' "$dir/dt.err" 2>/dev/null | head -5
    ok=0
  fi

  if [ "$ok" -eq 1 ]; then
    pass=$((pass+1))
    return 0
  fi
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES engine_probe"
  return 1
}
