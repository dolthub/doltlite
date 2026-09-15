#!/bin/bash

# True only for a handled non-zero exit. Status >=128 is a crash, not an orderly error.
vc_oracle_is_clean_error() {
  [ "$1" -ne 0 ] && [ "$1" -lt 128 ]
}

vc_oracle_translate_for_dolt() {
  printf '%s\n' "$1" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g'
}

vc_oracle_init_repo() {
  "$DOLT" init --name oracle --email oracle@test >/dev/null 2>"${1:-/dev/null}"
}

vc_oracle_resolve_binary() {
  local bin="$1" resolved
  if ! resolved=$(command -v "$bin") || [ ! -x "$resolved" ]; then
    echo "ERROR: not executable: $bin" >&2
    return 1
  fi
  case "$resolved" in
    /*) printf '%s\n' "$resolved" ;;
    *) printf '%s/%s\n' "$PWD" "$resolved" ;;
  esac
}

vc_oracle_run_dolt_setup_query() {
  local repo="$1" out="$2" err="$3" setup="$4" query="$5"
  : > "$out"
  (
    cd "$repo" 2>"$err" || exit 1
    vc_oracle_init_repo "$err" || exit $?
    printf '%s\n' "$setup" | "$DOLT" sql >/dev/null 2>>"$err" || exit $?
    "$DOLT" sql -r csv -q "$query" >"$out" 2>>"$err"
  )
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

# True when a compared value has no content besides separators/whitespace.
# Joining two empty fields with "|" must not count as a match.
vc_oracle_output_is_blank() {
  local s="$1"
  s="${s//|/}"
  s="${s//$'\t'/}"
  s="${s// /}"
  s="${s//$'\n'/}"
  [ -z "$s" ]
}

vc_oracle_assert_match() {
  local name="$1" dl_out="$2" dt_out="$3"
  compared=$((${compared:-0}+1))
  if vc_oracle_output_is_blank "$dl_out" && vc_oracle_output_is_blank "$dt_out"; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both sides empty — schema/function/vtable likely broke)"
    return 1
  fi
  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
    nonempty=$((${nonempty:-0}+1))
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

# The tally alone cannot prove a suite finished, since a run that stops early
# still prints whatever it reached. Only the real end emits the sentinel.
# A suite that compared nothing, or only separator-empty strings, is not a pass.
vc_oracle_finish() {
  nonempty=${nonempty:-0}
  compared=${compared:-0}
  if [ "$pass" -eq 0 ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES suite_floor"
    echo "  FAIL: suite needs passing comparisons"
  elif [ "$compared" -gt 0 ] && [ "$nonempty" -eq 0 ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES suite_floor"
    echo "  FAIL: suite needs passing comparisons and non-empty compared output"
  fi
  echo ""
  echo "=== Results: $pass passed, $fail failed ==="
  if [ "$fail" -gt 0 ]; then
    echo "Failed:$FAILED_NAMES"
    echo "__SUITE_COMPLETE__"
    return 1
  fi
  echo "__SUITE_COMPLETE__"
  return 0
}
