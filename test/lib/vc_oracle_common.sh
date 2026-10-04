#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/doltlite_integrity_common.sh"

vc_oracle_init_execution() {
  VC_ORACLE_EXECUTION_DIR="$1/execution"
  mkdir -p "$VC_ORACLE_EXECUTION_DIR" || exit 1
  export DLTEST_INTEGRITY_EXPECTATIONS="$VC_ORACLE_EXECUTION_DIR/integrity-exceptions"
}

vc_oracle_run_doltlite() (
  local expectation=${VC_ORACLE_EXPECTATION:-success} err rc statuses
  case "${1:-}" in
    --success) expectation=success; shift ;;
    --error|--expect-error) expectation=error; shift ;;
    --allow-error) expectation=allow-error; shift ;;
  esac
  local command=(env DLTEST_REAL_DOLTLITE="$DOLTLITE"
      DLTEST_ENGINE_INTEGRITY_LOG="$VC_ORACLE_EXECUTION_DIR/integrity.failure"
      "$_dltest_integrity_guard")
  err=$(mktemp "$VC_ORACLE_EXECUTION_DIR/session.XXXXXX") || exit 1
  printf '  FAIL: %s:%s (doltlite session did not complete; database %s)\n' \
    "${BASH_SOURCE[1]##*/}" "${BASH_LINENO[0]}" "${1:-}" > "$err.failure"
  if [ /dev/fd/1 -ef /dev/fd/2 ]; then
    if "${command[@]}" "$@" 2>&1 | tee "$err"; then
      statuses=("${PIPESTATUS[@]}")
    else
      statuses=("${PIPESTATUS[@]}")
    fi
    rc=${statuses[0]}
    [ "${statuses[1]}" -eq 0 ] || return 1
  else
    if "${command[@]}" "$@" 2>"$err"; then rc=0; else rc=$?; fi
    cat "$err" >&2
  fi
  if [ "$rc" -ge 128 ] \
     || { [ "$expectation" = success ] && [ "$rc" -ne 0 ]; } \
     || { [ "$expectation" = error ] && [ "$rc" -eq 0 ]; }; then
    printf '  FAIL: %s:%s (doltlite rc=%s, expected %s; database %s)\n' \
      "${BASH_SOURCE[1]##*/}" "${BASH_LINENO[0]}" "$rc" "$expectation" "${1:-}" > "$err.failure"
  else
    rm -f "$err" "$err.failure"
  fi
  return "$rc"
)

# Pipeline and command-substitution subshells cannot update the suite's tally.
vc_oracle_check_execution() {
  local failure
  [ -n "${VC_ORACLE_EXECUTION_DIR:-}" ] || return 0
  for failure in "$VC_ORACLE_EXECUTION_DIR"/*.failure; do
    [ -f "$failure" ] || continue
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES execution"
    cat "$failure"
    if [ -f "${failure%.failure}" ]; then
      sed 's/^/      /' "${failure%.failure}"
    fi
    rm -f "$failure" "${failure%.failure}"
  done
}

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
  printf '%s\n' "$sql" | vc_oracle_run_doltlite "${5:---${VC_ORACLE_EXPECTATION:-success}}" "$db" >"$out" 2>"$err"
}

vc_oracle_run_dolt_script() {
  local repo="$1"
  local out="$2"
  local err="$3"
  local sql="$4"
  shift 4
  : > "$out"
  (
    cd "$repo" 2>"$err" || exit 1
    vc_oracle_init_repo "$err" || exit $?
    printf '%s\n' "$sql" | "$DOLT" sql -c "$@" >"$out" 2>"$err"
  )
}

vc_oracle_run_dolt_script_for_error() {
  local repo="$1"
  local out="$2"
  local err="$3"
  local sql="$4"
  shift 4
  : > "$out"
  (
    cd "$repo" 2>"$err" || exit 1
    vc_oracle_init_repo "$err" || exit $?
    printf '%s\n' "$sql" | "$DOLT" sql "$@" >"$out" 2>"$err"
  )
}

vc_oracle_error() {
  local name="$1" setup="$2" mode="${3:-}" pattern="${4:-}"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc=0 dt_rc=0 dolt_setup
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" \
    "$setup" --expect-error || dl_rc=$?
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$dolt_setup" -r csv || dt_rc=$?

  local valid=1 checker
  checker="$(dirname "${BASH_SOURCE[0]}")/vc_oracle_refusals.py"
  printf '%s\n' "$setup" | python3 "$checker" target-error "$dir/dl.err" \
    "$mode" "$pattern" > "$dir/dl.validation" || valid=0
  printf '%s\n' "$dolt_setup" | python3 "$checker" target-error "$dir/dt.err" \
    "$mode" "$pattern" > "$dir/dt.validation" || valid=0
  if [ "$mode" = --conflicted-setup ]; then
    if ! grep -qE '^VC_ORACLE_CONFLICTS\|[1-9][0-9]*$' "$dir/dl.out" \
       || ! grep -qE '^VC_ORACLE_CONFLICTS,[1-9][0-9]*$' "$dir/dt.out"; then
      printf '%s\n' 'setup did not prove active conflicts on both engines' \
        >> "$dir/dl.validation"
      valid=0
    fi
  elif [ -n "$mode" ]; then
    valid=0
  fi

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" \
     && [ "$valid" -eq 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error at the final statement)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    sed 's/^/      /' "$dir/dl.err" "$dir/dt.err" \
      "$dir/dl.validation" "$dir/dt.validation"
  fi
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
  vc_oracle_check_execution
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

vc_oracle_check_dolt_version() {
  local pin expected output actual
  pin="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.dolt-oracle-version"
  expected=$(tr -d '[:space:]' < "$pin") || return 1
  if ! output=$("$1" version); then
    echo "ERROR: cannot read Dolt oracle version from $1" >&2
    return 1
  fi
  actual=$(awk '$1=="dolt" && $2=="version" {print "v" $3; exit}' <<<"$output")
  if [ "$actual" != "$expected" ]; then
    echo "ERROR: Dolt oracle reports ${actual:-an unrecognized version}; expected $expected from $pin" >&2
    echo "Install the pinned oracle with .github/scripts/install-dolt-oracle.sh." >&2
    return 1
  fi
}

# Tokens are 1 where that statement failed and 0 where it ran. Session-setup
# statements that only one engine runs are omitted, even when they fail. A
# documented conflict or constraint-violation refusal from a merge-family
# call is D: it agrees with a success or with another refusal of that call.
# Error text is not part of the vector.
_VC_ORACLE_REFUSALS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/vc_oracle_refusals.py"

vc_oracle_refusal_bits() {
  local sql="$1" err="${2:-}"
  printf '%s' "$sql" | python3 "$_VC_ORACLE_REFUSALS" bits "$err"
}

vc_oracle_refusal_rows() {
  local sql="$1" err="${2:-}"
  printf '%s' "$sql" | python3 "$_VC_ORACLE_REFUSALS" rows "$err"
}

vc_oracle_join_bits() {
  local acc="$1" part="$2"
  if [ -z "$part" ]; then
    printf '%s' "$acc"
  elif [ -z "$acc" ]; then
    printf '%s' "$part"
  else
    printf '%s,%s' "$acc" "$part"
  fi
}

# Compare refusal vectors. No-op unless this case is an allow-error case.
vc_oracle_assert_refusals() {
  local name="$1" dl="$2" dt="$3"
  [ "${VC_ORACLE_EXPECTATION:-}" = allow-error ] || return 0
  if python3 "$_VC_ORACLE_REFUSALS" match "$dl" "$dt"; then
    return 0
  fi
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES ${name}_refusal"
  echo "  FAIL: $name (statement refusals differ)"
  echo "    doltlite: ${dl:-<none>}"
  echo "    dolt:     ${dt:-<none>}"
  return 1
}

# Arguments after the name are (doltlite sql, doltlite stderr, dolt sql,
# dolt stderr) repeated once per session.
vc_oracle_assert_refusal_files() {
  local name="$1"
  shift
  [ "${VC_ORACLE_EXPECTATION:-}" = allow-error ] || return 0
  local dl="" dt="" bits
  local -a parts=()
  while [ "$#" -ge 4 ]; do
    bits=$(vc_oracle_refusal_bits "$1" "$2")
    dl=$(vc_oracle_join_bits "$dl" "$bits")
    bits=$(vc_oracle_refusal_bits "$3" "$4")
    dt=$(vc_oracle_join_bits "$dt" "$bits")
    parts+=("$1" "$2" "$3" "$4")
    shift 4
  done
  vc_oracle_assert_refusals "$name" "$dl" "$dt" || {
    local i=0
    while [ "$i" -lt "${#parts[@]}" ]; do
      echo "    --- session $((i / 4 + 1)) doltlite ---"
      vc_oracle_refusal_rows "${parts[$i]}" "${parts[$((i + 1))]}" | sed 's/^/    /'
      echo "    --- session $((i / 4 + 1)) dolt ---"
      vc_oracle_refusal_rows "${parts[$((i + 2))]}" "${parts[$((i + 3))]}" | sed 's/^/    /'
      i=$((i + 4))
    done
    return 1
  }
}

if [ -n "${DOLT:-}" ]; then
  vc_oracle_check_dolt_version "$DOLT" || exit 1
fi
if [ -n "${TMPROOT:-}" ]; then vc_oracle_init_execution "$TMPROOT"; fi
