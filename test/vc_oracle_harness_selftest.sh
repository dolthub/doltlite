#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/lib/vc_oracle_common.sh"
VC_HARNESS_DIR=$(mktemp -d)
trap 'rm -rf "$VC_HARNESS_DIR"' EXIT
export VC_HARNESS_DIR
mkdir -p "$VC_HARNESS_DIR/bin dir" "$VC_HARNESS_DIR/repo"

cat > "$VC_HARNESS_DIR/bin dir/reference" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  init) stage=init; rc=23 ;;
  sql)
    if [ "${2:-}" = -r ]; then
      stage=query; rc=25
    else
      stage=setup; rc=24
      cat >/dev/null
    fi
    ;;
  *) exit 99 ;;
esac
printf '%s\n' "$stage" >> "$VC_HARNESS_DIR/trace"
if [ "${VC_FAIL_STAGE:-}" = "$stage" ]; then
  echo "$stage failure detail" >&2
  printf 'result\nrow\n'
  exit "$rc"
fi
if [ "$stage" = query ]; then printf 'result\nrow\n'; fi
STUB
cat > "$VC_HARNESS_DIR/candidate" <<'STUB'
#!/usr/bin/env bash
cat >/dev/null
if [ "${VC_FAIL_STAGE:-}" = candidate ]; then
  echo 'candidate failure detail' >&2
  exit 26
fi
STUB
chmod +x "$VC_HARNESS_DIR/bin dir/reference" "$VC_HARNESS_DIR/candidate"
checks=0

check() {
  if ! "$@"; then
    echo "FAIL: $*" >&2
    exit 1
  fi
  checks=$((checks+1))
}

absolute="$VC_HARNESS_DIR/bin dir/reference"
resolved=$(vc_oracle_resolve_binary "$absolute")
check test "$resolved" = "$absolute"
resolved=$(cd "$VC_HARNESS_DIR" && vc_oracle_resolve_binary './bin dir/reference')
check test "$resolved" = "$VC_HARNESS_DIR/./bin dir/reference"
DOLT="$resolved"
resolved=$(PATH="$VC_HARNESS_DIR/bin dir:$PATH" vc_oracle_resolve_binary reference)
check test "$resolved" = "$absolute"
resolved=$(cd "$VC_HARNESS_DIR" && PATH="bin dir:$PATH" vc_oracle_resolve_binary reference)
check test "$resolved" = "$absolute"
if vc_oracle_resolve_binary "$VC_HARNESS_DIR/missing" > "$VC_HARNESS_DIR/out" 2> "$VC_HARNESS_DIR/err"; then
  echo 'FAIL: missing binary accepted' >&2
  exit 1
fi
check grep -q 'ERROR: not executable:' "$VC_HARNESS_DIR/err"

vc_oracle_run_dolt_setup_query "$VC_HARNESS_DIR/repo" "$VC_HARNESS_DIR/out" \
  "$VC_HARNESS_DIR/err" 'CREATE TABLE t(id INT PRIMARY KEY);' 'SELECT * FROM t;'
check test "$(cat "$VC_HARNESS_DIR/trace")" = $'init\nsetup\nquery'
check test "$(cat "$VC_HARNESS_DIR/out")" = $'result\nrow'
check test ! -s "$VC_HARNESS_DIR/err"

for stage in init setup query; do
  export VC_FAIL_STAGE="$stage"
  : > "$VC_HARNESS_DIR/trace"
  rc=0
  vc_oracle_run_dolt_setup_query "$VC_HARNESS_DIR/repo" "$VC_HARNESS_DIR/out" \
    "$VC_HARNESS_DIR/err" 'CREATE TABLE t(id INT PRIMARY KEY);' 'SELECT * FROM t;' || rc=$?
  case "$stage" in
    init) expected_rc=23; expected_trace=init ;;
    setup) expected_rc=24; expected_trace=$'init\nsetup' ;;
    query) expected_rc=25; expected_trace=$'init\nsetup\nquery' ;;
  esac
  check test "$rc" = "$expected_rc"
  check test "$(cat "$VC_HARNESS_DIR/trace")" = "$expected_trace"
  check grep -qx "$stage failure detail" "$VC_HARNESS_DIR/err"
  if [ "$stage" != query ]; then check test ! -s "$VC_HARNESS_DIR/out"; fi

  if bash "$SCRIPT_DIR/vc_oracle_status_test.sh" "$VC_HARNESS_DIR/candidate" \
      "$absolute" > "$VC_HARNESS_DIR/suite.log" 2>&1; then
    echo "FAIL: status suite accepted $stage failure" >&2
    exit 1
  fi
  check grep -q "FAIL: empty_fresh_db (execution failed: doltlite rc=0, dolt rc=$expected_rc)" "$VC_HARNESS_DIR/suite.log"
  check grep -q "$stage failure detail" "$VC_HARNESS_DIR/suite.log"
  check grep -qx '__SUITE_COMPLETE__' "$VC_HARNESS_DIR/suite.log"
done

export VC_FAIL_STAGE=candidate
if bash "$SCRIPT_DIR/vc_oracle_status_test.sh" "$VC_HARNESS_DIR/candidate" \
    "$absolute" > "$VC_HARNESS_DIR/suite.log" 2>&1; then
  echo 'FAIL: status suite accepted candidate failure' >&2
  exit 1
fi
check grep -q 'FAIL: empty_fresh_db (execution failed: doltlite rc=26, dolt rc=0)' "$VC_HARNESS_DIR/suite.log"
check grep -q 'candidate failure detail' "$VC_HARNESS_DIR/suite.log"

# Pass floor and separator-empty comparisons.
pass=0; fail=0; nonempty=0; compared=0; FAILED_NAMES=""
if vc_oracle_finish > "$VC_HARNESS_DIR/floor.log" 2>&1; then
  echo 'FAIL: empty suite accepted by vc_oracle_finish' >&2
  exit 1
fi
check grep -q 'suite needs passing' "$VC_HARNESS_DIR/floor.log"

pass=0; fail=0; nonempty=0; compared=0; FAILED_NAMES=""
vc_oracle_assert_match 'pipe_empty' '|' '|' > "$VC_HARNESS_DIR/pipe.log" || true
check test "$fail" = 1
check test "$pass" = 0
check grep -q 'both sides empty' "$VC_HARNESS_DIR/pipe.log"

pass=0; fail=0; nonempty=0; compared=0; FAILED_NAMES=""
vc_oracle_assert_match 'real_row' '1' '1' > "$VC_HARNESS_DIR/row.log"
check test "$pass" = 1
check test "$nonempty" = 1
if ! vc_oracle_finish > "$VC_HARNESS_DIR/okfloor.log" 2>&1; then
  echo 'FAIL: nonempty suite rejected by vc_oracle_finish' >&2
  exit 1
fi

# Sabotaged engines must not report success. /usr/bin/false used to make
# vc_oracle_clean_test.sh print 11 passed / 0 failed.
if bash "$SCRIPT_DIR/vc_oracle_clean_test.sh" /usr/bin/false /usr/bin/false \
    > "$VC_HARNESS_DIR/clean_false.log" 2>&1; then
  echo 'FAIL: clean oracle accepted /usr/bin/false' >&2
  tail -10 "$VC_HARNESS_DIR/clean_false.log" >&2
  exit 1
fi
check grep -q 'FAIL:' "$VC_HARNESS_DIR/clean_false.log"

# Representative sweep: the full matrix is too large for lint. The floor
# plus these suites cover empty/separator matches and startup probes.
for base in vc_oracle_clean_test.sh vc_oracle_status_test.sh \
            vc_oracle_commit_test.sh vc_oracle_branch_test.sh \
            vc_oracle_add_test.sh vc_oracle_docs_test.sh; do
  if bash "$SCRIPT_DIR/$base" /usr/bin/false /usr/bin/false \
      > "$VC_HARNESS_DIR/sabotage.log" 2>&1; then
    echo "FAIL: $base accepted /usr/bin/false" >&2
    tail -8 "$VC_HARNESS_DIR/sabotage.log" >&2
    exit 1
  fi
  checks=$((checks+1))
done

printf 'VC oracle harness: %s checks passed\n' "$checks"
