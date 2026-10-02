#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/lib/vc_oracle_common.sh"
VC_PINNED_VERSION=$(tr -d '[:space:]' < "$SCRIPT_DIR/../.dolt-oracle-version")
export VC_PINNED_VERSION
VC_HARNESS_DIR=$(mktemp -d)
trap 'rm -rf "$VC_HARNESS_DIR"' EXIT
vc_oracle_init_execution "$VC_HARNESS_DIR"
export VC_HARNESS_DIR
mkdir -p "$VC_HARNESS_DIR/bin dir" "$VC_HARNESS_DIR/repo"

cat > "$VC_HARNESS_DIR/bin dir/reference" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  version) echo "dolt version ${VC_DOLT_VERSION:-${VC_PINNED_VERSION#v}}"; exit "${VC_VERSION_RC:-0}" ;;
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
check vc_oracle_check_dolt_version "$absolute"
for version in 0.0.0 invalid; do
  if VC_DOLT_VERSION="$version" vc_oracle_check_dolt_version "$absolute" 2>"$VC_HARNESS_DIR/version.err"; then
    echo "FAIL: mismatched oracle version accepted" >&2
    exit 1
  fi
  check grep -q "expected $VC_PINNED_VERSION" "$VC_HARNESS_DIR/version.err"
done
if VC_VERSION_RC=23 vc_oracle_check_dolt_version "$absolute" 2>"$VC_HARNESS_DIR/version.err"; then
  echo "FAIL: failed version command accepted" >&2
  exit 1
fi
check grep -q 'cannot read Dolt oracle version' "$VC_HARNESS_DIR/version.err"

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

cat > "$VC_HARNESS_DIR/false-reference" <<'STUB'
#!/usr/bin/env bash
if [ "$1" = version ]; then echo "dolt version ${VC_PINNED_VERSION#v}"; exit 0; fi
exit 1
STUB
chmod +x "$VC_HARNESS_DIR/false-reference"

# Sabotaged engines must not report success. /usr/bin/false used to make
# vc_oracle_clean_test.sh print 11 passed / 0 failed.
if bash "$SCRIPT_DIR/vc_oracle_clean_test.sh" /usr/bin/false "$VC_HARNESS_DIR/false-reference" \
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
  if bash "$SCRIPT_DIR/$base" /usr/bin/false "$VC_HARNESS_DIR/false-reference" \
      > "$VC_HARNESS_DIR/sabotage.log" 2>&1; then
    echo "FAIL: $base accepted /usr/bin/false" >&2
    tail -8 "$VC_HARNESS_DIR/sabotage.log" >&2
    exit 1
  fi
  checks=$((checks+1))
done

cat > "$VC_HARNESS_DIR/branch-candidate" <<'STUB'
#!/usr/bin/env bash
cat >/dev/null
if [ "${VC_MERGED_STREAMS:-}" = 1 ]; then
  echo 'stdout first'
  echo 'stderr second' >&2
  echo 'stdout third'
  exit 0
fi
case "$1" in
  */checkout_feature/*) branch=feature ;;
  */checkout_create_branch/*) branch=new_branch ;;
  *) branch=main ;;
esac
printf '%s\n' "$branch"
echo 'candidate session diagnostic' >&2
exit "${VC_SESSION_RC:-0}"
STUB
cat > "$VC_HARNESS_DIR/branch-reference" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  version) echo "dolt version ${VC_PINNED_VERSION#v}"; exit 0 ;;
  init) exit 0 ;;
esac
cat >/dev/null
case "$PWD" in
  */checkout_feature/*) branch=feature ;;
  */checkout_create_branch/*) branch=new_branch ;;
  *) branch=main ;;
esac
printf 'active_branch()\n%s\n' "$branch"
STUB
chmod +x "$VC_HARNESS_DIR/branch-candidate" "$VC_HARNESS_DIR/branch-reference"

bash "$SCRIPT_DIR/vc_oracle_active_branch_test.sh" "$VC_HARNESS_DIR/branch-candidate" \
  "$VC_HARNESS_DIR/branch-reference" > "$VC_HARNESS_DIR/branch.log" 2>&1
check grep -q 'Results: 4 passed, 0 failed' "$VC_HARNESS_DIR/branch.log"
for rc in 1 139; do
  if VC_SESSION_RC="$rc" bash "$SCRIPT_DIR/vc_oracle_active_branch_test.sh" \
      "$VC_HARNESS_DIR/branch-candidate" "$VC_HARNESS_DIR/branch-reference" \
      > "$VC_HARNESS_DIR/branch.log" 2>&1; then
    echo "FAIL: active-branch suite accepted valid output with exit $rc" >&2
    exit 1
  fi
  check grep -q "doltlite rc=$rc, expected success" "$VC_HARNESS_DIR/branch.log"
  check grep -q 'candidate session diagnostic' "$VC_HARNESS_DIR/branch.log"
  check grep -qx '__SUITE_COMPLETE__' "$VC_HARNESS_DIR/branch.log"
done

DOLTLITE="$VC_HARNESS_DIR/branch-candidate"
for expectation in --success --expect-error --allow-error; do
  for rc in 0 1 127 128 139 143; do
    pass=1; fail=0; compared=0; nonempty=0; FAILED_NAMES=""
    export VC_SESSION_RC="$rc"
    output=$(printf 'SELECT 1;\n' | vc_oracle_run_doltlite "$expectation" "$VC_HARNESS_DIR/db" \
      2>/dev/null | tail -1) || true
    check test "$output" = main
    expected_fail=0
    if [ "$rc" -ge 128 ] \
       || { [ "$expectation" = --success ] && [ "$rc" -ne 0 ]; } \
       || { [ "$expectation" = --expect-error ] && [ "$rc" -eq 0 ]; }; then
      expected_fail=1
    fi
    finish_rc=0
    vc_oracle_finish > "$VC_HARNESS_DIR/execution.log" || finish_rc=$?
    check test "$finish_rc" = "$expected_fail"
    if [ "$expected_fail" -eq 1 ]; then
      check grep -q "doltlite rc=$rc" "$VC_HARNESS_DIR/execution.log"
      check grep -q 'candidate session diagnostic' "$VC_HARNESS_DIR/execution.log"
    fi
  done
done
unset VC_SESSION_RC

pass=1; fail=0; FAILED_NAMES=""
rc=0
VC_SESSION_RC=1 VC_ORACLE_EXPECTATION=error vc_oracle_run_doltlite_script \
  "$VC_HARNESS_DIR/checkout_feature/db" "$VC_HARNESS_DIR/scoped.out" \
  "$VC_HARNESS_DIR/scoped.err" 'SELECT active_branch();' || rc=$?
check test "$rc" = 1
check test "$(cat "$VC_HARNESS_DIR/scoped.out")" = feature
vc_oracle_finish > "$VC_HARNESS_DIR/scoped.log"
check test "$fail" = 0

pass=1; fail=0; FAILED_NAMES=""
VC_SESSION_RC=1 VC_ORACLE_EXPECTATION=allow-error vc_oracle_run_doltlite --success \
  "$VC_HARNESS_DIR/setup-db" >/dev/null 2>/dev/null || true
if vc_oracle_finish > "$VC_HARNESS_DIR/strict.log"; then
  echo 'FAIL: explicit success mode inherited an error expectation' >&2
  exit 1
fi
check grep -q 'expected success' "$VC_HARNESS_DIR/strict.log"

for attempt in 1 2 3; do
  VC_MERGED_STREAMS=1 vc_oracle_run_doltlite "$VC_HARNESS_DIR/order-db" \
    > "$VC_HARNESS_DIR/merged.out" 2>&1
  check test "$(cat "$VC_HARNESS_DIR/merged.out")" = $'stdout first\nstderr second\nstdout third'
done

pass=1; fail=0; FAILED_NAMES=""
VC_SESSION_RC=139 vc_oracle_run_doltlite "$VC_HARNESS_DIR/setup-db" \
  >/dev/null 2>/dev/null || true
if vc_oracle_finish > "$VC_HARNESS_DIR/setup.log"; then
  echo 'FAIL: discarded setup status accepted' >&2
  exit 1
fi
check grep -q 'setup-db' "$VC_HARNESS_DIR/setup.log"

python3 "$SCRIPT_DIR/lib/vc_oracle_refusals.py" selftest

printf 'VC oracle harness: %s checks passed\n' "$checks"
