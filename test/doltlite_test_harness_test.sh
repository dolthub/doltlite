#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cat > "$TMP/engine" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$DLTEST_STUB_ARGS"
cat >/dev/null
printf '%s' "${DLTEST_STUB_STDOUT:-}"
printf '%s' "${DLTEST_STUB_STDERR:-}" >&2
exit "${DLTEST_STUB_RC:-0}"
STUB
chmod +x "$TMP/engine"
export DLTEST_SKIP_ENGINE_FLOOR=1 DOLTLITE="$TMP/engine"
export DLTEST_STUB_ARGS="$TMP/args"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"
checks=0
expect() {
  local want="$1" helper="$2" pattern="$3"
  PASS=0; FAIL=0; ERRORS=""
  "$helper" probe 'SELECT 1;' "$pattern" :memory: || true
  if [ "$PASS" -ne "$want" ] || [ "$FAIL" -ne $((1-want)) ]; then
    printf '%s\n' "FAIL: $helper pattern=$pattern rc=${DLTEST_STUB_RC:-0}" "$ERRORS" >&2
    exit 1
  fi
  checks=$((checks+1))
}
export DLTEST_STUB_RC=1 DLTEST_STUB_STDERR='Runtime error near line 1: no such table'
for pattern in 1 b a m . ''; do expect 0 run_test_match "$pattern"; done
export DLTEST_STUB_RC=0 DLTEST_STUB_STDOUT=$'1\n' DLTEST_STUB_STDERR='warning'
expect 1 run_test_match '^1$'
grep -q -- '-bail' "$TMP/args"
expect 0 run_test_match '^warning$'
expect 0 run_test_match ''
export DLTEST_STUB_RC=7
expect 0 run_test_match '^1$'
export DLTEST_STUB_RC=0 DLTEST_STUB_STDOUT='' DLTEST_STUB_STDERR='Error: boom'
expect 0 run_test_error_match '^Error: boom$'
export DLTEST_STUB_RC=1
expect 1 run_test_error_match '^Error: boom$'
expect 0 run_test_error_match ''
for rc in 125 134 139 142; do
  export DLTEST_STUB_RC="$rc"
  expect 0 run_test_error_match '^Error: boom$'
done
export DLTEST_STUB_RC=0 DLTEST_STUB_STDOUT=$'1\r\n' DLTEST_STUB_STDERR=''
DLTEST_STRIP_CR=1 expect 1 run_test_match '^1$'
export DLTEST_STUB_RC=1 DLTEST_STUB_STDERR=$'Error near line 1: expected refusal\r\n'
DLTEST_STRIP_CR=1 expect 1 run_test $'1\nError near line 1: expected refusal'
DLTEST_STRIP_CR=1 expect 1 run_test_error_match '^Error near line 1: expected refusal$'
export DLTEST_STUB_RC=0
DLTEST_STRIP_CR=1 expect 0 run_test $'1\nError near line 1: expected refusal'
export DLTEST_STUB_RC=1 DLTEST_STUB_STDOUT=$'1\n'
expect 0 run_test_lastline 1
export DLTEST_STUB_STDERR='Error near line 1: expected refusal'
PASS=0; FAIL=0; ERRORS=""
run_test_error_output_match recovery 'SELECT 1;' '^1$' :memory: 'expected refusal'
[ "$PASS" -eq 1 ] && [ "$FAIL" -eq 0 ]
PASS=0; FAIL=0; ERRORS=""
run_test_error_output_match wrong_class 'SELECT 1;' '^1$' :memory: 'other refusal'
[ "$PASS" -eq 0 ] && [ "$FAIL" -eq 1 ]
export DLTEST_STUB_STDERR=$'Error near line 1: expected refusal\nError near line 2: unexpected failure'
PASS=0; FAIL=0; ERRORS=""
run_test_error_output_match extra_failure $'SELECT 1;\nSELECT 2;' '^1$' :memory: 'expected refusal'
[ "$PASS" -eq 0 ] && [ "$FAIL" -eq 1 ]
dltest_init_queries
result=$(dltest_query :memory: 'SELECT 1;' 2>"$TMP/query.err" | tail -1) || true
PASS=0; FAIL=0; ERRORS=""
dltest_assert_equal captured "$result" 1
if (dltest_finish) > "$TMP/finish.log"; then
  echo 'FAIL: matching captured output hid an engine failure' >&2
  exit 1
fi
grep -q 'engine rc=1' "$TMP/finish.log"
checks=$((checks+4))
printf 'DoltLite test harness: %s checks passed\n' "$checks"
