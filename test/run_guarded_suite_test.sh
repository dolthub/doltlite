#!/usr/bin/env bash
#
# The guard exists because a suite that dies mid-run leaves bash's exit status
# at 0. Prove it catches that, and that it does not invent failures.
set -uo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
guard="$root/test/run_guarded_suite.sh"
pass=0; fail=0

expect_rc() {
  local name="$1" want="$2" suite="$3"
  local out rc
  out=$(bash "$guard" "$suite" 2>&1); rc=$?
  if [ "$rc" = "$want" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo "  FAIL: $name (want rc=$want, got rc=$rc)"
    echo "$out" | sed 's/^/      /'
  fi
}

cat > "$tmp/complete_pass.sh" <<'SH'
set -u
echo "Results: 3 passed, 0 failed"
SH

cat > "$tmp/complete_fail.sh" <<'SH'
set -u
echo "Results: 2 passed, 1 failed"
exit 1
SH

# The #2865 shape: set -u, no set -e, arithmetic over non-numeric engine output.
cat > "$tmp/dies_midway.sh" <<'SH'
set -u
fail=0
X="BROKEN ENGINE OUTPUT"
[ "1" = "$((X*2))" ] && echo ok || { fail=1; echo "FAIL: recorded"; }
echo "Results: 0 passed, 1 failed"
if [ $fail -gt 0 ]; then exit 1; fi
SH

cat > "$tmp/silent_exit.sh" <<'SH'
set -u
echo "starting"
exit 0
SH

expect_rc "completed_pass_stays_zero" 0 "$tmp/complete_pass.sh"
expect_rc "completed_failure_stays_nonzero" 1 "$tmp/complete_fail.sh"
expect_rc "death_before_tally_is_a_failure" 1 "$tmp/dies_midway.sh"
expect_rc "silent_exit_is_a_failure" 1 "$tmp/silent_exit.sh"

# Without the guard the dying suite reports success; that is the bug.
bash "$tmp/dies_midway.sh" >/dev/null 2>&1
if [ $? -eq 0 ]; then
  pass=$((pass+1))
else
  fail=$((fail+1))
  echo "  FAIL: unguarded_death_still_exits_zero (fixture no longer reproduces the bug)"
fi

echo ""
echo "Results: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then exit 1; fi
