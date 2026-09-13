#!/bin/bash
#
# A suite that dies partway through must not read as a pass. Most suites set
# `set -u` without `set -e`, so an abort (an unbound variable, a syntax error,
# a killed child) skips the final tally and leaves bash's exit status at 0.
# Requiring the tally line makes that a failure.

set -uo pipefail

SUITE="${1:?usage: run_guarded_suite.sh <suite.sh> [suite args...]}"
shift

OUT=$(mktemp "${TMPDIR:-/tmp}/guarded_suite.XXXXXX")
trap 'rm -f "$OUT"' EXIT

bash "$SUITE" "$@" 2>&1 | tee "$OUT"
rc=${PIPESTATUS[0]}

# A suite that stops early still prints whatever tallies it reached, so the
# tally cannot prove the run finished. Only the real end emits the sentinel.
# A suite that never ran says so with a trailing SKIP instead.
# Only a suite claiming success has to prove it got there; a non-zero status
# already fails the run.
if [ "$rc" -eq 0 ]; then
  last_line=$(grep -v '^[[:space:]]*$' "$OUT" | tail -1)
  if ! grep -qx '__SUITE_COMPLETE__' "$OUT" \
     && ! printf '%s' "$last_line" | grep -qE '^[[:space:]]*SKIP[: ]'; then
    echo ""
    echo "GUARD FAIL: $(basename "$SUITE") exited 0 without reporting completion."
    echo "  A suite that stops early reports no failures; that is not a pass."
    echo "  End through dltest_finish / vc_oracle_finish / stock_oracle_finish,"
    echo "  or print __SUITE_COMPLETE__ as the last thing the suite does."
    rc=1
  fi
fi

exit "$rc"
