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

if ! grep -qE '[0-9]+ passed, [0-9]+ failed' "$OUT"; then
  echo ""
  echo "GUARD FAIL: $(basename "$SUITE") exited without reporting a result tally (rc=$rc)."
  echo "  A suite that stops early reports no failures; that is not a pass."
  if [ "$rc" -eq 0 ]; then rc=1; fi
fi

exit "$rc"
