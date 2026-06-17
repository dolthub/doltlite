#!/usr/bin/env bash
#
# Guard against orphaned test suites: every test/*.sh suite must actually be
# run somewhere, or be explicitly accounted for. A suite that exists but no
# job invokes is dead weight that rots silently (we found rebase, large-merge,
# persist-failure, and remote suites in exactly this state).
#
# A suite counts as "run" if its basename is referenced by:
#   - any .github/workflows/*.yml (explicit invocation), or
#   - a CI glob those workflows expand (oracle_*_test.sh, vc_oracle_*_test.sh), or
#   - test/lib/doltlite_suite_manifest.sh, test/run_c_tests.sh, or
#   - main.mk (lint targets run via `make`),
# or it is listed in one of the two manifests beside this script:
#   - test/ci_suite_allowlist.txt   suites run by a dedicated mechanism that
#                                   the greps above don't catch, with a reason.
#   - test/ci_suite_quarantine.txt  suites known NOT to run (broken/stale),
#                                   each with a reason — visible, tracked debt.
#
# Non-suite scripts (harnesses, libs, perf drivers) are skipped via EXCLUDE.
#
# Exit nonzero (listing the offenders) if any suite is neither run nor listed.

set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Scripts that are not test suites: runners/harnesses, the shared lib, perf
# drivers, and this guard itself.
is_excluded() {
  case "$1" in
    run_*.sh|lib/*|sysbench*.sh|time-*.sh|*wordcount*.sh|lint_orphaned_suites.sh)
      return 0 ;;
    *) return 1 ;;
  esac
}

allowlist="$SCRIPT_DIR/ci_suite_allowlist.txt"
quarantine="$SCRIPT_DIR/ci_suite_quarantine.txt"

listed_in() {  # listed_in <file> <basename>
  [ -f "$1" ] || return 1
  grep -vE '^\s*(#|$)' "$1" | grep -Fxq "$2"
}

# A reference is "real" if it appears outside this guard and the manifests.
referenced() {
  local base="$1"
  grep -rIlF "$base" \
      .github/workflows/ \
      test/lib/doltlite_suite_manifest.sh test/run_c_tests.sh main.mk \
      2>/dev/null | grep -q .
}

# CI globs the workflows expand at runtime.
matches_ci_glob() {
  case "$1" in
    oracle_*_test.sh|vc_oracle_*_test.sh)
      grep -rqE "oracle_\*_test\.sh|vc_oracle_\*_test\.sh" .github/workflows/ ;;
    *) return 1 ;;
  esac
}

orphans=()
quarantined=0
for path in test/*.sh; do
  base="$(basename "$path")"
  is_excluded "$base" && continue
  if referenced "$base" || matches_ci_glob "$base" || listed_in "$allowlist" "$base"; then
    continue
  fi
  if listed_in "$quarantine" "$base"; then
    quarantined=$((quarantined + 1))
    continue
  fi
  orphans+=("$base")
done

# Manifest hygiene: an allow/quarantine entry that no longer exists, or a
# quarantined suite that is now also wired in, is stale — flag it so the
# lists don't drift.
stale=()
for f in "$allowlist" "$quarantine"; do
  [ -f "$f" ] || continue
  while IFS= read -r entry; do
    [ -z "$entry" ] && continue
    [ -f "test/$entry" ] || stale+=("$entry (listed in $(basename "$f"), no such suite)")
  done < <(grep -vE '^\s*(#|$)' "$f")
done
while IFS= read -r entry; do
  [ -z "$entry" ] && continue
  if referenced "$entry" || matches_ci_glob "$entry"; then
    stale+=("$entry (quarantined but now run — remove from quarantine)")
  fi
done < <(grep -vE '^\s*(#|$)' "$quarantine" 2>/dev/null || true)

rc=0
if [ "${#orphans[@]}" -ne 0 ]; then
  echo "ERROR: orphaned test suites (run nowhere; wire into a runner/workflow,"
  echo "       or add to test/ci_suite_allowlist.txt / ci_suite_quarantine.txt):"
  printf '  - %s\n' "${orphans[@]}"
  rc=1
fi
if [ "${#stale[@]}" -ne 0 ]; then
  echo "ERROR: stale manifest entries:"
  printf '  - %s\n' "${stale[@]}"
  rc=1
fi

if [ "$rc" -eq 0 ]; then
  echo "OK: every test suite is run or accounted for (${quarantined} quarantined)."
fi
exit $rc
