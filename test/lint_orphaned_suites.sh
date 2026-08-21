#!/usr/bin/env bash
# Fail if a suite is neither run nor listed.
# "Run": basename in .github/workflows/*.yml, CI globs (oracle_*_test.sh,
# vc_oracle_*_test.sh), a regression bucket (*.test),
# test/lib/doltlite_suite_manifest.sh, test/run_c_tests.sh, or main.mk.
# Else listed in test/ci_suite_allowlist.txt or test/ci_suite_quarantine.txt
# (with a reason). Same for test/doltlite_*.test (buckets; inherited *.test
# are gated elsewhere) and test/*_test.c (run_c_tests.sh / workflow /
# manifest — not merely built by main.mk). Fuzzer GROUPS must be selected
# by a job. EXCLUDE skips harnesses/libs/perf.

set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Not suites: runners, lib, perf, this guard.
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

# *.test is gated if its basename is in a regression bucket.
in_bucket() {  # in_bucket <basename-without-.test>
  cat test/regression-buckets/*.txt 2>/dev/null \
    | grep -vE '^\s*(#|$)' | grep -Fxq "$1"
}

# Real if named outside this guard and the manifests.
referenced() {
  local base="$1"
  grep -rIlF "$base" \
      .github/workflows/ \
      test/lib/doltlite_suite_manifest.sh test/run_c_tests.sh main.mk \
      2>/dev/null | grep -q .
}

# CI globs workflows expand at runtime.
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

# doltlite_*.test must be in a bucket (or allowlisted). Inherited *.test are excluded.
for path in test/doltlite_*.test; do
  [ -e "$path" ] || continue
  base="$(basename "$path")"
  name="${base%.test}"
  if in_bucket "$name" || referenced "$base" || listed_in "$allowlist" "$base"; then
    continue
  fi
  if listed_in "$quarantine" "$base"; then
    quarantined=$((quarantined + 1))
    continue
  fi
  orphans+=("$base")
done

# main.mk only builds the binary; run_c_tests.sh / a workflow / the manifest must run it.
c_test_gated() {  # c_test_gated <name-without-.c>
  grep -rIlF "$1" \
      test/run_c_tests.sh .github/workflows/ \
      test/lib/doltlite_suite_manifest.sh \
      2>/dev/null | grep -q .
}

for path in test/*_test.c; do
  [ -e "$path" ] || continue
  base="$(basename "$path")"
  if c_test_gated "${base%.c}" || listed_in "$allowlist" "$base"; then
    continue
  fi
  if listed_in "$quarantine" "$base"; then
    quarantined=$((quarantined + 1))
    continue
  fi
  orphans+=("$base")
done

# Every generator group must be selected by some job.
group_orphans=()
if [ -f test/sql_differential_fuzzer.py ]; then
  # Ignore echo/printf reproduce hints; they run nothing.
  selections() {
    grep -rhE "DOLTLITE_DIFF_GROUPS=" .github/workflows/ 2>/dev/null \
      | grep -vE "^[[:space:]]*(#|echo|printf)"
  }
  if selections | grep -qE "DOLTLITE_DIFF_GROUPS=[\"']?all\b"; then
    : # a job sweeps every group
  else
    while IFS= read -r g; do
      [ -z "$g" ] && continue
      selections | grep -qF "$g" || group_orphans+=("$g")
    done < <(sed -n '/^GROUPS = \[/,/\]/p' test/sql_differential_fuzzer.py \
             | grep -oE '"[a-z-]+"' | tr -d '"')
  fi
fi

# Stale if the suite is gone or a quarantined suite is now also wired in.
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
if [ "${#group_orphans[@]}" -ne 0 ]; then
  echo "ERROR: differential-fuzzer groups no job selects (dead coverage):"
  printf '  - %s\n' "${group_orphans[@]}"
  echo "       Point a job at them (DOLTLITE_DIFF_GROUPS), or drop the group."
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
