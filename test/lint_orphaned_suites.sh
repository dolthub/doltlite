#!/usr/bin/env bash
# Fail if a suite is neither run nor listed.
# "Run": basename in CI workflows/actions, CI globs (oracle_*_test.sh,
# vc_oracle_*_test.sh), a regression bucket (*.test),
# test/lib/doltlite_suite_manifest.sh, test/run_c_tests.sh, or main.mk.
# Else listed in test/ci_suite_allowlist.txt or test/ci_suite_quarantine.txt
# (with a reason). Same for test/doltlite_*.test (buckets; inherited *.test
# are gated elsewhere) and test/*_test.c plus test/doltlite_*.c
# (run_c_tests.sh / workflow / manifest — not merely built by main.mk).
# Fuzzer GROUPS must be selected by a job. EXCLUDE skips harnesses/libs/perf.
# Upstream *.test under test/, ext/, mptest/ and tool/ must be bucketed,
# crash-listed, or in known_testfixture_unrun.txt; every regression and
# oracle bucket must be selected by each workflow matrix that reads buckets.

set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
ci_sources=(.github/workflows/ .github/actions/)

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
      "${ci_sources[@]}" \
      test/lib/doltlite_suite_manifest.sh test/run_c_tests.sh main.mk \
      2>/dev/null | grep -q .
}

# CI globs workflows expand at runtime.
matches_ci_glob() {
  case "$1" in
    oracle_*_test.sh|vc_oracle_*_test.sh)
      grep -rqE "oracle_\*_test\.sh|vc_oracle_\*_test\.sh" "${ci_sources[@]}" ;;
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
# A mention is not a runner: the name needs a token boundary, so a longer
# suite does not cover a shorter one, and a commented-out line does not count.
c_test_gated() {  # c_test_gated <name-without-.c>
  grep -rIhE "(^|[^A-Za-z0-9_])$1([^A-Za-z0-9_]|$)" \
      test/run_c_tests.sh "${ci_sources[@]}" \
      test/lib/doltlite_suite_manifest.sh \
      2>/dev/null | grep -qvE '^[[:space:]]*#'
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

# A DoltLite-authored C suite needs a runner whatever it is named; the
# inherited upstream tree is left to *_test.c above.
for path in test/doltlite_*.c; do
  [ -e "$path" ] || continue
  base="$(basename "$path")"
  case "$base" in *_test.c) continue ;; esac
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
    grep -rhE "DOLTLITE_DIFF_GROUPS=" "${ci_sources[@]}" 2>/dev/null \
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

# Every upstream testfixture file is bucketed, crash-listed, or listed as
# unrun with a reason. run_testfixture.sh resolves a bucket entry by
# basename across test/ and ext/, so a basename must name one file.
crash_list=test/known_testfixture_crashes.txt
unrun_list=test/known_testfixture_unrun.txt
vacuous_list=test/known_testfixture_vacuous.txt
fixture_orphans=()
fixture_dups=()
unrun_stale=()
entries() {  # entries <file>: first field of each non-comment line
  [ -f "$1" ] || return 0
  sed 's/#.*//' "$1" | awk 'NF {print $1}'
}
unrun_patterns=()
while IFS= read -r pat; do unrun_patterns+=("$pat"); done < <(entries "$unrun_list")
unrun_listed() {  # unrun_listed <repo-relative path>
  local pat
  for pat in ${unrun_patterns[@]+"${unrun_patterns[@]}"}; do
    # shellcheck disable=SC2053
    [[ "$1" == $pat ]] && return 0
  done
  return 1
}
fixture_files=()
while IFS= read -r f; do fixture_files+=("$f"); done < <(
  find test ext mptest tool -name '*.test' -type f 2>/dev/null | LC_ALL=C sort)
fixture_names=$(for f in ${fixture_files[@]+"${fixture_files[@]}"}; do basename "$f" .test; done)
while IFS= read -r d; do
  [ -n "$d" ] && fixture_dups+=("$d")
done < <(printf '%s\n' "$fixture_names" | LC_ALL=C sort | uniq -d)
for f in ${fixture_files[@]+"${fixture_files[@]}"}; do
  name=$(basename "$f" .test)
  case "$name" in doltlite_*) continue ;; esac
  if in_bucket "$name"; then
    unrun_listed "$f" && unrun_stale+=("$f (bucketed and listed in $(basename "$unrun_list"))")
    continue
  fi
  entries "$crash_list" | grep -Fxq "$name" && continue
  unrun_listed "$f" && continue
  fixture_orphans+=("$f")
done
for pat in ${unrun_patterns[@]+"${unrun_patterns[@]}"}; do
  matched=0
  for f in ${fixture_files[@]+"${fixture_files[@]}"}; do
    # shellcheck disable=SC2053
    [[ "$f" == $pat ]] && { matched=1; break; }
  done
  [ "$matched" -eq 1 ] || unrun_stale+=("$pat (listed in $(basename "$unrun_list"), matches no file)")
done
while IFS= read -r name; do
  [ -z "$name" ] && continue
  in_bucket "$name" || unrun_stale+=("$name (listed in $(basename "$vacuous_list"), not bucketed)")
done < <(entries "$vacuous_list")

# A bucket no workflow matrix selects runs nowhere, however carefully its
# files are listed. A matrix row is either "- name" or "bucket: a b".
matrix_orphans=()
bucket_selected() {  # bucket_selected <dir-name> <bucket>
  local wf found=0
  for wf in .github/workflows/*.yml; do
    grep -qF "$1/" "$wf" || continue
    grep -qF '${{ matrix.bucket }}' "$wf" || continue
    if grep -qE "^[[:space:]]*-[[:space:]]+$2[[:space:]]*$|^[[:space:]]*bucket:.*[[:space:]]$2([[:space:]]|$)" "$wf"; then
      found=1
    else
      echo "$wf"
    fi
  done
  [ "$found" -eq 1 ] || echo "(no workflow)"
}
for dir in regression-buckets oracle-buckets; do
  for b in test/$dir/*.txt; do
    [ -e "$b" ] || continue
    name=$(basename "$b" .txt)
    while IFS= read -r wf; do
      [ -n "$wf" ] && matrix_orphans+=("$dir/$name.txt (not selected by $wf)")
    done < <(bucket_selected "$dir" "$name")
  done
done

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
if [ "${#fixture_orphans[@]}" -ne 0 ]; then
  echo "ERROR: upstream testfixture files run nowhere (add to a bucket, or list"
  echo "       in test/known_testfixture_unrun.txt with a reason):"
  printf '  - %s\n' "${fixture_orphans[@]}"
  rc=1
fi
if [ "${#fixture_dups[@]}" -ne 0 ]; then
  echo "ERROR: testfixture basenames that name more than one file:"
  printf '  - %s\n' "${fixture_dups[@]}"
  rc=1
fi
if [ "${#matrix_orphans[@]}" -ne 0 ]; then
  echo "ERROR: buckets no workflow matrix runs:"
  printf '  - %s\n' "${matrix_orphans[@]}"
  rc=1
fi
if [ "${#unrun_stale[@]}" -ne 0 ]; then
  echo "ERROR: stale testfixture inventory entries:"
  printf '  - %s\n' "${unrun_stale[@]}"
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
