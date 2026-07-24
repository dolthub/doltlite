#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/build_artifacts.sh"

BUILD_DIR="${1:-.}"
SUITE_SET="${2:-all}"
BUILD_MODE="${DOLTLITE_C_TESTS_BUILD:-auto}"
case "${3:-}" in
  --build)    BUILD_MODE=always ;;
  --no-build) BUILD_MODE=never ;;
  "")         ;;
  *) echo "ERROR: unknown option: $3"; exit 2 ;;
esac

cd "$BUILD_DIR" || { echo "Build dir $BUILD_DIR not found"; exit 2; }

COVERAGE_TESTS=(
  ancestor_test
  sql_transaction_test
  invariant_test
  three_way_diff_test
  clone_error_code_test
  prolly_hashset_test
  prolly_chunker_boundary_test
  scoped_refs_push_test
  cross_branch_test
  corruption_test
  prepared_stmt_reuse_test
  catalog_serialize_determinism_test
  sequence_reload_test
  remotesrv_init_failure_test
  commit_deserialize_test
)

SPECIALIZED_TESTS=(
  concurrent_stress_test
  prolly_txn_stress_test
  vc_concurrency_test
  vc_ref_mutation_stress_test
  multi_process_test
  multi_process_gc_test
  multi_process_merge_rebase_test
  oom_dolt_fault_test
  chunk_store_fork_lock_test
)

case "$SUITE_SET" in
  all) GATING=("${COVERAGE_TESTS[@]}" "${SPECIALIZED_TESTS[@]}") ;;
  coverage) GATING=("${COVERAGE_TESTS[@]}") ;;
  specialized) GATING=("${SPECIALIZED_TESTS[@]}") ;;
  *)
    echo "ERROR: unknown C suite set: $SUITE_SET"
    exit 2
    ;;
esac

# Build what we gate on, so a developer running this never silently validates
# last week's binaries. Skipped under CI, where the build phase already produced
# these and the extracted tree's mtimes would force a pointless full rebuild.
did_build=0
if [ "$BUILD_MODE" = auto ] && dl_is_ci; then
  BUILD_MODE=never
fi
if [ "$BUILD_MODE" != never ] && [ -f Makefile ]; then
  echo "=== building gated C tests (make doltlite-c-tests-build) ==="
  if make doltlite-c-tests-build; then
    did_build=1
  else
    echo
    echo "ERROR: could not build the gated C tests; results below would describe" >&2
    echo "       whatever binaries happen to be lying around. Fix the build first." >&2
    exit 2
  fi
  echo
elif [ "$BUILD_MODE" = always ]; then
  echo "ERROR: --build requested but no Makefile in $(pwd)" >&2
  exit 2
fi

# Only meaningful when we did not just build, and only off CI: a stale binary is
# the failure mode that reports a pass for code that no longer exists.
src_epoch=""
if [ "$did_build" -eq 0 ] && ! dl_is_ci; then
  src_epoch=$(dl_newest_source_epoch "$REPO_ROOT")
fi

passed=0
failed=0
notbuilt=0
stale=0
FAILED_NAMES=""
NOTBUILT_NAMES=""
STALE_NAMES=""

run_one() {
  local name="$1"
  local kind="$2"
  local exe="./$name"

  if [ ! -x "$exe" ]; then
    echo "NOT BUILT: $exe"
    notbuilt=$((notbuilt + 1))
    NOTBUILT_NAMES="$NOTBUILT_NAMES $name"
    return
  fi

  if [ -n "$src_epoch" ] && dl_artifact_is_stale "$exe" "$src_epoch"; then
    echo "STALE: $exe predates src/ -- running it anyway, result is not trustworthy"
    stale=$((stale + 1))
    STALE_NAMES="$STALE_NAMES $name"
  fi

  echo "============================================================"
  echo "[$kind] running $name"
  echo "============================================================"
  "$exe"
  local ec=$?
  echo "[$kind] $name exited with $ec"
  if [ "$ec" -eq 0 ]; then
    passed=$((passed + 1))
  else
    failed=$((failed + 1))
    FAILED_NAMES="$FAILED_NAMES $name"
  fi
}

for t in "${GATING[@]}"; do
  run_one "$t" GATING
done

echo
echo "============================================================"
echo "Summary"
echo "============================================================"
echo "gated     : ${#GATING[@]}"
echo "passed    : $passed"
echo "failed    : $failed"
echo "not built : $notbuilt"
[ "$stale" -ne 0 ] && echo "stale     : $stale"

rc=0
if [ "$failed" -ne 0 ]; then
  echo
  echo "FAIL: $failed test(s) failed:$FAILED_NAMES"
  rc=1
fi
if [ "$notbuilt" -ne 0 ]; then
  echo
  echo "FAIL: $notbuilt test(s) never ran because they are not built:$NOTBUILT_NAMES"
  echo "      A test that did not run is not a test that passed."
  if [ -f Makefile ]; then
    echo "      Build them: make doltlite-c-tests-build"
  else
    echo "      This looks like a prebuilt artifact directory; the build that"
    echo "      produced it did not include these targets."
  fi
  rc=1
fi
if [ "$stale" -ne 0 ]; then
  echo
  echo "FAIL: $stale binary(ies) predate src/ and cannot be trusted:$STALE_NAMES"
  if [ -f Makefile ]; then
    dl_stale_hint "$(pwd)"
  else
    echo "      This directory has no Makefile, so these came from elsewhere;"
    echo "      rebuild them from current sources before believing the result."
  fi
  rc=1
fi

if [ "$rc" -eq 0 ]; then
  echo
  if [ "$did_build" -eq 1 ]; then
    echo "OK: all $passed gated tests built, ran, and passed"
  else
    echo "OK: all $passed gated tests ran and passed (prebuilt, not rebuilt here)"
  fi
fi
exit $rc
