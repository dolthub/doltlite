#!/usr/bin/env bash

set -u

BUILD_DIR="${1:-.}"
cd "$BUILD_DIR" || { echo "Build dir $BUILD_DIR not found"; exit 2; }

GATING=(
  ancestor_test
  sql_transaction_test
  invariant_test
  three_way_diff_test
  clone_error_code_test
  prolly_hashset_test
  prolly_chunker_boundary_test
  scoped_refs_push_test
  concurrent_stress_test
  vc_concurrency_test
  vc_ref_mutation_stress_test
  multi_process_test
  multi_process_gc_test
  multi_process_merge_rebase_test
  oom_dolt_fault_test
  cross_branch_test
  corruption_test
  prepared_stmt_reuse_test
  catalog_serialize_determinism_test
  sequence_reload_test
  chunk_store_fork_lock_test
  remotesrv_init_failure_test
  commit_deserialize_test
)

run_one() {
  local name="$1"
  local kind="$2"
  local exe="./$name"

  if [ ! -x "$exe" ]; then
    echo "MISSING: $exe (not built)"
    return 1
  fi

  echo "============================================================"
  echo "[$kind] running $name"
  echo "============================================================"
  "$exe"
  local ec=$?
  echo "[$kind] $name exited with $ec"
  return $ec
}

gating_failures=0

for t in "${GATING[@]}"; do
  if ! run_one "$t" GATING; then
    gating_failures=$((gating_failures + 1))
  fi
done

echo
echo "============================================================"
echo "Summary"
echo "============================================================"
echo "GATING failures        : $gating_failures / ${#GATING[@]}"

if [ "$gating_failures" -ne 0 ]; then
  echo "FAIL: $gating_failures gating tests failed"
  exit 1
fi

echo "OK: all gating tests passed"
exit 0
