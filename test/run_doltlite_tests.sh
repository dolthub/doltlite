#!/bin/bash
# run_doltlite_tests.sh — Run all doltlite feature test scripts
#
# Usage: bash run_doltlite_tests.sh
#
# Runs from the build directory. Exits non-zero on first failure.

set -euo pipefail

TESTS=(
  # Core SQL parity
  doltlite_parity.sh

  # Versioning features
  doltlite_commit.sh
  doltlite_staging.sh
  doltlite_diff.sh
  doltlite_reset.sh
  doltlite_branch.sh
  doltlite_tag.sh
  doltlite_merge.sh
  doltlite_conflicts.sh
  doltlite_conflict_rows.sh
  doltlite_cherry_pick.sh

  # Virtual tables
  doltlite_diff_table.sh
  doltlite_history.sh
  doltlite_at.sh
  doltlite_schema_diff.sh

  # Storage and persistence
  doltlite_persistence.sh
  doltlite_branding.sh
  doltlite_gc.sh
  doltlite_structural.sh
  doltlite_savepoint.sh
  doltlite_schema_merge.sh
  doltlite_branch_gc_stress.sh

  # Edge cases and integration
  doltlite_unicode_blob.sh
  doltlite_edge_cases.sh
  doltlite_advanced.sh
  doltlite_working_set.sh
  doltlite_feature_deep.sh
  doltlite_deep_history.sh
  doltlite_perf.sh
  doltlite_demo.sh
  doltlite_e2e.sh

  # Additional tests
  doltlite_attach_sqlite.sh
  doltlite_behavior.sh
  doltlite_branch_edge.sh
  doltlite_diff_alter.sh
  doltlite_gc_scale.sh
  doltlite_index_prefix.sh
  doltlite_issue179_180.sh
  concurrent_refs_test.sh
  checkout_persist_failure_test.sh
  savepoint_catalog_restore_test.sh
  review_regression_test.sh
)

total_pass=0
total_fail=0
failed=""

for t in "${TESTS[@]}"; do
  echo ""
  echo "━━━ $t ━━━"
  if bash "../test/$t"; then
    total_pass=$((total_pass + 1))
  else
    total_fail=$((total_fail + 1))
    failed="$failed $t"
    echo "FAIL: $t"
  fi
done

echo ""
echo "════════════════════════════════════════"
echo "Doltlite tests: $total_pass passed, $total_fail failed out of $((total_pass + total_fail)) suites"
if [ $total_fail -gt 0 ]; then
  echo "Failures:$failed"
  echo "════════════════════════════════════════"
  exit 1
fi
echo "════════════════════════════════════════"
