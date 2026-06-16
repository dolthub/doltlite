#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$REPO_ROOT/build}"

if [ ! -d "$BUILD_DIR" ]; then
  echo "ERROR: build directory not found: $BUILD_DIR"
  echo "Run configure/make first, or set DOLTLITE_BUILD_DIR."
  exit 1
fi

if [ ! -x "$BUILD_DIR/doltlite" ]; then
  echo "ERROR: $BUILD_DIR/doltlite not found or not executable"
  echo "Run make in the build directory first."
  exit 1
fi

TESTS=(
  doltlite_parity.sh

  doltlite_commit.sh
  doltlite_staging.sh
  doltlite_workspace.sh
  doltlite_diff.sh
  doltlite_reset.sh
  doltlite_branch.sh
  doltlite_default_branch.sh
  doltlite_connect_branch.sh
  doltlite_tag.sh
  doltlite_merge.sh
  doltlite_merge_index_conflict.sh
  doltlite_stats_merge.sh
  doltlite_conflicts.sh
  doltlite_conflict_rows.sh
  doltlite_cherry_pick.sh
  doltlite_revert_dirty.sh

  doltlite_diff_table.sh
  doltlite_history.sh
  doltlite_at.sh
  doltlite_schema_diff.sh
  doltlite_dolt_table_pushdown.sh

  doltlite_persistence.sh
  doltlite_branding.sh
  doltlite_memory_db.sh
  doltlite_gc.sh
  doltlite_gc_session_state.sh
  doltlite_vacuum.sh
  doltlite_structural.sh
  chunk_physical_dups_test.sh
  doltlite_savepoint.sh
  doltlite_rollback_durability.sh
  doltlite_delete_or.sh
  doltlite_nocase_index.sh
  doltlite_update_replace_trigger.sh
  doltlite_trigger_visibility.sh
  doltlite_schema_merge.sh
  doltlite_branch_gc_stress.sh

  doltlite_unicode_blob.sh
  doltlite_edge_cases.sh
  doltlite_advanced.sh
  doltlite_working_set.sh
  doltlite_feature_deep.sh
  doltlite_deep_history.sh
  doltlite_perf.sh
  doltlite_count_perf.sh
  doltlite_demo.sh
  doltlite_e2e.sh

  doltlite_attach_sqlite.sh
  doltlite_dbpage.sh
  doltlite_arm_correctness.sh
  doltlite_open_sqlite_file.sh
  doltlite_comparison.sh
  doltlite_pragma_auto_vacuum.sh
  doltlite_pragma_encoding.sh
  doltlite_pragma_journal_mode.sh
  doltlite_pragma_memory_journal_mode.sh
  doltlite_pragma_page_count.sh
  doltlite_pragma_wal_checkpoint.sh
  doltlite_merge_ignore_corners.sh
  doltlite_record_format.sh
  doltlite_schema_cookie.sh
  doltlite_snapshot_isolation.sh
  doltlite_storage_locking.sh
  doltlite_unknown_table_cursor.sh
  doltlite_behavior.sh
  doltlite_branch_edge.sh
  doltlite_diff_alter.sh
  doltlite_gc_scale.sh
  doltlite_index_prefix.sh
  doltlite_row_count_estimate.sh
  doltlite_regression_test_c.sh
  review_regression_test.sh
)

total_pass=0
total_fail=0
failed=""

cd "$BUILD_DIR"

for t in "${TESTS[@]}"; do
  echo ""
  echo "━━━ $t ━━━"
  if bash "$SCRIPT_DIR/$t"; then
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
