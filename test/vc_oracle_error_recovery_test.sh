#!/bin/bash
#
# Error recovery oracle tests (doltlite vs Dolt)
#
# Tests that trigger errors mid-stream and verify both engines land
# in the same state afterward. Uses -c (continue on error) so both
# engines execute the full script including statements after errors.
#
# These validate that failed operations don't corrupt state:
# - Failed merges (conflicts) leave table in pre-merge state
# - Failed commits don't create partial commits
# - Failed checkouts don't switch branches
# - Recovery operations (reset, resolve) work after failures
#
# Usage: bash vc_oracle_error_recovery_test.sh ./doltlite dolt
#

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize() {
  tr -d '\r' | grep -v '^$' | sort
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode csv\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^$' \
           | grep -vi 'already up to date' \
           | grep -vi 'Fast-forward' \
           | tr -d '"' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dolt_query
  dolt_query=$(vc_oracle_translate_for_dolt "$query")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n" "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>>"$dir/dt.err" \
      | tail -n +2 | tr -d '"'
  ) 2>/dev/null
  dt_out=$(echo "$dt_out" | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | head -20 | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | head -20 | sed 's/^/      /'
  fi
}

echo "=== Error Recovery Oracle Tests ==="
echo ""

# ═══════════════════════════════════════════════════════════════════
# Section 1: Failed merge (conflict) — table state preserved
# ═══════════════════════════════════════════════════════════════════
echo "--- failed merge: table state preserved ---"

oracle "conflict_preserves_unmodified_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'conflict_target'),(2,'safe'),(3,'safe2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t WHERE id >= 2 ORDER BY id;"

oracle "conflict_row_count_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "conflict_non_conflicting_rows_unchanged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1'),(2,'a2','b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='FEAT' WHERE id=1;
UPDATE t SET a='feat2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET a='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, b FROM t ORDER BY id;"

oracle "conflict_other_table_unaffected" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'conflict');
INSERT INTO t2 VALUES(1,'safe');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t1 SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t1 SET val='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t2 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 2: Failed commit — no partial state
# ═══════════════════════════════════════════════════════════════════
echo "--- failed commit: no partial state ---"

oracle "empty_commit_rejected_data_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','should fail empty');
" "SELECT id, val FROM t ORDER BY id;"

oracle "commit_no_message_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit();
" "SELECT id, val FROM t ORDER BY id;"

oracle "commit_after_failed_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty should fail');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','real commit');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 3: Failed checkout — branch unchanged
# ═══════════════════════════════════════════════════════════════════
echo "--- failed checkout: branch unchanged ---"

oracle "checkout_nonexistent_stays_on_current" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('nonexistent_branch');
INSERT INTO t VALUES(2,'still_on_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','still main');
" "SELECT id, val FROM t ORDER BY id;"

oracle "checkout_b_existing_stays_on_current" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','on feat', '--allow-empty');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'still_on_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 4: Reset after conflict
# ═══════════════════════════════════════════════════════════════════
echo "--- reset after conflict ---"

oracle "hard_reset_after_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard', 'HEAD');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 5: Operations after failed operations
# ═══════════════════════════════════════════════════════════════════
echo "--- operations after failures ---"

oracle "insert_after_failed_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'after_conflict');
" "SELECT id, val FROM t WHERE id=2;"

oracle "add_commit_after_failed_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty fail');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','works');
" "SELECT count(*) FROM dolt_log;"

oracle "checkout_after_failed_checkout" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('does_not_exist');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'after_recovery');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recovery commit');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_after_failed_merge_resolved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
UPDATE t SET val='F1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat1');
SELECT dolt_reset('--hard','HEAD');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 6: Delete-modify conflict state
# ═══════════════════════════════════════════════════════════════════
echo "--- delete-modify conflict state ---"

oracle "delete_modify_safe_rows_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'target'),(2,'safe1'),(3,'safe2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes');
SELECT dolt_checkout('main');
UPDATE t SET val='modified' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t WHERE id > 1 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 7: Multiple errors in sequence
# ═══════════════════════════════════════════════════════════════════
echo "--- multiple errors in sequence ---"

oracle "multiple_failed_commits_then_success" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','fail1');
SELECT dolt_commit('-m','fail2');
SELECT dolt_commit('-m','fail3');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','success');
" "SELECT id, val FROM t ORDER BY id;"

oracle "multiple_bad_checkouts_then_good" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('bad1');
SELECT dolt_checkout('bad2');
SELECT dolt_checkout('bad3');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'recovered');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recovery');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 8: Commit log integrity after errors
# ═══════════════════════════════════════════════════════════════════
echo "--- commit log integrity after errors ---"

oracle "log_count_after_failed_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_commit('-m','empty fail');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_commit('-m','empty fail 2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_log;"

oracle "log_count_after_failed_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

# ═══════════════════════════════════════════════════════════════════
# Section 9: Working set after errors
# ═══════════════════════════════════════════════════════════════════
echo "--- working set after errors ---"

oracle "uncommitted_data_survives_failed_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'uncommitted');
SELECT dolt_commit('-m','fail no add');
" "SELECT id, val FROM t ORDER BY id;"

oracle "working_set_after_bad_checkout" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'working');
SELECT dolt_checkout('nonexistent');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 10: FK constraint errors during merge
# ═══════════════════════════════════════════════════════════════════
echo "--- FK errors in merge ---"

oracle "fk_parent_data_safe_after_failed_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE child SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE child SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, name FROM parent ORDER BY id;"


# ═══════════════════════════════════════════════════════════════════
# Section 11: Cherry-pick error recovery
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick error recovery ---"

oracle "cherry_pick_bad_ref_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_cherry_pick('nonexistent_ref');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_conflict_preserves_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_cherry_pick('feat');
" "SELECT count(*) FROM t;"

oracle "operations_after_failed_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_cherry_pick('bad_ref');
INSERT INTO t VALUES(2,'still works');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after failed cherry pick');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 12: Revert error recovery
# ═══════════════════════════════════════════════════════════════════
echo "--- revert error recovery ---"

oracle "revert_bad_ref_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_revert('nonexistent_ref');
" "SELECT id, val FROM t ORDER BY id;"

oracle "operations_after_failed_revert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_revert('bad_ref');
INSERT INTO t VALUES(2,'still works');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after failed revert');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 13: Reset error recovery
# ═══════════════════════════════════════════════════════════════════
echo "--- reset error recovery ---"

oracle "reset_bad_ref_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','nonexistent_ref');
" "SELECT id, val FROM t ORDER BY id;"

oracle "reset_soft_after_failed_hard" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','bad_ref');
SELECT dolt_reset('HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 14: Conflict resolution flow
# ═══════════════════════════════════════════════════════════════════
echo "--- conflict resolution flow ---"

oracle "reset_hard_head_clears_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard','HEAD');
INSERT INTO t VALUES(3,'after reset');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post reset');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_works_after_reset_from_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','conflict_branch');
UPDATE t SET val='conflict' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','conflict');
SELECT dolt_checkout('main');
UPDATE t SET val='main_conflict' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main conflict');
SELECT dolt_merge('conflict_branch');
SELECT dolt_reset('--hard','HEAD');
SELECT dolt_checkout('-b','clean_branch');
INSERT INTO t VALUES(3,'clean');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','clean');
SELECT dolt_checkout('main');
SELECT dolt_merge('clean_branch');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 15: Multi-table error isolation
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-table error isolation ---"

oracle "conflict_in_one_table_other_tables_queryable" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t3(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'conflict');
INSERT INTO t2 VALUES(1,'safe');
INSERT INTO t3 VALUES(1,'safe');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t1 SET val='feat' WHERE id=1;
INSERT INTO t2 VALUES(2,'feat_t2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t1 SET val='main' WHERE id=1;
INSERT INTO t3 VALUES(2,'main_t3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t3 ORDER BY id;"

oracle "failed_commit_doesnt_affect_other_tables" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t1 VALUES(2,'b');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('t1');
SELECT dolt_commit('-m','only t1');
" "SELECT id, val FROM t1 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 16: Staged state after errors
# ═══════════════════════════════════════════════════════════════════
echo "--- staged state after errors ---"

oracle "staged_survives_failed_commit_then_succeeds" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'staged');
SELECT dolt_add('-A');
SELECT dolt_commit();
SELECT dolt_commit('-m','now with message');
" "SELECT id, val FROM t ORDER BY id;"

oracle "add_after_failed_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('nonexistent_table');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','works');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 17: Branch operations after errors
# ═══════════════════════════════════════════════════════════════════
echo "--- branch operations after errors ---"

oracle "create_branch_after_failed_create" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('feat');
SELECT dolt_branch('feat');
SELECT dolt_branch('feat2');
SELECT dolt_checkout('feat2');
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_branch_after_failed_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d','nonexistent');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 18: DML errors don't corrupt VC state
# ═══════════════════════════════════════════════════════════════════
echo "--- DML errors + VC state ---"

oracle "constraint_error_doesnt_break_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT NOT NULL);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2, NULL);
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after constraint error');
" "SELECT id, val FROM t ORDER BY id;"

oracle "unique_violation_doesnt_break_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT UNIQUE);
INSERT INTO t VALUES(1,'unique_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'unique_val');
INSERT INTO t VALUES(3,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after unique violation');
" "SELECT id, val FROM t ORDER BY id;"

oracle "fk_violation_doesnt_break_commit" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO child VALUES(2,999,'bad_fk');
INSERT INTO child VALUES(3,1,'good');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after fk error');
" "SELECT id, val FROM child ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 19: Merge state cleanup
# ═══════════════════════════════════════════════════════════════════
echo "--- merge state cleanup ---"

oracle "new_branch_after_conflict_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard','HEAD');
SELECT dolt_checkout('-b','fresh');
INSERT INTO t VALUES(2,'fresh');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','fresh branch');
SELECT dolt_checkout('main');
SELECT dolt_merge('fresh');
" "SELECT id, val FROM t ORDER BY id;"

oracle "commit_after_conflict_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard','HEAD');
UPDATE t SET val='post_reset' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post reset commit');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 20: Rapid error-success alternation
# ═══════════════════════════════════════════════════════════════════
echo "--- rapid error-success alternation ---"

oracle "alternating_good_bad_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_commit('-m','fail1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_commit('-m','fail2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_commit('-m','fail3');
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
" "SELECT id, val FROM t ORDER BY id;"

oracle "alternating_good_bad_checkouts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('nonexistent1');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('nonexistent2');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 21: Data type preservation through errors
# ═══════════════════════════════════════════════════════════════════
echo "--- data type preservation through errors ---"

oracle "integer_values_survive_failed_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, num INTEGER, big INTEGER);
INSERT INTO t VALUES(1, 42, 1000000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET num=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET num=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, big FROM t;"

oracle "blob_survives_failed_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB, other TEXT);
INSERT INTO t VALUES(1, X'DEADBEEF', 'conflict');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET other='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET other='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, hex(data) FROM t;"

oracle "null_values_survive_failed_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1, NULL, NULL),(2, 'val', NULL),(3, NULL, 'val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_cherry_pick('bad_ref');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 22: Log count verification after error sequences
# ═══════════════════════════════════════════════════════════════════
echo "--- log counts after error sequences ---"

oracle "log_count_3_after_5_attempts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_commit('-m','fail');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_commit('-m','fail');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_log;"

oracle "log_count_stable_after_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard','HEAD');
" "SELECT count(*) FROM dolt_log;"

# ═══════════════════════════════════════════════════════════════════
# Section 23: Complex error recovery sequences
# ═══════════════════════════════════════════════════════════════════
echo "--- complex error recovery ---"

oracle "full_workflow_with_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('bad_branch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "error_in_middle_of_branch_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-m','forgot add');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat with add');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "multiple_tables_error_recovery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_commit('-m','no add fail');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','commit both');
" "SELECT 't1' AS tbl, count(*) AS n FROM t1 UNION ALL SELECT 't2', count(*) FROM t2 ORDER BY 1;"

# ═══════════════════════════════════════════════════════════════════
# Results
# ═══════════════════════════════════════════════════════════════════
echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
