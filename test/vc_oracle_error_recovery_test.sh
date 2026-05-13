#!/bin/bash

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

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Error Recovery Oracle Tests ==="
echo ""

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

oracle "add_top_level_savepoint_bad_option_persists_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'dirty');
SELECT dolt_add('--bogus');
ROLLBACK TO sp1;
" "SELECT count(*) FROM t;"

oracle "rebase_top_level_savepoint_unknown_upstream_persists_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'dirty');
SELECT dolt_rebase('nope');
ROLLBACK TO sp1;
" "SELECT count(*) FROM t;"

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
PRAGMA foreign_keys = ON;
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

echo "--- drop table error recovery ---"

oracle "drop_nonexistent_table_other_tables_intact" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE nonexistent;
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad drop');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "drop_table_after_failed_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','fail empty');
DROP TABLE t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','dropped t2');
INSERT INTO t1 VALUES(2,'post_drop');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after drop');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "create_then_drop_then_create_after_error" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty fail');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(99,'recreated');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recreated');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- alter table error recovery ---"

oracle "alter_add_duplicate_col_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN val TEXT;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad alter');
" "SELECT id, val FROM t ORDER BY id;"

oracle "alter_add_col_to_nonexistent_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE nonexistent ADD COLUMN x INTEGER;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad alter');
" "SELECT id, val FROM t ORDER BY id;"

oracle "alter_then_commit_after_prior_failure" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_cherry_pick('bad_ref');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7;
INSERT INTO t VALUES(2,'b',99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','altered');
" "SELECT id, val, extra FROM t ORDER BY id;"

echo "--- tag error recovery ---"

oracle "tag_duplicate_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_tag('v1','HEAD');
SELECT dolt_tag('v1','HEAD');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after dup tag');
" "SELECT id, val FROM t ORDER BY id;"

oracle "tag_bad_ref_then_good_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_tag('badtag','nonexistent_ref');
SELECT dolt_tag('goodtag','HEAD');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_nonexistent_tag_then_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_tag('-d','nonexistent');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad tag delete');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- invalid SQL recovery ---"

oracle "insert_wrong_col_count_others_succeed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b','extra');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after type error');
" "SELECT id, val FROM t ORDER BY id;"

oracle "select_from_nonexistent_then_real_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT * FROM nonexistent;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad select');
" "SELECT id, val FROM t ORDER BY id;"

oracle "update_nonexistent_col_others_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET nonexistent_col='x' WHERE id=1;
UPDATE t SET val='updated' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad update');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- create table error recovery ---"

oracle "create_duplicate_table_original_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE t(id INTEGER PRIMARY KEY, other TEXT);
INSERT INTO t VALUES(2,'still_has_val_col');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after dup create');
" "SELECT id, val FROM t ORDER BY id;"

oracle "create_if_not_exists_idempotent" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after idempotent create');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- error during multi-branch work ---"

oracle "error_on_feat_branch_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','empty fail');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-m','forgot add');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "errors_on_multiple_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_commit('-m','forgot add b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1 ok');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_commit('-m','forgot add b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2 ok');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "mixed_errors_then_successful_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_reset('--hard','bogus');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat despite errors');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- long error sequences ---"

oracle "ten_failed_commits_then_real" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','f1');
SELECT dolt_commit('-m','f2');
SELECT dolt_commit('-m','f3');
SELECT dolt_commit('-m','f4');
SELECT dolt_commit('-m','f5');
SELECT dolt_commit('-m','f6');
SELECT dolt_commit('-m','f7');
SELECT dolt_commit('-m','f8');
SELECT dolt_commit('-m','f9');
SELECT dolt_commit('-m','f10');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','real');
" "SELECT count(*) FROM dolt_log;"

oracle "many_bad_checkouts_then_real_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('b1');
SELECT dolt_checkout('b2');
SELECT dolt_checkout('b3');
SELECT dolt_checkout('b4');
SELECT dolt_checkout('b5');
SELECT dolt_checkout('b6');
SELECT dolt_checkout('b7');
INSERT INTO t VALUES(2,'still_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','still on main');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- errors + data type integrity ---"

oracle "text_values_preserved_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'with space'),(2,'with,comma'),(3,'with''quote');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
" "SELECT id, v FROM t ORDER BY id;"

oracle "unicode_preserved_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'héllo'),(2,'日本'),(3,'👋');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t WHERE id>=2 ORDER BY id;"

oracle "large_int_preserved_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, big INTEGER);
INSERT INTO t VALUES(1, 2147483647),(2, -2147483648);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_checkout('nonexistent');
SELECT dolt_reset('--hard','bogus');
" "SELECT id, big FROM t ORDER BY id;"

echo "--- aggregate/join errors ---"

oracle "count_on_nonexistent_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT count(*) FROM nonexistent_t;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad agg');
" "SELECT count(*) FROM t;"

oracle "join_on_nonexistent_then_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT t1.v FROM t1 JOIN t2 ON t1.id=t2.id;
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad join');
" "SELECT id, v FROM t1 ORDER BY id;"

echo "--- update/delete error flows ---"

oracle "update_then_fail_commit_update_again" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='first_update' WHERE id=1;
SELECT dolt_commit('-m','forgot add');
UPDATE t SET val='second_update' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','with add');
" "SELECT id, val FROM t;"

oracle "delete_fail_commit_delete_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-m','forgot add');
DELETE FROM t WHERE id>=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','delete all');
" "SELECT count(*) FROM t;"

echo "--- reset + error + reset ---"

oracle "reset_after_error_after_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_reset('--hard','bogus_ref');
SELECT dolt_reset('--hard','HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

oracle "reset_soft_hard_error_mix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--soft','HEAD~1');
SELECT dolt_reset('--hard','bogus');
SELECT dolt_reset('--hard','HEAD');
" "SELECT count(*) FROM dolt_log;"

echo "--- errors during conflict state ---"

oracle "failed_commit_during_conflict_unchanged" "
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
SELECT dolt_commit('-m','invalid mid-conflict');
SELECT dolt_reset('--hard','HEAD');
INSERT INTO t VALUES(2,'after_cleanup');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cleanup');
" "SELECT id, val FROM t ORDER BY id;"

oracle "bad_checkout_during_conflict" "
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
SELECT dolt_checkout('nonexistent');
SELECT dolt_reset('--hard','HEAD');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- errors + commit log stability ---"

oracle "hash_of_last_commit_stable_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','only_commit');
SELECT dolt_commit('-m','fail1');
SELECT dolt_commit('-m','fail2');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_reset('--hard','bogus');
" "SELECT count(*) FROM dolt_log;"

oracle "message_list_unchanged_by_failed_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_commit('-m','fail_empty');
SELECT dolt_commit('-m','fail_empty_2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m3');
" "SELECT message FROM dolt_log WHERE message LIKE 'm%' ORDER BY message;"

echo "--- recovery from staged-modified mix ---"

oracle "modify_after_add_error_no_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'staged');
SELECT dolt_add('-A');
UPDATE t SET val='modified_after_add' WHERE id=2;
SELECT dolt_commit('-m','empty fail should not happen');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','final');
" "SELECT id, val FROM t ORDER BY id;"

oracle "add_specific_fail_fallback_to_all" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t1 VALUES(2,'b');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('does_not_exist');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','both');
" "SELECT 't1' AS tbl, count(*) AS n FROM t1 UNION ALL SELECT 't2', count(*) FROM t2 ORDER BY 1;"

echo "--- mixed DDL/DML errors ---"

oracle "ddl_error_then_dml_success" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','dml after ddl error');
" "SELECT id, val FROM t ORDER BY id;"

oracle "dml_error_then_ddl_success" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(1,'duplicate_pk');
CREATE TABLE other(x INTEGER);
INSERT INTO other VALUES(42);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after dml err');
" "SELECT x FROM other;"

oracle "alternating_ddl_dml_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE nonexistent;
INSERT INTO t VALUES(1,'duplicate');
CREATE TABLE t(x INTEGER);
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after alternating errors');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- orphan SELECT errors ---"

oracle "bad_select_before_any_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT * FROM nonexistent;
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad select');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "select_ambiguous_col_doesnt_block_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT v FROM t1, t2;
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad select');
" "SELECT id, v FROM t1 ORDER BY id;"

echo "--- error right before commit ---"

oracle "error_between_add_and_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT * FROM bogus;
SELECT dolt_commit('-m','should still work');
" "SELECT id, v FROM t ORDER BY id;"

oracle "error_between_insert_and_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT count(*) FROM bogus;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','ok');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- error after successful merge ---"

oracle "error_after_successful_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_commit('-m','empty after merge');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
INSERT INTO t VALUES(3,'after_err');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- errors during reset flow ---"

oracle "error_after_soft_reset_before_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--soft','HEAD~1');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_commit('-m','recommit');
" "SELECT id, v FROM t ORDER BY id;"

oracle "error_after_hard_reset_then_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_checkout('nonexistent');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after reset+errors');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- NULL-related error recovery ---"

oracle "update_matching_null_no_rows_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,NULL),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v='matched_null' WHERE v=NULL;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after null compare');
" "SELECT id, v FROM t ORDER BY id;"

oracle "insert_with_null_into_not_null_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,NULL);
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- rapid branch switches with errors ---"

oracle "rapid_checkouts_mixed_good_bad" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('bogus1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('bogus2');
SELECT dolt_checkout('feat');
SELECT dolt_checkout('bogus3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- nested branch work errors ---"

oracle "errors_on_each_branch_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
SELECT dolt_cherry_pick('bogus');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
SELECT dolt_revert('bogus');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_reset('--hard','bogus');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_branches stable through errors ---"

oracle "branches_listed_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');
SELECT dolt_branch('b3');
SELECT dolt_branch('b1');
SELECT dolt_checkout('bogus');
SELECT dolt_reset('--hard','bogus');
" "SELECT name FROM dolt_branches ORDER BY name;"

echo "--- GROUP BY/HAVING errors ---"

oracle "bad_group_by_no_effect" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT v FROM t GROUP BY nonexistent_col;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad group');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- multiple bad resets ---"

oracle "three_bad_resets_then_good" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','bogus1');
SELECT dolt_reset('--hard','bogus2');
SELECT dolt_reset('--hard','bogus3');
SELECT dolt_reset('--hard','HEAD~1');
" "SELECT id, v FROM t ORDER BY id;"

oracle "mixed_reset_variants_with_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--soft','bogus');
SELECT dolt_reset('--hard','bogus');
SELECT dolt_reset('bogus');
SELECT dolt_reset('--hard','HEAD');
" "SELECT count(*) FROM dolt_log;"

echo "--- branch creation errors ---"

oracle "create_branch_bad_start_point" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('feat','bogus_start');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad branch create');
" "SELECT id, v FROM t ORDER BY id;"

oracle "checkout_b_duplicate_no_effect" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'main_after');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main after dup');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- many-error stress ---"

oracle "twenty_mixed_errors_then_success" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','e1');
SELECT dolt_cherry_pick('e2');
SELECT dolt_revert('e3');
SELECT dolt_reset('--hard','e4');
SELECT dolt_checkout('e5');
SELECT dolt_branch('-d','e6');
DROP TABLE e7;
ALTER TABLE e8 ADD COLUMN x INTEGER;
SELECT * FROM e9;
UPDATE e10 SET x=1;
DELETE FROM e11;
INSERT INTO t VALUES(1,'duplicate e12');
CREATE TABLE t(x INTEGER);
SELECT dolt_tag('e13','bogus');
SELECT dolt_commit('-m','e14');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_checkout('bogus');
SELECT dolt_reset('--hard','bogus');
INSERT INTO t VALUES(2,'finally');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','survived');
" "SELECT id, v FROM t ORDER BY id;"

oracle "error_storm_commit_log_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','good1');
SELECT dolt_commit('-m','e');
SELECT dolt_commit('-m','e');
SELECT dolt_commit('-m','e');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','good2');
SELECT dolt_commit('-m','e');
SELECT dolt_commit('-m','e');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','good3');
" "SELECT message FROM dolt_log WHERE message LIKE 'good%' ORDER BY message;"

echo "--- drop + error + recreate ---"

oracle "drop_error_recreate_different_schema" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE t;
SELECT dolt_commit('-m','empty fail');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, extra INTEGER);
INSERT INTO t VALUES(1,'new',99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recreate schema');
" "SELECT id, v, extra FROM t;"

echo "--- errors during merge prep ---"

oracle "error_before_noff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_merge('feat','--no-ff','-m','merged');
" "SELECT id, v FROM t ORDER BY id;"

oracle "error_between_merges" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_merge('b2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- errors after tag success ---"

oracle "errors_after_tag_tag_still_listed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_tag('v1','HEAD');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_tag('v2','HEAD');
SELECT dolt_revert('bogus');
" "SELECT tag_name FROM dolt_tags ORDER BY tag_name;"

echo "--- errors + constraint variants ---"

oracle "default_value_after_error" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER DEFAULT 42);
INSERT INTO t VALUES(1,'a',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_cherry_pick('bogus');
INSERT INTO t(id,v) VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after error, default used');
" "SELECT id, v, n FROM t ORDER BY id;"

oracle "check_fail_others_succeed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK(n >= 0));
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2, -1);
INSERT INTO t VALUES(3, 5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after check fail');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- error before first commit ---"

oracle "bad_select_before_first_commit" "
SELECT * FROM nonexistent;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','first');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
" "SELECT id, v FROM t ORDER BY id;"

oracle "checkout_branch_before_any_commit" "
SELECT dolt_checkout('nonexistent');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','first');
" "SELECT id, v FROM t;"

echo "--- error chain recovery via reset ---"

oracle "error_chain_then_reset_hard_head" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'staged');
SELECT dolt_add('-A');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_reset('--hard','HEAD');
INSERT INTO t VALUES(3,'clean');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','clean after reset');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- divergent errored branches + merge ---"

oracle "both_branches_had_errors_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','empty fail');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('bogus_branch');
SELECT dolt_revert('bogus');
INSERT INTO t VALUES(10,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- numeric edge error flows ---"

oracle "zero_division_error_doesnt_break_vc" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v = v / 0 WHERE id=1;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post div err');
" "SELECT id, v FROM t ORDER BY id;"

oracle "negative_values_preserved_through_error_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,-1000),(2,-500),(3,500);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_checkout('bogus');
SELECT dolt_reset('--hard','bogus');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- error right after branch create ---"

oracle "error_immediately_after_new_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','new');
SELECT dolt_commit('-m','empty on new');
SELECT dolt_cherry_pick('bogus');
INSERT INTO t VALUES(2,'on_new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','on new ok');
SELECT dolt_checkout('main');
SELECT dolt_merge('new');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- reset/commit loop errors ---"

oracle "reset_commit_loop_with_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_cherry_pick('bogus');
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','new c2');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_revert('bogus');
INSERT INTO t VALUES(2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','newer c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- syntax error recovery ---"

oracle "syntax_error_then_ok_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSRT INTO t VALUES(99,'typo');
INSERT INTO t VALUES(2,'correct');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after typo');
" "SELECT id, v FROM t ORDER BY id;"

oracle "missing_paren_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES 2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after syntax err');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- DELETE flow errors ---"

oracle "delete_from_nonexistent_then_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM nonexistent_t WHERE id=1;
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad delete');
" "SELECT id, v FROM t ORDER BY id;"

oracle "delete_with_bad_where_then_good" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE bogus_col=1;
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad delete where');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- error during branch+merge chain ---"

oracle "errors_scattered_through_merge_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1 ok');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_commit('-m','empty fail');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_revert('bogus');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2 ok');
SELECT dolt_checkout('main');
SELECT dolt_merge('b2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- tag error deeper ---"

oracle "tag_errors_tags_dolt_tags_stable" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('good1','HEAD');
SELECT dolt_tag('bad','bogus_ref');
SELECT dolt_tag('good1','HEAD');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_tag('good2','HEAD');
SELECT dolt_tag('-d','nonexistent');
" "SELECT tag_name FROM dolt_tags ORDER BY tag_name;"

echo "--- DELETE after DROP TABLE ---"

oracle "delete_after_drop_other_survives" "
CREATE TABLE keeper(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE goner(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO keeper VALUES(1,'k');
INSERT INTO goner VALUES(1,'g'),(2,'g2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE goner;
DELETE FROM goner WHERE id=1;
INSERT INTO keeper VALUES(2,'k2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after drop and bad delete');
" "SELECT id, v FROM keeper ORDER BY id;"

echo "--- staged DDL + failed commit ---"

oracle "staged_create_failed_commit_recovery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE new(x INTEGER);
SELECT dolt_commit('-m','forgot add');
SELECT dolt_add('-A');
INSERT INTO new VALUES(42);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','now with data');
" "SELECT x FROM new;"

echo "--- explicit col INSERT errors ---"

oracle "insert_wrong_col_name_then_correct" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t(id, bogus_col) VALUES(2, 'b');
INSERT INTO t(id, v) VALUES(3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad col');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- branch stale state probes ---"

oracle "create_branch_then_bad_then_merge_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('feat');
SELECT dolt_branch('feat');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- GROUP/ORDER error probes ---"

oracle "order_by_nonexistent_col_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT * FROM t ORDER BY nonexistent;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad order');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- row-level error probes ---"

oracle "duplicate_pk_insert_others_succeed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(1,'dup');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after dup');
" "SELECT id, v FROM t ORDER BY id;"

oracle "check_fail_insert_others_succeed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK(n >= 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,-99);
INSERT INTO t VALUES(3,20);
INSERT INTO t VALUES(4,-1);
INSERT INTO t VALUES(5,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after check errs');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- VC vtable query error probes ---"

oracle "bad_dolt_log_filter_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT * FROM dolt_log WHERE nonexistent_column='x';
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "bad_dolt_branches_query_doesnt_poison" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT bogus_fn_name() FROM dolt_branches;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

oracle "hashof_bad_ref_then_ok_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_hashof('nonexistent_ref');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

oracle "hashof_table_nonexistent_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_hashof_table('does_not_exist');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- inter-merge error probes ---"

oracle "errors_between_three_merges" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(4,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_merge('b2');
SELECT dolt_reset('--hard','bogus');
SELECT dolt_merge('b3');
" "SELECT id, v FROM t ORDER BY id;"

oracle "merge_then_error_then_merge_same_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- diff/status error probes ---"

oracle "bad_diff_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT * FROM dolt_diff_nonexistent;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- nested call error probes ---"

oracle "nested_bad_select_doesnt_break_vc" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT (SELECT bogus FROM nonexistent) FROM t;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- fan-in with errors probes ---"

oracle "errors_during_fan_in_merges" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('bogus1');
SELECT dolt_merge('b2');
SELECT dolt_merge('bogus2');
SELECT dolt_merge('b3');
" "SELECT count(*) FROM t;"

echo "--- row state after error chains ---"

oracle "row_count_invariant_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','e1');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_checkout('bogus');
SELECT dolt_reset('--hard','bogus');
SELECT dolt_merge('bogus');
SELECT * FROM nonexistent;
" "SELECT count(*) FROM t;"

oracle "sum_invariant_through_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_commit('-m','e1');
SELECT dolt_cherry_pick('bogus');
UPDATE t SET n=n WHERE bogus_col=1;
DELETE FROM t WHERE nonexistent_col=1;
" "SELECT sum(n) AS s FROM t;"

echo "--- partial batch insert error probes ---"

oracle "insert_batch_with_one_duplicate_pk" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(1,'dup');
INSERT INTO t VALUES(3,'c');
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after dup in batch');
" "SELECT id, v FROM t ORDER BY id;"

oracle "insert_batch_with_multiple_check_failures" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK(n >= 0));
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,2);
INSERT INTO t VALUES(3,-1);
INSERT INTO t VALUES(4,4);
INSERT INTO t VALUES(5,-5);
INSERT INTO t VALUES(6,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after batch');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- SAVEPOINT error flow probes ---"

oracle "rollback_to_nonexistent_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
ROLLBACK TO sp_missing;
INSERT INTO t VALUES(3);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad sp');
" "SELECT count(*) FROM t;"

oracle "release_nonexistent_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
RELEASE sp_missing;
INSERT INTO t VALUES(3);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad release');
" "SELECT count(*) FROM t;"

echo "--- REPLACE error flow probes ---"

oracle "replace_with_check_violation_others_survive" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK(n >= 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
REPLACE INTO t VALUES(1,-5);
REPLACE INTO t VALUES(2,20);
REPLACE INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after replace err');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- txn + mixed error probes ---"

oracle "txn_with_bogus_cherry_pick_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
SELECT dolt_cherry_pick('bogus');
INSERT INTO t VALUES(3);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after mixed');
" "SELECT count(*) FROM t;"

oracle "txn_bogus_merge_then_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
SELECT dolt_merge('bogus');
ROLLBACK;
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id FROM t ORDER BY id;"

echo "--- JSON error probes ---"

oracle "json_extract_bad_path_then_ok_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, j JSON);
INSERT INTO t VALUES(1,'{\"a\":1}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT json_extract(j,'not_a_path') FROM t;
INSERT INTO t VALUES(2,'{\"a\":2}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad json');
" "SELECT id FROM t ORDER BY id;"

echo "--- repeat drop probes ---"

oracle "drop_same_table_twice" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE other(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
INSERT INTO other VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE t;
DROP TABLE t;
INSERT INTO other VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after double drop');
" "SELECT id FROM other ORDER BY id;"

echo "--- long mixed error sequence probes ---"

oracle "fifteen_errors_then_one_success" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_cherry_pick('e1');
SELECT dolt_revert('e2');
SELECT dolt_reset('--hard','e3');
SELECT dolt_checkout('e4');
SELECT dolt_branch('-d','e5');
DROP TABLE e6;
ALTER TABLE e7 ADD COLUMN x INTEGER;
SELECT * FROM e8;
UPDATE e9 SET x=1;
DELETE FROM e10;
INSERT INTO t VALUES(1,'dup');
CREATE TABLE t(x);
SELECT dolt_tag('e11','bogus');
SELECT dolt_commit('-m','e12');
SELECT dolt_merge('e13');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','success');
" "SELECT count(*) FROM t;"

echo "--- bad checkout target probes ---"

oracle "checkout_hash_prefix_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('abc');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

oracle "bad_checkout_then_merge_good" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('not_a_branch');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- rename error probes ---"

oracle "rename_col_that_does_not_exist_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t RENAME COLUMN nonexistent TO val;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad rename');
" "SELECT id, v FROM t ORDER BY id;"

oracle "rename_table_that_does_not_exist" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE nonexistent RENAME TO something;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad rename table');
" "SELECT id FROM t ORDER BY id;"

oracle "rename_table_to_existing_name" "
CREATE TABLE t1(id INTEGER PRIMARY KEY);
CREATE TABLE t2(id INTEGER PRIMARY KEY);
INSERT INTO t1 VALUES(1);
INSERT INTO t2 VALUES(10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t1 RENAME TO t2;
INSERT INTO t1 VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad rename');
" "SELECT id FROM t1 ORDER BY id;"

echo "--- empty db error probes ---"

oracle "merge_on_empty_repo" "
SELECT dolt_merge('bogus');
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
" "SELECT count(*) FROM t;"

oracle "tag_on_empty_repo_then_commit" "
SELECT dolt_tag('premature','HEAD');
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('now','HEAD');
" "SELECT count(*) FROM dolt_tags;"

echo "--- sequential merge error probes ---"

oracle "bad_then_good_then_bad_then_good_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('bogus1');
SELECT dolt_merge('b1');
SELECT dolt_merge('bogus2');
SELECT dolt_merge('b2');
" "SELECT count(*) FROM t;"

echo "--- drop index/col error probes ---"

oracle "drop_column_nonexistent_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t DROP COLUMN nonexistent;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after bad drop col');
" "SELECT id, v FROM t ORDER BY id;"

oracle "drop_index_nonexistent_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP INDEX IF EXISTS nonexistent_idx;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after drop idx');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- partial cherry-pick errors ---"

oracle "cherry_pick_bad_then_good_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('bogus1');
SELECT dolt_cherry_pick('feat');
SELECT dolt_cherry_pick('bogus2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "revert_bad_then_good_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_revert('bogus1');
SELECT dolt_revert('HEAD');
SELECT dolt_revert('bogus2');
" "SELECT id, v FROM t ORDER BY id;"
echo "--- set op error probes ---"

oracle "bad_union_then_ok_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT v FROM t UNION SELECT bogus FROM nonexistent;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- error between commits ---"

oracle "error_right_after_commit_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT * FROM bogus;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT * FROM bogus;
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_log;"

echo "--- UPDATE with bad subquery probes ---"

oracle "update_subquery_bogus_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v = (SELECT bogus_col FROM t LIMIT 1);
UPDATE t SET v = 99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t;"

echo "--- arithmetic error probes ---"

oracle "divide_by_zero_update_skipped" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET n = n / (SELECT count(*) - 2 FROM t) WHERE id=1;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- reset to nonexistent tag ---"

oracle "reset_to_deleted_tag_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('snap','HEAD');
SELECT dolt_tag('-d','snap');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','snap');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT id FROM t ORDER BY id;"

echo "--- bad merge_base args ---"

oracle "merge_base_bogus_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_merge_base('main','bogus');
SELECT dolt_merge_base('bogus','main');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- INSERT into view probes ---"

oracle "insert_into_view_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE VIEW v_pos AS SELECT id, v FROM t WHERE id > 0;
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO v_pos VALUES(2,'b');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- drop view errors ---"

oracle "drop_nonexistent_view_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
DROP VIEW nonexistent_view;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- merge error storm ---"

oracle "ten_bogus_merges_then_real" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
SELECT dolt_merge('b5');
SELECT dolt_merge('b6');
SELECT dolt_merge('b7');
SELECT dolt_merge('b8');
SELECT dolt_merge('b9');
SELECT dolt_merge('b10');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- wrong arity probes ---"

oracle "hashof_no_args_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_hashof();
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- savepoint add parity ---"

oracle "savepoint_add_releases_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'dirty');
SELECT dolt_add('.');
ROLLBACK TO sp1;
" "SELECT 'ROWS', count(*) FROM t;"
echo "--- cherry-pick no-op probes ---"

oracle "cherry_pick_then_cherry_pick_noop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
SELECT dolt_cherry_pick('feat');
INSERT INTO t VALUES(3,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- alter errors ---"

oracle "alter_bad_keyword_then_ok_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t BOGUS_KEYWORD extra INTEGER;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "add_col_name_clash_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN id INTEGER;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after dup col');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- drop index probes ---"

oracle "drop_index_twice_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX idx ON t(v);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP INDEX idx;
DROP INDEX idx;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- bad window funcs ---"

oracle "bad_window_partition_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT id, ROW_NUMBER() OVER (PARTITION BY nonexistent) FROM t;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- UNION arity error probes ---"

oracle "union_arity_mismatch_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT id FROM t UNION SELECT id, v FROM t;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- CTE error probes ---"

oracle "bad_cte_self_ref_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
WITH bad AS (SELECT id FROM bad) SELECT * FROM bad;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- JOIN errors ---"

oracle "join_on_nonexistent_col_then_ok" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT a.id FROM a JOIN b ON a.bogus=b.id;
INSERT INTO a VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM a ORDER BY id;"

echo "--- dolt_add error probes ---"

oracle "many_bad_adds_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('nonexistent1');
SELECT dolt_add('nonexistent2');
SELECT dolt_add('nonexistent3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','survived');
" "SELECT count(*) FROM dolt_log;"

echo "--- log-aware workflow errors ---"

oracle "log_query_after_bad_ref_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT commit_hash FROM dolt_log WHERE commit_hash = 'not_a_real_hash';
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_log;"
echo "--- GROUP_CONCAT error probes ---"

oracle "group_concat_on_nonexistent_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT GROUP_CONCAT(bogus) FROM t;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- self-join error probes ---"

oracle "self_join_bogus_alias_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO t VALUES(1,NULL),(2,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT bogus.id FROM t a JOIN t b ON a.pid=b.id;
INSERT INTO t VALUES(3,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- view chain error probes ---"

oracle "drop_base_view_then_query_dependent_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
CREATE VIEW base AS SELECT id, v FROM t;
CREATE VIEW derived AS SELECT id FROM base WHERE v>0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP VIEW base;
SELECT * FROM derived;
CREATE VIEW base AS SELECT id, v FROM t;
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','restored');
" "SELECT id FROM t ORDER BY id;"

echo "--- scalar subquery multiple rows probes ---"

oracle "scalar_subquery_returning_multiple_rows_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT (SELECT id FROM t) FROM t;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- merge errors with many branches ---"

oracle "errors_across_5_branch_merge_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','a');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','a');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','c');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c');
SELECT dolt_checkout('main');
SELECT dolt_merge('a');
SELECT dolt_merge('bogus1');
SELECT dolt_merge('b');
SELECT dolt_merge('bogus2');
SELECT dolt_merge('c');
SELECT dolt_merge('bogus3');
" "SELECT count(*) FROM t;"

echo "--- txn error probes ---"

oracle "rollback_outside_txn_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
ROLLBACK;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

oracle "commit_outside_txn_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
COMMIT;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- nested JOIN errors ---"

oracle "nested_join_bogus_table_then_ok" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT a.id FROM a JOIN b ON a.id=b.id JOIN bogus ON a.id=bogus.id;
INSERT INTO a VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM a;"

echo "--- create index error probes ---"

oracle "create_index_on_nonexistent_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE INDEX idx_bogus ON t(nonexistent_col);
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "create_index_on_nonexistent_table_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE INDEX idx ON nonexistent_table(col);
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"
echo "--- DATE error probes ---"

oracle "bad_date_value_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, d DATE);
INSERT INTO t VALUES(1,'2024-01-15');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'not-a-date');
INSERT INTO t VALUES(3,'2024-06-01');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, d FROM t WHERE id IN (1,3) ORDER BY id;"

echo "--- UPDATE subquery error chain ---"

oracle "update_bogus_subquery_table_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v = (SELECT n FROM nonexistent WHERE id=t.id);
UPDATE t SET v = 99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- DELETE error probes ---"

oracle "delete_with_bogus_subquery_table_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE id IN (SELECT id FROM bogus);
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- schema change error chain ---"

oracle "alter_add_drop_alter_chain_with_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN x INTEGER DEFAULT 0;
ALTER TABLE t ADD COLUMN x INTEGER;
ALTER TABLE t DROP COLUMN nonexistent;
ALTER TABLE t DROP COLUMN x;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- tag error probes ---"

oracle "tag_empty_name_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('','HEAD');
SELECT dolt_tag('good','HEAD');
" "SELECT count(*) FROM dolt_tags WHERE tag_name='good';"

echo "--- repeated merge errors ---"

oracle "merge_twice_bogus_twice_real" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('bogus');
SELECT dolt_merge('bogus');
SELECT dolt_merge('feat');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- drop chain probes ---"

oracle "drop_table_chain_with_errors" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(1);
INSERT INTO c VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE a;
DROP TABLE a;
DROP TABLE bogus;
DROP TABLE b;
DROP TABLE b;
INSERT INTO c VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after chain');
" "SELECT id FROM c ORDER BY id;"

echo "--- hashof error probes ---"

oracle "hashof_too_many_args_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_hashof('a','b','c');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- txn long error streak ---"

oracle "txn_many_errors_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
SELECT * FROM bogus1;
SELECT * FROM bogus2;
INSERT INTO t VALUES(3);
DELETE FROM bogus3;
INSERT INTO t VALUES(4);
UPDATE bogus4 SET x=1;
INSERT INTO t VALUES(5);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT count(*) FROM t;"

echo "--- view create errors ---"

oracle "create_view_dup_name_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE VIEW v AS SELECT id FROM t;
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
CREATE VIEW v AS SELECT id FROM t;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM v;"

oracle "create_view_bad_source_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
CREATE VIEW v_bad AS SELECT * FROM nonexistent;
CREATE VIEW v_good AS SELECT id FROM t;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM v_good;"
echo "--- UPDATE invalid SET probes ---"

oracle "update_set_nonexistent_col_assigned_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET bogus=99 WHERE id=1;
UPDATE t SET v=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t;"

echo "--- INSERT arity probes ---"

oracle "insert_too_few_values_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
INSERT INTO t VALUES(1,10,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "insert_too_many_values_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,20,30,40);
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- WHERE bogus func ---"

oracle "where_bogus_func_then_ok_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT id FROM t WHERE bogus_function(id)>0;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- CREATE TABLE bad cols ---"

oracle "create_table_bad_syntax_then_good" "
CREATE TABLE keep(id INTEGER PRIMARY KEY);
INSERT INTO keep VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE bad(,,);
CREATE TABLE good(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO good VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM good;"

echo "--- nested txn probes ---"

oracle "begin_begin_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
BEGIN;
BEGIN;
INSERT INTO t VALUES(2);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- CTE error probes ---"

oracle "cte_with_bogus_inner_ref_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
WITH x AS (SELECT bogus FROM t) SELECT * FROM x;
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_add specific errors ---"

oracle "add_mix_of_good_bad_tables_then_commit_a" "
CREATE TABLE t1(id INTEGER PRIMARY KEY);
CREATE TABLE t2(id INTEGER PRIMARY KEY);
INSERT INTO t1 VALUES(1);
INSERT INTO t2 VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t1 VALUES(2);
INSERT INTO t2 VALUES(2);
SELECT dolt_add('t1','nonexistent','t2');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- checkout existing probes ---"

oracle "checkout_existing_twice_with_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('feat');
SELECT dolt_checkout('bogus');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- mixed DDL error probes ---"

oracle "ddl_mix_with_errors_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE bad(;
ALTER TABLE bogus ADD COLUMN x INTEGER;
CREATE INDEX bad_idx ON bogus(x);
DROP TABLE bogus;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"
echo "--- savepoint edge error probes ---"

oracle "rollback_to_released_savepoint_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2);
RELEASE SAVEPOINT sp1;
ROLLBACK TO sp1;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT count(*) FROM t;"

oracle "savepoint_after_nested_dolt_fn_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2);
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
ROLLBACK TO sp1;
COMMIT;
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT count(*) FROM t;"

echo "--- BETWEEN error probes ---"

oracle "between_with_nonexistent_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT id FROM t WHERE bogus BETWEEN 1 AND 5;
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- mixed severity error probes ---"

oracle "syntax_error_semantic_error_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELCT bogus FROM t;
SELECT * FROM nonexistent;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- empty message probes ---"

oracle "commit_empty_message_then_good" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','');
SELECT dolt_commit('-m','good');
" "SELECT count(*) FROM dolt_log WHERE message IN ('good','');"

echo "--- multi drop mixed ---"

oracle "drop_three_tables_mix_bogus" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(1);
INSERT INTO c VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE a;
DROP TABLE bogus;
DROP TABLE b;
INSERT INTO c VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM c ORDER BY id;"

echo "--- post-merge errors ---"

oracle "errors_after_merge_then_ok_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_cherry_pick('bogus');
SELECT dolt_revert('bogus');
SELECT dolt_reset('--hard','bogus');
SELECT * FROM bogus;
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id FROM t ORDER BY id;"

echo "--- merge_base errors ---"

oracle "merge_base_both_bogus_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_merge_base('bogus1','bogus2');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- INSERT wrong type ---"

oracle "insert_wrong_type_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,10,20,30);
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- tag error chain ---"

oracle "tag_duplicate_delete_same_tag_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('v1','HEAD');
SELECT dolt_tag('v1','HEAD');
SELECT dolt_tag('-d','v1');
SELECT dolt_tag('-d','v1');
SELECT dolt_tag('v2','HEAD');
" "SELECT tag_name FROM dolt_tags ORDER BY tag_name;"
echo "--- revert error probes ---"

oracle "revert_on_empty_repo_then_ok" "
SELECT dolt_revert('HEAD');
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
" "SELECT count(*) FROM t;"

oracle "revert_tilde_past_end_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_revert('HEAD~5');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- merge rollback error probes ---"

oracle "merge_conflict_then_extra_bogus_then_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_cherry_pick('bogus1');
SELECT dolt_revert('bogus2');
SELECT dolt_reset('--hard','HEAD');
INSERT INTO t VALUES(2,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- view INSERT errors ---"

oracle "insert_into_view_agg_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE VIEW total AS SELECT sum(v) AS s FROM t;
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO total VALUES(100);
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT sum(v) FROM t;"

echo "--- DELETE all error probes ---"

oracle "delete_all_then_bogus_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
DELETE FROM t;
SELECT * FROM bogus;
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- FK violation recovery ---"

echo "--- INSERT expr error probes ---"

oracle "insert_with_bogus_func_expression_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2, bogus_func(5));
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- branch checkout error ---"

oracle "checkout_then_branch_delete_self_error" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
SELECT dolt_branch('-d','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- hashof error chain ---"

oracle "hashof_chain_of_bad_refs_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_hashof('bogus1');
SELECT dolt_hashof('bogus2');
SELECT dolt_hashof('bogus3');
SELECT dolt_hashof('HEAD');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- checkout conflict recovery ---"

oracle "bad_checkout_uncommitted_work_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_checkout('nonexistent');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','kept');
" "SELECT id FROM t ORDER BY id;"
echo "--- recursive CTE error probes ---"

oracle "recursive_cte_self_ref_bogus_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT bogus FROM x) SELECT * FROM x LIMIT 5;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- ORDER BY error probes ---"

oracle "order_by_bogus_expression_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT id FROM t ORDER BY bogus_fn(v);
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- window bad OVER ---"

oracle "window_bad_over_clause_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT id, ROW_NUMBER() OVER (PARTITION bogus ORDER BY v) FROM t;
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- nested txn error probes ---"

oracle "savepoint_error_storm_then_clean_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2);
SELECT * FROM bogus;
SELECT dolt_cherry_pick('bogus');
SELECT * FROM bogus;
COMMIT;
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT count(*) FROM t;"

echo "--- INSERT bogus cols ---"

oracle "insert_targeting_bogus_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t(id, bogus) VALUES(2,'x');
INSERT INTO t(id, v) VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- merge bogus hash probes ---"

oracle "merge_bogus_hash_like_ref_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('abc1234567890');
SELECT dolt_merge('0000000000000000000000000000000000000000');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- gc edge probes ---"

oracle "reset_hard_then_commit_no_gc" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id FROM t ORDER BY id;"

echo "--- multi-stmt error boundary ---"

oracle "unterminated_statement_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT bogus FROM
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- tag deep error sequence ---"

oracle "tag_error_storm_then_good" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('v1','HEAD');
SELECT dolt_tag('v1','HEAD');
SELECT dolt_tag('','HEAD');
SELECT dolt_tag('v2','bogus_ref');
SELECT dolt_tag('-d','nonexistent');
SELECT dolt_tag('-d','v1');
SELECT dolt_tag('v3','HEAD');
" "SELECT tag_name FROM dolt_tags ORDER BY tag_name;"

echo "--- UPDATE+DELETE bad chain ---"

oracle "update_delete_bogus_interleaved_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v=99 WHERE bogus=1;
DELETE FROM t WHERE bogus=2;
UPDATE t SET v=99 WHERE id=1;
DELETE FROM t WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"
echo "--- merge_base deeper errors ---"

oracle "merge_base_same_branch_twice_after_ff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_merge_base('main','main');
SELECT dolt_merge_base('bogus','main');
SELECT dolt_merge_base('main','bogus');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- insert expr error source ---"

oracle "insert_scalar_bogus_subquery_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2, (SELECT bogus_col FROM t));
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_diff errors ---"

oracle "dolt_diff_filter_bogus_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT * FROM dolt_diff WHERE bogus='x';
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id FROM t ORDER BY id;"

echo "--- HAVING errors ---"

oracle "having_bogus_col_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT grp, sum(n) FROM t GROUP BY grp HAVING bogus>0;
INSERT INTO t VALUES(4,'a',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- view after dropped source ---"

oracle "view_references_dropped_source_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE VIEW vt AS SELECT id, v FROM t;
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
DROP TABLE t;
SELECT id FROM vt;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- conflict state rollback error ---"

oracle "conflict_rolled_back_then_error_storm_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT * FROM bogus1;
SELECT dolt_cherry_pick('bogus2');
SELECT dolt_reset('--hard','bogus3');
INSERT INTO t VALUES(2,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- tag on deleted branch ---"

oracle "tag_after_branch_deleted_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_branch('-d','feat');
SELECT dolt_tag('v1','feat');
SELECT dolt_tag('v1','HEAD');
" "SELECT count(*) FROM dolt_tags WHERE tag_name='v1';"

echo "--- UPDATE cte bogus ---"

oracle "update_via_bogus_cte_subquery_then_ok" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v = (WITH x AS (SELECT bogus FROM nonexistent) SELECT * FROM x);
UPDATE t SET v=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, v FROM t;"

echo "--- DELETE from view ---"

oracle "delete_from_view_then_delete_from_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE VIEW v AS SELECT id, v FROM t;
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
DELETE FROM v WHERE id=1;
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM t;"

echo "--- ALTER rename non-existing ---"

oracle "alter_rename_bogus_col_then_real" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t RENAME COLUMN bogus TO something;
ALTER TABLE t RENAME COLUMN v TO val;
INSERT INTO t(id,val) VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT id, val FROM t ORDER BY id;"
echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
