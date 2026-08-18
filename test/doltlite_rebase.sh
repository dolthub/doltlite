#!/bin/bash
DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r
  r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r
  r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

echo "=== Doltlite Rebase Tests ==="
echo ""

DB=/tmp/test_rebase_$$.db; rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
INSERT INTO t VALUES (3, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');
INSERT INTO t VALUES (4, 4);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SQL

# Invalid-plan --continue must leave the rebase in progress so the plan
# can be edited and retried. Start, edit, and continue run in one session.
run_test_match "invalid_plan_continue_errors" \
  "SELECT dolt_rebase('-i', 'main');
   UPDATE dolt_rebase SET action = 'oops' WHERE commit_message = 'f1';
   SELECT dolt_rebase('--continue');" \
  "rebase failed|no rebase in progress" \
  "$DB"
run_test "invalid_plan_branch_preserved" \
  "SELECT active_branch();" \
  "main" \
  "$DB"
run_test "invalid_plan_table_preserved" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase';" \
  "0" \
  "$DB"
run_test "invalid_plan_abort_works" \
  "SELECT dolt_rebase('--abort');" \
  "Error near line 1: no rebase in progress" \
  "$DB"
run_test "invalid_plan_abort_restores_branch" \
  "SELECT active_branch();" \
  "main" \
  "$DB"

DB2=/tmp/test_rebase2_$$.db; rm -f "$DB2"
cat <<'SQL' | "$DOLTLITE" "$DB2" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
CREATE TABLE dolt_rebase(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SQL

run_test_match "start_failure_errors" \
  "SELECT dolt_rebase('-i', 'main');" \
  "didn't identify any commits!" \
  "$DB2"
run_test "start_failure_restores_branch" \
  "SELECT active_branch();" \
  "main" \
  "$DB2"
run_test "start_failure_temp_branch_removed" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" \
  "$DB2"

# An interactive rebase mirrors its working set onto the branch a reopen would
# land on, and clears that branch when it finishes. Rebase only refuses to
# start over a dirty *current* branch, so uncommitted work on the other branch
# used to be destroyed. The two controls establish that neither a plain
# checkout roundtrip nor a non-interactive rebase ever did this.
seed_dirty_main() {
  rm -f "$1"
  cat <<'SQL' | "$DOLTLITE" "$1" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'f1');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'m1');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(99,'row99');
SQL
}

DB3=/tmp/test_rebase_dirty_$$.db
seed_dirty_main "$DB3"
run_test "interactive_rebase_keeps_other_branch_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   SELECT dolt_rebase('--continue');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "0
interactive rebase started on branch dolt_rebase_feat; adjust the rebase plan in the dolt_rebase table, then continue rebasing by calling dolt_rebase('--continue')
Successfully rebased and updated refs/heads/feat
0
1" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "interactive_rebase_abort_keeps_other_branch_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   SELECT dolt_rebase('--abort');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "^1$" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "checkout_roundtrip_keeps_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "^1$" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "noninteractive_rebase_keeps_other_branch_uncommitted_work" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('main');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';" \
  "^1$" \
  "$DB3"

# Reopen used to lose the rebase when main was dirty: no mirror meant
# --abort said "no rebase in progress" and dolt_rebase_feat was left behind.
seed_dirty_main "$DB3"
run_test_match "interactive_rebase_dirty_main_starts" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB3"
run_test "interactive_rebase_abort_after_reopen_dirty_main" \
  "SELECT dolt_rebase('--abort');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "Interactive rebase aborted
0
1
0" \
  "$DB3"

seed_dirty_main "$DB3"
run_test_match "interactive_rebase_dirty_main_starts_for_continue" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB3"
run_test "interactive_rebase_continue_after_reopen_dirty_main" \
  "SELECT dolt_rebase('--continue');
   SELECT dolt_checkout('main');
   SELECT count(*) FROM t WHERE v='row99';
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "Successfully rebased and updated refs/heads/feat
0
1
0" \
  "$DB3"

# Rebase onto a non-default upstream used to stamp that tip as the default
# branch's workingCommit. Open of $DB then discarded the mirror, so continue
# and abort reported no rebase in progress and left dolt_rebase_<orig> behind.
seed_onto_other() {
  rm -f "$1"
  cat <<'SQL' | "$DOLTLITE" "$1" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','other');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','other');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'feat');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
SQL
}

DB_OTHER_CONT=/tmp/test_rebase_other_continue_$$.db
seed_onto_other "$DB_OTHER_CONT"
run_test_match "interactive_rebase_onto_other_starts" \
  "SELECT dolt_rebase('-i','other');" \
  "interactive rebase started" \
  "$DB_OTHER_CONT/feat"
run_test "interactive_rebase_continue_after_reopen_onto_other" \
  "SELECT dolt_rebase('--continue');
   SELECT active_branch();
   SELECT group_concat(v, ',') FROM t;
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';
   SELECT dolt_checkout('main');
   SELECT group_concat(v, ',') FROM t;" \
  "Successfully rebased and updated refs/heads/feat
feat
base,other,feat
0
0
base" \
  "$DB_OTHER_CONT"

DB_OTHER_ABORT=/tmp/test_rebase_other_abort_$$.db
seed_onto_other "$DB_OTHER_ABORT"
run_test_match "interactive_rebase_onto_other_starts_for_abort" \
  "SELECT dolt_rebase('-i','other');" \
  "interactive rebase started" \
  "$DB_OTHER_ABORT/feat"
run_test "interactive_rebase_abort_after_reopen_onto_other" \
  "SELECT dolt_rebase('--abort');
   SELECT active_branch();
   SELECT group_concat(v, ',') FROM t;
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';
   SELECT dolt_checkout('main');
   SELECT group_concat(v, ',') FROM t;" \
  "Interactive rebase aborted
feat
base,feat
0
0
base" \
  "$DB_OTHER_ABORT"

# An unrecognised plan verb used to report a bare "rebase failed", which reads
# as though the rebase had been rolled back. It is not: the plan and working
# branch are deliberately kept so the action can be corrected and --continue
# retried, so the error has to name what was wrong.
DB4=/tmp/test_rebase_verb_$$.db
seed_verb_repo() {
  rm -f "$1"
  cat <<'SQL' | "$DOLTLITE" "$1" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SQL
}

seed_verb_repo "$DB4"
run_test_match "unknown_plan_action_names_the_action" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   UPDATE dolt_rebase SET action='oops';
   SELECT dolt_rebase('--continue');" \
  'unknown rebase action "oops": expected pick, reword, squash, fixup or drop' \
  "$DB4"

seed_verb_repo "$DB4"
run_test_match "unknown_plan_action_stays_resumable" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   UPDATE dolt_rebase SET action='oops';
   SELECT dolt_rebase('--continue');
   UPDATE dolt_rebase SET action='pick';
   SELECT dolt_rebase('--continue');" \
  "Successfully rebased and updated refs/heads/feat" \
  "$DB4"

BRANCH63=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
BRANCH64=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb

seed_rebase_name_repo() {
  rm -f "$1"
  "$DOLTLITE" "$1" >/dev/null 2>&1 <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','$2');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feature');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
SQL
}

DB5=/tmp/test_rebase_name_continue_$$.db
seed_rebase_name_repo "$DB5" "$BRANCH63"
run_test_match "rebase_63_byte_branch_starts" \
  "SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB5/$BRANCH63"
run_test "rebase_63_byte_branch_continues_after_reopen" \
  "SELECT dolt_rebase('--continue');
   SELECT active_branch();
   SELECT group_concat(id, ',') FROM t;" \
  "Successfully rebased and updated refs/heads/$BRANCH63
$BRANCH63
1,2,3" \
  "$DB5/dolt_rebase_$BRANCH63"

DB5_SHORT=/tmp/test_rebase_short_continue_$$.db
seed_rebase_name_repo "$DB5_SHORT" "feat"
run_test_match "rebase_short_branch_starts" \
  "SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB5_SHORT/feat"
run_test "rebase_short_branch_continues_after_reopen" \
  "SELECT dolt_rebase('--continue');
   SELECT active_branch();
   SELECT group_concat(id, ',') FROM t;
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "Successfully rebased and updated refs/heads/feat
feat
1,2,3
0" \
  "$DB5_SHORT/dolt_rebase_feat"

# Reopen of the default DB used to see the mirrored plan then CAS-fail
# (issue 1961). Reopen of the original branch had no plan at all.
DB5_DEFAULT=/tmp/test_rebase_default_continue_$$.db
seed_rebase_name_repo "$DB5_DEFAULT" "feat"
run_test_match "rebase_default_reopen_starts" \
  "SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB5_DEFAULT/feat"
run_test "rebase_default_reopen_continues" \
  "SELECT dolt_rebase('--continue');
   SELECT active_branch();
   SELECT group_concat(id, ',') FROM t;
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "Successfully rebased and updated refs/heads/feat
feat
1,2,3
0" \
  "$DB5_DEFAULT"

DB5_ORIG=/tmp/test_rebase_orig_continue_$$.db
seed_rebase_name_repo "$DB5_ORIG" "feat"
run_test_match "rebase_orig_reopen_starts" \
  "SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB5_ORIG/feat"
run_test "rebase_orig_reopen_continues" \
  "SELECT dolt_rebase('--continue');
   SELECT active_branch();
   SELECT group_concat(id, ',') FROM t;
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "Successfully rebased and updated refs/heads/feat
feat
1,2,3
0" \
  "$DB5_ORIG/feat"

DB5_DEFAULT_ABORT=/tmp/test_rebase_default_abort_$$.db
seed_rebase_name_repo "$DB5_DEFAULT_ABORT" "feat"
run_test_match "rebase_default_reopen_starts_for_abort" \
  "SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB5_DEFAULT_ABORT/feat"
run_test "rebase_default_reopen_aborts" \
  "SELECT dolt_rebase('--abort');
   SELECT active_branch();
   SELECT group_concat(id, ',') FROM t;
   SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "Interactive rebase aborted
feat
1,2
0" \
  "$DB5_DEFAULT_ABORT"

DB6=/tmp/test_rebase_name_abort_$$.db
seed_rebase_name_repo "$DB6" "$BRANCH63"
run_test_match "rebase_63_byte_branch_starts_for_abort" \
  "SELECT dolt_rebase('-i','main');" \
  "interactive rebase started" \
  "$DB6/$BRANCH63"
run_test "rebase_63_byte_branch_aborts_after_reopen" \
  "SELECT dolt_rebase('--abort');
   SELECT active_branch();
   SELECT group_concat(id, ',') FROM t;" \
  "Interactive rebase aborted
$BRANCH63
1,2" \
  "$DB6"

DB7=/tmp/test_rebase_name_reject_$$.db
seed_rebase_name_repo "$DB7" "$BRANCH64"
run_test_match "rebase_64_byte_branch_rejected" \
  "SELECT dolt_rebase('-i','main');" \
  "current branch name exceeds the 63-byte persisted-state limit" \
  "$DB7/$BRANCH64"
run_test "rebase_64_byte_branch_rejection_is_atomic" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_$BRANCH64';
   SELECT count(*) FROM sqlite_master WHERE name='dolt_rebase';" \
  "0
0" \
  "$DB7"

DB8=/tmp/test_rebase_return_name_$$.db
seed_rebase_return_repo() {
  rm -f "$1"
  "$DOLTLITE" "$1" >/dev/null 2>&1 <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feature');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','$2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','upstream');
SELECT dolt_default_branch('$2');
SQL
}

seed_rebase_return_repo "$DB8" "$BRANCH63"
run_test_match "rebase_63_byte_default_branch_starts" \
  "SELECT dolt_rebase('-i','$BRANCH63');" \
  "interactive rebase started" \
  "$DB8/feat"
run_test "rebase_63_byte_default_branch_aborts_after_reopen" \
  "SELECT dolt_rebase('--abort'); SELECT active_branch();" \
  "Interactive rebase aborted
feat" \
  "$DB8"

seed_rebase_return_repo "$DB8" "$BRANCH64"
run_test_match "rebase_64_byte_default_branch_rejected" \
  "SELECT dolt_rebase('-i','$BRANCH64');" \
  "default branch name exceeds the 63-byte persisted-state limit" \
  "$DB8/feat"
run_test "rebase_64_byte_default_branch_rejection_is_atomic" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';
   SELECT count(*) FROM sqlite_master WHERE name='dolt_rebase';" \
  "0
0" \
  "$DB8"

# Constraint detection must run for --continue regardless of the enclosing
# transaction shape: a replayed commit that orphans an FK row has to abort
# the rebase in every mode, never advance the branch with the violation.
seed_fk_conflict_repo() {
  local d="$1"
  rm -f "$d"
  cat <<'SQL' | "$DOLTLITE" "$d" >/dev/null 2>&1
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
DELETE FROM parent WHERE id=1;
SELECT dolt_commit('-am','main deletes parent');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-am','feat adds child');
SQL
}

DB9=/tmp/test_rebase_fk_txn_$$.db
seed_fk_conflict_repo "$DB9"
run_test_match "continue_in_txn_detects_fk_violation" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   UPDATE dolt_rebase SET action='pick';
   BEGIN;
   SELECT dolt_rebase('--continue');
   COMMIT;" \
  "data conflicts from rebase" \
  "$DB9"
run_test "continue_in_txn_no_orphans" \
  "SELECT count(*) FROM pragma_foreign_key_check;
   SELECT message FROM dolt_log('feat') LIMIT 1;" \
  "0
feat adds child" \
  "$DB9"

seed_fk_conflict_repo "$DB9"
run_test_match "continue_in_savepoint_detects_fk_violation" \
  "SELECT dolt_checkout('feat');
   SELECT dolt_rebase('-i','main');
   UPDATE dolt_rebase SET action='pick';
   SAVEPOINT s1;
   SELECT dolt_rebase('--continue');" \
  "data conflicts from rebase" \
  "$DB9"
run_test "continue_in_savepoint_no_orphans" \
  "SELECT count(*) FROM pragma_foreign_key_check;
   SELECT message FROM dolt_log('feat') LIMIT 1;" \
  "0
feat adds child" \
  "$DB9"

# Linear rebase used the replayed commit message to decide revert vs
# cherry-pick. A DROP INDEX titled "Revert leftover" skipped index patches
# and left the index in place.
DB10=/tmp/test_rebase_revert_msg_$$.db; rm -f "$DB10"
cat <<'SQL' | "$DOLTLITE" "$DB10" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX t_v ON t(v);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feat');
DROP INDEX t_v;
SELECT dolt_commit('-A','-m','Revert leftover');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','main row');
SELECT dolt_checkout('feat');
SQL
TX_OUT=$(echo "SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
SELECT 'TX|' || (SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v') || '|' || (SELECT count(*) FROM t);" | "$DOLTLITE" "$DB10" 2>&1)
if echo "$TX_OUT" | grep -q "Successfully rebased"; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_revert_msg_ok\n  expected Successfully rebased\n  got: $TX_OUT"
fi
TX_LINE=$(echo "$TX_OUT" | grep '^TX|')
if [ "$TX_LINE" = "TX|0|2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_revert_msg_result\n  expected: TX|0|2\n  got:      $TX_LINE"
fi
rm -f "$DB10"

# A successful linear rebase is a transaction boundary like dolt_commit:
# it seals the enclosing BEGIN when it advances the ref.
DB11=/tmp/test_rebase_txn_seal_$$.db; rm -f "$DB11"
cat <<'SQL' | "$DOLTLITE" "$DB11" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SQL
TX_OUT=$(echo "SELECT dolt_checkout('feat');
BEGIN;
SELECT dolt_rebase('main');
ROLLBACK;
SELECT 'TX|' || (SELECT count(*) FROM t) || '|' || (SELECT count(*) FROM dolt_status) || '|' || (SELECT active_branch());" | "$DOLTLITE" "$DB11" 2>&1)
TX_LINE=$(echo "$TX_OUT" | grep '^TX|')
if [ "$TX_LINE" = "TX|3|0|feat" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_in_txn_seals\n  expected: TX|3|0|feat\n  got:      $TX_LINE"
fi
if echo "$TX_OUT" | grep -q "cannot rollback - no transaction is active"; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_in_txn_rollback_refused\n  expected the post-rebase ROLLBACK to find no open transaction"
fi
rm -f "$DB11"

# A failed linear rebase restores feat, but AdoptRollbackBaseline used to
# run only in autocommit. ROLLBACK of the enclosing BEGIN then reinstalled
# the working-branch (upstream) catalog on feat.
DB12=/tmp/test_rebase_fail_txn_$$.db; rm -f "$DB12"
cat <<'SQL' | "$DOLTLITE" "$DB12" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2 WHERE id=1;
SELECT dolt_commit('-am','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_commit('-am','main changes');
SELECT dolt_checkout('feat');
SQL
TX_OUT=$(echo "SELECT dolt_checkout('feat');
BEGIN;
SELECT dolt_rebase('main');
SELECT 'AFTER_FAIL|' || (SELECT active_branch()) || '|' || (SELECT v FROM t WHERE id=1);
ROLLBACK;
SELECT 'AFTER_RB|' || (SELECT active_branch()) || '|' || (SELECT v FROM t WHERE id=1) || '|' || (SELECT count(*) FROM dolt_status);" | "$DOLTLITE" "$DB12" 2>&1)
if echo "$TX_OUT" | grep -q 'conflict rebasing'; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_conflict_in_txn\n  expected conflict rebasing\n  got: $TX_OUT"
fi
AFTER_FAIL=$(echo "$TX_OUT" | grep '^AFTER_FAIL|')
AFTER_RB=$(echo "$TX_OUT" | grep '^AFTER_RB|')
if [ "$AFTER_FAIL" = "AFTER_FAIL|feat|2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_conflict_in_txn_restores\n  expected: AFTER_FAIL|feat|2\n  got:      $AFTER_FAIL"
fi
if [ "$AFTER_RB" = "AFTER_RB|feat|2|0" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: linear_rebase_conflict_in_txn_rollback\n  expected: AFTER_RB|feat|2|0\n  got:      $AFTER_RB"
fi
rm -f "$DB12"

# Same split after a conflicted interactive --continue aborts the rebase.
DB13=/tmp/test_rebase_iconflict_txn_$$.db; rm -f "$DB13"
cat <<'SQL' | "$DOLTLITE" "$DB13" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2 WHERE id=1;
SELECT dolt_commit('-am','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_commit('-am','main changes');
SELECT dolt_checkout('feat');
SQL
TX_OUT=$(echo "SELECT dolt_checkout('feat');
BEGIN;
SELECT dolt_rebase('-i','main');
UPDATE dolt_rebase SET action='pick';
SELECT dolt_rebase('--continue');
SELECT 'AFTER_CONT|' || (SELECT active_branch()) || '|' || (SELECT v FROM t WHERE id=1);
ROLLBACK;
SELECT 'AFTER_RB|' || (SELECT active_branch()) || '|' || (SELECT v FROM t WHERE id=1) || '|' || (SELECT count(*) FROM dolt_status);" | "$DOLTLITE" "$DB13" 2>&1)
if echo "$TX_OUT" | grep -q 'data conflicts from rebase'; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: interactive_rebase_conflict_in_txn\n  expected data conflicts from rebase\n  got: $TX_OUT"
fi
AFTER_CONT=$(echo "$TX_OUT" | grep '^AFTER_CONT|')
AFTER_RB=$(echo "$TX_OUT" | grep '^AFTER_RB|')
if [ "$AFTER_CONT" = "AFTER_CONT|feat|2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: interactive_rebase_conflict_in_txn_restores\n  expected: AFTER_CONT|feat|2\n  got:      $AFTER_CONT"
fi
if [ "$AFTER_RB" = "AFTER_RB|feat|2|0" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: interactive_rebase_conflict_in_txn_rollback\n  expected: AFTER_RB|feat|2|0\n  got:      $AFTER_RB"
fi
rm -f "$DB13"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB5_SHORT" "$DB6" "$DB7" "$DB8" "$DB9"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
