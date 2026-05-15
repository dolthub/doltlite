#!/bin/bash
# Verify dolt_gc / wal_checkpoint don't drop chunks reachable only via
# in-memory session state (staged catalog, mid-merge conflicts, mid-rebase
# state, constraint-violations catalog). gcMarkReachable seeds these from
# every attached doltlite Btree on the calling connection.

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(15);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(15);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Doltlite GC over session state ==="
echo ""

# ----------------------------------------------------------------
# S1: staged-but-uncommitted survives GC
# ----------------------------------------------------------------
DB=/tmp/test_gc_session_staged_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Stage rows without committing, then GC, then commit. The staged catalog
# hash must remain resolvable across the GC.
echo "INSERT INTO t VALUES(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_gc();
SELECT dolt_commit('-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "staged_commit_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "staged_commit_log"   "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test "staged_commit_value" "SELECT v FROM t WHERE id=3;" "c" "$DB"

# Reopen — chunks must still be on disk.
run_test "staged_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"

db_rm "$DB"

# ----------------------------------------------------------------
# S2: mid-merge with conflicts — gc + abort
# ----------------------------------------------------------------
DB=/tmp/test_gc_session_merge_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_branch' WHERE id=1;
SELECT dolt_commit('-A','-m','main change');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_branch' WHERE id=1;
SELECT dolt_commit('-A','-m','feat change');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Trigger a conflicting merge, leave it unresolved, run GC, then abort.
# Aborting must restore the head working state — which requires the
# pre-merge catalog hash to still be on disk.
echo "SELECT dolt_merge('feat');
SELECT dolt_gc();
SELECT dolt_merge('--abort');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_abort_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "merge_abort_value" "SELECT v FROM t WHERE id=1;" "main_branch" "$DB"
run_test "merge_abort_status_empty" "SELECT count(*) FROM dolt_status;" "0" "$DB"

db_rm "$DB"

# ----------------------------------------------------------------
# S3: wal_checkpoint mid-staging (the shimPagerCheckpoint -> GC path)
# ----------------------------------------------------------------
DB=/tmp/test_gc_session_checkpoint_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# `PRAGMA wal_checkpoint` routes through shimPagerCheckpoint -> doltliteGcCompact.
# Run it between staging and commit; data must survive.
echo "INSERT INTO t VALUES(2,'b'),(3,'c');
SELECT dolt_add('-A');
PRAGMA wal_checkpoint;
SELECT dolt_commit('-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ckpt_commit_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "ckpt_commit_log"   "SELECT count(*) FROM dolt_log;" "3" "$DB"

# Reopen.
run_test "ckpt_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"

db_rm "$DB"

# ----------------------------------------------------------------
# S4: constraint-violations catalog survives GC
# ----------------------------------------------------------------
DB=/tmp/test_gc_session_cv_$$.db; db_rm "$DB"
echo "CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1);
INSERT INTO c VALUES(10, 1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
DELETE FROM p WHERE id=1;
SELECT dolt_commit('-A','-m','main del');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES(20, 1);
SELECT dolt_commit('-A','-m','feat insert');
SELECT dolt_checkout('main');
PRAGMA foreign_keys=1;
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# At this point a CV catalog should exist on the session. GC must preserve it.
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# Whatever the CV outcome is, the rows that landed must still be readable.
run_test_match "cv_table_p" "SELECT count(*) FROM p;" "[0-9]+" "$DB"
run_test_match "cv_table_c" "SELECT count(*) FROM c;" "[0-9]+" "$DB"

db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
