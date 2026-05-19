#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== dolt_revert dirty-set behavior ==="
echo ""

# Case 1: dirty unstaged change to an UNRELATED table -> revert SUCCEEDS,
# dirty change is included in the revert commit, leaving a clean worktree.

DB=/tmp/test_rv_dirty_unrelated_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');
INSERT INTO meta VALUES(1,'side');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_dirty_unrelated_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_dirty_unrelated_t_reverted" "SELECT count(*) FROM t;" "0" "$DB"
run_test "rv_dirty_unrelated_meta_kept" "SELECT note FROM meta WHERE id=1;" "side" "$DB"
run_test "rv_dirty_unrelated_status_clean" \
  "SELECT count(*) FROM dolt_status WHERE table_name='meta';" \
  "0" "$DB"
run_test "rv_dirty_unrelated_log_has_revert" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "1" "$DB"

db_rm "$DB"

# Case 2: dirty unstaged change to the SAME table the revert would touch
# -> revert REFUSES, no new commit, dirty change still there.

DB=/tmp/test_rv_dirty_same_table_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');
INSERT INTO t VALUES(99,'side');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_dirty_same_refuses" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "cannot revert with uncommitted changes" "$DB"
run_test "rv_dirty_same_no_new_commit" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "0" "$DB"
run_test "rv_dirty_same_row_kept" \
  "SELECT x FROM t WHERE id=99;" "side" "$DB"

db_rm "$DB"

# Case 3: STAGED change to an unrelated table -> revert SUCCEEDS and includes
# the staged change in the revert commit, matching Dolt.

DB=/tmp/test_rv_staged_unrelated_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');
INSERT INTO meta VALUES(1,'side');
SELECT dolt_add('meta');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_staged_unrelated_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_staged_unrelated_log_has_revert" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "1" "$DB"
run_test "rv_staged_unrelated_status_clean" \
  "SELECT count(*) FROM dolt_status WHERE table_name='meta';" \
  "0" "$DB"

db_rm "$DB"

# Case 4: clean working set -> revert SUCCEEDS (regression guard).

DB=/tmp/test_rv_clean_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_clean_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_clean_t_reverted" "SELECT count(*) FROM t;" "0" "$DB"

db_rm "$DB"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
