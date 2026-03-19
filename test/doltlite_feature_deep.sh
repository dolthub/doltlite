#!/bin/bash
#
# Deep coverage tests for recent features: cherry-pick, revert, GC,
# per-row conflicts, dolt_diff_<table>, dolt_history_<table>,
# and dolt_merge_base.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }
db_size() { local s=0; for f in "$1" "${1}-wal"; do [ -f "$f" ] && s=$((s + $(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null))); done; echo $s; }
db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Deep Feature Coverage Tests ==="
echo ""

# ============================================================
# Cherry-pick: pick a commit that adds a new table
# ============================================================

DB=/tmp/test_deep_cp_newtbl_$$.db; db_rm "$DB"
echo "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO base VALUES(1,'x');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE extra(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO extra VALUES(1,'new');
SELECT dolt_commit('-A','-m','add extra table');
SELECT dolt_checkout('main');
INSERT INTO base VALUES(2,'y');
SELECT dolt_commit('-A','-m','main work');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_newtbl_hash" "SELECT dolt_cherry_pick('$HASH');" "^[0-9a-f]{40}$" "$DB"
run_test "cp_newtbl_base" "SELECT count(*) FROM base;" "2" "$DB"
run_test "cp_newtbl_extra" "SELECT count(*) FROM extra;" "1" "$DB"
run_test "cp_newtbl_extra_val" "SELECT w FROM extra WHERE id=1;" "new" "$DB"

db_rm "$DB"

# ============================================================
# Cherry-pick: pick same commit twice (should conflict/no-op)
# ============================================================

DB=/tmp/test_deep_cp_twice_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "cp_twice_first" "SELECT count(*) FROM t;" "2" "$DB"

# Second cherry-pick of same commit — row 2 already exists, should be convergent or no-op
run_test_match "cp_twice_second" \
  "SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_twice_still2" "SELECT count(*) FROM t;" "2" "$DB"

db_rm "$DB"

# ============================================================
# Revert: revert an INSERT, verify row gone
# ============================================================

DB=/tmp/test_deep_rv_insert_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'remove_me');
INSERT INTO t VALUES(3,'also_remove');
SELECT dolt_commit('-A','-m','add 2 rows');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_insert_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_insert_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "rv_insert_kept" "SELECT v FROM t WHERE id=1;" "keep" "$DB"

db_rm "$DB"

# ============================================================
# Revert: revert preserves later additions
# ============================================================

DB=/tmp/test_deep_rv_preserve_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2: add row 2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3: add row 3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Revert c2 (added row 2) — row 3 should survive
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
C2=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>&1)

run_test_match "rv_preserve_hash" "SELECT dolt_revert('$C2');" "^[0-9a-f]{40}$" "$DB"
run_test "rv_preserve_no2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test "rv_preserve_has3" "SELECT v FROM t WHERE id=3;" "c" "$DB"
run_test "rv_preserve_has1" "SELECT v FROM t WHERE id=1;" "a" "$DB"

db_rm "$DB"

# ============================================================
# GC: verify file size decreases after branch delete + GC
# ============================================================

DB=/tmp/test_deep_gc_size_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create branch with lots of data
echo "SELECT dolt_branch('big');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('big');" | $DOLTLITE "$DB" > /dev/null 2>&1
SQL=""; for i in $(seq 2 100); do SQL="$SQL INSERT INTO t VALUES($i,'row_$i');"; done
echo "$SQL SELECT dolt_commit('-A','-m','100 rows');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BEFORE=$(db_size "$DB")

echo "SELECT dolt_branch('-d','big');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_AFTER=$(db_size "$DB")

if [ "$SIZE_AFTER" -lt "$SIZE_BEFORE" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_size_decrease\n  before: $SIZE_BEFORE\n  after:  $SIZE_AFTER"; fi

run_test "gc_size_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_size_branch" "SELECT count(*) FROM dolt_branches;" "1" "$DB"

db_rm "$DB"

# ============================================================
# GC: doesn't break dolt_log traversal
# ============================================================

DB=/tmp/test_deep_gc_log_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_log_count" "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test "gc_log_first" "SELECT message FROM dolt_log LIMIT 1;" "c3" "$DB"
run_test "gc_log_last" "SELECT message FROM dolt_log LIMIT 1 OFFSET 2;" "c1" "$DB"

db_rm "$DB"

# ============================================================
# GC: doesn't break dolt_diff between commits
# ============================================================

DB=/tmp/test_deep_gc_diff_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_diff_works" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^1$" "$DB"

db_rm "$DB"

# ============================================================
# Conflict rows: resolve then make further changes
# ============================================================

DB=/tmp/test_deep_cfr_further_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf' WHERE id=1;
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

# Now make more changes and commit
echo "INSERT INTO t VALUES(3,'new_after_resolve');
SELECT dolt_commit('-A','-m','post-resolve');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cfr_further_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cfr_further_val" "SELECT v FROM t WHERE id=3;" "new_after_resolve" "$DB"
run_test_match "cfr_further_log" "SELECT message FROM dolt_log LIMIT 1;" "post-resolve" "$DB"
run_test "cfr_further_clean" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"

db_rm "$DB"

# ============================================================
# Conflict rows: persist across reopen, resolve, then reopen again
# ============================================================

DB=/tmp/test_deep_cfr_reopen_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf' WHERE id=1;
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Reopen — conflicts should be there
run_test "cfr_reopen_exists" "SELECT count(*) FROM dolt_conflicts;" "1" "$DB"
run_test "cfr_reopen_row" "SELECT base_rowid FROM dolt_conflicts_t;" "1" "$DB"

# Resolve in this session
echo "DELETE FROM dolt_conflicts_t WHERE base_rowid=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

# Reopen again — should be clean
run_test "cfr_reopen_clean" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "cfr_reopen_val" "SELECT v FROM t WHERE id=1;" "main" "$DB"

db_rm "$DB"

# ============================================================
# dolt_diff_<table>: shows delete operations
# ============================================================

DB=/tmp/test_deep_dt_delete_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','init');
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','delete row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "dt_delete_type" \
  "SELECT count(*) FROM dolt_diff_t WHERE diff_type='removed';" "^[1-9]" "$DB"
run_test "dt_delete_total" "SELECT count(*) FROM dolt_diff_t;" "4" "$DB"

db_rm "$DB"

# ============================================================
# dolt_diff_<table>: shows updates as "modified"
# ============================================================

DB=/tmp/test_deep_dt_update_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='changed' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "dt_update_count" "SELECT count(*) FROM dolt_diff_t;" "2" "$DB"
run_test_match "dt_update_mod" \
  "SELECT diff_type FROM dolt_diff_t WHERE rowid_val=1 AND diff_type='modified';" "modified" "$DB"

db_rm "$DB"

# ============================================================
# dolt_diff_<table>: multiple tables tracked independently
# ============================================================

DB=/tmp/test_deep_dt_multi_$$.db; db_rm "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, y TEXT);
INSERT INTO a VALUES(1,'a1');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO b VALUES(1,'b1');
INSERT INTO b VALUES(2,'b2');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# a has 1 row added in c1, unchanged in c2
run_test "dt_multi_a" "SELECT count(*) FROM dolt_diff_a;" "1" "$DB"
# b has 2 rows added in c2
run_test "dt_multi_b" "SELECT count(*) FROM dolt_diff_b;" "2" "$DB"

db_rm "$DB"

# ============================================================
# dolt_diff_<table>: from_commit is parent, to_commit is child
# ============================================================

DB=/tmp/test_deep_dt_commits_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# The "added row 2" audit entry should have from=c1, to=c2
run_test_match "dt_commits_from" \
  "SELECT from_commit FROM dolt_diff_t WHERE to_v IS NOT NULL LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
run_test_match "dt_commits_to" \
  "SELECT to_commit FROM dolt_diff_t WHERE to_v IS NOT NULL LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
# from and to should be different commits
run_test_match "dt_commits_diff" \
  "SELECT CASE WHEN from_commit != to_commit THEN 'different' ELSE 'same' END FROM dolt_diff_t WHERE to_v IS NOT NULL LIMIT 1;" "different" "$DB"

db_rm "$DB"

# ============================================================
# dolt_diff_<table>: survives GC
# ============================================================

DB=/tmp/test_deep_dt_gc_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "dt_gc_count" "SELECT count(*) FROM dolt_diff_t;" "2" "$DB"
run_test_match "dt_gc_has_added" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" "^2$" "$DB"

db_rm "$DB"

# ============================================================
# dolt_history_<table>: row count grows with commits
# ============================================================

DB=/tmp/test_deep_ht_grow_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ht_grow_1" "SELECT count(*) FROM dolt_history_t;" "1" "$DB"

echo "INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# c1: 1 row, c2: 2 rows → 3 total
run_test "ht_grow_2" "SELECT count(*) FROM dolt_history_t;" "3" "$DB"

echo "INSERT INTO t VALUES(3,'v3');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# c1: 1, c2: 2, c3: 3 → 6 total
run_test "ht_grow_3" "SELECT count(*) FROM dolt_history_t;" "6" "$DB"

db_rm "$DB"

# ============================================================
# dolt_history_<table>: deleted rows don't appear in later commits
# ============================================================

DB=/tmp/test_deep_ht_deleted_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','c2: delete row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# c1: rows 1,2 (2 rows), c2: row 1 only (1 row) → 3 total
run_test "ht_deleted_total" "SELECT count(*) FROM dolt_history_t;" "3" "$DB"
# Row 2 appears in 1 commit only (c1)
run_test "ht_deleted_row2" "SELECT count(*) FROM dolt_history_t WHERE id=2;" "1" "$DB"
# Row 1 appears in both
run_test "ht_deleted_row1" "SELECT count(*) FROM dolt_history_t WHERE id=1;" "2" "$DB"

db_rm "$DB"

# ============================================================
# dolt_history_<table>: different values per commit
# ============================================================

DB=/tmp/test_deep_ht_versions_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'version_1');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='version_2' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='version_3' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# 3 versions of row 1
run_test "ht_versions_count" "SELECT count(*) FROM dolt_history_t WHERE id=1;" "3" "$DB"
# Each has a different commit hash
run_test "ht_versions_distinct" \
  "SELECT count(DISTINCT commit_hash) FROM dolt_history_t WHERE id=1;" "3" "$DB"
# Values should be blobs (different at each commit)
run_test "ht_versions_blobs" \
  "SELECT count(DISTINCT v) FROM dolt_history_t WHERE id=1;" "3" "$DB"

db_rm "$DB"

# ============================================================
# dolt_history_<table>: survives GC
# ============================================================

DB=/tmp/test_deep_ht_gc_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ht_gc_count" "SELECT count(*) FROM dolt_history_t;" "3" "$DB"
run_test "ht_gc_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "2" "$DB"

db_rm "$DB"

# ============================================================
# dolt_history_<table>: works after merge
# ============================================================

DB=/tmp/test_deep_ht_merge_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# After merge, history should include merged data
run_test_match "ht_merge_total" "SELECT count(*) FROM dolt_history_t;" "^[6-9]" "$DB"

# The merge commit should have all 3 rows
MERGE_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
run_test "ht_merge_latest" \
  "SELECT count(*) FROM dolt_history_t WHERE commit_hash='$MERGE_HASH';" "3" "$DB"

db_rm "$DB"

# ============================================================
# dolt_merge_base: basic functionality
# ============================================================

DB=/tmp/test_deep_mb_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

MAIN_HEAD=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
FEAT_HEAD=$(echo "SELECT hash FROM dolt_branches WHERE name='feat';" | $DOLTLITE "$DB" 2>&1)

run_test_match "mb_basic" "SELECT dolt_merge_base('$MAIN_HEAD','$FEAT_HEAD');" "^[0-9a-f]{40}$" "$DB"

# Merge base should be the init commit (oldest)
INIT_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>&1)
run_test "mb_is_init" "SELECT dolt_merge_base('$MAIN_HEAD','$FEAT_HEAD');" "$INIT_HASH" "$DB"

db_rm "$DB"

# ============================================================
# dolt_merge_base: same commit returns itself
# ============================================================

DB=/tmp/test_deep_mb_self_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

HEAD=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
run_test "mb_self" "SELECT dolt_merge_base('$HEAD','$HEAD');" "$HEAD" "$DB"

db_rm "$DB"

# ============================================================
# dolt_merge_base: ancestor of linear history
# ============================================================

DB=/tmp/test_deep_mb_linear_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

C1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2;" | $DOLTLITE "$DB" 2>&1)
C3=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)

# Ancestor of c1 and c3 in linear history is c1
run_test "mb_linear" "SELECT dolt_merge_base('$C1','$C3');" "$C1" "$DB"

db_rm "$DB"

# ============================================================
# Cherry-pick: pick onto different branch
# ============================================================

DB=/tmp/test_deep_cp_branch_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('a');
SELECT dolt_branch('b');
SELECT dolt_checkout('a');
INSERT INTO t VALUES(10,'from_a');
SELECT dolt_commit('-A','-m','a adds row 10');
SELECT dolt_checkout('b');
INSERT INTO t VALUES(20,'from_b');
SELECT dolt_commit('-A','-m','b adds row 20');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Cherry-pick a's commit onto b
echo "SELECT dolt_checkout('a');" | $DOLTLITE "$DB" > /dev/null 2>&1
A_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)
echo "SELECT dolt_checkout('b');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_branch_hash" "SELECT dolt_cherry_pick('$A_HASH');" "^[0-9a-f]{40}$" "$DB"
run_test "cp_branch_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_branch_has10" "SELECT v FROM t WHERE id=10;" "from_a" "$DB"
run_test "cp_branch_has20" "SELECT v FROM t WHERE id=20;" "from_b" "$DB"

db_rm "$DB"

# ============================================================
# GC after revert: reverted data becomes garbage
# ============================================================

DB=/tmp/test_deep_gc_revert_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Add 50 rows then revert
SQL=""; for i in $(seq 2 51); do SQL="$SQL INSERT INTO t VALUES($i,'row_$i');"; done
echo "$SQL SELECT dolt_commit('-A','-m','add 50 rows');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BEFORE=$(db_size "$DB")
echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1
SIZE_AFTER=$(db_size "$DB")

run_test "gc_revert_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_revert_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

db_rm "$DB"

# ============================================================
# dolt_history + dolt_diff consistency
# ============================================================

DB=/tmp/test_deep_consistency_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# History should have c1(2 rows) + c2(3 rows) = 5 rows
run_test "consist_history" "SELECT count(*) FROM dolt_history_t;" "5" "$DB"

# Diff should have c1(2 added) + c2(1 added) = 3 changes
run_test "consist_diff" "SELECT count(*) FROM dolt_diff_t;" "3" "$DB"

# Number of distinct commits should match
run_test "consist_commits_hist" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "2" "$DB"
run_test "consist_commits_diff" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "2" "$DB"

db_rm "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
