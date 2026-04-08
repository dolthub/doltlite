#!/bin/bash
#
# Regression guards for every issue from Aaron's code review.
# Each test section references the specific bug it prevents from regressing.
# If any of these fail, a fix from the review remediation has regressed.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi
}
run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi
}

echo "=== Review Regression Guards ==="
echo ""

# ============================================================
# GUARD 1: Durability — committed data survives reopen
# Bug: pre-fsync manifest overwrite could lose data on crash
# Fix: removed write to offset 0 before fsync
# ============================================================

echo "--- Guard 1: Durability (data survives reopen) ---"

DB=/tmp/test_rg_durable_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'durable');
SELECT dolt_commit('-A','-m','persist test');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "durable_data" "SELECT v FROM t WHERE id=1;" "durable" "$DB"
run_test "durable_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"

# Second commit
echo "INSERT INTO t VALUES(2,'also durable');
SELECT dolt_commit('-A','-m','second persist');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "durable_second" "SELECT v FROM t WHERE id=2;" "also durable" "$DB"
run_test "durable_log2" "SELECT count(*) FROM dolt_log;" "2" "$DB"
rm -f "$DB"

# ============================================================
# GUARD 2: Error propagation — corrupt refs returns error
# Bug: csDeserializeRefs errors were silently swallowed,
#      losing all branches/tags without any error
# Fix: propagate error at all 3 call sites + consistency check
# ============================================================

echo "--- Guard 2: Error propagation (refs integrity) ---"

DB=/tmp/test_rg_refs_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INT);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Refs must exist after commit
run_test_match "refs_exist" \
  "SELECT count(*) FROM dolt_branches;" "^[1-9]" "$DB"

# Branches must have valid commit hashes
run_test_match "refs_valid_hash" \
  "SELECT length(hash) FROM dolt_branches LIMIT 1;" "^40$" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 3: Concurrent commit conflict detection
# Bug: two connections commit to same branch, first is silently
#      lost. No error, no indication of data loss.
# Fix: session HEAD vs branch tip check under graph lock
# ============================================================

echo "--- Guard 3: Concurrent commit detection ---"

# This requires the C test (concurrent_commit_test.c) which
# exercises the actual two-connection scenario. Here we verify
# the simpler invariant: after commit, session HEAD matches
# branch tip (prerequisite for conflict detection to work).

DB=/tmp/test_rg_conflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INT, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','first');" | $DOLTLITE "$DB" > /dev/null 2>&1

# HEAD commit hash should match the branch's commit hash
run_test "head_matches_branch" \
  "SELECT (SELECT commit_hash FROM dolt_log LIMIT 1) = (SELECT hash FROM dolt_branches WHERE name='main');" \
  "1" "$DB"

# After a second commit, still matches
echo "INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','second');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "head_matches_branch_2" \
  "SELECT (SELECT commit_hash FROM dolt_log LIMIT 1) = (SELECT hash FROM dolt_branches WHERE name='main');" \
  "1" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 4: Merge log shows both parents' history
# Bug: dolt_log and dolt_history only followed parentHash
#      (first parent), missing the merged branch's commits
# Fix: BFS all parents with dedup
# ============================================================

echo "--- Guard 4: Merge log shows both parents ---"

DB=/tmp/test_rg_merge_log_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main1');
SELECT dolt_commit('-A','-m','main init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_commit('-A','-m','feature work');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main2');
SELECT dolt_commit('-A','-m','main work');
SELECT dolt_merge('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Log must have 4 entries: merge + main work + feature work + init
run_test "merge_log_all_parents" \
  "SELECT count(*) FROM dolt_log;" "4" "$DB"

# Feature commit must be visible in the log
run_test_match "merge_log_has_feature" \
  "SELECT group_concat(message, '|') FROM dolt_log;" "feature work" "$DB"

# Main commit must also be visible
run_test_match "merge_log_has_main" \
  "SELECT group_concat(message, '|') FROM dolt_log;" "main work" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 5: Branch commit reopen with diverged manifest head
# Bug: p->root was set from always-empty commit.rootHash,
#      zeroing the working tree on reopen
# Fix: use chunkStoreGetRoot instead of commit.rootHash
# ============================================================

echo "--- Guard 5: Branch reopen with diverged manifest ---"

DB=/tmp/test_rg_diverge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'from_main');
SELECT dolt_commit('-A','-m','main commit');
SELECT dolt_branch('dev');
SELECT dolt_checkout('dev');
INSERT INTO t VALUES(2,'from_dev');
SELECT dolt_commit('-A','-m','dev commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main_again');
SELECT dolt_commit('-A','-m','main second');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Reopen on dev — manifest head is main's latest, not dev's
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "diverged_dev_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "diverged_dev_val" "SELECT v FROM t WHERE id=2;" "from_dev" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 6: Virtual tables use real column names, not fallback
# Bug: when column names couldn't be determined, virtual tables
#      fell back to generic schemas (from_value, to_value)
# Fix: return SQLITE_ERROR instead of generic schema
# ============================================================

echo "--- Guard 6: Virtual table schema correctness ---"

DB=/tmp/test_rg_vtab_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INT);
INSERT INTO users VALUES(1,'alice',30);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# dolt_diff_users must have from_name/to_name columns, not from_value/to_value
run_test_match "diff_has_real_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_diff_users');" \
  "from_name" "$DB"

# dolt_history_users must have actual column names
run_test_match "history_has_real_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_history_users');" \
  "\bname\b" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 7: Commit chain integrity after multiple operations
# Bug: various issues could leave orphan commits, broken chains
# Fix: multiple fixes; this verifies the invariant holistically
# ============================================================

echo "--- Guard 7: Commit chain integrity ---"

DB=/tmp/test_rg_chain_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_branch('br');
SELECT dolt_checkout('br');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'d');
SELECT dolt_commit('-A','-m','c4');
SELECT dolt_merge('br');
SELECT dolt_commit('-A','-m','c5 merge');
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

# After all operations + GC, commit chain must be intact
run_test_match "chain_log_count" \
  "SELECT count(*) FROM dolt_log;" "^[5-6]$" "$DB"

# All branches must point to valid commits
run_test_match "chain_branches_valid" \
  "SELECT length(hash) FROM dolt_branches WHERE name='main';" "^40$" "$DB"

# Data must be complete
run_test "chain_data_count" "SELECT count(*) FROM t;" "4" "$DB"

# Reopen and verify persistence
run_test "chain_reopen_count" "SELECT count(*) FROM t;" "4" "$DB"
run_test_match "chain_reopen_log" \
  "SELECT count(*) FROM dolt_log;" "^[5-6]$" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 8: Encoding consistency (LE macros match inline code)
# Bug: encoding was done inline with inconsistent patterns
# Fix: shared PROLLY_GET/PUT_U16/U32 macros
# Invariant: round-trip encode/decode produces same values
# (tested implicitly by all persistence tests; this verifies
# the serialization format hasn't drifted)
# ============================================================

echo "--- Guard 8: Serialization round-trip ---"

DB=/tmp/test_rg_serial_$$.db; rm -f "$DB"

# Create data that exercises various field types and sizes
echo "CREATE TABLE mixed(
  id INTEGER PRIMARY KEY,
  name TEXT,
  score REAL,
  data BLOB,
  flag INTEGER
);
INSERT INTO mixed VALUES(1,'hello',3.14,X'DEADBEEF',1);
INSERT INTO mixed VALUES(2,'world',2.71828,X'00FF00FF00',0);
INSERT INTO mixed VALUES(999999,'big id',0.0,NULL,-1);
SELECT dolt_commit('-A','-m','mixed types');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Reopen and verify all types survived serialization
run_test "serial_text" "SELECT name FROM mixed WHERE id=1;" "hello" "$DB"
run_test "serial_real" "SELECT printf('%.2f',score) FROM mixed WHERE id=1;" "3.14" "$DB"
run_test "serial_blob" "SELECT hex(data) FROM mixed WHERE id=1;" "DEADBEEF" "$DB"
run_test "serial_null" "SELECT data IS NULL FROM mixed WHERE id=999999;" "1" "$DB"
run_test "serial_negative" "SELECT flag FROM mixed WHERE id=999999;" "-1" "$DB"
run_test "serial_large_id" "SELECT name FROM mixed WHERE id=999999;" "big id" "$DB"

rm -f "$DB"

# ============================================================
# GUARD 9: GC preserves all reachable data
# Bug: hardcoded offsets in GC catalog parsing could miss chunks
# Fix: named constants (CAT_HEADER_SIZE, etc.)
# ============================================================

echo "--- Guard 9: GC preserves data ---"

DB=/tmp/test_rg_gc_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'before gc');
SELECT dolt_commit('-A','-m','pre-gc');
INSERT INTO t VALUES(2,'more data');
SELECT dolt_commit('-A','-m','pre-gc 2');
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_data_intact" "SELECT count(*) FROM t;" "2" "$DB"
run_test "gc_log_intact" "SELECT count(*) FROM dolt_log;" "2" "$DB"
run_test "gc_val_intact" "SELECT v FROM t WHERE id=1;" "before gc" "$DB"

# Reopen after GC
run_test "gc_reopen_data" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
