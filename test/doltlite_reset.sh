#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local name="$1" sql="$2" expected="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if [ "$result" = "$expected" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"; fi
}
run_test_match() {
  local name="$1" sql="$2" pattern="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if echo "$result" | grep -qE "$pattern"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"; fi
}

echo "=== Doltlite Reset Tests ==="
echo ""

# --- Soft reset ---
DB=/tmp/test_reset_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Make change, stage it
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_add('-A');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "staged_before_soft_reset" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB"

# Soft reset
echo "SELECT dolt_reset('--soft');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "unstaged_after_soft_reset" \
  "SELECT count(*) FROM dolt_status WHERE staged=0;" \
  "1" "$DB"

run_test "no_staged_after_soft_reset" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "0" "$DB"

run_test "data_preserved_soft_reset" \
  "SELECT count(*) FROM t;" \
  "2" "$DB"

# --- Hard reset ---
echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "clean_after_hard_reset" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

run_test "data_reverted_hard_reset" \
  "SELECT count(*) FROM t;" \
  "1" "$DB"

run_test "correct_data_after_hard" \
  "SELECT v FROM t;" \
  "a" "$DB"

# --- Hard reset discards multiple changes ---
DB2=/tmp/test_reset2_$$.db; rm -f "$DB2"
echo "CREATE TABLE a(x); CREATE TABLE b(y); INSERT INTO a VALUES(1); INSERT INTO b VALUES(2); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO a VALUES(10); INSERT INTO b VALUES(20); CREATE TABLE c(z);" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "changes_before_hard" \
  "SELECT count(*) FROM dolt_status;" \
  "3" "$DB2"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "clean_after_multi_hard" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB2"

run_test "a_reverted" \
  "SELECT * FROM a;" \
  "1" "$DB2"

run_test "b_reverted" \
  "SELECT * FROM b;" \
  "2" "$DB2"

# --- Soft reset after staging specific table ---
DB3=/tmp/test_reset3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_add('t');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "staged_before_reset" \
  "SELECT staged FROM dolt_status;" \
  "1" "$DB3"

echo "SELECT dolt_reset();" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "default_is_soft" \
  "SELECT staged FROM dolt_status;" \
  "0" "$DB3"

# --- Hard reset persists across reopen ---
DB4=/tmp/test_reset4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "hard_reset_persists" \
  "SELECT * FROM t;" \
  "1" "$DB4"

# --- Hard reset to a specific commit hash ---
DB5=/tmp/test_reset5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'v1'); SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB5" > /dev/null 2>&1
C1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB5" 2>/dev/null)
echo "UPDATE t SET v='v2' WHERE x=1; SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'new'); SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "pre_reset_count" "SELECT count(*) FROM t;" "2" "$DB5"
run_test "pre_reset_commits" "SELECT count(*) FROM dolt_log;" "4" "$DB5"

echo "SELECT dolt_reset('--hard','$C1');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "reset_to_hash_data" "SELECT v FROM t;" "v1" "$DB5"
run_test "reset_to_hash_count" "SELECT count(*) FROM t;" "1" "$DB5"
run_test "reset_to_hash_log" "SELECT count(*) FROM dolt_log;" "2" "$DB5"
run_test "reset_to_hash_head" "SELECT commit_hash FROM dolt_log LIMIT 1;" "$C1" "$DB5"
run_test "reset_to_hash_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB5"

# --- Reset to commit hash clears merge state ---
DB6=/tmp/test_reset6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
C_INIT=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB6" 2>/dev/null)
echo "SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET v='OTHER'; SELECT dolt_commit('-A','-m','other');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET v='MAIN'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_merge('other');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test "merge_has_conflicts" "SELECT count(*) FROM dolt_conflicts;" "1" "$DB6"

echo "SELECT dolt_reset('--hard','$C_INIT');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test "reset_clears_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB6"
run_test "reset_restores_init" "SELECT v FROM t;" "a" "$DB6"

# --- Bad ref errors gracefully ---
run_test_match "reset_bad_ref" \
  "SELECT dolt_reset('--hard','not_a_real_ref');" \
  "not found" "$DB6"

# Cleanup
rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
