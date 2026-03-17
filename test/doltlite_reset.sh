#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local name="$1" sql="$2" expected="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  if [ "$result" = "$expected" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"; fi
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

# Cleanup
rm -f "$DB" "$DB2" "$DB3" "$DB4"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
