#!/bin/bash

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Storage locking + WAL replay verify (S6 + S10) ==="
echo ""

DB=/tmp/test_lock_nested_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "s10_commit_with_nested_lock" \
  "SELECT count(*) FROM t;" "3" "$DB"
db_rm "$DB"

DB=/tmp/test_lock_seq_gc_$$.db; db_rm "$DB"
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  for i in $(seq 1 10); do
    echo "INSERT INTO t VALUES($i, 'row_$i');"
    echo "SELECT dolt_commit('-A','-m','c$i');"
  done
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "s10_first_gc" "SELECT dolt_gc();" "chunks" "$DB"
run_test_match "s10_second_gc" "SELECT dolt_gc();" "chunks" "$DB"
run_test_match "s10_third_gc" "SELECT dolt_gc();" "chunks" "$DB"
run_test "s10_data_after_three_gcs" "SELECT count(*) FROM t;" "10" "$DB"

db_rm "$DB"

DB=/tmp/test_s6_wal_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

file_size=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB" 2>/dev/null)
mid=$((file_size / 2))
perl -e '
  my ($f, $off) = @ARGV;
  open my $fh, "+<:raw", $f or die $!;
  seek $fh, $off, 0;
  my $b;
  read $fh, $b, 1;
  my $flipped = chr(ord($b) ^ 0xff);
  seek $fh, $off, 0;
  print $fh $flipped;
  close $fh;
' "$DB" "$mid"

out=$($DOLTLITE "$DB" "SELECT v FROM t WHERE id=1;" 2>&1)
exit_rc=$?
if [ "$exit_rc" -ge 128 ]; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: s6_post_corruption_no_crash\n  exit=$exit_rc (signal)"
else
  PASS=$((PASS+1))
fi
db_rm "$DB"

DB=/tmp/test_s6_healthy_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "s6_healthy_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "s6_healthy_reopen_value" "SELECT v FROM t WHERE id=2;" "b" "$DB"

db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
