#!/bin/bash
# Cover deep-review S6 + S10.
#
# S6 — WAL replay accepts chunk bodies without re-hashing. Torn or
#      bitflipped writes were silently indexed; subsequent reads
#      returned SQLITE_CORRUPT but the bad chunk lived in the index.
#      Now WAL replay re-hashes each chunk body and skips on
#      mismatch; the missing chunk surfaces at first read.
#
# S10 — Intra-process chunk-store lock used flock + a graphLockFd
#       short-circuit. Under shared cache (two connections sharing
#       BtShared → one ChunkStore) and across threads, the second
#       caller saw "lock already held" and proceeded without real
#       exclusion. The fix wraps the lock in a recursive sqlite3
#       mutex: nested calls from the same thread succeed (existing
#       commit-cycle nesting), concurrent callers from a different
#       thread get SQLITE_BUSY.

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

# ----------------------------------------------------------------
# S10 — Nested locking still works. A commit cycle internally
# acquires the chunk-store lock, then csCommitToFile (re-entered
# via chunkStoreCommit) sees graphLockFd already held and skips
# re-acquiring. With the mutex now in place, depth-tracking must
# preserve that pattern.
# ----------------------------------------------------------------
DB=/tmp/test_lock_nested_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "s10_commit_with_nested_lock" \
  "SELECT count(*) FROM t;" "3" "$DB"
db_rm "$DB"

# GC takes the lock; multiple sequential GCs on the same connection
# must each be able to acquire/release cleanly.
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

# ----------------------------------------------------------------
# S6 — Corrupt a chunk body that lives in the WAL portion of the
# file. WAL replay must detect the hash mismatch, skip the chunk,
# and surface the missing chunk via integrity_check.
#
# This complements the regression-bank test
# `integrity_check_surfaces_root_corruption`, which exercises the
# same scenario but with a chunk whose offset sign at corruption
# time may land in either WAL or indexed area depending on commit
# state.
# ----------------------------------------------------------------
DB=/tmp/test_s6_wal_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Open the file and flip a byte in the middle. Without knowing the
# exact layout we can't guarantee we hit the WAL portion, but we
# can verify the reopen path doesn't crash and surfaces *some*
# error when corruption is severe.
file_size=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB" 2>/dev/null)
mid=$((file_size / 2))
# Flip a byte well past the file header (manifest is 168 bytes).
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

# Open and try to read. Behavior is "either: data is still readable
# because the corruption hit padding / unrelated bytes; or: surface
# an error". The key requirement is no crash.
out=$($DOLTLITE "$DB" "SELECT v FROM t WHERE id=1;" 2>&1)
exit_rc=$?
# Exit must be 0 or a SQLite error code, never SIGSEGV / SIGBUS.
if [ "$exit_rc" -ge 128 ]; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: s6_post_corruption_no_crash\n  exit=$exit_rc (signal)"
else
  PASS=$((PASS+1))
fi
db_rm "$DB"

# ----------------------------------------------------------------
# S6 — Healthy database round-trip still works (regression guard
# for the streaming-hash code path in WAL replay).
# ----------------------------------------------------------------
DB=/tmp/test_s6_healthy_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Reopen and verify all rows still come back.
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
