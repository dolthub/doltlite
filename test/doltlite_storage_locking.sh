#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"
. "$(dirname "$0")/lib/doltlite_integrity_common.sh"

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""



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

dltest_expect_integrity "$DB" 'skip:deliberately flipped byte in stored chunk'
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

DB=/tmp/test_torn_creation_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "SELECT 1;" > /dev/null 2>&1
FULL=$(wc -c < "$DB" | tr -d ' ')
# The closed file ends with a clean-close root (169 bytes) written after the
# first fsync; a torn creation never reached it.
PRECLOSE=$((FULL - 169))
for cut in 168 169 200 400 500 600 1000 $((PRECLOSE - 100)); do
  T="$DB.cut$cut"; head -c "$cut" "$DB" > "$T"
  SUM=$(cksum < "$T")
  dltest_expect_integrity "$T" 'skip:truncated creation has no recoverable root'
  run_test_error_match "torn_creation_cut_${cut}_refused" \
    "CREATE TABLE t(a);" "database disk image is malformed" "$T"
  dltest_assert_equal "torn_creation_cut_${cut}_keeps_the_bytes" "$(cksum < "$T")" "$SUM"
  rm -f "$T" "$(dirname "$T")/.$(basename "$T")-lock"
done
T="$DB.zero"; head -c "$PRECLOSE" "$DB" > "$T"
dd if=/dev/zero of="$T" bs=1 seek=168 count=$((PRECLOSE - 169 - 168)) conv=notrunc 2>/dev/null
SUM=$(cksum < "$T")
dltest_expect_integrity "$T" 'skip:zeroed creation data is corrupt'
run_test_error_match "torn_creation_zeroed_data_refused" \
  "CREATE TABLE t(a);" "database disk image is malformed" "$T"
dltest_assert_equal "torn_creation_zeroed_data_keeps_the_bytes" "$(cksum < "$T")" "$SUM"
rm -f "$T" "$(dirname "$T")/.$(basename "$T")-lock"
db_rm "$DB"; rm -f "$(dirname "$DB")/.$(basename "$DB")-lock"

byte_size() { wc -c < "$1" | tr -d ' '; }
lock_rm() { rm -f "$1" "$(dirname "$1")/.$(basename "$1")-lock" "${1}-wal"; }

DB=/tmp/test_open_empty_$$.db
FRESH="$DB.fresh"
$DOLTLITE "$FRESH" "SELECT 1;" > /dev/null 2>&1
FULL=$(byte_size "$FRESH")
PRECLOSE=$((FULL - 169))
TORN="$DB.torn"
head -c "$PRECLOSE" "$FRESH" > "$TORN"
dd if=/dev/zero of="$TORN" bs=1 count=168 conv=notrunc 2>/dev/null
TORN_SUM=$(cksum < "$TORN")

ZERO="$DB.zero"
: > "$ZERO"
run_test "zero_byte_rw_opens_empty" \
  "SELECT count(*) FROM sqlite_master;" "0" "file:$ZERO?mode=rw"
lock_rm "$ZERO"
: > "$ZERO"
run_test "zero_byte_ro_opens_empty" \
  "SELECT count(*) FROM sqlite_master;" "0" "file:$ZERO?mode=ro"
dltest_assert_equal "zero_byte_ro_open_leaves_the_file_empty" "$(byte_size "$ZERO")" "0"
run_test_error_match "zero_byte_ro_does_not_create" \
  "CREATE TABLE t(a);" "attempt to write a readonly database" "file:$ZERO?mode=ro"
dltest_assert_equal "zero_byte_ro_write_leaves_the_file_empty" "$(byte_size "$ZERO")" "0"
lock_rm "$ZERO"

for mode in rw ro rwc; do
  X="$DB.$mode"
  cp "$TORN" "$X"
  dltest_expect_integrity "$X" 'skip:zeroed header is not a database'
  run_test_error_match "torn_header_${mode}_refused" \
    "SELECT count(*) FROM sqlite_master;" "file is not a database" "file:$X?mode=$mode"
  run_test_error_match "torn_header_${mode}_write_refused" \
    "CREATE TABLE t(a);" "file is not a database" "file:$X?mode=$mode"
  dltest_assert_equal "torn_header_${mode}_keeps_the_bytes" "$(cksum < "$X")" "$TORN_SUM"
  lock_rm "$X"
done

MISS="$DB.missing"
run_test_error_match "missing_rw_stays_closed" \
  "SELECT count(*) FROM sqlite_master;" "unable to open database file" "file:$MISS?mode=rw"
run_test_error_match "missing_ro_stays_closed" \
  "SELECT count(*) FROM sqlite_master;" "unable to open database file" "file:$MISS?mode=ro"

GARB="$DB.garb"
printf 'not a doltlite database file!!!!' > "$GARB"
dltest_expect_integrity "$GARB" 'skip:foreign bytes are not a database'
run_test_error_match "garbage_rw_stays_not_a_database" \
  "SELECT count(*) FROM sqlite_master;" "file is not a database" "file:$GARB?mode=rw"
run_test_error_match "garbage_ro_stays_not_a_database" \
  "SELECT count(*) FROM sqlite_master;" "file is not a database" "file:$GARB?mode=ro"
lock_rm "$GARB"

COMMITTED="$DB.committed"
$DOLTLITE "$COMMITTED" "CREATE TABLE t(a INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-Am','c1'); INSERT INTO t VALUES(2); SELECT dolt_commit('-am','c2');" > /dev/null 2>&1
cp "$COMMITTED" "$DB.poison"
dd if=/dev/zero of="$DB.poison" bs=1 count=168 conv=notrunc 2>/dev/null
dltest_expect_integrity "$DB.poison" 'skip:a later commit under a zeroed header stays closed'
run_test_error_match "zeroed_header_after_a_later_commit_stays_closed" \
  "SELECT count(*) FROM t;" "file is not a database" "file:$DB.poison?mode=rw"
lock_rm "$COMMITTED"
lock_rm "$DB.poison"

STOCK="$DB.stock"
$DOLTLITE "file:$STOCK?doltlite_engine=sqlite" "CREATE TABLE t(a); INSERT INTO t SELECT randomblob(1000) FROM generate_series(1,200);" > /dev/null 2>&1
dd if=/dev/zero of="$STOCK" bs=512 count=1 conv=notrunc 2>/dev/null
STOCK_SUM=$(cksum < "$STOCK")
for mode in rwc rw ro; do
  X="$DB.stock_$mode"
  cp "$STOCK" "$X"
  dltest_expect_integrity "$X" 'skip:an ordinary SQLite file without its header is not a database'
  run_test_error_match "zeroed_sqlite_header_${mode}_stays_not_a_database" \
    "SELECT count(*) FROM sqlite_master;" "file is not a database" "file:$X?mode=$mode"
  dltest_assert_equal "zeroed_sqlite_header_${mode}_keeps_the_bytes" "$(cksum < "$X")" "$STOCK_SUM"
  lock_rm "$X"
done
X="$DB.stock_attach"
cp "$STOCK" "$X"
run_test_error_match "zeroed_sqlite_header_attach_stays_not_a_database" \
  "ATTACH '$X' AS s; SELECT count(*) FROM s.sqlite_master;" "file is not a database" ":memory:"
dltest_assert_equal "zeroed_sqlite_header_attach_keeps_the_bytes" "$(cksum < "$X")" "$STOCK_SUM"
lock_rm "$X"
lock_rm "$STOCK"

ALLZERO="$DB.allzero"
head -c 4096 /dev/zero > "$ALLZERO"
dltest_expect_integrity "$ALLZERO" 'skip:zeros without a creation root are not a database'
run_test_error_match "all_zero_file_stays_not_a_database" \
  "SELECT count(*) FROM sqlite_master;" "file is not a database" "$ALLZERO"
dltest_assert_equal "all_zero_file_keeps_its_size" "$(byte_size "$ALLZERO")" "4096"
lock_rm "$ALLZERO"
lock_rm "$FRESH"
lock_rm "$TORN"
rm -f "$ZERO"

dltest_finish
