#!/bin/bash
# A run of small commits zero-fills a tail past the logical end of the file
# and commits into it. The file is then larger than its content while a
# writer is open, and a commit no longer grows it, so peers must find new
# commits without a size change, a crash must leave an openable file, and a
# clean close must give the space back.
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Zero-filled tail ==="
echo ""

ROOT="$(pwd)/$(mktemp -d ./.doltlite-zero-tail.XXXXXX)"
trap 'rm -rf "$ROOT"' EXIT
DB="$ROOT/t.db"
REAL="${DLTEST_REAL_DOLTLITE:-$DOLTLITE}"
TAIL=524288

cat >"$ROOT/peer.sh" <<EOF
#!/bin/sh
"$REAL" "$DB" "\$1"
EOF
cat >"$ROOT/size.sh" <<EOF
#!/bin/sh
wc -c <"$DB" | tr -d ' '
EOF

inserts() {
  local i
  for i in $(seq "$1" "$2"); do echo "INSERT INTO t VALUES($i,'row $i');"; done
}

seed() {
  rm -f "$DB" "$ROOT/.t.db-lock"
  "$DOLTLITE" "$DB" "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
    INSERT INTO t VALUES(0,'seed'); SELECT dolt_commit('-Am','seed');" >/dev/null
}

size() { wc -c <"$DB" | tr -d ' '; }

seed
base=$(size)

out=$( { inserts 1 20
         echo ".shell sh $ROOT/size.sh"
         inserts 21 25
         echo ".shell sh $ROOT/size.sh"
         echo ".shell sh $ROOT/peer.sh 'SELECT count(*) FROM t'"
         inserts 26 30
         echo ".shell sh $ROOT/peer.sh 'SELECT count(*) FROM t'"
         echo ".shell sh $ROOT/size.sh"
         inserts 31 35
       } | "$DOLTLITE" "$DB" 2>&1 )
during=$(printf '%s\n' "$out" | sed -n 1p)
dltest_assert_int_ge "tail_present_while_open" "$during" $((base+TAIL))
dltest_assert_equal "commits_do_not_grow_file" "$(printf '%s\n' "$out" | sed -n 2p)" "$during"
dltest_assert_equal "peer_reads_commit_in_tail" "$(printf '%s\n' "$out" | sed -n 3p)" "26"
dltest_assert_equal "peer_reads_later_commit_in_tail" "$(printf '%s\n' "$out" | sed -n 4p)" "31"
dltest_assert_equal "peer_reader_keeps_tail" "$(printf '%s\n' "$out" | sed -n 5p)" "$during"
closed=$(size)
dltest_assert_int_in "tail_trimmed_on_close" "$closed" "$base" $((during-TAIL))
run_test "rows_after_close" "SELECT count(*) FROM t; PRAGMA integrity_check;" "36
ok" "$DB"

seed
out=$( { inserts 1 20
         echo ".shell sh $ROOT/peer.sh \"INSERT INTO t VALUES(1000,'peer')\""
         inserts 21 40
         echo ".shell sh $ROOT/peer.sh \"INSERT INTO t VALUES(1001,'peer')\""
         echo "SELECT count(*) FROM t;"
       } | "$DOLTLITE" "$DB" 2>&1 )
dltest_assert_equal "writer_sees_peer_commits" "$out" "43"
run_test "peer_writer_rows" "SELECT count(*), sum(k>=1000) FROM t; PRAGMA integrity_check;" "43|2
ok" "$DB"

seed
base=$(size)
{ inserts 1 20
  echo ".shell kill -9 \$PPID"
} | "$REAL" "$DB" >/dev/null 2>&1
dltest_assert_int_ge "crash_leaves_tail" "$(size)" $((base+TAIL))
run_test "crash_keeps_acknowledged_commits" "SELECT count(*) FROM t; PRAGMA integrity_check;" "21
ok" "$DB"
run_test "write_after_crash" "INSERT INTO t VALUES(500,'after'); SELECT count(*) FROM t;" "22" "$DB"
dltest_assert_int_in "crash_tail_reclaimed" "$(size)" "$base" $((base+TAIL))
run_test "rows_after_crash_recovery" "SELECT count(*) FROM t; PRAGMA integrity_check;" "22
ok" "$DB"

seed
base=$(size)
dd if=/dev/zero bs=65536 count=16 2>/dev/null >>"$DB"
run_test "foreign_zero_tail_reads" "SELECT count(*) FROM t; PRAGMA integrity_check;" "1
ok" "$DB"
run_test "foreign_zero_tail_writes" "INSERT INTO t VALUES(1,'one'); SELECT count(*) FROM t;" "2" "$DB"
dltest_assert_int_in "foreign_zero_tail_reclaimed" "$(size)" "$base" $((base+TAIL))

dltest_finish
