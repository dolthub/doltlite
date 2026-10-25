#!/bin/bash
# DROP COLUMN rewrites every row. The rewritten rows must encode their values
# exactly as an ordinary write does, or untouched cells read as changed.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DROP COLUMN keeps the canonical row encoding ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_dropcol_enc_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

# The same table built two ways, in two databases, hashes the same.
same_hash() {  # same_hash <name> <pk type> <rows with legacy> <rows without>
  local a b
  a=$("$DOLTLITE" "$ROOT/$1_dropped.db" "
CREATE TABLE t(k $2 PRIMARY KEY, a INT, legacy TEXT, done INT);
INSERT INTO t VALUES $3;
ALTER TABLE t DROP COLUMN legacy;
SELECT dolt_hashof_table('t');")
  b=$("$DOLTLITE" "$ROOT/$1_fresh.db" "
CREATE TABLE t(k $2 PRIMARY KEY, a INT, done INT);
INSERT INTO t VALUES $4;
SELECT dolt_hashof_table('t');")
  if [ -n "$a" ] && [ "$a" = "$b" ]; then
    dltest_pass
  else
    dltest_fail "$1" "  dropped: $a\n  fresh:   $b"
  fi
}
same_hash dropped_table_hash_matches_fresh_write INTEGER \
  "(1,0,'x',1),(2,1,'y',0),(3,7,'z',1)" "(1,0,1),(2,1,0),(3,7,1)"
same_hash dropped_text_pk_table_hash_matches_fresh_write TEXT \
  "('a',0,'x',1),('b',1,'y',0)" "('a',0,1),('b',1,0)"

run_test "add_set_drop_column_leaves_status_clean" "
CREATE TABLE t(k INTEGER PRIMARY KEY, a INT, done INT);
INSERT INTO t VALUES(1,10,0),(2,20,1);
SELECT dolt_commit('-Am','base') IS NOT NULL;
ALTER TABLE t ADD COLUMN x INT;
UPDATE t SET x=5;
ALTER TABLE t DROP COLUMN x;
SELECT count(*) FROM dolt_status;
" "1
0" "$ROOT/status.db"

DB="$ROOT/merge.db"
"$DOLTLITE" "$DB" "
CREATE TABLE t(pk INTEGER PRIMARY KEY, a INT, legacy TEXT, done INT);
INSERT INTO t VALUES(1,10,'x',0),(2,20,'y',1);
SELECT dolt_commit('-Am','base');
ALTER TABLE t DROP COLUMN legacy; SELECT dolt_commit('-am','drop'); SELECT dolt_branch('g');
UPDATE t SET done=1 WHERE pk=1; SELECT dolt_commit('-am','m');
SELECT dolt_checkout('g'); UPDATE t SET a=11 WHERE pk=1; SELECT dolt_commit('-am','g');
SELECT dolt_checkout('main');" >/dev/null
run_test "merge_after_drop_column_has_no_false_conflict" "
SELECT dolt_merge('g') IS NOT NULL;
SELECT pk, a, done FROM t ORDER BY pk;
" "1
1|11|1
2|20|1" "$DB"

dltest_finish
