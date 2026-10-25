#!/bin/bash
# An integral value in a REAL column must be stored one way whichever write
# path produced it, or untouched cells read as changed.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Integral REAL values keep one encoding ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_real_enc_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

# Same rows, index created before vs after the insert; the table hashes match.
same_hash() {  # same_hash <name> <pk type>
  local a b
  a=$("$DOLTLITE" "$ROOT/$1_before.db" "
CREATE TABLE t(id $2 PRIMARY KEY, name TEXT, price REAL);
CREATE INDEX ip ON t(price);
INSERT INTO t VALUES(1,'apple',3),(2,'pear',4.5),(3,'fig',-2);
SELECT dolt_hashof_table('t');")
  b=$("$DOLTLITE" "$ROOT/$1_after.db" "
CREATE TABLE t(id $2 PRIMARY KEY, name TEXT, price REAL);
INSERT INTO t VALUES(1,'apple',3),(2,'pear',4.5),(3,'fig',-2);
CREATE INDEX ip ON t(price);
SELECT dolt_hashof_table('t');")
  if [ -n "$a" ] && [ "$a" = "$b" ]; then
    dltest_pass
  else
    dltest_fail "$1" "  index first: $a\n  index after: $b"
  fi
}
same_hash indexed_insert_rowid_hash_matches INTEGER
same_hash indexed_insert_text_pk_hash_matches TEXT

run_test "noop_update_beside_indexed_real_status_clean" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, price REAL);
CREATE INDEX ip ON t(price);
INSERT INTO t VALUES(1,'apple',3),(2,'pear',4);
SELECT dolt_commit('-Am','base') IS NOT NULL;
UPDATE t SET name='apple' WHERE id=1;
SELECT count(*) FROM dolt_status;
SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';
" "1
0
0" "$ROOT/status.db"

DB="$ROOT/merge.db"
"$DOLTLITE" "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, price REAL);
CREATE INDEX ip ON t(price);
INSERT INTO t VALUES(1,'apple',3),(2,'pear',4);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('f');
UPDATE t SET name='Apple' WHERE id=1; SELECT dolt_commit('-am','rename');
SELECT dolt_checkout('f'); UPDATE t SET price=5 WHERE id=1; SELECT dolt_commit('-am','reprice');
SELECT dolt_checkout('main');" >/dev/null
run_test "merge_beside_indexed_real_has_no_false_conflict" "
SELECT dolt_merge('f') IS NOT NULL;
SELECT id, name, price FROM t ORDER BY id;
" "1
1|Apple|5.0
2|pear|4.0" "$DB"

dltest_finish
