#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  tr -d '\r' \
    | sed -e 's/[[:space:]]\{1,\}/ /g' -e 's/^ //' -e 's/ $//' \
          -e 's/^Runtime error /Error /' \
          -e 's/^Error: in prepare, / /' \
          -e 's/ ([0-9]*)$//'
}

oracle() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/sq"

  local dl_out
  dl_out=$(printf '%s\n' "$sql" | "$DOLTLITE" "$dir/dl/db" 2>&1 | normalize)

  local sq_out
  sq_out=$(printf '%s\n' "$sql" | "$SQLITE3" "$dir/sq/db" 2>&1 | normalize)

  if [ "$dl_out" = "$sq_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    sqlite3:";  echo "$sq_out" | sed 's/^/      /'
  fi
}

echo "=== Oracle Tests: large BLOB / TEXT ==="
echo ""

echo "--- BLOB length round-trip ---"

for N in 1 512 1024 8191 8192 8193 16383 16384 16385 32768 65536 262144 1048576; do
  oracle "blob_length_${N}" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob($N));
SELECT id, length(b) FROM t;
"
done

echo "--- BLOB secondary index keys ---"

oracle "large_blob_index_key" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
CREATE INDEX i1 ON t(b);
INSERT INTO t VALUES(1, zeroblob(33000));
INSERT INTO t VALUES(2, zeroblob(34000));
INSERT INTO t VALUES(3, zeroblob(35000));
SELECT id, length(b) FROM t INDEXED BY i1 ORDER BY b, id;
"

oracle "replace_select_large_blob_index_key" "
CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB);
CREATE INDEX i1 ON t1(b);
CREATE TABLE t2(a, b);
INSERT INTO t2 VALUES(4, randomblob(31000));
INSERT INTO t2 VALUES(4, randomblob(32000));
INSERT INTO t2 VALUES(4, randomblob(33000));
REPLACE INTO t1 SELECT a, b FROM t2;
SELECT a, length(b) FROM t1;
"

echo "--- TEXT length round-trip ---"

for N in 1 512 16384 65536 1048576; do
  oracle "text_length_${N}" "
CREATE TABLE t(id INT PRIMARY KEY, s TEXT);
INSERT INTO t VALUES(1, printf('%.*c', $N, 'x'));
SELECT id, length(s) FROM t;
"
done

echo "--- content bytes at chunk boundaries ---"

oracle "blob_substr_at_16384" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(65536));
SELECT length(b), hex(substr(b, 16383, 4)), hex(substr(b, 16385, 4)) FROM t;
"

oracle "blob_substr_at_32768" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(131072));
SELECT length(b), hex(substr(b, 32767, 4)), hex(substr(b, 32769, 4)) FROM t;
"

oracle "blob_substr_last_bytes_1mb" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1048576));
SELECT length(b), hex(substr(b, 1048573, 4)) FROM t;
"

echo "--- UPDATE large ---"

oracle "update_blob_grow" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1024));
UPDATE t SET b = zeroblob(65536) WHERE id = 1;
SELECT id, length(b) FROM t;
"

oracle "update_blob_shrink" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(65536));
UPDATE t SET b = zeroblob(256) WHERE id = 1;
SELECT id, length(b) FROM t;
"

oracle "update_one_of_many_large" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(65536));
INSERT INTO t VALUES(2, zeroblob(65536));
INSERT INTO t VALUES(3, zeroblob(65536));
UPDATE t SET b = zeroblob(1024) WHERE id = 2;
SELECT id, length(b) FROM t ORDER BY id;
"

echo "--- DELETE large ---"

oracle "delete_large_row" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(65536));
INSERT INTO t VALUES(2, zeroblob(1024));
DELETE FROM t WHERE id = 1;
SELECT id, length(b) FROM t ORDER BY id;
"

oracle "delete_then_reinsert_same_id" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(65536));
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, zeroblob(256));
SELECT id, length(b) FROM t;
"

echo "--- mixed sizes ---"

oracle "mixed_small_large_interleaved" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(16));
INSERT INTO t VALUES(2, zeroblob(65536));
INSERT INTO t VALUES(3, zeroblob(16));
INSERT INTO t VALUES(4, zeroblob(65536));
INSERT INTO t VALUES(5, zeroblob(16));
INSERT INTO t VALUES(6, zeroblob(65536));
INSERT INTO t VALUES(7, zeroblob(16));
INSERT INTO t VALUES(8, zeroblob(65536));
INSERT INTO t VALUES(9, zeroblob(16));
INSERT INTO t VALUES(10, zeroblob(65536));
SELECT id, length(b) FROM t ORDER BY id;
"

echo "--- TEXT vs BLOB parity ---"

oracle "large_text_matches_expected_length" "
CREATE TABLE t(id INT PRIMARY KEY, s TEXT);
INSERT INTO t VALUES(1, printf('%.*c', 65536, 'a'));
SELECT id, length(s), substr(s, 1, 4), substr(s, 65534, 3) FROM t;
"

oracle "typeof_large_text_and_blob" "
CREATE TABLE t(id INT PRIMARY KEY, s TEXT, b BLOB);
INSERT INTO t VALUES(1, printf('%.*c', 65536, 'a'), zeroblob(65536));
SELECT typeof(s), typeof(b) FROM t;
"

echo "--- savepoint with large ---"

oracle "rollback_large_insert" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1024));
SAVEPOINT s;
INSERT INTO t VALUES(2, zeroblob(65536));
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, length(b) FROM t ORDER BY id;
"

oracle "rollback_large_update" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1024));
SAVEPOINT s;
UPDATE t SET b = zeroblob(65536) WHERE id = 1;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, length(b) FROM t;
"

echo "--- bulk ---"

make_large_inserts() {
  local n="$1" size="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES($i, zeroblob($size));"
  done
}

oracle "bulk_100_rows_16kb" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
$(make_large_inserts 100 16384)
SELECT count(*), sum(length(b)) FROM t;
"

oracle "bulk_10_rows_100kb" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
$(make_large_inserts 10 102400)
SELECT count(*), sum(length(b)) FROM t;
"

echo "--- query paths ---"

oracle "where_on_small_col_with_large_blob" "
CREATE TABLE t(id INT PRIMARY KEY, tag TEXT, b BLOB);
INSERT INTO t VALUES(1, 'a', zeroblob(65536));
INSERT INTO t VALUES(2, 'b', zeroblob(65536));
INSERT INTO t VALUES(3, 'a', zeroblob(65536));
SELECT id, tag, length(b) FROM t WHERE tag = 'a' ORDER BY id;
"

oracle "join_two_tables_with_large_rows" "
CREATE TABLE a(id INT PRIMARY KEY, b BLOB);
CREATE TABLE c(id INT PRIMARY KEY, aid INT, b BLOB);
INSERT INTO a VALUES(1, zeroblob(65536));
INSERT INTO c VALUES(10, 1, zeroblob(65536));
INSERT INTO c VALUES(11, 1, zeroblob(65536));
SELECT a.id, length(a.b), c.id, length(c.b) FROM a JOIN c ON a.id = c.aid ORDER BY c.id;
"


echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
