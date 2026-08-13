#!/usr/bin/env bash
# End-to-end golden hashes for a fixed prolly tree built through SQL.
# Complements the C-level prolly_chunker_boundary_test vectors.
set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found at $DOLTLITE"
  exit 1
fi

DB=$(mktemp)
trap 'rm -f "$DB"' EXIT

pass=0
fail=0
check() {
  local name="$1" want="$2" got="$3"
  if [ "$want" = "$got" ]; then
    echo "  PASS: $name"
    pass=$((pass+1))
  else
    echo "  FAIL: $name"
    echo "    want: $want"
    echo "    got:  $got"
    fail=$((fail+1))
  fi
}

echo "=== Chunker boundary golden (SQL) ==="

rm -f "$DB"
out=$("$DOLTLITE" "$DB" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t SELECT value, printf('val-%d', value) FROM generate_series(1, 500);
SELECT dolt_commit('-Am','golden');
SELECT dolt_hashof_table('t');
SELECT dolt_hashof_db();
SELECT count(*) FROM t;
SQL
)

table_hash=$(echo "$out" | sed -n '2p' | tr -d '\r')
db_hash=$(echo "$out" | sed -n '3p' | tr -d '\r')
nrows=$(echo "$out" | sed -n '4p' | tr -d '\r')

# First line is the commit hash (content-addressed commit object; may include
# author/timestamp and is not pinned here). Table and DB content hashes are.
check "row_count" "500" "$nrows"
check "hashof_table" "fe688e6266b01f5643b9cb17d4a51f4c9283d422" "$table_hash"
check "hashof_db" "200d68603844d3d6b9bc4d5ec69da5bf6c12190a" "$db_hash"

# Rebuild the same logical content and re-check; content-addressed roots must
# match even across a fresh file.
DB2=$(mktemp)
trap 'rm -f "$DB" "$DB2"' EXIT
out2=$("$DOLTLITE" "$DB2" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t SELECT value, printf('val-%d', value) FROM generate_series(1, 500);
SELECT dolt_commit('-Am','golden');
SELECT dolt_hashof_table('t');
SELECT dolt_hashof_db();
SQL
)
table_hash2=$(echo "$out2" | sed -n '2p' | tr -d '\r')
db_hash2=$(echo "$out2" | sed -n '3p' | tr -d '\r')
check "hashof_table_repeatable" "$table_hash" "$table_hash2"
check "hashof_db_repeatable" "$db_hash" "$db_hash2"

HASHDIR=$(mktemp -d)
trap 'rm -rf "$DB" "$DB2" "$HASHDIR"' EXIT

hashof_table() {
  local db="$1" sql="$2"
  rm -f "$db"
  "$DOLTLITE" "$db" "$sql" | tail -n 1 | tr -d '\r'
}

echo "--- oversized singleton leaf vs one-shot chunk ---"
small_two=$(hashof_table "$HASHDIR/small_two.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
SELECT dolt_commit('-Am','a');
INSERT INTO t VALUES(2, 2);
SELECT dolt_commit('-Am','b');
SELECT dolt_hashof_table('t');
")
small_one=$(hashof_table "$HASHDIR/small_one.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1), (2, 2);
SELECT dolt_commit('-Am','both');
SELECT dolt_hashof_table('t');
")
check "small_int_two_commit_vs_one" "$small_one" "$small_two"

z8_two=$(hashof_table "$HASHDIR/z8_two.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO t VALUES(1, zeroblob(8192));
SELECT dolt_commit('-Am','fat');
INSERT INTO t VALUES(2, x'78');
SELECT dolt_commit('-Am','small');
SELECT dolt_hashof_table('t');
")
z8_one=$(hashof_table "$HASHDIR/z8_one.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO t VALUES(1, zeroblob(8192)), (2, x'78');
SELECT dolt_commit('-Am','both');
SELECT dolt_hashof_table('t');
")
z8_flush=$(hashof_table "$HASHDIR/z8_flush.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO t VALUES(1, zeroblob(8192));
SELECT count(*) FROM t;
INSERT INTO t VALUES(2, x'78');
SELECT dolt_commit('-Am','both');
SELECT dolt_hashof_table('t');
")
check "zeroblob_8192_two_commit_vs_one" "$z8_one" "$z8_two"
check "zeroblob_8192_count_flush_vs_one" "$z8_one" "$z8_flush"

z16_two=$(hashof_table "$HASHDIR/z16_two.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO t VALUES(1, zeroblob(16384));
SELECT dolt_commit('-Am','fat');
INSERT INTO t VALUES(2, x'78');
SELECT dolt_commit('-Am','small');
SELECT dolt_hashof_table('t');
")
z16_one=$(hashof_table "$HASHDIR/z16_one.db" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO t VALUES(1, zeroblob(16384)), (2, x'78');
SELECT dolt_commit('-Am','both');
SELECT dolt_hashof_table('t');
")
check "zeroblob_16384_two_commit_vs_one" "$z16_one" "$z16_two"

echo
if [ "$fail" -eq 0 ]; then
  echo "Results: $pass passed, 0 failed"
  exit 0
fi
echo "Results: $pass passed, $fail failed"
exit 1
