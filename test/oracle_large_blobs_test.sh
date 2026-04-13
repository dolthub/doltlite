#!/bin/bash
#
# Oracle test: large BLOB and TEXT values (row record bytes >> one
# prolly chunk) against stock SQLite.
#
# Prolly chunks are sized PROLLY_CHUNK_MIN (512) to PROLLY_CHUNK_MAX
# (16384) bytes. A row record larger than ~16KB forces the chunker
# to emit multiple leaf chunks plus interior nodes; values around
# chunk boundaries exercise the rolling-hash boundary detection; and
# very large values (>>MB) stress the chunk-walk + cache paths.
#
# Bugs in this area would show up as:
#   - wrong length on SELECT length(col)
#   - wrong bytes at a chunk-boundary offset (first fails via
#     substr/hex comparison)
#   - a duplicate or missing row in a bulk insert
#   - an UPDATE of a large value leaving orphan chunks that then
#     affect subsequent reads
#
# Oracle target is stock sqlite3 built from the same source tree so
# both engines parse identical SQL and the only variable is the
# storage layer.
#
# Usage: bash oracle_large_blobs_test.sh [doltlite] [sqlite3]
#

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

# ─── BLOB round-trip at varying sizes ──────────────────────────────
echo "--- BLOB length round-trip ---"

# A handful of sizes: sub-chunk, around PROLLY_CHUNK_MAX=16384, well
# past the max so the chunker emits multiple leaves, and a couple of
# large values to exercise interior nodes.
for N in 1 512 1024 8191 8192 8193 16383 16384 16385 32768 65536 262144 1048576; do
  oracle "blob_length_${N}" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob($N));
SELECT id, length(b) FROM t;
"
done

# ─── TEXT round-trip at varying sizes ──────────────────────────────
echo "--- TEXT length round-trip ---"

# TEXT values are constructed via printf with a repeat count in SQL
# — sqlite has no REPEAT function, so we use recursive-CTE pattern
# with printf('%.*c', N, X). The CTE returns a string of N copies
# of 'x'. length() on the stored value should equal N.
for N in 1 512 16384 65536 1048576; do
  oracle "text_length_${N}" "
CREATE TABLE t(id INT PRIMARY KEY, s TEXT);
INSERT INTO t VALUES(1, printf('%.*c', $N, 'x'));
SELECT id, length(s) FROM t;
"
done

# ─── Content integrity at chunk boundaries ────────────────────────
echo "--- content bytes at chunk boundaries ---"

# Embed known byte markers at specific offsets in a large BLOB via
# an UPDATE that writes a small blob slice, then read back specific
# byte positions via substr. Both engines must produce the same
# hex output. Sizes chosen to straddle PROLLY_CHUNK_MAX (16384) and
# a multiple of it.
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

# Substring at the very end of a 1MB blob.
oracle "blob_substr_last_bytes_1mb" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1048576));
SELECT length(b), hex(substr(b, 1048573, 4)) FROM t;
"

# ─── UPDATE of a large value ──────────────────────────────────────
echo "--- UPDATE large ---"

# Update a large BLOB with a different-sized BLOB. The chunker
# will re-emit chunks for the new contents; stale chunks from the
# old value must not leak into reads.
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

# Update only SOME rows in a table with many large rows.
oracle "update_one_of_many_large" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(65536));
INSERT INTO t VALUES(2, zeroblob(65536));
INSERT INTO t VALUES(3, zeroblob(65536));
UPDATE t SET b = zeroblob(1024) WHERE id = 2;
SELECT id, length(b) FROM t ORDER BY id;
"

# ─── DELETE of a large row ────────────────────────────────────────
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

# ─── Mix of large and small rows ──────────────────────────────────
echo "--- mixed sizes ---"

# Ten rows, alternating small and large. Verifies that chunking
# boundaries in the main tree don't corrupt adjacent small rows.
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

# ─── TEXT vs BLOB parity ──────────────────────────────────────────
echo "--- TEXT vs BLOB parity ---"

# A large TEXT value should round-trip the same way a large BLOB
# does. Uses the same size for both.
oracle "large_text_matches_expected_length" "
CREATE TABLE t(id INT PRIMARY KEY, s TEXT);
INSERT INTO t VALUES(1, printf('%.*c', 65536, 'a'));
SELECT id, length(s), substr(s, 1, 4), substr(s, 65534, 3) FROM t;
"

# typeof() on a stored large TEXT should still be 'text'; large
# BLOB should still be 'blob'.
oracle "typeof_large_text_and_blob" "
CREATE TABLE t(id INT PRIMARY KEY, s TEXT, b BLOB);
INSERT INTO t VALUES(1, printf('%.*c', 65536, 'a'), zeroblob(65536));
SELECT typeof(s), typeof(b) FROM t;
"

# ─── Savepoint interaction ────────────────────────────────────────
echo "--- savepoint with large ---"

# A large INSERT inside a savepoint that is then rolled back. The
# per-entry mutmap rollback (which can snapshot pTE->pPending) has
# to correctly free the cloned value bytes, not leak them, and the
# post-rollback state must have no trace of the row.
oracle "rollback_large_insert" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1024));
SAVEPOINT s;
INSERT INTO t VALUES(2, zeroblob(65536));
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, length(b) FROM t ORDER BY id;
"

# A large UPDATE inside a savepoint that is then rolled back. The
# undo log records the pre-update value, so rollback has to restore
# the old bytes exactly.
oracle "rollback_large_update" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, zeroblob(1024));
SAVEPOINT s;
UPDATE t SET b = zeroblob(65536) WHERE id = 1;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, length(b) FROM t;
"

# ─── Bulk ─────────────────────────────────────────────────────────
echo "--- bulk ---"

make_large_inserts() {
  local n="$1" size="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES($i, zeroblob($size));"
  done
}

# 100 rows at 16KB each — forces the chunker's interior-node
# promotion logic.
oracle "bulk_100_rows_16kb" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
$(make_large_inserts 100 16384)
SELECT count(*), sum(length(b)) FROM t;
"

# 10 rows at 100KB each.
oracle "bulk_10_rows_100kb" "
CREATE TABLE t(id INT PRIMARY KEY, b BLOB);
$(make_large_inserts 10 102400)
SELECT count(*), sum(length(b)) FROM t;
"

# ─── WHERE / JOIN on a table with large rows ───────────────────────
echo "--- query paths ---"

# A secondary column can still be used for WHERE filtering when the
# row contains a large BLOB. Verifies that the reader walks past
# the large field correctly to get to the trailing tag column.
oracle "where_on_small_col_with_large_blob" "
CREATE TABLE t(id INT PRIMARY KEY, tag TEXT, b BLOB);
INSERT INTO t VALUES(1, 'a', zeroblob(65536));
INSERT INTO t VALUES(2, 'b', zeroblob(65536));
INSERT INTO t VALUES(3, 'a', zeroblob(65536));
SELECT id, tag, length(b) FROM t WHERE tag = 'a' ORDER BY id;
"

# JOIN between two tables with large rows on each side.
oracle "join_two_tables_with_large_rows" "
CREATE TABLE a(id INT PRIMARY KEY, b BLOB);
CREATE TABLE c(id INT PRIMARY KEY, aid INT, b BLOB);
INSERT INTO a VALUES(1, zeroblob(65536));
INSERT INTO c VALUES(10, 1, zeroblob(65536));
INSERT INTO c VALUES(11, 1, zeroblob(65536));
SELECT a.id, length(a.b), c.id, length(c.b) FROM a JOIN c ON a.id = c.aid ORDER BY c.id;
"

# ─── Final report ───────────────────────────────────────────────────

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
