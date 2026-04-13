#!/bin/bash
#
# Oracle test: FTS5 virtual table against stock SQLite.
#
# FTS5's write path keeps a long-lived sqlite3_blob read cursor alive
# across inserts and re-seeks it via sqlite3_blob_reopen after new
# segment rows have been buffered through a separate write cursor.
# That exercises a cross-cursor visibility path in the prolly btree
# that is NOT touched by plain SELECT queries. The canonical failure
# mode is reached at exactly 16 inserts — FTS5's crisis-merge
# threshold — where automerge reads back all buffered segments and
# the blob read cursor's cached view misses the most recent row.
#
# Bug report: repro/doltlite-fts5-corruption-noshell.sql
#
# Usage: bash oracle_fts5_test.sh [path/to/doltlite] [path/to/sqlite3]
#

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Runs identical SQL against both engines in one invocation each,
# compares normalized stdout. Oracle catches any FTS5 shadow-table
# divergence that surfaces as either a query error or a row-count
# mismatch.
oracle() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/sq"

  local dl_out
  dl_out=$(printf '%s\n' "$sql" | "$DOLTLITE" "$dir/dl/db" 2>&1 | tr -d '\r')
  local sq_out
  sq_out=$(printf '%s\n' "$sql" | "$SQLITE3" "$dir/sq/db" 2>&1 | tr -d '\r')

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

make_inserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO chunks(rowid, content) VALUES($i, 'x');"
  done
}

echo "=== Oracle Tests: FTS5 virtual table ==="
echo ""

echo "--- FTS5 crisis-merge threshold ---"

# The exact shape from the original bug report: 16 inserts of a
# single-character document, then a count. 16 is the default FTS5
# auto-merge trigger. doltlite 0.6.1-49 and earlier returned
# "fts5: corruption found reading blob ... from table 'chunks'"
# because the cross-cursor flush in the prolly btree was missing
# in the integer-PK seek path.
SETUP="CREATE VIRTUAL TABLE chunks USING fts5(content, tokenize='unicode61');"

oracle "fts5_16_inserts_count" "$SETUP
$(make_inserts 16)
SELECT count(*) FROM chunks;"

oracle "fts5_16_inserts_match" "$SETUP
$(make_inserts 16)
SELECT count(*) FROM chunks WHERE chunks MATCH 'x';"

# Past the threshold — exercises the merged segment plus a fresh
# level-0 segment round. If the cross-cursor drain is only right
# for the first merge, this shape catches it.
oracle "fts5_32_inserts_count" "$SETUP
$(make_inserts 32)
SELECT count(*) FROM chunks;"

oracle "fts5_64_inserts_count" "$SETUP
$(make_inserts 64)
SELECT count(*) FROM chunks;"

echo "--- FTS5 mixed scenarios ---"

# Inserts interleaved with a MATCH query that forces FTS5 to read
# back its segments mid-transaction.
oracle "fts5_insert_match_interleave" "$SETUP
$(make_inserts 15)
SELECT count(*) FROM chunks WHERE chunks MATCH 'x';
$(make_inserts 15)
SELECT count(*) FROM chunks WHERE chunks MATCH 'x';"

# Rebuild scenario — forces segment compaction of existing rows.
oracle "fts5_rebuild_after_16" "$SETUP
$(make_inserts 16)
INSERT INTO chunks(chunks) VALUES('rebuild');
SELECT count(*) FROM chunks;"

# Delete a row after crisis merge — exercises tombstones.
oracle "fts5_delete_after_16" "$SETUP
$(make_inserts 16)
DELETE FROM chunks WHERE rowid=8;
SELECT count(*) FROM chunks;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
