#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

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

SETUP="CREATE VIRTUAL TABLE chunks USING fts5(content, tokenize='unicode61');"

oracle "fts5_16_inserts_count" "$SETUP
$(make_inserts 16)
SELECT count(*) FROM chunks;"

oracle "fts5_16_inserts_match" "$SETUP
$(make_inserts 16)
SELECT count(*) FROM chunks WHERE chunks MATCH 'x';"

oracle "fts5_32_inserts_count" "$SETUP
$(make_inserts 32)
SELECT count(*) FROM chunks;"

oracle "fts5_64_inserts_count" "$SETUP
$(make_inserts 64)
SELECT count(*) FROM chunks;"

echo "--- FTS5 mixed scenarios ---"

oracle "fts5_insert_match_interleave" "$SETUP
$(make_inserts 15)
SELECT count(*) FROM chunks WHERE chunks MATCH 'x';
$(make_inserts 15)
SELECT count(*) FROM chunks WHERE chunks MATCH 'x';"

oracle "fts5_rebuild_after_16" "$SETUP
$(make_inserts 16)
INSERT INTO chunks(chunks) VALUES('rebuild');
SELECT count(*) FROM chunks;"

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
