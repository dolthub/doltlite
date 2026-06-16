#!/bin/bash

set -euo pipefail

DOLTLITE="${1:-./doltlite}"
DBFILE=$(mktemp)
TMPROOT=$(mktemp -d)
trap 'rm -f "$DBFILE"; rm -rf "$TMPROOT"' EXIT

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCANNER="$THIS_DIR/tools/chunk_physical_dups.py"

if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found at $DOLTLITE"
  exit 1
fi
if [ ! -f "$SCANNER" ]; then
  echo "scanner not found at $SCANNER"
  exit 1
fi

pass=0
fail=0
failed=""

scan_value() {
  local key="$1"
  python3 "$SCANNER" "$DBFILE" --json \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['$key'])"
}

check_eq() {
  local name="$1"
  local got="$2"
  local want="$3"
  if [ "$got" = "$want" ]; then
    pass=$((pass + 1))
    echo "  PASS: $name ($got)"
  else
    fail=$((fail + 1))
    failed="$failed $name"
    echo "  FAIL: $name"
    echo "    got=$got"
    echo "    want=$want"
  fi
}

check_gt() {
  local name="$1"
  local got="$2"
  local min="$3"
  if [ "$got" -gt "$min" ]; then
    pass=$((pass + 1))
    echo "  PASS: $name ($got)"
  else
    fail=$((fail + 1))
    failed="$failed $name"
    echo "  FAIL: $name"
    echo "    got=$got"
    echo "    expected > $min"
  fi
}

echo "=== Physical Chunk Deduplication Test ==="
echo

rm -f "$DBFILE"
"$DOLTLITE" "$DBFILE" >/dev/null <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, pad TEXT);
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < 1500
)
INSERT INTO t
SELECT x, printf('v%04d', x), hex(zeroblob(80)) FROM seq;
SELECT dolt_commit('-Am', 'base');

SELECT dolt_branch('left');
SELECT dolt_branch('right');

SELECT dolt_checkout('left');
UPDATE t SET v = 'left' WHERE id = 10;
SELECT dolt_commit('-Am', 'left edit');
UPDATE t SET v = printf('v%04d', id) WHERE id = 10;
SELECT dolt_commit('-Am', 'left restores base bytes');

SELECT dolt_checkout('right');
UPDATE t SET v = 'right' WHERE id = 1490;
SELECT dolt_commit('-Am', 'right edit');
UPDATE t SET v = printf('v%04d', id) WHERE id = 1490;
SELECT dolt_commit('-Am', 'right restores base bytes');

SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');

CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, pad TEXT);
INSERT INTO u SELECT * FROM t;
SELECT dolt_commit('-Am', 'transient duplicate-looking table');
DROP TABLE u;
SELECT dolt_commit('-Am', 'remove transient table');
SQL

before_records=$(scan_value physical_records)
before_distinct=$(scan_value distinct_hashes)
before_dups=$(scan_value duplicate_hashes)
before_dup_records=$(scan_value duplicate_records)

check_gt "pre_gc_physical_records" "$before_records" 0
check_eq "pre_gc_records_match_distinct_hashes" "$before_records" "$before_distinct"
check_eq "pre_gc_duplicate_hashes" "$before_dups" 0
check_eq "pre_gc_duplicate_records" "$before_dup_records" 0

"$DOLTLITE" "$DBFILE" "SELECT dolt_gc();" >"$TMPROOT/gc.out"

after_records=$(scan_value physical_records)
after_distinct=$(scan_value distinct_hashes)
after_dups=$(scan_value duplicate_hashes)
after_dup_records=$(scan_value duplicate_records)

check_gt "post_gc_physical_records" "$after_records" 0
check_eq "post_gc_records_match_distinct_hashes" "$after_records" "$after_distinct"
check_eq "post_gc_duplicate_hashes" "$after_dups" 0
check_eq "post_gc_duplicate_records" "$after_dup_records" 0

echo
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$failed"
  echo
  python3 "$SCANNER" "$DBFILE"
  exit 1
fi
