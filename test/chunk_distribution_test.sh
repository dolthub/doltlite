#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DBFILE=$(mktemp)
trap "rm -f $DBFILE" EXIT
THIS_DIR=$(cd "$(dirname "$0")" && pwd)
ANALYZER="$THIS_DIR/tools/chunk_size_dist.py"

if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found at $DOLTLITE"
  exit 1
fi
if [ ! -f "$ANALYZER" ]; then
  echo "analyzer not found at $ANALYZER"
  exit 1
fi

echo "=== Chunk Distribution Test ==="
echo

echo "Materializing 50,000-row database..."
rm -f "$DBFILE"
"$DOLTLITE" "$DBFILE" >/dev/null <<EOF
CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v TEXT);
INSERT INTO t
  SELECT value, hex(randomblob(20)), hex(randomblob(60))
  FROM generate_series(1, 50000);
SELECT dolt_commit('-Am','50k rows');
EOF

DB_BYTES=$(wc -c < "$DBFILE" | tr -d ' ')
echo "  database size: $DB_BYTES bytes"
echo

STATS=$(python3 "$ANALYZER" "$DBFILE" --json)
echo "$STATS"
echo

fail=0
check() {
  local name="$1"
  local expr="$2"
  local got="$3"
  if eval "$expr"; then
    echo "  PASS  $name ($got)"
  else
    echo "  FAIL  $name ($got)"
    fail=$((fail+1))
  fi
}

mean=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_mean"])')
stdev=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_stdev"])')
median=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_median"])')
pmin=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_min"])')
pmax=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_max"])')
p10=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_p10"])')
p90=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_p90"])')
n=$(echo "$STATS" | python3 -c 'import sys,json;print(json.load(sys.stdin)["prolly_chunks"])')

echo "Assertions:"
check "enough chunks for stats"           "[ $n -ge 500 ]"                     "$n chunks"
check "mean in [3200, 4500]"              "python3 -c 'import sys; sys.exit(0 if 3200 <= $mean <= 4500 else 1)'" "mean=$mean"
check "stdev < 1500"                       "python3 -c 'import sys; sys.exit(0 if $stdev < 1500 else 1)'"        "stdev=$stdev"
check "max <= MAX clamp (16384)"          "[ $pmax -le 16384 ]"                "max=$pmax"
check "min >= MIN clamp (512)"            "[ $pmin -ge 512 ]"                  "min=$pmin"
check "|median - mean| < 600"             "python3 -c 'import sys; sys.exit(0 if abs($median - $mean) < 600 else 1)'" "median=$median, mean=$mean"
check "p90 - p10 < 4000 (tight middle)"   "python3 -c 'import sys; sys.exit(0 if ($p90 - $p10) < 4000 else 1)'" "p10=$p10, p90=$p90"

echo
if [ "$fail" -eq 0 ]; then
  echo "=== Results: all chunk distribution assertions passed ==="
  exit 0
else
  echo "=== Results: $fail assertion(s) failed ==="
  exit 1
fi
