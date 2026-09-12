#!/usr/bin/env bash
# Stock sqlite3 must not satisfy DoltLite suites or sql-differential.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STOCK="${2:-${SQLITE3:-$SCRIPT_DIR/../build-stockref/sqlite3}}"
ENG="${1:-${DOLTLITE:-$SCRIPT_DIR/../build/doltlite}}"

if [ ! -x "$STOCK" ]; then
  echo "ERROR: stock reference not executable: $STOCK"
  exit 1
fi

echo "=== engine floor ==="

if bash "$SCRIPT_DIR/lib/assert_doltlite_engine.sh" "$STOCK" >/tmp/floor-stock.out 2>&1; then
  echo "FAIL: assert_doltlite_engine.sh accepted stock sqlite3"
  cat /tmp/floor-stock.out
  exit 1
fi
echo "PASS: stock sqlite3 rejected as engine"

if [ -x "$ENG" ]; then
  if ! bash "$SCRIPT_DIR/lib/assert_doltlite_engine.sh" "$ENG"; then
    echo "FAIL: assert_doltlite_engine.sh rejected $ENG"
    exit 1
  fi
  echo "PASS: $ENG accepted as engine"
fi

if DOLTLITE="$STOCK" bash -c '. "'"$SCRIPT_DIR"'/lib/doltlite_test_common.sh"' \
     >/tmp/floor-common.out 2>&1; then
  echo "FAIL: common.sh accepted stock sqlite3"
  cat /tmp/floor-common.out
  exit 1
fi
echo "PASS: common.sh rejects stock sqlite3"

if bash "$SCRIPT_DIR/sql_differential_test.sh" "$STOCK" "$STOCK" 1 1 \
     >/tmp/floor-diff.out 2>&1; then
  echo "FAIL: sql-differential stock vs stock passed"
  cat /tmp/floor-diff.out
  exit 1
fi
if ! grep -q "SQLite database header\|same file" /tmp/floor-diff.out; then
  echo "FAIL: sql-differential stock vs stock failed for the wrong reason"
  cat /tmp/floor-diff.out
  exit 1
fi
echo "PASS: sql-differential stock vs stock rejected"

echo "engine floor: PASS"
