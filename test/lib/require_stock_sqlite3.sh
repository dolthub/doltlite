#!/usr/bin/env bash
# SQLITE3 here creates or is compared as a stock file. Fail if it is DoltLite
# storage, which would make the comparison a self-check.
require_stock_sqlite3() {
  local ref="$1"
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  if [ ! -x "$ref" ]; then
    return 1
  fi
  bash "$here/assert_stock_reference.sh" "$ref"
}
