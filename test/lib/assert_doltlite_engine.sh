#!/usr/bin/env bash
#
# Assert that a binary is a DoltLite prolly engine: it writes a DoltLite
# image, not an SQLite header, and doltlite_engine() is prolly.
#
# Usage: assert_doltlite_engine.sh <engine>

set -uo pipefail

ENG="${1:?Usage: assert_doltlite_engine.sh <engine>}"

if [ ! -x "$ENG" ]; then
  echo "ERROR: not executable: $ENG"
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if ! "$ENG" "$WORK/eng.db" "CREATE TABLE x(y); INSERT INTO x VALUES(1);" \
     >/dev/null 2>&1; then
  echo "ERROR: $ENG could not create a database, so it cannot be the engine"
  exit 1
fi

if [ ! -s "$WORK/eng.db" ]; then
  echo "ERROR: $ENG wrote no database file, so it cannot be the engine"
  exit 1
fi

header="$(head -c 15 "$WORK/eng.db" 2>/dev/null)"
if [ "$header" = "SQLite format 3" ]; then
  echo "ERROR: $ENG writes an SQLite database header."
  echo "       Suites that only check SELECT count(*) / PRAGMA integrity_check"
  echo "       go green on stock sqlite3. Build the engine with DOLTLITE_PROLLY=1."
  echo "       header bytes: $(head -c 15 "$WORK/eng.db" | od -An -c | tr -s ' ')"
  exit 1
fi

got="$("$ENG" "$WORK/eng.db" "SELECT doltlite_engine();" 2>/dev/null | tail -1)"
if [ "$got" != "prolly" ]; then
  echo "ERROR: $ENG doltlite_engine() is '${got:-empty}', want prolly"
  exit 1
fi

echo "OK: $ENG is a DoltLite prolly engine"
