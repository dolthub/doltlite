#!/usr/bin/env bash
#
# Assert that a binary is a usable stock-SQLite reference, meaning it does not
# share doltlite's storage.
#
# Every oracle suite, the differential sweep and the performance baselines
# compare doltlite against a binary built from this tree with the prolly storage
# compiled out. If that binary is built with the storage in, the comparison is
# the engine against itself: it agrees with doltlite by construction and passes
# no matter what breaks. Nothing about the build says out loud which one you
# got, so this asserts it, and CI runs it at the point the reference is created
# rather than trusting the recipe.
#
# The test is what the binary writes, not what it links. The dolt_* SQL
# functions can be compiled in while the storage is stock, and those functions
# are harmless to a comparison; sharing the storage is not.
#
# Usage: assert_stock_reference.sh <binary>

set -uo pipefail

REF="${1:?Usage: assert_stock_reference.sh <binary>}"

if [ ! -x "$REF" ]; then
  echo "ERROR: not executable: $REF"
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if ! "$REF" "$WORK/ref.db" "CREATE TABLE x(y); INSERT INTO x VALUES(1);" \
     >/dev/null 2>&1; then
  echo "ERROR: $REF could not create a database, so it cannot be the reference"
  exit 1
fi

if [ ! -s "$WORK/ref.db" ]; then
  echo "ERROR: $REF wrote no database file, so it cannot be the reference"
  exit 1
fi

header="$(head -c 15 "$WORK/ref.db" 2>/dev/null)"
if [ "$header" != "SQLite format 3" ]; then
  echo "ERROR: $REF does not write an SQLite database header."
  echo "       It shares doltlite's storage, so comparing doltlite against it"
  echo "       compares the engine with itself and cannot fail."
  echo "       Build it with DOLTLITE_PROLLY=0, and make sure the build"
  echo "       actually relinks rather than reusing an existing binary."
  echo "       header bytes: $(head -c 15 "$WORK/ref.db" | od -An -c | tr -s ' ')"
  exit 1
fi

echo "OK: $REF is a stock reference (writes an SQLite database header)"
