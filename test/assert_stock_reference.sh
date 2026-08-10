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
# A reference at a different SQLite version is also unusable, for the opposite
# reason: it disagrees with doltlite about things that are not bugs. Text
# rendering of large reals changed in 3.44, and error messages get reworded
# between releases, so a mismatched version reports differences that are only
# version skew. Pass the engine as the second argument to require the versions
# match, which is what a comparison against this tree's own build should mean.
#
# Usage: assert_stock_reference.sh <reference> [engine-binary|version]

set -uo pipefail

REF="${1:?Usage: assert_stock_reference.sh <reference> [engine|version]}"
WANT="${2-}"

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

refver="$("$REF" :memory: "SELECT sqlite_version();" 2>/dev/null)"
if [ -z "$refver" ]; then
  echo "ERROR: $REF did not report a SQLite version"
  exit 1
fi

if [ -n "$WANT" ]; then
  if [ -x "$WANT" ]; then
    wantver="$("$WANT" :memory: "SELECT sqlite_version();" 2>/dev/null)"
  elif case "$WANT" in */*) true ;; *) false ;; esac; then
    # Looks like a path but is not runnable: say so rather than compare the
    # path text against a version and report a confusing mismatch.
    echo "ERROR: expected engine binary '$WANT' is not executable"
    exit 1
  else
    wantver="$WANT"
  fi
  if [ -z "$wantver" ]; then
    echo "ERROR: could not determine the expected SQLite version from '$WANT'"
    exit 1
  fi
  if [ "$refver" != "$wantver" ]; then
    echo "ERROR: $REF is SQLite $refver but the engine is $wantver."
    echo "       A reference at another version disagrees about things that are"
    echo "       not bugs -- large-real rendering changed in 3.44, and error"
    echo "       wording moves between releases -- so it reports version skew"
    echo "       as divergence. Build the reference from this tree."
    exit 1
  fi
fi

echo "OK: $REF is a stock reference (SQLite $refver, writes an SQLite header)"
