#!/usr/bin/env bash
#
# Build a stock-SQLite reference in a directory of its own, and assert it.
#
# The amalgamation carries its own `#define DOLTLITE_PROLLY 1` when generated
# with the storage on -- tool/mksqlite3c.tcl bakes it in deliberately, so that
# anyone compiling sqlite3.c gets doltlite without passing the flag. That makes
# `make DOLTLITE_PROLLY=0 sqlite3` unreliable in a directory that has already
# generated the amalgamation for doltlite or testfixture: whether the file gets
# regenerated decides whether the reference is stock, and CI and a local build
# have disagreed about that. CI shipped a reference that shared doltlite's
# storage, which made every comparison against it pass by construction.
#
# A directory that has never built doltlite cannot get this wrong, so that is
# where the reference is built. It costs one amalgamation and one compile.
#
# Note this is only for the reference. A build directory's own `sqlite3` is
# expected to be doltlite-flavoured -- the inherited shell tests run it -- so
# this never touches one.
#
# Usage: build_stock_reference.sh <build-dir> [engine-binary]
#
# Leaves <build-dir>/sqlite3 and <build-dir>/sqlite3.o, the latter for callers
# that link a benchmark harness against stock instead of driving the shell.

set -euo pipefail

DIR="${1:?Usage: build_stock_reference.sh <build-dir> [engine-binary]}"
ENGINE="${2-}"

# Resolved before the cd below, or a relative path stops meaning what it did.
if [ -n "$ENGINE" ] && [ -e "$ENGINE" ]; then
  ENGINE="$(cd "$(dirname "$ENGINE")" && pwd)/$(basename "$ENGINE")"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOP="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -e "$DIR" ] && [ ! -d "$DIR" ]; then
  echo "ERROR: $DIR exists and is not a directory"
  exit 1
fi

# Always from scratch: a directory reused from an earlier run could be carrying
# an amalgamation generated with the storage on, which is the whole problem.
rm -rf "$DIR"
mkdir -p "$DIR"
cd "$DIR"

"$TOP/configure" >configure.log 2>&1 || {
  echo "ERROR: configure failed in $DIR"
  tail -20 configure.log
  exit 1
}

make DOLTLITE_PROLLY=0 sqlite3 sqlite3.o >build.log 2>&1 || {
  echo "ERROR: building the stock reference failed in $DIR"
  tail -30 build.log
  exit 1
}

bash "$SCRIPT_DIR/assert_stock_reference.sh" ./sqlite3 ${ENGINE:+"$ENGINE"}
