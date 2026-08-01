#!/bin/bash
# Growth-axis benchmark gates: how per-operation cost scales with history
# size (multi-writer refresh, open latency, commit depth). Gates are growth
# RATIOS, not absolute times, so shared-runner load does not flake them.
# See test/growth_axis_driver.c for the axes and thresholds.
#
# Usage: growth_axis_test.sh [build-dir] [--quick]
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${1:-$REPO_ROOT/build}"

SEED=200000
ROUNDS=40
DEPTH=1000
if [ "${2:-}" = "--quick" ]; then
  SEED=20000
  ROUNDS=25
  DEPTH=250
fi

if [ ! -f "$BUILD_DIR/libdoltlite.a" ]; then
  echo "ERROR: $BUILD_DIR/libdoltlite.a not found (make libdoltlite.a first)"
  exit 2
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

link_libs=(-lz -lpthread -lm)
case "$(uname -s 2>/dev/null || echo unknown)" in
  MINGW*|MSYS*|CYGWIN*) link_libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac

"${CC:-cc}" ${CFLAGS:-"-O2"} -I"$BUILD_DIR" -I"$REPO_ROOT/src" \
  -o "$TMP/growth_axis_driver" "$SCRIPT_DIR/growth_axis_driver.c" \
  "$BUILD_DIR/libdoltlite.a" ${LDFLAGS:-} "${link_libs[@]}" || exit 2

"$TMP/growth_axis_driver" "$TMP" "$SEED" "$ROUNDS" "$DEPTH"
