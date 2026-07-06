#!/bin/bash
#
# Dead-code gate for the doltlite-specific sources. Fails CI if a function is
# defined but never used:
#
#   Part A: unused static functions and unused locals, detected exactly by the
#           compiler (-Werror=unused-function / -unused-variable).
#   Part B: extern functions with no caller anywhere in the tree. This is a
#           heuristic (an occurrence count of <=2 means a definition plus at
#           most one declaration, i.e. no call site); intentional exports that
#           legitimately have no in-tree caller are listed in ALLOW below.
#
# Needs the generated headers, so run it after a configure+build (the build
# dir defaults to ./build, override with DOLTLITE_BUILD_DIR).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$ROOT/build}"
CC="${CC:-cc}"

SRCS=(src/doltlite.c src/doltlite_*.c src/chunk_*.c src/prolly_*.c src/remotesrv_main.c)

CFLAGS=(
  -DNDEBUG -O1 -g
  -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_THREADSAFE=1
  -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION='"dev"'
  -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE
  -D_HAVE_SQLITE_CONFIG_H -DBUILD_sqlite
  "-I$BUILD_DIR" -Isrc -Iext/rtree -Iext/icu -Iext/fts3 -Iext/session
  -Iext/misc -Iext/blake3
)

# Intentional exports that have no in-tree caller (documented/embeddable API,
# or build-glue entry points reached from outside this tree).
ALLOW="doltliteServeAsync doltliteServerStop"

fail=0

echo "== Part A: unused static functions / locals =="
for f in "${SRCS[@]}"; do
  # A real compile (not -fsyntax-only): GCC only runs its unused-function
  # pass during code generation, so -fsyntax-only silently suppresses the
  # warning under GCC (the CI compiler) while Clang still emits it. Compiling
  # to /dev/null keeps the check compiler-portable.
  if ! "$CC" "${CFLAGS[@]}" \
        -Werror=unused-function -Werror=unused-variable \
        -Werror=unused-but-set-variable -c -o /dev/null "$f" 2>/tmp/dc_err.$$; then
    grep -iE 'error:' /tmp/dc_err.$$ | sed "s|^|  |"
    fail=1
  fi
done
rm -f /tmp/dc_err.$$

echo "== Part B: extern functions with no caller =="
for f in "${SRCS[@]}"; do
  grep -nE '^[a-zA-Z_][a-zA-Z0-9_ ]*[ \*]([a-z][a-zA-Z0-9_]+)\(' "$f" \
    | grep -vE '^[0-9]+:(static|typedef|return|else|case|do|while|for|if|switch)\b' \
    | sed -nE 's/^[0-9]+:[a-zA-Z_][a-zA-Z0-9_ ]*[ \*]([a-z][a-zA-Z0-9_]+)\(.*/\1/p'
done | sort -u > /tmp/dc_fns.$$
: > /tmp/dc_dead.$$
while read -r fn; do
  [ -z "$fn" ] && continue
  case " $ALLOW " in *" $fn "*) continue;; esac
  cnt=$(grep -rhoE "\b$fn\b" src/*.c src/*.h ext/*/*.c ext/*/*.h test/*.c 2>/dev/null | wc -l | tr -d ' ')
  [ "$cnt" -le 2 ] && echo "  dead (no caller): $fn" >> /tmp/dc_dead.$$
done < /tmp/dc_fns.$$
if [ -s /tmp/dc_dead.$$ ]; then cat /tmp/dc_dead.$$; fail=1; fi
rm -f /tmp/dc_fns.$$ /tmp/dc_dead.$$

if [ "$fail" != 0 ]; then
  echo "DEAD CODE GATE: FAIL"
  exit 1
fi
echo "DEAD CODE GATE: PASS"
