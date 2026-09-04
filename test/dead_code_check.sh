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
#   Part C: static inline helpers in headers that never appear outside their
#           definition (the compiler will not warn; each TU that includes the
#           header just omits the unused inline).
#   Part D: non-static functions with no identifier occurrence outside the
#           defining file. Those should be static so Part A can see them.
#
# Needs the generated headers, so run it after a configure+build (the build
# dir defaults to ./build, override with DOLTLITE_BUILD_DIR). Point
# DOLTLITE_SRC_ROOT at an alternate src/ tree for the self-test.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$ROOT/build}"
SRC_ROOT="${DOLTLITE_SRC_ROOT:-$ROOT/src}"
CC="${CC:-cc}"

SRCS=(
  "$SRC_ROOT"/doltlite.c
  "$SRC_ROOT"/doltlite_*.c
  "$SRC_ROOT"/chunk_*.c
  "$SRC_ROOT"/prolly_*.c
  "$SRC_ROOT"/remotesrv_main.c
  "$SRC_ROOT"/pager_shim.c
  "$SRC_ROOT"/sortkey.c
)

CFLAGS=(
  -DNDEBUG -O1 -g
  -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_THREADSAFE=1
  -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION='"dev"'
  -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE
  -D_HAVE_SQLITE_CONFIG_H -DBUILD_sqlite
  "-I$BUILD_DIR" "-I$SRC_ROOT" -Iext/rtree -Iext/icu -Iext/fts3 -Iext/session
  -Iext/misc -Iext/blake3 -Iext/ed25519 -Iext/mbedtls/include
)

# Intentional exports that have no in-tree caller (documented/embeddable API,
# or build-glue entry points reached from outside this tree).
ALLOW="doltliteServe doltliteServeAsync doltliteServerStop"

fail=0
ERR=$(mktemp)
FNS=$(mktemp)
DEAD=$(mktemp)
trap 'rm -f "$ERR" "$FNS" "$DEAD"' EXIT

echo "== Part A: unused static functions / locals =="
if [ "${DEAD_CODE_SKIP_PART_A:-}" = 1 ]; then
  echo "  (skipped)"
else
  for f in "${SRCS[@]}"; do
    [ -f "$f" ] || continue
    # A real compile (not -fsyntax-only): GCC only runs its unused-function
    # pass during code generation, so -fsyntax-only silently suppresses the
    # warning under GCC (the CI compiler) while Clang still emits it. Compiling
    # to /dev/null keeps the check compiler-portable.
    if ! "$CC" "${CFLAGS[@]}" \
          -Werror=unused-function -Werror=unused-variable \
          -Werror=unused-but-set-variable -c -o /dev/null "$f" 2>"$ERR"; then
      grep -iE 'error:' "$ERR" | sed "s|^|  |"
      fail=1
    fi
  done
fi

echo "== Part B: extern functions with no caller =="
for f in "${SRCS[@]}"; do
  [ -f "$f" ] || continue
  grep -nE '^[a-zA-Z_][a-zA-Z0-9_ ]*[ \*]([a-z][a-zA-Z0-9_]+)\(' "$f" \
    | grep -vE '^[0-9]+:(static|typedef|return|else|case|do|while|for|if|switch)\b' \
    | sed -nE 's/^[0-9]+:[a-zA-Z_][a-zA-Z0-9_ ]*[ \*]([a-z][a-zA-Z0-9_]+)\(.*/\1/p'
done | sort -u > "$FNS"
: > "$DEAD"
while read -r fn; do
  [ -z "$fn" ] && continue
  case " $ALLOW " in *" $fn "*) continue;; esac
  cnt=$(grep -rhoE "\b$fn\b" "$SRC_ROOT"/*.c "$SRC_ROOT"/*.h \
          ext/*/*.c ext/*/*.h test/*.c 2>/dev/null | wc -l | tr -d ' ')
  [ "$cnt" -le 2 ] && echo "  dead (no caller): $fn" >> "$DEAD"
done < "$FNS"
if [ -s "$DEAD" ]; then cat "$DEAD"; fail=1; fi

echo "== Part C/D: unused header inlines / should-be-static =="
if ! python3 "$SCRIPT_DIR/lib/dead_code_scan.py" --root "$ROOT" --src-root "$SRC_ROOT"
then
  fail=1
fi

if [ "$fail" != 0 ]; then
  echo "DEAD CODE GATE: FAIL"
  exit 1
fi
echo "DEAD CODE GATE: PASS"
