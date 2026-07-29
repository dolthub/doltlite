#!/usr/bin/env bash
# `make install` must place the files the README documents, and they must
# actually work: an embedder following "Using as a C Library" includes
# <doltlite.h>, and the in-process server section includes
# <doltlite_remotesrv.h>. Both were missing from a source install for a while
# because the header rule inherited from upstream installs sqlite3.h only,
# while the deb and Homebrew packages install the doltlite names. Staged via
# DESTDIR, so this needs no privileges and touches nothing outside TMPDIR.
set -uo pipefail

BUILD_DIR="${1:-build}"
CC="${CC:-cc}"

if [ ! -f "$BUILD_DIR/Makefile" ]; then
  echo "SKIP: no configured build in $BUILD_DIR"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-install-layout.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
have() { if [ -e "$2" ]; then ok "$1"; else bad "$1 (missing $2)"; fi; }

# USE_AMALGAMATION=0 because `install` pulls in install-lib -> libsqlite3.a,
# which by default recompiles the generated sqlite3.c. That amalgamation's
# "Begin file" banners sit inside block comments, so a caller with -Werror in
# CFLAGS (CI does) dies on -Wcomment -- unrelated to install layout, and the
# objects the non-amalgamated build needs are already present.
echo "=== staging make install ==="
if ! ( cd "$BUILD_DIR" && make install USE_AMALGAMATION=0 DESTDIR="$TMP/stage" ) \
     >"$TMP/install.log" 2>&1; then
  echo "FAIL: make install failed"
  tail -30 "$TMP/install.log"
  exit 1
fi

# Derive the prefix from the staged tree rather than assuming /usr/local, so a
# build configured with --prefix elsewhere still validates.
hdr="$(find "$TMP/stage" -name doltlite.h -print -quit 2>/dev/null)"
if [ -z "$hdr" ]; then
  echo "FAIL: doltlite.h was not installed anywhere under DESTDIR"
  find "$TMP/stage" -type f | sed 's/^/    /' | head -30
  exit 1
fi
P="$(cd "$(dirname "$hdr")/.." && pwd)"
# pwd resolves symlinks (macOS /var -> /private/var), so strip on the marker.
echo "prefix: $(printf '%s' "$P" | sed 's|.*/stage||')"

echo "=== 1. Documented files are present ==="
have "include/doltlite.h"            "$P/include/doltlite.h"
have "include/doltlite_remotesrv.h"  "$P/include/doltlite_remotesrv.h"
have "bin/doltlite"                  "$P/bin/doltlite"
have "bin/doltlite-remotesrv"        "$P/bin/doltlite-remotesrv"
have "lib/libdoltlite.a"             "$P/lib/libdoltlite.a"
shared=""
for cand in "$P/lib/libdoltlite.so" "$P/lib/libdoltlite.dylib"; do
  if [ -e "$cand" ]; then shared="$cand"; fi
done
if [ -n "$shared" ]; then ok "lib/libdoltlite.{so,dylib}"
else bad "lib/libdoltlite.{so,dylib} (missing)"; fi

echo "=== 2. Installed binaries run the prolly engine ==="
got=$("$P/bin/doltlite" :memory: 'SELECT doltlite_engine();' 2>&1)
if [ "$got" = "prolly" ]; then ok "installed doltlite reports prolly"
else bad "installed doltlite reports prolly (got: $got)"; fi
if "$P/bin/doltlite-remotesrv" -h 2>&1 | grep -q 'auth-keys'; then
  ok "installed doltlite-remotesrv runs"
else
  bad "installed doltlite-remotesrv runs"
fi

echo "=== 3. Both documented headers compile and link ==="
cat >"$TMP/probe.c" <<'EOF'
#include <doltlite.h>
#include <doltlite_remotesrv.h>
#include <stdio.h>
int main(void){
  sqlite3 *db = 0; sqlite3_stmt *st = 0;
  DoltliteServer *srv;
  if( sqlite3_open(":memory:", &db)!=SQLITE_OK ) return 1;
  if( sqlite3_prepare_v2(db, "SELECT doltlite_engine()", -1, &st, 0)!=SQLITE_OK ) return 2;
  if( sqlite3_step(st)!=SQLITE_ROW ) return 3;
  printf("%s\n", sqlite3_column_text(st, 0));
  sqlite3_finalize(st); sqlite3_close(db);
  srv = doltliteServeAsync(".", 0, "127.0.0.1");
  if( !srv ) return 4;
  doltliteServerStop(srv);
  return 0;
}
EOF
probe_libs=(-lz -lpthread -lm)
case "$(uname -s)" in
  Linux*) probe_libs+=(-ldl) ;;
  MINGW*|MSYS*|CYGWIN*) probe_libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac

if "$CC" -w -I"$P/include" "$TMP/probe.c" "$P/lib/libdoltlite.a" \
     "${probe_libs[@]}" -o "$TMP/static" 2>"$TMP/static.err"; then
  got=$("$TMP/static" 2>&1)
  if [ "$got" = "prolly" ]; then ok "static link via <doltlite.h>"
  else bad "static link via <doltlite.h> (ran, got: $got)"; fi
else
  bad "static link via <doltlite.h> (compile/link failed)"
  sed 's/^/    /' "$TMP/static.err" | head -10
fi

if [ -n "$shared" ]; then
  if "$CC" -w -I"$P/include" "$TMP/probe.c" -L"$P/lib" -ldoltlite \
       "${probe_libs[@]}" -o "$TMP/shared" 2>"$TMP/shared.err"; then
    case "$(uname -s)" in
      Darwin) got=$(DYLD_LIBRARY_PATH="$P/lib" "$TMP/shared" 2>&1) ;;
      *)      got=$(LD_LIBRARY_PATH="$P/lib" "$TMP/shared" 2>&1) ;;
    esac
    if [ "$got" = "prolly" ]; then ok "shared link via <doltlite.h>"
    else bad "shared link via <doltlite.h> (ran, got: $got)"; fi
  else
    bad "shared link via <doltlite.h> (compile/link failed)"
    sed 's/^/    /' "$TMP/shared.err" | head -10
  fi
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
