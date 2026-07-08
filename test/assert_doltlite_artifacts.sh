#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cd "$build_dir"

cc_bin="${CC:-cc}"
want="prolly"
probe_cflags=(-w)
probe_libs=(-lz -lpthread -lm)
case "$(uname -s)" in
  Linux*) probe_libs+=(-ldl) ;;
  MINGW*|MSYS*|CYGWIN*) probe_libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac

check_value() {
  label="$1"
  got="$2"
  if [ "$got" != "$want" ]; then
    echo "$label: expected doltlite_engine() => $want, got: $got" >&2
    exit 1
  fi
  echo "$label: doltlite_engine() => $got"
}

probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-artifact-probe.XXXXXX")"
probe_c="$probe_dir/probe.c"
trap 'rm -rf "$probe_dir"' EXIT

cat >"$probe_c" <<'EOF'
#include "sqlite3.h"
#include <stdio.h>

int main(void){
  sqlite3 *db = 0;
  sqlite3_stmt *st = 0;
  int rc = sqlite3_open(":memory:", &db);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "sqlite3_open: %s\n", db ? sqlite3_errmsg(db) : "no db");
    return 1;
  }
  rc = sqlite3_prepare_v2(db, "SELECT doltlite_engine()", -1, &st, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "prepare: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 2;
  }
  rc = sqlite3_step(st);
  if( rc!=SQLITE_ROW ){
    fprintf(stderr, "step: rc=%d\n", rc);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 3;
  }
  printf("%s\n", sqlite3_column_text(st, 0));
  sqlite3_finalize(st);
  sqlite3_close(db);
  return 0;
}
EOF

if [ "${DOLTLITE_CHECK_CLI:-1}" != 0 ]; then
  if [ -x ./doltlite ]; then
    check_value "doltlite CLI" "$(./doltlite :memory: 'SELECT doltlite_engine();')"
  elif [ -x ./doltlite.exe ]; then
    check_value "doltlite CLI" "$(./doltlite.exe :memory: 'SELECT doltlite_engine();')"
  else
    echo "doltlite CLI: skipped, binary not found" >&2
  fi
fi

if [ "${DOLTLITE_CHECK_STATIC:-1}" != 0 ] && [ -f ./libdoltlite.a ]; then
  "$cc_bin" "${probe_cflags[@]}" -I. "$probe_c" ./libdoltlite.a "${probe_libs[@]}" -o "$probe_dir/static"
  check_value "libdoltlite.a" "$("$probe_dir/static")"
fi

if [ "${DOLTLITE_CHECK_SHARED:-1}" != 0 ]; then
  if [ -f ./libdoltlite.so ] || [ -f ./libdoltlite.dylib ]; then
    if [ "$(uname -s)" = "Linux" ] && [ ! -e ./libdoltlite.so.0 ]; then
      echo "libdoltlite shared: missing libdoltlite.so.0 for Linux SONAME" >&2
      exit 1
    fi
    "$cc_bin" "${probe_cflags[@]}" -I. "$probe_c" -L. -ldoltlite "${probe_libs[@]}" -o "$probe_dir/shared"
    case "$(uname -s)" in
      Darwin) got="$(DYLD_LIBRARY_PATH="$PWD${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" "$probe_dir/shared")" ;;
      *) got="$(LD_LIBRARY_PATH="$PWD${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$probe_dir/shared")" ;;
    esac
    check_value "libdoltlite shared" "$got"
  fi
fi

if [ "${DOLTLITE_CHECK_AMALGAMATION:-1}" != 0 ] && [ -f ./sqlite3.c ]; then
  if ! grep -q 'Begin file prolly_btree.c' sqlite3.c; then
    echo "sqlite3.c: missing prolly_btree.c marker; this looks like stock SQLite" >&2
    exit 1
  fi
  "$cc_bin" "${probe_cflags[@]}" -I. "$probe_c" ./sqlite3.c "${probe_libs[@]}" -o "$probe_dir/amalgamation"
  check_value "sqlite3.c amalgamation" "$("$probe_dir/amalgamation")"
fi
