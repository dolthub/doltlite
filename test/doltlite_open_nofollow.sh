#!/bin/bash
DOLTLITE_SRC="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${DOLTLITE_BUILD_DIR:-$DOLTLITE_SRC/build}"
# shellcheck source=lib/build_artifacts.sh
. "$DOLTLITE_SRC/test/lib/build_artifacts.sh"

if [ ! -f "$BUILD/libdoltlite.a" ]; then
  echo "SKIP: no libdoltlite.a in $BUILD"
  exit 0
fi

case "$(uname -s)" in
  Darwin|Linux) ;;
  *) echo "SKIP: nofollow test is unix-only"; exit 0 ;;
esac

TMP=$(mktemp -d /tmp/dl_nofollow_XXXXXX)
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/nofollow.c" <<'EOF'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"

int main(void){
  sqlite3 *db = 0;
  int rc;
  char realp[512], linkp[512];
  const char *tmp = getenv("DL_TMP");
  if( !tmp || !tmp[0] ){ fprintf(stderr, "DL_TMP not set\n"); return 1; }
  snprintf(realp, sizeof(realp), "%s/real.db", tmp);
  snprintf(linkp, sizeof(linkp), "%s/link.db", tmp);
  unlink(realp); unlink(linkp);

  rc = sqlite3_open(realp, &db);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "open real failed %d\n", rc);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_exec(db, "CREATE TABLE t(x);", 0, 0, 0);
  sqlite3_close(db);
  db = 0;

  if( symlink(realp, linkp)!=0 ){ perror("symlink"); return 1; }

  rc = sqlite3_open_v2(linkp, &db,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOFOLLOW, 0);
  /* A refused open still returns a handle. */
  sqlite3_close(db);
  db = 0;
  if( rc==SQLITE_OK ){
    fprintf(stderr, "FAIL: NOFOLLOW open of symlink succeeded\n");
    return 1;
  }
  if( (rc & 0xff)!=SQLITE_CANTOPEN ){
    fprintf(stderr, "FAIL: expected CANTOPEN, got %d (%s)\n",
            rc, sqlite3_errstr(rc));
    return 1;
  }

  /* Without NOFOLLOW, following the symlink still works. */
  rc = sqlite3_open_v2(linkp, &db, SQLITE_OPEN_READWRITE, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "FAIL: follow-symlink open failed %d\n", rc);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);
  printf("OK: NOFOLLOW refuses symlink; follow succeeds\n");
  return 0;
}
EOF

# The sanitizer job ships an instrumented archive and does not export the
# build's CFLAGS. Link with those runtimes, or the probe never runs.
cc_bin="${CC:-cc}"
compile_flags=${CFLAGS:-"-O2"}
link_flags=${LDFLAGS:-}
sans=$(dl_archive_sanitizers "$BUILD/libdoltlite.a")
if [ -n "$sans" ]; then
  case "$compile_flags $link_flags" in
    *-fsanitize*) ;;
    *)
      compile_flags="$compile_flags -fsanitize=$sans"
      link_flags="$link_flags -fsanitize=$sans"
      ;;
  esac
fi

# shellcheck disable=SC2086
if ! "$cc_bin" $compile_flags -I"$BUILD" -I"$DOLTLITE_SRC/src" \
     -o "$TMP/nofollow" "$TMP/nofollow.c" \
     "$BUILD/libdoltlite.a" $link_flags -lpthread -lz -lm; then
  echo "FAIL: could not link nofollow probe"
  echo "__SUITE_COMPLETE__"
  exit 1
fi

if DL_TMP="$TMP" "$TMP/nofollow"; then
  echo "__SUITE_COMPLETE__"
else
  echo "FAIL: nofollow probe"
  echo "__SUITE_COMPLETE__"
  exit 1
fi
