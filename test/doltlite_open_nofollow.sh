#!/bin/bash
DOLTLITE_SRC="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${DOLTLITE_BUILD_DIR:-$DOLTLITE_SRC/build}"

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
  if( rc!=SQLITE_OK ){ fprintf(stderr, "open real failed %d\n", rc); return 1; }
  sqlite3_exec(db, "CREATE TABLE t(x);", 0, 0, 0);
  sqlite3_close(db);

  if( symlink(realp, linkp)!=0 ){ perror("symlink"); return 1; }

  rc = sqlite3_open_v2(linkp, &db,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOFOLLOW, 0);
  if( rc==SQLITE_OK ){
    fprintf(stderr, "FAIL: NOFOLLOW open of symlink succeeded\n");
    sqlite3_close(db);
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
    return 1;
  }
  sqlite3_close(db);
  printf("OK: NOFOLLOW refuses symlink; follow succeeds\n");
  return 0;
}
EOF

cc -O2 -I"$BUILD" -I"$DOLTLITE_SRC/src" -o "$TMP/nofollow" "$TMP/nofollow.c" \
  "$BUILD/libdoltlite.a" -lpthread -lz -lm || {
  echo "SKIP: could not link nofollow probe"
  exit 0
}

DL_TMP="$TMP" "$TMP/nofollow"
