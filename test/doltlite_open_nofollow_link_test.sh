#!/bin/bash
# The nofollow probe must link an instrumented archive, and a link failure
# with that archive present must fail the suite instead of skipping it.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
probe="$root/test/doltlite_open_nofollow.sh"
cc_bin="${CC:-cc}"

case "$(uname -s)" in
  Darwin|Linux) ;;
  *) echo "SKIP: nofollow link test is unix-only"; echo "__SUITE_COMPLETE__"; exit 0 ;;
esac

work=$(mktemp -d /tmp/dl_nofollow_link_XXXXXX)
trap 'rm -rf "$work"' EXIT

if ! "$cc_bin" -fsanitize=address,undefined -x c -c -o "$work/san_check.o" - <<'EOF' >/dev/null 2>&1
int sanitizer_probe(int a, int b){ return a / b; }
EOF
then
  echo "SKIP: compiler has no address/undefined sanitizer runtime"
  echo "__SUITE_COMPLETE__"
  exit 0
fi

cat > "$work/sqlite3.h" <<'EOF'
#define SQLITE_OK 0
#define SQLITE_CANTOPEN 14
#define SQLITE_OPEN_READWRITE 0x00000002
#define SQLITE_OPEN_NOFOLLOW 0x01000000
typedef struct sqlite3 sqlite3;
int sqlite3_open(const char*, sqlite3**);
int sqlite3_open_v2(const char*, sqlite3**, int, const char*);
int sqlite3_exec(sqlite3*, const char*,
                 int (*)(void*, int, char**, char**), void*, char**);
int sqlite3_close(sqlite3*);
const char *sqlite3_errstr(int);
EOF

cat > "$work/stub.c" <<'EOF'
#include "sqlite3.h"
#include <stdio.h>
#include <unistd.h>

int sqlite3_open(const char *filename, sqlite3 **ppDb){
  FILE *f = fopen(filename, "a");
  if( !f ) return SQLITE_CANTOPEN;
  fclose(f);
  *ppDb = (sqlite3*)1;
  return SQLITE_OK;
}

int sqlite3_exec(sqlite3 *db, const char *sql,
                 int (*cb)(void*, int, char**, char**), void *arg, char **err){
  (void)db; (void)sql; (void)cb; (void)arg; (void)err;
  return SQLITE_OK;
}

int sqlite3_close(sqlite3 *db){
  (void)db;
  return SQLITE_OK;
}

int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags,
                    const char *zVfs){
  (void)zVfs;
  if( flags & SQLITE_OPEN_NOFOLLOW ){
    char linkbuf[8];
    if( readlink(filename, linkbuf, sizeof(linkbuf))>=0 ) return SQLITE_CANTOPEN;
  }
  *ppDb = (sqlite3*)1;
  return SQLITE_OK;
}

const char *sqlite3_errstr(int rc){
  return rc ? "error" : "ok";
}

int ub_div(int a, int b){ return a / b; }
EOF

cat > "$work/missing.c" <<'EOF'
#include "sqlite3.h"
void doltlite_nofollow_missing_symbol(void);
int sqlite3_open(const char *filename, sqlite3 **ppDb){
  (void)filename; (void)ppDb;
  doltlite_nofollow_missing_symbol();
  return 0;
}
int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags,
                    const char *zVfs){
  (void)filename; (void)ppDb; (void)flags; (void)zVfs;
  return 0;
}
int sqlite3_exec(sqlite3 *db, const char *sql,
                 int (*cb)(void*, int, char**, char**), void *arg, char **err){
  (void)db; (void)sql; (void)cb; (void)arg; (void)err;
  return 0;
}
int sqlite3_close(sqlite3 *db){ (void)db; return 0; }
const char *sqlite3_errstr(int rc){ (void)rc; return ""; }
EOF

fail=0
check() {
  local name="$1" expect_rc="$2" pattern="$3" forbidden="$4"
  shift 4
  local out rc
  set +e
  out=$("$@" 2>&1)
  rc=$?
  set -e
  if [ "$rc" -eq "$expect_rc" ] && printf '%s\n' "$out" | grep -q "$pattern" \
     && { [ -z "$forbidden" ] || ! printf '%s\n' "$out" | grep -q "$forbidden"; }
  then
    echo "PASS: $name"
  else
    echo "FAIL: $name (rc=$rc, expected $expect_rc)"
    printf '%s\n' "$out" | tail -40
    fail=1
  fi
}

san="$work/san"
mkdir -p "$san"
cp "$work/sqlite3.h" "$san/sqlite3.h"
"$cc_bin" -fsanitize=address,undefined -I"$san" -c "$work/stub.c" -o "$san/stub.o"
ar rcs "$san/libdoltlite.a" "$san/stub.o"
check instrumented_archive_runs 0 "OK: NOFOLLOW refuses symlink" "SKIP:" \
  env -u CFLAGS -u LDFLAGS DOLTLITE_BUILD_DIR="$san" bash "$probe"

plain="$work/plain"
mkdir -p "$plain"
cp "$work/sqlite3.h" "$plain/sqlite3.h"
"$cc_bin" -I"$plain" -c "$work/stub.c" -o "$plain/stub.o"
ar rcs "$plain/libdoltlite.a" "$plain/stub.o"
check plain_archive_runs 0 "OK: NOFOLLOW refuses symlink" "SKIP:" \
  env -u CFLAGS -u LDFLAGS DOLTLITE_BUILD_DIR="$plain" bash "$probe"

bad="$work/bad"
mkdir -p "$bad"
cp "$work/sqlite3.h" "$bad/sqlite3.h"
"$cc_bin" -I"$bad" -c "$work/missing.c" -o "$bad/missing.o"
ar rcs "$bad/libdoltlite.a" "$bad/missing.o"
check link_failure_fails_suite 1 "FAIL: could not link nofollow probe" \
  "SKIP: could not link nofollow probe" \
  env -u CFLAGS -u LDFLAGS DOLTLITE_BUILD_DIR="$bad" bash "$probe"

empty="$work/empty"
mkdir -p "$empty"
check missing_archive_skips 0 "SKIP: no libdoltlite.a" "" \
  env -u CFLAGS -u LDFLAGS DOLTLITE_BUILD_DIR="$empty" bash "$probe"

echo "__SUITE_COMPLETE__"
exit "$fail"
