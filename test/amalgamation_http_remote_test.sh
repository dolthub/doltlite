#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cd "$build_dir"

cc_bin="${CC:-cc}"
remotesrv="${REMOTESRV:-./doltlite-remotesrv}"
if [ ! -f ./sqlite3.c ] || [ ! -f ./sqlite3.h ]; then
  echo "SKIP: sqlite3.c/sqlite3.h not found in $PWD"
  exit 0
fi
if [ ! -x "$remotesrv" ]; then
  echo "SKIP: doltlite-remotesrv not found at $remotesrv"
  exit 0
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-http.XXXXXX")"
srv_pid=""
cleanup() {
  [ -n "$srv_pid" ] && kill "$srv_pid" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

probe_libs=(-lz -lpthread -lm)
case "$(uname -s)" in
  Linux*) probe_libs+=(-ldl) ;;
  MINGW*|MSYS*|CYGWIN*) probe_libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac

probe_c="$tmp/amalg_http_probe.c"
cat >"$probe_c" <<'EOF'
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>

extern int doltliteInstallAutoExt(void);

static int exec_sql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "%s\n", zErr ? zErr : sqlite3_errmsg(db));
    sqlite3_free(zErr);
    return 1;
  }
  return 0;
}

static int scalar_int(sqlite3 *db, const char *zSql, int *pVal){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "%s\n", sqlite3_errmsg(db));
    return 1;
  }
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    fprintf(stderr, "expected row, got rc=%d\n", rc);
    sqlite3_finalize(pStmt);
    return 1;
  }
  *pVal = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  return 0;
}

int main(int argc, char **argv){
  sqlite3 *db = 0;
  int n = 0;
  char *zSql;
  const char *zSrc;
  const char *zClone;
  const char *zUrl;

  if( argc!=4 ){
    fprintf(stderr, "usage: %s SRC_DB CLONE_DB HTTP_URL\n", argv[0]);
    return 1;
  }
  zSrc = argv[1];
  zClone = argv[2];
  zUrl = argv[3];

  if( doltliteInstallAutoExt()!=SQLITE_OK ){
    fprintf(stderr, "doltliteInstallAutoExt failed\n");
    return 1;
  }

  if( sqlite3_open(zSrc, &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  if( exec_sql(db,
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);"
        "INSERT INTO users VALUES(1,'alice'),(2,'bob'),(3,'charlie');"
        "SELECT dolt_add('-A');"
        "SELECT dolt_commit('-m','initial');") ){
    sqlite3_close(db);
    return 1;
  }
  zSql = sqlite3_mprintf(
      "SELECT dolt_remote('add','origin',%Q);"
      "SELECT dolt_push('origin','main');", zUrl);
  if( !zSql || exec_sql(db, zSql) ){
    sqlite3_free(zSql);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_free(zSql);
  sqlite3_close(db);

  if( sqlite3_open(zClone, &db)!=SQLITE_OK ){
    fprintf(stderr, "%s\n", db ? sqlite3_errmsg(db) : "sqlite3_open failed");
    return 1;
  }
  zSql = sqlite3_mprintf("SELECT dolt_clone(%Q);", zUrl);
  if( !zSql || exec_sql(db, zSql) ){
    sqlite3_free(zSql);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_free(zSql);
  if( scalar_int(db, "SELECT count(*) FROM users;", &n) ){
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);

  if( n!=3 ){
    fprintf(stderr, "expected 3 cloned users, got %d\n", n);
    return 1;
  }
  return 0;
}
EOF

"$cc_bin" -w -I. "$probe_c" ./sqlite3.c "${probe_libs[@]}" -o "$tmp/amalg_http_probe"

mkdir -p "$tmp/srv"
"$remotesrv" -p 0 --bind 127.0.0.1 "$tmp/srv" >"$tmp/srv.log" 2>&1 &
srv_pid=$!

port=""
for _ in $(seq 1 50); do
  port="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$tmp/srv.log" | head -1)"
  [ -n "$port" ] && break
  sleep 0.1
done
if [ -z "$port" ]; then
  echo "FAIL: server did not start"
  cat "$tmp/srv.log"
  exit 1
fi

"$tmp/amalg_http_probe" "$tmp/src.db" "$tmp/clone.db" "http://127.0.0.1:$port/repo.db"
echo "amalgamation plaintext HTTP remote: PASS"
