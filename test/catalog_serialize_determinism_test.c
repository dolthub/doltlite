/*
** Catalog serialization must be a pure function of the logical catalog:
** the same committed database must serialize to identical bytes no matter
** what session state (parsed schema, prior reads, checkouts, rolled-back
** DDL) the serializing connection carries. Context-dependent output writes
** differently-hashed working sets for identical content, which surfaces as
** sticky false "uncommitted changes" (rebase/merge preconditions) and
** unstable dirty flags.
**
** For every context below, a clean tree must also serialize to a hash the
** engine itself considers clean (doltliteHasUncommittedChanges == 0).
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"

extern int doltliteFlushAndSerializeCatalog(sqlite3 *db,
                                            unsigned char **ppOut, int *pnOut);
extern int doltliteHasUncommittedChanges(sqlite3 *db);

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int cond){
  if( cond ){ nPass++; }
  else { nFail++; fprintf(stderr, "FAIL: %s\n", name); }
}

static void execSql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  if( sqlite3_exec(db, zSql, 0, 0, &zErr)!=SQLITE_OK ){
    fprintf(stderr, "  sql error: %s\n  in: %s\n", zErr ? zErr : "?", zSql);
    sqlite3_free(zErr);
  }
}

/* Serialize the catalog of an open connection into a malloc'd buffer. */
static int snapshotCatalog(sqlite3 *db, unsigned char **ppBuf, int *pnBuf){
  return doltliteFlushAndSerializeCatalog(db, ppBuf, pnBuf);
}

static int sameBytes(const unsigned char *a, int na,
                     const unsigned char *b, int nb){
  return na==nb && (na==0 || memcmp(a, b, na)==0);
}

#define DBPATH "/tmp/test_catser_determinism.db"

static void buildFixture(void){
  sqlite3 *db = 0;
  unlink(DBPATH);
  sqlite3_open(DBPATH, &db);
  execSql(db,
    "CREATE TABLE plain(id INTEGER PRIMARY KEY, v TEXT);"
    "CREATE TABLE clustered(a TEXT, b INTEGER, v TEXT, PRIMARY KEY(a,b));"
    "CREATE INDEX plain_v ON plain(v);"
    "CREATE VIRTUAL TABLE ft USING fts3(x);"
    "INSERT INTO plain VALUES (1,'one'),(2,'two');"
    "INSERT INTO clustered VALUES ('k',1,'x');"
    "INSERT INTO ft VALUES ('hello world');");
  {
    sqlite3_stmt *p = 0;
    sqlite3_prepare_v2(db, "SELECT dolt_commit('-A','-m','fixture')", -1, &p, 0);
    sqlite3_step(p);
    sqlite3_finalize(p);
  }
  execSql(db, "SELECT dolt_branch('side');");
  sqlite3_close(db);
}

/* Each context opens its own connection to the SAME committed database,
** manipulates only session state, and serializes. */
typedef void (*CtxFn)(sqlite3 *db);

static void ctx_untouched(sqlite3 *db){ (void)db; }

static void ctx_schema_loaded(sqlite3 *db){
  execSql(db, "SELECT count(*) FROM plain;");
}

static void ctx_all_tables_read(sqlite3 *db){
  execSql(db, "SELECT count(*) FROM plain;");
  execSql(db, "SELECT count(*) FROM clustered;");
  execSql(db, "SELECT count(*) FROM ft WHERE ft MATCH 'hello';");
}

static void ctx_ddl_rolled_back(sqlite3 *db){
  execSql(db, "BEGIN; CREATE TABLE scratch(x); ROLLBACK;");
}

static void ctx_dml_rolled_back(sqlite3 *db){
  execSql(db, "BEGIN; INSERT INTO plain VALUES (99,'tmp'); ROLLBACK;");
}

static void ctx_checkout_round_trip(sqlite3 *db){
  execSql(db, "SELECT dolt_checkout('side');");
  execSql(db, "SELECT dolt_checkout('main');");
}

static void ctx_second_serialize(sqlite3 *db){
  unsigned char *p = 0; int n = 0;
  if( snapshotCatalog(db, &p, &n)==SQLITE_OK ) sqlite3_free(p);
}

/* Schema-changing rollback runs resetConnectionSchema, after which the
** rollback path re-serializes and persists the branch working set with the
** session schema torn down -- the internal serializeCatalog callers do not
** force a schema load the way the exported entry point does. */
static void ctx_schema_reset_rollback(sqlite3 *db){
  execSql(db,
    "BEGIN;"
    "CREATE TABLE scratch(x INTEGER PRIMARY KEY);"
    "INSERT INTO scratch VALUES (1);"
    "CREATE INDEX scratch_x ON scratch(x);"
    "ROLLBACK;");
}

static void ctx_vtab_ddl_rollback(sqlite3 *db){
  execSql(db, "BEGIN; CREATE VIRTUAL TABLE ft2 USING fts3(y); ROLLBACK;");
}

int main(int argc, char **argv){
  static const struct { const char *zName; CtxFn xFn; } aCtx[] = {
    { "untouched",           ctx_untouched },
    { "schema_loaded",       ctx_schema_loaded },
    { "all_tables_read",     ctx_all_tables_read },
    { "ddl_rolled_back",     ctx_ddl_rolled_back },
    { "dml_rolled_back",     ctx_dml_rolled_back },
    { "checkout_round_trip", ctx_checkout_round_trip },
    { "second_serialize",    ctx_second_serialize },
    { "schema_reset_rollback", ctx_schema_reset_rollback },
    { "vtab_ddl_rollback",   ctx_vtab_ddl_rollback },
  };
  unsigned char *aRef = 0; int nRef = 0;
  int i;
  (void)argc; (void)argv;

  printf("=== Catalog serialization determinism ===\n\n");
  buildFixture();

  for(i=0; i<(int)(sizeof(aCtx)/sizeof(aCtx[0])); i++){
    sqlite3 *db = 0;
    unsigned char *pBuf = 0; int nBuf = 0;
    char zLabel[128];
    int rc;

    rc = sqlite3_open(DBPATH, &db);
    check("open", rc==SQLITE_OK);
    sqlite3_busy_timeout(db, 30000);

    aCtx[i].xFn(db);

    rc = snapshotCatalog(db, &pBuf, &nBuf);
    snprintf(zLabel, sizeof(zLabel), "serialize_ok[%s]", aCtx[i].zName);
    check(zLabel, rc==SQLITE_OK && pBuf && nBuf>0);

    if( i==0 ){
      aRef = pBuf; nRef = nBuf;
    }else{
      snprintf(zLabel, sizeof(zLabel), "bytes_match_untouched[%s]",
               aCtx[i].zName);
      check(zLabel, sameBytes(aRef, nRef, pBuf, nBuf));
      sqlite3_free(pBuf);
    }

    snprintf(zLabel, sizeof(zLabel), "clean_tree_not_dirty[%s]",
             aCtx[i].zName);
    check(zLabel, doltliteHasUncommittedChanges(db)==0);

    sqlite3_close(db);

    /* Whatever the context persisted (rollback paths write the branch
    ** working set) must leave a fresh connection seeing a clean tree that
    ** serializes to the same bytes. This is where a context-dependent
    ** serialization surfaces: as a sticky dirty state for later sessions. */
    db = 0;
    rc = sqlite3_open(DBPATH, &db);
    check("reopen", rc==SQLITE_OK);
    sqlite3_busy_timeout(db, 30000);
    {
      unsigned char *pAfter = 0; int nAfter = 0;
      rc = snapshotCatalog(db, &pAfter, &nAfter);
      snprintf(zLabel, sizeof(zLabel), "reopen_serialize_ok[%s]",
               aCtx[i].zName);
      check(zLabel, rc==SQLITE_OK && pAfter && nAfter>0);
      snprintf(zLabel, sizeof(zLabel), "reopen_bytes_match[%s]",
               aCtx[i].zName);
      check(zLabel, sameBytes(aRef, nRef, pAfter, nAfter));
      sqlite3_free(pAfter);
    }
    snprintf(zLabel, sizeof(zLabel), "reopen_not_dirty[%s]", aCtx[i].zName);
    check(zLabel, doltliteHasUncommittedChanges(db)==0);
    sqlite3_close(db);
  }
  sqlite3_free(aRef);
  unlink(DBPATH);

  printf("\n=== Results: %d passed, %d failed ===\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
