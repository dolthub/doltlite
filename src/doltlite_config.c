#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "doltlite_name_index.h"
#include <stddef.h>
#include "doltlite_ignore.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

static void doltliteConfigFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zKey;

  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_config(key [, value])", -1);
    return;
  }
  if( argc>2 ){
    sqlite3_result_error(context,
      "too many positional arguments to dolt_config", -1);
    return;
  }
  zKey = (const char*)sqlite3_value_text(argv[0]);
  if( !zKey ){
    sqlite3_result_error(context, "key required", -1);
    return;
  }

  if( argc==1 ){

    if( strcmp(zKey, "user.name")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorName(db), -1, SQLITE_TRANSIENT);
    }else if( strcmp(zKey, "user.email")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorEmail(db), -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
    }
  }else{

    int eValType = sqlite3_value_type(argv[1]);
    const char *zVal = (const char*)sqlite3_value_text(argv[1]);
    int rc;
    if( !zVal && eValType!=SQLITE_NULL ){
      sqlite3_result_error_nomem(context);
      return;
    }
    if( strcmp(zKey, "user.name")==0 ){
      rc = doltliteSetAuthorName(db, zVal);
    }else if( strcmp(zKey, "user.email")==0 ){
      rc = doltliteSetAuthorEmail(db, zVal);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
      return;
    }
    if( rc==SQLITE_OK ) sqlite3_result_int(context, 0);
    else sqlite3_result_error_code(context, rc);
  }
}

static void doltliteVersionFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argv;
  if( argc!=0 ){
    sqlite3_result_error(ctx,
        "dolt_version() takes exactly zero arguments", -1);
    return;
  }
  sqlite3_result_text(ctx, DOLTLITE_VERSION, -1, SQLITE_STATIC);
}

static int mutateDefaultBranch(sqlite3 *db, ChunkStore *cs, void *pArg){
  const char *zName = (const char*)pArg;
  ProllyHash unused;
  int rc;
  (void)db;
  rc = chunkStoreFindBranch(cs, zName, &unused);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreSetDefaultBranch(cs, zName);
}

static void doltliteDefaultBranchFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  if( !cs ){
    sqlite3_result_error(ctx, "no chunk store", -1);
    return;
  }
  if( argc==0 ){
    const char *zDef = chunkStoreGetDefaultBranch(cs);
    sqlite3_result_text(ctx, zDef ? zDef : "main", -1, SQLITE_TRANSIENT);
    return;
  }
  if( argc==1 ){
    const char *zNew;
    int rc;
    if( sqlite3_value_type(argv[0])!=SQLITE_TEXT ){
      sqlite3_result_error(ctx,
        "dolt_default_branch(name): name must be text", -1);
      return;
    }
    zNew = (const char*)sqlite3_value_text(argv[0]);
    if( !zNew || !zNew[0] ){
      sqlite3_result_error(ctx, "branch name required", -1);
      return;
    }
    rc = doltliteMutateRefs(db, mutateDefaultBranch, (void*)zNew);
    if( rc==SQLITE_NOTFOUND ){
      char *zErr = sqlite3_mprintf("branch '%s' not found", zNew);
      sqlite3_result_error(ctx, zErr ? zErr : "branch not found", -1);
      sqlite3_free(zErr);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);
    return;
  }
  sqlite3_result_error(ctx,
    "dolt_default_branch() takes 0 or 1 arguments", -1);
}

static void doltliteInternalMaterializeDefaultColumnFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *zDb;
  const char *zTable;
  const char *zColumn;
  char *zSql;
  int rc;

  if( argc!=3 ){
    sqlite3_result_error(ctx,
        "doltlite_internal_materialize_default_column() takes 3 arguments", -1);
    return;
  }
  zDb = (const char*)sqlite3_value_text(argv[0]);
  zTable = (const char*)sqlite3_value_text(argv[1]);
  zColumn = (const char*)sqlite3_value_text(argv[2]);
  if( !zDb || !zTable || !zColumn ){
    sqlite3_result_null(ctx);
    return;
  }

  zSql = sqlite3_mprintf("UPDATE \"%w\".\"%w\" SET \"%w\"=\"%w\"",
                         zDb, zTable, zColumn, zColumn);
  if( !zSql ){
    sqlite3_result_error_nomem(ctx);
    return;
  }

  /* Every row has to be rewritten to carry the new column, but the user wrote
  ** ALTER TABLE, not an UPDATE. DBFLAG_InternalDml keeps triggers from firing
  ** for rows nobody changed, and the change counters are put back afterwards,
  ** so the statement is as invisible as stock's, which touches no rows at all.
  */
  {
    u32 savedFlags = db->mDbFlags;
    i64 nChange = db->nChange;
    i64 nTotalChange = db->nTotalChange;
    db->mDbFlags |= DBFLAG_InternalDml;
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    db->mDbFlags = savedFlags;
    db->nChange = nChange;
    db->nTotalChange = nTotalChange;
  }
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

int doltliteMaybeSeedRepo(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch;
  ProllyHash emptyParent;
  ProllyHash emptyCatalog;
  ProllyHash seedHash;
  ProllyHash tip;
  int seeded = 0;
  int rc;

  if( !cs ) return SQLITE_OK;
  if( sqlite3_db_readonly(db, "main")==1 ) return SQLITE_OK;
  if( refsTableBranchCount(&cs->refs)>0 ){
    zBranch = chunkStoreGetDefaultBranch(cs);
    if( !zBranch
     || chunkStoreFindBranch(cs, zBranch, &tip)!=SQLITE_OK
     || !prollyHashIsEmpty(&tip) ){
      return SQLITE_OK;
    }
    rc = doltliteSeedStoreIfNeeded(db, cs, zBranch, &seedHash, &seeded);
    if( rc==SQLITE_OK && seeded
     && strcmp(doltliteGetSessionBranch(db), zBranch)==0 ){
      doltliteSetSessionHead(db, &seedHash);
    }
    return rc;
  }

  memset(&emptyParent, 0, sizeof(emptyParent));
  memset(&emptyCatalog, 0, sizeof(emptyCatalog));

  rc = doltliteCreateAndStoreCommit(db, &emptyParent, &emptyCatalog,
      "Initialize data repository", NULL, NULL, 0, 0, &seedHash);
  if( rc!=SQLITE_OK ) return rc;

  return doltliteAdvanceBranch(db, &seedHash, &emptyCatalog, 0);
}

int doltliteConfigRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_config", -1,
                               DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                               doltliteConfigFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_version", 0,
                                                   SQLITE_UTF8, 0,
                                                   doltliteVersionFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_default_branch", -1,
                                                   DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                                   doltliteDefaultBranchFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db,
      "doltlite_internal_materialize_default_column",
      3, DOLTLITE_COMMAND_FUNC_FLAGS, 0,
      doltliteInternalMaterializeDefaultColumnFunc, 0, 0);
  return rc;
}

#endif
