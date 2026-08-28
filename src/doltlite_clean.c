#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "chunk_store.h"
#include "doltlite_internal.h"

#include <string.h>

typedef struct CleanNames CleanNames;
struct CleanNames {
  char **az;
  int n;
  int nAlloc;
};

static void cleanNamesClear(CleanNames *pNames){
  int i;
  for(i=0; i<pNames->n; i++) sqlite3_free(pNames->az[i]);
  sqlite3_free(pNames->az);
  memset(pNames, 0, sizeof(*pNames));
}

static int cleanNamesContains(const CleanNames *pNames, const char *zName){
  int i;
  for(i=0; i<pNames->n; i++){
    if( sqlite3_stricmp(pNames->az[i], zName)==0 ) return 1;
  }
  return 0;
}

static int cleanNamesAppend(CleanNames *pNames, const char *zName){
  char *zCopy;
  int rc;
  if( cleanNamesContains(pNames, zName) ) return SQLITE_OK;
  rc = DOLTLITE_GROW_ARRAY(&pNames->az, &pNames->nAlloc, pNames->n+1, 8);
  if( rc!=SQLITE_OK ) return rc;
  zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  pNames->az[pNames->n++] = zCopy;
  return SQLITE_OK;
}

static int cleanIsSystemName(const char *zName){
  return sqlite3_strnicmp(zName, "sqlite_", 7)==0
      || sqlite3_strnicmp(zName, "dolt_", 5)==0;
}

static int cleanLoadStagedNames(
  sqlite3 *db,
  ChunkStore *cs,
  CleanNames *pStaged
){
  ProllyHash stagedHash;
  SchemaEntry *aSchema = 0;
  int nSchema = 0;
  int i;
  int rc;

  doltliteGetSessionStaged(db, &stagedHash);
  if( prollyHashIsEmpty(&stagedHash) ){
    rc = doltliteGetHeadCatalogHash(db, &stagedHash);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( prollyHashIsEmpty(&stagedHash) ) return SQLITE_OK;

  rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &stagedHash,
                             &aSchema, &nSchema);
  for(i=0; rc==SQLITE_OK && i<nSchema; i++){
    if( aSchema[i].zType && strcmp(aSchema[i].zType, "table")==0
     && aSchema[i].zName && !cleanIsSystemName(aSchema[i].zName) ){
      rc = cleanNamesAppend(pStaged, aSchema[i].zName);
    }
  }
  freeSchemaEntries(aSchema, nSchema);
  return rc;
}

static int cleanResolveLiveName(
  sqlite3 *db,
  const char *zName,
  char **pzResolved
){
  sqlite3_stmt *pStmt = 0;
  int rc;
  *pzResolved = 0;
  if( !zName || cleanIsSystemName(zName) ) return SQLITE_NOTFOUND;
  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM pragma_table_list "
      "WHERE schema='main' AND name=? COLLATE NOCASE "
      "AND type IN ('table','virtual')",
      -1, &pStmt, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_bind_text(pStmt, 1, zName, -1, SQLITE_STATIC);
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      const char *zFound = (const char*)sqlite3_column_text(pStmt, 0);
      *pzResolved = zFound ? sqlite3_mprintf("%s", zFound) : 0;
      rc = *pzResolved ? SQLITE_OK : SQLITE_NOMEM;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_NOTFOUND;
    }
  }
  {
    int rc2 = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  return rc;
}

static int cleanLoadAllLiveNames(sqlite3 *db, CleanNames *pNames){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db,
      "SELECT name FROM pragma_table_list "
      "WHERE schema='main' AND type IN ('table','virtual') "
      "AND substr(name,1,7)!='sqlite_' AND substr(name,1,5)!='dolt_' "
      "ORDER BY name",
      -1, &pStmt, 0);
  while( rc==SQLITE_OK && (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
    if( zName && !cleanIsSystemName(zName) ){
      rc = cleanNamesAppend(pNames, zName);
    }
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  {
    int rc2 = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  return rc;
}

static int cleanDropTables(sqlite3 *db, const CleanNames *pNames){
  int i;
  int rc = SQLITE_OK;
  int fkeys = 0;
  int ignored = 0;
  rc = sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, -1, &fkeys);
  if( rc==SQLITE_OK && fkeys ){
    rc = sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, 0, &ignored);
  }
  for(i=pNames->n-1; rc==SQLITE_OK && i>=0; i--){
    char *zSql = sqlite3_mprintf("DROP TABLE \"%w\"", pNames->az[i]);
    if( !zSql ){
      rc = SQLITE_NOMEM;
      break;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  if( fkeys ){
    int rc2 = sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, 1, &ignored);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  return rc;
}

static void doltliteCleanFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCmdArgs args;
  CleanNames staged;
  CleanNames selected;
  CleanNames untracked;
  int dryRun = 0;
  DoltliteCmdOption aOption[] = {
    { "dry-run", 0, DOLTLITE_CMD_OPTION_FLAG, &dryRun, 0 }
  };
  int i;
  int rc;

  memset(&args, 0, sizeof(args));
  memset(&staged, 0, sizeof(staged));
  memset(&selected, 0, sizeof(selected));
  memset(&untracked, 0, sizeof(untracked));

  if( doltliteCmdRejectDetached(context) ) return;
  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    return;
  }
  for(i=0; i<argc; i++){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ){
      sqlite3_result_error(context,
                           "failed to clean; table not found: ''", -1);
      goto clean_done;
    }
  }
  rc = doltliteCmdParseArgs(context, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ) goto clean_done;

  if( args.nPositional ){
    for(i=0; i<args.nPositional; i++){
      char *zResolved = 0;
      rc = cleanResolveLiveName(db, args.azPositional[i], &zResolved);
      if( rc==SQLITE_NOTFOUND ){
        char *zErr = sqlite3_mprintf("failed to clean; table not found: '%s'",
                                     args.azPositional[i]);
        sqlite3_result_error(context, zErr ? zErr : "table not found", -1);
        sqlite3_free(zErr);
        goto clean_done;
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(zResolved);
        goto clean_error;
      }
      rc = cleanNamesAppend(&selected, zResolved);
      sqlite3_free(zResolved);
      if( rc!=SQLITE_OK ) goto clean_error;
    }
  }else{
    rc = cleanLoadAllLiveNames(db, &selected);
    if( rc!=SQLITE_OK ) goto clean_error;
  }

  rc = cleanLoadStagedNames(db, cs, &staged);
  if( rc!=SQLITE_OK ) goto clean_error;
  for(i=0; i<selected.n; i++){
    if( !cleanNamesContains(&staged, selected.az[i]) ){
      rc = cleanNamesAppend(&untracked, selected.az[i]);
      if( rc!=SQLITE_OK ) goto clean_error;
    }
  }

  if( !dryRun ){
    rc = cleanDropTables(db, &untracked);
    if( rc!=SQLITE_OK ) goto clean_error;
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ) goto clean_error;
  }
  sqlite3_result_int(context, 0);
  goto clean_done;

clean_error:
  sqlite3_result_error(context, sqlite3_errmsg(db), -1);
  sqlite3_result_error_code(context, rc);
clean_done:
  cleanNamesClear(&untracked);
  cleanNamesClear(&selected);
  cleanNamesClear(&staged);
  doltliteCmdArgsClear(&args);
}

int doltliteCleanRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_clean", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteCleanFunc, 0, 0);
}

#endif
