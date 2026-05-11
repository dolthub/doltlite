
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>
#include <ctype.h>

char *doltliteCanonicalizeSchemaSql(const char *zSql, const char *zName);

static void doltliteHashofFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  const char *zRef;
  ProllyHash commitHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=1 ){
    sqlite3_result_error(ctx, "dolt_hashof() takes exactly one argument", -1);
    return;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_result_null(ctx);
    return;
  }
  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_null(ctx);
    return;
  }
  db = sqlite3_context_db_handle(ctx);
  rc = doltliteResolveRef(db, zRef, &commitHash);
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_null(ctx);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof: ref resolve failed", -1);
    return;
  }
  doltliteHashToHex(&commitHash, hex);
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

static int hashofTableInCatalog(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTable,
  char *pHex
){
  struct TableEntry *aTables = 0;
  struct TableEntry *pTE = 0;
  ChunkStore *cs;
  ProllyCache *cache;
  SchemaEntry *aSchema = 0;
  SchemaEntry *pSchema = 0;
  int nTables = 0;
  int nSchema = 0;
  ProllyHash schemaHash;
  ProllyHash tableHash;
  u8 aBuf[PROLLY_HASH_SIZE*2];
  int rc;

  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pTE = doltliteFindTableByName(aTables, nTables, zTable);
  if( !pTE ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_NOTFOUND;
  }

  cs = doltliteGetChunkStore(db);
  cache = doltliteGetCache(db);
  if( !cs || !cache ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_ERROR;
  }

  rc = loadSchemaFromCatalog(db, cs, cache, pCatHash, &aSchema, &nSchema);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aTables, nTables);
    return rc;
  }
  pSchema = findSchemaEntry(aSchema, nSchema, zTable);
  if( pSchema ){
    char *zCanon = doltliteCanonicalizeSchemaSql(pSchema->zSql, pSchema->zName);
    if( !zCanon ){
      freeSchemaEntries(aSchema, nSchema);
      doltliteFreeCatalog(aTables, nTables);
      return SQLITE_NOMEM;
    }
    prollyHashCompute(zCanon, (int)strlen(zCanon), &schemaHash);
    sqlite3_free(zCanon);
  }else{
    schemaHash = pTE->schemaHash;
  }

  memcpy(aBuf, pTE->root.data, PROLLY_HASH_SIZE);
  memcpy(aBuf + PROLLY_HASH_SIZE, schemaHash.data, PROLLY_HASH_SIZE);
  prollyHashCompute(aBuf, sizeof(aBuf), &tableHash);
  freeSchemaEntries(aSchema, nSchema);
  doltliteFreeCatalog(aTables, nTables);
  doltliteHashToHex(&tableHash, pHex);
  return SQLITE_OK;
}

static int ciStartsWith(const char *z, const char *zPrefix){
  while( *zPrefix ){
    if( sqlite3Tolower((unsigned char)*z) != sqlite3Tolower((unsigned char)*zPrefix) ){
      return 0;
    }
    z++;
    zPrefix++;
  }
  return 1;
}

static void appendQuotedIdent(sqlite3_str *pStr, const char *zName){
  const char *z = zName ? zName : "";
  sqlite3_str_appendchar(pStr, 1, '"');
  while( *z ){
    if( *z=='"' ){
      sqlite3_str_appendall(pStr, "\"\"");
    }else{
      sqlite3_str_appendchar(pStr, 1, *z);
    }
    z++;
  }
  sqlite3_str_appendchar(pStr, 1, '"');
}

static int isSimpleSqlIdent(const char *zName){
  int i;
  unsigned char c;
  if( !zName || !zName[0] ) return 0;
  c = (unsigned char)zName[0];
  if( !(sqlite3Isalpha(c) || c=='_') ) return 0;
  for(i=1; zName[i]; i++){
    c = (unsigned char)zName[i];
    if( !(sqlite3Isalnum(c) || c=='_') ) return 0;
  }
  return 1;
}

static void appendCanonicalIdent(sqlite3_str *pStr, const char *zName){
  if( isSimpleSqlIdent(zName) ){
    sqlite3_str_appendall(pStr, zName);
  }else{
    appendQuotedIdent(pStr, zName);
  }
}

static const char *skipSchemaIdent(const char *z){
  if( !z ) return z;
  if( *z=='"' || *z=='`' ){
    char q = *z++;
    while( *z ){
      if( *z==q ){
        if( z[1]==q ){
          z += 2;
          continue;
        }
        z++;
        break;
      }
      z++;
    }
    return z;
  }
  if( *z=='[' ){
    z++;
    while( *z ){
      if( *z==']' ){
        z++;
        break;
      }
      z++;
    }
    return z;
  }
  while( *z && !isspace((unsigned char)*z) && *z!='(' ) z++;
  return z;
}

char *doltliteCanonicalizeSchemaSql(const char *zSql, const char *zName){
  sqlite3_str *pStr;
  const char *z = zSql;
  int inSingle = 0;
  int inDouble = 0;
  int pendingSpace = 0;
  int rc;

  if( !zSql ) return sqlite3_mprintf("");
  pStr = sqlite3_str_new(0);
  if( !pStr ) return 0;

  if( zName && ciStartsWith(z, "CREATE TABLE") ){
    sqlite3_str_appendall(pStr, "CREATE TABLE ");
    appendCanonicalIdent(pStr, zName);
    z += 12;
    while( *z && isspace((unsigned char)*z) ) z++;
    z = skipSchemaIdent(z);
  }else if( zName && ciStartsWith(z, "CREATE UNIQUE INDEX") ){
    sqlite3_str_appendall(pStr, "CREATE UNIQUE INDEX ");
    appendCanonicalIdent(pStr, zName);
    z += 19;
    while( *z && isspace((unsigned char)*z) ) z++;
    z = skipSchemaIdent(z);
  }else if( zName && ciStartsWith(z, "CREATE INDEX") ){
    sqlite3_str_appendall(pStr, "CREATE INDEX ");
    appendCanonicalIdent(pStr, zName);
    z += 12;
    while( *z && isspace((unsigned char)*z) ) z++;
    z = skipSchemaIdent(z);
  }

  for(; *z; z++){
    unsigned char c = (unsigned char)*z;
    if( inSingle ){
      sqlite3_str_appendchar(pStr, 1, (char)c);
      if( c=='\'' ){
        if( z[1]=='\'' ){
          sqlite3_str_appendchar(pStr, 1, '\'');
          z++;
        }else{
          inSingle = 0;
        }
      }
      continue;
    }
    if( inDouble ){
      sqlite3_str_appendchar(pStr, 1, (char)c);
      if( c=='"' ) inDouble = 0;
      continue;
    }
    if( c=='\'' ){
      if( pendingSpace && sqlite3_str_length(pStr)>0 ){
        sqlite3_str_appendchar(pStr, 1, ' ');
      }
      pendingSpace = 0;
      inSingle = 1;
      sqlite3_str_appendchar(pStr, 1, '\'');
      continue;
    }
    if( c=='"' ){
      if( pendingSpace && sqlite3_str_length(pStr)>0 ){
        sqlite3_str_appendchar(pStr, 1, ' ');
      }
      pendingSpace = 0;
      inDouble = 1;
      sqlite3_str_appendchar(pStr, 1, '"');
      continue;
    }
    if( isspace(c) ){
      pendingSpace = 1;
      continue;
    }
    if( c=='(' || c==')' || c==',' ){
      pendingSpace = 0;
      sqlite3_str_appendchar(pStr, 1, (char)c);
      continue;
    }
    if( pendingSpace && sqlite3_str_length(pStr)>0 ){
      sqlite3_str_appendchar(pStr, 1, ' ');
    }
    pendingSpace = 0;
    sqlite3_str_appendchar(pStr, 1, (char)c);
  }

  rc = sqlite3_str_errcode(pStr);
  if( rc!=SQLITE_OK ){
    sqlite3_str_finish(pStr);
    return 0;
  }
  return sqlite3_str_finish(pStr);
}

static int schemaEntryCmp(const void *a, const void *b){
  const SchemaEntry *ea = (const SchemaEntry*)a;
  const SchemaEntry *eb = (const SchemaEntry*)b;
  int c;
  const char *za = ea->zType ? ea->zType : "";
  const char *zb = eb->zType ? eb->zType : "";
  c = strcmp(za, zb);
  if( c ) return c;
  za = ea->zName ? ea->zName : "";
  zb = eb->zName ? eb->zName : "";
  c = strcmp(za, zb);
  if( c ) return c;
  za = ea->zTblName ? ea->zTblName : "";
  zb = eb->zTblName ? eb->zTblName : "";
  return strcmp(za, zb);
}

static int tableEntryLogicalCmp(const void *a, const void *b){
  const struct TableEntry *ea = (const struct TableEntry *)a;
  const struct TableEntry *eb = (const struct TableEntry *)b;
  const char *za = ea->zName ? ea->zName : "";
  const char *zb = eb->zName ? eb->zName : "";
  int c;
  if( ea->iTable==1 && eb->iTable!=1 ) return -1;
  if( ea->iTable!=1 && eb->iTable==1 ) return 1;
  c = (int)ea->flags - (int)eb->flags;
  if( c ) return c;
  c = strcmp(za, zb);
  if( c ) return c;
  c = memcmp(ea->root.data, eb->root.data, PROLLY_HASH_SIZE);
  if( c ) return c;
  c = memcmp(ea->schemaHash.data, eb->schemaHash.data, PROLLY_HASH_SIZE);
  if( c ) return c;
  if( ea->iTable < eb->iTable ) return -1;
  if( ea->iTable > eb->iTable ) return 1;
  return 0;
}

static int canonicalizeCatalogForDbHash(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  struct TableEntry *aTables,
  int nTables
){
  ChunkStore *cs;
  ProllyCache *cache;
  SchemaEntry *aSchema = 0;
  SchemaEntry *aSorted = 0;
  int nSchema = 0;
  int i;
  sqlite3_str *pStr = 0;
  char *zCanon = 0;
  int rc;

  cs = doltliteGetChunkStore(db);
  cache = doltliteGetCache(db);
  if( !cs || !cache ) return SQLITE_ERROR;

  rc = loadSchemaFromCatalog(db, cs, cache, pCatHash, &aSchema, &nSchema);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nSchema; i++){
    if( aSchema[i].zType && strcmp(aSchema[i].zType, "table")==0 && aSchema[i].zName ){
      struct TableEntry *pTE = doltliteFindTableByName(aTables, nTables, aSchema[i].zName);
      if( pTE ){
        ProllyHash h;
        zCanon = doltliteCanonicalizeSchemaSql(aSchema[i].zSql, aSchema[i].zName);
        if( !zCanon ){
          freeSchemaEntries(aSchema, nSchema);
          return SQLITE_NOMEM;
        }
        prollyHashCompute(zCanon, (int)strlen(zCanon), &h);
        sqlite3_free(zCanon);
        zCanon = 0;
        memcpy(&pTE->schemaHash, &h, sizeof(h));
      }
    }
  }

  if( nSchema>0 ){
    aSorted = sqlite3_malloc64((sqlite3_uint64)nSchema * sizeof(SchemaEntry));
    if( !aSorted ){
      freeSchemaEntries(aSchema, nSchema);
      return SQLITE_NOMEM;
    }
    memcpy(aSorted, aSchema, (sqlite3_uint64)nSchema * sizeof(SchemaEntry));
    qsort(aSorted, nSchema, sizeof(SchemaEntry), schemaEntryCmp);
  }

  pStr = sqlite3_str_new(0);
  if( !pStr ){
    sqlite3_free(aSorted);
    freeSchemaEntries(aSchema, nSchema);
    return SQLITE_NOMEM;
  }
  for(i=0; i<nSchema; i++){
    const char *zType = aSorted[i].zType ? aSorted[i].zType : "";
    const char *zName = aSorted[i].zName ? aSorted[i].zName : "";
    const char *zTbl = aSorted[i].zTblName ? aSorted[i].zTblName : "";
    zCanon = doltliteCanonicalizeSchemaSql(aSorted[i].zSql, aSorted[i].zName);
    if( !zCanon ){
      sqlite3_str_finish(pStr);
      sqlite3_free(aSorted);
      freeSchemaEntries(aSchema, nSchema);
      return SQLITE_NOMEM;
    }
    sqlite3_str_appendf(pStr, "%s|%s|%s|%s\n", zType, zName, zTbl, zCanon);
    sqlite3_free(zCanon);
    zCanon = 0;
    if( sqlite3_str_errcode(pStr)!=SQLITE_OK ){
      sqlite3_str_finish(pStr);
      sqlite3_free(aSorted);
      freeSchemaEntries(aSchema, nSchema);
      return SQLITE_NOMEM;
    }
  }
  zCanon = sqlite3_str_finish(pStr);
  pStr = 0;
  if( !zCanon && nSchema>0 ){
    sqlite3_free(aSorted);
    freeSchemaEntries(aSchema, nSchema);
    return SQLITE_NOMEM;
  }

  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==1 ){
      ProllyHash h;
      prollyHashCompute(zCanon ? zCanon : "", zCanon ? (int)strlen(zCanon) : 0, &h);
      memcpy(&aTables[i].root, &h, sizeof(h));
      memcpy(&aTables[i].schemaHash, &h, sizeof(h));
      break;
    }
  }

  sqlite3_free(zCanon);
  sqlite3_free(aSorted);
  freeSchemaEntries(aSchema, nSchema);
  return SQLITE_OK;
}

static int canonicalizeTableNumbersForDbHash(
  struct TableEntry *aTables,
  int nTables
){
  struct TableEntry *aSorted = 0;
  Pgno *aOldIds = 0;
  int i;
  Pgno iNext = 2;

  if( nTables<=0 ) return SQLITE_OK;
  aSorted = sqlite3_malloc64((sqlite3_uint64)nTables * sizeof(struct TableEntry));
  aOldIds = sqlite3_malloc64((sqlite3_uint64)nTables * sizeof(Pgno));
  if( !aSorted || !aOldIds ){
    sqlite3_free(aSorted);
    sqlite3_free(aOldIds);
    return SQLITE_NOMEM;
  }
  memcpy(aSorted, aTables, (sqlite3_uint64)nTables * sizeof(struct TableEntry));
  qsort(aSorted, nTables, sizeof(struct TableEntry), tableEntryLogicalCmp);

  for(i=0; i<nTables; i++){
    aOldIds[i] = aSorted[i].iTable;
    if( aSorted[i].iTable==1 ) continue;
    aSorted[i].iTable = iNext++;
  }
  for(i=0; i<nTables; i++){
    int j;
    for(j=0; j<nTables; j++){
      if( aTables[i].iTable==aOldIds[j] ){
        aTables[i].iTable = aSorted[j].iTable;
        break;
      }
    }
  }
  sqlite3_free(aOldIds);
  sqlite3_free(aSorted);
  return SQLITE_OK;
}

static int hashofDbInCatalog(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  char *pHex
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  struct TableEntry *aSorted = 0;
  sqlite3_str *pStr = 0;
  ProllyHash h;
  int rc;
  int i;

  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = canonicalizeCatalogForDbHash(db, pCatHash, aTables, nTables);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aTables, nTables);
    return rc;
  }
  rc = canonicalizeTableNumbersForDbHash(aTables, nTables);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aTables, nTables);
    return rc;
  }
  if( nTables>0 ){
    aSorted = sqlite3_malloc64((sqlite3_uint64)nTables * sizeof(struct TableEntry));
    if( !aSorted ){
      doltliteFreeCatalog(aTables, nTables);
      return SQLITE_NOMEM;
    }
    memcpy(aSorted, aTables, (sqlite3_uint64)nTables * sizeof(struct TableEntry));
    qsort(aSorted, nTables, sizeof(struct TableEntry), tableEntryLogicalCmp);
  }
  pStr = sqlite3_str_new(0);
  if( !pStr ){
    sqlite3_free(aSorted);
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_NOMEM;
  }
  for(i=0; i<nTables; i++){
    char zRoot[PROLLY_HASH_SIZE*2+1];
    char zSchema[PROLLY_HASH_SIZE*2+1];
    doltliteHashToHex(&aSorted[i].root, zRoot);
    doltliteHashToHex(&aSorted[i].schemaHash, zSchema);
    sqlite3_str_appendf(pStr, "%u|%u|%s|%s|%s\n",
        (unsigned)aSorted[i].iTable,
        (unsigned)aSorted[i].flags,
        zRoot,
        zSchema,
        aSorted[i].zName ? aSorted[i].zName : "");
    if( sqlite3_str_errcode(pStr)!=SQLITE_OK ){
      sqlite3_str_finish(pStr);
      sqlite3_free(aSorted);
      doltliteFreeCatalog(aTables, nTables);
      return SQLITE_NOMEM;
    }
  }
  {
    char *zCanon = sqlite3_str_finish(pStr);
    if( !zCanon && nTables>0 ){
      sqlite3_free(aSorted);
      doltliteFreeCatalog(aTables, nTables);
      return SQLITE_NOMEM;
    }
    prollyHashCompute(zCanon ? zCanon : "", zCanon ? (int)strlen(zCanon) : 0, &h);
    sqlite3_free(zCanon);
  }
  sqlite3_free(aSorted);
  doltliteFreeCatalog(aTables, nTables);
  doltliteHashToHex(&h, pHex);
  return SQLITE_OK;
}

static void doltliteHashofTableFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  const char *zTable;
  ProllyHash catHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=1 && argc!=2 ){
    sqlite3_result_error(ctx, "dolt_hashof_table() takes 1 or 2 arguments", -1);
    return;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_result_null(ctx);
    return;
  }
  zTable = (const char*)sqlite3_value_text(argv[0]);
  if( !zTable || !*zTable ){
    sqlite3_result_null(ctx);
    return;
  }
  db = sqlite3_context_db_handle(ctx);

  if( argc==1 ){
    rc = doltliteFlushCatalogToHash(db, &catHash);
  }else{
    const char *zRef;
    ProllyHash commitHash;
    DoltliteCommit commit;
    if( sqlite3_value_type(argv[1])==SQLITE_NULL ){
      sqlite3_result_null(ctx);
      return;
    }
    zRef = (const char*)sqlite3_value_text(argv[1]);
    if( !zRef ){
      sqlite3_result_null(ctx);
      return;
    }
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_result_null(ctx);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_table: ref resolve failed", -1);
      return;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_table: commit load failed", -1);
      return;
    }
    catHash = commit.catalogHash;
    doltliteCommitClear(&commit);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof_table: catalog flush failed", -1);
    return;
  }

  rc = hashofTableInCatalog(db, &catHash, zTable, hex);
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_null(ctx);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof_table: table not found in catalog", -1);
    return;
  }
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

static void doltliteHashofDbFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  ProllyHash catHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=0 && argc!=1 ){
    sqlite3_result_error(ctx, "dolt_hashof_db() takes 0 or 1 argument", -1);
    return;
  }
  db = sqlite3_context_db_handle(ctx);

  if( argc==0 ){
    rc = doltliteFlushCatalogToHash(db, &catHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_db: catalog flush failed", -1);
      return;
    }
  }else{
    const char *zRef;
    ProllyHash commitHash;
    DoltliteCommit commit;
    if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
      sqlite3_result_null(ctx);
      return;
    }
    zRef = (const char*)sqlite3_value_text(argv[0]);
    if( !zRef ){
      sqlite3_result_null(ctx);
      return;
    }
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_result_null(ctx);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_db: ref resolve failed", -1);
      return;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_db: commit load failed", -1);
      return;
    }
    catHash = commit.catalogHash;
    doltliteCommitClear(&commit);
  }

  rc = hashofDbInCatalog(db, &catHash, hex);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof_db: catalog hash failed", -1);
    return;
  }
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

static void doltliteHashofCatalogFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  ProllyHash catHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=0 && argc!=1 ){
    sqlite3_result_error(ctx, "dolt_hashof_catalog() takes 0 or 1 argument", -1);
    return;
  }
  db = sqlite3_context_db_handle(ctx);

  if( argc==0 ){
    if( doltliteHasUncommittedChanges(db) ){
      rc = doltliteFlushCatalogToHash(db, &catHash);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(ctx, "dolt_hashof_catalog: catalog flush failed", -1);
        return;
      }
    }else{
      rc = doltliteGetPersistedWorkingCatalogHash(db, &catHash);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(ctx, "dolt_hashof_catalog: persisted catalog read failed", -1);
        return;
      }
    }
  }else{
    const char *zRef;
    ProllyHash commitHash;
    DoltliteCommit commit;
    if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
      sqlite3_result_null(ctx);
      return;
    }
    zRef = (const char*)sqlite3_value_text(argv[0]);
    if( !zRef ){
      sqlite3_result_null(ctx);
      return;
    }
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_result_null(ctx);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_catalog: ref resolve failed", -1);
      return;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_catalog: commit load failed", -1);
      return;
    }
    catHash = commit.catalogHash;
    doltliteCommitClear(&commit);
  }

  doltliteHashToHex(&catHash, hex);
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

int doltliteHashofRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_hashof", 1, SQLITE_UTF8, 0,
                               doltliteHashofFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_table", 1, SQLITE_UTF8, 0,
                                 doltliteHashofTableFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_table", 2, SQLITE_UTF8, 0,
                                 doltliteHashofTableFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_db", 0, SQLITE_UTF8, 0,
                                 doltliteHashofDbFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_db", 1, SQLITE_UTF8, 0,
                                 doltliteHashofDbFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_catalog", 0, SQLITE_UTF8, 0,
                                 doltliteHashofCatalogFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_catalog", 1, SQLITE_UTF8, 0,
                                 doltliteHashofCatalogFunc, 0, 0);
  }
  return rc;
}

#endif
