
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>

/* SchemaEntry struct is declared in doltlite_internal.h */

typedef struct SchemaDiffRow SchemaDiffRow;
struct SchemaDiffRow {
  char *zName;
  char *zFromSql;
  char *zToSql;
  char *zDiffType;    
};

typedef struct SdVtab SdVtab;
struct SdVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct SdCursor SdCursor;
struct SdCursor {
  sqlite3_vtab_cursor base;
  SchemaDiffRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

static int schemaTextField(
  const u8 *pVal,
  int nVal,
  DoltliteRecordInfo *pRi,
  int iField,
  char **pzOut
){
  int st, off, len;
  char *zOut;

  *pzOut = 0;
  if( iField>=pRi->nField ) return SQLITE_CORRUPT;
  st = pRi->aType[iField];
  off = pRi->aOffset[iField];
  if( st==0 ) return SQLITE_OK;
  if( st<13 || (st&1)==0 ) return SQLITE_CORRUPT;
  len = (st-13)/2;
  if( off<0 || off+len>nVal ) return SQLITE_CORRUPT;
  zOut = sqlite3_malloc(len+1);
  if( !zOut ) return SQLITE_NOMEM;
  memcpy(zOut, pVal+off, len);
  zOut[len] = 0;
  *pzOut = zOut;
  return SQLITE_OK;
}

static void freeSchemaDiffRows(SdCursor *c){
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aRows[i].zName);
    sqlite3_free(c->aRows[i].zFromSql);
    sqlite3_free(c->aRows[i].zToSql);
  }
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
  c->nAlloc = 0;
}

static int appendSchemaEntry(
  SchemaEntry **paEntries,
  int *pnEntries,
  int *pnAlloc,
  char *zName,
  char *zSql,
  char *zType
){
  SchemaEntry *aEntries = *paEntries;
  int nEntries = *pnEntries;
  int nAlloc = *pnAlloc;
  if( nEntries >= nAlloc ){
    int nNew = nAlloc ? nAlloc*2 : 16;
    SchemaEntry *aNew = sqlite3_realloc(aEntries, nNew*(int)sizeof(SchemaEntry));
    if( !aNew ) return SQLITE_NOMEM;
    aEntries = aNew;
    nAlloc = nNew;
  }
  aEntries[nEntries].zName = zName;
  aEntries[nEntries].zSql = zSql;
  aEntries[nEntries].zType = zType;
  *paEntries = aEntries;
  *pnEntries = nEntries + 1;
  *pnAlloc = nAlloc;
  return SQLITE_OK;
}

static int appendSchemaDiffRow(
  SdCursor *pCur,
  const char *zName,
  const char *zFromSql,
  const char *zToSql,
  char *zDiffType
){
  SchemaDiffRow *r;
  if( pCur->nRows >= pCur->nAlloc ){
    int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 16;
    SchemaDiffRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(SchemaDiffRow));
    if( !aNew ) return SQLITE_NOMEM;
    pCur->aRows = aNew;
    pCur->nAlloc = nNew;
  }
  r = &pCur->aRows[pCur->nRows];
  memset(r, 0, sizeof(*r));
  r->zName = sqlite3_mprintf("%s", zName ? zName : "");
  if( !r->zName ) return SQLITE_NOMEM;
  if( zFromSql ){
    r->zFromSql = sqlite3_mprintf("%s", zFromSql);
    if( !r->zFromSql ){
      sqlite3_free(r->zName);
      memset(r, 0, sizeof(*r));
      return SQLITE_NOMEM;
    }
  }
  if( zToSql ){
    r->zToSql = sqlite3_mprintf("%s", zToSql);
    if( !r->zToSql ){
      sqlite3_free(r->zName);
      sqlite3_free(r->zFromSql);
      memset(r, 0, sizeof(*r));
      return SQLITE_NOMEM;
    }
  }
  r->zDiffType = zDiffType;
  pCur->nRows++;
  return SQLITE_OK;
}



int loadSchemaFromCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pCatHash,
  SchemaEntry **ppEntries, int *pnEntries
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  ProllyHash masterRoot;
  u8 masterFlags = 0;
  ProllyCursor cur;
  int res, rc, i;
  SchemaEntry *aEntries = 0;
  int nEntries = 0, nAlloc = 0;

  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ){ *ppEntries = 0; *pnEntries = 0; return rc; }

  
  memset(&masterRoot, 0, sizeof(masterRoot));
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==1 ){
      memcpy(&masterRoot, &aTables[i].root, sizeof(ProllyHash));
      masterFlags = aTables[i].flags;
      break;
    }
  }
  sqlite3_free(aTables);

  if( prollyHashIsEmpty(&masterRoot) ){
    *ppEntries = 0; *pnEntries = 0;
    return SQLITE_OK;
  }

  
  prollyCursorInit(&cur, cs, pCache, &masterRoot, masterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){ prollyCursorClose(&cur); *ppEntries = 0; *pnEntries = 0; return rc; }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    DoltliteRecordInfo ri;

    prollyCursorValue(&cur, &pVal, &nVal);

    if( pVal && nVal > 0 ){
      doltliteParseRecord(pVal, nVal, &ri);

      if( ri.nField < 5 ){
        rc = SQLITE_CORRUPT;
        goto load_schema_done;
      }else{
        char *zType = 0, *zName = 0, *zSql = 0;

        rc = schemaTextField(pVal, nVal, &ri, 0, &zType);
        if( rc!=SQLITE_OK ) goto load_schema_done;
        rc = schemaTextField(pVal, nVal, &ri, 1, &zName);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zType);
          goto load_schema_done;
        }
        rc = schemaTextField(pVal, nVal, &ri, 4, &zSql);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zType);
          sqlite3_free(zName);
          goto load_schema_done;
        }

        if( zName ){
          rc = appendSchemaEntry(&aEntries, &nEntries, &nAlloc, zName, zSql, zType);
          if( rc!=SQLITE_OK ) goto load_schema_done;
          zName = 0;
          zSql = 0;
          zType = 0;
        }
        sqlite3_free(zType);
        sqlite3_free(zName);
        sqlite3_free(zSql);
      }
    }

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }

load_schema_done:
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK ){
    freeSchemaEntries(aEntries, nEntries);
    *ppEntries = 0;
    *pnEntries = 0;
    return rc;
  }
  *ppEntries = aEntries;
  *pnEntries = nEntries;
  return rc;
}

void freeSchemaEntries(SchemaEntry *a, int n){
  int i;
  for(i=0; i<n; i++){
    sqlite3_free(a[i].zName);
    sqlite3_free(a[i].zSql);
    sqlite3_free(a[i].zType);
  }
  sqlite3_free(a);
}

SchemaEntry *findSchemaEntry(SchemaEntry *a, int n, const char *zName){
  int i;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ) return &a[i];
  }
  return 0;
}

static int computeSchemaDiff(
  SdCursor *pCur,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo
){
  int i;

  
  for(i=0; i<nTo; i++){
    SchemaEntry *fromEntry = findSchemaEntry(aFrom, nFrom, aTo[i].zName);

    if( !fromEntry ){
      int rc = appendSchemaDiffRow(pCur, aTo[i].zName, 0,
                                   aTo[i].zSql ? aTo[i].zSql : "", "added");
      if( rc!=SQLITE_OK ) return rc;
    }else if( fromEntry->zSql && aTo[i].zSql
           && strcmp(fromEntry->zSql, aTo[i].zSql)!=0 ){
      int rc = appendSchemaDiffRow(pCur, aTo[i].zName, fromEntry->zSql,
                                   aTo[i].zSql, "modified");
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  
  for(i=0; i<nFrom; i++){
    SchemaEntry *toEntry = findSchemaEntry(aTo, nTo, aFrom[i].zName);
    if( !toEntry ){
      int rc = appendSchemaDiffRow(pCur, aFrom[i].zName,
                                   aFrom[i].zSql ? aFrom[i].zSql : "",
                                   0, "dropped");
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

static const char *sdSchema =
  "CREATE TABLE x("
  "  table_name TEXT,"
  "  from_create_stmt TEXT,"
  "  to_create_stmt TEXT,"
  "  diff_type TEXT,"
  "  from_ref TEXT HIDDEN,"
  "  to_ref TEXT HIDDEN"
  ")";

static int sdConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  SdVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, sdSchema);
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int sdDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }

static int sdBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iFrom = -1, iTo = -1;
  int i, argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case 4: iFrom = i; break;
      case 5: iTo = i; break;
    }
  }

  if( iFrom>=0 ){
    pInfo->aConstraintUsage[iFrom].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFrom].omit = 1;
  }
  if( iTo>=0 ){
    pInfo->aConstraintUsage[iTo].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTo].omit = 1;
  }

  pInfo->idxNum = (iFrom>=0 ? 1 : 0) | (iTo>=0 ? 2 : 0);
  pInfo->estimatedCost = 1000.0;
  return SQLITE_OK;
}

static int sdOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  SdCursor *c; (void)v;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int sdClose(sqlite3_vtab_cursor *cur){
  SdCursor *c = (SdCursor*)cur;
  freeSchemaDiffRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int sdFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  SdCursor *c = (SdCursor*)cur;
  SdVtab *v = (SdVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt;
  ProllyCache *pCache;
  const char *zFromRef = 0, *zToRef = 0;
  ProllyHash fromCommit, toCommit;
  DoltliteCommit commit;
  ProllyHash fromCatHash, toCatHash;
  SchemaEntry *aFrom = 0, *aTo = 0;
  int nFrom = 0, nTo = 0;
  int rc, argIdx = 0;
  (void)idxStr;

  freeSchemaDiffRows(c);
  c->iRow = 0;

  if( !cs ) return SQLITE_OK;
  pBt = doltliteGetBtShared(db);
  if( !pBt ) return SQLITE_OK;
  pCache = doltliteGetCache(db);

  if( (idxNum & 1) && argIdx<argc ){
    zFromRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 2) && argIdx<argc ){
    zToRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }

  
  {
    const char *zTableFilter = 0;
    if( zFromRef && !zToRef ){
      ProllyHash testHash;
      rc = doltliteResolveRef(db,zFromRef, &testHash);
      if( rc==SQLITE_NOTFOUND ){
        zTableFilter = zFromRef;
        zFromRef = 0;
      }else if( rc!=SQLITE_OK ){
        return rc;
      }
    }

    
    if( zFromRef ){
      rc = doltliteResolveRef(db,zFromRef, &fromCommit);
      if( rc!=SQLITE_OK ) return rc;
      memset(&commit, 0, sizeof(commit));
      rc = doltliteLoadCommit(db, &fromCommit, &commit);
      if( rc!=SQLITE_OK ) return rc;
      memcpy(&fromCatHash, &commit.catalogHash, sizeof(ProllyHash));
      doltliteCommitClear(&commit);
    }else{
      rc = doltliteGetHeadCatalogHash(db, &fromCatHash);
      if( rc!=SQLITE_OK ) return rc;
    }

  
    if( zToRef ){
      rc = doltliteResolveRef(db,zToRef, &toCommit);
      if( rc!=SQLITE_OK ) return rc;
      memset(&commit, 0, sizeof(commit));
      rc = doltliteLoadCommit(db, &toCommit, &commit);
      if( rc!=SQLITE_OK ) return rc;
      memcpy(&toCatHash, &commit.catalogHash, sizeof(ProllyHash));
      doltliteCommitClear(&commit);
    }else{
      u8 *catData = 0; int nCatData = 0;
      rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(cs, catData, nCatData, &toCatHash);
        sqlite3_free(catData);
      }
      if( rc!=SQLITE_OK ) return rc;
    }

  
    rc = loadSchemaFromCatalog(db, cs, pCache, &fromCatHash, &aFrom, &nFrom);
    if( rc!=SQLITE_OK ) goto sd_filter_done;
    rc = loadSchemaFromCatalog(db, cs, pCache, &toCatHash, &aTo, &nTo);
    if( rc!=SQLITE_OK ) goto sd_filter_done;

  
    rc = computeSchemaDiff(c, aFrom, nFrom, aTo, nTo);
    if( rc!=SQLITE_OK ) goto sd_filter_done;

  
    if( zTableFilter ){
      int j, k=0;
      for(j=0; j<c->nRows; j++){
        if( c->aRows[j].zName && strcmp(c->aRows[j].zName, zTableFilter)==0 ){
          if( k!=j ) c->aRows[k] = c->aRows[j];
          k++;
        }else{
          sqlite3_free(c->aRows[j].zName);
          sqlite3_free(c->aRows[j].zFromSql);
          sqlite3_free(c->aRows[j].zToSql);
        }
      }
      c->nRows = k;
    }

  }

sd_filter_done:
  freeSchemaEntries(aFrom, nFrom);
  freeSchemaEntries(aTo, nTo);
  return rc;
}

static int sdNext(sqlite3_vtab_cursor *cur){ ((SdCursor*)cur)->iRow++; return SQLITE_OK; }
static int sdEof(sqlite3_vtab_cursor *cur){ return ((SdCursor*)cur)->iRow >= ((SdCursor*)cur)->nRows; }

static int sdColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  SdCursor *c = (SdCursor*)cur;
  SchemaDiffRow *r = &c->aRows[c->iRow];
  switch( col ){
    case 0: sqlite3_result_text(ctx, r->zName, -1, SQLITE_TRANSIENT); break;
    case 1:
      if(r->zFromSql) sqlite3_result_text(ctx, r->zFromSql, -1, SQLITE_TRANSIENT);
      else sqlite3_result_null(ctx);
      break;
    case 2:
      if(r->zToSql) sqlite3_result_text(ctx, r->zToSql, -1, SQLITE_TRANSIENT);
      else sqlite3_result_null(ctx);
      break;
    case 3: sqlite3_result_text(ctx, r->zDiffType, -1, SQLITE_STATIC); break;
  }
  return SQLITE_OK;
}

static int sdRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((SdCursor*)cur)->iRow; return SQLITE_OK;
}

static sqlite3_module schemaDiffModule = {
  0, 0, sdConnect, sdBestIndex, sdDisconnect, 0,
  sdOpen, sdClose, sdFilter, sdNext, sdEof,
  sdColumn, sdRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteSchemaDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_schema_diff", &schemaDiffModule, 0);
}

#endif 
