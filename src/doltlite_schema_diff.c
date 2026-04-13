
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

typedef struct SchemaDiffRow SchemaDiffRow;
struct SchemaDiffRow {
  char *zFromName;
  char *zToName;
  char *zFromSql;
  char *zToSql;
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
    sqlite3_free(c->aRows[i].zFromName);
    sqlite3_free(c->aRows[i].zToName);
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
  const char *zFromName,
  const char *zToName,
  const char *zFromSql,
  const char *zToSql
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
  r->zFromName = sqlite3_mprintf("%s", zFromName ? zFromName : "");
  r->zToName   = sqlite3_mprintf("%s", zToName   ? zToName   : "");
  r->zFromSql  = sqlite3_mprintf("%s", zFromSql  ? zFromSql  : "");
  r->zToSql    = sqlite3_mprintf("%s", zToSql    ? zToSql    : "");
  if( !r->zFromName || !r->zToName || !r->zFromSql || !r->zToSql ){
    sqlite3_free(r->zFromName);
    sqlite3_free(r->zToName);
    sqlite3_free(r->zFromSql);
    sqlite3_free(r->zToSql);
    memset(r, 0, sizeof(*r));
    return SQLITE_NOMEM;
  }
  pCur->nRows++;
  return SQLITE_OK;
}

/* Schema records live in the sqlite_master (iTable==1) prolly tree,
** same layout as stock SQLite: [type, name, tbl_name, rootpage, sql].
** We pull fields 0 (type), 1 (name), 4 (sql). Callers use this to
** reproduce a CREATE TABLE for introspection and for schema-merge. */
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
  SchemaEntry *aTo, int nTo,
  struct TableEntry *aFromTables, int nFromTables,
  struct TableEntry *aToTables, int nToTables
){
  int i;
  u8 *fromConsumed = 0, *toConsumed = 0;
  int rc = SQLITE_OK;

  if( nFrom > 0 ){
    fromConsumed = sqlite3_malloc(nFrom);
    if( !fromConsumed ) return SQLITE_NOMEM;
    memset(fromConsumed, 0, nFrom);
  }
  if( nTo > 0 ){
    toConsumed = sqlite3_malloc(nTo);
    if( !toConsumed ){
      sqlite3_free(fromConsumed);
      return SQLITE_NOMEM;
    }
    memset(toConsumed, 0, nTo);
  }


  for(i=0; i<nTo; i++){
    SchemaEntry *fromEntry;
    struct TableEntry *toTE;
    int j;

    fromEntry = findSchemaEntry(aFrom, nFrom, aTo[i].zName);
    if( fromEntry ) continue;

    toTE = doltliteFindTableByName(aToTables, nToTables, aTo[i].zName);
    if( !toTE || toTE->iTable==0 ) continue;

    for(j=0; j<nFromTables; j++){
      SchemaEntry *dropped;
      int k;
      if( aFromTables[j].iTable != toTE->iTable ) continue;
      if( !aFromTables[j].zName ) continue;
      if( prollyHashCompare(&aFromTables[j].root, &toTE->root)!=0 ) break;

      if( doltliteFindTableByName(aToTables, nToTables, aFromTables[j].zName) ) break;

      dropped = findSchemaEntry(aFrom, nFrom, aFromTables[j].zName);
      if( !dropped ) break;

      rc = appendSchemaDiffRow(pCur, dropped->zName, aTo[i].zName,
                               dropped->zSql, aTo[i].zSql);
      if( rc!=SQLITE_OK ) goto done;

      toConsumed[i] = 1;
      for(k=0; k<nFrom; k++){
        if( &aFrom[k] == dropped ){ fromConsumed[k] = 1; break; }
      }
      break;
    }
  }


  for(i=0; i<nTo; i++){
    SchemaEntry *fromEntry;
    if( toConsumed && toConsumed[i] ) continue;
    fromEntry = findSchemaEntry(aFrom, nFrom, aTo[i].zName);

    if( !fromEntry ){

      rc = appendSchemaDiffRow(pCur, "", aTo[i].zName,
                               "", aTo[i].zSql);
      if( rc!=SQLITE_OK ) goto done;
    }else if( fromEntry->zSql && aTo[i].zSql
           && strcmp(fromEntry->zSql, aTo[i].zSql)!=0 ){

      rc = appendSchemaDiffRow(pCur, aTo[i].zName, aTo[i].zName,
                               fromEntry->zSql, aTo[i].zSql);
      if( rc!=SQLITE_OK ) goto done;
    }
  }


  for(i=0; i<nFrom; i++){
    SchemaEntry *toEntry;
    if( fromConsumed && fromConsumed[i] ) continue;
    toEntry = findSchemaEntry(aTo, nTo, aFrom[i].zName);
    if( !toEntry ){

      rc = appendSchemaDiffRow(pCur, aFrom[i].zName, "",
                               aFrom[i].zSql, "");
      if( rc!=SQLITE_OK ) goto done;
    }
  }

done:
  sqlite3_free(fromConsumed);
  sqlite3_free(toConsumed);
  return rc;
}

static const char *sdSchema =
  "CREATE TABLE x("
  "  from_table_name TEXT,"
  "  to_table_name TEXT,"
  "  from_create_statement TEXT,"
  "  to_create_statement TEXT,"
  "  from_ref TEXT HIDDEN,"
  "  to_ref TEXT HIDDEN,"
  "  table_name TEXT HIDDEN"
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
  int iFrom = -1, iTo = -1, iTbl = -1;
  int i, argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case 4: iFrom = i; break;
      case 5: iTo   = i; break;
      case 6: iTbl  = i; break;
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
  if( iTbl>=0 ){
    pInfo->aConstraintUsage[iTbl].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTbl].omit = 1;
  }

  pInfo->idxNum = (iFrom>=0 ? 1 : 0) | (iTo>=0 ? 2 : 0) | (iTbl>=0 ? 4 : 0);
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

static int sdResolveRefs(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  const char *zFromRef,
  const char *zToRef,
  ProllyHash *pFromCatHash,
  ProllyHash *pToCatHash
){
  DoltliteCommit commit;
  ProllyHash commitHash;
  int rc;

  if( !zFromRef || !zToRef ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf(
      "dolt_schema_diff requires from_ref and to_ref"
    );
    return SQLITE_ERROR;
  }

  rc = doltliteResolveRef(db, zFromRef, &commitHash);
  if( rc!=SQLITE_OK ) return rc;
  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, &commitHash, &commit);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(pFromCatHash, &commit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&commit);

  rc = doltliteResolveRef(db, zToRef, &commitHash);
  if( rc!=SQLITE_OK ) return rc;
  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, &commitHash, &commit);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(pToCatHash, &commit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&commit);

  return SQLITE_OK;
}

static int sdParseArgs(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  int idxNum,
  int argc,
  sqlite3_value **argv,
  const char **pzFromRef,
  const char **pzToRef,
  const char **pzTableFilter
){
  int argIdx = 0;
  const char *zFromRef = 0;
  const char *zToRef = 0;
  const char *zTableFilter = 0;

  if( (idxNum & 1) && argIdx<argc ){
    zFromRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 2) && argIdx<argc ){
    zToRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 4) && argIdx<argc ){
    zTableFilter = (const char*)sqlite3_value_text(argv[argIdx++]);
  }

  if( zFromRef && !zToRef ){
    const char *zDots = strstr(zFromRef, "..");
    if( zDots ){
      int nFrom = (int)(zDots - zFromRef);
      int nTo = (int)strlen(zDots + 2);
      char *zRangeFrom = 0;
      char *zRangeTo = 0;
      ProllyHash probe;
      int rc;

      if( nFrom<=0 || nTo<=0 ){
        sqlite3_free(pVtab->zErrMsg);
        pVtab->zErrMsg = sqlite3_mprintf(
          "Invalid argument to dolt_schema_diff: %s",
          zFromRef
        );
        return SQLITE_ERROR;
      }

      zRangeFrom = sqlite3_mprintf("%.*s", nFrom, zFromRef);
      zRangeTo = sqlite3_mprintf("%s", zDots + 2);
      if( !zRangeFrom || !zRangeTo ){
        sqlite3_free(zRangeFrom);
        sqlite3_free(zRangeTo);
        return SQLITE_NOMEM;
      }

      rc = doltliteResolveRef(db, zRangeFrom, &probe);
      if( rc==SQLITE_OK ) rc = doltliteResolveRef(db, zRangeTo, &probe);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zRangeFrom);
        sqlite3_free(zRangeTo);
        return rc;
      }

      zFromRef = zRangeFrom;
      zToRef = zRangeTo;
    }else{
      sqlite3_free(pVtab->zErrMsg);
      pVtab->zErrMsg = sqlite3_mprintf(
        "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'"
      );
      return SQLITE_ERROR;
    }
  }

  *pzFromRef = zFromRef;
  *pzToRef = zToRef;
  *pzTableFilter = zTableFilter;
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
  const char *zTableFilter = 0;
  ProllyHash fromCatHash, toCatHash;
  SchemaEntry *aFrom = 0, *aTo = 0;
  int nFrom = 0, nTo = 0;
  struct TableEntry *aFromTables = 0, *aToTables = 0;
  int nFromTables = 0, nToTables = 0, freeRangeRefs = 0;
  int rc;
  (void)idxStr;

  freeSchemaDiffRows(c);
  c->iRow = 0;

  if( !cs ) return SQLITE_OK;
  pBt = doltliteGetBtShared(db);
  if( !pBt ) return SQLITE_OK;
  pCache = doltliteGetCache(db);

  rc = sdParseArgs(db, &v->base, idxNum, argc, argv,
                   &zFromRef, &zToRef, &zTableFilter);
  if( rc!=SQLITE_OK ) return rc;
  freeRangeRefs = (zFromRef && zToRef && !(idxNum & 2));

  rc = sdResolveRefs(db, &v->base, zFromRef, zToRef, &fromCatHash, &toCatHash);
  if( rc!=SQLITE_OK ) goto sd_filter_done;

  rc = loadSchemaFromCatalog(db, cs, pCache, &fromCatHash, &aFrom, &nFrom);
  if( rc!=SQLITE_OK ) goto sd_filter_done;
  rc = loadSchemaFromCatalog(db, cs, pCache, &toCatHash, &aTo, &nTo);
  if( rc!=SQLITE_OK ) goto sd_filter_done;


  rc = doltliteLoadCatalog(db, &fromCatHash, &aFromTables, &nFromTables, 0);
  if( rc!=SQLITE_OK ) goto sd_filter_done;
  rc = doltliteLoadCatalog(db, &toCatHash, &aToTables, &nToTables, 0);
  if( rc!=SQLITE_OK ) goto sd_filter_done;

  rc = computeSchemaDiff(c, aFrom, nFrom, aTo, nTo,
                         aFromTables, nFromTables,
                         aToTables, nToTables);
  if( rc!=SQLITE_OK ) goto sd_filter_done;

  if( zTableFilter ){
    int j, k=0;
    for(j=0; j<c->nRows; j++){
      int matchFrom = c->aRows[j].zFromName
                   && c->aRows[j].zFromName[0]
                   && strcmp(c->aRows[j].zFromName, zTableFilter)==0;
      int matchTo = c->aRows[j].zToName
                 && c->aRows[j].zToName[0]
                 && strcmp(c->aRows[j].zToName, zTableFilter)==0;
      if( matchFrom || matchTo ){
        if( k!=j ) c->aRows[k] = c->aRows[j];
        k++;
      }else{
        sqlite3_free(c->aRows[j].zFromName);
        sqlite3_free(c->aRows[j].zToName);
        sqlite3_free(c->aRows[j].zFromSql);
        sqlite3_free(c->aRows[j].zToSql);
      }
    }
    c->nRows = k;
  }

sd_filter_done:
  freeSchemaEntries(aFrom, nFrom);
  freeSchemaEntries(aTo, nTo);
  sqlite3_free(aFromTables);
  sqlite3_free(aToTables);
  if( freeRangeRefs ){
    sqlite3_free((char*)zFromRef);
    sqlite3_free((char*)zToRef);
  }
  return rc;
}

static int sdNext(sqlite3_vtab_cursor *cur){ ((SdCursor*)cur)->iRow++; return SQLITE_OK; }
static int sdEof(sqlite3_vtab_cursor *cur){ return ((SdCursor*)cur)->iRow >= ((SdCursor*)cur)->nRows; }

static int sdColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  SdCursor *c = (SdCursor*)cur;
  SchemaDiffRow *r = &c->aRows[c->iRow];

  switch( col ){
    case 0: sqlite3_result_text(ctx, r->zFromName, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, r->zToName,   -1, SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx, r->zFromSql,  -1, SQLITE_TRANSIENT); break;
    case 3: sqlite3_result_text(ctx, r->zToSql,    -1, SQLITE_TRANSIENT); break;
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
