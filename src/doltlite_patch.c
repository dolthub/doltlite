#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_vtab_util.h"
#include "doltlite_internal.h"
#include "record_codec.h"
#include "prolly_cursor.h"
#include "prolly_diff.h"
#include "prolly_record.h"
#include <math.h>
#include <string.h>

typedef struct PatchRow PatchRow;
typedef struct PatchVtab PatchVtab;
typedef struct PatchCursor PatchCursor;
typedef struct PatchTable PatchTable;
typedef struct PatchSchema PatchSchema;
typedef struct PatchValue PatchValue;

struct PatchRow {
  char *zTable;
  char *zType;
  char *zSql;
};

struct PatchVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

struct PatchCursor {
  sqlite3_vtab_cursor base;
  PatchRow *aRow;
  int nRow;
  int nAlloc;
  int iRow;
  char *zFrom;
  char *zTo;
};

struct PatchTable {
  SchemaEntry *pFromSchema;
  SchemaEntry *pToSchema;
  struct TableEntry *pFromTable;
  struct TableEntry *pToTable;
  const char *zFromName;
  const char *zToName;
};

struct PatchSchema {
  sqlite3 *db;
  DoltliteColInfo col;
  int *aPk;
};

struct PatchValue {
  int eType;
  i64 i;
  double r;
  const u8 *p;
  int n;
};

static void patchRowsClear(PatchCursor *pCur){
  int i;
  for(i=0; i<pCur->nRow; i++){
    sqlite3_free(pCur->aRow[i].zTable);
    sqlite3_free(pCur->aRow[i].zType);
    sqlite3_free(pCur->aRow[i].zSql);
  }
  sqlite3_free(pCur->aRow);
  sqlite3_free(pCur->zFrom);
  sqlite3_free(pCur->zTo);
  pCur->aRow = 0;
  pCur->nRow = 0;
  pCur->nAlloc = 0;
  pCur->iRow = 0;
  pCur->zFrom = 0;
  pCur->zTo = 0;
}

static int patchAppendRow(
  PatchCursor *pCur,
  const char *zTable,
  const char *zType,
  const char *zSql
){
  PatchRow *p;
  int rc;
  if( !zSql || !zSql[0] ) return SQLITE_OK;
  rc = DOLTLITE_GROW_ARRAY(&pCur->aRow, &pCur->nAlloc, pCur->nRow+1, 32);
  if( rc!=SQLITE_OK ) return rc;
  p = &pCur->aRow[pCur->nRow];
  memset(p, 0, sizeof(*p));
  p->zTable = sqlite3_mprintf("%s", zTable ? zTable : "");
  p->zType = sqlite3_mprintf("%s", zType ? zType : "");
  p->zSql = sqlite3_mprintf("%s%s", zSql,
      zSql[strlen(zSql)-1]==';' ? "" : ";");
  if( !p->zTable || !p->zType || !p->zSql ){
    sqlite3_free(p->zTable);
    sqlite3_free(p->zType);
    sqlite3_free(p->zSql);
    memset(p, 0, sizeof(*p));
    return SQLITE_NOMEM;
  }
  pCur->nRow++;
  return SQLITE_OK;
}

static void patchSetError(sqlite3_vtab *pVtab, const char *zFmt,
                          const char *zArg){
  sqlite3_free(pVtab->zErrMsg);
  pVtab->zErrMsg = sqlite3_mprintf(zFmt, zArg ? zArg : "");
}

static int patchResolveOne(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  const char *zRef,
  ProllyHash *pCommit,
  ProllyHash *pCatalog,
  char **pzLabel
){
  int rc;
  memset(pCommit, 0, sizeof(*pCommit));
  memset(pCatalog, 0, sizeof(*pCatalog));
  *pzLabel = 0;
  if( sqlite3_stricmp(zRef, "WORKING")==0 ){
    rc = doltliteFlushCatalogToHash(db, pCatalog);
    if( rc==SQLITE_OK ) *pzLabel = sqlite3_mprintf("WORKING");
  }else if( sqlite3_stricmp(zRef, "STAGED")==0 ){
    doltliteGetSessionStaged(db, pCatalog);
    if( prollyHashIsEmpty(pCatalog) ){
      rc = doltliteGetHeadCatalogHash(db, pCatalog);
    }
    else rc = SQLITE_OK;
    if( rc==SQLITE_OK ) *pzLabel = sqlite3_mprintf("STAGED");
  }else{
    char zHex[PROLLY_HASH_SIZE*2+1];
    rc = doltliteResolveRef(db, zRef, pCommit);
    if( rc==SQLITE_OK ) rc = doltliteCommitCatalogHash(db, pCommit, pCatalog);
    if( rc==SQLITE_OK ){
      doltliteHashToHex(pCommit, zHex);
      *pzLabel = sqlite3_mprintf("%s", zHex);
    }
  }
  if( rc!=SQLITE_OK ){
    patchSetError(pVtab, "dolt_patch: ref '%s' could not be resolved", zRef);
    return SQLITE_ERROR;
  }
  return *pzLabel ? SQLITE_OK : SQLITE_NOMEM;
}

static int patchResolveArgs(
  PatchVtab *pVtab,
  const char *zFromArg,
  const char *zToArg,
  int bRange,
  ProllyHash *pFromCat,
  ProllyHash *pToCat,
  char **pzFromLabel,
  char **pzToLabel
){
  ProllyHash fromCommit, toCommit;
  int rc;
  rc = patchResolveOne(pVtab->db, &pVtab->base, zFromArg,
                       &fromCommit, pFromCat, pzFromLabel);
  if( rc!=SQLITE_OK ) return rc;
  rc = patchResolveOne(pVtab->db, &pVtab->base, zToArg,
                       &toCommit, pToCat, pzToLabel);
  if( rc!=SQLITE_OK ) return rc;
  if( bRange==DOLTLITE_RANGE_THREE_DOT ){
    ProllyHash ancestor;
    char zHex[PROLLY_HASH_SIZE*2+1];
    if( prollyHashIsEmpty(&fromCommit) || prollyHashIsEmpty(&toCommit) ){
      patchSetError(&pVtab->base,
        "dolt_patch: three-dot range requires commit refs, not '%s'",
        prollyHashIsEmpty(&fromCommit) ? zFromArg : zToArg);
      return SQLITE_ERROR;
    }
    rc = doltliteFindAncestor(pVtab->db, &fromCommit, &toCommit, &ancestor);
    if( rc!=SQLITE_OK ) return rc;
    rc = doltliteCommitCatalogHash(pVtab->db, &ancestor, pFromCat);
    if( rc!=SQLITE_OK ) return rc;
    doltliteHashToHex(&ancestor, zHex);
    sqlite3_free(*pzFromLabel);
    *pzFromLabel = sqlite3_mprintf("%s", zHex);
    if( !*pzFromLabel ) return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int patchIsTable(const SchemaEntry *p){
  return p && p->zType && sqlite3_stricmp(p->zType, "table")==0
      && p->zName && sqlite3_strnicmp(p->zName, "sqlite_", 7)!=0;
}

static SchemaEntry *patchFindTableSchema(SchemaEntry *a, int n,
                                         const char *zName){
  SchemaEntry *p = findSchemaEntry(a, n, zName);
  return patchIsTable(p) ? p : 0;
}

static int patchTableCmp(const void *a, const void *b){
  const PatchTable *pA = (const PatchTable*)a;
  const PatchTable *pB = (const PatchTable*)b;
  const char *zA = pA->zToName ? pA->zToName : pA->zFromName;
  const char *zB = pB->zToName ? pB->zToName : pB->zFromName;
  return strcmp(zA, zB);
}

static int patchRootsShareKey(
  sqlite3 *db,
  const struct TableEntry *pFrom,
  const struct TableEntry *pTo
){
  ChunkStore *cs;
  ProllyCache *cache;
  ProllyCursor fromCur, toCur;
  int rc, res, found = 0;
  if( pFrom->flags!=pTo->flags
   || prollyHashIsEmpty(&pFrom->root)
   || prollyHashIsEmpty(&pTo->root) ) return 0;
  cs = doltliteGetChunkStore(db);
  cache = doltliteGetCache(db);
  if( !cs || !cache ) return 0;
  prollyCursorInit(&fromCur,cs,cache,&pFrom->root,pFrom->flags);
  prollyCursorInit(&toCur,cs,cache,&pTo->root,pTo->flags);
  rc = prollyCursorFirst(&fromCur,&res);
  if( rc==SQLITE_OK && res!=0 ) rc = SQLITE_DONE;
  while( rc==SQLITE_OK && prollyCursorIsValid(&fromCur) ){
    if( pFrom->flags & BTREE_INTKEY ){
      rc = prollyCursorSeekInt(&toCur,prollyCursorIntKey(&fromCur),&res);
    }else{
      const u8 *pKey = 0;
      int nKey = 0;
      prollyCursorKey(&fromCur,&pKey,&nKey);
      rc = prollyCursorSeekBlob(&toCur,pKey,nKey,&res);
    }
    if( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&toCur) ){
      found = 1;
      break;
    }
    if( rc==SQLITE_OK ) rc = prollyCursorNext(&fromCur);
  }
  prollyCursorClose(&fromCur);
  prollyCursorClose(&toCur);
  return found;
}

/* First '(' outside quotes; a quoted table name may contain parens. */
static const char *patchSchemaBody(const char *zSql){
  const char *z = zSql;
  while( *z ){
    char c = *z;
    if( c=='"' || c=='`' || c=='\'' ){
      z++;
      while( *z ){
        if( *z==c ){
          if( z[1]==c ){ z += 2; continue; }
          z++;
          break;
        }
        z++;
      }
    }else if( c=='[' ){
      z++;
      while( *z && *z!=']' ) z++;
      if( *z ) z++;
    }else if( c=='(' ){
      return z;
    }else{
      z++;
    }
  }
  return 0;
}

static int patchIsRenamePair(
  sqlite3 *db,
  const struct TableEntry *pFrom,
  const struct TableEntry *pTo,
  const SchemaEntry *pFromSchema,
  const SchemaEntry *pToSchema
){
  const char *zFromBody;
  const char *zToBody;
  if( pFrom->iTable!=pTo->iTable ) return 0;
  if( prollyHashCompare(&pFrom->root,&pTo->root)==0 ) return 1;
  if( !pFromSchema || !pToSchema ) return 0;
  zFromBody = patchSchemaBody(pFromSchema->zSql);
  zToBody = patchSchemaBody(pToSchema->zSql);
  if( !zFromBody || !zToBody || strcmp(zFromBody,zToBody)!=0 ) return 0;
  return patchRootsShareKey(db,pFrom,pTo);
}

static int patchBuildTables(
  sqlite3 *db,
  SchemaEntry *aFromSchema, int nFromSchema,
  SchemaEntry *aToSchema, int nToSchema,
  struct TableEntry *aFromTable, int nFromTable,
  struct TableEntry *aToTable, int nToTable,
  PatchTable **ppTable, int *pnTable
){
  PatchTable *aOut = 0;
  u8 *aFromUsed = 0;
  int nOut = 0;
  int i;
  if( nFromTable>0 ){
    aFromUsed = sqlite3_malloc64((sqlite3_uint64)nFromTable);
    if( !aFromUsed ) return SQLITE_NOMEM;
    memset(aFromUsed,0,(size_t)nFromTable);
  }
  for(i=0; i<nToTable; i++){
    struct TableEntry *pTo = &aToTable[i];
    struct TableEntry *pFrom = 0;
    SchemaEntry *pToSchema;
    int jFrom = -1;
    int j;
    if( pTo->iTable<=1 || !pTo->zName ) continue;
    pToSchema = patchFindTableSchema(aToSchema, nToSchema, pTo->zName);
    if( !pToSchema ) continue;
    /* Names identify ordinary changes. Infer a rename from page number only if
    ** the old name disappeared and roots are equal, or schemas match with a surviving key. */
    for(j=0; j<nFromTable; j++){
      if( aFromUsed[j] || !aFromTable[j].zName ) continue;
      if( strcmp(aFromTable[j].zName,pTo->zName)==0 ){
        pFrom=&aFromTable[j]; jFrom=j; break;
      }
    }
    if( !pFrom ){
      for(j=0; j<nFromTable; j++){
        SchemaEntry *pFromSchema;
        if( aFromUsed[j] || !aFromTable[j].zName ) continue;
        if( doltliteFindTableByName(aToTable,nToTable,aFromTable[j].zName) ){
          continue;
        }
        pFromSchema = patchFindTableSchema(aFromSchema,nFromSchema,
                                           aFromTable[j].zName);
        if( patchIsRenamePair(db,&aFromTable[j],pTo,pFromSchema,pToSchema) ){
          pFrom=&aFromTable[j]; jFrom=j; break;
        }
      }
    }
    {
      PatchTable *aNew = sqlite3_realloc64(
          aOut,(sqlite3_uint64)(nOut+1)*sizeof(PatchTable));
      if( !aNew ){
        sqlite3_free(aFromUsed);
        sqlite3_free(aOut);
        return SQLITE_NOMEM;
      }
      aOut = aNew;
    }
    memset(&aOut[nOut], 0, sizeof(PatchTable));
    aOut[nOut].pToTable = pTo;
    aOut[nOut].pToSchema = pToSchema;
    aOut[nOut].zToName = pTo->zName;
    if( pFrom && pFrom->iTable>1 && pFrom->zName ){
      aFromUsed[jFrom]=1;
      aOut[nOut].pFromTable = pFrom;
      aOut[nOut].zFromName = pFrom->zName;
      aOut[nOut].pFromSchema = patchFindTableSchema(aFromSchema, nFromSchema,
                                                    pFrom->zName);
    }
    nOut++;
  }
  for(i=0; i<nFromTable; i++){
    struct TableEntry *pFrom = &aFromTable[i];
    PatchTable *aNew;
    if( aFromUsed[i] || pFrom->iTable<=1 || !pFrom->zName ) continue;
    if( !patchFindTableSchema(aFromSchema, nFromSchema, pFrom->zName) ) continue;
    aNew = sqlite3_realloc64(aOut, (sqlite3_uint64)(nOut+1)*sizeof(PatchTable));
    if( !aNew ){
      sqlite3_free(aFromUsed);
      sqlite3_free(aOut);
      return SQLITE_NOMEM;
    }
    aOut = aNew;
    memset(&aOut[nOut], 0, sizeof(PatchTable));
    aOut[nOut].pFromTable = pFrom;
    aOut[nOut].pFromSchema = patchFindTableSchema(aFromSchema, nFromSchema,
                                                  pFrom->zName);
    aOut[nOut].zFromName = pFrom->zName;
    nOut++;
  }
  qsort(aOut, (size_t)nOut, sizeof(PatchTable), patchTableCmp);
  sqlite3_free(aFromUsed);
  *ppTable = aOut;
  *pnTable = nOut;
  return SQLITE_OK;
}

static void patchSchemaClear(PatchSchema *p){
  doltliteFreeColInfo(&p->col);
  sqlite3_free(p->aPk);
  if( p->db ) sqlite3_close(p->db);
  memset(p, 0, sizeof(*p));
}

static int patchSchemaLoad(const SchemaEntry *pEntry, PatchSchema *pOut){
  sqlite3_stmt *pStmt = 0;
  char *zPragma = 0;
  int rc;
  int i = 0;
  memset(pOut, 0, sizeof(*pOut));
  if( !pEntry || !pEntry->zSql ) return SQLITE_NOTFOUND;
  rc = sqlite3_open(":memory:", &pOut->db);
  if( rc!=SQLITE_OK ) goto done;
  rc = sqlite3_exec(pOut->db, pEntry->zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteGetColumnNames(pOut->db, pEntry->zName, &pOut->col);
  if( rc!=SQLITE_OK ) goto done;
  pOut->aPk = sqlite3_malloc64((sqlite3_uint64)pOut->col.nCol*sizeof(int));
  if( pOut->col.nCol && !pOut->aPk ){ rc = SQLITE_NOMEM; goto done; }
  memset(pOut->aPk, 0, (size_t)pOut->col.nCol*sizeof(int));
  zPragma = sqlite3_mprintf("PRAGMA main.table_info(\"%w\")", pEntry->zName);
  if( !zPragma ){ rc = SQLITE_NOMEM; goto done; }
  rc = sqlite3_prepare_v2(pOut->db, zPragma, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) goto done;
  while( (rc=sqlite3_step(pStmt))==SQLITE_ROW && i<pOut->col.nCol ){
    pOut->aPk[i++] = sqlite3_column_int(pStmt, 5);
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
done:
  sqlite3_finalize(pStmt);
  sqlite3_free(zPragma);
  if( rc!=SQLITE_OK ) patchSchemaClear(pOut);
  return rc;
}

static void patchAppendIdent(sqlite3_str *pStr, const char *zName){
  sqlite3_str_appendf(pStr, "\"%w\"", zName);
}

static int patchColumnIndex(const PatchSchema *p, const char *zName){
  int i;
  for(i=0; i<p->col.nCol; i++){
    if( strcmp(p->col.azName[i], zName)==0 ) return i;
  }
  return -1;
}

static const char *patchRowidName(const PatchSchema *p){
  static const char *const azRowid[] = {"rowid", "_rowid_", "oid"};
  int i;
  for(i=0; i<ArraySize(azRowid); i++){
    if( patchColumnIndex(p,azRowid[i])<0 ) return azRowid[i];
  }
  return 0;
}

static int patchHasPrimaryKey(const PatchSchema *p){
  int i;
  for(i=0; i<p->col.nCol; i++) if( p->aPk[i]>0 ) return 1;
  return 0;
}

static int patchPrimaryKeyChanged(
  const PatchSchema *pFrom,
  const PatchSchema *pTo
){
  int i, j, nFrom = 0, nTo = 0;
  for(i=0; i<pFrom->col.nCol; i++) if( pFrom->aPk[i]>0 ) nFrom++;
  for(i=0; i<pTo->col.nCol; i++) if( pTo->aPk[i]>0 ) nTo++;
  if( nFrom!=nTo ) return 1;
  for(i=0; i<pTo->col.nCol; i++){
    if( pTo->aPk[i]<=0 ) continue;
    j = patchColumnIndex(pFrom,pTo->col.azName[i]);
    if( j<0 && pFrom->col.nCol==pTo->col.nCol ) j = i;
    if( j<0 || pFrom->aPk[j]!=pTo->aPk[i] ) return 1;
  }
  return 0;
}

static int patchAppendAssociated(
  PatchCursor *pCur,
  const char *zTable,
  SchemaEntry *aSchema,
  int nSchema
){
  int i, rc;
  for(i=0; i<nSchema; i++){
    SchemaEntry *p = &aSchema[i];
    if( !p->zTblName || strcmp(p->zTblName, zTable)!=0 || !p->zSql ) continue;
    if( p->zType && sqlite3_stricmp(p->zType,"index")==0 ){
      rc = patchAppendRow(pCur, zTable, "schema", p->zSql);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int patchAppendDropObject(PatchCursor *pCur, const char *zTable,
                                 const SchemaEntry *p){
  char *zSql;
  int rc;
  if( !p->zType || !p->zName ) return SQLITE_OK;
  if( sqlite3_stricmp(p->zType,"index")!=0
   && sqlite3_stricmp(p->zType,"trigger")!=0 ) return SQLITE_OK;
  zSql = sqlite3_mprintf("DROP %s \"%w\"", p->zType, p->zName);
  if( !zSql ) return SQLITE_NOMEM;
  rc = patchAppendRow(pCur, zTable, "schema", zSql);
  sqlite3_free(zSql);
  return rc;
}

static int patchAppendObjectDrops(
  PatchCursor *pCur, const char *zTable,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo
){
  int i, rc;
  for(i=0; i<nFrom; i++){
    SchemaEntry *p = &aFrom[i];
    SchemaEntry *q;
    if( !p->zTblName || strcmp(p->zTblName,zTable)!=0 ) continue;
    if( !p->zSql ) continue;
    q = findSchemaEntry(aTo, nTo, p->zName);
    /* Drop every trigger on a changed table so patch DML does not fire them. */
    if( (p->zType && sqlite3_stricmp(p->zType,"trigger")==0)
     || !q || !q->zSql || strcmp(p->zSql,q->zSql)!=0 ){
      rc = patchAppendDropObject(pCur, zTable, p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int patchAppendIndexCreates(
  PatchCursor *pCur, const char *zTable,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo
){
  int i, rc;
  for(i=0; i<nTo; i++){
    SchemaEntry *p = &aTo[i];
    SchemaEntry *q;
    if( !p->zTblName || strcmp(p->zTblName,zTable)!=0 || !p->zSql ) continue;
    if( !p->zType || sqlite3_stricmp(p->zType,"index")!=0 ) continue;
    q = findSchemaEntry(aFrom, nFrom, p->zName);
    if( !q || !q->zSql || strcmp(q->zSql,p->zSql)!=0 ){
      rc = patchAppendRow(pCur, zTable, "schema", p->zSql);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int patchAppendObjectDiffs(
  PatchCursor *pCur, const char *zTable,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo
){
  int rc = patchAppendObjectDrops(pCur,zTable,aFrom,nFrom,aTo,nTo);
  if( rc==SQLITE_OK ){
    rc = patchAppendIndexCreates(pCur,zTable,aFrom,nFrom,aTo,nTo);
  }
  return rc;
}

/* Views are not in the table walk. Drop views first (name reuse); create last
** (they may reference tables/triggers). */
static int patchAppendViewDrops(
  PatchCursor *pCur, const char *zFilter,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo,
  int *pFound
){
  int i, rc;
  for(i=0; i<nFrom; i++){
    SchemaEntry *p = &aFrom[i];
    SchemaEntry *q;
    if( !p->zType || sqlite3_stricmp(p->zType,"view")!=0 ) continue;
    if( !p->zName || !p->zSql ) continue;
    if( zFilter && sqlite3_stricmp(zFilter,p->zName)!=0 ) continue;
    q = findSchemaEntry(aTo, nTo, p->zName);
    if( !q || !q->zSql || strcmp(p->zSql,q->zSql)!=0 ){
      char *zSql = sqlite3_mprintf("DROP VIEW \"%w\"", p->zName);
      if( !zSql ) return SQLITE_NOMEM;
      rc = patchAppendRow(pCur, p->zName, "schema", zSql);
      sqlite3_free(zSql);
      if( rc!=SQLITE_OK ) return rc;
      if( pFound ) *pFound = 1;
    }
  }
  return SQLITE_OK;
}

static int patchAppendViewCreates(
  PatchCursor *pCur, const char *zFilter,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo,
  int *pFound
){
  int i, rc;
  for(i=0; i<nTo; i++){
    SchemaEntry *p = &aTo[i];
    SchemaEntry *q;
    if( !p->zType || sqlite3_stricmp(p->zType,"view")!=0 ) continue;
    if( !p->zName || !p->zSql ) continue;
    if( zFilter && sqlite3_stricmp(zFilter,p->zName)!=0 ) continue;
    q = findSchemaEntry(aFrom, nFrom, p->zName);
    if( !q || !q->zSql || strcmp(p->zSql,q->zSql)!=0 ){
      rc = patchAppendRow(pCur, p->zName, "schema", p->zSql);
      if( rc!=SQLITE_OK ) return rc;
      if( pFound ) *pFound = 1;
    }
  }
  return SQLITE_OK;
}

static int patchAppendTargetTriggers(
  PatchCursor *pCur,
  const PatchTable *pTable,
  SchemaEntry *aTo,
  int nTo
){
  int i, rc;
  if( !pTable->pToSchema ) return SQLITE_OK;
  for(i=0; i<nTo; i++){
    SchemaEntry *p = &aTo[i];
    if( !p->zTblName || strcmp(p->zTblName,pTable->zToName)!=0 || !p->zSql ){
      continue;
    }
    if( !p->zType || sqlite3_stricmp(p->zType,"trigger")!=0 ) continue;
    rc = patchAppendRow(pCur,pTable->zToName,"schema",p->zSql);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int patchObjectsDiffer(
  const char *zTable,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo
){
  int i;
  for(i=0; i<nFrom; i++){
    SchemaEntry *p = &aFrom[i];
    SchemaEntry *q;
    if( !p->zTblName || strcmp(p->zTblName,zTable)!=0 ) continue;
    if( !p->zSql ) continue;
    if( !p->zType || (sqlite3_stricmp(p->zType,"index")!=0
                   && sqlite3_stricmp(p->zType,"trigger")!=0) ) continue;
    q = findSchemaEntry(aTo,nTo,p->zName);
    if( !q || !q->zSql || strcmp(p->zSql,q->zSql)!=0 ) return 1;
  }
  for(i=0; i<nTo; i++){
    SchemaEntry *p = &aTo[i];
    SchemaEntry *q;
    if( !p->zTblName || strcmp(p->zTblName,zTable)!=0 ) continue;
    if( !p->zSql ) continue;
    if( !p->zType || (sqlite3_stricmp(p->zType,"index")!=0
                   && sqlite3_stricmp(p->zType,"trigger")!=0) ) continue;
    q = findSchemaEntry(aFrom,nFrom,p->zName);
    if( !q || !q->zSql || strcmp(p->zSql,q->zSql)!=0 ) return 1;
  }
  return 0;
}

static int patchAppendRebuild(
  PatchCursor *pCur,
  const PatchTable *pTable,
  const PatchSchema *pFrom,
  const PatchSchema *pTo,
  SchemaEntry *aFromSchema,
  int nFromSchema,
  SchemaEntry *aToSchema,
  int nToSchema,
  int iTemp,
  int bCopyData
){
  const char *zBody = patchSchemaBody(pTable->pToSchema->zSql);
  sqlite3_str *pStr;
  char *zSql;
  char *zTemp = 0;
  int *aUsed = 0;
  int i, j, nMap = 0, rc = SQLITE_OK;
  if( !zBody ) return SQLITE_CORRUPT;
  do{
    sqlite3_free(zTemp);
    zTemp = sqlite3_mprintf("__doltlite_patch_%d", iTemp++);
    if( !zTemp ) return SQLITE_NOMEM;
  }while( findSchemaEntry(aFromSchema,nFromSchema,zTemp)
       || findSchemaEntry(aToSchema,nToSchema,zTemp) );

  pStr = sqlite3_str_new(0);
  sqlite3_str_appendall(pStr, "CREATE TABLE ");
  patchAppendIdent(pStr, zTemp);
  sqlite3_str_appendall(pStr, zBody);
  zSql = sqlite3_str_finish(pStr);
  if( !zSql ){ rc = SQLITE_NOMEM; goto done; }
  rc = patchAppendRow(pCur, pTable->zToName, "schema", zSql);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) goto done;

  if( bCopyData ){
    aUsed = sqlite3_malloc64((sqlite3_uint64)pFrom->col.nCol*sizeof(int));
    if( pFrom->col.nCol && !aUsed ){ rc = SQLITE_NOMEM; goto done; }
    memset(aUsed, 0, (size_t)pFrom->col.nCol*sizeof(int));
    pStr = sqlite3_str_new(0);
    sqlite3_str_appendall(pStr, "INSERT INTO ");
    patchAppendIdent(pStr, zTemp);
    sqlite3_str_appendchar(pStr, 1, '(');
    for(i=0; i<pTo->col.nCol; i++){
      j = patchColumnIndex(pFrom, pTo->col.azName[i]);
      if( j<0 && pFrom->col.nCol==pTo->col.nCol && i<pFrom->col.nCol
       && !aUsed[i] ) j = i;
      if( j<0 ) continue;
      if( nMap++ ) sqlite3_str_appendall(pStr, ",");
      patchAppendIdent(pStr, pTo->col.azName[i]);
      aUsed[j] = 1;
    }
    sqlite3_str_appendall(pStr, ") SELECT ");
    nMap = 0;
    memset(aUsed, 0, (size_t)pFrom->col.nCol*sizeof(int));
    for(i=0; i<pTo->col.nCol; i++){
      j = patchColumnIndex(pFrom, pTo->col.azName[i]);
      if( j<0 && pFrom->col.nCol==pTo->col.nCol && i<pFrom->col.nCol
       && !aUsed[i] ) j = i;
      if( j<0 ) continue;
      if( nMap++ ) sqlite3_str_appendall(pStr, ",");
      patchAppendIdent(pStr, pFrom->col.azName[j]);
      aUsed[j] = 1;
    }
    sqlite3_str_appendall(pStr, " FROM ");
    patchAppendIdent(pStr, pTable->zFromName);
    zSql = sqlite3_str_finish(pStr);
    if( !zSql ){ rc = SQLITE_NOMEM; goto done; }
    if( nMap>0 ) rc = patchAppendRow(pCur,pTable->zToName,"schema",zSql);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ) goto done;
  }

  zSql = sqlite3_mprintf("DROP TABLE \"%w\"", pTable->zFromName);
  if( !zSql ){ rc = SQLITE_NOMEM; goto done; }
  rc = patchAppendRow(pCur,pTable->zToName,"schema",zSql);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) goto done;
  zSql = sqlite3_mprintf("ALTER TABLE \"%w\" RENAME TO \"%w\"",
                         zTemp, pTable->zToName);
  if( !zSql ){ rc = SQLITE_NOMEM; goto done; }
  rc = patchAppendRow(pCur,pTable->zToName,"schema",zSql);
  sqlite3_free(zSql);
  if( rc==SQLITE_OK ){
    rc = patchAppendAssociated(pCur,pTable->zToName,aToSchema,nToSchema);
  }
done:
  sqlite3_free(aUsed);
  sqlite3_free(zTemp);
  return rc;
}

static const char *patchSchemaBodyEnd(const char *zBody){
  const char *z = zBody;
  int depth = 0;
  while( *z ){
    char c = *z;
    if( c=='"' || c=='`' || c=='\'' ){
      z++;
      while( *z ){
        if( *z==c ){
          if( z[1]==c ){ z += 2; continue; }
          z++;
          break;
        }
        z++;
      }
    }else if( c=='[' ){
      z++;
      while( *z && *z!=']' ) z++;
      if( *z ) z++;
    }else{
      if( c=='(' ) depth++;
      if( c==')' && --depth==0 ) return z;
      z++;
    }
  }
  return 0;
}

static int patchSchemaItem(
  const char *zSql,
  int iWanted,
  const char **pzItem,
  int *pnItem
){
  const char *zBody = patchSchemaBody(zSql);
  const char *zEnd;
  const char *z;
  const char *zItem;
  int iItem = 0;
  int depth = 0;
  *pzItem = 0;
  *pnItem = 0;
  if( !zBody ) return SQLITE_CORRUPT;
  zEnd = patchSchemaBodyEnd(zBody);
  if( !zEnd ) return SQLITE_CORRUPT;
  zItem = zBody + 1;
  for(z=zItem; z<=zEnd; z++){
    char c = *z;
    if( z==zEnd || (c==',' && depth==0) ){
      const char *zStart = zItem;
      const char *zStop = z;
      while( zStart<zStop && sqlite3Isspace(*zStart) ) zStart++;
      while( zStop>zStart && sqlite3Isspace(zStop[-1]) ) zStop--;
      if( zStop==zStart ) return SQLITE_CORRUPT;
      if( iWanted<0 || iWanted==iItem ){
        *pzItem = zStart;
        *pnItem = (int)(zStop-zStart);
        if( iWanted>=0 ) return SQLITE_OK;
      }
      iItem++;
      zItem = z + 1;
    }else if( c=='"' || c=='`' || c=='\'' ){
      char q = c;
      z++;
      while( z<zEnd ){
        if( *z==q ){
          if( z+1<zEnd && z[1]==q ){ z++; }
          else break;
        }
        z++;
      }
    }else if( c=='[' ){
      while( z<zEnd && *z!=']' ) z++;
    }else if( c=='(' ){
      depth++;
    }else if( c==')' ){
      depth--;
    }
  }
  return *pzItem ? SQLITE_OK : SQLITE_NOTFOUND;
}

static int patchNativeAlter(
  const PatchTable *pTable,
  const PatchSchema *pFrom,
  const PatchSchema *pTo,
  char **pzAlter
){
  sqlite3 *tmp = 0;
  sqlite3_stmt *pStmt = 0;
  char *zAlter = 0;
  const char *zActual = 0;
  int i, nDiff = 0, iDiff = -1;
  int isCandidate = 0;
  int rc = SQLITE_OK;
  *pzAlter = 0;
  if( strcmp(pTable->zFromName,pTable->zToName)!=0 ) return SQLITE_OK;
  if( pFrom->col.nCol==pTo->col.nCol ){
    for(i=0; i<pFrom->col.nCol; i++){
      if( strcmp(pFrom->col.azName[i],pTo->col.azName[i])!=0 ){
        nDiff++;
        iDiff = i;
      }
    }
    if( nDiff==1 ){
      const char *zItem = 0;
      int nItem = 0;
      int nIdent = 0;
      rc = patchSchemaItem(pTable->pToSchema->zSql,iDiff,&zItem,&nItem);
      if( rc!=SQLITE_OK ) return rc;
      if( zItem[0]=='"' || zItem[0]=='`' ){
        char q = zItem[0];
        for(nIdent=1; nIdent<nItem; nIdent++){
          if( zItem[nIdent]==q ){
            if( nIdent+1<nItem && zItem[nIdent+1]==q ){ nIdent++; continue; }
            nIdent++;
            break;
          }
        }
      }else if( zItem[0]=='[' ){
        for(nIdent=1; nIdent<nItem && zItem[nIdent-1]!=']'; nIdent++){}
      }else{
        while( nIdent<nItem && !sqlite3Isspace(zItem[nIdent])
            && zItem[nIdent]!='(' && zItem[nIdent]!=',' ) nIdent++;
      }
      if( nIdent<=0 || nIdent>nItem ) return SQLITE_CORRUPT;
      isCandidate = 1;
      zAlter = sqlite3_mprintf(
          "ALTER TABLE \"%w\" RENAME COLUMN \"%w\" TO %.*s",
          pTable->zFromName,pFrom->col.azName[iDiff],nIdent,zItem);
    }
  }else if( pTo->col.nCol==pFrom->col.nCol+1 ){
    const char *zItem = 0;
    int nItem = 0;
    for(i=0; i<pFrom->col.nCol; i++){
      if( strcmp(pFrom->col.azName[i],pTo->col.azName[i])!=0 ) return SQLITE_OK;
    }
    rc = patchSchemaItem(pTable->pToSchema->zSql,-1,&zItem,&nItem);
    if( rc!=SQLITE_OK ) return rc;
    isCandidate = 1;
    zAlter = sqlite3_mprintf("ALTER TABLE \"%w\" ADD COLUMN %.*s",
                             pTable->zFromName,nItem,zItem);
  }
  if( !zAlter ) return isCandidate ? SQLITE_NOMEM : SQLITE_OK;
  rc = sqlite3_open(":memory:",&tmp);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(tmp,pTable->pFromSchema->zSql,0,0,0);
  }
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp,zAlter,0,0,0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(tmp,
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
        -1,&pStmt,0);
  }
  if( rc==SQLITE_OK ){
    sqlite3_bind_text(pStmt,1,pTable->zToName,-1,SQLITE_STATIC);
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      zActual = (const char*)sqlite3_column_text(pStmt,0);
      if( zActual && strcmp(zActual,pTable->pToSchema->zSql)==0 ){
        *pzAlter = zAlter;
        zAlter = 0;
      }
      rc = SQLITE_OK;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_CORRUPT;
    }
  }
  if( rc!=SQLITE_OK && rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
  sqlite3_finalize(pStmt);
  if( tmp ) sqlite3_close(tmp);
  sqlite3_free(zAlter);
  return rc;
}

static void patchGetValue(
  const PatchSchema *pSchema,
  const u8 *pRec, int nRec,
  i64 intKey,
  int iCol,
  PatchValue *pOut
){
  DoltliteRecordInfo ri;
  int iField, st, off, n;
  memset(pOut, 0, sizeof(*pOut));
  pOut->eType = SQLITE_NULL;
  if( iCol==pSchema->col.iPkCol && iCol>=0 ){
    pOut->eType = SQLITE_INTEGER;
    pOut->i = intKey;
    return;
  }
  if( !pRec || nRec<=0 ) return;
  doltliteParseRecord(pRec, nRec, &ri);
  iField = pSchema->col.aColToRec ? pSchema->col.aColToRec[iCol] : iCol;
  if( iField<0 || iField>=ri.nField ) return;
  st = ri.aType[iField];
  off = ri.aOffset[iField];
  if( st==0 ) return;
  if( st==8 || st==9 ){
    pOut->eType = SQLITE_INTEGER; pOut->i = st==9; return;
  }
  if( st>=1 && st<=6 ){
    n = dlSerialTypeLen((u64)st);
    if( off>=0 && off<=nRec-n ){
      pOut->eType = SQLITE_INTEGER;
      pOut->i = dlReadIntBytes(pRec+off, n);
    }
    return;
  }
  if( st==7 && off>=0 && off<=nRec-8 ){
    u64 bits = 0;
    int i;
    for(i=0; i<8; i++) bits = (bits<<8) | pRec[off+i];
    pOut->eType = SQLITE_FLOAT;
    memcpy(&pOut->r, &bits, 8);
    return;
  }
  if( st>=12 ){
    n = dlSerialTypeLen((u64)st);
    if( off>=0 && off<=nRec-n ){
      pOut->eType = (st&1) ? SQLITE_TEXT : SQLITE_BLOB;
      pOut->p = pRec+off;
      pOut->n = n;
    }
  }
}

static void patchAppendHex(sqlite3_str *pStr, const u8 *p, int n){
  static const char zHex[] = "0123456789ABCDEF";
  int i;
  for(i=0; i<n; i++){
    char z[2];
    z[0] = zHex[p[i]>>4];
    z[1] = zHex[p[i]&15];
    sqlite3_str_append(pStr, z, 2);
  }
}

static void patchAppendValue(sqlite3_str *pStr, const PatchValue *pVal){
  int i, hasNul = 0;
  switch( pVal->eType ){
    case SQLITE_INTEGER:
      sqlite3_str_appendf(pStr, "%lld", pVal->i);
      break;
    case SQLITE_FLOAT:
      if( sqlite3IsNaN(pVal->r) ) sqlite3_str_appendall(pStr, "NULL");
      else if( isinf(pVal->r) ) sqlite3_str_appendf(pStr,
          pVal->r<0 ? "-9.0e999" : "9.0e999");
      else sqlite3_str_appendf(pStr, "%!.17g", pVal->r);
      break;
    case SQLITE_TEXT:
      for(i=0; i<pVal->n; i++) if( pVal->p[i]==0 ){ hasNul=1; break; }
      if( hasNul ){
        sqlite3_str_appendall(pStr, "CAST(X'");
        patchAppendHex(pStr,pVal->p,pVal->n);
        sqlite3_str_appendall(pStr, "' AS TEXT)");
      }else{
        sqlite3_str_appendchar(pStr,1,'\'');
        for(i=0; i<pVal->n; i++){
          if( pVal->p[i]=='\'' ) sqlite3_str_appendchar(pStr,1,'\'');
          sqlite3_str_appendchar(pStr,1,(char)pVal->p[i]);
        }
        sqlite3_str_appendchar(pStr,1,'\'');
      }
      break;
    case SQLITE_BLOB:
      sqlite3_str_appendall(pStr,"X'");
      patchAppendHex(pStr,pVal->p,pVal->n);
      sqlite3_str_appendchar(pStr,1,'\'');
      break;
    default:
      sqlite3_str_appendall(pStr,"NULL");
      break;
  }
}

static int patchValuesEqual(const PatchValue *a, const PatchValue *b){
  if( a->eType!=b->eType ) return 0;
  switch( a->eType ){
    case SQLITE_NULL: return 1;
    case SQLITE_INTEGER: return a->i==b->i;
    case SQLITE_FLOAT: return a->r==b->r;
    default: return a->n==b->n && memcmp(a->p,b->p,(size_t)a->n)==0;
  }
}

static void patchAppendWhere(
  sqlite3_str *pStr,
  const PatchSchema *pSchema,
  const u8 *pRec, int nRec,
  i64 intKey
){
  int i, nPk = 0;
  PatchValue v;
  sqlite3_str_appendall(pStr," WHERE ");
  for(i=0; i<pSchema->col.nCol; i++) if( pSchema->aPk[i]>0 ) nPk++;
  if( nPk==0 ){
    const char *zRowid = patchRowidName(pSchema);
    assert( zRowid!=0 );
    patchAppendIdent(pStr,zRowid);
    sqlite3_str_appendf(pStr,"=%lld",intKey);
    return;
  }
  nPk = 0;
  for(i=0; i<pSchema->col.nCol; i++){
    if( pSchema->aPk[i]<=0 ) continue;
    if( nPk++ ) sqlite3_str_appendall(pStr," AND ");
    patchAppendIdent(pStr,pSchema->col.azName[i]);
    patchGetValue(pSchema,pRec,nRec,intKey,i,&v);
    if( v.eType==SQLITE_NULL ) sqlite3_str_appendall(pStr," IS NULL");
    else{
      sqlite3_str_appendchar(pStr,1,'=');
      patchAppendValue(pStr,&v);
    }
  }
}

static int patchAppendInsert(PatchCursor *pCur, const char *zTable,
  const PatchSchema *pTo, const u8 *pRec, int nRec, i64 intKey){
  sqlite3_str *pStr;
  PatchValue v;
  const char *zRowid;
  char *zSql;
  int i, nPk = 0, rc;
  for(i=0; i<pTo->col.nCol; i++) if( pTo->aPk[i]>0 ) nPk++;
  zRowid = nPk==0 ? patchRowidName(pTo) : 0;
  if( nPk==0 && !zRowid ){
    patchSetError(pCur->base.pVtab,
      "dolt_patch: table '%s' shadows every rowid alias",zTable);
    return SQLITE_ERROR;
  }
  pStr = sqlite3_str_new(0);
  sqlite3_str_appendall(pStr,"INSERT INTO ");
  patchAppendIdent(pStr,zTable);
  sqlite3_str_appendchar(pStr,1,'(');
  if( zRowid ) patchAppendIdent(pStr,zRowid);
  for(i=0; i<pTo->col.nCol; i++){
    if( i || zRowid ) sqlite3_str_appendall(pStr,",");
    patchAppendIdent(pStr,pTo->col.azName[i]);
  }
  sqlite3_str_appendall(pStr,") VALUES (");
  if( zRowid ) sqlite3_str_appendf(pStr,"%lld",intKey);
  for(i=0; i<pTo->col.nCol; i++){
    if( i || zRowid ) sqlite3_str_appendall(pStr,",");
    patchGetValue(pTo,pRec,nRec,intKey,i,&v);
    patchAppendValue(pStr,&v);
  }
  sqlite3_str_appendchar(pStr,1,')');
  zSql = sqlite3_str_finish(pStr);
  if( !zSql ) return SQLITE_NOMEM;
  rc = patchAppendRow(pCur,zTable,"data",zSql);
  sqlite3_free(zSql);
  return rc;
}

static int patchAppendDelete(PatchCursor *pCur, const char *zTable,
  const PatchSchema *pFrom, const u8 *pRec, int nRec, i64 intKey){
  sqlite3_str *pStr;
  char *zSql;
  int rc;
  if( !patchHasPrimaryKey(pFrom) && !patchRowidName(pFrom) ){
    patchSetError(pCur->base.pVtab,
      "dolt_patch: table '%s' shadows every rowid alias",zTable);
    return SQLITE_ERROR;
  }
  pStr = sqlite3_str_new(0);
  sqlite3_str_appendall(pStr,"DELETE FROM ");
  patchAppendIdent(pStr,zTable);
  patchAppendWhere(pStr,pFrom,pRec,nRec,intKey);
  zSql = sqlite3_str_finish(pStr);
  if( !zSql ) return SQLITE_NOMEM;
  rc = patchAppendRow(pCur,zTable,"data",zSql);
  sqlite3_free(zSql);
  return rc;
}

static int patchAppendUpdate(PatchCursor *pCur, const char *zTable,
  const PatchSchema *pFrom, const PatchSchema *pTo,
  const u8 *pOld, int nOld, const u8 *pNew, int nNew, i64 intKey){
  sqlite3_str *pStr;
  PatchValue oldV, newV;
  char *zSql;
  int i, j, nSet = 0, rc;
  if( !patchHasPrimaryKey(pFrom) && !patchRowidName(pFrom) ){
    patchSetError(pCur->base.pVtab,
      "dolt_patch: table '%s' shadows every rowid alias",zTable);
    return SQLITE_ERROR;
  }
  pStr = sqlite3_str_new(0);
  sqlite3_str_appendall(pStr,"UPDATE ");
  patchAppendIdent(pStr,zTable);
  sqlite3_str_appendall(pStr," SET ");
  for(i=0; i<pTo->col.nCol; i++){
    if( pTo->aPk[i]>0 ) continue;
    j = patchColumnIndex(pFrom,pTo->col.azName[i]);
    patchGetValue(pTo,pNew,nNew,intKey,i,&newV);
    if( j>=0 ) patchGetValue(pFrom,pOld,nOld,intKey,j,&oldV);
    else memset(&oldV,0,sizeof(oldV));
    if( j>=0 && patchValuesEqual(&oldV,&newV) ) continue;
    if( nSet++ ) sqlite3_str_appendall(pStr,",");
    patchAppendIdent(pStr,pTo->col.azName[i]);
    sqlite3_str_appendchar(pStr,1,'=');
    patchAppendValue(pStr,&newV);
  }
  if( nSet==0 ){
    rc = sqlite3_str_errcode(pStr);
    zSql = sqlite3_str_finish(pStr);
    sqlite3_free(zSql);
    return rc;
  }
  patchAppendWhere(pStr,pFrom,pOld,nOld,intKey);
  zSql = sqlite3_str_finish(pStr);
  if( !zSql ) return SQLITE_NOMEM;
  rc = patchAppendRow(pCur,zTable,"data",zSql);
  sqlite3_free(zSql);
  return rc;
}

static int patchAppendData(
  PatchCursor *pCur,
  sqlite3 *db,
  const PatchTable *pTable,
  const PatchSchema *pFrom,
  const PatchSchema *pTo
){
  ProllyDiffIter it;
  ProllyDiffChange *pChange = 0;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash oldRoot, newRoot;
  u8 fromFlags;
  u8 toFlags;
  int rc;
  memset(&oldRoot,0,sizeof(oldRoot));
  memset(&newRoot,0,sizeof(newRoot));
  if( pTable->pFromTable ) oldRoot = pTable->pFromTable->root;
  if( pTable->pToTable ) newRoot = pTable->pToTable->root;
  fromFlags = pTable->pFromTable ? pTable->pFromTable->flags
                                 : pTable->pToTable->flags;
  toFlags = pTable->pToTable ? pTable->pToTable->flags
                             : pTable->pFromTable->flags;
  rc = prollyDiffIterOpen(&it,cs,pCache,&oldRoot,&newRoot,fromFlags,toFlags);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc=prollyDiffIterStep(&it,&pChange))==SQLITE_ROW ){
    const u8 *pOld = pChange->pOldVal;
    const u8 *pNew = pChange->pNewVal;
    int nOld = pChange->pOldVal ? pChange->nOldVal : 0;
    int nNew = pChange->pNewVal ? pChange->nNewVal : 0;
    u8 *pOldKey = 0, *pNewKey = 0;
    int nOldKey = 0, nNewKey = 0;
    if( pTable->pFromSchema && nOld==0 && pChange->type!=PROLLY_DIFF_ADD ){
      rc = doltliteRecordFromClusteredKey(pFrom->db,pTable->zFromName,
                pChange->pKey,pChange->nKey,&pOldKey,&nOldKey);
      if( rc!=SQLITE_OK ) goto data_step_done;
      if( pOldKey ){ pOld=pOldKey; nOld=nOldKey; }
    }
    if( pTable->pToSchema && nNew==0 && pChange->type!=PROLLY_DIFF_DELETE ){
      rc = doltliteRecordFromClusteredKey(pTo->db,pTable->zToName,
                pChange->pKey,pChange->nKey,&pNewKey,&nNewKey);
      if( rc!=SQLITE_OK ) goto data_step_done;
      if( pNewKey ){ pNew=pNewKey; nNew=nNewKey; }
    }
    if( pChange->type==PROLLY_DIFF_ADD ){
      rc = patchAppendInsert(pCur,pTable->zToName,pTo,pNew,nNew,pChange->intKey);
    }else if( pChange->type==PROLLY_DIFF_DELETE ){
      rc = patchAppendDelete(pCur,pTable->zToName,pFrom,pOld,nOld,pChange->intKey);
    }else{
      rc = patchAppendUpdate(pCur,pTable->zToName,pFrom,pTo,pOld,nOld,pNew,nNew,
                             pChange->intKey);
    }
data_step_done:
    sqlite3_free(pOldKey);
    sqlite3_free(pNewKey);
    if( rc!=SQLITE_OK ) break;
  }
  prollyDiffIterClose(&it);
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
}

static int patchAppendAllData(
  PatchCursor *pCur,
  sqlite3 *db,
  const PatchTable *pTable,
  const PatchSchema *pTo
){
  PatchTable allTo = *pTable;
  PatchSchema empty;
  memset(&empty,0,sizeof(empty));
  allTo.pFromSchema = 0;
  allTo.pFromTable = 0;
  allTo.zFromName = 0;
  return patchAppendData(pCur,db,&allTo,&empty,pTo);
}

static int patchTableUnchanged(
  const PatchTable*, SchemaEntry*, int, SchemaEntry*, int
);

static int patchGenerateTable(
  PatchCursor *pCur,
  sqlite3 *db,
  const PatchTable *pTable,
  SchemaEntry *aFromSchema, int nFromSchema,
  SchemaEntry *aToSchema, int nToSchema,
  int iTemp
){
  PatchSchema from, to;
  char *zNativeAlter = 0;
  int schemaChanged, pkChanged = 0;
  int rc = SQLITE_OK;
  memset(&from,0,sizeof(from));
  memset(&to,0,sizeof(to));
  if( patchTableUnchanged(pTable,aFromSchema,nFromSchema,
                          aToSchema,nToSchema) ){
    return SQLITE_OK;
  }
  if( pTable->pFromSchema ){
    rc = patchSchemaLoad(pTable->pFromSchema,&from);
    if( rc!=SQLITE_OK ) goto done;
  }
  if( pTable->pToSchema ){
    rc = patchSchemaLoad(pTable->pToSchema,&to);
    if( rc!=SQLITE_OK ) goto done;
  }
  if( !pTable->pToSchema ){
    char *zSql = sqlite3_mprintf("DROP TABLE \"%w\"",pTable->zFromName);
    if( !zSql ){ rc=SQLITE_NOMEM; goto done; }
    rc = patchAppendRow(pCur,pTable->zFromName,"schema",zSql);
    sqlite3_free(zSql);
    goto done;
  }
  if( !pTable->pFromSchema ){
    rc = patchAppendRow(pCur,pTable->zToName,"schema",pTable->pToSchema->zSql);
    if( rc==SQLITE_OK ) rc=patchAppendAssociated(pCur,pTable->zToName,
                                                  aToSchema,nToSchema);
    if( rc==SQLITE_OK ) rc=patchAppendData(pCur,db,pTable,&from,&to);
    goto done;
  }
  schemaChanged = strcmp(pTable->zFromName,pTable->zToName)!=0
      || strcmp(pTable->pFromSchema->zSql,pTable->pToSchema->zSql)!=0;
  if( schemaChanged ){
    pkChanged = pTable->pFromTable->flags!=pTable->pToTable->flags
             || patchPrimaryKeyChanged(&from,&to);
    rc = patchNativeAlter(pTable,&from,&to,&zNativeAlter);
    if( rc==SQLITE_OK && zNativeAlter ){
      rc = patchAppendObjectDrops(pCur,pTable->zFromName,
              aFromSchema,nFromSchema,aToSchema,nToSchema);
      if( rc==SQLITE_OK ){
        rc = patchAppendRow(pCur,pTable->zToName,"schema",zNativeAlter);
      }
      if( rc==SQLITE_OK ){
        rc = patchAppendIndexCreates(pCur,pTable->zToName,
                aFromSchema,nFromSchema,aToSchema,nToSchema);
      }
    }else if( rc==SQLITE_OK ){
      rc = patchAppendRebuild(pCur,pTable,&from,&to,aFromSchema,nFromSchema,
                              aToSchema,nToSchema,iTemp,!pkChanged);
    }
  }else{
    rc = patchAppendObjectDiffs(pCur,pTable->zToName,
           aFromSchema,nFromSchema,aToSchema,nToSchema);
  }
  if( rc==SQLITE_OK ){
    if( pkChanged ) rc=patchAppendAllData(pCur,db,pTable,&to);
    else rc=patchAppendData(pCur,db,pTable,&from,&to);
  }
done:
  sqlite3_free(zNativeAlter);
  patchSchemaClear(&from);
  patchSchemaClear(&to);
  return rc;
}

static int patchTableUnchanged(
  const PatchTable *pTable,
  SchemaEntry *aFromSchema, int nFromSchema,
  SchemaEntry *aToSchema, int nToSchema
){
  return pTable->pFromSchema && pTable->pToSchema
      && strcmp(pTable->zFromName,pTable->zToName)==0
      && strcmp(pTable->pFromSchema->zSql,pTable->pToSchema->zSql)==0
      && prollyHashCompare(&pTable->pFromTable->root,
                           &pTable->pToTable->root)==0
      && !patchObjectsDiffer(pTable->zToName,aFromSchema,nFromSchema,
                             aToSchema,nToSchema);
}

static const char *patchSchemaSql =
  "CREATE TABLE x("
  "statement_order INTEGER,"
  "from_commit_hash TEXT,"
  "to_commit_hash TEXT,"
  "table_name TEXT,"
  "diff_type TEXT,"
  "statement TEXT,"
  "arg1 TEXT HIDDEN,"
  "arg2 TEXT HIDDEN,"
  "arg3 TEXT HIDDEN)";

static int patchConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  PatchVtab *p;
  int rc;
  (void)pAux; (void)argc; (void)argv;
  rc = sqlite3_declare_vtab(db,patchSchemaSql);
  if( rc!=SQLITE_OK ){ *pzErr=sqlite3_mprintf("%s",sqlite3_errmsg(db)); return rc; }
  p = sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p,0,sizeof(*p));
  p->db=db;
  *ppVtab=&p->base;
  return SQLITE_OK;
}

static int patchBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i, iFrom=-1, iTo=-1, iTable=-1, iType=-1, iArg=1;
  (void)pVtab;
  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable
     || pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case 4: iType=i; break;
      case 6: iFrom=i; break;
      case 7: iTo=i; break;
      case 8: iTable=i; break;
    }
  }
  if( iFrom>=0 ){
    pInfo->aConstraintUsage[iFrom].argvIndex=iArg++;
    pInfo->aConstraintUsage[iFrom].omit=1;
  }
  if( iTo>=0 ){
    pInfo->aConstraintUsage[iTo].argvIndex=iArg++;
    pInfo->aConstraintUsage[iTo].omit=1;
  }
  if( iTable>=0 ){
    pInfo->aConstraintUsage[iTable].argvIndex=iArg++;
    pInfo->aConstraintUsage[iTable].omit=1;
  }
  if( iType>=0 ){
    pInfo->aConstraintUsage[iType].argvIndex=iArg++;
    pInfo->aConstraintUsage[iType].omit=1;
  }
  pInfo->idxNum=(iFrom>=0?1:0)|(iTo>=0?2:0)|(iTable>=0?4:0)|(iType>=0?8:0);
  pInfo->estimatedCost=1000.0;
  return SQLITE_OK;
}

static int patchOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCur){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCur,sizeof(PatchCursor));
}

static int patchClose(sqlite3_vtab_cursor *pCursor){
  PatchCursor *pCur=(PatchCursor*)pCursor;
  patchRowsClear(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int patchFilter(sqlite3_vtab_cursor *pCursor, int idxNum,
    const char *idxStr, int argc, sqlite3_value **argv){
  PatchCursor *pCur=(PatchCursor*)pCursor;
  PatchVtab *pVtab=(PatchVtab*)pCursor->pVtab;
  const char *zArg1=0,*zArg2=0,*zArg3=0,*zFrom=0,*zTo=0,*zFilter=0;
  const char *zTypeFilter=0;
  char *zLeft=0,*zRight=0;
  int rangeType=DOLTLITE_RANGE_NONE;
  ProllyHash fromCat,toCat;
  SchemaEntry *aFromSchema=0,*aToSchema=0;
  struct TableEntry *aFromTable=0,*aToTable=0;
  PatchTable *aTable=0;
  int nFromSchema=0,nToSchema=0,nFromTable=0,nToTable=0,nTable=0;
  int iArg=0,i,found=0,rc=SQLITE_OK;
  int bTypeFilter=(idxNum&8)!=0;
  ChunkStore *cs=doltliteGetChunkStore(pVtab->db);
  ProllyCache *pCache=doltliteGetCache(pVtab->db);
  (void)idxStr;
  patchRowsClear(pCur);
  if( !cs ) return SQLITE_OK;
  if( (idxNum&1) && iArg<argc ) zArg1=(const char*)sqlite3_value_text(argv[iArg++]);
  if( (idxNum&2) && iArg<argc ) zArg2=(const char*)sqlite3_value_text(argv[iArg++]);
  if( (idxNum&4) && iArg<argc ) zArg3=(const char*)sqlite3_value_text(argv[iArg++]);
  if( (idxNum&8) && iArg<argc ) zTypeFilter=(const char*)sqlite3_value_text(argv[iArg++]);
  if( !zArg1 || ((idxNum&2) && !zArg2) || ((idxNum&4) && !zArg3) ){
    patchSetError(&pVtab->base,"dolt_patch: invalid arguments%s","");
    return SQLITE_ERROR;
  }
  rc=doltliteSplitRevisionRange(zArg1,&zLeft,&zRight,&rangeType);
  if( rc==SQLITE_OK ){
    zFrom=zLeft; zTo=zRight; zFilter=zArg2;
    if( zArg3 ) rc=SQLITE_MISMATCH;
  }else if( rc==SQLITE_NOTFOUND ){
    rangeType=DOLTLITE_RANGE_NONE;
    zFrom=zArg1; zTo=zArg2; zFilter=zArg3;
    rc=zTo ? SQLITE_OK : SQLITE_MISMATCH;
  }
  if( rc!=SQLITE_OK ){
    patchSetError(&pVtab->base,"dolt_patch: invalid arguments near '%s'",zArg1);
    rc=SQLITE_ERROR; goto done;
  }
  rc=patchResolveArgs(pVtab,zFrom,zTo,rangeType,&fromCat,&toCat,
                      &pCur->zFrom,&pCur->zTo);
  if( rc!=SQLITE_OK ) goto done;
  rc=doltliteLoadCatalog(pVtab->db,&fromCat,&aFromTable,&nFromTable,0);
  if( rc==SQLITE_OK ) rc=doltliteLoadCatalog(pVtab->db,&toCat,&aToTable,&nToTable,0);
  if( rc==SQLITE_OK ) rc=loadSchemaFromCatalog(pVtab->db,cs,pCache,&fromCat,
                                               &aFromSchema,&nFromSchema);
  if( rc==SQLITE_OK ) rc=loadSchemaFromCatalog(pVtab->db,cs,pCache,&toCat,
                                               &aToSchema,&nToSchema);
  if( rc==SQLITE_OK ) rc=patchBuildTables(pVtab->db,aFromSchema,nFromSchema,
      aToSchema,nToSchema,aFromTable,nFromTable,aToTable,nToTable,&aTable,&nTable);
  if( rc==SQLITE_OK ){
    rc=patchAppendViewDrops(pCur,zFilter,aFromSchema,nFromSchema,
                            aToSchema,nToSchema,&found);
  }
  for(i=0; rc==SQLITE_OK && i<nTable; i++){
    if( zFilter && (!aTable[i].zFromName
                 || sqlite3_stricmp(zFilter,aTable[i].zFromName)!=0)
                && (!aTable[i].zToName
                 || sqlite3_stricmp(zFilter,aTable[i].zToName)!=0) ){
      continue;
    }
    found=1;
    rc=patchGenerateTable(pCur,pVtab->db,&aTable[i],aFromSchema,nFromSchema,
                          aToSchema,nToSchema,i+1);
  }
  /* Recreate triggers after all table data, or replayed DML would fire them. */
  for(i=0; rc==SQLITE_OK && i<nTable; i++){
    if( zFilter && (!aTable[i].zFromName
                 || sqlite3_stricmp(zFilter,aTable[i].zFromName)!=0)
                && (!aTable[i].zToName
                 || sqlite3_stricmp(zFilter,aTable[i].zToName)!=0) ){
      continue;
    }
    if( patchTableUnchanged(&aTable[i],aFromSchema,nFromSchema,
                            aToSchema,nToSchema) ) continue;
    rc=patchAppendTargetTriggers(pCur,&aTable[i],aToSchema,nToSchema);
  }
  if( rc==SQLITE_OK ){
    rc=patchAppendViewCreates(pCur,zFilter,aFromSchema,nFromSchema,
                              aToSchema,nToSchema,&found);
  }
  if( rc==SQLITE_OK && zFilter && !found ){
    patchSetError(&pVtab->base,"dolt_patch: table '%s' does not exist",zFilter);
    rc=SQLITE_ERROR;
  }
  if( rc==SQLITE_OK && bTypeFilter ){
    int iRead, iWrite=0;
    for(iRead=0; iRead<pCur->nRow; iRead++){
      PatchRow *pRow=&pCur->aRow[iRead];
      if( zTypeFilter && strcmp(pRow->zType,zTypeFilter)==0 ){
        if( iWrite!=iRead ){
          pCur->aRow[iWrite]=*pRow;
          memset(pRow,0,sizeof(*pRow));
        }
        iWrite++;
      }else{
        sqlite3_free(pRow->zTable);
        sqlite3_free(pRow->zType);
        sqlite3_free(pRow->zSql);
        memset(pRow,0,sizeof(*pRow));
      }
    }
    pCur->nRow=iWrite;
  }
done:
  sqlite3_free(zLeft); sqlite3_free(zRight); sqlite3_free(aTable);
  freeSchemaEntries(aFromSchema,nFromSchema);
  freeSchemaEntries(aToSchema,nToSchema);
  doltliteFreeCatalog(aFromTable,nFromTable);
  doltliteFreeCatalog(aToTable,nToTable);
  return rc;
}

static int patchNext(sqlite3_vtab_cursor *p){ ((PatchCursor*)p)->iRow++; return SQLITE_OK; }
static int patchEof(sqlite3_vtab_cursor *p){ PatchCursor *c=(PatchCursor*)p; return c->iRow>=c->nRow; }
static int patchColumn(sqlite3_vtab_cursor *p, sqlite3_context *ctx, int iCol){
  PatchCursor *c=(PatchCursor*)p;
  PatchRow *r=&c->aRow[c->iRow];
  switch(iCol){
    case 0: sqlite3_result_int64(ctx,c->iRow+1); break;
    case 1: sqlite3_result_text(ctx,c->zFrom,-1,SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx,c->zTo,-1,SQLITE_TRANSIENT); break;
    case 3: sqlite3_result_text(ctx,r->zTable,-1,SQLITE_TRANSIENT); break;
    case 4: sqlite3_result_text(ctx,r->zType,-1,SQLITE_TRANSIENT); break;
    case 5: sqlite3_result_text(ctx,r->zSql,-1,SQLITE_TRANSIENT); break;
  }
  return SQLITE_OK;
}
static int patchRowid(sqlite3_vtab_cursor *p, sqlite3_int64 *pRowid){
  *pRowid=((PatchCursor*)p)->iRow+1; return SQLITE_OK;
}

static sqlite3_module patchModule = {
  0,0,patchConnect,patchBestIndex,doltliteVtabDisconnect,0,
  patchOpen,patchClose,patchFilter,patchNext,patchEof,patchColumn,patchRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltlitePatchRegister(sqlite3 *db){
  return sqlite3_create_module(db,"dolt_patch",&patchModule,0);
}

#endif
