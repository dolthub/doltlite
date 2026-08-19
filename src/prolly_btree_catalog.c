#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Catalog serialization, schema metadata, and schema invalidation. */

u32 prollyBtreeGetU32LE(const u8 *p){
  return ((u32)p[0]) | ((u32)p[1]<<8) | ((u32)p[2]<<16) | ((u32)p[3]<<24);
}

static void putU32LE(u8 *p, u32 v){
  p[0] = (u8)v;
  p[1] = (u8)(v>>8);
  p[2] = (u8)(v>>16);
  p[3] = (u8)(v>>24);
}

static int btreeSchemaIndex(Btree *pBtree){
  sqlite3 *db;
  int i;
  if( !pBtree || !(db = pBtree->db) ) return -1;
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].pBt==pBtree ) return i;
  }
  return -1;
}

static const char *btreeSchemaName(Btree *pBtree){
  sqlite3 *db = pBtree ? pBtree->db : 0;
  int iDb = btreeSchemaIndex(pBtree);
  if( !db || iDb<0 || iDb>=db->nDb ) return "main";
  return db->aDb[iDb].zDbSName ? db->aDb[iDb].zDbSName : "main";
}

void catRemove(Catalog *cat, Pgno iTable){
  int i;
  assert( cat!=0 );
  for(i=0; i<cat->n; i++){
    if( cat->a[i].iTable==iTable ){
      sqlite3_free(cat->a[i].zName);
      if( cat->a[i].pPending ){
        ProllyMutMap *pMap = (ProllyMutMap*)cat->a[i].pPending;
        prollyMutMapFree(pMap);
        sqlite3_free(pMap);
        cat->a[i].pPending = 0;
      }
      if( i<cat->n-1 ){
        memmove(&cat->a[i], &cat->a[i+1],
                (cat->n-i-1)*(int)sizeof(struct TableEntry));
      }
      cat->n--;
#ifndef NDEBUG
      for(i=1; i<cat->n; i++){
        assert( cat->a[i-1].iTable < cat->a[i].iTable );
      }
#endif
      return;
    }
  }
}

void catFree(Catalog *cat){
  int k;
  assert( cat!=0 );
  for(k=0; k<cat->n; k++){
    sqlite3_free(cat->a[k].zName);
    if( cat->a[k].pPending ){
      ProllyMutMap *pMap = (ProllyMutMap*)cat->a[k].pPending;
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
      cat->a[k].pPending = 0;
    }
  }
  sqlite3_free(cat->a);
  cat->a = 0;
  cat->n = 0;
  cat->nAlloc = 0;
}

void invalidateSchema(Btree *pBtree){
  assert( pBtree!=0 );
  if( pBtree->pSchema && pBtree->xFreeSchema ){
    pBtree->xFreeSchema(pBtree->pSchema);
  }
}

void resetConnectionSchema(Btree *pBtree){
  assert( pBtree!=0 );
  invalidateSchema(pBtree);
  if( pBtree->db ){
    sqlite3ExpirePreparedStatements(pBtree->db, 0);
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }
}

static int hasActiveSchemaProgram(Btree *pBtree){
  Vdbe *pVdbe;
  if( !pBtree->db ) return 0;
  for(pVdbe=pBtree->db->pVdbe; pVdbe; pVdbe=pVdbe->pVNext){
    int i;
    if( pVdbe->eVdbeState!=VDBE_RUN_STATE ) continue;
    for(i=0; i<pVdbe->nOp; i++){
      int op = pVdbe->aOp[i].opcode;
      if( op==OP_SetCookie
       || op==OP_ParseSchema
       || op==OP_CreateBtree
       || op==OP_Destroy
       || op==OP_DropTable
       || op==OP_DropIndex ){
        return 1;
      }
    }
  }
  return 0;
}

int rollbackNeedsSchemaReset(Btree *pBtree){
  return pBtree->bSchemaChangedTxn
      || pBtree->bMasterRootChangedTxn
      || (pBtree->db
          && pBtree->db->init.busy==0
          && (pBtree->db->mDbFlags & DBFLAG_SchemaChange)!=0)
      || hasActiveSchemaProgram(pBtree);
}

void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode){
  BtCursor *p;
  assert( pBt!=0 );
  for(p=pBt->pCursor; p; p=p->pNext){
    if( iTable==0 || p->pgnoRoot==iTable ){
      p->eState = CURSOR_FAULT;
      p->skipNext = errCode;
      p->mmActive = 0;
      p->mmPhysActive = 0;
      p->mmIdx = -1;
      p->mmPhysIdx = -1;
      prollyCursorReleaseAll(&p->pCur);
    }
  }
}

/* Stock invalidateIncrblobCursors: a SQL-level write to a row aborts any
** incrblob handle open on it. Blob-handle writes also route through
** sqlite3BtreeInsert here (stock patches pages in place), so callers skip
** this when the writer is itself an incrblob cursor. */
void prollyInvalidateIncrblobCursors(BtShared *pBt, Pgno pgnoRoot,
                                      i64 iRow, int isClearTable){
  BtCursor *p;
  for(p=pBt->pCursor; p; p=p->pNext){
    if( (p->curFlags & BTCF_Incrblob)==0 || p->pgnoRoot!=pgnoRoot ) continue;
    if( !isClearTable ){
      if( p->eState==CURSOR_REQUIRESEEK ){
        if( p->nKey!=iRow ) continue;
      }else if( p->curFlags & BTCF_ValidNKey ){
        if( p->cachedIntKey!=iRow ) continue;
      }
    }
    p->eState = CURSOR_FAULT;
    p->skipNext = SQLITE_ABORT;
    p->mmActive = 0;
    p->mmPhysActive = 0;
    p->mmIdx = -1;
    p->mmPhysIdx = -1;
    prollyCursorReleaseAll(&p->pCur);
  }
}

void refreshCursorRoot(BtCursor *pCur){
  struct TableEntry *pTE;
  PROLLY_ASSERT_CURSOR_OWNED(pCur);
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE ){
    pCur->pCur.root = pTE->root;
  }
}

typedef struct CatalogEntryMeta CatalogEntryMeta;
struct CatalogEntryMeta {
  Pgno iTable;
  Pgno iPersistTable;
  char *zType;
  char *zName;
  char *zTblName;
  ProllyHash schemaHash;
};

typedef struct CatalogSerializeEntry CatalogSerializeEntry;
struct CatalogSerializeEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  const char *zType;
  const char *zName;
  const char *zTblName;
};

typedef struct CatalogPgnoRef CatalogPgnoRef;
struct CatalogPgnoRef {
  Pgno iTable;
  int iEntry;
};

typedef struct CatalogNameRef CatalogNameRef;
struct CatalogNameRef {
  const char *zName;
  int iEntry;
};

#define CATALOG_INDEX_MIN_ENTRIES 32

static int catalogNameRefCmp(const void *a, const void *b){
  const CatalogNameRef *ea = (const CatalogNameRef*)a;
  const CatalogNameRef *eb = (const CatalogNameRef*)b;
  int c = strcmp(ea->zName, eb->zName);
  return c ? c : ea->iEntry - eb->iEntry;
}

static int catalogNameRefFind(
  CatalogNameRef *aRef,
  int nRef,
  struct TableEntry *aEntry,
  int nEntry,
  const char *zName
){
  int lo = 0;
  int hi = nRef;
  int i;
  if( !zName ) return -1;
  if( !aRef ){
    for(i=0; i<nEntry; i++){
      if( aEntry[i].zName && strcmp(aEntry[i].zName, zName)==0 ) return i;
    }
    return -1;
  }
  while( lo<hi ){
    int mid = lo + (hi - lo) / 2;
    if( strcmp(aRef[mid].zName, zName)<0 ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return lo<nRef && strcmp(aRef[lo].zName, zName)==0 ? aRef[lo].iEntry : -1;
}

static int catalogPgnoRefCmp(const void *a, const void *b){
  const CatalogPgnoRef *ea = (const CatalogPgnoRef*)a;
  const CatalogPgnoRef *eb = (const CatalogPgnoRef*)b;
  if( ea->iTable < eb->iTable ) return -1;
  if( ea->iTable > eb->iTable ) return 1;
  return ea->iEntry - eb->iEntry;
}

static int catalogPgnoRefFind(CatalogPgnoRef *aRef, int nRef, Pgno iTable){
  int lo = 0;
  int hi = nRef;
  while( lo<hi ){
    int mid = lo + (hi - lo) / 2;
    if( aRef[mid].iTable < iTable ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return lo<nRef && aRef[lo].iTable==iTable ? lo : -1;
}

static void freeCatalogEntryMeta(CatalogEntryMeta *aMeta, int nMeta){
  int i;
  for(i=0; i<nMeta; i++){
    sqlite3_free(aMeta[i].zType);
    sqlite3_free(aMeta[i].zName);
    sqlite3_free(aMeta[i].zTblName);
  }
  sqlite3_free(aMeta);
}

static int catalogEntryMetaCmp(const void *a, const void *b){
  const CatalogEntryMeta *ea = (const CatalogEntryMeta*)a;
  const CatalogEntryMeta *eb = (const CatalogEntryMeta*)b;
  int c = strcmp(ea->zType ? ea->zType : "", eb->zType ? eb->zType : "");
  if( c ) return c;
  c = strcmp(ea->zTblName ? ea->zTblName : "", eb->zTblName ? eb->zTblName : "");
  if( c ) return c;
  c = strcmp(ea->zName ? ea->zName : "", eb->zName ? eb->zName : "");
  if( c ) return c;
  if( ea->iTable < eb->iTable ) return -1;
  if( ea->iTable > eb->iTable ) return 1;
  return 0;
}

static const CatalogEntryMeta *findCatalogEntryMetaByPgno(
  CatalogEntryMeta *aMeta,
  int nMeta,
  Pgno iTable
){
  int i;
  for(i=0; i<nMeta; i++){
    if( aMeta[i].iTable==iTable ) return &aMeta[i];
  }
  return 0;
}

static int addCatalogEntryMeta(
  CatalogEntryMeta **paMeta,
  int *pnMeta,
  int *pnAlloc,
  Pgno iTable,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  CatalogEntryMeta *aMeta = *paMeta;
  CatalogEntryMeta *pNew;
  if( findCatalogEntryMetaByPgno(aMeta, *pnMeta, iTable) ) return SQLITE_OK;
  if( *pnMeta>=*pnAlloc ){
    i64 nNew = *pnAlloc ? (i64)*pnAlloc * 2 : (i64)16;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(CatalogEntryMeta) ){
      return SQLITE_NOMEM;
    }
    pNew = sqlite3_realloc(aMeta, (int)(nNew * (i64)sizeof(CatalogEntryMeta)));
    if( !pNew ) return SQLITE_NOMEM;
    aMeta = pNew;
    *paMeta = aMeta;
    *pnAlloc = (int)nNew;
  }
  memset(&aMeta[*pnMeta], 0, sizeof(CatalogEntryMeta));
  aMeta[*pnMeta].iTable = iTable;
  aMeta[*pnMeta].iPersistTable = iTable;
  aMeta[*pnMeta].zType = sqlite3_mprintf("%s", zType ? zType : "");
  aMeta[*pnMeta].zName = sqlite3_mprintf("%s", zName ? zName : "");
  aMeta[*pnMeta].zTblName = sqlite3_mprintf("%s", zTblName ? zTblName : "");
  if( !aMeta[*pnMeta].zType || !aMeta[*pnMeta].zName || !aMeta[*pnMeta].zTblName ){
    sqlite3_free(aMeta[*pnMeta].zType);
    sqlite3_free(aMeta[*pnMeta].zName);
    sqlite3_free(aMeta[*pnMeta].zTblName);
    memset(&aMeta[*pnMeta], 0, sizeof(CatalogEntryMeta));
    return SQLITE_NOMEM;
  }
  (*pnMeta)++;
  return SQLITE_OK;
}

static int buildLiveCatalogEntryMeta(Btree *pBtree, CatalogEntryMeta **ppMeta, int *pnMeta){
  sqlite3 *db;
  Schema *pSchema;
  HashElem *k;
  CatalogNameRef *aNameRef = 0;
  CatalogEntryMeta *aMeta = 0;
  int nNameRef = 0, nMeta = 0, nAlloc = 0, rc = SQLITE_OK, i;
  if( !pBtree || !(db = pBtree->db) || db->nDb<=0 ){
    *ppMeta = 0;
    *pnMeta = 0;
    return SQLITE_OK;
  }
  i = btreeSchemaIndex(pBtree);
  if( i<0 || i>=db->nDb || !(pSchema = db->aDb[i].pSchema) ){
    *ppMeta = 0;
    *pnMeta = 0;
    return SQLITE_OK;
  }
  if( pBtree->cat.n>=CATALOG_INDEX_MIN_ENTRIES ){
    aNameRef = sqlite3_malloc(pBtree->cat.n * (int)sizeof(CatalogNameRef));
    if( !aNameRef ) return SQLITE_NOMEM;
    for(i=0; i<pBtree->cat.n; i++){
      if( !pBtree->cat.a[i].zName ) continue;
      aNameRef[nNameRef].zName = pBtree->cat.a[i].zName;
      aNameRef[nNameRef].iEntry = i;
      nNameRef++;
    }
    qsort(aNameRef, nNameRef, sizeof(CatalogNameRef), catalogNameRefCmp);
  }
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    Index *pIdx;
    Pgno iTable = 0;
    int iEntry;
    if( !pTab ) continue;
    iEntry = catalogNameRefFind(aNameRef, nNameRef, pBtree->cat.a,
                                pBtree->cat.n, pTab->zName);
    if( iEntry>=0 ) iTable = pBtree->cat.a[iEntry].iTable;
    if( iTable==0 ) iTable = pTab->tnum;
    if( iTable>1 ){
      rc = addCatalogEntryMeta(&aMeta, &nMeta, &nAlloc, iTable,
                               "table", pTab->zName, "");
      if( rc!=SQLITE_OK ) goto done;
    }
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      Pgno iIndexTable = 0;
      iEntry = catalogNameRefFind(aNameRef, nNameRef, pBtree->cat.a,
                                  pBtree->cat.n, pIdx->zName);
      if( iEntry>=0 ) iIndexTable = pBtree->cat.a[iEntry].iTable;
      if( iIndexTable==0 ) iIndexTable = pIdx->tnum;
      if( iIndexTable<=1 ) continue;
      if( iIndexTable==iTable ) continue;
      rc = addCatalogEntryMeta(&aMeta, &nMeta, &nAlloc, iIndexTable, "index",
                               pIdx->zName, pTab->zName);
      if( rc!=SQLITE_OK ) goto done;
    }
  }
  if( nMeta>1 ){
    qsort(aMeta, nMeta, sizeof(CatalogEntryMeta), catalogEntryMetaCmp);
  }
  for(i=0; i<nMeta; i++){
    aMeta[i].iPersistTable = 0;
  }
done:
  sqlite3_free(aNameRef);
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  *ppMeta = aMeta;
  *pnMeta = nMeta;
  return SQLITE_OK;
}

static int catalogSerializeEntryCmp(const void *a, const void *b){
  const CatalogSerializeEntry *ea = (const CatalogSerializeEntry*)a;
  const CatalogSerializeEntry *eb = (const CatalogSerializeEntry*)b;
  int c = strcmp(ea->zType ? ea->zType : "", eb->zType ? eb->zType : "");
  if( c ) return c;
  c = strcmp(ea->zTblName ? ea->zTblName : "", eb->zTblName ? eb->zTblName : "");
  if( c ) return c;
  c = strcmp(ea->zName ? ea->zName : "", eb->zName ? eb->zName : "");
  if( c ) return c;
  if( ea->iTable < eb->iTable ) return -1;
  if( ea->iTable > eb->iTable ) return 1;
  return 0;
}

typedef struct SchemaCatalogRow SchemaCatalogRow;
struct SchemaCatalogRow {
  i64 iRowid;
  Pgno oldPg;
  Pgno newPg;
  char *zType;
  char *zName;
  char *zTblName;
  char *zSql;
};

static int schemaCatalogHasPgno(SchemaCatalogRow *aRows, int nRows, Pgno iTable){
  int i;
  for(i=0; i<nRows; i++){
    if( aRows[i].oldPg==iTable ) return 1;
  }
  return 0;
}

static void freeSchemaCatalogRows(SchemaCatalogRow *aRows, int nRows){
  int i;
  for(i=0; i<nRows; i++){
    sqlite3_free(aRows[i].zType);
    sqlite3_free(aRows[i].zName);
    sqlite3_free(aRows[i].zTblName);
    sqlite3_free(aRows[i].zSql);
  }
  sqlite3_free(aRows);
}

/* Reload order must place each object after everything it references: tables,
** then their indexes, then views, then triggers (which fire on tables/views). */
static int schemaTypeRank(const char *zType){
  if( strcmp(zType, "table")==0 ) return 0;
  if( strcmp(zType, "index")==0 ) return 1;
  if( strcmp(zType, "view")==0 )  return 2;
  return 3;
}

static int schemaCatalogRowCmp(const void *a, const void *b){
  const SchemaCatalogRow *ra = (const SchemaCatalogRow*)a;
  const SchemaCatalogRow *rb = (const SchemaCatalogRow*)b;
  const char *za = ra->zType ? ra->zType : "";
  const char *zb = rb->zType ? rb->zType : "";
  int c = schemaTypeRank(za) - schemaTypeRank(zb);
  if( c ) return c;
  c = strcmp(ra->zTblName ? ra->zTblName : "", rb->zTblName ? rb->zTblName : "");
  if( c ) return c;
  c = strcmp(ra->zName ? ra->zName : "", rb->zName ? rb->zName : "");
  if( c ) return c;
  if( ra->iRowid < rb->iRowid ) return -1;
  if( ra->iRowid > rb->iRowid ) return 1;
  return 0;
}

static int schemaCatalogRowIsVirtualTable(const SchemaCatalogRow *pRow){
  const char *zSql;
  if( !pRow || !pRow->zType || !pRow->zSql ) return 0;
  if( strcmp(pRow->zType, "table")!=0 ) return 0;
  zSql = pRow->zSql;
  while( zSql[0]==' ' || zSql[0]=='\t' || zSql[0]=='\n' || zSql[0]=='\r' ){
    zSql++;
  }
  return sqlite3_strnicmp(zSql, "CREATE VIRTUAL TABLE ", 21)==0;
}

static int schemaNameIsInternalAutoindex(const char *zName){
  return zName && strncmp(zName, "sqlite_autoindex_", 17)==0;
}

typedef struct SchemaFieldValue SchemaFieldValue;
struct SchemaFieldValue {
  int eType;
  i64 i;
  const void *p;
  int n;
};

static u32 schemaCatalogSerialType(const SchemaFieldValue *pMem, u32 *pLen){
  if( pMem->eType == SQLITE_NULL ){ *pLen = 0; return 0; }
  if( pMem->eType == SQLITE_INTEGER ){
    i64 v = pMem->i;
    if( v==0 ){ *pLen = 0; return 8; }
    if( v==1 ){ *pLen = 0; return 9; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return 1; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return 2; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return 3; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return 4; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return 5; }
    *pLen = 8; return 6;
  }
  if( pMem->eType == SQLITE_TEXT ){
    *pLen = (u32)pMem->n;
    return (u32)(pMem->n * 2 + 13);
  }
  *pLen = 0;
  return 0;
}

static void schemaCatalogWriteIntBe(u8 *pOut, i64 v, int nByte){
  int i;
  for(i=nByte-1; i>=0; i--){
    pOut[i] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

static void schemaCatalogSerialPut(u8 *pOut, const SchemaFieldValue *pMem, u32 serialType){
  switch( serialType ){
    case 0:
    case 8:
    case 9:
      return;
    case 1: schemaCatalogWriteIntBe(pOut, pMem->i, 1); return;
    case 2: schemaCatalogWriteIntBe(pOut, pMem->i, 2); return;
    case 3: schemaCatalogWriteIntBe(pOut, pMem->i, 3); return;
    case 4: schemaCatalogWriteIntBe(pOut, pMem->i, 4); return;
    case 5: schemaCatalogWriteIntBe(pOut, pMem->i, 6); return;
    case 6: schemaCatalogWriteIntBe(pOut, pMem->i, 8); return;
    default:
      if( serialType>=13 ) memcpy(pOut, pMem->p, (size_t)pMem->n);
      return;
  }
}

static u8 *buildSchemaCatalogRecord(
  const char *zType,
  const char *zName,
  const char *zTblName,
  i64 iRootpage,
  const char *zSql,
  int *pnOut
){
  SchemaFieldValue aMem[5];
  u32 aType[5];
  u32 aLen[5];
  int i, hdrSize = 0, bodySize = 0, pos;
  u8 *pOut, *pHdr, *pBody;

  memset(aMem, 0, sizeof(aMem));
  *pnOut = 0;
  aMem[0].eType = SQLITE_TEXT;    aMem[0].p = zType;    aMem[0].n = (int)strlen(zType);
  aMem[1].eType = SQLITE_TEXT;    aMem[1].p = zName;    aMem[1].n = (int)strlen(zName);
  aMem[2].eType = SQLITE_TEXT;    aMem[2].p = zTblName; aMem[2].n = (int)strlen(zTblName);
  aMem[3].eType = SQLITE_INTEGER; aMem[3].i = iRootpage;
  if( zSql ){
    aMem[4].eType = SQLITE_TEXT;
    aMem[4].p = zSql;
    aMem[4].n = (int)strlen(zSql);
  }else{
    aMem[4].eType = SQLITE_NULL;
  }

  for(i=0; i<5; i++){
    aType[i] = schemaCatalogSerialType(&aMem[i], &aLen[i]);
    hdrSize += sqlite3VarintLen(aType[i]);
    bodySize += (int)aLen[i];
  }
  hdrSize += sqlite3VarintLen(hdrSize);
  pOut = sqlite3_malloc(hdrSize + bodySize);
  if( !pOut ) return 0;
  pos = sqlite3PutVarint(pOut, hdrSize);
  pHdr = pOut + pos;
  pBody = pOut + hdrSize;
  for(i=0; i<5; i++){
    pHdr += sqlite3PutVarint(pHdr, aType[i]);
    if( aLen[i]>0 ){
      schemaCatalogSerialPut(pBody, &aMem[i], aType[i]);
      pBody += aLen[i];
    }
  }
  *pnOut = hdrSize + bodySize;
  return pOut;
}

static int loadSchemaCatalogRows(
  Btree *pBtree,
  struct TableEntry *aTables,
  int nTables,
  SchemaCatalogRow **ppRows,
  int *pnRows,
  ProllyHash *pMasterRoot,
  u8 *pMasterFlags
){
  ChunkStore *cs = &pBtree->pBt->store;
  ProllyCache *pCache = &pBtree->pBt->cache;
  ProllyCursor cur;
  SchemaCatalogRow *aRows = 0;
  int nRows = 0, nAlloc = 0, i, rc, res;

  *ppRows = 0;
  *pnRows = 0;
  memset(pMasterRoot, 0, sizeof(*pMasterRoot));
  *pMasterFlags = 0;
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==1 ){
      *pMasterRoot = aTables[i].root;
      *pMasterFlags = aTables[i].flags;
      break;
    }
  }
  if( prollyHashIsEmpty(pMasterRoot) ) return SQLITE_OK;

  prollyCursorInit(&cur, cs, pCache, pMasterRoot, *pMasterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal;
    int nVal;
    DoltliteRecordInfo ri;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && nVal>0 ){
      char *zType = 0, *zName = 0, *zTblName = 0, *zSql = 0;
      i64 iRootpage = 0;
      doltliteParseRecord(pVal, nVal, &ri);
      if( ri.nField<5 ){ rc = SQLITE_CORRUPT; break; }
      rc = dlRecordTextField(pVal, nVal, &ri, 0, &zType);
      if( rc!=SQLITE_OK ) break;
      rc = dlRecordTextField(pVal, nVal, &ri, 1, &zName);
      if( rc!=SQLITE_OK ){ sqlite3_free(zType); break; }
      rc = dlRecordTextField(pVal, nVal, &ri, 2, &zTblName);
      if( rc!=SQLITE_OK ){ sqlite3_free(zType); sqlite3_free(zName); break; }
      iRootpage = dlRecordIntField(pVal, nVal, &ri, 3);
      rc = dlRecordTextField(pVal, nVal, &ri, 4, &zSql);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTblName); break;
      }
      if( nRows>=nAlloc ){
        i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
        SchemaCatalogRow *aNew;
        if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
          sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTblName); sqlite3_free(zSql);
          rc = SQLITE_NOMEM;
          break;
        }
        aNew = sqlite3_realloc(aRows,
                               (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
        if( !aNew ){
          sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTblName); sqlite3_free(zSql);
          rc = SQLITE_NOMEM;
          break;
        }
        aRows = aNew;
        nAlloc = (int)nNew;
      }
      memset(&aRows[nRows], 0, sizeof(SchemaCatalogRow));
      aRows[nRows].iRowid = prollyCursorIntKey(&cur);
      aRows[nRows].oldPg = (Pgno)iRootpage;
      aRows[nRows].zType = zType;
      aRows[nRows].zName = zName;
      aRows[nRows].zTblName = zTblName;
      aRows[nRows].zSql = zSql;
      nRows++;
    }
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK ){
    freeSchemaCatalogRows(aRows, nRows);
    return rc;
  }
  *ppRows = aRows;
  *pnRows = nRows;
  return SQLITE_OK;
}

static int schemaCatalogRowWanted(
  const SchemaCatalogRow *pRow,
  struct TableEntry *aTables,
  int nTables
){
  int i;
  if( !pRow ) return 0;
  if( !pRow->zType ) return 0;
  if( schemaCatalogRowIsVirtualTable(pRow) ) return 1;
  if( strcmp(pRow->zType, "table")!=0 && strcmp(pRow->zType, "index")!=0 ){
    return 1;
  }
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==pRow->oldPg ) return 1;
  }
  if( pRow->zType && strcmp(pRow->zType, "table")==0 && pRow->zName ){
    for(i=0; i<nTables; i++){
      if( aTables[i].zName && strcmp(aTables[i].zName, pRow->zName)==0 ){
        return 1;
      }
    }
  }
  if( pRow->zType && strcmp(pRow->zType, "index")==0 ){
    for(i=0; i<nTables; i++){
      if( !aTables[i].zName ) continue;
      if( pRow->zName && strcmp(aTables[i].zName, pRow->zName)==0 ){
        return 1;
      }
      if( schemaNameIsInternalAutoindex(pRow->zName)
       && pRow->zTblName
       && strcmp(aTables[i].zName, pRow->zTblName)==0 ){
        return 1;
      }
    }
  }
  return 0;
}

static void filterSchemaCatalogRows(
  SchemaCatalogRow *aRows,
  int *pnRows,
  struct TableEntry *aTables,
  int nTables
){
  int i, nOut = 0;
  int nRows = *pnRows;
  for(i=0; i<nRows; i++){
    if( schemaCatalogRowWanted(&aRows[i], aTables, nTables) ){
      if( nOut!=i ){
        aRows[nOut] = aRows[i];
        memset(&aRows[i], 0, sizeof(aRows[i]));
      }
      nOut++;
    }else{
      sqlite3_free(aRows[i].zType);
      sqlite3_free(aRows[i].zName);
      sqlite3_free(aRows[i].zTblName);
      sqlite3_free(aRows[i].zSql);
      memset(&aRows[i], 0, sizeof(aRows[i]));
    }
  }
  *pnRows = nOut;
}

static int appendMissingSchemaCatalogRows(
  sqlite3 *db,
  const char *zDb,
  SchemaCatalogRow **paRows,
  int *pnRows,
  CatalogEntryMeta *aMeta,
  int nMeta,
  struct TableEntry *aTables,
  int nTables
){
  SchemaCatalogRow *aRows = *paRows;
  int nRows = *pnRows;
  int nAlloc = nRows;
  i64 iNextRowid = 1;
  int i, j, rc;

  for(i=0; i<nRows; i++){
    if( aRows[i].iRowid >= iNextRowid ) iNextRowid = aRows[i].iRowid + 1;
  }

  for(i=0; i<nMeta; i++){
    SchemaCatalogRow *pRow;
    int wanted = 0;
    if( schemaCatalogHasPgno(aRows, nRows, aMeta[i].iTable) ) continue;
    if( !aMeta[i].zType || !aMeta[i].zName || !aMeta[i].zTblName ) continue;
    if( strcmp(aMeta[i].zType, "table")!=0 && strcmp(aMeta[i].zType, "index")!=0 ){
      continue;
    }
    /* Live numbers can coincide with entries restored from another domain
    ** (a reset rename). Adopting a live index row whose parent table is
    ** not part of this catalog would retarget the index at a table the
    ** catalog does not contain; its correct row comes from the fallback. */
    if( strcmp(aMeta[i].zType, "index")==0 ){
      int parentHere = 0;
      for(j=0; j<nRows && !parentHere; j++){
        if( aRows[j].zType && strcmp(aRows[j].zType, "table")==0
         && aRows[j].zName && strcmp(aRows[j].zName, aMeta[i].zTblName)==0 ){
          parentHere = 1;
        }
      }
      for(j=0; j<nTables && !parentHere; j++){
        if( aTables[j].zName
         && strcmp(aTables[j].zName, aMeta[i].zTblName)==0 ){
          parentHere = 1;
        }
      }
      if( !parentHere ) continue;
    }
    for(j=0; j<nTables; j++){
      if( aTables[j].iTable==aMeta[i].iTable ){
        wanted = 1;
        break;
      }
      if( aTables[j].zName==0 ) continue;
      if( strcmp(aMeta[i].zType, "table")==0 ){
        if( strcmp(aTables[j].zName, aMeta[i].zName)==0 ){
          wanted = 1;
          break;
        }
      }else if( strcmp(aMeta[i].zType, "index")==0 ){
        if( strcmp(aTables[j].zName, aMeta[i].zName)==0
         || (schemaNameIsInternalAutoindex(aMeta[i].zName)
             && strcmp(aTables[j].zName, aMeta[i].zTblName)==0) ){
          wanted = 1;
          break;
        }
      }
    }
    if( !wanted ) continue;
    if( nRows>=nAlloc ){
      i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
      SchemaCatalogRow *aNew;
      if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aNew = sqlite3_realloc(aRows,
                             (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
      if( !aNew ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aRows = aNew;
      nAlloc = (int)nNew;
    }
    pRow = &aRows[nRows];
    memset(pRow, 0, sizeof(*pRow));
    pRow->iRowid = iNextRowid++;
    pRow->oldPg = aMeta[i].iTable;
    pRow->zType = sqlite3_mprintf("%s", aMeta[i].zType);
    pRow->zName = sqlite3_mprintf("%s", aMeta[i].zName);
    pRow->zTblName = sqlite3_mprintf("%s", aMeta[i].zTblName);
    pRow->zSql = 0;
    if( !pRow->zType || !pRow->zName || !pRow->zTblName ){
      sqlite3_free(pRow->zType);
      sqlite3_free(pRow->zName);
      sqlite3_free(pRow->zTblName);
      for(j=0; j<nRows; j++){
        sqlite3_free(aRows[j].zType);
        sqlite3_free(aRows[j].zName);
        sqlite3_free(aRows[j].zTblName);
        sqlite3_free(aRows[j].zSql);
      }
      sqlite3_free(aRows);
      return SQLITE_NOMEM;
    }
    rc = doltliteLoadLiveSchemaSql(db, pRow->zType, zDb, pRow->zName,
                                   pRow->zTblName, &pRow->zSql);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aRows, nRows+1);
      return rc;
    }
    nRows++;
  }

  *paRows = aRows;
  *pnRows = nRows;
  return SQLITE_OK;
}

static int appendFallbackSchemaCatalogRows(
  SchemaCatalogRow **paRows,
  int *pnRows,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallback,
  int nFallback
){
  SchemaCatalogRow *aRows = *paRows;
  int nRows = *pnRows;
  int nAlloc = nRows;
  i64 iNextRowid = 1;
  int i, j;

  if( !aFallback || nFallback<=0 ) return SQLITE_OK;
  for(i=0; i<nRows; i++){
    if( aRows[i].iRowid >= iNextRowid ) iNextRowid = aRows[i].iRowid + 1;
  }

  for(i=0; i<nTables; i++){
    SchemaEntry *pSe = 0;
    SchemaCatalogRow *pRow;
    if( aTables[i].iTable<=1 ) continue;
    if( schemaCatalogHasPgno(aRows, nRows, aTables[i].iTable) ) continue;
    for(j=0; j<nFallback; j++){
      if( !aFallback[j].zName || !aFallback[j].zType ) continue;
      if( aTables[i].zName ){
        if( strcmp(aFallback[j].zName, aTables[i].zName)!=0 ) continue;
      }else{
        if( strcmp(aFallback[j].zType, "index")!=0 ) continue;
        if( aFallback[j].iRootpage!=aTables[i].iTable ) continue;
      }
      pSe = &aFallback[j];
      break;
    }
    if( !pSe ) continue;
    if( nRows>=nAlloc ){
      i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
      SchemaCatalogRow *aNew;
      if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aNew = sqlite3_realloc(aRows,
                             (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
      if( !aNew ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aRows = aNew;
      nAlloc = (int)nNew;
    }
    pRow = &aRows[nRows];
    memset(pRow, 0, sizeof(*pRow));
    pRow->iRowid = iNextRowid++;
    pRow->oldPg = aTables[i].iTable;
    pRow->zType = sqlite3_mprintf("%s", pSe->zType ? pSe->zType : "");
    pRow->zName = sqlite3_mprintf("%s", pSe->zName ? pSe->zName : "");
    pRow->zTblName = sqlite3_mprintf("%s", pSe->zTblName ? pSe->zTblName : "");
    pRow->zSql = pSe->zSql ? sqlite3_mprintf("%s", pSe->zSql) : 0;
    if( !pRow->zType || !pRow->zName || !pRow->zTblName || (pSe->zSql && !pRow->zSql) ){
      freeSchemaCatalogRows(aRows, nRows+1);
      return SQLITE_NOMEM;
    }
    nRows++;
  }

  for(i=0; i<nFallback; i++){
    SchemaEntry *pSe = &aFallback[i];
    SchemaCatalogRow *pRow;
    int exists = 0;
    if( !pSe->zName || !pSe->zType ) continue;
    if( strcmp(pSe->zType, "table")==0 || strcmp(pSe->zType, "index")==0 ){
      continue;
    }
    for(j=0; j<nRows; j++){
      if( aRows[j].zType && aRows[j].zName
       && strcmp(aRows[j].zType, pSe->zType)==0
       && strcmp(aRows[j].zName, pSe->zName)==0 ){
        exists = 1;
        break;
      }
    }
    if( exists ) continue;
    if( nRows>=nAlloc ){
      i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
      SchemaCatalogRow *aNew;
      if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aNew = sqlite3_realloc(aRows,
                             (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
      if( !aNew ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aRows = aNew;
      nAlloc = (int)nNew;
    }
    pRow = &aRows[nRows];
    memset(pRow, 0, sizeof(*pRow));
    pRow->iRowid = iNextRowid++;
    pRow->oldPg = pSe->iRootpage;
    pRow->zType = sqlite3_mprintf("%s", pSe->zType ? pSe->zType : "");
    pRow->zName = sqlite3_mprintf("%s", pSe->zName ? pSe->zName : "");
    pRow->zTblName = sqlite3_mprintf("%s", pSe->zTblName ? pSe->zTblName : "");
    pRow->zSql = pSe->zSql ? sqlite3_mprintf("%s", pSe->zSql) : 0;
    if( !pRow->zType || !pRow->zName || !pRow->zTblName || (pSe->zSql && !pRow->zSql) ){
      freeSchemaCatalogRows(aRows, nRows+1);
      return SQLITE_NOMEM;
    }
    nRows++;
  }

  *paRows = aRows;
  *pnRows = nRows;
  return SQLITE_OK;
}

int doltliteSerializeCatalogEntries(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  u8 **ppOut,
  int *pnOut
){
  return doltliteSerializeCatalogEntriesWithFallbackSchema(
      db, aTables, nTables, 0, 0, ppOut, pnOut);
}

/* Build a master root for a NAMED staging operation. Rows follow the
** source of their object's staging: table and index rows for objects
** named in this operation (azTouched — the staged tables, staged drops,
** and a staged vtab's shadows) come from the working master, everything
** else — other tables' rows, view and trigger rows — keeps the
** previously staged state. Wholesale working adoption leaked unstaged
** schema changes of untouched objects into the commit; taking only the
** touched objects' rows keeps the numbering domain of the freshly staged
** entries while leaving the rest of the staged picture alone. A touched
** name with no working rows is a staged drop: its old rows simply vanish. */
int doltliteBuildNamedStageMasterRoot(
  sqlite3 *db,
  const ProllyHash *pWorkingMaster, u8 workingFlags,
  const ProllyHash *pOldMaster, u8 oldFlags,
  const char **azTouched, int nTouched,
  struct TableEntry *aFinal, int nFinal,
  int bEntrylessFromWorking,
  ProllyHash *pNewRoot
){
  Btree *pBtree;
  struct TableEntry m;
  SchemaCatalogRow *aWork = 0, *aOld = 0;
  int nWork = 0, nOld = 0;
  ProllyHash ignoreRoot;
  u8 ignoreFlags;
  ProllyMutMap mm;
  struct TableEntry masterEntry;
  i64 iRowid = 1;
  int i, t, rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  memset(pNewRoot, 0, sizeof(*pNewRoot));

  memset(&m, 0, sizeof(m));
  m.iTable = 1;
  m.root = *pWorkingMaster;
  m.flags = workingFlags;
  rc = loadSchemaCatalogRows(pBtree, &m, 1, &aWork, &nWork,
                             &ignoreRoot, &ignoreFlags);
  if( rc!=SQLITE_OK ) return rc;
  if( pOldMaster && !prollyHashIsEmpty(pOldMaster) ){
    m.root = *pOldMaster;
    m.flags = oldFlags;
    rc = loadSchemaCatalogRows(pBtree, &m, 1, &aOld, &nOld,
                               &ignoreRoot, &ignoreFlags);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aWork, nWork);
      return rc;
    }
  }

  memset(&mm, 0, sizeof(mm));
  rc = prollyMutMapInit(&mm, 1);
  if( rc!=SQLITE_OK ){
    freeSchemaCatalogRows(aWork, nWork);
    freeSchemaCatalogRows(aOld, nOld);
    return rc;
  }
  for(i=0; i<nWork+nOld && rc==SQLITE_OK; i++){
    SchemaCatalogRow *pRow = i<nWork ? &aWork[i] : &aOld[i-nWork];
    int fromWorking = i<nWork;
    int isEntryless;
    int touched = 0;
    const char *zParent;
    Pgno iPg;
    u8 *pRec;
    int nRec = 0;

    if( !pRow->zType ) continue;
    isEntryless = strcmp(pRow->zType, "view")==0
               || strcmp(pRow->zType, "trigger")==0;
    if( isEntryless ){
      /* A named add keeps views and triggers at their staged state; -a
      ** stages them from working, as Dolt does through dolt_schemas. */
      if( fromWorking != (bEntrylessFromWorking!=0) ) continue;
    }else{
      zParent = strcmp(pRow->zType, "index")==0
                  ? pRow->zTblName : pRow->zName;
      for(t=0; t<nTouched; t++){
        if( zParent && azTouched[t]
         && strcmp(zParent, azTouched[t])==0 ){
          touched = 1;
          break;
        }
      }
      if( fromWorking != touched ) continue;
    }
    /* Old-sourced table rows must carry the number their entry holds in
    ** the FINAL entry list, or the composed rows collide across numbering
    ** domains and no longer pair with their entries: aligned entries hold
    ** working numbers, and entries whose tables left the working tree
    ** hold fresh numbers. Index entries are unnamed and never renumbered;
    ** their rows keep the old number. */
    iPg = pRow->oldPg;
    if( !fromWorking && strcmp(pRow->zType, "table")==0 && pRow->zName ){
      const struct TableEntry *pFinal = 0;
      for(t=0; t<nFinal; t++){
        if( aFinal[t].zName && strcmp(aFinal[t].zName, pRow->zName)==0 ){
          pFinal = &aFinal[t];
          break;
        }
      }
      if( pFinal ){
        iPg = pFinal->iTable;
      }else{
        for(t=0; t<nWork; t++){
          if( aWork[t].zType && strcmp(aWork[t].zType, "table")==0
           && aWork[t].zName && strcmp(aWork[t].zName, pRow->zName)==0 ){
            iPg = aWork[t].oldPg;
            break;
          }
        }
      }
    }
    pRec = buildSchemaCatalogRecord(pRow->zType, pRow->zName,
                                    pRow->zTblName, iPg,
                                    pRow->zSql, &nRec);
    if( !pRec ){
      rc = SQLITE_NOMEM;
      break;
    }
    rc = prollyMutMapInsert(&mm, 0, 0, iRowid++, pRec, nRec);
    sqlite3_free(pRec);
  }
  freeSchemaCatalogRows(aWork, nWork);
  freeSchemaCatalogRows(aOld, nOld);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  memset(&masterEntry, 0, sizeof(masterEntry));
  masterEntry.iTable = 1;
  masterEntry.flags = workingFlags;
  rc = applyMutMapToTableRoot(pBtree->pBt, &masterEntry, &mm);
  prollyMutMapFree(&mm);
  if( rc==SQLITE_OK ) *pNewRoot = masterEntry.root;
  return rc;
}

static int doltliteSerializeCatalogEntriesForBtreeImpl(
  Btree *pBtree,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  int bForeignDomain,
  u8 **ppOut,
  int *pnOut
){
  int sz = CAT_HEADER_SIZE_V5;
  u8 *buf, *q;
  sqlite3 *db;
  SchemaCatalogRow *aRows = 0;
  CatalogEntryMeta *aMeta = 0;
  CatalogPgnoRef *aRowRef = 0;
  ProllyHash masterRoot;
  u8 masterFlags = 0;
  CatalogSerializeEntry *aSorted = 0;
  int nRows = 0, nMeta = 0;
  int i, j;
  int rc;

  if( !pBtree ) return SQLITE_MISUSE;
  db = pBtree->db;
  rc = buildLiveCatalogEntryMeta(pBtree, &aMeta, &nMeta);
  if( rc!=SQLITE_OK ) return rc;
  rc = loadSchemaCatalogRows(pBtree, aTables, nTables, &aRows, &nRows, &masterRoot, &masterFlags);
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  filterSchemaCatalogRows(aRows, &nRows, aTables, nTables);
  /* A retired object can leave an index row behind at a number a restored
  ** entry now occupies: the entry filter pairs one row to one entry and
  ** cannot see that the row's parent table is gone from the set, and a
  ** serialized catalog whose index has no table does not load. Purge by
  ** parent before the supplementation passes, so the freed number can
  ** receive the correct row from live or fallback schema. A parent whose
  ** row arrives in those passes has a named entry here, so requiring a
  ** parent row OR a parent entry keeps every live pairing. Constructed
  ** arrays only: the live catalog never composes rows across domains. */
  if( aTables!=pBtree->cat.a ){
    int nOut = 0;
    for(i=0; i<nRows; i++){
      int keep = 1;
      if( aRows[i].zType && strcmp(aRows[i].zType, "index")==0
       && aRows[i].zTblName ){
        keep = 0;
        for(j=0; j<nRows && !keep; j++){
          if( aRows[j].zType && strcmp(aRows[j].zType, "table")==0
           && aRows[j].zName
           && strcmp(aRows[j].zName, aRows[i].zTblName)==0 ){
            keep = 1;
          }
        }
        for(j=0; j<nTables && !keep; j++){
          if( aTables[j].zName
           && strcmp(aTables[j].zName, aRows[i].zTblName)==0 ){
            keep = 1;
          }
        }
      }
      if( keep ){
        if( nOut!=i ){
          aRows[nOut] = aRows[i];
          memset(&aRows[i], 0, sizeof(aRows[i]));
        }
        nOut++;
      }else{
        sqlite3_free(aRows[i].zType);
        sqlite3_free(aRows[i].zName);
        sqlite3_free(aRows[i].zTblName);
        sqlite3_free(aRows[i].zSql);
        memset(&aRows[i], 0, sizeof(aRows[i]));
      }
    }
    nRows = nOut;
  }
  /* Live-schema supplementation keys rows by the CONNECTION's table
  ** numbers. Arrays numbered in a foreign domain (a reset target catalog)
  ** must not use it -- a live number there can belong to a different table
  ** entirely, and their missing rows come from the fallback schema. */
  if( !bForeignDomain ){
    rc = appendMissingSchemaCatalogRows(db, btreeSchemaName(pBtree),
                                        &aRows, &nRows, aMeta, nMeta,
                                        aTables, nTables);
  }
  if( rc==SQLITE_OK ){
    rc = appendFallbackSchemaCatalogRows(&aRows, &nRows, aTables, nTables,
                                         aFallbackSchema, nFallbackSchema);
  }
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  if( nRows>0 ){
    ProllyMutMap mm;
    struct TableEntry masterEntry;
    qsort(aRows, nRows, sizeof(SchemaCatalogRow), schemaCatalogRowCmp);
    /* Canonical numbering: rows sort by type rank then name, and numbering
    ** is positional, so the blob is a pure function of the logical catalog
    ** (same schema reached through any DDL order hashes identically -- the
    ** history-independence contract). The live session adopts this form at
    ** every schema commit, so live and persisted never diverge. */
    for(i=0; i<nRows; i++){
      if( schemaCatalogRowIsVirtualTable(&aRows[i]) ){
        aRows[i].newPg = 0;
      }else if( strcmp(aRows[i].zType, "table")==0 || strcmp(aRows[i].zType, "index")==0 ){
        aRows[i].newPg = (Pgno)(i + 2);
      }else{
        aRows[i].newPg = aRows[i].oldPg;
      }
    }

    memset(&mm, 0, sizeof(mm));
    rc = prollyMutMapInit(&mm, 1);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return rc;
    }
    for(i=0; i<nRows; i++){
      int nRec = 0;
      u8 *pRec;
      ProllyHash h;
      /* Canonicalized SQL text in the stored row: ADD COLUMN and RENAME
      ** rewrite the statement differently than typing the final form, and
      ** history independence requires equivalent schemas to serialize
      ** identically. */
      if( (strcmp(aRows[i].zType, "table")==0 || strcmp(aRows[i].zType, "index")==0)
       && aRows[i].zSql!=0 && aRows[i].zSql[0]!=0 ){
        char *zCanon = doltliteCanonicalizeSchemaSql(aRows[i].zSql, aRows[i].zName);
        if( !zCanon ){
          prollyMutMapFree(&mm);
          freeSchemaCatalogRows(aRows, nRows);
          freeCatalogEntryMeta(aMeta, nMeta);
          return SQLITE_NOMEM;
        }
        prollyHashCompute((const u8*)zCanon, (int)strlen(zCanon), &h);
        pRec = buildSchemaCatalogRecord(aRows[i].zType, aRows[i].zName,
                                        aRows[i].zTblName, aRows[i].newPg,
                                        zCanon, &nRec);
        sqlite3_free(zCanon);
      }else{
        memset(&h, 0, sizeof(h));
        pRec = buildSchemaCatalogRecord(aRows[i].zType, aRows[i].zName,
                                        aRows[i].zTblName, aRows[i].newPg,
                                        aRows[i].zSql, &nRec);
      }
      for(j=0; j<nTables; j++){
        if( aTables[j].iTable==aRows[i].oldPg ){
          aTables[j].schemaHash = h;
          break;
        }
      }
      if( !pRec ){
        prollyMutMapFree(&mm);
        freeSchemaCatalogRows(aRows, nRows);
        freeCatalogEntryMeta(aMeta, nMeta);
        return SQLITE_NOMEM;
      }
      rc = prollyMutMapInsert(&mm, 0, 0, (i64)(i + 1), pRec, nRec);
      sqlite3_free(pRec);
      if( rc!=SQLITE_OK ){
        prollyMutMapFree(&mm);
        freeSchemaCatalogRows(aRows, nRows);
        freeCatalogEntryMeta(aMeta, nMeta);
        return rc;
      }
    }
    memset(&masterEntry, 0, sizeof(masterEntry));
    masterEntry.iTable = 1;
    memset(&masterEntry.root, 0, sizeof(masterEntry.root));
    masterEntry.flags = masterFlags;
    rc = applyMutMapToTableRoot(pBtree->pBt, &masterEntry, &mm);
    prollyMutMapFree(&mm);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return rc;
    }
    masterRoot = masterEntry.root;
  }

  if( nRows>=CATALOG_INDEX_MIN_ENTRIES ){
    aRowRef = sqlite3_malloc(nRows * (int)sizeof(CatalogPgnoRef));
    if( !aRowRef ){
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return SQLITE_NOMEM;
    }
    for(i=0; i<nRows; i++){
      aRowRef[i].iTable = aRows[i].oldPg;
      aRowRef[i].iEntry = i;
    }
    qsort(aRowRef, nRows, sizeof(CatalogPgnoRef), catalogPgnoRefCmp);
  }

  if( nMeta>0 ){
    Pgno iNextHidden = 2;
    for(i=0; i<nRows; i++){
      if( aRows[i].newPg >= iNextHidden ) iNextHidden = aRows[i].newPg + 1;
    }
    for(i=0; i<nMeta; i++){
      if( aRowRef
        ? catalogPgnoRefFind(aRowRef, nRows, aMeta[i].iTable)>=0
        : schemaCatalogHasPgno(aRows, nRows, aMeta[i].iTable) ){
        continue;
      }
      aMeta[i].iPersistTable = iNextHidden++;
    }
  }

  if( nTables > 0 ){
    aSorted = sqlite3_malloc(nTables * (int)sizeof(CatalogSerializeEntry));
    if( !aSorted ){
      sqlite3_free(aRowRef);
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return SQLITE_NOMEM;
    }
    memset(aSorted, 0, nTables * (int)sizeof(CatalogSerializeEntry));
    for(i=0; i<nTables; i++){
      const SchemaCatalogRow *pRow = 0;
      const CatalogEntryMeta *pMeta = 0;
      aSorted[i].iTable = aTables[i].iTable;
      aSorted[i].root = aTables[i].root;
      aSorted[i].schemaHash = aTables[i].schemaHash;
      aSorted[i].flags = aTables[i].flags;
      if( aTables[i].iTable==1 ){
        aSorted[i].root = masterRoot;
        aSorted[i].zType = "catalog";
        aSorted[i].zName = "sqlite_master";
        aSorted[i].zTblName = "";
        continue;
      }
      /* The table number is the entry's identity; match it first. For the
      ** LIVE catalog that match is unconditional -- cached entry names go
      ** stale across RENAME, so a name match is only a fallback for entries
      ** whose number has no schema row. Constructed arrays (staging, merge,
      ** reset) carry authoritative loaded names but may mix entries from
      ** two numbering domains, so a number match that disagrees on name is
      ** a cross-domain collision and must be rejected. */
      {
        int bLiveCatalog = (aTables==pBtree->cat.a);
        /* Constructed arrays can mix numbering domains, and an
        ** unnamed entry is always an index: a bare number match
        ** against a table row is a cross-domain collision, not a
        ** pairing. */
        if( aRowRef ){
          int iRef = catalogPgnoRefFind(aRowRef, nRows, aTables[i].iTable);
          for(; iRef>=0 && iRef<nRows
            && aRowRef[iRef].iTable==aTables[i].iTable; iRef++){
            j = aRowRef[iRef].iEntry;
            if( aRows[j].oldPg==aTables[i].iTable ){
              if( !bLiveCatalog
               && aTables[i].zName && aRows[j].zName
               && strcmp(aRows[j].zName, aTables[i].zName)!=0 ){
                continue;
              }
              if( !bLiveCatalog
               && !aTables[i].zName
               && aRows[j].zType && strcmp(aRows[j].zType, "index")!=0 ){
                continue;
              }
              pRow = &aRows[j];
              break;
            }
          }
        }else{
          for(j=0; j<nRows; j++){
            if( aRows[j].oldPg==aTables[i].iTable ){
              if( !bLiveCatalog
               && aTables[i].zName && aRows[j].zName
               && strcmp(aRows[j].zName, aTables[i].zName)!=0 ){
                continue;
              }
              if( !bLiveCatalog
               && !aTables[i].zName
               && aRows[j].zType && strcmp(aRows[j].zType, "index")!=0 ){
                continue;
              }
              pRow = &aRows[j];
              break;
            }
          }
        }
      }
      if( !pRow && aTables[i].zName ){
        for(j=0; j<nRows; j++){
          if( strcmp(aRows[j].zType, "table")==0
           && aRows[j].zName
           && strcmp(aRows[j].zName, aTables[i].zName)==0 ){
            pRow = &aRows[j];
            break;
          }
        }
      }
      if( pRow ){
        aSorted[i].iTable = pRow->newPg;
        aSorted[i].zType = pRow->zType;
        aSorted[i].zName = pRow->zName;
        aSorted[i].zTblName = pRow->zTblName;
      }else{
        pMeta = findCatalogEntryMetaByPgno(aMeta, nMeta, aTables[i].iTable);
        if( pMeta && pMeta->iPersistTable>0 ){
          aSorted[i].iTable = pMeta->iPersistTable;
          aSorted[i].zType = pMeta->zType;
          aSorted[i].zName = pMeta->zName;
          aSorted[i].zTblName = pMeta->zTblName;
        }else{
          aSorted[i].zType = "unknown";
          aSorted[i].zName = aTables[i].zName ? aTables[i].zName : "";
          aSorted[i].zTblName = "";
        }
      }
    }
    sqlite3_free(aRowRef);
    qsort(aSorted, nTables, sizeof(CatalogSerializeEntry), catalogSerializeEntryCmp);
  }else{
    sqlite3_free(aRowRef);
  }

#ifdef DOLTLITE_PROLLY_CHECK
  /* Two structural failures are detectable at write time regardless of
  ** intent: an entry that pairs with no schema row and no live meta has
  ** lost its identity crossing catalog numbering domains, and two entries
  ** sharing a number would make the serialized catalog ambiguous. Both
  ** are write-time signatures of the unnamed-entry overlay bug class —
  ** fail fast instead of publishing the catalog. Only CONSTRUCTED arrays
  ** (staging, merge, reset) are checked: that is where the overlay bugs
  ** live, while the LIVE catalog can legitimately reach these states
  ** through writable_schema vandalism that stock tolerates (deleted or
  ** rootpage-aliased schema rows). A missing entry for an emitted row
  ** cannot be checked at all: the row filter intentionally expresses
  ** subset catalogs, so absence is indistinguishable from a staged
  ** drop. */
  for(i=0; aTables!=pBtree->cat.a && i<nTables; i++){
    if( aSorted[i].iTable<=1 ) continue;
    if( aSorted[i].zType && strcmp(aSorted[i].zType, "unknown")==0 ){
      fprintf(stderr,
        "doltlite: catalog invariant violated: entry %u (%s) pairs with no "
        "schema row or live meta\n",
        (unsigned)aSorted[i].iTable,
        aSorted[i].zName && aSorted[i].zName[0] ? aSorted[i].zName : "unnamed");
      sqlite3_log(SQLITE_CORRUPT,
        "catalog entry invariant violated: unpairable entry %u",
        (unsigned)aSorted[i].iTable);
      abort();
    }
    for(j=i+1; j<nTables; j++){
      if( aSorted[j].iTable==aSorted[i].iTable ){
        fprintf(stderr,
          "doltlite: catalog invariant violated: duplicate entry number %u "
          "(%s / %s)\n",
          (unsigned)aSorted[i].iTable,
          aSorted[i].zName ? aSorted[i].zName : "unnamed",
          aSorted[j].zName ? aSorted[j].zName : "unnamed");
        sqlite3_log(SQLITE_CORRUPT,
          "catalog entry invariant violated: duplicate entry %u",
          (unsigned)aSorted[i].iTable);
        abort();
      }
    }
  }
#endif

  for(i=0; i<nTables; i++){
    int nType = aSorted[i].zType ? (int)strlen(aSorted[i].zType) : 0;
    int nName = aSorted[i].zName ? (int)strlen(aSorted[i].zName) : 0;
    int nTbl = aSorted[i].zTblName ? (int)strlen(aSorted[i].zTblName) : 0;
    if( sz > 0x7FFFFFFF - (4 + 1 + PROLLY_HASH_SIZE*2 + 6 + nType + nName + nTbl) ){
      sqlite3_free(aSorted);
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return SQLITE_TOOBIG;
    }
    sz += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 6 + nType + nName + nTbl;
  }

  buf = sqlite3_malloc(sz);
  if( !buf ){
    sqlite3_free(aSorted);
    freeSchemaCatalogRows(aRows, nRows);
    freeCatalogEntryMeta(aMeta, nMeta);
    return SQLITE_NOMEM;
  }
  q = buf;

  *q++ = CATALOG_FORMAT_V5;
  q[0]=(u8)nTables; q[1]=(u8)(nTables>>8);
  q[2]=(u8)(nTables>>16); q[3]=(u8)(nTables>>24);
  q += 4;
  putU32LE(q, pBtree->aMeta[BTREE_USER_VERSION]);
  q += 4;
  putU32LE(q, pBtree->aMeta[BTREE_APPLICATION_ID]);
  q += 4;

  for(i=0; i<nTables; i++){
    const CatalogSerializeEntry *e = &aSorted[i];
    u32 pg = e->iTable;
    int nType = e->zType ? (int)strlen(e->zType) : 0;
    int nName = e->zName ? (int)strlen(e->zName) : 0;
    int nTbl = e->zTblName ? (int)strlen(e->zTblName) : 0;
    q[0]=(u8)pg; q[1]=(u8)(pg>>8); q[2]=(u8)(pg>>16); q[3]=(u8)(pg>>24);
    q += 4;
    *q++ = e->flags;
    memcpy(q, e->root.data, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(q, e->schemaHash.data, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    q[0]=(u8)nType; q[1]=(u8)(nType>>8); q+=2;
    q[0]=(u8)nName; q[1]=(u8)(nName>>8); q+=2;
    q[0]=(u8)nTbl; q[1]=(u8)(nTbl>>8); q+=2;
    if( nType>0 ) memcpy(q, e->zType, nType);
    q += nType;
    if( nName>0 ) memcpy(q, e->zName, nName);
    q += nName;
    if( nTbl>0 ) memcpy(q, e->zTblName, nTbl);
    q += nTbl;
  }
  sqlite3_free(aSorted);
  freeSchemaCatalogRows(aRows, nRows);
  freeCatalogEntryMeta(aMeta, nMeta);
  *ppOut = buf;
  *pnOut = (int)(q - buf);
  return SQLITE_OK;
}

int doltliteSerializeCatalogEntriesWithFallbackSchema(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  u8 **ppOut,
  int *pnOut
){
  if( !db ) return SQLITE_MISUSE;
  if( db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  return doltliteSerializeCatalogEntriesForBtreeImpl(
      db->aDb[0].pBt, aTables, nTables,
      aFallbackSchema, nFallbackSchema, 0, ppOut, pnOut);
}

int doltliteSerializeCatalogEntriesForeignDomain(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  u8 **ppOut,
  int *pnOut
){
  if( !db ) return SQLITE_MISUSE;
  if( db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  return doltliteSerializeCatalogEntriesForBtreeImpl(
      db->aDb[0].pBt, aTables, nTables,
      aFallbackSchema, nFallbackSchema, 1, ppOut, pnOut);
}

static int serializeCatalogPatchRoots(Btree *pBtree, u8 **ppOut, int *pnOut){
  u8 *buf = 0;
  const u8 *pEntries;
  u8 *q;
  int nData = 0;
  int nTables = 0;
  int iFormat = 0;
  int i, nPatched = 0;
  int rc;

  if( pBtree->bSchemaChangedTxn
   || prollyHashIsEmpty(&pBtree->committedCatalogHash) ){
    return SQLITE_NOTFOUND;
  }

  if( pBtree->pCatalogCache
   && pBtree->nCatalogCache>0
   && prollyHashCompare(&pBtree->catalogCacheHash,
                        &pBtree->committedCatalogHash)==0 ){
    buf = sqlite3_malloc(pBtree->nCatalogCache);
    if( !buf ) return SQLITE_NOMEM;
    memcpy(buf, pBtree->pCatalogCache, pBtree->nCatalogCache);
    nData = pBtree->nCatalogCache;
  }else{
    rc = chunkStoreGet(&pBtree->pBt->store, &pBtree->committedCatalogHash,
                       &buf, &nData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(buf);
      return rc;
    }
  }
  if( !buf || !catalogParseHeaderEx(buf, nData, &iFormat, &nTables, &pEntries)
   || nTables!=pBtree->cat.n ){
    sqlite3_free(buf);
    return SQLITE_NOTFOUND;
  }
  if( iFormat!=CATALOG_FORMAT_V5 ){
    sqlite3_free(buf);
    return SQLITE_NOTFOUND;
  }
  putU32LE(buf + CAT_HEADER_SIZE_V3, pBtree->aMeta[BTREE_USER_VERSION]);
  putU32LE(buf + CAT_HEADER_SIZE_V3 + 4, pBtree->aMeta[BTREE_APPLICATION_ID]);

  q = buf + (pEntries - (const u8*)buf);
  for(i=0; i<nTables; i++){
    Pgno iTable;
    struct TableEntry *pTE;
    int nSkip;

    if( q + CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE
        + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE > buf + nData ){
      sqlite3_free(buf);
      return SQLITE_NOTFOUND;
    }

    iTable = (Pgno)prollyBtreeGetU32LE(q);
    pTE = findTable(pBtree, iTable);
    if( !pTE ){
      sqlite3_free(buf);
      return SQLITE_NOTFOUND;
    }

    q += CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE;
    /* Patch sqlite_master only after a surviving page-1 write. */
    if( iTable!=1 || pBtree->bMasterRootChangedTxn ){
      memcpy(q, pTE->root.data, PROLLY_HASH_SIZE);
    }
    q += PROLLY_HASH_SIZE;
    /* Only roots are patched, so every object the baseline names is published
    ** as it stands there. Matching entry counts and table numbers do not make
    ** it this catalog -- a connection that switched branches has a baseline
    ** describing the branch it left -- and its schema hash is what tells the
    ** two apart. */
    if( iTable!=1 && memcmp(q, pTE->schemaHash.data, PROLLY_HASH_SIZE)!=0 ){
      sqlite3_free(buf);
      return SQLITE_NOTFOUND;
    }
    q += PROLLY_HASH_SIZE;
    nPatched++;

    if( iFormat!=CATALOG_FORMAT_V3 ){
      int nType, nName, nTbl;
      if( q + 6 > buf + nData ){
        sqlite3_free(buf);
        return SQLITE_NOTFOUND;
      }
      nType = q[0] | (q[1]<<8);
      nName = q[2] | (q[3]<<8);
      nTbl = q[4] | (q[5]<<8);
      q += 6;
      nSkip = nType + nName + nTbl;
    }else{
      if( q + 2 > buf + nData ){
        sqlite3_free(buf);
        return SQLITE_NOTFOUND;
      }
      nSkip = q[0] | (q[1]<<8);
      q += 2;
    }
    if( nSkip<0 || q + nSkip > buf + nData ){
      sqlite3_free(buf);
      return SQLITE_NOTFOUND;
    }
    q += nSkip;
  }

  if( q!=buf+nData || nPatched!=pBtree->cat.n ){
    sqlite3_free(buf);
    return SQLITE_NOTFOUND;
  }

  *ppOut = buf;
  *pnOut = nData;
  return SQLITE_OK;
}

int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut){
  assert( pBtree!=0 && ppOut!=0 && pnOut!=0 );
  return doltliteSerializeCatalogEntriesForBtreeImpl(
      pBtree, pBtree->cat.a, pBtree->cat.n, 0, 0, 0, ppOut, pnOut);
}

int serializeCatalogForCommit(Btree *pBtree, u8 **ppOut, int *pnOut){
  int rc;
  assert( pBtree!=0 && ppOut!=0 && pnOut!=0 );
  PROLLY_ASSERT_WRITE_TXN(pBtree);
  rc = serializeCatalogPatchRoots(pBtree, ppOut, pnOut);
  if( rc==SQLITE_OK ) return SQLITE_OK;
  if( rc!=SQLITE_NOTFOUND ) return rc;
  return serializeCatalog(pBtree, ppOut, pnOut);
}

void initDefaultMeta(Btree *pBtree){
  assert( pBtree!=0 );
  memset(pBtree->aMeta, 0, sizeof(pBtree->aMeta));
  pBtree->aMeta[BTREE_FILE_FORMAT] = 4;
  pBtree->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;

}

int deserializeCatalog(Btree *pBtree, const u8 *data, int nData){
  const u8 *q = data;
  int nTables, i;
  int iFormat = 0;
  Catalog catNew;
  u32 aMetaNew[16];

  assert( pBtree!=0 );
  assert( data!=0 || nData==0 );
  {
    const u8 *pEntries;
    if( !catalogParseHeaderEx(data, nData, &iFormat, &nTables, &pEntries) ){
      return SQLITE_CORRUPT;
    }
    q = pEntries;
  }

  memset(&catNew, 0, sizeof(catNew));
  memset(aMetaNew, 0, sizeof(aMetaNew));
  aMetaNew[BTREE_FILE_FORMAT] = 4;
  aMetaNew[BTREE_TEXT_ENCODING] = SQLITE_UTF8;
  /* Keep session-scoped meta across in-connection catalog reloads. V5
  ** catalogs persist user-visible header meta so new connections see it too. */
  aMetaNew[BTREE_DEFAULT_CACHE_SIZE] = pBtree->aMeta[BTREE_DEFAULT_CACHE_SIZE];
  aMetaNew[BTREE_USER_VERSION] = pBtree->aMeta[BTREE_USER_VERSION];
  aMetaNew[BTREE_INCR_VACUUM] = pBtree->aMeta[BTREE_INCR_VACUUM];
  aMetaNew[BTREE_APPLICATION_ID] = pBtree->aMeta[BTREE_APPLICATION_ID];
  if( iFormat==CATALOG_FORMAT_V5 ){
    aMetaNew[BTREE_USER_VERSION] = prollyBtreeGetU32LE(data + CAT_HEADER_SIZE_V3);
    aMetaNew[BTREE_APPLICATION_ID] = prollyBtreeGetU32LE(data + CAT_HEADER_SIZE_V3 + 4);
  }

  {
    ProllyHash h;
    u32 schemaHash;
    if( iFormat==CATALOG_FORMAT_V5 ){
      prollyHashCompute(q, (int)(nData - (q - data)), &h);
    }else{
      prollyHashCompute(data, nData, &h);
    }
    schemaHash = ((u32)h.data[0])
               | ((u32)h.data[1] << 8)
               | ((u32)h.data[2] << 16)
               | ((u32)h.data[3] << 24);
    aMetaNew[BTREE_SCHEMA_VERSION] = schemaHash | 1;
  }

  for(i=0; i<nTables; i++){
    Pgno iTable;
    u8 flags;
    struct TableEntry *pTE;
    int nLen;
    if( q+4+1+PROLLY_HASH_SIZE+PROLLY_HASH_SIZE > data+nData ){
      catFree(&catNew);
      return SQLITE_CORRUPT;
    }
    iTable = (Pgno)prollyBtreeGetU32LE(q);
    q += 4;
    flags = *q++;
    pTE = catAdd(&catNew, iTable, flags);
    if( !pTE ){
      catFree(&catNew);
      return SQLITE_NOMEM;
    }
    memcpy(pTE->root.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(pTE->schemaHash.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    if( iFormat!=CATALOG_FORMAT_V3 ){
      int nType, nName, nTbl;
      const u8 *pType, *pName, *pTbl;
      if( q+6 > data+nData ){
        catFree(&catNew);
        return SQLITE_CORRUPT;
      }
      nType = q[0] | (q[1]<<8); q += 2;
      nName = q[0] | (q[1]<<8); q += 2;
      nTbl = q[0] | (q[1]<<8); q += 2;
      if( q+nType+nName+nTbl > data+nData ){
        catFree(&catNew);
        return SQLITE_CORRUPT;
      }
      pType = q; q += nType;
      pName = q; q += nName;
      pTbl = q; q += nTbl;
      (void)pTbl;
      pTE->tableRootKnown = 1;
      if( nType==5 && memcmp(pType, "table", 5)==0 && nName>0 ){
        pTE->isTableRoot = 1;
        /* Views, triggers and virtual tables all serialize with table number
        ** zero, so several entries legitimately share one number and catAdd
        ** hands each of them the same entry. Release the name the previous
        ** one left here before taking ownership of a new one. */
        sqlite3_free(pTE->zName);
        pTE->zName = sqlite3_malloc(nName+1);
        if( !pTE->zName ){
          catFree(&catNew);
          return SQLITE_NOMEM;
        }
        memcpy(pTE->zName, pName, nName);
        pTE->zName[nName] = 0;
      }
    }else{
      if( q+2 > data+nData ){
        catFree(&catNew);
        return SQLITE_CORRUPT;
      }
      nLen = q[0] | (q[1]<<8); q += 2;
      if( q+nLen > data+nData ){
        catFree(&catNew);
        return SQLITE_CORRUPT;
      }
      if( nLen>0 ){
        sqlite3_free(pTE->zName);
        pTE->zName = sqlite3_malloc(nLen+1);
        if( pTE->zName ){
          memcpy(pTE->zName, q, nLen);
          pTE->zName[nLen] = 0;
        }else{
          catFree(&catNew);
          return SQLITE_NOMEM;
        }
      }
      q += nLen;
    }
  }

  if( q!=data+nData ){
    catFree(&catNew);
    return SQLITE_CORRUPT;
  }

  {
    Pgno maxPage = 0;
    Pgno maxCatalogTable = 0;
    for(i=0; i<catNew.n; i++){
      if( catNew.a[i].iTable > maxCatalogTable ){
        maxCatalogTable = catNew.a[i].iTable;
      }
    }
    maxPage = maxCatalogTable;
    aMetaNew[BTREE_LARGEST_ROOT_PAGE] = maxPage;

    catNew.iNextTable = maxCatalogTable + 1;
  }

  btreeFreeCatalogTables(pBtree);
  pBtree->cat = catNew;
  memcpy(pBtree->aMeta, aMetaNew, sizeof(aMetaNew));
  return SQLITE_OK;
}


int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut){
  BtShared *pBt = doltliteGetBtShared(db);
  Btree *pBtree;
  int rc;
  if( !pBt ) return SQLITE_ERROR;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  /* flushAllPending / flushDeferredEdits require a write transaction
  ** (PROLLY_ASSERT_WRITE_TXN). Read-side callers such as the dirty-state
  ** check in doltliteHasUncommittedChanges only need a consistent catalog
  ** snapshot; outside a write txn there are no pending maps to flush. */
  if( pBtree->inTrans==TRANS_WRITE ){
    rc = flushAllPending(pBtree, pBt, 0);
    if( rc!=SQLITE_OK ) return rc;

    rc = flushDeferredEdits(pBtree, pBt);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* A new connection starts with only the runtime sqlite_master entry.
  ** Repository seeding runs before that schema can be queried, and there
  ** are no user-table hashes to refresh in that state. */
  if( pBtree->cat.n>1 ){
    rc = doltliteUpdateSchemaHashes(db);
    if( rc!=SQLITE_OK ) return rc;
  }
  return serializeCatalog(db->aDb[0].pBt, ppOut, pnOut);
}

int doltliteDeserializeCatalogForTest(sqlite3 *db, const u8 *data, int nData){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  return deserializeCatalog(db->aDb[0].pBt, data, nData);
}

int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                        struct TableEntry **ppTables, int *pnTables,
                        Pgno *piNextTable){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  int rc;
  Btree temp;

  if( !cs ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(catHash) ){
    *ppTables = 0;
    *pnTables = 0;
    if( piNextTable ) *piNextTable = 2;
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, catHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  memset(&temp, 0, sizeof(temp));
  rc = deserializeCatalog(&temp, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  *ppTables = temp.cat.a;
  *pnTables = temp.cat.n;
  if( piNextTable ) *piNextTable = temp.cat.iNextTable;
  return SQLITE_OK;
}

int doltliteLoadTableRootByName(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  ProllyHash *pRoot,
  u8 *pFlags,
  ProllyHash *pSchemaHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  const u8 *q;
  const u8 *pEntries;
  int nTables = 0;
  int iFormat = 0;
  int nWant;
  int found = 0;
  ProllyHash foundRoot;
  ProllyHash foundSchemaHash;
  u8 foundFlags = 0;
  int i;
  int rc;

  memset(pRoot, 0, sizeof(*pRoot));
  if( pFlags ) *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(*pSchemaHash));
  if( !cs ) return SQLITE_ERROR;
  if( !zTableName || !pCatHash || prollyHashIsEmpty(pCatHash) ){
    return SQLITE_NOTFOUND;
  }
  nWant = (int)strlen(zTableName);

  rc = chunkStoreGet(cs, pCatHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( !catalogParseHeaderEx(data, nData, &iFormat, &nTables, &pEntries) ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  q = pEntries;
  for(i=0; i<nTables; i++){
    u8 flags;
    ProllyHash root;
    ProllyHash schemaHash;

    if( q+CAT_ENTRY_ITABLE_SIZE+CAT_ENTRY_FLAGS_SIZE
        +PROLLY_HASH_SIZE+PROLLY_HASH_SIZE > data+nData ){
      sqlite3_free(data);
      return SQLITE_CORRUPT;
    }
    q += CAT_ENTRY_ITABLE_SIZE;
    flags = *q++;
    memcpy(root.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(schemaHash.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;

    if( iFormat!=CATALOG_FORMAT_V3 ){
      int nType, nName, nTbl;
      const u8 *pType;
      const u8 *pName;
      if( q+6 > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      nType = q[0] | (q[1]<<8);
      nName = q[2] | (q[3]<<8);
      nTbl = q[4] | (q[5]<<8);
      q += 6;
      if( q+nType+nName+nTbl > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      pType = q;
      q += nType;
      pName = q;
      q += nName+nTbl;
      if( !found
       && nType==5 && memcmp(pType, "table", 5)==0
       && nName==nWant && memcmp(pName, zTableName, nName)==0 ){
        found = 1;
        memcpy(&foundRoot, &root, sizeof(foundRoot));
        memcpy(&foundSchemaHash, &schemaHash, sizeof(foundSchemaHash));
        foundFlags = flags;
      }
    }else{
      int nLen;
      const u8 *pName;
      if( q+2 > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      nLen = q[0] | (q[1]<<8);
      q += 2;
      if( q+nLen > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      pName = q;
      q += nLen;
      if( !found && nLen==nWant && memcmp(pName, zTableName, nLen)==0 ){
        found = 1;
        memcpy(&foundRoot, &root, sizeof(foundRoot));
        memcpy(&foundSchemaHash, &schemaHash, sizeof(foundSchemaHash));
        foundFlags = flags;
      }
    }
  }

  if( q!=data+nData ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  sqlite3_free(data);
  if( !found ) return SQLITE_NOTFOUND;
  memcpy(pRoot, &foundRoot, sizeof(*pRoot));
  if( pFlags ) *pFlags = foundFlags;
  if( pSchemaHash ) memcpy(pSchemaHash, &foundSchemaHash, sizeof(*pSchemaHash));
  return SQLITE_OK;
}

int doltliteLoadTableRootById(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  Pgno iTable,
  ProllyHash *pRoot,
  u8 *pFlags,
  ProllyHash *pSchemaHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  const u8 *q;
  const u8 *pEntries;
  int nTables = 0;
  int iFormat = 0;
  int i;
  int rc;

  memset(pRoot, 0, sizeof(*pRoot));
  if( pFlags ) *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(*pSchemaHash));
  if( !cs ) return SQLITE_ERROR;
  if( !pCatHash || iTable==0 || prollyHashIsEmpty(pCatHash) ){
    return SQLITE_NOTFOUND;
  }

  rc = chunkStoreGet(cs, pCatHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( !catalogParseHeaderEx(data, nData, &iFormat, &nTables, &pEntries) ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  q = pEntries;
  for(i=0; i<nTables; i++){
    Pgno entryTable;
    u8 flags;
    ProllyHash root;
    ProllyHash schemaHash;

    if( q+CAT_ENTRY_ITABLE_SIZE+CAT_ENTRY_FLAGS_SIZE
        +PROLLY_HASH_SIZE+PROLLY_HASH_SIZE > data+nData ){
      sqlite3_free(data);
      return SQLITE_CORRUPT;
    }
    entryTable = (Pgno)prollyBtreeGetU32LE(q);
    q += CAT_ENTRY_ITABLE_SIZE;
    flags = *q++;
    memcpy(root.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(schemaHash.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;

    if( iFormat!=CATALOG_FORMAT_V3 ){
      int nType, nName, nTbl;
      if( q+6 > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      nType = q[0] | (q[1]<<8);
      nName = q[2] | (q[3]<<8);
      nTbl = q[4] | (q[5]<<8);
      q += 6;
      if( q+nType+nName+nTbl > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      if( entryTable==iTable ){
        memcpy(pRoot, &root, sizeof(*pRoot));
        if( pFlags ) *pFlags = flags;
        if( pSchemaHash ) memcpy(pSchemaHash, &schemaHash, sizeof(*pSchemaHash));
        sqlite3_free(data);
        return SQLITE_OK;
      }
      q += nType+nName+nTbl;
    }else{
      int nLen;
      if( q+2 > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      nLen = q[0] | (q[1]<<8);
      q += 2;
      if( q+nLen > data+nData ){
        sqlite3_free(data);
        return SQLITE_CORRUPT;
      }
      if( entryTable==iTable ){
        memcpy(pRoot, &root, sizeof(*pRoot));
        if( pFlags ) *pFlags = flags;
        if( pSchemaHash ) memcpy(pSchemaHash, &schemaHash, sizeof(*pSchemaHash));
        sqlite3_free(data);
        return SQLITE_OK;
      }
      q += nLen;
    }
  }

  sqlite3_free(data);
  return SQLITE_NOTFOUND;
}

void doltliteFreeCatalog(struct TableEntry *a, int n){
  int i;
  if( !a ) return;
  for(i=0; i<n; i++) sqlite3_free(a[i].zName);
  sqlite3_free(a);
}

int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  u8 *data = 0;
  int nData = 0;
  int rc;
  DoltliteCommit commit;

  if( !cs ) return SQLITE_ERROR;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    memset(pCatHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, &headHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  memcpy(pCatHash, &commit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&commit);
  return SQLITE_OK;
}

int doltliteGetWorkingTableState(sqlite3 *db, const char *zTable,
                                 ProllyHash *pRoot, u8 *pFlags,
                                 ProllyHash *pSchemaHash){
  Btree *pBtree;
  Pgno iTable;
  struct TableEntry *pEntry;

  if( pRoot ) memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(ProllyHash));

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( doltliteResolveTableName(db, zTable, &iTable)!=SQLITE_OK ){
    return SQLITE_NOTFOUND;
  }
  pEntry = findTable(pBtree, iTable);
  if( !pEntry ) return SQLITE_NOTFOUND;

  if( pRoot ) memcpy(pRoot, &pEntry->root, sizeof(ProllyHash));
  if( pFlags ) *pFlags = pEntry->flags;
  if( pSchemaHash ) memcpy(pSchemaHash, &pEntry->schemaHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable){
  Btree *pBtree;
  Schema *pSchema;
  HashElem *k;
  int i;
  if( !db || db->nDb<=0 ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( pBtree && pBtree->cat.a ){
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].zName
       && strcmp(pBtree->cat.a[i].zName, zTable)==0 ){
        *piTable = pBtree->cat.a[i].iTable;
        return SQLITE_OK;
      }
    }
  }
  pSchema = db->aDb[0].pSchema;
  if( !pSchema ) return SQLITE_ERROR;
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    if( pTab && strcmp(pTab->zName, zTable)==0 ){
      *piTable = pTab->tnum;
      return SQLITE_OK;
    }
  }
  return SQLITE_ERROR;
}

char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable){
  const char *z = findTableNumberName(db, iTable);
  return z ? sqlite3_mprintf("%s", z) : 0;
}

int doltliteSwitchCatalog(sqlite3 *db, const ProllyHash *catHash){
  BtShared *pBt = doltliteGetBtShared(db);
  Btree *pBtree;
  ChunkStore *cs;
  u8 *data = 0;
  int nData = 0;
  int rc;

  assert( catHash!=0 );
  if( !pBt ) return SQLITE_ERROR;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  cs = &pBt->store;

  if( prollyHashIsEmpty(catHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, catHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  invalidateCursors(pBt, 0, SQLITE_ABORT);

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
  btreeBumpLocalDataVersion(pBtree);

  if( pBtree->db ){
    sqlite3ExpirePreparedStatements(pBtree->db, 0);
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }else{
    invalidateSchema(pBtree);
  }

  return SQLITE_OK;
}

int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash){
  BtShared *pBt = doltliteGetBtShared(db);
  Btree *pBtree;
  ChunkStore *cs;
  u8 *oldCatData = 0;
  int nOldCatData = 0;
  ProllyHash oldStagedCatalog;
  u8 oldIsMerging;
  ProllyHash oldMergeCommitHash;
  ProllyHash oldConflictsCatalogHash;
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !pBt ) return SQLITE_ERROR;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  cs = &pBt->store;

  if( prollyHashIsEmpty(catHash) ) return SQLITE_OK;

  rc = serializeCatalog(pBtree, &oldCatData, &nOldCatData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGet(cs, catHash, &data, &nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(oldCatData);
    return rc;
  }

  oldStagedCatalog = pBtree->vc.stagedCatalog;
  oldIsMerging = pBtree->vc.isMerging;
  oldMergeCommitHash = pBtree->vc.mergeCommitHash;
  oldConflictsCatalogHash = pBtree->vc.conflictsCatalogHash;

  invalidateCursors(pBt, 0, SQLITE_ABORT);

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ){
    btreeFreeCatalogTables(pBtree);
    if( oldCatData ){
      rc = deserializeCatalog(pBtree, oldCatData, nOldCatData);
    }
    sqlite3_free(oldCatData);
    pBtree->vc.stagedCatalog = oldStagedCatalog;
    pBtree->vc.isMerging = oldIsMerging;
    pBtree->vc.mergeCommitHash = oldMergeCommitHash;
    pBtree->vc.conflictsCatalogHash = oldConflictsCatalogHash;
    return rc;
  }

  pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
  btreeBumpLocalDataVersion(pBtree);

  if( pBtree->db ){
    sqlite3ExpirePreparedStatements(pBtree->db, 0);
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }else{
    invalidateSchema(pBtree);
  }

  memcpy(&pBtree->vc.stagedCatalog, catHash, sizeof(ProllyHash));

  {
    const char *zBr = pBtree->zBranch ? pBtree->zBranch : "main";
    rc = btreeWriteWorkingState(cs, zBr, catHash, NULL);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefs(cs);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreCommit(cs);
  }
  if( rc!=SQLITE_OK ){
    btreeFreeCatalogTables(pBtree);
    if( oldCatData ){
      int rc2 = deserializeCatalog(pBtree, oldCatData, nOldCatData);
      if( rc2!=SQLITE_OK ){
        sqlite3_free(oldCatData);
        chunkStoreRollback(cs);
        return rc;
      }
    }
    pBtree->vc.stagedCatalog = oldStagedCatalog;
    pBtree->vc.isMerging = oldIsMerging;
    pBtree->vc.mergeCommitHash = oldMergeCommitHash;
    pBtree->vc.conflictsCatalogHash = oldConflictsCatalogHash;
    if( pBtree->db ){
      sqlite3ExpirePreparedStatements(pBtree->db, 0);
      sqlite3ResetAllSchemasOfConnection(pBtree->db);
    }else{
      invalidateSchema(pBtree);
    }
    pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
    btreeBumpLocalDataVersion(pBtree);
    chunkStoreRollback(cs);
    sqlite3_free(oldCatData);
    return rc;
  }

  sqlite3_free(oldCatData);
  return SQLITE_OK;
}

int doltliteUpdateBranchWorkingState(sqlite3 *db, const char *zBranch,
                                     const ProllyHash *pCatHash,
                                     const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  if( !cs ) return SQLITE_ERROR;
  return btreeWriteWorkingState(cs, zBranch, pCatHash, pCommitHash);
}

int doltliteWriteBranchCleanWorkingState(sqlite3 *db, const char *zBranch,
                                         const ProllyHash *pCatHash,
                                         const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash emptyHash;
  if( !cs ) return SQLITE_ERROR;
  memset(&emptyHash, 0, sizeof(emptyHash));
  return btreeStoreWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                  &emptyHash, 0,
                                  &emptyHash, &emptyHash,
                                  0, &emptyHash, &emptyHash,
                                  0, 0, &emptyHash);
}

int chunkStoreReadBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                       ProllyHash *pCatHash,
                                       ProllyHash *pCommitHash){
  return btreeReadWorkingCatalog(cs, zBranch, pCatHash, pCommitHash);
}


#endif /* DOLTLITE_PROLLY */
