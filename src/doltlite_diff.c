
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "prolly_record.h"
#include "doltlite_name_index.h"
#include <stddef.h>
#include <string.h>
#include <time.h>



/* View and trigger rows are compared by content. The master keys its rows by
** rowid, and a rowid is a position in the canonical row order, so adding or
** dropping a table or index moves every view and trigger behind it; a rowid
** diff would report them changed when nothing about them changed. */
static int masterRowCanonical(const u8 *pRec, int nRec, char **pzOut){
  DoltliteRecordInfo ri;
  char *zType = 0, *zName = 0, *zTbl = 0, *zSql = 0;
  int rc;
  *pzOut = 0;
  if( !pRec || nRec<=0 ) return SQLITE_OK;
  doltliteParseRecord(pRec, nRec, &ri);
  if( ri.nField<5 ) return SQLITE_OK;
  rc = dlRecordTextField(pRec, nRec, &ri, 0, &zType);
  if( rc==SQLITE_OK ) rc = dlRecordTextField(pRec, nRec, &ri, 1, &zName);
  if( rc==SQLITE_OK ) rc = dlRecordTextField(pRec, nRec, &ri, 2, &zTbl);
  if( rc==SQLITE_OK ) rc = dlRecordTextField(pRec, nRec, &ri, 4, &zSql);
  if( rc==SQLITE_OK && zType
   && (strcmp(zType, "view")==0 || strcmp(zType, "trigger")==0) ){
    *pzOut = sqlite3_mprintf("%s\x01%s\x01%s\x01%s", zType,
                             zName ? zName : "", zTbl ? zTbl : "",
                             zSql ? zSql : "");
    if( !*pzOut ) rc = SQLITE_NOMEM;
  }
  sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTbl); sqlite3_free(zSql);
  return rc;
}

static int masterCollectViewTriggerRows(
  sqlite3 *db,
  const ProllyHash *pRoot,
  u8 flags,
  char ***pazRows,
  int *pnRows
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyCursor cur;
  char **az = 0;
  int n = 0, nAlloc = 0;
  int rc, res;
  *pazRows = 0;
  *pnRows = 0;
  if( !cs || !pCache ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal; char *z = 0;
    prollyCursorValue(&cur, &pVal, &nVal);
    rc = masterRowCanonical(pVal, nVal, &z);
    if( rc!=SQLITE_OK ) break;
    if( z ){
      if( n>=nAlloc ){
        int nNew = nAlloc ? nAlloc*2 : 8;
        char **aNew = sqlite3_realloc(az, nNew*(int)sizeof(char*));
        if( !aNew ){ sqlite3_free(z); rc = SQLITE_NOMEM; break; }
        az = aNew; nAlloc = nNew;
      }
      az[n++] = z;
    }
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK ){
    int i;
    for(i=0; i<n; i++) sqlite3_free(az[i]);
    sqlite3_free(az);
    return rc;
  }
  *pazRows = az;
  *pnRows = n;
  return SQLITE_OK;
}

static int masterRowStrCmp(const void *a, const void *b){
  return strcmp(*(char*const*)a, *(char*const*)b);
}

int doltliteMasterViewTriggerRowsDiffer(
  sqlite3 *db,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 flags,
  int *pDiffer
){
  char **azOld = 0, **azNew = 0;
  int nOld = 0, nNew = 0, i, rc;
  *pDiffer = 0;
  if( prollyHashCompare(pOldRoot, pNewRoot)==0 ) return SQLITE_OK;
  rc = sqlite3FaultSim(957) ? SQLITE_IOERR
     : masterCollectViewTriggerRows(db, pOldRoot, flags, &azOld, &nOld);
  if( rc==SQLITE_OK ){
    rc = masterCollectViewTriggerRows(db, pNewRoot, flags, &azNew, &nNew);
  }
  if( rc==SQLITE_OK ){
    if( nOld!=nNew ){
      *pDiffer = 1;
    }else if( nOld>0 ){
      qsort(azOld, nOld, sizeof(char*), masterRowStrCmp);
      qsort(azNew, nNew, sizeof(char*), masterRowStrCmp);
      for(i=0; i<nOld; i++){
        if( strcmp(azOld[i], azNew[i])!=0 ){ *pDiffer = 1; break; }
      }
    }
  }
  for(i=0; i<nOld; i++) sqlite3_free(azOld[i]);
  for(i=0; i<nNew; i++) sqlite3_free(azNew[i]);
  sqlite3_free(azOld);
  sqlite3_free(azNew);
  return rc;
}

static int schemaHasAnyViewOrTrigger(sqlite3 *db,
                                     const ProllyHash *pRoot,
                                     u8 flags,
                                     int *pFound){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyCursor cur;
  int rc, res;
  *pFound = 0;
  if( !cs || !pCache ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = sqlite3FaultSim(958) ? SQLITE_IOERR : prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( doltliteSchemaRecordIsViewOrTrigger(pVal, nVal) ){
      *pFound = 1;
      break;
    }
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  return rc;
}

typedef struct DiffSummaryRow DiffSummaryRow;
struct DiffSummaryRow {
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zTableName;
  char *zCommitter;
  char *zEmail;
  i64  timestamp;
  char *zMessage;
  u8   dataChange;
  u8   schemaChange;
};

typedef struct DoltliteDiffVtab DoltliteDiffVtab;
struct DoltliteDiffVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

#define DIFF_IDX_TABLE_NAME  0x01
#define DIFF_IDX_COMMIT_HASH 0x02

typedef struct DoltliteDiffCursor DoltliteDiffCursor;
struct DoltliteDiffCursor {
  sqlite3_vtab_cursor base;
  DoltliteCommitQueue queue;
  DiffSummaryRow *aBatch;
  int nBatch;
  int nBatchAlloc;
  int iBatch;
  char *zFilterTable;
  int phase;
  int hasRow;
  int singleCommit;
  int pseudoFilter;   /* 0=STAGED+WORKING, 1=WORKING, 2=STAGED */
};



static const char *diffSchema =
  "CREATE TABLE x("
  "  commit_hash   TEXT,"
  "  committer     TEXT,"
  "  email         TEXT,"
  "  date          TEXT,"
  "  message       TEXT,"
  "  data_change   INTEGER,"
  "  schema_change INTEGER,"
  "  table_name    TEXT"
  ")";

#define DIFF_COL_COMMIT_HASH   0
#define DIFF_COL_COMMITTER     1
#define DIFF_COL_EMAIL         2
#define DIFF_COL_DATE          3
#define DIFF_COL_MESSAGE       4
#define DIFF_COL_DATA_CHANGE   5
#define DIFF_COL_SCHEMA_CHANGE 6
#define DIFF_COL_TABLE_NAME    7

typedef DoltliteNameIndex DiffNameIndex;

static int diffNameIndexInit(
  DiffNameIndex *pIdx,
  struct TableEntry *aEntry,
  int nEntry
){
  return doltliteNameIndexInit(pIdx, aEntry, nEntry,
                               (int)sizeof(struct TableEntry),
                               (int)offsetof(struct TableEntry, zName));
}

static void diffNameIndexFree(DiffNameIndex *pIdx){
  doltliteNameIndexFree(pIdx);
}



static void freeBatch(DoltliteDiffCursor *pCur){
  int i;
  for(i=0; i<pCur->nBatch; i++){
    sqlite3_free(pCur->aBatch[i].zTableName);
    sqlite3_free(pCur->aBatch[i].zCommitter);
    sqlite3_free(pCur->aBatch[i].zEmail);
    sqlite3_free(pCur->aBatch[i].zMessage);
  }
  sqlite3_free(pCur->aBatch);
  pCur->aBatch = 0;
  pCur->nBatch = 0;
  pCur->nBatchAlloc = 0;
  pCur->iBatch = 0;
}

static int batchAppend(DoltliteDiffCursor *pCur,
                       const char *zCommitHex,
                       const char *zTableName,
                       const DoltliteCommit *pCommit,
                       u8 dataChange, u8 schemaChange){
  DiffSummaryRow *r;
  int rc;
  if( pCur->zFilterTable && strcmp(pCur->zFilterTable, zTableName)!=0 ){
    return SQLITE_OK;
  }
  rc = DOLTLITE_GROW_ARRAY(&pCur->aBatch, &pCur->nBatchAlloc, pCur->nBatch+1, 16);
  if( rc!=SQLITE_OK ) return rc;
  r = &pCur->aBatch[pCur->nBatch];
  memset(r, 0, sizeof(*r));
  memcpy(r->zCommitHex, zCommitHex, PROLLY_HASH_SIZE*2+1);
  r->zTableName = sqlite3_mprintf("%s", zTableName ? zTableName : "");
  if( !r->zTableName ) return SQLITE_NOMEM;
  if( pCommit ){
    r->zCommitter = sqlite3_mprintf("%s", pCommit->zName  ? pCommit->zName  : "");
    r->zEmail     = sqlite3_mprintf("%s", pCommit->zEmail ? pCommit->zEmail : "");
    r->zMessage   = sqlite3_mprintf("%s", pCommit->zMessage ? pCommit->zMessage : "");
    if( !r->zCommitter || !r->zEmail || !r->zMessage ){
      sqlite3_free(r->zTableName);
      sqlite3_free(r->zCommitter);
      sqlite3_free(r->zEmail);
      sqlite3_free(r->zMessage);
      return SQLITE_NOMEM;
    }
    r->timestamp = pCommit->timestamp;
  }
  r->dataChange   = dataChange;
  r->schemaChange = schemaChange;
  pCur->nBatch++;
  return SQLITE_OK;
}

static int loadIndexSchemaRows(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  SchemaEntry **ppRows,
  int *pnRows
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  *ppRows = 0;
  *pnRows = 0;
  if( !cs || !pCatHash || prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;
  return loadSchemaFromCatalog(db, cs, doltliteGetCache(db), pCatHash,
                               ppRows, pnRows);
}

static int diffRootHasRows(sqlite3 *db, const ProllyHash *pRoot, u8 *pHasRows){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  u64 n = 0;
  int rc;
  *pHasRows = 0;
  if( !cs || !pCache ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  rc = prollySubtreeCount(cs, pCache, pRoot, &n);
  if( rc==SQLITE_OK ) *pHasRows = n>0;
  return rc;
}

static int diffFilteredTableRoots(
  DoltliteDiffCursor *pCur,
  sqlite3 *db,
  const ProllyHash *pChildCat,
  const ProllyHash *pParentCat,
  const char *zHex,
  const DoltliteCommit *pCommit
){
  ProllyHash childRoot;
  ProllyHash parentRoot;
  ProllyHash childSchema;
  ProllyHash parentSchema;
  int childFound;
  int parentFound;
  int rc;

  if( !pCur->zFilterTable ) return SQLITE_OK;
  if( strcmp(pCur->zFilterTable, "dolt_schemas")==0 ) return SQLITE_NOTFOUND;

  rc = doltliteLoadTableRootByName(db, pChildCat, pCur->zFilterTable,
                                   &childRoot, 0, &childSchema);
  if( rc==SQLITE_NOTFOUND ){
    rc = doltliteVtabMapChunkSourceError(pCur->base.pVtab, db, rc, SQLITE_OK);
    if( rc!=SQLITE_OK ) return rc;
    childFound = 0;
  }else{
    if( rc!=SQLITE_OK ) return rc;
    childFound = 1;
  }
  if( !childFound ){
    memset(&childRoot, 0, sizeof(childRoot));
    memset(&childSchema, 0, sizeof(childSchema));
  }

  rc = doltliteLoadTableRootByName(db, pParentCat, pCur->zFilterTable,
                                   &parentRoot, 0, &parentSchema);
  if( rc==SQLITE_NOTFOUND ){
    rc = doltliteVtabMapChunkSourceError(pCur->base.pVtab, db, rc, SQLITE_OK);
    if( rc!=SQLITE_OK ) return rc;
    parentFound = 0;
  }else{
    if( rc!=SQLITE_OK ) return rc;
    parentFound = 1;
  }
  if( !parentFound ){
    memset(&parentRoot, 0, sizeof(parentRoot));
    memset(&parentSchema, 0, sizeof(parentSchema));
  }

  if( !childFound && !parentFound ) return SQLITE_OK;

  if( !childFound || !parentFound ){
    u8 dataChange;
    rc = diffRootHasRows(db, childFound ? &childRoot : &parentRoot,
                         &dataChange);
    if( rc!=SQLITE_OK ) return rc;
    return batchAppend(pCur, zHex, pCur->zFilterTable, pCommit,
                       dataChange, 1);
  }
  {
    u8 dataChange = prollyHashCompare(&childRoot, &parentRoot)!=0;
    u8 schemaChange = prollyHashCompare(&childSchema, &parentSchema)!=0;
    if( !schemaChange ){
      SchemaEntry *aChildRows = 0, *aParentRows = 0;
      int nChildRows = 0, nParentRows = 0;
      rc = loadIndexSchemaRows(db, pChildCat, &aChildRows, &nChildRows);
      if( rc==SQLITE_OK ){
        rc = loadIndexSchemaRows(db, pParentCat, &aParentRows, &nParentRows);
      }
      if( rc==SQLITE_OK
       && doltliteIndexSchemaRowsDifferForTable(aChildRows, nChildRows,
                                  aParentRows, nParentRows,
                                  pCur->zFilterTable) ){
        schemaChange = 1;
      }
      freeSchemaEntries(aChildRows, nChildRows);
      freeSchemaEntries(aParentRows, nParentRows);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( !dataChange && !schemaChange ) return SQLITE_OK;
    return batchAppend(pCur, zHex, pCur->zFilterTable, pCommit,
                       dataChange, schemaChange);
  }
}

static int diffCatalogPairOne(
  DoltliteDiffCursor *pCur, sqlite3 *db,
  struct TableEntry *aChild, int nChild,
  struct TableEntry *aParent, int nParent,
  const char *zHex, const DoltliteCommit *pCommit
){
  struct TableEntry *e;
  struct TableEntry *p;
  u8 dataChange;
  u8 schemaChange;
  int rc;

  if( !pCur->zFilterTable ) return SQLITE_OK;

  if( strcmp(pCur->zFilterTable, "dolt_schemas")==0 ){
    ProllyHash emptyRoot;
    const ProllyHash *pOldRoot;
    struct TableEntry *pNewMaster;
    struct TableEntry *pOldMaster;
    int hasDiff;
    int oldHas;

    memset(&emptyRoot, 0, sizeof(emptyRoot));
    pNewMaster = doltliteFindTableByNumber(aChild, nChild, 1);
    pOldMaster = doltliteFindTableByNumber(aParent, nParent, 1);
    if( !pNewMaster ) return SQLITE_OK;

    pOldRoot = pOldMaster ? &pOldMaster->root : &emptyRoot;
    rc = doltliteMasterViewTriggerRowsDiffer(db, pOldRoot, &pNewMaster->root,
                                    pNewMaster->flags, &hasDiff);
    if( rc!=SQLITE_OK ) return rc;
    if( hasDiff ){
      rc = schemaHasAnyViewOrTrigger(db, pOldRoot, pNewMaster->flags, &oldHas);
      if( rc!=SQLITE_OK ) return rc;
      return batchAppend(pCur, zHex, "dolt_schemas", pCommit,
                         1, oldHas ? 0 : 1);
    }
    return SQLITE_OK;
  }

  e = doltliteFindTableByName(aChild, nChild, pCur->zFilterTable);
  p = doltliteFindTableByName(aParent, nParent, pCur->zFilterTable);
  if( !e && !p ) return SQLITE_OK;

  if( !p || !e ){
    rc = diffRootHasRows(db, e ? &e->root : &p->root, &dataChange);
    if( rc!=SQLITE_OK ) return rc;
    schemaChange = 1;
  }else{
    dataChange   = (prollyHashCompare(&e->root, &p->root) != 0) ? 1 : 0;
    schemaChange =
      (prollyHashCompare(&e->schemaHash, &p->schemaHash) != 0) ? 1 : 0;
    if( !dataChange && !schemaChange ) return SQLITE_OK;
  }
  return batchAppend(pCur, zHex, pCur->zFilterTable, pCommit,
                     dataChange, schemaChange);
}

static int diffCatalogPair(
  DoltliteDiffCursor *pCur, sqlite3 *db,
  struct TableEntry *aChild, int nChild,
  struct TableEntry *aParent, int nParent,
  SchemaEntry *aChildRows, int nChildRows,
  SchemaEntry *aParentRows, int nParentRows,
  const char *zHex, const DoltliteCommit *pCommit
){
  int rc = SQLITE_OK, i;
  DiffNameIndex childIdx;
  DiffNameIndex parentIdx;
  memset(&childIdx, 0, sizeof(childIdx));
  memset(&parentIdx, 0, sizeof(parentIdx));

  if( pCur->zFilterTable ){
    return diffCatalogPairOne(pCur, db, aChild, nChild, aParent, nParent,
                              zHex, pCommit);
  }

  rc = diffNameIndexInit(&childIdx, aChild, nChild);
  if( rc!=SQLITE_OK ) goto diff_done;
  rc = diffNameIndexInit(&parentIdx, aParent, nParent);
  if( rc!=SQLITE_OK ) goto diff_done;

  for(i=0; i<nChild; i++){
    struct TableEntry *e = &aChild[i];
    struct TableEntry *p;
    u8 dataChange, schemaChange;
    if( !e->zName ){
      ProllyHash emptyRoot;
      const ProllyHash *pOldRoot;
      struct TableEntry *pOldMaster;
      int hasDiff;
      int oldHas;
      /* Only the master entry carries schema rows; indexes attribute via row comparison. */
      if( e->iTable!=1 ) continue;
      memset(&emptyRoot, 0, sizeof(emptyRoot));
      pOldMaster = doltliteFindTableByNumber(aParent, nParent, 1);
      pOldRoot = pOldMaster ? &pOldMaster->root : &emptyRoot;
      rc = doltliteMasterViewTriggerRowsDiffer(db, pOldRoot, &e->root, e->flags,
                                      &hasDiff);
      if( rc!=SQLITE_OK ) goto diff_done;
      if( hasDiff ){
        rc = schemaHasAnyViewOrTrigger(db, pOldRoot, e->flags, &oldHas);
        if( rc!=SQLITE_OK ) goto diff_done;
        rc = batchAppend(pCur, zHex, "dolt_schemas", pCommit,
                         1, oldHas ? 0 : 1);
        if( rc!=SQLITE_OK ) goto diff_done;
      }
      continue;
    }
    p = addNameIndexFind(&parentIdx, e->zName);
    if( !p ){
      rc = diffRootHasRows(db, &e->root, &dataChange);
      if( rc!=SQLITE_OK ) goto diff_done;
      schemaChange = 1;
    }else{
      dataChange   = (prollyHashCompare(&e->root, &p->root) != 0) ? 1 : 0;
      schemaChange = (prollyHashCompare(&e->schemaHash, &p->schemaHash) != 0) ? 1 : 0;
      if( !schemaChange
       && doltliteIndexSchemaRowsDifferForTable(aChildRows, nChildRows,
                                  aParentRows, nParentRows, e->zName) ){
        schemaChange = 1;
      }
      if( !dataChange && !schemaChange ) continue;
    }
    rc = batchAppend(pCur, zHex, e->zName, pCommit, dataChange, schemaChange);
    if( rc!=SQLITE_OK ) goto diff_done;
  }
  for(i=0; i<nParent; i++){
    struct TableEntry *p = &aParent[i];
    u8 dataChange;
    if( !p->zName ) continue;
    if( addNameIndexFind(&childIdx, p->zName) ) continue;
    rc = diffRootHasRows(db, &p->root, &dataChange);
    if( rc!=SQLITE_OK ) goto diff_done;
    rc = batchAppend(pCur, zHex, p->zName, pCommit, dataChange, 1);
    if( rc!=SQLITE_OK ) goto diff_done;
  }
diff_done:
  diffNameIndexFree(&childIdx);
  diffNameIndexFree(&parentIdx);
  return rc;
}

static int computeCatalogPairBatch(
  DoltliteDiffCursor *pCur, sqlite3 *db,
  const ProllyHash *pChildCat, const ProllyHash *pParentCat,
  const char *zLabel
){
  struct TableEntry *aChild = 0, *aParent = 0;
  int nChild = 0, nParent = 0;
  int rc;
  char zHexBuf[PROLLY_HASH_SIZE*2+1];

  if( prollyHashCompare(pChildCat, pParentCat)==0 ) return SQLITE_OK;

  memset(zHexBuf, 0, sizeof(zHexBuf));
  sqlite3_snprintf(sizeof(zHexBuf), zHexBuf, "%s", zLabel);

  if( pCur->zFilterTable && strcmp(pCur->zFilterTable, "dolt_schemas")!=0 ){
    return diffFilteredTableRoots(pCur, db, pChildCat, pParentCat, zHexBuf, 0);
  }

  rc = doltliteLoadCatalog(db, pParentCat, &aParent, &nParent, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, pChildCat, &aChild, &nChild, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aParent, nParent);
    return rc;
  }

  {
    SchemaEntry *aChildRows = 0, *aParentRows = 0;
    int nChildRows = 0, nParentRows = 0;
    rc = loadIndexSchemaRows(db, pChildCat, &aChildRows, &nChildRows);
    if( rc==SQLITE_OK ){
      rc = loadIndexSchemaRows(db, pParentCat, &aParentRows, &nParentRows);
    }
    if( rc==SQLITE_OK ){
      rc = diffCatalogPair(pCur, db, aChild, nChild, aParent, nParent,
                           aChildRows, nChildRows, aParentRows, nParentRows,
                           zHexBuf, 0);
    }
    freeSchemaEntries(aChildRows, nChildRows);
    freeSchemaEntries(aParentRows, nParentRows);
  }

  doltliteFreeCatalog(aParent, nParent);
  doltliteFreeCatalog(aChild, nChild);
  return rc;
}

/* Uncommitted diff: STAGED vs HEAD, WORKING vs staged. pseudoFilter may keep one label. */
static int computeWorkingBatch(DoltliteDiffCursor *pCur, sqlite3 *db){
  ProllyHash headCat, stagedCat, workCat;
  int rc;

  memset(&headCat, 0, sizeof(headCat));
  memset(&stagedCat, 0, sizeof(stagedCat));
  memset(&workCat, 0, sizeof(workCat));

  rc = doltliteGetHeadCatalogHash(db, &headCat);
  if( rc!=SQLITE_OK ) return rc;
  doltliteGetSessionStaged(db, &stagedCat);
  if( prollyHashIsEmpty(&stagedCat) ) stagedCat = headCat;
  rc = doltliteFlushCatalogToHash(db, &workCat);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->pseudoFilter!=1 ){
    rc = computeCatalogPairBatch(pCur, db, &stagedCat, &headCat, "STAGED");
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->pseudoFilter!=2 ){
    rc = computeCatalogPairBatch(pCur, db, &workCat, &stagedCat, "WORKING");
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int computeCommitBatch(DoltliteDiffCursor *pCur, sqlite3 *db,
                              const DoltliteCommit *pCommit,
                              const char *zCommitHex){
  struct TableEntry *aChild = 0, *aParent = 0;
  int nChild = 0, nParent = 0;
  int rc;
  const ProllyHash *pParentHash = doltliteCommitParentHash(pCommit, 0);
  int hasParent = (pParentHash && !prollyHashIsEmpty(pParentHash));

  if( pCur->zFilterTable && strcmp(pCur->zFilterTable, "dolt_schemas")!=0 ){
    ProllyHash parentCat;
    memset(&parentCat, 0, sizeof(parentCat));
    if( hasParent ){
      rc = doltliteCommitCatalogHash(db, pParentHash, &parentCat);
      if( rc!=SQLITE_OK ) return rc;
    }
    return diffFilteredTableRoots(pCur, db, &pCommit->catalogHash, &parentCat,
                                  zCommitHex, pCommit);
  }

  rc = doltliteLoadCatalog(db, &pCommit->catalogHash, &aChild, &nChild, 0);
  if( rc!=SQLITE_OK ) return rc;

  if( hasParent ){
    ProllyHash parentCat;
    rc = doltliteCommitCatalogHash(db, pParentHash, &parentCat);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &parentCat, &aParent, &nParent, 0);
    }
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aChild, nChild);
      return rc;
    }
  }

  {
    SchemaEntry *aChildRows = 0, *aParentRows = 0;
    int nChildRows = 0, nParentRows = 0;
    ProllyHash parentCat;
    memset(&parentCat, 0, sizeof(parentCat));
    if( hasParent ){
      rc = doltliteCommitCatalogHash(db, pParentHash, &parentCat);
      if( rc!=SQLITE_OK ){
        doltliteFreeCatalog(aChild, nChild);
        doltliteFreeCatalog(aParent, nParent);
        return rc;
      }
    }
    rc = loadIndexSchemaRows(db, &pCommit->catalogHash,
                             &aChildRows, &nChildRows);
    if( rc==SQLITE_OK && hasParent ){
      rc = loadIndexSchemaRows(db, &parentCat, &aParentRows, &nParentRows);
    }
    if( rc==SQLITE_OK ){
      rc = diffCatalogPair(pCur, db, aChild, nChild, aParent, nParent,
                           aChildRows, nChildRows, aParentRows, nParentRows,
                           zCommitHex, pCommit);
    }
    freeSchemaEntries(aChildRows, nChildRows);
    freeSchemaEntries(aParentRows, nParentRows);
  }

  doltliteFreeCatalog(aChild, nChild);
  doltliteFreeCatalog(aParent, nParent);
  return rc;
}

static int diffAdvance(DoltliteDiffCursor *pCur, sqlite3 *db){
  int rc;

  if( pCur->iBatch < pCur->nBatch ){
    pCur->hasRow = 1;
    return SQLITE_OK;
  }

  freeBatch(pCur);

  if( pCur->phase==0 ){
    pCur->phase = 1;
    rc = computeWorkingBatch(pCur, db);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->nBatch>0 ){
      pCur->hasRow = 1;
      return SQLITE_OK;
    }
  }

  for(;;){
    ProllyHash cur;
    DoltliteCommit commit;
    char zHex[PROLLY_HASH_SIZE*2+1];
    int hasHash;

    rc = doltliteCommitQueueNext(&pCur->queue, &cur, &hasHash);
    if( rc!=SQLITE_OK ) return rc;
    if( !hasHash ) break;

    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ) return rc;

    doltliteHashToHex(&cur, zHex);
    rc = computeCommitBatch(pCur, db, &commit, zHex);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      return rc;
    }

    if( !pCur->singleCommit ){
      rc = doltliteCommitQueueEnqueueParents(&pCur->queue, &commit);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }
    }
    doltliteCommitClear(&commit);

    if( pCur->nBatch>0 ){
      pCur->hasRow = 1;
      return SQLITE_OK;
    }
  }

  pCur->hasRow = 0;
  return SQLITE_OK;
}

static void diffCursorReset(DoltliteDiffCursor *pCur){
  freeBatch(pCur);
  doltliteCommitQueueClear(&pCur->queue);
  sqlite3_free(pCur->zFilterTable);
  pCur->zFilterTable = 0;
  pCur->phase = 0;
  pCur->hasRow = 0;
  pCur->pseudoFilter = 0;
}

static int diffConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DoltliteDiffVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, diffSchema, sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (DoltliteDiffVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int diffDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int diffBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iTableName = -1;
  int iCommitHash = -1;
  int i;
  int argvIdx = 1;
  int idxNum = 0;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( !doltliteVtabConstraintIsBinary(pInfo, i) ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case DIFF_COL_TABLE_NAME:
        if( iTableName<0 ) iTableName = i;
        break;
      case DIFF_COL_COMMIT_HASH:
        if( iCommitHash<0 ) iCommitHash = i;
        break;
    }
  }

  if( iCommitHash>=0 ){
    pInfo->aConstraintUsage[iCommitHash].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iCommitHash].omit = 0;
    idxNum |= DIFF_IDX_COMMIT_HASH;
  }
  if( iTableName>=0 ){
    pInfo->aConstraintUsage[iTableName].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTableName].omit = 0;
    idxNum |= DIFF_IDX_TABLE_NAME;
  }

  pInfo->idxNum = idxNum;
  if( idxNum & DIFF_IDX_COMMIT_HASH ){
    pInfo->estimatedCost = 10.0;
    pInfo->estimatedRows = 1;
  }else if( idxNum & DIFF_IDX_TABLE_NAME ){
    pInfo->estimatedCost = 1000.0;
    pInfo->estimatedRows = 100;
  }else{
    pInfo->estimatedCost = 100000.0;
    pInfo->estimatedRows = 100;
  }
  return SQLITE_OK;
}

static int diffOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(DoltliteDiffCursor));
}

static int diffClose(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  diffCursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int diffFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DoltliteDiffVtab *pVtab = (DoltliteDiffVtab*)pCursor->pVtab;
  sqlite3 *db = pVtab->db;
  ChunkStore *cs;
  ProllyHash head;
  ProllyHash startHash;
  int argIdx = 0;
  int rc;
  int useStart = 0;
  int workingOnly = 0;
  const char *zHashArg = 0;
  (void)idxStr;

  diffCursorReset(pCur);
  memset(&startHash, 0, sizeof(startHash));

  if( (idxNum & DIFF_IDX_COMMIT_HASH) && argIdx<argc ){
    zHashArg = (const char*)sqlite3_value_text(argv[argIdx++]);
    if( zHashArg ){
      if( strcmp(zHashArg, "WORKING")==0 ){
        workingOnly = 1;
        pCur->pseudoFilter = 1;
      }else if( strcmp(zHashArg, "STAGED")==0 ){
        workingOnly = 1;
        pCur->pseudoFilter = 2;
      }else if( doltliteHexToHash(zHashArg, &startHash)==SQLITE_OK
                && !prollyHashIsEmpty(&startHash) ){
        useStart = 1;
      }else{
        return SQLITE_OK;
      }
    }else{
      return SQLITE_OK;
    }
  }

  if( (idxNum & DIFF_IDX_TABLE_NAME) && argIdx<argc ){
    const char *z;
    /* NULL name matches no table. Unset filter would mean "no filter"; xBestIndex omits it. */
    if( sqlite3_value_type(argv[argIdx])==SQLITE_NULL ) return SQLITE_OK;
    z = (const char*)sqlite3_value_text(argv[argIdx++]);
    if( z ){
      pCur->zFilterTable = sqlite3_mprintf("%s", z);
      if( !pCur->zFilterTable ) return SQLITE_NOMEM;
    }
  }

  cs = doltliteGetChunkStore(db);
  if( !cs ) return SQLITE_OK;

  if( useStart ){
    head = startHash;
    pCur->singleCommit = 1;
  }else if( workingOnly ){
    pCur->phase = 0;
    rc = computeWorkingBatch(pCur, db);
    if( rc!=SQLITE_OK ) return rc;
    pCur->phase = 2;
    if( pCur->nBatch>0 ){
      pCur->hasRow = 1;
    }
    return SQLITE_OK;
  }else{
    doltliteGetSessionHead(db, &head);
    if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
    pCur->singleCommit = 0;
  }

  rc = doltliteCommitQueueInit(&pCur->queue, &head);
  if( rc!=SQLITE_OK ) return rc;

  if( useStart ){
    pCur->phase = 1;
  }else{
    pCur->phase = 0;
  }
  return diffAdvance(pCur, db);
}

static int diffNext(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DoltliteDiffVtab *pVtab = (DoltliteDiffVtab*)pCursor->pVtab;
  pCur->iBatch++;
  return diffAdvance(pCur, pVtab->db);
}

static int diffEof(sqlite3_vtab_cursor *pCursor){
  return !((DoltliteDiffCursor*)pCursor)->hasRow;
}

static int diffColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DiffSummaryRow *r;
  if( !pCur->hasRow || pCur->iBatch >= pCur->nBatch ) return SQLITE_OK;
  r = &pCur->aBatch[pCur->iBatch];
  switch( iCol ){
    case DIFF_COL_COMMIT_HASH:
      sqlite3_result_text(ctx, r->zCommitHex, -1, SQLITE_TRANSIENT);
      break;
    case DIFF_COL_COMMITTER:
      if( r->zCommitter ){
        sqlite3_result_text(ctx, r->zCommitter, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_EMAIL:
      if( r->zEmail ){
        sqlite3_result_text(ctx, r->zEmail, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_DATE: {
      if( r->timestamp==0 && r->zCommitter==0 ){
        sqlite3_result_null(ctx);
      }else{
        doltliteResultTimestamp(ctx, r->timestamp);
      }
      break;
    }
    case DIFF_COL_MESSAGE:
      if( r->zMessage ){
        sqlite3_result_text(ctx, r->zMessage, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_DATA_CHANGE:
      sqlite3_result_int(ctx, r->dataChange ? 1 : 0);
      break;
    case DIFF_COL_SCHEMA_CHANGE:
      sqlite3_result_int(ctx, r->schemaChange ? 1 : 0);
      break;
    case DIFF_COL_TABLE_NAME:
      sqlite3_result_text(ctx, r->zTableName, -1, SQLITE_TRANSIENT);
      break;
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int diffRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DiffSummaryRow *r = &pCur->aBatch[pCur->iBatch];
  u64 h = doltliteFnv1aStr(DOLTLITE_FNV1A_OFFSET, r->zCommitHex);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aStr(h, r->zTableName);
  *pRowid = doltliteFnv1aRowid(h);
  return SQLITE_OK;
}

static sqlite3_module doltliteDiffModule = {
  0, 0, diffConnect, diffBestIndex, diffDisconnect, 0,
  diffOpen, diffClose, diffFilter, diffNext, diffEof,
  diffColumn, diffRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_diff", &doltliteDiffModule, 0);
}

#endif
