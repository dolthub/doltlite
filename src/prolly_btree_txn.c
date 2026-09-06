#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

static void btreeTakeCatalogCache(Btree *p, u8 **ppData, int nData,
                                  const ProllyHash *pHash){
  sqlite3_free(p->pCatalogCache);
  p->pCatalogCache = *ppData;
  *ppData = 0;
  p->nCatalogCache = nData;
  if( pHash ){
    p->catalogCacheHash = *pHash;
  }else{
    memset(&p->catalogCacheHash, 0, sizeof(p->catalogCacheHash));
  }
}

static int findITableIndex(const void *a, int n, int stride, Pgno iTable);
static int restoreFromCommitted(Btree *p);
static void btreeDiscardAllSavepoints(Btree *p);

void freeSavepointTables(struct SavepointTableState *pState){
  sqlite3_free(pState->zRebaseOrigBranch);
  pState->zRebaseOrigBranch = 0;
  sqlite3_free(pState->zRebaseReturnBranch);
  pState->zRebaseReturnBranch = 0;
  if( pState->aCatalogSnapshot ){
    int i;
    for(i=0; i<pState->nTables; i++){
      sqlite3_free(pState->aCatalogSnapshot[i].zName);
    }
    sqlite3_free(pState->aCatalogSnapshot);
    pState->aCatalogSnapshot = 0;
    pState->bCatalogSnapshot = 0;
  }
  if( pState->aPendingSnapshot ){
    int i;
    for(i=0; i<pState->nPendingSnapshot; i++){
      if( pState->aPendingSnapshot[i].pPending ){
        prollyMutMapFree(pState->aPendingSnapshot[i].pPending);
        sqlite3_free(pState->aPendingSnapshot[i].pPending);
      }
    }
    sqlite3_free(pState->aPendingSnapshot);
    pState->aPendingSnapshot = 0;
    pState->nPendingSnapshot = 0;
    pState->nPendingSnapshotAlloc = 0;
  }
  if( pState->aTables ){
    int i;
    for(i=0; i<pState->nTables; i++){
      if( pState->aTables[i].pPending ){
        prollyMutMapFree(pState->aTables[i].pPending);
        sqlite3_free(pState->aTables[i].pPending);
      }
    }
    sqlite3_free(pState->aTables);
    pState->aTables = 0;
  }
  memset(&pState->vc, 0, sizeof(pState->vc));
  pState->isRebasing = 0;
  pState->bSchemaChangedTxn = 0;
  pState->bMasterRootChangedTxn = 0;
  memset(&pState->preRebaseWorkingCat, 0, sizeof(pState->preRebaseWorkingCat));
  memset(&pState->rebaseOntoCommit, 0, sizeof(pState->rebaseOntoCommit));
}

static int captureSavepointCatalogSnapshot(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  int i;
  int n = pBtree->cat.n;
  SavepointCatalogEntry *aSnapshot = 0;

  if( n<=0 ) return SQLITE_OK;
  aSnapshot = sqlite3_malloc64((sqlite3_uint64)n * sizeof(SavepointCatalogEntry));
  if( !aSnapshot ) return SQLITE_NOMEM;
  memset(aSnapshot, 0, (size_t)n * sizeof(SavepointCatalogEntry));

  for(i=0; i<n; i++){
    struct TableEntry *pSrc = &pBtree->cat.a[i];
    SavepointCatalogEntry *pDst = &aSnapshot[i];
    pDst->iTable = pSrc->iTable;
    pDst->root = pSrc->root;
    pDst->schemaHash = pSrc->schemaHash;
    pDst->flags = pSrc->flags;
    pDst->pendingFlushSeekEdits = pSrc->pendingFlushSeekEdits;
    pDst->tableRootKnown = pSrc->tableRootKnown;
    pDst->isTableRoot = pSrc->isTableRoot;
    if( pSrc->zName ){
      pDst->zName = sqlite3_mprintf("%s", pSrc->zName);
      if( !pDst->zName ){
        int j;
        for(j=0; j<=i; j++){
          sqlite3_free(aSnapshot[j].zName);
        }
        sqlite3_free(aSnapshot);
        return SQLITE_NOMEM;
      }
    }
  }

  pState->aCatalogSnapshot = aSnapshot;
  pState->bCatalogSnapshot = 1;
  pState->nTables = n;
  return SQLITE_OK;
}

static int captureSavepointSessionState(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  char *zOrigBranch = 0;
  char *zReturnBranch = 0;
  if( pBtree->zRebaseOrigBranch ){
    zOrigBranch = sqlite3_mprintf("%s", pBtree->zRebaseOrigBranch);
    if( !zOrigBranch ) return SQLITE_NOMEM;
  }
  if( pBtree->zRebaseReturnBranch ){
    zReturnBranch = sqlite3_mprintf("%s", pBtree->zRebaseReturnBranch);
    if( !zReturnBranch ){
      sqlite3_free(zOrigBranch);
      return SQLITE_NOMEM;
    }
  }
  pState->iNextTable = pBtree->cat.iNextTable;
  pState->iLargestRootPage = pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE];
  memcpy(pState->aMeta, pBtree->aMeta, sizeof(pState->aMeta));
  pState->vc = pBtree->vc;
  pState->isRebasing = pBtree->isRebasing;
  pState->preRebaseWorkingCat = pBtree->preRebaseWorkingCat;
  pState->rebaseOntoCommit = pBtree->rebaseOntoCommit;
  pState->zRebaseOrigBranch = zOrigBranch;
  pState->zRebaseReturnBranch = zReturnBranch;
  return SQLITE_OK;
}

static void restoreSavepointSessionState(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  pBtree->vc = pState->vc;
  memcpy(pBtree->aMeta, pState->aMeta, sizeof(pBtree->aMeta));
  pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE] = pState->iLargestRootPage;
  pBtree->isRebasing = pState->isRebasing;
  pBtree->preRebaseWorkingCat = pState->preRebaseWorkingCat;
  pBtree->rebaseOntoCommit = pState->rebaseOntoCommit;
  sqlite3_free(pBtree->zRebaseOrigBranch);
  pBtree->zRebaseOrigBranch = pState->zRebaseOrigBranch;
  pState->zRebaseOrigBranch = 0;
  sqlite3_free(pBtree->zRebaseReturnBranch);
  pBtree->zRebaseReturnBranch = pState->zRebaseReturnBranch;
  pState->zRebaseReturnBranch = 0;
}

static int captureSavepointTables(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  int k;
  int rc;
  if( pState->bTablesCaptured ) return SQLITE_OK;
  if( pState->aCatalogSnapshot ){
    for(k=0; k<pState->nTables; k++){
      sqlite3_free(pState->aCatalogSnapshot[k].zName);
    }
    sqlite3_free(pState->aCatalogSnapshot);
    pState->aCatalogSnapshot = 0;
    pState->bCatalogSnapshot = 0;
  }
  /* Snapshot live entries: the commit serializer renumbers iTable. */
  rc = captureSavepointCatalogSnapshot(pBtree, pState);
  if( rc!=SQLITE_OK ) return rc;
  if( pBtree->cat.n<=0 ){
    pState->bTablesCaptured = 1;
    return SQLITE_OK;
  }
  pState->aTables = sqlite3_malloc(
      pBtree->cat.n * (int)sizeof(SavepointTableEntry));
  if( !pState->aTables ){
    if( pState->aCatalogSnapshot ){
      for(k=0; k<pBtree->cat.n; k++){
        sqlite3_free(pState->aCatalogSnapshot[k].zName);
      }
      sqlite3_free(pState->aCatalogSnapshot);
      pState->aCatalogSnapshot = 0;
      pState->bCatalogSnapshot = 0;
    }
    return SQLITE_NOMEM;
  }
  for(k=0; k<pBtree->cat.n; k++){
    pState->aTables[k].iTable = pBtree->cat.a[k].iTable;
    pState->aTables[k].pendingFlushSeekEdits =
        pBtree->cat.a[k].pendingFlushSeekEdits;
    pState->aTables[k].pPending = 0;
  }
  pState->nTables = pBtree->cat.n;
  pState->bTablesCaptured = 1;
  return SQLITE_OK;
}

static int ensureSavepointTablesCaptured(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  if( pState->bTablesCaptured ) return SQLITE_OK;
  return captureSavepointTables(pBtree, pState);
}

int ensureStatementSavepointsCaptured(Btree *pBtree){
  int i;
  assert( pBtree!=0 );
  assert( pBtree->nSavepoint==0 || pBtree->aSavepointTables!=0 );
  for(i=0; i<pBtree->nSavepoint; i++){
    struct SavepointTableState *pState = &pBtree->aSavepointTables[i];
    if( pState->bStatement && !pState->bTablesCaptured ){
      int rc = ensureSavepointTablesCaptured(pBtree, pState);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static void pushSavepointOnMutMaps(Btree *pBtree, int level){
  int k;
  assert( pBtree!=0 );
  assert( level>=0 );
  for(k=0; k<pBtree->cat.n; k++){
    ProllyMutMap *pMap = (ProllyMutMap*)pBtree->cat.a[k].pPending;
    if( pMap ) prollyMutMapPushSavepoint(pMap, level);
  }
}

static int findPendingSnapshotIndex(
  struct SavepointTableState *pState,
  Pgno iTable
){
  int i;
  for(i=0; i<pState->nPendingSnapshot; i++){
    if( pState->aPendingSnapshot[i].iTable==iTable ){
      return i;
    }
  }
  return -1;
}

static ProllyMutMap *findPendingSnapshot(Btree *pBtree, int iFromSavepoint,
                                          Pgno iTable, int *piSavepoint,
                                          int *piSnapshot){
  int i;
  if( piSavepoint ) *piSavepoint = -1;
  if( piSnapshot ) *piSnapshot = -1;
  for(i=iFromSavepoint; i<pBtree->nSavepoint; i++){
    struct SavepointTableState *pState = &pBtree->aSavepointTables[i];
    int j;
    if( !pState->aPendingSnapshot ) continue;
    j = findPendingSnapshotIndex(pState, iTable);
    if( j >= 0 ){
      if( piSavepoint ) *piSavepoint = i;
      if( piSnapshot ) *piSnapshot = j;
      return pState->aPendingSnapshot[j].pPending;
    }
  }
  return 0;
}

static int allocEmptyPendingLike(ProllyMutMap *pSrc, ProllyMutMap **ppOut){
  ProllyMutMap *pNew;
  int rc;
  *ppOut = 0;
  pNew = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !pNew ) return SQLITE_NOMEM;
  rc = prollyMutMapInitMode(pNew, pSrc->isIntKey, pSrc->keepSorted);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pNew);
    return rc;
  }
  pNew->currentSavepointLevel = pSrc->currentSavepointLevel;
  *ppOut = pNew;
  return SQLITE_OK;
}

static int appendPendingSnapshot(
  struct SavepointTableState *pState,
  Pgno iTable,
  ProllyMutMap *pPending
){
  if( pState->nPendingSnapshot >= pState->nPendingSnapshotAlloc ){
    i64 nNew = pState->nPendingSnapshotAlloc
                 ? (i64)pState->nPendingSnapshotAlloc * 2 : (i64)4;
    SavepointPendingSnapshot *aNew;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(SavepointPendingSnapshot) ){
      return SQLITE_NOMEM;
    }
    aNew = sqlite3_realloc(pState->aPendingSnapshot,
        (int)(nNew * (i64)sizeof(SavepointPendingSnapshot)));
    if( !aNew ) return SQLITE_NOMEM;
    pState->aPendingSnapshot = aNew;
    pState->nPendingSnapshotAlloc = (int)nNew;
  }
  pState->aPendingSnapshot[pState->nPendingSnapshot].iTable = iTable;
  pState->aPendingSnapshot[pState->nPendingSnapshot].pPending = pPending;
  pState->nPendingSnapshot++;
  return SQLITE_OK;
}

static int inheritPendingSnapshots(
  struct SavepointTableState *pParent,
  struct SavepointTableState *pChild
){
  int i;
  if( !pParent || !pChild ) return SQLITE_OK;
  for(i=0; i<pChild->nPendingSnapshot; i++){
    SavepointPendingSnapshot *pSnap = &pChild->aPendingSnapshot[i];
    if( !pSnap->pPending ) continue;
    if( findITableIndex(pParent->aTables, pParent->nTables,
                        (int)sizeof(pParent->aTables[0]), pSnap->iTable) < 0 ){
      prollyMutMapFree(pSnap->pPending);
      sqlite3_free(pSnap->pPending);
      pSnap->pPending = 0;
      continue;
    }
    if( findPendingSnapshotIndex(pParent, pSnap->iTable) >= 0 ){
      prollyMutMapFree(pSnap->pPending);
      sqlite3_free(pSnap->pPending);
      pSnap->pPending = 0;
      continue;
    }
    if( appendPendingSnapshot(pParent, pSnap->iTable, pSnap->pPending)!=SQLITE_OK ){
      return SQLITE_NOMEM;
    }
    pSnap->pPending = 0;
  }
  return SQLITE_OK;
}

static int rollbackMutMapsToSavepoint(Btree *pBtree, int level,
                                       int iFromSavepoint){
  int k, rc;
  for(k=0; k<pBtree->cat.n; k++){
    struct TableEntry *pTE = &pBtree->cat.a[k];
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    int iSavepoint = -1;
    int iSnapshot = -1;
    ProllyMutMap *pSnap = findPendingSnapshot(pBtree, iFromSavepoint,
                                               pTE->iTable,
                                               &iSavepoint, &iSnapshot);
    if( pSnap ){
      if( pMap ){
        prollyMutMapFree(pMap);
        sqlite3_free(pMap);
      }
      pTE->pPending = pSnap;
      pBtree->aSavepointTables[iSavepoint].aPendingSnapshot[iSnapshot].pPending = 0;
      rc = prollyMutMapRollbackToSavepoint(pSnap, level);
      if( rc!=SQLITE_OK ) return rc;
    }else if( pMap ){
      rc = prollyMutMapRollbackToSavepoint(pMap, level);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static void releaseMutMapsToSavepoint(Btree *pBtree, int level){
  int k;
  for(k=0; k<pBtree->cat.n; k++){
    ProllyMutMap *pMap = (ProllyMutMap*)pBtree->cat.a[k].pPending;
    if( pMap ) prollyMutMapReleaseSavepoint(pMap, level);
  }
}

/* Move a flushed map into the savepoint snapshot; rollback restores it. */
int snapshotPendingForFlush(Btree *pBtree, Pgno iTable,
                                   ProllyMutMap **ppPending,
                                   ProllyMutMap **ppFlushMap,
                                   int *pCaptured){
  int i;
  ProllyMutMap *pPending;
  if( pCaptured ) *pCaptured = 0;
  if( !ppFlushMap ) return SQLITE_MISUSE;
  *ppFlushMap = 0;
  if( !pBtree || !ppPending || !*ppPending ) return SQLITE_OK;
  assert( pBtree->inTrans==TRANS_WRITE );
  pPending = *ppPending;
  *ppFlushMap = pPending;
  if( pBtree->nSavepoint <= 0 ) return SQLITE_OK;
  if( prollyMutMapIsEmpty(pPending) ) return SQLITE_OK;

  for(i = pBtree->nSavepoint - 1; i >= 0; i--){
    struct SavepointTableState *pState = &pBtree->aSavepointTables[i];
    int j;
    if( pState->bStatement && !pState->bTablesCaptured ){
      int rc = ensureSavepointTablesCaptured(pBtree, pState);
      if( rc!=SQLITE_OK ) return rc;
    }
    j = findITableIndex(pState->aTables, pState->nTables,
                        (int)sizeof(pState->aTables[0]), iTable);
    if( j < 0 ) continue;
    if( findPendingSnapshotIndex(pState, iTable) >= 0 ) return SQLITE_OK;
    {
      ProllyMutMap *pNewPending = 0;
      int rc = allocEmptyPendingLike(pPending, &pNewPending);
      if( rc!=SQLITE_OK ) return rc;
      rc = appendPendingSnapshot(pState, iTable, pPending);
      if( rc!=SQLITE_OK ){
        prollyMutMapFree(pNewPending);
        sqlite3_free(pNewPending);
        return rc;
      }
      *ppPending = pNewPending;
      if( pCaptured ) *pCaptured = 1;
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int findITableIndex(
  const void *a,
  int n,
  int stride,
  Pgno iTable
){
  const unsigned char *p = (const unsigned char*)a;
  int lo = 0;
  int hi = n;
  if( !a || n<=0 || stride<=0 ) return -1;
  while( lo < hi ){
    int mid = lo + ((hi - lo) / 2);
    Pgno midTable = *(const Pgno*)(p + (size_t)mid * (size_t)stride);
    if( midTable==iTable ){
      return mid;
    }
    if( midTable < iTable ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return -1;
}

static int restoreTablesFromSavepoint(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  int k;
  int rc;
  struct TableEntry *aCurrent = pBtree->cat.a;
  int nCurrent = pBtree->cat.n;
  int bRestoredCatalogInPlace = 0;

  if( pState->nTables>0 ){
    if( pBtree->cat.nAlloc < pState->nTables ){
      struct TableEntry *aNew = sqlite3_realloc(
          pBtree->cat.a, pState->nTables * (int)sizeof(struct TableEntry));
      if( !aNew ){
        return SQLITE_NOMEM;
      }
      pBtree->cat.a = aNew;
      pBtree->cat.nAlloc = pState->nTables;
      aCurrent = pBtree->cat.a;
    }
  }

  for(k=0; k<nCurrent; k++){
    ProllyMutMap *pMap = aCurrent[k].pPending;
    int iSaved;
    if( !pMap ){
      continue;
    }
    iSaved = findITableIndex(
        pState->aTables, pState->nTables,
        (int)sizeof(pState->aTables[0]), aCurrent[k].iTable);
    if( iSaved>=0 ){
      pState->aTables[iSaved].pPending = pMap;
    }else{
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
    }
    aCurrent[k].pPending = 0;
  }

  if( pState->bCatalogSnapshot ){
    if( pBtree->cat.n==pState->nTables ){
      int bSameShape = 1;
      for(k=0; k<pState->nTables; k++){
        if( pBtree->cat.a[k].iTable!=pState->aCatalogSnapshot[k].iTable ){
          bSameShape = 0;
          break;
        }
      }
      if( bSameShape ){
        for(k=0; k<pState->nTables; k++){
          struct TableEntry *pDst = &pBtree->cat.a[k];
          SavepointCatalogEntry *pSrc = &pState->aCatalogSnapshot[k];
          pDst->root = pSrc->root;
          pDst->schemaHash = pSrc->schemaHash;
          pDst->flags = pSrc->flags;
          pDst->pendingFlushSeekEdits = pSrc->pendingFlushSeekEdits;
          pDst->tableRootKnown = pSrc->tableRootKnown;
          pDst->isTableRoot = pSrc->isTableRoot;
        }
        bRestoredCatalogInPlace = 1;
      }
    }
    if( !bRestoredCatalogInPlace ){
      btreeFreeCatalogTables(pBtree);
      initDefaultMeta(pBtree);
      if( pState->nTables>0 ){
        if( pBtree->cat.nAlloc < pState->nTables ){
          struct TableEntry *aNew = sqlite3_realloc(
              pBtree->cat.a, pState->nTables * (int)sizeof(struct TableEntry));
          if( !aNew ){
            return SQLITE_NOMEM;
          }
          pBtree->cat.a = aNew;
          pBtree->cat.nAlloc = pState->nTables;
        }
        memset(pBtree->cat.a, 0, pState->nTables * sizeof(struct TableEntry));
        for(k=0; k<pState->nTables; k++){
          struct TableEntry *pDst = &pBtree->cat.a[k];
          SavepointCatalogEntry *pSrc = &pState->aCatalogSnapshot[k];
          pDst->iTable = pSrc->iTable;
          pDst->root = pSrc->root;
          pDst->schemaHash = pSrc->schemaHash;
          pDst->flags = pSrc->flags;
          pDst->pendingFlushSeekEdits = pSrc->pendingFlushSeekEdits;
          pDst->tableRootKnown = pSrc->tableRootKnown;
          pDst->isTableRoot = pSrc->isTableRoot;
          if( pSrc->zName ){
            pDst->zName = sqlite3_mprintf("%s", pSrc->zName);
            if( !pDst->zName ){
              btreeFreeCatalogTables(pBtree);
              initDefaultMeta(pBtree);
              return SQLITE_NOMEM;
            }
          }
        }
      }
      pBtree->cat.n = pState->nTables;
    }
    pBtree->cat.iNextTable = pState->iNextTable;
  }else{
    btreeFreeCatalogTables(pBtree);
    initDefaultMeta(pBtree);
    pBtree->cat.iNextTable = 2;
  }

  if( pState->nTables>0 ){
    for(k=0; k<pState->nTables; k++){
      int idx = findITableIndex(
          pBtree->cat.a, pBtree->cat.n,
          (int)sizeof(pBtree->cat.a[0]), pState->aTables[k].iTable);
      if( idx < 0 ){
        return SQLITE_CORRUPT;
      }
      pBtree->cat.a[idx].pendingFlushSeekEdits =
          pState->aTables[k].pendingFlushSeekEdits;
      if( pState->aTables[k].pPending==0 ){
        int iThis = (int)(pState - pBtree->aSavepointTables);
        int iSp = -1, iSnap = -1;
        ProllyMutMap *pSnap = findPendingSnapshot(
            pBtree, iThis, pState->aTables[k].iTable, &iSp, &iSnap);
        if( pSnap ){
          pBtree->aSavepointTables[iSp].aPendingSnapshot[iSnap].pPending = 0;
          rc = prollyMutMapRollbackToSavepoint(pSnap, iThis + 1);
          if( rc!=SQLITE_OK ){
            prollyMutMapFree(pSnap);
            sqlite3_free(pSnap);
            return rc;
          }
          pState->aTables[k].pPending = pSnap;
        }
      }
      pBtree->cat.a[idx].pPending = pState->aTables[k].pPending;
      pState->aTables[k].pPending = 0;
    }
  }

  pBtree->cat.iNextTable = pState->iNextTable;
  return SQLITE_OK;
}

int pushSavepoint(Btree *pBtree, int bStatement){
  struct SavepointTableState *pState;
  int rc;

  assert( pBtree!=0 );
  PROLLY_ASSERT_WRITE_TXN(pBtree);
  if( pBtree->nSavepoint>=pBtree->nSavepointAlloc ){
    i64 nNew = pBtree->nSavepointAlloc
                 ? (i64)pBtree->nSavepointAlloc * 2 : (i64)8;
    struct SavepointTableState *aNewT;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(struct SavepointTableState) ){
      return SQLITE_NOMEM;
    }
    aNewT = sqlite3_realloc(pBtree->aSavepointTables,
        (int)(nNew * (i64)sizeof(struct SavepointTableState)));
    if( !aNewT ) return SQLITE_NOMEM;
    pBtree->aSavepointTables = aNewT;
    pBtree->nSavepointAlloc = (int)nNew;
  }

  pState = &pBtree->aSavepointTables[pBtree->nSavepoint];
  pState->aTables = 0;
  pState->aCatalogSnapshot = 0;
  pState->bCatalogSnapshot = 0;
  pState->aPendingSnapshot = 0;
  pState->nPendingSnapshot = 0;
  pState->nPendingSnapshotAlloc = 0;
  pState->bStatement = (u8)bStatement;
  pState->bTablesCaptured = 0;
  pState->bSchemaChangedTxn = pBtree->bSchemaChangedTxn;
  pState->bMasterRootChangedTxn = pBtree->bMasterRootChangedTxn;
  pState->nTables = 0;
  pState->iLargestRootPage = 0;
  pState->isRebasing = 0;
  memset(&pState->preRebaseWorkingCat, 0, sizeof(pState->preRebaseWorkingCat));
  memset(&pState->rebaseOntoCommit, 0, sizeof(pState->rebaseOntoCommit));
  pState->zRebaseOrigBranch = 0;
  pState->zRebaseReturnBranch = 0;
  rc = captureSavepointSessionState(pBtree, pState);
  if( rc!=SQLITE_OK ){
    freeSavepointTables(pState);
    return rc;
  }
  if( !bStatement ){
    rc = captureSavepointTables(pBtree, pState);
    if( rc!=SQLITE_OK ){
      freeSavepointTables(pState);
      return rc;
    }
  }

  pBtree->nSavepoint++;
  pushSavepointOnMutMaps(pBtree, pBtree->nSavepoint);
  assert( pBtree->nSavepoint<=pBtree->nSavepointAlloc );
  return SQLITE_OK;
}


int prollyBtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  BtShared *pBt = p->pBt;
  int rc;

  p->bBeginTransBranchMissing = 0;
  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }

  /* Open succeeds on garbage (stock timing); first use is SQLITE_NOTADB. */
  if( pBt->store.notADatabase ) return SQLITE_NOTADB;

  if( p->bDeferredOpen ){
    rc = doltliteBtreeHydrateDeferred(p);
    if( rc!=SQLITE_OK ) return rc;
    if( pSchemaVersion ){
      *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
    }
  }

  if( p->inTrans==TRANS_WRITE ){
    return SQLITE_OK;
  }

  if( p->inTrans==TRANS_READ && !wrFlag ){
    if( p->db && p->db->autoCommit && !p->db->pSavepoint ){
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      pBt->store.snapshotPinned = 0;
    }else{
      return SQLITE_OK;
    }
  }

  if( p->inTrans==TRANS_READ
   && wrFlag
   && p->iLoadedWorkingStateVersion!=pBt->iWorkingStateVersion ){
    return SQLITE_BUSY_SNAPSHOT;
  }

  if( !wrFlag ){
    rc = btreeRefreshFromDisk(p);
    if( rc!=SQLITE_OK ) return rc;
    if( p->inTrans==TRANS_NONE ){
      rc = btreeRefreshSharedWorkingState(p);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( pSchemaVersion ){
      *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
    }
  }else if( p->inTrans==TRANS_NONE ){
    rc = btreeRefreshSharedWorkingState(p);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( wrFlag ){
    int bStoreChanged = 0;
    int nSavepointStart = p->nSavepoint;
    if( p->isDetached || (pBt->btsFlags & BTS_READ_ONLY)
     || (p->db && (p->db->flags & SQLITE_QueryOnly)!=0) ){
      return SQLITE_READONLY;
    }

    do {
      rc = chunkStoreLockAndRefreshChanged(&pBt->store, &bStoreChanged);
    }while( rc==SQLITE_BUSY && prollyInvokeBusyHandler(pBt) );
    if( rc!=SQLITE_OK ) return rc;

    if( p->inTrans==TRANS_READ ){
      int bChanged = 0;
      rc = chunkStoreHasExternalChanges(&pBt->store, &bChanged);
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        return rc;
      }
      if( bChanged ){
        chunkStoreUnlock(&pBt->store);
        return SQLITE_BUSY_SNAPSHOT;
      }
    }

    if( p->inTrans==TRANS_READ
     || bStoreChanged
     || p->iLoadedWorkingStateVersion!=pBt->iWorkingStateVersion
     || (prollyHashIsEmpty(&p->committedCatalogHash) && p->cat.n>1) ){
      ProllyHash loadedCatHash;
      memset(&loadedCatHash, 0, sizeof(loadedCatHash));
      rc = btreeReloadBranchWorkingStateInto(p, 1, &loadedCatHash);
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        return rc;
      }
      btreeStoreCommittedFromCurrent(p, &loadedCatHash);
      p->bCatalogDropped = 0;
    }
    btreeStoreCommittedFromCurrent(p, 0);

    /* Refuse writes onto a peer-deleted branch: persist would resurrect it.
    ** Last safe refusal point before phase two. Empty refs/head are seeding. */
    if( !prollyHashIsEmpty(&p->headCommit)
     && !p->isDetached
     && pBt->store.refs.nBranches>0 ){
      const char *zBranchName = p->zBranch ? p->zBranch : "main";
      if( chunkStoreFindBranch(&pBt->store, zBranchName, 0)==SQLITE_NOTFOUND ){
        p->bBeginTransBranchMissing = 1;
        chunkStoreUnlock(&pBt->store);
        return SQLITE_BUSY_SNAPSHOT;
      }
    }

    /* Become a write before pushSavepoint, which asserts TRANS_WRITE. */
    {
      u8 inTransBefore = p->inTrans;
      u8 inTransactionBefore = p->inTransaction;
      p->inTrans = TRANS_WRITE;
      p->inTransaction = TRANS_WRITE;
      if( p->db ){
        while( p->nSavepoint < p->db->nSavepoint ){
          int rc2 = pushSavepoint(p, 0);
          if( rc2!=SQLITE_OK ){
            while( p->nSavepoint > nSavepointStart ){
              p->nSavepoint--;
              freeSavepointTables(&p->aSavepointTables[p->nSavepoint]);
            }
            p->inTrans = inTransBefore;
            p->inTransaction = inTransactionBefore;
            chunkStoreUnlock(&pBt->store);
            return rc2;
          }
        }
      }
    }
    PROLLY_ASSERT_GRAPH_LOCKED(pBt);
  } else {
    if( p->inTrans==TRANS_NONE ){
      p->inTrans = TRANS_READ;
      if( p->inTransaction==TRANS_NONE ){
        p->inTransaction = TRANS_READ;
      }
    }
  }

  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }
  pBt->store.snapshotPinned = 1;

  return SQLITE_OK;
}

const char *doltliteBtreeMissingWriteBranch(Btree *p){
  if( !p || !p->bBeginTransBranchMissing ) return 0;
  return p->zBranch ? p->zBranch : "main";
}

int sqlite3BtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  if( wrFlag && p && p->db && p->db->nDb>0 && p->db->aDb[0].pBt
   && p->db->aDb[0].pBt->isDetached ){
    return SQLITE_READONLY;
  }
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginTrans(p, wrFlag, pSchemaVersion);
}

/* Conflicts are in-txn only. Veto here (phase two already writes). Nested
** VdbeExec>1 is inner SQL inside dolt_* and must not be refused. */
int prollyBtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  (void)zSuperJrnl;
  if( !p || p->inTrans!=TRANS_WRITE ) return SQLITE_OK;
  if( p->pBt && p->pBt->inCatalogSerialize ) return SQLITE_OK;
  if( p->db->nVdbeExec>1 ) return SQLITE_OK;
  if( !prollyHashIsEmpty(&p->vc.conflictsCatalogHash) ){
    sqlite3ErrorWithMsg(p->db, SQLITE_CONSTRAINT,
      "cannot merge: unresolved conflicts cannot be committed");
    return SQLITE_CONSTRAINT;
  }
  return SQLITE_OK;
}
int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommitPhaseOne(p, zSuperJrnl);
}

static void commitPhaseTwoReleaseGraph(BtShared *pBt){
  chunkStoreUnlock(&pBt->store);
  pBt->store.snapshotPinned = 0;
}

static void commitPhaseTwoAbort(
  BtShared *pBt,
  u8 **ppCatData,
  Btree *pReload,
  int *pbHaveReload
){
  if( pbHaveReload && *pbHaveReload && pReload ){
    catFree(&pReload->cat);
    *pbHaveReload = 0;
  }
  if( ppCatData ){
    sqlite3_free(*ppCatData);
    *ppCatData = 0;
  }
  chunkStoreRollback(&pBt->store);
  commitPhaseTwoReleaseGraph(pBt);
}

static int commitPhaseTwoStageCatalog(
  Btree *p,
  BtShared *pBt,
  u8 **ppCatData,
  int *pnCatData,
  ProllyHash *pCatHash
){
  int rc;
  const char *zBr;

  pBt->inCatalogSerialize = 1;
  rc = serializeCatalogForCommit(p, ppCatData, pnCatData);
  pBt->inCatalogSerialize = 0;
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(&pBt->store, *ppCatData, *pnCatData, pCatHash);
  }
  if( rc!=SQLITE_OK ) return rc;

  zBr = p->zBranch ? p->zBranch : "main";
  rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, pCatHash,
                                &p->headCommit,
                                &p->vc.stagedCatalog, p->vc.isMerging,
                                &p->vc.mergeCommitHash,
                                &p->vc.conflictsCatalogHash,
                                p->isRebasing,
                                &p->preRebaseWorkingCat,
                                &p->rebaseOntoCommit,
                                p->zRebaseOrigBranch,
                                p->zRebaseReturnBranch,
                                &p->vc.constraintViolationsHash);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreSerializeRefs(&pBt->store);
}

static int commitPhaseTwoPrepSchemaReload(
  Btree *p,
  BtShared *pBt,
  const u8 *catData,
  int nCatData,
  Btree *pReload,
  u32 *pNativeSchemaCookie
){
  BtCursor *pC;
  int rc;

  *pNativeSchemaCookie = p->aMeta[BTREE_SCHEMA_VERSION];
  memcpy(pReload->aMeta, p->aMeta, sizeof(pReload->aMeta));
  rc = deserializeCatalog(pReload, catData, nCatData);
  if( rc!=SQLITE_OK ){
    catFree(&pReload->cat);
    return rc;
  }
  for(pC = pBt->pCursor; pC; pC = pC->pNext){
    if( pC->pBtree==p ){
      if( (pC->eState==CURSOR_VALID || pC->eState==CURSOR_SKIPNEXT)
       && saveCursorPosition(pC)!=SQLITE_OK ){
        pC->eState = CURSOR_FAULT;
        pC->skipNext = SQLITE_ABORT;
        prollyCursorReleaseAll(&pC->pCur);
      }
      pC->pMutMap = 0;
      pC->mmActive = 0;
      pC->mmPhysActive = 0;
      pC->deferredTreeSeek = 0;
      pC->mmIdx = -1;
      pC->mmPhysIdx = -1;
    }else{
      pC->eState = CURSOR_FAULT;
      pC->skipNext = SQLITE_ABORT;
      pC->mmActive = 0;
      prollyCursorReleaseAll(&pC->pCur);
    }
  }
  return SQLITE_OK;
}

static void commitPhaseTwoAdoptReloadedCatalog(
  Btree *p,
  BtShared *pBt,
  Btree *pReload,
  int *pbHaveReload,
  u32 nativeSchemaCookie
){
  BtCursor *pC;

  if( *pbHaveReload ){
    btreeFreeCatalogTables(p);
    p->cat = pReload->cat;
    memcpy(p->aMeta, pReload->aMeta, sizeof(p->aMeta));
    /* Keep sqlite's cookie, not the reload's hash-derived one. */
    p->aMeta[BTREE_SCHEMA_VERSION] = nativeSchemaCookie;
    memset(&pReload->cat, 0, sizeof(pReload->cat));
    *pbHaveReload = 0;
  }
  /* Fault saved cursors whose table vanished in the reload. */
  for(pC = pBt->pCursor; pC; pC = pC->pNext){
    if( pC->pBtree==p && pC->eState==CURSOR_REQUIRESEEK
     && findTable(p, pC->pgnoRoot)==0 ){
      pC->eState = CURSOR_FAULT;
      pC->skipNext = SQLITE_ABORT;
    }
  }
  invalidateSchema(p);
  if( p->db ){
    /* Hard expiry: canonical numbering can move compiled rootpages. */
    sqlite3ExpirePreparedStatements(p->db, 1);
    sqlite3ResetAllSchemasOfConnection(p->db);
  }
}

static void commitPhaseTwoEndWriteTxn(Btree *p){
  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  btreeDiscardAllSavepoints(p);
  p->bSchemaChangedTxn = 0;
  p->bMasterRootChangedTxn = 0;
}

static int commitPhaseTwoFailRestore(
  Btree *p,
  BtShared *pBt,
  int commitRc,
  u8 **ppCatData,
  Btree *pReload,
  int *pbHaveReload
){
  int rc2;

  if( *pbHaveReload ){
    catFree(&pReload->cat);
    *pbHaveReload = 0;
  }
  sqlite3_free(*ppCatData);
  *ppCatData = 0;
  /* Null map aliases before restoreFromCommitted frees them (UAF). */
  {
    BtCursor *pC;
    for(pC = pBt->pCursor; pC; pC = pC->pNext){
      if( pC->pBtree!=p || pC->pMutMap==0 ) continue;
      pC->pMutMap = 0;
      pC->mmActive = 0;
      pC->mmPhysActive = 0;
      pC->deferredTreeSeek = 0;
      pC->mmIdx = -1;
      pC->mmPhysIdx = -1;
    }
  }
  rc2 = restoreFromCommitted(p);
  if( rc2!=SQLITE_OK ){
    /* Restore OOM'd; discard partial state so autocommit cannot publish it. */
    btreeFreeCatalogTables(p);
    memset(&p->committedCatalogHash, 0, sizeof(p->committedCatalogHash));
    p->bCatalogDropped = 1;
    p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion - 1;
    resetConnectionSchema(p);
    chunkStoreRollback(&pBt->store);
    commitPhaseTwoEndWriteTxn(p);
    commitPhaseTwoReleaseGraph(pBt);
    return commitRc;
  }
  invalidateCursors(pBt, 0, commitRc);
  resetConnectionSchema(p);
  chunkStoreRollback(&pBt->store);
  commitPhaseTwoEndWriteTxn(p);
  commitPhaseTwoReleaseGraph(pBt);
  return commitRc;
}

static SQLITE_NOINLINE int commitPhaseTwoWrite(Btree *p, BtShared *pBt){
  int rc = SQLITE_OK;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash catHash;
  Btree reloadBtree;
  int bReloadSchema = 0;
  int bHaveReloadCatalog = 0;
  u32 nativeSchemaCookie = 0;
  memset(&reloadBtree, 0, sizeof(reloadBtree));

  PROLLY_ASSERT_GRAPH_LOCKED(pBt);
  rc = flushAllPending(p, pBt, 0);
  if( rc!=SQLITE_OK ){
    commitPhaseTwoAbort(pBt, 0, 0, 0);
    return rc;
  }

  rc = commitPhaseTwoStageCatalog(p, pBt, &catData, &nCatData, &catHash);
  if( rc!=SQLITE_OK ){
    commitPhaseTwoAbort(pBt, &catData, 0, 0);
    return rc;
  }

  /* Schema commits adopt the canonical catalog (live == persisted). */
  bReloadSchema = p->bSchemaChangedTxn;
  if( bReloadSchema ){
    rc = commitPhaseTwoPrepSchemaReload(
        p, pBt, catData, nCatData, &reloadBtree, &nativeSchemaCookie);
    if( rc!=SQLITE_OK ){
      commitPhaseTwoAbort(pBt, &catData, 0, 0);
      return rc;
    }
    bHaveReloadCatalog = 1;
  }

  /* Nested dolt_* statement commit: leave conflicted chunks pending. */
  if( !prollyHashIsEmpty(&p->vc.conflictsCatalogHash) ){
    if( bHaveReloadCatalog ){
      catFree(&reloadBtree.cat);
      bHaveReloadCatalog = 0;
    }
    sqlite3_free(catData);
    commitPhaseTwoReleaseGraph(pBt);
    p->inTrans = TRANS_READ;
    return SQLITE_OK;
  }

  rc = chunkStoreCommit(&pBt->store);
  if( rc!=SQLITE_OK ){
    return commitPhaseTwoFailRestore(
        p, pBt, rc, &catData, &reloadBtree, &bHaveReloadCatalog);
  }

  p->committedCatalogHash = catHash;
  p->committedVc = p->vc;
  memcpy(p->committedAMeta, p->aMeta, sizeof(p->committedAMeta));
  btreeMarkWorkingStateChanged(p, 1);
  if( bReloadSchema ){
    commitPhaseTwoAdoptReloadedCatalog(
        p, pBt, &reloadBtree, &bHaveReloadCatalog, nativeSchemaCookie);
  }
  commitPhaseTwoEndWriteTxn(p);

  btreeTakeCatalogCache(p, &catData, nCatData, &catHash);
  assert( p->nCatalogCache==nCatData );
  assert( prollyHashCompare(&p->catalogCacheHash, &catHash)==0 );
  sqlite3_free(catData);
  commitPhaseTwoReleaseGraph(pBt);
  return SQLITE_OK;
}

int prollyBtreeCommitPhaseTwo(Btree *p, int bCleanup){
  BtShared *pBt = p->pBt;
  (void)bCleanup;

  /* Nested commit from catalog-serialize SELECT finalize: no-op. */
  if( pBt->inCatalogSerialize ) return SQLITE_OK;

  if( p->inTrans!=TRANS_WRITE ){
    commitPhaseTwoEndWriteTxn(p);
    commitPhaseTwoReleaseGraph(pBt);
    return SQLITE_OK;
  }
  return commitPhaseTwoWrite(p, pBt);
}
int sqlite3BtreeCommitPhaseTwo(Btree *p, int bCleanup){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommitPhaseTwo(p, bCleanup);
}

int prollyBtreeCommit(Btree *p){
  int rc;
  rc = p->pOps->xCommitPhaseOne(p, 0);
  if( rc==SQLITE_OK ){
    rc = p->pOps->xCommitPhaseTwo(p, 0);
  }
  return rc;
}
int sqlite3BtreeCommit(Btree *p){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommit(p);
}

static int restoreFromCommitted(Btree *p){
  assert( p!=0 && p->pBt!=0 );
  if( prollyHashIsEmpty(&p->committedCatalogHash) ){
    if( p->bCatalogDropped ){
      /* OOM drop: do not install an empty catalog over real tables. Reload. */
      ProllyHash loadedCatHash;
      int rc;
      memset(&loadedCatHash, 0, sizeof(loadedCatHash));
      rc = btreeReloadBranchWorkingStateInto(p, 1, &loadedCatHash);
      if( rc!=SQLITE_OK ) return rc;
      memcpy(p->aMeta, p->committedAMeta, sizeof(p->aMeta));
      btreeStoreCommittedFromCurrent(p, &loadedCatHash);
      p->bCatalogDropped = 0;
      return SQLITE_OK;
    }
    btreeFreeCatalogTables(p);
    initDefaultMeta(p);
    p->cat.iNextTable = 2;
  }else{
    u8 *catData = 0;
    int nCatData = 0;
    int rc = SQLITE_NOTFOUND;
    if( p->pCatalogCache
     && p->nCatalogCache>0
     && !p->bSchemaChangedTxn
     && !p->bMasterRootChangedTxn
     && prollyHashCompare(&p->catalogCacheHash,
                          &p->committedCatalogHash)==0 ){
      const u8 *q;
      int nTables = 0;
      int iFormat = 0;
      int i;
      if( catalogParseHeaderEx(p->pCatalogCache, p->nCatalogCache,
                               &iFormat, &nTables, &q)
       && nTables==p->cat.n ){
        rc = SQLITE_OK;
        for(i=0; i<nTables; i++){
          Pgno iTable;
          u8 flags;
          int nSkip;
          struct TableEntry *pTE;
          if( q + CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE
              + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE
              > p->pCatalogCache + p->nCatalogCache ){
            rc = SQLITE_CORRUPT;
            break;
          }
          iTable = (Pgno)prollyBtreeGetU32LE(q);
          pTE = &p->cat.a[i];
          if( pTE->iTable!=iTable ){
            rc = SQLITE_NOTFOUND;
            break;
          }
          if( pTE->pPending ){
            prollyMutMapFree(pTE->pPending);
            sqlite3_free(pTE->pPending);
            pTE->pPending = 0;
          }
          q += CAT_ENTRY_ITABLE_SIZE;
          flags = *q++;
          pTE->flags = flags;
          memcpy(pTE->root.data, q, PROLLY_HASH_SIZE);
          q += PROLLY_HASH_SIZE;
          memcpy(pTE->schemaHash.data, q, PROLLY_HASH_SIZE);
          q += PROLLY_HASH_SIZE;
          if( iFormat!=CATALOG_FORMAT_V3 ){
            int nType, nName, nTbl;
            if( q + 6 > p->pCatalogCache + p->nCatalogCache ){
              rc = SQLITE_CORRUPT;
              break;
            }
            nType = q[0] | (q[1]<<8);
            nName = q[2] | (q[3]<<8);
            nTbl = q[4] | (q[5]<<8);
            q += 6;
            nSkip = nType + nName + nTbl;
          }else{
            if( q + 2 > p->pCatalogCache + p->nCatalogCache ){
              rc = SQLITE_CORRUPT;
              break;
            }
            nSkip = q[0] | (q[1]<<8);
            q += 2;
          }
          if( nSkip<0 || q + nSkip > p->pCatalogCache + p->nCatalogCache ){
            rc = SQLITE_CORRUPT;
            break;
          }
          q += nSkip;
        }
        if( rc==SQLITE_OK && q!=p->pCatalogCache+p->nCatalogCache ){
          rc = SQLITE_CORRUPT;
        }
      }
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = chunkStoreGet(&p->pBt->store, &p->committedCatalogHash,
                         &catData, &nCatData);
      if( rc!=SQLITE_OK ) return rc;
      rc = deserializeCatalog(p, catData, nCatData);
      sqlite3_free(catData);
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  p->vc = p->committedVc;
  memcpy(p->aMeta, p->committedAMeta, sizeof(p->aMeta));
  return SQLITE_OK;
}

/* Clear TRANS_WRITE before unlocking; sqlite3RollbackAll discards rc. */
static void rollbackAbandonWriteTxn(Btree *p, BtShared *pBt){
  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  btreeDiscardAllSavepoints(p);
  chunkStoreUnlock(&pBt->store);
  pBt->store.snapshotPinned = 0;
}

int prollyBtreeRollback(Btree *p, int tripCode, int writeOnly){
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  int bSchemaChangedRollback = rollbackNeedsSchemaReset(p);
  int bAutocommitOomRollback = writeOnly
      && p->db
      && p->db->autoCommit
      && p->db->mallocFailed;

  if( pBt->inCatalogSerialize ) return SQLITE_OK;

  if( p->inTrans==TRANS_WRITE ){
    PROLLY_ASSERT_GRAPH_LOCKED(pBt);
    /* Save read cursors; fault writers. Null map aliases after save (UAF). */
    {
      BtCursor *pC;
      int tc = tripCode ? tripCode : SQLITE_ABORT;
      for(pC = pBt->pCursor; pC; pC = pC->pNext){
        if( pC->pBtree!=p ) continue;
        if( writeOnly
         && (pC->curFlags & BTCF_WriteFlag)==0
         && (pC->eState==CURSOR_VALID || pC->eState==CURSOR_SKIPNEXT)
         && saveCursorPosition(pC)==SQLITE_OK ){
        }else{
          pC->eState = CURSOR_FAULT;
          pC->skipNext = tc;
          pC->mmActive = 0;
          prollyCursorReleaseAll(&pC->pCur);
        }
        if( pC->pMutMap ){
          pC->pMutMap = 0;
          pC->mmActive = 0;
          pC->mmPhysActive = 0;
          pC->deferredTreeSeek = 0;
          pC->mmIdx = -1;
          pC->mmPhysIdx = -1;
        }
      }
    }
    rc = restoreFromCommitted(p);
    if( rc!=SQLITE_OK ){
      btreeFreeCatalogTables(p);
      memset(&p->committedCatalogHash, 0, sizeof(p->committedCatalogHash));
      p->bCatalogDropped = 1;
      p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion - 1;
      resetConnectionSchema(p);
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      btreeDiscardAllSavepoints(p);
      chunkStoreRollback(&pBt->store);
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
      return rc;
    }
    if( bSchemaChangedRollback ){
      resetConnectionSchema(p);
    }
    chunkStoreRollback(&pBt->store);
    if( bAutocommitOomRollback ){
      p->bFilterSchemaPlaceholders = 1;
    }
    /* Autocommit OOM: do not persist a failed DDL as the new baseline. */
    if( !bAutocommitOomRollback && !p->bCatalogDropped
     && !pBt->store.bRefsStale ){
      u8 *catData = 0;
      int nCatData = 0;
      ProllyHash catHash;
      ProllyHash wsHashWouldBe;
      ProllyHash wsHashOnDisk;
      u8 wsBuf[WS_TOTAL_SIZE];
      int bMatchesDisk = 0;
      const char *zBr = p->zBranch ? p->zBranch : "main";

      pBt->inCatalogSerialize = 1;
      rc = sqlite3FaultSim(957) ? SQLITE_NOMEM
                                : serializeCatalog(p, &catData, &nCatData);
      pBt->inCatalogSerialize = 0;
      if( rc!=SQLITE_OK ){
        rollbackAbandonWriteTxn(p, pBt);
        return rc;
      }
      prollyHashCompute(catData, nCatData, &catHash);

      rc = btreeFillWorkingSetBlob(wsBuf, &catHash, &p->headCommit,
                                   &p->vc.stagedCatalog, p->vc.isMerging,
                                   &p->vc.mergeCommitHash,
                                   &p->vc.conflictsCatalogHash,
                                   p->isRebasing, &p->preRebaseWorkingCat,
                                   &p->rebaseOntoCommit,
                                   p->zRebaseOrigBranch,
                                   p->zRebaseReturnBranch,
                                   &p->vc.constraintViolationsHash);
      if( rc!=SQLITE_OK ){
        sqlite3_free(catData);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }
      prollyHashCompute(wsBuf, WS_TOTAL_SIZE, &wsHashWouldBe);

      if( chunkStoreGetBranchWorkingSet(&pBt->store, zBr, &wsHashOnDisk)
          ==SQLITE_OK
       && prollyHashCompare(&wsHashWouldBe, &wsHashOnDisk)==0 ){
        bMatchesDisk = 1;
      }

      if( !bMatchesDisk ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
        if( rc==SQLITE_OK ){
          rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                        &p->headCommit, &p->vc.stagedCatalog,
                                        p->vc.isMerging, &p->vc.mergeCommitHash,
                                        &p->vc.conflictsCatalogHash,
                                        p->isRebasing,
                                        &p->preRebaseWorkingCat,
                                        &p->rebaseOntoCommit,
                                        p->zRebaseOrigBranch,
                                        p->zRebaseReturnBranch,
                                        &p->vc.constraintViolationsHash);
        }
        if( rc==SQLITE_OK ){
          rc = chunkStoreSerializeRefs(&pBt->store);
        }
        if( rc==SQLITE_OK ){
          rc = chunkStoreCommit(&pBt->store);
        }
      }
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        rollbackAbandonWriteTxn(p, pBt);
        return rc;
      }
    }
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  btreeDiscardAllSavepoints(p);

  chunkStoreUnlock(&pBt->store);
  pBt->store.snapshotPinned = 0;

  return rc;
}
int sqlite3BtreeRollback(Btree *p, int tripCode, int writeOnly){
  if( !p ) return SQLITE_OK;
  return p->pOps->xRollback(p, tripCode, writeOnly);
}

int prollyBtreeBeginStmt(Btree *p, int iStatement){
  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  /* First nSavepoint slots are named SAVEPOINTs and must capture tables. */
  while( p->nSavepoint < iStatement ){
    int bStatement = 1;
    if( p->db && p->nSavepoint < p->db->nSavepoint ){
      bStatement = 0;
    }
    {
      int rc = pushSavepoint(p, bStatement);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}
int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginStmt(p, iStatement);
}

int doltliteBtreeCaptureStatement(void *pArg){
  Btree *p = (Btree*)pArg;
  if( !p || p->pOps!=&prollyBtreeOps ) return SQLITE_OK;
  return ensureStatementSavepointsCaptured(p);
}

static int rollbackCommittedState(Btree *p, BtShared *pBt){
  BtCursor *pC;
  int bSchemaChangedRollback = rollbackNeedsSchemaReset(p);
  int rc = restoreFromCommitted(p);
  if( rc!=SQLITE_OK ){
    /* Reload OOM: drop pointers into chunks rollback is about to discard. */
    btreeFreeCatalogTables(p);
    memset(&p->committedCatalogHash, 0, sizeof(p->committedCatalogHash));
    p->bCatalogDropped = 1;
    p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion - 1;
    invalidateCursors(pBt, 0, SQLITE_ABORT);
    resetConnectionSchema(p);
    return rc;
  }
  /* Keep an existing fault code; do not clobber SQLITE_ABORT_ROLLBACK. */
  for(pC=pBt->pCursor; pC; pC=pC->pNext){
    if( pC->eState!=CURSOR_FAULT ){
      pC->eState = CURSOR_FAULT;
      pC->skipNext = SQLITE_ABORT;
    }
    pC->mmActive = 0;
    prollyCursorReleaseAll(&pC->pCur);
  }
  if( bSchemaChangedRollback ) resetConnectionSchema(p);
  return SQLITE_OK;
}

static int persistRolledBackSessionState(Btree *p, BtShared *pBt){
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash catHash;
  const char *zBr = p->zBranch ? p->zBranch : "main";
  int rc;

  PROLLY_ASSERT_GRAPH_LOCKED(pBt);

  rc = serializeCatalog(p, &catData, &nCatData);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
  }
  sqlite3_free(catData);
  if( rc!=SQLITE_OK ) return rc;
  rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                &p->headCommit, &p->vc.stagedCatalog,
                                p->vc.isMerging, &p->vc.mergeCommitHash,
                                &p->vc.conflictsCatalogHash,
                                p->isRebasing,
                                &p->preRebaseWorkingCat,
                                &p->rebaseOntoCommit,
                                p->zRebaseOrigBranch,
                                p->zRebaseReturnBranch,
                                &p->vc.constraintViolationsHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(&pBt->store);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(&pBt->store);
}

static void btreeDiscardAllSavepoints(Btree *p){
  int j;
  for(j=0; j<p->nSavepoint; j++){
    freeSavepointTables(&p->aSavepointTables[j]);
  }
  p->nSavepoint = 0;
}

static int rollbackAllSavepoints(Btree *p, BtShared *pBt){
  int rc;
  btreeDiscardAllSavepoints(p);
  rc = rollbackCommittedState(p, pBt);
  if( rc!=SQLITE_OK ) return rc;
  if( p->db && p->db->isTransactionSavepoint ){
    return persistRolledBackSessionState(p, pBt);
  }
  return SQLITE_OK;
}

static int rollbackNamedSavepoint(Btree *p, BtShared *pBt, int iSavepoint){
  struct SavepointTableState *pState = &p->aSavepointTables[iSavepoint];
  BtCursor *pC;
  int j;
  int rc;
  int bSchemaChangedRollback = rollbackNeedsSchemaReset(p);
  if( p->db && p->db->mallocFailed ){
    for(pC=pBt->pCursor; pC; pC=pC->pNext){
      if( pC->pBtree!=p ) continue;
      pC->eState = CURSOR_FAULT;
      pC->skipNext = SQLITE_NOMEM;
      pC->mmActive = 0;
      pC->mmPhysActive = 0;
      pC->deferredTreeSeek = 0;
      pC->mmIdx = -1;
      pC->mmPhysIdx = -1;
      pC->pMutMap = 0;
      prollyCursorReleaseAll(&pC->pCur);
    }
  }else{
    rc = saveAllCursors(p, pBt, 0, 0);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = rollbackMutMapsToSavepoint(p, iSavepoint + 1, iSavepoint);
  if( rc!=SQLITE_OK ) return rc;
  if( pState->bTablesCaptured ){
    rc = restoreTablesFromSavepoint(p, pState);
    if( rc!=SQLITE_OK ) return rc;
  }
  restoreSavepointSessionState(p, pState);
  p->bSchemaChangedTxn = pState->bSchemaChangedTxn;
  p->bMasterRootChangedTxn = pState->bMasterRootChangedTxn;
  freeSavepointTables(pState);
  for(j=iSavepoint+1; j<p->nSavepoint; j++){
    freeSavepointTables(&p->aSavepointTables[j]);
  }
  p->nSavepoint = iSavepoint;
  for(pC=pBt->pCursor; pC; pC=pC->pNext){
    if( pC->pBtree!=p ) continue;
    pC->pMutMap = 0;
    pC->mmActive = 0;
    pC->mmPhysActive = 0;
    pC->deferredTreeSeek = 0;
    pC->mmIdx = -1;
    pC->mmPhysIdx = -1;
  }
  if( bSchemaChangedRollback ){
    resetConnectionSchema(p);
  }else{
    invalidateSchema(p);
  }
  return SQLITE_OK;
}

static int releaseSavepointsFrom(Btree *p, int iSavepoint){
  int j;
  releaseMutMapsToSavepoint(p, iSavepoint + 1);
  if( iSavepoint > 0 ){
    for(j=iSavepoint; j<p->nSavepoint; j++){
      if( p->aSavepointTables[j].nPendingSnapshot>0
       && !p->aSavepointTables[iSavepoint-1].bTablesCaptured ){
        int rc = ensureSavepointTablesCaptured(
            p, &p->aSavepointTables[iSavepoint-1]);
        if( rc!=SQLITE_OK ) return rc;
      }
    }
    for(j=iSavepoint; j<p->nSavepoint; j++){
      int rc = inheritPendingSnapshots(&p->aSavepointTables[iSavepoint-1],
                                       &p->aSavepointTables[j]);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  for(j=iSavepoint; j<p->nSavepoint; j++){
    freeSavepointTables(&p->aSavepointTables[j]);
  }
  p->nSavepoint = iSavepoint;
  return SQLITE_OK;
}

int prollyBtreeSavepoint(Btree *p, int op, int iSavepoint){
  BtShared *pBt;

  pBt = p->pBt;
  if( pBt==0 || p->inTrans!=TRANS_WRITE ){
    return SQLITE_OK;
  }

  if( op==SAVEPOINT_BEGIN ){
    while( p->nSavepoint < iSavepoint ){
      int rc = pushSavepoint(p, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
    return SQLITE_OK;
  }

  if( op==SAVEPOINT_ROLLBACK ){

    if( iSavepoint>=0 && iSavepoint<p->nSavepoint
     && p->aSavepointTables ){
      return rollbackNamedSavepoint(p, pBt, iSavepoint);
    } else if( iSavepoint>=0 && iSavepoint>=p->nSavepoint ){
      return SQLITE_OK;
    } else if( iSavepoint<0 ){
      return rollbackAllSavepoints(p, pBt);
    }
  } else {

    if( iSavepoint>=0 && iSavepoint<p->nSavepoint ){
      return releaseSavepointsFrom(p, iSavepoint);
    }
  }

  return SQLITE_OK;
}
int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSavepoint(p, op, iSavepoint);
}


#endif /* DOLTLITE_PROLLY */
