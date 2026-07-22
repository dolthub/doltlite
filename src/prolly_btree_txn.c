#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Transactions, savepoints, commit, rollback, and mutation flushing. */

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

static int findSavepointTableIndexInArray(
  SavepointTableEntry*, int, Pgno
);

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
  memset(&pState->stagedCatalog, 0, sizeof(pState->stagedCatalog));
  pState->isMerging = 0;
  memset(&pState->mergeCommitHash, 0, sizeof(pState->mergeCommitHash));
  memset(&pState->conflictsCatalogHash, 0, sizeof(pState->conflictsCatalogHash));
  memset(&pState->constraintViolationsHash, 0, sizeof(pState->constraintViolationsHash));
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

static void captureSavepointSessionState(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  pState->iNextTable = pBtree->cat.iNextTable;
  pState->iLargestRootPage = pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE];
  memcpy(pState->aMeta, pBtree->aMeta, sizeof(pState->aMeta));
  pState->stagedCatalog = pBtree->stagedCatalog;
  pState->isMerging = pBtree->isMerging;
  pState->mergeCommitHash = pBtree->mergeCommitHash;
  pState->conflictsCatalogHash = pBtree->conflictsCatalogHash;
  pState->constraintViolationsHash = pBtree->constraintViolationsHash;
  pState->isRebasing = pBtree->isRebasing;
  pState->preRebaseWorkingCat = pBtree->preRebaseWorkingCat;
  pState->rebaseOntoCommit = pBtree->rebaseOntoCommit;
  sqlite3_free(pState->zRebaseOrigBranch);
  pState->zRebaseOrigBranch = pBtree->zRebaseOrigBranch
      ? sqlite3_mprintf("%s", pBtree->zRebaseOrigBranch) : 0;
  sqlite3_free(pState->zRebaseReturnBranch);
  pState->zRebaseReturnBranch = pBtree->zRebaseReturnBranch
      ? sqlite3_mprintf("%s", pBtree->zRebaseReturnBranch) : 0;
}

static void restoreSavepointSessionState(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  pBtree->stagedCatalog = pState->stagedCatalog;
  pBtree->isMerging = pState->isMerging;
  pBtree->mergeCommitHash = pState->mergeCommitHash;
  pBtree->conflictsCatalogHash = pState->conflictsCatalogHash;
  pBtree->constraintViolationsHash = pState->constraintViolationsHash;
  memcpy(pBtree->aMeta, pState->aMeta, sizeof(pBtree->aMeta));
  pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE] = pState->iLargestRootPage;
  pBtree->isRebasing = pState->isRebasing;
  pBtree->preRebaseWorkingCat = pState->preRebaseWorkingCat;
  pBtree->rebaseOntoCommit = pState->rebaseOntoCommit;
  sqlite3_free(pBtree->zRebaseOrigBranch);
  pBtree->zRebaseOrigBranch = pState->zRebaseOrigBranch
      ? sqlite3_mprintf("%s", pState->zRebaseOrigBranch) : 0;
  sqlite3_free(pBtree->zRebaseReturnBranch);
  pBtree->zRebaseReturnBranch = pState->zRebaseReturnBranch
      ? sqlite3_mprintf("%s", pState->zRebaseReturnBranch) : 0;
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
  /* Snapshot the live catalog entries verbatim. The commit-form catalog
  ** serializer canonicalizes (sorts rows and renumbers roots), so a
  ** round-trip through it reassigns iTable numbers and rebuilds the master
  ** root, scrambling tables created inside the transaction. */
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
    if( findSavepointTableIndexInArray(pParent->aTables, pParent->nTables,
                                       pSnap->iTable) < 0 ){
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

/* If a dirty map is flushed while a savepoint is open, move that map into the
** savepoint snapshot and continue with a fresh map. Rollback can then restore
** the exact pre-flush edits even though the table root has already changed. */
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
    j = findSavepointTableIndexInArray(pState->aTables, pState->nTables, iTable);
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

static int findTableIndexInArray(
  struct TableEntry *aTables,
  int nTables,
  Pgno iTable
){
  int lo = 0;
  int hi = nTables;
  while( lo < hi ){
    int mid = lo + ((hi - lo) / 2);
    Pgno midTable = aTables[mid].iTable;
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

static int findSavepointTableIndexInArray(
  SavepointTableEntry *aTables,
  int nTables,
  Pgno iTable
){
  int lo = 0;
  int hi = nTables;
  while( lo < hi ){
    int mid = lo + ((hi - lo) / 2);
    Pgno midTable = aTables[mid].iTable;
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
    iSaved = findSavepointTableIndexInArray(
        pState->aTables, pState->nTables, aCurrent[k].iTable);
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
      int idx = findTableIndexInArray(
          pBtree->cat.a, pBtree->cat.n, pState->aTables[k].iTable);
      if( idx < 0 ){
        return SQLITE_CORRUPT;
      }
      pBtree->cat.a[idx].pendingFlushSeekEdits =
          pState->aTables[k].pendingFlushSeekEdits;
      if( pState->aTables[k].pPending==0 ){
        /* Restore pre-drop edits captured by this savepoint. */
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
  captureSavepointSessionState(pBtree, pState);
  if( !bStatement ){
    int rc = captureSavepointTables(pBtree, pState);
    if( rc!=SQLITE_OK ) return rc;
  }

  pBtree->nSavepoint++;
  pushSavepointOnMutMaps(pBtree, pBtree->nSavepoint);
  return SQLITE_OK;
}


int prollyBtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  BtShared *pBt = p->pBt;
  int rc;

  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
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
    if( pBt->btsFlags & BTS_READ_ONLY ){
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

    if( p->db ){
      while( p->nSavepoint < p->db->nSavepoint ){
        int rc2 = pushSavepoint(p, 0);
        if( rc2!=SQLITE_OK ){
          while( p->nSavepoint > nSavepointStart ){
            p->nSavepoint--;
            freeSavepointTables(&p->aSavepointTables[p->nSavepoint]);
          }
          chunkStoreUnlock(&pBt->store);
          return rc2;
        }
      }
    }
    p->inTrans = TRANS_WRITE;
    p->inTransaction = TRANS_WRITE;
    assert( pBt->store.isMemory || pBt->store.lockDepth > 0 );
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
int sqlite3BtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginTrans(p, wrFlag, pSchemaVersion);
}

int prollyBtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  (void)p; (void)zSuperJrnl;
  return SQLITE_OK;
}
int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommitPhaseOne(p, zSuperJrnl);
}

int prollyBtreeCommitPhaseTwo(Btree *p, int bCleanup){
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash catHash;
  Btree reloadBtree;
  int bReloadSchema = 0;
  int bHaveReloadCatalog = 0;
  u32 nativeSchemaCookie = 0;
  (void)bCleanup;
  memset(&reloadBtree, 0, sizeof(reloadBtree));

  /* Catalog serialization runs a SELECT whose finalize re-enters
  ** commit/rollback; that nested call is read-only, so no-op it instead of
  ** recursing into a stack overflow. */
  if( pBt->inCatalogSerialize ) return SQLITE_OK;

  if( p->inTrans==TRANS_WRITE ){
    assert( pBt->store.isMemory || pBt->store.pGraphLockFile!=0 );
    assert( pBt->store.isMemory || pBt->store.lockDepth > 0 );
    rc = flushAllPending(p, pBt, 0);
    if( rc!=SQLITE_OK ){
      chunkStoreRollback(&pBt->store);
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
      return rc;
    }

    {
      pBt->inCatalogSerialize = 1;
      rc = serializeCatalogForCommit(p, &catData, &nCatData);
      pBt->inCatalogSerialize = 0;
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(catData);
        chunkStoreRollback(&pBt->store);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }

      {
        const char *zBr = p->zBranch ? p->zBranch : "main";
        rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                      &p->headCommit,
                                      &p->stagedCatalog, p->isMerging,
                                      &p->mergeCommitHash,
                                      &p->conflictsCatalogHash,
                                      p->isRebasing,
                                      &p->preRebaseWorkingCat,
                                      &p->rebaseOntoCommit,
                                      p->zRebaseOrigBranch,
                                      p->zRebaseReturnBranch,
                                      &p->constraintViolationsHash);
        if( rc!=SQLITE_OK ){
          sqlite3_free(catData);
          chunkStoreRollback(&pBt->store);
          chunkStoreUnlock(&pBt->store);
          pBt->store.snapshotPinned = 0;
          return rc;
        }
        rc = chunkStoreSerializeRefs(&pBt->store);
        if( rc!=SQLITE_OK ){
          sqlite3_free(catData);
          chunkStoreRollback(&pBt->store);
          chunkStoreUnlock(&pBt->store);
          pBt->store.snapshotPinned = 0;
          return rc;
        }
      }
    }

    /* A schema-changing commit adopts the canonical catalog wholesale --
    ** including the canonical master root, so the live view and the
    ** persisted form never diverge (rowid bindings, rootpages, and row
    ** order all match what any reload would see). */
    bReloadSchema = p->bSchemaChangedTxn;
    if( bReloadSchema ){
      BtCursor *pC;
      nativeSchemaCookie = p->aMeta[BTREE_SCHEMA_VERSION];
      memcpy(reloadBtree.aMeta, p->aMeta, sizeof(reloadBtree.aMeta));
      rc = deserializeCatalog(&reloadBtree, catData, nCatData);
      if( rc!=SQLITE_OK ){
        catFree(&reloadBtree.cat);
        sqlite3_free(catData);
        chunkStoreRollback(&pBt->store);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }
      bHaveReloadCatalog = 1;
      /* Save this handle's live cursors before refreshing the schema root. */
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
    }

    rc = chunkStoreCommit(&pBt->store);
    if( rc==SQLITE_OK ){
      p->committedCatalogHash = catHash;
      p->committedStagedCatalog = p->stagedCatalog;
      p->committedIsMerging = p->isMerging;
      p->committedMergeCommitHash = p->mergeCommitHash;
      p->committedConflictsCatalogHash = p->conflictsCatalogHash;
      p->committedConstraintViolationsHash = p->constraintViolationsHash;
      memcpy(p->committedAMeta, p->aMeta, sizeof(p->committedAMeta));
      btreeMarkWorkingStateChanged(p, 1);
      if( bReloadSchema ){
        BtCursor *pC;
        if( bHaveReloadCatalog ){
          btreeFreeCatalogTables(p);
          p->cat = reloadBtree.cat;
          memcpy(p->aMeta, reloadBtree.aMeta, sizeof(p->aMeta));
          /* sqlite already bumped its cookie for this DDL; keep the stock
          ** value instead of the reload's hash-derived one so in-session
          ** cookie semantics stay stock (writes stick, DDL increments). */
          p->aMeta[BTREE_SCHEMA_VERSION] = nativeSchemaCookie;
          memset(&reloadBtree.cat, 0, sizeof(reloadBtree.cat));
          bHaveReloadCatalog = 0;
        }
        /* A saved cursor whose table vanished in the reload would re-seek a
        ** stale root; fault it. */
        for(pC = pBt->pCursor; pC; pC = pC->pNext){
          if( pC->pBtree==p && pC->eState==CURSOR_REQUIRESEEK
           && findTable(p, pC->pgnoRoot)==0 ){
            pC->eState = CURSOR_FAULT;
            pC->skipNext = SQLITE_ABORT;
          }
        }
        invalidateSchema(p);
        if( p->db ){
          /* Hard expiry: canonical renumbering can move rootpages compiled
          ** into running statements (tkt-c694113d5). */
          sqlite3ExpirePreparedStatements(p->db, 1);
          sqlite3ResetAllSchemasOfConnection(p->db);
        }
      }
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      btreeDiscardAllSavepoints(p);
      p->bSchemaChangedTxn = 0;
      p->bMasterRootChangedTxn = 0;

      btreeTakeCatalogCache(p, &catData, nCatData, &catHash);
      assert( p->nCatalogCache==nCatData );
      assert( prollyHashCompare(&p->catalogCacheHash, &catHash)==0 );
      sqlite3_free(catData);
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
    }else{
      int rc2;
      if( bHaveReloadCatalog ){
        catFree(&reloadBtree.cat);
        bHaveReloadCatalog = 0;
      }
      sqlite3_free(catData);
      rc2 = restoreFromCommitted(p);
      if( rc2!=SQLITE_OK ){
        /* Same shape as the rollback path: the restore OOM'd, but the
        ** transaction must end with the partial state discarded, or the
        ** next statement's autocommit publishes it. */
        btreeFreeCatalogTables(p);
        memset(&p->committedCatalogHash, 0, sizeof(p->committedCatalogHash));
        p->bCatalogDropped = 1;
        p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion - 1;
        resetConnectionSchema(p);
        chunkStoreRollback(&pBt->store);
        p->inTrans = TRANS_NONE;
        p->inTransaction = TRANS_NONE;
        btreeDiscardAllSavepoints(p);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        /* Preserve the original commit error; restore failure is secondary. */
        return rc;
      }
      {
        BtCursor *pC;
        for(pC = pBt->pCursor; pC; pC = pC->pNext){
          if( pC->pBtree==p && pC->pMutMap ) prollyMutMapClear(pC->pMutMap);
        }
      }
      invalidateCursors(pBt, 0, rc);
      resetConnectionSchema(p);
      chunkStoreRollback(&pBt->store);
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      btreeDiscardAllSavepoints(p);
      p->bSchemaChangedTxn = 0;
      p->bMasterRootChangedTxn = 0;
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
    }
    return rc;
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  p->bSchemaChangedTxn = 0;
  p->bMasterRootChangedTxn = 0;
  btreeDiscardAllSavepoints(p);

  chunkStoreUnlock(&pBt->store);
  pBt->store.snapshotPinned = 0;

  return rc;
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

int restoreFromCommitted(Btree *p){
  if( prollyHashIsEmpty(&p->committedCatalogHash) ){
    if( p->bCatalogDropped ){
      /* A prior OOM rollback dropped the catalog. Installing the default
      ** empty catalog here would let the persist-on-rollback path write an
      ** empty working set over real tables. Reload the committed state
      ** from the store instead; failure keeps the dropped state. */
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
  p->stagedCatalog = p->committedStagedCatalog;
  p->isMerging = p->committedIsMerging;
  p->mergeCommitHash = p->committedMergeCommitHash;
  p->conflictsCatalogHash = p->committedConflictsCatalogHash;
  p->constraintViolationsHash = p->committedConstraintViolationsHash;
  memcpy(p->aMeta, p->committedAMeta, sizeof(p->aMeta));
  return SQLITE_OK;
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
    assert( pBt->store.isMemory || pBt->store.pGraphLockFile!=0 );
    assert( pBt->store.isMemory || pBt->store.lockDepth > 0 );
    /* Read cursors survive write-only rollback by reseeking the restored tree.
    ** Write cursors, and all cursors after schema changes, are faulted.
    ** Either way detach the cursor's pending-edit map alias before
    ** restoreFromCommitted() (via catFree) frees it. Detach AFTER
    ** saveCursorPosition(), which copies out the key it needs first. We only
    ** null the alias — catFree owns the map, and a prior savepoint rollback may
    ** already have freed the one this cursor points at, so clearing it would be
    ** a use-after-free. ensureMutMap re-aliases on reuse. */
    {
      BtCursor *pC;
      int tc = tripCode ? tripCode : SQLITE_ABORT;
      for(pC = pBt->pCursor; pC; pC = pC->pNext){
        if( pC->pBtree!=p ) continue;
        if( writeOnly
         && (pC->curFlags & BTCF_WriteFlag)==0
         && (pC->eState==CURSOR_VALID || pC->eState==CURSOR_SKIPNEXT)
         && saveCursorPosition(pC)==SQLITE_OK ){
          /* read cursor saved -> CURSOR_REQUIRESEEK; survives the rollback */
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
      /* Drop catalog pointers into staging before rollback discards chunks. */
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
    /* Autocommit OOM rollback restores in-memory state only. Persisting the
    ** restored catalog here can make a failed DDL attempt the new baseline. */
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
      rc = serializeCatalog(p, &catData, &nCatData);
      pBt->inCatalogSerialize = 0;
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }
      prollyHashCompute(catData, nCatData, &catHash);

      btreeFillWorkingSetBlob(wsBuf, &catHash, &p->headCommit,
                              &p->stagedCatalog, p->isMerging,
                              &p->mergeCommitHash, &p->conflictsCatalogHash,
                              p->isRebasing, &p->preRebaseWorkingCat,
                              &p->rebaseOntoCommit,
                              p->zRebaseOrigBranch, p->zRebaseReturnBranch,
                              &p->constraintViolationsHash);
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
                                        &p->headCommit, &p->stagedCatalog,
                                        p->isMerging, &p->mergeCommitHash,
                                        &p->conflictsCatalogHash,
                                        p->isRebasing,
                                        &p->preRebaseWorkingCat,
                                        &p->rebaseOntoCommit,
                                        p->zRebaseOrigBranch,
                                        p->zRebaseReturnBranch,
                                        &p->constraintViolationsHash);
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
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
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

  while( p->nSavepoint < iStatement ){
    int rc = pushSavepoint(p, 1);
    if( rc!=SQLITE_OK ) return rc;
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
    /* The reload OOM'd partway; the catalog may still point at staged
    ** chunks the rollback is about to discard (a DDL attempt repointed
    ** the sqlite_master root). Drop it without allocating and let the
    ** next BeginTrans reload the committed working state. */
    btreeFreeCatalogTables(p);
    memset(&p->committedCatalogHash, 0, sizeof(p->committedCatalogHash));
    p->bCatalogDropped = 1;
    p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion - 1;
    invalidateCursors(pBt, 0, SQLITE_ABORT);
    resetConnectionSchema(p);
    return rc;
  }
  /* Like invalidateCursors, but keep the code of an existing fault:
  ** OP_Savepoint's cursor trip already stamped SQLITE_ABORT_ROLLBACK and
  ** clobbering it to SQLITE_ABORT misreported the abort (savepoint7-2.2). */
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

  assert( pBt->store.isMemory || pBt->store.pGraphLockFile!=0 );
  assert( pBt->store.isMemory || pBt->store.lockDepth > 0 );

  rc = serializeCatalog(p, &catData, &nCatData);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
  }
  sqlite3_free(catData);
  if( rc!=SQLITE_OK ) return rc;
  rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                &p->headCommit, &p->stagedCatalog,
                                p->isMerging, &p->mergeCommitHash,
                                &p->conflictsCatalogHash,
                                p->isRebasing,
                                &p->preRebaseWorkingCat,
                                &p->rebaseOntoCommit,
                                p->zRebaseOrigBranch,
                                p->zRebaseReturnBranch,
                                &p->constraintViolationsHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(&pBt->store);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(&pBt->store);
}

void btreeDiscardAllSavepoints(Btree *p){
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
  /* Save cursors so parent statements survive nested-statement rollback. */
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
  /* The rollback may have freed or replaced pending-edit maps; detach every
  ** cursor's now-stale map alias. Saved cursors re-seek; a cursor that could
  ** not be saved (already invalid/faulted) is left as-is. */
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
      /* No pushed savepoint means this btree has no writes at that level. */
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
