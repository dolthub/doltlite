#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Branch working-set persistence and connection-visible state changes. */

int btreeLoadWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pWorkingCat,
  ProllyHash *pWorkingCommit,
  ProllyHash *pStaged,
  u8 *pIsMerging,
  ProllyHash *pMergeCommit,
  ProllyHash *pConflicts,
  u8 *pIsRebasing,
  ProllyHash *pPreRebaseCat,
  ProllyHash *pRebaseOnto,
  char **pzRebaseOrigBranch,
  char **pzRebaseReturnBranch,
  ProllyHash *pConstraintViolations
){
  ProllyHash wsHash;
  u8 *data = 0;
  int nData = 0;
  int rc;
  u8 version;

  assert( cs!=0 && zBranch!=0 );
  if( pWorkingCat ) memset(pWorkingCat, 0, sizeof(ProllyHash));
  if( pWorkingCommit ) memset(pWorkingCommit, 0, sizeof(ProllyHash));
  if( pStaged ) memset(pStaged, 0, sizeof(ProllyHash));
  if( pIsMerging ) *pIsMerging = 0;
  if( pMergeCommit ) memset(pMergeCommit, 0, sizeof(ProllyHash));
  if( pConflicts ) memset(pConflicts, 0, sizeof(ProllyHash));
  if( pIsRebasing ) *pIsRebasing = 0;
  if( pPreRebaseCat ) memset(pPreRebaseCat, 0, sizeof(ProllyHash));
  if( pRebaseOnto ) memset(pRebaseOnto, 0, sizeof(ProllyHash));
  if( pzRebaseOrigBranch ) *pzRebaseOrigBranch = 0;
  if( pzRebaseReturnBranch ) *pzRebaseReturnBranch = 0;
  if( pConstraintViolations ) memset(pConstraintViolations, 0, sizeof(ProllyHash));

  rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&wsHash) ) return SQLITE_NOTFOUND;

  rc = chunkStoreGet(cs, &wsHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreValidateWorkingSetBlob(data, nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(data);
    return rc;
  }
  version = data[0];

  if( pWorkingCat ) memcpy(pWorkingCat->data, data + WS_WORKING_CAT_OFF, PROLLY_HASH_SIZE);
  if( pWorkingCommit ) memcpy(pWorkingCommit->data, data + WS_WORKING_COMMIT_OFF, PROLLY_HASH_SIZE);
  if( pStaged ) memcpy(pStaged->data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  if( pIsMerging ) *pIsMerging = data[WS_MERGING_OFF];
  if( pMergeCommit ) memcpy(pMergeCommit->data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
  if( pConflicts ) memcpy(pConflicts->data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);

  if( (version == WS_FORMAT_VERSION_V3
    || version == WS_FORMAT_VERSION_V4
    || version == WS_FORMAT_VERSION_V5)
   && nData >= WS_TOTAL_SIZE_V3 ){
    if( pIsRebasing ) *pIsRebasing = data[WS_REBASING_OFF];
    if( pPreRebaseCat ) memcpy(pPreRebaseCat->data,
                                data + WS_PRE_REBASE_CAT_OFF, PROLLY_HASH_SIZE);
    if( pRebaseOnto ) memcpy(pRebaseOnto->data,
                              data + WS_REBASE_ONTO_OFF, PROLLY_HASH_SIZE);
    if( pzRebaseOrigBranch ){
      const char *src = (const char*)(data + WS_REBASE_BRANCH_OFF);
      int n = 0;
      while( n < WS_REBASE_BRANCH_LEN && src[n] ) n++;
      if( n > 0 ){
        char *z = sqlite3_malloc(n + 1);
        if( !z ){ sqlite3_free(data); return SQLITE_NOMEM; }
        memcpy(z, src, n);
        z[n] = 0;
        *pzRebaseOrigBranch = z;
      }
    }
  }
  if( version == WS_FORMAT_VERSION_V4 && nData >= WS_TOTAL_SIZE_V4 ){
    if( pConstraintViolations ){
      memcpy(pConstraintViolations->data,
             data + WS_CONSTRAINT_VIOLATIONS_OFF_V4, PROLLY_HASH_SIZE);
    }
  }else if( version == WS_FORMAT_VERSION_V5 && nData >= WS_TOTAL_SIZE ){
    if( pzRebaseReturnBranch ){
      const char *src = (const char*)(data + WS_REBASE_RETURN_BRANCH_OFF);
      int n = 0;
      while( n < WS_REBASE_BRANCH_LEN && src[n] ) n++;
      if( n > 0 ){
        char *z = sqlite3_malloc(n + 1);
        if( !z ){ sqlite3_free(data); return SQLITE_NOMEM; }
        memcpy(z, src, n);
        z[n] = 0;
        *pzRebaseReturnBranch = z;
      }
    }
    if( pConstraintViolations ){
      memcpy(pConstraintViolations->data,
             data + WS_CONSTRAINT_VIOLATIONS_OFF, PROLLY_HASH_SIZE);
    }
  }
  sqlite3_free(data);
  return SQLITE_OK;
}

int btreeLoadBranchHeadCatalog(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCatHash,
  ProllyHash *pHeadCommit
){
  ProllyHash headCommit;
  u8 *pData = 0;
  int nData = 0;
  DoltliteCommit c;
  int rc;

  if( pCatHash ) memset(pCatHash, 0, sizeof(*pCatHash));
  if( pHeadCommit ) memset(pHeadCommit, 0, sizeof(*pHeadCommit));
  if( !cs || !zBranch ) return SQLITE_ERROR;

  rc = chunkStoreFindBranch(cs, zBranch, &headCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGet(cs, &headCommit, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  memset(&c, 0, sizeof(c));
  rc = doltliteCommitDeserialize(pData, nData, &c);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&c);
    return rc;
  }

  if( pCatHash ) memcpy(pCatHash, &c.catalogHash, sizeof(*pCatHash));
  if( pHeadCommit ) memcpy(pHeadCommit, &headCommit, sizeof(*pHeadCommit));
  doltliteCommitClear(&c);
  return SQLITE_OK;
}

void btreeClearBranchState(BtreeBranchState *pState){
  if( !pState ) return;
  sqlite3_free(pState->zRebaseOrigBranch);
  sqlite3_free(pState->zRebaseReturnBranch);
  memset(pState, 0, sizeof(*pState));
}

int btreeLoadBranchState(
  ChunkStore *cs,
  const char *zBranch,
  int bSeedRepair,
  BtreeBranchState *pState
){
  ProllyHash committedCatalog;
  ProllyHash headCommit;
  ProllyHash workingCommit;
  int useWorkingState;
  int rc;

  assert( cs!=0 && zBranch!=0 && pState!=0 );
  memset(pState, 0, sizeof(*pState));
  memset(&committedCatalog, 0, sizeof(committedCatalog));
  memset(&headCommit, 0, sizeof(headCommit));
  memset(&workingCommit, 0, sizeof(workingCommit));

  rc = chunkStoreFindBranch(cs, zBranch, &headCommit);
  if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  rc = btreeLoadWorkingSetBlob(
      cs, zBranch, &pState->catalog, &workingCommit,
      &pState->stagedCatalog, &pState->isMerging,
      &pState->mergeCommit, &pState->conflictsCatalog,
      &pState->isRebasing, &pState->preRebaseCatalog,
      &pState->rebaseOnto, &pState->zRebaseOrigBranch,
      &pState->zRebaseReturnBranch, &pState->constraintViolations);
  if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ){
    btreeClearBranchState(pState);
    return rc;
  }
  useWorkingState = rc==SQLITE_OK
    && (prollyHashCompare(&workingCommit, &headCommit)==0
        || (bSeedRepair && prollyHashIsEmpty(&headCommit)));
  if( !useWorkingState ){
    btreeClearBranchState(pState);
  }
  if( !useWorkingState || prollyHashIsEmpty(&pState->catalog) ){
    if( !prollyHashIsEmpty(&headCommit) ){
      rc = btreeLoadBranchHeadCatalog(cs, zBranch, &committedCatalog, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
    pState->catalog = committedCatalog;
  }
  pState->headCommit = headCommit;
  return SQLITE_OK;
}

int btreeFillWorkingSetBlob(
  u8 *buf,
  const ProllyHash *pWorkingCat,
  const ProllyHash *pWorkingCommit,
  const ProllyHash *pStaged,
  u8 isMerging,
  const ProllyHash *pMergeCommit,
  const ProllyHash *pConflicts,
  u8 isRebasing,
  const ProllyHash *pPreRebaseCat,
  const ProllyHash *pRebaseOnto,
  const char *zRebaseOrigBranch,
  const char *zRebaseReturnBranch,
  const ProllyHash *pConstraintViolations
){
  static const ProllyHash emptyHash = {{0}};
  int nOrig = zRebaseOrigBranch ? (int)strlen(zRebaseOrigBranch) : 0;
  int nReturn = zRebaseReturnBranch ? (int)strlen(zRebaseReturnBranch) : 0;

  if( nOrig >= WS_REBASE_BRANCH_LEN
   || nReturn >= WS_REBASE_BRANCH_LEN ){
    return SQLITE_TOOBIG;
  }

  memset(buf, 0, WS_TOTAL_SIZE);
  buf[0] = WS_FORMAT_VERSION;
  memcpy(buf + WS_WORKING_CAT_OFF,
         (pWorkingCat ? pWorkingCat : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_WORKING_COMMIT_OFF,
         (pWorkingCommit ? pWorkingCommit : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_STAGED_OFF,
         (pStaged ? pStaged : &emptyHash)->data, PROLLY_HASH_SIZE);
  buf[WS_MERGING_OFF] = isMerging;
  memcpy(buf + WS_MERGE_COMMIT_OFF,
         (pMergeCommit ? pMergeCommit : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_CONFLICTS_OFF,
         (pConflicts ? pConflicts : &emptyHash)->data, PROLLY_HASH_SIZE);
  buf[WS_REBASING_OFF] = isRebasing;
  memcpy(buf + WS_PRE_REBASE_CAT_OFF,
         (pPreRebaseCat ? pPreRebaseCat : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_REBASE_ONTO_OFF,
         (pRebaseOnto ? pRebaseOnto : &emptyHash)->data, PROLLY_HASH_SIZE);
  if( zRebaseOrigBranch ){
    memcpy(buf + WS_REBASE_BRANCH_OFF, zRebaseOrigBranch, nOrig);
  }
  if( zRebaseReturnBranch ){
    memcpy(buf + WS_REBASE_RETURN_BRANCH_OFF, zRebaseReturnBranch, nReturn);
  }
  memcpy(buf + WS_CONSTRAINT_VIOLATIONS_OFF,
         (pConstraintViolations ? pConstraintViolations : &emptyHash)->data,
         PROLLY_HASH_SIZE);
  return SQLITE_OK;
}

int btreeStoreWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pWorkingCat,
  const ProllyHash *pWorkingCommit,
  const ProllyHash *pStaged,
  u8 isMerging,
  const ProllyHash *pMergeCommit,
  const ProllyHash *pConflicts,
  u8 isRebasing,
  const ProllyHash *pPreRebaseCat,
  const ProllyHash *pRebaseOnto,
  const char *zRebaseOrigBranch,
  const char *zRebaseReturnBranch,
  const ProllyHash *pConstraintViolations
){
  u8 buf[WS_TOTAL_SIZE];
  ProllyHash wsHash;
  int rc;

  rc = btreeFillWorkingSetBlob(buf, pWorkingCat, pWorkingCommit, pStaged,
                               isMerging, pMergeCommit, pConflicts,
                               isRebasing, pPreRebaseCat, pRebaseOnto,
                               zRebaseOrigBranch, zRebaseReturnBranch,
                               pConstraintViolations);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStorePut(cs, buf, WS_TOTAL_SIZE, &wsHash);
  if( rc != SQLITE_OK ) return rc;
  rc = chunkStoreSetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc == SQLITE_NOTFOUND ){
    rc = chunkStoreAddBranch(cs, zBranch, pWorkingCommit);
    if( rc == SQLITE_OK ){
      rc = chunkStoreSetBranchWorkingSet(cs, zBranch, &wsHash);
    }
  }
  return rc;
}

int btreeReadWorkingCatalog(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCatHash,
  ProllyHash *pCommitHash
){
  return btreeLoadWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
}

int btreeWriteWorkingState(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pCatHash,
  const ProllyHash *pCommitHash
){
  ProllyHash stagedCatalog;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;
  ProllyHash preRebaseCat;
  ProllyHash rebaseOnto;
  ProllyHash constraintViolationsHash;
  char *zRebaseOrigBranch = 0;
  char *zRebaseReturnBranch = 0;
  u8 isMerging = 0;
  u8 isRebasing = 0;
  int rc;

  /* pCommitHash is passed straight through to btreeFillWorkingSetBlob, which
  ** writes the empty hash for a null one. doltliteHardReset relies on that to
  ** clear the working commit, so requiring it here only broke assert builds. */
  assert( cs!=0 && zBranch!=0 && pCatHash!=0 );
  rc = btreeLoadWorkingSetBlob(cs, zBranch, 0, 0, &stagedCatalog, &isMerging,
                               &mergeCommitHash, &conflictsCatalogHash,
                               &isRebasing, &preRebaseCat, &rebaseOnto,
                               &zRebaseOrigBranch, &zRebaseReturnBranch,
                               &constraintViolationsHash);
  if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ){
    sqlite3_free(zRebaseOrigBranch);
    sqlite3_free(zRebaseReturnBranch);
    return rc;
  }
  if( rc==SQLITE_NOTFOUND ){
    memset(&stagedCatalog, 0, sizeof(ProllyHash));
    memset(&mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&conflictsCatalogHash, 0, sizeof(ProllyHash));
    memset(&preRebaseCat, 0, sizeof(ProllyHash));
    memset(&rebaseOnto, 0, sizeof(ProllyHash));
    memset(&constraintViolationsHash, 0, sizeof(ProllyHash));
    isMerging = 0;
    isRebasing = 0;
  }

  rc = btreeStoreWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                &stagedCatalog, isMerging,
                                &mergeCommitHash, &conflictsCatalogHash,
                                isRebasing, &preRebaseCat, &rebaseOnto,
                                zRebaseOrigBranch, zRebaseReturnBranch,
                                &constraintViolationsHash);
  sqlite3_free(zRebaseOrigBranch);
  sqlite3_free(zRebaseReturnBranch);
  return rc;
}

int btreeReloadBranchWorkingStateInto(
  Btree *p,
  int bLoadCatalog,
  ProllyHash *pLoadedCatHash
){
  BtShared *pBt;
  BtreeBranchState state;
  const char *zBr;
  int hadUserCatalog;
  int rc;

  assert( p!=0 && p->pBt!=0 );
  pBt = p->pBt;
  zBr = p->zBranch ? p->zBranch : "main";
  hadUserCatalog = p->cat.n > 1;

  rc = chunkStoreEnsureRefsFresh(&pBt->store);
  if( rc!=SQLITE_OK ) return rc;
  rc = btreeLoadBranchState(&pBt->store, zBr, 0, &state);
  if( rc!=SQLITE_OK ) return rc;

  if( bLoadCatalog
   && !prollyHashIsEmpty(&state.catalog)
   && (prollyHashIsEmpty(&p->committedCatalogHash)
       || prollyHashCompare(&state.catalog, &p->committedCatalogHash)!=0) ){
    u8 *catData = 0;
    int nCatData = 0;
    rc = chunkStoreGet(&pBt->store, &state.catalog, &catData, &nCatData);
    if( rc==SQLITE_OK && catData ){
      rc = deserializeCatalog(p, catData, nCatData);
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        btreeClearBranchState(&state);
        return rc;
      }
    }else{
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        btreeClearBranchState(&state);
        return rc;
      }
    }
  }
  if( prollyHashIsEmpty(&p->headCommit) || !hadUserCatalog ){
    p->headCommit = state.headCommit;
  }
  if( pLoadedCatHash ){
    *pLoadedCatHash = state.catalog;
  }

  p->vc.stagedCatalog = state.stagedCatalog;
  p->vc.isMerging = state.isMerging;
  p->vc.mergeCommitHash = state.mergeCommit;
  p->vc.conflictsCatalogHash = state.conflictsCatalog;
  p->isRebasing = state.isRebasing;
  p->preRebaseWorkingCat = state.preRebaseCatalog;
  p->rebaseOntoCommit = state.rebaseOnto;
  sqlite3_free(p->zRebaseOrigBranch);
  p->zRebaseOrigBranch = state.zRebaseOrigBranch;
  sqlite3_free(p->zRebaseReturnBranch);
  p->zRebaseReturnBranch = state.zRebaseReturnBranch;
  p->vc.constraintViolationsHash = state.constraintViolations;
  return SQLITE_OK;
}

void btreeStoreCommittedFromCurrent(Btree *p, const ProllyHash *pCatHash){
  assert( p!=0 );
  if( pCatHash ){
    p->committedCatalogHash = *pCatHash;
  }
  p->committedVc = p->vc;
  memcpy(p->committedAMeta, p->aMeta, sizeof(p->committedAMeta));
}

void btreeBumpExternalDataVersion(Btree *p){
  assert( p!=0 && p->pBt!=0 );
  if( p->pBt->pPagerShim ){
    p->pBt->pPagerShim->iDataVersion++;
  }else{
    p->iBDataVersion++;
  }
}

void btreeBumpLocalDataVersion(Btree *p){
  assert( p!=0 && p->pBt!=0 );
  if( p->pBt->pPagerShim ){
    p->pBt->pPagerShim->iDataVersion++;
    p->iBDataVersion--;
  }else{
    p->iBDataVersion++;
  }
}

void btreeMarkWorkingStateChanged(Btree *p, int bLocal){
  BtShared *pBt;
  assert( p!=0 && p->pBt!=0 );
  pBt = p->pBt;
  pBt->iWorkingStateVersion++;
  if( pBt->iWorkingStateVersion==0 ){
    pBt->iWorkingStateVersion = 1;
  }
  p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion;
  if( bLocal ){
    btreeBumpLocalDataVersion(p);
  }else{
    btreeBumpExternalDataVersion(p);
  }
  assert( p->iLoadedWorkingStateVersion==pBt->iWorkingStateVersion );
}

int btreeRefreshSharedWorkingState(Btree *p){
  BtShared *pBt;
  ProllyHash loadedCatHash;
  int rc;
  assert( p!=0 && p->pBt!=0 );
  pBt = p->pBt;
  if( p->iLoadedWorkingStateVersion==pBt->iWorkingStateVersion ){
    return SQLITE_OK;
  }
  memset(&loadedCatHash, 0, sizeof(loadedCatHash));
  rc = btreeReloadBranchWorkingStateInto(p, 1, &loadedCatHash);
  if( rc!=SQLITE_OK ) return rc;
  btreeStoreCommittedFromCurrent(p, &loadedCatHash);
  p->bCatalogDropped = 0;
  p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion;
  btreeBumpExternalDataVersion(p);
  return SQLITE_OK;
}

int btreeRefreshFromDisk(Btree *p){
  BtShared *pBt;
  int bChanged = 0;
  u8 snapshotPinned;
  int bAutocommitBoundary;
  ProllyHash loadedCatHash;
  int rc;

  assert( p!=0 && p->pBt!=0 );
  pBt = p->pBt;
  snapshotPinned = pBt->store.snapshotPinned;
  bAutocommitBoundary = p->inTrans==TRANS_NONE
    && p->db && p->db->autoCommit && !p->db->pSavepoint;

  if( bAutocommitBoundary ){
    pBt->store.snapshotPinned = 0;
  }
  rc = chunkStoreRefreshIfChanged(&pBt->store, &bChanged);
  if( bAutocommitBoundary ){
    pBt->store.snapshotPinned = snapshotPinned;
  }
  if( rc!=SQLITE_OK ) return rc;
  if( !bChanged ) return SQLITE_OK;

  memset(&loadedCatHash, 0, sizeof(loadedCatHash));
  rc = btreeReloadBranchWorkingStateInto(p, 1, &loadedCatHash);
  if( rc!=SQLITE_OK ) return rc;

  btreeStoreCommittedFromCurrent(p, &loadedCatHash);
  btreeMarkWorkingStateChanged(p, 0);

  return SQLITE_OK;
}


const char *doltliteGetSessionBranch(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zBranch ? p->zBranch : "main";
  }
  return "main";
}

static int replaceSessionString(char **pzDest, const char *zValue){
  char *zNew = 0;
  if( zValue ){
    if( sqlite3FaultSim(955) ) return SQLITE_NOMEM;
    zNew = sqlite3_mprintf("%s", zValue);
    if( !zNew ) return SQLITE_NOMEM;
  }
  sqlite3_free(*pzDest);
  *pzDest = zNew;
  return SQLITE_OK;
}

int doltlitePrepareSessionBranch(
  sqlite3 *db,
  const char *zBranch,
  char **pzPrepared
){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt
   || !zBranch || !pzPrepared ){
    return SQLITE_MISUSE;
  }
  *pzPrepared = 0;
  if( strcmp(doltliteGetSessionBranch(db), zBranch)==0 ){
    return SQLITE_OK;
  }
  if( sqlite3FaultSim(955) ) return SQLITE_NOMEM;
  *pzPrepared = sqlite3_mprintf("%s", zBranch);
  return *pzPrepared ? SQLITE_OK : SQLITE_NOMEM;
}

void doltliteInstallPreparedSessionBranch(
  sqlite3 *db,
  char *zPrepared
){
  Btree *p;
  assert( db!=0 && db->nDb>0 && db->aDb[0].pBt!=0 );
  assert( zPrepared!=0 );
  p = db->aDb[0].pBt;
  sqlite3_free(p->zBranch);
  p->zBranch = zPrepared;
}

int doltliteSetSessionBranch(sqlite3 *db, const char *zBranch){
  char *zPrepared = 0;
  int rc = doltlitePrepareSessionBranch(db, zBranch, &zPrepared);
  if( rc==SQLITE_OK && zPrepared ){
    doltliteInstallPreparedSessionBranch(db, zPrepared);
  }
  return rc;
}

const char *doltliteGetAuthorName(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zAuthorName ? p->zAuthorName : "doltlite";
  }
  return "doltlite";
}

int doltliteSetAuthorName(sqlite3 *db, const char *zName){
  Btree *p;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ){
    return SQLITE_MISUSE;
  }
  p = db->aDb[0].pBt;
  if( (p->zAuthorName==0 && zName==0)
   || (p->zAuthorName && zName && strcmp(p->zAuthorName, zName)==0) ){
    return SQLITE_OK;
  }
  return replaceSessionString(&p->zAuthorName, zName);
}

const char *doltliteGetAuthorEmail(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zAuthorEmail ? p->zAuthorEmail : "";
  }
  return "";
}

int doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail){
  Btree *p;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ){
    return SQLITE_MISUSE;
  }
  p = db->aDb[0].pBt;
  if( (p->zAuthorEmail==0 && zEmail==0)
   || (p->zAuthorEmail && zEmail && strcmp(p->zAuthorEmail, zEmail)==0) ){
    return SQLITE_OK;
  }
  return replaceSessionString(&p->zAuthorEmail, zEmail);
}

void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(pHead, &db->aDb[0].pBt->headCommit, sizeof(ProllyHash));
  }else{
    memset(pHead, 0, sizeof(ProllyHash));
  }
}

void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->headCommit, pHead, sizeof(ProllyHash));
  }
}

void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(pStaged, &db->aDb[0].pBt->vc.stagedCatalog, sizeof(ProllyHash));
  }else{
    memset(pStaged, 0, sizeof(ProllyHash));
  }
}

/* Session VC state is captured per savepoint level by
** captureSavepointSessionState when a level is pushed, and levels push
** lazily at write time. Mutating this state is a write: push any pending
** levels first so ROLLBACK TO restores the pre-mutation values. */
static int sessionStateSyncSavepoints(Btree *p){
  if( p->inTrans==TRANS_WRITE ) return syncBtreeSavepoints(p);
  return SQLITE_OK;
}

int doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    int rc = sessionStateSyncSavepoints(p);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(&p->vc.stagedCatalog, pStaged, sizeof(ProllyHash));
  }
  return SQLITE_OK;
}

void doltliteGetSessionMergeState(sqlite3 *db, u8 *pIsMerging,
                                   ProllyHash *pMergeCommit,
                                   ProllyHash *pConflictsCatalog){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    if( pIsMerging ) *pIsMerging = p->vc.isMerging;
    if( pMergeCommit ) memcpy(pMergeCommit, &p->vc.mergeCommitHash, sizeof(ProllyHash));
    if( pConflictsCatalog ) memcpy(pConflictsCatalog, &p->vc.conflictsCatalogHash, sizeof(ProllyHash));
  }else{
    if( pIsMerging ) *pIsMerging = 0;
    if( pMergeCommit ) memset(pMergeCommit, 0, sizeof(ProllyHash));
    if( pConflictsCatalog ) memset(pConflictsCatalog, 0, sizeof(ProllyHash));
  }
}

int doltliteSetSessionMergeState(sqlite3 *db, u8 isMerging,
                                  const ProllyHash *pMergeCommit,
                                  const ProllyHash *pConflictsCatalog){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    int rc = sessionStateSyncSavepoints(p);
    if( rc!=SQLITE_OK ) return rc;
    p->vc.isMerging = isMerging;
    if( pMergeCommit ) memcpy(&p->vc.mergeCommitHash, pMergeCommit, sizeof(ProllyHash));
    else memset(&p->vc.mergeCommitHash, 0, sizeof(ProllyHash));
    if( pConflictsCatalog ) memcpy(&p->vc.conflictsCatalogHash, pConflictsCatalog, sizeof(ProllyHash));
    else memset(&p->vc.conflictsCatalogHash, 0, sizeof(ProllyHash));
  }
  return SQLITE_OK;
}

int doltliteClearSessionMergeState(sqlite3 *db){
  return doltliteSetSessionMergeState(db, 0, 0, 0);
}

/* Record a new conflicts catalog without disturbing the source commit already
** on the session. Reaching for doltliteSetSessionMergeState with a null
** pMergeCommit zeroes it, which drops the second parent commit owes the merged
** branch and blanks dolt_merge_status.source. */
int doltliteSetSessionMergeConflicts(sqlite3 *db, const ProllyHash *pConflicts){
  ProllyHash mergeCommit;
  doltliteGetSessionMergeState(db, 0, &mergeCommit, 0);
  return doltliteSetSessionMergeState(db, 1, &mergeCommit, pConflicts);
}

int doltliteSetSessionMergeSourceSpec(sqlite3 *db, const char *zSpec,
                                      const ProllyHash *pMergeCommit){
  Btree *p;
  char *zNew = 0;
  if( !db || db->nDb<=0 || db->aDb[0].pBt==0 ) return SQLITE_OK;
  p = db->aDb[0].pBt;
  if( zSpec && pMergeCommit ){
    zNew = sqlite3_mprintf("%s", zSpec);
    if( !zNew ) return SQLITE_NOMEM;
  }
  sqlite3_free(p->zMergeSourceSpec);
  p->zMergeSourceSpec = zNew;
  if( zNew ){
    memcpy(&p->mergeSourceSpecCommit, pMergeCommit, sizeof(ProllyHash));
  }else{
    memset(&p->mergeSourceSpecCommit, 0, sizeof(ProllyHash));
  }
  return SQLITE_OK;
}

const char *doltliteGetSessionMergeSourceSpec(sqlite3 *db,
                                              const ProllyHash *pMergeCommit){
  Btree *p;
  if( !db || db->nDb<=0 || db->aDb[0].pBt==0 || !pMergeCommit ) return 0;
  p = db->aDb[0].pBt;
  if( !p->zMergeSourceSpec ) return 0;
  if( prollyHashCompare(&p->mergeSourceSpecCommit, pMergeCommit)!=0 ) return 0;
  return p->zMergeSourceSpec;
}

void doltliteGetSessionRebaseState(sqlite3 *db, u8 *pIsRebasing,
                                    ProllyHash *pPreRebaseCat,
                                    ProllyHash *pRebaseOnto,
                                    const char **pzOrigBranch,
                                    const char **pzReturnBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    if( pIsRebasing ) *pIsRebasing = p->isRebasing;
    if( pPreRebaseCat ) memcpy(pPreRebaseCat, &p->preRebaseWorkingCat, sizeof(ProllyHash));
    if( pRebaseOnto ) memcpy(pRebaseOnto, &p->rebaseOntoCommit, sizeof(ProllyHash));
    if( pzOrigBranch ) *pzOrigBranch = p->zRebaseOrigBranch;
    if( pzReturnBranch ) *pzReturnBranch = p->zRebaseReturnBranch;
  }else{
    if( pIsRebasing ) *pIsRebasing = 0;
    if( pPreRebaseCat ) memset(pPreRebaseCat, 0, sizeof(ProllyHash));
    if( pRebaseOnto ) memset(pRebaseOnto, 0, sizeof(ProllyHash));
    if( pzOrigBranch ) *pzOrigBranch = 0;
    if( pzReturnBranch ) *pzReturnBranch = 0;
  }
}

int doltliteSetSessionRebaseState(sqlite3 *db, u8 isRebasing,
                                   const ProllyHash *pPreRebaseCat,
                                   const ProllyHash *pRebaseOnto,
                                   const char *zOrigBranch,
                                   const char *zReturnBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    char *zNewOrigBranch = 0;
    char *zNewReturnBranch = 0;
    int rc;
    if( (zOrigBranch && strlen(zOrigBranch)>=WS_REBASE_BRANCH_LEN)
     || (zReturnBranch && strlen(zReturnBranch)>=WS_REBASE_BRANCH_LEN) ){
      return SQLITE_TOOBIG;
    }
    if( zOrigBranch ){
      zNewOrigBranch = sqlite3_mprintf("%s", zOrigBranch);
      if( !zNewOrigBranch ) return SQLITE_NOMEM;
    }
    if( zReturnBranch ){
      zNewReturnBranch = sqlite3_mprintf("%s", zReturnBranch);
      if( !zNewReturnBranch ){
        sqlite3_free(zNewOrigBranch);
        return SQLITE_NOMEM;
      }
    }
    rc = sessionStateSyncSavepoints(p);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zNewOrigBranch);
      sqlite3_free(zNewReturnBranch);
      return rc;
    }
    p->isRebasing = isRebasing;
    if( pPreRebaseCat ) memcpy(&p->preRebaseWorkingCat, pPreRebaseCat, sizeof(ProllyHash));
    else memset(&p->preRebaseWorkingCat, 0, sizeof(ProllyHash));
    if( pRebaseOnto ) memcpy(&p->rebaseOntoCommit, pRebaseOnto, sizeof(ProllyHash));
    else memset(&p->rebaseOntoCommit, 0, sizeof(ProllyHash));
    sqlite3_free(p->zRebaseOrigBranch);
    p->zRebaseOrigBranch = zNewOrigBranch;
    sqlite3_free(p->zRebaseReturnBranch);
    p->zRebaseReturnBranch = zNewReturnBranch;
  }
  return SQLITE_OK;
}

int doltliteClearSessionRebaseState(sqlite3 *db){
  return doltliteSetSessionRebaseState(db, 0, 0, 0, 0, 0);
}

void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash){
  u8 isMerging = 0;
  if( pHash ) memset(pHash, 0, sizeof(*pHash));
  if( !db || db->nDb<=0 || !db->aDb[0].pBt || !pHash ) return;
  {
    Btree *p = db->aDb[0].pBt;
    if( !db->autoCommit || sqlite3_txn_state(db, "main")!=SQLITE_TXN_NONE || db->pSavepoint ){
      if( p->vc.isMerging ){
        memcpy(pHash, &p->vc.conflictsCatalogHash, sizeof(*pHash));
      }
      return;
    }
  }
  if( db->autoCommit && sqlite3_txn_state(db, "main")==SQLITE_TXN_NONE ){
    /* Idle sessions must see the DURABLE working set, not this session's
    ** cached view. A locked refresh (cheap when the store is unchanged)
    ** gives the in-memory read below the same guarantee the throwaway
    ** read-only connection this used to open per call did — without paying
    ** a full store open and WAL replay every time. */
    ChunkStore *pStore = &db->aDb[0].pBt->pBt->store;
    if( chunkStoreLockAndRefresh(pStore)==SQLITE_OK ){
      (void)chunkStoreForceRefresh(pStore);
      chunkStoreUnlock(pStore);
    }
  }
  {
    Btree *p = db->aDb[0].pBt;
    const char *zBr = p->zBranch ? p->zBranch : "main";
    int rc = btreeLoadWorkingSetBlob(&p->pBt->store, zBr,
                                     0, 0, 0, &isMerging,
                                     0, pHash, 0, 0, 0, 0, 0, 0);
    if( rc!=SQLITE_OK || !isMerging ){
      memset(pHash, 0, sizeof(*pHash));
    }
  }
}

int doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    int rc = sessionStateSyncSavepoints(p);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(&p->vc.conflictsCatalogHash, pHash, sizeof(ProllyHash));
  }
  return SQLITE_OK;
}

void doltliteGetSessionConstraintViolationsCatalog(sqlite3 *db, ProllyHash *pHash){
  if( pHash ) memset(pHash, 0, sizeof(*pHash));
  if( db && db->nDb>0 && db->aDb[0].pBt && pHash ){
    memcpy(pHash, &db->aDb[0].pBt->vc.constraintViolationsHash, sizeof(ProllyHash));
  }
}

int doltliteGetSessionTableRoot(
  sqlite3 *db, Pgno iTable, ProllyHash *pRoot, u8 *pFlags
){
  Btree *pBtree;
  struct TableEntry *pTE;
  if( pRoot ) memset(pRoot, 0, sizeof(*pRoot));
  if( pFlags ) *pFlags = 0;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  pTE = catFind(&pBtree->cat, iTable);
  if( !pTE ) return SQLITE_NOTFOUND;
  if( pRoot ) memcpy(pRoot, &pTE->root, sizeof(ProllyHash));
  if( pFlags ) *pFlags = pTE->flags;
  return SQLITE_OK;
}

int doltliteSetSessionConstraintViolationsCatalog(sqlite3 *db, const ProllyHash *pHash){
  static const ProllyHash emptyHash = {{0}};
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    int rc = sessionStateSyncSavepoints(p);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(&p->vc.constraintViolationsHash,
           pHash ? pHash : &emptyHash, sizeof(ProllyHash));
  }
  return SQLITE_OK;
}

int doltliteSessionHasConstraintViolations(sqlite3 *db){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return 0;
  return !prollyHashIsEmpty(&db->aDb[0].pBt->vc.constraintViolationsHash);
}

void *doltliteGetCvBatch(sqlite3 *db){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return 0;
  return db->aDb[0].pBt->pCvBatch;
}

void doltliteSetCvBatch(sqlite3 *db, void *pBatch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    db->aDb[0].pBt->pCvBatch = pBatch;
  }
}

int doltliteSeedSessionHashes(
  sqlite3 *db,
  ChunkStore *cs,
  int (*xPush)(void*, const ProllyHash*),
  void *pCtx
){
  int i, rc = SQLITE_OK;
  if( !db || !cs || !xPush ) return SQLITE_OK;
  for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
    Btree *pBt = db->aDb[i].pBt;
    if( !pBt || pBt->pOps!=&prollyBtreeOps || !pBt->pBt ) continue;
    if( &pBt->pBt->store!=cs ) continue;
    rc = xPush(pCtx, &pBt->vc.stagedCatalog);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->vc.mergeCommitHash);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->vc.conflictsCatalogHash);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->preRebaseWorkingCat);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->rebaseOntoCommit);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->vc.constraintViolationsHash);
    /* The live catalog's roots: table 1 can be the runtime master-root view,
    ** which is intentionally absent from the persisted catalog — without this
    ** a GC collects it while the open session still reads through it. */
    {
      int k;
      for(k=0; rc==SQLITE_OK && k<pBt->cat.n; k++){
        rc = xPush(pCtx, &pBt->cat.a[k].root);
      }
    }
  }
  return rc;
}

int doltliteSaveWorkingSetWithHash(sqlite3 *db, const ProllyHash *pWorkingCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash workingCatHash;
  ProllyHash wsHash;
  const char *zBranch;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  zBranch = pBtree->zBranch ? pBtree->zBranch : "main";

  if( pWorkingCatHash ){
    workingCatHash = *pWorkingCatHash;
  }else{
    rc = serializeCatalog(pBtree, &catData, &nCatData);
    if( rc != SQLITE_OK ) return rc;
    rc = chunkStorePut(cs, catData, nCatData, &workingCatHash);
    sqlite3_free(catData);
    if( rc != SQLITE_OK ) return rc;
  }

  rc = btreeStoreWorkingSetBlob(cs, zBranch, &workingCatHash,
                                &pBtree->headCommit, &pBtree->vc.stagedCatalog,
                                pBtree->vc.isMerging, &pBtree->vc.mergeCommitHash,
                                &pBtree->vc.conflictsCatalogHash,
                                pBtree->isRebasing,
                                &pBtree->preRebaseWorkingCat,
                                &pBtree->rebaseOntoCommit,
                                pBtree->zRebaseOrigBranch,
                                pBtree->zRebaseReturnBranch,
                                &pBtree->vc.constraintViolationsHash);
  if( rc!=SQLITE_OK ) return rc;

  if( pBtree->isRebasing
   && pBtree->zRebaseReturnBranch
   && pBtree->zRebaseReturnBranch[0]
   && sqlite3_stricmp(zBranch, pBtree->zRebaseReturnBranch)!=0 ){
    rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
    if( rc!=SQLITE_OK ) return rc;
    rc = chunkStoreSetBranchWorkingSet(cs, pBtree->zRebaseReturnBranch, &wsHash);
    if( rc!=SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
}

int doltliteSaveWorkingSet(sqlite3 *db){
  return doltliteSaveWorkingSetWithHash(db, 0);
}

int doltlitePersistWorkingSetWithHash(sqlite3 *db, const ProllyHash *pWorkingCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *p;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = doltliteSaveWorkingSetWithHash(db, pWorkingCatHash);
  if( rc!=SQLITE_OK ) return rc;
  /* Saving keeps the working set in the pending chunk set, which is where an
  ** unresolved conflict is allowed to live; committing is what would make it
  ** durable, and DoltLite never persists conflicts. Callers reach here from
  ** paths that persist as a side effect before diagnosing the conflict
  ** themselves -- dolt_commit's staging, for one -- so this stays silent and
  ** leaves the refusal to that caller and to commit phase one. */
  p = (db && db->nDb>0) ? db->aDb[0].pBt : 0;
  if( p && !prollyHashIsEmpty(&p->vc.conflictsCatalogHash) ) return SQLITE_OK;
  rc = chunkStoreSerializeRefs(cs);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(cs);
}

int doltlitePersistWorkingSet(sqlite3 *db){
  return doltlitePersistWorkingSetWithHash(db, 0);
}

void doltliteAdoptRollbackBaseline(
  sqlite3 *db,
  const ProllyHash *pCatalogHash
){
  Btree *pBtree;
  if( !db || db->nDb<=0 || !pCatalogHash ) return;
  pBtree = db->aDb[0].pBt;
  if( !pBtree ) return;
  btreeStoreCommittedFromCurrent(pBtree, pCatalogHash);
}

int doltliteGetPersistedWorkingCatalogHash(sqlite3 *db, ProllyHash *pCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  const char *zBranch;

  if( pCatHash ) memset(pCatHash, 0, sizeof(*pCatHash));
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;
  zBranch = pBtree->zBranch ? pBtree->zBranch : "main";
  return btreeReadWorkingCatalog(cs, zBranch, pCatHash, 0);
}

int doltliteLoadWorkingSet(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  char *zNewRebaseOrigBranch = 0;
  char *zNewRebaseReturnBranch = 0;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  rc = btreeLoadWorkingSetBlob(cs, zBranch, 0, 0,
                               &pBtree->vc.stagedCatalog,
                               &pBtree->vc.isMerging, &pBtree->vc.mergeCommitHash,
                               &pBtree->vc.conflictsCatalogHash,
                               &pBtree->isRebasing,
                               &pBtree->preRebaseWorkingCat,
                               &pBtree->rebaseOntoCommit,
                               &zNewRebaseOrigBranch,
                               &zNewRebaseReturnBranch,
                               &pBtree->vc.constraintViolationsHash);
  if( rc == SQLITE_NOTFOUND ){
    memset(&pBtree->vc.stagedCatalog, 0, sizeof(ProllyHash));
    pBtree->vc.isMerging = 0;
    memset(&pBtree->vc.mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&pBtree->vc.conflictsCatalogHash, 0, sizeof(ProllyHash));
    pBtree->isRebasing = 0;
    memset(&pBtree->preRebaseWorkingCat, 0, sizeof(ProllyHash));
    memset(&pBtree->rebaseOntoCommit, 0, sizeof(ProllyHash));
    sqlite3_free(pBtree->zRebaseOrigBranch);
    pBtree->zRebaseOrigBranch = 0;
    sqlite3_free(pBtree->zRebaseReturnBranch);
    pBtree->zRebaseReturnBranch = 0;
    memset(&pBtree->vc.constraintViolationsHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    sqlite3_free(pBtree->zRebaseOrigBranch);
    pBtree->zRebaseOrigBranch = zNewRebaseOrigBranch;
    sqlite3_free(pBtree->zRebaseReturnBranch);
    pBtree->zRebaseReturnBranch = zNewRebaseReturnBranch;
  }else{
    sqlite3_free(zNewRebaseOrigBranch);
    sqlite3_free(zNewRebaseReturnBranch);
  }
  return rc;
}


#endif /* DOLTLITE_PROLLY */
