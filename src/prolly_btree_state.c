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
  if( !data || nData < WS_TOTAL_SIZE_V2 ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  version = data[0];
  if( version != WS_FORMAT_VERSION_V2
   && version != WS_FORMAT_VERSION_V3
   && version != WS_FORMAT_VERSION_V4
   && version != WS_FORMAT_VERSION_V5 ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

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

void btreeFillWorkingSetBlob(
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
    int n = (int)strlen(zRebaseOrigBranch);
    if( n > WS_REBASE_BRANCH_LEN - 1 ) n = WS_REBASE_BRANCH_LEN - 1;
    memcpy(buf + WS_REBASE_BRANCH_OFF, zRebaseOrigBranch, n);
  }
  if( zRebaseReturnBranch ){
    int n = (int)strlen(zRebaseReturnBranch);
    if( n > WS_REBASE_BRANCH_LEN - 1 ) n = WS_REBASE_BRANCH_LEN - 1;
    memcpy(buf + WS_REBASE_RETURN_BRANCH_OFF, zRebaseReturnBranch, n);
  }
  memcpy(buf + WS_CONSTRAINT_VIOLATIONS_OFF,
         (pConstraintViolations ? pConstraintViolations : &emptyHash)->data,
         PROLLY_HASH_SIZE);
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

  btreeFillWorkingSetBlob(buf, pWorkingCat, pWorkingCommit, pStaged,
                          isMerging, pMergeCommit, pConflicts,
                          isRebasing, pPreRebaseCat, pRebaseOnto,
                          zRebaseOrigBranch, zRebaseReturnBranch,
                          pConstraintViolations);

  rc = chunkStorePut(cs, buf, WS_TOTAL_SIZE, &wsHash);
  if( rc != SQLITE_OK ) return rc;
  rc = chunkStoreSetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc == SQLITE_NOTFOUND ){
    /* A freshly attached doltlite db is never seeded — doltliteMaybeSeedRepo
    ** only runs for the main db — so its default branch doesn't exist. Create
    ** it on first persist (pointing at the current head, empty if uncommitted)
    ** so this and later writes succeed instead of failing SQLITE_NOTFOUND. */
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

  assert( cs!=0 && zBranch!=0 && pCatHash!=0 && pCommitHash!=0 );
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
  ProllyHash catHash;
  ProllyHash workingCommitHash;
  ProllyHash stagedCatalog;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;
  ProllyHash preRebaseCat;
  ProllyHash rebaseOnto;
  ProllyHash constraintViolationsHash;
  char *zRebaseOrigBranch = 0;
  char *zRebaseReturnBranch = 0;
  const char *zBr;
  u8 isMerging = 0;
  u8 isRebasing = 0;
  int hadUserCatalog;
  int rc;

  assert( p!=0 && p->pBt!=0 );
  pBt = p->pBt;
  zBr = p->zBranch ? p->zBranch : "main";
  hadUserCatalog = p->cat.n > 1;
  memset(&catHash, 0, sizeof(catHash));
  memset(&workingCommitHash, 0, sizeof(workingCommitHash));
  memset(&stagedCatalog, 0, sizeof(stagedCatalog));
  memset(&mergeCommitHash, 0, sizeof(mergeCommitHash));
  memset(&conflictsCatalogHash, 0, sizeof(conflictsCatalogHash));
  memset(&preRebaseCat, 0, sizeof(preRebaseCat));
  memset(&rebaseOnto, 0, sizeof(rebaseOnto));
  memset(&constraintViolationsHash, 0, sizeof(constraintViolationsHash));

  rc = chunkStoreEnsureRefsFresh(&pBt->store);
  if( rc!=SQLITE_OK ) return rc;
  rc = btreeLoadWorkingSetBlob(
      &pBt->store, zBr, &catHash, &workingCommitHash, &stagedCatalog, &isMerging,
      &mergeCommitHash, &conflictsCatalogHash,
      &isRebasing, &preRebaseCat, &rebaseOnto, &zRebaseOrigBranch,
      &zRebaseReturnBranch,
      &constraintViolationsHash);
  if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(zRebaseOrigBranch);
    sqlite3_free(zRebaseReturnBranch);
    return rc;
  }
  if( prollyHashIsEmpty(&catHash) ){
    rc = btreeLoadBranchHeadCatalog(&pBt->store, zBr, &catHash,
                                    &workingCommitHash);
    if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zRebaseOrigBranch);
      sqlite3_free(zRebaseReturnBranch);
      return rc;
    }
  }

  if( bLoadCatalog
   && !prollyHashIsEmpty(&catHash)
   && (prollyHashIsEmpty(&p->committedCatalogHash)
       || prollyHashCompare(&catHash, &p->committedCatalogHash)!=0) ){
    u8 *catData = 0;
    int nCatData = 0;
    rc = chunkStoreGet(&pBt->store, &catHash, &catData, &nCatData);
    if( rc==SQLITE_OK && catData ){
      rc = deserializeCatalog(p, catData, nCatData);
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zRebaseOrigBranch);
        sqlite3_free(zRebaseReturnBranch);
        return rc;
      }
    }else{
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zRebaseOrigBranch);
        sqlite3_free(zRebaseReturnBranch);
        return rc;
      }
    }
  }
  if( prollyHashIsEmpty(&p->headCommit) || !hadUserCatalog ){
    p->headCommit = workingCommitHash;
  }
  if( pLoadedCatHash ){
    *pLoadedCatHash = catHash;
  }

  p->stagedCatalog = stagedCatalog;
  p->isMerging = isMerging;
  p->mergeCommitHash = mergeCommitHash;
  p->conflictsCatalogHash = conflictsCatalogHash;
  p->isRebasing = isRebasing;
  p->preRebaseWorkingCat = preRebaseCat;
  p->rebaseOntoCommit = rebaseOnto;
  sqlite3_free(p->zRebaseOrigBranch);
  p->zRebaseOrigBranch = zRebaseOrigBranch;
  sqlite3_free(p->zRebaseReturnBranch);
  p->zRebaseReturnBranch = zRebaseReturnBranch;
  p->constraintViolationsHash = constraintViolationsHash;
  return SQLITE_OK;
}

void btreeStoreCommittedFromCurrent(Btree *p, const ProllyHash *pCatHash){
  assert( p!=0 );
  if( pCatHash ){
    p->committedCatalogHash = *pCatHash;
  }
  p->committedStagedCatalog = p->stagedCatalog;
  p->committedIsMerging = p->isMerging;
  p->committedMergeCommitHash = p->mergeCommitHash;
  p->committedConflictsCatalogHash = p->conflictsCatalogHash;
  p->committedConstraintViolationsHash = p->constraintViolationsHash;
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

void doltliteSetSessionBranch(sqlite3 *db, const char *zBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    assert( zBranch!=0 );
    sqlite3_free(p->zBranch);
    p->zBranch = sqlite3_mprintf("%s", zBranch);
  }
}

const char *doltliteGetAuthorName(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zAuthorName ? p->zAuthorName : "doltlite";
  }
  return "doltlite";
}

void doltliteSetAuthorName(sqlite3 *db, const char *zName){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sqlite3_free(p->zAuthorName);
    p->zAuthorName = zName ? sqlite3_mprintf("%s", zName) : 0;
  }
}

const char *doltliteGetAuthorEmail(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zAuthorEmail ? p->zAuthorEmail : "";
  }
  return "";
}

void doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sqlite3_free(p->zAuthorEmail);
    p->zAuthorEmail = zEmail ? sqlite3_mprintf("%s", zEmail) : 0;
  }
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
    memcpy(pStaged, &db->aDb[0].pBt->stagedCatalog, sizeof(ProllyHash));
  }else{
    memset(pStaged, 0, sizeof(ProllyHash));
  }
}

/* Session VC state is captured per savepoint level by
** captureSavepointSessionState when a level is pushed, and levels push
** lazily at write time. Mutating this state is a write: push any pending
** levels first so ROLLBACK TO restores the pre-mutation values. */
static void sessionStateSyncSavepoints(Btree *p){
  if( p->inTrans==TRANS_WRITE ) syncBtreeSavepoints(p);
}

void doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    sessionStateSyncSavepoints(db->aDb[0].pBt);
    memcpy(&db->aDb[0].pBt->stagedCatalog, pStaged, sizeof(ProllyHash));
  }
}

void doltliteGetSessionMergeState(sqlite3 *db, u8 *pIsMerging,
                                   ProllyHash *pMergeCommit,
                                   ProllyHash *pConflictsCatalog){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    if( pIsMerging ) *pIsMerging = p->isMerging;
    if( pMergeCommit ) memcpy(pMergeCommit, &p->mergeCommitHash, sizeof(ProllyHash));
    if( pConflictsCatalog ) memcpy(pConflictsCatalog, &p->conflictsCatalogHash, sizeof(ProllyHash));
  }else{
    if( pIsMerging ) *pIsMerging = 0;
    if( pMergeCommit ) memset(pMergeCommit, 0, sizeof(ProllyHash));
    if( pConflictsCatalog ) memset(pConflictsCatalog, 0, sizeof(ProllyHash));
  }
}

void doltliteSetSessionMergeState(sqlite3 *db, u8 isMerging,
                                   const ProllyHash *pMergeCommit,
                                   const ProllyHash *pConflictsCatalog){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sessionStateSyncSavepoints(p);
    p->isMerging = isMerging;
    if( pMergeCommit ) memcpy(&p->mergeCommitHash, pMergeCommit, sizeof(ProllyHash));
    else memset(&p->mergeCommitHash, 0, sizeof(ProllyHash));
    if( pConflictsCatalog ) memcpy(&p->conflictsCatalogHash, pConflictsCatalog, sizeof(ProllyHash));
    else memset(&p->conflictsCatalogHash, 0, sizeof(ProllyHash));
  }
}

void doltliteClearSessionMergeState(sqlite3 *db){
  doltliteSetSessionMergeState(db, 0, 0, 0);
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

void doltliteSetSessionRebaseState(sqlite3 *db, u8 isRebasing,
                                    const ProllyHash *pPreRebaseCat,
                                    const ProllyHash *pRebaseOnto,
                                    const char *zOrigBranch,
                                    const char *zReturnBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sessionStateSyncSavepoints(p);
    p->isRebasing = isRebasing;
    if( pPreRebaseCat ) memcpy(&p->preRebaseWorkingCat, pPreRebaseCat, sizeof(ProllyHash));
    else memset(&p->preRebaseWorkingCat, 0, sizeof(ProllyHash));
    if( pRebaseOnto ) memcpy(&p->rebaseOntoCommit, pRebaseOnto, sizeof(ProllyHash));
    else memset(&p->rebaseOntoCommit, 0, sizeof(ProllyHash));
    sqlite3_free(p->zRebaseOrigBranch);
    p->zRebaseOrigBranch = zOrigBranch ? sqlite3_mprintf("%s", zOrigBranch) : 0;
    sqlite3_free(p->zRebaseReturnBranch);
    p->zRebaseReturnBranch = zReturnBranch ? sqlite3_mprintf("%s", zReturnBranch) : 0;
  }
}

void doltliteClearSessionRebaseState(sqlite3 *db){
  doltliteSetSessionRebaseState(db, 0, 0, 0, 0, 0);
}

void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash){
  u8 isMerging = 0;
  if( pHash ) memset(pHash, 0, sizeof(*pHash));
  if( !db || db->nDb<=0 || !db->aDb[0].pBt || !pHash ) return;
  {
    Btree *p = db->aDb[0].pBt;
    if( !db->autoCommit || sqlite3_txn_state(db, "main")!=SQLITE_TXN_NONE || db->pSavepoint ){
      if( p->isMerging ){
        memcpy(pHash, &p->conflictsCatalogHash, sizeof(*pHash));
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

void doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->conflictsCatalogHash, pHash, sizeof(ProllyHash));
  }
}

void doltliteGetSessionConstraintViolationsCatalog(sqlite3 *db, ProllyHash *pHash){
  if( pHash ) memset(pHash, 0, sizeof(*pHash));
  if( db && db->nDb>0 && db->aDb[0].pBt && pHash ){
    memcpy(pHash, &db->aDb[0].pBt->constraintViolationsHash, sizeof(ProllyHash));
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

void doltliteSetSessionConstraintViolationsCatalog(sqlite3 *db, const ProllyHash *pHash){
  static const ProllyHash emptyHash = {{0}};
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->constraintViolationsHash,
           pHash ? pHash : &emptyHash, sizeof(ProllyHash));
  }
}

int doltliteSessionHasConstraintViolations(sqlite3 *db){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return 0;
  return !prollyHashIsEmpty(&db->aDb[0].pBt->constraintViolationsHash);
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
    rc = xPush(pCtx, &pBt->stagedCatalog);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->mergeCommitHash);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->conflictsCatalogHash);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->preRebaseWorkingCat);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->rebaseOntoCommit);
    if( rc==SQLITE_OK ) rc = xPush(pCtx, &pBt->constraintViolationsHash);
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
                                &pBtree->headCommit, &pBtree->stagedCatalog,
                                pBtree->isMerging, &pBtree->mergeCommitHash,
                                &pBtree->conflictsCatalogHash,
                                pBtree->isRebasing,
                                &pBtree->preRebaseWorkingCat,
                                &pBtree->rebaseOntoCommit,
                                pBtree->zRebaseOrigBranch,
                                pBtree->zRebaseReturnBranch,
                                &pBtree->constraintViolationsHash);
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
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = doltliteSaveWorkingSetWithHash(db, pWorkingCatHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(cs);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(cs);
}

int doltlitePersistWorkingSet(sqlite3 *db){
  return doltlitePersistWorkingSetWithHash(db, 0);
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
                               &pBtree->stagedCatalog,
                               &pBtree->isMerging, &pBtree->mergeCommitHash,
                               &pBtree->conflictsCatalogHash,
                               &pBtree->isRebasing,
                               &pBtree->preRebaseWorkingCat,
                               &pBtree->rebaseOntoCommit,
                               &zNewRebaseOrigBranch,
                               &zNewRebaseReturnBranch,
                               &pBtree->constraintViolationsHash);
  if( rc == SQLITE_NOTFOUND ){
    memset(&pBtree->stagedCatalog, 0, sizeof(ProllyHash));
    pBtree->isMerging = 0;
    memset(&pBtree->mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&pBtree->conflictsCatalogHash, 0, sizeof(ProllyHash));
    pBtree->isRebasing = 0;
    memset(&pBtree->preRebaseWorkingCat, 0, sizeof(ProllyHash));
    memset(&pBtree->rebaseOntoCommit, 0, sizeof(ProllyHash));
    sqlite3_free(pBtree->zRebaseOrigBranch);
    pBtree->zRebaseOrigBranch = 0;
    sqlite3_free(pBtree->zRebaseReturnBranch);
    pBtree->zRebaseReturnBranch = 0;
    memset(&pBtree->constraintViolationsHash, 0, sizeof(ProllyHash));
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
