#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_btree_int.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "doltlite_name_index.h"
#include <stddef.h>
#include "doltlite_ignore.h"

#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>

void doltliteTestCrashFinalize(const char *zOperation){
#ifdef SQLITE_TEST
  const char *zCrash = getenv("DOLTLITE_CRASH_FINALIZE");
  if( zCrash && strcmp(zCrash, zOperation)==0 ) _Exit(99);
#else
  UNUSED_PARAMETER(zOperation);
#endif
}

static int failNextVcSeal = 0;
static int failNextHeadConfirm = 0;

void doltliteTestFailNextVcSeal(void){
  failNextVcSeal = 1;
}

void doltliteTestFailNextHeadConfirm(void){
  failNextHeadConfirm = 1;
}

static void (*xTestBeforeRefInstall)(void*) = 0;
static void *pTestBeforeRefInstallArg = 0;

/* Unlocked hook after fetch commits chunks but before a tracking ref roots
** them — the window a concurrent gc can empty. */
void doltliteTestSetBeforeRefInstallHook(void (*xHook)(void*), void *pArg){
  xTestBeforeRefInstall = xHook;
  pTestBeforeRefInstallArg = pArg;
}

void doltliteTestRunBeforeRefInstallHook(void){
  void (*xHook)(void*) = xTestBeforeRefInstall;
  if( xHook ){
    xTestBeforeRefInstall = 0;
    xHook(pTestBeforeRefInstallArg);
  }
}

void doltliteTxnStateClear(DoltliteTxnState *p){
  assert( p!=0 );
  sqlite3_free(p->zSessionBranch);
  memset(p, 0, sizeof(*p));
}

int doltliteSaveTxnState(sqlite3 *db, DoltliteTxnState *p){
  ChunkStore *cs;
  int rc;
  assert( db!=0 && p!=0 );
  cs = doltliteGetChunkStore(db);

  memset(p, 0, sizeof(*p));
  if( !cs ) return SQLITE_ERROR;

  memcpy(&p->refsHash, refsTableGetHash(&cs->refs), sizeof(ProllyHash));
  memcpy(&p->committedRefsHash, &cs->refs.committedRefsHash,
         sizeof(ProllyHash));

  p->zSessionBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !p->zSessionBranch ){
    doltliteTxnStateClear(p);
    return SQLITE_NOMEM;
  }
  doltliteGetSessionHead(db, &p->sessionHead);
  doltliteGetSessionStaged(db, &p->sessionStaged);
  doltliteGetSessionMergeState(db, &p->sessionIsMerging,
                               &p->sessionMergeCommit,
                               &p->sessionConflictsCatalog);
  doltliteGetSessionConstraintViolationsCatalog(
      db, &p->sessionConstraintViolationsCatalog);

  rc = doltliteFlushCatalogToHash(db, &p->sessionCatalogHash);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(p);
  }
  return rc;
}

int doltliteRestoreTxnState(sqlite3 *db, DoltliteTxnState *p){
  ChunkStore *cs;
  int rc;
  assert( db!=0 && p!=0 );
  assert( p->zSessionBranch!=0 );
  cs = doltliteGetChunkStore(db);

  if( !cs ) return SQLITE_ERROR;

  if( prollyHashCompare(&p->committedRefsHash,
                        &cs->refs.committedRefsHash)==0 ){
    refsTableSetHash(&cs->refs, &p->refsHash);
  }else{
    csRestoreCommittedRefsHash(cs);
  }
  if( prollyHashIsEmpty(&cs->refs.refsHash) ){
    chunkStoreClearRefs(cs);
  }else{
    rc = chunkStoreReloadRefs(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = doltliteSwitchCatalog(db, &p->sessionCatalogHash);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteSetSessionBranch(db, p->zSessionBranch);
  if( rc!=SQLITE_OK ) return rc;
  doltliteSetSessionHead(db, &p->sessionHead);
  rc = doltliteSetSessionStaged(db, &p->sessionStaged);
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionMergeState(db, p->sessionIsMerging,
                                      &p->sessionMergeCommit,
                                      &p->sessionConflictsCatalog);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(
        db, &p->sessionConstraintViolationsCatalog);
  }
  return rc;
}

int doltliteRestoreTxnStateOnFailure(
  sqlite3 *db,
  DoltliteTxnState *pSaved,
  int opRc
){
  int rc = doltliteRestoreTxnState(db, pSaved);
  doltliteTxnStateClear(pSaved);
  return rc==SQLITE_OK ? opRc : rc;
}

int doltliteRefreshAndConfirmHead(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pExpectedHead
){
  const char *zBranch;
  ProllyHash branchTip;
  int found = 0;
  int rc;
  assert( db!=0 && cs!=0 && pExpectedHead!=0 );

  if( failNextHeadConfirm ){
    failNextHeadConfirm = 0;
    return SQLITE_BUSY;
  }

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  /* Refresh in-memory state. No-op under a reentrant lock, so not a CAS basis. */
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }

  /* Compare the on-disk tip, not in-memory refs. Force-refresh is a no-op
  ** under a reentrant lock, and WAL reuse can hide a peer commit. */
  zBranch = doltliteGetSessionBranch(db);
  rc = chunkStoreReadDiskBranchTip(cs, zBranch, &branchTip, &found);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }
  /* A non-empty expected tip requires the branch to exist on disk; treating
  ** missing as confirmed would skip a peer that already created it. */
  if( !found ){
    if( !prollyHashIsEmpty(pExpectedHead) ){
      chunkStoreUnlock(cs);
      return SQLITE_BUSY;
    }
    return SQLITE_OK;
  }
  if( prollyHashCompare(&branchTip, pExpectedHead)!=0 ){
    chunkStoreUnlock(cs);
    return SQLITE_BUSY;
  }
  return SQLITE_OK;
}

int doltliteHasUncommittedChanges(sqlite3 *db, int *pDirty){
  ProllyHash headCatHash, stagedHash, workingCatHash;
  u8 *wCatData = 0; int nWCat = 0;
  int rc;

  if( !db || !pDirty ) return SQLITE_MISUSE;
  *pDirty = 0;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&headCatHash) ){
    sqlite3_stmt *pStmt = 0;
    int stepRc;
    int finalizeRc;
    rc = sqlite3_prepare_v2(db,
      "SELECT 1 FROM sqlite_master "
      "WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' "
      "AND name NOT LIKE 'dolt_%' "
      "LIMIT 1",
      -1, &pStmt, 0);
    if( rc!=SQLITE_OK ) return rc;
    stepRc = sqlite3_step(pStmt);
    if( stepRc==SQLITE_ROW ){
      *pDirty = 1;
      rc = SQLITE_OK;
    }else if( stepRc==SQLITE_DONE ){
      rc = SQLITE_OK;
    }else{
      rc = stepRc;
    }
    finalizeRc = sqlite3_finalize(pStmt);
    return rc==SQLITE_OK ? finalizeRc : rc;
  }

  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash)
   && prollyHashCompare(&headCatHash, &stagedHash)!=0 ){
    *pDirty = 1;
    return SQLITE_OK;
  }

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    if( !cs ) return SQLITE_ERROR;
    rc = doltliteFlushAndSerializeCatalog(db, &wCatData, &nWCat);
    if( rc!=SQLITE_OK ){
      sqlite3_free(wCatData);
      return rc;
    }
    rc = chunkStorePut(cs, wCatData, nWCat, &workingCatHash);
    sqlite3_free(wCatData);
    if( rc!=SQLITE_OK ) return rc;
    if( prollyHashCompare(&headCatHash, &workingCatHash)!=0 ){
      *pDirty = 1;
    }
    return SQLITE_OK;
  }
}

int doltliteUpdateSchemaHashes(sqlite3 *db){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int rc2;
  /* One sqlite_master scan for tables and indexes, keyed by rootpage.
  ** Virtual tables have rootpage 0 and no catalog entry. */
  if( !db ) return SQLITE_MISUSE;
  rc = sqlite3_prepare_v2(
      db,
      "SELECT name, rootpage, sql "
      "FROM main.sqlite_master "
      "WHERE type IN ('table','index') AND sql IS NOT NULL AND rootpage>0",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pStmt);
    return rc;
  }
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
    i64 iRoot = sqlite3_column_int64(pStmt, 1);
    const char *zCreate = (const char*)sqlite3_column_text(pStmt, 2);
    ProllyHash h;
    char *zCanon;
    if( !zName || !zCreate ){
      rc = db->mallocFailed ? SQLITE_NOMEM : SQLITE_CORRUPT;
      break;
    }
    if( iRoot<=0 || iRoot>(i64)0xffffffff ){
      rc = SQLITE_CORRUPT;
      break;
    }
    if( sqlite3FaultSim(954) ){
      rc = SQLITE_NOMEM;
      break;
    }
    zCanon = doltliteCanonicalizeSchemaSql(zCreate, zName);
    if( !zCanon ){
      rc = SQLITE_NOMEM;
      break;
    }
    prollyHashCompute(zCanon, (int)strlen(zCanon), &h);
    sqlite3_free(zCanon);
    rc = doltliteSetTableSchemaHash(db, (Pgno)iRoot, &h);
    if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_CORRUPT;
    }
    if( rc!=SQLITE_OK ){
      break;
    }
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  rc2 = sqlite3_finalize(pStmt);
  if( rc==SQLITE_OK ) rc = rc2;
  return rc;
}

int doltliteLoadLiveSchemaSql(
  sqlite3 *db,
  const char *zType,
  const char *zDb,
  const char *zName,
  const char *zTblName,
  char **pzSql
){
  sqlite3_stmt *pStmt = 0;
  char *zQuery = 0;
  int rc;

  *pzSql = 0;
  if( !db || !zType || !zName ) return SQLITE_OK;

  if( zTblName && zTblName[0] ){
    zQuery = sqlite3_mprintf(
      "SELECT sql FROM \"%w\".sqlite_master "
      "WHERE type=%Q AND name=%Q AND tbl_name=%Q",
      zDb ? zDb : "main", zType, zName, zTblName
    );
  }else{
    zQuery = sqlite3_mprintf(
      "SELECT sql FROM \"%w\".sqlite_master "
      "WHERE type=%Q AND name=%Q",
      zDb ? zDb : "main", zType, zName
    );
  }
  if( !zQuery ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    const char *zSql = (const char*)sqlite3_column_text(pStmt, 0);
    if( zSql ){
      *pzSql = sqlite3_mprintf("%s", zSql);
      if( !*pzSql ){
        sqlite3_finalize(pStmt);
        return SQLITE_NOMEM;
      }
    }
    rc = SQLITE_OK;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
  }
  sqlite3_finalize(pStmt);
  return rc;
}

int doltliteMutateRefsExpected(
  sqlite3 *db,
  const DoltliteBranchExpectation *aExpected,
  int nExpected,
  DoltliteRefsMutation xMutate,
  void *pArg
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ChunkStoreRefsSnapshot snapshot;
  const BranchRef *aBr = 0;
  int haveSnapshot = 0;
  int i;
  int nBr = 0;
  int iBr;
  int rc;

  memset(&snapshot, 0, sizeof(snapshot));

  if( !cs || !xMutate || nExpected<0 || (nExpected>0 && !aExpected) ){
    return SQLITE_MISUSE;
  }
  if( doltliteIsDetached(db) ) return SQLITE_READONLY;

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  /* xMutate + serializeRefs rewrite the whole refs blob; reload persisted
  ** refs rather than trusting the lock-time size heuristic. */
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }

  rc = chunkStoreSnapshotRefs(cs, &snapshot);
  if( rc==SQLITE_OK ) haveSnapshot = 1;
  for(i=0; rc==SQLITE_OK && i<nExpected; i++){
    ProllyHash tip;
    int found = 0;
    rc = chunkStoreReadDiskBranchTip(
        cs, aExpected[i].zBranch, &tip, &found);
    if( rc==SQLITE_OK ){
      if( aExpected[i].pTip ){
        if( !found || prollyHashCompare(&tip, aExpected[i].pTip)!=0 ){
          rc = SQLITE_BUSY;
        }
      }else if( found ){
        rc = SQLITE_BUSY;
      }
    }
  }

  if( rc==SQLITE_OK ) rc = xMutate(db, cs, pArg);
  /* PersistWorkingSet refuses a conflicted working set. Walk the refs about
  ** to land, not the session hash, so abort helpers can still finish. */
  if( rc==SQLITE_OK ){
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    for(iBr=0; rc==SQLITE_OK && iBr<nBr; iBr++){
      ProllyHash conflicts;
      int loadRc;
      if( !aBr[iBr].zName ) continue;
      loadRc = btreeLoadWorkingSetBlob(cs, aBr[iBr].zName, 0, 0, 0, 0, 0,
                                       &conflicts, 0, 0, 0, 0, 0, 0);
      if( loadRc==SQLITE_NOTFOUND ) continue;
      if( loadRc!=SQLITE_OK ){
        rc = loadRc;
      }else if( !prollyHashIsEmpty(&conflicts) ){
        rc = SQLITE_CONSTRAINT;
      }
    }
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefs(cs);
    if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  }
  if( haveSnapshot ){
    if( rc==SQLITE_OK ){
      chunkStoreDiscardRefsSnapshot(&snapshot);
    }else{
      chunkStoreRestoreRefsSnapshot(cs, &snapshot);
    }
  }

  chunkStoreUnlock(cs);
  return rc;
}

int doltliteMutateRefs(sqlite3 *db, DoltliteRefsMutation xMutate, void *pArg){
  return doltliteMutateRefsExpected(db, 0, 0, xMutate, pArg);
}

int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *catData = 0;
  int nCatData = 0;
  int rc;
  rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, catData, nCatData, pHash);
  sqlite3_free(catData);
  return rc;
}

int doltliteSerializeDb(sqlite3 *db, Btree *pBt,
                        unsigned char **ppData, sqlite3_int64 *pnData){
  const char *zBranch = 0;
  ProllyHash liveHash;
  const void *pLive = 0;
  if( db && db->nDb>0 && db->aDb[0].pBt==pBt ){
    int rc = doltliteFlushCatalogToHash(db, &liveHash);
    if( rc!=SQLITE_OK ) return rc;
    zBranch = doltliteGetSessionBranch(db);
    pLive = &liveHash;
  }
  return doltliteBtreeSerialize(pBt, zBranch, pLive, ppData, pnData);
}

int doltlitePrepareCatalogForPersistence(sqlite3 *db){
  UNUSED_PARAMETER(db);
  return SQLITE_OK;
}

void freeSchemaMergeActions(SchemaMergeAction *a, int n){
  int i, j;
  for(i=0; i<n; i++){
    for(j=0; j<a[i].nAddColumns; j++){
      sqlite3_free(a[i].azAddColumns[j]);
    }
    sqlite3_free(a[i].azAddColumns);
    for(j=0; j<a[i].nDropColumns; j++){
      sqlite3_free(a[i].azDropColumns[j]);
    }
    sqlite3_free(a[i].azDropColumns);
    for(j=0; j<a[i].nRenameColumns; j++){
      sqlite3_free(a[i].azRenameColumns[j]);
    }
    sqlite3_free(a[i].azRenameColumns);
    sqlite3_free(a[i].zRenameTable);
    sqlite3_free(a[i].zTableName);
  }
  sqlite3_free(a);
}

static int doltliteCreateAndStoreCommitOnStore(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
);

int doltliteCreateAndStoreCommit(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  ProllyHash *pCommitHash
){
  return doltliteCreateAndStoreCommitWithTime(db, pParent, pCatalog, zMessage,
      zAuthorName, zAuthorEmail, aExtraParents, nExtraParents, 0, pCommitHash);
}

static int doltliteCreateAndStoreCommitOnStore(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
){
  DoltliteCommit c;
  u8 *commitData = 0;
  int nCommitData = 0;
  int rc, i;
  assert( db!=0 && cs!=0 && pParent!=0 && pCatalog!=0 && pCommitHash!=0 );
  assert( nExtraParents>=0 );
  assert( nExtraParents==0 || aExtraParents!=0 );

  memset(&c, 0, sizeof(c));
  memcpy(&c.parentHash, pParent, sizeof(ProllyHash));
  memcpy(&c.catalogHash, pCatalog, sizeof(ProllyHash));
  c.timestamp = explicitTimestamp ? explicitTimestamp : (i64)time(0);
  c.zName  = sqlite3_mprintf("%s", zAuthorName  ? zAuthorName  : doltliteGetAuthorName(db));
  if( c.zName==0 ){
    rc = SQLITE_NOMEM;
    goto create_commit_done;
  }
  c.zEmail = sqlite3_mprintf("%s", zAuthorEmail ? zAuthorEmail : doltliteGetAuthorEmail(db));
  if( c.zEmail==0 ){
    rc = SQLITE_NOMEM;
    goto create_commit_done;
  }
  c.zMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
  if( c.zMessage==0 ){
    rc = SQLITE_NOMEM;
    goto create_commit_done;
  }

  if( nExtraParents > 0 && aExtraParents ){
    c.aParents[0] = *pParent;
    for(i=0; i<nExtraParents && (i+1)<DOLTLITE_MAX_PARENTS; i++){
      c.aParents[i+1] = aExtraParents[i];
    }
    c.nParents = 1 + (nExtraParents < DOLTLITE_MAX_PARENTS-1
                       ? nExtraParents : DOLTLITE_MAX_PARENTS-1);
  }

  rc = doltliteCommitSerialize(&c, &commitData, &nCommitData);
  if( rc==SQLITE_OK ) rc = chunkStorePut(cs, commitData, nCommitData, pCommitHash);
create_commit_done:
  sqlite3_free(commitData);
  doltliteCommitClear(&c);
  return rc;
}

int doltliteCreateAndStoreCommitWithTime(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  assert( cs!=0 );
  return doltliteCreateAndStoreCommitOnStore(db, cs, pParent, pCatalog,
      zMessage, zAuthorName, zAuthorEmail, aExtraParents, nExtraParents,
      explicitTimestamp, pCommitHash);
}

int doltliteSeedStoreIfNeeded(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pSeedHash,
  int *pSeeded
){
  ProllyHash tip;
  ProllyHash empty;
  int nBranches;
  int rc;

  *pSeeded = 0;
  if( cs->readOnly ) return SQLITE_OK;
  nBranches = refsTableBranchCount(&cs->refs);
  if( nBranches>1 ) return SQLITE_OK;
  if( nBranches>0 ){
    rc = chunkStoreFindBranch(cs, zBranch, &tip);
    if( rc!=SQLITE_OK || !prollyHashIsEmpty(&tip) ){
      return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
    }
  }
  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  nBranches = refsTableBranchCount(&cs->refs);
  if( nBranches>1 ){
    chunkStoreUnlock(cs);
    return SQLITE_OK;
  }
  if( nBranches>0 ){
    rc = chunkStoreFindBranch(cs, zBranch, &tip);
    if( rc!=SQLITE_OK || !prollyHashIsEmpty(&tip) ){
      chunkStoreUnlock(cs);
      return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
    }
  }

  memset(&empty, 0, sizeof(empty));
  rc = doltliteCreateAndStoreCommitOnStore(db, cs, &empty, &empty,
      "Initialize data repository", 0, 0, 0, 0, 0, pSeedHash);
  if( rc==SQLITE_OK ){
    if( nBranches==0 ){
      rc = chunkStoreAddBranch(cs, zBranch, pSeedHash);
      if( rc==SQLITE_OK ) rc = chunkStoreSetDefaultBranch(cs, zBranch);
    }else{
      rc = chunkStoreUpdateBranch(cs, zBranch, pSeedHash);
    }
  }
  if( rc==SQLITE_OK ) rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc==SQLITE_OK ){
    *pSeeded = 1;
  }else{
    chunkStoreRollback(cs);
  }
  chunkStoreUnlock(cs);
  return rc;
}

/* Advance the branch tip and persist. CAS leaves bSwitchBeforePersist
** clear so SwitchCatalog cannot drop lockDepth between confirm and commit. */
static int doltliteAdvanceBranchWithState(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash,
  const ProllyHash *pWorkingCatHash,
  DoltliteTxnState *pSaved,
  int bSwitchBeforePersist
){
  ChunkStore *cs;
  const char *branch;
  int rc;
  assert( db!=0 && pNewHead!=0 && pCatalogHash!=0 );
  cs = doltliteGetChunkStore(db);
  assert( cs!=0 );
  branch = doltliteGetSessionBranch(db);
  assert( branch!=0 && branch[0]!=0 );

  if( refsTableBranchCount(&cs->refs)==0 ){
    rc = chunkStoreAddBranch(cs, branch, pNewHead);
    if( rc==SQLITE_OK ){
      rc = chunkStoreSetDefaultBranch(cs, branch);
    }
  }else{
    rc = chunkStoreUpdateBranch(cs, branch, pNewHead);
  }
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, pSaved, rc);
  }

  doltliteSetSessionHead(db, pNewHead);
  rc = doltliteSetSessionStaged(db, pCatalogHash);
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, pSaved, rc);
  }
  if( bSwitchBeforePersist ){
    if( pWorkingCatHash && !prollyHashIsEmpty(pWorkingCatHash) ){
      rc = doltliteSwitchCatalog(db, pWorkingCatHash);
    }else{
      rc = doltliteSwitchCatalog(db, pCatalogHash);
    }
    if( rc!=SQLITE_OK ){
      return doltliteRestoreTxnStateOnFailure(db, pSaved, rc);
    }
  }

  /* A null working catalog used to flush the live session here, but the
  ** head confirm can have reloaded that session from a peer's freshly
  ** written working set: the persist then binds the peer's content to the
  ** new head and the staleness gate waves it through. Advancing with no
  ** explicit working catalog means the working set IS the new catalog. */
  rc = doltlitePersistWorkingSetWithHash(
      db, pWorkingCatHash ? pWorkingCatHash : pCatalogHash);
  if( rc!=SQLITE_OK ){
    return doltliteRestoreTxnStateOnFailure(db, pSaved, rc);
  }

  doltliteTxnStateClear(pSaved);
  return SQLITE_OK;
}

int doltliteAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash,
  const ProllyHash *pWorkingCatHash
){
  DoltliteTxnState saved;
  int rc;

  rc = doltliteSaveTxnState(db, &saved);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteAdvanceBranchWithState(
      db, pNewHead, pCatalogHash, pWorkingCatHash, &saved, 1);
}

int doltliteCompareAndAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pExpectedHead,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash,
  const ProllyHash *pWorkingCatHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteTxnState saved;
  ProllyHash diskTip;
  int found = 0;
  int rc;
  if( !cs ) return SQLITE_ERROR;
  rc = doltliteSaveTxnState(db, &saved);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteRefreshAndConfirmHead(db, cs, pExpectedHead);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(&saved);
    return rc;
  }
  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);

  /* Persist the tip under the confirm lock without SwitchCatalog; lock-cycling
  ** SQL between confirm and commit can let a peer land and then be clobbered. */
  rc = doltliteAdvanceBranchWithState(
      db, pNewHead, pCatalogHash, pWorkingCatHash, &saved, 0);
  if( rc==SQLITE_OK ){
    /* Durable tip must be ours before unlock. PersistWorkingSetWithHash can
    ** skip the commit when a conflicts catalog is present. */
    rc = chunkStoreReadDiskBranchTip(
        cs, doltliteGetSessionBranch(db), &diskTip, &found);
    if( rc==SQLITE_OK
     && (!found || prollyHashCompare(&diskTip, pNewHead)!=0) ){
      rc = SQLITE_BUSY;
    }
    if( rc!=SQLITE_OK ){
      /* AdvanceBranchWithState already cleared saved on success; cannot rebuild
      ** a restore here. BUSY so the caller retries on a fresh view. */
    }
  }
  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);
  chunkStoreUnlock(cs);

  if( rc==SQLITE_OK ){
    /* Adopt the catalog after the durable tip is on disk. Failure leaves HEAD
    ** advanced with a recoverable working-set mismatch on reopen. */
    if( pWorkingCatHash && !prollyHashIsEmpty(pWorkingCatHash) ){
      int src = doltliteSwitchCatalog(db, pWorkingCatHash);
      if( src!=SQLITE_OK ) rc = src;
    }else{
      int src = doltliteSwitchCatalog(db, pCatalogHash);
      if( src!=SQLITE_OK ) rc = src;
    }
  }
  return rc;
}

int doltlitePersistOrSaveWorkingSet(sqlite3 *db){
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    return doltlitePersistWorkingSet(db);
  }
  return doltliteSaveWorkingSet(db);
}

int doltliteDetectPostMergeConstraintViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  int *pnViolations
){
  return doltliteDetectConstraintViolationsFiltered(
      db, pAncCatHash, 0, 0, 1, pnViolations);
}

int doltliteDetectConstraintViolationsFiltered(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  const char **azTables,
  int nTables,
  int bPersist,
  int *pnViolations
){
  int nViolations = 0;
  int nUnique = 0;
  int nCheck = 0;
  int nNotNull = 0;
  int nStrict = 0;
  char *zDetectErrMsg = 0;
  int rc;
  sqlite3_stmt *pStmt = 0;
  int needsDetection = 0;

  rc = sqlite3_prepare_v2(db,
      "SELECT 1 "
      "FROM main.sqlite_master "
      "WHERE (type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%' "
      "       AND sql IS NOT NULL "
      "       AND (instr(upper(sql), 'REFERENCES')>0 "
      "            OR instr(upper(sql), 'CHECK')>0 "
      "            OR instr(upper(sql), 'UNIQUE')>0 "
      /* NOT NULL has a detector too; without this term a NOT-NULL-only table
      ** is invisible to dolt_verify_constraints. */
      "            OR instr(upper(sql), 'NOT NULL')>0 "
      /* STRICT tables have a type detector; the keyword is at the tail of CREATE. */
      "            OR instr(upper(sql), 'STRICT')>0)) "
      "   OR (type='index' AND sql IS NOT NULL "
      "       AND instr(upper(sql), 'CREATE UNIQUE INDEX')>0) "
      "LIMIT 1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    needsDetection = 1;
    rc = SQLITE_OK;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
  }
  {
    int finalizeRc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = finalizeRc;
  }
  if( rc!=SQLITE_OK ) return rc;
  if( !needsDetection ){
    if( pnViolations ) *pnViolations = 0;
    return SQLITE_OK;
  }

  rc = doltliteConstraintViolationBatchBegin(db);
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeFkViolations(db, pAncCatHash,
                                         &zDetectErrMsg, &nViolations,
                                         azTables, nTables);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeUniqueViolations(db, pAncCatHash,
                                             &zDetectErrMsg, &nUnique,
                                             azTables, nTables);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeCheckViolations(db, pAncCatHash,
                                            &zDetectErrMsg, &nCheck,
                                            azTables, nTables);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeNotNullViolations(db, pAncCatHash,
                                             &zDetectErrMsg, &nNotNull,
                                             azTables, nTables);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteDetectMergeStrictViolations(db, pAncCatHash,
                                            &zDetectErrMsg, &nStrict,
                                            azTables, nTables);
  }
  {
    int erc = doltliteConstraintViolationBatchEnd(
        db, rc==SQLITE_OK && bPersist);
    if( rc==SQLITE_OK ) rc = erc;
  }
  sqlite3_free(zDetectErrMsg);
  if( rc!=SQLITE_OK ) return rc;

  if( pnViolations ){
    *pnViolations = nViolations + nUnique + nCheck + nNotNull + nStrict;
  }
  return SQLITE_OK;
}

int doltliteSavepointIsTopLevelTxn(sqlite3 *db){
  assert( db!=0 );
  assert( db->nSavepoint>=0 );
  return db->pSavepoint!=0 && db->nSavepoint==0;
}

/* A top-level SAVEPOINT is the txn boundary; seal it like autocommit so
** later ROLLBACK TO cannot undo the VC op. */
int doltliteVcSealTopLevelSavepointTxn(sqlite3 *db){
  if( doltliteSavepointIsTopLevelTxn(db) ){
    return sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  return SQLITE_OK;
}

DoltliteVcTxnMode doltliteVcTxnMode(sqlite3 *db){
  if( db->autoCommit || doltliteSavepointIsTopLevelTxn(db) ){
    return DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE;
  }
  if( db->pSavepoint ){
    return DOLTLITE_VC_TXN_NESTED_SAVEPOINT;
  }
  return DOLTLITE_VC_TXN_PLAIN;
}

int doltliteVcSealActiveSavepoints(sqlite3 *db){
  int rc = SQLITE_OK;
  if( failNextVcSeal ){
    failNextVcSeal = 0;
    return SQLITE_ERROR;
  }
  while( rc==SQLITE_OK && db->pSavepoint ){
    char *zSql = sqlite3_mprintf("RELEASE SAVEPOINT \"%w\"", db->pSavepoint->zName);
    if( !zSql ) return SQLITE_NOMEM;
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  return rc;
}

/* End the enclosing SQL txn after a ref advance. Releasing savepoints
** alone leaves BEGIN open; ROLLBACK then splits HEAD from the data. */
int doltliteVcSealEnclosingTxn(sqlite3 *db){
  if( failNextVcSeal ){
    failNextVcSeal = 0;
    return SQLITE_ERROR;
  }
  if( !db->autoCommit
   || sqlite3_txn_state(db, "main")!=SQLITE_TXN_NONE
   || db->pSavepoint ){
    return sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  return SQLITE_OK;
}

int doltliteVcSealSavepointError(sqlite3 *db){
  if( db->pSavepoint ){
    return doltliteVcSealActiveSavepoints(db);
  }
  return SQLITE_OK;
}

void doltliteVcResultError(sqlite3_context *ctx, sqlite3 *db, const char *zMsg){
  (void)doltliteVcSealSavepointError(db);
  sqlite3_result_error(ctx, zMsg, -1);
}

int doltliteVcSealBranchStyleTxn(sqlite3 *db){
  int rc;
  if( db->autoCommit ) return SQLITE_OK;
  if( db->pSavepoint ){
    rc = doltliteVcSealActiveSavepoints(db);
    if( rc!=SQLITE_OK ) return rc;
    /* Releasing savepoints leaves an enclosing BEGIN open; ROLLBACK would
    ** revert the working set while the ref already names the new branch. */
    if( db->autoCommit ) return SQLITE_OK;
  }
  rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  return sqlite3_exec(db, "BEGIN", 0, 0, 0);
}

int doltlitePrimeSchemaCache(sqlite3 *db){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(
      db, "SELECT name FROM main.sqlite_master LIMIT 1", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){}
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pStmt);
  return rc;
}

#endif
