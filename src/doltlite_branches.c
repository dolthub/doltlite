#ifdef DOLTLITE_PROLLY

#include "doltlite_branch_int.h"
#include "doltlite_vtab_util.h"
#include "doltlite_commit.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "doltlite_internal.h"
#include <string.h>


typedef struct BrVtab BrVtab;
struct BrVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct BrCur BrCur;
struct BrCur {
  sqlite3_vtab_cursor base;
  int iRow;
  /* Cursor-owned snapshot at xFilter. Same-statement VC functions may
  ** reallocate cs->refs; commit metadata is immutable and loaded by hash. */
  int nRows;
  BranchRef *aSnap;
  int iCommitRow;
  DoltliteCommit commit;
};

static void brClearCommit(BrCur *c){
  doltliteCommitClear(&c->commit);
  c->iCommitRow = -1;
}

static void brCurClearSnapshot(BrCur *c){
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aSnap[i].zName);
  }
  sqlite3_free(c->aSnap);
  c->aSnap = 0;
  c->nRows = 0;
  c->iRow = 0;
}

static int brCurSnapshot(BrCur *c, const BranchRef *aBr, int nBr){
  int i;
  brCurClearSnapshot(c);
  if( nBr<=0 ) return SQLITE_OK;
  c->aSnap = (BranchRef*)sqlite3_malloc64(
      (sqlite3_uint64)nBr*sizeof(BranchRef));
  if( !c->aSnap ) return SQLITE_NOMEM;
  memset(c->aSnap, 0, (size_t)nBr*sizeof(BranchRef));
  for(i=0; i<nBr; i++){
    c->nRows = i+1;
    c->aSnap[i] = aBr[i];
    c->aSnap[i].zName = sqlite3_mprintf("%s", aBr[i].zName ? aBr[i].zName : "");
    if( !c->aSnap[i].zName ){
      brCurClearSnapshot(c);
      return SQLITE_NOMEM;
    }
  }
  return SQLITE_OK;
}

static int brConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  BrVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db,
    "CREATE TABLE x("
      "name TEXT, "
      "hash TEXT, "
      "latest_committer TEXT, "
      "latest_committer_email TEXT, "
      "latest_commit_date TEXT, "
      "latest_commit_message TEXT, "
      "remote TEXT, "
      "branch TEXT, "
      "dirty INTEGER"
    ")",
    sizeof(*p), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  p = (BrVtab*)*ppVtab;
  p->db = db;
  return SQLITE_OK;
}
static int brOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  int rc;
  BrCur *c;
  (void)v;
  rc = doltliteVtabOpenCursor(pp, sizeof(BrCur));
  if( rc!=SQLITE_OK ) return rc;
  c = (BrCur*)*pp;
  c->iCommitRow = -1;
  return SQLITE_OK;
}
static int brClose(sqlite3_vtab_cursor *cur){
  BrCur *c = (BrCur*)cur;
  brClearCommit(c);
  brCurClearSnapshot(c);
  sqlite3_free(c);
  return SQLITE_OK;
}
static int brFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  BrVtab *pVtab = (BrVtab*)c->pVtab;
  BrCur *pCur = (BrCur*)c;
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  int nBr = 0;
  const BranchRef *aBr = 0;
  (void)s;
  (void)a;
  brClearCommit(pCur);
  brCurClearSnapshot(pCur);
  if( !cs ) return SQLITE_OK;
  refsTableGetBranches(&cs->refs, &nBr, &aBr);
  if( n==1 ){
    const char *zName = (const char*)sqlite3_value_text(v[0]);
    int i = csFindNamedRef(cs->refs.aBranches, cs->refs.nBranches,
                           (int)sizeof(BranchRef), zName);
    if( i<0 ) return SQLITE_OK;
    aBr += i;
    nBr = 1;
  }
  return brCurSnapshot(pCur, aBr, nBr);
}
static int brNext(sqlite3_vtab_cursor *c){
  brClearCommit((BrCur*)c);
  ((BrCur*)c)->iRow++;
  return SQLITE_OK;
}
static int brEof(sqlite3_vtab_cursor *c){
  BrCur *pCur = (BrCur*)c;
  return pCur->iRow >= pCur->nRows;
}

static int brIsDirty(
  sqlite3 *db,
  ChunkStore *cs,
  const BranchRef *br,
  int *pDirty
){
  ProllyHash workingCat;
  ProllyHash stagedCat;
  ProllyHash commitCat;
  u8 *wsData = 0;
  int nWsData = 0;
  int rc;

  *pDirty = 0;

  if( strcmp(br->zName, doltliteGetSessionBranch(db))==0 ){
    return doltliteHasUncommittedChanges(db, pDirty);
  }
  if( prollyHashIsEmpty(&br->workingSetHash) ){
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, &br->workingSetHash, &wsData, &nWsData);
  if( rc==SQLITE_OK ){
    rc = chunkStoreValidateWorkingSetBlob(wsData, nWsData);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(wsData);
    return rc;
  }
  memcpy(workingCat.data, wsData + WS_WORKING_CAT_OFF, PROLLY_HASH_SIZE);
  memcpy(stagedCat.data, wsData + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  sqlite3_free(wsData);

  if( prollyHashIsEmpty(&br->commitHash) ){
    *pDirty = !prollyHashIsEmpty(&workingCat)
           || !prollyHashIsEmpty(&stagedCat);
    return SQLITE_OK;
  }
  rc = doltliteCommitCatalogHash(db, &br->commitHash, &commitCat);
  if( rc==SQLITE_OK ){
    *pDirty = (!prollyHashIsEmpty(&workingCat)
            && prollyHashCompare(&workingCat, &commitCat)!=0)
           || (!prollyHashIsEmpty(&stagedCat)
            && prollyHashCompare(&stagedCat, &commitCat)!=0);
  }
  return rc;
}

static int brLoadCommit(
  BrVtab *v,
  BrCur *c,
  const BranchRef *br,
  DoltliteCommit **ppCommit
){
  int rc;
  if( c->iCommitRow==c->iRow ){
    *ppCommit = &c->commit;
    return SQLITE_OK;
  }
  brClearCommit(c);
  rc = doltliteLoadCommit(v->db, &br->commitHash, &c->commit);
  if( rc!=SQLITE_OK ){
    brClearCommit(c);
    return rc;
  }
  c->iCommitRow = c->iRow;
  *ppCommit = &c->commit;
  return SQLITE_OK;
}

static int brColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  BrVtab *v = (BrVtab*)c->pVtab;
  BrCur *pCur = (BrCur*)c;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  const BranchRef *br;
  if(!cs) return SQLITE_OK;
  if( pCur->iRow >= pCur->nRows ) return SQLITE_OK;
  br = &pCur->aSnap[pCur->iRow];

  switch(col){
    case 0:
      sqlite3_result_text(ctx, br->zName, -1, SQLITE_TRANSIENT);
      return SQLITE_OK;
    case 1: {
      char h[PROLLY_HASH_SIZE*2+1];
      doltliteHashToHex(&br->commitHash, h);
      sqlite3_result_text(ctx, h, -1, SQLITE_TRANSIENT);
      return SQLITE_OK;
    }
    case 6: case 7:

      sqlite3_result_text(ctx, "", -1, SQLITE_STATIC);
      return SQLITE_OK;
    case 8: {
      int dirty = 0;
      int rc = brIsDirty(v->db, cs, br, &dirty);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return rc;
      }
      sqlite3_result_int(ctx, dirty);
      return SQLITE_OK;
    }
  }

  {
    DoltliteCommit *cm;
    int rc;
    /* Unborn branch has an all-zero head; there is no commit to describe. */
    if( prollyHashIsEmpty(&br->commitHash) ){
      sqlite3_result_null(ctx);
      return SQLITE_OK;
    }
    rc = brLoadCommit(v, pCur, br, &cm);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return rc;
    }
    switch(col){
      case 2:
        sqlite3_result_text(ctx, cm->zName ? cm->zName : "",
                            -1, SQLITE_TRANSIENT);
        break;
      case 3:
        sqlite3_result_text(ctx, cm->zEmail ? cm->zEmail : "",
                            -1, SQLITE_TRANSIENT);
        break;
      case 4: {
        doltliteResultTimestamp(ctx, cm->timestamp);
        break;
      }
      case 5:
        sqlite3_result_text(ctx, cm->zMessage ? cm->zMessage : "",
                            -1, SQLITE_TRANSIENT);
        break;
    }
  }
  return SQLITE_OK;
}
static int brRowid(sqlite3_vtab_cursor *c, sqlite3_int64 *r){
  *r=((BrCur*)c)->iRow; return SQLITE_OK;
}
static int brBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost=10;
  p->estimatedRows=5;
  return doltliteBestIndexEq(p, 0);
}
sqlite3_module doltliteBranchesModule = {
  0,0,brConnect,brBestIndex,doltliteVtabDisconnect,0,
  brOpen,brClose,brFilter,brNext,brEof,brColumn,brRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

static int rbConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  BrVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db,
    "CREATE TABLE x("
      "name TEXT, "
      "hash TEXT, "
      "latest_committer TEXT, "
      "latest_committer_email TEXT, "
      "latest_commit_date TEXT, "
      "latest_commit_message TEXT"
    ")",
    sizeof(*p), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  p = (BrVtab*)*ppVtab;
  p->db = db;
  return SQLITE_OK;
}

/* Snapshot name+hash so dolt_remote_branches can share dolt_branches machinery. */
static int rbFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  BrVtab *pVtab = (BrVtab*)c->pVtab;
  BrCur *pCur = (BrCur*)c;
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  int nTr = 0;
  const TrackingBranch *aTr = 0;
  const char *zName = 0;
  int i;
  (void)s;
  (void)a;
  brClearCommit(pCur);
  brCurClearSnapshot(pCur);
  if( !cs ) return SQLITE_OK;
  refsTableGetTracking(&cs->refs, &nTr, &aTr);
  if( n==1 ){
    zName = (const char*)sqlite3_value_text(v[0]);
    if( !zName ) return SQLITE_OK;
  }
  if( nTr<=0 ) return SQLITE_OK;
  pCur->aSnap = (BranchRef*)sqlite3_malloc64(
      (sqlite3_uint64)nTr*sizeof(BranchRef));
  if( !pCur->aSnap ) return SQLITE_NOMEM;
  memset(pCur->aSnap, 0, (size_t)nTr*sizeof(BranchRef));
  for(i=0; i<nTr; i++){
    char *z = sqlite3_mprintf("remotes/%s/%s", aTr[i].zRemote, aTr[i].zBranch);
    if( !z ){
      brCurClearSnapshot(pCur);
      return SQLITE_NOMEM;
    }
    if( zName && strcmp(z, zName)!=0 ){
      sqlite3_free(z);
      continue;
    }
    pCur->aSnap[pCur->nRows].zName = z;
    pCur->aSnap[pCur->nRows].commitHash = aTr[i].commitHash;
    pCur->nRows++;
  }
  return SQLITE_OK;
}

sqlite3_module doltliteRemoteBranchesModule = {
  0,0,rbConnect,brBestIndex,doltliteVtabDisconnect,0,
  brOpen,brClose,rbFilter,brNext,brEof,brColumn,brRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};


#endif
