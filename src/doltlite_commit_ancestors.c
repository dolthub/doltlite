
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"

#include "doltlite_internal.h"
#include <string.h>

#define CA_IDX_COMMIT_EQ 0x01

typedef struct CommitAncestorsVtab CommitAncestorsVtab;
struct CommitAncestorsVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct CommitAncestorsCursor CommitAncestorsCursor;
struct CommitAncestorsCursor {
  sqlite3_vtab_cursor base;
  ProllyHash *aQueue;
  int qHead, qTail, qAlloc;
  ProllyHashSet visited;
  int visitedInit;
  char zCurHex[PROLLY_HASH_SIZE*2+1];
  DoltliteCommit curCommit;
  int curParents;
  int curParentIdx;
  int hasRow;
  int singleCommit;
};

static int caMapChunkSourceError(
  CommitAncestorsCursor *pCur,
  sqlite3 *db,
  int sourceRc,
  int mappedRc
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int pendingRc = SQLITE_OK;
  char *zErr = cs ? chunkStoreSourceTakeError(cs, &pendingRc) : 0;
  if( !zErr && pendingRc==SQLITE_OK ) return mappedRc;
  if( zErr ){
    sqlite3_free(pCur->base.pVtab->zErrMsg);
    pCur->base.pVtab->zErrMsg = zErr;
  }
  return pendingRc!=SQLITE_OK ? pendingRc : sourceRc;
}

static const char *commitAncestorsSchema =
  "CREATE TABLE x("
  "  commit_hash TEXT,"
  "  parent_hash TEXT,"
  "  parent_index INTEGER"
  ")";

static int caConnect(
  sqlite3 *db, void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab, char **pzErr
){
  CommitAncestorsVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;

  rc = doltliteVtabConnectSimple(db, commitAncestorsSchema,
                                 sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (CommitAncestorsVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int caOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(CommitAncestorsCursor));
}

static void caCursorReset(CommitAncestorsCursor *pCur){
  doltliteCommitClear(&pCur->curCommit);
  memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
  sqlite3_free(pCur->aQueue);
  pCur->aQueue = 0;
  pCur->qHead = pCur->qTail = pCur->qAlloc = 0;
  if( pCur->visitedInit ){
    prollyHashSetFree(&pCur->visited);
    pCur->visitedInit = 0;
  }
  pCur->hasRow = 0;
  pCur->curParents = 0;
  pCur->curParentIdx = 0;
  pCur->singleCommit = 0;
}

static int caClose(sqlite3_vtab_cursor *pCursor){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  caCursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int caEnqueue(CommitAncestorsCursor *pCur, const ProllyHash *p);

static int caLoadNextCommit(CommitAncestorsCursor *pCur, sqlite3 *db){
  int i, rc;

  doltliteCommitClear(&pCur->curCommit);
  memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
  pCur->hasRow = 0;
  pCur->curParents = 0;
  pCur->curParentIdx = 0;

  while( pCur->qHead < pCur->qTail ){
    ProllyHash cur = pCur->aQueue[pCur->qHead++];

    if( prollyHashSetContains(&pCur->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&pCur->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;

    rc = doltliteLoadCommit(db, &cur, &pCur->curCommit);
    if( rc!=SQLITE_OK ){
      if( pCur->singleCommit ){
        rc = caMapChunkSourceError(pCur, db, rc, SQLITE_OK);
        doltliteCommitClear(&pCur->curCommit);
        memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
        return rc;
      }
      return rc;
    }

    doltliteHashToHex(&cur, pCur->zCurHex);
    pCur->hasRow = 1;
    pCur->curParents = doltliteCommitParentCount(&pCur->curCommit);
    if( pCur->curParents == 0 ) pCur->curParents = 1;
    pCur->curParentIdx = 0;

    if( !pCur->singleCommit ){
      for(i = 0; i < doltliteCommitParentCount(&pCur->curCommit); i++){
        const ProllyHash *pParent;
        pParent = doltliteCommitParentHash(&pCur->curCommit, i);
        if( pParent ){
          rc = caEnqueue(pCur, pParent);
          if( rc!=SQLITE_OK ) return rc;
        }
      }
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int caNext(sqlite3_vtab_cursor *pCursor){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  CommitAncestorsVtab *pVtab = (CommitAncestorsVtab*)pCursor->pVtab;
  pCur->curParentIdx++;
  if( pCur->curParentIdx >= pCur->curParents ){
    return caLoadNextCommit(pCur, pVtab->db);
  }
  return SQLITE_OK;
}

static int caEnqueue(CommitAncestorsCursor *pCur, const ProllyHash *p){
  int rc;
  if( prollyHashIsEmpty(p) ) return SQLITE_OK;
  rc = DOLTLITE_GROW_ARRAY(&pCur->aQueue, &pCur->qAlloc, pCur->qTail+1, 16);
  if( rc!=SQLITE_OK ) return rc;
  pCur->aQueue[pCur->qTail++] = *p;
  return SQLITE_OK;
}

static int caFilter(
  sqlite3_vtab_cursor *pCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  CommitAncestorsVtab *pVtab = (CommitAncestorsVtab*)pCursor->pVtab;
  ChunkStore *cs;
  ProllyHash head;
  int rc;
  int i;
  (void)idxStr;

  caCursorReset(pCur);

  if( idxNum & CA_IDX_COMMIT_EQ ){
    const char *z;
    if( argc<=0 ) return SQLITE_OK;
    z = (const char*)sqlite3_value_text(argv[0]);
    if( !z || doltliteHexToHash(z, &head)!=SQLITE_OK
        || prollyHashIsEmpty(&head) ){
      return SQLITE_OK;
    }
    pCur->singleCommit = 1;
  }

  cs = doltliteGetChunkStore(pVtab->db);
  if( !cs ) return SQLITE_OK;

  rc = prollyHashSetInit(&pCur->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  pCur->visitedInit = 1;

  if( !pCur->singleCommit ){
    doltliteGetSessionHead(pVtab->db, &head);
  }

  rc = caEnqueue(pCur, &head);
  if( rc!=SQLITE_OK ) return rc;

  if( !pCur->singleCommit ){
    int nBr; const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    for( i = 0; i < nBr; i++ ){
      rc = caEnqueue(pCur, &aBr[i].commitHash);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  if( !pCur->singleCommit ){
    int nTg; const TagRef *aTg;
    refsTableGetTags(&cs->refs, &nTg, &aTg);
    for( i = 0; i < nTg; i++ ){
      rc = caEnqueue(pCur, &aTg[i].commitHash);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  if( pCur->qTail == 0 ) return SQLITE_OK;
  return caLoadNextCommit(pCur, pVtab->db);
}

static int caEof(sqlite3_vtab_cursor *pCursor){
  return !((CommitAncestorsCursor*)pCursor)->hasRow;
}

static int caColumn(
  sqlite3_vtab_cursor *pCursor,
  sqlite3_context *ctx,
  int iCol
){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  DoltliteCommit *c;
  int idx;

  if( !pCur->hasRow ) return SQLITE_OK;
  c = &pCur->curCommit;
  idx = pCur->curParentIdx;

  switch( iCol ){
    case 0:
      sqlite3_result_text(ctx, pCur->zCurHex, -1, SQLITE_TRANSIENT);
      break;
    case 1: {
      int realParents = doltliteCommitParentCount(c);
      if( realParents == 0 ){
        sqlite3_result_null(ctx);
      }else if( idx < realParents ){
        const ProllyHash *pParent = doltliteCommitParentHash(c, idx);
        if( !pParent || prollyHashIsEmpty(pParent) ){
          sqlite3_result_null(ctx);
        }else{
          char zHex[PROLLY_HASH_SIZE*2+1];
          doltliteHashToHex(pParent, zHex);
          sqlite3_result_text(ctx, zHex, -1, SQLITE_TRANSIENT);
        }
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 2:
      sqlite3_result_int(ctx, idx);
      break;
  }
  return SQLITE_OK;
}

static int caRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  u64 h = doltliteFnv1aStr(DOLTLITE_FNV1A_OFFSET, pCur->zCurHex);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aI64(h, pCur->curParentIdx);
  *pRowid = doltliteFnv1aRowid(h);
  return SQLITE_OK;
}

static int caBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i;
  int iCommitEq = -1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->iColumn==0 && pC->op==SQLITE_INDEX_CONSTRAINT_EQ
     && doltliteVtabConstraintIsBinary(pInfo, i) ){
      iCommitEq = i;
      break;
    }
  }

  if( iCommitEq>=0 ){
    pInfo->aConstraintUsage[iCommitEq].argvIndex = 1;
    pInfo->aConstraintUsage[iCommitEq].omit = 0;
    pInfo->idxNum = CA_IDX_COMMIT_EQ;
    pInfo->estimatedCost = 10.0;
    pInfo->estimatedRows = 2;
  }else{
    pInfo->idxNum = 0;
    pInfo->estimatedCost = 1000.0;
    pInfo->estimatedRows = 100;
  }
  return SQLITE_OK;
}

static sqlite3_module commitAncestorsModule = {
  0, 0,
  caConnect, caBestIndex, doltliteVtabDisconnect, 0,
  caOpen, caClose, caFilter, caNext,
  caEof, caColumn, caRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteCommitAncestorsRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_commit_ancestors",
                                &commitAncestorsModule, 0);
}

#endif
