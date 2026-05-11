
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"

#include "doltlite_internal.h"
#include <string.h>

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
  ProllyHash curHash;
  char zCurHex[PROLLY_HASH_SIZE*2+1];
  DoltliteCommit curCommit;
  int curParents;
  int curParentIdx;
  int hasRow;
  i64 iRowid;
};

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

  rc = sqlite3_declare_vtab(db, commitAncestorsSchema);
  if( rc!=SQLITE_OK ) return rc;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int caDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int caOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  CommitAncestorsCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
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
  pCur->iRowid = 0;
}

static int caClose(sqlite3_vtab_cursor *pCursor){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  caCursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

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
    if( rc!=SQLITE_OK ) return rc;

    pCur->curHash = cur;
    doltliteHashToHex(&cur, pCur->zCurHex);
    pCur->hasRow = 1;
    pCur->curParents = doltliteCommitParentCount(&pCur->curCommit);
    if( pCur->curParents == 0 ) pCur->curParents = 1;
    pCur->curParentIdx = 0;

    for(i = 0; i < doltliteCommitParentCount(&pCur->curCommit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&pCur->curCommit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( pCur->qTail >= pCur->qAlloc ){
        int nNew = pCur->qAlloc ? pCur->qAlloc*2 : 16;
        ProllyHash *tmp = sqlite3_realloc(pCur->aQueue,
                                           nNew*(int)sizeof(ProllyHash));
        if( !tmp ) return SQLITE_NOMEM;
        pCur->aQueue = tmp;
        pCur->qAlloc = nNew;
      }
      pCur->aQueue[pCur->qTail++] = *pParent;
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int caNext(sqlite3_vtab_cursor *pCursor){
  CommitAncestorsCursor *pCur = (CommitAncestorsCursor*)pCursor;
  CommitAncestorsVtab *pVtab = (CommitAncestorsVtab*)pCursor->pVtab;
  pCur->iRowid++;
  pCur->curParentIdx++;
  if( pCur->curParentIdx >= pCur->curParents ){
    return caLoadNextCommit(pCur, pVtab->db);
  }
  return SQLITE_OK;
}

static int caEnqueue(CommitAncestorsCursor *pCur, const ProllyHash *p){
  if( prollyHashIsEmpty(p) ) return SQLITE_OK;
  if( pCur->qTail >= pCur->qAlloc ){
    int nNew = pCur->qAlloc ? pCur->qAlloc*2 : 16;
    ProllyHash *tmp = sqlite3_realloc(pCur->aQueue,
                                       nNew*(int)sizeof(ProllyHash));
    if( !tmp ) return SQLITE_NOMEM;
    pCur->aQueue = tmp;
    pCur->qAlloc = nNew;
  }
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
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  caCursorReset(pCur);

  cs = doltliteGetChunkStore(pVtab->db);
  if( !cs ) return SQLITE_OK;

  rc = prollyHashSetInit(&pCur->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  pCur->visitedInit = 1;

  doltliteGetSessionHead(pVtab->db, &head);
  rc = caEnqueue(pCur, &head);
  if( rc!=SQLITE_OK ) return rc;

  for( i = 0; i < cs->nBranches; i++ ){
    rc = caEnqueue(pCur, &cs->aBranches[i].commitHash);
    if( rc!=SQLITE_OK ) return rc;
  }
  for( i = 0; i < cs->nTags; i++ ){
    rc = caEnqueue(pCur, &cs->aTags[i].commitHash);
    if( rc!=SQLITE_OK ) return rc;
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
  *pRowid = ((CommitAncestorsCursor*)pCursor)->iRowid;
  return SQLITE_OK;
}

static int caBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 1000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

static sqlite3_module commitAncestorsModule = {
  0, 0,
  caConnect, caBestIndex, caDisconnect, 0,
  caOpen, caClose, caFilter, caNext,
  caEof, caColumn, caRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteCommitAncestorsRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_commit_ancestors",
                                &commitAncestorsModule, 0);
}

#endif
