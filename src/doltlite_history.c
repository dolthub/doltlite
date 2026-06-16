
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "prolly_hashset.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

static char *htBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, 0, 0)!=SQLITE_OK ){
    sqlite3_str_reset(pStr);
    return 0;
  }
  sqlite3_str_appendall(pStr, ", commit_hash TEXT, committer TEXT, commit_date TEXT)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct HistVtab HistVtab;
struct HistVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

#define HIST_IDX_PK_EQ 0x01
#define HIST_IDX_PK_GE 0x02
#define HIST_IDX_PK_LE 0x04
#define HIST_IDX_PK_GT 0x08
#define HIST_IDX_PK_LT 0x10
#define HIST_IDX_PK_ANY (HIST_IDX_PK_EQ|HIST_IDX_PK_GE|HIST_IDX_PK_LE|HIST_IDX_PK_GT|HIST_IDX_PK_LT)

typedef struct HistCursor HistCursor;
struct HistCursor {
  DoltliteVtabCursorCommon common;
  ProllyHash *aQueue;
  int qHead, qTail, qAlloc;
  ProllyHashSet visited, queued;
  int visitedInit, queuedInit;
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  i64 commitDate;
  int idxNum;
  i64 pkLo;
  i64 pkHi;
  int hasPkLo;
  int hasPkHi;
  int pkLoStrict;
  int pkHiStrict;
};

static void htCursorReset(HistCursor *c){
  doltliteVtabCommonReset(&c->common);
  sqlite3_free(c->zCommitter);
  c->zCommitter = 0;
  sqlite3_free(c->aQueue);
  c->aQueue = 0;
  c->qHead = c->qTail = c->qAlloc = 0;
  if( c->visitedInit ){
    prollyHashSetFree(&c->visited);
    c->visitedInit = 0;
  }
  if( c->queuedInit ){
    prollyHashSetFree(&c->queued);
    c->queuedInit = 0;
  }
}

static int htRowMatchesUpper(HistCursor *c){
  i64 k;
  if( !c->hasPkHi ) return 1;
  k = prollyCursorIntKey(&c->common.tblCur);
  if( c->pkHiStrict ){
    return k < c->pkHi;
  }
  return k <= c->pkHi;
}

static int htOpenTableAtCommit(HistCursor *c, sqlite3 *db,
    const char *zTableName, const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DoltliteCommit commit;
  struct TableEntry *aT = 0; int nT = 0;
  ProllyHash tableRoot; u8 flags = 0;
  int rc, res;
  int seekable;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, pCommitHash, &commit);
  if( rc!=SQLITE_OK ) return rc;

  doltliteHashToHex(pCommitHash, c->zCommitHex);
  sqlite3_free(c->zCommitter);
  c->zCommitter = sqlite3_mprintf("%s", commit.zName ? commit.zName : "");
  c->commitDate = commit.timestamp;
  if( !c->zCommitter ){
    doltliteCommitClear(&commit);
    return SQLITE_NOMEM;
  }

  {
    int i;
    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( prollyHashSetContains(&c->visited, pParent) ) continue;
      if( prollyHashSetContains(&c->queued, pParent) ) continue;
      if( c->qTail >= c->qAlloc ){
        i64 nNew = c->qAlloc ? (i64)c->qAlloc * 2 : (i64)16;
        ProllyHash *tmp;
        if( nNew > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ){
          doltliteCommitClear(&commit);
          return SQLITE_NOMEM;
        }
        tmp = sqlite3_realloc(c->aQueue,
                              (int)(nNew * (i64)sizeof(ProllyHash)));
        if( !tmp ){
          doltliteCommitClear(&commit);
          return SQLITE_NOMEM;
        }
        c->aQueue = tmp; c->qAlloc = (int)nNew;
      }
      c->aQueue[c->qTail++] = *pParent;
      rc = prollyHashSetAdd(&c->queued, pParent);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }
    }
  }

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aT, &nT, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;

  if( doltliteFindTableRootByName(aT, nT, zTableName, &tableRoot, &flags)
      !=SQLITE_OK || prollyHashIsEmpty(&tableRoot) ){
    doltliteFreeCatalog(aT, nT);
    return SQLITE_OK;
  }
  doltliteFreeCatalog(aT, nT);

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);

  seekable = (flags & PROLLY_NODE_INTKEY) != 0
          && (c->idxNum & HIST_IDX_PK_ANY) != 0;

  if( seekable && (c->idxNum & HIST_IDX_PK_EQ) ){
    rc = prollyCursorSeekInt(&c->common.tblCur, c->pkLo, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res!=0 || !prollyCursorIsValid(&c->common.tblCur) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return SQLITE_OK;
  }

  if( seekable && c->hasPkLo ){
    i64 startKey = c->pkLo;
    if( c->pkLoStrict ) startKey++;
    rc = prollyCursorSeekInt(&c->common.tblCur, startKey, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res<0 ){
      rc = prollyCursorNext(&c->common.tblCur);
      if( rc!=SQLITE_OK ){
        prollyCursorClose(&c->common.tblCur);
        return rc;
      }
    }
    if( !prollyCursorIsValid(&c->common.tblCur) || !htRowMatchesUpper(c) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return SQLITE_OK;
  }

  rc = prollyCursorFirst(&c->common.tblCur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->common.tblCur);
    return rc;
  }
  if( res ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  if( seekable && c->hasPkHi && !htRowMatchesUpper(c) ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  c->common.tblCurOpen = 1;
  return SQLITE_OK;
}

static int htAdvance(HistCursor *c, sqlite3 *db, const char *zTableName){
  int rc;

  if( c->common.tblCurOpen ){
    if( c->idxNum & HIST_IDX_PK_EQ ){
      prollyCursorClose(&c->common.tblCur);
      c->common.tblCurOpen = 0;
    }else{
      rc = prollyCursorNext(&c->common.tblCur);
      if( rc!=SQLITE_OK ){
        prollyCursorClose(&c->common.tblCur);
        c->common.tblCurOpen = 0;
        return rc;
      }
      if( prollyCursorIsValid(&c->common.tblCur) && htRowMatchesUpper(c) ){
        return doltliteVtabCommonCaptureRow(&c->common);
      }
      prollyCursorClose(&c->common.tblCur);
      c->common.tblCurOpen = 0;
    }
  }

  while( c->qHead < c->qTail ){
    ProllyHash cur = c->aQueue[c->qHead++];

    if( prollyHashSetContains(&c->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&c->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;

    rc = htOpenTableAtCommit(c, db, zTableName, &cur);
    if( rc!=SQLITE_OK ) return rc;

    if( c->common.tblCurOpen ){
      return doltliteVtabCommonCaptureRow(&c->common);
    }
  }

  c->common.hasRow = 0;
  return SQLITE_OK;
}

static int htConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectUserTable(db, argc, argv, "dolt_history_",
                                      sizeof(HistVtab), htBuildSchema,
                                      ppVtab, pzErr);
}

static int htBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  DoltliteVtabCommon *vt = (DoltliteVtabCommon*)v;
  int iEq = -1, iGe = -1, iLe = -1, iGt = -1, iLt = -1;
  int i, nArg = 0, idxNum = 0;
  int iPk = vt->cols.iPkCol;

  p->estimatedCost = 100000.0;
  p->estimatedRows = 100000;

  if( iPk < 0 ){
    p->idxNum = 0;
    return SQLITE_OK;
  }

  for(i=0; i<p->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &p->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->iColumn != iPk ) continue;
    switch( pC->op ){
      case SQLITE_INDEX_CONSTRAINT_EQ: if( iEq<0 ) iEq = i; break;
      case SQLITE_INDEX_CONSTRAINT_GE: if( iGe<0 ) iGe = i; break;
      case SQLITE_INDEX_CONSTRAINT_LE: if( iLe<0 ) iLe = i; break;
      case SQLITE_INDEX_CONSTRAINT_GT: if( iGt<0 ) iGt = i; break;
      case SQLITE_INDEX_CONSTRAINT_LT: if( iLt<0 ) iLt = i; break;
      default: break;
    }
  }

  if( iEq >= 0 ){
    p->aConstraintUsage[iEq].argvIndex = ++nArg;
    p->aConstraintUsage[iEq].omit = 1;
    idxNum |= HIST_IDX_PK_EQ;
    p->estimatedCost = 100.0;
    p->estimatedRows = 100;
  }else{
    if( iGe >= 0 ){
      p->aConstraintUsage[iGe].argvIndex = ++nArg;
      p->aConstraintUsage[iGe].omit = 1;
      idxNum |= HIST_IDX_PK_GE;
    }
    if( iGt >= 0 ){
      p->aConstraintUsage[iGt].argvIndex = ++nArg;
      p->aConstraintUsage[iGt].omit = 1;
      idxNum |= HIST_IDX_PK_GT;
    }
    if( iLe >= 0 ){
      p->aConstraintUsage[iLe].argvIndex = ++nArg;
      p->aConstraintUsage[iLe].omit = 1;
      idxNum |= HIST_IDX_PK_LE;
    }
    if( iLt >= 0 ){
      p->aConstraintUsage[iLt].argvIndex = ++nArg;
      p->aConstraintUsage[iLt].omit = 1;
      idxNum |= HIST_IDX_PK_LT;
    }
    if( idxNum != 0 ){
      p->estimatedCost = 1000.0;
      p->estimatedRows = 1000;
    }
  }

  p->idxNum = idxNum;
  return SQLITE_OK;
}

static int htOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  return doltliteVtabOpenCursor(pp, sizeof(HistCursor));
}

static int htClose(sqlite3_vtab_cursor *cur){
  HistCursor *c=(HistCursor*)cur;
  htCursorReset(c); sqlite3_free(c); return SQLITE_OK;
}

static int htFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  HistCursor *c=(HistCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  ProllyHash head;
  ChunkStore *cs;
  int rc;
  int iArg = 0;
  (void)idxStr;(void)argc;

  htCursorReset(c);

  c->idxNum = idxNum;
  c->hasPkLo = c->hasPkHi = 0;
  c->pkLoStrict = c->pkHiStrict = 0;
  c->pkLo = c->pkHi = 0;

  if( idxNum & HIST_IDX_PK_EQ ){
    if( iArg < argc ){
      c->pkLo = sqlite3_value_int64(argv[iArg++]);
      c->hasPkLo = 1;
    }
  }else{
    if( idxNum & HIST_IDX_PK_GE ){
      if( iArg < argc ){
        c->pkLo = sqlite3_value_int64(argv[iArg++]);
        c->hasPkLo = 1;
        c->pkLoStrict = 0;
      }
    }
    if( idxNum & HIST_IDX_PK_GT ){
      if( iArg < argc ){
        i64 v2 = sqlite3_value_int64(argv[iArg++]);
        if( !c->hasPkLo || v2 >= c->pkLo ){
          c->pkLo = v2;
          c->hasPkLo = 1;
          c->pkLoStrict = 1;
        }
      }
    }
    if( idxNum & HIST_IDX_PK_LE ){
      if( iArg < argc ){
        c->pkHi = sqlite3_value_int64(argv[iArg++]);
        c->hasPkHi = 1;
        c->pkHiStrict = 0;
      }
    }
    if( idxNum & HIST_IDX_PK_LT ){
      if( iArg < argc ){
        i64 v2 = sqlite3_value_int64(argv[iArg++]);
        if( !c->hasPkHi || v2 <= c->pkHi ){
          c->pkHi = v2;
          c->hasPkHi = 1;
          c->pkHiStrict = 1;
        }
      }
    }
  }

  cs = doltliteGetChunkStore(v->db);
  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(v->db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;

  rc = prollyHashSetInit(&c->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  c->visitedInit = 1;

  rc = prollyHashSetInit(&c->queued, 64);
  if( rc!=SQLITE_OK ) return rc;
  c->queuedInit = 1;

  c->qAlloc = 16;
  c->aQueue = sqlite3_malloc(c->qAlloc * (int)sizeof(ProllyHash));
  if( !c->aQueue ) return SQLITE_NOMEM;
  c->aQueue[0] = head;
  c->qTail = 1;
  rc = prollyHashSetAdd(&c->queued, &head);
  if( rc!=SQLITE_OK ) return rc;

  return htAdvance(c, v->db, v->zTableName);
}

static int htNext(sqlite3_vtab_cursor *cur){
  HistCursor *c=(HistCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  c->common.iRowid++;
  return htAdvance(c, v->db, v->zTableName);
}

static int htColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  HistCursor *c=(HistCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  int nCols;
  if( !c->common.hasRow ) return SQLITE_OK;
  nCols=v->cols.nCol;

  if(nCols>0 && col<nCols){
    doltliteResultUserCol(ctx, &v->cols, c->common.pVal, c->common.nVal,
                          c->common.intKey, col);
  }else{
    int fixedCol=col-nCols;
    switch(fixedCol){
      case 0:
        sqlite3_result_text(ctx,c->zCommitHex,-1,SQLITE_TRANSIENT);
        break;
      case 1:
        sqlite3_result_text(ctx,c->zCommitter,-1,SQLITE_TRANSIENT);
        break;
      case 2:
        doltliteResultTimestamp(ctx, c->commitDate);
        break;
    }
  }
  return SQLITE_OK;
}

static sqlite3_module historyModule = {
  0, htConnect, htConnect, htBestIndex,
  doltliteVtabCommonDisconnect, doltliteVtabCommonDisconnect,
  htOpen, htClose, htFilter, htNext,
  doltliteVtabCommonEof, htColumn, doltliteVtabCommonRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterHistoryTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_history_", &historyModule);
}

#endif
