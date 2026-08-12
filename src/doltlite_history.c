
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
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
  sqlite3_str_appendall(pStr, ", commit_hash TEXT, committer TEXT, commit_date TEXT"
                              ", start_ref TEXT HIDDEN)");
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
#define HIST_IDX_COMMIT_EQ 0x20
#define HIST_IDX_START_REF 0x40
#define HIST_IDX_PK_ANY (HIST_IDX_PK_EQ|HIST_IDX_PK_GE|HIST_IDX_PK_LE|HIST_IDX_PK_GT|HIST_IDX_PK_LT)

typedef struct HistCursor HistCursor;
struct HistCursor {
  DoltliteVtabCursorCommon common;
  /* Columns as the visited commit's schema declares them, cached by schema
  ** hash across commits. Invalid renders with the declared layout. */
  DoltliteSideCols side;
  DoltliteCommitQueue queue;
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  i64 commitDate;
  int idxNum;
  DoltlitePkRange pkRange;
  int singleCommit;
};

static void htCursorReset(HistCursor *c){
  doltliteVtabCommonReset(&c->common);
  doltliteSideColsClear(&c->side);
  sqlite3_free(c->zCommitter);
  c->zCommitter = 0;
  doltliteCommitQueueClear(&c->queue);
}

static int htRowMatchesUpper(HistCursor *c){
  i64 k;
  if( !c->pkRange.hasPkHi ) return 1;
  k = prollyCursorIntKey(&c->common.tblCur);
  if( c->pkRange.pkHiStrict ){
    return k < c->pkRange.pkHi;
  }
  return k <= c->pkRange.pkHi;
}

static int htOpenTableAtCommit(HistCursor *c, sqlite3 *db,
    const char *zTableName, const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DoltliteCommit commit;
  ProllyHash tableRoot; u8 flags = 0;
  ProllyHash schemaHash;
  int rc, res;
  int seekable;

  memset(&schemaHash, 0, sizeof(schemaHash));
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

  if( !c->singleCommit ){
    rc = doltliteCommitQueueEnqueueParents(&c->queue, &commit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      return rc;
    }
  }

  rc = doltliteLoadTableRootByName(db, &commit.catalogHash, zTableName,
                                   &tableRoot, &flags, &schemaHash);
  if( rc==SQLITE_OK ){
    DoltliteVtabCommon *v = (DoltliteVtabCommon*)c->common.base.pVtab;
    rc = doltliteSideColsLoad(db, &commit.catalogHash, &schemaHash,
                              zTableName, &v->cols, &c->side);
  }
  doltliteCommitClear(&commit);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  if( prollyHashIsEmpty(&tableRoot) ){
    return SQLITE_OK;
  }

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);
  c->common.rootIntKey = (flags & PROLLY_NODE_INTKEY) != 0;

  seekable = c->common.rootIntKey
          && (c->idxNum & HIST_IDX_PK_ANY) != 0;

  if( seekable && (c->idxNum & HIST_IDX_PK_EQ) ){
    rc = prollyCursorSeekInt(&c->common.tblCur, c->pkRange.pkLo, &res);
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

  if( seekable && c->pkRange.hasPkLo ){
    i64 startKey = c->pkRange.pkLo;
    if( c->pkRange.pkLoStrict ) startKey++;
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
  if( seekable && c->pkRange.hasPkHi && !htRowMatchesUpper(c) ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  c->common.tblCurOpen = 1;
  return SQLITE_OK;
}

static int htAdvance(HistCursor *c, sqlite3 *db, const char *zTableName){
  int rc;

  if( c->common.tblCurOpen ){
    if( (c->idxNum & HIST_IDX_PK_EQ) && c->common.rootIntKey ){
      prollyCursorClose(&c->common.tblCur);
      c->common.tblCurOpen = 0;
    }else{
      rc = prollyCursorNext(&c->common.tblCur);
      if( rc!=SQLITE_OK ){
        prollyCursorClose(&c->common.tblCur);
        c->common.tblCurOpen = 0;
        return rc;
      }
      if( prollyCursorIsValid(&c->common.tblCur)
       && (!c->common.rootIntKey || htRowMatchesUpper(c)) ){
        return doltliteVtabCommonCaptureRowSide(&c->common, db, zTableName,
                                                &c->side);
      }
      prollyCursorClose(&c->common.tblCur);
      c->common.tblCurOpen = 0;
    }
  }

  for(;;){
    ProllyHash cur;
    int hasHash;

    rc = doltliteCommitQueueNext(&c->queue, &cur, &hasHash);
    if( rc!=SQLITE_OK ) return rc;
    if( !hasHash ) break;

    rc = htOpenTableAtCommit(c, db, zTableName, &cur);
    if( rc!=SQLITE_OK ) return rc;

    if( c->common.tblCurOpen ){
      return doltliteVtabCommonCaptureRowSide(&c->common, db, zTableName,
                                              &c->side);
    }
  }

  c->common.hasRow = 0;
  return SQLITE_OK;
}

static int htConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectHistoricalTable(db, argc, argv,
                                            "dolt_history_",
                                            sizeof(HistVtab), htBuildSchema,
                                            ppVtab, pzErr);
}

static int htBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  DoltliteVtabCommon *vt = (DoltliteVtabCommon*)v;
  int i, iCommitEq = -1, iStartRef = -1, nArg = 0;
  int idxNum;
  int iPkCol = vt->cols.iPkCol;
  int iCommitCol = vt->cols.nCol;
  int iStartRefCol = vt->cols.nCol + 3;

  p->estimatedCost = 100000.0;
  p->estimatedRows = 100000;

  if( iPkCol>=0 ){
    (void)doltliteBestIndexIntPkRange(p, iPkCol,
        HIST_IDX_PK_EQ, HIST_IDX_PK_GE, HIST_IDX_PK_LE,
        HIST_IDX_PK_GT, HIST_IDX_PK_LT,
        100000.0, 100000, 100.0, 100, 1000.0, 1000);
  }else{
    p->idxNum = 0;
  }
  idxNum = p->idxNum;
  for(i=0; i<p->nConstraint; i++){
    if( p->aConstraintUsage[i].argvIndex>nArg ){
      nArg = p->aConstraintUsage[i].argvIndex;
    }
  }

  for(i=0; i<p->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &p->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->iColumn==iCommitCol
     && pC->op==SQLITE_INDEX_CONSTRAINT_EQ
     && !sqlite3DoltliteVtabConstraintIsCorrelated(p, i) ){
      iCommitEq = i;
    }else if( pC->iColumn==iStartRefCol
           && pC->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iStartRef = i;
    }
  }
  if( iCommitEq>=0 ){
    p->aConstraintUsage[iCommitEq].argvIndex = ++nArg;
    p->aConstraintUsage[iCommitEq].omit = 1;
    idxNum |= HIST_IDX_COMMIT_EQ;
    p->estimatedCost = (idxNum & HIST_IDX_PK_ANY) ? 10.0 : 50.0;
    p->estimatedRows = (idxNum & HIST_IDX_PK_ANY) ? 1 : 10;
  }
  if( iStartRef>=0 ){
    p->aConstraintUsage[iStartRef].argvIndex = ++nArg;
    p->aConstraintUsage[iStartRef].omit = 1;
    idxNum |= HIST_IDX_START_REF;
    if( iCommitEq<0 ){
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
  ProllyHash startHash;
  ChunkStore *cs;
  int rc;
  int iArg = 0;
  (void)idxStr;

  htCursorReset(c);
  memset(&startHash, 0, sizeof(startHash));
  c->singleCommit = 0;

  c->idxNum = idxNum;
  doltlitePkRangeFromArgs(idxNum,
      HIST_IDX_PK_EQ, HIST_IDX_PK_GE, HIST_IDX_PK_LE,
      HIST_IDX_PK_GT, HIST_IDX_PK_LT,
      argc, argv, &c->pkRange);
  if( c->pkRange.isEmpty ) return SQLITE_OK;
  if( idxNum & HIST_IDX_PK_EQ ){
    iArg = 1;
  }else{
    if( idxNum & HIST_IDX_PK_GE ) iArg++;
    if( idxNum & HIST_IDX_PK_GT ) iArg++;
    if( idxNum & HIST_IDX_PK_LE ) iArg++;
    if( idxNum & HIST_IDX_PK_LT ) iArg++;
  }
  if( idxNum & HIST_IDX_COMMIT_EQ ){
    const char *zHash = iArg<argc ? (const char*)sqlite3_value_text(argv[iArg]) : 0;
    if( !zHash || doltliteHexToHash(zHash, &startHash)!=SQLITE_OK
     || prollyHashIsEmpty(&startHash) ){
      return SQLITE_OK;
    }
    c->singleCommit = 1;
    iArg++;
  }

  cs = doltliteGetChunkStore(v->db);
  if( !cs ) return SQLITE_OK;

  if( c->singleCommit ){
    head = startHash;
  }else if( idxNum & HIST_IDX_START_REF ){
    const char *zRef = iArg<argc
                     ? (const char*)sqlite3_value_text(argv[iArg]) : 0;
    if( !zRef ) return SQLITE_OK;
    rc = doltliteResolveRef(v->db, zRef, &head);
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_free(cur->pVtab->zErrMsg);
      cur->pVtab->zErrMsg = sqlite3_mprintf("ref not found: %s", zRef);
      return SQLITE_ERROR;
    }
    if( rc!=SQLITE_OK ) return rc;
  }else{
    doltliteGetSessionHead(v->db, &head);
    if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
  }

  rc = doltliteCommitQueueInit(&c->queue, &head);
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
    doltliteResultSideCol(ctx, &c->side, &v->cols,
                          c->common.pVal, c->common.nVal,
                          c->common.intKey, c->common.rootIntKey, col);
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

const sqlite3_module *doltliteHistoryTableModule(void){
  return &historyModule;
}

#endif
