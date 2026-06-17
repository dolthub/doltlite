
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
  DoltliteCommitQueue queue;
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  i64 commitDate;
  int idxNum;
  DoltlitePkRange pkRange;
};

static void htCursorReset(HistCursor *c){
  doltliteVtabCommonReset(&c->common);
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

  rc = doltliteCommitQueueEnqueueParents(&c->queue, &commit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&commit);
    return rc;
  }

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aT, &nT, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;

  if( doltliteFindTableRootByName(aT, nT, zTableName, &tableRoot, &flags, 0)
      !=SQLITE_OK || prollyHashIsEmpty(&tableRoot) ){
    doltliteFreeCatalog(aT, nT);
    return SQLITE_OK;
  }
  doltliteFreeCatalog(aT, nT);

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);

  seekable = (flags & PROLLY_NODE_INTKEY) != 0
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

  for(;;){
    ProllyHash cur;
    int hasHash;

    rc = doltliteCommitQueueNext(&c->queue, &cur, &hasHash);
    if( rc!=SQLITE_OK ) return rc;
    if( !hasHash ) break;

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
  return doltliteBestIndexIntPkRange(p, vt->cols.iPkCol,
      HIST_IDX_PK_EQ, HIST_IDX_PK_GE, HIST_IDX_PK_LE,
      HIST_IDX_PK_GT, HIST_IDX_PK_LT,
      100000.0, 100000, 100.0, 100, 1000.0, 1000);
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
  (void)idxStr;

  htCursorReset(c);

  c->idxNum = idxNum;
  doltlitePkRangeFromArgs(idxNum,
      HIST_IDX_PK_EQ, HIST_IDX_PK_GE, HIST_IDX_PK_LE,
      HIST_IDX_PK_GT, HIST_IDX_PK_LT,
      argc, argv, &c->pkRange);

  cs = doltliteGetChunkStore(v->db);
  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(v->db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;

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
