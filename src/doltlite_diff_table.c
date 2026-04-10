
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <assert.h>
#include <string.h>
#include <time.h>



static char *buildDiffSchema(DoltliteColInfo *ci){
  int i;
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  char *zColName;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  for(i=0; i<ci->nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, ", ");
    zColName = sqlite3_mprintf("from_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !zColName ){
      sqlite3_str_reset(pStr);
      return 0;
    }
    if( doltliteAppendQuotedIdent(pStr, zColName)!=SQLITE_OK ){
      sqlite3_free(zColName);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(zColName);
  }
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendall(pStr, ", ");
    zColName = sqlite3_mprintf("to_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !zColName ){
      sqlite3_str_reset(pStr);
      return 0;
    }
    if( doltliteAppendQuotedIdent(pStr, zColName)!=SQLITE_OK ){
      sqlite3_free(zColName);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(zColName);
  }
  sqlite3_str_appendall(pStr, ", from_commit TEXT, to_commit TEXT"
                              ", from_commit_date TEXT, to_commit_date TEXT"
                              ", diff_type TEXT)");
  z = sqlite3_str_finish(pStr);
  return z;
}

/* A single row of diff output — holds copies of value data so it survives
** cursor movement within the prolly tree. */
typedef struct AuditRow AuditRow;
struct AuditRow {
  u8 diffType;
  i64 intKey;
  u8 *pOldVal; int nOldVal;
  u8 *pNewVal; int nNewVal;
  char zFromCommit[PROLLY_HASH_SIZE*2+1];
  char zToCommit[PROLLY_HASH_SIZE*2+1];
  i64 fromDate;
  i64 toDate;
};

typedef struct DiffTblVtab DiffTblVtab;
struct DiffTblVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

/*
** Streaming cursor: walks commit history lazily and yields one diff row
** at a time via a ProllyDiffIter for each commit pair.
*/
typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;

  /* Commit walk state */
  ProllyHash curCommitHash;     /* Current commit being diffed (the "to" side) */
  int commitWalkDone;           /* 1 when no more commits to process */

  /* Diff iterator for the current commit pair */
  ProllyDiffIter diffIter;
  int diffIterOpen;             /* 1 if diffIter is currently open */

  /* Current row data */
  AuditRow row;
  int hasRow;                   /* 1 if row contains valid data */
  i64 iRowid;                   /* Monotonically increasing rowid */
};

static int diffRecordField(
  const u8 *pData,
  int nData,
  int iField,
  int *pType,
  int *pOff
){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  const u8 *pHdrEnd;
  u64 hdrSize;
  int hdrBytes;
  int off;
  int i;

  if( !pData || nData < 1 || iField < 0 ) return SQLITE_CORRUPT;
  hdrBytes = dlReadVarint(p, pEnd, &hdrSize);
  if( hdrBytes <= 0 || hdrSize < (u64)hdrBytes || hdrSize > (u64)nData ){
    return SQLITE_CORRUPT;
  }
  p += hdrBytes;
  pHdrEnd = pData + (int)hdrSize;
  off = (int)hdrSize;
  for(i=0; p < pHdrEnd; i++){
    u64 st;
    int stBytes = dlReadVarint(p, pHdrEnd, &st);
    int len;
    if( stBytes <= 0 ) return SQLITE_CORRUPT;
    p += stBytes;
    len = dlSerialTypeLen(st);
    if( off < 0 || off + len > nData ) return SQLITE_CORRUPT;
    if( i==iField ){
      *pType = (int)st;
      *pOff = off;
      return SQLITE_OK;
    }
    off += len;
  }
  return SQLITE_NOTFOUND;
}

static void clearAuditRow(AuditRow *r){
  sqlite3_free(r->pOldVal);
  sqlite3_free(r->pNewVal);
  memset(r, 0, sizeof(*r));
}

static void closeDiffIter(DiffTblCursor *pCur){
  if( pCur->diffIterOpen ){
    prollyDiffIterClose(&pCur->diffIter);
    pCur->diffIterOpen = 0;
  }
}

/*
** Load the commit at pCur->curCommitHash, find the table roots for
** it and its parent, and open a ProllyDiffIter between them.
** Sets commitWalkDone=1 if the commit is the initial commit (no parent).
** Advances curCommitHash to the parent for the next call.
** Returns SQLITE_OK on success, or an error code.
*/
static int openNextCommitPairIter(DiffTblCursor *pCur, sqlite3 *db,
                                  const char *zTableName){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DoltliteCommit commit, parentCommit;
  ProllyHash curRoot, parentRoot;
  ProllyHash emptyRoot;
  u8 flags = 0;
  int rc;

  if( !cs ) return SQLITE_OK;

  for(;;){
    memset(&commit, 0, sizeof(commit));
    memset(&parentCommit, 0, sizeof(parentCommit));
    memset(&curRoot, 0, sizeof(curRoot));
    memset(&parentRoot, 0, sizeof(parentRoot));
    flags = 0;

    rc = doltliteLoadCommit(db, &pCur->curCommitHash, &commit);
    if( rc!=SQLITE_OK ) return rc;

    {
      struct TableEntry *aTables = 0; int nTables = 0;
      rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }
      doltliteFindTableRootByName(aTables, nTables, zTableName, &curRoot, &flags);
      sqlite3_free(aTables);
    }

    doltliteHashToHex(&pCur->curCommitHash, pCur->row.zToCommit);
    pCur->row.toDate = commit.timestamp;

    if( !prollyHashIsEmpty(&commit.parentHash) ){
      ProllyHash nextHash;
      memset(&nextHash, 0, sizeof(nextHash));
      rc = doltliteLoadCommit(db, &commit.parentHash, &parentCommit);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }

      doltliteHashToHex(&commit.parentHash, pCur->row.zFromCommit);
      pCur->row.fromDate = parentCommit.timestamp;

      {
        struct TableEntry *aPT = 0; int nPT = 0;
        rc = doltliteLoadCatalog(db, &parentCommit.catalogHash, &aPT, &nPT, 0);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&parentCommit);
          doltliteCommitClear(&commit);
          return rc;
        }
        doltliteFindTableRootByName(aPT, nPT, zTableName, &parentRoot, 0);
        sqlite3_free(aPT);
      }

      memcpy(&nextHash, &commit.parentHash, sizeof(ProllyHash));
      doltliteCommitClear(&parentCommit);

      if( prollyHashCompare(&parentRoot, &curRoot)==0 ){
        doltliteCommitClear(&commit);
        memcpy(&pCur->curCommitHash, &nextHash, sizeof(ProllyHash));
        continue;
      }

      rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                              &parentRoot, &curRoot, flags);
      if( rc==SQLITE_OK ) pCur->diffIterOpen = 1;
      doltliteCommitClear(&commit);
      memcpy(&pCur->curCommitHash, &nextHash, sizeof(ProllyHash));
      return rc;
    }

    if( !prollyHashIsEmpty(&curRoot) ){
      memset(&emptyRoot, 0, sizeof(emptyRoot));
      memset(pCur->row.zFromCommit, '0', PROLLY_HASH_SIZE*2);
      pCur->row.zFromCommit[PROLLY_HASH_SIZE*2] = 0;
      pCur->row.fromDate = 0;

      rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                              &emptyRoot, &curRoot, flags);
      if( rc==SQLITE_OK ) pCur->diffIterOpen = 1;
      doltliteCommitClear(&commit);
      memset(&pCur->curCommitHash, 0, sizeof(ProllyHash));
      pCur->commitWalkDone = 1;
      return rc;
    }

    doltliteCommitClear(&commit);
    memset(&pCur->curCommitHash, 0, sizeof(ProllyHash));
    pCur->commitWalkDone = 1;
    return SQLITE_OK;
  }
}

/*
** Try to produce the next row. Steps the current diff iterator; if
** exhausted, moves to the next commit pair. Sets hasRow=1 if a row
** is available, or leaves hasRow=0 if all diffs are exhausted.
*/
static int advanceToNextRow(DiffTblCursor *pCur, sqlite3 *db,
                            const char *zTableName){
  int rc;

  pCur->hasRow = 0;

  for(;;){
    if( pCur->diffIterOpen ){
      ProllyDiffChange *pChange = 0;
      rc = prollyDiffIterStep(&pCur->diffIter, &pChange);
      if( rc==SQLITE_ROW && pChange ){
        u8 *pOldVal = 0;
        u8 *pNewVal = 0;
        /* Free previous value copies (preserves commit-pair metadata) */
        sqlite3_free(pCur->row.pOldVal);
        sqlite3_free(pCur->row.pNewVal);
        pCur->row.pOldVal = 0;
        pCur->row.nOldVal = 0;
        pCur->row.pNewVal = 0;
        pCur->row.nNewVal = 0;

        pCur->row.diffType = pChange->type;
        pCur->row.intKey = pChange->intKey;

        /* Copy value data — the iterator's copies are valid until next Step */
        if( pChange->pOldVal && pChange->nOldVal > 0 ){
          pOldVal = sqlite3_malloc(pChange->nOldVal);
          if( !pOldVal ) return SQLITE_NOMEM;
          memcpy(pOldVal, pChange->pOldVal, pChange->nOldVal);
        }

        if( pChange->pNewVal && pChange->nNewVal > 0 ){
          pNewVal = sqlite3_malloc(pChange->nNewVal);
          if( !pNewVal ){
            sqlite3_free(pOldVal);
            return SQLITE_NOMEM;
          }
          memcpy(pNewVal, pChange->pNewVal, pChange->nNewVal);
        }

        pCur->row.pOldVal = pOldVal;
        pCur->row.nOldVal = pChange->pOldVal ? pChange->nOldVal : 0;
        pCur->row.pNewVal = pNewVal;
        pCur->row.nNewVal = pChange->pNewVal ? pChange->nNewVal : 0;

        pCur->hasRow = 1;
        pCur->iRowid++;
        return SQLITE_OK;
      }
      if( rc!=SQLITE_DONE && rc!=SQLITE_ROW ){
        /* Error from the iterator */
        return rc;
      }
      /* This commit pair's diff is exhausted */
      closeDiffIter(pCur);
    }

    /* Try the next commit pair */
    if( pCur->commitWalkDone || prollyHashIsEmpty(&pCur->curCommitHash) ){
      /* All commits processed */
      return SQLITE_OK;
    }

    rc = openNextCommitPairIter(pCur, db, zTableName);
    if( rc!=SQLITE_OK ) return rc;

    /* If openNextCommitPairIter didn't open an iterator (e.g., table didn't
    ** exist in these commits, or roots were the same and it recursed all
    ** the way to done), loop again to check. */
  }
}

static int dtConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DiffTblVtab *pVtab;
  int rc;
  const char *zModName;
  char *zSchema;
  (void)pAux; (void)pzErr;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;


  zModName = argv[0];
  if( zModName && strncmp(zModName, "dolt_diff_", 10)==0 ){
    pVtab->zTableName = sqlite3_mprintf("%s", zModName + 10);
  }else if( argc > 3 ){
    pVtab->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    pVtab->zTableName = sqlite3_mprintf("");
  }


  rc = doltliteLoadUserTableColumns(db, pVtab->zTableName, &pVtab->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }
  zSchema = buildDiffSchema(&pVtab->cols);

  if( !zSchema ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return SQLITE_NOMEM;
  }

  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dtDisconnect(sqlite3_vtab *pBase){
  DiffTblVtab *pVtab = (DiffTblVtab*)pBase;
  sqlite3_free(pVtab->zTableName);
  doltliteFreeColInfo(&pVtab->cols);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int dtBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10000.0;
  return SQLITE_OK;
}

static int dtOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  DiffTblCursor *c; (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dtClose(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  closeDiffIter(c);
  clearAuditRow(&c->row);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dtFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  sqlite3 *db = pVtab->db;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  /* Reset state */
  closeDiffIter(c);
  clearAuditRow(&c->row);
  c->commitWalkDone = 0;
  c->hasRow = 0;
  c->iRowid = 0;

  /* Start commit walk at HEAD */
  doltliteGetSessionHead(db, &c->curCommitHash);
  if( prollyHashIsEmpty(&c->curCommitHash) ){
    c->commitWalkDone = 1;
    return SQLITE_OK;
  }

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    void *pBt = doltliteGetBtShared(db);
    if( !cs || !pBt ){
      c->commitWalkDone = 1;
      return SQLITE_OK;
    }
  }

  /* Open the first commit pair iterator */
  {
    int rc = openNextCommitPairIter(c, db, pVtab->zTableName);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* Advance to the first row */
  return advanceToNextRow(c, db, pVtab->zTableName);
}

static int dtNext(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  return advanceToNextRow(c, pVtab->db, pVtab->zTableName);
}

static int dtEof(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  return !c->hasRow;
}

static int dtColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  AuditRow *r = &c->row;
  int nCols = pVtab->cols.nCol;



  if( nCols > 0 && col < nCols ){

    int colIdx = col;
    if( colIdx == pVtab->cols.iPkCol ){

      if( r->pOldVal && r->nOldVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{

      if( r->pOldVal && r->nOldVal > 0 ){
        int st, off;
        int rc = diffRecordField(r->pOldVal, r->nOldVal, colIdx, &st, &off);
        if( rc==SQLITE_OK ){
          doltliteResultField(ctx, r->pOldVal, r->nOldVal,
                        st, off);
        }else if( rc==SQLITE_NOTFOUND ){
          sqlite3_result_null(ctx);
        }else{
          sqlite3_result_error_code(ctx, rc);
          return rc;
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col < 2*nCols ){

    int colIdx = col - nCols;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pNewVal && r->nNewVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pNewVal && r->nNewVal > 0 ){
        int st, off;
        int rc = diffRecordField(r->pNewVal, r->nNewVal, colIdx, &st, &off);
        if( rc==SQLITE_OK ){
          doltliteResultField(ctx, r->pNewVal, r->nNewVal,
                        st, off);
        }else if( rc==SQLITE_NOTFOUND ){
          sqlite3_result_null(ctx);
        }else{
          sqlite3_result_error_code(ctx, rc);
          return rc;
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else{

    int fixedCol = col - 2*nCols;

    switch( fixedCol ){
      case 0:
        sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
        break;
      case 1:
        sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
        break;
      case 2:
        { time_t t = (time_t)r->fromDate; struct tm *tm = gmtime(&t);
          if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx); }
        break;
      case 3:
        { time_t t = (time_t)r->toDate; struct tm *tm = gmtime(&t);
          if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
            sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
          }else sqlite3_result_null(ctx); }
        break;
      case 4:
        switch( r->diffType ){
          case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
          case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
          case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
        }
        break;
    }
  }

  return SQLITE_OK;
}

static int dtRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DiffTblCursor*)cur)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module diffTableModule = {
  0, dtConnect, dtConnect, dtBestIndex, dtDisconnect, dtDisconnect,
  dtOpen, dtClose, dtFilter, dtNext, dtEof, dtColumn, dtRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterDiffTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_diff_", &diffTableModule);
}

#endif
