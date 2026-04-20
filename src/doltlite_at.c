
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <string.h>
#include <time.h>

static char *atBuildSchema(DoltliteColInfo *ci){
  int i;
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  for(i=0; i<ci->nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedIdent(pStr, ci->azName[i])!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ", commit_ref TEXT HIDDEN)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct AtVtab AtVtab;
struct AtVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct AtCursor AtCursor;
struct AtCursor {
  sqlite3_vtab_cursor base;
  ProllyCursor tblCur;
  int tblCurOpen;
  /* Current row (copied from cursor) */
  i64 intKey;
  u8 *pVal; int nVal;
  int hasRow;
  i64 iRowid;
  char *zCommitRef;
};

static void atCursorReset(AtCursor *c){
  if( c->tblCurOpen ){
    prollyCursorClose(&c->tblCur);
    c->tblCurOpen = 0;
  }
  sqlite3_free(c->pVal);
  c->pVal = 0; c->nVal = 0;
  sqlite3_free(c->zCommitRef);
  c->zCommitRef = 0;
  c->hasRow = 0;
  c->iRowid = 0;
}

/* Capture current cursor row. The prolly cursor's value pointer
** may be invalidated on next step, so we copy the bytes. */
static int atCaptureRow(AtCursor *c){
  const u8 *pVal; int nVal;
  sqlite3_free(c->pVal);
  c->pVal = 0; c->nVal = 0;
  c->intKey = prollyCursorIntKey(&c->tblCur);
  prollyCursorValue(&c->tblCur, &pVal, &nVal);
  if( pVal && nVal>0 ){
    c->pVal = sqlite3_malloc(nVal);
    if( !c->pVal ) return SQLITE_NOMEM;
    memcpy(c->pVal, pVal, nVal);
    c->nVal = nVal;
  }
  c->hasRow = 1;
  return SQLITE_OK;
}

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  AtVtab *pVtab;
  int rc;
  const char *zMod;
  char *zSchema;
  (void)pAux;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;

  zMod = argv[0];
  if( zMod && strncmp(zMod, "dolt_at_", 8) == 0 ){
    pVtab->zTableName = sqlite3_mprintf("%s", zMod + 8);
  }else if( argc > 3 ){
    pVtab->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    pVtab->zTableName = sqlite3_mprintf("");
  }

  rc = doltliteLoadUserTableColumns(db, pVtab->zTableName, &pVtab->cols, pzErr);
  if( rc != SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }
  zSchema = atBuildSchema(&pVtab->cols);
  if( !zSchema ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return SQLITE_NOMEM;
  }

  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc != SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int atDisconnect(sqlite3_vtab *pVtab){
  AtVtab *v=(AtVtab*)pVtab;
  sqlite3_free(v->zTableName); doltliteFreeColInfo(&v->cols);
  sqlite3_free(v); return SQLITE_OK;
}

static int atBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  AtVtab *v=(AtVtab*)pVtab;
  int nCols=v->cols.nCol;
  int iRef=-1, i, argvIdx=1;

  int refCol = nCols > 0 ? nCols : 2;
  (void)pVtab;

  for(i=0;i<pInfo->nConstraint;i++){
    if(!pInfo->aConstraint[i].usable) continue;
    if(pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ) continue;
    if(pInfo->aConstraint[i].iColumn==refCol) iRef=i;
  }

  if(iRef>=0){
    pInfo->aConstraintUsage[iRef].argvIndex=argvIdx++;
    pInfo->aConstraintUsage[iRef].omit=1;
    pInfo->idxNum=1;
    pInfo->estimatedCost=1000.0;
  }else{
    pInfo->estimatedCost=1e12;
  }
  return SQLITE_OK;
}

static int atOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  AtCursor *c;(void)pVtab;
  c=sqlite3_malloc(sizeof(*c)); if(!c) return SQLITE_NOMEM;
  memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}

static int atClose(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  atCursorReset(c); sqlite3_free(c); return SQLITE_OK;
}

static int atFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  AtCursor *c=(AtCursor*)cur;
  AtVtab *v=(AtVtab*)cur->pVtab;
  sqlite3 *db=v->db;
  ChunkStore *cs=doltliteGetChunkStore(db);
  void *pBt; ProllyCache *pCache;
  const char *zRef;
  ProllyHash commitHash;
  DoltliteCommit commit;
  struct TableEntry *aTables=0; int nTables=0;
  ProllyHash tableRoot; u8 flags=0;
  int rc, res;
  (void)idxStr;

  atCursorReset(c);
  if(!cs||idxNum!=1||argc<1) return SQLITE_OK;

  pBt=doltliteGetBtShared(db);
  if(!pBt) return SQLITE_OK;
  pCache=doltliteGetCache(db);

  zRef=(const char*)sqlite3_value_text(argv[0]);
  if(!zRef) return SQLITE_OK;
  c->zCommitRef = sqlite3_mprintf("%s", zRef);
  if( !c->zCommitRef ) return SQLITE_NOMEM;

  rc=doltliteResolveRef(db,zRef,&commitHash);
  if(rc==SQLITE_NOTFOUND){
    sqlite3_free(cur->pVtab->zErrMsg);
    cur->pVtab->zErrMsg = sqlite3_mprintf("ref not found: %s", zRef);
    return SQLITE_ERROR;
  }
  if(rc!=SQLITE_OK) return rc;

  memset(&commit,0,sizeof(commit));
  rc=doltliteLoadCommit(db,&commitHash,&commit);
  if(rc!=SQLITE_OK) return rc;

  /* When the ref is a branch name (not a commit hash or tag), prefer
  ** its working catalog over the committed catalog IF the working set
  ** is still based on HEAD — that lets dolt_at_<table>@branch show
  ** staged-but-uncommitted rows on that branch. If the working set
  ** points at a different commit (mid-merge, stale WIP) fall through
  ** and use the committed catalog so the output still makes sense. */
  {
    ProllyHash branchCommit;
    int isBranch = (chunkStoreFindBranch(cs,zRef,&branchCommit)==SQLITE_OK
                    && !prollyHashIsEmpty(&branchCommit));
    if( isBranch ){
      ProllyHash wsCatHash, wsCommitHash;
      memset(&wsCatHash,0,sizeof(wsCatHash));
      memset(&wsCommitHash,0,sizeof(wsCommitHash));
      if( chunkStoreReadBranchWorkingCatalog(cs,zRef,&wsCatHash,&wsCommitHash)==SQLITE_OK
       && !prollyHashIsEmpty(&wsCommitHash)
       && memcmp(wsCommitHash.data,branchCommit.data,PROLLY_HASH_SIZE)==0
       && memcmp(wsCatHash.data,commit.catalogHash.data,PROLLY_HASH_SIZE)!=0 ){

        rc=doltliteLoadCatalog(db,&wsCatHash,&aTables,&nTables,0);
        doltliteCommitClear(&commit);
        if(rc!=SQLITE_OK) return rc;
        goto at_find_root;
      }
    }
  }

  rc=doltliteLoadCatalog(db,&commit.catalogHash,&aTables,&nTables,0);
  doltliteCommitClear(&commit);
  if(rc!=SQLITE_OK) return rc;

at_find_root:

  rc=doltliteFindTableRootByName(aTables,nTables,v->zTableName,&tableRoot,&flags);
  doltliteFreeCatalog(aTables,nTables);
  if(rc==SQLITE_NOTFOUND) return SQLITE_OK;
  if(rc!=SQLITE_OK) return rc;

  if( prollyHashIsEmpty(&tableRoot) ) return SQLITE_OK;

  /* Open streaming cursor on the table at this commit. */
  prollyCursorInit(&c->tblCur, cs, pCache, &tableRoot, flags);
  rc = prollyCursorFirst(&c->tblCur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->tblCur);
    return rc;
  }
  if( res ){
    prollyCursorClose(&c->tblCur);
    return SQLITE_OK;
  }
  c->tblCurOpen = 1;
  return atCaptureRow(c);
}

static int atNext(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  int rc;
  c->iRowid++;
  if( !c->tblCurOpen ){
    c->hasRow = 0;
    return SQLITE_OK;
  }
  rc = prollyCursorNext(&c->tblCur);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->tblCur);
    c->tblCurOpen = 0;
    c->hasRow = 0;
    return rc;
  }
  if( !prollyCursorIsValid(&c->tblCur) ){
    prollyCursorClose(&c->tblCur);
    c->tblCurOpen = 0;
    c->hasRow = 0;
    return SQLITE_OK;
  }
  return atCaptureRow(c);
}

static int atEof(sqlite3_vtab_cursor *cur){
  return !((AtCursor*)cur)->hasRow;
}

static int atColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  AtCursor *c=(AtCursor*)cur;
  AtVtab *v=(AtVtab*)cur->pVtab;
  int nCols=v->cols.nCol;

  if( !c->hasRow ) return SQLITE_OK;

  if( col==nCols ){
    sqlite3_result_text(ctx, c->zCommitRef ? c->zCommitRef : "",
                        -1, SQLITE_TRANSIENT);
  }else if(nCols>0 && col<nCols){
    doltliteResultUserCol(ctx, &v->cols, c->pVal, c->nVal, c->intKey, col);
  }

  return SQLITE_OK;
}

static int atRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r=((AtCursor*)cur)->iRowid; return SQLITE_OK;
}

static sqlite3_module atModule = {
  0, atConnect, atConnect, atBestIndex, atDisconnect, atDisconnect,
  atOpen, atClose, atFilter, atNext, atEof, atColumn, atRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterAtTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_at_", &atModule);
}

int doltliteAtRegister(sqlite3 *db){
  return doltliteRegisterAtTables(db);
}

#endif
