/*
** dolt_diff table-valued function: row-level diff between two commits
** or between HEAD and working state.
**
** Usage:
**   SELECT * FROM dolt_diff('tablename');                        -- working vs HEAD
**   SELECT * FROM dolt_diff('tablename', 'from_hash', 'to_hash'); -- between commits
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "doltlite_record.h"
#include <string.h>

/* From prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);

struct TableEntry { Pgno iTable; ProllyHash root; u8 flags; char *zName; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* BtShared — we need the cache pointer */
typedef struct BtShared BtShared;
extern BtShared *doltliteGetBtShared(sqlite3 *db);
/* Access cache from BtShared — defined as second field after ChunkStore */

/* --------------------------------------------------------------------------
** Buffered diff row
** -------------------------------------------------------------------------- */

typedef struct DiffRow DiffRow;
struct DiffRow {
  u8 type;        /* PROLLY_DIFF_ADD, DELETE, MODIFY */
  i64 intKey;     /* rowid for INTKEY tables */
  u8 *pOldVal;    /* old value (owned, NULL for ADD) */
  int nOldVal;
  u8 *pNewVal;    /* new value (owned, NULL for DELETE) */
  int nNewVal;
  u8 *pKey;       /* blob key for BLOBKEY tables (owned) */
  int nKey;
};

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct DoltliteDiffVtab DoltliteDiffVtab;
struct DoltliteDiffVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DoltliteDiffCursor DoltliteDiffCursor;
struct DoltliteDiffCursor {
  sqlite3_vtab_cursor base;
  DiffRow *aRows;
  int nRows;
  int iRow;
};

static const char *diffSchema =
  "CREATE TABLE x("
  "  diff_type   TEXT,"
  "  rowid_val   INTEGER,"
  "  from_value  TEXT,"
  "  to_value    TEXT,"
  "  table_name  TEXT HIDDEN,"
  "  from_commit TEXT HIDDEN,"
  "  to_commit   TEXT HIDDEN"
  ")";

/* --------------------------------------------------------------------------
** Diff callback: collect changes into buffer
** -------------------------------------------------------------------------- */

static int diffCollect(void *pCtx, const ProllyDiffChange *pChange){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCtx;
  DiffRow *aNew;
  DiffRow *r;

  aNew = sqlite3_realloc(pCur->aRows, (pCur->nRows+1)*(int)sizeof(DiffRow));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aRows = aNew;
  r = &aNew[pCur->nRows];
  memset(r, 0, sizeof(*r));

  r->type = pChange->type;
  r->intKey = pChange->intKey;

  if( pChange->pKey && pChange->nKey>0 ){
    r->pKey = sqlite3_malloc(pChange->nKey);
    if( r->pKey ) memcpy(r->pKey, pChange->pKey, pChange->nKey);
    r->nKey = pChange->nKey;
  }
  if( pChange->pOldVal && pChange->nOldVal>0 ){
    r->pOldVal = sqlite3_malloc(pChange->nOldVal);
    if( r->pOldVal ) memcpy(r->pOldVal, pChange->pOldVal, pChange->nOldVal);
    r->nOldVal = pChange->nOldVal;
  }
  if( pChange->pNewVal && pChange->nNewVal>0 ){
    r->pNewVal = sqlite3_malloc(pChange->nNewVal);
    if( r->pNewVal ) memcpy(r->pNewVal, pChange->pNewVal, pChange->nNewVal);
    r->nNewVal = pChange->nNewVal;
  }

  pCur->nRows++;
  return SQLITE_OK;
}

static void freeDiffRows(DoltliteDiffCursor *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aRows[i].pOldVal);
    sqlite3_free(pCur->aRows[i].pNewVal);
    sqlite3_free(pCur->aRows[i].pKey);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
}

/* --------------------------------------------------------------------------
** Helper: find a table's root hash in a catalog
** -------------------------------------------------------------------------- */

static int findTableRoot(struct TableEntry *a, int n, Pgno iTable,
                         ProllyHash *pRoot, u8 *pFlags){
  int i;
  for(i=0; i<n; i++){
    if( a[i].iTable==iTable ){
      memcpy(pRoot, &a[i].root, sizeof(ProllyHash));
      if( pFlags ) *pFlags = a[i].flags;
      return SQLITE_OK;
    }
  }
  memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  return SQLITE_NOTFOUND;
}

/* --------------------------------------------------------------------------
** Resolve a commit hash string to a table root hash
** -------------------------------------------------------------------------- */

static int resolveCommitToTableRoot(
  sqlite3 *db, const char *zCommitHash, Pgno iTable,
  ProllyHash *pRoot, u8 *pFlags
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash commitHash;
  u8 *data = 0;
  int nData = 0;
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int rc;

  rc = doltliteHexToHash(zCommitHash, &commitHash);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreGet(cs, &commitHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;

  rc = findTableRoot(aTables, nTables, iTable, pRoot, pFlags);
  sqlite3_free(aTables);
  return rc;
}

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static int diffConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DoltliteDiffVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, diffSchema);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int diffDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int diffBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iTableName = -1;
  int iFromCommit = -1;
  int iToCommit = -1;
  int i;
  int argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case 4: iTableName = i; break;   /* table_name */
      case 5: iFromCommit = i; break;  /* from_commit */
      case 6: iToCommit = i; break;    /* to_commit */
    }
  }

  if( iTableName<0 ){
    /* table_name is required */
    pInfo->estimatedCost = 1e12;
    return SQLITE_OK;
  }

  pInfo->aConstraintUsage[iTableName].argvIndex = argvIdx++;
  pInfo->aConstraintUsage[iTableName].omit = 1;
  if( iFromCommit>=0 ){
    pInfo->aConstraintUsage[iFromCommit].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFromCommit].omit = 1;
  }
  if( iToCommit>=0 ){
    pInfo->aConstraintUsage[iToCommit].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iToCommit].omit = 1;
  }

  /* Encode which args are present in idxNum */
  pInfo->idxNum = (iTableName>=0 ? 1 : 0)
                | (iFromCommit>=0 ? 2 : 0)
                | (iToCommit>=0 ? 4 : 0);
  pInfo->estimatedCost = 1000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

static int diffOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  DoltliteDiffCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int diffClose(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  freeDiffRows(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int diffFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DoltliteDiffVtab *pVtab = (DoltliteDiffVtab*)pCursor->pVtab;
  sqlite3 *db = pVtab->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  BtShared *pBt = doltliteGetBtShared(db);
  const char *zTableName = 0;
  const char *zFromCommit = 0;
  const char *zToCommit = 0;
  ProllyHash oldRoot, newRoot;
  u8 flags = 0;
  Pgno iTable;
  int rc;
  int argIdx = 0;
  (void)idxStr;

  freeDiffRows(pCur);
  pCur->iRow = 0;

  if( !cs || !pBt ) return SQLITE_OK;

  /* Extract arguments */
  if( (idxNum & 1) && argIdx<argc ){
    zTableName = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 2) && argIdx<argc ){
    zFromCommit = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 4) && argIdx<argc ){
    zToCommit = (const char*)sqlite3_value_text(argv[argIdx++]);
  }

  if( !zTableName ) return SQLITE_OK;

  /* Resolve table name to Pgno */
  rc = doltliteResolveTableName(db, zTableName, &iTable);
  if( rc!=SQLITE_OK ) return SQLITE_OK; /* Unknown table = empty diff */

  /* Determine old root hash */
  if( zFromCommit ){
    rc = resolveCommitToTableRoot(db, zFromCommit, iTable, &oldRoot, &flags);
  }else{
    /* Default: HEAD */
    ProllyHash headCatHash;
    struct TableEntry *aHead = 0;
    int nHead = 0;
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
      if( rc==SQLITE_OK ){
        findTableRoot(aHead, nHead, iTable, &oldRoot, &flags);
        sqlite3_free(aHead);
      }
    }
  }

  /* Determine new root hash */
  if( zToCommit ){
    u8 f2;
    rc = resolveCommitToTableRoot(db, zToCommit, iTable, &newRoot, &f2);
    if( flags==0 ) flags = f2;
  }else{
    /* Default: working state */
    u8 *catData = 0; int nCatData = 0;
    ProllyHash workingCatHash;
    struct TableEntry *aWork = 0;
    int nWork = 0;
    rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
    if( rc==SQLITE_OK ){
      rc = chunkStorePut(cs, catData, nCatData, &workingCatHash);
      sqlite3_free(catData);
      if( rc==SQLITE_OK ){
        rc = doltliteLoadCatalog(db, &workingCatHash, &aWork, &nWork, 0);
        if( rc==SQLITE_OK ){
          findTableRoot(aWork, nWork, iTable, &newRoot, &flags);
          sqlite3_free(aWork);
        }
      }
    }
  }

  /* Same root = no changes */
  if( prollyHashCompare(&oldRoot, &newRoot)==0 ) return SQLITE_OK;

  /* Run the diff */
  {
    /* Access the ProllyCache — it's the second field of BtShared
    ** after ChunkStore. This is fragile but we're inside the fork. */
    ProllyCache *pCache = (ProllyCache*)(((char*)pBt) + sizeof(ChunkStore));

    rc = prollyDiff(cs, pCache, &oldRoot, &newRoot, flags,
                    diffCollect, (void*)pCur);
  }

  return SQLITE_OK;
}

static int diffNext(sqlite3_vtab_cursor *pCursor){
  ((DoltliteDiffCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int diffEof(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  return pCur->iRow >= pCur->nRows;
}

static int diffColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DiffRow *r = &pCur->aRows[pCur->iRow];

  switch( iCol ){
    case 0: /* diff_type */
      switch( r->type ){
        case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
        case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
        case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
      }
      break;
    case 1: /* rowid_val */
      sqlite3_result_int64(ctx, r->intKey);
      break;
    case 2: /* from_value */
      doltliteResultRecord(ctx, r->pOldVal, r->nOldVal);
      break;
    case 3: /* to_value */
      doltliteResultRecord(ctx, r->pNewVal, r->nNewVal);
      break;
  }
  return SQLITE_OK;
}

static int diffRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteDiffCursor*)pCursor)->iRow;
  return SQLITE_OK;
}

/* -------------------------------------------------------------------------- */

static sqlite3_module doltliteDiffModule = {
  0, 0, diffConnect, diffBestIndex, diffDisconnect, 0,
  diffOpen, diffClose, diffFilter, diffNext, diffEof,
  diffColumn, diffRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_diff", &doltliteDiffModule, 0);
}

#endif /* DOLTLITE_PROLLY */
