
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_ancestor.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

/* dolt_blame_<table> semantics:
**
** For each LIVE row in the current table, report the most recent
** commit that introduced the row's current value. Walked first-
** parent from HEAD. At a linear commit, blame = that commit if the
** row's value differs from its value in the parent. At a merge
** commit (2+ parents), blame = merge commit if the row's value
** differs from its value in the merge base (LCA of parents).
**
** Walking past the root is handled by treating a missing parent
** table as "row not present" — any still-unblamed row at the root
** commit will therefore be blamed to it. */

typedef struct BlameRow BlameRow;
struct BlameRow {
  i64 intKey;
  u8 *pKey;    int nKey;
  u8 *pCurVal; int nCurVal;
  u8  blamed;

  char zCommit[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  char *zEmail;
  char *zMessage;
  i64  commitDate;
};

typedef struct BlameVtab BlameVtab;
struct BlameVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  char **azPkNames;
  int   *aPkColIdx;
  int    nPkCols;
  int    intPkCid;
};

typedef struct BlameCursor BlameCursor;
struct BlameCursor {
  sqlite3_vtab_cursor base;
  BlameRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

typedef struct BlamePkTmp BlamePkTmp;
struct BlamePkTmp {
  int cid;
  char *zName;
  int pkPos;
  int isIntegerType;
};

/* Collect PK column names (in pk-position order) and their record
** field indices (cid from PRAGMA table_info). Also returns the cid
** of the rowid-aliased INTEGER PK column if any, else -1 — used at
** emit time to decide whether to pull the value from cursor intKey
** or from the record payload. */
static int blameLoadPkColumns(
  sqlite3 *db,
  const char *zTable,
  char ***pazNames,
  int **paColIdx,
  int *pnCols,
  int *pIntPkCid
){
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  int rc;
  int n = 0;
  char **azNames = 0;
  int *aCid = 0;
  int intPkCid = -1;
  BlamePkTmp tmp[64];
  int nTmp = 0;
  int i, j;

  *pazNames = 0;
  *paColIdx = 0;
  *pnCols = 0;
  *pIntPkCid = -1;

  zSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    int cid = sqlite3_column_int(pStmt, 0);
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
    int pkPos = sqlite3_column_int(pStmt, 5);
    if( pkPos <= 0 ) continue;
    if( nTmp >= (int)(sizeof(tmp)/sizeof(tmp[0])) ) continue;
    tmp[nTmp].cid = cid;
    tmp[nTmp].zName = sqlite3_mprintf("%s", zName ? zName : "");
    tmp[nTmp].pkPos = pkPos;
    tmp[nTmp].isIntegerType =
        zType && sqlite3_stricmp(zType, "INTEGER")==0;
    if( !tmp[nTmp].zName ){ rc = SQLITE_NOMEM; break; }
    nTmp++;
  }
  sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ){
    for(i=0; i<nTmp; i++) sqlite3_free(tmp[i].zName);
    return rc;
  }

  /* Insertion-sort tmp[] by pkPos so emit order matches PK declaration. */
  for(i=1; i<nTmp; i++){
    BlamePkTmp t = tmp[i];
    j = i - 1;
    while( j>=0 && tmp[j].pkPos > t.pkPos ){
      tmp[j+1] = tmp[j];
      j--;
    }
    tmp[j+1] = t;
  }

  if( nTmp > 0 ){
    azNames = sqlite3_malloc(nTmp * (int)sizeof(char*));
    aCid = sqlite3_malloc(nTmp * (int)sizeof(int));
    if( !azNames || !aCid ){
      sqlite3_free(azNames);
      sqlite3_free(aCid);
      for(i=0; i<nTmp; i++) sqlite3_free(tmp[i].zName);
      return SQLITE_NOMEM;
    }
  }
  for(i=0; i<nTmp; i++){
    azNames[i] = tmp[i].zName;
    aCid[i] = tmp[i].cid;
    n++;
  }

  /* Identify the rowid-aliased INTEGER PK column if this is a
  ** single-column INTEGER PRIMARY KEY: SQLite treats it as the
  ** rowid, so its value must come from cursor.intKey, not the
  ** record payload which won't include a field for it. */
  if( nTmp == 1 && tmp[0].isIntegerType ){
    intPkCid = tmp[0].cid;
  }

  *pazNames = azNames;
  *paColIdx = aCid;
  *pnCols = n;
  *pIntPkCid = intPkCid;
  return SQLITE_OK;
}

static void blameFreePkColumns(char **azNames, int *aColIdx, int nCols){
  int i;
  if( azNames ){
    for(i=0; i<nCols; i++) sqlite3_free(azNames[i]);
    sqlite3_free(azNames);
  }
  sqlite3_free(aColIdx);
}

static char *blameBuildSchema(BlameVtab *v){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  int i;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  for(i=0; i<v->nPkCols; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedIdent(pStr, v->azPkNames[i])!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  if( v->nPkCols>0 ) sqlite3_str_appendall(pStr, ", ");
  sqlite3_str_appendall(pStr,
    "\"commit\" TEXT, commit_date TEXT, committer TEXT, email TEXT, message TEXT)");
  z = sqlite3_str_finish(pStr);
  return z;
}

static void blameFreeRow(BlameRow *r){
  sqlite3_free(r->pKey);
  sqlite3_free(r->pCurVal);
  sqlite3_free(r->zCommitter);
  sqlite3_free(r->zEmail);
  sqlite3_free(r->zMessage);
  memset(r, 0, sizeof(*r));
}

static void blameFreeRows(BlameCursor *c){
  int i;
  for(i=0; i<c->nRows; i++) blameFreeRow(&c->aRows[i]);
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
  c->nAlloc = 0;
}

/* Load every live row of the current table into the cursor. Returns
** SQLITE_OK with nRows==0 if the table has no rows at HEAD. */
static int blameCollectLiveRows(
  BlameCursor *pCur,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags
){
  ProllyCursor cur;
  int res, rc;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){ prollyCursorClose(&cur); return rc; }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pKey = 0, *pVal = 0;
    int nKey = 0, nVal = 0;
    BlameRow *r;

    if( pCur->nRows >= pCur->nAlloc ){
      int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 64;
      BlameRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(BlameRow));
      if( !aNew ){ prollyCursorClose(&cur); return SQLITE_NOMEM; }
      pCur->aRows = aNew;
      pCur->nAlloc = nNew;
    }
    r = &pCur->aRows[pCur->nRows];
    memset(r, 0, sizeof(*r));

    if( flags & PROLLY_NODE_INTKEY ){
      r->intKey = prollyCursorIntKey(&cur);
    }else{
      prollyCursorKey(&cur, &pKey, &nKey);
      if( pKey && nKey>0 ){
        r->pKey = sqlite3_malloc(nKey);
        if( !r->pKey ){ prollyCursorClose(&cur); return SQLITE_NOMEM; }
        memcpy(r->pKey, pKey, nKey);
        r->nKey = nKey;
      }
    }

    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && nVal>0 ){
      r->pCurVal = sqlite3_malloc(nVal);
      if( !r->pCurVal ){ prollyCursorClose(&cur); return SQLITE_NOMEM; }
      memcpy(r->pCurVal, pVal, nVal);
      r->nCurVal = nVal;
    }

    pCur->nRows++;
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ){ prollyCursorClose(&cur); return rc; }
  }
  prollyCursorClose(&cur);
  return SQLITE_OK;
}

/* Seek a single row by its stored key in the given table root and
** return SQLITE_OK with `ppVal`/`pnVal` set to the row's value
** bytes if found, or NULL/0 if the row doesn't exist. On error,
** returns the error code. */
static int blameSeekRowInTree(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const BlameRow *pRow,
  const u8 **ppVal,
  int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppVal = 0;
  *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);

  if( flags & PROLLY_NODE_INTKEY ){
    rc = prollyCursorSeekInt(&cur, pRow->intKey, &res);
  }else{
    rc = prollyCursorSeekBlob(&cur, pRow->pKey, pRow->nKey, &res);
  }
  if( rc!=SQLITE_OK ){ prollyCursorClose(&cur); return rc; }

  if( res!=0 || !prollyCursorIsValid(&cur) ){
    prollyCursorClose(&cur);
    return SQLITE_OK;
  }

  prollyCursorValue(&cur, ppVal, pnVal);
  prollyCursorClose(&cur);
  return SQLITE_OK;
}

/* Returns 1 if pA..+nA and pB..+nB encode the same row value for
** blame purposes. Treats NULL-vs-missing as "missing"; missing vs
** present is always different. */
static int blameRowValueEqual(const u8 *pA, int nA, const u8 *pB, int nB){
  int isA = (pA && nA>0);
  int isB = (pB && nB>0);
  if( !isA && !isB ) return 1;
  if( !isA || !isB ) return 0;
  if( nA != nB ) return 0;
  return memcmp(pA, pB, nA)==0;
}

/* Load a table's root hash and flags at the given catalog hash.
** Returns SQLITE_NOTFOUND if the table doesn't exist there. */
static int blameLoadTableRoot(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  ProllyHash *pOutRoot,
  u8 *pOutFlags
){
  struct TableEntry *aTables = 0;
  int nTables = 0, rc;
  struct TableEntry *pEntry;

  memset(pOutRoot, 0, sizeof(*pOutRoot));
  *pOutFlags = 0;

  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_NOTFOUND;
  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;

  pEntry = doltliteFindTableByName(aTables, nTables, zTableName);
  if( !pEntry ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_NOTFOUND;
  }
  memcpy(pOutRoot, &pEntry->root, sizeof(ProllyHash));
  *pOutFlags = pEntry->flags;
  doltliteFreeCatalog(aTables, nTables);
  return SQLITE_OK;
}

/* Record blame info on pRow. Duplicates the commit's strings since
** the DoltliteCommit is freed immediately after. */
static int blameAssign(
  BlameRow *pRow,
  const ProllyHash *pCommitHash,
  const DoltliteCommit *pCommit
){
  if( pRow->zCommitter ) sqlite3_free(pRow->zCommitter);
  if( pRow->zEmail ) sqlite3_free(pRow->zEmail);
  if( pRow->zMessage ) sqlite3_free(pRow->zMessage);

  doltliteHashToHex(pCommitHash, pRow->zCommit);
  pRow->zCommitter = sqlite3_mprintf("%s", pCommit->zName ? pCommit->zName : "");
  pRow->zEmail     = sqlite3_mprintf("%s", pCommit->zEmail ? pCommit->zEmail : "");
  pRow->zMessage   = sqlite3_mprintf("%s", pCommit->zMessage ? pCommit->zMessage : "");
  pRow->commitDate = pCommit->timestamp;
  pRow->blamed = 1;
  if( !pRow->zCommitter || !pRow->zEmail || !pRow->zMessage ) return SQLITE_NOMEM;
  return SQLITE_OK;
}

/* Compare each unblamed row against its value in pRefRoot (parent
** or merge base) and blame the row to pCommitHash if different.
** pRefRoot may be NULL/empty to indicate "row not present in ref". */
static int blameCompareAgainstRef(
  sqlite3 *db,
  BlameCursor *pCur,
  const char *zTableName,
  const ProllyHash *pCurRoot,
  u8 curFlags,
  const ProllyHash *pRefCatHash,
  const ProllyHash *pCommitHash,
  const DoltliteCommit *pCommit
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash refRoot;
  u8 refFlags = 0;
  int haveRef = 0;
  int rc, i;

  (void)pCurRoot;

  if( pRefCatHash && !prollyHashIsEmpty(pRefCatHash) ){
    rc = blameLoadTableRoot(db, pRefCatHash, zTableName, &refRoot, &refFlags);
    if( rc==SQLITE_OK ) haveRef = 1;
    else if( rc!=SQLITE_NOTFOUND ) return rc;
  }

  for(i=0; i<pCur->nRows; i++){
    BlameRow *r = &pCur->aRows[i];
    const u8 *pRefVal = 0;
    int nRefVal = 0;
    if( r->blamed ) continue;

    if( haveRef ){
      rc = blameSeekRowInTree(cs, pCache, &refRoot, refFlags, r, &pRefVal, &nRefVal);
      if( rc!=SQLITE_OK ) return rc;
    }

    if( !blameRowValueEqual(r->pCurVal, r->nCurVal, pRefVal, nRefVal) ){
      rc = blameAssign(r, pCommitHash, pCommit);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

/* Count rows still awaiting a blame. Used as the walk terminator. */
static int blameUnresolvedCount(BlameCursor *pCur){
  int i, n = 0;
  for(i=0; i<pCur->nRows; i++){
    if( !pCur->aRows[i].blamed ) n++;
  }
  return n;
}

/* Main blame walk. Starts from HEAD, walks first-parent, and at
** each commit applies the linear or merge comparison rule. */
static int blameWalk(
  BlameCursor *pCur,
  sqlite3 *db,
  const char *zTableName
){
  ProllyHash walk;
  int rc;

  doltliteGetSessionHead(db, &walk);
  if( prollyHashIsEmpty(&walk) ) return SQLITE_OK;

  while( !prollyHashIsEmpty(&walk) && blameUnresolvedCount(pCur)>0 ){
    DoltliteCommit commit;
    ProllyHash curTableRoot;
    u8 curFlags = 0;
    int haveCurTable = 0;

    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &walk, &commit);
    if( rc!=SQLITE_OK ) return rc;

    rc = blameLoadTableRoot(db, &commit.catalogHash, zTableName,
                            &curTableRoot, &curFlags);
    if( rc==SQLITE_OK ) haveCurTable = 1;
    else if( rc!=SQLITE_NOTFOUND ){
      doltliteCommitClear(&commit);
      return rc;
    }

    if( commit.nParents >= 2 ){
      /* Merge commit: compare against the merge base of parents. */
      ProllyHash baseHash;
      DoltliteCommit baseCommit;
      memset(&baseHash, 0, sizeof(baseHash));
      rc = doltliteFindAncestor(db, &commit.aParents[0], &commit.aParents[1], &baseHash);
      if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ){
        doltliteCommitClear(&commit);
        return rc;
      }

      if( haveCurTable && !prollyHashIsEmpty(&baseHash) ){
        memset(&baseCommit, 0, sizeof(baseCommit));
        rc = doltliteLoadCommit(db, &baseHash, &baseCommit);
        if( rc==SQLITE_OK ){
          rc = blameCompareAgainstRef(db, pCur, zTableName,
                                      &curTableRoot, curFlags,
                                      &baseCommit.catalogHash,
                                      &walk, &commit);
          doltliteCommitClear(&baseCommit);
        }
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&commit);
          return rc;
        }
      }else if( haveCurTable ){
        /* No merge base (disjoint histories): any live row here is
        ** blamed to this merge. */
        rc = blameCompareAgainstRef(db, pCur, zTableName,
                                    &curTableRoot, curFlags,
                                    0, &walk, &commit);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&commit);
          return rc;
        }
      }
    }else{
      /* Linear commit: compare against first parent's table. */
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, 0);
      if( pParent && !prollyHashIsEmpty(pParent) ){
        DoltliteCommit parentCommit;
        memset(&parentCommit, 0, sizeof(parentCommit));
        rc = doltliteLoadCommit(db, pParent, &parentCommit);
        if( rc==SQLITE_OK ){
          rc = blameCompareAgainstRef(db, pCur, zTableName,
                                      &curTableRoot, curFlags,
                                      &parentCommit.catalogHash,
                                      &walk, &commit);
          doltliteCommitClear(&parentCommit);
        }
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&commit);
          return rc;
        }
      }else if( haveCurTable ){
        /* Root commit: any unblamed row that exists here is blamed
        ** to the root. */
        rc = blameCompareAgainstRef(db, pCur, zTableName,
                                    &curTableRoot, curFlags,
                                    0, &walk, &commit);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&commit);
          return rc;
        }
      }
    }

    /* Advance first-parent. */
    {
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, 0);
      if( pParent && !prollyHashIsEmpty(pParent) ){
        walk = *pParent;
      }else{
        memset(&walk, 0, sizeof(walk));
      }
    }
    doltliteCommitClear(&commit);
  }

  return SQLITE_OK;
}

static int bmConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  BlameVtab *v;
  int rc;
  const char *zMod;
  char *zSchema;
  (void)pAux;

  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  v->intPkCid = -1;

  zMod = argv[0];
  if( zMod && strncmp(zMod, "dolt_blame_", 11)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zMod + 11);
  }else if( argc>3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }
  if( !v->zTableName ){ sqlite3_free(v); return SQLITE_NOMEM; }

  rc = blameLoadPkColumns(db, v->zTableName,
                          &v->azPkNames, &v->aPkColIdx, &v->nPkCols,
                          &v->intPkCid);
  if( rc!=SQLITE_OK ){
    if( pzErr ){
      *pzErr = sqlite3_mprintf("dolt_blame_%s: failed to read schema", v->zTableName);
    }
    sqlite3_free(v->zTableName);
    sqlite3_free(v);
    return rc;
  }
  if( v->nPkCols == 0 ){
    if( pzErr ){
      *pzErr = sqlite3_mprintf(
        "dolt_blame_%s: table has no primary key", v->zTableName);
    }
    sqlite3_free(v->zTableName);
    sqlite3_free(v);
    return SQLITE_ERROR;
  }

  zSchema = blameBuildSchema(v);
  if( !zSchema ){
    blameFreePkColumns(v->azPkNames, v->aPkColIdx, v->nPkCols);
    sqlite3_free(v->zTableName);
    sqlite3_free(v);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    blameFreePkColumns(v->azPkNames, v->aPkColIdx, v->nPkCols);
    sqlite3_free(v->zTableName);
    sqlite3_free(v);
    return rc;
  }

  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int bmDisconnect(sqlite3_vtab *pVtab){
  BlameVtab *v = (BlameVtab*)pVtab;
  blameFreePkColumns(v->azPkNames, v->aPkColIdx, v->nPkCols);
  sqlite3_free(v->zTableName);
  sqlite3_free(v);
  return SQLITE_OK;
}

static int bmBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 100000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

static int bmOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  BlameCursor *c;
  (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *ppCursor = &c->base;
  return SQLITE_OK;
}

static int bmClose(sqlite3_vtab_cursor *pCursor){
  BlameCursor *c = (BlameCursor*)pCursor;
  blameFreeRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int bmFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  BlameCursor *c = (BlameCursor*)pCursor;
  BlameVtab *v = (BlameVtab*)pCursor->pVtab;
  sqlite3 *db = v->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash headHash;
  DoltliteCommit headCommit;
  ProllyHash tableRoot;
  u8 tableFlags = 0;
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  blameFreeRows(c);
  c->iRow = 0;

  if( !cs || !pCache ) return SQLITE_OK;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;

  memset(&headCommit, 0, sizeof(headCommit));
  rc = doltliteLoadCommit(db, &headHash, &headCommit);
  if( rc!=SQLITE_OK ) return rc;

  rc = blameLoadTableRoot(db, &headCommit.catalogHash, v->zTableName,
                          &tableRoot, &tableFlags);
  doltliteCommitClear(&headCommit);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  rc = blameCollectLiveRows(c, cs, pCache, &tableRoot, tableFlags);
  if( rc!=SQLITE_OK ){ blameFreeRows(c); return rc; }

  rc = blameWalk(c, db, v->zTableName);
  if( rc!=SQLITE_OK ){ blameFreeRows(c); return rc; }

  return SQLITE_OK;
}

static int bmNext(sqlite3_vtab_cursor *pCursor){
  ((BlameCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int bmEof(sqlite3_vtab_cursor *pCursor){
  BlameCursor *c = (BlameCursor*)pCursor;
  return c->iRow >= c->nRows;
}

static int bmColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  BlameCursor *c = (BlameCursor*)pCursor;
  BlameVtab *v = (BlameVtab*)pCursor->pVtab;
  BlameRow *r;
  int nPk = v->nPkCols;

  if( c->iRow >= c->nRows ) return SQLITE_OK;
  r = &c->aRows[c->iRow];

  if( iCol < nPk ){
    int cid = v->aPkColIdx[iCol];
    if( cid == v->intPkCid ){
      /* Rowid-alias INTEGER PK — value is the cursor's intKey, not
      ** a record field. */
      sqlite3_result_int64(ctx, r->intKey);
    }else if( r->pCurVal && r->nCurVal>0 ){
      DoltliteRecordInfo ri;
      /* Non-rowid PK tables are auto-converted to WITHOUT ROWID in
      ** build.c, so records store the PK columns first in PRIMARY KEY
      ** declaration order. aPkColIdx is sorted by pkPos, so iCol is
      ** exactly the PK col's record-field index regardless of its
      ** declared cid. (Blame projects only PK cols so non-PK record
      ** fields never come into play here.) */
      doltliteParseRecord(r->pCurVal, r->nCurVal, &ri);
      if( iCol < ri.nField ){
        doltliteResultField(ctx, r->pCurVal, r->nCurVal,
                            ri.aType[iCol], ri.aOffset[iCol]);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      sqlite3_result_null(ctx);
    }
    return SQLITE_OK;
  }

  switch( iCol - nPk ){
    case 0:
      if( r->blamed ) sqlite3_result_text(ctx, r->zCommit, -1, SQLITE_TRANSIENT);
      else sqlite3_result_null(ctx);
      break;
    case 1: {
      time_t t;
      struct tm *tm;
      if( !r->blamed ){ sqlite3_result_null(ctx); break; }
      t = (time_t)r->commitDate;
      tm = gmtime(&t);
      if( tm ){
        char buf[32];
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
        sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 2:
      sqlite3_result_text(ctx, r->zCommitter ? r->zCommitter : "", -1, SQLITE_TRANSIENT);
      break;
    case 3:
      sqlite3_result_text(ctx, r->zEmail ? r->zEmail : "", -1, SQLITE_TRANSIENT);
      break;
    case 4:
      sqlite3_result_text(ctx, r->zMessage ? r->zMessage : "", -1, SQLITE_TRANSIENT);
      break;
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int bmRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((BlameCursor*)pCursor)->iRow;
  return SQLITE_OK;
}

static sqlite3_module doltliteBlameModule = {
  0, bmConnect, bmConnect, bmBestIndex, bmDisconnect, bmDisconnect,
  bmOpen, bmClose, bmFilter, bmNext, bmEof, bmColumn, bmRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteRegisterBlameTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_blame_", &doltliteBlameModule);
}

#endif
