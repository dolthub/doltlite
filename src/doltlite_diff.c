
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

/* Per-row diff (legacy form: dolt_diff('table', from, to)). */
typedef struct DiffRow DiffRow;
struct DiffRow {
  u8 type;
  i64 intKey;
  u8 *pOldVal;
  int nOldVal;
  u8 *pNewVal;
  int nNewVal;
};

/* Summary row (no-arg form: SELECT * FROM dolt_diff). One row per
** (commit, changed_table) describing whether the table's data and/or
** schema changed at that commit relative to its first parent. */
typedef struct DiffSummaryRow DiffSummaryRow;
struct DiffSummaryRow {
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zTableName;
  char *zCommitter;
  char *zEmail;
  i64  timestamp;
  char *zMessage;
  u8   dataChange;
  u8   schemaChange;
};

typedef struct DoltliteDiffVtab DoltliteDiffVtab;
struct DoltliteDiffVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

/* idxNum bit layout:
**   bit 0: table_name constraint present (per-row legacy mode)
**   bit 1: from_commit constraint present
**   bit 2: to_commit  constraint present
** When bit 0 is clear, the cursor runs in summary mode and walks the
** commit history. */
#define DIFF_IDX_TABLE_NAME  0x01
#define DIFF_IDX_FROM_COMMIT 0x02
#define DIFF_IDX_TO_COMMIT   0x04

typedef struct DoltliteDiffCursor DoltliteDiffCursor;
struct DoltliteDiffCursor {
  sqlite3_vtab_cursor base;
  int isSummary;
  /* Per-row legacy mode */
  DiffRow *aRows;
  int nRows;
  int iPkField;
  /* Summary mode */
  DiffSummaryRow *aSummary;
  int nSummary;
  /* Shared cursor position */
  int iRow;
};

/* The schema declares both the summary-form columns (visible, matching
** Dolt's `SELECT * FROM dolt_diff`) and the legacy per-row form columns
** (visible, retained for the 90+ existing self-tests that select them by
** name). The HIDDEN trailing columns are the function-call args that
** trigger per-row mode. In summary-mode rows, the per-row columns are
** NULL; in per-row-mode rows, the summary columns are NULL. */
static const char *diffSchema =
  "CREATE TABLE x("
  "  commit_hash   TEXT,"     /* 0  summary */
  "  committer     TEXT,"     /* 1  summary */
  "  email         TEXT,"     /* 2  summary */
  "  date          TEXT,"     /* 3  summary */
  "  message       TEXT,"     /* 4  summary */
  "  data_change   INTEGER,"  /* 5  summary */
  "  schema_change INTEGER,"  /* 6  summary */
  "  diff_type     TEXT,"     /* 7  per-row */
  "  rowid_val     INTEGER,"  /* 8  per-row */
  "  from_value    TEXT,"     /* 9  per-row */
  "  to_value      TEXT,"     /* 10 per-row */
  "  table_name    TEXT HIDDEN,"  /* 11 constraint */
  "  from_commit   TEXT HIDDEN,"  /* 12 constraint */
  "  to_commit     TEXT HIDDEN"   /* 13 constraint */
  ")";

#define DIFF_COL_COMMIT_HASH   0
#define DIFF_COL_COMMITTER     1
#define DIFF_COL_EMAIL         2
#define DIFF_COL_DATE          3
#define DIFF_COL_MESSAGE       4
#define DIFF_COL_DATA_CHANGE   5
#define DIFF_COL_SCHEMA_CHANGE 6
#define DIFF_COL_DIFF_TYPE     7
#define DIFF_COL_ROWID_VAL     8
#define DIFF_COL_FROM_VALUE    9
#define DIFF_COL_TO_VALUE      10
#define DIFF_COL_TABLE_NAME    11
#define DIFF_COL_FROM_COMMIT   12
#define DIFF_COL_TO_COMMIT     13

static int diffDupValue(const u8 *pIn, int nIn, u8 **ppOut){
  u8 *pCopy;
  *ppOut = 0;
  if( !pIn || nIn<=0 ) return SQLITE_OK;
  pCopy = sqlite3_malloc(nIn);
  if( !pCopy ) return SQLITE_NOMEM;
  memcpy(pCopy, pIn, nIn);
  *ppOut = pCopy;
  return SQLITE_OK;
}

static int detectPkField(sqlite3 *db, const char *zTable){
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  int rc, pkField = -1, colIdx = 0;

  zSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
  if( !zSql ) return -1;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return -1;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    int pk = sqlite3_column_int(pStmt, 5);
    if( pk==1 ){
      const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
      if( zType && sqlite3_stricmp(zType, "INTEGER")==0 ){
        
        sqlite3_finalize(pStmt);
        return -1;
      }
      
      pkField = colIdx;
    }
    colIdx++;
  }
  sqlite3_finalize(pStmt);
  return pkField;
}

static int diffCollect(void *pCtx, const ProllyDiffChange *pChange){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCtx;
  DiffRow *aNew;
  DiffRow *r;
  int rc;

  aNew = sqlite3_realloc(pCur->aRows, (pCur->nRows+1)*(int)sizeof(DiffRow));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aRows = aNew;
  r = &aNew[pCur->nRows];
  memset(r, 0, sizeof(*r));

  r->type = pChange->type;
  r->intKey = pChange->intKey;

  if( pChange->pOldVal && pChange->nOldVal>0 ){
    rc = diffDupValue(pChange->pOldVal, pChange->nOldVal, &r->pOldVal);
    if( rc!=SQLITE_OK ) return rc;
    r->nOldVal = pChange->nOldVal;
  }
  if( pChange->pNewVal && pChange->nNewVal>0 ){
    rc = diffDupValue(pChange->pNewVal, pChange->nNewVal, &r->pNewVal);
    if( rc!=SQLITE_OK ){
      sqlite3_free(r->pOldVal);
      r->pOldVal = 0;
      r->nOldVal = 0;
      return rc;
    }
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
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
}

static void freeSummaryRows(DoltliteDiffCursor *pCur){
  int i;
  for(i=0; i<pCur->nSummary; i++){
    sqlite3_free(pCur->aSummary[i].zTableName);
    sqlite3_free(pCur->aSummary[i].zCommitter);
    sqlite3_free(pCur->aSummary[i].zEmail);
    sqlite3_free(pCur->aSummary[i].zMessage);
  }
  sqlite3_free(pCur->aSummary);
  pCur->aSummary = 0;
  pCur->nSummary = 0;
}

/* Append one summary row to the cursor. If pCommit is NULL, the row
** represents the working set: committer/email/message are left NULL
** and timestamp is 0. */
static int appendSummaryRow(DoltliteDiffCursor *pCur,
                            const char *zCommitHex,
                            const char *zTableName,
                            const DoltliteCommit *pCommit,
                            u8 dataChange, u8 schemaChange){
  DiffSummaryRow *aNew, *r;
  aNew = sqlite3_realloc(pCur->aSummary,
                         (pCur->nSummary+1)*(int)sizeof(DiffSummaryRow));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aSummary = aNew;
  r = &aNew[pCur->nSummary];
  memset(r, 0, sizeof(*r));
  memcpy(r->zCommitHex, zCommitHex, PROLLY_HASH_SIZE*2+1);
  r->zTableName = sqlite3_mprintf("%s", zTableName ? zTableName : "");
  if( !r->zTableName ) return SQLITE_NOMEM;
  if( pCommit ){
    r->zCommitter = sqlite3_mprintf("%s", pCommit->zName  ? pCommit->zName  : "");
    r->zEmail     = sqlite3_mprintf("%s", pCommit->zEmail ? pCommit->zEmail : "");
    r->zMessage   = sqlite3_mprintf("%s", pCommit->zMessage ? pCommit->zMessage : "");
    if( !r->zCommitter || !r->zEmail || !r->zMessage ){
      sqlite3_free(r->zTableName);
      sqlite3_free(r->zCommitter);
      sqlite3_free(r->zEmail);
      sqlite3_free(r->zMessage);
      return SQLITE_NOMEM;
    }
    r->timestamp = pCommit->timestamp;
  }
  /* Else: zCommitter, zEmail, zMessage stay NULL; timestamp stays 0. */
  r->dataChange   = dataChange;
  r->schemaChange = schemaChange;
  pCur->nSummary++;
  return SQLITE_OK;
}

/* Compare HEAD against the working set and emit one summary row per
** table that differs. The synthetic commit_hash for the working-set
** row is "WORKING", matching Dolt. */
static int collectWorkingSetSummary(DoltliteDiffCursor *pCur, sqlite3 *db){
  ProllyHash headCat, workCat;
  struct TableEntry *aHead = 0, *aWork = 0;
  int nHead = 0, nWork = 0;
  int rc, i;
  static const char zWorkingHex[] = "WORKING";
  char zHexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&headCat, 0, sizeof(headCat));
  memset(&workCat, 0, sizeof(workCat));

  rc = doltliteGetHeadCatalogHash(db, &headCat);
  if( rc!=SQLITE_OK ){
    /* No HEAD yet (fresh repo): no commits to compare against, skip. */
    return SQLITE_OK;
  }
  rc = doltliteFlushCatalogToHash(db, &workCat);
  if( rc!=SQLITE_OK ) return rc;

  /* Identical catalogs => no working-set diff. */
  if( prollyHashCompare(&headCat, &workCat)==0 ) return SQLITE_OK;

  rc = doltliteLoadCatalog(db, &headCat, &aHead, &nHead, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, &workCat, &aWork, &nWork, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aHead);
    return rc;
  }

  memset(zHexBuf, 0, sizeof(zHexBuf));
  memcpy(zHexBuf, zWorkingHex, sizeof(zWorkingHex));

  for(i=0; i<nWork; i++){
    struct TableEntry *e = &aWork[i];
    struct TableEntry *p;
    u8 dataChange, schemaChange;
    if( !e->zName ) continue;
    p = doltliteFindTableByName(aHead, nHead, e->zName);
    if( !p ){
      dataChange = 1;
      schemaChange = 1;
    }else{
      dataChange   = (prollyHashCompare(&e->root, &p->root) != 0) ? 1 : 0;
      schemaChange = (prollyHashCompare(&e->schemaHash, &p->schemaHash) != 0) ? 1 : 0;
      if( !dataChange && !schemaChange ) continue;
    }
    rc = appendSummaryRow(pCur, zHexBuf, e->zName, 0, dataChange, schemaChange);
    if( rc!=SQLITE_OK ) goto done;
  }
  for(i=0; i<nHead; i++){
    struct TableEntry *p = &aHead[i];
    if( !p->zName ) continue;
    if( doltliteFindTableByName(aWork, nWork, p->zName) ) continue;
    rc = appendSummaryRow(pCur, zHexBuf, p->zName, 0, 1, 1);
    if( rc!=SQLITE_OK ) goto done;
  }

done:
  sqlite3_free(aHead);
  sqlite3_free(aWork);
  return rc;
}

/* For one commit, compare its catalog against its first parent's catalog
** (or against an empty catalog if it has no parent) and emit one summary
** row per table that changed. */
static int collectSummaryForCommit(DoltliteDiffCursor *pCur, sqlite3 *db,
                                   const ProllyHash *pCommitHash,
                                   const DoltliteCommit *pCommit,
                                   const char *zCommitHex){
  struct TableEntry *aChild = 0, *aParent = 0;
  int nChild = 0, nParent = 0;
  int rc, i;
  int hasParent = (pCommit->nParents > 0
                   && !prollyHashIsEmpty(&pCommit->aParents[0]));
  (void)pCommitHash;

  rc = doltliteLoadCatalog(db, &pCommit->catalogHash, &aChild, &nChild, 0);
  if( rc!=SQLITE_OK ) return rc;

  if( hasParent ){
    DoltliteCommit parent;
    memset(&parent, 0, sizeof(parent));
    rc = doltliteLoadCommit(db, &pCommit->aParents[0], &parent);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &parent.catalogHash, &aParent, &nParent, 0);
      doltliteCommitClear(&parent);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(aChild);
      return rc;
    }
  }

  /* Tables present in the child commit. */
  for(i=0; i<nChild; i++){
    struct TableEntry *e = &aChild[i];
    struct TableEntry *p;
    u8 dataChange, schemaChange;
    if( !e->zName ) continue;  /* skip sqlite_master */
    p = doltliteFindTableByName(aParent, nParent, e->zName);
    if( !p ){
      /* Newly added in this commit. */
      dataChange = 1;
      schemaChange = 1;
    }else{
      dataChange   = (prollyHashCompare(&e->root, &p->root) != 0) ? 1 : 0;
      schemaChange = (prollyHashCompare(&e->schemaHash, &p->schemaHash) != 0) ? 1 : 0;
      if( !dataChange && !schemaChange ) continue;
    }
    rc = appendSummaryRow(pCur, zCommitHex, e->zName, pCommit,
                          dataChange, schemaChange);
    if( rc!=SQLITE_OK ) goto done;
  }

  /* Tables present in the parent but absent from the child = dropped. */
  for(i=0; i<nParent; i++){
    struct TableEntry *p = &aParent[i];
    if( !p->zName ) continue;
    if( doltliteFindTableByName(aChild, nChild, p->zName) ) continue;
    rc = appendSummaryRow(pCur, zCommitHex, p->zName, pCommit, 1, 1);
    if( rc!=SQLITE_OK ) goto done;
  }

done:
  sqlite3_free(aChild);
  sqlite3_free(aParent);
  return rc;
}

/* Walk the commit graph from HEAD, BFS over all parents (matching
** dolt_log), and emit summary rows for every (commit, changed_table). */
static int collectSummary(DoltliteDiffCursor *pCur, sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash head;
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 0;
  ProllyHashSet visited, queued;
  int visInit = 0, queInit = 0;
  int rc = SQLITE_OK;
  int i;

  if( !cs ) return SQLITE_OK;

  /* Working-set diff is emitted first, matching Dolt's ordering
  ** (newest changes at the top). Empty if HEAD == working catalog. */
  rc = collectWorkingSetSummary(pCur, db);
  if( rc!=SQLITE_OK ) return rc;

  doltliteGetSessionHead(db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;

  rc = prollyHashSetInit(&visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  visInit = 1;
  rc = prollyHashSetInit(&queued, 64);
  if( rc!=SQLITE_OK ) goto walk_done;
  queInit = 1;

  qAlloc = 16;
  queue = sqlite3_malloc(qAlloc*(int)sizeof(ProllyHash));
  if( !queue ){ rc = SQLITE_NOMEM; goto walk_done; }
  queue[qTail++] = head;
  rc = prollyHashSetAdd(&queued, &head);
  if( rc!=SQLITE_OK ) goto walk_done;

  while( qHead<qTail ){
    ProllyHash cur = queue[qHead++];
    DoltliteCommit commit;
    char zHex[PROLLY_HASH_SIZE*2+1];

    if( prollyHashSetContains(&visited, &cur) ) continue;
    rc = prollyHashSetAdd(&visited, &cur);
    if( rc!=SQLITE_OK ) break;

    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ) break;

    doltliteHashToHex(&cur, zHex);
    rc = collectSummaryForCommit(pCur, db, &cur, &commit, zHex);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      break;
    }

    for(i=0; i<commit.nParents; i++){
      if( prollyHashIsEmpty(&commit.aParents[i]) ) continue;
      if( prollyHashSetContains(&visited, &commit.aParents[i]) ) continue;
      if( prollyHashSetContains(&queued,  &commit.aParents[i]) ) continue;
      if( qTail>=qAlloc ){
        int newAlloc = qAlloc*2;
        ProllyHash *tmp = sqlite3_realloc(queue,
                                          newAlloc*(int)sizeof(ProllyHash));
        if( !tmp ){ rc = SQLITE_NOMEM; break; }
        queue = tmp; qAlloc = newAlloc;
      }
      queue[qTail++] = commit.aParents[i];
      rc = prollyHashSetAdd(&queued, &commit.aParents[i]);
      if( rc!=SQLITE_OK ) break;
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
  }

walk_done:
  sqlite3_free(queue);
  if( visInit ) prollyHashSetFree(&visited);
  if( queInit ) prollyHashSetFree(&queued);
  return rc;
}

static int resolveCommitToTableRoot(
  sqlite3 *db, const ProllyHash *pCommitHash, Pgno iTable,
  ProllyHash *pRoot, u8 *pFlags
){
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int rc;

  rc = doltliteLoadCommit(db, pCommitHash, &commit);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteFindTableRoot(aTables, nTables, iTable, pRoot, pFlags);
  sqlite3_free(aTables);
  return rc;
}

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
      case DIFF_COL_TABLE_NAME:  iTableName  = i; break;
      case DIFF_COL_FROM_COMMIT: iFromCommit = i; break;
      case DIFF_COL_TO_COMMIT:   iToCommit   = i; break;
    }
  }

  if( iTableName>=0 ){
    pInfo->aConstraintUsage[iTableName].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTableName].omit = 1;
  }
  if( iFromCommit>=0 ){
    pInfo->aConstraintUsage[iFromCommit].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFromCommit].omit = 1;
  }
  if( iToCommit>=0 ){
    pInfo->aConstraintUsage[iToCommit].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iToCommit].omit = 1;
  }

  pInfo->idxNum = (iTableName>=0  ? DIFF_IDX_TABLE_NAME  : 0)
                | (iFromCommit>=0 ? DIFF_IDX_FROM_COMMIT : 0)
                | (iToCommit>=0   ? DIFF_IDX_TO_COMMIT   : 0);
  /* Summary mode (no table_name) walks the full commit history; per-row
  ** mode is bounded by a single (table, from, to) lookup. */
  pInfo->estimatedCost = (iTableName>=0) ? 1000.0 : 100000.0;
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
  freeSummaryRows(pCur);
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
  ProllyHash oldRoot, newRoot, headCatHash, workingCatHash;
  u8 flags = 0;
  Pgno iTable;
  int rc = SQLITE_OK;
  int argIdx = 0;
  struct TableEntry *aHead = 0;
  struct TableEntry *aWork = 0;
  int nHead = 0;
  int nWork = 0;
  (void)idxStr;

  freeDiffRows(pCur);
  freeSummaryRows(pCur);
  pCur->iRow = 0;
  pCur->isSummary = 0;
  memset(&oldRoot, 0, sizeof(oldRoot));
  memset(&newRoot, 0, sizeof(newRoot));
  memset(&headCatHash, 0, sizeof(headCatHash));
  memset(&workingCatHash, 0, sizeof(workingCatHash));

  if( !cs || !pBt ) return SQLITE_OK;

  /* No table_name constraint -> commits-touching-tables summary mode. */
  if( (idxNum & DIFF_IDX_TABLE_NAME)==0 ){
    pCur->isSummary = 1;
    return collectSummary(pCur, db);
  }

  if( (idxNum & DIFF_IDX_TABLE_NAME) && argIdx<argc ){
    zTableName = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & DIFF_IDX_FROM_COMMIT) && argIdx<argc ){
    zFromCommit = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & DIFF_IDX_TO_COMMIT) && argIdx<argc ){
    zToCommit = (const char*)sqlite3_value_text(argv[argIdx++]);
  }

  if( !zTableName ) return SQLITE_OK;

  pCur->iPkField = detectPkField(db, zTableName);

  rc = doltliteResolveTableName(db, zTableName, &iTable);
  if( rc!=SQLITE_OK ){
    /* The named object isn't a user table — but the user may be
    ** filtering dolt_diff's summary output by table_name (e.g.
    ** `SELECT * FROM dolt_diff WHERE table_name='dolt_schemas'`).
    ** The HIDDEN column + bestIndex routing can't distinguish a
    ** filter from a TVF call, so fall through to summary mode and
    ** filter the result by name. Only do this when no commit bounds
    ** are specified — a 3-arg call names a missing table for a
    ** real reason and should stay empty. */
    if( zFromCommit==0 && zToCommit==0 ){
      int i;
      pCur->isSummary = 1;
      rc = collectSummary(pCur, db);
      if( rc!=SQLITE_OK ) return rc;
      /* In-place filter. Shift-compact kept rows to the front. */
      {
        int out = 0;
        for(i=0; i<pCur->nSummary; i++){
          if( pCur->aSummary[i].zTableName
           && strcmp(pCur->aSummary[i].zTableName, zTableName)==0 ){
            if( out!=i ) pCur->aSummary[out] = pCur->aSummary[i];
            out++;
          }else{
            sqlite3_free(pCur->aSummary[i].zTableName);
            sqlite3_free(pCur->aSummary[i].zCommitter);
            sqlite3_free(pCur->aSummary[i].zEmail);
            sqlite3_free(pCur->aSummary[i].zMessage);
          }
        }
        pCur->nSummary = out;
      }
    }
    return SQLITE_OK;
  }

  
  if( zFromCommit ){
    ProllyHash fromCommitHash;
    rc = doltliteResolveRef(db, zFromCommit, &fromCommitHash);
    if( rc==SQLITE_OK ){
      rc = resolveCommitToTableRoot(db, &fromCommitHash, iTable, &oldRoot, &flags);
      if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    }
  }else{
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
      if( rc==SQLITE_OK ){
        rc = doltliteFindTableRoot(aHead, nHead, iTable, &oldRoot, &flags);
        if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
      }
    }
  }
  if( rc!=SQLITE_OK ) goto diff_filter_done;

  
  if( zToCommit ){
    ProllyHash toCommitHash;
    u8 f2;
    rc = doltliteResolveRef(db, zToCommit, &toCommitHash);
    if( rc==SQLITE_OK ){
      rc = resolveCommitToTableRoot(db, &toCommitHash, iTable, &newRoot, &f2);
      if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    }
    if( flags==0 ) flags = f2;
  }else{
    rc = doltliteFlushCatalogToHash(db, &workingCatHash);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &workingCatHash, &aWork, &nWork, 0);
      if( rc==SQLITE_OK ){
        rc = doltliteFindTableRoot(aWork, nWork, iTable, &newRoot, &flags);
        if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
      }
    }
  }
  if( rc!=SQLITE_OK ) goto diff_filter_done;

  
  if( prollyHashCompare(&oldRoot, &newRoot)==0 ) goto diff_filter_done;

  
  {
    ProllyCache *pCache = doltliteGetCache(db);

    rc = prollyDiff(cs, pCache, &oldRoot, &newRoot, flags,
                    diffCollect, (void*)pCur);
  }

diff_filter_done:
  sqlite3_free(aHead);
  sqlite3_free(aWork);
  return rc;
}

static int diffNext(sqlite3_vtab_cursor *pCursor){
  ((DoltliteDiffCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int diffEof(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  if( pCur->isSummary ) return pCur->iRow >= pCur->nSummary;
  return pCur->iRow >= pCur->nRows;
}

static int summaryColumn(DoltliteDiffCursor *pCur, sqlite3_context *ctx,
                         int iCol){
  DiffSummaryRow *r;
  if( pCur->iRow >= pCur->nSummary ) return SQLITE_OK;
  r = &pCur->aSummary[pCur->iRow];
  switch( iCol ){
    case DIFF_COL_COMMIT_HASH:
      sqlite3_result_text(ctx, r->zCommitHex, -1, SQLITE_TRANSIENT);
      break;
    case DIFF_COL_COMMITTER:
      if( r->zCommitter ){
        sqlite3_result_text(ctx, r->zCommitter, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_EMAIL:
      if( r->zEmail ){
        sqlite3_result_text(ctx, r->zEmail, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_DATE: {
      /* Working-set rows have no timestamp; emit NULL to match Dolt. */
      if( r->timestamp==0 && r->zCommitter==0 ){
        sqlite3_result_null(ctx);
      }else{
        time_t t = (time_t)r->timestamp;
        struct tm *tm = gmtime(&t);
        if( tm ){
          char buf[32];
          strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
          sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
        }else{
          sqlite3_result_null(ctx);
        }
      }
      break;
    }
    case DIFF_COL_MESSAGE:
      if( r->zMessage ){
        sqlite3_result_text(ctx, r->zMessage, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_DATA_CHANGE:
      sqlite3_result_int(ctx, r->dataChange ? 1 : 0);
      break;
    case DIFF_COL_SCHEMA_CHANGE:
      sqlite3_result_int(ctx, r->schemaChange ? 1 : 0);
      break;
    case DIFF_COL_TABLE_NAME:
      sqlite3_result_text(ctx, r->zTableName, -1, SQLITE_TRANSIENT);
      break;
    default:
      /* Per-row legacy columns and the from/to_commit constraint slots
      ** are NULL in summary mode. */
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int diffColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DiffRow *r;

  if( pCur->isSummary ) return summaryColumn(pCur, ctx, iCol);

  if( pCur->iRow >= pCur->nRows ) return SQLITE_OK;
  r = &pCur->aRows[pCur->iRow];

  switch( iCol ){
    case DIFF_COL_DIFF_TYPE:
      switch( r->type ){
        case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
        case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
        case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
      }
      break;
    case DIFF_COL_ROWID_VAL:
      if( pCur->iPkField >= 0 ){
        const u8 *pRec = r->pNewVal ? r->pNewVal : r->pOldVal;
        int nRec = r->pNewVal ? r->nNewVal : r->nOldVal;
        doltliteResultRecordPkField(ctx, pRec, nRec, pCur->iPkField);
      }else{
        sqlite3_result_int64(ctx, r->intKey);
      }
      break;
    case DIFF_COL_FROM_VALUE:
      doltliteResultRecord(ctx, r->pOldVal, r->nOldVal);
      break;
    case DIFF_COL_TO_VALUE:
      doltliteResultRecord(ctx, r->pNewVal, r->nNewVal);
      break;
    default:
      /* Summary columns and the constraint columns are NULL in per-row
      ** mode. */
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int diffRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteDiffCursor*)pCursor)->iRow;
  return SQLITE_OK;
}

static sqlite3_module doltliteDiffModule = {
  0, 0, diffConnect, diffBestIndex, diffDisconnect, 0,
  diffOpen, diffClose, diffFilter, diffNext, diffEof,
  diffColumn, diffRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_diff", &doltliteDiffModule, 0);
}

#endif 
