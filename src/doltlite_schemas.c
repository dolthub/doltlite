

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

#define SCHEMAS_IDX_TYPE_EQ 0x01
#define SCHEMAS_IDX_NAME_EQ 0x02

typedef struct SchemasRow SchemasRow;
struct SchemasRow {
  char *zType;
  char *zName;
  char *zFragment;
};

typedef struct SchemasVtab SchemasVtab;
struct SchemasVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct SchemasCursor SchemasCursor;
struct SchemasCursor {
  sqlite3_vtab_cursor base;
  SchemasRow *aRows;
  int nRows;
  int iRow;
};

static const char *zSchemasSchema =
  "CREATE TABLE x("
  "  type     TEXT,"
  "  name     TEXT,"
  "  fragment TEXT,"
  "  extra    TEXT,"
  "  sql_mode TEXT"
  ")";

static int schemasConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  SchemasVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zSchemasSchema, sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (SchemasVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int schemasBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i;
  int iTypeEq = -1;
  int iNameEq = -1;
  int idxNum = 0;
  int argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( !doltliteVtabConstraintIsBinary(pInfo, i) ) continue;
    if( pC->iColumn==0 && iTypeEq<0 ){
      iTypeEq = i;
    }else if( pC->iColumn==1 && iNameEq<0 ){
      iNameEq = i;
    }
  }

  if( iTypeEq>=0 ){
    pInfo->aConstraintUsage[iTypeEq].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTypeEq].omit = 0;
    idxNum |= SCHEMAS_IDX_TYPE_EQ;
  }
  if( iNameEq>=0 ){
    pInfo->aConstraintUsage[iNameEq].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iNameEq].omit = 0;
    idxNum |= SCHEMAS_IDX_NAME_EQ;
  }

  pInfo->idxNum = idxNum;
  if( idxNum & SCHEMAS_IDX_NAME_EQ ){
    pInfo->estimatedCost = 10.0;
    pInfo->estimatedRows = 1;
  }else if( idxNum & SCHEMAS_IDX_TYPE_EQ ){
    pInfo->estimatedCost = 50.0;
    pInfo->estimatedRows = 10;
  }else{
    pInfo->estimatedCost = 100.0;
    pInfo->estimatedRows = 10;
  }
  return SQLITE_OK;
}

static int schemasOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(SchemasCursor));
}

static void freeRows(SchemasCursor *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aRows[i].zType);
    sqlite3_free(pCur->aRows[i].zName);
    sqlite3_free(pCur->aRows[i].zFragment);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
}

static int schemasClose(sqlite3_vtab_cursor *pCursor){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  freeRows(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int schemasFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  SchemasVtab *pVtab = (SchemasVtab*)pCursor->pVtab;
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pSql;
  int rc;
  int iArg = 0;
  char *zSql;
  (void)idxStr;

  freeRows(pCur);
  pCur->iRow = 0;

  pSql = sqlite3_str_new(pVtab->db);
  if( !pSql ) return SQLITE_NOMEM;
  sqlite3_str_appendall(pSql,
    "SELECT type, name, sql FROM sqlite_schema "
    "WHERE type IN ('view','trigger')");
  if( idxNum & SCHEMAS_IDX_TYPE_EQ ){
    sqlite3_str_appendall(pSql, " AND type=?");
  }
  if( idxNum & SCHEMAS_IDX_NAME_EQ ){
    sqlite3_str_appendall(pSql, " AND name=?");
  }
  sqlite3_str_appendall(pSql, " ORDER BY type, name");
  zSql = sqlite3_str_finish(pSql);
  if( !zSql ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(pVtab->db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;
  if( idxNum & SCHEMAS_IDX_TYPE_EQ ){
    iArg++;
    if( iArg>argc ){
      sqlite3_finalize(pStmt);
      return SQLITE_OK;
    }
    sqlite3_bind_value(pStmt, iArg, argv[iArg-1]);
  }
  if( idxNum & SCHEMAS_IDX_NAME_EQ ){
    iArg++;
    if( iArg>argc ){
      sqlite3_finalize(pStmt);
      return SQLITE_OK;
    }
    sqlite3_bind_value(pStmt, iArg, argv[iArg-1]);
  }

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zType = (const char*)sqlite3_column_text(pStmt, 0);
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zSql  = (const char*)sqlite3_column_text(pStmt, 2);
    SchemasRow *aNew = sqlite3_realloc(pCur->aRows,
                                       (pCur->nRows+1)*(int)sizeof(SchemasRow));
    if( !aNew ){ rc = SQLITE_NOMEM; break; }
    pCur->aRows = aNew;
    memset(&pCur->aRows[pCur->nRows], 0, sizeof(SchemasRow));
    pCur->aRows[pCur->nRows].zType     = sqlite3_mprintf("%s", zType ? zType : "");
    pCur->aRows[pCur->nRows].zName     = sqlite3_mprintf("%s", zName ? zName : "");
    pCur->aRows[pCur->nRows].zFragment = sqlite3_mprintf("%s", zSql  ? zSql  : "");
    if( !pCur->aRows[pCur->nRows].zType
     || !pCur->aRows[pCur->nRows].zName
     || !pCur->aRows[pCur->nRows].zFragment ){
      rc = SQLITE_NOMEM;
      break;
    }
    pCur->nRows++;
  }
  sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
    freeRows(pCur);
    return rc;
  }
  return SQLITE_OK;
}

static int schemasNext(sqlite3_vtab_cursor *pCursor){
  ((SchemasCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int schemasEof(sqlite3_vtab_cursor *pCursor){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  return pCur->iRow >= pCur->nRows;
}

static int schemasColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  SchemasRow *r;
  if( pCur->iRow >= pCur->nRows ) return SQLITE_OK;
  r = &pCur->aRows[pCur->iRow];
  switch( iCol ){
    case 0: sqlite3_result_text(ctx, r->zType,     -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, r->zName,     -1, SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx, r->zFragment, -1, SQLITE_TRANSIENT); break;
    case 3:
    case 4:
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int schemasRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  SchemasRow *r = &pCur->aRows[pCur->iRow];
  u64 h = doltliteFnv1aStr(DOLTLITE_FNV1A_OFFSET, r->zType);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aStr(h, r->zName);
  *pRowid = doltliteFnv1aRowid(h);
  return SQLITE_OK;
}

static sqlite3_module doltliteSchemasModule = {
  0, 0, schemasConnect, schemasBestIndex, doltliteVtabDisconnect, 0,
  schemasOpen, schemasClose, schemasFilter, schemasNext, schemasEof,
  schemasColumn, schemasRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

/* dolt_schemas is a projection of the catalog, not a stored table, so the
** per-table history, diff and blame modules never see it. These three
** read the views and triggers of each commit's catalog instead. */
#define SH_HISTORY 0
#define SH_DIFF    1
#define SH_BLAME   2

#define SH_NTEXT 9
#define SH_NULL  (-1)
#define SH_DATE0 (-2)
#define SH_DATE1 (-3)

#define SH_IDX_FROM_REF 0x01
#define SH_IDX_TO_REF   0x02
#define SH_COL_FROM_REF 15
#define SH_COL_TO_REF   16

typedef struct ShRow ShRow;
struct ShRow {
  char *az[SH_NTEXT];
  i64 aDate[2];
  u8 aHasDate[2];
};

typedef struct ShVtab ShVtab;
struct ShVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  int eKind;
};

typedef struct ShCursor ShCursor;
struct ShCursor {
  sqlite3_vtab_cursor base;
  ShRow *aRow;
  int nRow;
  int nAlloc;
  int iRow;
};

typedef struct ShEnd ShEnd;
struct ShEnd {
  ProllyHash cat;
  char zLabel[PROLLY_HASH_SIZE*2+1];
  i64 date;
  u8 hasDate;
};

static const char *const azShSchema[] = {
  "CREATE TABLE x(type TEXT, name TEXT, fragment TEXT, extra TEXT,"
  " sql_mode TEXT, commit_hash TEXT, committer TEXT, commit_date TEXT)",
  "CREATE TABLE x(to_type TEXT, to_name TEXT, to_fragment TEXT,"
  " to_extra TEXT, to_sql_mode TEXT, to_commit TEXT, to_commit_date TEXT,"
  " from_type TEXT, from_name TEXT, from_fragment TEXT, from_extra TEXT,"
  " from_sql_mode TEXT, from_commit TEXT, from_commit_date TEXT,"
  " diff_type TEXT, from_ref TEXT HIDDEN, to_ref TEXT HIDDEN)",
  "CREATE TABLE x(type TEXT, name TEXT, \"commit\" TEXT, commit_date TEXT,"
  " committer TEXT, email TEXT, message TEXT)"
};
static const char *const azShName[] = {
  "dolt_history_dolt_schemas", "dolt_diff_dolt_schemas",
  "dolt_blame_dolt_schemas"
};
static const signed char aShHistoryCol[] = {
  0, 1, 2, SH_NULL, SH_NULL, 3, 4, SH_DATE0
};
static const signed char aShDiffCol[] = {
  0, 1, 2, SH_NULL, SH_NULL, 3, SH_DATE0,
  4, 5, 6, SH_NULL, SH_NULL, 7, SH_DATE1, 8, SH_NULL, SH_NULL
};
static const signed char aShBlameCol[] = { 0, 1, 2, SH_DATE0, 3, 4, 5 };

static void shClearRows(ShCursor *pCur){
  int i, j;
  for(i=0; i<pCur->nRow; i++){
    for(j=0; j<SH_NTEXT; j++) sqlite3_free(pCur->aRow[i].az[j]);
  }
  sqlite3_free(pCur->aRow);
  pCur->aRow = 0;
  pCur->nRow = 0;
  pCur->nAlloc = 0;
  pCur->iRow = 0;
}

/* Appends a row holding copies of the non-NULL strings of az[0..n-1]. */
static int shAppend(ShCursor *pCur, const char **az, int n, ShRow **ppRow){
  ShRow *p;
  int i;
  int rc = DOLTLITE_GROW_ARRAY(&pCur->aRow, &pCur->nAlloc, pCur->nRow+1, 16);
  if( rc!=SQLITE_OK ) return rc;
  p = &pCur->aRow[pCur->nRow++];
  memset(p, 0, sizeof(*p));
  for(i=0; i<n; i++){
    if( !az[i] ) continue;
    p->az[i] = sqlite3_mprintf("%s", az[i]);
    if( !p->az[i] ) return SQLITE_NOMEM;
  }
  *ppRow = p;
  return SQLITE_OK;
}

static int shEntryCmp(const void *a, const void *b){
  const SchemaEntry *pA = (const SchemaEntry*)a;
  const SchemaEntry *pB = (const SchemaEntry*)b;
  int c = strcmp(pA->zType, pB->zType);
  return c ? c : strcmp(pA->zName, pB->zName);
}

/* The views and triggers of a catalog, ordered by (type, name). */
static int shLoad(
  sqlite3 *db,
  const ProllyHash *pCat,
  SchemaEntry **paEntry,
  int *pnAll,
  int *pnEntry
){
  SchemaEntry *a = 0;
  int n = 0;
  int i, nKeep = 0;
  int rc = SQLITE_OK;
  *paEntry = 0;
  *pnAll = 0;
  *pnEntry = 0;
  if( prollyHashIsEmpty(pCat) ) return SQLITE_OK;
  rc = loadSchemaFromCatalog(db, doltliteGetChunkStore(db),
                             doltliteGetCache(db), pCat, &a, &n);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<n; i++){
    if( a[i].zType && a[i].zName
     && (strcmp(a[i].zType,"view")==0 || strcmp(a[i].zType,"trigger")==0) ){
      SchemaEntry t = a[nKeep];
      a[nKeep++] = a[i];
      a[i] = t;
    }
  }
  if( nKeep>1 ) qsort(a, (size_t)nKeep, sizeof(SchemaEntry), shEntryCmp);
  *paEntry = a;
  *pnAll = n;
  *pnEntry = nKeep;
  return SQLITE_OK;
}

static int shSameSql(const SchemaEntry *pA, const SchemaEntry *pB){
  return strcmp(pA->zSql ? pA->zSql : "", pB->zSql ? pB->zSql : "")==0;
}

static int shEndForCommit(sqlite3 *db, const ProllyHash *pCommit, ShEnd *pEnd){
  DoltliteCommit commit;
  int rc;
  memset(&commit, 0, sizeof(commit));
  memset(pEnd, 0, sizeof(*pEnd));
  rc = doltliteLoadCommit(db, pCommit, &commit);
  if( rc!=SQLITE_OK ) return rc;
  pEnd->cat = commit.catalogHash;
  pEnd->date = commit.timestamp;
  pEnd->hasDate = 1;
  doltliteHashToHex(pCommit, pEnd->zLabel);
  doltliteCommitClear(&commit);
  return SQLITE_OK;
}

static int shDiffRow(
  ShCursor *pCur,
  const SchemaEntry *pFrom,
  const SchemaEntry *pTo,
  const ShEnd *pFromEnd,
  const ShEnd *pToEnd
){
  const char *az[SH_NTEXT];
  ShRow *pRow;
  int rc;
  memset((void*)az, 0, sizeof(az));
  if( pTo ){
    az[0] = pTo->zType;
    az[1] = pTo->zName;
    az[2] = pTo->zSql ? pTo->zSql : "";
  }
  az[3] = pToEnd->zLabel;
  if( pFrom ){
    az[4] = pFrom->zType;
    az[5] = pFrom->zName;
    az[6] = pFrom->zSql ? pFrom->zSql : "";
  }
  az[7] = pFromEnd->zLabel;
  az[8] = !pFrom ? "added" : !pTo ? "removed" : "modified";
  rc = shAppend(pCur, az, SH_NTEXT, &pRow);
  if( rc!=SQLITE_OK ) return rc;
  pRow->aDate[0] = pToEnd->date;
  pRow->aHasDate[0] = pToEnd->hasDate;
  pRow->aDate[1] = pFromEnd->date;
  pRow->aHasDate[1] = pFromEnd->hasDate;
  return SQLITE_OK;
}

static int shDiffEnds(
  ShCursor *pCur,
  sqlite3 *db,
  const ShEnd *pFromEnd,
  const ShEnd *pToEnd
){
  SchemaEntry *aFrom = 0, *aTo = 0;
  int nFromAll = 0, nToAll = 0, nFrom = 0, nTo = 0;
  int i = 0, j = 0;
  int rc = shLoad(db, &pFromEnd->cat, &aFrom, &nFromAll, &nFrom);
  if( rc==SQLITE_OK ) rc = shLoad(db, &pToEnd->cat, &aTo, &nToAll, &nTo);
  while( rc==SQLITE_OK && (i<nFrom || j<nTo) ){
    int c = i>=nFrom ? 1 : j>=nTo ? -1 : shEntryCmp(&aFrom[i], &aTo[j]);
    if( c<0 ){
      rc = shDiffRow(pCur, &aFrom[i++], 0, pFromEnd, pToEnd);
    }else if( c>0 ){
      rc = shDiffRow(pCur, 0, &aTo[j++], pFromEnd, pToEnd);
    }else{
      if( !shSameSql(&aFrom[i], &aTo[j]) ){
        rc = shDiffRow(pCur, &aFrom[i], &aTo[j], pFromEnd, pToEnd);
      }
      i++;
      j++;
    }
  }
  freeSchemaEntries(aFrom, nFromAll);
  freeSchemaEntries(aTo, nToAll);
  return rc;
}

static int shHistory(ShCursor *pCur, sqlite3 *db){
  DoltliteCommitQueue queue;
  ProllyHash head;
  int rc;
  doltliteGetSessionHead(db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
  memset(&queue, 0, sizeof(queue));
  rc = doltliteCommitQueueInit(&queue, &head);
  while( rc==SQLITE_OK ){
    ProllyHash cur;
    DoltliteCommit commit;
    SchemaEntry *a = 0;
    char zHex[PROLLY_HASH_SIZE*2+1];
    int nAll = 0, n = 0, i;
    int hasHash = 0;
    rc = doltliteCommitQueueNext(&queue, &cur, &hasHash);
    if( rc!=SQLITE_OK || !hasHash ) break;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ) break;
    rc = doltliteCommitQueueEnqueueParents(&queue, &commit);
    if( rc==SQLITE_OK ) rc = shLoad(db, &commit.catalogHash, &a, &nAll, &n);
    doltliteHashToHex(&cur, zHex);
    for(i=0; rc==SQLITE_OK && i<n; i++){
      const char *az[5];
      ShRow *pRow;
      az[0] = a[i].zType;
      az[1] = a[i].zName;
      az[2] = a[i].zSql ? a[i].zSql : "";
      az[3] = zHex;
      az[4] = commit.zName ? commit.zName : "";
      rc = shAppend(pCur, az, 5, &pRow);
      if( rc==SQLITE_OK ){
        pRow->aDate[0] = commit.timestamp;
        pRow->aHasDate[0] = 1;
      }
    }
    freeSchemaEntries(a, nAll);
    doltliteCommitClear(&commit);
  }
  doltliteCommitQueueClear(&queue);
  return rc;
}

/* WORKING against HEAD, then every commit reachable from HEAD against
** each of its parents, as the per-table diff modules walk history. */
static int shDiffWalk(ShCursor *pCur, sqlite3 *db){
  typedef struct ShEdge { ProllyHash parent; ShEnd child; } ShEdge;
  ShEdge *aStack = 0;
  int nStack = 0, nAlloc = 0;
  ProllyHashSet seen;
  ProllyHash head;
  ShEnd from, to;
  int rc;
  doltliteGetSessionHead(db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
  rc = prollyHashSetInit(&seen, 64);
  if( rc!=SQLITE_OK ) return rc;
  memset(&to, 0, sizeof(to));
  memcpy(to.zLabel, "WORKING", 8);
  rc = doltliteFlushCatalogToHash(db, &to.cat);
  if( rc==SQLITE_OK ){
    rc = DOLTLITE_GROW_ARRAY(&aStack, &nAlloc, 1, 16);
  }
  if( rc==SQLITE_OK ){
    aStack[0].parent = head;
    aStack[0].child = to;
    nStack = 1;
  }
  while( rc==SQLITE_OK && nStack>0 ){
    ShEdge edge = aStack[--nStack];
    DoltliteCommit commit;
    int i, nParent;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &edge.parent, &commit);
    if( rc!=SQLITE_OK ) break;
    memset(&from, 0, sizeof(from));
    from.cat = commit.catalogHash;
    from.date = commit.timestamp;
    from.hasDate = 1;
    doltliteHashToHex(&edge.parent, from.zLabel);
    rc = shDiffEnds(pCur, db, &from, &edge.child);
    if( rc==SQLITE_OK && !prollyHashSetContains(&seen, &edge.parent) ){
      rc = prollyHashSetAdd(&seen, &edge.parent);
      nParent = doltliteCommitParentCount(&commit);
      if( rc==SQLITE_OK ){
        rc = DOLTLITE_GROW_ARRAY(&aStack, &nAlloc, nStack+nParent, 16);
      }
      for(i=0; rc==SQLITE_OK && i<nParent; i++){
        const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
        if( !pParent ) continue;
        aStack[nStack].parent = *pParent;
        aStack[nStack].child = from;
        nStack++;
      }
    }
    doltliteCommitClear(&commit);
  }
  sqlite3_free(aStack);
  prollyHashSetFree(&seen);
  return rc;
}

static int shSliceEnd(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  sqlite3_value *pArg,
  ShEnd *pEnd
){
  const char *zRef;
  const char *zErr = 0;
  int rc = SQLITE_OK;
  memset(pEnd, 0, sizeof(*pEnd));
  zRef = (const char*)sqlite3_value_text(pArg);
  if( sqlite3_value_type(pArg)==SQLITE_NULL || !zRef ){
    zErr = "invalid argument: NULL";
    zRef = "";
  }else if( doltliteValueHasNul(pArg) ){
    zErr = "invalid argument: contains a NUL byte";
    zRef = "";
  }else if( doltliteRefIsWorking(zRef) || doltliteRefIsStaged(zRef) ){
    i64 now = 0;
    rc = doltliteResolveCatalogHashForRef(db, zRef, &pEnd->cat);
    if( rc==SQLITE_OK ) rc = sqlite3OsCurrentTimeInt64(db->pVfs, &now);
    pEnd->date = now/1000 - 10000*(i64)21086676;
    pEnd->hasDate = 1;
    memcpy(pEnd->zLabel, doltliteRefIsWorking(zRef) ? "WORKING" : "STAGED",
           doltliteRefIsWorking(zRef) ? 8 : 7);
  }else{
    ProllyHash commit;
    rc = doltliteResolveRef(db, zRef, &commit);
    if( rc==SQLITE_OK ) rc = shEndForCommit(db, &commit, pEnd);
  }
  if( !zErr && (rc==SQLITE_NOTFOUND || rc==SQLITE_ERROR) ){
    zErr = rc==SQLITE_NOTFOUND ? "ref not found: " : "invalid ref: ";
  }
  if( zErr ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf("dolt_diff_dolt_schemas: %s%s",
                                     zErr, zRef);
    return pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
  }
  return rc;
}

/* Each view and trigger of HEAD with the newest commit on the first-parent
** chain that changed it, as the per-table blame modules attribute rows. */
static int shBlame(ShCursor *pCur, sqlite3 *db){
  SchemaEntry *aHead = 0;
  int nHeadAll = 0, nHead = 0;
  u8 *aDone = 0;
  int nLeft;
  ProllyHash cur;
  DoltliteCommit commit;
  int rc;
  doltliteGetSessionHead(db, &cur);
  if( prollyHashIsEmpty(&cur) ) return SQLITE_OK;
  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, &cur, &commit);
  if( rc!=SQLITE_OK ) return rc;
  rc = shLoad(db, &commit.catalogHash, &aHead, &nHeadAll, &nHead);
  nLeft = nHead;
  if( rc==SQLITE_OK && nHead>0 ){
    aDone = sqlite3_malloc64((sqlite3_uint64)nHead);
    if( !aDone ) rc = SQLITE_NOMEM;
    else memset(aDone, 0, (size_t)nHead);
  }
  while( rc==SQLITE_OK && nLeft>0 ){
    const ProllyHash *pParent = doltliteCommitParentHash(&commit, 0);
    ProllyHash parentHash;
    DoltliteCommit parent;
    SchemaEntry *aPar = 0;
    int nParAll = 0, nPar = 0;
    char zHex[PROLLY_HASH_SIZE*2+1];
    int i, j;
    memset(&parent, 0, sizeof(parent));
    if( pParent ){
      parentHash = *pParent;
      rc = doltliteLoadCommit(db, &parentHash, &parent);
      if( rc==SQLITE_OK ){
        rc = shLoad(db, &parent.catalogHash, &aPar, &nParAll, &nPar);
      }
    }
    doltliteHashToHex(&cur, zHex);
    for(i=0; rc==SQLITE_OK && i<nHead; i++){
      const char *az[6];
      ShRow *pRow;
      if( aDone[i] ) continue;
      for(j=0; j<nPar && shEntryCmp(&aHead[i], &aPar[j])!=0; j++){}
      if( j<nPar && shSameSql(&aHead[i], &aPar[j]) ) continue;
      az[0] = aHead[i].zType;
      az[1] = aHead[i].zName;
      az[2] = zHex;
      az[3] = commit.zName ? commit.zName : "";
      az[4] = commit.zEmail ? commit.zEmail : "";
      az[5] = commit.zMessage ? commit.zMessage : "";
      rc = shAppend(pCur, az, 6, &pRow);
      if( rc==SQLITE_OK ){
        pRow->aDate[0] = commit.timestamp;
        pRow->aHasDate[0] = 1;
      }
      aDone[i] = 1;
      nLeft--;
    }
    freeSchemaEntries(aPar, nParAll);
    doltliteCommitClear(&commit);
    commit = parent;
    if( !pParent ) break;
    cur = parentHash;
  }
  doltliteCommitClear(&commit);
  freeSchemaEntries(aHead, nHeadAll);
  sqlite3_free(aDone);
  return rc;
}

static int shConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  int eKind = SQLITE_PTR_TO_INT(pAux);
  ShVtab *pVtab;
  int rc;
  (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, azShSchema[eKind], sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (ShVtab*)*ppVtab;
  pVtab->db = db;
  pVtab->eKind = eKind;
  return SQLITE_OK;
}

static int shBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iFrom = -1, iTo = -1;
  int i;
  pInfo->estimatedCost = 1000.0;
  pInfo->estimatedRows = 100;
  if( ((ShVtab*)pVtab)->eKind!=SH_DIFF ) return SQLITE_OK;
  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( pC->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pC->iColumn!=SH_COL_FROM_REF && pC->iColumn!=SH_COL_TO_REF ) continue;
    if( !pC->usable ) return SQLITE_CONSTRAINT;
    if( pC->iColumn==SH_COL_FROM_REF ) iFrom = i;
    else iTo = i;
  }
  if( iFrom<0 && iTo<0 ) return SQLITE_OK;
  if( iFrom<0 || iTo<0 ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf(
        "dolt_diff_dolt_schemas requires both a from and a to revision");
    return SQLITE_ERROR;
  }
  pInfo->aConstraintUsage[iFrom].argvIndex = 1;
  pInfo->aConstraintUsage[iFrom].omit = 1;
  pInfo->aConstraintUsage[iTo].argvIndex = 2;
  pInfo->aConstraintUsage[iTo].omit = 1;
  pInfo->idxNum = SH_IDX_FROM_REF|SH_IDX_TO_REF;
  pInfo->estimatedCost = 100.0;
  return SQLITE_OK;
}

static int shOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(ShCursor));
}

static int shClose(sqlite3_vtab_cursor *pCursor){
  shClearRows((ShCursor*)pCursor);
  sqlite3_free(pCursor);
  return SQLITE_OK;
}

static int shFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  ShCursor *pCur = (ShCursor*)pCursor;
  ShVtab *pVtab = (ShVtab*)pCursor->pVtab;
  int rc;
  (void)idxStr;
  shClearRows(pCur);
  if( !doltliteGetChunkStore(pVtab->db) ) return SQLITE_OK;
  if( pVtab->eKind==SH_HISTORY ){
    rc = shHistory(pCur, pVtab->db);
  }else if( pVtab->eKind==SH_BLAME ){
    rc = shBlame(pCur, pVtab->db);
  }else if( idxNum && argc>=2 ){
    ShEnd from, to;
    rc = shSliceEnd(pVtab->db, &pVtab->base, argv[0], &from);
    if( rc==SQLITE_OK ) rc = shSliceEnd(pVtab->db, &pVtab->base, argv[1], &to);
    if( rc==SQLITE_OK ) rc = shDiffEnds(pCur, pVtab->db, &from, &to);
  }else{
    rc = shDiffWalk(pCur, pVtab->db);
  }
  if( rc!=SQLITE_OK ) shClearRows(pCur);
  return rc;
}

static int shNext(sqlite3_vtab_cursor *pCursor){
  ((ShCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int shEof(sqlite3_vtab_cursor *pCursor){
  ShCursor *pCur = (ShCursor*)pCursor;
  return pCur->iRow>=pCur->nRow;
}

static int shColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  static const signed char *const apMap[] = {
    aShHistoryCol, aShDiffCol, aShBlameCol
  };
  static const int anMap[] = {
    ArraySize(aShHistoryCol), ArraySize(aShDiffCol), ArraySize(aShBlameCol)
  };
  ShCursor *pCur = (ShCursor*)pCursor;
  int eKind = ((ShVtab*)pCursor->pVtab)->eKind;
  const ShRow *pRow = &pCur->aRow[pCur->iRow];
  int iSlot = iCol<anMap[eKind] ? apMap[eKind][iCol] : SH_NULL;
  if( iSlot>=0 ){
    if( pRow->az[iSlot] ){
      sqlite3_result_text(ctx, pRow->az[iSlot], -1, SQLITE_TRANSIENT);
    }
  }else if( iSlot!=SH_NULL ){
    int iDate = iSlot==SH_DATE0 ? 0 : 1;
    if( pRow->aHasDate[iDate] ){
      doltliteResultTimestamp(ctx, pRow->aDate[iDate]);
    }
  }
  return SQLITE_OK;
}

static int shRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((ShCursor*)pCursor)->iRow + 1;
  return SQLITE_OK;
}

static sqlite3_module doltliteSchemasHistoryModule = {
  0, 0, shConnect, shBestIndex, doltliteVtabDisconnect, 0,
  shOpen, shClose, shFilter, shNext, shEof,
  shColumn, shRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteSchemasRegister(sqlite3 *db){
  int rc = doltliteCreateShieldedModule(db, "dolt_schemas",
                                        &doltliteSchemasModule, 0);
  int i;
  for(i=0; rc==SQLITE_OK && i<ArraySize(azShName); i++){
    rc = doltliteCreateShieldedModule(db, azShName[i],
            &doltliteSchemasHistoryModule, SQLITE_INT_TO_PTR(i));
  }
  return rc;
}

int doltliteRevertViewsAndTriggers(
  sqlite3 *db,
  SchemaEntry *aSourceSchema,
  int nSourceSchema
){
  sqlite3_stmt *pStmt = 0;
  char **azDrop = 0;
  int nDrop = 0;
  int i, rc;

  /* Collect first: dropping while the statement walks the schema would
  ** invalidate it. */
  rc = sqlite3_prepare_v2(db,
      "SELECT type, name FROM main.sqlite_master"
      " WHERE type IN ('view','trigger')", -1, &pStmt, 0);
  while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zType = (const char*)sqlite3_column_text(pStmt, 0);
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    char **azNew;
    if( !zType || !zName ) continue;
    azNew = sqlite3_realloc(azDrop, (nDrop+1)*(int)sizeof(char*));
    if( !azNew ){ rc = SQLITE_NOMEM; break; }
    azDrop = azNew;
    azDrop[nDrop] = sqlite3_mprintf("DROP %s \"%w\"",
        strcmp(zType, "view")==0 ? "VIEW" : "TRIGGER", zName);
    if( !azDrop[nDrop] ){ rc = SQLITE_NOMEM; break; }
    nDrop++;
  }
  if( pStmt ){
    int rc2 = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  for(i=0; rc==SQLITE_OK && i<nDrop; i++){
    rc = sqlite3_exec(db, azDrop[i], 0, 0, 0);
  }
  for(i=0; i<nDrop; i++) sqlite3_free(azDrop[i]);
  sqlite3_free(azDrop);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; rc==SQLITE_OK && i<nSourceSchema; i++){
    const char *zType = aSourceSchema[i].zType;
    char *zDetail = 0;
    char *zMsg = 0;
    if( !zType || !aSourceSchema[i].zSql ) continue;
    if( strcmp(zType, "view")!=0 && strcmp(zType, "trigger")!=0 ) continue;
    rc = sqlite3_exec(db, aSourceSchema[i].zSql, 0, 0, 0);
    if( rc==SQLITE_OK || rc==SQLITE_NOMEM ) continue;
    /* Copy first: ErrorWithMsg replaces the connection string this reads. */
    zDetail = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    if( !zDetail ){ rc = SQLITE_NOMEM; break; }
    zMsg = sqlite3_mprintf("cannot restore %s %s: %s",
        strcmp(zType, "view")==0 ? "view" : "trigger",
        aSourceSchema[i].zName ? aSourceSchema[i].zName : "",
        zDetail);
    sqlite3_free(zDetail);
    if( !zMsg ){ rc = SQLITE_NOMEM; break; }
    sqlite3ErrorWithMsg(db, rc, "%s", zMsg);
    sqlite3_free(zMsg);
    break;
  }
  return rc;
}

int doltliteLoadLiveTableSql(
  sqlite3 *db,
  const char *zName,
  int *pFound,
  char **pzSql
){
  sqlite3_stmt *pStmt = 0;
  char *zQry;
  int rc;

  *pFound = 0;
  *pzSql = 0;
  zQry = sqlite3_mprintf(
      "SELECT sql FROM main.sqlite_master "
      "WHERE type='table' AND name='%q' COLLATE NOCASE",
      zName);
  if( !zQry ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQry, -1, &pStmt, 0);
  sqlite3_free(zQry);
  if( rc!=SQLITE_OK ) return rc;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zSql = (const char*)sqlite3_column_text(pStmt, 0);
    *pFound = 1;
    *pzSql = sqlite3_mprintf("%s", zSql ? zSql : "");
    if( !*pzSql ){
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

#endif
