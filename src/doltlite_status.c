
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "doltlite_ignore.h"
#include "prolly_cursor.h"

typedef struct StatusRow StatusRow;
struct StatusRow {
  char *zName;
  int staged;
  const char *zStatus;
};

typedef struct DoltliteStatusVtab DoltliteStatusVtab;
struct DoltliteStatusVtab { sqlite3_vtab base; sqlite3 *db; };

typedef struct DoltliteStatusCursor DoltliteStatusCursor;
struct DoltliteStatusCursor {
  sqlite3_vtab_cursor base;
  StatusRow *aRows; int nRows; int iRow;
};

static void statusFreeRows(DoltliteStatusCursor *pCur){
  int i;
  for( i = 0; i < pCur->nRows; i++ ){
    sqlite3_free(pCur->aRows[i].zName);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
}

static const char *statusSchema =
  "CREATE TABLE x(table_name TEXT, staged INTEGER, status TEXT)";

static int statusTableName(sqlite3 *db, const struct TableEntry *pEntry, char **pzName){
  *pzName = 0;
  if( pEntry->zName ){
    *pzName = sqlite3_mprintf("%s", pEntry->zName);
    return *pzName ? SQLITE_OK : SQLITE_NOMEM;
  }

  if( pEntry->flags & BTREE_BLOBKEY ){
    return SQLITE_NOTFOUND;
  }
  *pzName = doltliteResolveTableNumber(db, pEntry->iTable);
  return *pzName ? SQLITE_OK : SQLITE_NOTFOUND;
}

static struct TableEntry *findCatalogEntry(
  struct TableEntry *a, int n, const struct TableEntry *pNeedle
){
  if( pNeedle->zName ){
    return doltliteFindTableByName(a, n, pNeedle->zName);
  }
  return doltliteFindTableByNumber(a, n, pNeedle->iTable);
}

static int addRow(DoltliteStatusCursor *pCur, const char *zName,
                  int staged, const char *zStatus){
  StatusRow *aNew = sqlite3_realloc(pCur->aRows,
      (pCur->nRows+1)*(int)sizeof(StatusRow));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aRows = aNew;
  aNew[pCur->nRows].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[pCur->nRows].zName ) return SQLITE_NOMEM;
  aNew[pCur->nRows].staged = staged;
  aNew[pCur->nRows].zStatus = zStatus;
  pCur->nRows++;
  return SQLITE_OK;
}

static int statusLoadLiveTableSql(
  sqlite3 *db,
  const char *zName,
  int *pFound,
  char **pzSql
){
  sqlite3_stmt *pStmt = 0;
  char *zQuery;
  int rc;

  *pFound = 0;
  *pzSql = 0;
  zQuery = sqlite3_mprintf(
    "SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name='%q'",
    zName
  );
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    const unsigned char *zSql = sqlite3_column_text(pStmt, 0);
    *pFound = 1;
    if( zSql ){
      *pzSql = sqlite3_mprintf("%s", zSql);
      if( !*pzSql ){
        sqlite3_finalize(pStmt);
        return SQLITE_NOMEM;
      }
    }
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static int statusSchemaHashMatchesRename(
  const ProllyHash *pOldSchemaHash,
  const char *zCurrentSql,
  const char *zOldName
){
  static const char *azFmt[] = {
    "CREATE TABLE %w%s",
    "CREATE TABLE \"%w\"%s",
    "CREATE TABLE `%w`%s",
    "CREATE TABLE [%w]%s"
  };
  const char *zParen;
  int i;

  if( !pOldSchemaHash || !zCurrentSql || !zOldName ) return 0;
  zParen = strchr(zCurrentSql, '(');
  if( !zParen ) return 0;

  for(i=0; i<(int)(sizeof(azFmt)/sizeof(azFmt[0])); i++){
    char *zCandidate = sqlite3_mprintf(azFmt[i], zOldName, zParen);
    if( zCandidate ){
      ProllyHash h;
      prollyHashCompute(zCandidate, (int)strlen(zCandidate), &h);
      sqlite3_free(zCandidate);
      if( prollyHashCompare(&h, pOldSchemaHash)==0 ){
        return 1;
      }
    }
  }
  return 0;
}

static int statusRootsShareAnyKey(
  sqlite3 *db,
  const struct TableEntry *pOld,
  const struct TableEntry *pNew
){
  ChunkStore *cs;
  ProllyCache *cache;
  ProllyCursor curOld, curNew;
  int rc, res;

  if( !pOld || !pNew ) return 0;
  if( prollyHashIsEmpty(&pOld->root) || prollyHashIsEmpty(&pNew->root) ) return 0;

  cs = doltliteGetChunkStore(db);
  cache = doltliteGetCache(db);
  if( !cs || !cache ) return 0;

  prollyCursorInit(&curOld, cs, cache, &pOld->root, pOld->flags);
  rc = prollyCursorFirst(&curOld, &res);
  if( rc!=SQLITE_OK || res!=0 || !prollyCursorIsValid(&curOld) ){
    prollyCursorClose(&curOld);
    return 0;
  }

  prollyCursorInit(&curNew, cs, cache, &pNew->root, pNew->flags);
  if( pOld->flags & BTREE_INTKEY ){
    i64 iKey = prollyCursorIntKey(&curOld);
    rc = prollyCursorSeekInt(&curNew, iKey, &res);
  }else{
    const u8 *pKey = 0;
    int nKey = 0;
    prollyCursorKey(&curOld, &pKey, &nKey);
    rc = prollyCursorSeekBlob(&curNew, pKey, nKey, &res);
  }

  prollyCursorClose(&curOld);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&curNew);
    return 0;
  }

  rc = (res==0 && prollyCursorIsValid(&curNew));
  prollyCursorClose(&curNew);
  return rc;
}

static int isRenamePair(
  sqlite3 *db,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  const struct TableEntry *pA,
  const struct TableEntry *pB
){
  int rc;
  int foundLive = 0;
  char *zLiveSql = 0;
  int bMatch = 0;

  if( pA->iTable != pB->iTable ) return 0;
  if( !pA->zName || !pB->zName ) return 0;
  if( strcmp(pA->zName, pB->zName)==0 ) return 0;
  if( doltliteFindTableByName(aFrom, nFrom, pB->zName)!=0 ) return 0;
  if( doltliteFindTableByName(aTo, nTo, pA->zName)!=0 ) return 0;
  if( prollyHashCompare(&pA->root, &pB->root)==0 ){
    bMatch = 1;
    goto rename_done;
  }

  rc = statusLoadLiveTableSql(db, pB->zName, &foundLive, &zLiveSql);
  if( rc!=SQLITE_OK || !foundLive ) goto rename_done;
  if( statusSchemaHashMatchesRename(&pA->schemaHash, zLiveSql, pA->zName)
   && statusRootsShareAnyKey(db, pA, pB) ){
    bMatch = 1;
  }

rename_done:
  sqlite3_free(zLiveSql);
  return bMatch;
}

static int compareCatalogs(
  DoltliteStatusCursor *pCur, sqlite3 *db,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  int staged
){
  int i, j, rc;

  #define DOLT_STATUS_RENAME_CAP 4096
  unsigned char fromHandled[DOLT_STATUS_RENAME_CAP] = {0};
  unsigned char toHandled[DOLT_STATUS_RENAME_CAP] = {0};
  int useRename = (nFrom <= DOLT_STATUS_RENAME_CAP && nTo <= DOLT_STATUS_RENAME_CAP);

  if( useRename ){
    for(i=0; i<nFrom; i++){
      if( aFrom[i].iTable<=1 || fromHandled[i] ) continue;
      for(j=0; j<nTo; j++){
        if( aTo[j].iTable<=1 || toHandled[j] ) continue;
        if( isRenamePair(db, aFrom, nFrom, aTo, nTo, &aFrom[i], &aTo[j]) ){
          char *zCompound = sqlite3_mprintf("%s -> %s", aFrom[i].zName, aTo[j].zName);
          if( !zCompound ) return SQLITE_NOMEM;
          rc = addRow(pCur, zCompound, staged, "renamed");
          sqlite3_free(zCompound);
          if( rc!=SQLITE_OK ) return rc;
          fromHandled[i] = 1;
          toHandled[j] = 1;
          break;
        }
      }
    }
  }

  for(i=0; i<nTo; i++){
    struct TableEntry *pFrom;
    char *zName;
    if(aTo[i].iTable<=1) continue;
    if( useRename && toHandled[i] ) continue;
    pFrom = findCatalogEntry(aFrom, nFrom, &aTo[i]);
    rc = statusTableName(db, &aTo[i], &zName);
    if( rc==SQLITE_NOTFOUND ) continue;
    if( rc!=SQLITE_OK ) return rc;
    if(!pFrom){
      if( staged==0 ){
        int ignored = 0;
        char *zIgnErr = 0;
        int irc = doltliteCheckIgnore(db, zName, &ignored, &zIgnErr);
        if( irc==SQLITE_CONSTRAINT ){
          if( pCur->base.pVtab->zErrMsg ){
            sqlite3_free(pCur->base.pVtab->zErrMsg);
          }
          pCur->base.pVtab->zErrMsg = zIgnErr;
          sqlite3_free(zName);
          return SQLITE_ERROR;
        }
        if( irc!=SQLITE_OK ){
          sqlite3_free(zIgnErr);
          sqlite3_free(zName);
          return irc;
        }
        if( ignored ){
          sqlite3_free(zName);
          continue;
        }
      }
      rc = addRow(pCur, zName, staged, "new table");
    }else{
      int bRootChanged =
        prollyHashCompare(&pFrom->root, &aTo[i].root)!=0;
      int bSchemaChanged =
        !prollyHashIsEmpty(&pFrom->schemaHash)
        && !prollyHashIsEmpty(&aTo[i].schemaHash)
        && prollyHashCompare(&pFrom->schemaHash, &aTo[i].schemaHash)!=0;
      rc = SQLITE_OK;
      if( bRootChanged || bSchemaChanged ){
        rc = addRow(pCur, zName, staged, "modified");
      }
    }
    sqlite3_free(zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nFrom; i++){
    char *zName;
    if(aFrom[i].iTable<=1) continue;
    if( useRename && fromHandled[i] ) continue;
    if(!findCatalogEntry(aTo, nTo, &aFrom[i])){
      rc = statusTableName(db, &aFrom[i], &zName);
      if( rc==SQLITE_NOTFOUND ) continue;
      if( rc!=SQLITE_OK ) return rc;
      rc = addRow(pCur, zName, staged, "deleted");
      sqlite3_free(zName);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
  #undef DOLT_STATUS_RENAME_CAP
}

static int statusConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DoltliteStatusVtab *pVtab;
  int rc;
  (void)pAux;
  (void)argc;
  (void)argv;
  (void)pzErr;
  rc = sqlite3_declare_vtab(db, statusSchema);
  if( rc != SQLITE_OK ) return rc;
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}
static int statusDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}
static int statusOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  DoltliteStatusCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}
static int statusClose(sqlite3_vtab_cursor *pCursor){
  DoltliteStatusCursor *pCur = (DoltliteStatusCursor*)pCursor;
  statusFreeRows(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int statusFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DoltliteStatusCursor *pCur = (DoltliteStatusCursor*)pCursor;
  DoltliteStatusVtab *pVtab = (DoltliteStatusVtab*)pCursor->pVtab;
  sqlite3 *db = pVtab->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCatHash, stagedCatHash, workingCatHash;
  struct TableEntry *aHead = 0, *aStaged = 0, *aWorking = 0;
  int nHead = 0, nStaged = 0, nWorking = 0, rc;
  (void)idxNum;
  (void)idxStr;
  (void)argc;
  (void)argv;

  statusFreeRows(pCur);
  pCur->iRow = 0;
  if( !cs ) return SQLITE_OK;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc != SQLITE_OK ) goto status_done;
  rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
  if( rc != SQLITE_OK ) goto status_done;

  doltliteGetSessionStaged(db, &stagedCatHash);
  if( !prollyHashIsEmpty(&stagedCatHash) ){
    rc = doltliteLoadCatalog(db, &stagedCatHash, &aStaged, &nStaged, 0);
    if( rc != SQLITE_OK ) goto status_done;
  }

  rc = doltliteFlushCatalogToHash(db, &workingCatHash);
  if( rc == SQLITE_OK ){
    rc = doltliteLoadCatalog(db, &workingCatHash, &aWorking, &nWorking, 0);
  }
  if( rc != SQLITE_OK ) goto status_done;

  if( aStaged ){
    rc = compareCatalogs(pCur, db, aHead, nHead, aStaged, nStaged, 1);
    if( rc != SQLITE_OK ) goto status_done;
  }
  {
    struct TableEntry *aBase = aStaged ? aStaged : aHead;
    int nBase = aStaged ? nStaged : nHead;
    if( aWorking && aBase ){
      rc = compareCatalogs(pCur, db, aBase, nBase, aWorking, nWorking, 0);
    }else if( aWorking && !aBase ){
      rc = compareCatalogs(pCur, db, 0, 0, aWorking, nWorking, 0);
    }
    if( rc != SQLITE_OK ) goto status_done;
  }

status_done:
  doltliteFreeCatalog(aHead, nHead);
  doltliteFreeCatalog(aStaged, nStaged);
  doltliteFreeCatalog(aWorking, nWorking);
  return rc;
}

static int statusNext(sqlite3_vtab_cursor *pCursor){
  ((DoltliteStatusCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}
static int statusEof(sqlite3_vtab_cursor *pCursor){
  DoltliteStatusCursor *pCur = (DoltliteStatusCursor*)pCursor;
  return pCur->iRow >= pCur->nRows;
}
static int statusColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int iCol){
  DoltliteStatusCursor *pCur = (DoltliteStatusCursor*)pCursor;
  StatusRow *pRow;
  if( pCur->iRow >= pCur->nRows ) return SQLITE_OK;
  pRow = &pCur->aRows[pCur->iRow];
  switch( iCol ){
    case 0:
      sqlite3_result_text(ctx, pRow->zName, -1, SQLITE_TRANSIENT);
      break;
    case 1:
      sqlite3_result_int(ctx, pRow->staged);
      break;
    case 2:
      sqlite3_result_text(ctx, pRow->zStatus, -1, SQLITE_STATIC);
      break;
  }
  return SQLITE_OK;
}
static int statusRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteStatusCursor*)pCursor)->iRow;
  return SQLITE_OK;
}
static int statusBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 100.0;
  return SQLITE_OK;
}

static sqlite3_module doltliteStatusModule = {
  0,0,statusConnect,statusBestIndex,statusDisconnect,0,
  statusOpen,statusClose,statusFilter,statusNext,statusEof,
  statusColumn,statusRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteStatusRegister(sqlite3 *db){
  return sqlite3_create_module(db,"dolt_status",&doltliteStatusModule,0);
}

#endif
