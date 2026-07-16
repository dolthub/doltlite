
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "doltlite_ignore.h"
#include "doltlite_record.h"
#include "prolly_cursor.h"

#define STATUS_IDX_STAGED_EQ 0x01
#define STATUS_IDX_TABLE_EQ  0x02

typedef struct StatusRow StatusRow;
struct StatusRow {
  char *zName;
  int staged;
  const char *zStatus;
};

typedef struct StatusNameSlot StatusNameSlot;
struct StatusNameSlot {
  const char *zName;
  int iEntry;
};

typedef struct StatusNumberSlot StatusNumberSlot;
struct StatusNumberSlot {
  Pgno iTable;
  int iEntry;
};

typedef struct StatusCatalogIndex StatusCatalogIndex;
struct StatusCatalogIndex {
  struct TableEntry *aEntry;
  int nEntry;
  StatusNameSlot *aNameSlot;
  StatusNumberSlot *aNumberSlot;
  int nSlot;
};

typedef struct DoltliteStatusVtab DoltliteStatusVtab;
struct DoltliteStatusVtab { sqlite3_vtab base; sqlite3 *db; };

typedef struct DoltliteStatusCursor DoltliteStatusCursor;
struct DoltliteStatusCursor {
  sqlite3_vtab_cursor base;
  StatusRow *aRows; int nRows; int nRowsAlloc; int iRow;
};

static void statusFreeRows(DoltliteStatusCursor *pCur){
  int i;
  for( i = 0; i < pCur->nRows; i++ ){
    sqlite3_free(pCur->aRows[i].zName);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
  pCur->nRowsAlloc = 0;
}

static const char *statusSchema =
  "CREATE TABLE x(table_name TEXT, staged INTEGER, status TEXT)";

static u32 statusStringHash(const char *z){
  u32 h = 2166136261u;
  while( z && *z ){
    h ^= (unsigned char)*z;
    h *= 16777619u;
    z++;
  }
  return h;
}

static u32 statusNumberHash(Pgno iTable){
  return ((u32)iTable * 2654435761u);
}

static int statusCatalogIndexInit(
  StatusCatalogIndex *pIdx,
  struct TableEntry *aEntry,
  int nEntry
){
  int i;
  int nSlot = 16;

  memset(pIdx, 0, sizeof(*pIdx));
  pIdx->aEntry = aEntry;
  pIdx->nEntry = nEntry;
  if( nEntry<=0 ) return SQLITE_OK;

  while( nSlot < nEntry*2 ) nSlot *= 2;
  pIdx->aNameSlot = sqlite3_malloc(nSlot * (int)sizeof(StatusNameSlot));
  pIdx->aNumberSlot = sqlite3_malloc(nSlot * (int)sizeof(StatusNumberSlot));
  if( !pIdx->aNameSlot || !pIdx->aNumberSlot ){
    sqlite3_free(pIdx->aNameSlot);
    sqlite3_free(pIdx->aNumberSlot);
    memset(pIdx, 0, sizeof(*pIdx));
    return SQLITE_NOMEM;
  }
  memset(pIdx->aNameSlot, 0, nSlot * (int)sizeof(StatusNameSlot));
  memset(pIdx->aNumberSlot, 0, nSlot * (int)sizeof(StatusNumberSlot));
  pIdx->nSlot = nSlot;

  for(i=0; i<nEntry; i++){
    u32 slot;
    if( aEntry[i].zName ){
      slot = statusStringHash(aEntry[i].zName) & (u32)(nSlot - 1);
      while( pIdx->aNameSlot[slot].zName ){
        if( strcmp(pIdx->aNameSlot[slot].zName, aEntry[i].zName)==0 ){
          break;
        }
        slot = (slot + 1) & (u32)(nSlot - 1);
      }
      if( !pIdx->aNameSlot[slot].zName ){
        pIdx->aNameSlot[slot].zName = aEntry[i].zName;
        pIdx->aNameSlot[slot].iEntry = i + 1;
      }
    }

    slot = statusNumberHash(aEntry[i].iTable) & (u32)(nSlot - 1);
    while( pIdx->aNumberSlot[slot].iEntry ){
      if( pIdx->aNumberSlot[slot].iTable==aEntry[i].iTable ){
        break;
      }
      slot = (slot + 1) & (u32)(nSlot - 1);
    }
    if( !pIdx->aNumberSlot[slot].iEntry ){
      pIdx->aNumberSlot[slot].iTable = aEntry[i].iTable;
      pIdx->aNumberSlot[slot].iEntry = i + 1;
    }
  }
  return SQLITE_OK;
}

static void statusCatalogIndexFree(StatusCatalogIndex *pIdx){
  sqlite3_free(pIdx->aNameSlot);
  sqlite3_free(pIdx->aNumberSlot);
  memset(pIdx, 0, sizeof(*pIdx));
}

static struct TableEntry *statusCatalogFindName(
  const StatusCatalogIndex *pIdx,
  const char *zName
){
  u32 slot;
  int i;
  if( !zName || pIdx->nSlot==0 ) return 0;
  slot = statusStringHash(zName) & (u32)(pIdx->nSlot - 1);
  for(i=0; i<pIdx->nSlot; i++){
    StatusNameSlot *pSlot = &pIdx->aNameSlot[slot];
    if( !pSlot->zName ) return 0;
    if( strcmp(pSlot->zName, zName)==0 ){
      return &pIdx->aEntry[pSlot->iEntry - 1];
    }
    slot = (slot + 1) & (u32)(pIdx->nSlot - 1);
  }
  return 0;
}

static struct TableEntry *statusCatalogFindNumber(
  const StatusCatalogIndex *pIdx,
  Pgno iTable
){
  u32 slot;
  int i;
  if( pIdx->nSlot==0 ) return 0;
  slot = statusNumberHash(iTable) & (u32)(pIdx->nSlot - 1);
  for(i=0; i<pIdx->nSlot; i++){
    StatusNumberSlot *pSlot = &pIdx->aNumberSlot[slot];
    if( !pSlot->iEntry ) return 0;
    if( pSlot->iTable==iTable ){
      return &pIdx->aEntry[pSlot->iEntry - 1];
    }
    slot = (slot + 1) & (u32)(pIdx->nSlot - 1);
  }
  return 0;
}

static struct TableEntry *statusCatalogFindEntry(
  const StatusCatalogIndex *pIdx,
  const struct TableEntry *pNeedle
){
  if( pNeedle->zName ){
    return statusCatalogFindName(pIdx, pNeedle->zName);
  }
  return statusCatalogFindNumber(pIdx, pNeedle->iTable);
}

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

static int addRow(DoltliteStatusCursor *pCur, const char *zName,
                  int staged, const char *zStatus){
  StatusRow *aNew;
  StatusRow *pRow;
  int nNew;
  if( pCur->nRows>=pCur->nRowsAlloc ){
    nNew = pCur->nRowsAlloc ? pCur->nRowsAlloc*2 : 16;
    aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(StatusRow));
    if( !aNew ) return SQLITE_NOMEM;
    pCur->aRows = aNew;
    pCur->nRowsAlloc = nNew;
  }
  pRow = &pCur->aRows[pCur->nRows];
  pRow->zName = sqlite3_mprintf("%s", zName);
  if( !pRow->zName ) return SQLITE_NOMEM;
  pRow->staged = staged;
  pRow->zStatus = zStatus;
  pCur->nRows++;
  return SQLITE_OK;
}

static int statusSchemaRecordIsViewOrTrigger(const u8 *pRec, int nRec){
  DoltliteRecordInfo ri;
  int st, off, len;
  const u8 *pBody;
  if( !pRec || nRec<=0 ) return 0;
  doltliteParseRecord(pRec, nRec, &ri);
  if( ri.nField < 1 ) return 0;
  st = ri.aType[0];
  off = ri.aOffset[0];
  if( st < 13 || (st & 1)==0 ) return 0;
  len = (st - 13) / 2;
  if( off < 0 || off + len > nRec ) return 0;
  pBody = pRec + off;
  if( len==4 && memcmp(pBody, "view", 4)==0 ) return 1;
  if( len==7 && memcmp(pBody, "trigger", 7)==0 ) return 1;
  return 0;
}

static int statusSchemaHasViewOrTrigger(sqlite3 *db,
                                        const ProllyHash *pRoot,
                                        u8 flags){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyCursor cur;
  int rc, res;
  int found = 0;
  if( !cs || !pCache ) return 0;
  if( prollyHashIsEmpty(pRoot) ) return 0;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return 0;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal;
    int nVal;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( statusSchemaRecordIsViewOrTrigger(pVal, nVal) ){
      found = 1;
      break;
    }
    if( prollyCursorNext(&cur)!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  return found;
}

static int statusSchemaHasViewOrTriggerDiff(sqlite3 *db,
                                            const ProllyHash *pOldRoot,
                                            const ProllyHash *pNewRoot,
                                            u8 flags){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  int rc;
  int found = 0;
  if( !cs || !pCache ) return 0;
  if( prollyHashCompare(pOldRoot, pNewRoot)==0 ) return 0;
  rc = prollyDiffIterOpen(&iter, cs, pCache, pOldRoot, pNewRoot, flags);
  if( rc!=SQLITE_OK ) return 0;
  while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW && pChange ){
    if( statusSchemaRecordIsViewOrTrigger(pChange->pNewVal, pChange->nNewVal)
     || statusSchemaRecordIsViewOrTrigger(pChange->pOldVal, pChange->nOldVal) ){
      found = 1;
      break;
    }
  }
  prollyDiffIterClose(&iter);
  return found;
}

static int statusCompareDoltSchemas(
  DoltliteStatusCursor *pCur,
  sqlite3 *db,
  struct TableEntry *aFrom,
  int nFrom,
  struct TableEntry *aTo,
  int nTo,
  int staged,
  const char *zFilter
){
  ProllyHash emptyRoot;
  const ProllyHash *pOldRoot;
  const ProllyHash *pNewRoot;
  struct TableEntry *pOldMaster;
  struct TableEntry *pNewMaster;
  u8 flags;
  int oldHas;
  int newHas;

  if( zFilter && strcmp(zFilter, "dolt_schemas")!=0 ) return SQLITE_OK;

  memset(&emptyRoot, 0, sizeof(emptyRoot));
  pOldMaster = doltliteFindTableByNumber(aFrom, nFrom, 1);
  pNewMaster = doltliteFindTableByNumber(aTo, nTo, 1);
  if( !pOldMaster && !pNewMaster ) return SQLITE_OK;

  pOldRoot = pOldMaster ? &pOldMaster->root : &emptyRoot;
  pNewRoot = pNewMaster ? &pNewMaster->root : &emptyRoot;
  flags = pNewMaster ? pNewMaster->flags : pOldMaster->flags;

  if( !statusSchemaHasViewOrTriggerDiff(db, pOldRoot, pNewRoot, flags) ){
    return SQLITE_OK;
  }

  oldHas = statusSchemaHasViewOrTrigger(db, pOldRoot, flags);
  newHas = statusSchemaHasViewOrTrigger(db, pNewRoot, flags);
  if( !oldHas && newHas ){
    return addRow(pCur, "dolt_schemas", staged, "new table");
  }
  if( oldHas && !newHas ){
    return addRow(pCur, "dolt_schemas", staged, "deleted");
  }
  return addRow(pCur, "dolt_schemas", staged, "modified");
}

static int statusRowExists(
  DoltliteStatusCursor *pCur,
  const char *zName,
  int staged
){
  int i;
  for(i=0; i<pCur->nRows; i++){
    if( pCur->aRows[i].staged==staged
     && pCur->aRows[i].zName
     && strcmp(pCur->aRows[i].zName, zName)==0 ){
      return 1;
    }
  }
  return 0;
}

static int statusMaybeAddParentSchemaChange(
  DoltliteStatusCursor *pCur,
  const char *zParent,
  int staged,
  const char *zFilter
){
  if( !zParent ) return SQLITE_OK;
  if( zFilter && strcmp(zFilter, zParent)!=0 ) return SQLITE_OK;
  if( statusRowExists(pCur, zParent, staged) ) return SQLITE_OK;
  return addRow(pCur, zParent, staged, "modified");
}

static int statusCompareIndexSchemaObjects(
  DoltliteStatusCursor *pCur,
  sqlite3 *db,
  const ProllyHash *pFromCat,
  const ProllyHash *pToCat,
  int staged,
  const char *zFilter
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry *aFrom = 0;
  SchemaEntry *aTo = 0;
  int nFrom = 0;
  int nTo = 0;
  int i, j;
  int rc;

  if( !cs || !pCache ) return SQLITE_OK;
  rc = loadSchemaFromCatalog(db, cs, pCache, pFromCat, &aFrom, &nFrom);
  if( rc!=SQLITE_OK ) goto index_schema_done;
  rc = loadSchemaFromCatalog(db, cs, pCache, pToCat, &aTo, &nTo);
  if( rc!=SQLITE_OK ) goto index_schema_done;

  /* One comparison per distinct parent table across both row sets; the
  ** shared comparator decides whether that table's index set changed. */
  for(i=0; i<nFrom+nTo; i++){
    SchemaEntry *pRow = i<nFrom ? &aFrom[i] : &aTo[i-nFrom];
    int seen = 0;
    if( !pRow->zType || strcmp(pRow->zType, "index")!=0
     || !pRow->zTblName ){
      continue;
    }
    for(j=0; j<i; j++){
      SchemaEntry *pPrev = j<nFrom ? &aFrom[j] : &aTo[j-nFrom];
      if( pPrev->zType && strcmp(pPrev->zType, "index")==0
       && pPrev->zTblName
       && strcmp(pPrev->zTblName, pRow->zTblName)==0 ){
        seen = 1;
        break;
      }
    }
    if( seen ) continue;
    if( doltliteIndexSchemaRowsDifferForTable(aFrom, nFrom, aTo, nTo,
                                              pRow->zTblName) ){
      rc = statusMaybeAddParentSchemaChange(pCur, pRow->zTblName,
                                            staged, zFilter);
      if( rc!=SQLITE_OK ) goto index_schema_done;
    }
  }

index_schema_done:
  freeSchemaEntries(aFrom, nFrom);
  freeSchemaEntries(aTo, nTo);
  return rc;
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
  const StatusCatalogIndex *pFromIdx,
  const StatusCatalogIndex *pToIdx,
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
  if( statusCatalogFindName(pFromIdx, pB->zName)!=0 ) return 0;
  if( statusCatalogFindName(pToIdx, pA->zName)!=0 ) return 0;
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
  const ProllyHash *pFromCat, const ProllyHash *pToCat,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  int staged
){
  int i, j, rc;
  StatusCatalogIndex fromIdx;
  StatusCatalogIndex toIdx;

  #define DOLT_STATUS_RENAME_CAP 4096
  unsigned char fromHandled[DOLT_STATUS_RENAME_CAP] = {0};
  unsigned char toHandled[DOLT_STATUS_RENAME_CAP] = {0};
  int useRename = (nFrom <= DOLT_STATUS_RENAME_CAP && nTo <= DOLT_STATUS_RENAME_CAP);

  rc = statusCatalogIndexInit(&fromIdx, aFrom, nFrom);
  if( rc!=SQLITE_OK ) return rc;
  rc = statusCatalogIndexInit(&toIdx, aTo, nTo);
  if( rc!=SQLITE_OK ){
    statusCatalogIndexFree(&fromIdx);
    return rc;
  }

  if( useRename ){
    for(i=0; i<nFrom; i++){
      struct TableEntry *pTo;
      if( aFrom[i].iTable<=1 || fromHandled[i] ) continue;
      pTo = statusCatalogFindNumber(&toIdx, aFrom[i].iTable);
      if( !pTo ) continue;
      j = (int)(pTo - aTo);
      if( j<0 || j>=nTo || toHandled[j] || pTo->iTable<=1 ) continue;
      if( isRenamePair(db, &fromIdx, &toIdx, &aFrom[i], pTo) ){
        char *zCompound = sqlite3_mprintf("%s -> %s", aFrom[i].zName, pTo->zName);
        if( !zCompound ){ rc = SQLITE_NOMEM; goto compare_done; }
        rc = addRow(pCur, zCompound, staged, "renamed");
        sqlite3_free(zCompound);
        if( rc!=SQLITE_OK ) goto compare_done;
        fromHandled[i] = 1;
        toHandled[j] = 1;
      }
    }
  }

  rc = statusCompareDoltSchemas(pCur, db, aFrom, nFrom, aTo, nTo,
                                staged, 0);
  if( rc!=SQLITE_OK ) goto compare_done;

  for(i=0; i<nTo; i++){
    struct TableEntry *pFrom;
    char *zName;
    if(aTo[i].iTable<=1) continue;
    if( useRename && toHandled[i] ) continue;
    pFrom = statusCatalogFindEntry(&fromIdx, &aTo[i]);
    rc = statusTableName(db, &aTo[i], &zName);
    if( rc==SQLITE_NOTFOUND ) continue;
    if( rc!=SQLITE_OK ) goto compare_done;
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
          rc = SQLITE_ERROR;
          goto compare_done;
        }
        if( irc!=SQLITE_OK ){
          sqlite3_free(zIgnErr);
          sqlite3_free(zName);
          rc = irc;
          goto compare_done;
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
    if( rc!=SQLITE_OK ) goto compare_done;
  }
  for(i=0; i<nFrom; i++){
    char *zName;
    if(aFrom[i].iTable<=1) continue;
    if( useRename && fromHandled[i] ) continue;
    if(!statusCatalogFindEntry(&toIdx, &aFrom[i])){
      rc = statusTableName(db, &aFrom[i], &zName);
      if( rc==SQLITE_NOTFOUND ) continue;
      if( rc!=SQLITE_OK ) goto compare_done;
      rc = addRow(pCur, zName, staged, "deleted");
      sqlite3_free(zName);
      if( rc!=SQLITE_OK ) goto compare_done;
    }
  }
  rc = statusCompareIndexSchemaObjects(pCur, db, pFromCat, pToCat,
                                       staged, 0);

compare_done:
  statusCatalogIndexFree(&fromIdx);
  statusCatalogIndexFree(&toIdx);
  return rc;
  #undef DOLT_STATUS_RENAME_CAP
}

static int statusHasUnnamedUserTable(struct TableEntry *aEntry, int nEntry){
  int i;
  for(i=0; i<nEntry; i++){
    if( aEntry[i].iTable>1 && !aEntry[i].zName ) return 1;
  }
  return 0;
}

static int statusMaybeAddRename(
  DoltliteStatusCursor *pCur,
  sqlite3 *db,
  const StatusCatalogIndex *pFromIdx,
  const StatusCatalogIndex *pToIdx,
  const struct TableEntry *pFrom,
  const struct TableEntry *pTo,
  int staged,
  const char *zFilter,
  int *pIsRename
){
  int rc = SQLITE_OK;
  *pIsRename = 0;
  if( !pFrom || !pTo ) return SQLITE_OK;
  if( isRenamePair(db, pFromIdx, pToIdx, pFrom, pTo) ){
    char *zCompound;
    *pIsRename = 1;
    zCompound = sqlite3_mprintf("%s -> %s", pFrom->zName, pTo->zName);
    if( !zCompound ) return SQLITE_NOMEM;
    if( strcmp(zCompound, zFilter)==0 ){
      rc = addRow(pCur, zCompound, staged, "renamed");
    }
    sqlite3_free(zCompound);
  }
  return rc;
}

static int statusInitPairIndexes(
  StatusCatalogIndex *pFromIdx,
  StatusCatalogIndex *pToIdx,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo
){
  int rc;
  rc = statusCatalogIndexInit(pFromIdx, aFrom, nFrom);
  if( rc!=SQLITE_OK ) return rc;
  rc = statusCatalogIndexInit(pToIdx, aTo, nTo);
  if( rc!=SQLITE_OK ){
    statusCatalogIndexFree(pFromIdx);
    return rc;
  }
  return SQLITE_OK;
}

static int compareCatalogsFiltered(
  DoltliteStatusCursor *pCur, sqlite3 *db,
  const ProllyHash *pFromCat, const ProllyHash *pToCat,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  int staged,
  const char *zFilter
){
  StatusCatalogIndex fromIdx;
  StatusCatalogIndex toIdx;
  struct TableEntry *pFrom;
  struct TableEntry *pTo;
  int rc;
  int useRename;
  int idxInit = 0;

  #define DOLT_STATUS_RENAME_CAP 4096
  useRename = (nFrom <= DOLT_STATUS_RENAME_CAP && nTo <= DOLT_STATUS_RENAME_CAP);

  if( !zFilter ){
    return compareCatalogs(pCur, db, pFromCat, pToCat,
                           aFrom, nFrom, aTo, nTo, staged);
  }

  if( strcmp(zFilter, "dolt_schemas")==0 ){
    return statusCompareDoltSchemas(pCur, db, aFrom, nFrom, aTo, nTo,
                                    staged, zFilter);
  }

  if( statusHasUnnamedUserTable(aFrom, nFrom)
   || statusHasUnnamedUserTable(aTo, nTo) ){
    int nStart = pCur->nRows;
    rc = compareCatalogs(pCur, db, pFromCat, pToCat,
                         aFrom, nFrom, aTo, nTo, staged);
    if( rc==SQLITE_OK ){
      int i, j = nStart;
      for(i=nStart; i<pCur->nRows; i++){
        if( pCur->aRows[i].zName
         && strcmp(pCur->aRows[i].zName, zFilter)==0 ){
          if( j!=i ) pCur->aRows[j] = pCur->aRows[i];
          j++;
        }else{
          sqlite3_free(pCur->aRows[i].zName);
        }
      }
      pCur->nRows = j;
    }
    return rc;
  }

  memset(&fromIdx, 0, sizeof(fromIdx));
  memset(&toIdx, 0, sizeof(toIdx));

  if( useRename && strstr(zFilter, " -> ")!=0 ){
    int i;
    rc = statusInitPairIndexes(&fromIdx, &toIdx, aFrom, nFrom, aTo, nTo);
    if( rc!=SQLITE_OK ) return rc;
    idxInit = 1;
    for(i=0; i<nFrom; i++){
      int isRename = 0;
      if( aFrom[i].iTable<=1 ) continue;
      pTo = statusCatalogFindNumber(&toIdx, aFrom[i].iTable);
      if( !pTo || pTo->iTable<=1 ) continue;
      rc = statusMaybeAddRename(pCur, db, &fromIdx, &toIdx,
                                &aFrom[i], pTo, staged, zFilter, &isRename);
      if( rc!=SQLITE_OK ) goto filtered_done;
    }
  }

  pTo = doltliteFindTableByName(aTo, nTo, zFilter);
  if( pTo && pTo->iTable>1 ){
    int isRename = 0;
    pFrom = doltliteFindTableByNumber(aFrom, nFrom, pTo->iTable);
    if( useRename && pFrom && pFrom->zName && pTo->zName
     && strcmp(pFrom->zName, pTo->zName)!=0 ){
      if( !idxInit ){
        rc = statusInitPairIndexes(&fromIdx, &toIdx, aFrom, nFrom, aTo, nTo);
        if( rc!=SQLITE_OK ) return rc;
        idxInit = 1;
      }
      rc = statusMaybeAddRename(pCur, db, &fromIdx, &toIdx,
                                pFrom, pTo, staged, zFilter, &isRename);
      if( rc!=SQLITE_OK ) goto filtered_done;
    }
    if( !isRename ){
      pFrom = pTo->zName ? doltliteFindTableByName(aFrom, nFrom, pTo->zName)
                         : doltliteFindTableByNumber(aFrom, nFrom, pTo->iTable);
      if( !pFrom ){
        if( staged==0 ){
          int ignored = 0;
          char *zIgnErr = 0;
          int irc = doltliteCheckIgnore(db, zFilter, &ignored, &zIgnErr);
          if( irc==SQLITE_CONSTRAINT ){
            if( pCur->base.pVtab->zErrMsg ){
              sqlite3_free(pCur->base.pVtab->zErrMsg);
            }
            pCur->base.pVtab->zErrMsg = zIgnErr;
            rc = SQLITE_ERROR;
            goto filtered_done;
          }
          if( irc!=SQLITE_OK ){
            sqlite3_free(zIgnErr);
            rc = irc;
            goto filtered_done;
          }
          if( ignored ) goto check_deleted;
        }
        rc = addRow(pCur, zFilter, staged, "new table");
        if( rc!=SQLITE_OK ) goto filtered_done;
      }else{
        int bRootChanged =
          prollyHashCompare(&pFrom->root, &pTo->root)!=0;
        int bSchemaChanged =
          !prollyHashIsEmpty(&pFrom->schemaHash)
          && !prollyHashIsEmpty(&pTo->schemaHash)
          && prollyHashCompare(&pFrom->schemaHash, &pTo->schemaHash)!=0;
        if( bRootChanged || bSchemaChanged ){
          rc = addRow(pCur, zFilter, staged, "modified");
          if( rc!=SQLITE_OK ) goto filtered_done;
        }
      }
    }
  }

check_deleted:
  pFrom = doltliteFindTableByName(aFrom, nFrom, zFilter);
  if( pFrom && pFrom->iTable>1 ){
    int isRename = 0;
    pTo = doltliteFindTableByNumber(aTo, nTo, pFrom->iTable);
    if( useRename && pTo && pFrom->zName && pTo->zName
     && strcmp(pFrom->zName, pTo->zName)!=0 ){
      if( !idxInit ){
        rc = statusInitPairIndexes(&fromIdx, &toIdx, aFrom, nFrom, aTo, nTo);
        if( rc!=SQLITE_OK ) return rc;
        idxInit = 1;
      }
      rc = statusMaybeAddRename(pCur, db, &fromIdx, &toIdx,
                                pFrom, pTo, staged, zFilter, &isRename);
      if( rc!=SQLITE_OK ) goto filtered_done;
    }
    if( !isRename
     && !(pFrom->zName ? doltliteFindTableByName(aTo, nTo, pFrom->zName)
                       : doltliteFindTableByNumber(aTo, nTo, pFrom->iTable)) ){
      rc = addRow(pCur, zFilter, staged, "deleted");
      if( rc!=SQLITE_OK ) goto filtered_done;
    }
  }
  rc = statusCompareIndexSchemaObjects(pCur, db, pFromCat, pToCat,
                                       staged, zFilter);

filtered_done:
  if( idxInit ){
    statusCatalogIndexFree(&fromIdx);
    statusCatalogIndexFree(&toIdx);
  }
  return rc;
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
  rc = doltliteVtabConnectSimple(db, statusSchema, sizeof(*pVtab), ppVtab);
  if( rc != SQLITE_OK ) return rc;
  pVtab = (DoltliteStatusVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}
static int statusOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(DoltliteStatusCursor));
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
  ProllyHash baseCatHash;
  struct TableEntry *aHead = 0, *aStaged = 0, *aWorking = 0;
  struct TableEntry *aBase = 0;
  int nHead = 0, nStaged = 0, nWorking = 0, rc = SQLITE_OK;
  int nBase = 0;
  int iStagedOnly = -1;
  const char *zTableFilter = 0;
  int iArg = 0;
  int haveStaged = 0;
  int compareStaged = 0;
  int compareWorking = 0;
  int headLoaded = 0;
  int stagedLoaded = 0;
  int workingLoaded = 0;
  (void)idxStr;

  statusFreeRows(pCur);
  pCur->iRow = 0;
  memset(&headCatHash, 0, sizeof(headCatHash));
  memset(&stagedCatHash, 0, sizeof(stagedCatHash));
  memset(&workingCatHash, 0, sizeof(workingCatHash));
  memset(&baseCatHash, 0, sizeof(baseCatHash));
  if( idxNum & STATUS_IDX_STAGED_EQ ){
    if( iArg>=argc ) return SQLITE_OK;
    iStagedOnly = sqlite3_value_int(argv[iArg++]);
    if( iStagedOnly!=0 && iStagedOnly!=1 ) return SQLITE_OK;
  }
  if( idxNum & STATUS_IDX_TABLE_EQ ){
    if( iArg>=argc ) return SQLITE_OK;
    zTableFilter = (const char*)sqlite3_value_text(argv[iArg++]);
    if( !zTableFilter ) return SQLITE_OK;
  }

  if( !cs ){
    if( doltliteIsStockSqliteDb(db) ){
      pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s",
          doltliteVcUnavailableMessage(db));
      return pCursor->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }

  doltliteGetSessionStaged(db, &stagedCatHash);
  if( iStagedOnly==1 && prollyHashIsEmpty(&stagedCatHash) ){
    goto status_done;
  }
  haveStaged = !prollyHashIsEmpty(&stagedCatHash);

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc != SQLITE_OK ) goto status_done;
  baseCatHash = haveStaged ? stagedCatHash : headCatHash;
  compareStaged = haveStaged
      && iStagedOnly!=0
      && prollyHashCompare(&headCatHash, &stagedCatHash)!=0;

  if( compareStaged ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
    if( rc != SQLITE_OK ) goto status_done;
    headLoaded = 1;

    rc = doltliteLoadCatalog(db, &stagedCatHash, &aStaged, &nStaged, 0);
    if( rc != SQLITE_OK ) goto status_done;
    stagedLoaded = 1;
    rc = compareCatalogsFiltered(pCur, db, &headCatHash, &stagedCatHash,
                                 aHead, nHead, aStaged, nStaged,
                                 1, zTableFilter);
    if( rc != SQLITE_OK ) goto status_done;
  }

  if( iStagedOnly!=1 ){
    rc = doltliteFlushCatalogToHash(db, &workingCatHash);
    if( rc != SQLITE_OK ) goto status_done;
    compareWorking = prollyHashCompare(&baseCatHash, &workingCatHash)!=0;
    if( compareWorking ){
      if( haveStaged ){
        if( !stagedLoaded ){
          rc = doltliteLoadCatalog(db, &stagedCatHash, &aStaged, &nStaged, 0);
          if( rc != SQLITE_OK ) goto status_done;
          stagedLoaded = 1;
        }
        aBase = aStaged;
        nBase = nStaged;
      }else{
        if( !headLoaded ){
          rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
          if( rc != SQLITE_OK ) goto status_done;
          headLoaded = 1;
        }
        aBase = aHead;
        nBase = nHead;
      }

      rc = doltliteLoadCatalog(db, &workingCatHash, &aWorking, &nWorking, 0);
      if( rc != SQLITE_OK ) goto status_done;
      workingLoaded = 1;
      rc = compareCatalogsFiltered(pCur, db, &baseCatHash, &workingCatHash,
                                   aBase, nBase, aWorking, nWorking,
                                   0, zTableFilter);
      if( rc != SQLITE_OK ) goto status_done;
    }
  }

status_done:
  if( headLoaded ) doltliteFreeCatalog(aHead, nHead);
  if( stagedLoaded ) doltliteFreeCatalog(aStaged, nStaged);
  if( workingLoaded ) doltliteFreeCatalog(aWorking, nWorking);
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
  int i;
  int iStagedEq = -1;
  int iTableEq = -1;
  int argvIdx = 1;
  int idxNum = 0;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pC->iColumn==1 ){
      iStagedEq = i;
    }else if( pC->iColumn==0 ){
      iTableEq = i;
    }
  }

  if( iStagedEq>=0 ){
    pInfo->aConstraintUsage[iStagedEq].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iStagedEq].omit = 1;
    idxNum |= STATUS_IDX_STAGED_EQ;
  }
  if( iTableEq>=0 ){
    pInfo->aConstraintUsage[iTableEq].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTableEq].omit = 1;
    idxNum |= STATUS_IDX_TABLE_EQ;
  }

  pInfo->idxNum = idxNum;
  if( idxNum & STATUS_IDX_TABLE_EQ ){
    pInfo->estimatedCost = (idxNum & STATUS_IDX_STAGED_EQ) ? 5.0 : 10.0;
    pInfo->estimatedRows = (idxNum & STATUS_IDX_STAGED_EQ) ? 1 : 2;
  }else if( idxNum & STATUS_IDX_STAGED_EQ ){
    pInfo->estimatedCost = 50.0;
    pInfo->estimatedRows = 10;
  }else{
    pInfo->estimatedCost = 100.0;
  }
  return SQLITE_OK;
}

static sqlite3_module doltliteStatusModule = {
  0,0,statusConnect,statusBestIndex,doltliteVtabDisconnect,0,
  statusOpen,statusClose,statusFilter,statusNext,statusEof,
  statusColumn,statusRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteStatusRegister(sqlite3 *db){
  return sqlite3_create_module(db,"dolt_status",&doltliteStatusModule,0);
}

#endif
