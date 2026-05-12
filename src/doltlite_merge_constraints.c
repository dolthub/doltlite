
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include "doltlite_record.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

static int fetchRowByRowid(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  i64 targetRowid,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorSeekInt(&cur, targetRowid, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }

  {
    const u8 *pKey, *pVal;
    int nKey, nVal;
    prollyCursorKey(&cur, &pKey, &nKey);
    prollyCursorValue(&cur, &pVal, &nVal);
    if( pKey && nKey > 0 ){
      *ppKey = sqlite3_malloc(nKey);
      if( !*ppKey ){
        prollyCursorClose(&cur);
        return SQLITE_NOMEM;
      }
      memcpy(*ppKey, pKey, nKey);
      *pnKey = nKey;
    }
    if( pVal && nVal > 0 ){
      *ppVal = sqlite3_malloc(nVal);
      if( !*ppVal ){
        sqlite3_free(*ppKey);
        *ppKey = 0; *pnKey = 0;
        prollyCursorClose(&cur);
        return SQLITE_NOMEM;
      }
      memcpy(*ppVal, pVal, nVal);
      *pnVal = nVal;
    }
  }

  prollyCursorClose(&cur);
  return SQLITE_OK;
}

static int copyCursorRow(
  ProllyCursor *pCur,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  const u8 *pKey = 0, *pVal = 0;
  int nKey = 0, nVal = 0;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  prollyCursorKey(pCur, &pKey, &nKey);
  prollyCursorValue(pCur, &pVal, &nVal);
  if( pKey && nKey>0 ){
    *ppKey = sqlite3_malloc(nKey);
    if( !*ppKey ) return SQLITE_NOMEM;
    memcpy(*ppKey, pKey, nKey);
    *pnKey = nKey;
  }
  if( pVal && nVal>0 ){
    *ppVal = sqlite3_malloc(nVal);
    if( !*ppVal ){
      sqlite3_free(*ppKey);
      *ppKey = 0; *pnKey = 0;
      return SQLITE_NOMEM;
    }
    memcpy(*ppVal, pVal, nVal);
    *pnVal = nVal;
  }
  return SQLITE_OK;
}

static int fetchRowByBlobKey(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorSeekBlob(&cur, pKey, nKey, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }
  rc = copyCursorRow(&cur, ppKey, pnKey, ppVal, pnVal);
  prollyCursorClose(&cur);
  return rc;
}

static int tableHasRowid(sqlite3 *db, const char *zTable);

static int fkRefreshAppendName(char ***pazNames, int *pnNames, const char *zName){
  char **azNames = *pazNames;
  int nNames = *pnNames;
  int i;
  char **azNew;

  if( !zName || !zName[0] ) return SQLITE_OK;
  for(i=0; i<nNames; i++){
    if( strcmp(azNames[i], zName)==0 ) return SQLITE_OK;
  }

  azNew = sqlite3_realloc64(azNames, (sqlite3_uint64)(nNames+1) * sizeof(char*));
  if( !azNew ) return SQLITE_NOMEM;
  azNames = azNew;
  azNames[nNames] = sqlite3_mprintf("%s", zName);
  if( !azNames[nNames] ) return SQLITE_NOMEM;
  *pazNames = azNames;
  *pnNames = nNames + 1;
  return SQLITE_OK;
}

static void fkRefreshFreeNames(char **azNames, int nNames){
  int i;
  for(i=0; i<nNames; i++) sqlite3_free(azNames[i]);
  sqlite3_free(azNames);
}

static int fkRefreshCandidateTables(sqlite3 *db, int *pChanged){
  sqlite3_stmt *pStmt = 0;
  char **azNames = 0;
  int nNames = 0;
  int rc, stepRc, i;

  if( pChanged ) *pChanged = 0;

  rc = sqlite3_prepare_v2(db, "PRAGMA main.foreign_key_check", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  while( (stepRc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zChild = (const char*)sqlite3_column_text(pStmt, 0);
    const char *zParent = (const char*)sqlite3_column_text(pStmt, 2);
    rc = fkRefreshAppendName(&azNames, &nNames, zChild);
    if( rc==SQLITE_OK ) rc = fkRefreshAppendName(&azNames, &nNames, zParent);
    if( rc!=SQLITE_OK ){
      sqlite3_finalize(pStmt);
      fkRefreshFreeNames(azNames, nNames);
      return rc;
    }
  }
  sqlite3_finalize(pStmt);
  if( stepRc!=SQLITE_DONE ){
    fkRefreshFreeNames(azNames, nNames);
    return stepRc;
  }

  for(i=0; i<nNames; i++){
    char *zSql;
    if( !tableHasRowid(db, azNames[i]) ){
      continue;
    }
    zSql = sqlite3_mprintf("REINDEX \"%w\"", azNames[i]);
    if( !zSql ){
      fkRefreshFreeNames(azNames, nNames);
      return SQLITE_NOMEM;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ){
      fkRefreshFreeNames(azNames, nNames);
      return rc;
    }
  }
  fkRefreshFreeNames(azNames, nNames);
  if( pChanged ) *pChanged = (nNames>0);
  return SQLITE_OK;
}

typedef struct MergePkInfo MergePkInfo;
struct MergePkInfo {
  int nPk;
  char **azPk;
  char *zPkCols;
};

static void freeMergePkInfo(MergePkInfo *pPk){
  int i;
  if( !pPk ) return;
  for(i=0; i<pPk->nPk; i++) sqlite3_free(pPk->azPk[i]);
  sqlite3_free(pPk->azPk);
  sqlite3_free(pPk->zPkCols);
  memset(pPk, 0, sizeof(*pPk));
}

static int loadMergePkInfo(sqlite3 *db, const char *zTable, MergePkInfo *pPk){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pCols = 0;
  char *zQuery = 0;
  int rc;
  int nPk = 0;
  char **azPk = 0;
  int i;

  memset(pPk, 0, sizeof(*pPk));
  zQuery = sqlite3_mprintf("PRAGMA main.table_info(%Q)", zTable);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    int pk = sqlite3_column_int(pStmt, 5);
    if( pk>nPk ) nPk = pk;
  }
  rc = sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pStmt);
    return rc;
  }
  if( nPk<=0 ){
    sqlite3_finalize(pStmt);
    return SQLITE_NOTFOUND;
  }

  azPk = sqlite3_malloc64((sqlite3_int64)nPk * sizeof(char*));
  if( !azPk ){
    sqlite3_finalize(pStmt);
    return SQLITE_NOMEM;
  }
  memset(azPk, 0, (size_t)nPk * sizeof(char*));

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zCol = (const char*)sqlite3_column_text(pStmt, 1);
    int pk = sqlite3_column_int(pStmt, 5);
    if( pk<=0 || pk>nPk ) continue;
    azPk[pk-1] = sqlite3_mprintf("%s", zCol ? zCol : "");
    if( !azPk[pk-1] ){
      rc = SQLITE_NOMEM;
      break;
    }
  }
  sqlite3_finalize(pStmt);
  if( rc!=SQLITE_DONE && rc!=SQLITE_OK ){
    for(i=0; i<nPk; i++) sqlite3_free(azPk[i]);
    sqlite3_free(azPk);
    return rc;
  }

  pCols = sqlite3_str_new(0);
  for(i=0; i<nPk; i++){
    if( !azPk[i] ){
      sqlite3_free(sqlite3_str_finish(pCols));
      for(i=0; i<nPk; i++) sqlite3_free(azPk[i]);
      sqlite3_free(azPk);
      return SQLITE_CORRUPT;
    }
    if( i>0 ) sqlite3_str_appendall(pCols, ", ");
    sqlite3_str_appendf(pCols, "\"%w\"", azPk[i]);
  }

  pPk->nPk = nPk;
  pPk->azPk = azPk;
  pPk->zPkCols = sqlite3_str_finish(pCols);
  if( !pPk->zPkCols ){
    freeMergePkInfo(pPk);
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

typedef struct MergeFieldValue MergeFieldValue;
struct MergeFieldValue {
  int eType;
  i64 i;
  double r;
  const void *p;
  int n;
};

static u32 mergeSerialType(const MergeFieldValue *pMem, u32 *pLen){
  if( pMem->eType == SQLITE_NULL ){ *pLen = 0; return 0; }
  if( pMem->eType == SQLITE_INTEGER ){
    i64 v = pMem->i;
    if( v==0 ){ *pLen = 0; return 8; }
    if( v==1 ){ *pLen = 0; return 9; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return 1; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return 2; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return 3; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return 4; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return 5; }
    *pLen = 8; return 6;
  }
  if( pMem->eType == SQLITE_FLOAT ){ *pLen = 8; return 7; }
  if( pMem->eType == SQLITE_TEXT ){
    *pLen = (u32)pMem->n;
    return (u32)(pMem->n * 2 + 13);
  }
  if( pMem->eType == SQLITE_BLOB ){
    *pLen = (u32)pMem->n;
    return (u32)(pMem->n * 2 + 12);
  }
  *pLen = 0;
  return 0;
}

static void mergeWriteIntBe(u8 *pOut, i64 v, int nByte){
  int i;
  for(i=nByte-1; i>=0; i--){
    pOut[i] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

static void mergeSerialPut(u8 *pOut, const MergeFieldValue *pMem, u32 serialType){
  switch( serialType ){
    case 0:
    case 8:
    case 9:
      return;
    case 1: mergeWriteIntBe(pOut, pMem->i, 1); return;
    case 2: mergeWriteIntBe(pOut, pMem->i, 2); return;
    case 3: mergeWriteIntBe(pOut, pMem->i, 3); return;
    case 4: mergeWriteIntBe(pOut, pMem->i, 4); return;
    case 5: mergeWriteIntBe(pOut, pMem->i, 6); return;
    case 6: mergeWriteIntBe(pOut, pMem->i, 8); return;
    case 7: {
      sqlite3_uint64 u;
      memcpy(&u, &pMem->r, 8);
      mergeWriteIntBe(pOut, (i64)u, 8);
      return;
    }
    default:
      if( serialType>=12 ){
        memcpy(pOut, pMem->p, (size_t)pMem->n);
      }
      return;
  }
}

static u8 *buildRecordFromStmtCols(
  sqlite3_stmt *pStmt,
  int iStart,
  int nField,
  int *pnOut
){
  MergeFieldValue *aMem = 0;
  u32 *aType = 0;
  u32 *aLen = 0;
  u8 *pOut = 0;
  int hdrSize = 0, bodySize = 0, i, pos;
  u8 *pHdr, *pBody;

  *pnOut = 0;
  if( nField<=0 ) return 0;

  aMem = sqlite3_malloc64((sqlite3_int64)nField * sizeof(MergeFieldValue));
  aType = sqlite3_malloc64((sqlite3_int64)nField * sizeof(u32));
  aLen = sqlite3_malloc64((sqlite3_int64)nField * sizeof(u32));
  if( !aMem || !aType || !aLen ){
    sqlite3_free(aMem); sqlite3_free(aType); sqlite3_free(aLen);
    return 0;
  }
  memset(aMem, 0, (size_t)nField * sizeof(MergeFieldValue));

  for(i=0; i<nField; i++){
    int iCol = iStart + i;
    int eType = sqlite3_column_type(pStmt, iCol);
    aMem[i].eType = eType;
    switch( eType ){
      case SQLITE_INTEGER:
        aMem[i].i = sqlite3_column_int64(pStmt, iCol);
        break;
      case SQLITE_FLOAT:
        aMem[i].r = sqlite3_column_double(pStmt, iCol);
        break;
      case SQLITE_BLOB:
        aMem[i].n = sqlite3_column_bytes(pStmt, iCol);
        aMem[i].p = sqlite3_column_blob(pStmt, iCol);
        break;
      case SQLITE_TEXT:
        aMem[i].p = sqlite3_column_text(pStmt, iCol);
        aMem[i].n = sqlite3_column_bytes(pStmt, iCol);
        break;
      case SQLITE_NULL:
      default:
        break;
    }
    aType[i] = mergeSerialType(&aMem[i], &aLen[i]);
    hdrSize += sqlite3VarintLen(aType[i]);
    bodySize += (int)aLen[i];
  }
  hdrSize += sqlite3VarintLen(hdrSize);

  pOut = sqlite3_malloc(hdrSize + bodySize);
  if( !pOut ){
    sqlite3_free(aMem); sqlite3_free(aType); sqlite3_free(aLen);
    return 0;
  }

  pos = sqlite3PutVarint(pOut, hdrSize);
  pHdr = pOut + pos;
  pBody = pOut + hdrSize;
  for(i=0; i<nField; i++){
    pHdr += sqlite3PutVarint(pHdr, aType[i]);
    if( aLen[i]>0 ){
      mergeSerialPut(pBody, &aMem[i], aType[i]);
      pBody += aLen[i];
    }
  }

  *pnOut = hdrSize + bodySize;
  sqlite3_free(aMem);
  sqlite3_free(aType);
  sqlite3_free(aLen);
  return pOut;
}

static int recordPrefixEquals(
  const u8 *pLeft, int nLeft,
  const u8 *pRight, int nRight,
  int nField
){
  DoltliteRecordInfo a, b;
  int i;

  if( nField<=0 ) return 1;
  if( doltliteParseRecordStrict(pLeft, nLeft, &a)!=SQLITE_OK ) return 0;
  if( doltliteParseRecordStrict(pRight, nRight, &b)!=SQLITE_OK ) return 0;
  if( a.nField < nField || b.nField < nField ) return 0;

  for(i=0; i<nField; i++){
    int nA = dlSerialTypeLen((u64)a.aType[i]);
    int nB = dlSerialTypeLen((u64)b.aType[i]);
    if( a.aType[i]!=b.aType[i] ) return 0;
    if( nA!=nB ) return 0;
    if( nA>0 && memcmp(pLeft + a.aOffset[i], pRight + b.aOffset[i], (size_t)nA)!=0 ){
      return 0;
    }
  }
  return 1;
}

static int fetchRowByPkRecord(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pPkRec, int nPkRec,
  int nPkField,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int rc, res;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( res==0 && prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0;
    int nVal = 0;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && recordPrefixEquals(pVal, nVal, pPkRec, nPkRec, nPkField) ){
      rc = copyCursorRow(&cur, ppKey, pnKey, ppVal, pnVal);
      prollyCursorClose(&cur);
      return rc;
    }
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
  }
  prollyCursorClose(&cur);
  return SQLITE_NOTFOUND;
}

static char *buildFkViolationInfo(
  sqlite3 *db,
  const char *zChildTable,
  int fkid
){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pJson;
  sqlite3_str *pCols;
  sqlite3_str *pRefCols;
  char *zColsBuf = 0;
  char *zRefColsBuf = 0;
  char *zParentBuf = 0;
  char *zOnUpBuf = 0;
  char *zOnDelBuf = 0;
  char *zQuery;
  char *zResult;
  int rc;
  int nMatches = 0;
  int fatal = 0;

  pJson = sqlite3_str_new(0);
  pCols = sqlite3_str_new(0);
  pRefCols = sqlite3_str_new(0);

  zQuery = sqlite3_mprintf("PRAGMA main.foreign_key_list(%Q)", zChildTable);
  if( !zQuery ){
    fatal = 1;
  }else{
    rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
    sqlite3_free(zQuery);
    if( rc != SQLITE_OK ){
      fatal = 1;
    }else{
      while( sqlite3_step(pStmt) == SQLITE_ROW ){
        int id = sqlite3_column_int(pStmt, 0);
        const char *zParent, *zFrom, *zTo, *zOnUp, *zOnDel;
        if( id != fkid ) continue;
        zParent = (const char*)sqlite3_column_text(pStmt, 2);
        zFrom   = (const char*)sqlite3_column_text(pStmt, 3);
        zTo     = (const char*)sqlite3_column_text(pStmt, 4);
        zOnUp   = (const char*)sqlite3_column_text(pStmt, 5);
        zOnDel  = (const char*)sqlite3_column_text(pStmt, 6);
        if( nMatches>0 ){
          sqlite3_str_appendall(pCols, ", ");
          sqlite3_str_appendall(pRefCols, ", ");
        }
        sqlite3_str_appendf(pCols, "\"%w\"", zFrom ? zFrom : "");
        sqlite3_str_appendf(pRefCols, "\"%w\"", zTo ? zTo : "");
        if( nMatches==0 ){
          if( zParent ) zParentBuf = sqlite3_mprintf("%s", zParent);
          zOnUpBuf  = sqlite3_mprintf("%s", zOnUp  ? zOnUp  : "NO ACTION");
          zOnDelBuf = sqlite3_mprintf("%s", zOnDel ? zOnDel : "NO ACTION");
        }
        nMatches++;
      }
    }
  }
  sqlite3_finalize(pStmt);

  zColsBuf    = sqlite3_str_finish(pCols);
  zRefColsBuf = sqlite3_str_finish(pRefCols);

  if( fatal ){
    sqlite3_free(sqlite3_str_finish(pJson));
    sqlite3_free(zColsBuf);
    sqlite3_free(zRefColsBuf);
    sqlite3_free(zParentBuf);
    sqlite3_free(zOnUpBuf);
    sqlite3_free(zOnDelBuf);
    return 0;
  }

  sqlite3_str_appendall(pJson, "{");
  sqlite3_str_appendf(pJson,
      "\"Columns\": [%s], \"ReferencedTable\": \"%w\", "
      "\"ReferencedColumns\": [%s], "
      "\"OnUpdate\": \"%w\", \"OnDelete\": \"%w\"}",
      zColsBuf ? zColsBuf : "",
      zParentBuf ? zParentBuf : "",
      zRefColsBuf ? zRefColsBuf : "",
      zOnUpBuf ? zOnUpBuf : "NO ACTION",
      zOnDelBuf ? zOnDelBuf : "NO ACTION");
  zResult = sqlite3_str_finish(pJson);
  sqlite3_free(zColsBuf);
  sqlite3_free(zRefColsBuf);
  sqlite3_free(zParentBuf);
  sqlite3_free(zOnUpBuf);
  sqlite3_free(zOnDelBuf);
  return zResult;
}

static int fetchAncestorRowByName(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  i64 rowid,
  u8 **ppAncVal, int *pnAncVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  struct TableEntry *pTE;
  u8 *pAncKey = 0; int nAncKey = 0;
  int rc;

  *ppAncVal = 0;
  *pnAncVal = 0;

  if( !aAnc || nAnc==0 ) return SQLITE_NOTFOUND;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  pTE = doltliteFindTableByName(aAnc, nAnc, zTable);
  if( !pTE ) return SQLITE_NOTFOUND;

  rc = fetchRowByRowid(cs, pCache, &pTE->root, pTE->flags, rowid,
                       &pAncKey, &nAncKey, ppAncVal, pnAncVal);
  sqlite3_free(pAncKey);
  return rc;
}

static int fetchAncestorRowByKey(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const u8 *pKey, int nKey,
  u8 **ppAncVal, int *pnAncVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  struct TableEntry *pTE;
  u8 *pAncKey = 0;
  int nAncKey = 0;
  int rc;

  *ppAncVal = 0;
  *pnAncVal = 0;

  if( !aAnc || nAnc==0 ) return SQLITE_NOTFOUND;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  pTE = doltliteFindTableByName(aAnc, nAnc, zTable);
  if( !pTE ) return SQLITE_NOTFOUND;
  if( pTE->flags & PROLLY_NODE_INTKEY ) return SQLITE_NOTFOUND;

  rc = fetchRowByBlobKey(cs, pCache, &pTE->root, pTE->flags, pKey, nKey,
                         &pAncKey, &nAncKey, ppAncVal, pnAncVal);
  sqlite3_free(pAncKey);
  return rc;
}

static int isRowPreExisting(
  const u8 *pMergedVal, int nMergedVal,
  const u8 *pAncVal, int nAncVal
){
  if( !pMergedVal || !pAncVal ) return 0;
  if( nMergedVal != nAncVal ) return 0;
  if( nMergedVal == 0 ) return 1;
  return memcmp(pMergedVal, pAncVal, nMergedVal)==0 ? 1 : 0;
}

static int tableHasRowid(sqlite3 *db, const char *zTable){
  sqlite3_stmt *pStmt = 0;
  char *zQuery;
  int rc;
  zQuery = sqlite3_mprintf("SELECT rowid FROM main.\"%w\" LIMIT 0", zTable);
  if( !zQuery ) return 1;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( pStmt ) sqlite3_finalize(pStmt);
  return rc == SQLITE_OK ? 1 : 0;
}

static int fetchOrphanRow(
  sqlite3 *db,
  const char *zTable,
  i64 rowid,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash root;
  u8 flags = 0;
  Pgno iTable;
  int rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  rc = doltliteResolveTableName(db, zTable, &iTable);
  if( rc != SQLITE_OK ) return rc;

  rc = doltliteGetSessionTableRoot(db, iTable, &root, &flags);
  if( rc != SQLITE_OK ) return rc;

  return fetchRowByRowid(cs, pCache, &root, flags, rowid,
                         ppKey, pnKey, ppVal, pnVal);
}

static int fetchRowByPkFromTable(
  sqlite3 *db,
  const char *zTable,
  const u8 *pPkRec, int nPkRec,
  int nPkField,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash root;
  u8 flags = 0;
  Pgno iTable;
  int rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  rc = doltliteResolveTableName(db, zTable, &iTable);
  if( rc != SQLITE_OK ) return rc;

  rc = doltliteGetSessionTableRoot(db, iTable, &root, &flags);
  if( rc != SQLITE_OK ) return rc;
  if( flags & PROLLY_NODE_INTKEY ) return SQLITE_NOTFOUND;

  return fetchRowByPkRecord(cs, pCache, &root, flags, pPkRec, nPkRec, nPkField,
                            ppKey, pnKey, ppVal, pnVal);
}

static int appendUniqueViolationByRowid(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  sqlite3_int64 rowid,
  int *pAppended
){
  u8 *pKey = 0;
  int nKey = 0;
  u8 *pVal = 0;
  int nVal = 0;
  char *zInfo = 0;
  int rc;

  if( pAppended ) *pAppended = 0;
  rc = fetchOrphanRow(db, zTable, rowid, &pKey, &nKey, &pVal, &nVal);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  if( aAnc ){
    u8 *pAncVal = 0;
    int nAncVal = 0;
    int ancRc = fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                       rowid, &pAncVal, &nAncVal);
    int preExisting = (ancRc==SQLITE_OK)
        && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
    sqlite3_free(pAncVal);
    if( preExisting ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      return SQLITE_OK;
    }
  }

  zInfo = sqlite3_mprintf(
      "{\"Columns\": [%s], \"Name\": \"%w\"}",
      zCols, zIndexName);
  rc = doltliteAppendConstraintViolation(
      db, zTable, DOLTLITE_CV_UNIQUE_INDEX,
      rowid, pKey, nKey, pVal, nVal, zInfo);
  sqlite3_free(zInfo);
  sqlite3_free(pKey);
  sqlite3_free(pVal);
  if( rc==SQLITE_OK && pAppended ) *pAppended = 1;
  return rc;
}

static int appendUniqueViolationByPk(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  const MergePkInfo *pPk,
  const u8 *pPkRec, int nPkRec,
  int *pAppended
){
  u8 *pKey = 0;
  int nKey = 0;
  u8 *pVal = 0;
  int nVal = 0;
  char *zInfo = 0;
  int rc;

  if( pAppended ) *pAppended = 0;
  rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pPk->nPk,
                             &pKey, &nKey, &pVal, &nVal);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;

  if( aAnc ){
    u8 *pAncVal = 0;
    int nAncVal = 0;
    int ancRc = fetchAncestorRowByKey(db, aAnc, nAnc, zTable,
                                      pKey, nKey, &pAncVal, &nAncVal);
    int preExisting = (ancRc==SQLITE_OK)
        && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
    sqlite3_free(pAncVal);
    if( preExisting ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      return SQLITE_OK;
    }
  }

  zInfo = sqlite3_mprintf(
      "{\"Columns\": [%s], \"Name\": \"%w\"}",
      zCols, zIndexName);
  rc = doltliteAppendConstraintViolation(
      db, zTable, DOLTLITE_CV_UNIQUE_INDEX,
      0, pKey, nKey, pVal, nVal, zInfo);
  sqlite3_free(zInfo);
  sqlite3_free(pKey);
  sqlite3_free(pVal);
  if( rc==SQLITE_OK && pAppended ) *pAppended = 1;
  return rc;
}

static int uniqueIndexRowHasNull(sqlite3_stmt *pScan, int colFirst, int colLast){
  int i;
  for(i=colFirst; i<colLast; i++){
    if( sqlite3_column_type(pScan, i) == SQLITE_NULL ) return 1;
  }
  return 0;
}

static int detectUniqueViolationsForIndex(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  char **pzErrMsg,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  char *zQuery;
  char *zWinnerKey = 0;
  sqlite3_int64 winnerRowid = 0;
  int winnerHandled = 0;
  int rc;
  (void)pzErrMsg;

  zQuery = sqlite3_mprintf(
    "SELECT rowid, %s FROM main.\"%w\" NOT INDEXED ORDER BY %s, rowid",
    zCols, zTable, zCols);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  sqlite3_free(zQuery);
  if( rc != SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pScan)) == SQLITE_ROW ){
    sqlite3_int64 rowid = sqlite3_column_int64(pScan, 0);
    int nc = sqlite3_column_count(pScan);
    int i;
    sqlite3_str *pS;
    char *zRowKey;
    int isDup;

    if( uniqueIndexRowHasNull(pScan, 1, nc) ) continue;

    pS = sqlite3_str_new(0);
    for(i=1; i<nc; i++){
      const char *zV = (const char*)sqlite3_column_text(pScan, i);
      sqlite3_str_appendf(pS, "%s%Q", i>1?"|":"", zV ? zV : "");
    }
    zRowKey = sqlite3_str_finish(pS);
    if( !zRowKey ){ rc = SQLITE_NOMEM; break; }

    isDup = zWinnerKey && strcmp(zWinnerKey, zRowKey)==0;
    if( !isDup ){
      sqlite3_free(zWinnerKey);
      zWinnerKey = zRowKey;
      winnerRowid = rowid;
      winnerHandled = 0;
      continue;
    }
    sqlite3_free(zRowKey);

    if( !winnerHandled ){
      int appended = 0;
      rc = appendUniqueViolationByRowid(db, aAnc, nAnc, zTable, zIndexName,
                                        zCols, winnerRowid, &appended);
      if( rc != SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
      winnerHandled = 1;
    }

    {
      int appended = 0;
      rc = appendUniqueViolationByRowid(db, aAnc, nAnc, zTable, zIndexName,
                                        zCols, rowid, &appended);
      if( rc != SQLITE_OK ) break;
      if( appended && pnFound ) (*pnFound)++;
    }
  }
  sqlite3_free(zWinnerKey);
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pScan);
  return rc;
}

static int detectUniqueViolationsForIndexWithoutRowid(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  const MergePkInfo *pPk,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  sqlite3_str *pSql = 0;
  char *zQuery = 0;
  char *zWinnerKey = 0;
  u8 *pWinnerPkRec = 0;
  int nWinnerPkRec = 0;
  int winnerHandled = 0;
  int rc;

  pSql = sqlite3_str_new(0);
  sqlite3_str_appendf(pSql,
      "SELECT %s, %s FROM main.\"%w\" NOT INDEXED ORDER BY %s, %s",
      zCols, pPk->zPkCols, zTable, zCols, pPk->zPkCols);
  zQuery = sqlite3_str_finish(pSql);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pScan)) == SQLITE_ROW ){
    int nc = sqlite3_column_count(pScan);
    int nDupKeyCol = nc - pPk->nPk;
    sqlite3_str *pS;
    char *zRowKey = 0;
    int i, isDup;

    if( uniqueIndexRowHasNull(pScan, 0, nDupKeyCol) ) continue;

    pS = sqlite3_str_new(0);
    for(i=0; i<nDupKeyCol; i++){
      const char *zV = (const char*)sqlite3_column_text(pScan, i);
      sqlite3_str_appendf(pS, "%s%Q", i>0?"|":"", zV ? zV : "");
    }
    zRowKey = sqlite3_str_finish(pS);
    if( !zRowKey ){ rc = SQLITE_NOMEM; break; }

    isDup = zWinnerKey && strcmp(zWinnerKey, zRowKey)==0;
    if( !isDup ){
      u8 *pPkRec = 0;
      int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pScan, nDupKeyCol, pPk->nPk, &nPkRec);
      if( !pPkRec ){
        sqlite3_free(zRowKey);
        rc = SQLITE_NOMEM;
        break;
      }
      sqlite3_free(zWinnerKey);
      sqlite3_free(pWinnerPkRec);
      zWinnerKey = zRowKey;
      pWinnerPkRec = pPkRec;
      nWinnerPkRec = nPkRec;
      winnerHandled = 0;
      continue;
    }
    sqlite3_free(zRowKey);

    {
      u8 *pPkRec = 0; int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pScan, nDupKeyCol, pPk->nPk, &nPkRec);
      if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
      if( !winnerHandled ){
        int appended = 0;
        rc = appendUniqueViolationByPk(db, aAnc, nAnc, zTable, zIndexName,
                                       zCols, pPk, pWinnerPkRec, nWinnerPkRec,
                                       &appended);
        if( rc != SQLITE_OK ){
          sqlite3_free(pPkRec);
          break;
        }
        if( appended && pnFound ) (*pnFound)++;
        winnerHandled = 1;
      }
      {
        int appended = 0;
        rc = appendUniqueViolationByPk(db, aAnc, nAnc, zTable, zIndexName,
                                       zCols, pPk, pPkRec, nPkRec, &appended);
        sqlite3_free(pPkRec);
        if( rc != SQLITE_OK ) break;
        if( appended && pnFound ) (*pnFound)++;
      }
    }
  }

  sqlite3_free(zWinnerKey);
  sqlite3_free(pWinnerPkRec);
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pScan);
  return rc;
}

int doltliteDetectMergeUniqueViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound
){
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  Pgno iNextAnc = 0;
  int haveAnc = 0;
  int rc;

  if( pnFound ) *pnFound = 0;

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    if( doltliteLoadCatalog(db, pAncCatHash, &aAnc, &nAnc, &iNextAnc)==SQLITE_OK ){
      haveAnc = 1;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
    return rc;
  }

  while( (rc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    char *zTable;
    sqlite3_stmt *pIdxList = 0;
    char *zIdxQ;
    int hasRowid = 1;
    MergePkInfo pkInfo;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    memset(&pkInfo, 0, sizeof(pkInfo));
    hasRowid = tableHasRowid(db, zTable);
    if( !hasRowid ){
      rc = loadMergePkInfo(db, zTable, &pkInfo);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTable);
        break;
      }
    }

    zIdxQ = sqlite3_mprintf("PRAGMA main.index_list(%Q)", zTable);
    if( !zIdxQ ){ sqlite3_free(zTable); rc = SQLITE_NOMEM; break; }
    rc = sqlite3_prepare_v2(db, zIdxQ, -1, &pIdxList, 0);
    sqlite3_free(zIdxQ);
    if( rc != SQLITE_OK ){
      freeMergePkInfo(&pkInfo);
      sqlite3_free(zTable);
      break;
    }

    while( sqlite3_step(pIdxList) == SQLITE_ROW ){
      int unique = sqlite3_column_int(pIdxList, 2);
      const char *zIdxRaw;
      const char *zOrigin;
      char *zIdx;
      char *zColsQ;
      sqlite3_stmt *pCols = 0;
      sqlite3_str *pColList;
      char *zColList;
      int idxRc;

      if( !unique ) continue;
      zIdxRaw = (const char*)sqlite3_column_text(pIdxList, 1);
      if( !zIdxRaw ) continue;

      zOrigin = (const char*)sqlite3_column_text(pIdxList, 3);
      if( zOrigin && strcmp(zOrigin, "pk")==0 ) continue;

      zIdx = sqlite3_mprintf("%s", zIdxRaw);
      if( !zIdx ) break;
      zColsQ = sqlite3_mprintf("PRAGMA main.index_xinfo(%Q)", zIdx);
      if( !zColsQ ){ sqlite3_free(zIdx); break; }
      idxRc = sqlite3_prepare_v2(db, zColsQ, -1, &pCols, 0);
      sqlite3_free(zColsQ);
      if( idxRc != SQLITE_OK ){ sqlite3_free(zIdx); continue; }

      pColList = sqlite3_str_new(0);
      while( sqlite3_step(pCols) == SQLITE_ROW ){
        int cno = sqlite3_column_int(pCols, 1);
        int isKey = sqlite3_column_int(pCols, 5);
        const char *zCol = (const char*)sqlite3_column_text(pCols, 2);
        if( cno < 0 || !zCol || !isKey ) continue;
        if( sqlite3_str_length(pColList) > 0 ){
          sqlite3_str_appendall(pColList, ", ");
        }
        sqlite3_str_appendf(pColList, "\"%w\"", zCol);
      }
      sqlite3_finalize(pCols);
      zColList = sqlite3_str_finish(pColList);
      if( zColList && *zColList ){
        if( hasRowid ){
          rc = detectUniqueViolationsForIndex(db, aAnc, nAnc, zTable,
                                               zIdx, zColList, pzErrMsg,
                                               pnFound);
        }else{
          rc = detectUniqueViolationsForIndexWithoutRowid(
              db, aAnc, nAnc, zTable, zIdx, zColList, &pkInfo, pnFound);
        }
      }
      sqlite3_free(zColList);
      sqlite3_free(zIdx);
      if( rc != SQLITE_OK ) break;
    }

    sqlite3_finalize(pIdxList);
    freeMergePkInfo(&pkInfo);
    sqlite3_free(zTable);
    if( rc != SQLITE_OK ) break;
  }
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pTbls);
  return rc;
}

static int nextCheckClause(
  const char *zSql, int *pOffset, char **pzExpr, char **pzName
){
  const char *p = zSql + *pOffset;
  const char *pEnd;
  char lastConstraintName[128] = {0};
  int depth;
  const char *pExprStart;

  *pzExpr = 0;
  *pzName = 0;

  while( *p ){
    if( (p[0]=='C' || p[0]=='c')
     && strncasecmp(p, "CONSTRAINT", 10)==0
     && (p[10]==' ' || p[10]=='\t' || p[10]=='\n') ){
      int i = 0;
      p += 10;
      while( *p==' ' || *p=='\t' || *p=='\n' ) p++;
      while( *p && *p!=' ' && *p!='\t' && *p!='\n' && *p!='(' && i<127 ){
        lastConstraintName[i++] = *p++;
      }
      lastConstraintName[i] = 0;
      continue;
    }
    if( (p[0]=='C' || p[0]=='c')
     && strncasecmp(p, "CHECK", 5)==0
     && (p[5]==' ' || p[5]=='\t' || p[5]=='(' || p[5]=='\n') ){
      p += 5;
      while( *p==' ' || *p=='\t' || *p=='\n' ) p++;
      if( *p!='(' ){ p++; lastConstraintName[0] = 0; continue; }
      p++;
      pExprStart = p;
      depth = 1;
      while( *p && depth>0 ){
        char c = *p;
        if( c=='\'' ){
          p++;
          while( *p && !(*p=='\'' && p[1]!='\'') ){
            if( *p=='\'' && p[1]=='\'' ) p++;
            p++;
          }
          if( *p=='\'' ) p++;
          continue;
        }
        if( c=='"' ){
          p++;
          while( *p && *p!='"' ) p++;
          if( *p=='"' ) p++;
          continue;
        }
        if( c=='(' ) depth++;
        else if( c==')' ) depth--;
        if( depth>0 ) p++;
      }
      if( depth!=0 ) return -1;
      pEnd = p;
      p++;
      *pzExpr = sqlite3_malloc((int)(pEnd - pExprStart) + 1);
      if( !*pzExpr ) return -1;
      memcpy(*pzExpr, pExprStart, (size_t)(pEnd - pExprStart));
      (*pzExpr)[pEnd - pExprStart] = 0;
      if( lastConstraintName[0] ){
        *pzName = sqlite3_mprintf("%s", lastConstraintName);
      }
      *pOffset = (int)(p - zSql);
      return 1;
    }
    if( *p=='\'' ){
      p++;
      while( *p && !(*p=='\'' && p[1]!='\'') ){
        if( *p=='\'' && p[1]=='\'' ) p++;
        p++;
      }
      if( *p=='\'' ) p++;
      continue;
    }
    if( *p=='"' ){
      p++;
      while( *p && *p!='"' ) p++;
      if( *p=='"' ) p++;
      continue;
    }
    if( *p==',' ) lastConstraintName[0] = 0;
    p++;
  }
  *pOffset = (int)(p - zSql);
  return 0;
}

int doltliteDetectMergeCheckViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound
){
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  Pgno iNextAnc = 0;
  int haveAnc = 0;
  int rc;
  int stepRc;

  if( pnFound ) *pnFound = 0;

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    if( doltliteLoadCatalog(db, pAncCatHash, &aAnc, &nAnc, &iNextAnc)==SQLITE_OK ){
      haveAnc = 1;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT name, sql FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    const char *zSqlRaw   = (const char*)sqlite3_column_text(pTbls, 1);
    char *zTable;
    char *zSql;
    int offset = 0;
    int hasRowid = 1;
    MergePkInfo pkInfo;

    if( !zTableRaw || !zSqlRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    zSql   = sqlite3_mprintf("%s", zSqlRaw);
    if( !zTable || !zSql ){
      sqlite3_free(zTable);
      sqlite3_free(zSql);
      rc = SQLITE_NOMEM;
      break;
    }
    memset(&pkInfo, 0, sizeof(pkInfo));
    hasRowid = tableHasRowid(db, zTable);
    if( !hasRowid ){
      rc = loadMergePkInfo(db, zTable, &pkInfo);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTable);
        sqlite3_free(zSql);
        break;
      }
    }

    for(;;){
      char *zExpr = 0;
      char *zCkName = 0;
      int clauseRc = nextCheckClause(zSql, &offset, &zExpr, &zCkName);
      char *zQuery;
      sqlite3_stmt *pQ = 0;
      int prepareRc;

      if( clauseRc <= 0 ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        break;
      }

      if( hasRowid ){
        zQuery = sqlite3_mprintf(
            "SELECT rowid FROM main.\"%w\" NOT INDEXED WHERE NOT (%s)",
            zTable, zExpr);
      }else{
        zQuery = sqlite3_mprintf(
            "SELECT %s FROM main.\"%w\" NOT INDEXED WHERE NOT (%s)",
            pkInfo.zPkCols, zTable, zExpr);
      }
      if( !zQuery ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        rc = SQLITE_NOMEM;
        break;
      }
      prepareRc = sqlite3_prepare_v2(db, zQuery, -1, &pQ, 0);
      sqlite3_free(zQuery);
      if( prepareRc != SQLITE_OK ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        continue;
      }

      while( sqlite3_step(pQ) == SQLITE_ROW ){
        u8 *pKey = 0; int nKey = 0;
        u8 *pVal = 0; int nVal = 0;
        char *zInfo;
        int appendRc;
        i64 intKey = 0;

        if( hasRowid ){
          intKey = sqlite3_column_int64(pQ, 0);
          rc = fetchOrphanRow(db, zTable, intKey, &pKey, &nKey, &pVal, &nVal);
        }else{
          u8 *pPkRec = 0; int nPkRec = 0;
          pPkRec = buildRecordFromStmtCols(pQ, 0, pkInfo.nPk, &nPkRec);
          if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
          rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pkInfo.nPk,
                                     &pKey, &nKey, &pVal, &nVal);
          sqlite3_free(pPkRec);
        }
        if( rc == SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
        if( rc != SQLITE_OK ){
          sqlite3_free(pKey);
          sqlite3_free(pVal);
          break;
        }

        if( haveAnc ){
          u8 *pAncVal = 0; int nAncVal = 0;
          int ancRc = hasRowid
              ? fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                       intKey, &pAncVal, &nAncVal)
              : fetchAncestorRowByKey(db, aAnc, nAnc, zTable,
                                      pKey, nKey, &pAncVal, &nAncVal);
          int preExisting = (ancRc==SQLITE_OK)
              && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
          sqlite3_free(pAncVal);
          if( preExisting ){
            sqlite3_free(pKey);
            sqlite3_free(pVal);
            continue;
          }
        }

        zInfo = sqlite3_mprintf(
            "{\"Name\": \"%w\", \"Expression\": \"%w\"}",
            zCkName ? zCkName : "", zExpr);
        appendRc = doltliteAppendConstraintViolation(
            db, zTable, DOLTLITE_CV_CHECK_CONSTRAINT,
            intKey, pKey, nKey, pVal, nVal, zInfo);
        sqlite3_free(zInfo);
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        if( appendRc != SQLITE_OK ){ rc = appendRc; break; }
        if( pnFound ) (*pnFound)++;
      }
      sqlite3_finalize(pQ);
      sqlite3_free(zExpr);
      sqlite3_free(zCkName);
      if( rc != SQLITE_OK ) break;
    }

    sqlite3_free(zTable);
    sqlite3_free(zSql);
    freeMergePkInfo(&pkInfo);
    if( rc != SQLITE_OK ) break;
  }
  if( rc == SQLITE_OK && stepRc != SQLITE_DONE && stepRc != SQLITE_ROW ){
    rc = stepRc;
  }
  sqlite3_finalize(pTbls);
  if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
  return rc;
}

static void freeStringArray(char **az, int n){
  int i;
  if( !az ) return;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

static int detectFkViolationsForSpec(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zChildTable,
  int hasRowid,
  const MergePkInfo *pChildPk,
  const char *zParentTable,
  int fkid,
  char **azFrom,
  char **azTo,
  int nCol,
  int *pnFound
){
  sqlite3_str *pSql = 0;
  char *zQuery = 0;
  sqlite3_stmt *pStmt = 0;
  int rc;

  pSql = sqlite3_str_new(0);
  sqlite3_str_appendf(pSql, "SELECT %s FROM main.\"%w\" AS c WHERE ",
      hasRowid ? "rowid" : pChildPk->zPkCols, zChildTable);
  for(int i=0; i<nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pSql, " AND ");
    sqlite3_str_appendf(pSql, "c.\"%w\" IS NOT NULL", azFrom[i]);
  }
  sqlite3_str_appendf(pSql, " AND NOT EXISTS (SELECT 1 FROM main.\"%w\" AS p WHERE ",
      zParentTable);
  for(int i=0; i<nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pSql, " AND ");
    sqlite3_str_appendf(pSql, "p.\"%w\" = c.\"%w\"", azTo[i], azFrom[i]);
  }
  sqlite3_str_appendall(pSql, ")");
  zQuery = sqlite3_str_finish(pSql);
  if( !zQuery ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
  sqlite3_free(zQuery);
  if( rc!=SQLITE_OK ) return rc;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    u8 *pKey = 0; int nKey = 0;
    u8 *pVal = 0; int nVal = 0;
    i64 intKey = 0;
    char *zInfo;
    int appendRc;

    if( hasRowid ){
      intKey = sqlite3_column_int64(pStmt, 0);
      rc = fetchOrphanRow(db, zChildTable, intKey, &pKey, &nKey, &pVal, &nVal);
    }else{
      u8 *pPkRec = 0; int nPkRec = 0;
      pPkRec = buildRecordFromStmtCols(pStmt, 0, pChildPk->nPk, &nPkRec);
      if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
      rc = fetchRowByPkFromTable(db, zChildTable, pPkRec, nPkRec, pChildPk->nPk,
                                 &pKey, &nKey, &pVal, &nVal);
      sqlite3_free(pPkRec);
    }
    if( rc == SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
    if( rc != SQLITE_OK ){
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      break;
    }

    if( aAnc ){
      u8 *pAncVal = 0; int nAncVal = 0;
      int ancRc = hasRowid
          ? fetchAncestorRowByName(db, aAnc, nAnc, zChildTable,
                                   intKey, &pAncVal, &nAncVal)
          : fetchAncestorRowByKey(db, aAnc, nAnc, zChildTable,
                                  pKey, nKey, &pAncVal, &nAncVal);
      int preExisting = (ancRc==SQLITE_OK)
          && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
      sqlite3_free(pAncVal);
      if( preExisting ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        continue;
      }
    }

    zInfo = buildFkViolationInfo(db, zChildTable, fkid);
    appendRc = doltliteAppendConstraintViolation(
        db, zChildTable, DOLTLITE_CV_FOREIGN_KEY,
        intKey, pKey, nKey, pVal, nVal, zInfo);
    sqlite3_free(zInfo);
    sqlite3_free(pKey);
    sqlite3_free(pVal);
    if( appendRc != SQLITE_OK ){
      rc = appendRc;
      break;
    }
    if( pnFound ) (*pnFound)++;
  }

  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pStmt);
  return rc;
}

int doltliteDetectMergeFkViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound
){
  sqlite3_stmt *pQuick = 0;
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  Pgno iNextAnc = 0;
  int haveAnc = 0;
  int rc;
  int nFound = 0;
  int stepRc;

  (void)pzErrMsg;

  if( pnFound ) *pnFound = 0;

  rc = sqlite3_prepare_v2(db, "PRAGMA main.foreign_key_check", -1, &pQuick, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  stepRc = sqlite3_step(pQuick);
  sqlite3_finalize(pQuick);
  pQuick = 0;
  if( stepRc==SQLITE_DONE ){
    return SQLITE_OK;
  }
  if( stepRc!=SQLITE_ROW ){
    return stepRc;
  }

  {
    int didRefresh = 0;
    rc = fkRefreshCandidateTables(db, &didRefresh);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    if( didRefresh ){
      rc = sqlite3_prepare_v2(db, "PRAGMA main.foreign_key_check", -1, &pQuick, 0);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      stepRc = sqlite3_step(pQuick);
      sqlite3_finalize(pQuick);
      pQuick = 0;
      if( stepRc==SQLITE_DONE ){
        return SQLITE_OK;
      }
      if( stepRc!=SQLITE_ROW ){
        return stepRc;
      }
    }
  }

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    if( doltliteLoadCatalog(db, pAncCatHash, &aAnc, &nAnc, &iNextAnc)==SQLITE_OK ){
      haveAnc = 1;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    char *zTable = 0;
    char *zFkQ = 0;
    sqlite3_stmt *pFk = 0;
    int hasRowid = 1;
    MergePkInfo childPk;
    int curId = -1;
    char *zParent = 0;
    char **azFrom = 0;
    char **azTo = 0;
    int nCol = 0;
    int nAlloc = 0;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }
    memset(&childPk, 0, sizeof(childPk));
    hasRowid = tableHasRowid(db, zTable);
    if( !hasRowid ){
      rc = loadMergePkInfo(db, zTable, &childPk);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTable);
        break;
      }
    }

    zFkQ = sqlite3_mprintf("PRAGMA main.foreign_key_list(%Q)", zTable);
    if( !zFkQ ){
      freeMergePkInfo(&childPk);
      sqlite3_free(zTable);
      rc = SQLITE_NOMEM;
      break;
    }
    rc = sqlite3_prepare_v2(db, zFkQ, -1, &pFk, 0);
    sqlite3_free(zFkQ);
    if( rc != SQLITE_OK ){
      freeMergePkInfo(&childPk);
      sqlite3_free(zTable);
      break;
    }

    while( sqlite3_step(pFk) == SQLITE_ROW ){
      int id = sqlite3_column_int(pFk, 0);
      const char *zParentRaw = (const char*)sqlite3_column_text(pFk, 2);
      const char *zFromRaw = (const char*)sqlite3_column_text(pFk, 3);
      const char *zToRaw = (const char*)sqlite3_column_text(pFk, 4);

      if( curId>=0 && id!=curId ){
        int needParentPk = 0;
        for(int i=0; i<nCol; i++) if( !azTo[i] ) needParentPk = 1;
        if( needParentPk ){
          MergePkInfo parentPk;
          memset(&parentPk, 0, sizeof(parentPk));
          rc = loadMergePkInfo(db, zParent, &parentPk);
          if( rc == SQLITE_OK ){
            for(int i=0; i<nCol; i++){
              if( !azTo[i] && i < parentPk.nPk ){
                azTo[i] = sqlite3_mprintf("%s", parentPk.azPk[i]);
              }
            }
          }
          freeMergePkInfo(&parentPk);
          if( rc != SQLITE_OK ) break;
        }
        rc = detectFkViolationsForSpec(db,
            haveAnc ? aAnc : 0, haveAnc ? nAnc : 0,
            zTable, hasRowid, &childPk, zParent, curId,
            azFrom, azTo, nCol, &nFound);
        freeStringArray(azFrom, nCol);
        freeStringArray(azTo, nCol);
        azFrom = 0; azTo = 0; nCol = 0; nAlloc = 0;
        sqlite3_free(zParent); zParent = 0;
        if( rc != SQLITE_OK ) break;
      }

      if( curId != id ){
        curId = id;
        zParent = sqlite3_mprintf("%s", zParentRaw ? zParentRaw : "");
        if( !zParent ){ rc = SQLITE_NOMEM; break; }
      }

      if( nCol >= nAlloc ){
        int nNew = nAlloc ? nAlloc*2 : 4;
        char **azFromNew;
        char **azToNew;
        azFromNew = sqlite3_realloc64(azFrom, (sqlite3_int64)nNew * sizeof(char*));
        if( !azFromNew ){
          rc = SQLITE_NOMEM;
          break;
        }
        azFrom = azFromNew;
        azToNew = sqlite3_realloc64(azTo, (sqlite3_int64)nNew * sizeof(char*));
        if( !azToNew ){
          rc = SQLITE_NOMEM;
          break;
        }
        azTo = azToNew;
        nAlloc = nNew;
      }
      azFrom[nCol] = sqlite3_mprintf("%s", zFromRaw ? zFromRaw : "");
      azTo[nCol] = zToRaw ? sqlite3_mprintf("%s", zToRaw) : 0;
      if( !azFrom[nCol] || (zToRaw && !azTo[nCol]) ){
        rc = SQLITE_NOMEM;
        break;
      }
      nCol++;
    }

    if( rc==SQLITE_ROW || rc==SQLITE_DONE ){
      rc = SQLITE_OK;
    }
    if( rc==SQLITE_OK && curId>=0 ){
      int needParentPk = 0;
      for(int i=0; i<nCol; i++) if( !azTo[i] ) needParentPk = 1;
      if( needParentPk ){
        MergePkInfo parentPk;
        memset(&parentPk, 0, sizeof(parentPk));
        rc = loadMergePkInfo(db, zParent, &parentPk);
        if( rc == SQLITE_OK ){
          for(int i=0; i<nCol; i++){
            if( !azTo[i] && i < parentPk.nPk ){
              azTo[i] = sqlite3_mprintf("%s", parentPk.azPk[i]);
            }
          }
        }
        freeMergePkInfo(&parentPk);
      }
      if( rc==SQLITE_OK ){
        rc = detectFkViolationsForSpec(db,
            haveAnc ? aAnc : 0, haveAnc ? nAnc : 0,
            zTable, hasRowid, &childPk, zParent, curId,
            azFrom, azTo, nCol, &nFound);
      }
    }

    freeStringArray(azFrom, nCol);
    freeStringArray(azTo, nCol);
    sqlite3_free(zParent);
    sqlite3_finalize(pFk);
    freeMergePkInfo(&childPk);
    sqlite3_free(zTable);
    if( rc != SQLITE_OK ) break;
  }

  if( rc == SQLITE_OK && stepRc != SQLITE_DONE && stepRc != SQLITE_ROW ){
    rc = stepRc;
  }
  sqlite3_finalize(pTbls);
  if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
  if( pnFound ) *pnFound = nFound;
  return rc;
}

#endif
