
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>

static i64 cfReadInt(const u8 *pRec, int nBytes){
  i64 v;
  int i;
  assert( nBytes >= 1 && nBytes <= 8 );
  if( nBytes < 1 || nBytes > 8 ) return 0;
  v = (pRec[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++){
    v = (v << 8) | pRec[i];
  }
  return v;
}

static int cfSqlRc(sqlite3_str *pStr){
  int rc = sqlite3_str_errcode(pStr);
  return rc==SQLITE_OK ? SQLITE_OK : rc;
}

static int cfAppendLiteral(
  sqlite3_str *pStr,
  const u8 **ppBodyPos,
  const u8 *pRecEnd,
  u64 st
){
  const u8 *pBodyPos = *ppBodyPos;
  int len;

  if( st==0 ){
    sqlite3_str_appendall(pStr, "NULL");
  }else if( st==8 ){
    sqlite3_str_appendall(pStr, "0");
  }else if( st==9 ){
    sqlite3_str_appendall(pStr, "1");
  }else if( st>=1 && st<=6 ){
    static const int sizes[] = {0,1,2,3,4,6,8};
    int nBytes = sizes[st];
    if( pBodyPos + nBytes > pRecEnd ) return SQLITE_CORRUPT;
    sqlite3_str_appendf(pStr, "%lld", cfReadInt(pBodyPos, nBytes));
    pBodyPos += nBytes;
  }else if( st==7 ){
    double v;
    u64 bits = 0;
    int k;
    if( pBodyPos + 8 > pRecEnd ) return SQLITE_CORRUPT;
    for(k=0; k<8; k++) bits = (bits<<8) | pBodyPos[k];
    memcpy(&v, &bits, 8);
    sqlite3_str_appendf(pStr, "%!.15g", v);
    pBodyPos += 8;
  }else if( st>=12 && (st&1)==0 ){
    int k;
    len = ((int)st - 12) / 2;
    if( pBodyPos + len > pRecEnd ) return SQLITE_CORRUPT;
    sqlite3_str_appendall(pStr, "X'");
    for(k=0; k<len; k++){
      sqlite3_str_appendf(pStr, "%02x", pBodyPos[k]);
    }
    sqlite3_str_appendall(pStr, "'");
    pBodyPos += len;
  }else if( st>=13 && (st&1)==1 ){
    char *zText;
    len = ((int)st - 13) / 2;
    if( pBodyPos + len > pRecEnd ) return SQLITE_CORRUPT;
    zText = sqlite3_malloc(len + 1);
    if( !zText ) return SQLITE_NOMEM;
    memcpy(zText, pBodyPos, len);
    zText[len] = 0;
    sqlite3_str_appendf(pStr, "%Q", zText);
    sqlite3_free(zText);
    pBodyPos += len;
  }else{
    return SQLITE_CORRUPT;
  }

  *ppBodyPos = pBodyPos;
  return cfSqlRc(pStr);
}

/* Advance pBody past the value for a single serial-type code without
** writing anything. Returns SQLITE_OK or SQLITE_CORRUPT. */
static int cfSkipField(const u8 **ppBody, const u8 *pRecEnd, u64 st){
  const u8 *p = *ppBody;
  int len = 0;
  if( st==0 || st==8 || st==9 ){
    /* NULL or integer constants 0/1: zero body bytes. */
  }else if( st>=1 && st<=6 ){
    static const int sizes[] = {0,1,2,3,4,6,8};
    len = sizes[st];
  }else if( st==7 ){
    len = 8;
  }else if( st>=12 && (st&1)==0 ){
    len = ((int)st - 12) / 2;
  }else if( st>=13 && (st&1)==1 ){
    len = ((int)st - 13) / 2;
  }else{
    return SQLITE_CORRUPT;
  }
  if( p + len > pRecEnd ) return SQLITE_CORRUPT;
  *ppBody = p + len;
  return SQLITE_OK;
}

/* Build a DELETE-by-PK statement for a user table, using a conflict
** row's existing record body (typically pOurVal) as the source of the
** PK value(s). Used by the --theirs resolution path when the theirs
** side is a delete: we need to remove the row currently in the table,
** and its PK must come from somewhere.
**
** Two modes:
**
**   - rowid-aliased (INTEGER PRIMARY KEY, single PK column): the
**     record body does NOT include the PK. Caller passes the row's
**     intKey and we emit `DELETE ... WHERE <pk>=intKey`.
**
**   - user PK (INT/TEXT/composite): the record body includes all
**     columns. We walk PRAGMA table_info, find the PK column(s), and
**     extract the values for the PK ordinal(s) from the body.
*/
static int buildDeleteByKeySql(
  sqlite3 *db,
  const char *zTable,
  i64 intKey,
  const u8 *pRec,
  int nRec,
  char **pzSql
){
  sqlite3_stmt *pInfo = 0;
  char *zInfoSql;
  int rc;
  sqlite3_str *pDel = 0;
  const u8 *pPos, *pRecEnd, *pHdrEnd, *pBody;
  u64 hdrSize;
  int hdrBytes;
  int colIdx;
  int nPk = 0;
  int nAllCols = 0;
  int pkEmitted = 0;
  int rowidAliased = 0;
  int iPkCol = -1;

  typedef struct {
    char *zName;
    int  colIdx;  /* ordinal in the table_info listing */
  } PkCol;
  PkCol *aPk = 0;

  *pzSql = 0;

  zInfoSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
  if( !zInfoSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zInfoSql, -1, &pInfo, 0);
  sqlite3_free(zInfoSql);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pInfo))==SQLITE_ROW ){
    int pk = sqlite3_column_int(pInfo, 5);
    const char *zType = (const char*)sqlite3_column_text(pInfo, 2);
    if( pk>0 ){
      const char *zName = (const char*)sqlite3_column_text(pInfo, 1);
      PkCol *aNew = sqlite3_realloc(aPk, (nPk+1)*(int)sizeof(PkCol));
      if( !aNew ){ rc = SQLITE_NOMEM; break; }
      aPk = aNew;
      aPk[nPk].zName = sqlite3_mprintf("%s", zName ? zName : "");
      aPk[nPk].colIdx = nAllCols;
      if( !aPk[nPk].zName ){ rc = SQLITE_NOMEM; break; }
      if( pk==1 && zType && sqlite3_stricmp(zType, "INTEGER")==0 ){
        iPkCol = nAllCols;
      }
      nPk++;
    }
    nAllCols++;
  }
  sqlite3_finalize(pInfo);
  if( rc!=SQLITE_OK && rc!=SQLITE_DONE ) goto delete_cleanup;
  rc = SQLITE_OK;
  if( nPk==0 ){
    rc = SQLITE_CONSTRAINT;
    goto delete_cleanup;
  }

  rowidAliased = (nPk==1 && iPkCol>=0) ? 1 : 0;

  if( rowidAliased ){
    /* Simple case: single INTEGER PK, record body has no PK. */
    pDel = sqlite3_str_new(0);
    if( !pDel ){ rc = SQLITE_NOMEM; goto delete_cleanup; }
    sqlite3_str_appendf(pDel, "DELETE FROM \"%w\" WHERE \"%w\"=%lld",
                        zTable, aPk[0].zName, intKey);
    goto delete_finalize;
  }

  /* Walk the record body, appending only the PK columns to the
  ** WHERE clause and skipping the others. */
  pPos = pRec;
  pRecEnd = pRec + nRec;
  hdrBytes = dlReadVarint(pPos, pRecEnd, &hdrSize);
  if( hdrBytes <= 0 || hdrSize < (u64)hdrBytes || hdrSize > (u64)nRec ){
    rc = SQLITE_CORRUPT;
    goto delete_cleanup;
  }
  pPos += hdrBytes;
  pHdrEnd = pRec + (int)hdrSize;
  pBody = pRec + (int)hdrSize;

  pDel = sqlite3_str_new(0);
  if( !pDel ){ rc = SQLITE_NOMEM; goto delete_cleanup; }
  sqlite3_str_appendf(pDel, "DELETE FROM \"%w\" WHERE ", zTable);

  colIdx = 0;
  while( pPos < pHdrEnd && colIdx < nAllCols ){
    u64 st;
    int stBytes = dlReadVarint(pPos, pHdrEnd, &st);
    int k, isPk = 0;
    if( stBytes <= 0 ){ rc = SQLITE_CORRUPT; break; }
    pPos += stBytes;

    for(k=0; k<nPk; k++){
      if( aPk[k].colIdx==colIdx ){ isPk = 1; break; }
    }
    if( isPk ){
      if( pkEmitted>0 ) sqlite3_str_appendall(pDel, " AND ");
      sqlite3_str_appendf(pDel, "\"%w\"=", aPk[k].zName);
      rc = cfAppendLiteral(pDel, &pBody, pRecEnd, st);
      if( rc!=SQLITE_OK ) break;
      pkEmitted++;
    }else{
      rc = cfSkipField(&pBody, pRecEnd, st);
      if( rc!=SQLITE_OK ) break;
    }
    colIdx++;
  }
  if( rc==SQLITE_OK && pkEmitted!=nPk ) rc = SQLITE_CORRUPT;

delete_finalize:
delete_cleanup:
  {
    int k;
    for(k=0; k<nPk; k++) sqlite3_free(aPk[k].zName);
    sqlite3_free(aPk);
  }
  if( rc!=SQLITE_OK ){
    if( pDel ) sqlite3_str_reset(pDel);
    return rc;
  }
  *pzSql = sqlite3_str_finish(pDel);
  if( !*pzSql ) return SQLITE_NOMEM;
  return SQLITE_OK;
}

static int buildInsertSql(
  const char *zTable,
  i64 intKey,
  int rowidAliased,   /* 1: INTEGER PK case, use intKey for PK column */
  int iPkCol,         /* ordinal of the aliased PK column (if rowidAliased) */
  char **azCol, int nCol,
  const u8 *pRec, const u8 *pRecEnd,
  const u8 *pHdrEnd, const u8 *pBody,
  char **pzSql
){
  sqlite3_str *pIns = sqlite3_str_new(0);
  sqlite3_str *pVals = sqlite3_str_new(0);
  const u8 *pBodyPos = pBody;
  int colIdx = 0;
  int emitted = 0;
  char *zIns;
  char *zVals;
  int rc;

  *pzSql = 0;
  if( !pIns || !pVals ){
    sqlite3_str_reset(pIns);
    sqlite3_str_reset(pVals);
    return SQLITE_NOMEM;
  }
  sqlite3_str_appendf(pIns, "INSERT OR REPLACE INTO \"%w\"(", zTable);
  sqlite3_str_appendf(pVals, "VALUES(");
  rc = cfSqlRc(pIns);
  if( rc==SQLITE_OK ) rc = cfSqlRc(pVals);
  if( rc!=SQLITE_OK ){
    sqlite3_str_reset(pIns);
    sqlite3_str_reset(pVals);
    return rc;
  }

  /* For rowid-aliased INTEGER PK tables the record body does NOT
  ** include the PK; emit it explicitly from intKey first, then walk
  ** the record for the remaining columns (in azCol order, skipping
  ** the PK ordinal). For user-PK tables (INT/TEXT/composite) the
  ** record includes all columns and we walk them linearly. */
  if( rowidAliased && iPkCol>=0 && iPkCol<nCol ){
    sqlite3_str_appendf(pIns, "\"%w\"", azCol[iPkCol]);
    sqlite3_str_appendf(pVals, "%lld", intKey);
    rc = cfSqlRc(pIns);
    if( rc==SQLITE_OK ) rc = cfSqlRc(pVals);
    if( rc!=SQLITE_OK ){
      sqlite3_str_reset(pIns);
      sqlite3_str_reset(pVals);
      return rc;
    }
    emitted = 1;
  }

  while( pRec < pHdrEnd && pRec < pRecEnd && colIdx < nCol ){
    u64 st;
    int stBytes;
    /* Skip the aliased-PK ordinal in azCol since it was emitted above. */
    if( rowidAliased && colIdx==iPkCol ){ colIdx++; continue; }

    stBytes = dlReadVarint(pRec, pHdrEnd, &st);
    if( stBytes <= 0 ) rc = SQLITE_CORRUPT;
    pRec += stBytes;
    if( rc==SQLITE_OK ){
      if( emitted>0 ){
        sqlite3_str_appendall(pIns, ",");
        sqlite3_str_appendall(pVals, ",");
      }
      sqlite3_str_appendf(pIns, "\"%w\"", azCol[colIdx]);
      rc = cfSqlRc(pIns);
    }
    if( rc==SQLITE_OK ){
      rc = cfAppendLiteral(pVals, &pBodyPos, pRecEnd, st);
    }
    if( rc!=SQLITE_OK ) break;
    emitted++;
    colIdx++;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_str_reset(pIns);
    sqlite3_str_reset(pVals);
    return rc;
  }
  zIns = sqlite3_str_finish(pIns);
  zVals = sqlite3_str_finish(pVals);
  if( !zIns || !zVals ){
    sqlite3_free(zIns);
    sqlite3_free(zVals);
    return SQLITE_NOMEM;
  }
  *pzSql = sqlite3_mprintf("%s) %s)", zIns, zVals);
  sqlite3_free(zIns);
  sqlite3_free(zVals);
  if( !*pzSql ) return SQLITE_NOMEM;
  return SQLITE_OK;
}

static int applyTheirRecord(
  sqlite3 *db,
  const char *zTable,
  i64 intKey,
  const u8 *pRec,
  int nRec
){
  sqlite3_stmt *pInfo = 0;
  char *zSql;
  int rc, nCol = 0;
  const u8 *pPos, *pRecEnd, *pHdrEnd, *pBody;
  u64 hdrSize;
  int hdrBytes;
  int rowidAliased = 0;
  int iPkCol = -1;
  int nPk = 0;

  char **azCol = 0;
  int nColAlloc = 0;

  zSql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTable);
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pInfo, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pInfo))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pInfo, 1);
    const char *zType = (const char*)sqlite3_column_text(pInfo, 2);
    int pk = sqlite3_column_int(pInfo, 5);
    if( pk>0 ){
      nPk++;
      if( pk==1 && zType && sqlite3_stricmp(zType, "INTEGER")==0 ){
        iPkCol = nCol;
      }
    }
    if( nCol >= nColAlloc ){
      char **azNew;
      nColAlloc = nColAlloc ? nColAlloc*2 : 8;
      azNew = sqlite3_realloc(azCol, nColAlloc * (int)sizeof(char*));
      if( !azNew ){
        int k;
        sqlite3_finalize(pInfo);
        for(k=0; k<nCol; k++) sqlite3_free(azCol[k]);
        sqlite3_free(azCol);
        return SQLITE_NOMEM;
      }
      azCol = azNew;
    }
    azCol[nCol] = sqlite3_mprintf("%s", zName);
    if( !azCol[nCol] ){
      int k;
      sqlite3_finalize(pInfo);
      for(k=0; k<nCol; k++) sqlite3_free(azCol[k]);
      sqlite3_free(azCol);
      return SQLITE_NOMEM;
    }
    nCol++;
  }
  sqlite3_finalize(pInfo);
  if( rc!=SQLITE_DONE ){
    int k;
    for(k=0; k<nCol; k++) sqlite3_free(azCol[k]);
    sqlite3_free(azCol);
    return rc;
  }

  /* Rowid-aliased iff the single PK column is declared INTEGER. In
  ** that case the record body omits the PK (it's the intKey). For
  ** any other PK shape (INT, TEXT, composite) the record body
  ** includes every column in declaration order. */
  rowidAliased = (nPk==1 && iPkCol>=0) ? 1 : 0;

  pPos = pRec;
  pRecEnd = pRec + nRec;
  hdrBytes = dlReadVarint(pPos, pRecEnd, &hdrSize);
  if( hdrBytes <= 0 || hdrSize < (u64)hdrBytes || hdrSize > (u64)nRec ){
    int k;
    for(k=0; k<nCol; k++) sqlite3_free(azCol[k]);
    sqlite3_free(azCol);
    return SQLITE_CORRUPT;
  }
  pPos += hdrBytes;
  pHdrEnd = pRec + (int)hdrSize;
  pBody = pRec + (int)hdrSize;

  /* No field skipping — the record body includes ALL columns (including
  ** the PK) for doltlite user-PK tables. buildInsertSql reads them all
  ** and uses the PK column name directly (not "rowid", which doesn't
  ** exist on doltlite user-PK tables). */
  rc = buildInsertSql(zTable, intKey, rowidAliased, iPkCol, azCol, nCol,
                      pPos, pRecEnd, pHdrEnd, pBody, &zSql);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }

  
  {
    int k;
    for(k=0; k<nCol; k++) sqlite3_free(azCol[k]);
    sqlite3_free(azCol);
  }

  return rc;
}

typedef struct ConflictTableInfo ConflictTableInfo;
struct ConflictTableInfo {
  char *zName;
  int nConflicts;
  struct ConflictRow {
    i64 intKey;
    u8 *pKey; int nKey;       
    u8 *pBaseVal; int nBaseVal;
    u8 *pOurVal; int nOurVal;
    u8 *pTheirVal; int nTheirVal;
  } *aRows;
};

static void freeConflictTables(ConflictTableInfo *aTables, int nTables);

static void freeConflictRow(struct ConflictRow *pRow){
  if( !pRow ) return;
  sqlite3_free(pRow->pKey);
  sqlite3_free(pRow->pBaseVal);
  sqlite3_free(pRow->pOurVal);
  sqlite3_free(pRow->pTheirVal);
  memset(pRow, 0, sizeof(*pRow));
}

static int dupConflictBytes(const u8 *pIn, int nIn, u8 **ppOut){
  u8 *pCopy;
  *ppOut = 0;
  if( !pIn || nIn<=0 ) return SQLITE_OK;
  pCopy = sqlite3_malloc(nIn);
  if( !pCopy ) return SQLITE_NOMEM;
  memcpy(pCopy, pIn, nIn);
  *ppOut = pCopy;
  return SQLITE_OK;
}

static void removeConflictRow(ConflictTableInfo *pTable, int iRow){
  if( !pTable || iRow<0 || iRow>=pTable->nConflicts ) return;
  freeConflictRow(&pTable->aRows[iRow]);
  if( iRow < pTable->nConflicts - 1 ){
    memmove(&pTable->aRows[iRow], &pTable->aRows[iRow+1],
            (pTable->nConflicts - iRow - 1) * sizeof(struct ConflictRow));
  }
  pTable->nConflicts--;
  if( pTable->aRows ){
    memset(&pTable->aRows[pTable->nConflicts], 0, sizeof(struct ConflictRow));
  }
}

static void removeConflictTable(ConflictTableInfo *aTables, int *pnTables, int iTable){
  int nTables;
  if( !aTables || !pnTables ) return;
  nTables = *pnTables;
  if( iTable<0 || iTable>=nTables ) return;
  sqlite3_free(aTables[iTable].zName);
  {
    int j;
    for(j=0; j<aTables[iTable].nConflicts; j++){
      freeConflictRow(&aTables[iTable].aRows[j]);
    }
  }
  sqlite3_free(aTables[iTable].aRows);
  memset(&aTables[iTable], 0, sizeof(aTables[iTable]));
  if( iTable < nTables - 1 ){
    memmove(&aTables[iTable], &aTables[iTable+1],
            (nTables - iTable - 1) * sizeof(ConflictTableInfo));
  }
  (*pnTables)--;
  memset(&aTables[*pnTables], 0, sizeof(aTables[*pnTables]));
}

int doltliteSerializeConflicts(
  ChunkStore *cs,
  ConflictTableInfo *aTables, int nTables,
  ProllyHash *pHash
){
  int sz = 2;  
  int i, j, rc;
  u8 *buf, *p;

  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    sz += 2 + nl + 4;
    for(j=0; j<aTables[i].nConflicts; j++){
      sz += 8 + 4 + aTables[i].aRows[j].nBaseVal
              + 4 + aTables[i].aRows[j].nOurVal
              + 4 + aTables[i].aRows[j].nTheirVal;
    }
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  p[0]=(u8)nTables; p[1]=(u8)(nTables>>8); p+=2;
  for(i=0; i<nTables; i++){
    int nl = aTables[i].zName ? (int)strlen(aTables[i].zName) : 0;
    int nc = aTables[i].nConflicts;
    p[0]=(u8)nl; p[1]=(u8)(nl>>8); p+=2;
    if(nl>0) memcpy(p, aTables[i].zName, nl);
    p += nl;
    p[0]=(u8)nc; p[1]=(u8)(nc>>8); p[2]=(u8)(nc>>16); p[3]=(u8)(nc>>24); p+=4;
    for(j=0; j<nc; j++){
      struct ConflictRow *cr = &aTables[i].aRows[j];
      i64 k = cr->intKey;
      p[0]=(u8)k; p[1]=(u8)(k>>8); p[2]=(u8)(k>>16); p[3]=(u8)(k>>24);
      p[4]=(u8)(k>>32); p[5]=(u8)(k>>40); p[6]=(u8)(k>>48); p[7]=(u8)(k>>56);
      p+=8;
      { int n=cr->nBaseVal; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nBaseVal>0){ memcpy(p, cr->pBaseVal, cr->nBaseVal); p+=cr->nBaseVal; }
      { int n=cr->nOurVal; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nOurVal>0){ memcpy(p, cr->pOurVal, cr->nOurVal); p+=cr->nOurVal; }
      { int n=cr->nTheirVal; p[0]=(u8)n; p[1]=(u8)(n>>8); p[2]=(u8)(n>>16); p[3]=(u8)(n>>24); p+=4; }
      if(cr->nTheirVal>0){ memcpy(p, cr->pTheirVal, cr->nTheirVal); p+=cr->nTheirVal; }
    }
  }

  rc = chunkStorePut(cs, buf, (int)(p-buf), pHash);
  sqlite3_free(buf);
  return rc;
}

static int loadAllConflicts(
  sqlite3 *db,
  ChunkStore *cs,
  ConflictTableInfo **ppTables, int *pnTables
){
  ProllyHash hash;
  u8 *data = 0; int nData = 0;
  extern void doltliteGetSessionConflictsCatalog(sqlite3*, ProllyHash*);
  const u8 *p;
  int nTables, i, j, rc;
  ConflictTableInfo *aTables;

  doltliteGetSessionConflictsCatalog(db, &hash);
  if( prollyHashIsEmpty(&hash) ){ *ppTables = 0; *pnTables = 0; return SQLITE_OK; }

  rc = chunkStoreGet(cs, &hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData<2 ){ sqlite3_free(data); return SQLITE_CORRUPT; }

  p = data;
  nTables = p[0]|(p[1]<<8); p+=2;

  aTables = sqlite3_malloc(nTables * (int)sizeof(ConflictTableInfo));
  if( !aTables ){ sqlite3_free(data); return SQLITE_NOMEM; }
  memset(aTables, 0, nTables * (int)sizeof(ConflictTableInfo));

  for(i=0; i<nTables; i++){
    int nl, nc;
    if( p+2 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    nl = p[0]|(p[1]<<8); p+=2;
    if( nl<0 || p+nl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    aTables[i].zName = sqlite3_malloc(nl+1);
    if( !aTables[i].zName ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
    memcpy(aTables[i].zName, p, nl); aTables[i].zName[nl]=0;
    p += nl;
    if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    nc = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
    if( nc<0 ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
    aTables[i].nConflicts = nc;
    aTables[i].aRows = sqlite3_malloc(nc * (int)sizeof(struct ConflictRow));
    if( !aTables[i].aRows ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
    memset(aTables[i].aRows, 0, nc * (int)sizeof(struct ConflictRow));

    for(j=0; j<nc; j++){
      struct ConflictRow *cr = &aTables[i].aRows[j];
      int bvl, ovl, tvl;
      if( p+8 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      cr->intKey = (i64)((u64)p[0] | ((u64)p[1]<<8) | ((u64)p[2]<<16) | ((u64)p[3]<<24) |
                         ((u64)p[4]<<32) | ((u64)p[5]<<40) | ((u64)p[6]<<48) | ((u64)p[7]<<56));
      p+=8;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      bvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( bvl<0 || p+bvl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(bvl>0){
        cr->pBaseVal = sqlite3_malloc(bvl);
        if( !cr->pBaseVal ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pBaseVal, p, bvl);
        cr->nBaseVal = bvl;
      }
      p += bvl;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      ovl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( ovl<0 || p+ovl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(ovl>0){
        cr->pOurVal = sqlite3_malloc(ovl);
        if( !cr->pOurVal ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pOurVal, p, ovl);
        cr->nOurVal = ovl;
      }
      p += ovl;
      if( p+4 > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      tvl = p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); p+=4;
      if( tvl<0 || p+tvl > data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }
      if(tvl>0){
        cr->pTheirVal = sqlite3_malloc(tvl);
        if( !cr->pTheirVal ){ rc = SQLITE_NOMEM; goto conflicts_cleanup; }
        memcpy(cr->pTheirVal, p, tvl);
        cr->nTheirVal = tvl;
      }
      p += tvl;
    }
  }

  if( p!=data+nData ){ rc = SQLITE_CORRUPT; goto conflicts_cleanup; }

  *ppTables = aTables;
  *pnTables = nTables;
  sqlite3_free(data);
  return SQLITE_OK;

conflicts_cleanup:
  freeConflictTables(aTables, nTables);
  sqlite3_free(data);
  return rc;
}

static void freeConflictTables(ConflictTableInfo *aTables, int nTables){
  int i, j;
  for(i=0; i<nTables; i++){
    for(j=0; j<aTables[i].nConflicts; j++){
      freeConflictRow(&aTables[i].aRows[j]);
    }
    sqlite3_free(aTables[i].aRows);
    sqlite3_free(aTables[i].zName);
  }
  sqlite3_free(aTables);
}

static int storeUpdatedConflicts(
  sqlite3 *db,
  ChunkStore *cs,
  ConflictTableInfo *aTables, int nTables
){
  int totalConflicts = 0;
  int i;
  for(i=0; i<nTables; i++) totalConflicts += aTables[i].nConflicts;

  {
    int rc;
    extern void doltliteSetSessionConflictsCatalog(sqlite3*, const ProllyHash*);
    extern void doltliteSetSessionMergeState(sqlite3*, u8, const ProllyHash*, const ProllyHash*);
    extern int doltliteSaveWorkingSet(sqlite3*);
    if( totalConflicts==0 ){
      doltliteSetSessionConflictsCatalog(db, &(ProllyHash){{0}});
    }else{
      ProllyHash newHash;
      int rc = doltliteSerializeConflicts(cs, aTables, nTables, &newHash);
      if( rc!=SQLITE_OK ) return rc;
      doltliteSetSessionConflictsCatalog(db, &newHash);
      doltliteSetSessionMergeState(db, 1, 0, &newHash);
    }
    rc = doltliteSaveWorkingSet(db);
    if( rc!=SQLITE_OK ) return rc;
    rc = chunkStoreSerializeRefs(cs);
    if( rc!=SQLITE_OK ) return rc;
    return chunkStoreCommit(cs);
  }
}

typedef struct ConflictsVtab ConflictsVtab;
struct ConflictsVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct ConflictsCur ConflictsCur;
struct ConflictsCur {
  sqlite3_vtab_cursor base;
  ConflictTableInfo *aTables; int nTables; int iRow;
};

static int cfConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  ConflictsVtab *v; int rc;
  (void)pAux;(void)argc;(void)argv;(void)pzErr;
  /* Column is named "table" to match Dolt; the quoting is required because
  ** "table" is a reserved keyword in SQL. Callers must quote it on access:
  ** SELECT "table" FROM dolt_conflicts. */
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(\"table\" TEXT, num_conflicts INTEGER)");
  if(rc!=SQLITE_OK) return rc;
  v = sqlite3_malloc(sizeof(*v)); if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v)); v->db=db; *ppVtab=&v->base; return SQLITE_OK;
}
static int cfDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int cfOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  ConflictsCur *c=sqlite3_malloc(sizeof(*c)); (void)v;
  if(!c) return SQLITE_NOMEM; memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}
static int cfClose(sqlite3_vtab_cursor *cur){
  ConflictsCur *c=(ConflictsCur*)cur;
  freeConflictTables(c->aTables, c->nTables);
  sqlite3_free(c); return SQLITE_OK;
}
static int cfFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **v){
  ConflictsCur *c=(ConflictsCur*)cur;
  ConflictsVtab *vt=(ConflictsVtab*)cur->pVtab;
  (void)n;(void)s;(void)a;(void)v;
  c->iRow=0;
  return loadAllConflicts(vt->db, doltliteGetChunkStore(vt->db), &c->aTables, &c->nTables);
}
static int cfNext(sqlite3_vtab_cursor *cur){ ((ConflictsCur*)cur)->iRow++; return SQLITE_OK; }
static int cfEof(sqlite3_vtab_cursor *cur){ ConflictsCur *c=(ConflictsCur*)cur; return c->iRow>=c->nTables; }
static int cfColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  ConflictsCur *c=(ConflictsCur*)cur;
  switch(col){
    case 0: sqlite3_result_text(ctx, c->aTables[c->iRow].zName, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_int(ctx, c->aTables[c->iRow].nConflicts); break;
  }
  return SQLITE_OK;
}
static int cfRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){ *r=((ConflictsCur*)cur)->iRow; return SQLITE_OK; }
static int cfBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){ (void)v; p->estimatedCost=10; return SQLITE_OK; }

static sqlite3_module conflictsModule = {
  0,0,cfConnect,cfBestIndex,cfDisconnect,0,cfOpen,cfClose,cfFilter,cfNext,cfEof,
  cfColumn,cfRowid,0,0,0,0,0,0,0,0,0,0,0,0
};

typedef struct CfRowVtab CfRowVtab;
struct CfRowVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct CfRowCur CfRowCur;
struct CfRowCur {
  sqlite3_vtab_cursor base;
  ConflictTableInfo *aTables;
  int nTables;
  int iTableIdx;       
  int iRow;            
};

/* Build a dolt-conformant dolt_conflicts_<table> schema by projecting
** every user column of `t` under three prefixes (base_, our_, their_),
** plus the standard metadata columns Dolt emits: from_root_ish,
** our_diff_type, their_diff_type, dolt_conflict_id. */
static char *cfrBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  int i;
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(from_root_ish TEXT");
  /* base_* */
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"base_%w\"", ci->azName[i]);
  }
  /* our_* + our_diff_type */
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"our_%w\"", ci->azName[i]);
  }
  sqlite3_str_appendall(pStr, ", our_diff_type TEXT");
  /* their_* + their_diff_type */
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendf(pStr, ", \"their_%w\"", ci->azName[i]);
  }
  sqlite3_str_appendall(pStr, ", their_diff_type TEXT");
  sqlite3_str_appendall(pStr, ", dolt_conflict_id TEXT");
  sqlite3_str_appendall(pStr, ")");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int cfrConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  CfRowVtab *v;
  int rc;
  const char *zModuleName;
  char *zSchema;
  (void)pAux;

  v = sqlite3_malloc(sizeof(*v));
  if(!v) return SQLITE_NOMEM;
  memset(v,0,sizeof(*v));
  v->db = db;

  zModuleName = argv[0];
  if( zModuleName && strncmp(zModuleName, "dolt_conflicts_", 15)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zModuleName + 15);
  }else if( argc > 3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }
  if( !v->zTableName ){
    sqlite3_free(v);
    return SQLITE_NOMEM;
  }

  rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return rc;
  }

  zSchema = cfrBuildSchema(&v->cols);
  if( !zSchema ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(v->zTableName);
    doltliteFreeColInfo(&v->cols);
    sqlite3_free(v);
    return rc;
  }

  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int cfrDisconnect(sqlite3_vtab *pVtab){
  CfRowVtab *v = (CfRowVtab*)pVtab;
  sqlite3_free(v->zTableName);
  doltliteFreeColInfo(&v->cols);
  sqlite3_free(v);
  return SQLITE_OK;
}

static int cfrOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  CfRowCur *c = sqlite3_malloc(sizeof(*c));
  (void)pVtab;
  if(!c) return SQLITE_NOMEM;
  memset(c,0,sizeof(*c));
  c->iTableIdx = -1;
  *pp = &c->base;
  return SQLITE_OK;
}

static int cfrClose(sqlite3_vtab_cursor *cur){
  CfRowCur *c = (CfRowCur*)cur;
  freeConflictTables(c->aTables, c->nTables);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int cfrFilter(sqlite3_vtab_cursor *cur, int n, const char *s, int a, sqlite3_value **v){
  CfRowCur *c = (CfRowCur*)cur;
  CfRowVtab *vt = (CfRowVtab*)cur->pVtab;
  int i, rc;
  (void)n;(void)s;(void)a;(void)v;

  c->iRow = 0;
  c->iTableIdx = -1;
  rc = loadAllConflicts(vt->db, doltliteGetChunkStore(vt->db), &c->aTables, &c->nTables);
  if( rc!=SQLITE_OK ) return rc;

  
  for(i=0; i<c->nTables; i++){
    if( c->aTables[i].zName && strcmp(c->aTables[i].zName, vt->zTableName)==0 ){
      c->iTableIdx = i;
      break;
    }
  }
  return SQLITE_OK;
}

static int cfrNext(sqlite3_vtab_cursor *cur){
  ((CfRowCur*)cur)->iRow++;
  return SQLITE_OK;
}

static int cfrEof(sqlite3_vtab_cursor *cur){
  CfRowCur *c = (CfRowCur*)cur;
  if( c->iTableIdx < 0 ) return 1;
  return c->iRow >= c->aTables[c->iTableIdx].nConflicts;
}

/* Emit one projected column from a record body. If iPkCol>=0 and
** iUserCol==iPkCol, the PK value comes from intKey (rowid-aliased
** INTEGER PRIMARY KEY case) rather than the record body. */
static void cfrEmitRecordCol(
  sqlite3_context *ctx,
  const u8 *pRec, int nRec,
  int iUserCol,
  int iPkCol,
  i64 intKey
){
  if( !pRec || nRec<=0 ){
    sqlite3_result_null(ctx);
    return;
  }
  if( iUserCol==iPkCol ){
    sqlite3_result_int64(ctx, intKey);
    return;
  }
  {
    DoltliteRecordInfo ri;
    doltliteParseRecord(pRec, nRec, &ri);
    if( iUserCol >= ri.nField ){
      sqlite3_result_null(ctx);
      return;
    }
    doltliteResultField(ctx, pRec, nRec, ri.aType[iUserCol], ri.aOffset[iUserCol]);
  }
}

/* Classify a conflict row on one side as added/modified/removed
** relative to its base. Dolt uses "added" if the base is missing,
** "removed" if the side is missing, otherwise "modified". */
static const char *cfrDiffType(const u8 *pBase, int nBase,
                               const u8 *pSide, int nSide){
  int baseHas = (pBase && nBase>0);
  int sideHas = (pSide && nSide>0);
  if( !sideHas ) return "removed";
  if( !baseHas ) return "added";
  return "modified";
}

static int cfrColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  CfRowCur *c = (CfRowCur*)cur;
  CfRowVtab *v = (CfRowVtab*)cur->pVtab;
  struct ConflictRow *cr;
  int nUserCols;
  int colBaseStart, colOurStart, colOurDiff;
  int colTheirStart, colTheirDiff, colConflictId;

  if( c->iTableIdx < 0 ) return SQLITE_OK;
  if( c->iRow >= c->aTables[c->iTableIdx].nConflicts ) return SQLITE_OK;
  cr = &c->aTables[c->iTableIdx].aRows[c->iRow];

  /* Schema layout:
  **   col 0                     : from_root_ish
  **   col 1..nUserCols          : base_<col>
  **   col nUserCols+1..2*nUserCols : our_<col>
  **   col 2*nUserCols+1         : our_diff_type
  **   col 2*nUserCols+2..3*nUserCols+1 : their_<col>
  **   col 3*nUserCols+2         : their_diff_type
  **   col 3*nUserCols+3         : dolt_conflict_id
  */
  nUserCols = v->cols.nCol;
  colBaseStart  = 1;
  colOurStart   = 1 + nUserCols;
  colOurDiff    = 1 + 2*nUserCols;
  colTheirStart = 2 + 2*nUserCols;
  colTheirDiff  = 2 + 3*nUserCols;
  colConflictId = 3 + 3*nUserCols;

  if( col==0 ){
    /* from_root_ish: doltlite doesn't track this per-conflict. Emit
    ** NULL — oracle tests should not compare this column. */
    sqlite3_result_null(ctx);
  }else if( col>=colBaseStart && col<colOurStart ){
    cfrEmitRecordCol(ctx, cr->pBaseVal, cr->nBaseVal,
                     col - colBaseStart, v->cols.iPkCol, cr->intKey);
  }else if( col>=colOurStart && col<colOurDiff ){
    cfrEmitRecordCol(ctx, cr->pOurVal, cr->nOurVal,
                     col - colOurStart, v->cols.iPkCol, cr->intKey);
  }else if( col==colOurDiff ){
    sqlite3_result_text(ctx,
      cfrDiffType(cr->pBaseVal, cr->nBaseVal, cr->pOurVal, cr->nOurVal),
      -1, SQLITE_STATIC);
  }else if( col>=colTheirStart && col<colTheirDiff ){
    cfrEmitRecordCol(ctx, cr->pTheirVal, cr->nTheirVal,
                     col - colTheirStart, v->cols.iPkCol, cr->intKey);
  }else if( col==colTheirDiff ){
    sqlite3_result_text(ctx,
      cfrDiffType(cr->pBaseVal, cr->nBaseVal, cr->pTheirVal, cr->nTheirVal),
      -1, SQLITE_STATIC);
  }else if( col==colConflictId ){
    /* Stable synthetic id: intKey + iRow. Oracle tests should not
    ** compare this column since Dolt uses a different scheme. */
    char buf[64];
    sqlite3_snprintf(sizeof(buf), buf, "%lld:%d", cr->intKey, c->iRow);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int cfrRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  CfRowCur *c = (CfRowCur*)cur;
  if( c->iTableIdx >= 0 && c->iRow < c->aTables[c->iTableIdx].nConflicts ){
    *r = c->aTables[c->iTableIdx].aRows[c->iRow].intKey;
  }else{
    *r = 0;
  }
  return SQLITE_OK;
}

static int cfrBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost = 10;
  return SQLITE_OK;
}

static int cfrUpdate(
  sqlite3_vtab *pVtab,
  int nArg,
  sqlite3_value **apArg,
  sqlite3_int64 *pRowid
){
  CfRowVtab *v = (CfRowVtab*)pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i, j, rc;
  i64 deleteRowid;

  (void)pRowid;

  
  if( nArg != 1 ){
    pVtab->zErrMsg = sqlite3_mprintf("only DELETE is supported on conflict tables");
    return SQLITE_ERROR;
  }

  deleteRowid = sqlite3_value_int64(apArg[0]);

  
  rc = loadAllConflicts(v->db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ) return rc;

  
  for(i=0; i<nTables; i++){
    if( !aTables[i].zName || strcmp(aTables[i].zName, v->zTableName)!=0 )
      continue;

    for(j=0; j<aTables[i].nConflicts; j++){
      if( aTables[i].aRows[j].intKey == deleteRowid ){
        removeConflictRow(&aTables[i], j);

        
        if( aTables[i].nConflicts == 0 ){
          removeConflictTable(aTables, &nTables, i);
        }

        
        rc = storeUpdatedConflicts(v->db, cs, aTables, nTables);
        freeConflictTables(aTables, nTables);
        return rc;
      }
    }
    break;
  }

  freeConflictTables(aTables, nTables);
  return SQLITE_OK; 
}

static sqlite3_module cfRowModule = {
  0,                   
  cfrConnect,          
  cfrConnect,          
  cfrBestIndex,        
  cfrDisconnect,       
  cfrDisconnect,       
  cfrOpen,             
  cfrClose,            
  cfrFilter,           
  cfrNext,             
  cfrEof,              
  cfrColumn,           
  cfrRowid,            
  cfrUpdate,           
  0,0,0,0,0,0,0,0,0,0,0  
};

int doltliteRegisterConflictTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_conflicts_", &cfRowModule);
}

static void conflictsResolveFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zMode, *zTable;
  ConflictTableInfo *aTables = 0;
  int nTables = 0;
  int i, j, rc;

  if(!cs){ sqlite3_result_error(ctx,"no database",-1); return; }
  if(argc<2){ sqlite3_result_error(ctx,"usage: dolt_conflicts_resolve('--ours'|'--theirs','table')",-1); return; }

  zMode = (const char*)sqlite3_value_text(argv[0]);
  zTable = (const char*)sqlite3_value_text(argv[1]);
  if(!zMode||!zTable){ sqlite3_result_error(ctx,"invalid args",-1); return; }

  rc = loadAllConflicts(db, cs, &aTables, &nTables);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  if( strcmp(zMode,"--ours")==0 ){
    
    for(i=0; i<nTables; i++){
      if( aTables[i].zName && strcmp(aTables[i].zName, zTable)==0 ){
        removeConflictTable(aTables, &nTables, i);
        break;
      }
    }
    rc = storeUpdatedConflicts(db, cs, aTables, nTables);
    freeConflictTables(aTables, nTables);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);

  }else if( strcmp(zMode,"--theirs")==0 ){
    
    for(i=0; i<nTables; i++){
      if( !aTables[i].zName || strcmp(aTables[i].zName, zTable)!=0 ) continue;

      
      for(j=0; j<aTables[i].nConflicts; j++){
        struct ConflictRow *cr = &aTables[i].aRows[j];
        if( cr->pTheirVal && cr->nTheirVal > 0 ){
          
          rc = applyTheirRecord(db, zTable, cr->intKey,
                                cr->pTheirVal, cr->nTheirVal);
          if( rc!=SQLITE_OK ){
            freeConflictTables(aTables, nTables);
            sqlite3_result_error(ctx, "failed to apply theirs value", -1);
            return;
          }
        }else{
          /* theirs is a delete: remove the row from the table. The PK
          ** value is extracted from the ours-side record body
          ** (required), or falls back to base if ours is missing
          ** (shouldn't happen in practice). doltlite user-PK tables
          ** don't expose a "rowid" column so we build the DELETE
          ** against the actual PK columns. */
          const u8 *pSrcRec = cr->pOurVal ? cr->pOurVal : cr->pBaseVal;
          int nSrcRec       = cr->pOurVal ? cr->nOurVal : cr->nBaseVal;
          char *zSql = 0;
          if( !pSrcRec || nSrcRec<=0 ){
            freeConflictTables(aTables, nTables);
            sqlite3_result_error(ctx, "failed to apply theirs value (no key)", -1);
            return;
          }
          rc = buildDeleteByKeySql(db, zTable, cr->intKey, pSrcRec, nSrcRec, &zSql);
          if( rc==SQLITE_OK ){
            rc = sqlite3_exec(db, zSql, 0, 0, 0);
          }
          sqlite3_free(zSql);
          if( rc!=SQLITE_OK ){
            freeConflictTables(aTables, nTables);
            sqlite3_result_error(ctx, "failed to apply theirs value", -1);
            return;
          }
        }
      }

      removeConflictTable(aTables, &nTables, i);
      break;
    }
    rc = storeUpdatedConflicts(db, cs, aTables, nTables);
    freeConflictTables(aTables, nTables);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);

  }else{
    freeConflictTables(aTables, nTables);
    sqlite3_result_error(ctx, "use --ours or --theirs", -1);
  }
}

int doltliteConflictsRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module(db, "dolt_conflicts", &conflictsModule, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_function(db, "dolt_conflicts_resolve", -1, SQLITE_UTF8, 0,
                                  conflictsResolveFunc, 0, 0);
  
  if( rc==SQLITE_OK )
    rc = doltliteRegisterConflictTables(db);
  return rc;
}

#endif 
