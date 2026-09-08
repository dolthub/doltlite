#ifdef DOLTLITE_PROLLY

#include "doltlite_internal.h"

#include <string.h>

static char **framedNameSlot(void *pTab, size_t offName){
  return (char**)((char*)pTab + offName);
}

static int *framedNRowsSlot(void *pTab, size_t offNRows){
  return (int*)((char*)pTab + offNRows);
}

static void **framedARowsSlot(void *pTab, size_t offARows){
  return (void**)((char*)pTab + offARows);
}

static const char *framedNameAt(
  const void *aTables, size_t szTable, size_t offName, int i
){
  return *(char * const *)((const char*)aTables + (size_t)i * szTable + offName);
}

static int framedNRowsAt(
  const void *aTables, size_t szTable, size_t offNRows, int i
){
  return *(const int*)((const char*)aTables + (size_t)i * szTable + offNRows);
}

static const void *framedARowsAt(
  const void *aTables, size_t szTable, size_t offARows, int i
){
  return *(void * const *)((const char*)aTables + (size_t)i * szTable + offARows);
}

static const void *framedRowAt(const void *aRows, size_t szRow, int j){
  return (const char*)aRows + (size_t)j * szRow;
}

static int framedNameEq(
  const DlFramedCodec *pCodec, const char *zHave, const char *zWant
){
  if( !zHave || !zWant ) return 0;
  return pCodec->bNameNocase
      ? sqlite3_stricmp(zHave, zWant)==0
      : strcmp(zHave, zWant)==0;
}

int dlFramedSerialize(
  ChunkStore *cs,
  ProllyHash *pHash,
  const DlFramedCodec *pCodec,
  int nTables,
  size_t szTable,
  size_t offName,
  size_t offNRows,
  size_t offARows,
  const void *aTables
){
  sqlite3_int64 sz = 4 + 2;
  int i, j, rc;
  u8 *buf;
  DlByteWriter w;

  if( nTables<0 || nTables>0xffff ) return SQLITE_TOOBIG;
  if( nTables>0 && !aTables ) return SQLITE_CORRUPT;
  for(i=0; i<nTables; i++){
    const char *zName = framedNameAt(aTables, szTable, offName, i);
    int nRows = framedNRowsAt(aTables, szTable, offNRows, i);
    const void *aRows = framedARowsAt(aTables, szTable, offARows, i);
    size_t nName = zName ? strlen(zName) : 0;
    int nl;
    if( nName>0xffff || nRows<0 ) return SQLITE_TOOBIG;
    if( nRows>0 && !aRows ) return SQLITE_CORRUPT;
    nl = (int)nName;
    rc = dlAddSize(&sz, 2 + nl + 4);
    if( rc!=SQLITE_OK ) return rc;
    for(j=0; j<nRows; j++){
      rc = pCodec->xMeasureRow(framedRowAt(aRows, pCodec->szRow, j), &sz);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  buf = sqlite3_malloc64((sqlite3_uint64)sz);
  if( !buf ) return SQLITE_NOMEM;
  dlWriterInit(&w, buf, (int)sz);

  dlWriteFramedHeader(&w, pCodec->m0, pCodec->m1, pCodec->m2, pCodec->ver, nTables);
  for(i=0; i<nTables; i++){
    const char *zName = framedNameAt(aTables, szTable, offName, i);
    int nRows = framedNRowsAt(aTables, szTable, offNRows, i);
    const void *aRows = framedARowsAt(aTables, szTable, offARows, i);
    int nl = zName ? (int)strlen(zName) : 0;
    dlWriteU16Name(&w, zName, nl);
    dlWriteU32(&w, nRows);
    for(j=0; j<nRows; j++){
      pCodec->xWriteRow(&w, framedRowAt(aRows, pCodec->szRow, j));
    }
  }

  if( w.err || w.p!=w.end ){
    sqlite3_free(buf);
    return SQLITE_CORRUPT;
  }
  rc = (pCodec->iPutFaultSim && sqlite3FaultSim(pCodec->iPutFaultSim))
      ? SQLITE_IOERR
      : chunkStorePut(cs, buf, (int)sz, pHash);
  sqlite3_free(buf);
  return rc;
}

int dlFramedDeserialize(
  const u8 *data, int nData,
  const DlFramedCodec *pCodec,
  size_t szTable,
  size_t offName,
  size_t offNRows,
  size_t offARows,
  void **ppTables, int *pnTables,
  void (*xFreeTables)(void *aTables, int nTables)
){
  DlByteReader r;
  int nTables, i, rc;
  void *aTables;

  *ppTables = 0;
  *pnTables = 0;
  if( !data || nData<(4+2) ) return SQLITE_CORRUPT;

  dlReaderInit(&r, data, nData);
  if( dlReadFramedHeader(&r, pCodec->m0, pCodec->m1, pCodec->m2, pCodec->ver,
                         &nTables)!=SQLITE_OK ){
    return SQLITE_CORRUPT;
  }

  aTables = sqlite3_malloc(nTables ? nTables * (int)szTable : 1);
  if( !aTables ) return SQLITE_NOMEM;
  memset(aTables, 0, nTables ? nTables * (int)szTable : 1);

  for(i=0; i<nTables; i++){
    void *pTab = (char*)aTables + (size_t)i * szTable;
    void *aRows = 0;
    rc = dlReadNamedRowTable(&r, framedNameSlot(pTab, offName),
                             framedNRowsSlot(pTab, offNRows),
                             &aRows, pCodec->szRow, pCodec->bAllocEmpty,
                             pCodec->xRead, pCodec->xFree);
    if( rc!=SQLITE_OK ){
      xFreeTables(aTables, nTables);
      return rc;
    }
    *framedARowsSlot(pTab, offARows) = aRows;
  }

  if( r.err || r.p != r.end ){
    xFreeTables(aTables, nTables);
    return SQLITE_CORRUPT;
  }

  *ppTables = aTables;
  *pnTables = nTables;
  return SQLITE_OK;
}

int dlFramedLoadNamed(
  const u8 *data, int nData,
  const DlFramedCodec *pCodec,
  const char *zTableName,
  char **pzName, int *pnRows, void **ppRows, int *pFound
){
  DlByteReader r;
  int nTables, i, rc;

  *pzName = 0;
  *pnRows = 0;
  *ppRows = 0;
  *pFound = 0;
  if( !data || nData<(4+2) ) return SQLITE_CORRUPT;

  dlReaderInit(&r, data, nData);
  if( dlReadFramedHeader(&r, pCodec->m0, pCodec->m1, pCodec->m2, pCodec->ver,
                         &nTables)!=SQLITE_OK ){
    return SQLITE_CORRUPT;
  }

  for(i=0; i<nTables; i++){
    void *aRows = 0;
    rc = dlMatchOrSkipNamedTable(&r, zTableName, pFound,
                                 pzName, pnRows, &aRows,
                                 pCodec->szRow, pCodec->bAllocEmpty,
                                 pCodec->xRead, pCodec->xSkip, pCodec->xFree);
    if( rc!=SQLITE_OK ) return rc;
    if( aRows ) *ppRows = aRows;
  }

  if( r.err || r.p != r.end ) return SQLITE_CORRUPT;
  return SQLITE_OK;
}

static int framedRewriteOpen(
  const u8 *data, int nData,
  const DlFramedCodec *pCodec,
  u8 **ppOut,
  DlByteReader *pR,
  DlByteWriter *pW,
  int *pnTables
){
  if( !data || nData<(4+2) ) return SQLITE_CORRUPT;
  *ppOut = sqlite3_malloc(nData);
  if( !*ppOut ) return SQLITE_NOMEM;
  dlReaderInit(pR, data, nData);
  if( dlReadFramedHeader(pR, pCodec->m0, pCodec->m1, pCodec->m2, pCodec->ver,
                         pnTables)!=SQLITE_OK ){
    sqlite3_free(*ppOut);
    *ppOut = 0;
    return SQLITE_CORRUPT;
  }
  dlWriterInit(pW, *ppOut, nData);
  dlWriteFramedHeader(pW, pCodec->m0, pCodec->m1, pCodec->m2, pCodec->ver, 0);
  return SQLITE_OK;
}

static int framedRewriteFinish(
  const u8 *data, int nData,
  const DlFramedCodec *pCodec,
  DlByteReader *pR,
  DlByteWriter *pW,
  u8 *out,
  int nOutTables,
  int bChanged,
  u8 **ppOut, int *pnOut, int *pnOutTables
){
  (void)data; (void)nData;
  if( pR->err || pR->p != pR->end || pW->err ){
    sqlite3_free(out);
    return SQLITE_CORRUPT;
  }
  if( !bChanged ){
    sqlite3_free(out);
    *ppOut = 0;
    *pnOut = 0;
    *pnOutTables = 0;
    return SQLITE_OK;
  }
  {
    DlByteWriter hw;
    dlWriterInit(&hw, out, 6);
    dlWriteFramedHeader(&hw, pCodec->m0, pCodec->m1, pCodec->m2, pCodec->ver,
                        nOutTables);
    if( hw.err ){
      sqlite3_free(out);
      return SQLITE_CORRUPT;
    }
  }
  *ppOut = out;
  *pnOut = (int)(pW->p - out);
  *pnOutTables = nOutTables;
  return SQLITE_OK;
}

int dlFramedDeleteRow(
  const u8 *data, int nData,
  const DlFramedCodec *pCodec,
  const char *zTableName,
  i64 deleteRowid,
  u8 **ppOut, int *pnOut, int *pnOutTables, int *pDeleted
){
  u8 *out = 0;
  DlByteReader r;
  DlByteWriter w;
  int nTables, nOutTables = 0;
  int i, j, rc;
  int deleted = 0;
  void *pRow = 0;

  *ppOut = 0;
  *pnOut = 0;
  *pnOutTables = 0;
  *pDeleted = 0;

  rc = framedRewriteOpen(data, nData, pCodec, &out, &r, &w, &nTables);
  if( rc!=SQLITE_OK ) return rc;

  pRow = sqlite3_malloc64(pCodec->szRow ? (sqlite3_uint64)pCodec->szRow : 1);
  if( !pRow ){
    sqlite3_free(out);
    return SQLITE_NOMEM;
  }

  for(i=0; i<nTables; i++){
    const u8 *pTableStart = r.p;
    u8 *pOutTableStart = w.p;
    char *zName = 0;
    int nr;
    int isMatch;

    rc = dlReadU16Name(&r, &zName);
    if( rc!=SQLITE_OK ) goto delete_done;
    nr = dlReadU32(&r);
    if( r.err || nr<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto delete_done;
    }
    if( (sqlite3_uint64)nr > (sqlite3_uint64)(r.end - r.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto delete_done;
    }

    isMatch = (!deleted && framedNameEq(pCodec, zName, zTableName));
    if( isMatch ){
      u8 *pCountOut = 0;
      int nKeep = 0;
      int nl = (int)strlen(zName);

      dlWriteU16Name(&w, zName, nl);
      pCountOut = w.p;
      dlWriteU32(&w, 0);
      for(j=0; j<nr; j++){
        memset(pRow, 0, pCodec->szRow);
        rc = pCodec->xRead(&r, pRow);
        if( rc!=SQLITE_OK ){
          pCodec->xFree(pRow);
          sqlite3_free(zName);
          goto delete_done;
        }
        if( !deleted && pCodec->xRowid(pRow)==deleteRowid ){
          deleted = 1;
        }else{
          pCodec->xWriteRow(&w, pRow);
          nKeep++;
        }
        pCodec->xFree(pRow);
      }
      if( nKeep==0 ){
        w.p = pOutTableStart;
      }else{
        DlByteWriter cw;
        dlWriterInit(&cw, pCountOut, 4);
        dlWriteU32(&cw, nKeep);
        if( cw.err ){
          sqlite3_free(zName);
          rc = SQLITE_CORRUPT;
          goto delete_done;
        }
        nOutTables++;
      }
    }else{
      for(j=0; j<nr; j++){
        rc = pCodec->xSkip(&r, 0);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zName);
          goto delete_done;
        }
      }
      {
        int nCopy = (int)(r.p - pTableStart);
        dlWriteBytes(&w, pTableStart, nCopy);
        nOutTables++;
      }
    }
    sqlite3_free(zName);
  }

  sqlite3_free(pRow);
  pRow = 0;
  *pDeleted = deleted;
  return framedRewriteFinish(data, nData, pCodec, &r, &w, out, nOutTables,
                             deleted, ppOut, pnOut, pnOutTables);

delete_done:
  sqlite3_free(pRow);
  sqlite3_free(out);
  return rc;
}

int dlFramedDropTable(
  const u8 *data, int nData,
  const DlFramedCodec *pCodec,
  const char *zTableName,
  u8 **ppOut, int *pnOut, int *pnOutTables, int *pFound
){
  u8 *out = 0;
  DlByteReader r;
  DlByteWriter w;
  int nTables, nOutTables = 0;
  int i, j, rc;
  int found = 0;

  *ppOut = 0;
  *pnOut = 0;
  *pnOutTables = 0;
  *pFound = 0;

  rc = framedRewriteOpen(data, nData, pCodec, &out, &r, &w, &nTables);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nTables; i++){
    const u8 *pTableStart = r.p;
    char *zName = 0;
    int nr;
    int isMatch;

    rc = dlReadU16Name(&r, &zName);
    if( rc!=SQLITE_OK ) goto drop_done;
    nr = dlReadU32(&r);
    if( r.err || nr<0 ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto drop_done;
    }
    if( (sqlite3_uint64)nr > (sqlite3_uint64)(r.end - r.p) ){
      sqlite3_free(zName);
      rc = SQLITE_CORRUPT;
      goto drop_done;
    }

    isMatch = (!found && framedNameEq(pCodec, zName, zTableName));
    for(j=0; j<nr; j++){
      rc = pCodec->xSkip(&r, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zName);
        goto drop_done;
      }
    }
    if( isMatch ){
      found = 1;
    }else{
      int nCopy = (int)(r.p - pTableStart);
      dlWriteBytes(&w, pTableStart, nCopy);
      nOutTables++;
    }
    sqlite3_free(zName);
  }

  *pFound = found;
  return framedRewriteFinish(data, nData, pCodec, &r, &w, out, nOutTables,
                             found, ppOut, pnOut, pnOutTables);

drop_done:
  sqlite3_free(out);
  return rc;
}

#endif
