

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

#define DOLTLITE_DBPAGE_PAGE_BYTES 4096

typedef struct DbpageVtab DbpageVtab;
struct DbpageVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DlDbpageCursor DlDbpageCursor;
struct DlDbpageCursor {
  sqlite3_vtab_cursor base;
  int iRow;
  int hasRow;
  unsigned char aPage[DOLTLITE_DBPAGE_PAGE_BYTES];
};

static void put2byteBE(unsigned char *p, unsigned int v){
  p[0] = (unsigned char)(v >> 8);
  p[1] = (unsigned char)(v & 0xff);
}

static void put4byteBE(unsigned char *p, unsigned int v){
  p[0] = (unsigned char)(v >> 24);
  p[1] = (unsigned char)((v >> 16) & 0xff);
  p[2] = (unsigned char)((v >> 8) & 0xff);
  p[3] = (unsigned char)(v & 0xff);
}

static int dbpageMapChunkSourceError(
  DbpageVtab *pVtab,
  int sourceRc,
  int mappedRc
){
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  int pendingRc = SQLITE_OK;
  char *zErr = cs ? chunkStoreSourceTakeError(cs, &pendingRc) : 0;
  if( !zErr && pendingRc==SQLITE_OK ) return mappedRc;
  if( zErr ){
    sqlite3_free(pVtab->base.zErrMsg);
    pVtab->base.zErrMsg = zErr;
  }
  return pendingRc!=SQLITE_OK ? pendingRc : sourceRc;
}

static int synthesizeHeader(DbpageVtab *pVtab, unsigned char *aPage){
  sqlite3 *db = pVtab->db;
  ProllyHash headHash;
  ProllyHash catHash;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  Pgno iNextTable = 0;
  unsigned int changeCounter = 0;
  unsigned int schemaCookie = 0;
  unsigned int pageCount = 0;
  unsigned int largestRoot = 0;
  unsigned char *aHdr = aPage;
  int rc;
  int i;

  memset(aPage, 0, DOLTLITE_DBPAGE_PAGE_BYTES);

  memcpy(aHdr, "SQLite format 3", 16);
  put2byteBE(aHdr + 16, 4096);
  aHdr[18] = 1;
  aHdr[19] = 1;
  aHdr[20] = 0;
  aHdr[21] = 64;
  aHdr[22] = 32;
  aHdr[23] = 32;

  doltliteGetSessionHead(db, &headHash);
  if( !prollyHashIsEmpty(&headHash) ){
    for(i=0; i<4; i++){
      changeCounter = (changeCounter << 8) | headHash.data[i];
    }
  }
  put4byteBE(aHdr + 24, changeCounter);

  rc = doltliteGetHeadCatalogHash(db, &catHash);
  if( rc!=SQLITE_OK ){
    return dbpageMapChunkSourceError(pVtab, rc, SQLITE_OK);
  }
  if( !prollyHashIsEmpty(&catHash) ){
    int userTables = 0;
    rc = doltliteLoadCatalog(db, &catHash, &aTables, &nTables, &iNextTable);
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aTables, nTables);
      return dbpageMapChunkSourceError(pVtab, rc, SQLITE_OK);
    }
    for(i=0; i<nTables; i++){

      if( aTables[i].iTable<=1 ) continue;
      userTables++;
      if( (unsigned int)aTables[i].iTable > largestRoot ){
        largestRoot = (unsigned int)aTables[i].iTable;
      }
    }
    pageCount = (unsigned int)userTables;
    for(i=0; i<4; i++){
      schemaCookie = (schemaCookie << 8) | catHash.data[i];
    }
    doltliteFreeCatalog(aTables, nTables);
  }

  put4byteBE(aHdr + 28, pageCount);

  put4byteBE(aHdr + 40, schemaCookie);
  put4byteBE(aHdr + 44, 4);

  put4byteBE(aHdr + 52, largestRoot);
  put4byteBE(aHdr + 56, 1);

  put4byteBE(aHdr + 92, changeCounter);
  put4byteBE(aHdr + 96, SQLITE_VERSION_NUMBER);
  return SQLITE_OK;
}

static const char *zDbpageSchema =
  "CREATE TABLE x(pgno INTEGER PRIMARY KEY, data BLOB, schema HIDDEN)";

static int dlDbpageConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DbpageVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zDbpageSchema, sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (DbpageVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int dlDbpageBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i, idxNum = 0, nArg = 0;
  int iPgno = -1, iSchema = -1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn==2 ){
      if( !pInfo->aConstraint[i].usable ) return SQLITE_CONSTRAINT;
      if( iSchema<0 ) iSchema = i;
    }else if( pInfo->aConstraint[i].usable
           && pInfo->aConstraint[i].iColumn==0 && iPgno<0 ){
      iPgno = i;
    }
  }

  if( iSchema>=0 ){
    pInfo->aConstraintUsage[iSchema].argvIndex = ++nArg;
    pInfo->aConstraintUsage[iSchema].omit = 1;
    idxNum |= 2;
  }
  if( iPgno>=0 ){
    pInfo->aConstraintUsage[iPgno].argvIndex = ++nArg;
    pInfo->aConstraintUsage[iPgno].omit = 1;
    idxNum |= 1;
  }

  pInfo->idxNum = idxNum;
  pInfo->estimatedCost = 1.0;
  pInfo->estimatedRows = 1;
  return SQLITE_OK;
}

static int dlDbpageOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(DlDbpageCursor));
}

static int dlDbpageFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
  int iArg = 0;
  int rc;
  (void)idxStr; (void)argc;

  pCur->iRow = 0;
  pCur->hasRow = 0;

  if( idxNum & 2 ){
    const char *zSchema = (const char*)sqlite3_value_text(argv[iArg++]);
    if( sqlite3FindDbName(pVtab->db, zSchema)!=0 ) return SQLITE_OK;
  }

  if( idxNum & 1 ){
    sqlite3_int64 pgno = sqlite3_value_int64(argv[iArg++]);
    if( pgno!=1 ){
      sqlite3_free(pVtab->base.zErrMsg);
      pVtab->base.zErrMsg = sqlite3_mprintf(
        "doltlite: sqlite_dbpage only supports pgno=1 "
        "(content-addressed chunk store has no page layout)");
      return SQLITE_ERROR;
    }
  }

  rc = synthesizeHeader(pVtab, pCur->aPage);
  if( rc!=SQLITE_OK ) return rc;
  pCur->hasRow = 1;
  return SQLITE_OK;
}

static int dlDbpageNext(sqlite3_vtab_cursor *pCursor){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  pCur->iRow++;
  return SQLITE_OK;
}

static int dlDbpageEof(sqlite3_vtab_cursor *pCursor){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  return !pCur->hasRow || pCur->iRow>=1;
}

static int dlDbpageColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  switch( iCol ){
    case 0:
      sqlite3_result_int64(ctx, 1);
      break;
    case 1:
      sqlite3_result_blob(ctx, pCur->aPage, DOLTLITE_DBPAGE_PAGE_BYTES,
                          SQLITE_TRANSIENT);
      break;
    case 2:
      sqlite3_result_text(ctx, "main", -1, SQLITE_STATIC);
      break;
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int dlDbpageRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = 1;
  (void)pCursor;
  return SQLITE_OK;
}

static sqlite3_module doltliteDbpageModule = {
  0, 0, dlDbpageConnect, dlDbpageBestIndex, doltliteVtabDisconnect, 0,
  dlDbpageOpen, doltliteVtabClose, dlDbpageFilter, dlDbpageNext, dlDbpageEof,
  dlDbpageColumn, dlDbpageRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDbpageRegister(sqlite3 *db){
  return sqlite3_create_module(db, "sqlite_dbpage", &doltliteDbpageModule, 0);
}

#endif
