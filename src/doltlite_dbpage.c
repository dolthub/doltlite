

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

#define DOLTLITE_DBPAGE_PAGE_BYTES 4096

/* An ordinary SQLite schema has real pages; stock sqlite_dbpage serves it. */
typedef struct DbpageVtab DbpageVtab;
struct DbpageVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  const sqlite3_module *pStock;
  sqlite3_vtab *pStockTab;
};

typedef struct DlDbpageCursor DlDbpageCursor;
struct DlDbpageCursor {
  sqlite3_vtab_cursor base;
  int iRow;
  int hasRow;
  sqlite3_vtab_cursor *pStockCur;
  int bStock;
  unsigned char aPage[DOLTLITE_DBPAGE_PAGE_BYTES];
};

static int dlDbpageStockSchema(sqlite3 *db, int iDb){
  return iDb>=0 && iDb<db->nDb && db->aDb[iDb].pBt
      && sqlite3BtreeUsesOrig(db->aDb[iDb].pBt);
}

static int dlDbpageStockRc(DbpageVtab *pVtab, int rc){
  if( pVtab->pStockTab && pVtab->pStockTab->zErrMsg ){
    sqlite3_free(pVtab->base.zErrMsg);
    pVtab->base.zErrMsg = pVtab->pStockTab->zErrMsg;
    pVtab->pStockTab->zErrMsg = 0;
  }
  return rc;
}

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
    return doltliteVtabMapChunkSourceError(&pVtab->base, pVtab->db, rc, SQLITE_OK);
  }
  if( !prollyHashIsEmpty(&catHash) ){
    int userTables = 0;
    rc = doltliteLoadCatalog(db, &catHash, &aTables, &nTables, &iNextTable);
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aTables, nTables);
      return doltliteVtabMapChunkSourceError(&pVtab->base, pVtab->db, rc, SQLITE_OK);
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
  const sqlite3_module *pStock = (const sqlite3_module*)pAux;
  sqlite3_vtab *pStockTab = 0;
  DbpageVtab *pVtab;
  int rc;
  *ppVtab = 0;
  if( pStock ){
    rc = pStock->xConnect(db, 0, argc, argv, &pStockTab, pzErr);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    rc = sqlite3_declare_vtab(db, zDbpageSchema);
    if( rc!=SQLITE_OK ) return rc;
    sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
  }
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ){
    if( pStockTab ) pStock->xDisconnect(pStockTab);
    return SQLITE_NOMEM;
  }
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  pVtab->pStock = pStock;
  pVtab->pStockTab = pStockTab;
  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dlDbpageDisconnect(sqlite3_vtab *pBase){
  DbpageVtab *pVtab = (DbpageVtab*)pBase;
  if( pVtab->pStockTab ) pVtab->pStock->xDisconnect(pVtab->pStockTab);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/* The stock plan: idxNum bit 2 binds schema= as argv[0], bit 1 binds pgno=. */
static int dlDbpageBestIndex(sqlite3_vtab *pBase, sqlite3_index_info *pInfo){
  DbpageVtab *pVtab = (DbpageVtab*)pBase;
  int i, idxNum = 0, nArg = 0;
  int iPgno = -1, iSchema = -1;

  if( pVtab->pStock ){
    return pVtab->pStock->xBestIndex(pVtab->pStockTab, pInfo);
  }
  for(i=0; i<pInfo->nConstraint; i++){
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn==2 ){
      if( !pInfo->aConstraint[i].usable ) return SQLITE_CONSTRAINT;
      if( iSchema<0 ) iSchema = i;
    }else if( pInfo->aConstraint[i].usable
           && pInfo->aConstraint[i].iColumn<=0 && iPgno<0 ){
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

static int dlDbpageClose(sqlite3_vtab_cursor *pCursor){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
  if( pCur->pStockCur ) pVtab->pStock->xClose(pCur->pStockCur);
  return doltliteVtabClose(pCursor);
}

static int dlDbpageFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
  int iDb = 0;
  int rc;

  pCur->iRow = 0;
  pCur->hasRow = 0;
  pCur->bStock = 0;

  if( idxNum & 2 ){
    const char *zSchema = (const char*)sqlite3_value_text(argv[0]);
    iDb = sqlite3FindDbName(pVtab->db, zSchema);
  }

  if( pVtab->pStock && dlDbpageStockSchema(pVtab->db, iDb) ){
    if( !pCur->pStockCur ){
      rc = pVtab->pStock->xOpen(pVtab->pStockTab, &pCur->pStockCur);
      if( rc!=SQLITE_OK ) return dlDbpageStockRc(pVtab, rc);
      pCur->pStockCur->pVtab = pVtab->pStockTab;
    }
    pCur->bStock = 1;
    return dlDbpageStockRc(pVtab,
        pVtab->pStock->xFilter(pCur->pStockCur, idxNum, idxStr, argc, argv));
  }

  if( iDb!=0 ) return SQLITE_OK;

  if( idxNum & 1 ){
    sqlite3_int64 pgno = sqlite3_value_int64(argv[idxNum>>1]);
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
  if( pCur->bStock ){
    DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
    return dlDbpageStockRc(pVtab, pVtab->pStock->xNext(pCur->pStockCur));
  }
  pCur->iRow++;
  return SQLITE_OK;
}

static int dlDbpageEof(sqlite3_vtab_cursor *pCursor){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  if( pCur->bStock ){
    DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
    return pVtab->pStock->xEof(pCur->pStockCur);
  }
  return !pCur->hasRow || pCur->iRow>=1;
}

static int dlDbpageColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  if( pCur->bStock ){
    DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
    return dlDbpageStockRc(pVtab,
        pVtab->pStock->xColumn(pCur->pStockCur, ctx, iCol));
  }
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
  DlDbpageCursor *pCur = (DlDbpageCursor*)pCursor;
  if( pCur->bStock ){
    DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
    return pVtab->pStock->xRowid(pCur->pStockCur, pRowid);
  }
  *pRowid = 1;
  return SQLITE_OK;
}

/* Writes reach real pages, so only an ordinary SQLite schema accepts them. */
static int dlDbpageUpdate(sqlite3_vtab *pBase, int argc,
    sqlite3_value **argv, sqlite3_int64 *pRowid){
  DbpageVtab *pVtab = (DbpageVtab*)pBase;
  int iDb = 0;
  if( argc>4 && sqlite3_value_type(argv[4])!=SQLITE_NULL ){
    iDb = sqlite3FindDbName(pVtab->db, (const char*)sqlite3_value_text(argv[4]));
  }
  if( pVtab->pStock && pVtab->pStock->xUpdate
   && (iDb<0 || dlDbpageStockSchema(pVtab->db, iDb)) ){
    return dlDbpageStockRc(pVtab,
        pVtab->pStock->xUpdate(pVtab->pStockTab, argc, argv, pRowid));
  }
  sqlite3_free(pVtab->base.zErrMsg);
  pVtab->base.zErrMsg = sqlite3_mprintf("table sqlite_dbpage may not be modified");
  return SQLITE_ERROR;
}

static int dlDbpageBegin(sqlite3_vtab *pBase){
  DbpageVtab *pVtab = (DbpageVtab*)pBase;
  if( !pVtab->pStock || !pVtab->pStock->xBegin ) return SQLITE_OK;
  return dlDbpageStockRc(pVtab, pVtab->pStock->xBegin(pVtab->pStockTab));
}

static int dlDbpageSync(sqlite3_vtab *pBase){
  DbpageVtab *pVtab = (DbpageVtab*)pBase;
  if( !pVtab->pStock || !pVtab->pStock->xSync ) return SQLITE_OK;
  return dlDbpageStockRc(pVtab, pVtab->pStock->xSync(pVtab->pStockTab));
}

static int dlDbpageRollbackTo(sqlite3_vtab *pBase, int iSavepoint){
  DbpageVtab *pVtab = (DbpageVtab*)pBase;
  if( !pVtab->pStock || !pVtab->pStock->xRollbackTo ) return SQLITE_OK;
  return pVtab->pStock->xRollbackTo(pVtab->pStockTab, iSavepoint);
}

static sqlite3_module doltliteDbpageModule = {
  2, dlDbpageConnect, dlDbpageConnect, dlDbpageBestIndex, dlDbpageDisconnect,
  dlDbpageDisconnect, dlDbpageOpen, dlDbpageClose, dlDbpageFilter,
  dlDbpageNext, dlDbpageEof, dlDbpageColumn, dlDbpageRowid, dlDbpageUpdate,
  dlDbpageBegin, dlDbpageSync, 0, 0, 0, 0, 0, 0, dlDbpageRollbackTo, 0, 0
};

int doltliteDbpageRegister(sqlite3 *db){
  const sqlite3_module *pStock = 0;
#if (defined(SQLITE_ENABLE_DBPAGE_VTAB) || defined(SQLITE_TEST)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)
  Module *pMod;
  int rc = sqlite3DbpageRegister(db);
  if( rc!=SQLITE_OK ) return rc;
  pMod = (Module*)sqlite3HashFind(&db->aModule, "sqlite_dbpage");
  if( pMod ) pStock = pMod->pModule;
#endif
  return sqlite3_create_module(db, "sqlite_dbpage", &doltliteDbpageModule,
                               (void*)pStock);
}

#endif
