

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

typedef struct DbpageCursor DbpageCursor;
struct DbpageCursor {
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

/* sqlite_dbpage is normally a view into the real 4k-paged database
** file, but doltlite's on-disk format is a chunk store — there ARE
** no pages. Instead we fabricate a single synthetic page 1 that
** looks enough like a SQLite header for tools that parse it
** (shell .dbinfo, backup-utilities) to read meta without crashing.
** Higher pgnos return EOF. Field values are derived from the
** current HEAD: change_counter from the commit hash, schema_cookie
** from the catalog hash, pageCount = user-table count. */
static void synthesizeHeader(sqlite3 *db, unsigned char *aPage){
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


  if( doltliteGetHeadCatalogHash(db, &catHash)==SQLITE_OK
   && !prollyHashIsEmpty(&catHash)
   && doltliteLoadCatalog(db, &catHash, &aTables, &nTables, &iNextTable)==SQLITE_OK
  ){
    int userTables = 0;
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
    sqlite3_free(aTables);
  }

  put4byteBE(aHdr + 28, pageCount);


  put4byteBE(aHdr + 40, schemaCookie);
  put4byteBE(aHdr + 44, 4);

  put4byteBE(aHdr + 52, largestRoot);
  put4byteBE(aHdr + 56, 1);




  put4byteBE(aHdr + 92, changeCounter);
  put4byteBE(aHdr + 96, SQLITE_VERSION_NUMBER);
}

static const char *zDbpageSchema =
  "CREATE TABLE x(pgno INTEGER PRIMARY KEY, data BLOB, schema HIDDEN)";

static int dbpageConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DbpageVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, zDbpageSchema);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dbpageDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int dbpageBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i, idxNum = 0, nArg = 0;
  int iPgno = -1, iSchema = -1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn==0 && iPgno<0 ){
      iPgno = i;
    }else if( pInfo->aConstraint[i].iColumn==2 && iSchema<0 ){
      iSchema = i;
    }
  }

  if( iPgno>=0 ){
    pInfo->aConstraintUsage[iPgno].argvIndex = ++nArg;
    pInfo->aConstraintUsage[iPgno].omit = 1;
    idxNum |= 1;
  }
  if( iSchema>=0 ){
    pInfo->aConstraintUsage[iSchema].argvIndex = ++nArg;
    pInfo->aConstraintUsage[iSchema].omit = 1;
    idxNum |= 2;
  }

  pInfo->idxNum = idxNum;
  pInfo->estimatedCost = 1.0;
  pInfo->estimatedRows = 1;
  return SQLITE_OK;
}

static int dbpageOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  DbpageCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int dbpageClose(sqlite3_vtab_cursor *pCursor){
  sqlite3_free(pCursor);
  return SQLITE_OK;
}

static int dbpageFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DbpageCursor *pCur = (DbpageCursor*)pCursor;
  DbpageVtab *pVtab = (DbpageVtab*)pCursor->pVtab;
  int wantPage1 = 1;
  int iArg = 0;
  (void)idxStr; (void)argc;

  pCur->iRow = 0;
  pCur->hasRow = 0;


  if( idxNum & 1 ){
    sqlite3_int64 pgno = sqlite3_value_int64(argv[iArg++]);
    wantPage1 = (pgno==1);
  }

  if( idxNum & 2 ){
    iArg++;
  }

  if( wantPage1 ){
    synthesizeHeader(pVtab->db, pCur->aPage);
    pCur->hasRow = 1;
  }
  return SQLITE_OK;
}

static int dbpageNext(sqlite3_vtab_cursor *pCursor){
  DbpageCursor *pCur = (DbpageCursor*)pCursor;
  pCur->iRow++;
  return SQLITE_OK;
}

static int dbpageEof(sqlite3_vtab_cursor *pCursor){
  DbpageCursor *pCur = (DbpageCursor*)pCursor;
  return !pCur->hasRow || pCur->iRow>=1;
}

static int dbpageColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DbpageCursor *pCur = (DbpageCursor*)pCursor;
  switch( iCol ){
    case 0:
      sqlite3_result_int64(ctx, 1);
      break;
    case 1:
      sqlite3_result_blob(ctx, pCur->aPage, DOLTLITE_DBPAGE_PAGE_BYTES,
                          SQLITE_TRANSIENT);
      break;
    case 2:
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int dbpageRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = 1;
  (void)pCursor;
  return SQLITE_OK;
}

static sqlite3_module doltliteDbpageModule = {
  0, 0, dbpageConnect, dbpageBestIndex, dbpageDisconnect, 0,
  dbpageOpen, dbpageClose, dbpageFilter, dbpageNext, dbpageEof,
  dbpageColumn, dbpageRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDbpageRegister(sqlite3 *db){
  return sqlite3_create_module(db, "sqlite_dbpage", &doltliteDbpageModule, 0);
}

static int doltliteDbpageExtInit(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  (void)pzErrMsg; (void)pApi;
  return doltliteDbpageRegister(db);
}

int doltliteDbpageInstallAutoExt(void){
  return sqlite3_auto_extension((void(*)(void))doltliteDbpageExtInit);
}

#endif
