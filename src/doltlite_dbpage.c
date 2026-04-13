/* doltlite_dbpage: override of the stock sqlite_dbpage vtable.
**
** The stock sqlite_dbpage reads raw b-tree pages through the sqlite
** pager. doltlite's storage is prolly trees, not pages, so the stock
** implementation returns NULL for every page and .dbinfo reports
** "unable to read database header".
**
** This module registers a doltlite-specific sqlite_dbpage AFTER the
** stock one so sqlite3_create_module's override wins. It only
** synthesizes pgno=1 — the 100-byte SQLite-format-3 header that
** .dbinfo parses — with values drawn from the active catalog:
**
**   file change counter = low 32 bits of the head commit hash
**   schema cookie       = low 32 bits of the catalog hash
**   database page count = number of user tables in the catalog
**   largest root page   = max iTable in the catalog
**   software version    = SQLITE_VERSION_NUMBER
**   everything else     = zero / standard defaults
**
** The synthesized page is enough for .dbinfo to print a coherent
** report — the rest of its output comes from follow-up queries
** against sqlite_schema, which doltlite already supports.
*/

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

/* Return a full 4096-byte page so sqlite3_column_bytes() > 100 — the
** strict inequality that .dbinfo uses to decide the read succeeded.
** Only the first 100 bytes (the SQLite file header) are populated;
** the rest is zero-filled page content. */
#define DOLTLITE_DBPAGE_PAGE_BYTES 4096

typedef struct DbpageVtab DbpageVtab;
struct DbpageVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DbpageCursor DbpageCursor;
struct DbpageCursor {
  sqlite3_vtab_cursor base;
  int iRow;                   /* 0 before first row, 1 after */
  int hasRow;                 /* 1 if a row should be emitted */
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

/*
** Fill the first 100 bytes of aPage with a synthesized SQLite
** format-3 db header. The remaining page bytes are zeroed — no
** b-tree contents are synthesized. Non-fatal: if any doltlite state
** lookup fails, the corresponding header field is left at zero.
*/
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

  /* Magic + fixed header constants. */
  memcpy(aHdr, "SQLite format 3", 16);  /* 16th byte is the \0 from memset */
  put2byteBE(aHdr + 16, 4096);          /* page size (display-only) */
  aHdr[18] = 1;                         /* write format */
  aHdr[19] = 1;                         /* read format */
  aHdr[20] = 0;                         /* reserved bytes per page */
  aHdr[21] = 64;                        /* max embedded payload fraction */
  aHdr[22] = 32;                        /* min embedded payload fraction */
  aHdr[23] = 32;                        /* leaf payload fraction */

  /* Change counter = low 32 bits of the head commit hash. Zero on an
  ** empty repo (no commits yet). */
  doltliteGetSessionHead(db, &headHash);
  if( !prollyHashIsEmpty(&headHash) ){
    for(i=0; i<4; i++){
      changeCounter = (changeCounter << 8) | headHash.data[i];
    }
  }
  put4byteBE(aHdr + 24, changeCounter);

  /* Walk the current catalog for table count and max iTable. */
  if( doltliteGetHeadCatalogHash(db, &catHash)==SQLITE_OK
   && !prollyHashIsEmpty(&catHash)
   && doltliteLoadCatalog(db, &catHash, &aTables, &nTables, &iNextTable)==SQLITE_OK
  ){
    int userTables = 0;
    for(i=0; i<nTables; i++){
      /* Skip the sqlite_master slot (iTable==1, name==NULL). */
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

  put4byteBE(aHdr + 28, pageCount);      /* database size in pages */
  /* 32..35: first freelist trunk page — 0 */
  /* 36..39: total freelist pages — 0 */
  put4byteBE(aHdr + 40, schemaCookie);
  put4byteBE(aHdr + 44, 4);              /* schema format number */
  /* 48..51: default page cache size — 0 */
  put4byteBE(aHdr + 52, largestRoot);    /* largest root b-tree page */
  put4byteBE(aHdr + 56, 1);              /* text encoding: utf8 */
  /* 60..63: user version — 0 */
  /* 64..67: incremental vacuum mode — 0 */
  /* 68..71: application id — 0 */
  /* 72..91: reserved for expansion — all zero */
  put4byteBE(aHdr + 92, changeCounter);  /* version-valid-for */
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

/*
** Accept pgno= (column 0) and schema= (column 2 HIDDEN) equality
** constraints. The shell's .dbinfo uses the table-valued-function
** form sqlite_dbpage(?1) WHERE pgno=1 which binds the schema name
** into the hidden schema column — we accept and discard it (doltlite
** only synthesizes the main schema's page 1). idxNum bit 1 = pgno
** present, bit 2 = schema present.
*/
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

  /* idxNum bit 1: pgno= present at argv[iArg]. Only emit if pgno==1. */
  if( idxNum & 1 ){
    sqlite3_int64 pgno = sqlite3_value_int64(argv[iArg++]);
    wantPage1 = (pgno==1);
  }
  /* idxNum bit 2: schema= present at argv[iArg]. Accept but ignore —
  ** doltlite only synthesizes the main schema's page 1. */
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
    case 0:  /* pgno */
      sqlite3_result_int64(ctx, 1);
      break;
    case 1:  /* data */
      sqlite3_result_blob(ctx, pCur->aPage, DOLTLITE_DBPAGE_PAGE_BYTES,
                          SQLITE_TRANSIENT);
      break;
    case 2:  /* schema (HIDDEN) */
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

/*
** Extension-init wrapper matching the sqlite3_load_extension / auto-
** extension signature. Installed via sqlite3_auto_extension from
** process startup so every sqlite3_open picks up the doltlite
** sqlite_dbpage override AFTER the stock sqlite3BuiltinExtensions
** loop has registered its version. Without this ordering the stock
** module clobbers ours and .dbinfo fails on prolly-backed databases.
*/
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
