#ifndef DOLTLITE_VTAB_UTIL_H
#define DOLTLITE_VTAB_UTIL_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

int sqlite3DoltliteVtabConstraintIsCorrelated(sqlite3_index_info *pIdxInfo,
                                              int iConstraint);

/* Shared (db, zTableName, cols) prefix; AtVtab and HistVtab start with this. */
typedef struct DoltliteVtabCommon DoltliteVtabCommon;
struct DoltliteVtabCommon {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

/* Shared cursor prefix (prolly cursor, intKey, pVal/nVal, hasRow, iRowid). */
typedef struct DoltliteVtabCursorCommon DoltliteVtabCursorCommon;
struct DoltliteVtabCursorCommon {
  sqlite3_vtab_cursor base;
  ProllyCursor tblCur;
  int tblCurOpen;
  u8 rootIntKey;
  i64 intKey;
  u8 *pVal;
  int nVal;
  int hasRow;
  i64 iRowid;
  u64 keyHash;
};

static SQLITE_INLINE void doltliteVtabCommonReset(
  DoltliteVtabCursorCommon *c
){
  if( c->tblCurOpen ){
    prollyCursorClose(&c->tblCur);
    c->tblCurOpen = 0;
  }
  sqlite3_free(c->pVal);
  c->pVal = 0;
  c->nVal = 0;
  c->hasRow = 0;
  c->iRowid = 0;
  c->rootIntKey = 0;
}

static SQLITE_INLINE int doltliteVtabCommonCaptureRowSide(
  DoltliteVtabCursorCommon *c, sqlite3 *db, const char *zTableName,
  const DoltliteSideCols *pSide
){
  const u8 *pVal; int nVal;
  const u8 *pRowKey; int nRowKey;
  sqlite3_free(c->pVal);
  c->pVal = 0; c->nVal = 0;
  prollyCursorKey(&c->tblCur, &pRowKey, &nRowKey);
  c->keyHash = doltliteFnv1aBytes(DOLTLITE_FNV1A_OFFSET, pRowKey, nRowKey);
  /* Blob-key nodes have no integer key; follow the node, not the table, when
  ** history spans a key-shape change. */
  c->intKey = c->rootIntKey ? prollyCursorIntKey(&c->tblCur) : 0;
  prollyCursorValue(&c->tblCur, &pVal, &nVal);
  if( pVal && nVal>0 ){
    c->pVal = sqlite3_malloc(nVal);
    if( !c->pVal ) return SQLITE_NOMEM;
    memcpy(c->pVal, pVal, nVal);
    c->nVal = nVal;
  }else{
    /* Clustered all-PK rows store an empty value; rebuild the record from the key. */
    const u8 *pKey; int nKey;
    int rc;
    prollyCursorKey(&c->tblCur, &pKey, &nKey);
    rc = pSide && pSide->valid
      ? doltliteRecordFromClusteredKeyCols(db, &pSide->ci, pKey, nKey,
                                           &c->pVal, &c->nVal)
      : doltliteRecordFromClusteredKey(db, zTableName, pKey, nKey,
                                       &c->pVal, &c->nVal);
    if( rc!=SQLITE_OK ) return rc;
  }
  c->hasRow = 1;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltlitePkRangeMatchesCursorUpper(
  const DoltlitePkRange *pRange,
  ProllyCursor *pCur
){
  return doltlitePkRangeMatchesUpper(pRange, prollyCursorIntKey(pCur));
}

static SQLITE_INLINE int doltliteVtabCommonDisconnect(
  sqlite3_vtab *pVtab
){
  DoltliteVtabCommon *v = (DoltliteVtabCommon*)pVtab;
  sqlite3_free(v->zTableName);
  doltliteFreeColInfo(&v->cols);
  sqlite3_free(v);
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteVtabCommonEof(
  sqlite3_vtab_cursor *cur
){
  DoltliteVtabCursorCommon *c = (DoltliteVtabCursorCommon*)cur;
  return !c->hasRow;
}

static SQLITE_INLINE int doltliteVtabCommonRowid(
  sqlite3_vtab_cursor *cur,
  sqlite3_int64 *r
){
  *r = ((DoltliteVtabCursorCommon*)cur)->iRowid;
  return SQLITE_OK;
}

/* Allocates nByte (>= sizeof(DoltliteVtabCommon)); caller fills trailing fields. */
int doltliteLoadHistoricalTableColumns(sqlite3*, const char*,
                                       DoltliteColInfo*, char**);

static SQLITE_INLINE int doltliteVtabConnectTable(
  sqlite3 *db, int argc, const char *const *argv,
  const char *zPrefix, int nByte,
  char *(*xBuildSchema)(const DoltliteColInfo*),
  int historical,
  sqlite3_vtab **ppVtab, char **pzErr
){
  DoltliteVtabCommon *v;
  const char *zMod = argv[0];
  size_t nPrefix = strlen(zPrefix);
  char *zSchema;
  int rc;

  v = sqlite3_malloc(nByte);
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, nByte);
  v->db = db;

  if( zMod && strncmp(zMod, zPrefix, nPrefix)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zMod + nPrefix);
  }else if( argc > 3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }
  if( !v->zTableName ){
    doltliteVtabCommonDisconnect(&v->base);
    return SQLITE_NOMEM;
  }

  if( historical ){
    rc = doltliteLoadHistoricalTableColumns(db, v->zTableName,
                                             &v->cols, pzErr);
  }else{
    rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  }
  if( rc==SQLITE_OK ){
    zSchema = xBuildSchema(&v->cols);
    if( !zSchema ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_declare_vtab(db, zSchema);
      sqlite3_free(zSchema);
    }
  }
  if( rc!=SQLITE_OK ){
    doltliteVtabCommonDisconnect(&v->base);
    return rc;
  }
  sqlite3_vtab_config(db, SQLITE_VTAB_INNOCUOUS);
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteVtabConnectUserTable(
  sqlite3 *db, int argc, const char *const *argv,
  const char *zPrefix, int nByte,
  char *(*xBuildSchema)(const DoltliteColInfo*),
  sqlite3_vtab **ppVtab, char **pzErr
){
  return doltliteVtabConnectTable(db, argc, argv, zPrefix, nByte,
                                  xBuildSchema, 0, ppVtab, pzErr);
}

static SQLITE_INLINE int doltliteVtabConnectHistoricalTable(
  sqlite3 *db, int argc, const char *const *argv,
  const char *zPrefix, int nByte,
  char *(*xBuildSchema)(const DoltliteColInfo*),
  sqlite3_vtab **ppVtab, char **pzErr
){
  return doltliteVtabConnectTable(db, argc, argv, zPrefix, nByte,
                                  xBuildSchema, 1, ppVtab, pzErr);
}

#endif
