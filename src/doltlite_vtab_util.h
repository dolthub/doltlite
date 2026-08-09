#ifndef DOLTLITE_VTAB_UTIL_H
#define DOLTLITE_VTAB_UTIL_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_record.h"

int sqlite3DoltliteVtabConstraintIsCorrelated(sqlite3_index_info *pIdxInfo,
                                              int iConstraint);

/*
 * Shared prefix for virtual table instances whose per-table metadata
 * consist of (db, zTableName, cols) immediately after the base vtab.
 * AtVtab and HistVtab both start with this exact layout, so a pointer
 * to either can safely be cast to DoltliteVtabCommon*.
 */
typedef struct DoltliteVtabCommon DoltliteVtabCommon;
struct DoltliteVtabCommon {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

/*
 * Shared prefix for virtual table cursors whose per-row state includes
 * a prolly cursor, intKey, pVal/nVal, hasRow, and iRowid.  Both
 * AtCursor and HistCursor embed this as their first member so that
 * the shared inline helpers below can operate through a
 * DoltliteVtabCursorCommon pointer.
 */
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

static SQLITE_INLINE int doltliteVtabCommonCaptureRow(
  DoltliteVtabCursorCommon *c, sqlite3 *db, const char *zTableName
){
  const u8 *pVal; int nVal;
  sqlite3_free(c->pVal);
  c->pVal = 0; c->nVal = 0;
  c->intKey = prollyCursorIntKey(&c->tblCur);
  prollyCursorValue(&c->tblCur, &pVal, &nVal);
  if( pVal && nVal>0 ){
    c->pVal = sqlite3_malloc(nVal);
    if( !c->pVal ) return SQLITE_NOMEM;
    memcpy(c->pVal, pVal, nVal);
    c->nVal = nVal;
  }else{
    /* Clustered rows whose PRIMARY KEY covers every column store an empty
    ** value; the row lives in the sort key. Rebuild the record from the key
    ** so column reads see the PK values instead of NULLs. */
    const u8 *pKey; int nKey;
    int rc;
    prollyCursorKey(&c->tblCur, &pKey, &nKey);
    rc = doltliteRecordFromClusteredKey(db, zTableName, pKey, nKey,
                                        &c->pVal, &c->nVal);
    if( rc!=SQLITE_OK ) return rc;
  }
  c->hasRow = 1;
  return SQLITE_OK;
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

/*
 * xConnect/xCreate body for a per-user-table vtab whose instance begins with
 * DoltliteVtabCommon: derive the table name from the module name (zPrefix) or
 * argv[3], load its columns, build the schema via xBuildSchema, and declare
 * the vtab. Allocates nByte (>= sizeof(DoltliteVtabCommon)) so callers with
 * trailing fields can use it and fill them in after success. On any failure
 * the partial instance is freed through doltliteVtabCommonDisconnect.
 */
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

#endif /* DOLTLITE_VTAB_UTIL_H */
