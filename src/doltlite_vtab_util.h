#ifndef DOLTLITE_VTAB_UTIL_H
#define DOLTLITE_VTAB_UTIL_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_record.h"
#include "record_codec.h"

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
}

static SQLITE_INLINE int doltliteVtabCommonCaptureRow(
  DoltliteVtabCursorCommon *c
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
  }
  c->hasRow = 1;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteVtabCommonClose(
  sqlite3_vtab_cursor *cur
){
  DoltliteVtabCursorCommon *c = (DoltliteVtabCursorCommon*)cur;
  doltliteVtabCommonReset(c);
  sqlite3_free(c);
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

#endif /* DOLTLITE_VTAB_UTIL_H */
