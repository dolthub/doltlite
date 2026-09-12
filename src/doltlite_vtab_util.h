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

static SQLITE_INLINE int doltliteSideColsMatchIntPk(
  const DoltliteSideCols *pSide,
  const DoltliteColInfo *pDeclared
){
  int iPk = pDeclared->iPkCol;
  return iPk>=0 && (!pSide->valid
      || (pSide->ci.iPkCol>=0
          && pSide->aDeclToSide[iPk]==pSide->ci.iPkCol));
}

static SQLITE_INLINE int doltlitePkRangeMatchesCursorUpper(
  const DoltlitePkRange *pRange,
  ProllyCursor *pCur
){
  return doltlitePkRangeMatchesUpper(pRange, prollyCursorIntKey(pCur));
}

/* Equality on every clustered PK column, in key order. INTEGER PRIMARY KEY
** uses doltliteBestIndexIntPkRange instead (iPkCol>=0). */
static SQLITE_INLINE int doltliteBestIndexClusteredPkEq(
  sqlite3_index_info *pInfo,
  const DoltliteColInfo *ci,
  int idxEq,
  int *pnArg
){
  int *aDecl = 0;
  int *aEq = 0;
  int i, j, nArg;

  if( !pInfo || !ci || !pnArg ) return SQLITE_OK;
  if( ci->bHasRowid || ci->nPk<=0 || !ci->aColToRec ) return SQLITE_OK;

  aDecl = sqlite3_malloc64((sqlite3_int64)ci->nPk * sizeof(int));
  aEq = sqlite3_malloc64((sqlite3_int64)ci->nPk * sizeof(int));
  if( !aDecl || !aEq ){
    sqlite3_free(aDecl);
    sqlite3_free(aEq);
    return SQLITE_NOMEM;
  }
  for(i=0; i<ci->nPk; i++){
    aDecl[i] = -1;
    aEq[i] = -1;
  }
  for(i=0; i<ci->nCol; i++){
    int slot = ci->aColToRec[i];
    if( slot>=0 && slot<ci->nPk ) aDecl[slot] = i;
  }
  for(i=0; i<ci->nPk; i++){
    if( aDecl[i]<0 ) goto clustered_done;
  }
  for(i=0; i<ci->nPk; i++){
    for(j=0; j<pInfo->nConstraint; j++){
      const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[j];
      if( !pC->usable ) continue;
      if( pC->iColumn!=aDecl[i] ) continue;
      if( pC->op==SQLITE_INDEX_CONSTRAINT_EQ ){
        aEq[i] = j;
        break;
      }
    }
    if( aEq[i]<0 ) goto clustered_done;
  }
  nArg = *pnArg;
  if( nArg<1 ) nArg = 1;
  for(i=0; i<ci->nPk; i++){
    pInfo->aConstraintUsage[aEq[i]].argvIndex = nArg++;
    pInfo->aConstraintUsage[aEq[i]].omit = 0;
  }
  *pnArg = nArg;
  pInfo->idxNum |= idxEq;
  pInfo->estimatedCost = 10.0;
  pInfo->estimatedRows = 1;

clustered_done:
  sqlite3_free(aDecl);
  sqlite3_free(aEq);
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

/* Allocates nByte (>= sizeof(DoltliteVtabCommon)); caller fills trailing fields. */
int doltliteLoadHistoricalTableColumns(sqlite3*, const char*, const char*,
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
    rc = doltliteLoadHistoricalTableColumns(db, zMod, v->zTableName,
                                             &v->cols, pzErr);
    if( rc==SQLITE_NOTFOUND && (!pzErr || !*pzErr) ){
      if( pzErr ) *pzErr = sqlite3_mprintf("no such table: %s", zMod);
      rc = pzErr && !*pzErr ? SQLITE_NOMEM : SQLITE_ERROR;
    }
  }else{
    rc = doltliteLoadUserTableColumns(db, v->zTableName, &v->cols, pzErr);
  }
  if( rc==SQLITE_OK ){
    zSchema = xBuildSchema(&v->cols);
    if( !zSchema ){
      rc = SQLITE_NOMEM;
    }else{
      rc = doltliteDeclareVtab(db, zSchema);
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
