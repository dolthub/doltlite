

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

#define SCHEMAS_IDX_TYPE_EQ 0x01
#define SCHEMAS_IDX_NAME_EQ 0x02

typedef struct SchemasRow SchemasRow;
struct SchemasRow {
  char *zType;
  char *zName;
  char *zFragment;
};

typedef struct SchemasVtab SchemasVtab;
struct SchemasVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct SchemasCursor SchemasCursor;
struct SchemasCursor {
  sqlite3_vtab_cursor base;
  SchemasRow *aRows;
  int nRows;
  int iRow;
};

static const char *zSchemasSchema =
  "CREATE TABLE x("
  "  type     TEXT,"
  "  name     TEXT,"
  "  fragment TEXT,"
  "  extra    TEXT,"
  "  sql_mode TEXT"
  ")";

static int schemasConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  SchemasVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zSchemasSchema, sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (SchemasVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int schemasBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i;
  int iTypeEq = -1;
  int iNameEq = -1;
  int idxNum = 0;
  int argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( !doltliteVtabConstraintIsBinary(pInfo, i) ) continue;
    if( pC->iColumn==0 && iTypeEq<0 ){
      iTypeEq = i;
    }else if( pC->iColumn==1 && iNameEq<0 ){
      iNameEq = i;
    }
  }

  if( iTypeEq>=0 ){
    pInfo->aConstraintUsage[iTypeEq].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTypeEq].omit = 0;
    idxNum |= SCHEMAS_IDX_TYPE_EQ;
  }
  if( iNameEq>=0 ){
    pInfo->aConstraintUsage[iNameEq].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iNameEq].omit = 0;
    idxNum |= SCHEMAS_IDX_NAME_EQ;
  }

  pInfo->idxNum = idxNum;
  if( idxNum & SCHEMAS_IDX_NAME_EQ ){
    pInfo->estimatedCost = 10.0;
    pInfo->estimatedRows = 1;
  }else if( idxNum & SCHEMAS_IDX_TYPE_EQ ){
    pInfo->estimatedCost = 50.0;
    pInfo->estimatedRows = 10;
  }else{
    pInfo->estimatedCost = 100.0;
    pInfo->estimatedRows = 10;
  }
  return SQLITE_OK;
}

static int schemasOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(SchemasCursor));
}

static void freeRows(SchemasCursor *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aRows[i].zType);
    sqlite3_free(pCur->aRows[i].zName);
    sqlite3_free(pCur->aRows[i].zFragment);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
}

static int schemasClose(sqlite3_vtab_cursor *pCursor){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  freeRows(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int schemasFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  SchemasVtab *pVtab = (SchemasVtab*)pCursor->pVtab;
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pSql;
  int rc;
  int iArg = 0;
  char *zSql;
  (void)idxStr;

  freeRows(pCur);
  pCur->iRow = 0;

  pSql = sqlite3_str_new(pVtab->db);
  if( !pSql ) return SQLITE_NOMEM;
  sqlite3_str_appendall(pSql,
    "SELECT type, name, sql FROM sqlite_schema "
    "WHERE type IN ('view','trigger')");
  if( idxNum & SCHEMAS_IDX_TYPE_EQ ){
    sqlite3_str_appendall(pSql, " AND type=?");
  }
  if( idxNum & SCHEMAS_IDX_NAME_EQ ){
    sqlite3_str_appendall(pSql, " AND name=?");
  }
  sqlite3_str_appendall(pSql, " ORDER BY type, name");
  zSql = sqlite3_str_finish(pSql);
  if( !zSql ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(pVtab->db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;
  if( idxNum & SCHEMAS_IDX_TYPE_EQ ){
    iArg++;
    if( iArg>argc ){
      sqlite3_finalize(pStmt);
      return SQLITE_OK;
    }
    sqlite3_bind_value(pStmt, iArg, argv[iArg-1]);
  }
  if( idxNum & SCHEMAS_IDX_NAME_EQ ){
    iArg++;
    if( iArg>argc ){
      sqlite3_finalize(pStmt);
      return SQLITE_OK;
    }
    sqlite3_bind_value(pStmt, iArg, argv[iArg-1]);
  }

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zType = (const char*)sqlite3_column_text(pStmt, 0);
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zSql  = (const char*)sqlite3_column_text(pStmt, 2);
    SchemasRow *aNew = sqlite3_realloc(pCur->aRows,
                                       (pCur->nRows+1)*(int)sizeof(SchemasRow));
    if( !aNew ){ rc = SQLITE_NOMEM; break; }
    pCur->aRows = aNew;
    memset(&pCur->aRows[pCur->nRows], 0, sizeof(SchemasRow));
    pCur->aRows[pCur->nRows].zType     = sqlite3_mprintf("%s", zType ? zType : "");
    pCur->aRows[pCur->nRows].zName     = sqlite3_mprintf("%s", zName ? zName : "");
    pCur->aRows[pCur->nRows].zFragment = sqlite3_mprintf("%s", zSql  ? zSql  : "");
    if( !pCur->aRows[pCur->nRows].zType
     || !pCur->aRows[pCur->nRows].zName
     || !pCur->aRows[pCur->nRows].zFragment ){
      rc = SQLITE_NOMEM;
      break;
    }
    pCur->nRows++;
  }
  sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
    freeRows(pCur);
    return rc;
  }
  return SQLITE_OK;
}

static int schemasNext(sqlite3_vtab_cursor *pCursor){
  ((SchemasCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int schemasEof(sqlite3_vtab_cursor *pCursor){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  return pCur->iRow >= pCur->nRows;
}

static int schemasColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  SchemasRow *r;
  if( pCur->iRow >= pCur->nRows ) return SQLITE_OK;
  r = &pCur->aRows[pCur->iRow];
  switch( iCol ){
    case 0: sqlite3_result_text(ctx, r->zType,     -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, r->zName,     -1, SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx, r->zFragment, -1, SQLITE_TRANSIENT); break;
    case 3:
    case 4:
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int schemasRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  SchemasCursor *pCur = (SchemasCursor*)pCursor;
  SchemasRow *r = &pCur->aRows[pCur->iRow];
  u64 h = doltliteFnv1aStr(DOLTLITE_FNV1A_OFFSET, r->zType);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aStr(h, r->zName);
  *pRowid = doltliteFnv1aRowid(h);
  return SQLITE_OK;
}

static sqlite3_module doltliteSchemasModule = {
  0, 0, schemasConnect, schemasBestIndex, doltliteVtabDisconnect, 0,
  schemasOpen, schemasClose, schemasFilter, schemasNext, schemasEof,
  schemasColumn, schemasRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteSchemasRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_schemas", &doltliteSchemasModule, 0);
}

#endif
