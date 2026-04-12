/* dolt_schemas: projection over sqlite_schema, filtered to views and
** triggers. Mirrors Dolt's dolt_schemas system table so user-level
** view/trigger management is visible, diffable, and version-controlled
** alongside regular table data.
**
** Storage layer: sqlite_schema is already branch-scoped in the
** doltlite catalog (table #1 in each commit), so every branch sees
** its own view set automatically. This vtable is a read-only
** projection that runs an internal SELECT against sqlite_schema on
** every xFilter and caches the result rows for the cursor.
**
** Columns (matching Dolt's dolt_schemas for oracle comparability):
**   type     TEXT  -- 'view' or 'trigger'
**   name     TEXT  -- object name
**   fragment TEXT  -- full CREATE statement (Dolt stores the same)
**   extra    TEXT  -- always NULL in doltlite (Dolt uses this for JSON
**                    metadata; we don't track it)
**   sql_mode TEXT  -- always NULL in doltlite (MySQL-specific)
*/

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

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
  rc = sqlite3_declare_vtab(db, zSchemasSchema);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int schemasDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int schemasBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 100.0;
  pInfo->estimatedRows = 10;
  return SQLITE_OK;
}

static int schemasOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  SchemasCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
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
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  freeRows(pCur);
  pCur->iRow = 0;

  /* Pull view/trigger rows from the live sqlite_schema, which is
  ** already branch-scoped. Order by (type, name) so the output is
  ** deterministic across runs — same ordering the oracle test uses. */
  rc = sqlite3_prepare_v2(pVtab->db,
    "SELECT type, name, sql FROM sqlite_schema "
    "WHERE type IN ('view','trigger') ORDER BY type, name",
    -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;

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
    case 3: /* extra: JSON metadata Dolt tracks, doltlite does not. */
    case 4: /* sql_mode: MySQL-specific. */
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int schemasRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((SchemasCursor*)pCursor)->iRow;
  return SQLITE_OK;
}

static sqlite3_module doltliteSchemasModule = {
  0, 0, schemasConnect, schemasBestIndex, schemasDisconnect, 0,
  schemasOpen, schemasClose, schemasFilter, schemasNext, schemasEof,
  schemasColumn, schemasRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteSchemasRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_schemas", &doltliteSchemasModule, 0);
}

#endif
