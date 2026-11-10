
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include <string.h>

typedef struct DocsVtab DocsVtab;
struct DocsVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DocsCursor DocsCursor;
struct DocsCursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pStmt;
  int eof;
  int synthetic;
};

static const char *zDocsVtabSchema =
  "CREATE TABLE x(doc_name TEXT NOT NULL PRIMARY KEY, "
  "doc_text TEXT NOT NULL) WITHOUT ROWID";

static const char *zDocsCreate =
  "CREATE TABLE main.dolt_docs("
  "doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name))";

static const char *zDocsAgentName = "AGENT.md";

static const char *zDocsAgentDefault =
  "# DoltLite database guide\n"
  "\n"
  "Use SQLite SQL through `doltlite database.db` or the embedded library.\n"
  "\n"
  "```sql\n"
  "SELECT * FROM dolt_status;\n"
  "SELECT * FROM dolt_log LIMIT 10;\n"
  "SELECT dolt_add('my_table');\n"
  "SELECT dolt_commit('-m', 'Describe the change');\n"
  "SELECT dolt_checkout('-b', 'feature');\n"
  "```\n"
  "\n"
  "Version control uses SQL functions and system tables. Each branch keeps its\n"
  "own uncommitted work; open `database.db/feature` to use it directly.\n"
  "Query `dolt_diff_my_table('HEAD', 'WORKING')` for row changes.\n"
  "\n"
  "Run `SELECT dolt_merge('feature')` inside `BEGIN` to inspect conflicts.\n"
  "Read `dolt_conflicts`, `dolt_schema_conflicts`, and\n"
  "`dolt_constraint_violations`. Resolve conflicts before committing, or use\n"
  "`ROLLBACK` to abandon a conflicted merge. Conflicts never persist.\n"
  "Inside `BEGIN`, `dolt_commit` commits and starts a new SQL transaction;\n"
  "finish later writes with `COMMIT`.\n"
  "\n"
  "For a configured remote: `SELECT dolt_push('origin', 'feature')`.\n"
  "\n"
  "DoltLite storage format version 12 cannot be opened by stock SQLite.\n"
  "With ATTACH, one transaction may write only one file-backed database;\n"
  "branches of the same file can commit together.\n"
  "\n"
  "Reference: https://github.com/dolthub/doltlite/tree/master/doc/doltlite\n";

static int docsConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DocsVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zDocsVtabSchema, sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (DocsVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int docsBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10.0;
  pInfo->estimatedRows = 1;
  return SQLITE_OK;
}

static int docsOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(DocsCursor));
}



static int docsAdvance(DocsCursor *c){
  DocsVtab *p = (DocsVtab*)c->base.pVtab;
  int rc;
  if( !c->pStmt ){
    c->eof = 1;
    return SQLITE_OK;
  }
  rc = sqlite3_step(c->pStmt);
  if( rc==SQLITE_ROW ) return SQLITE_OK;
  rc = sqlite3_finalize(c->pStmt);
  c->pStmt = 0;
  if( rc!=SQLITE_OK ) return doltliteVtabSetErrmsgFromDb(&p->base, p->db, rc);
  c->eof = 1;
  return SQLITE_OK;
}

static int docsFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DocsCursor *c = (DocsCursor*)pCursor;
  DocsVtab *p = (DocsVtab*)pCursor->pVtab;
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  if( doltliteIsStockSqliteDb(p->db) ){
    sqlite3_free(p->base.zErrMsg);
    p->base.zErrMsg = sqlite3_mprintf("%s", doltliteVcUnavailableMessage(p->db));
    return p->base.zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
  }
  sqlite3_finalize(c->pStmt);
  c->pStmt = 0;
  c->eof = 0;
  c->synthetic = 0;
  if( !sqlite3FindTable(p->db, "dolt_docs", "main") ){
    c->synthetic = 1;
    return SQLITE_OK;
  }
  rc = sqlite3_prepare_v3(p->db,
      "SELECT doc_name, doc_text FROM main.dolt_docs", -1,
      SQLITE_PREPARE_NO_VTAB, &c->pStmt, 0);
  if( rc!=SQLITE_OK ) return doltliteVtabSetErrmsgFromDb(&p->base, p->db, rc);
  return docsAdvance(c);
}

static int docsNext(sqlite3_vtab_cursor *pCursor){
  DocsCursor *c = (DocsCursor*)pCursor;
  if( c->synthetic ){
    c->synthetic = 0;
    c->eof = 1;
    return SQLITE_OK;
  }
  return docsAdvance(c);
}

static int docsEof(sqlite3_vtab_cursor *pCursor){
  return ((DocsCursor*)pCursor)->eof;
}

static int docsColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DocsCursor *c = (DocsCursor*)pCursor;
  if( c->synthetic && iCol==0 ){
    sqlite3_result_text(ctx, zDocsAgentName, -1, SQLITE_STATIC);
  }else if( c->synthetic ){
    sqlite3_result_text(ctx, zDocsAgentDefault, -1, SQLITE_STATIC);
  }else{
    sqlite3_result_value(ctx, sqlite3_column_value(c->pStmt, iCol));
  }
  return SQLITE_OK;
}

static int docsClose(sqlite3_vtab_cursor *pCursor){
  DocsCursor *c = (DocsCursor*)pCursor;
  sqlite3_finalize(c->pStmt);
  sqlite3_free(c);
  return SQLITE_OK;
}



static int docsSeedAgent(DocsVtab *p){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v3(p->db,
      "INSERT INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)", -1,
      SQLITE_PREPARE_NO_VTAB, &pStmt, 0);
  if( rc!=SQLITE_OK ) return doltliteVtabSetErrmsgFromDb(&p->base, p->db, rc);
  sqlite3_bind_text(pStmt, 1, zDocsAgentName, -1, SQLITE_STATIC);
  sqlite3_bind_text(pStmt, 2, zDocsAgentDefault, -1, SQLITE_STATIC);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return doltliteVtabSetErrmsgFromDb(&p->base, p->db, rc);
  return SQLITE_OK;
}

static int docsMaterialize(DocsVtab *p){
  int existed = sqlite3FindTable(p->db, "dolt_docs", "main")!=0;
  int rc = doltliteLazyCreateTable(&p->base, p->db, "dolt_docs", zDocsCreate);
  if( rc!=SQLITE_OK ) return rc;
  if( existed ) return SQLITE_OK;
  return docsSeedAgent(p);
}

static int docsBegin(sqlite3_vtab *pBase){
  return docsMaterialize((DocsVtab*)pBase);
}

static const char *docsInsertSql(sqlite3 *db){
  return doltliteVtabOnConflictOr(db,
      "INSERT OR REPLACE INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)",
      "INSERT OR IGNORE INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)",
      "INSERT OR FAIL INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)",
      "INSERT OR ROLLBACK INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)",
      "INSERT INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)");
}

static const char *docsUpdateSql(sqlite3 *db){
  return doltliteVtabOnConflictOr(db,
      "UPDATE OR REPLACE main.dolt_docs SET doc_name=?1, doc_text=?2 WHERE doc_name=?3",
      "UPDATE OR IGNORE main.dolt_docs SET doc_name=?1, doc_text=?2 WHERE doc_name=?3",
      "UPDATE OR FAIL main.dolt_docs SET doc_name=?1, doc_text=?2 WHERE doc_name=?3",
      "UPDATE OR ROLLBACK main.dolt_docs SET doc_name=?1, doc_text=?2 WHERE doc_name=?3",
      "UPDATE main.dolt_docs SET doc_name=?1, doc_text=?2 WHERE doc_name=?3");
}

static int docsUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                      sqlite3_int64 *pRowid){
  DocsVtab *p = (DocsVtab*)pBase;
  return doltliteLazyTwoColUpdate(pBase, p->db, argc, argv, pRowid,
      (int(*)(void*))docsMaterialize, p,
      "DELETE FROM main.dolt_docs WHERE doc_name=?1",
      docsUpdateSql(p->db), docsInsertSql(p->db));
}

static sqlite3_module doltliteDocsModule = {
  0, 0, docsConnect, docsBestIndex, doltliteVtabDisconnect, 0,
  docsOpen, docsClose, docsFilter, docsNext, docsEof,
  docsColumn, doltliteVtabZeroRowid,
  docsUpdate, docsBegin, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDocsRegister(sqlite3 *db){
  return doltliteCreateShieldedModule(db, "dolt_docs", &doltliteDocsModule, 0);
}

#endif
