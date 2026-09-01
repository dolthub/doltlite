
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
  "# AGENT.md - DoltLite Database Operations Guide\n"
  "\n"
  "This is a DoltLite database: SQLite-compatible SQL with Dolt-style\n"
  "version control (commits, branches, diffs, merges) built in.\n"
  "Version control operations are SQL function calls, not stored\n"
  "procedures: use `SELECT dolt_commit(...)`, never `CALL dolt_commit(...)`.\n"
  "\n"
  "## Core Workflow\n"
  "\n"
  "```sql\n"
  "SELECT dolt_add('-A');                    -- stage all changes\n"
  "SELECT dolt_commit('-m', 'message');      -- commit staged changes\n"
  "SELECT dolt_commit('-A', '-m', 'message');-- stage and commit at once\n"
  "SELECT * FROM dolt_status;                -- what is staged / modified\n"
  "SELECT * FROM dolt_log;                   -- commit history\n"
  "```\n"
  "\n"
  "## Branches\n"
  "\n"
  "```sql\n"
  "SELECT dolt_branch('feature');            -- create\n"
  "SELECT dolt_checkout('feature');          -- switch\n"
  "SELECT dolt_checkout('-b', 'feature2');   -- create and switch\n"
  "SELECT active_branch();\n"
  "SELECT * FROM dolt_branches;\n"
  "SELECT dolt_merge('feature');\n"
  "```\n"
  "\n"
  "Each branch has its own working state; uncommitted changes are\n"
  "per-branch.\n"
  "\n"
  "## Remotes\n"
  "\n"
  "```sql\n"
  "SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.db');\n"
  "SELECT dolt_push('origin', 'main');\n"
  "SELECT dolt_pull('origin', 'main');\n"
  "SELECT dolt_clone('file:///path/to/source.db');\n"
  "```\n"
  "\n"
  "## Diffs and History\n"
  "\n"
  "```sql\n"
  "SELECT * FROM dolt_diff;                     -- tables changed per commit\n"
  "SELECT * FROM dolt_diff_stat('v1', 'HEAD');  -- row/cell counts\n"
  "SELECT * FROM dolt_patch('v1', 'v2');        -- executable SQL statements\n"
  "-- Per user table <t>: dolt_diff_<t>, dolt_history_<t>, dolt_workspace_<t>\n"
  "```\n"
  "\n"
  "## Merge Conflicts\n"
  "\n"
  "A merge that hits conflicts in autocommit mode rolls back. Run it inside\n"
  "an explicit transaction to inspect and resolve:\n"
  "\n"
  "```sql\n"
  "BEGIN;\n"
  "SELECT dolt_merge('feature');\n"
  "SELECT * FROM dolt_conflicts;              -- summary per table\n"
  "SELECT * FROM dolt_conflicts_<t>;          -- base/ours/theirs rows\n"
  "SELECT dolt_conflicts_resolve('--ours', '<t>');\n"
  "COMMIT;\n"
  "SELECT dolt_commit('-m', 'merged');\n"
  "```\n"
  "\n"
  "Constraint violations may persist after merges. Inspect\n"
  "`dolt_constraint_violations` and `dolt_constraint_violations_<t>`, then\n"
  "run `SELECT dolt_verify_constraints('--all')` after repairs.\n"
  "\n"
  "## Undoing Changes\n"
  "\n"
  "```sql\n"
  "SELECT dolt_reset('--hard');               -- discard working changes\n"
  "SELECT dolt_reset('--hard', 'HEAD~1');     -- move HEAD back one commit\n"
  "SELECT dolt_revert('HEAD');                -- new commit undoing HEAD\n"
  "```\n"
  "\n"
  "## Notes\n"
  "\n"
  "- `dolt_docs` (this table) stores versioned documents keyed by name;\n"
  "  `dolt_ignore` holds patterns for tables `dolt_add` should skip. Both\n"
  "  commit, diff, branch and merge like ordinary tables.\n"
  "- Beta storage format version 12 is not SQLite's page format; stock SQLite\n"
  "  cannot open it, and no SQLite journal, `-wal`, or `-shm` sidecars exist.\n"
  "- ATTACH works, but one transaction may write only one file-backed database.\n"
  "- Most SQLite SQL features remain available, including triggers, views,\n"
  "  FTS5, and R-Tree. For storage-coupled API and PRAGMA differences, see\n"
  "  https://github.com/dolthub/doltlite#sqlite-compatibility.\n";

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

static int docsSetErrMsg(DocsVtab *p, int rc){
  sqlite3_free(p->base.zErrMsg);
  p->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
  return rc;
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
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  c->eof = 1;
  return SQLITE_OK;
}

static int docsFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DocsCursor *c = (DocsCursor*)pCursor;
  DocsVtab *p = (DocsVtab*)pCursor->pVtab;
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
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
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
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

static int docsRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  (void)pCursor;
  *pRowid = 0;
  return SQLITE_OK;
}

static int docsClose(sqlite3_vtab_cursor *pCursor){
  DocsCursor *c = (DocsCursor*)pCursor;
  sqlite3_finalize(c->pStmt);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int docsExecBound(DocsVtab *p, const char *zSql,
                         sqlite3_value *pArg1, sqlite3_value *pArg2,
                         sqlite3_value *pArg3){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v3(p->db, zSql, -1,
                              SQLITE_PREPARE_NO_VTAB, &pStmt, 0);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  if( pArg1 ) sqlite3_bind_value(pStmt, 1, pArg1);
  if( pArg2 ) sqlite3_bind_value(pStmt, 2, pArg2);
  if( pArg3 ) sqlite3_bind_value(pStmt, 3, pArg3);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  return SQLITE_OK;
}

static int docsSeedAgent(DocsVtab *p){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v3(p->db,
      "INSERT INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)", -1,
      SQLITE_PREPARE_NO_VTAB, &pStmt, 0);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  sqlite3_bind_text(pStmt, 1, zDocsAgentName, -1, SQLITE_STATIC);
  sqlite3_bind_text(pStmt, 2, zDocsAgentDefault, -1, SQLITE_STATIC);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  return SQLITE_OK;
}

static int docsMaterialize(DocsVtab *p){
  int rc;
  char *zErr = 0;
  if( sqlite3FindTable(p->db, "dolt_docs", "main") ) return SQLITE_OK;
  rc = sqlite3_exec(p->db, zDocsCreate, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(p->base.zErrMsg);
    p->base.zErrMsg = sqlite3_mprintf("%s", zErr ? zErr : sqlite3_errstr(rc));
    sqlite3_free(zErr);
    return rc;
  }
  return docsSeedAgent(p);
}

static int docsBegin(sqlite3_vtab *pBase){
  return docsMaterialize((DocsVtab*)pBase);
}

static const char *docsInsertSql(sqlite3 *db){
  switch( sqlite3_vtab_on_conflict(db) ){
    case SQLITE_REPLACE:
      return "INSERT OR REPLACE INTO main.dolt_docs(doc_name, doc_text) "
             "VALUES(?1, ?2)";
    case SQLITE_IGNORE:
      return "INSERT OR IGNORE INTO main.dolt_docs(doc_name, doc_text) "
             "VALUES(?1, ?2)";
    case SQLITE_FAIL:
      return "INSERT OR FAIL INTO main.dolt_docs(doc_name, doc_text) "
             "VALUES(?1, ?2)";
    case SQLITE_ROLLBACK:
      return "INSERT OR ROLLBACK INTO main.dolt_docs(doc_name, doc_text) "
             "VALUES(?1, ?2)";
    default:
      return "INSERT INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)";
  }
}

static const char *docsUpdateSql(sqlite3 *db){
  switch( sqlite3_vtab_on_conflict(db) ){
    case SQLITE_REPLACE:
      return "UPDATE OR REPLACE main.dolt_docs SET doc_name=?1, doc_text=?2 "
             "WHERE doc_name=?3";
    case SQLITE_IGNORE:
      return "UPDATE OR IGNORE main.dolt_docs SET doc_name=?1, doc_text=?2 "
             "WHERE doc_name=?3";
    case SQLITE_FAIL:
      return "UPDATE OR FAIL main.dolt_docs SET doc_name=?1, doc_text=?2 "
             "WHERE doc_name=?3";
    case SQLITE_ROLLBACK:
      return "UPDATE OR ROLLBACK main.dolt_docs SET doc_name=?1, doc_text=?2 "
             "WHERE doc_name=?3";
    default:
      return "UPDATE main.dolt_docs SET doc_name=?1, doc_text=?2 "
             "WHERE doc_name=?3";
  }
}

static int docsUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                      sqlite3_int64 *pRowid){
  DocsVtab *p = (DocsVtab*)pBase;
  int rc;

  rc = docsMaterialize(p);
  if( rc!=SQLITE_OK ) return rc;

  if( argc==1 ){
    return docsExecBound(p,
        "DELETE FROM main.dolt_docs WHERE doc_name=?1", argv[0], 0, 0);
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    return docsExecBound(p,
        docsUpdateSql(p->db), argv[2], argv[3], argv[0]);
  }

  rc = docsExecBound(p, docsInsertSql(p->db), argv[2], argv[3], 0);
  if( rc!=SQLITE_OK ) return rc;
  if( pRowid ) *pRowid = sqlite3_last_insert_rowid(p->db);
  return SQLITE_OK;
}

static sqlite3_module doltliteDocsModule = {
  0, 0, docsConnect, docsBestIndex, doltliteVtabDisconnect, 0,
  docsOpen, docsClose, docsFilter, docsNext, docsEof,
  docsColumn, docsRowid,
  docsUpdate, docsBegin, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDocsRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_docs", &doltliteDocsModule, 0);
}

#endif
