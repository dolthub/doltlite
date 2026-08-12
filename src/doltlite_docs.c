
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include <string.h>

/* Eponymous stand-in for dolt_docs while no real table exists. Reads are
** always empty; the first write statement materializes a real dolt_docs
** table in main, which then shadows this module in name resolution
** (sqlite3LocateTable only reaches eponymous modules when sqlite3FindTable
** misses), so status/diff/merge/branching treat docs as an ordinary
** versioned table exactly like Dolt's user-space system tables. */

typedef struct DocsVtab DocsVtab;
struct DocsVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DocsCursor DocsCursor;
struct DocsCursor {
  sqlite3_vtab_cursor base;
  int iRow;
};

static const char *zDocsVtabSchema =
  "CREATE TABLE x(doc_name TEXT, doc_text TEXT)";

static const char *zDocsCreate =
  "CREATE TABLE main.dolt_docs("
  "doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name))";

static const char *zDocsAgentName = "AGENT.md";

/* Default AGENT.md, seeded when the backing table materializes and served
** by the module before that. DoltLite's counterpart to Dolt's embedded
** doc/AGENT.md, rewritten for this engine's SQL surface. */
static const char *zDocsAgentDefault =
  "# AGENT.md - DoltLite Database Operations Guide\n"
  "\n"
  "This database is a DoltLite database: SQLite-compatible SQL with\n"
  "Dolt-style version control (commits, branches, diffs, merges) built in.\n"
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
  "- Standard SQLite applies everywhere else: `.tables`, `PRAGMA`, ATTACH,\n"
  "  triggers, views, FTS5, R-Tree.\n";

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

static int docsFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  ((DocsCursor*)pCursor)->iRow = 0;
  return SQLITE_OK;
}

static int docsNext(sqlite3_vtab_cursor *pCursor){
  ((DocsCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int docsEof(sqlite3_vtab_cursor *pCursor){
  return ((DocsCursor*)pCursor)->iRow >= 1;
}

static int docsColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  (void)pCursor;
  if( iCol==0 ){
    sqlite3_result_text(ctx, zDocsAgentName, -1, SQLITE_STATIC);
  }else{
    sqlite3_result_text(ctx, zDocsAgentDefault, -1, SQLITE_STATIC);
  }
  return SQLITE_OK;
}

static int docsRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  (void)pCursor;
  *pRowid = 0;
  return SQLITE_OK;
}

static int docsSetErrMsg(DocsVtab *p, int rc){
  sqlite3_free(p->base.zErrMsg);
  p->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
  return rc;
}

static int docsExecBound(DocsVtab *p, const char *zSql,
                         sqlite3_value *pArg1, sqlite3_value *pArg2){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  if( pArg1 ) sqlite3_bind_value(pStmt, 1, pArg1);
  if( pArg2 ) sqlite3_bind_value(pStmt, 2, pArg2);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  return SQLITE_OK;
}

static int docsMaterialize(DocsVtab *p){
  int rc;
  char *zErr = 0;
  sqlite3_stmt *pStmt = 0;
  if( sqlite3FindTable(p->db, "dolt_docs", "main") ) return SQLITE_OK;
  rc = sqlite3_exec(p->db, zDocsCreate, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(p->base.zErrMsg);
    p->base.zErrMsg = sqlite3_mprintf("%s", zErr ? zErr : sqlite3_errstr(rc));
    sqlite3_free(zErr);
    return rc;
  }
  rc = sqlite3_prepare_v2(p->db,
      "INSERT INTO main.dolt_docs(doc_name, doc_text) VALUES('AGENT.md', ?1)",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  sqlite3_bind_text(pStmt, 1, zDocsAgentDefault, -1, SQLITE_STATIC);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return docsSetErrMsg(p, rc);
  return SQLITE_OK;
}

static int docsBegin(sqlite3_vtab *pBase){
  return docsMaterialize((DocsVtab*)pBase);
}

/* True when the stored AGENT.md row still carries the text this statement's
** materialization seeded, so an INSERT of AGENT.md should behave as if the
** row did not exist yet — matching Dolt, where the default row is synthetic
** and never blocks an insert. */
static int docsAgentSeedUntouched(DocsVtab *p){
  sqlite3_stmt *pStmt = 0;
  int hit = 0;
  int rc = sqlite3_prepare_v2(p->db,
      "SELECT 1 FROM main.dolt_docs WHERE doc_name='AGENT.md' AND doc_text=?1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return 0;
  sqlite3_bind_text(pStmt, 1, zDocsAgentDefault, -1, SQLITE_STATIC);
  hit = sqlite3_step(pStmt)==SQLITE_ROW;
  sqlite3_finalize(pStmt);
  return hit;
}

static int docsUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                      sqlite3_int64 *pRowid){
  DocsVtab *p = (DocsVtab*)pBase;
  const char *zSql;
  int rc;

  rc = docsMaterialize(p);
  if( rc!=SQLITE_OK ) return rc;

  if( argc==1 ){
    return docsExecBound(p,
        "DELETE FROM main.dolt_docs WHERE doc_name='AGENT.md'", 0, 0);
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    return docsExecBound(p,
        "UPDATE main.dolt_docs SET doc_name=?1, doc_text=?2 "
        "WHERE doc_name='AGENT.md'", argv[2], argv[3]);
  }

  switch( sqlite3_vtab_on_conflict(p->db) ){
    case SQLITE_REPLACE:
      zSql = "INSERT OR REPLACE INTO main.dolt_docs(doc_name, doc_text) "
             "VALUES(?1, ?2)";
      break;
    case SQLITE_IGNORE:
      zSql = "INSERT OR IGNORE INTO main.dolt_docs(doc_name, doc_text) "
             "VALUES(?1, ?2)";
      break;
    default:
      zSql = "INSERT INTO main.dolt_docs(doc_name, doc_text) VALUES(?1, ?2)";
      break;
  }
  if( sqlite3_value_type(argv[2])==SQLITE_TEXT
   && strcmp((const char*)sqlite3_value_text(argv[2]), zDocsAgentName)==0
   && docsAgentSeedUntouched(p) ){
    zSql = "INSERT OR REPLACE INTO main.dolt_docs(doc_name, doc_text) "
           "VALUES(?1, ?2)";
  }
  rc = docsExecBound(p, zSql, argv[2], argv[3]);
  if( rc!=SQLITE_OK ) return rc;
  if( pRowid ) *pRowid = sqlite3_last_insert_rowid(p->db);
  return SQLITE_OK;
}

static sqlite3_module doltliteDocsModule = {
  0, 0, docsConnect, docsBestIndex, doltliteVtabDisconnect, 0,
  docsOpen, doltliteVtabClose, docsFilter, docsNext, docsEof,
  docsColumn, docsRowid,
  docsUpdate, docsBegin, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDocsRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_docs", &doltliteDocsModule, 0);
}

#endif
