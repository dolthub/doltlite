#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

typedef struct MergeStatusVtab MergeStatusVtab;
struct MergeStatusVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct MergeStatusCursor MergeStatusCursor;
struct MergeStatusCursor {
  sqlite3_vtab_cursor base;
  int iRow;
  u8 isMerging;
  char *zSource;
  char *zSourceCommit;
  char *zTarget;
  char *zUnmergedTables;
};

static const char *zMergeStatusSchema =
  "CREATE TABLE x("
  "  is_merging      INTEGER,"
  "  source          TEXT,"
  "  source_commit   TEXT,"
  "  target          TEXT,"
  "  unmerged_tables TEXT"
  ")";

static int mergeStatusConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  MergeStatusVtab *v;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zMergeStatusSchema, sizeof(*v), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  v = (MergeStatusVtab*)*ppVtab;
  v->db = db;
  return SQLITE_OK;
}

static int mergeStatusBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->idxNum = 0;
  pInfo->estimatedCost = 1.0;
  pInfo->estimatedRows = 1;
  return SQLITE_OK;
}

static int mergeStatusOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(MergeStatusCursor));
}

static void mergeStatusClearRow(MergeStatusCursor *pCur){
  sqlite3_free(pCur->zSource);
  sqlite3_free(pCur->zSourceCommit);
  sqlite3_free(pCur->zTarget);
  sqlite3_free(pCur->zUnmergedTables);
  pCur->zSource = 0;
  pCur->zSourceCommit = 0;
  pCur->zTarget = 0;
  pCur->zUnmergedTables = 0;
  pCur->isMerging = 0;
}

static int mergeStatusClose(sqlite3_vtab_cursor *pCursor){
  MergeStatusCursor *pCur = (MergeStatusCursor*)pCursor;
  mergeStatusClearRow(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/* Fallback for a merge this connection did not run: DoltLite's working set
** persists only the resolved merge commit, so recover a name by finding the
** branch that still points at it. A raw-hash spec has no matching branch and
** falls through to the hash, which is what Dolt echoes back for that shape. The
** merged-into branch is skipped so a just-fast-forwarded target can never
** masquerade as the source. */
static char *mergeStatusSourceName(
  sqlite3 *db,
  const ProllyHash *pMergeCommit,
  const char *zTarget
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const BranchRef *aBranches = 0;
  int nBranches = 0;
  int i;

  if( cs ) refsTableGetBranches(&cs->refs, &nBranches, &aBranches);
  for(i=0; i<nBranches; i++){
    if( !aBranches[i].zName ) continue;
    if( zTarget && strcmp(aBranches[i].zName, zTarget)==0 ) continue;
    if( prollyHashCompare(&aBranches[i].commitHash, pMergeCommit)==0 ){
      return sqlite3_mprintf("%s", aBranches[i].zName);
    }
  }
  return 0;
}

/* Data conflicts, constraint violations and schema conflicts, deduplicated and
** name-ordered. Dolt builds the same union from a Go map, so its ordering is
** unspecified when more than one table is unmerged; sorting here makes the
** column deterministic. An active merge with everything already resolved
** reports the empty string, matching Dolt -- which is why the empty case cannot
** go through sqlite3_str_finish, whose NULL return is indistinguishable from
** OOM. */
static int mergeStatusUnmergedTables(sqlite3 *db, char **pzOut){
  static const char *zSql =
    "SELECT \"table\" FROM dolt_conflicts"
    " UNION SELECT \"table\" FROM dolt_constraint_violations"
    " UNION SELECT table_name FROM dolt_schema_conflicts"
    " ORDER BY 1";
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pStr;
  int rc;
  int n = 0;

  *pzOut = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;

  pStr = sqlite3_str_new(db);
  if( !pStr ){
    sqlite3_finalize(pStmt);
    return SQLITE_NOMEM;
  }
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
    if( !zName ) continue;
    if( n++ ) sqlite3_str_appendall(pStr, ", ");
    sqlite3_str_appendall(pStr, zName);
  }
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ){
    sqlite3_free(sqlite3_str_finish(pStr));
    return rc;
  }
  if( sqlite3_str_errcode(pStr) ){
    sqlite3_free(sqlite3_str_finish(pStr));
    return SQLITE_NOMEM;
  }
  if( n==0 ){
    sqlite3_free(sqlite3_str_finish(pStr));
    *pzOut = sqlite3_mprintf("%s", "");
    return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
  }
  *pzOut = sqlite3_str_finish(pStr);
  return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
}

static int mergeStatusFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  MergeStatusCursor *pCur = (MergeStatusCursor*)pCursor;
  sqlite3 *db = ((MergeStatusVtab*)pCursor->pVtab)->db;
  ProllyHash mergeCommit;
  char hex[PROLLY_HASH_SIZE*2+1];
  const char *zBranch;
  const char *zSpec;
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  mergeStatusClearRow(pCur);
  pCur->iRow = 0;

  doltliteGetSessionMergeState(db, &pCur->isMerging, &mergeCommit, 0);
  if( !pCur->isMerging ) return SQLITE_OK;

  doltliteHashToHex(&mergeCommit, hex);
  pCur->zSourceCommit = sqlite3_mprintf("%s", hex);
  if( !pCur->zSourceCommit ) return SQLITE_NOMEM;

  zBranch = doltliteGetSessionBranch(db);
  if( zBranch ){
    pCur->zTarget = sqlite3_mprintf("refs/heads/%s", zBranch);
    if( !pCur->zTarget ) return SQLITE_NOMEM;
  }

  zSpec = doltliteGetSessionMergeSourceSpec(db, &mergeCommit);
  pCur->zSource = zSpec ? sqlite3_mprintf("%s", zSpec)
                        : mergeStatusSourceName(db, &mergeCommit, zBranch);
  if( !pCur->zSource ){
    pCur->zSource = sqlite3_mprintf("%s", hex);
    if( !pCur->zSource ) return SQLITE_NOMEM;
  }

  rc = mergeStatusUnmergedTables(db, &pCur->zUnmergedTables);
  if( rc!=SQLITE_OK ) return rc;
  return SQLITE_OK;
}

static int mergeStatusNext(sqlite3_vtab_cursor *pCursor){
  ((MergeStatusCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int mergeStatusEof(sqlite3_vtab_cursor *pCursor){
  return ((MergeStatusCursor*)pCursor)->iRow >= 1;
}

static int mergeStatusColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  MergeStatusCursor *pCur = (MergeStatusCursor*)pCursor;
  const char *z = 0;

  if( pCur->iRow >= 1 ) return SQLITE_OK;
  if( iCol==0 ){
    sqlite3_result_int(ctx, pCur->isMerging ? 1 : 0);
    return SQLITE_OK;
  }
  switch( iCol ){
    case 1: z = pCur->zSource;         break;
    case 2: z = pCur->zSourceCommit;   break;
    case 3: z = pCur->zTarget;         break;
    case 4: z = pCur->zUnmergedTables; break;
  }
  if( z ){
    sqlite3_result_text(ctx, z, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int mergeStatusRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((MergeStatusCursor*)pCursor)->iRow;
  return SQLITE_OK;
}

static sqlite3_module doltliteMergeStatusModule = {
  0, 0, mergeStatusConnect, mergeStatusBestIndex, doltliteVtabDisconnect, 0,
  mergeStatusOpen, mergeStatusClose, mergeStatusFilter, mergeStatusNext,
  mergeStatusEof, mergeStatusColumn, mergeStatusRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteMergeStatusRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_merge_status",
                               &doltliteMergeStatusModule, 0);
}

#endif
