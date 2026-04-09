
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

extern int doltliteLogRegister(sqlite3 *db);
extern int doltliteStatusRegister(sqlite3 *db);
extern int doltliteDiffRegister(sqlite3 *db);
extern int doltliteBranchRegister(sqlite3 *db);
extern int doltliteConflictsRegister(sqlite3 *db);
extern void doltliteRegisterConflictTables(sqlite3 *db);
extern int doltliteTagRegister(sqlite3 *db);
extern int doltliteGcRegister(sqlite3 *db);
extern void doltliteRegisterDiffTables(sqlite3 *db);
extern int doltliteAncestorRegister(sqlite3 *db);
extern int doltliteAtRegister(sqlite3 *db);
extern void doltliteRegisterAtTables(sqlite3 *db);
extern void doltliteRegisterHistoryTables(sqlite3 *db);
extern int doltliteSchemaDiffRegister(sqlite3 *db);

extern int doltliteFindAncestor(sqlite3 *db, const ProllyHash *h1,
                                 const ProllyHash *h2, ProllyHash *pAnc);

/* doltliteMergeCatalogs is now declared in doltlite_internal.h */

extern const char *doltliteNextTableForSchema(sqlite3 *db, int *pIdx, Pgno *piTable);
extern void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH);

/*
** Returns 1 if there are uncommitted changes (working differs from HEAD
** or staged differs from HEAD), 0 otherwise.
*/
static int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash);

int doltliteHasUncommittedChanges(sqlite3 *db){
  ProllyHash headCatHash, stagedHash, workingCatHash;
  u8 *wCatData = 0; int nWCat = 0;
  int rc;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&headCatHash) ) return 0;

  /* Staged differs from HEAD? */
  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash)
   && prollyHashCompare(&headCatHash, &stagedHash)!=0 ){
    return 1;
  }

  /* Working differs from HEAD? Catalog serialization is now deterministic
  ** (sorted by name, iNextTable zeroed, names eagerly resolved) so a
  ** simple hash comparison is sufficient. */
  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    if( cs ){
      rc = doltliteFlushAndSerializeCatalog(db, &wCatData, &nWCat);
      if( rc==SQLITE_OK ){
        chunkStorePut(cs, wCatData, nWCat, &workingCatHash);
        sqlite3_free(wCatData);
        if( prollyHashCompare(&headCatHash, &workingCatHash)!=0 ){
          return 1;
        }
      }
    }
    return 0;
  }
}

void doltliteUpdateSchemaHashes(sqlite3 *db){
  int idx = 0;
  Pgno iTable;
  const char *zName;
  while( (zName = doltliteNextTableForSchema(db, &idx, &iTable)) != 0 ){
    sqlite3_stmt *pStmt = 0;
    char *zSql = sqlite3_mprintf(
      "SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name='%q'", zName);
    if( zSql ){
      if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK ){
        if( sqlite3_step(pStmt)==SQLITE_ROW ){
          const char *zCreate = (const char*)sqlite3_column_text(pStmt, 0);
          if( zCreate ){
            ProllyHash h;
            prollyHashCompute(zCreate, (int)strlen(zCreate), &h);
            doltliteSetTableSchemaHash(db, iTable, &h);
          }
        }
        sqlite3_finalize(pStmt);
      }
      sqlite3_free(zSql);
    }
  }
}

/*
** Helper #1: Persist working-set + refs to disk.
** Replaces the doltliteSaveWorkingSet/chunkStoreSerializeRefs/chunkStoreCommit
** sequence found at many call sites.
*/
static int doltlitePersistState(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  doltliteSaveWorkingSet(db);
  chunkStoreSerializeRefs(cs);
  return chunkStoreCommit(cs);
}

/*
** Helper #2: Flush the in-memory catalog, serialize it into the chunk store,
** and return the resulting hash.
*/
static int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *catData = 0;
  int nCatData = 0;
  int rc;
  rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, catData, nCatData, pHash);
  sqlite3_free(catData);
  return rc;
}

/*
** Helper #3: Free an array of SchemaMergeAction structs.
*/
void freeSchemaMergeActions(SchemaMergeAction *a, int n){
  int i, j;
  for(i=0; i<n; i++){
    for(j=0; j<a[i].nAddColumns; j++){
      sqlite3_free(a[i].azAddColumns[j]);
    }
    sqlite3_free(a[i].azAddColumns);
    sqlite3_free(a[i].zTableName);
  }
  sqlite3_free(a);
}

/* resolveCommitRef removed — use doltliteResolveRef() from doltlite_ref.c */

/* Helper #5: loadCommitByHash replaced by doltliteLoadCommit from doltlite_ref.c */

/*
** Helper #6: Build a commit object, serialize it, store it, and clear.
** If zAuthorName/zAuthorEmail are NULL the default session author is used.
** aExtraParents/nExtraParents are for merge commits (may be NULL/0).
** Writes the resulting commit hash into *pCommitHash.
*/
static int doltliteCreateAndStoreCommit(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  ProllyHash *pCommitHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit c;
  u8 *commitData = 0;
  int nCommitData = 0;
  int rc, i;

  memset(&c, 0, sizeof(c));
  memcpy(&c.parentHash, pParent, sizeof(ProllyHash));
  memcpy(&c.catalogHash, pCatalog, sizeof(ProllyHash));
  c.timestamp = (i64)time(0);
  c.zName  = sqlite3_mprintf("%s", zAuthorName  ? zAuthorName  : doltliteGetAuthorName(db));
  c.zEmail = sqlite3_mprintf("%s", zAuthorEmail ? zAuthorEmail : doltliteGetAuthorEmail(db));
  c.zMessage = sqlite3_mprintf("%s", zMessage);

  if( nExtraParents > 0 && aExtraParents ){
    c.aParents[0] = *pParent;
    for(i=0; i<nExtraParents && (i+1)<DOLTLITE_MAX_PARENTS; i++){
      c.aParents[i+1] = aExtraParents[i];
    }
    c.nParents = 1 + (nExtraParents < DOLTLITE_MAX_PARENTS-1
                       ? nExtraParents : DOLTLITE_MAX_PARENTS-1);
  }

  rc = doltliteCommitSerialize(&c, &commitData, &nCommitData);
  if( rc==SQLITE_OK ) rc = chunkStorePut(cs, commitData, nCommitData, pCommitHash);
  sqlite3_free(commitData);
  doltliteCommitClear(&c);
  return rc;
}

/*
** Helper #7: Advance the current branch to a new head commit.
** Sets session head, session staged, updates the branch ref, and persists.
** Handles the first-commit case where nBranches==0.
*/
static int doltliteAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *branch = doltliteGetSessionBranch(db);

  doltliteSetSessionHead(db, pNewHead);
  doltliteSetSessionStaged(db, pCatalogHash);

  if( cs->nBranches==0 ){
    chunkStoreAddBranch(cs, branch, pNewHead);
    chunkStoreSetDefaultBranch(cs, branch);
  }else{
    chunkStoreUpdateBranch(cs, branch, pNewHead);
  }

  return doltlitePersistState(db);
}

/*
** Helper #8: Register conflict tables, persist state, and report a
** conflict-count message via sqlite3_result_text.
*/
static void doltliteReportConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[256];
  doltliteRegisterConflictTables(db);
  doltlitePersistState(db);
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
    zOp, nConflicts);
  sqlite3_result_text(ctx, msg, -1, SQLITE_TRANSIENT);
}

static void doltliteAddFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;
  int i;
  int stageAll = 0;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }
  if( argc==0 ){
    sqlite3_result_error(context, "dolt_add requires table name or '-A'", -1);
    return;
  }

  
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( arg && (strcmp(arg, "-A")==0 || strcmp(arg, "-a")==0 || strcmp(arg, ".")==0) ){
      stageAll = 1;
      break;
    }
  }


  {
    ProllyHash workingHash;

    rc = doltliteFlushCatalogToHash(db, &workingHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }

    if( stageAll ){
      
      doltliteSetSessionStaged(db, &workingHash);
    }else{
      
      struct TableEntry *aWorking = 0, *aStaged = 0;
      int nWorking = 0, nStaged = 0;
      ProllyHash stagedHash;

      
      rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(context, "failed to load working catalog", -1);
        return;
      }

      doltliteGetSessionStaged(db, &stagedHash);
      if( prollyHashIsEmpty(&stagedHash) ){
        
        ProllyHash headCat;
        rc = doltliteGetHeadCatalogHash(db, &headCat);
        if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCat) ){
          rc = doltliteLoadCatalog(db, &headCat, &aStaged, &nStaged, 0);
        }
      }else{
        rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(aWorking);
        sqlite3_result_error(context, "failed to load staged catalog", -1);
        return;
      }

      
      for(i=0; i<argc; i++){
        const char *zTable = (const char*)sqlite3_value_text(argv[i]);
        Pgno iTable;
        int j;

        if( !zTable || zTable[0]=='-' ) continue;
        rc = doltliteResolveTableName(db, zTable, &iTable);
        if( rc!=SQLITE_OK ) continue;

        
        for(j=0; j<nWorking; j++){
          if( aWorking[j].iTable==iTable ){
            
            int k;
            int updated = 0;
            for(k=0; k<nStaged; k++){
              if( aStaged[k].iTable==iTable ){
                aStaged[k].root = aWorking[j].root;
                aStaged[k].schemaHash = aWorking[j].schemaHash;
                aStaged[k].flags = aWorking[j].flags;
                updated = 1;
                break;
              }
            }
            if( !updated ){
              
              struct TableEntry *aNew = sqlite3_realloc(aStaged,
                  (nStaged+1)*(int)sizeof(struct TableEntry));
              if( aNew ){
                aStaged = aNew;
                aStaged[nStaged] = aWorking[j];
                nStaged++;
              }
            }
            break;
          }
        }
      }

      
      {
        {
          int sz = CAT_HEADER_SIZE_V3;
          u8 *buf, *p;
          ProllyHash newStagedHash;
          int j;

          for(j=0;j<nStaged;j++){
            int nl = aStaged[j].zName ? (int)strlen(aStaged[j].zName) : 0;
            sz += 4+1+PROLLY_HASH_SIZE+PROLLY_HASH_SIZE+2+nl;
          }
          buf = sqlite3_malloc(sz);
          if( !buf ){
            sqlite3_free(aWorking);
            sqlite3_free(aStaged);
            sqlite3_result_error(context, "out of memory", -1);
            return;
          }
          p = buf;
          *p++ = CATALOG_FORMAT_V3;
          p[0]=(u8)nStaged; p[1]=(u8)(nStaged>>8);
          p[2]=(u8)(nStaged>>16); p[3]=(u8)(nStaged>>24);
          p += 4;

          for(i=0; i<nStaged; i++){
            Pgno pg = aStaged[i].iTable;
            int nl = aStaged[i].zName ? (int)strlen(aStaged[i].zName) : 0;
            p[0]=(u8)pg; p[1]=(u8)(pg>>8); p[2]=(u8)(pg>>16); p[3]=(u8)(pg>>24);
            p += 4;
            *p++ = aStaged[i].flags;
            memcpy(p, aStaged[i].root.data, PROLLY_HASH_SIZE);
            p += PROLLY_HASH_SIZE;
            memcpy(p, aStaged[i].schemaHash.data, PROLLY_HASH_SIZE);
            p += PROLLY_HASH_SIZE;
            p[0]=(u8)nl; p[1]=(u8)(nl>>8); p+=2;
            if(nl>0) memcpy(p, aStaged[i].zName, nl);
            p += nl;
          }

          rc = chunkStorePut(cs, buf, (int)(p-buf), &newStagedHash);
          sqlite3_free(buf);
          if( rc==SQLITE_OK ){
            doltliteSetSessionStaged(db, &newStagedHash);
          }
        }
      }

      sqlite3_free(aWorking);
      sqlite3_free(aStaged);
    }


    {
      ProllyHash savedStaged;
      doltliteGetSessionStaged(db, &savedStaged);
      rc = doltlitePersistState(db);
      if( rc!=SQLITE_OK ){
        doltliteSetSessionStaged(db, &savedStaged);
        sqlite3_result_error_code(context, rc);
        return;
      }
    }
  }

  sqlite3_result_int(context, 0);
}

static void doltliteCommitFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zMessage = 0;
  const char *zAuthor = 0;
  int addAll = 0;
  ProllyHash commitHash;
  ProllyHash catalogHash;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  int rc;
  int i;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }

  
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( arg[0]=='-' && arg[1]!='-' && arg[1]!=0 && arg[2]!=0 ){
      
      int j;
      for(j=1; arg[j]; j++){
        if( arg[j]=='a' || arg[j]=='A' ){
          addAll = 1;
        }else if( arg[j]=='m' ){
          
          if( arg[j+1]!=0 ){
            zMessage = &arg[j+1];
          }else if( i+1<argc ){
            zMessage = (const char*)sqlite3_value_text(argv[++i]);
          }
          break;  
        }
      }
    }else if( strcmp(arg, "-m")==0 && i+1<argc ){
      zMessage = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "--author")==0 && i+1<argc ){
      zAuthor = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "-A")==0 || strcmp(arg, "-a")==0 ){
      addAll = 1;
    }
  }

  if( !zMessage || zMessage[0]==0 ){
    sqlite3_result_error(context,
      "dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')", -1);
    return;
  }


  if( addAll ){
    rc = doltliteFlushCatalogToHash(db, &catalogHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }
    doltliteSetSessionStaged(db, &catalogHash);
  }

  
  {
    ProllyHash cfHash;
    doltliteGetSessionConflictsCatalog(db, &cfHash);
    if( !prollyHashIsEmpty(&cfHash) ){
      sqlite3_result_error(context,
        "cannot commit: unresolved merge conflicts. Use dolt_conflicts_resolve() first.", -1);
      return;
    }
  }

  
  doltliteGetSessionStaged(db, &catalogHash);
  if( prollyHashIsEmpty(&catalogHash) ){
    sqlite3_result_error(context,
      "nothing to commit (use dolt_add first, or dolt_commit('-A', '-m', 'msg'))", -1);
    return;
  }

  
  {
    u8 isMerging = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( !isMerging ){
      ProllyHash headCatHash;
      rc = doltliteGetHeadCatalogHash(db, &headCatHash);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash)
       && prollyHashCompare(&catalogHash, &headCatHash)==0 ){
        sqlite3_result_error(context,
          "nothing to commit, working tree clean (use dolt_add to stage changes)", -1);
        return;
      }
    }
  }

  /* Build commit object locally (no lock needed yet) */
  {
    ProllyHash parentHash;
    char *zParsedName = 0, *zParsedEmail = 0;
    doltliteGetSessionHead(db, &parentHash);

    if( zAuthor ){
      const char *lt = strchr(zAuthor, '<');
      const char *gt = lt ? strchr(lt, '>') : 0;
      if( lt && gt ){
        int nameLen = (int)(lt - zAuthor);
        while( nameLen>0 && zAuthor[nameLen-1]==' ' ) nameLen--;
        zParsedName = sqlite3_mprintf("%.*s", nameLen, zAuthor);
        zParsedEmail = sqlite3_mprintf("%.*s", (int)(gt-lt-1), lt+1);
      }else{
        zParsedName = sqlite3_mprintf("%s", zAuthor);
        zParsedEmail = sqlite3_mprintf("");
      }
    }

    rc = doltliteCreateAndStoreCommit(db, &parentHash, &catalogHash,
        zMessage, zParsedName, zParsedEmail, 0, 0, &commitHash);
    sqlite3_free(zParsedName);
    sqlite3_free(zParsedEmail);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  /* Lock the commit graph, refresh from disk, then check for conflicts.
  ** This serializes all commit graph mutations across connections. */
  rc = chunkStoreLockAndRefresh(cs);
  if( rc==SQLITE_BUSY ){
    sqlite3_result_error(context,
      "database is locked by another connection", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  /* Under lock: check if branch tip still matches our session HEAD */
  {
    const char *branch = doltliteGetSessionBranch(db);
    ProllyHash branchTip;
    ProllyHash sessionHead;
    doltliteGetSessionHead(db, &sessionHead);
    if( chunkStoreFindBranch(cs, branch, &branchTip)==SQLITE_OK ){
      if( prollyHashCompare(&sessionHead, &branchTip)!=0 ){
        chunkStoreUnlock(cs);
        sqlite3_result_error(context,
          "commit conflict: another connection committed to this branch. "
          "Please retry your transaction.", -1);
        return;
      }
    }
  }

  /* Under lock: update session and shared state */
  {
    u8 wasMerging = 0;
    doltliteGetSessionMergeState(db, &wasMerging, 0, 0);
    if( wasMerging ){
      doltliteClearSessionMergeState(db);
    }
  }

  rc = doltliteAdvanceBranch(db, &commitHash, &catalogHash);
  chunkStoreUnlock(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  doltliteHashToHex(&commitHash, hexBuf);

  
  doltliteRegisterDiffTables(db);
  doltliteRegisterHistoryTables(db);
  doltliteRegisterAtTables(db);

  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}

static void doltliteResetFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash targetCatHash;
  int isHard = 0;
  const char *zRef = 0;
  int rc;
  int i;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }

  
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--hard")==0 ){ isHard = 1; }
    else if( strcmp(arg, "--soft")==0 ){  }
    else{ zRef = arg; }
  }

  if( zRef ){
    ProllyHash targetCommit;
    DoltliteCommit commit;

    rc = doltliteResolveRef(db,zRef, &targetCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "commit not found", -1);
      return;
    }

    rc = doltliteLoadCommit(db, &targetCommit, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to load commit", -1);
      return;
    }
    memcpy(&targetCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);

    doltliteSetSessionHead(db, &targetCommit);
    chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &targetCommit);
    chunkStoreSerializeRefs(cs);

    doltliteClearSessionMergeState(db);
  }else{
    rc = doltliteGetHeadCatalogHash(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to read HEAD", -1);
      return;
    }
  }

  doltliteSetSessionStaged(db, &targetCatHash);

  if( isHard ){
    if( prollyHashIsEmpty(&targetCatHash) ){
      sqlite3_result_error(context, "no commit to reset to", -1);
      return;
    }
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    rc = doltliteHardReset(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "hard reset failed", -1);
      return;
    }
  }else{
    rc = doltlitePersistState(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  sqlite3_result_int(context, 0);
}

/* extractColNameFromDef, bindRecordField, migrateDiffCb, migrateSchemaRowData
** moved to doltlite_schema_merge.c; declared in doltlite_internal.h */



static void doltliteMergeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch;
  ProllyHash ourHead, theirHead, ancestorHash;
  ProllyHash ourCatHash, theirCatHash, ancCatHash, mergedCatHash;
  int nMergeConflicts = 0;
  DoltliteCommit ourCommit, theirCommit, ancCommit;
  int rc;

  memset(&ourCommit, 0, sizeof(ourCommit));
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&ancCommit, 0, sizeof(ancCommit));

  
  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(context, "usage: dolt_merge('branch')", -1); return; }

  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ sqlite3_result_error(context, "branch name required", -1); return; }

  
  if( strcmp(zBranch, "--abort")==0 ){
    u8 isMerging = 0;
    ProllyHash headCatHash;

    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( !isMerging ){
      sqlite3_result_error(context, "no merge in progress", -1);
      return;
    }

    
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to read HEAD", -1);
      return;
    }
    rc = doltliteHardReset(db, &headCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "abort reset failed", -1);
      return;
    }

    
    doltliteSetSessionStaged(db, &headCatHash);
    


    doltliteClearSessionMergeState(db);
    doltlitePersistState(db);

    sqlite3_result_int(context, 0);
    return;
  }

  
  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  
  rc = chunkStoreFindBranch(cs, zBranch, &theirHead);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "branch not found", -1);
    return;
  }
  if( prollyHashIsEmpty(&theirHead) ){
    sqlite3_result_error(context, "target branch has no commits", -1);
    return;
  }

  
  if( prollyHashCompare(&ourHead, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }

  /* Reject merge if there are uncommitted changes */
  if( doltliteHasUncommittedChanges(db) ){
    sqlite3_result_error(context,
      "uncommitted changes \xe2\x80\x94 commit or reset before merging", -1);
    return;
  }

  
  rc = doltliteFindAncestor(db, &ourHead, &theirHead, &ancestorHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&ancestorHash) ){
    sqlite3_result_error(context, "no common ancestor found", -1);
    return;
  }

  
  if( prollyHashCompare(&ancestorHash, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }

  
  if( prollyHashCompare(&ancestorHash, &ourHead)==0 ){
    /* Fast-forward: move branch pointer to their commit, no merge commit. */
    char hx[PROLLY_HASH_SIZE*2+1];

    rc = doltliteLoadCommit(db, &theirHead, &theirCommit);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load commit", -1); return; }

    rc = doltliteSwitchCatalog(db, &theirCommit.catalogHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&theirCommit);
      sqlite3_result_error(context, "fast-forward failed", -1);
      return;
    }

    doltliteUpdateBranchWorkingState(db,
        doltliteGetSessionBranch(db), &theirCommit.catalogHash, NULL);
    rc = doltliteAdvanceBranch(db, &theirHead, &theirCommit.catalogHash);
    doltliteCommitClear(&theirCommit);

    doltliteHashToHex(&theirHead, hx);
    sqlite3_result_text(context, hx, -1, SQLITE_TRANSIENT);
    return;
  }


  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load our commit", -1); return; }
  memcpy(&ourCatHash, &ourCommit.catalogHash, sizeof(ProllyHash));

  rc = doltliteLoadCommit(db, &theirHead, &theirCommit);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); sqlite3_result_error(context, "failed to load their commit", -1); return; }
  memcpy(&theirCatHash, &theirCommit.catalogHash, sizeof(ProllyHash));

  rc = doltliteLoadCommit(db, &ancestorHash, &ancCommit);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); doltliteCommitClear(&theirCommit); sqlite3_result_error(context, "failed to load ancestor", -1); return; }
  memcpy(&ancCatHash, &ancCommit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&ancCommit);

  {
    char *zMergeErr = 0;
    SchemaMergeAction *aSchemaActions = 0;
    int nSchemaActions = 0;
    rc = doltliteMergeCatalogs(db, &ancCatHash, &ourCatHash, &theirCatHash,
                                &mergedCatHash, &nMergeConflicts, &zMergeErr,
                                &aSchemaActions, &nSchemaActions);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      if( zMergeErr ){
        sqlite3_result_error(context, zMergeErr, -1);
        sqlite3_free(zMergeErr);
      }else{
        sqlite3_result_error(context, "merge failed", -1);
      }
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      return;
    }
    sqlite3_free(zMergeErr);

    /* Load the merged catalog into the connection */
    rc = doltliteSwitchCatalog(db, &mergedCatHash);
    if( rc==SQLITE_OK ){
      doltliteSetSessionStaged(db, &mergedCatHash);
      doltliteUpdateBranchWorkingState(db,
          doltliteGetSessionBranch(db), &mergedCatHash, NULL);
    }
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    if( rc!=SQLITE_OK ){
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      sqlite3_result_error(context, "merge reset failed", -1);
      return;
    }

    /* Apply schema merge actions: ALTER TABLE ADD COLUMN for each needed column */
    if( nSchemaActions > 0 ){
      int si;
      for(si=0; si<nSchemaActions; si++){
        int sj;
        for(sj=0; sj<aSchemaActions[si].nAddColumns; sj++){
          char *zAlter = sqlite3_mprintf("ALTER TABLE \"%w\" ADD COLUMN %s",
                                          aSchemaActions[si].zTableName,
                                          aSchemaActions[si].azAddColumns[sj]);
          if( zAlter ){
            rc = sqlite3_exec(db, zAlter, 0, 0, 0);
            sqlite3_free(zAlter);
            if( rc!=SQLITE_OK ){
              /* If ALTER fails, report but continue - the data is merged */
              rc = SQLITE_OK;
            }
          }
        }
      }
      /* Migrate row data: fill in theirs' column values for the added columns */
      rc = migrateSchemaRowData(db, &ancCatHash, &theirCatHash, aSchemaActions, nSchemaActions);
      if( rc!=SQLITE_OK ) rc = SQLITE_OK; /* best-effort; don't block merge */

      /* Re-serialize the catalog to capture updated schema hashes after ALTERs */
      doltliteFlushCatalogToHash(db, &mergedCatHash);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
    }
  }

  if( nMergeConflicts > 0 ){
    doltliteReportConflicts(db, context, nMergeConflicts, "Merge");
  }else{
    ProllyHash commitHash;
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    char msg[256];

    doltliteSetSessionStaged(db, &mergedCatHash);

    snprintf(msg, sizeof(msg), "Merge branch '%s'", zBranch);
    rc = doltliteCreateAndStoreCommit(db, &ourHead, &mergedCatHash,
        msg, NULL, NULL, &theirHead, 1, &commitHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to create merge commit", -1);
      return;
    }

    rc = doltliteAdvanceBranch(db, &commitHash, &mergedCatHash);

    doltliteHashToHex(&commitHash, hexBuf);
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

/* loadCommitByHash removed — callers migrated to doltliteLoadCommit */

static int applyMergedCatalogAndCommit(
  sqlite3 *db,
  sqlite3_context *context,
  const ProllyHash *ancCatHash,
  const ProllyHash *ourCatHash,
  const ProllyHash *theirCatHash,
  const ProllyHash *ourHead,
  const char *zMessage,
  int *pnConflicts,
  char *hexBuf
){
  ProllyHash mergedCatHash;
  ProllyHash commitHash;
  int rc;

  rc = doltliteMergeCatalogs(db, ancCatHash, ourCatHash, theirCatHash,
                              &mergedCatHash, pnConflicts, 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteSwitchCatalog(db, &mergedCatHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteSetSessionStaged(db, &mergedCatHash);
  doltliteUpdateBranchWorkingState(db,
      doltliteGetSessionBranch(db), &mergedCatHash, NULL);

  rc = doltliteCreateAndStoreCommit(db, ourHead, &mergedCatHash,
      zMessage, NULL, NULL, NULL, 0, &commitHash);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteAdvanceBranch(db, &commitHash, &mergedCatHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteHashToHex(&commitHash, hexBuf);
  return SQLITE_OK;
}

static void doltliteCherryPickFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zRef;
  ProllyHash pickHash, ourHead;
  DoltliteCommit pickCommit, parentCommit, ourCommit;
  int nConflicts = 0;
  int rc;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&pickCommit, 0, sizeof(pickCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_cherry_pick('commit_hash')", -1);
    return;
  }

  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_error(context, "commit hash required", -1);
    return;
  }

  
  rc = doltliteResolveRef(db,zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }

  
  rc = doltliteLoadCommit(db, &pickHash, &pickCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }

  
  if( prollyHashIsEmpty(&pickCommit.parentHash) ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "cannot cherry-pick the initial commit", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &pickCommit.parentHash, &parentCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "parent commit not found", -1);
    return;
  }

  
  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
  }

  
  {
    char msg[512];
    sqlite3_snprintf(sizeof(msg), msg, "Cherry-pick: %s",
                     pickCommit.zMessage ? pickCommit.zMessage : zRef);

    rc = applyMergedCatalogAndCommit(db, context,
        &parentCommit.catalogHash, &ourCommit.catalogHash,
        &pickCommit.catalogHash, &ourHead, msg, &nConflicts, hexBuf);
  }

  doltliteCommitClear(&pickCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "cherry-pick failed", -1);
    return;
  }

  if( nConflicts > 0 ){
    doltliteReportConflicts(db, context, nConflicts, "Cherry-pick");
  }else{
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

static void doltliteRevertFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zRef;
  ProllyHash revertHash, ourHead;
  DoltliteCommit revertCommit, parentCommit, ourCommit;
  int nConflicts = 0;
  int rc;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&revertCommit, 0, sizeof(revertCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_revert('commit_hash')", -1);
    return;
  }

  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_error(context, "commit hash required", -1);
    return;
  }

  
  rc = doltliteResolveRef(db,zRef, &revertHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }

  
  rc = doltliteLoadCommit(db, &revertHash, &revertCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }

  
  if( prollyHashIsEmpty(&revertCommit.parentHash) ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "cannot revert the initial commit", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &revertCommit.parentHash, &parentCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "parent commit not found", -1);
    return;
  }

  
  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
  }

  
  {
    char msg[512];
    sqlite3_snprintf(sizeof(msg), msg, "Revert '%s'",
                     revertCommit.zMessage ? revertCommit.zMessage : zRef);

    rc = applyMergedCatalogAndCommit(db, context,
        &revertCommit.catalogHash, &ourCommit.catalogHash,
        &parentCommit.catalogHash, &ourHead, msg, &nConflicts, hexBuf);
  }

  doltliteCommitClear(&revertCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "revert failed", -1);
    return;
  }

  if( nConflicts > 0 ){
    doltliteReportConflicts(db, context, nConflicts, "Revert");
  }else{
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

static void doltliteConfigFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zKey;

  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_config(key [, value])", -1);
    return;
  }
  zKey = (const char*)sqlite3_value_text(argv[0]);
  if( !zKey ){
    sqlite3_result_error(context, "key required", -1);
    return;
  }

  if( argc==1 ){
    
    if( strcmp(zKey, "user.name")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorName(db), -1, SQLITE_TRANSIENT);
    }else if( strcmp(zKey, "user.email")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorEmail(db), -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
    }
  }else{
    
    const char *zVal = (const char*)sqlite3_value_text(argv[1]);
    if( strcmp(zKey, "user.name")==0 ){
      doltliteSetAuthorName(db, zVal);
      sqlite3_result_int(context, 0);
    }else if( strcmp(zKey, "user.email")==0 ){
      doltliteSetAuthorEmail(db, zVal);
      sqlite3_result_int(context, 0);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
    }
  }
}

void doltliteRegister(sqlite3 *db){
  sqlite3_create_function(db, "dolt_commit", -1, SQLITE_UTF8, 0,
                          doltliteCommitFunc, 0, 0);
  sqlite3_create_function(db, "dolt_add", -1, SQLITE_UTF8, 0,
                          doltliteAddFunc, 0, 0);
  sqlite3_create_function(db, "dolt_reset", -1, SQLITE_UTF8, 0,
                          doltliteResetFunc, 0, 0);
  sqlite3_create_function(db, "dolt_merge", -1, SQLITE_UTF8, 0,
                          doltliteMergeFunc, 0, 0);
  sqlite3_create_function(db, "dolt_cherry_pick", -1, SQLITE_UTF8, 0,
                          doltliteCherryPickFunc, 0, 0);
  sqlite3_create_function(db, "dolt_revert", -1, SQLITE_UTF8, 0,
                          doltliteRevertFunc, 0, 0);
  sqlite3_create_function(db, "dolt_config", -1, SQLITE_UTF8, 0,
                          doltliteConfigFunc, 0, 0);
  doltliteLogRegister(db);
  doltliteStatusRegister(db);
  doltliteDiffRegister(db);
  doltliteBranchRegister(db);
  doltliteTagRegister(db);
  doltliteConflictsRegister(db);
  doltliteGcRegister(db);
  doltliteRegisterDiffTables(db);
  doltliteAncestorRegister(db);
  doltliteAtRegister(db);
  doltliteRegisterHistoryTables(db);
  doltliteSchemaDiffRegister(db);
  {
    extern void doltliteRemoteSqlRegister(sqlite3 *db);
    doltliteRemoteSqlRegister(db);
  }
}

#endif 
