/*
** Doltlite version control features.
**
** SQL interface:
**   SELECT dolt_add('tablename');       -- stage a table
**   SELECT dolt_add('-A');              -- stage all changes
**   SELECT dolt_commit('-m', 'msg');    -- commit staged changes
**   SELECT * FROM dolt_log;             -- commit history
**   SELECT * FROM dolt_status;          -- working/staged changes
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "chunk_store.h"

#include <string.h>
#include <time.h>

/* Provided by prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);

/* Per-session branch state */
extern const char *doltliteGetSessionBranch(sqlite3 *db);
extern void doltliteSetSessionBranch(sqlite3 *db, const char *zBranch);
extern void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
extern void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead);
extern void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged);
extern void doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged);
extern void doltliteGetSessionMergeState(sqlite3 *db, u8 *pIsMerging,
                                          ProllyHash *pMergeCommit,
                                          ProllyHash *pConflictsCatalog);
extern void doltliteSetSessionMergeState(sqlite3 *db, u8 isMerging,
                                          const ProllyHash *pMergeCommit,
                                          const ProllyHash *pConflictsCatalog);
extern void doltliteClearSessionMergeState(sqlite3 *db);
extern void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash);
extern void doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash);
extern int doltliteSaveWorkingSet(sqlite3 *db);

/* TableEntry struct — must match prolly_btree.c definition */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  char *zName;
  void *pPending;
};
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

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

/* Per-session author config */
extern const char *doltliteGetAuthorName(sqlite3 *db);
extern void doltliteSetAuthorName(sqlite3 *db, const char *zName);
extern const char *doltliteGetAuthorEmail(sqlite3 *db);
extern void doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail);

/* From doltlite_ancestor.c */
extern int doltliteFindAncestor(sqlite3 *db, const ProllyHash *h1,
                                 const ProllyHash *h2, ProllyHash *pAnc);
/* From doltlite_merge.c */
extern int doltliteMergeCatalogs(sqlite3 *db, const ProllyHash *ancestor,
                                  const ProllyHash *ours, const ProllyHash *theirs,
                                  ProllyHash *pMergedHash, int *pnConflicts);
/* From prolly_btree.c */
extern int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);

/* --------------------------------------------------------------------------
** dolt_add('tablename') or dolt_add('-A')
**
** Stages table changes for the next dolt_commit.
** -------------------------------------------------------------------------- */

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

  /* Check for -A flag */
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( arg && (strcmp(arg, "-A")==0 || strcmp(arg, ".")==0) ){
      stageAll = 1;
      break;
    }
  }

  /* Flush pending mutations and serialize working catalog */
  {
    u8 *catData = 0;
    int nCatData = 0;
    ProllyHash workingHash;

    rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }
    rc = chunkStorePut(cs, catData, nCatData, &workingHash);
    sqlite3_free(catData);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }

    if( stageAll ){
      /* Stage everything: staged = working */
      doltliteSetSessionStaged(db, &workingHash);
    }else{
      /* Stage specific tables */
      struct TableEntry *aWorking = 0, *aStaged = 0;
      int nWorking = 0, nStaged = 0;
      ProllyHash stagedHash;

      /* Load working and staged catalogs */
      rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(context, "failed to load working catalog", -1);
        return;
      }

      doltliteGetSessionStaged(db, &stagedHash);
      if( prollyHashIsEmpty(&stagedHash) ){
        /* No staged state yet — start from HEAD */
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

      /* For each named table, copy its root hash from working to staged */
      for(i=0; i<argc; i++){
        const char *zTable = (const char*)sqlite3_value_text(argv[i]);
        Pgno iTable;
        int j;

        if( !zTable || zTable[0]=='-' ) continue;
        rc = doltliteResolveTableName(db, zTable, &iTable);
        if( rc!=SQLITE_OK ) continue;

        /* Find in working */
        for(j=0; j<nWorking; j++){
          if( aWorking[j].iTable==iTable ){
            /* Update or add in staged */
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
              /* Add new entry to staged */
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

      /* Re-serialize the modified staged catalog (V2 format) */
      {
        Pgno iNextTable = 2;
        /* Get iNextTable from working catalog */
        {
          u8 *wData = 0; int wn = 0;
          rc = chunkStoreGet(cs, &workingHash, &wData, &wn);
          if( rc==SQLITE_OK && wn>=5 && wData[0]==0x43 ){
            iNextTable = (Pgno)(wData[1]|(wData[2]<<8)|(wData[3]<<16)|(wData[4]<<24));
          }
          sqlite3_free(wData);
        }
        {
          int sz = 1 + 4 + 4;  /* V2: version + iNextTable + nTables */
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
          *p++ = 0x43;  /* CATALOG_FORMAT_V2 = 'C' */
          p[0]=(u8)iNextTable; p[1]=(u8)(iNextTable>>8);
          p[2]=(u8)(iNextTable>>16); p[3]=(u8)(iNextTable>>24);
          p += 4;
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

    /* Persist — if commit fails, revert session state to avoid
    ** inconsistency between in-memory session and on-disk store. */
    {
      ProllyHash savedStaged;
      doltliteGetSessionStaged(db, &savedStaged);  /* Save before commit */
      doltliteSaveWorkingSet(db);
      chunkStoreSerializeRefs(cs);
      rc = chunkStoreCommit(cs);
      if( rc!=SQLITE_OK ){
        /* Rollback session staged state to pre-commit value */
        doltliteSetSessionStaged(db, &savedStaged);
        sqlite3_result_error_code(context, rc);
        return;
      }
    }
  }

  sqlite3_result_int(context, 0);
}

/* --------------------------------------------------------------------------
** dolt_commit('-m', 'message' [, '--author', 'Name <email>'] [, '-A'])
**
** Commits the staged catalog. If -A is passed, stages everything first.
** -------------------------------------------------------------------------- */

static void doltliteCommitFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit commit;
  const char *zMessage = 0;
  const char *zAuthor = 0;
  int addAll = 0;
  u8 *commitData = 0;
  int nCommitData = 0;
  ProllyHash commitHash;
  ProllyHash catalogHash;
  ProllyHash rootHash;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  int rc;
  int i;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }

  /* Parse arguments */
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-m")==0 && i+1<argc ){
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

  /* If -A, stage everything first */
  if( addAll ){
    u8 *catData = 0;
    int nCatData = 0;
    rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }
    rc = chunkStorePut(cs, catData, nCatData, &catalogHash);
    sqlite3_free(catData);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    doltliteSetSessionStaged(db, &catalogHash);
  }

  /* Block commit while merge conflicts exist */
  {
    ProllyHash cfHash;
    doltliteGetSessionConflictsCatalog(db, &cfHash);
    if( !prollyHashIsEmpty(&cfHash) ){
      sqlite3_result_error(context,
        "cannot commit: unresolved merge conflicts. Use dolt_conflicts_resolve() first.", -1);
      return;
    }
  }

  /* Get the staged catalog hash — this is what we commit */
  doltliteGetSessionStaged(db, &catalogHash);
  if( prollyHashIsEmpty(&catalogHash) ){
    sqlite3_result_error(context,
      "nothing to commit (use dolt_add first, or dolt_commit('-A', '-m', 'msg'))", -1);
    return;
  }

  /* Check if staged == HEAD (nothing actually changed).
  ** Skip this check during a merge — committing a merge is always valid
  ** even if the resolved state matches HEAD (e.g. kept all "ours" values). */
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

  /* Build commit object — use session HEAD as parent */
  memset(&commit, 0, sizeof(commit));
  doltliteGetSessionHead(db, &commit.parentHash);
  chunkStoreGetRoot(cs, &rootHash);
  memcpy(&commit.rootHash, &rootHash, sizeof(ProllyHash));
  memcpy(&commit.catalogHash, &catalogHash, sizeof(ProllyHash));
  commit.timestamp = (i64)time(0);

  /* Parse author */
  if( zAuthor ){
    const char *lt = strchr(zAuthor, '<');
    const char *gt = lt ? strchr(lt, '>') : 0;
    if( lt && gt ){
      int nameLen = (int)(lt - zAuthor);
      while( nameLen>0 && zAuthor[nameLen-1]==' ' ) nameLen--;
      commit.zName = sqlite3_mprintf("%.*s", nameLen, zAuthor);
      commit.zEmail = sqlite3_mprintf("%.*s", (int)(gt-lt-1), lt+1);
    }else{
      commit.zName = sqlite3_mprintf("%s", zAuthor);
      commit.zEmail = sqlite3_mprintf("");
    }
  }else{
    commit.zName = sqlite3_mprintf("%s", doltliteGetAuthorName(db));
    commit.zEmail = sqlite3_mprintf("%s", doltliteGetAuthorEmail(db));
  }
  commit.zMessage = sqlite3_mprintf("%s", zMessage);

  /* Serialize and store commit */
  rc = doltliteCommitSerialize(&commit, &commitData, &nCommitData);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&commit);
    sqlite3_result_error_code(context, rc);
    return;
  }

  rc = chunkStorePut(cs, commitData, nCommitData, &commitHash);
  sqlite3_free(commitData);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  /* Update session HEAD and staged */
  doltliteSetSessionHead(db, &commitHash);
  doltliteSetSessionStaged(db, &catalogHash);

  /* Update shared state too (for manifest persistence) */
  chunkStoreSetHeadCommit(cs, &commitHash);
  doltliteSetSessionStaged(db, &catalogHash);

  /* Update branch refs — bootstrap "main" on first commit */
  {
    const char *branch = doltliteGetSessionBranch(db);
    if( cs->nBranches==0 ){
      chunkStoreAddBranch(cs, branch, &commitHash);
      chunkStoreSetDefaultBranch(cs, branch);
    }else{
      chunkStoreUpdateBranch(cs, branch, &commitHash);
    }
    chunkStoreSerializeRefs(cs);
  }

  /* Clear merge state if we were in a merge (commit concludes it) */
  {
    u8 wasMerging = 0;
    doltliteGetSessionMergeState(db, &wasMerging, 0, 0);
    if( wasMerging ){
      doltliteClearSessionMergeState(db);
    }
  }

  doltliteSaveWorkingSet(db);
  chunkStoreSerializeRefs(cs);
  rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  doltliteHashToHex(&commitHash, hexBuf);

  /* Register per-table modules for newly created tables */
  doltliteRegisterDiffTables(db);
  doltliteRegisterHistoryTables(db);
  doltliteRegisterAtTables(db);

  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}

/* --------------------------------------------------------------------------
** dolt_reset('--soft') or dolt_reset('--hard')
**
** --soft: unstage everything (staged = HEAD catalog), keep working changes
** --hard: reset working state to HEAD, discard all uncommitted changes
** -------------------------------------------------------------------------- */

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

  /* Parse args: flags (--hard, --soft) and optional commit ref */
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--hard")==0 ){ isHard = 1; }
    else if( strcmp(arg, "--soft")==0 ){ /* default */ }
    else{ zRef = arg; }
  }

  if( zRef ){
    /* Reset to a specific commit — resolve ref, load its catalog */
    ProllyHash targetCommit;
    DoltliteCommit commit;
    u8 *data = 0;
    int nData = 0;

    /* Try hex hash first, then branch, then tag */
    rc = SQLITE_NOTFOUND;
    if( zRef && strlen(zRef)==PROLLY_HASH_SIZE*2 ){
      rc = doltliteHexToHash(zRef, &targetCommit);
      if( rc==SQLITE_OK && !chunkStoreHas(cs, &targetCommit) ) rc = SQLITE_NOTFOUND;
    }
    if( rc!=SQLITE_OK ){
      rc = chunkStoreFindBranch(cs, zRef, &targetCommit);
      if( rc!=SQLITE_OK || prollyHashIsEmpty(&targetCommit) ){
        rc = chunkStoreFindTag(cs, zRef, &targetCommit);
      }
    }
    if( rc!=SQLITE_OK || prollyHashIsEmpty(&targetCommit) ){
      sqlite3_result_error(context, "commit not found", -1);
      return;
    }

    /* Load the target commit to get its catalog hash */
    rc = chunkStoreGet(cs, &targetCommit, &data, &nData);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to load commit", -1);
      return;
    }
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "corrupt commit", -1);
      return;
    }
    memcpy(&targetCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);

    /* Move HEAD and branch ref to the target commit */
    doltliteSetSessionHead(db, &targetCommit);
    chunkStoreSetHeadCommit(cs, &targetCommit);
    chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &targetCommit);
    chunkStoreSerializeRefs(cs);

    /* Clear any merge state */
    doltliteClearSessionMergeState(db);
  }else{
    /* No ref: reset to current HEAD */
    rc = doltliteGetHeadCatalogHash(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to read HEAD", -1);
      return;
    }
  }

  /* Soft reset: staged = target catalog (unstage everything) */
  doltliteSetSessionStaged(db, &targetCatHash);
  /* WorkingSet: staged already set in session */

  if( isHard ){
    /* Hard reset: also reset working state */
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
    /* Persist soft reset */
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    rc = chunkStoreCommit(cs);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  sqlite3_result_int(context, 0);
}

/* --------------------------------------------------------------------------
** dolt_merge('branch_name')
**
** Three-way merge of another branch into the current branch.
** Finds common ancestor, merges catalogs, creates merge commit.
** Fails if both branches modified the same table (conflict).
** -------------------------------------------------------------------------- */

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
  u8 *data = 0;
  int nData = 0;
  int rc;

  memset(&ourCommit, 0, sizeof(ourCommit));
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&ancCommit, 0, sizeof(ancCommit));

  /* --- Phase 1: Argument parsing and ref resolution --- */
  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(context, "usage: dolt_merge('branch')", -1); return; }

  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ sqlite3_result_error(context, "branch name required", -1); return; }

  /* Handle --abort: discard a conflicted merge, restore to HEAD */
  if( strcmp(zBranch, "--abort")==0 ){
    u8 isMerging = 0;
    ProllyHash headCatHash;

    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( !isMerging ){
      sqlite3_result_error(context, "no merge in progress", -1);
      return;
    }

    /* Reset working set back to HEAD (merge didn't commit) */
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

    /* Restore staged to HEAD catalog */
    doltliteSetSessionStaged(db, &headCatHash);
    /* WorkingSet: staged already set in session */

    /* Clear merge state and conflicts */
    doltliteClearSessionMergeState(db);
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);

    sqlite3_result_int(context, 0);
    return;
  }

  /* Get our HEAD */
  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  /* Get their HEAD */
  rc = chunkStoreFindBranch(cs, zBranch, &theirHead);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "branch not found", -1);
    return;
  }
  if( prollyHashIsEmpty(&theirHead) ){
    sqlite3_result_error(context, "target branch has no commits", -1);
    return;
  }

  /* Already up to date? */
  if( prollyHashCompare(&ourHead, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }

  /* Find common ancestor */
  rc = doltliteFindAncestor(db, &ourHead, &theirHead, &ancestorHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&ancestorHash) ){
    sqlite3_result_error(context, "no common ancestor found", -1);
    return;
  }


  /* Already up to date: their HEAD is an ancestor of ours */
  if( prollyHashCompare(&ancestorHash, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }

  /* Fast-forward: ancestor == our HEAD, create merge commit on their tip */
  if( prollyHashCompare(&ancestorHash, &ourHead)==0 ){
    rc = chunkStoreGet(cs, &theirHead, &data, &nData);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load commit", -1); return; }
    rc = doltliteCommitDeserialize(data, nData, &theirCommit);
    sqlite3_free(data); data = 0;
    if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "corrupt commit", -1); return; }

    rc = doltliteHardReset(db, &theirCommit.catalogHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&theirCommit);
      sqlite3_result_error(context, "fast-forward failed", -1);
      return;
    }

    {
      DoltliteCommit mc;
      u8 *cd2 = 0;
      int ncd2 = 0;
      ProllyHash ch2, sc2;
      char hx2[PROLLY_HASH_SIZE*2+1];
      char mg2[256];

      memcpy(&sc2, &theirCommit.catalogHash, sizeof(ProllyHash));
      doltliteCommitClear(&theirCommit);

      memset(&mc, 0, sizeof(mc));
      memcpy(&mc.parentHash, &theirHead, sizeof(ProllyHash));
      memcpy(&mc.catalogHash, &sc2, sizeof(ProllyHash));
      mc.timestamp = (i64)time(0);
      sqlite3_snprintf(sizeof(mg2), mg2, "Merge branch '%s'", zBranch);
      mc.zName = sqlite3_mprintf("%s", doltliteGetAuthorName(db));
      mc.zEmail = sqlite3_mprintf("%s", doltliteGetAuthorEmail(db));
      mc.zMessage = sqlite3_mprintf("%s", mg2);

      rc = doltliteCommitSerialize(&mc, &cd2, &ncd2);
      if( rc==SQLITE_OK ) rc = chunkStorePut(cs, cd2, ncd2, &ch2);
      sqlite3_free(cd2);
      doltliteCommitClear(&mc);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(context, "failed to create merge commit", -1);
        return;
      }

      doltliteSetSessionHead(db, &ch2);
      doltliteSetSessionStaged(db, &sc2);
      chunkStoreSetHeadCommit(cs, &ch2);
      /* WorkingSet: staged already set in session */
      chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &ch2);
      doltliteSaveWorkingSet(db);
      chunkStoreSerializeRefs(cs);
      chunkStoreCommit(cs);

      doltliteHashToHex(&ch2, hx2);
      sqlite3_result_text(context, hx2, -1, SQLITE_TRANSIENT);
      return;
    }
  }

  /* --- Phase 2: Load ancestor, ours, theirs catalogs --- */
  rc = chunkStoreGet(cs, &ourHead, &data, &nData);
  if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load our commit", -1); return; }
  rc = doltliteCommitDeserialize(data, nData, &ourCommit);
  sqlite3_free(data); data = 0;
  if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "corrupt commit", -1); return; }
  memcpy(&ourCatHash, &ourCommit.catalogHash, sizeof(ProllyHash));

  rc = chunkStoreGet(cs, &theirHead, &data, &nData);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); sqlite3_result_error(context, "failed to load their commit", -1); return; }
  rc = doltliteCommitDeserialize(data, nData, &theirCommit);
  sqlite3_free(data); data = 0;
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); sqlite3_result_error(context, "corrupt commit", -1); return; }
  memcpy(&theirCatHash, &theirCommit.catalogHash, sizeof(ProllyHash));

  rc = chunkStoreGet(cs, &ancestorHash, &data, &nData);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); doltliteCommitClear(&theirCommit); sqlite3_result_error(context, "failed to load ancestor", -1); return; }
  rc = doltliteCommitDeserialize(data, nData, &ancCommit);
  sqlite3_free(data); data = 0;
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); doltliteCommitClear(&theirCommit); sqlite3_result_error(context, "corrupt ancestor", -1); return; }
  memcpy(&ancCatHash, &ancCommit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&ancCommit);

  /* --- Phase 3: Table-level and row-level merge --- */
  rc = doltliteMergeCatalogs(db, &ancCatHash, &ourCatHash, &theirCatHash, &mergedCatHash, &nMergeConflicts);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error(context, "merge failed", -1);
    return;
  }

  /* --- Phase 4: Apply merge result and commit --- */
  rc = doltliteHardReset(db, &mergedCatHash);
  doltliteCommitClear(&ourCommit);
  doltliteCommitClear(&theirCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "merge reset failed", -1);
    return;
  }

  if( nMergeConflicts > 0 ){
    /* Conflicts: leave working set dirty, don't commit.
    ** User resolves conflicts then runs dolt_commit. */
    doltliteRegisterConflictTables(db);
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);
    {
      char msg[256];
      sqlite3_snprintf(sizeof(msg), msg,
        "Merge has %d conflict(s). Resolve and then commit with dolt_commit.",
        nMergeConflicts);
      sqlite3_result_text(context, msg, -1, SQLITE_TRANSIENT);
    }
  }else{
    /* Clean merge: create merge commit automatically */
    doltliteSetSessionStaged(db, &mergedCatHash);
    /* WorkingSet: staged already set in session */

    {
      DoltliteCommit mergeCommit;
      u8 *commitData = 0;
      int nCommitData = 0;
      ProllyHash commitHash;
      char hexBuf[PROLLY_HASH_SIZE*2+1];
      char msg[256];

      memset(&mergeCommit, 0, sizeof(mergeCommit));
      /* Merge commit has TWO parents: ours (first) and theirs (second) */
      mergeCommit.aParents[0] = ourHead;
      mergeCommit.aParents[1] = theirHead;
      mergeCommit.nParents = 2;
      mergeCommit.parentHash = ourHead;  /* convenience field */
      memcpy(&mergeCommit.catalogHash, &mergedCatHash, sizeof(ProllyHash));
      mergeCommit.timestamp = (i64)time(0);
      sqlite3_snprintf(sizeof(msg), msg, "Merge branch '%s'", zBranch);
      mergeCommit.zName = sqlite3_mprintf("%s", doltliteGetAuthorName(db));
      mergeCommit.zEmail = sqlite3_mprintf("%s", doltliteGetAuthorEmail(db));
      mergeCommit.zMessage = sqlite3_mprintf("%s", msg);

      rc = doltliteCommitSerialize(&mergeCommit, &commitData, &nCommitData);
      if( rc==SQLITE_OK ) rc = chunkStorePut(cs, commitData, nCommitData, &commitHash);
      sqlite3_free(commitData);
      doltliteCommitClear(&mergeCommit);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(context, "failed to create merge commit", -1);
        return;
      }

      doltliteSetSessionHead(db, &commitHash);
      doltliteSetSessionStaged(db, &mergedCatHash);
      chunkStoreSetHeadCommit(cs, &commitHash);
      /* WorkingSet: staged already set in session */
      chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &commitHash);
      doltliteSaveWorkingSet(db);
      chunkStoreSerializeRefs(cs);
      chunkStoreCommit(cs);

      doltliteHashToHex(&commitHash, hexBuf);
      sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
    }
  }
}

/* --------------------------------------------------------------------------
** Helper: load a commit by hash, returning its deserialized form.
** -------------------------------------------------------------------------- */
static int loadCommitByHash(
  ChunkStore *cs,
  const ProllyHash *pHash,
  DoltliteCommit *pCommit
){
  u8 *data = 0;
  int nData = 0;
  int rc;
  memset(pCommit, 0, sizeof(*pCommit));
  rc = chunkStoreGet(cs, pHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteCommitDeserialize(data, nData, pCommit);
  sqlite3_free(data);
  return rc;
}

/* --------------------------------------------------------------------------
** Helper: resolve a commit reference (40-char hex hash) to a ProllyHash.
** -------------------------------------------------------------------------- */
static int resolveCommitRef(
  ChunkStore *cs,
  const char *zRef,
  ProllyHash *pHash
){
  if( !zRef || strlen(zRef)!=40 ) return SQLITE_ERROR;
  return doltliteHexToHash(zRef, pHash);
}

/* --------------------------------------------------------------------------
** Helper: apply a three-way merge result and create a new commit.
**
** Performs doltliteMergeCatalogs, hard-resets, creates a commit object,
** and updates HEAD/branch refs.  Used by cherry-pick and revert.
**
** Returns SQLITE_OK on success; sets *pnConflicts.
** On success, writes the new commit hash hex to hexBuf (>= 41 bytes).
** -------------------------------------------------------------------------- */
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
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash mergedCatHash;
  int rc;

  rc = doltliteMergeCatalogs(db, ancCatHash, ourCatHash, theirCatHash,
                              &mergedCatHash, pnConflicts);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteHardReset(db, &mergedCatHash);
  if( rc!=SQLITE_OK ) return rc;

  /* Stage the merged catalog */
  doltliteSetSessionStaged(db, &mergedCatHash);
  /* WorkingSet: staged already set in session */

  /* Build and store the new commit */
  {
    DoltliteCommit newCommit;
    u8 *commitData = 0;
    int nCommitData = 0;
    ProllyHash commitHash;

    memset(&newCommit, 0, sizeof(newCommit));
    memcpy(&newCommit.parentHash, ourHead, sizeof(ProllyHash));
    memcpy(&newCommit.catalogHash, &mergedCatHash, sizeof(ProllyHash));
    newCommit.timestamp = (i64)time(0);
    newCommit.zName = sqlite3_mprintf("%s", doltliteGetAuthorName(db));
    newCommit.zEmail = sqlite3_mprintf("%s", doltliteGetAuthorEmail(db));
    newCommit.zMessage = sqlite3_mprintf("%s", zMessage);

    rc = doltliteCommitSerialize(&newCommit, &commitData, &nCommitData);
    if( rc==SQLITE_OK ) rc = chunkStorePut(cs, commitData, nCommitData, &commitHash);
    sqlite3_free(commitData);
    doltliteCommitClear(&newCommit);
    if( rc!=SQLITE_OK ) return rc;

    /* Update HEAD and branch ref */
    doltliteSetSessionHead(db, &commitHash);
    doltliteSetSessionStaged(db, &mergedCatHash);
    chunkStoreSetHeadCommit(cs, &commitHash);
    /* WorkingSet: staged already set in session */
    chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &commitHash);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);

    doltliteHashToHex(&commitHash, hexBuf);
  }

  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** dolt_cherry_pick('commit_hash')
**
** Apply the changes from a specific commit onto the current branch.
** This is a three-way merge where:
**   ancestor = commit's parent catalog
**   ours     = current HEAD catalog
**   theirs   = the cherry-picked commit's catalog
**
** Creates a new commit with message "Cherry-pick: <original message>".
** Returns the new commit hash, or conflict info if conflicts arise.
** -------------------------------------------------------------------------- */

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

  /* Resolve the commit hash */
  rc = resolveCommitRef(cs, zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }

  /* Load the cherry-pick commit */
  rc = loadCommitByHash(cs, &pickHash, &pickCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }

  /* Load the cherry-pick commit's parent */
  if( prollyHashIsEmpty(&pickCommit.parentHash) ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "cannot cherry-pick the initial commit", -1);
    return;
  }

  rc = loadCommitByHash(cs, &pickCommit.parentHash, &parentCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "parent commit not found", -1);
    return;
  }

  /* Load our HEAD */
  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = loadCommitByHash(cs, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
  }

  /* Three-way merge: ancestor=parent, ours=HEAD, theirs=pick */
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
    char msg[256];
    doltliteRegisterConflictTables(db);
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);
    sqlite3_snprintf(sizeof(msg), msg,
      "Cherry-pick completed with %d conflict(s). Use SELECT * FROM dolt_conflicts to view.",
      nConflicts);
    sqlite3_result_text(context, msg, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

/* --------------------------------------------------------------------------
** dolt_revert('commit_hash')
**
** Create a new commit that undoes the changes introduced by a specific
** commit.  This is a three-way merge where:
**   ancestor = the commit being reverted (its catalog)
**   ours     = current HEAD catalog
**   theirs   = the reverted commit's parent catalog
**
** Creates a new commit with message "Revert '<original message>'".
** Returns the new commit hash, or conflict info if conflicts arise.
** -------------------------------------------------------------------------- */

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

  /* Resolve the commit hash */
  rc = resolveCommitRef(cs, zRef, &revertHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }

  /* Load the commit to revert */
  rc = loadCommitByHash(cs, &revertHash, &revertCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }

  /* Load the reverted commit's parent */
  if( prollyHashIsEmpty(&revertCommit.parentHash) ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "cannot revert the initial commit", -1);
    return;
  }

  rc = loadCommitByHash(cs, &revertCommit.parentHash, &parentCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "parent commit not found", -1);
    return;
  }

  /* Load our HEAD */
  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = loadCommitByHash(cs, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
  }

  /* Three-way merge: ancestor=revert commit, ours=HEAD, theirs=parent */
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
    char msg[256];
    doltliteRegisterConflictTables(db);
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);
    sqlite3_snprintf(sizeof(msg), msg,
      "Revert completed with %d conflict(s). Use SELECT * FROM dolt_conflicts to view.",
      nConflicts);
    sqlite3_result_text(context, msg, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

/* --------------------------------------------------------------------------
** dolt_config('key' [, 'value']) — get or set per-session configuration.
**
** Supported keys:
**   user.name  — author name for commits (default: "doltlite")
**   user.email — author email for commits (default: "")
** -------------------------------------------------------------------------- */
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
    /* GET */
    if( strcmp(zKey, "user.name")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorName(db), -1, SQLITE_TRANSIENT);
    }else if( strcmp(zKey, "user.email")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorEmail(db), -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
    }
  }else{
    /* SET */
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

/* --------------------------------------------------------------------------
** Registration
** -------------------------------------------------------------------------- */

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

#endif /* DOLTLITE_PROLLY */
