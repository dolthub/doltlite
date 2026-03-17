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

/* TableEntry struct — must match prolly_btree.c definition */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  u8 flags;
};
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

extern int doltliteLogRegister(sqlite3 *db);
extern int doltliteStatusRegister(sqlite3 *db);
extern int doltliteDiffRegister(sqlite3 *db);
extern int doltliteBranchRegister(sqlite3 *db);
extern int doltliteAncestorRegister(sqlite3 *db);

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
      doltliteSetSessionStaged(db, &workingHash); chunkStoreSetStagedCatalog(cs, &workingHash);
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
        int j, found;

        if( !zTable || zTable[0]=='-' ) continue;
        rc = doltliteResolveTableName(db, zTable, &iTable);
        if( rc!=SQLITE_OK ) continue;

        /* Find in working */
        found = 0;
        for(j=0; j<nWorking; j++){
          if( aWorking[j].iTable==iTable ){
            /* Update or add in staged */
            int k;
            int updated = 0;
            for(k=0; k<nStaged; k++){
              if( aStaged[k].iTable==iTable ){
                aStaged[k].root = aWorking[j].root;
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
            found = 1;
            break;
          }
        }
        (void)found;
      }

      /* Re-serialize the modified staged catalog */
      {
        /* Build a minimal catalog blob: iNextTable + nTables + meta[16] + entries */
        /* We need iNextTable and meta from working. Load them. */
        Pgno iNextTable = 2;
        u32 aMeta[16];
        memset(aMeta, 0, sizeof(aMeta));

        /* Get meta from working state (it's the current BtShared meta) */
        /* For simplicity, re-serialize from the staged entries with working's meta */
        {
          int sz = 4 + 4 + 64 + nStaged * 25;
          u8 *buf = sqlite3_malloc(sz);
          u8 *p;
          ProllyHash newStagedHash;

          if( !buf ){
            sqlite3_free(aWorking);
            sqlite3_free(aStaged);
            sqlite3_result_error(context, "out of memory", -1);
            return;
          }
          p = buf;
          /* Copy iNextTable and meta from working catalog chunk */
          {
            u8 *wData = 0; int wn = 0;
            rc = chunkStoreGet(cs, &workingHash, &wData, &wn);
            if( rc==SQLITE_OK && wn>=72 ){
              memcpy(p, wData, 72); /* iNextTable(4) + nTables(4) + meta(64) */
            }else{
              memset(p, 0, 72);
            }
            sqlite3_free(wData);
          }
          /* Override nTables */
          p[4]=(u8)nStaged; p[5]=(u8)(nStaged>>8);
          p[6]=(u8)(nStaged>>16); p[7]=(u8)(nStaged>>24);
          p += 72;

          /* Write table entries */
          for(i=0; i<nStaged; i++){
            Pgno pg = aStaged[i].iTable;
            p[0]=(u8)pg; p[1]=(u8)(pg>>8); p[2]=(u8)(pg>>16); p[3]=(u8)(pg>>24);
            p += 4;
            *p++ = aStaged[i].flags;
            memcpy(p, aStaged[i].root.data, PROLLY_HASH_SIZE);
            p += PROLLY_HASH_SIZE;
          }

          rc = chunkStorePut(cs, buf, sz, &newStagedHash);
          sqlite3_free(buf);
          if( rc==SQLITE_OK ){
            doltliteSetSessionStaged(db, &newStagedHash); chunkStoreSetStagedCatalog(cs, &newStagedHash);
          }
        }
      }

      sqlite3_free(aWorking);
      sqlite3_free(aStaged);
    }

    /* Persist */
    rc = chunkStoreCommit(cs);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
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
    doltliteSetSessionStaged(db, &catalogHash); chunkStoreSetStagedCatalog(cs, &catalogHash);
  }

  /* Get the staged catalog hash — this is what we commit */
  doltliteGetSessionStaged(db, &catalogHash);
  if( prollyHashIsEmpty(&catalogHash) ){
    sqlite3_result_error(context,
      "nothing to commit (use dolt_add first, or dolt_commit('-A', '-m', 'msg'))", -1);
    return;
  }

  /* Check if staged == HEAD (nothing actually changed) */
  {
    ProllyHash headCatHash;
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash)
     && prollyHashCompare(&catalogHash, &headCatHash)==0 ){
      sqlite3_result_error(context,
        "nothing to commit, working tree clean (use dolt_add to stage changes)", -1);
      return;
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
    commit.zName = sqlite3_mprintf("doltlite");
    commit.zEmail = sqlite3_mprintf("");
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
  doltliteSetSessionStaged(db, &catalogHash); chunkStoreSetStagedCatalog(cs, &catalogHash);

  /* Update shared state too (for manifest persistence) */
  chunkStoreSetHeadCommit(cs, &commitHash);
  doltliteSetSessionStaged(db, &catalogHash); chunkStoreSetStagedCatalog(cs, &catalogHash);

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

  rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  doltliteHashToHex(&commitHash, hexBuf);
  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}

/* --------------------------------------------------------------------------
** dolt_reset('--soft') or dolt_reset('--hard')
**
** --soft: unstage everything (staged = HEAD catalog), keep working changes
** --hard: reset working state to HEAD, discard all uncommitted changes
** -------------------------------------------------------------------------- */

extern int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);

static void doltliteResetFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCatHash;
  int isHard = 0;
  int rc;
  int i;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }

  /* Parse args */
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( arg && strcmp(arg, "--hard")==0 ) isHard = 1;
  }

  /* Get HEAD catalog hash */
  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to read HEAD", -1);
    return;
  }

  /* Soft reset: staged = HEAD catalog (unstage everything) */
  doltliteSetSessionStaged(db, &headCatHash); chunkStoreSetStagedCatalog(cs, &headCatHash);

  if( isHard ){
    /* Hard reset: also reset working state to HEAD */
    if( prollyHashIsEmpty(&headCatHash) ){
      sqlite3_result_error(context, "no HEAD commit to reset to", -1);
      return;
    }
    rc = doltliteHardReset(db, &headCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "hard reset failed", -1);
      return;
    }
  }else{
    /* Persist soft reset (just the staged catalog change) */
    rc = chunkStoreCommit(cs);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  sqlite3_result_int(context, 0);
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
  doltliteLogRegister(db);
  doltliteStatusRegister(db);
  doltliteDiffRegister(db);
  doltliteBranchRegister(db);
  doltliteAncestorRegister(db);
}

#endif /* DOLTLITE_PROLLY */
