
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>
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

extern int doltliteMergeCatalogs(sqlite3 *db, const ProllyHash *ancestor,
                                  const ProllyHash *ours, const ProllyHash *theirs,
                                  ProllyHash *pMergedHash, int *pnConflicts);

extern const char *doltliteNextTableForSchema(sqlite3 *db, int *pIdx, Pgno *piTable);
extern void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH);

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
        Pgno iNextTable = 2;
        
        {
          u8 *wData = 0; int wn = 0;
          rc = chunkStoreGet(cs, &workingHash, &wData, &wn);
          if( rc==SQLITE_OK && wn>=5 && wData[0]==0x43 ){
            iNextTable = (Pgno)(wData[1]|(wData[2]<<8)|(wData[3]<<16)|(wData[4]<<24));
          }
          sqlite3_free(wData);
        }
        {
          int sz = 1 + 4 + 4;  
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
          *p++ = 0x43;  
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

    
    {
      ProllyHash savedStaged;
      doltliteGetSessionStaged(db, &savedStaged);  
      doltliteSaveWorkingSet(db);
      chunkStoreSerializeRefs(cs);
      rc = chunkStoreCommit(cs);
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

  /* Optimistic concurrency: reject if another connection committed to this
  ** branch since our session last read it. Session HEAD was captured at
  ** checkout/open; if it no longer matches the branch tip in shared refs,
  ** someone else committed and our staged catalog is based on stale state. */
  {
    const char *branch = doltliteGetSessionBranch(db);
    ProllyHash branchTip;
    ProllyHash sessionHead;
    doltliteGetSessionHead(db, &sessionHead);
    if( chunkStoreFindBranch(cs, branch, &branchTip)==SQLITE_OK ){
      if( prollyHashCompare(&sessionHead, &branchTip)!=0 ){
        sqlite3_result_error(context,
          "commit conflict: another connection committed to this branch. "
          "Please retry your transaction.", -1);
        return;
      }
    }
  }

  
  memset(&commit, 0, sizeof(commit));
  doltliteGetSessionHead(db, &commit.parentHash);
  chunkStoreGetRoot(cs, &rootHash);
  memcpy(&commit.rootHash, &rootHash, sizeof(ProllyHash));
  memcpy(&commit.catalogHash, &catalogHash, sizeof(ProllyHash));
  commit.timestamp = (i64)time(0);

  
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

  
  doltliteSetSessionHead(db, &commitHash);
  doltliteSetSessionStaged(db, &catalogHash);

  
  chunkStoreSetHeadCommit(cs, &commitHash);
  doltliteSetSessionStaged(db, &catalogHash);

  
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
  if( rc==SQLITE_BUSY_SNAPSHOT ){
    sqlite3_result_error(context,
      "commit conflict: another connection modified the database. "
      "Please retry your transaction.", -1);
    return;
  }
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
    u8 *data = 0;
    int nData = 0;

    
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

    
    doltliteSetSessionHead(db, &targetCommit);
    chunkStoreSetHeadCommit(cs, &targetCommit);
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
    doltliteSaveWorkingSet(db);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);

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
      
      chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &ch2);
      doltliteSaveWorkingSet(db);
      chunkStoreSerializeRefs(cs);
      chunkStoreCommit(cs);

      doltliteHashToHex(&ch2, hx2);
      sqlite3_result_text(context, hx2, -1, SQLITE_TRANSIENT);
      return;
    }
  }

  
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

  
  rc = doltliteMergeCatalogs(db, &ancCatHash, &ourCatHash, &theirCatHash, &mergedCatHash, &nMergeConflicts);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    sqlite3_result_error(context, "merge failed", -1);
    return;
  }

  
  rc = doltliteHardReset(db, &mergedCatHash);
  doltliteCommitClear(&ourCommit);
  doltliteCommitClear(&theirCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "merge reset failed", -1);
    return;
  }

  if( nMergeConflicts > 0 ){
    
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
    
    doltliteSetSessionStaged(db, &mergedCatHash);
    

    {
      DoltliteCommit mergeCommit;
      u8 *commitData = 0;
      int nCommitData = 0;
      ProllyHash commitHash;
      char hexBuf[PROLLY_HASH_SIZE*2+1];
      char msg[256];

      memset(&mergeCommit, 0, sizeof(mergeCommit));
      /* Merge commit has two parents: ours (index 0) and theirs (index 1) */
      mergeCommit.aParents[0] = ourHead;
      mergeCommit.aParents[1] = theirHead;
      mergeCommit.nParents = 2;
      mergeCommit.parentHash = ourHead;  
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
      
      chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &commitHash);
      doltliteSaveWorkingSet(db);
      chunkStoreSerializeRefs(cs);
      chunkStoreCommit(cs);

      doltliteHashToHex(&commitHash, hexBuf);
      sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
    }
  }
}

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

static int resolveCommitRef(
  ChunkStore *cs,
  const char *zRef,
  ProllyHash *pHash
){
  if( !zRef || strlen(zRef)!=40 ) return SQLITE_ERROR;
  return doltliteHexToHash(zRef, pHash);
}

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

  
  doltliteSetSessionStaged(db, &mergedCatHash);
  

  
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

    
    doltliteSetSessionHead(db, &commitHash);
    doltliteSetSessionStaged(db, &mergedCatHash);
    chunkStoreSetHeadCommit(cs, &commitHash);
    
    chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &commitHash);
    chunkStoreSerializeRefs(cs);
    chunkStoreCommit(cs);

    doltliteHashToHex(&commitHash, hexBuf);
  }

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

  
  rc = resolveCommitRef(cs, zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }

  
  rc = loadCommitByHash(cs, &pickHash, &pickCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }

  
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

  
  rc = resolveCommitRef(cs, zRef, &revertHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }

  
  rc = loadCommitByHash(cs, &revertHash, &revertCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }

  
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
