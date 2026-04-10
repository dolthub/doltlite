
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include <string.h>

typedef struct RemoteMutationCtx RemoteMutationCtx;
struct RemoteMutationCtx {
  const char *zName;
  const char *zUrl;
  int isDelete;
};

typedef struct RemoteSqlState RemoteSqlState;
struct RemoteSqlState {
  u8 *pRefsBlob;
  int nRefsBlob;
  ProllyHash refsHash;
  char *zSessionBranch;
  ProllyHash sessionHead;
  ProllyHash sessionStaged;
  ProllyHash sessionMergeCommit;
  ProllyHash sessionConflictsCatalog;
  ProllyHash sessionCatalogHash;
  u8 sessionIsMerging;
};

static void remoteSqlStateClear(RemoteSqlState *p){
  sqlite3_free(p->pRefsBlob);
  sqlite3_free(p->zSessionBranch);
  memset(p, 0, sizeof(*p));
}

static int remoteSqlStateSave(sqlite3 *db, ChunkStore *cs, RemoteSqlState *p){
  u8 *pCatalog = 0;
  int nCatalog = 0;
  int rc;

  memset(p, 0, sizeof(*p));

  rc = chunkStoreSerializeRefsToBlob(cs, &p->pRefsBlob, &p->nRefsBlob);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(&p->refsHash, &cs->refsHash, sizeof(ProllyHash));

  p->zSessionBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !p->zSessionBranch ){
    remoteSqlStateClear(p);
    return SQLITE_NOMEM;
  }
  doltliteGetSessionHead(db, &p->sessionHead);
  doltliteGetSessionStaged(db, &p->sessionStaged);
  doltliteGetSessionMergeState(db, &p->sessionIsMerging,
                               &p->sessionMergeCommit,
                               &p->sessionConflictsCatalog);

  rc = doltliteFlushAndSerializeCatalog(db, &pCatalog, &nCatalog);
  if( rc!=SQLITE_OK ){
    remoteSqlStateClear(p);
    return rc;
  }
  rc = chunkStorePut(cs, pCatalog, nCatalog, &p->sessionCatalogHash);
  sqlite3_free(pCatalog);
  if( rc!=SQLITE_OK ){
    remoteSqlStateClear(p);
  }
  return rc;
}

static int remoteSqlStateRestore(sqlite3 *db, ChunkStore *cs, RemoteSqlState *p){
  int rc;

  rc = chunkStoreLoadRefsFromBlob(cs, p->pRefsBlob, p->nRefsBlob);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(&cs->refsHash, &p->refsHash, sizeof(ProllyHash));

  rc = doltliteSwitchCatalog(db, &p->sessionCatalogHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteSetSessionBranch(db, p->zSessionBranch);
  doltliteSetSessionHead(db, &p->sessionHead);
  doltliteSetSessionStaged(db, &p->sessionStaged);
  doltliteSetSessionMergeState(db, p->sessionIsMerging,
                               &p->sessionMergeCommit,
                               &p->sessionConflictsCatalog);
  return SQLITE_OK;
}

static int mutateRemoteRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  RemoteMutationCtx *p = (RemoteMutationCtx*)pArg;
  (void)db;
  if( p->isDelete ) return chunkStoreDeleteRemote(cs, p->zName);
  return chunkStoreAddRemote(cs, p->zName, p->zUrl);
}

static DoltliteRemote *openRemoteByUrl(sqlite3_vfs *pVfs, const char *zUrl){
  if( strncmp(zUrl, "file://", 7)==0 ){
    return doltliteFsRemoteOpen(pVfs, zUrl + 7);
  }
  if( strncmp(zUrl, "http://", 7)==0 ){
    return doltliteHttpRemoteOpen(zUrl);
  }
  
  return 0;
}

static void remoteSqlResultError(
  sqlite3_context *ctx,
  int rc,
  const char *zMsg
){
  if( zMsg ){
    sqlite3_result_error(ctx, zMsg, -1);
    sqlite3_result_error_code(ctx, rc);
  }else{
    sqlite3_result_error_code(ctx, rc);
  }
}

static void freeNameList(char **azNames, int nNames);

static void doltRemoteFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  RemoteMutationCtx m;
  const char *zAction;
  const char *zName;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<2 ){ sqlite3_result_error(ctx, "usage: dolt_remote(action, name [, url])", -1); return; }

  memset(&m, 0, sizeof(m));

  zAction = (const char*)sqlite3_value_text(argv[0]);
  zName = (const char*)sqlite3_value_text(argv[1]);
  if( !zAction || !zName ){
    sqlite3_result_error(ctx, "action and name required", -1);
    return;
  }

  if( strcmp(zAction, "add")==0 ){
    const char *zUrl;
    if( argc<3 ){
      sqlite3_result_error(ctx, "url required for add", -1);
      return;
    }
    zUrl = (const char*)sqlite3_value_text(argv[2]);
    if( !zUrl ){
      sqlite3_result_error(ctx, "url required for add", -1);
      return;
    }
    m.zName = zName;
    m.zUrl = zUrl;
    rc = doltliteMutateRefs(db, mutateRemoteRef, &m);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "remote already exists or error", -1);
      return;
    }
  }else if( strcmp(zAction, "remove")==0 ){
    m.zName = zName;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateRemoteRef, &m);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "remote not found", -1);
      return;
    }
  }else{
    sqlite3_result_error(ctx, "unknown action: use 'add' or 'remove'", -1);
    return;
  }

  sqlite3_result_int(ctx, 0);
}

static void doltPushFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  const char *zRemoteName;
  const char *zBranch;
  int bForce = 0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<2 ){
    sqlite3_result_error(ctx, "usage: dolt_push(remote, branch [, '--force'])", -1);
    return;
  }

  zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  zBranch = (const char*)sqlite3_value_text(argv[1]);
  if( !zRemoteName || !zBranch ){
    sqlite3_result_error(ctx, "remote and branch required", -1);
    return;
  }

  if( argc>=3 ){
    const char *zOpt = (const char*)sqlite3_value_text(argv[2]);
    if( zOpt && strcmp(zOpt, "--force")==0 ) bForce = 1;
  }

  rc = chunkStoreFindRemote(cs, zRemoteName, &zUrl);
  if( rc!=SQLITE_OK || !zUrl ){
    sqlite3_result_error(ctx, "remote not found", -1);
    return;
  }

  pRemote = openRemoteByUrl(cs->pVfs, zUrl);
  if( !pRemote ){
    sqlite3_result_error(ctx, "failed to open remote (URL must start with file://)", -1);
    return;
  }

  rc = doltlitePush(cs, pRemote, zBranch, bForce);
  pRemote->xClose(pRemote);

  if( rc!=SQLITE_OK ){
    remoteSqlResultError(ctx, rc,
      rc==SQLITE_ERROR ? "push failed (not a fast-forward?)" : 0);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

static int parseRemoteBranchNames(
  DoltliteRemote *pRemote,
  char ***pazNames,
  int *pnNames
){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;
  ChunkStore refsView;
  char **azNames = 0;
  int nNames = 0;
  int i;

  *pazNames = 0;
  *pnNames = 0;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;
  if( !refsData || nRefsData <= 0 ){
    sqlite3_free(refsData);
    return SQLITE_CORRUPT;
  }

  memset(&refsView, 0, sizeof(refsView));
  rc = chunkStoreLoadRefsFromBlob(&refsView, refsData, nRefsData);
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ){
    chunkStoreClose(&refsView);
    return rc;
  }

  if( refsView.nBranches>0 ){
    azNames = sqlite3_malloc(refsView.nBranches * sizeof(char*));
    if( !azNames ){
      chunkStoreClose(&refsView);
      return SQLITE_NOMEM;
    }
    memset(azNames, 0, refsView.nBranches * sizeof(char*));
    for(i=0; i<refsView.nBranches; i++){
      azNames[nNames] = sqlite3_mprintf("%s", refsView.aBranches[i].zName);
      if( !azNames[nNames] ){
        freeNameList(azNames, nNames);
        chunkStoreClose(&refsView);
        return SQLITE_NOMEM;
      }
      nNames++;
    }
  }
  chunkStoreClose(&refsView);
  *pazNames = azNames;
  *pnNames = nNames;
  return SQLITE_OK;
}

static void freeNameList(char **azNames, int nNames){
  int i;
  for(i=0; i<nNames; i++) sqlite3_free(azNames[i]);
  sqlite3_free(azNames);
}

static void doltFetchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  const char *zRemoteName;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(ctx, "usage: dolt_fetch(remote [, branch])", -1);
    return;
  }

  zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  if( !zRemoteName ){
    sqlite3_result_error(ctx, "remote name required", -1);
    return;
  }

  rc = chunkStoreFindRemote(cs, zRemoteName, &zUrl);
  if( rc!=SQLITE_OK || !zUrl ){
    sqlite3_result_error(ctx, "remote not found", -1);
    return;
  }

  pRemote = openRemoteByUrl(cs->pVfs, zUrl);
  if( !pRemote ){
    sqlite3_result_error(ctx, "failed to open remote (URL must start with file://)", -1);
    return;
  }

  if( argc>=2 && sqlite3_value_type(argv[1])!=SQLITE_NULL ){
    
    const char *zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( !zBranch ){
      pRemote->xClose(pRemote);
      sqlite3_result_error(ctx, "branch name required", -1);
      return;
    }
    rc = doltliteFetch(cs, pRemote, zRemoteName, zBranch);
    if( rc!=SQLITE_OK ){
      pRemote->xClose(pRemote);
      remoteSqlResultError(ctx, rc,
        rc==SQLITE_NOTFOUND ? "fetch failed: branch not found on remote" : 0);
      return;
    }
  }else{
    
    char **azNames = 0;
    int nNames = 0;
    int i;

    rc = parseRemoteBranchNames(pRemote, &azNames, &nNames);
    if( rc!=SQLITE_OK ){
      pRemote->xClose(pRemote);
      sqlite3_result_error(ctx, "failed to read remote refs", -1);
      return;
    }

    
    pRemote->xClose(pRemote);
    pRemote = 0;

    for(i=0; i<nNames; i++){
      DoltliteRemote *pBrRemote = openRemoteByUrl(cs->pVfs, zUrl);
      if( !pBrRemote ){
        freeNameList(azNames, nNames);
        sqlite3_result_error(ctx, "failed to open remote (URL must start with file://)", -1);
        return;
      }
      rc = doltliteFetch(cs, pBrRemote, zRemoteName, azNames[i]);
      pBrRemote->xClose(pBrRemote);
      if( rc!=SQLITE_OK ){
        freeNameList(azNames, nNames);
        remoteSqlResultError(ctx, rc, "fetch failed");
        return;
      }
    }
    freeNameList(azNames, nNames);
  }

  if( pRemote ) pRemote->xClose(pRemote);
  sqlite3_result_int(ctx, 0);
}

static void doltPullFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  const char *zRemoteName;
  const char *zBranch;
  ProllyHash trackingCommit, localCommit;
  RemoteSqlState savedState;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<2 ){
    sqlite3_result_error(ctx, "usage: dolt_pull(remote, branch)", -1);
    return;
  }

  zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  zBranch = (const char*)sqlite3_value_text(argv[1]);
  if( !zRemoteName || !zBranch ){
    sqlite3_result_error(ctx, "remote and branch required", -1);
    return;
  }
  memset(&savedState, 0, sizeof(savedState));

  rc = remoteSqlStateSave(db, cs, &savedState);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  
  rc = chunkStoreFindRemote(cs, zRemoteName, &zUrl);
  if( rc!=SQLITE_OK || !zUrl ){
    remoteSqlStateClear(&savedState);
    sqlite3_result_error(ctx, "remote not found", -1);
    return;
  }

  pRemote = openRemoteByUrl(cs->pVfs, zUrl);
  if( !pRemote ){
    remoteSqlStateClear(&savedState);
    sqlite3_result_error(ctx, "failed to open remote (URL must start with file://)", -1);
    return;
  }

  rc = doltliteFetch(cs, pRemote, zRemoteName, zBranch);
  pRemote->xClose(pRemote);
  if( rc!=SQLITE_OK ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    remoteSqlResultError(ctx, rc,
      rc==SQLITE_NOTFOUND ? "fetch failed: branch not found on remote" : 0);
    return;
  }

  
  rc = chunkStoreFindTracking(cs, zRemoteName, zBranch, &trackingCommit);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&trackingCommit) ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error(ctx, "tracking branch not found after fetch", -1);
    return;
  }

  
  rc = chunkStoreFindBranch(cs, zBranch, &localCommit);
  if( rc!=SQLITE_OK ){
    
    rc = chunkStoreAddBranch(cs, zBranch, &trackingCommit);
    if( rc!=SQLITE_OK ){
      rc = remoteSqlStateRestore(db, cs, &savedState);
      remoteSqlStateClear(&savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_error(ctx, "failed to create local branch", -1);
      return;
    }
    localCommit = trackingCommit;
  }

  
  if( prollyHashCompare(&localCommit, &trackingCommit)==0 ){
    remoteSqlStateClear(&savedState);
    sqlite3_result_int(ctx, 0); 
    return;
  }

  
  {
    ProllyHash walk;
    int maxDepth = 1000;
    int canFF = 0;
    int walkRc = SQLITE_OK;
    memcpy(&walk, &trackingCommit, sizeof(ProllyHash));

    while( maxDepth-- > 0 ){
      DoltliteCommit commit;

      if( prollyHashCompare(&walk, &localCommit)==0 ){
        canFF = 1;
        break;
      }
      if( prollyHashIsEmpty(&walk) ) break;

      memset(&commit, 0, sizeof(commit));
      walkRc = doltliteLoadCommit(db, &walk, &commit);
      if( walkRc!=SQLITE_OK ) break;

      if( commit.nParents > 0 ){
        memcpy(&walk, &commit.aParents[0], sizeof(ProllyHash));
      }else{
        memset(&walk, 0, sizeof(ProllyHash));
      }
      doltliteCommitClear(&commit);
    }

    if( walkRc!=SQLITE_OK ){
      int restoreRc = remoteSqlStateRestore(db, cs, &savedState);
      remoteSqlStateClear(&savedState);
      if( restoreRc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, restoreRc);
        return;
      }
      sqlite3_result_error_code(ctx, walkRc);
      return;
    }

    if( !canFF ){
      rc = remoteSqlStateRestore(db, cs, &savedState);
      remoteSqlStateClear(&savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_error(ctx,
        "cannot fast-forward — use dolt_merge with the tracking branch instead", -1);
      return;
    }
  }

  
  rc = chunkStoreUpdateBranch(cs, zBranch, &trackingCommit);
  if( rc!=SQLITE_OK ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error(ctx, "failed to update branch", -1);
    return;
  }

  
  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    DoltliteCommit commit;
    u8 *data = 0; int nData = 0;

    rc = chunkStoreGet(cs, &trackingCommit, &data, &nData);
    if( rc!=SQLITE_OK ){
      rc = remoteSqlStateRestore(db, cs, &savedState);
      remoteSqlStateClear(&savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_error(ctx, "failed to load commit", -1);
      return;
    }
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    if( rc!=SQLITE_OK ){
      rc = remoteSqlStateRestore(db, cs, &savedState);
      remoteSqlStateClear(&savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_error(ctx, "failed to deserialize commit", -1);
      return;
    }

    rc = doltliteHardReset(db, &commit.catalogHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      rc = remoteSqlStateRestore(db, cs, &savedState);
      remoteSqlStateClear(&savedState);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return;
      }
      sqlite3_result_error(ctx, "hard reset failed", -1);
      return;
    }

    doltliteSetSessionHead(db, &trackingCommit);
    doltliteSetSessionStaged(db, &commit.catalogHash);
    doltliteCommitClear(&commit);
  }

  
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  remoteSqlStateClear(&savedState);
  sqlite3_result_int(ctx, 0);
}

static void doltCloneFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl;
  RemoteSqlState savedState;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(ctx, "usage: dolt_clone(url)", -1);
    return;
  }

  zUrl = (const char*)sqlite3_value_text(argv[0]);
  if( !zUrl ){
    sqlite3_result_error(ctx, "url required", -1);
    return;
  }
  memset(&savedState, 0, sizeof(savedState));

  
  /* Allow cloning over a pristine, auto-seeded repository: a single branch
  ** whose head commit is a root (empty parent). Any real commit descends
  ** from the seed and thus has a non-empty parent. */
  if( !chunkStoreIsEmpty(cs) ){
    int virgin = 0;
    if( cs->nBranches==1 ){
      DoltliteCommit c;
      memset(&c, 0, sizeof(c));
      if( doltliteLoadCommit(db, &cs->aBranches[0].commitHash, &c)==SQLITE_OK
       && prollyHashIsEmpty(&c.parentHash) ){
        virgin = 1;
      }
      doltliteCommitClear(&c);
    }
    if( !virgin ){
      sqlite3_result_error(ctx, "database is not empty — clone into a fresh database", -1);
      return;
    }
  }

  rc = remoteSqlStateSave(db, cs, &savedState);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  if( !chunkStoreIsEmpty(cs) ){
    chunkStoreClearRefs(cs);
  }

  pRemote = openRemoteByUrl(cs->pVfs, zUrl);
  if( !pRemote ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error(ctx, "failed to open remote (URL must start with file://)", -1);
    return;
  }

  rc = doltliteClone(cs, pRemote);
  pRemote->xClose(pRemote);
  if( rc!=SQLITE_OK ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error(ctx, "clone failed", -1);
    return;
  }

  
  rc = chunkStoreAddRemote(cs, "origin", zUrl);
  if( rc!=SQLITE_OK ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error(ctx, "failed to add origin remote", -1);
    return;
  }
  

  
  {
    u8 *refsData = 0; int nRefsData = 0;
    ProllyHash refsHash;
    memcpy(&refsHash, &cs->refsHash, sizeof(ProllyHash));
    if( !prollyHashIsEmpty(&refsHash) ){
      rc = chunkStoreGet(cs, &refsHash, &refsData, &nRefsData);
      (void)refsData; 
      sqlite3_free(refsData);
    }
  }

  
  {
    const char *zDefault = chunkStoreGetDefaultBranch(cs);
    ProllyHash branchCommit;

    if( !zDefault && cs->nBranches > 0 ){
      zDefault = cs->aBranches[0].zName;
    }

    if( zDefault ){
      rc = chunkStoreFindBranch(cs, zDefault, &branchCommit);
      if( rc!=SQLITE_OK || prollyHashIsEmpty(&branchCommit) ){
        rc = remoteSqlStateRestore(db, cs, &savedState);
        remoteSqlStateClear(&savedState);
        if( rc!=SQLITE_OK ){
          sqlite3_result_error_code(ctx, rc);
          return;
        }
        sqlite3_result_error(ctx, "default branch missing from cloned refs", -1);
        return;
      }
      {
        
        u8 *data = 0; int nData = 0;
        DoltliteCommit commit;

        rc = chunkStoreGet(cs, &branchCommit, &data, &nData);
        if( rc!=SQLITE_OK || !data ){
          if( data ) sqlite3_free(data);
          rc = remoteSqlStateRestore(db, cs, &savedState);
          remoteSqlStateClear(&savedState);
          if( rc!=SQLITE_OK ){
            sqlite3_result_error_code(ctx, rc);
            return;
          }
          sqlite3_result_error(ctx, "failed to load default branch commit", -1);
          return;
        }
        rc = doltliteCommitDeserialize(data, nData, &commit);
        sqlite3_free(data);
        if( rc!=SQLITE_OK ){
          rc = remoteSqlStateRestore(db, cs, &savedState);
          remoteSqlStateClear(&savedState);
          if( rc!=SQLITE_OK ){
            sqlite3_result_error_code(ctx, rc);
            return;
          }
          sqlite3_result_error(ctx, "failed to deserialize default branch commit", -1);
          return;
        }
        rc = doltliteHardReset(db, &commit.catalogHash);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&commit);
          rc = remoteSqlStateRestore(db, cs, &savedState);
          remoteSqlStateClear(&savedState);
          if( rc!=SQLITE_OK ){
            sqlite3_result_error_code(ctx, rc);
            return;
          }
          sqlite3_result_error(ctx, "failed to initialize working tree from default branch", -1);
          return;
        }
        doltliteSetSessionBranch(db, zDefault);
        doltliteSetSessionHead(db, &branchCommit);
        doltliteSetSessionStaged(db, &commit.catalogHash);
        rc = chunkStoreSetDefaultBranch(cs, zDefault);
        doltliteCommitClear(&commit);
        if( rc!=SQLITE_OK ){
          rc = remoteSqlStateRestore(db, cs, &savedState);
          remoteSqlStateClear(&savedState);
          if( rc!=SQLITE_OK ){
            sqlite3_result_error_code(ctx, rc);
            return;
          }
          sqlite3_result_error(ctx, "failed to record default branch", -1);
          return;
        }
      }
    }
  }

  
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    rc = remoteSqlStateRestore(db, cs, &savedState);
    remoteSqlStateClear(&savedState);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  remoteSqlStateClear(&savedState);
  sqlite3_result_int(ctx, 0);
}

typedef struct RemVtab RemVtab;
struct RemVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct RemCur RemCur;
struct RemCur { sqlite3_vtab_cursor base; int iRow; };

static int remConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  RemVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x("
      "name TEXT, "
      "url TEXT, "
      "fetch_specs TEXT, "
      "params TEXT"
    ")");
  if( rc!=SQLITE_OK ) return rc;
  p = sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p)); p->db = db;
  *ppVtab = &p->base;
  return SQLITE_OK;
}
static int remDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int remOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  RemCur *c = sqlite3_malloc(sizeof(*c)); (void)v;
  if(!c) return SQLITE_NOMEM; memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}
static int remClose(sqlite3_vtab_cursor *c){ sqlite3_free(c); return SQLITE_OK; }
static int remFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  (void)n;(void)s;(void)a;(void)v;
  ((RemCur*)c)->iRow = 0; return SQLITE_OK;
}
static int remNext(sqlite3_vtab_cursor *c){ ((RemCur*)c)->iRow++; return SQLITE_OK; }
static int remEof(sqlite3_vtab_cursor *c){
  RemVtab *v = (RemVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  return !cs || ((RemCur*)c)->iRow >= cs->nRemotes;
}
static int remColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  RemVtab *v = (RemVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  struct RemoteRef *rem;
  if(!cs) return SQLITE_OK;
  rem = &cs->aRemotes[((RemCur*)c)->iRow];
  switch(col){
    case 0:
      sqlite3_result_text(ctx, rem->zName, -1, SQLITE_TRANSIENT);
      break;
    case 1:
      sqlite3_result_text(ctx, rem->zUrl, -1, SQLITE_TRANSIENT);
      break;
    case 2: {
      /* Default fetch refspec derived from the remote name, matching the
      ** shape Dolt emits for a vanilla dolt_remote add call. doltlite has
      ** no concept of customizable refspecs, so this is always derived. */
      char *zSpec = sqlite3_mprintf(
          "[\"refs/heads/*:refs/remotes/%s/*\"]", rem->zName);
      if( zSpec ){
        sqlite3_result_text(ctx, zSpec, -1, SQLITE_TRANSIENT);
        sqlite3_free(zSpec);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 3:
      /* Dolt uses params for auth/config overrides; doltlite has none,
      ** so this is always an empty JSON object. */
      sqlite3_result_text(ctx, "{}", -1, SQLITE_STATIC);
      break;
  }
  return SQLITE_OK;
}
static int remRowid(sqlite3_vtab_cursor *c, sqlite3_int64 *r){
  *r=((RemCur*)c)->iRow; return SQLITE_OK;
}
static int remBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v; p->estimatedCost=10; p->estimatedRows=5; return SQLITE_OK;
}

static sqlite3_module remotesModule = {
  0,0,remConnect,remBestIndex,remDisconnect,0,
  remOpen,remClose,remFilter,remNext,remEof,remColumn,remRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

void doltliteRemoteSqlRegister(sqlite3 *db){
  sqlite3_create_function(db, "dolt_remote", -1, SQLITE_UTF8, 0,
                          doltRemoteFunc, 0, 0);
  sqlite3_create_function(db, "dolt_push", -1, SQLITE_UTF8, 0,
                          doltPushFunc, 0, 0);
  sqlite3_create_function(db, "dolt_fetch", -1, SQLITE_UTF8, 0,
                          doltFetchFunc, 0, 0);
  sqlite3_create_function(db, "dolt_pull", -1, SQLITE_UTF8, 0,
                          doltPullFunc, 0, 0);
  sqlite3_create_function(db, "dolt_clone", -1, SQLITE_UTF8, 0,
                          doltCloneFunc, 0, 0);
  sqlite3_create_module(db, "dolt_remotes", &remotesModule, 0);
}

#endif 
