/*
** SQL function interface for doltlite remotes: push, fetch, pull, clone.
**
**   SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');
**   SELECT dolt_remote('remove', 'origin');
**   SELECT dolt_push('origin', 'main');
**   SELECT dolt_fetch('origin');
**   SELECT dolt_fetch('origin', 'main');
**   SELECT dolt_pull('origin', 'main');
**   SELECT dolt_clone('file:///path/to/source.doltlite');
**   SELECT * FROM dolt_remotes;
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);

/* Per-session branch state */
extern const char *doltliteGetSessionBranch(sqlite3 *db);
extern void doltliteSetSessionBranch(sqlite3 *db, const char *zBranch);
extern void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
extern void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead);
extern void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged);
extern void doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged);

/*
** Open a remote by URL. Dispatches based on scheme:
**   file:///path  or  file://relative/path  →  filesystem remote
**   (future: https://... → HTTP remote)
** Returns NULL on error. Caller must close via pRemote->xClose().
*/
static DoltliteRemote *openRemoteByUrl(sqlite3_vfs *pVfs, const char *zUrl){
  if( strncmp(zUrl, "file://", 7)==0 ){
    return doltliteFsRemoteOpen(pVfs, zUrl + 7);
  }
  if( strncmp(zUrl, "http://", 7)==0 ){
    return doltliteHttpRemoteOpen(zUrl);
  }
  /* No recognized scheme */
  return 0;
}

/* --------------------------------------------------------------------------
** dolt_remote('add'|'remove', name [, url]) — manage remotes
** -------------------------------------------------------------------------- */
static void doltRemoteFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<2 ){ sqlite3_result_error(ctx, "usage: dolt_remote(action, name [, url])", -1); return; }

  const char *zAction = (const char*)sqlite3_value_text(argv[0]);
  const char *zName = (const char*)sqlite3_value_text(argv[1]);
  if( !zAction || !zName ){
    sqlite3_result_error(ctx, "action and name required", -1);
    return;
  }

  if( strcmp(zAction, "add")==0 ){
    if( argc<3 ){
      sqlite3_result_error(ctx, "url required for add", -1);
      return;
    }
    const char *zUrl = (const char*)sqlite3_value_text(argv[2]);
    if( !zUrl ){
      sqlite3_result_error(ctx, "url required for add", -1);
      return;
    }
    rc = chunkStoreAddRemote(cs, zName, zUrl);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "remote already exists or error", -1);
      return;
    }
  }else if( strcmp(zAction, "remove")==0 ){
    rc = chunkStoreDeleteRemote(cs, zName);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "remote not found", -1);
      return;
    }
  }else{
    sqlite3_result_error(ctx, "unknown action: use 'add' or 'remove'", -1);
    return;
  }

  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){ sqlite3_result_error_code(ctx, rc); return; }
  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_push('remote', 'branch' [, '--force']) — push branch to remote
** -------------------------------------------------------------------------- */
static void doltPushFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  int bForce = 0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<2 ){
    sqlite3_result_error(ctx, "usage: dolt_push(remote, branch [, '--force'])", -1);
    return;
  }

  const char *zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  const char *zBranch = (const char*)sqlite3_value_text(argv[1]);
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
    sqlite3_result_error(ctx, "push failed (not a fast-forward?)", -1);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** Helper: parse remote refs to get branch names for fetch-all.
** Returns an array of branch name strings (caller frees each + array).
** -------------------------------------------------------------------------- */
static int parseRemoteBranchNames(
  DoltliteRemote *pRemote,
  char ***pazNames,
  int *pnNames
){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;
  char **azNames = 0;
  int nNames = 0;

  *pazNames = 0;
  *pnNames = 0;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;
  if( !refsData || nRefsData < 5 ){
    sqlite3_free(refsData);
    return SQLITE_OK; /* no refs, no branches */
  }

  {
    const u8 *p = refsData;
    u8 ver = p[0]; p++;
    int defLen = p[0]|(p[1]<<8); p += 2;
    if( p + defLen + 2 <= refsData + nRefsData ){
      int nBranches, i;
      p += defLen;
      nBranches = p[0]|(p[1]<<8); p += 2;

      azNames = sqlite3_malloc(nBranches * sizeof(char*));
      if( !azNames ){
        sqlite3_free(refsData);
        return SQLITE_NOMEM;
      }
      memset(azNames, 0, nBranches * sizeof(char*));

      for(i=0; i<nBranches; i++){
        int nameLen;
        if( p+2 > refsData+nRefsData ) break;
        nameLen = p[0]|(p[1]<<8); p += 2;
        if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
        azNames[nNames] = sqlite3_malloc(nameLen+1);
        if( azNames[nNames] ){
          memcpy(azNames[nNames], p, nameLen);
          azNames[nNames][nameLen] = 0;
          nNames++;
        }
        p += nameLen + PROLLY_HASH_SIZE;
        /* Skip workingSetHash for v3+ */
        if( ver >= 3 && p+PROLLY_HASH_SIZE <= refsData+nRefsData ){
          p += PROLLY_HASH_SIZE;
        }
      }
    }
  }

  sqlite3_free(refsData);
  *pazNames = azNames;
  *pnNames = nNames;
  return SQLITE_OK;
}

static void freeNameList(char **azNames, int nNames){
  int i;
  for(i=0; i<nNames; i++) sqlite3_free(azNames[i]);
  sqlite3_free(azNames);
}

/* --------------------------------------------------------------------------
** dolt_fetch('remote' [, 'branch']) — fetch from remote
** -------------------------------------------------------------------------- */
static void doltFetchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(ctx, "usage: dolt_fetch(remote [, branch])", -1);
    return;
  }

  const char *zRemoteName = (const char*)sqlite3_value_text(argv[0]);
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
    /* Fetch a specific branch */
    const char *zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( !zBranch ){
      pRemote->xClose(pRemote);
      sqlite3_result_error(ctx, "branch name required", -1);
      return;
    }
    rc = doltliteFetch(cs, pRemote, zRemoteName, zBranch);
    if( rc!=SQLITE_OK ){
      pRemote->xClose(pRemote);
      sqlite3_result_error(ctx, "fetch failed: branch not found on remote", -1);
      return;
    }
  }else{
    /* Fetch all branches from remote */
    char **azNames = 0;
    int nNames = 0;
    int i;

    rc = parseRemoteBranchNames(pRemote, &azNames, &nNames);
    if( rc!=SQLITE_OK ){
      pRemote->xClose(pRemote);
      sqlite3_result_error(ctx, "failed to read remote refs", -1);
      return;
    }

    /* Need to close and reopen remote for each fetch since doltliteFetch
    ** may modify the remote's state. Actually, doltliteFetch reads from
    ** the remote, so we can reuse it, but we need to reopen for each
    ** branch since the function opens its own remote refs. */
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
      if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ){
        freeNameList(azNames, nNames);
        sqlite3_result_error(ctx, "fetch failed", -1);
        return;
      }
    }
    freeNameList(azNames, nNames);
  }

  if( pRemote ) pRemote->xClose(pRemote);
  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_pull('remote', 'branch') — fetch + fast-forward
** -------------------------------------------------------------------------- */
static void doltPullFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  ProllyHash trackingCommit, localCommit;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<2 ){
    sqlite3_result_error(ctx, "usage: dolt_pull(remote, branch)", -1);
    return;
  }

  const char *zRemoteName = (const char*)sqlite3_value_text(argv[0]);
  const char *zBranch = (const char*)sqlite3_value_text(argv[1]);
  if( !zRemoteName || !zBranch ){
    sqlite3_result_error(ctx, "remote and branch required", -1);
    return;
  }

  /* 1. Fetch the branch */
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

  rc = doltliteFetch(cs, pRemote, zRemoteName, zBranch);
  pRemote->xClose(pRemote);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "fetch failed: branch not found on remote", -1);
    return;
  }

  /* 2. Find tracking branch commit */
  rc = chunkStoreFindTracking(cs, zRemoteName, zBranch, &trackingCommit);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&trackingCommit) ){
    sqlite3_result_error(ctx, "tracking branch not found after fetch", -1);
    return;
  }

  /* 3. Find local branch commit */
  rc = chunkStoreFindBranch(cs, zBranch, &localCommit);
  if( rc!=SQLITE_OK ){
    /* Local branch doesn't exist — create it at tracking commit */
    rc = chunkStoreAddBranch(cs, zBranch, &trackingCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "failed to create local branch", -1);
      return;
    }
    localCommit = trackingCommit;
  }

  /* 4. Check if already up-to-date */
  if( prollyHashCompare(&localCommit, &trackingCommit)==0 ){
    sqlite3_result_int(ctx, 0); /* already up-to-date */
    return;
  }

  /* 5. Fast-forward check: tracking commit must be a descendant of local.
  ** For v1, we only support fast-forward. Walk the tracking commit's
  ** parent chain and see if we hit localCommit. */
  {
    ProllyHash walk;
    int maxDepth = 1000;
    int canFF = 0;
    memcpy(&walk, &trackingCommit, sizeof(ProllyHash));

    while( maxDepth-- > 0 ){
      u8 *data = 0; int nData = 0;
      DoltliteCommit commit;

      if( prollyHashCompare(&walk, &localCommit)==0 ){
        canFF = 1;
        break;
      }
      if( prollyHashIsEmpty(&walk) ) break;

      rc = chunkStoreGet(cs, &walk, &data, &nData);
      if( rc!=SQLITE_OK || !data ) break;
      rc = doltliteCommitDeserialize(data, nData, &commit);
      sqlite3_free(data);
      if( rc!=SQLITE_OK ) break;

      if( commit.nParents > 0 ){
        memcpy(&walk, &commit.aParents[0], sizeof(ProllyHash));
      }else{
        memset(&walk, 0, sizeof(ProllyHash));
      }
      doltliteCommitClear(&commit);
    }

    if( !canFF ){
      sqlite3_result_error(ctx,
        "cannot fast-forward — use dolt_merge with the tracking branch instead", -1);
      return;
    }
  }

  /* 6. Fast-forward: update local branch to tracking commit */
  rc = chunkStoreUpdateBranch(cs, zBranch, &trackingCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "failed to update branch", -1);
    return;
  }

  /* 7. If this is the current session branch, hard-reset to the new commit */
  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    DoltliteCommit commit;
    u8 *data = 0; int nData = 0;

    rc = chunkStoreGet(cs, &trackingCommit, &data, &nData);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "failed to load commit", -1);
      return;
    }
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "failed to deserialize commit", -1);
      return;
    }

    rc = doltliteHardReset(db, &commit.catalogHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      sqlite3_result_error(ctx, "hard reset failed", -1);
      return;
    }

    doltliteSetSessionHead(db, &trackingCommit);
    doltliteSetSessionStaged(db, &commit.catalogHash);
    chunkStoreSetHeadCommit(cs, &trackingCommit);
    doltliteCommitClear(&commit);
  }

  /* 8. Persist refs */
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_clone('url') — clone a remote repository
** -------------------------------------------------------------------------- */
static void doltCloneFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteRemote *pRemote = 0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(ctx, "usage: dolt_clone(url)", -1);
    return;
  }

  const char *zUrl = (const char*)sqlite3_value_text(argv[0]);
  if( !zUrl ){
    sqlite3_result_error(ctx, "url required", -1);
    return;
  }

  /* Check that local store is empty */
  if( !chunkStoreIsEmpty(cs) ){
    sqlite3_result_error(ctx, "database is not empty — clone into a fresh database", -1);
    return;
  }

  pRemote = openRemoteByUrl(cs->pVfs, zUrl);
  if( !pRemote ){
    sqlite3_result_error(ctx, "failed to open remote (URL must start with file://)", -1);
    return;
  }

  rc = doltliteClone(cs, pRemote);
  pRemote->xClose(pRemote);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "clone failed", -1);
    return;
  }

  /* Set up the remote as 'origin' */
  rc = chunkStoreAddRemote(cs, "origin", zUrl);
  /* Ignore error if remote already exists (e.g., refs already had remotes) */

  /* Reload refs from the cloned data to populate branches/tags in memory */
  {
    u8 *refsData = 0; int nRefsData = 0;
    ProllyHash refsHash;
    memcpy(&refsHash, &cs->refsHash, sizeof(ProllyHash));
    if( !prollyHashIsEmpty(&refsHash) ){
      rc = chunkStoreGet(cs, &refsHash, &refsData, &nRefsData);
      (void)refsData; /* refs are already loaded by clone */
      sqlite3_free(refsData);
    }
  }

  /* Find default branch and set up session state */
  {
    const char *zDefault = chunkStoreGetDefaultBranch(cs);
    ProllyHash branchCommit;

    if( !zDefault && cs->nBranches > 0 ){
      zDefault = cs->aBranches[0].zName;
    }

    if( zDefault ){
      rc = chunkStoreFindBranch(cs, zDefault, &branchCommit);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&branchCommit) ){
        /* Load commit to get catalog hash */
        u8 *data = 0; int nData = 0;
        DoltliteCommit commit;

        rc = chunkStoreGet(cs, &branchCommit, &data, &nData);
        if( rc==SQLITE_OK && data ){
          rc = doltliteCommitDeserialize(data, nData, &commit);
          sqlite3_free(data);
          if( rc==SQLITE_OK ){
            rc = doltliteHardReset(db, &commit.catalogHash);
            if( rc==SQLITE_OK ){
              doltliteSetSessionBranch(db, zDefault);
              doltliteSetSessionHead(db, &branchCommit);
              doltliteSetSessionStaged(db, &commit.catalogHash);
              chunkStoreSetHeadCommit(cs, &branchCommit);
              chunkStoreSetDefaultBranch(cs, zDefault);
            }
            doltliteCommitClear(&commit);
          }
        }
      }
    }
  }

  /* Persist refs (including the new 'origin' remote) */
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_remotes virtual table (read-only)
** -------------------------------------------------------------------------- */
typedef struct RemVtab RemVtab;
struct RemVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct RemCur RemCur;
struct RemCur { sqlite3_vtab_cursor base; int iRow; };

static int remConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  RemVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(name TEXT, url TEXT)");
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
    case 0: sqlite3_result_text(ctx, rem->zName, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, rem->zUrl, -1, SQLITE_TRANSIENT); break;
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

/* --------------------------------------------------------------------------
** Registration
** -------------------------------------------------------------------------- */
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

#endif /* DOLTLITE_PROLLY */
