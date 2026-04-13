/*
** Shared ref resolution and commit loading.
**
** doltliteResolveRef: resolve a string (hex hash, branch, tag) to a commit.
** doltliteLoadCommit: load a commit object by hash from the chunk store.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

static int doltliteValidateCommitHash(
  sqlite3 *db,
  const ProllyHash *pHash
){
  DoltliteCommit commit;
  int rc;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, pHash, &commit);
  if( rc==SQLITE_OK ){
    doltliteCommitClear(&commit);
  }
  return rc;
}

/* Resolve a base ref (no ~N / ^N suffix) to a commit hash. Tries
** the literal "HEAD" keyword, then 40-char hex, then branch name,
** then tag name. Returns SQLITE_NOTFOUND if none match. */
static int doltliteResolveBaseRef(
  sqlite3 *db,
  const char *zRef,
  ProllyHash *pCommit
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  /* "HEAD" keyword resolves to the current session head */
  if( strcmp(zRef, "HEAD")==0 ){
    doltliteGetSessionHead(db, pCommit);
    if( prollyHashIsEmpty(pCommit) ) return SQLITE_NOTFOUND;
    return SQLITE_OK;
  }

  /* Try 40-char hex hash */
  if( strlen(zRef)==PROLLY_HASH_SIZE*2 ){
    rc = doltliteHexToHash(zRef, pCommit);
    if( rc==SQLITE_OK ){
      rc = doltliteValidateCommitHash(db, pCommit);
      if( rc==SQLITE_OK ) return SQLITE_OK;
      if( rc!=SQLITE_NOTFOUND ) return rc;
    }
  }

  /* Try branch name */
  rc = chunkStoreFindBranch(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ){
    rc = doltliteValidateCommitHash(db, pCommit);
    if( rc==SQLITE_OK ) return SQLITE_OK;
    return rc;
  }

  /* Try tag name */
  rc = chunkStoreFindTag(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ){
    rc = doltliteValidateCommitHash(db, pCommit);
    if( rc==SQLITE_OK ) return SQLITE_OK;
    return rc;
  }

  return SQLITE_NOTFOUND;
}

/* Walk back N commits along the first-parent chain. */
static int doltliteWalkFirstParent(
  sqlite3 *db,
  ProllyHash *pCommit,
  int n
){
  int i;
  for(i=0; i<n; i++){
    DoltliteCommit commit;
    const ProllyHash *pParent;
    int rc;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, pCommit, &commit);
    if( rc!=SQLITE_OK ) return rc;
    pParent = doltliteCommitParentHash(&commit, 0);
    if( !pParent ){
      doltliteCommitClear(&commit);
      return SQLITE_NOTFOUND;
    }
    memcpy(pCommit, pParent, sizeof(ProllyHash));
    doltliteCommitClear(&commit);
  }
  return SQLITE_OK;
}

static int doltliteSelectParent(
  sqlite3 *db,
  ProllyHash *pCommit,
  int iParent
){
  DoltliteCommit commit;
  const ProllyHash *pParent;
  int rc;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, pCommit, &commit);
  if( rc!=SQLITE_OK ) return rc;
  pParent = doltliteCommitParentHash(&commit, iParent);
  if( !pParent ){
    doltliteCommitClear(&commit);
    return SQLITE_NOTFOUND;
  }
  memcpy(pCommit, pParent, sizeof(ProllyHash));
  doltliteCommitClear(&commit);
  return SQLITE_OK;
}

int doltliteResolveRef(sqlite3 *db, const char *zRef, ProllyHash *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int len, j, n_back, parent_sel, rc;
  int suffix_pos = -1;
  char suffix_op = 0;
  char *base_buf = 0;
  const char *base;

  if( !zRef || !cs ) return SQLITE_ERROR;

  /* Look for a trailing ~N or ^N (rightmost) revision suffix.
  **  ~N walks back N commits along the first-parent chain.
  **  ^N selects the Nth parent of the resolved commit (default 1).
  ** Only the rightmost suffix is honored — chained suffixes such as
  ** HEAD~2~3 are not supported. */
  len = (int)strlen(zRef);
  for(j=len-1; j>=0; j--){
    if( zRef[j]=='~' || zRef[j]=='^' ){
      int k, allDigits = 1;
      for(k=j+1; k<len; k++){
        if( zRef[k]<'0' || zRef[k]>'9' ){ allDigits = 0; break; }
      }
      if( allDigits ){
        suffix_pos = j;
        suffix_op = zRef[j];
      }
      break;
    }
  }

  n_back = 0;
  parent_sel = 0;
  base = zRef;
  if( suffix_pos>=0 ){
    if( suffix_pos==len-1 ){
      if( suffix_op=='~' ){
        n_back = 1;
      }else{
        parent_sel = 1;
      }
    }else{
      int n = atoi(zRef + suffix_pos + 1);
      if( n<=0 ) n = 0;
      if( suffix_op=='~' ){
        n_back = n;
      }else{
        parent_sel = n;
      }
    }
    if( n_back>0 || parent_sel>0 ){
      if( suffix_pos==0 ){
        base = "HEAD";
      }else{
        base_buf = sqlite3_malloc(suffix_pos + 1);
        if( !base_buf ) return SQLITE_NOMEM;
        memcpy(base_buf, zRef, suffix_pos);
        base_buf[suffix_pos] = '\0';
        base = base_buf;
      }
    }
  }

  rc = doltliteResolveBaseRef(db, base, pCommit);
  if( rc==SQLITE_OK && n_back>0 ){
    rc = doltliteWalkFirstParent(db, pCommit, n_back);
  }else if( rc==SQLITE_OK && parent_sel>0 ){
    rc = doltliteSelectParent(db, pCommit, parent_sel-1);
  }
  if( base_buf ) sqlite3_free(base_buf);
  return rc;
}

int doltliteLoadCommit(sqlite3 *db, const ProllyHash *pHash,
                       DoltliteCommit *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  int rc;
  if( !cs ) return SQLITE_ERROR;
  rc = chunkStoreGet(cs, pHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteCommitDeserialize(data, nData, pCommit);
  sqlite3_free(data);
  return rc;
}

int doltliteForEachUserTable(
  sqlite3 *db,
  const char *zPrefix,
  const sqlite3_module *pModule
){
  ProllyHash headCommit;
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0, i, rc;

  doltliteGetSessionHead(db, &headCommit);
  if( prollyHashIsEmpty(&headCommit) ) return SQLITE_OK;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, &headCommit, &commit);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nTables; i++){
    if( aTables[i].zName && aTables[i].iTable > 1 ){
      char *zMod = sqlite3_mprintf("%s%s", zPrefix, aTables[i].zName);
      if( !zMod ){
        sqlite3_free(aTables);
        return SQLITE_NOMEM;
      }
      rc = sqlite3_create_module(db, zMod, pModule, 0);
      sqlite3_free(zMod);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aTables);
        return rc;
      }
    }
  }
  sqlite3_free(aTables);
  return SQLITE_OK;
}

#endif
