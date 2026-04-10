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

int doltliteResolveRef(sqlite3 *db, const char *zRef, ProllyHash *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;
  if( !zRef || !cs ) return SQLITE_ERROR;

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
