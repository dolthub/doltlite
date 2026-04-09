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

int doltliteResolveRef(sqlite3 *db, const char *zRef, ProllyHash *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;
  if( !zRef || !cs ) return SQLITE_ERROR;

  /* Try 40-char hex hash */
  if( strlen(zRef)==PROLLY_HASH_SIZE*2 ){
    rc = doltliteHexToHash(zRef, pCommit);
    if( rc==SQLITE_OK && chunkStoreHas(cs, pCommit) ) return SQLITE_OK;
  }

  /* Try branch name */
  rc = chunkStoreFindBranch(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ) return SQLITE_OK;

  /* Try tag name */
  rc = chunkStoreFindTag(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ) return SQLITE_OK;

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

#endif
