#ifndef DOLTLITE_ENABLE_CHUNK_SOURCE
# define DOLTLITE_ENABLE_CHUNK_SOURCE 1
#endif

#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

#if DOLTLITE_ENABLE_CHUNK_SOURCE

#define CS_SOURCE_CACHE_BUCKETS 256
#define CS_SOURCE_CACHE_MAX_ENTRIES 2048
#define CS_SOURCE_CACHE_MAX_BYTES (16*1024*1024)

typedef struct DoltliteChunkSourceEntry DoltliteChunkSourceEntry;
struct DoltliteChunkSourceEntry {
  ProllyHash hash;
  u8 *pData;
  int nData;
  DoltliteChunkSourceEntry *pHashNext;
  DoltliteChunkSourceEntry *pLruPrev;
  DoltliteChunkSourceEntry *pLruNext;
};

struct DoltliteChunkSourceState {
  doltlite_chunk_source *pSource;
  sqlite3 *db;
  DoltliteChunkSourceEntry *aHash[CS_SOURCE_CACHE_BUCKETS];
  DoltliteChunkSourceEntry *pLruHead;
  DoltliteChunkSourceEntry *pLruTail;
  i64 nCacheBytes;
  int nCacheEntry;
  ChunkStore writer;
  u8 writerOpen;
  u8 memoryOnly;
  int errRc;
  char *zErr;
};

static unsigned int csSourceBucket(const ProllyHash *pHash){
  return ((unsigned int)pHash->data[0]
       | ((unsigned int)pHash->data[1] << 8))
       & (CS_SOURCE_CACHE_BUCKETS - 1);
}

static DoltliteChunkSourceEntry *csSourceCacheFind(
  DoltliteChunkSourceState *p,
  const ProllyHash *pHash
){
  DoltliteChunkSourceEntry *pEntry;
  unsigned int iBucket = csSourceBucket(pHash);
  for(pEntry=p->aHash[iBucket]; pEntry; pEntry=pEntry->pHashNext){
    if( memcmp(&pEntry->hash, pHash, sizeof(*pHash))==0 ) return pEntry;
  }
  return 0;
}

static void csSourceCacheUnlinkLru(
  DoltliteChunkSourceState *p,
  DoltliteChunkSourceEntry *pEntry
){
  if( pEntry->pLruPrev ){
    pEntry->pLruPrev->pLruNext = pEntry->pLruNext;
  }else{
    p->pLruHead = pEntry->pLruNext;
  }
  if( pEntry->pLruNext ){
    pEntry->pLruNext->pLruPrev = pEntry->pLruPrev;
  }else{
    p->pLruTail = pEntry->pLruPrev;
  }
}

static void csSourceCacheLinkHead(
  DoltliteChunkSourceState *p,
  DoltliteChunkSourceEntry *pEntry
){
  pEntry->pLruPrev = 0;
  pEntry->pLruNext = p->pLruHead;
  if( p->pLruHead ) p->pLruHead->pLruPrev = pEntry;
  else p->pLruTail = pEntry;
  p->pLruHead = pEntry;
}

static void csSourceCacheTouch(
  DoltliteChunkSourceState *p,
  DoltliteChunkSourceEntry *pEntry
){
  if( p->pLruHead==pEntry ) return;
  csSourceCacheUnlinkLru(p, pEntry);
  csSourceCacheLinkHead(p, pEntry);
}

static void csSourceCacheRemove(
  DoltliteChunkSourceState *p,
  DoltliteChunkSourceEntry *pEntry
){
  DoltliteChunkSourceEntry **pp;
  unsigned int iBucket = csSourceBucket(&pEntry->hash);
  for(pp=&p->aHash[iBucket]; *pp; pp=&(*pp)->pHashNext){
    if( *pp==pEntry ){
      *pp = pEntry->pHashNext;
      break;
    }
  }
  csSourceCacheUnlinkLru(p, pEntry);
  p->nCacheEntry--;
  p->nCacheBytes -= pEntry->nData;
  sqlite3_free(pEntry);
}

static int csSourceCachePut(
  DoltliteChunkSourceState *p,
  const ProllyHash *pHash,
  const u8 *pData,
  int nData
){
  DoltliteChunkSourceEntry *pEntry;
  sqlite3_uint64 nAlloc;
  unsigned int iBucket;
  if( nData<0 ) return SQLITE_IOERR_CHUNK_SOURCE;
  pEntry = csSourceCacheFind(p, pHash);
  if( pEntry ){
    csSourceCacheTouch(p, pEntry);
    return SQLITE_OK;
  }
  nAlloc = sizeof(*pEntry) + (sqlite3_uint64)(nData>0 ? nData : 1);
  pEntry = (DoltliteChunkSourceEntry*)sqlite3_malloc64(nAlloc);
  if( !pEntry ) return SQLITE_NOMEM;
  memset(pEntry, 0, sizeof(*pEntry));
  pEntry->hash = *pHash;
  pEntry->pData = (u8*)&pEntry[1];
  pEntry->nData = nData;
  if( nData>0 ) memcpy(pEntry->pData, pData, (size_t)nData);
  iBucket = csSourceBucket(pHash);
  pEntry->pHashNext = p->aHash[iBucket];
  p->aHash[iBucket] = pEntry;
  csSourceCacheLinkHead(p, pEntry);
  p->nCacheEntry++;
  p->nCacheBytes += nData;
  while( p->pLruTail
      && (p->nCacheEntry>CS_SOURCE_CACHE_MAX_ENTRIES
       || (p->nCacheBytes>CS_SOURCE_CACHE_MAX_BYTES
        && p->nCacheEntry>1)) ){
    csSourceCacheRemove(p, p->pLruTail);
  }
  return SQLITE_OK;
}

static int csSourceCacheGet(
  DoltliteChunkSourceState *p,
  const ProllyHash *pHash,
  u8 **ppData,
  int *pnData
){
  DoltliteChunkSourceEntry *pEntry = csSourceCacheFind(p, pHash);
  u8 *pCopy;
  if( !pEntry ) return SQLITE_NOTFOUND;
  pCopy = (u8*)sqlite3_malloc(pEntry->nData>0 ? pEntry->nData : 1);
  if( !pCopy ) return SQLITE_NOMEM;
  if( pEntry->nData>0 ){
    memcpy(pCopy, pEntry->pData, (size_t)pEntry->nData);
  }
  csSourceCacheTouch(p, pEntry);
  *ppData = pCopy;
  *pnData = pEntry->nData;
  return chunkStoreVerifyChunk(pHash, ppData, pnData);
}

static void csSourceClearError(DoltliteChunkSourceState *p){
  sqlite3_free(p->zErr);
  p->zErr = 0;
  p->errRc = SQLITE_OK;
}

static void csSourceSetHashError(
  DoltliteChunkSourceState *p,
  int rc,
  const char *zPrefix,
  const ProllyHash *pHash
){
  static const char zHex[] = "0123456789abcdef";
  char zHash[PROLLY_HASH_SIZE*2 + 1];
  int i;
  csSourceClearError(p);
  p->errRc = rc;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    zHash[i*2] = zHex[pHash->data[i] >> 4];
    zHash[i*2 + 1] = zHex[pHash->data[i] & 0x0f];
  }
  zHash[PROLLY_HASH_SIZE*2] = 0;
  p->zErr = sqlite3_mprintf("%s %s", zPrefix, zHash);
}

static int csSourceGraphLockHeld(
  ChunkStore *cs,
  DoltliteChunkSourceState *p
){
  int i;
  if( p->db ){
    for(i=0; i<p->db->nDb; i++){
      ChunkStore *pAttached = doltliteBtreeChunkStore(p->db->aDb[i].pBt);
      if( pAttached && pAttached->lockDepth>0 ) return 1;
    }
  }else if( cs->lockDepth>0 ){
    return 1;
  }
  return p->writerOpen && p->writer.lockDepth>0;
}

static int csSourceOpenWriter(
  ChunkStore *cs,
  DoltliteChunkSourceState *p,
  int *pChanged
){
  int changed = 0;
  int rc;
  if( pChanged ) *pChanged = 0;
  if( p->writerOpen || p->memoryOnly ) return SQLITE_OK;
  if( cs->readOnly || cs->isMemory || cs->isBuffer || cs->movedReadOnly ){
    p->memoryOnly = 1;
    return SQLITE_OK;
  }
  if( cs->file.pFile==0 ) return SQLITE_OK;
  if( cs->lockDepth>0 ) return SQLITE_BUSY;
  rc = chunkStoreLockAndRefreshChanged(cs, &changed);
  if( rc!=SQLITE_OK ) return rc;
  if( cs->movedReadOnly ){
    p->memoryOnly = 1;
  }else{
    rc = chunkStoreOpen(&p->writer, chunkFileGetVfs(&cs->file),
                        chunkFileGetFilename(&cs->file),
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
    if( rc==SQLITE_OK && (p->writer.readOnly || p->writer.movedReadOnly) ){
      chunkStoreClose(&p->writer);
      p->memoryOnly = 1;
    }else if( rc==SQLITE_OK ){
      p->writerOpen = 1;
    }
  }
  chunkStoreUnlock(cs);
  if( pChanged ) *pChanged = changed;
  return rc;
}

static int csSourcePersistMany(
  ChunkStore *cs,
  DoltliteChunkSourceState *p,
  const ProllyHash *aHash,
  u8 **apData,
  const int *anData,
  int nHash
){
  ProllyHash putHash;
  int nPut = 0;
  int rc;
  int i;
  if( p->memoryOnly ) return SQLITE_OK;
  for(i=0; i<nHash && !apData[i]; i++){}
  if( i==nHash ) return SQLITE_OK;
  rc = csSourceOpenWriter(cs, p, 0);
  if( rc!=SQLITE_OK || !p->writerOpen ) return rc;
  rc = chunkStoreLockAndRefresh(&p->writer);
  if( rc==SQLITE_OK && p->writer.movedReadOnly ){
    p->memoryOnly = 1;
  }
  if( rc==SQLITE_OK && !p->memoryOnly ){
    rc = chunkStoreForceRefresh(&p->writer);
    if( rc==SQLITE_READONLY && p->writer.movedReadOnly ){
      p->memoryOnly = 1;
      rc = SQLITE_OK;
    }
  }
  for(i=0; rc==SQLITE_OK && !p->memoryOnly && i<nHash; i++){
    int has = 0;
    if( !apData[i] ) continue;
    rc = chunkStoreHas(&p->writer, &aHash[i], &has);
    if( rc==SQLITE_OK && !has ){
      rc = chunkStorePut(&p->writer, apData[i], anData[i], &putHash);
      if( rc==SQLITE_OK
       && memcmp(&putHash, &aHash[i], sizeof(aHash[i]))!=0 ){
        rc = SQLITE_CORRUPT;
      }
      if( rc==SQLITE_OK ) nPut++;
    }
  }
  if( rc==SQLITE_OK && !p->memoryOnly && nPut>0 ){
    rc = chunkStoreCommit(&p->writer);
  }
  if( rc!=SQLITE_OK ) chunkStoreRollback(&p->writer);
  chunkStoreUnlock(&p->writer);
  return rc;
}

int chunkStoreSourceHas(
  ChunkStore *cs,
  const ProllyHash *pHash,
  int *pHas
){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  int rc;
  *pHas = 0;
  if( !p ) return SQLITE_OK;
  if( csSourceCacheFind(p, pHash) ){
    *pHas = 1;
    return SQLITE_OK;
  }
  if( !p->writerOpen ) return SQLITE_OK;
  rc = chunkStoreHas(&p->writer, pHash, pHas);
  return rc;
}

int chunkStoreSourceGet(
  ChunkStore *cs,
  const ProllyHash *pHash,
  u8 **ppData,
  int *pnData
){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  u8 *pData = 0;
  u8 *apData[1];
  int nData = 0;
  int anData[1];
  int sourceRc;
  int rc;
  if( !p ) return SQLITE_NOTFOUND;
  csSourceClearError(p);
  rc = csSourceCacheGet(p, pHash, ppData, pnData);
  if( rc!=SQLITE_NOTFOUND ) return rc;
  if( p->writerOpen ){
    rc = chunkStoreGet(&p->writer, pHash, ppData, pnData);
    if( rc==SQLITE_OK ){
      rc = csSourceCachePut(p, pHash, *ppData, *pnData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(*ppData);
        *ppData = 0;
        *pnData = 0;
      }
      return rc;
    }
    if( rc!=SQLITE_NOTFOUND ) return rc;
  }
  if( csSourceGraphLockHeld(cs, p) ){
    csSourceSetHashError(p, SQLITE_BUSY,
      "chunk source fetch blocked by active graph lock for", pHash);
    return SQLITE_BUSY;
  }
  sourceRc = p->pSource->xGet(p->pSource->pCtx, pHash->data,
                              &pData, &nData);
  if( sourceRc==DOLTLITE_SOURCE_NOTFOUND ){
    sqlite3_free(pData);
    csSourceSetHashError(
        p, SQLITE_NOTFOUND, "chunk source did not contain", pHash);
    return SQLITE_NOTFOUND;
  }
  if( sourceRc!=DOLTLITE_SOURCE_OK || !pData || nData<0 ){
    sqlite3_free(pData);
    csSourceSetHashError(
        p, SQLITE_IOERR_CHUNK_SOURCE, "chunk source I/O error for", pHash);
    return SQLITE_IOERR_CHUNK_SOURCE;
  }
  rc = chunkStoreVerifyChunk(pHash, &pData, &nData);
  if( rc!=SQLITE_OK ){
    csSourceSetHashError(
        p, rc, "chunk source returned corrupt bytes for", pHash);
    return rc;
  }
  apData[0] = pData;
  anData[0] = nData;
  rc = csSourcePersistMany(cs, p, pHash, apData, anData, 1);
  if( rc==SQLITE_OK ) rc = csSourceCachePut(p, pHash, pData, nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pData);
    return rc;
  }
  *ppData = pData;
  *pnData = nData;
  return SQLITE_OK;
}

int chunkStoreSourcePrefetchMany(
  ChunkStore *cs,
  const ProllyHash *aHash,
  int nHash
){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  u8 *aPresent = 0;
  ProllyHash *aMissing = 0;
  u8 **apData = 0;
  int *anData = 0;
  int nMissing = 0;
  int sourceRc;
  int rc = SQLITE_OK;
  int i;

  if( !p || nHash==0 ) return SQLITE_OK;
  if( nHash<0 ) return SQLITE_MISUSE;
  csSourceClearError(p);

  aPresent = (u8*)sqlite3_malloc64((sqlite3_uint64)nHash);
  aMissing = (ProllyHash*)sqlite3_malloc64(
      (sqlite3_uint64)nHash * sizeof(ProllyHash));
  if( !aPresent || !aMissing ){
    rc = SQLITE_NOMEM;
    goto prefetch_done;
  }
  rc = chunkStoreHasMany(cs, aHash, nHash, aPresent);
  if( rc!=SQLITE_OK ) goto prefetch_done;
  for(i=0; i<nHash; i++){
    if( !aPresent[i] ) aMissing[nMissing++] = aHash[i];
  }
  if( nMissing==0 ) goto prefetch_done;
  if( csSourceGraphLockHeld(cs, p) ){
    csSourceSetHashError(p, SQLITE_BUSY,
      "chunk source fetch blocked by active graph lock for", &aMissing[0]);
    rc = SQLITE_BUSY;
    goto prefetch_done;
  }

  apData = (u8**)sqlite3_malloc64(
      (sqlite3_uint64)nMissing * sizeof(u8*));
  anData = (int*)sqlite3_malloc64(
      (sqlite3_uint64)nMissing * sizeof(int));
  if( !apData || !anData ){
    rc = SQLITE_NOMEM;
    goto prefetch_done;
  }
  memset(apData, 0, (size_t)nMissing * sizeof(u8*));
  memset(anData, 0, (size_t)nMissing * sizeof(int));

  sourceRc = p->pSource->xGetMany(
      p->pSource->pCtx, nMissing, (const u8*)aMissing, apData, anData);
  if( sourceRc==DOLTLITE_SOURCE_NOTFOUND ) goto prefetch_done;
  if( sourceRc!=DOLTLITE_SOURCE_OK ){
    int iErr = 0;
    for(i=0; i<nMissing; i++){
      if( !apData[i] ){
        iErr = i;
        break;
      }
    }
    csSourceSetHashError(p, SQLITE_IOERR_CHUNK_SOURCE,
                         "chunk source batch I/O error for",
                         &aMissing[iErr]);
    rc = SQLITE_IOERR_CHUNK_SOURCE;
    goto prefetch_done;
  }

  for(i=0; i<nMissing; i++){
    if( !apData[i] ) continue;
    if( anData[i]<0 ){
      csSourceSetHashError(p, SQLITE_IOERR_CHUNK_SOURCE,
                           "chunk source I/O error for", &aMissing[i]);
      rc = SQLITE_IOERR_CHUNK_SOURCE;
      goto prefetch_done;
    }
    rc = chunkStoreVerifyChunk(&aMissing[i], &apData[i], &anData[i]);
    if( rc!=SQLITE_OK ){
      csSourceSetHashError(p, rc, "chunk source returned corrupt bytes for",
                           &aMissing[i]);
      goto prefetch_done;
    }
  }

  rc = csSourcePersistMany(cs, p, aMissing, apData, anData, nMissing);
  if( rc!=SQLITE_OK ) goto prefetch_done;
  for(i=0; i<nMissing; i++){
    if( !apData[i] ) continue;
    rc = csSourceCachePut(p, &aMissing[i], apData[i], anData[i]);
    if( rc!=SQLITE_OK ) goto prefetch_done;
  }

prefetch_done:
  if( apData ){
    for(i=0; i<nMissing; i++) sqlite3_free(apData[i]);
  }
  sqlite3_free(anData);
  sqlite3_free(apData);
  sqlite3_free(aMissing);
  sqlite3_free(aPresent);
  return rc;
}

void chunkStoreSourceClose(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  if( !p ) return;
  cs->pChunkSource = 0;
  if( p->writerOpen ) chunkStoreClose(&p->writer);
  while( p->pLruTail ) csSourceCacheRemove(p, p->pLruTail);
  csSourceClearError(p);
  sqlite3_free(p);
}

int chunkStoreSourceSet(
  ChunkStore *cs,
  sqlite3 *db,
  doltlite_chunk_source *pSource,
  int *pChanged
){
  DoltliteChunkSourceState *p;
  int rc = SQLITE_OK;
  if( pChanged ) *pChanged = 0;
  chunkStoreSourceClose(cs);
  if( !pSource ) return chunkStoreRefreshIfChanged(cs, pChanged);
  p = (DoltliteChunkSourceState*)sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->pSource = pSource;
  p->db = db;
  rc = csSourceOpenWriter(cs, p, pChanged);
  if( rc!=SQLITE_OK ){
    if( p->writerOpen ) chunkStoreClose(&p->writer);
    sqlite3_free(p);
    return rc;
  }
  cs->pChunkSource = p;
  return SQLITE_OK;
}

static char *csSourceTakeError(
  DoltliteChunkSourceState *p,
  int *pRc
){
  char *zErr = 0;
  int rc = SQLITE_OK;
  if( p ){
    zErr = p->zErr;
    rc = p->errRc;
    p->zErr = 0;
    p->errRc = SQLITE_OK;
  }
  if( pRc ) *pRc = rc;
  return zErr;
}

char *chunkStoreSourceTakeError(ChunkStore *cs, int *pRc){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  return csSourceTakeError(p, pRc);
}

char *doltliteTakeChunkSourceError(sqlite3 *db, int *pRc){
  int i;
  if( pRc ) *pRc = SQLITE_OK;
  for(i=0; i<db->nDb; i++){
    ChunkStore *cs = doltliteBtreeChunkStore(db->aDb[i].pBt);
    char *zErr;
    int rc;
    if( !cs ) continue;
    zErr = csSourceTakeError(cs->pChunkSource, &rc);
    if( zErr || rc!=SQLITE_OK ){
      if( pRc ) *pRc = rc;
      return zErr;
    }
  }
  return 0;
}

static void csSourceSetApiError(sqlite3 *db, ChunkStore *cs, int rc){
  char *zErr = chunkStoreSourceTakeError(cs, 0);
  if( zErr ){
    sqlite3ErrorWithMsg(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }else{
    sqlite3ErrorWithMsg(db, rc, "%s", sqlite3ErrStr(rc));
  }
}

SQLITE_API int SQLITE_APICALL doltlite_set_chunk_source(
  sqlite3 *db,
  const char *zDbName,
  doltlite_chunk_source *pSource
){
  Btree *pBtree;
  ChunkStore *cs;
  char *zSourceErr = 0;
  int installed = 0;
  int storeChanged = 0;
  int rc = SQLITE_OK;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlite3_mutex_enter(db->mutex);
  pBtree = sqlite3DbNameToBtree(db, zDbName);
  if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
    rc = SQLITE_NOTFOUND;
    sqlite3ErrorWithMsg(db, rc, "database is not a DoltLite database");
  }else if( !db->autoCommit || pBtree->inTrans!=TRANS_NONE
         || pBtree->inTransaction!=TRANS_NONE ){
    rc = SQLITE_BUSY;
    sqlite3ErrorWithMsg(db, rc,
                        "cannot change chunk source during a transaction");
  }else if( pSource && (pSource->iVersion!=1
                      || !pSource->xGet || !pSource->xGetMany) ){
    rc = SQLITE_MISUSE;
    sqlite3ErrorWithMsg(db, rc, "invalid DoltLite chunk source version 1");
  }else{
    sqlite3BtreeEnter(pBtree);
    cs = doltliteBtreeChunkStore(pBtree);
    rc = chunkStoreSourceSet(cs, db, pSource, &storeChanged);
    if( storeChanged && !pBtree->isDetached ){
      pBtree->iLoadedWorkingStateVersion =
          pBtree->pBt->iWorkingStateVersion - 1;
    }
    if( rc==SQLITE_OK && pSource ){
      installed = 1;
      rc = doltliteBtreeHydrateDeferred(pBtree);
    }
    if( rc!=SQLITE_OK && installed ){
      zSourceErr = chunkStoreSourceTakeError(cs, 0);
      chunkStoreSourceClose(cs);
    }
    sqlite3BtreeLeave(pBtree);
    if( rc==SQLITE_OK && pSource
     && pBtree==sqlite3DbNameToBtree(db, 0)
     && pBtree->bDeferredRegister ){
      rc = doltliteRegister(db);
      if( rc!=SQLITE_OK ){
        sqlite3BtreeEnter(pBtree);
        zSourceErr = chunkStoreSourceTakeError(cs, 0);
        chunkStoreSourceClose(cs);
        sqlite3BtreeLeave(pBtree);
      }
    }
    if( rc==SQLITE_OK ) sqlite3Error(db, SQLITE_OK);
    else if( zSourceErr ) sqlite3ErrorWithMsg(db, rc, "%s", zSourceErr);
    else csSourceSetApiError(db, cs, rc);
  }
  sqlite3_free(zSourceErr);
  rc = sqlite3ApiExit(db, rc);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

SQLITE_API int SQLITE_APICALL doltlite_init_lazy(
  sqlite3 *db,
  const void *pRefs,
  int nRefs
){
  Btree *pBtree;
  ChunkStore *cs = 0;
  char *zOldErr;
  char *zPreparedBranch = 0;
  int locked = 0;
  int rc = SQLITE_OK;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlite3_mutex_enter(db->mutex);
  pBtree = sqlite3DbNameToBtree(db, 0);
  if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
    rc = SQLITE_NOTFOUND;
    sqlite3ErrorWithMsg(db, rc, "main is not a DoltLite database");
  }else if( pBtree->isDetached ){
    rc = SQLITE_READONLY;
    sqlite3ErrorWithMsg(db, rc,
                        "cannot install DoltLite refs in a detached session");
  }else if( !pRefs || nRefs<=0 ){
    rc = SQLITE_MISUSE;
    sqlite3ErrorWithMsg(db, rc, "invalid DoltLite refs blob");
  }else if( !db->autoCommit || pBtree->inTrans!=TRANS_NONE
         || pBtree->inTransaction!=TRANS_NONE ){
    rc = SQLITE_BUSY;
    sqlite3ErrorWithMsg(db, rc,
                        "cannot install DoltLite refs during a transaction");
  }else{
    sqlite3BtreeEnter(pBtree);
    cs = doltliteBtreeChunkStore(pBtree);
    zOldErr = chunkStoreSourceTakeError(cs, 0);
    sqlite3_free(zOldErr);
    rc = chunkStoreLockAndRefresh(cs);
    if( rc==SQLITE_OK ){
      locked = 1;
      rc = chunkStoreForceRefresh(cs);
    }
    if( rc==SQLITE_OK ){
      rc = chunkStoreInstallRefsBlob(cs, (const u8*)pRefs, nRefs);
    }
    if( rc==SQLITE_OK ){
      rc = chunkStoreCommit(cs);
      if( rc==SQLITE_OK ) pBtree->bDeferredOpen = 1;
    }
    if( rc!=SQLITE_OK ) chunkStoreRollback(cs);
    if( locked ) chunkStoreUnlock(cs);
    if( rc==SQLITE_OK ){
      rc = doltliteBtreePrepareBackupBranch(
          pBtree, cs, &zPreparedBranch);
    }
    if( rc==SQLITE_OK && zPreparedBranch ){
      doltliteBtreeInstallBackupBranch(pBtree, zPreparedBranch);
      zPreparedBranch = 0;
    }
    if( rc==SQLITE_OK && cs->pChunkSource ){
      rc = doltliteBtreeHydrateDeferred(pBtree);
    }
    sqlite3BtreeLeave(pBtree);
    if( rc==SQLITE_OK && cs->pChunkSource && pBtree->bDeferredRegister ){
      rc = doltliteRegister(db);
    }
    if( rc==SQLITE_OK ) sqlite3Error(db, SQLITE_OK);
    else csSourceSetApiError(db, cs, rc);
  }
  sqlite3_free(zPreparedBranch);
  rc = sqlite3ApiExit(db, rc);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

#else

int chunkStoreSourceHas(ChunkStore *cs, const ProllyHash *pHash, int *pHas){
  UNUSED_PARAMETER(cs);
  UNUSED_PARAMETER(pHash);
  *pHas = 0;
  return SQLITE_OK;
}

int chunkStoreSourceGet(ChunkStore *cs, const ProllyHash *pHash,
                        u8 **ppData, int *pnData){
  UNUSED_PARAMETER(cs);
  UNUSED_PARAMETER(pHash);
  UNUSED_PARAMETER(ppData);
  UNUSED_PARAMETER(pnData);
  return SQLITE_NOTFOUND;
}

int chunkStoreSourcePrefetchMany(ChunkStore *cs, const ProllyHash *aHash,
                                 int nHash){
  UNUSED_PARAMETER(cs);
  UNUSED_PARAMETER(aHash);
  UNUSED_PARAMETER(nHash);
  return SQLITE_OK;
}

int chunkStoreSourceSet(ChunkStore *cs, sqlite3 *db,
                        doltlite_chunk_source *pSource, int *pChanged){
  UNUSED_PARAMETER(cs);
  UNUSED_PARAMETER(db);
  if( pChanged ) *pChanged = 0;
  return pSource ? SQLITE_NOTFOUND : SQLITE_OK;
}

void chunkStoreSourceClose(ChunkStore *cs){ UNUSED_PARAMETER(cs); }
char *chunkStoreSourceTakeError(ChunkStore *cs, int *pRc){
  UNUSED_PARAMETER(cs);
  if( pRc ) *pRc = SQLITE_OK;
  return 0;
}
char *doltliteTakeChunkSourceError(sqlite3 *db, int *pRc){
  UNUSED_PARAMETER(db);
  if( pRc ) *pRc = SQLITE_OK;
  return 0;
}

SQLITE_API int SQLITE_APICALL doltlite_set_chunk_source(
  sqlite3 *db,
  const char *zDbName,
  doltlite_chunk_source *pSource
){
  int rc;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  UNUSED_PARAMETER(zDbName);
  sqlite3_mutex_enter(db->mutex);
  rc = pSource ? SQLITE_NOTFOUND : SQLITE_OK;
  if( rc==SQLITE_OK ) sqlite3Error(db, SQLITE_OK);
  else sqlite3ErrorWithMsg(db, rc, "DoltLite chunk source support is disabled");
  rc = sqlite3ApiExit(db, rc);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

SQLITE_API int SQLITE_APICALL doltlite_init_lazy(
  sqlite3 *db,
  const void *pRefs,
  int nRefs
){
  int rc = SQLITE_NOTFOUND;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  UNUSED_PARAMETER(pRefs);
  UNUSED_PARAMETER(nRefs);
  sqlite3_mutex_enter(db->mutex);
  sqlite3ErrorWithMsg(db, rc, "DoltLite chunk source support is disabled");
  rc = sqlite3ApiExit(db, rc);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

#endif
#endif
