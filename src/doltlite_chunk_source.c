#ifndef DOLTLITE_ENABLE_CHUNK_SOURCE
# define DOLTLITE_ENABLE_CHUNK_SOURCE 1
#endif
#ifndef DOLTLITE_ENABLE_REMOTES
# define DOLTLITE_ENABLE_REMOTES 1
#endif

#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"
#if DOLTLITE_ENABLE_REMOTES
#include "doltlite_remote.h"
#endif

#if DOLTLITE_ENABLE_CHUNK_SOURCE

#define CS_SOURCE_CACHE_BUCKETS 256
#define CS_SOURCE_CACHE_MAX_ENTRIES 2048
#define CS_SOURCE_CACHE_MAX_BYTES (16*1024*1024)
#define CS_SOURCE_BUSY_RETRY_COUNT 200
#define CS_SOURCE_BUSY_RETRY_US 5000
#define CS_SOURCE_REMOTE_BATCH_SIZE 256

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
  doltlite_chunk_source *pHostSource;
  doltlite_chunk_source originSource;
  sqlite3 *db;
  DoltliteChunkSourceEntry *aHash[CS_SOURCE_CACHE_BUCKETS];
  DoltliteChunkSourceEntry *pLruHead;
  DoltliteChunkSourceEntry *pLruTail;
  i64 nCacheBytes;
  int nCacheEntry;
  ChunkStore writer;
  u8 writerOpen;
  u8 memoryOnly;
  u8 originEnabled;
  int errRc;
  char *zErr;
};

static int csSourceHasOrigin(ChunkStore *cs){
  const char *zUrl = 0;
  return chunkStoreFindRemote(cs, "origin", &zUrl)==SQLITE_OK
      && zUrl && zUrl[0];
}

static doltlite_chunk_source *csSourceActive(DoltliteChunkSourceState *p){
  if( p->pHostSource ) return p->pHostSource;
  if( p->originEnabled
   && csSourceHasOrigin((ChunkStore*)p->originSource.pCtx) ){
    return &p->originSource;
  }
  return 0;
}

#if DOLTLITE_ENABLE_REMOTES
static int SQLITE_CALLBACK csOriginGetMany(
  void *pCtx,
  int nHash,
  const unsigned char *aHash,
  unsigned char **apBytes,
  int *anBytes
){
  ChunkStore *cs = (ChunkStore*)pCtx;
  DoltliteRemote *pRemote = 0;
  const char *zUrl = 0;
  int any = 0;
  int i;
  int offset;
  int rc;

  for(i=0; i<nHash; i++){
    apBytes[i] = 0;
    anBytes[i] = 0;
  }
  if( nHash<=0 ) return DOLTLITE_SOURCE_OK;
  rc = chunkStoreFindRemote(cs, "origin", &zUrl);
  if( rc!=SQLITE_OK || !zUrl ) return DOLTLITE_SOURCE_IOERR;
  pRemote = doltliteRemoteOpenReadOnly(chunkFileGetVfs(&cs->file), zUrl);
  if( !pRemote ) return DOLTLITE_SOURCE_IOERR;

  for(offset=0; offset<nHash; offset += CS_SOURCE_REMOTE_BATCH_SIZE){
    ProllyHash aBatch[CS_SOURCE_REMOTE_BATCH_SIZE];
    int nBatch = nHash - offset;
    if( nBatch>CS_SOURCE_REMOTE_BATCH_SIZE ){
      nBatch = CS_SOURCE_REMOTE_BATCH_SIZE;
    }
    for(i=0; i<nBatch; i++){
      memcpy(aBatch[i].data,
             aHash + (offset+i)*PROLLY_HASH_SIZE, PROLLY_HASH_SIZE);
    }
    if( pRemote->xGetChunks ){
      rc = pRemote->xGetChunks(pRemote, aBatch, nBatch,
                               (u8**)&apBytes[offset], &anBytes[offset]);
      if( rc!=SQLITE_OK ){
        pRemote->xClose(pRemote);
        return DOLTLITE_SOURCE_IOERR;
      }
    }else{
      for(i=0; i<nBatch; i++){
        int iOut = offset + i;
        rc = pRemote->xGetChunk(
            pRemote, &aBatch[i], (u8**)&apBytes[iOut], &anBytes[iOut]);
        if( rc==SQLITE_NOTFOUND ){
          sqlite3_free(apBytes[iOut]);
          apBytes[iOut] = 0;
          anBytes[iOut] = 0;
          continue;
        }
        if( rc!=SQLITE_OK ){
          pRemote->xClose(pRemote);
          return DOLTLITE_SOURCE_IOERR;
        }
      }
    }
  }
  pRemote->xClose(pRemote);
  for(i=0; i<nHash; i++){
    if( apBytes[i] ){
      any = 1;
      break;
    }
  }
  return any ? DOLTLITE_SOURCE_OK : DOLTLITE_SOURCE_NOTFOUND;
}
#else
static int SQLITE_CALLBACK csOriginGetMany(
  void *pCtx,
  int nHash,
  const unsigned char *aHash,
  unsigned char **apBytes,
  int *anBytes
){
  int i;
  UNUSED_PARAMETER(pCtx);
  UNUSED_PARAMETER(aHash);
  for(i=0; i<nHash; i++){
    apBytes[i] = 0;
    anBytes[i] = 0;
  }
  return DOLTLITE_SOURCE_IOERR;
}
#endif

static int SQLITE_CALLBACK csOriginGet(
  void *pCtx,
  const unsigned char aHash[PROLLY_HASH_SIZE],
  unsigned char **ppBytes,
  int *pnBytes
){
  int rc = csOriginGetMany(pCtx, 1, aHash, ppBytes, pnBytes);
  if( rc==DOLTLITE_SOURCE_OK && !*ppBytes ){
    rc = DOLTLITE_SOURCE_NOTFOUND;
  }
  return rc;
}

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

static int csSourceSetModeError(
  ChunkStore *cs,
  const ProllyHash *pHash
){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  if( !p ){
    p = (DoltliteChunkSourceState*)sqlite3_malloc(sizeof(*p));
    if( !p ) return SQLITE_NOMEM;
    memset(p, 0, sizeof(*p));
    cs->pChunkSource = p;
  }
  csSourceSetHashError(p, SQLITE_NOTFOUND,
      "origin chunk source is not enabled; reopen with lazy_origin=1 for",
      pHash);
  return SQLITE_NOTFOUND;
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

static int csSourceBusyRetry(
  DoltliteChunkSourceState *p,
  int nBusy
){
  if( p->db && p->db->busyHandler.xBusyHandler ){
    return sqlite3InvokeBusyHandler(&p->db->busyHandler);
  }
  if( !p->db || nBusy>=CS_SOURCE_BUSY_RETRY_COUNT ) return 0;
  sqlite3OsSleep(p->db->pVfs, CS_SOURCE_BUSY_RETRY_US);
  return 1;
}

static int csSourceLockAndRefresh(
  ChunkStore *cs,
  DoltliteChunkSourceState *p,
  int *pChanged
){
  int nBusy = 0;
  int rc;
  do {
    rc = chunkStoreLockAndRefreshChanged(cs, pChanged);
  }while( (rc&0xff)==SQLITE_BUSY && csSourceBusyRetry(p, nBusy++) );
  return rc;
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
  rc = csSourceLockAndRefresh(cs, p, &changed);
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
  rc = csSourceLockAndRefresh(&p->writer, p, 0);
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
  doltlite_chunk_source *pSource;
  u8 *pData = 0;
  u8 *apData[1];
  int nData = 0;
  int anData[1];
  int sourceRc;
  int rc;
  if( !p ){
    if( csSourceHasOrigin(cs) ) return csSourceSetModeError(cs, pHash);
    return SQLITE_NOTFOUND;
  }
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
  pSource = csSourceActive(p);
  if( !pSource ){
    if( csSourceHasOrigin(cs) && !p->originEnabled ){
      return csSourceSetModeError(cs, pHash);
    }
    return SQLITE_NOTFOUND;
  }
  sourceRc = pSource->xGet(pSource->pCtx, pHash->data, &pData, &nData);
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
  if( !csSourceGraphLockHeld(cs, p) ){
    rc = csSourcePersistMany(cs, p, pHash, apData, anData, 1);
  }
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
  doltlite_chunk_source *pSource;
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
  pSource = csSourceActive(p);
  if( !pSource ) return SQLITE_OK;
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

  sourceRc = pSource->xGetMany(
      pSource->pCtx, nMissing, (const u8*)aMissing, apData, anData);
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

  if( !csSourceGraphLockHeld(cs, p) ){
    rc = csSourcePersistMany(cs, p, aMissing, apData, anData, nMissing);
  }
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

void chunkStoreSourceCloseWriter(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  if( !p || !p->writerOpen ) return;
  chunkStoreClose(&p->writer);
  p->writerOpen = 0;
}

static int csSourceCreate(
  ChunkStore *cs,
  sqlite3 *db,
  DoltliteChunkSourceState **ppState,
  int *pChanged
){
  DoltliteChunkSourceState *p;
  int rc;
  p = (DoltliteChunkSourceState*)sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->db = db;
  rc = csSourceOpenWriter(cs, p, pChanged);
  if( rc!=SQLITE_OK ){
    if( p->writerOpen ) chunkStoreClose(&p->writer);
    sqlite3_free(p);
    return rc;
  }
  cs->pChunkSource = p;
  *ppState = p;
  return SQLITE_OK;
}

int chunkStoreSourceSet(
  ChunkStore *cs,
  sqlite3 *db,
  doltlite_chunk_source *pSource,
  int *pChanged
){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  int openChanged = 0;
  int refreshChanged = 0;
  int rc = SQLITE_OK;
  if( pChanged ) *pChanged = 0;
  if( pSource ){
    if( !p ){
      rc = csSourceCreate(cs, db, &p, pChanged);
      if( rc!=SQLITE_OK ) return rc;
    }else{
      rc = csSourceOpenWriter(cs, p, &openChanged);
      if( rc==SQLITE_OK ){
        rc = chunkStoreRefreshIfChanged(cs, &refreshChanged);
      }
      if( pChanged ) *pChanged = openChanged || refreshChanged;
      if( rc!=SQLITE_OK ) return rc;
      p->db = db;
    }
    p->pHostSource = pSource;
    return SQLITE_OK;
  }
  if( p ){
    p->pHostSource = 0;
  }
  rc = chunkStoreRefreshIfChanged(cs, pChanged);
  if( rc==SQLITE_OK && p && !p->originEnabled ){
    chunkStoreSourceClose(cs);
  }
  return rc;
}

int doltliteOriginSourceEnable(
  ChunkStore *cs,
  sqlite3 *db,
  int *pChanged
){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  int rc;
  if( pChanged ) *pChanged = 0;
  if( !p ){
    rc = csSourceCreate(cs, db, &p, pChanged);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    rc = csSourceOpenWriter(cs, p, pChanged);
    if( rc!=SQLITE_OK ) return rc;
  }
  p->db = db;
  p->originSource.iVersion = 1;
  p->originSource.pCtx = cs;
  p->originSource.xGet = csOriginGet;
  p->originSource.xGetMany = csOriginGetMany;
  p->originEnabled = 1;
  return SQLITE_OK;
}

int chunkStoreOriginSourceEnabled(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  return p && p->originEnabled && csSourceHasOrigin(cs);
}

static void csSourceDropHost(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  if( !p ) return;
  p->pHostSource = 0;
  if( !p->originEnabled ) chunkStoreSourceClose(cs);
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
    if( rc==SQLITE_OK && cs->pChunkSource
     && csSourceActive(cs->pChunkSource) ){
      installed = pSource!=0;
      rc = doltliteBtreeHydrateDeferred(pBtree);
    }
    if( rc!=SQLITE_OK && installed ){
      zSourceErr = chunkStoreSourceTakeError(cs, 0);
      csSourceDropHost(cs);
    }
    sqlite3BtreeLeave(pBtree);
    if( rc==SQLITE_OK && cs->pChunkSource
     && csSourceActive(cs->pChunkSource)
     && pBtree==sqlite3DbNameToBtree(db, 0)
     && pBtree->bDeferredRegister ){
      rc = doltliteRegister(db);
      if( rc!=SQLITE_OK ){
        sqlite3BtreeEnter(pBtree);
        zSourceErr = chunkStoreSourceTakeError(cs, 0);
        if( installed ) csSourceDropHost(cs);
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
    if( rc==SQLITE_OK && cs->pChunkSource
     && csSourceActive(cs->pChunkSource) ){
      rc = doltliteBtreeHydrateDeferred(pBtree);
    }
    sqlite3BtreeLeave(pBtree);
    if( rc==SQLITE_OK && cs->pChunkSource
     && csSourceActive(cs->pChunkSource)
     && pBtree->bDeferredRegister ){
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

struct DoltliteChunkSourceState {
  u8 originEnabled;
  int errRc;
  char *zErr;
};

static int csDisabledEnsureState(ChunkStore *cs){
  DoltliteChunkSourceState *p;
  if( cs->pChunkSource ) return SQLITE_OK;
  p = (DoltliteChunkSourceState*)sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  cs->pChunkSource = p;
  return SQLITE_OK;
}

static int csDisabledHasOrigin(ChunkStore *cs){
  const char *zUrl = 0;
  return chunkStoreFindRemote(cs, "origin", &zUrl)==SQLITE_OK
      && zUrl && zUrl[0];
}

static int csDisabledSetHashError(
  ChunkStore *cs,
  const ProllyHash *pHash,
  const char *zPrefix,
  int errRc
){
  static const char zHex[] = "0123456789abcdef";
  DoltliteChunkSourceState *p;
  char zHash[PROLLY_HASH_SIZE*2 + 1];
  int rc;
  int i;
  rc = csDisabledEnsureState(cs);
  if( rc!=SQLITE_OK ) return rc;
  p = cs->pChunkSource;
  sqlite3_free(p->zErr);
  p->zErr = 0;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    zHash[i*2] = zHex[pHash->data[i] >> 4];
    zHash[i*2 + 1] = zHex[pHash->data[i] & 0x0f];
  }
  zHash[PROLLY_HASH_SIZE*2] = 0;
  p->zErr = sqlite3_mprintf("%s %s", zPrefix, zHash);
  if( !p->zErr ) return SQLITE_NOMEM;
  p->errRc = errRc;
  return errRc;
}

int chunkStoreSourceHas(ChunkStore *cs, const ProllyHash *pHash, int *pHas){
  UNUSED_PARAMETER(cs);
  UNUSED_PARAMETER(pHash);
  *pHas = 0;
  return SQLITE_OK;
}

int chunkStoreSourceGet(ChunkStore *cs, const ProllyHash *pHash,
                        u8 **ppData, int *pnData){
  *ppData = 0;
  *pnData = 0;
  if( !csDisabledHasOrigin(cs) ) return SQLITE_NOTFOUND;
  if( chunkStoreOriginSourceEnabled(cs) ){
    return csDisabledSetHashError(
        cs, pHash, "DoltLite chunk source support is disabled for",
        SQLITE_IOERR_CHUNK_SOURCE);
  }
  return csDisabledSetHashError(
      cs, pHash,
      "origin chunk source is not enabled; reopen with lazy_origin=1 for",
      SQLITE_NOTFOUND);
}

int chunkStoreSourcePrefetchMany(ChunkStore *cs, const ProllyHash *aHash,
                                 int nHash){
  int errRc;
  int i;
  int rc;
  if( nHash<0 ) return SQLITE_MISUSE;
  if( nHash==0 || !chunkStoreOriginSourceEnabled(cs) ) return SQLITE_OK;
  for(i=0; i<nHash; i++){
    int has = 0;
    rc = chunkStoreHas(cs, &aHash[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    if( !has ) break;
  }
  if( i==nHash ) return SQLITE_OK;
  errRc = SQLITE_IOERR_CHUNK_SOURCE;
  return csDisabledSetHashError(
      cs, &aHash[i], "DoltLite chunk source support is disabled for", errRc);
}

int chunkStoreSourceSet(ChunkStore *cs, sqlite3 *db,
                        doltlite_chunk_source *pSource, int *pChanged){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  UNUSED_PARAMETER(db);
  if( pChanged ) *pChanged = 0;
  if( !pSource && (!p || !p->originEnabled) ){
    chunkStoreSourceClose(cs);
  }
  return pSource ? SQLITE_NOTFOUND : SQLITE_OK;
}

int doltliteOriginSourceEnable(ChunkStore *cs, sqlite3 *db, int *pChanged){
  DoltliteChunkSourceState *p;
  int rc;
  UNUSED_PARAMETER(db);
  if( pChanged ) *pChanged = 0;
  rc = csDisabledEnsureState(cs);
  if( rc!=SQLITE_OK ) return rc;
  p = cs->pChunkSource;
  p->originEnabled = 1;
  return SQLITE_OK;
}

int chunkStoreOriginSourceEnabled(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  return p && p->originEnabled && csDisabledHasOrigin(cs);
}

void chunkStoreSourceClose(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  if( !p ) return;
  cs->pChunkSource = 0;
  sqlite3_free(p->zErr);
  sqlite3_free(p);
}
void chunkStoreSourceCloseWriter(ChunkStore *cs){
  UNUSED_PARAMETER(cs);
}
char *chunkStoreSourceTakeError(ChunkStore *cs, int *pRc){
  DoltliteChunkSourceState *p = cs->pChunkSource;
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
char *doltliteTakeChunkSourceError(sqlite3 *db, int *pRc){
  int i;
  if( pRc ) *pRc = SQLITE_OK;
  for(i=0; i<db->nDb; i++){
    ChunkStore *cs = doltliteBtreeChunkStore(db->aDb[i].pBt);
    char *zErr;
    int rc;
    if( !cs ) continue;
    zErr = chunkStoreSourceTakeError(cs, &rc);
    if( zErr || rc!=SQLITE_OK ){
      if( pRc ) *pRc = rc;
      return zErr;
    }
  }
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
