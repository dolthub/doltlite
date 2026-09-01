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
  DoltliteChunkSourceEntry *aHash[CS_SOURCE_CACHE_BUCKETS];
  DoltliteChunkSourceEntry *pLruHead;
  DoltliteChunkSourceEntry *pLruTail;
  i64 nCacheBytes;
  int nCacheEntry;
  ChunkStore writer;
  u8 writerOpen;
  u8 memoryOnly;
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

static void csSourceSetHashError(
  DoltliteChunkSourceState *p,
  const char *zPrefix,
  const ProllyHash *pHash
){
  static const char zHex[] = "0123456789abcdef";
  char zHash[PROLLY_HASH_SIZE*2 + 1];
  int i;
  sqlite3_free(p->zErr);
  p->zErr = 0;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    zHash[i*2] = zHex[pHash->data[i] >> 4];
    zHash[i*2 + 1] = zHex[pHash->data[i] & 0x0f];
  }
  zHash[PROLLY_HASH_SIZE*2] = 0;
  p->zErr = sqlite3_mprintf("%s %s", zPrefix, zHash);
}

static int csSourceOpenWriter(
  ChunkStore *cs,
  DoltliteChunkSourceState *p
){
  int rc;
  if( p->writerOpen || p->memoryOnly ) return SQLITE_OK;
  if( cs->readOnly || cs->isMemory || cs->isBuffer || cs->movedReadOnly ){
    p->memoryOnly = 1;
    return SQLITE_OK;
  }
  if( cs->file.pFile==0 ) return SQLITE_OK;
  if( cs->lockDepth>0 ) return SQLITE_BUSY;
  rc = chunkStoreLockAndRefresh(cs);
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
  return rc;
}

static int csSourcePersistOne(
  ChunkStore *cs,
  DoltliteChunkSourceState *p,
  const ProllyHash *pHash,
  const u8 *pData,
  int nData
){
  ProllyHash putHash;
  int has = 0;
  int rc;
  if( p->memoryOnly ) return SQLITE_OK;
  rc = csSourceOpenWriter(cs, p);
  if( rc!=SQLITE_OK || !p->writerOpen ) return rc;
  rc = chunkStoreLockAndRefresh(&p->writer);
  if( rc==SQLITE_OK ) rc = chunkStoreForceRefresh(&p->writer);
  if( rc==SQLITE_OK && p->writer.movedReadOnly ){
    p->memoryOnly = 1;
  }
  if( rc==SQLITE_OK && !p->memoryOnly ){
    rc = chunkStoreHas(&p->writer, pHash, &has);
  }
  if( rc==SQLITE_OK && !p->memoryOnly && !has ){
    rc = chunkStorePut(&p->writer, pData, nData, &putHash);
    if( rc==SQLITE_OK
     && memcmp(&putHash, pHash, sizeof(*pHash))!=0 ) rc = SQLITE_CORRUPT;
  }
  if( rc==SQLITE_OK && !p->memoryOnly && !has ){
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
  int nData = 0;
  int sourceRc;
  int rc;
  if( !p ) return SQLITE_NOTFOUND;
  sqlite3_free(p->zErr);
  p->zErr = 0;
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
  if( cs->lockDepth>0 ){
    csSourceSetHashError(p,
      "chunk source fetch blocked by active graph lock for", pHash);
    return SQLITE_BUSY;
  }
  sourceRc = p->pSource->xGet(p->pSource->pCtx, pHash->data,
                              &pData, &nData);
  if( sourceRc==DOLTLITE_SOURCE_NOTFOUND ){
    sqlite3_free(pData);
    csSourceSetHashError(p, "chunk source did not contain", pHash);
    return SQLITE_NOTFOUND;
  }
  if( sourceRc!=DOLTLITE_SOURCE_OK || !pData || nData<0 ){
    sqlite3_free(pData);
    csSourceSetHashError(p, "chunk source I/O error for", pHash);
    return SQLITE_IOERR_CHUNK_SOURCE;
  }
  rc = chunkStoreVerifyChunk(pHash, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = csSourcePersistOne(cs, p, pHash, pData, nData);
  if( rc==SQLITE_OK ) rc = csSourceCachePut(p, pHash, pData, nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pData);
    return rc;
  }
  *ppData = pData;
  *pnData = nData;
  return SQLITE_OK;
}

void chunkStoreSourceClose(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  if( !p ) return;
  cs->pChunkSource = 0;
  if( p->writerOpen ) chunkStoreClose(&p->writer);
  while( p->pLruTail ) csSourceCacheRemove(p, p->pLruTail);
  sqlite3_free(p->zErr);
  sqlite3_free(p);
}

int chunkStoreSourceSet(
  ChunkStore *cs,
  doltlite_chunk_source *pSource
){
  DoltliteChunkSourceState *p;
  int changed = 0;
  int rc = SQLITE_OK;
  chunkStoreSourceClose(cs);
  if( !pSource ) return chunkStoreRefreshIfChanged(cs, &changed);
  p = (DoltliteChunkSourceState*)sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->pSource = pSource;
  rc = csSourceOpenWriter(cs, p);
  if( rc!=SQLITE_OK ){
    if( p->writerOpen ) chunkStoreClose(&p->writer);
    sqlite3_free(p);
    return rc;
  }
  cs->pChunkSource = p;
  return SQLITE_OK;
}

char *chunkStoreSourceTakeError(ChunkStore *cs){
  DoltliteChunkSourceState *p = cs->pChunkSource;
  char *zErr;
  if( !p ) return 0;
  zErr = p->zErr;
  p->zErr = 0;
  return zErr;
}

char *doltliteTakeChunkSourceError(sqlite3 *db){
  int i;
  for(i=0; i<db->nDb; i++){
    ChunkStore *cs = doltliteBtreeChunkStore(db->aDb[i].pBt);
    char *zErr;
    if( !cs ) continue;
    zErr = chunkStoreSourceTakeError(cs);
    if( zErr ) return zErr;
  }
  return 0;
}

SQLITE_API int SQLITE_APICALL doltlite_set_chunk_source(
  sqlite3 *db,
  const char *zDbName,
  doltlite_chunk_source *pSource
){
  Btree *pBtree;
  int rc = SQLITE_OK;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) ) return SQLITE_MISUSE_BKPT;
#endif
  sqlite3_mutex_enter(db->mutex);
  pBtree = sqlite3DbNameToBtree(db, zDbName);
  if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
    rc = SQLITE_NOTFOUND;
    sqlite3ErrorWithMsg(db, rc, "database is not a DoltLite database");
  }else if( pBtree->inTransaction!=TRANS_NONE ){
    rc = SQLITE_BUSY;
    sqlite3ErrorWithMsg(db, rc,
                        "cannot change chunk source during a transaction");
  }else if( pSource && (pSource->iVersion!=1
                      || !pSource->xGet || !pSource->xGetMany) ){
    rc = SQLITE_MISUSE;
    sqlite3ErrorWithMsg(db, rc, "invalid DoltLite chunk source version 1");
  }else{
    sqlite3BtreeEnter(pBtree);
    rc = chunkStoreSourceSet(doltliteBtreeChunkStore(pBtree), pSource);
    sqlite3BtreeLeave(pBtree);
    if( rc==SQLITE_OK ) sqlite3Error(db, SQLITE_OK);
    else sqlite3ErrorWithMsg(db, rc, "%s", sqlite3ErrStr(rc));
  }
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

int chunkStoreSourceSet(ChunkStore *cs, doltlite_chunk_source *pSource){
  UNUSED_PARAMETER(cs);
  return pSource ? SQLITE_NOTFOUND : SQLITE_OK;
}

void chunkStoreSourceClose(ChunkStore *cs){ UNUSED_PARAMETER(cs); }
char *chunkStoreSourceTakeError(ChunkStore *cs){
  UNUSED_PARAMETER(cs);
  return 0;
}
char *doltliteTakeChunkSourceError(sqlite3 *db){
  UNUSED_PARAMETER(db);
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

#endif
#endif
