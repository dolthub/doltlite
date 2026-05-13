
#ifdef DOLTLITE_PROLLY

#include "chunk_staging.h"
#include "chunk_store.h"
#include <string.h>

#define CS_INIT_PENDING_ALLOC 16
#define CS_INIT_WRITEBUF_SIZE 4096
#define CS_PEND_HT_INIT_BITS 12
#define CS_PEND_HT_MAX_LOAD  4

void chunkStagingInit(ChunkStaging *st){
  memset(st, 0, sizeof(*st));
}

void chunkStagingReset(ChunkStaging *st){
  memset(st, 0, sizeof(*st));
}

void chunkStagingGetPending(const ChunkStaging *st, int *pn, const ChunkIndexEntry **par){
  *pn = st->nPending;
  *par = st->aPending;
}

void chunkStagingGetRecent(const ChunkStaging *st, int *pn, const ChunkIndexEntry **par){
  *pn = st->nRecent;
  *par = st->aRecent;
}

int chunkStagingPendingCount(const ChunkStaging *st){
  return st->nPending;
}

int chunkStagingRecentCount(const ChunkStaging *st){
  return st->nRecent;
}

void chunkStagingResetAfterSweep(ChunkStaging *st){
  st->nPending = 0;
  st->nRecent = 0;
  sqlite3_free(st->aRecentHT);
  sqlite3_free(st->aRecentHTNext);
  st->aRecentHT = 0;
  st->aRecentHTNext = 0;
  st->nRecentHTBuilt = 0;
  st->nRecentHTNextAlloc = 0;
  st->nRecentHTSize = 0;
  st->nWriteBuf = 0;
}

static u32 csPendBucket(const ProllyHash *h, int nHTMask){
  return ((u32)h->data[0] | ((u32)h->data[1]<<8)
        | ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24)) & (u32)nHTMask;
}

void csPendHTClear(ChunkStore *cs){
  sqlite3_free(cs->staging.aPendingHT);
  sqlite3_free(cs->staging.aPendingHTNext);
  cs->staging.aPendingHT = 0;
  cs->staging.aPendingHTNext = 0;
  cs->staging.nPendingHTBuilt = 0;
  cs->staging.nPendingHTSize = 0;
}

void csRecentHTClear(ChunkStore *cs){
  sqlite3_free(cs->staging.aRecentHT);
  sqlite3_free(cs->staging.aRecentHTNext);
  cs->staging.aRecentHT = 0;
  cs->staging.aRecentHTNext = 0;
  cs->staging.nRecentHTBuilt = 0;
  cs->staging.nRecentHTSize = 0;
  cs->staging.nRecentHTNextAlloc = 0;
}

static int csPendHTRebuild(ChunkStore *cs){
  int i;
  memset(cs->staging.aPendingHT, 0xff, cs->staging.nPendingHTSize * sizeof(int));
  for(i=0; i<cs->staging.nPending; i++){
    u32 b = csPendBucket(&cs->staging.aPending[i].hash, cs->staging.nPendingHTSize - 1);
    cs->staging.aPendingHTNext[i] = cs->staging.aPendingHT[b];
    cs->staging.aPendingHT[b] = i;
  }
  cs->staging.nPendingHTBuilt = cs->staging.nPending;
  return SQLITE_OK;
}

static int csPendHTEnsure(ChunkStore *cs){
  int i;
  if( cs->staging.nPending==0 ) return SQLITE_OK;
  if( !cs->staging.aPendingHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->staging.aPendingHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->staging.aPendingHT ) return SQLITE_NOMEM;
    memset(cs->staging.aPendingHT, 0xff, initSize * sizeof(int));
    cs->staging.nPendingHTSize = initSize;
    cs->staging.nPendingHTBuilt = 0;
  }
  if( cs->staging.nPending > cs->staging.nPendingHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->staging.nPendingHTSize * 4;
    int *aNew = sqlite3_realloc(cs->staging.aPendingHT, newSize * (int)sizeof(int));
    if( aNew ){
      cs->staging.aPendingHT = aNew;
      cs->staging.nPendingHTSize = newSize;
      if( !cs->staging.aPendingHTNext || cs->staging.nPendingAlloc > cs->staging.nPendingHTNextAlloc ){
        int nAlloc = cs->staging.nPendingAlloc > 0 ? cs->staging.nPendingAlloc : 64;
        int *aNew2 = sqlite3_realloc(cs->staging.aPendingHTNext, nAlloc*(int)sizeof(int));
        if( !aNew2 ) return SQLITE_NOMEM;
        cs->staging.aPendingHTNext = aNew2;
        cs->staging.nPendingHTNextAlloc = nAlloc;
      }
      return csPendHTRebuild(cs);
    }

  }
  if( !cs->staging.aPendingHTNext || cs->staging.nPendingAlloc > cs->staging.nPendingHTNextAlloc ){
    int nAlloc = cs->staging.nPendingAlloc > 0 ? cs->staging.nPendingAlloc : 64;
    int *aNew = sqlite3_realloc(cs->staging.aPendingHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->staging.aPendingHTNext = aNew;
    cs->staging.nPendingHTNextAlloc = nAlloc;
  }

  for(i=cs->staging.nPendingHTBuilt; i<cs->staging.nPending; i++){
    u32 b = csPendBucket(&cs->staging.aPending[i].hash, cs->staging.nPendingHTSize - 1);
    cs->staging.aPendingHTNext[i] = cs->staging.aPendingHT[b];
    cs->staging.aPendingHT[b] = i;
  }
  cs->staging.nPendingHTBuilt = cs->staging.nPending;
  return SQLITE_OK;
}

int csSearchPending(ChunkStore *cs, const ProllyHash *pHash, int *pIdx){
  int i; u32 b; int rc;
  *pIdx = -1;
  if( cs->staging.nPending==0 ) return SQLITE_OK;
  rc = csPendHTEnsure(cs);
  if( rc!=SQLITE_OK ){

    for(i=0; i<cs->staging.nPending; i++){
      if( prollyHashCompare(&cs->staging.aPending[i].hash, pHash)==0 ){
        *pIdx = i;
        return SQLITE_OK;
      }
    }
    return rc;
  }
  b = csPendBucket(pHash, cs->staging.nPendingHTSize - 1);
  i = cs->staging.aPendingHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->staging.aPending[i].hash, pHash)==0 ){
      *pIdx = i;
      return SQLITE_OK;
    }
    i = cs->staging.aPendingHTNext[i];
  }
  return SQLITE_OK;
}

static int csRecentHTEnsure(ChunkStore *cs){
  int i;
  if( cs->staging.nRecent==0 ) return SQLITE_OK;
  if( !cs->staging.aRecentHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->staging.aRecentHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->staging.aRecentHT ) return SQLITE_NOMEM;
    memset(cs->staging.aRecentHT, 0xff, initSize * sizeof(int));
    cs->staging.nRecentHTSize = initSize;
    cs->staging.nRecentHTBuilt = 0;
  }
  if( cs->staging.nRecent > cs->staging.nRecentHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->staging.nRecentHTSize * 4;
    int *aNew = sqlite3_realloc(cs->staging.aRecentHT, newSize * (int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->staging.aRecentHT = aNew;
    cs->staging.nRecentHTSize = newSize;
    memset(cs->staging.aRecentHT, 0xff, cs->staging.nRecentHTSize * sizeof(int));
    cs->staging.nRecentHTBuilt = 0;
  }
  if( !cs->staging.aRecentHTNext || cs->staging.nRecentAlloc > cs->staging.nRecentHTNextAlloc ){
    int nAlloc = cs->staging.nRecentAlloc > 0 ? cs->staging.nRecentAlloc : 64;
    int *aNew = sqlite3_realloc(cs->staging.aRecentHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->staging.aRecentHTNext = aNew;
    cs->staging.nRecentHTNextAlloc = nAlloc;
  }
  for(i=cs->staging.nRecentHTBuilt; i<cs->staging.nRecent; i++){
    u32 b = csPendBucket(&cs->staging.aRecent[i].hash, cs->staging.nRecentHTSize - 1);
    cs->staging.aRecentHTNext[i] = cs->staging.aRecentHT[b];
    cs->staging.aRecentHT[b] = i;
  }
  cs->staging.nRecentHTBuilt = cs->staging.nRecent;
  return SQLITE_OK;
}

int csSearchRecent(ChunkStore *cs, const ProllyHash *pHash, int *pIdx){
  int i; u32 b; int rc;
  *pIdx = -1;
  if( cs->staging.nRecent==0 ) return SQLITE_OK;
  rc = csRecentHTEnsure(cs);
  if( rc!=SQLITE_OK ){
    for(i=cs->staging.nRecent-1; i>=0; i--){
      if( prollyHashCompare(&cs->staging.aRecent[i].hash, pHash)==0 ){
        *pIdx = i;
        return SQLITE_OK;
      }
    }
    return rc;
  }
  b = csPendBucket(pHash, cs->staging.nRecentHTSize - 1);
  i = cs->staging.aRecentHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->staging.aRecent[i].hash, pHash)==0 ){
      *pIdx = i;
      return SQLITE_OK;
    }
    i = cs->staging.aRecentHTNext[i];
  }
  return SQLITE_OK;
}

int csGrowPending(ChunkStore *cs){
  if( cs->staging.nPending >= cs->staging.nPendingAlloc ){
    int nNew = cs->staging.nPendingAlloc ? cs->staging.nPendingAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->staging.aPending, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->staging.aPending = aNew;
    cs->staging.nPendingAlloc = nNew;
  }
  return SQLITE_OK;
}

int csGrowRecent(ChunkStore *cs, int nAdd){
  int nNeed = cs->staging.nRecent + nAdd;
  if( nNeed > cs->staging.nRecentAlloc ){
    int nNew = cs->staging.nRecentAlloc ? cs->staging.nRecentAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew;
    while( nNew < nNeed ) nNew *= 2;
    aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->staging.aRecent, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->staging.aRecent = aNew;
    cs->staging.nRecentAlloc = nNew;
  }
  return SQLITE_OK;
}

int csGrowWriteBuf(ChunkStore *cs, int nNeeded){
  i64 nRequired = cs->staging.nWriteBuf + (i64)nNeeded;
  if( nRequired > cs->staging.nWriteBufAlloc ){
    i64 nNew = cs->staging.nWriteBufAlloc ? cs->staging.nWriteBufAlloc : CS_INIT_WRITEBUF_SIZE;
    u8 *pNew;

    while( nNew < nRequired ){
      if( nNew < 64*1024*1024 ){
        nNew *= 2;
      }else{
        nNew += nNew / 2;
      }
    }
    pNew = (u8 *)sqlite3_realloc64(cs->staging.pWriteBuf, (sqlite3_uint64)nNew);
    if( pNew == 0 ) return SQLITE_NOMEM;
    cs->staging.pWriteBuf = pNew;
    cs->staging.nWriteBufAlloc = nNew;
  }
  return SQLITE_OK;
}

#endif
