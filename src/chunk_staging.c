
#ifdef DOLTLITE_PROLLY

#include "chunk_staging.h"
#include <string.h>

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

#endif
