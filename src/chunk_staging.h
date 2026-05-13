
#ifndef DOLTLITE_CHUNK_STAGING_H
#define DOLTLITE_CHUNK_STAGING_H

#include "sqliteInt.h"
#include "chunk_index.h"

typedef struct ChunkStaging ChunkStaging;

struct ChunkStaging {
  ChunkIndexEntry *aPending;
  int nPending;
  int nPendingAlloc;
  ChunkIndexEntry *aRecent;
  int nRecent;
  int nRecentAlloc;
  int *aRecentHT;
  int *aRecentHTNext;
  int nRecentHTBuilt;
  int nRecentHTNextAlloc;
  int nRecentHTSize;
  int *aPendingHT;
  int *aPendingHTNext;
  int nPendingHTBuilt;
  int nPendingHTNextAlloc;
  int nPendingHTSize;
  u8 *pWriteBuf;
  i64 nWriteBuf;
  i64 nWriteBufAlloc;
  i64 nCommittedWriteBuf;
};

void chunkStagingInit(ChunkStaging *st);
void chunkStagingReset(ChunkStaging *st);

void chunkStagingGetPending(const ChunkStaging *st, int *pn, const ChunkIndexEntry **par);
void chunkStagingGetRecent(const ChunkStaging *st, int *pn, const ChunkIndexEntry **par);
int chunkStagingPendingCount(const ChunkStaging *st);
int chunkStagingRecentCount(const ChunkStaging *st);

void chunkStagingResetAfterSweep(ChunkStaging *st);

#endif
