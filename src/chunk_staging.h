
#ifndef DOLTLITE_CHUNK_STAGING_H
#define DOLTLITE_CHUNK_STAGING_H

#include "sqliteInt.h"
#include "chunk_index.h"

typedef struct ChunkStaging ChunkStaging;

struct ChunkStaging {
  ChunkIndexEntry *aPending;
  int nPending;
  int nPendingAlloc;
  /* Parallel to aPending: trailing zero bytes of each staged chunk that are
  ** not present in pWriteBuf (the chunk's logical size is e->size; only
  ** e->size - aPendingZeroTail[i] bytes follow its length word in the
  ** buffer). Zero for ordinary chunks; written as literal zeros at commit,
  ** so nothing on disk or in the index changes shape. */
  i64 *aPendingZeroTail;
  ChunkIndexEntry *aRecent;
  int nRecent;
  int nRecentAlloc;
  int nRecentUncommitted;
  i64 iUncommittedStart;
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

void chunkStagingGetPending(const ChunkStaging *st, int *pn, const ChunkIndexEntry **par);
void chunkStagingGetRecent(const ChunkStaging *st, int *pn, const ChunkIndexEntry **par);
int chunkStagingPendingCount(const ChunkStaging *st);
int chunkStagingRecentCount(const ChunkStaging *st);

void chunkStagingResetAfterSweep(ChunkStaging *st);

#include "prolly_hash.h"

struct ChunkStore;
void csPendHTClear(struct ChunkStore *cs);
void csPendHTReset(struct ChunkStore *cs);
void csRecentHTClear(struct ChunkStore *cs);
int csGrowPending(struct ChunkStore *cs);
int csGrowRecent(struct ChunkStore *cs, int nAdd);
int csGrowWriteBuf(struct ChunkStore *cs, int nNeeded);
int csSearchPending(struct ChunkStore *cs, const ProllyHash *pHash, int *pIdx);
int csSearchRecent(struct ChunkStore *cs, const ProllyHash *pHash, int *pIdx);

#endif
