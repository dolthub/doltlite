
#ifndef DOLTLITE_CHUNK_WAL_H
#define DOLTLITE_CHUNK_WAL_H

#include "sqliteInt.h"
#include "chunk_index.h"
#include "chunk_refs.h"

typedef struct WalState WalState;
typedef struct ChunkStoreReplayState ChunkStoreReplayState;
typedef struct ChunkStoreReloadState ChunkStoreReloadState;

struct WalState {
  i64 iWalOffset;
  i64 nWalData;
  u8 recoveredMidStream;
  u8 cleanCloseMarker;
};

struct ChunkStoreReplayState {
  ChunkIndexEntry *aIndex;
  int nIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  SavedRefsState refs;
};

struct ChunkStoreReloadState {
  sqlite3_file *pFile;
  ChunkIndexEntry *aIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  SavedRefsState refs;
};

i64 walStateGetOffset(const WalState *w);
i64 walStateGetDataSize(const WalState *w);

void walStateSetOffset(WalState *w, i64 iOffset);
void walStateSetDataSize(WalState *w, i64 nData);

struct ChunkStore;
int csReplayWal(struct ChunkStore *cs);
/* Replay only WAL records at offsets >= startPos (relative to iWalOffset)
** on top of the current in-memory state; csReplayWal is startPos==0 with
** fresh state. */
int csReplayWalRange(struct ChunkStore *cs, i64 startPos);
void csCaptureReloadState(struct ChunkStore *cs, ChunkStoreReloadState *pSaved);
void csFreeReloadState(ChunkStoreReloadState *pSaved);
void csAdoptOpenedStoreState(struct ChunkStore *pDst, struct ChunkStore *pSrc);

#endif
