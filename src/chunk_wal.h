
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
  i64 iCheckpointOffset;
  i64 nCheckpointIndex;
  i64 iCheckpointReplay;
  i64 iCheckpointDataEnd;
  int nCheckpointEntries;
  ProllyHash checkpointHash;
  u32 checkpointMagic;
  u8 recoveredMidStream;
  u8 cleanCloseMarker;
};

struct ChunkStoreReplayState {
  ChunkIndexEntry *aIndex;
  int nIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  ChunkIndexLazy lazy;
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
int csReplayWalSkipping(struct ChunkStore *cs, i64 iSkipStart, i64 iSkipEnd);
int csTryLoadWalCheckpoint(struct ChunkStore *cs, int *pLoaded,
                           i64 *pSkipStart, i64 *pSkipEnd);
int csWriteWalCheckpoint(struct ChunkStore *cs, int sectorSize, int *pWritten);
int csWalCheckpointDue(const struct ChunkStore *cs);
void csStampWalCheckpoint(const struct ChunkStore *cs, u8 *aManifest);
void csCaptureReloadState(struct ChunkStore *cs, ChunkStoreReloadState *pSaved);
void csFreeReloadState(ChunkStoreReloadState *pSaved);
void csAdoptOpenedStoreState(struct ChunkStore *pDst, struct ChunkStore *pSrc);

#endif
