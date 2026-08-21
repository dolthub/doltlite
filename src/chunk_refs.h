
#ifndef DOLTLITE_CHUNK_REFS_H
#define DOLTLITE_CHUNK_REFS_H

#include "sqliteInt.h"
#include "prolly_hash.h"

typedef struct BranchRef BranchRef;
typedef struct TagRef TagRef;
typedef struct RemoteRef RemoteRef;
typedef struct TrackingBranch TrackingBranch;
typedef struct SequenceRef SequenceRef;
typedef struct RefsTable RefsTable;
typedef int (*CsRefsHashCb)(void *ctx, const ProllyHash *pHash);

struct BranchRef {
  char *zName;
  ProllyHash commitHash;
  ProllyHash workingSetHash;
};

struct TagRef {
  char *zName;
  ProllyHash commitHash;
  char *zTagger;
  char *zEmail;
  i64 timestamp;
  char *zMessage;
};

struct RemoteRef {
  char *zName;
  char *zUrl;
};

struct TrackingBranch {
  char *zRemote;
  char *zBranch;
  ProllyHash commitHash;
};

/* Non-versioned AUTOINCREMENT high-water, shared across branches. */
struct SequenceRef {
  char *zTableName;
  i64 iSeq;
};

struct RefsTable {
  ProllyHash refsHash;
  ProllyHash committedRefsHash;
  BranchRef *aBranches;
  int nBranches;
  char *zDefaultBranch;
  TagRef *aTags;
  int nTags;
  RemoteRef *aRemotes;
  int nRemotes;
  TrackingBranch *aTracking;
  int nTracking;
  SequenceRef *aSequences;
  int nSequences;
};

void refsTableGetBranches(const RefsTable *rt, int *pn, const BranchRef **par);
void refsTableGetTags(const RefsTable *rt, int *pn, const TagRef **par);
void refsTableGetRemotes(const RefsTable *rt, int *pn, const RemoteRef **par);
void refsTableGetTracking(const RefsTable *rt, int *pn, const TrackingBranch **par);
void refsTableGetSequences(const RefsTable *rt, int *pn, const SequenceRef **par);

int csFindNamedRef(const void *aBase, int n, int stride, const char *zName);
int csRefArrayGrow(void **paBase, int n, int stride);

int refsTableBranchCount(const RefsTable *rt);
int refsTableRemoteCount(const RefsTable *rt);

i64 refsTableGetSequence(const RefsTable *rt, const char *zTableName);

const ProllyHash *refsTableGetHash(const RefsTable *rt);

void refsTableSetHash(RefsTable *rt, const ProllyHash *h);

typedef struct SavedRefsState SavedRefsState;
struct SavedRefsState {
  char *zDefaultBranch;
  BranchRef *aBranches;
  int nBranches;
  TagRef *aTags;
  int nTags;
  RemoteRef *aRemotes;
  int nRemotes;
  TrackingBranch *aTracking;
  int nTracking;
  SequenceRef *aSequences;
  int nSequences;
};

#define REFS_OWNED_COPY(D, S) do {           \
  (D).zDefaultBranch = (S).zDefaultBranch;   \
  (D).aBranches      = (S).aBranches;        \
  (D).nBranches      = (S).nBranches;        \
  (D).aTags          = (S).aTags;            \
  (D).nTags          = (S).nTags;            \
  (D).aRemotes       = (S).aRemotes;         \
  (D).nRemotes       = (S).nRemotes;         \
  (D).aTracking      = (S).aTracking;        \
  (D).nTracking      = (S).nTracking;        \
  (D).aSequences     = (S).aSequences;       \
  (D).nSequences     = (S).nSequences;       \
} while(0)

#define REFS_OWNED_CLEAR(D) do {             \
  (D).zDefaultBranch = 0;                    \
  (D).aBranches      = 0;                    \
  (D).nBranches      = 0;                    \
  (D).aTags          = 0;                    \
  (D).nTags          = 0;                    \
  (D).aRemotes       = 0;                    \
  (D).nRemotes       = 0;                    \
  (D).aTracking      = 0;                    \
  (D).nTracking      = 0;                    \
  (D).aSequences     = 0;                    \
  (D).nSequences     = 0;                    \
} while(0)

struct ChunkStore;
void csCaptureSavedRefsState(struct ChunkStore *cs, SavedRefsState *pSaved);
void csRestoreSavedRefsState(struct ChunkStore *cs, const SavedRefsState *pSaved);
void csFreeSavedRefsState(SavedRefsState *pSaved);
int csReplaceRefsStateFromBlob(struct ChunkStore *cs, const u8 *data, int nData,
                               int adopt);
void csFreeRefsState(struct ChunkStore *cs);
void csAdoptRefsState(struct ChunkStore *pDst, struct ChunkStore *pSrc);
int csMergeSavedRefsOntoDisk(struct ChunkStore *cs,
                             const SavedRefsState *pLocal,
                             const RefsTable *pBase);
int csEnsureDefaultBranch(struct ChunkStore *cs);

void csFreeBranches(struct ChunkStore *cs);
void csFreeTags(struct ChunkStore *cs);
void csFreeRemotes(struct ChunkStore *cs);
void csFreeTracking(struct ChunkStore *cs);
void csFreeSequences(struct ChunkStore *cs);

i64 chunkStoreGetSequenceValue(struct ChunkStore *cs, const char *zTableName);
/* max(existing, newSeq); creates the row if absent. Caller holds the lock. */
int chunkStoreBumpSequence(struct ChunkStore *cs, const char *zTableName,
                           i64 newSeq);
void chunkStoreDropSequence(struct ChunkStore *cs, const char *zTableName);
int chunkStoreRenameSequence(struct ChunkStore *cs, const char *zOld,
                             const char *zNew);
void csMarkRefsCommitted(struct ChunkStore *cs);
void csRestoreCommittedRefsHash(struct ChunkStore *cs);
void csDetachSavedRefsState(struct ChunkStore *cs, SavedRefsState *pSaved);
int csDeserializeRefs(struct ChunkStore *cs, const u8 *data, int nData);
int csDeserializeRefsIntoTemp(struct ChunkStore *pTmp, const u8 *data, int nData);
int csDecodeRefsV7(const u8 *data, int nData, RefsTable *pRefs,
                   CsRefsHashCb xHash, void *pCtx);

#endif
