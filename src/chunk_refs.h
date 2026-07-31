
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

/* Non-versioned AUTOINCREMENT counter shared across all branches. */
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

/* Index of the ref whose leading char* name field equals zName, or -1. */
int csFindNamedRef(const void *aBase, int n, int stride, const char *zName);

/* Grow a packed ref array by one zeroed element; returns SQLITE_OK/NOMEM. */
int csRefArrayGrow(void **paBase, int n, int stride);

int refsTableBranchCount(const RefsTable *rt);
int refsTableRemoteCount(const RefsTable *rt);

/* Return the stored sequence for zTableName, or 0 if absent. */
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
int csEnsureDefaultBranch(struct ChunkStore *cs);

void csFreeBranches(struct ChunkStore *cs);
void csFreeTags(struct ChunkStore *cs);
void csFreeRemotes(struct ChunkStore *cs);
void csFreeTracking(struct ChunkStore *cs);
void csFreeSequences(struct ChunkStore *cs);

/* Look up the stored sequence for zTableName, or 0 if absent. */
i64 chunkStoreGetSequenceValue(struct ChunkStore *cs, const char *zTableName);
/* Set the stored sequence to max(existing, newSeq); creates row if absent.
** Caller must hold the chunk-store lock. */
int chunkStoreBumpSequence(struct ChunkStore *cs, const char *zTableName,
                           i64 newSeq);
/* Delete the row for zTableName if present (mirrors SQLite's
** "DELETE FROM sqlite_sequence WHERE name=…" on DROP TABLE). */
void chunkStoreDropSequence(struct ChunkStore *cs, const char *zTableName);
/* Rename zOld → zNew (mirrors ALTER TABLE RENAME). */
int chunkStoreRenameSequence(struct ChunkStore *cs, const char *zOld,
                             const char *zNew);
void csMarkRefsCommitted(struct ChunkStore *cs);
void csRestoreCommittedRefsHash(struct ChunkStore *cs);
void csDetachSavedRefsState(struct ChunkStore *cs, SavedRefsState *pSaved);
int csDeserializeRefs(struct ChunkStore *cs, const u8 *data, int nData);
int csDeserializeRefsIntoTemp(struct ChunkStore *pTmp, const u8 *data, int nData);

#endif
