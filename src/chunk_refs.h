
#ifndef DOLTLITE_CHUNK_REFS_H
#define DOLTLITE_CHUNK_REFS_H

#include "sqliteInt.h"
#include "prolly_hash.h"

typedef struct BranchRef BranchRef;
typedef struct TagRef TagRef;
typedef struct RemoteRef RemoteRef;
typedef struct TrackingBranch TrackingBranch;
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
};

void refsTableInit(RefsTable *rt);
void refsTableReset(RefsTable *rt);

void refsTableGetBranches(const RefsTable *rt, int *pn, const BranchRef **par);
void refsTableGetTags(const RefsTable *rt, int *pn, const TagRef **par);
void refsTableGetRemotes(const RefsTable *rt, int *pn, const RemoteRef **par);
void refsTableGetTracking(const RefsTable *rt, int *pn, const TrackingBranch **par);

int refsTableBranchCount(const RefsTable *rt);
int refsTableTagCount(const RefsTable *rt);
int refsTableRemoteCount(const RefsTable *rt);
int refsTableTrackingCount(const RefsTable *rt);

const char *refsTableGetDefaultBranchName(const RefsTable *rt);
const ProllyHash *refsTableGetHash(const RefsTable *rt);
const ProllyHash *refsTableGetCommittedHash(const RefsTable *rt);

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
};

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
void csMarkRefsCommitted(struct ChunkStore *cs);
void csRestoreCommittedRefsHash(struct ChunkStore *cs);
void csDetachSavedRefsState(struct ChunkStore *cs, SavedRefsState *pSaved);
int csDeserializeRefs(struct ChunkStore *cs, const u8 *data, int nData);
int csDeserializeRefsIntoTemp(struct ChunkStore *pTmp, const u8 *data, int nData);

#endif
