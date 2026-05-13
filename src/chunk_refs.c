
#ifdef DOLTLITE_PROLLY

#include "chunk_refs.h"
#include <string.h>

void refsTableInit(RefsTable *rt){
  memset(rt, 0, sizeof(*rt));
}

void refsTableReset(RefsTable *rt){
  memset(rt, 0, sizeof(*rt));
}

void refsTableGetBranches(const RefsTable *rt, int *pn, const BranchRef **par){
  *pn = rt->nBranches;
  *par = rt->aBranches;
}

void refsTableGetTags(const RefsTable *rt, int *pn, const TagRef **par){
  *pn = rt->nTags;
  *par = rt->aTags;
}

void refsTableGetRemotes(const RefsTable *rt, int *pn, const RemoteRef **par){
  *pn = rt->nRemotes;
  *par = rt->aRemotes;
}

void refsTableGetTracking(const RefsTable *rt, int *pn, const TrackingBranch **par){
  *pn = rt->nTracking;
  *par = rt->aTracking;
}

const char *refsTableGetDefaultBranchName(const RefsTable *rt){
  return rt->zDefaultBranch;
}

const ProllyHash *refsTableGetHash(const RefsTable *rt){
  return &rt->refsHash;
}

const ProllyHash *refsTableGetCommittedHash(const RefsTable *rt){
  return &rt->committedRefsHash;
}

int refsTableBranchCount(const RefsTable *rt){
  return rt->nBranches;
}

int refsTableTagCount(const RefsTable *rt){
  return rt->nTags;
}

int refsTableRemoteCount(const RefsTable *rt){
  return rt->nRemotes;
}

int refsTableTrackingCount(const RefsTable *rt){
  return rt->nTracking;
}

void refsTableSetHash(RefsTable *rt, const ProllyHash *h){
  memcpy(&rt->refsHash, h, sizeof(ProllyHash));
}

#endif
