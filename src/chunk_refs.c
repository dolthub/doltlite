
#ifdef DOLTLITE_PROLLY

#include "chunk_refs.h"
#include "chunk_store.h"
#include <limits.h>
#include <string.h>

int csFindNamedRef(const void *aBase, int n, int stride, const char *zName){
  const char *p = (const char*)aBase;
  int i;
  assert( n>=0 && stride>0 );
  assert( n==0 || aBase!=0 );
  if( !zName ) return -1;
  for(i=0; i<n; i++){
    const char *zHave = *(const char *const*)(p + (size_t)i*stride);
    if( zHave && strcmp(zHave, zName)==0 ) return i;
  }
  return -1;
}

int csRefArrayGrow(void **paBase, int n, int stride){
  void *aNew;
  i64 nByte;
  assert( paBase!=0 );
  assert( n>=0 && stride>0 );
  /* (n+1)*stride as int wraps; realloc then gets a small size. */
  nByte = ((i64)n + 1) * (i64)stride;
  if( nByte > (i64)INT_MAX ) return SQLITE_TOOBIG;
  aNew = sqlite3_realloc(*paBase, (int)nByte);
  if( !aNew ) return SQLITE_NOMEM;
  *paBase = aNew;
  memset((char*)aNew + (size_t)n*stride, 0, stride);
  return SQLITE_OK;
}

void refsTableGetBranches(const RefsTable *rt, int *pn, const BranchRef **par){
  assert( rt!=0 && pn!=0 && par!=0 );
  assert( rt->nBranches>=0 );
  assert( rt->nBranches==0 || rt->aBranches!=0 );
  *pn = rt->nBranches;
  *par = rt->aBranches;
}

void refsTableGetTags(const RefsTable *rt, int *pn, const TagRef **par){
  assert( rt!=0 && pn!=0 && par!=0 );
  assert( rt->nTags>=0 );
  assert( rt->nTags==0 || rt->aTags!=0 );
  *pn = rt->nTags;
  *par = rt->aTags;
}

void refsTableGetRemotes(const RefsTable *rt, int *pn, const RemoteRef **par){
  assert( rt!=0 && pn!=0 && par!=0 );
  assert( rt->nRemotes>=0 );
  assert( rt->nRemotes==0 || rt->aRemotes!=0 );
  *pn = rt->nRemotes;
  *par = rt->aRemotes;
}

void refsTableGetTracking(const RefsTable *rt, int *pn, const TrackingBranch **par){
  assert( rt!=0 && pn!=0 && par!=0 );
  assert( rt->nTracking>=0 );
  assert( rt->nTracking==0 || rt->aTracking!=0 );
  *pn = rt->nTracking;
  *par = rt->aTracking;
}

void refsTableGetSequences(const RefsTable *rt, int *pn, const SequenceRef **par){
  assert( rt!=0 && pn!=0 && par!=0 );
  assert( rt->nSequences>=0 );
  assert( rt->nSequences==0 || rt->aSequences!=0 );
  *pn = rt->nSequences;
  *par = rt->aSequences;
}

i64 refsTableGetSequence(const RefsTable *rt, const char *zTableName){
  int i = csFindNamedRef(rt->aSequences, rt->nSequences,
                         (int)sizeof(SequenceRef), zTableName);
  return i<0 ? 0 : rt->aSequences[i].iSeq;
}

const ProllyHash *refsTableGetHash(const RefsTable *rt){
  return &rt->refsHash;
}

int refsTableBranchCount(const RefsTable *rt){
  return rt->nBranches;
}

int refsTableRemoteCount(const RefsTable *rt){
  return rt->nRemotes;
}

void refsTableSetHash(RefsTable *rt, const ProllyHash *h){
  assert( rt!=0 && h!=0 );
  memcpy(&rt->refsHash, h, sizeof(ProllyHash));
}

void csFreeBranches(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nBranches; k++) sqlite3_free(cs->refs.aBranches[k].zName);
  sqlite3_free(cs->refs.aBranches);
  cs->refs.aBranches = 0;
  cs->refs.nBranches = 0;
}

void csFreeTags(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nTags; k++){
    sqlite3_free(cs->refs.aTags[k].zName);
    sqlite3_free(cs->refs.aTags[k].zTagger);
    sqlite3_free(cs->refs.aTags[k].zEmail);
    sqlite3_free(cs->refs.aTags[k].zMessage);
  }
  sqlite3_free(cs->refs.aTags);
  cs->refs.aTags = 0;
  cs->refs.nTags = 0;
}

void csFreeRemotes(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nRemotes; k++){
    sqlite3_free(cs->refs.aRemotes[k].zName);
    sqlite3_free(cs->refs.aRemotes[k].zUrl);
  }
  sqlite3_free(cs->refs.aRemotes);
  cs->refs.aRemotes = 0;
  cs->refs.nRemotes = 0;
}

void csFreeTracking(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nTracking; k++){
    sqlite3_free(cs->refs.aTracking[k].zRemote);
    sqlite3_free(cs->refs.aTracking[k].zBranch);
  }
  sqlite3_free(cs->refs.aTracking);
  cs->refs.aTracking = 0;
  cs->refs.nTracking = 0;
}

void csFreeSequences(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nSequences; k++){
    sqlite3_free(cs->refs.aSequences[k].zTableName);
  }
  sqlite3_free(cs->refs.aSequences);
  cs->refs.aSequences = 0;
  cs->refs.nSequences = 0;
}

i64 chunkStoreGetSequenceValue(ChunkStore *cs, const char *zTableName){
  if( !cs || !zTableName ) return 0;
  return refsTableGetSequence(&cs->refs, zTableName);
}

int chunkStoreBumpSequence(ChunkStore *cs, const char *zTableName,
                           i64 newSeq){
  int i;
  if( !cs || !zTableName ) return SQLITE_MISUSE;
  i = csFindNamedRef(cs->refs.aSequences, cs->refs.nSequences,
                     (int)sizeof(SequenceRef), zTableName);
  if( i>=0 ){
    if( newSeq > cs->refs.aSequences[i].iSeq ){
      cs->refs.aSequences[i].iSeq = newSeq;
    }
    return SQLITE_OK;
  }
  return chunkStoreSetSequence(cs, zTableName, newSeq);
}

int chunkStoreSetSequence(ChunkStore *cs, const char *zTableName, i64 seq){
  int i, n;
  char *zCopy;
  if( !cs || !zTableName ) return SQLITE_MISUSE;
  i = csFindNamedRef(cs->refs.aSequences, cs->refs.nSequences,
                     (int)sizeof(SequenceRef), zTableName);
  if( i>=0 ){
    cs->refs.aSequences[i].iSeq = seq;
    return SQLITE_OK;
  }
  n = cs->refs.nSequences;
  if( csRefArrayGrow((void**)&cs->refs.aSequences, n, (int)sizeof(SequenceRef)) ) return SQLITE_NOMEM;
  zCopy = sqlite3_mprintf("%s", zTableName);
  if( !zCopy ) return SQLITE_NOMEM;
  cs->refs.aSequences[n].zTableName = zCopy;
  cs->refs.aSequences[n].iSeq = seq;
  cs->refs.nSequences++;
  return SQLITE_OK;
}

void chunkStoreDropSequence(ChunkStore *cs, const char *zTableName){
  int i;
  if( !cs || !zTableName ) return;
  i = csFindNamedRef(cs->refs.aSequences, cs->refs.nSequences,
                     (int)sizeof(SequenceRef), zTableName);
  if( i<0 ) return;
  sqlite3_free(cs->refs.aSequences[i].zTableName);
  if( i < cs->refs.nSequences-1 ){
    memmove(&cs->refs.aSequences[i], &cs->refs.aSequences[i+1],
            (cs->refs.nSequences-i-1)*sizeof(SequenceRef));
  }
  cs->refs.nSequences--;
}

int chunkStoreRenameSequence(ChunkStore *cs, const char *zOld,
                             const char *zNew){
  int i;
  char *zCopy;
  if( !cs || !zOld || !zNew ) return SQLITE_MISUSE;
  i = csFindNamedRef(cs->refs.aSequences, cs->refs.nSequences,
                     (int)sizeof(SequenceRef), zOld);
  if( i<0 ) return SQLITE_OK;
  zCopy = sqlite3_mprintf("%s", zNew);
  if( !zCopy ) return SQLITE_NOMEM;
  sqlite3_free(cs->refs.aSequences[i].zTableName);
  cs->refs.aSequences[i].zTableName = zCopy;
  return SQLITE_OK;
}

void csMarkRefsCommitted(ChunkStore *cs){
  cs->refs.committedRefsHash = cs->refs.refsHash;
}

void csRestoreCommittedRefsHash(ChunkStore *cs){
  cs->refs.refsHash = cs->refs.committedRefsHash;
}

void csCaptureSavedRefsState(ChunkStore *cs, SavedRefsState *pSaved){
  memset(pSaved, 0, sizeof(*pSaved));
  REFS_OWNED_COPY(*pSaved, cs->refs);
}

void csDetachSavedRefsState(ChunkStore *cs, SavedRefsState *pSaved){
  csCaptureSavedRefsState(cs, pSaved);
  REFS_OWNED_CLEAR(cs->refs);
}

void csRestoreSavedRefsState(ChunkStore *cs, const SavedRefsState *pSaved){
  REFS_OWNED_COPY(cs->refs, *pSaved);
}

void csFreeSavedRefsState(SavedRefsState *pSaved){
  ChunkStore refsStore;
  memset(&refsStore, 0, sizeof(refsStore));
  REFS_OWNED_COPY(refsStore.refs, *pSaved);
  csFreeRefsState(&refsStore);
  memset(pSaved, 0, sizeof(*pSaved));
}

void csFreeRefsState(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  csFreeSequences(cs);
  sqlite3_free(cs->refs.zDefaultBranch);
  cs->refs.zDefaultBranch = 0;
}

int csEnsureDefaultBranch(ChunkStore *cs){
  if( !cs->refs.zDefaultBranch ){
    cs->refs.zDefaultBranch = sqlite3_mprintf("main");
    if( !cs->refs.zDefaultBranch ) return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

typedef struct CsRefsReader CsRefsReader;
struct CsRefsReader {
  const u8 *p;
  int n;
};

static int refsTake(CsRefsReader *pReader, int n, const u8 **ppData){
  if( n<0 || n>pReader->n ) return SQLITE_CORRUPT;
  if( ppData ) *ppData = pReader->p;
  pReader->p += n;
  pReader->n -= n;
  return SQLITE_OK;
}

static int refsReadU32(CsRefsReader *pReader, int *pValue){
  const u8 *p;
  u32 value;
  int rc = refsTake(pReader, 4, &p);
  if( rc!=SQLITE_OK ) return rc;
  value = CS_READ_U32(p);
  if( value>INT_MAX ) return SQLITE_CORRUPT;
  *pValue = (int)value;
  return SQLITE_OK;
}

static int refsReadCount(CsRefsReader *pReader, int nMin, int *pCount){
  int rc = refsReadU32(pReader, pCount);
  if( rc!=SQLITE_OK ) return rc;
  if( *pCount>pReader->n/nMin ) return SQLITE_CORRUPT;
  return SQLITE_OK;
}

static int refsReadStringView(
  CsRefsReader *pReader,
  char **pzOut,
  const u8 **ppView,
  int *pnView
){
  const u8 *p;
  char *z;
  int n;
  int rc = refsReadU32(pReader, &n);
  if( rc!=SQLITE_OK ) return rc;
  rc = refsTake(pReader, n, &p);
  if( rc!=SQLITE_OK ) return rc;
  if( ppView ) *ppView = p;
  if( pnView ) *pnView = n;
  if( !pzOut ) return SQLITE_OK;
  z = sqlite3_malloc64((sqlite3_uint64)n + 1);
  if( !z ) return SQLITE_NOMEM;
  memcpy(z, p, n);
  z[n] = 0;
  *pzOut = z;
  return SQLITE_OK;
}

static int refsReadString(CsRefsReader *pReader, char **pzOut){
  return refsReadStringView(pReader, pzOut, 0, 0);
}

static int refsReadHash(
  CsRefsReader *pReader,
  ProllyHash *pHash,
  CsRefsHashCb xHash,
  void *pCtx
){
  const u8 *p;
  ProllyHash hash;
  int rc = refsTake(pReader, PROLLY_HASH_SIZE, &p);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(hash.data, p, PROLLY_HASH_SIZE);
  if( pHash ) *pHash = hash;
  return xHash ? xHash(pCtx, &hash) : SQLITE_OK;
}

static int refsReadI64(CsRefsReader *pReader, i64 *pValue){
  const u8 *p;
  int rc = refsTake(pReader, 8, &p);
  if( rc!=SQLITE_OK ) return rc;
  if( pValue ) *pValue = CS_READ_I64(p);
  return SQLITE_OK;
}

static int refsAllocArray(void **ppArray, int n, sqlite3_uint64 nElem){
  sqlite3_uint64 nByte;
  if( n==0 ) return SQLITE_OK;
  nByte = (sqlite3_uint64)n * nElem;
  *ppArray = sqlite3_malloc64(nByte);
  if( !*ppArray ) return SQLITE_NOMEM;
  memset(*ppArray, 0, (size_t)nByte);
  return SQLITE_OK;
}

int csDecodeRefsV7(
  const u8 *data,
  int nData,
  RefsTable *pRefs,
  CsRefsHashCb xHash,
  void *pCtx
){
  CsRefsReader reader;
  const u8 *pVersion;
  const u8 *pDefaultBranch;
  int nDefaultBranch;
  int nBranches;
  int nTags;
  int nRemotes;
  int nTracking;
  int nSequences;
  int hasDefaultBranch = 0;
  int i;
  int rc;

  if( !data || nData<0 ) return SQLITE_CORRUPT;
  reader.p = data;
  reader.n = nData;
  rc = refsTake(&reader, 1, &pVersion);
  if( rc!=SQLITE_OK || pVersion[0]!=7 ) return SQLITE_CORRUPT;
  rc = refsReadStringView(&reader,
      pRefs ? &pRefs->zDefaultBranch : 0,
      &pDefaultBranch, &nDefaultBranch);
  if( rc!=SQLITE_OK ) return rc;

  rc = refsReadCount(&reader, 4 + 2*PROLLY_HASH_SIZE, &nBranches);
  if( rc!=SQLITE_OK ) return rc;
  if( pRefs ){
    rc = refsAllocArray((void**)&pRefs->aBranches, nBranches,
                        sizeof(BranchRef));
    if( rc!=SQLITE_OK ) return rc;
    pRefs->nBranches = nBranches;
  }
  for(i=0; i<nBranches; i++){
    BranchRef *pBranch = pRefs ? &pRefs->aBranches[i] : 0;
    const u8 *pBranchName;
    int nBranchName;
    rc = refsReadStringView(&reader, pBranch ? &pBranch->zName : 0,
                            &pBranchName, &nBranchName);
    if( rc==SQLITE_OK
     && nBranchName==nDefaultBranch
     && memcmp(pBranchName, pDefaultBranch, nBranchName)==0 ){
      hasDefaultBranch = 1;
    }
    if( rc==SQLITE_OK ){
      rc = refsReadHash(&reader, pBranch ? &pBranch->commitHash : 0,
                        xHash, pCtx);
    }
    if( rc==SQLITE_OK ){
      rc = refsReadHash(&reader, pBranch ? &pBranch->workingSetHash : 0,
                        xHash, pCtx);
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  if( nBranches>0 && !hasDefaultBranch ) return SQLITE_CORRUPT;

  rc = refsReadCount(&reader, 24 + PROLLY_HASH_SIZE, &nTags);
  if( rc!=SQLITE_OK ) return rc;
  if( pRefs ){
    rc = refsAllocArray((void**)&pRefs->aTags, nTags, sizeof(TagRef));
    if( rc!=SQLITE_OK ) return rc;
    pRefs->nTags = nTags;
  }
  for(i=0; i<nTags; i++){
    TagRef *pTag = pRefs ? &pRefs->aTags[i] : 0;
    rc = refsReadString(&reader, pTag ? &pTag->zName : 0);
    if( rc==SQLITE_OK ){
      rc = refsReadHash(&reader, pTag ? &pTag->commitHash : 0,
                        xHash, pCtx);
    }
    if( rc==SQLITE_OK ){
      rc = refsReadString(&reader, pTag ? &pTag->zTagger : 0);
    }
    if( rc==SQLITE_OK ){
      rc = refsReadString(&reader, pTag ? &pTag->zEmail : 0);
    }
    if( rc==SQLITE_OK ){
      rc = refsReadI64(&reader, pTag ? &pTag->timestamp : 0);
    }
    if( rc==SQLITE_OK ){
      rc = refsReadString(&reader, pTag ? &pTag->zMessage : 0);
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = refsReadCount(&reader, 8, &nRemotes);
  if( rc!=SQLITE_OK ) return rc;
  if( pRefs ){
    rc = refsAllocArray((void**)&pRefs->aRemotes, nRemotes,
                        sizeof(RemoteRef));
    if( rc!=SQLITE_OK ) return rc;
    pRefs->nRemotes = nRemotes;
  }
  for(i=0; i<nRemotes; i++){
    RemoteRef *pRemote = pRefs ? &pRefs->aRemotes[i] : 0;
    rc = refsReadString(&reader, pRemote ? &pRemote->zName : 0);
    if( rc==SQLITE_OK ){
      rc = refsReadString(&reader, pRemote ? &pRemote->zUrl : 0);
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = refsReadCount(&reader, 8 + PROLLY_HASH_SIZE, &nTracking);
  if( rc!=SQLITE_OK ) return rc;
  if( pRefs ){
    rc = refsAllocArray((void**)&pRefs->aTracking, nTracking,
                        sizeof(TrackingBranch));
    if( rc!=SQLITE_OK ) return rc;
    pRefs->nTracking = nTracking;
  }
  for(i=0; i<nTracking; i++){
    TrackingBranch *pTracking = pRefs ? &pRefs->aTracking[i] : 0;
    rc = refsReadString(&reader, pTracking ? &pTracking->zRemote : 0);
    if( rc==SQLITE_OK ){
      rc = refsReadString(&reader, pTracking ? &pTracking->zBranch : 0);
    }
    if( rc==SQLITE_OK ){
      rc = refsReadHash(&reader, pTracking ? &pTracking->commitHash : 0,
                        xHash, pCtx);
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = refsReadCount(&reader, 12, &nSequences);
  if( rc!=SQLITE_OK ) return rc;
  if( pRefs ){
    rc = refsAllocArray((void**)&pRefs->aSequences, nSequences,
                        sizeof(SequenceRef));
    if( rc!=SQLITE_OK ) return rc;
    pRefs->nSequences = nSequences;
  }
  for(i=0; i<nSequences; i++){
    SequenceRef *pSequence = pRefs ? &pRefs->aSequences[i] : 0;
    rc = refsReadString(&reader,
                        pSequence ? &pSequence->zTableName : 0);
    if( rc==SQLITE_OK ){
      rc = refsReadI64(&reader, pSequence ? &pSequence->iSeq : 0);
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  return reader.n==0 ? SQLITE_OK : SQLITE_CORRUPT;
}

int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData){
  ChunkStore tmp;
  int rc;
  memset(&tmp, 0, sizeof(tmp));
  rc = csDecodeRefsV7(data, nData, &tmp.refs, 0, 0);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&tmp);
    return rc;
  }
  csFreeRefsState(cs);
  csAdoptRefsState(cs, &tmp);
  return SQLITE_OK;
}

int csDeserializeRefsIntoTemp(ChunkStore *pTmp, const u8 *data, int nData){
  int rc;
  memset(pTmp, 0, sizeof(*pTmp));
  rc = csDecodeRefsV7(data, nData, &pTmp->refs, 0, 0);
  if( rc!=SQLITE_OK ) csFreeRefsState(pTmp);
  return rc;
}

void csAdoptRefsState(ChunkStore *pDst, ChunkStore *pSrc){
  REFS_OWNED_COPY(pDst->refs, pSrc->refs);
  REFS_OWNED_CLEAR(pSrc->refs);
}

int csReplaceRefsStateFromBlob(
  ChunkStore *cs,
  const u8 *data,
  int nData,
  int markCommitted
){
  ChunkStore tmp;
  int rc = csDeserializeRefsIntoTemp(&tmp, data, nData);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&tmp);
    return rc;
  }
  csFreeRefsState(cs);
  csAdoptRefsState(cs, &tmp);
  if( markCommitted ){
    csMarkRefsCommitted(cs);
  }
  return SQLITE_OK;
}

#endif

/* Three-way merge of uncommitted refs onto reloaded disk. Untouched refs
** keep disk (wholesale restore clobbered peer tips). Local changes apply
** unless a peer moved the same ref (BUSY_SNAPSHOT). Sequences merge by
** max. Detect conflicts first so disk is untouched unless the merge applies. */

static int refsMergeHashEqual(const ProllyHash *a, const ProllyHash *b){
  return prollyHashCompare(a, b)==0;
}

static int refsMergeStrEqual(const char *a, const char *b){
  if( a==0 && b==0 ) return 1;
  if( a==0 || b==0 ) return 0;
  return strcmp(a, b)==0;
}

static int refsMergeBranchEqual(const BranchRef *a, const BranchRef *b){
  return refsMergeHashEqual(&a->commitHash, &b->commitHash)
      && refsMergeHashEqual(&a->workingSetHash, &b->workingSetHash);
}

static int refsMergeTagEqual(const TagRef *a, const TagRef *b){
  return refsMergeHashEqual(&a->commitHash, &b->commitHash)
      && a->timestamp==b->timestamp
      && refsMergeStrEqual(a->zTagger, b->zTagger)
      && refsMergeStrEqual(a->zEmail, b->zEmail)
      && refsMergeStrEqual(a->zMessage, b->zMessage);
}

static int refsMergeRemoteEqual(const RemoteRef *a, const RemoteRef *b){
  return refsMergeStrEqual(a->zUrl, b->zUrl);
}

static int refsMergeTrackingEqual(const TrackingBranch *a, const TrackingBranch *b){
  return refsMergeHashEqual(&a->commitHash, &b->commitHash);
}

static int refsMergeFindTrackingIdx(
  const TrackingBranch *a, int n,
  const char *zRemote, const char *zBranch
){
  int i;
  for(i=0; i<n; i++){
    if( refsMergeStrEqual(a[i].zRemote, zRemote)
     && refsMergeStrEqual(a[i].zBranch, zBranch) ){
      return i;
    }
  }
  return -1;
}

/* Name-keyed category: checkOnly detects conflicts, else apply. */
typedef struct RefsMergeCat RefsMergeCat;
struct RefsMergeCat {
  const void *aLocal; int nLocal;
  const void *aBase;  int nBase;
  void **paDisk;      int *pnDisk;
  int stride;
  int (*xEqual)(const void*, const void*);
  int (*xCopyInto)(void *pDst, const void *pSrc);
  void (*xFreeEntry)(void*);
  int (*xFind)(const void *aBase, int n, const void *pEntry);
};

static int refsMergeCategory(RefsMergeCat *p, int checkOnly){
  int i;
  const u8 *aLocal = (const u8*)p->aLocal;
  const u8 *aBase = (const u8*)p->aBase;

  for(i=0; i<p->nLocal; i++){
    const void *pL = aLocal + (size_t)i*p->stride;
    int iB = p->xFind(p->aBase, p->nBase, pL);
    const void *pB = iB<0 ? 0 : (const void*)(aBase + (size_t)iB*p->stride);
    int iD;
    void *pD;
    if( pB && p->xEqual(pL, pB) ) continue;
    iD = p->xFind(*p->paDisk, *p->pnDisk, pL);
    pD = iD<0 ? 0 : (void*)((u8*)(*p->paDisk) + (size_t)iD*p->stride);
    if( pD ){
      if( pB && !p->xEqual(pD, pB) && !p->xEqual(pD, pL) ){
        return SQLITE_BUSY_SNAPSHOT;
      }
      if( !pB && !p->xEqual(pD, pL) ){
        return SQLITE_BUSY_SNAPSHOT;
      }
      if( !checkOnly ){
        p->xFreeEntry(pD);
        if( p->xCopyInto(pD, pL) ) return SQLITE_NOMEM;
      }
    }else{
      if( pB ){
        return SQLITE_BUSY_SNAPSHOT;
      }
      if( !checkOnly ){
        int rc = csRefArrayGrow(p->paDisk, *p->pnDisk, p->stride);
        if( rc!=SQLITE_OK ) return rc;
        pD = (void*)((u8*)(*p->paDisk) + (size_t)(*p->pnDisk)*p->stride);
        if( p->xCopyInto(pD, pL) ) return SQLITE_NOMEM;
        (*p->pnDisk)++;
      }
    }
  }
  for(i=0; i<p->nBase; i++){
    const void *pB = aBase + (size_t)i*p->stride;
    int iL = p->xFind(p->aLocal, p->nLocal, pB);
    int iD;
    if( iL>=0 ) continue;
    iD = p->xFind(*p->paDisk, *p->pnDisk, pB);
    if( iD>=0 ){
      void *pD = (void*)((u8*)(*p->paDisk) + (size_t)iD*p->stride);
      if( !p->xEqual(pD, pB) ){
        return SQLITE_BUSY_SNAPSHOT;
      }
      if( !checkOnly ){
        p->xFreeEntry(pD);
        memmove(pD, (u8*)pD + p->stride,
                (size_t)(*p->pnDisk - iD - 1)*p->stride);
        (*p->pnDisk)--;
      }
    }
  }
  return SQLITE_OK;
}

static int refsMergeBranchFind(const void *aBase, int n, const void *pEntry){
  return csFindNamedRef(aBase, n, sizeof(BranchRef), *(char* const*)pEntry);
}

static int refsMergeTagFind(const void *aBase, int n, const void *pEntry){
  return csFindNamedRef(aBase, n, sizeof(TagRef), *(char* const*)pEntry);
}

static int refsMergeRemoteFind(const void *aBase, int n, const void *pEntry){
  return csFindNamedRef(aBase, n, sizeof(RemoteRef), *(char* const*)pEntry);
}

static int refsMergeTrackingFind(const void *aBase, int n, const void *pEntry){
  const TrackingBranch *e = (const TrackingBranch*)pEntry;
  return refsMergeFindTrackingIdx((const TrackingBranch*)aBase, n,
                         e->zRemote, e->zBranch);
}

static int refsMergeBranchCopy(void *pDst, const void *pSrc){
  const BranchRef *s = (const BranchRef*)pSrc;
  BranchRef *d = (BranchRef*)pDst;
  memset(d, 0, sizeof(*d));
  d->zName = sqlite3_mprintf("%s", s->zName);
  d->commitHash = s->commitHash;
  d->workingSetHash = s->workingSetHash;
  return d->zName==0;
}

static void refsMergeBranchFree(void *pEntry){
  sqlite3_free(((BranchRef*)pEntry)->zName);
}

static int refsMergeTagCopy(void *pDst, const void *pSrc){
  const TagRef *s = (const TagRef*)pSrc;
  TagRef *d = (TagRef*)pDst;
  memset(d, 0, sizeof(*d));
  d->zName = sqlite3_mprintf("%s", s->zName);
  d->commitHash = s->commitHash;
  d->timestamp = s->timestamp;
  d->zTagger = s->zTagger ? sqlite3_mprintf("%s", s->zTagger) : 0;
  d->zEmail = s->zEmail ? sqlite3_mprintf("%s", s->zEmail) : 0;
  d->zMessage = s->zMessage ? sqlite3_mprintf("%s", s->zMessage) : 0;
  return d->zName==0
      || (s->zTagger && !d->zTagger)
      || (s->zEmail && !d->zEmail)
      || (s->zMessage && !d->zMessage);
}

static void refsMergeTagFree(void *pEntry){
  TagRef *t = (TagRef*)pEntry;
  sqlite3_free(t->zName);
  sqlite3_free(t->zTagger);
  sqlite3_free(t->zEmail);
  sqlite3_free(t->zMessage);
}

static int refsMergeRemoteCopy(void *pDst, const void *pSrc){
  const RemoteRef *s = (const RemoteRef*)pSrc;
  RemoteRef *d = (RemoteRef*)pDst;
  memset(d, 0, sizeof(*d));
  d->zName = sqlite3_mprintf("%s", s->zName);
  d->zUrl = s->zUrl ? sqlite3_mprintf("%s", s->zUrl) : 0;
  return d->zName==0 || (s->zUrl && !d->zUrl);
}

static void refsMergeRemoteFree(void *pEntry){
  RemoteRef *r = (RemoteRef*)pEntry;
  sqlite3_free(r->zName);
  sqlite3_free(r->zUrl);
}

static int refsMergeTrackingCopy(void *pDst, const void *pSrc){
  const TrackingBranch *s = (const TrackingBranch*)pSrc;
  TrackingBranch *d = (TrackingBranch*)pDst;
  memset(d, 0, sizeof(*d));
  d->zRemote = sqlite3_mprintf("%s", s->zRemote);
  d->zBranch = sqlite3_mprintf("%s", s->zBranch);
  d->commitHash = s->commitHash;
  return d->zRemote==0 || d->zBranch==0;
}

static void refsMergeTrackingFree(void *pEntry){
  TrackingBranch *t = (TrackingBranch*)pEntry;
  sqlite3_free(t->zRemote);
  sqlite3_free(t->zBranch);
}

int csMergeSavedRefsOntoDisk(
  ChunkStore *cs,
  const SavedRefsState *pLocal,
  const RefsTable *pBase
){
  RefsMergeCat aCat[4];
  int pass, i, rc;

  memset(aCat, 0, sizeof(aCat));
  aCat[0].aLocal = pLocal->aBranches; aCat[0].nLocal = pLocal->nBranches;
  aCat[0].aBase = pBase->aBranches;   aCat[0].nBase = pBase->nBranches;
  aCat[0].paDisk = (void**)&cs->refs.aBranches;
  aCat[0].pnDisk = &cs->refs.nBranches;
  aCat[0].stride = (int)sizeof(BranchRef);
  aCat[0].xEqual = (int(*)(const void*,const void*))refsMergeBranchEqual;
  aCat[0].xCopyInto = refsMergeBranchCopy;
  aCat[0].xFreeEntry = refsMergeBranchFree;
  aCat[0].xFind = refsMergeBranchFind;

  aCat[1].aLocal = pLocal->aTags; aCat[1].nLocal = pLocal->nTags;
  aCat[1].aBase = pBase->aTags;   aCat[1].nBase = pBase->nTags;
  aCat[1].paDisk = (void**)&cs->refs.aTags;
  aCat[1].pnDisk = &cs->refs.nTags;
  aCat[1].stride = (int)sizeof(TagRef);
  aCat[1].xEqual = (int(*)(const void*,const void*))refsMergeTagEqual;
  aCat[1].xCopyInto = refsMergeTagCopy;
  aCat[1].xFreeEntry = refsMergeTagFree;
  aCat[1].xFind = refsMergeTagFind;

  aCat[2].aLocal = pLocal->aRemotes; aCat[2].nLocal = pLocal->nRemotes;
  aCat[2].aBase = pBase->aRemotes;   aCat[2].nBase = pBase->nRemotes;
  aCat[2].paDisk = (void**)&cs->refs.aRemotes;
  aCat[2].pnDisk = &cs->refs.nRemotes;
  aCat[2].stride = (int)sizeof(RemoteRef);
  aCat[2].xEqual = (int(*)(const void*,const void*))refsMergeRemoteEqual;
  aCat[2].xCopyInto = refsMergeRemoteCopy;
  aCat[2].xFreeEntry = refsMergeRemoteFree;
  aCat[2].xFind = refsMergeRemoteFind;

  aCat[3].aLocal = pLocal->aTracking; aCat[3].nLocal = pLocal->nTracking;
  aCat[3].aBase = pBase->aTracking;   aCat[3].nBase = pBase->nTracking;
  aCat[3].paDisk = (void**)&cs->refs.aTracking;
  aCat[3].pnDisk = &cs->refs.nTracking;
  aCat[3].stride = (int)sizeof(TrackingBranch);
  aCat[3].xEqual = (int(*)(const void*,const void*))refsMergeTrackingEqual;
  aCat[3].xCopyInto = refsMergeTrackingCopy;
  aCat[3].xFreeEntry = refsMergeTrackingFree;
  aCat[3].xFind = refsMergeTrackingFind;

  {
    const char *zL = pLocal->zDefaultBranch;
    const char *zB = pBase->zDefaultBranch;
    const char *zD = cs->refs.zDefaultBranch;
    if( !refsMergeStrEqual(zL, zB)
     && !refsMergeStrEqual(zD, zB)
     && !refsMergeStrEqual(zD, zL) ){
      return SQLITE_BUSY_SNAPSHOT;
    }
  }

  for(pass=0; pass<2; pass++){
    for(i=0; i<4; i++){
      rc = refsMergeCategory(&aCat[i], pass==0);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  if( pLocal->zDefaultBranch
   && !refsMergeStrEqual(pLocal->zDefaultBranch, pBase->zDefaultBranch) ){
    char *zDup = sqlite3_mprintf("%s", pLocal->zDefaultBranch);
    if( !zDup ) return SQLITE_NOMEM;
    sqlite3_free(cs->refs.zDefaultBranch);
    cs->refs.zDefaultBranch = zDup;
  }

  /* Isolated category merges can still pair a new default with a deleted
  ** branch; the decoder requires a live default, so that is a conflict. */
  if( cs->refs.zDefaultBranch
   && csFindNamedRef(cs->refs.aBranches, cs->refs.nBranches,
                     (int)sizeof(BranchRef), cs->refs.zDefaultBranch)<0 ){
    return SQLITE_BUSY_SNAPSHOT;
  }

  /* Sequences merge by max. Locally dropped counters go away; untouched
  ** ones stay the peer's (do not resurrect a dropped high-water). */
  for(i=0; i<pLocal->nSequences; i++){
    const SequenceRef *pL = &pLocal->aSequences[i];
    int iBase = csFindNamedRef(pBase->aSequences, pBase->nSequences,
                               (int)sizeof(SequenceRef), pL->zTableName);
    int rc2;
    if( iBase>=0 && pBase->aSequences[iBase].iSeq==pL->iSeq ) continue;
    rc2 = chunkStoreBumpSequence(cs, pL->zTableName, pL->iSeq);
    if( rc2!=SQLITE_OK ) return rc2;
  }
  for(i=0; i<pBase->nSequences; i++){
    const SequenceRef *pB = &pBase->aSequences[i];
    if( csFindNamedRef(pLocal->aSequences, pLocal->nSequences,
                       (int)sizeof(SequenceRef), pB->zTableName)<0 ){
      chunkStoreDropSequence(cs, pB->zTableName);
    }
  }

  return SQLITE_OK;
}
