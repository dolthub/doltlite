
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
  assert( paBase!=0 );
  assert( n>=0 && stride>0 );
  aNew = sqlite3_realloc(*paBase, (n+1)*stride);
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
  int i, n;
  char *zCopy;
  if( !cs || !zTableName ) return SQLITE_MISUSE;
  i = csFindNamedRef(cs->refs.aSequences, cs->refs.nSequences,
                     (int)sizeof(SequenceRef), zTableName);
  if( i>=0 ){
    if( newSeq > cs->refs.aSequences[i].iSeq ){
      cs->refs.aSequences[i].iSeq = newSeq;
    }
    return SQLITE_OK;
  }
  n = cs->refs.nSequences;
  if( csRefArrayGrow((void**)&cs->refs.aSequences, n, (int)sizeof(SequenceRef)) ) return SQLITE_NOMEM;
  zCopy = sqlite3_mprintf("%s", zTableName);
  if( !zCopy ) return SQLITE_NOMEM;
  cs->refs.aSequences[n].zTableName = zCopy;
  cs->refs.aSequences[n].iSeq = newSeq;
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
