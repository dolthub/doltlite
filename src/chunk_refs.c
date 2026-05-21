
#ifdef DOLTLITE_PROLLY

#include "chunk_refs.h"
#include "chunk_store.h"
#include <string.h>

#define CS_READ_U32(p) (             \
  (u32)(((const u8*)(p))[0])       | \
  (u32)(((const u8*)(p))[1]) << 8  | \
  (u32)(((const u8*)(p))[2]) << 16 | \
  (u32)(((const u8*)(p))[3]) << 24   \
)

#define CS_READ_I64(p) (                  \
  ((i64)(((const u8*)(p))[0]))       |    \
  ((i64)(((const u8*)(p))[1]) << 8)  |    \
  ((i64)(((const u8*)(p))[2]) << 16) |    \
  ((i64)(((const u8*)(p))[3]) << 24) |    \
  ((i64)(((const u8*)(p))[4]) << 32) |    \
  ((i64)(((const u8*)(p))[5]) << 40) |    \
  ((i64)(((const u8*)(p))[6]) << 48) |    \
  ((i64)(((const u8*)(p))[7]) << 56)      \
)

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

void refsTableGetSequences(const RefsTable *rt, int *pn, const SequenceRef **par){
  *pn = rt->nSequences;
  *par = rt->aSequences;
}

i64 refsTableGetSequence(const RefsTable *rt, const char *zTableName){
  int i;
  if( !zTableName ) return 0;
  for(i=0; i<rt->nSequences; i++){
    if( rt->aSequences[i].zTableName
     && strcmp(rt->aSequences[i].zTableName, zTableName)==0 ){
      return rt->aSequences[i].iSeq;
    }
  }
  return 0;
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

int refsTableSequenceCount(const RefsTable *rt){
  return rt->nSequences;
}

void refsTableSetHash(RefsTable *rt, const ProllyHash *h){
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
  SequenceRef *aNew;
  char *zCopy;
  if( !cs || !zTableName ) return SQLITE_MISUSE;
  for(i=0; i<cs->refs.nSequences; i++){
    if( cs->refs.aSequences[i].zTableName
     && strcmp(cs->refs.aSequences[i].zTableName, zTableName)==0 ){
      if( newSeq > cs->refs.aSequences[i].iSeq ){
        cs->refs.aSequences[i].iSeq = newSeq;
      }
      return SQLITE_OK;
    }
  }
  aNew = sqlite3_realloc(cs->refs.aSequences,
                         (cs->refs.nSequences+1)*(int)sizeof(SequenceRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->refs.aSequences = aNew;
  memset(&aNew[cs->refs.nSequences], 0, sizeof(SequenceRef));
  zCopy = sqlite3_mprintf("%s", zTableName);
  if( !zCopy ) return SQLITE_NOMEM;
  aNew[cs->refs.nSequences].zTableName = zCopy;
  aNew[cs->refs.nSequences].iSeq = newSeq;
  cs->refs.nSequences++;
  return SQLITE_OK;
}

void chunkStoreDropSequence(ChunkStore *cs, const char *zTableName){
  int i;
  if( !cs || !zTableName ) return;
  for(i=0; i<cs->refs.nSequences; i++){
    if( cs->refs.aSequences[i].zTableName
     && strcmp(cs->refs.aSequences[i].zTableName, zTableName)==0 ){
      sqlite3_free(cs->refs.aSequences[i].zTableName);
      if( i < cs->refs.nSequences-1 ){
        memmove(&cs->refs.aSequences[i], &cs->refs.aSequences[i+1],
                (cs->refs.nSequences-i-1)*sizeof(SequenceRef));
      }
      cs->refs.nSequences--;
      return;
    }
  }
}

int chunkStoreRenameSequence(ChunkStore *cs, const char *zOld,
                             const char *zNew){
  int i;
  if( !cs || !zOld || !zNew ) return SQLITE_MISUSE;
  for(i=0; i<cs->refs.nSequences; i++){
    if( cs->refs.aSequences[i].zTableName
     && strcmp(cs->refs.aSequences[i].zTableName, zOld)==0 ){
      char *zCopy = sqlite3_mprintf("%s", zNew);
      if( !zCopy ) return SQLITE_NOMEM;
      sqlite3_free(cs->refs.aSequences[i].zTableName);
      cs->refs.aSequences[i].zTableName = zCopy;
      return SQLITE_OK;
    }
  }
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
  pSaved->zDefaultBranch = cs->refs.zDefaultBranch;
  pSaved->aBranches = cs->refs.aBranches;
  pSaved->nBranches = cs->refs.nBranches;
  pSaved->aTags = cs->refs.aTags;
  pSaved->nTags = cs->refs.nTags;
  pSaved->aRemotes = cs->refs.aRemotes;
  pSaved->nRemotes = cs->refs.nRemotes;
  pSaved->aTracking = cs->refs.aTracking;
  pSaved->nTracking = cs->refs.nTracking;
  pSaved->aSequences = cs->refs.aSequences;
  pSaved->nSequences = cs->refs.nSequences;
}

void csDetachSavedRefsState(ChunkStore *cs, SavedRefsState *pSaved){
  csCaptureSavedRefsState(cs, pSaved);
  cs->refs.zDefaultBranch = 0;
  cs->refs.aBranches = 0;
  cs->refs.nBranches = 0;
  cs->refs.aTags = 0;
  cs->refs.nTags = 0;
  cs->refs.aRemotes = 0;
  cs->refs.nRemotes = 0;
  cs->refs.aTracking = 0;
  cs->refs.nTracking = 0;
  cs->refs.aSequences = 0;
  cs->refs.nSequences = 0;
}

void csRestoreSavedRefsState(ChunkStore *cs, const SavedRefsState *pSaved){
  cs->refs.zDefaultBranch = pSaved->zDefaultBranch;
  cs->refs.aBranches = pSaved->aBranches;
  cs->refs.nBranches = pSaved->nBranches;
  cs->refs.aTags = pSaved->aTags;
  cs->refs.nTags = pSaved->nTags;
  cs->refs.aRemotes = pSaved->aRemotes;
  cs->refs.nRemotes = pSaved->nRemotes;
  cs->refs.aTracking = pSaved->aTracking;
  cs->refs.nTracking = pSaved->nTracking;
  cs->refs.aSequences = pSaved->aSequences;
  cs->refs.nSequences = pSaved->nSequences;
}

void csFreeSavedRefsState(SavedRefsState *pSaved){
  ChunkStore refsStore;
  memset(&refsStore, 0, sizeof(refsStore));
  refsStore.refs.zDefaultBranch = pSaved->zDefaultBranch;
  refsStore.refs.aBranches = pSaved->aBranches;
  refsStore.refs.nBranches = pSaved->nBranches;
  refsStore.refs.aTags = pSaved->aTags;
  refsStore.refs.nTags = pSaved->nTags;
  refsStore.refs.aRemotes = pSaved->aRemotes;
  refsStore.refs.nRemotes = pSaved->nRemotes;
  refsStore.refs.aTracking = pSaved->aTracking;
  refsStore.refs.nTracking = pSaved->nTracking;
  refsStore.refs.aSequences = pSaved->aSequences;
  refsStore.refs.nSequences = pSaved->nSequences;
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

int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData){
  const u8 *bufCur = data;
  int defLen, nBranches, nTags, i;
  u8 version;
  if( nData<5 ) return SQLITE_CORRUPT;
  version = *bufCur++;
  /* v7 added the SequenceRef section (shared AUTOINCREMENT counters).
  ** 0.11.0 dropped support for older formats; rebuild from scratch on
  ** older databases. */
  if( version!=7 ) return SQLITE_CORRUPT;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  defLen = (int)CS_READ_U32(bufCur); bufCur+=4;
  if( defLen<0 ) return SQLITE_CORRUPT;
  if( bufCur+defLen>data+nData ) return SQLITE_CORRUPT;
  sqlite3_free(cs->refs.zDefaultBranch);
  cs->refs.zDefaultBranch = sqlite3_malloc(defLen+1);
  if(!cs->refs.zDefaultBranch) return SQLITE_NOMEM;
  memcpy(cs->refs.zDefaultBranch, bufCur, defLen); cs->refs.zDefaultBranch[defLen]=0; bufCur+=defLen;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  nBranches = (int)CS_READ_U32(bufCur); bufCur+=4;
  if( nBranches<0 || nBranches>(int)(data+nData-bufCur)/4 ) return SQLITE_CORRUPT;
  csFreeBranches(cs);
  if( nBranches>0 ){
    cs->refs.aBranches = sqlite3_malloc(nBranches*(int)sizeof(struct BranchRef));
    if(!cs->refs.aBranches) return SQLITE_NOMEM;
    for(i=0;i<nBranches;i++){
      int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
      nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
      if( nameLen<0 ) return SQLITE_CORRUPT;
      if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
      memset(&cs->refs.aBranches[i], 0, sizeof(struct BranchRef));
      cs->refs.aBranches[i].zName=sqlite3_malloc(nameLen+1);
      if(!cs->refs.aBranches[i].zName) return SQLITE_NOMEM;
      memcpy(cs->refs.aBranches[i].zName,bufCur,nameLen); cs->refs.aBranches[i].zName[nameLen]=0; bufCur+=nameLen;
      memcpy(cs->refs.aBranches[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      if( bufCur+PROLLY_HASH_SIZE<=data+nData ){
        memcpy(cs->refs.aBranches[i].workingSetHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      }
      cs->refs.nBranches++;
    }
  }

  csFreeTags(cs);
  if( bufCur+4<=data+nData ){
    nTags = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nTags<0 || nTags>(int)(data+nData-bufCur)/4 ) return SQLITE_CORRUPT;
    if( nTags>0 ){
      cs->refs.aTags = sqlite3_malloc(nTags*(int)sizeof(struct TagRef));
      if(!cs->refs.aTags) return SQLITE_NOMEM;
      memset(cs->refs.aTags, 0, nTags*(int)sizeof(struct TagRef));
      for(i=0;i<nTags;i++){
        int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if( nameLen<0 ) return SQLITE_CORRUPT;
        if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
        cs->refs.aTags[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->refs.aTags[i].zName) return SQLITE_NOMEM;
        memcpy(cs->refs.aTags[i].zName,bufCur,nameLen); cs->refs.aTags[i].zName[nameLen]=0; bufCur+=nameLen;
        memcpy(cs->refs.aTags[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
        {
          int taggerLen, emailLen, messageLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          taggerLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if( taggerLen<0 ) return SQLITE_CORRUPT;
          if(bufCur+taggerLen>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].zTagger=sqlite3_malloc(taggerLen+1);
          if(!cs->refs.aTags[i].zTagger) return SQLITE_NOMEM;
          memcpy(cs->refs.aTags[i].zTagger,bufCur,taggerLen); cs->refs.aTags[i].zTagger[taggerLen]=0; bufCur+=taggerLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          emailLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if( emailLen<0 ) return SQLITE_CORRUPT;
          if(bufCur+emailLen>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].zEmail=sqlite3_malloc(emailLen+1);
          if(!cs->refs.aTags[i].zEmail) return SQLITE_NOMEM;
          memcpy(cs->refs.aTags[i].zEmail,bufCur,emailLen); cs->refs.aTags[i].zEmail[emailLen]=0; bufCur+=emailLen;
          if(bufCur+8>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].timestamp=CS_READ_I64(bufCur); bufCur+=8;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          messageLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if( messageLen<0 ) return SQLITE_CORRUPT;
          if(bufCur+messageLen>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].zMessage=sqlite3_malloc(messageLen+1);
          if(!cs->refs.aTags[i].zMessage) return SQLITE_NOMEM;
          memcpy(cs->refs.aTags[i].zMessage,bufCur,messageLen); cs->refs.aTags[i].zMessage[messageLen]=0; bufCur+=messageLen;
        }
        cs->refs.nTags++;
      }
    }
  }

  csFreeRemotes(cs);
  csFreeTracking(cs);
  if( bufCur+4<=data+nData ){
    int nRemotes = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nRemotes<0 || nRemotes>(int)(data+nData-bufCur)/4 ) return SQLITE_CORRUPT;
    if( nRemotes>0 ){
      cs->refs.aRemotes = sqlite3_malloc(nRemotes*(int)sizeof(struct RemoteRef));
      if(!cs->refs.aRemotes) return SQLITE_NOMEM;
      memset(cs->refs.aRemotes, 0, nRemotes*(int)sizeof(struct RemoteRef));
      for(i=0;i<nRemotes;i++){
        int nameLen, urlLen;
        if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if( nameLen<0 ) return SQLITE_CORRUPT;
        if(bufCur+nameLen+4>data+nData) return SQLITE_CORRUPT;
        cs->refs.aRemotes[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->refs.aRemotes[i].zName) return SQLITE_NOMEM;
        memcpy(cs->refs.aRemotes[i].zName,bufCur,nameLen); cs->refs.aRemotes[i].zName[nameLen]=0; bufCur+=nameLen;
        urlLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if( urlLen<0 ) return SQLITE_CORRUPT;
        if(bufCur+urlLen>data+nData) return SQLITE_CORRUPT;
        cs->refs.aRemotes[i].zUrl=sqlite3_malloc(urlLen+1);
        if(!cs->refs.aRemotes[i].zUrl) return SQLITE_NOMEM;
        memcpy(cs->refs.aRemotes[i].zUrl,bufCur,urlLen); cs->refs.aRemotes[i].zUrl[urlLen]=0; bufCur+=urlLen;
        cs->refs.nRemotes++;
      }
    }
    if( bufCur+4<=data+nData ){
      int nTracking = (int)CS_READ_U32(bufCur); bufCur+=4;
      if( nTracking<0 || nTracking>(int)(data+nData-bufCur)/4 ) return SQLITE_CORRUPT;
      if( nTracking>0 ){
        cs->refs.aTracking = sqlite3_malloc(nTracking*(int)sizeof(struct TrackingBranch));
        if(!cs->refs.aTracking) return SQLITE_NOMEM;
        memset(cs->refs.aTracking, 0, nTracking*(int)sizeof(struct TrackingBranch));
        for(i=0;i<nTracking;i++){
          int remoteLen, branchLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          remoteLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if( remoteLen<0 ) return SQLITE_CORRUPT;
          if(bufCur+remoteLen+4>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTracking[i].zRemote=sqlite3_malloc(remoteLen+1);
          if(!cs->refs.aTracking[i].zRemote) return SQLITE_NOMEM;
          memcpy(cs->refs.aTracking[i].zRemote,bufCur,remoteLen); cs->refs.aTracking[i].zRemote[remoteLen]=0; bufCur+=remoteLen;
          branchLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if( branchLen<0 ) return SQLITE_CORRUPT;
          if(bufCur+branchLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTracking[i].zBranch=sqlite3_malloc(branchLen+1);
          if(!cs->refs.aTracking[i].zBranch) return SQLITE_NOMEM;
          memcpy(cs->refs.aTracking[i].zBranch,bufCur,branchLen); cs->refs.aTracking[i].zBranch[branchLen]=0; bufCur+=branchLen;
          memcpy(cs->refs.aTracking[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
          cs->refs.nTracking++;
        }
      }
    }
    csFreeSequences(cs);
    if( bufCur+4<=data+nData ){
      int nSequences = (int)CS_READ_U32(bufCur); bufCur+=4;
      if( nSequences<0
       || nSequences>(int)(data+nData-bufCur)/(4+8) ){
        return SQLITE_CORRUPT;
      }
      if( nSequences>0 ){
        cs->refs.aSequences = sqlite3_malloc(nSequences*(int)sizeof(SequenceRef));
        if(!cs->refs.aSequences) return SQLITE_NOMEM;
        memset(cs->refs.aSequences, 0, nSequences*(int)sizeof(SequenceRef));
        for(i=0;i<nSequences;i++){
          int nameLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if( nameLen<0 ) return SQLITE_CORRUPT;
          if(bufCur+nameLen+8>data+nData) return SQLITE_CORRUPT;
          cs->refs.aSequences[i].zTableName=sqlite3_malloc(nameLen+1);
          if(!cs->refs.aSequences[i].zTableName) return SQLITE_NOMEM;
          memcpy(cs->refs.aSequences[i].zTableName,bufCur,nameLen);
          cs->refs.aSequences[i].zTableName[nameLen]=0;
          bufCur+=nameLen;
          cs->refs.aSequences[i].iSeq=CS_READ_I64(bufCur);
          bufCur+=8;
          cs->refs.nSequences++;
        }
      }
    }
  }

  return SQLITE_OK;
}

int csDeserializeRefsIntoTemp(ChunkStore *pTmp, const u8 *data, int nData){
  memset(pTmp, 0, sizeof(*pTmp));
  return csDeserializeRefs(pTmp, data, nData);
}

void csAdoptRefsState(ChunkStore *pDst, ChunkStore *pSrc){
  pDst->refs.aBranches = pSrc->refs.aBranches;
  pDst->refs.nBranches = pSrc->refs.nBranches;
  pDst->refs.zDefaultBranch = pSrc->refs.zDefaultBranch;
  pDst->refs.aTags = pSrc->refs.aTags;
  pDst->refs.nTags = pSrc->refs.nTags;
  pDst->refs.aRemotes = pSrc->refs.aRemotes;
  pDst->refs.nRemotes = pSrc->refs.nRemotes;
  pDst->refs.aTracking = pSrc->refs.aTracking;
  pDst->refs.nTracking = pSrc->refs.nTracking;
  pDst->refs.aSequences = pSrc->refs.aSequences;
  pDst->refs.nSequences = pSrc->refs.nSequences;

  pSrc->refs.aBranches = 0;
  pSrc->refs.nBranches = 0;
  pSrc->refs.zDefaultBranch = 0;
  pSrc->refs.aTags = 0;
  pSrc->refs.nTags = 0;
  pSrc->refs.aRemotes = 0;
  pSrc->refs.nRemotes = 0;
  pSrc->refs.aTracking = 0;
  pSrc->refs.nTracking = 0;
  pSrc->refs.aSequences = 0;
  pSrc->refs.nSequences = 0;
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
