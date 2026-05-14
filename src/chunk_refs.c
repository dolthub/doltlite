
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
  csFreeRefsState(&refsStore);
  memset(pSaved, 0, sizeof(*pSaved));
}

void csFreeRefsState(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
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
  if( version!=6 ) return SQLITE_CORRUPT;
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
        if( version>=6 ){
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
        }else{
          cs->refs.aTags[i].zTagger  = sqlite3_mprintf("");
          cs->refs.aTags[i].zEmail   = sqlite3_mprintf("");
          cs->refs.aTags[i].zMessage = sqlite3_mprintf("");
          if( !cs->refs.aTags[i].zTagger || !cs->refs.aTags[i].zEmail || !cs->refs.aTags[i].zMessage ){
            return SQLITE_NOMEM;
          }
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

  pSrc->refs.aBranches = 0;
  pSrc->refs.nBranches = 0;
  pSrc->refs.zDefaultBranch = 0;
  pSrc->refs.aTags = 0;
  pSrc->refs.nTags = 0;
  pSrc->refs.aRemotes = 0;
  pSrc->refs.nRemotes = 0;
  pSrc->refs.aTracking = 0;
  pSrc->refs.nTracking = 0;
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
