#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "prolly_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName){
  char *zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  sqlite3_free(cs->refs.zDefaultBranch);
  cs->refs.zDefaultBranch = zCopy;
  return SQLITE_OK;
}

static int findBranchIdx(ChunkStore *cs, const char *zName){
  return csFindNamedRef(cs->refs.aBranches, cs->refs.nBranches,
                        (int)sizeof(struct BranchRef), zName);
}

static int findTagIdx(ChunkStore *cs, const char *zName){
  return csFindNamedRef(cs->refs.aTags, cs->refs.nTags,
                        (int)sizeof(struct TagRef), zName);
}

static int findRemoteIdx(ChunkStore *cs, const char *zName){
  return csFindNamedRef(cs->refs.aRemotes, cs->refs.nRemotes,
                        (int)sizeof(struct RemoteRef), zName);
}

static int findTrackingIdx(ChunkStore *cs, const char *zRemote, const char *zBranch){
  int i;
  for(i=0; i<cs->refs.nTracking; i++){
    if( strcmp(cs->refs.aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->refs.aTracking[i].zBranch, zBranch)==0 ) return i;
  }
  return -1;
}

int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i = findBranchIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  if( pCommit ) memcpy(pCommit, &cs->refs.aBranches[i].commitHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

/* Disk matches memory: same inode, size equals committed extent, and a
** sealed tail root carries this handle's live refs hash. */
int csDiskStateMatchesMemory(ChunkStore *cs){
  int bMoved = 0;
  int hashState;
  i64 contentEnd;
  i64 physSize = 0;
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  if( cs->file.pFile==0 ) return 0;
  if( cs->wal.nWalData < 1 + CHUNK_MANIFEST_SIZE ) return 0;
  if( sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED,
                           &bMoved)!=SQLITE_OK || bMoved ){
    return 0;
  }
  contentEnd = cs->wal.iWalOffset + cs->wal.nWalData;
  if( sqlite3OsFileSize(cs->file.pFile, &physSize)!=SQLITE_OK
   || physSize!=contentEnd ){
    return 0;
  }
  if( sqlite3OsRead(cs->file.pFile, aRoot, (int)sizeof(aRoot),
                    contentEnd - (i64)sizeof(aRoot))!=SQLITE_OK ){
    return 0;
  }
  hashState = csManifestHashState(aRoot+1,
                                  contentEnd - (i64)sizeof(aRoot));
  if( hashState==CS_MANIFEST_HASH_BAD ){
    hashState = csManifestHashStateOffsetless(aRoot+1);
  }
  return aRoot[0]==CS_WAL_TAG_ROOT
      && hashState==CS_MANIFEST_HASH_OK
      && csValidateWalRootManifest(
           cs, aRoot+1, contentEnd-(i64)sizeof(aRoot))==SQLITE_OK
      && memcmp(aRoot + 1 + CS_MANIFEST_REFS_HASH_OFF,
                cs->refs.refsHash.data, PROLLY_HASH_SIZE)==0;
}

/* Fresh refs hash from the sealed tail. committedRefsHash is a proxy
** (pre-merge restore can disagree). 0 means cannot prove disk unmoved. */
int csReadDiskRefsHash(ChunkStore *cs, ProllyHash *pOut){
  i64 physSize = 0;
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  int hashState;

  memset(pOut, 0, sizeof(*pOut));
  if( cs->file.pFile==0 ) return 0;
  /* Tail from the file, not WAL bookkeeping: a peer may have appended. */
  if( sqlite3OsFileSize(cs->file.pFile, &physSize)!=SQLITE_OK ) return 0;
  if( physSize < (i64)sizeof(aRoot) ) return 0;
  if( sqlite3OsRead(cs->file.pFile, aRoot, (int)sizeof(aRoot),
                    physSize - (i64)sizeof(aRoot))!=SQLITE_OK ){
    return 0;
  }
  /* Unsealed tail is unpublished; caller falls back to in-memory. */
  if( aRoot[0]!=CS_WAL_TAG_ROOT ) return 0;
  hashState = csManifestHashState(aRoot+1, physSize-(i64)sizeof(aRoot));
  if( hashState==CS_MANIFEST_HASH_BAD ){
    hashState = csManifestHashStateOffsetless(aRoot+1);
  }
  if( hashState!=CS_MANIFEST_HASH_OK
   || csValidateWalRootManifest(
        cs, aRoot+1, physSize-(i64)sizeof(aRoot))!=SQLITE_OK ){
    return 0;
  }
  memcpy(pOut->data, aRoot + 1 + CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);
  return 1;
}

/* Read zName's disk tip without touching this handle. Skip the throwaway
** open when the tail root already matches. Caller holds the store lock. */
int chunkStoreReadDiskBranchTip(ChunkStore *cs, const char *zName,
                                ProllyHash *pTip, int *pFound){
  ChunkStore tmp;
  int rc;

  *pFound = 0;
  if( cs->isMemory || cs->isBuffer || !cs->file.zFilename ){
    if( chunkStoreFindBranch(cs, zName, pTip)==SQLITE_OK ) *pFound = 1;
    return SQLITE_OK;
  }

  if( csDiskStateMatchesMemory(cs) ){
    if( chunkStoreFindBranch(cs, zName, pTip)==SQLITE_OK ) *pFound = 1;
    return SQLITE_OK;
  }

  rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;
  if( chunkStoreFindBranch(&tmp, zName, pTip)==SQLITE_OK ) *pFound = 1;
  chunkStoreClose(&tmp);
  return SQLITE_OK;
}

int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int n = cs->refs.nBranches;
  if( chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  if( csRefArrayGrow((void**)&cs->refs.aBranches, n, (int)sizeof(struct BranchRef)) ) return SQLITE_NOMEM;
  cs->refs.aBranches[n].zName = sqlite3_mprintf("%s", zName);
  if( !cs->refs.aBranches[n].zName ) return SQLITE_NOMEM;
  memcpy(&cs->refs.aBranches[n].commitHash, pCommit, sizeof(ProllyHash));
  cs->refs.nBranches++;
  return SQLITE_OK;
}

int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int i = findBranchIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  memcpy(&cs->refs.aBranches[i].commitHash, pCommit, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName){
  int i = findBranchIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  sqlite3_free(cs->refs.aBranches[i].zName);
  cs->refs.aBranches[i] = cs->refs.aBranches[cs->refs.nBranches-1];
  cs->refs.nBranches--;
  return SQLITE_OK;
}

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash){
  int i = findBranchIdx(cs, zBranch);
  if( i<0 ){
    memset(pHash, 0, sizeof(ProllyHash));
    return SQLITE_NOTFOUND;
  }
  memcpy(pHash, &cs->refs.aBranches[i].workingSetHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash){
  int i = findBranchIdx(cs, zBranch);
  if( i<0 ) return SQLITE_NOTFOUND;
  memcpy(&cs->refs.aBranches[i].workingSetHash, pHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i = findTagIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  if( pCommit ) memcpy(pCommit, &cs->refs.aTags[i].commitHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreAddTagFull(
  ChunkStore *cs,
  const char *zName,
  const ProllyHash *pCommit,
  const char *zTagger,
  const char *zEmail,
  i64 timestamp,
  const char *zMessage
){
  struct TagRef *t;
  int n = cs->refs.nTags;
  if( chunkStoreFindTag(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  if( csRefArrayGrow((void**)&cs->refs.aTags, n, (int)sizeof(struct TagRef)) ) return SQLITE_NOMEM;
  t = &cs->refs.aTags[n];
  t->zName = sqlite3_mprintf("%s", zName);
  if( !t->zName ) return SQLITE_NOMEM;
  memcpy(&t->commitHash, pCommit, sizeof(ProllyHash));
  t->zTagger  = sqlite3_mprintf("%s", zTagger  ? zTagger  : "");
  t->zEmail   = sqlite3_mprintf("%s", zEmail   ? zEmail   : "");
  t->zMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
  if( !t->zTagger || !t->zEmail || !t->zMessage ){
    sqlite3_free(t->zName);
    sqlite3_free(t->zTagger);
    sqlite3_free(t->zEmail);
    sqlite3_free(t->zMessage);
    memset(t, 0, sizeof(struct TagRef));
    return SQLITE_NOMEM;
  }
  t->timestamp = timestamp;
  cs->refs.nTags++;
  return SQLITE_OK;
}

int chunkStoreDeleteTag(ChunkStore *cs, const char *zName){
  int i = findTagIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  sqlite3_free(cs->refs.aTags[i].zName);
  sqlite3_free(cs->refs.aTags[i].zTagger);
  sqlite3_free(cs->refs.aTags[i].zEmail);
  sqlite3_free(cs->refs.aTags[i].zMessage);
  cs->refs.aTags[i] = cs->refs.aTags[cs->refs.nTags-1];
  cs->refs.nTags--;
  return SQLITE_OK;
}

int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl){
  int i = findRemoteIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  if( pzUrl ) *pzUrl = cs->refs.aRemotes[i].zUrl;
  return SQLITE_OK;
}

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl){
  int n = cs->refs.nRemotes;
  if( chunkStoreFindRemote(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  if( csRefArrayGrow((void**)&cs->refs.aRemotes, n, (int)sizeof(struct RemoteRef)) ) return SQLITE_NOMEM;
  cs->refs.aRemotes[n].zName = sqlite3_mprintf("%s", zName);
  if( !cs->refs.aRemotes[n].zName ) return SQLITE_NOMEM;
  cs->refs.aRemotes[n].zUrl = sqlite3_mprintf("%s", zUrl);
  if( !cs->refs.aRemotes[n].zUrl ){
    sqlite3_free(cs->refs.aRemotes[n].zName);
    return SQLITE_NOMEM;
  }
  cs->refs.nRemotes++;
  return SQLITE_OK;
}

int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName){
  int i = findRemoteIdx(cs, zName);
  int j;
  if( i<0 ) return SQLITE_NOTFOUND;
  sqlite3_free(cs->refs.aRemotes[i].zName);
  sqlite3_free(cs->refs.aRemotes[i].zUrl);
  cs->refs.aRemotes[i] = cs->refs.aRemotes[cs->refs.nRemotes-1];
  cs->refs.nRemotes--;

  for(j=cs->refs.nTracking-1; j>=0; j--){
    if( strcmp(cs->refs.aTracking[j].zRemote, zName)==0 ){
      sqlite3_free(cs->refs.aTracking[j].zRemote);
      sqlite3_free(cs->refs.aTracking[j].zBranch);
      cs->refs.aTracking[j] = cs->refs.aTracking[cs->refs.nTracking-1];
      cs->refs.nTracking--;
    }
  }
  return SQLITE_OK;
}

int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit){
  int i = findTrackingIdx(cs, zRemote, zBranch);
  if( i>=0 ){
    if( pCommit ) memcpy(pCommit, &cs->refs.aTracking[i].commitHash, sizeof(ProllyHash));
    return SQLITE_OK;
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit){
  int i = findTrackingIdx(cs, zRemote, zBranch);
  if( i>=0 ){
    memcpy(&cs->refs.aTracking[i].commitHash, pCommit, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  {
    int n = cs->refs.nTracking;
    if( csRefArrayGrow((void**)&cs->refs.aTracking, n, (int)sizeof(struct TrackingBranch)) ) return SQLITE_NOMEM;
    cs->refs.aTracking[n].zRemote = sqlite3_mprintf("%s", zRemote);
    if( !cs->refs.aTracking[n].zRemote ) return SQLITE_NOMEM;
    cs->refs.aTracking[n].zBranch = sqlite3_mprintf("%s", zBranch);
    if( !cs->refs.aTracking[n].zBranch ){
      sqlite3_free(cs->refs.aTracking[n].zRemote);
      return SQLITE_NOMEM;
    }
    memcpy(&cs->refs.aTracking[n].commitHash, pCommit, sizeof(ProllyHash));
    cs->refs.nTracking++;
  }
  return SQLITE_OK;
}

static int csSerializeRefsBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  const char *def = cs->refs.zDefaultBranch ? cs->refs.zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  int sz = 1 + 4 + defLen + 4 + 4 + 4 + 4;
  int i;
  u8 *buf, *bufCur;

  *ppOut = 0;
  *pnOut = 0;

  for(i=0; i<cs->refs.nBranches; i++){
    int inc = 4 + (int)strlen(cs->refs.aBranches[i].zName) + PROLLY_HASH_SIZE*2;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->refs.nTags; i++){
    int taggerLen  = cs->refs.aTags[i].zTagger  ? (int)strlen(cs->refs.aTags[i].zTagger)  : 0;
    int emailLen   = cs->refs.aTags[i].zEmail   ? (int)strlen(cs->refs.aTags[i].zEmail)   : 0;
    int messageLen = cs->refs.aTags[i].zMessage ? (int)strlen(cs->refs.aTags[i].zMessage) : 0;
    int inc = 4 + (int)strlen(cs->refs.aTags[i].zName) + PROLLY_HASH_SIZE
            + 4 + taggerLen
            + 4 + emailLen
            + 8
            + 4 + messageLen;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->refs.nRemotes; i++){
    int inc = 4 + (int)strlen(cs->refs.aRemotes[i].zName) + 4 + (int)strlen(cs->refs.aRemotes[i].zUrl);
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->refs.nTracking; i++){
    int inc = 4 + (int)strlen(cs->refs.aTracking[i].zRemote) + 4 + (int)strlen(cs->refs.aTracking[i].zBranch) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  sz += 4;
  for(i=0; i<cs->refs.nSequences; i++){
    int nameLen = cs->refs.aSequences[i].zTableName
                ? (int)strlen(cs->refs.aSequences[i].zTableName) : 0;
    int inc = 4 + nameLen + 8;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  bufCur = buf;
  *bufCur++ = 7;
  CS_WRITE_U32(bufCur,defLen); bufCur+=4;
  memcpy(bufCur, def, defLen); bufCur+=defLen;
  CS_WRITE_U32(bufCur,cs->refs.nBranches); bufCur+=4;
  for(i=0; i<cs->refs.nBranches; i++){
    int nameLen = (int)strlen(cs->refs.aBranches[i].zName);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aBranches[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->refs.aBranches[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    memcpy(bufCur, cs->refs.aBranches[i].workingSetHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->refs.nTags); bufCur+=4;
  for(i=0; i<cs->refs.nTags; i++){
    int nameLen    = (int)strlen(cs->refs.aTags[i].zName);
    int taggerLen  = cs->refs.aTags[i].zTagger  ? (int)strlen(cs->refs.aTags[i].zTagger)  : 0;
    int emailLen   = cs->refs.aTags[i].zEmail   ? (int)strlen(cs->refs.aTags[i].zEmail)   : 0;
    int messageLen = cs->refs.aTags[i].zMessage ? (int)strlen(cs->refs.aTags[i].zMessage) : 0;
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aTags[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->refs.aTags[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    CS_WRITE_U32(bufCur,taggerLen); bufCur+=4;
    if( taggerLen ) memcpy(bufCur, cs->refs.aTags[i].zTagger, taggerLen);
    bufCur+=taggerLen;
    CS_WRITE_U32(bufCur,emailLen); bufCur+=4;
    if( emailLen ) memcpy(bufCur, cs->refs.aTags[i].zEmail, emailLen);
    bufCur+=emailLen;
    CS_WRITE_I64(bufCur,cs->refs.aTags[i].timestamp); bufCur+=8;
    CS_WRITE_U32(bufCur,messageLen); bufCur+=4;
    if( messageLen ) memcpy(bufCur, cs->refs.aTags[i].zMessage, messageLen);
    bufCur+=messageLen;
  }
  CS_WRITE_U32(bufCur,cs->refs.nRemotes); bufCur+=4;
  for(i=0; i<cs->refs.nRemotes; i++){
    int nameLen = (int)strlen(cs->refs.aRemotes[i].zName);
    int urlLen = (int)strlen(cs->refs.aRemotes[i].zUrl);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aRemotes[i].zName, nameLen); bufCur+=nameLen;
    CS_WRITE_U32(bufCur,urlLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aRemotes[i].zUrl, urlLen); bufCur+=urlLen;
  }
  CS_WRITE_U32(bufCur,cs->refs.nTracking); bufCur+=4;
  for(i=0; i<cs->refs.nTracking; i++){
    int remoteLen = (int)strlen(cs->refs.aTracking[i].zRemote);
    int branchLen = (int)strlen(cs->refs.aTracking[i].zBranch);
    CS_WRITE_U32(bufCur,remoteLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aTracking[i].zRemote, remoteLen); bufCur+=remoteLen;
    CS_WRITE_U32(bufCur,branchLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aTracking[i].zBranch, branchLen); bufCur+=branchLen;
    memcpy(bufCur, cs->refs.aTracking[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->refs.nSequences); bufCur+=4;
  for(i=0; i<cs->refs.nSequences; i++){
    int nameLen = cs->refs.aSequences[i].zTableName
                ? (int)strlen(cs->refs.aSequences[i].zTableName) : 0;
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    if( nameLen ){
      memcpy(bufCur, cs->refs.aSequences[i].zTableName, nameLen);
      bufCur+=nameLen;
    }
    CS_WRITE_I64(bufCur, cs->refs.aSequences[i].iSeq); bufCur+=8;
  }
  *ppOut = buf;
  *pnOut = sz;
  return SQLITE_OK;
}

int chunkStoreSerializeRefs(ChunkStore *cs){
  int rc;
  u8 *buf = 0;
  int sz = 0;
  ProllyHash refsHash;

  if( cs->refs.nBranches==1
   && cs->refs.nTags==0
   && cs->refs.nRemotes==0
   && cs->refs.nTracking==0
   && cs->refs.nSequences==0
   && (!cs->refs.zDefaultBranch || strcmp(cs->refs.zDefaultBranch, "main")==0)
   && strcmp(cs->refs.aBranches[0].zName, "main")==0 ){
    u8 aBuf[77];
    u8 *p = aBuf;
    *p++ = 7;
    CS_WRITE_U32(p,4); p+=4;
    memcpy(p, "main", 4); p+=4;
    CS_WRITE_U32(p,1); p+=4;
    CS_WRITE_U32(p,4); p+=4;
    memcpy(p, "main", 4); p+=4;
    memcpy(p, cs->refs.aBranches[0].commitHash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    memcpy(p, cs->refs.aBranches[0].workingSetHash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    CS_WRITE_U32(p,0); p+=4;
    CS_WRITE_U32(p,0); p+=4;
    CS_WRITE_U32(p,0); p+=4;
    CS_WRITE_U32(p,0); p+=4;
    assert( p==aBuf+sizeof(aBuf) );
    rc = chunkStorePut(cs, aBuf, (int)sizeof(aBuf), &refsHash);
    if( rc==SQLITE_OK ) memcpy(&cs->refs.refsHash, &refsHash, sizeof(ProllyHash));
    return rc;
  }

  rc = csSerializeRefsBlob(cs, &buf, &sz);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, buf, sz, &refsHash);
  sqlite3_free(buf);
  if( rc==SQLITE_OK ) memcpy(&cs->refs.refsHash, &refsHash, sizeof(ProllyHash));
  return rc;
}


int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData){
  return csReplaceRefsStateFromBlob(cs, data, nData, 1);
}

int chunkStoreInstallRefsBlob(ChunkStore *cs, const u8 *data, int nData){
  ChunkStore refsView;
  ProllyHash refsHash;
  int rc;

  if( !data || nData<=0 ) return SQLITE_ERROR;
  rc = csDeserializeRefsIntoTemp(&refsView, data, nData);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&refsView);
    return rc;
  }

  rc = chunkStorePut(cs, data, nData, &refsHash);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&refsView);
    return rc;
  }

  csFreeRefsState(cs);
  csAdoptRefsState(cs, &refsView);
  refsTableSetHash(&cs->refs, &refsHash);
  cs->bRefsStale = 0;
  return SQLITE_OK;
}

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  return csSerializeRefsBlob(cs, ppOut, pnOut);
}

int chunkStoreSnapshotRefs(ChunkStore *cs, ChunkStoreRefsSnapshot *pSnapshot){
  u8 *pRefs = 0;
  int nRefs = 0;
  int rc;

  memset(pSnapshot, 0, sizeof(*pSnapshot));
  rc = csSerializeRefsBlob(cs, &pRefs, &nRefs);
  if( rc!=SQLITE_OK ) return rc;
  pSnapshot->refsHash = cs->refs.refsHash;
  pSnapshot->committedRefsHash = cs->refs.committedRefsHash;
  csDetachSavedRefsState(cs, &pSnapshot->state);
  rc = csReplaceRefsStateFromBlob(cs, pRefs, nRefs, 0);
  sqlite3_free(pRefs);
  if( rc!=SQLITE_OK ){
    csRestoreSavedRefsState(cs, &pSnapshot->state);
    REFS_OWNED_CLEAR(pSnapshot->state);
    cs->refs.refsHash = pSnapshot->refsHash;
    cs->refs.committedRefsHash = pSnapshot->committedRefsHash;
    cs->bRefsStale = 0;
    memset(pSnapshot, 0, sizeof(*pSnapshot));
    return rc;
  }
  cs->refs.refsHash = pSnapshot->refsHash;
  cs->refs.committedRefsHash = pSnapshot->committedRefsHash;
  cs->bRefsStale = 0;
  return SQLITE_OK;
}

void chunkStoreRestoreRefsSnapshot(
  ChunkStore *cs,
  ChunkStoreRefsSnapshot *pSnapshot
){
  csFreeRefsState(cs);
  csRestoreSavedRefsState(cs, &pSnapshot->state);
  REFS_OWNED_CLEAR(pSnapshot->state);
  cs->refs.refsHash = pSnapshot->refsHash;
  cs->refs.committedRefsHash = pSnapshot->committedRefsHash;
  cs->bRefsStale = 0;
  memset(pSnapshot, 0, sizeof(*pSnapshot));
}

void chunkStoreDiscardRefsSnapshot(ChunkStoreRefsSnapshot *pSnapshot){
  csFreeSavedRefsState(&pSnapshot->state);
  memset(pSnapshot, 0, sizeof(*pSnapshot));
}


#endif
