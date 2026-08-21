
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_chunk_walk.h"

#include <string.h>
#include <stdio.h>
#ifdef SQLITE_CRASH_TEST
#include <unistd.h>
#endif

extern void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
#include "doltlite_internal.h"

typedef struct GcQueue GcQueue;
typedef struct GcQueueItem GcQueueItem;
struct GcQueueItem {
  ProllyHash hash;
  ProllyHash parent;
  const char *zSource;
  const char *zParentType;
  int iParentLevel;
  int nParentItems;
  int iChild;
};
struct GcQueue {
  GcQueueItem *aItems;
  int nItems;
  int nAlloc;
  int iHead;
};

typedef struct GcChildCtx GcChildCtx;
struct GcChildCtx {
  GcQueue *q;
  ChunkStore *cs;
  ProllyHash parent;
  const char *zSource;
  const char *zParentType;
  int iParentLevel;
  int nParentItems;
  int iNextChild;
};

typedef struct GcMarkTrace GcMarkTrace;
struct GcMarkTrace {
  int hasMissing;
  int rc;
  ProllyHash missing;
  ProllyHash parent;
  const char *zSource;
  const char *zParentType;
  int iParentLevel;
  int nParentItems;
  int iChild;
};

static const char *gcChunkTypeName(DoltliteChunkType type){
  switch( type ){
    case CHUNK_COMMIT: return "commit";
    case CHUNK_PROLLY_NODE: return "prolly-node";
    case CHUNK_CATALOG: return "catalog";
    case CHUNK_WORKING_SET: return "working-set";
    case CHUNK_REFS: return "refs";
    case CHUNK_CONFLICTS: return "conflicts";
    case CHUNK_CONSTRAINT_VIOLATIONS: return "constraint-violations";
    case CHUNK_UNKNOWN:
    default: return "unknown";
  }
}

static int gcQueueInit(GcQueue *q){
  q->nAlloc = 256;
  q->aItems = sqlite3_malloc(q->nAlloc * (int)sizeof(GcQueueItem));
  if( !q->aItems ) return SQLITE_NOMEM;
  q->nItems = 0;
  q->iHead = 0;
  return SQLITE_OK;
}

static void gcQueueFree(GcQueue *q){
  sqlite3_free(q->aItems);
  memset(q, 0, sizeof(*q));
}

static int gcQueuePush(
  GcQueue *q,
  const ProllyHash *h,
  const ProllyHash *pParent,
  const char *zSource,
  const char *zParentType,
  int iParentLevel,
  int nParentItems,
  int iChild
){
  assert( q!=0 && h!=0 );
  assert( q->nItems>=0 && q->nItems<=q->nAlloc );
  assert( q->iHead>=0 && q->iHead<=q->nItems );
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  if( q->nItems >= q->nAlloc ){
    i64 nNew = q->nAlloc ? (i64)q->nAlloc * 2 : (i64)256;
    GcQueueItem *aNew;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(GcQueueItem) ) return SQLITE_NOMEM;
    aNew = (GcQueueItem*)sqlite3_realloc(
      q->aItems, (int)(nNew * (i64)sizeof(GcQueueItem))
    );
    if( !aNew ) return SQLITE_NOMEM;
    q->aItems = aNew;
    q->nAlloc = (int)nNew;
  }
  memcpy(&q->aItems[q->nItems].hash, h, sizeof(ProllyHash));
  if( pParent ){
    memcpy(&q->aItems[q->nItems].parent, pParent, sizeof(ProllyHash));
  }else{
    memset(&q->aItems[q->nItems].parent, 0, sizeof(ProllyHash));
  }
  q->aItems[q->nItems].zSource = zSource;
  q->aItems[q->nItems].zParentType = zParentType;
  q->aItems[q->nItems].iParentLevel = iParentLevel;
  q->aItems[q->nItems].nParentItems = nParentItems;
  q->aItems[q->nItems].iChild = iChild;
  q->nItems++;
  return SQLITE_OK;
}

static int gcQueuePop(GcQueue *q, GcQueueItem *pItem){
  assert( q!=0 && pItem!=0 );
  assert( q->iHead>=0 && q->iHead<=q->nItems );
  assert( q->nItems<=q->nAlloc );
  if( q->iHead >= q->nItems ) return 0;
  *pItem = q->aItems[q->iHead];
  q->iHead++;
  return 1;
}

static int gcChildCb(void *ctx, const ProllyHash *pHash){
  GcChildCtx *p = (GcChildCtx*)ctx;
  int iChild = p->iNextChild++;
  return gcQueuePush(p->q, pHash, &p->parent, p->zSource,
                     p->zParentType, p->iParentLevel, p->nParentItems,
                     iChild);
}

static int gcSessionChildCb(void *ctx, const ProllyHash *pHash){
  GcChildCtx *p = (GcChildCtx*)ctx;
  int bHas = 0;
  int rc;

  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  rc = chunkStoreHas(p->cs, pHash, &bHas);
  if( rc!=SQLITE_OK ) return rc;
  if( !bHas ) return SQLITE_OK;
  return gcChildCb(ctx, pHash);
}

#ifdef DOLTLITE_PROLLY_CHECK
typedef struct GcVerifyCtx GcVerifyCtx;
struct GcVerifyCtx { ChunkStore *cs; int rc; };

static int gcVerifyHashCb(void *ctx, const ProllyHash *pHash){
  GcVerifyCtx *v = (GcVerifyCtx*)ctx;
  u8 *data = 0; int nData = 0;
  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  v->rc = chunkStoreGet(v->cs, pHash, &data, &nData);
  sqlite3_free(data);
  /* Only NOTFOUND means the sweep collected a live hash; other errors
  ** leave the check inconclusive. */
  if( v->rc==SQLITE_NOTFOUND ){
    fprintf(stderr,
            "doltlite: GC invariant: session hash unreachable after sweep\n");
    abort();
  }
  return SQLITE_OK;
}

static int gcVerifySessionHashCb(void *ctx, const ProllyHash *pHash){
  GcVerifyCtx *v = (GcVerifyCtx*)ctx;
  int bHas = 0;

  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  v->rc = chunkStoreHas(v->cs, pHash, &bHas);
  if( v->rc!=SQLITE_OK ) return SQLITE_OK;
  if( !bHas ) return SQLITE_OK;
  return gcVerifyHashCb(ctx, pHash);
}

static void gcVerifySessionResolvable(sqlite3 *db, ChunkStore *cs){
  GcVerifyCtx v;
  v.cs = cs; v.rc = SQLITE_OK;
  /* Diagnostic-only; allocation failures are inconclusive (gcVerifyHashCb). */
  sqlite3BeginBenignMalloc();
  (void)doltliteSeedSessionHashes(db, cs, gcVerifySessionHashCb, &v);
  sqlite3EndBenignMalloc();
}
#endif

static int gcMarkReachable(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyHashSet *marked,
  GcMarkTrace *pTrace
){
  GcQueue queue;
  GcQueueItem current;
  GcChildCtx seedCtx;
  int rc, i;

  memset(pTrace, 0, sizeof(*pTrace));
  rc = gcQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcQueuePush(&queue, refsTableGetHash(&cs->refs), 0, "refs-table",
                   0, -1, -1, -1);

  {
    int nBr; const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    for(i=0; rc==SQLITE_OK && i<nBr; i++){
      rc = gcQueuePush(&queue, &aBr[i].commitHash, 0, "branch-commit",
                       0, -1, -1, -1);
      if( rc==SQLITE_OK ){
        rc = gcQueuePush(&queue, &aBr[i].workingSetHash, 0,
                         "branch-working-set", 0, -1, -1, -1);
      }
    }
  }

  {
    int nTg; const TagRef *aTg;
    refsTableGetTags(&cs->refs, &nTg, &aTg);
    for(i=0; rc==SQLITE_OK && i<nTg; i++){
      rc = gcQueuePush(&queue, &aTg[i].commitHash, 0, "tag-commit",
                       0, -1, -1, -1);
    }
  }

  {
    int nTk; const TrackingBranch *aTk;
    refsTableGetTracking(&cs->refs, &nTk, &aTk);
    for(i=0; rc==SQLITE_OK && i<nTk; i++){
      rc = gcQueuePush(&queue, &aTk[i].commitHash, 0, "tracking-commit",
                       0, -1, -1, -1);
    }
  }

  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; rc==SQLITE_OK && i<nPend; i++){
      rc = gcQueuePush(&queue, &aPend[i].hash, 0, "pending",
                       0, -1, -1, -1);
    }
  }

  if( rc==SQLITE_OK ){
    memset(&seedCtx, 0, sizeof(seedCtx));
    seedCtx.q = &queue;
    seedCtx.cs = cs;
    seedCtx.zSource = "session";
    rc = doltliteSeedSessionHashes(db, cs, gcSessionChildCb, &seedCtx);
  }

  if( rc!=SQLITE_OK ){
    gcQueueFree(&queue);
    return rc;
  }

  while( gcQueuePop(&queue, &current) ){
    u8 *data = 0;
    int nData = 0;
    GcChildCtx childCtx;

    if( prollyHashIsEmpty(&current.hash) ) continue;
    if( prollyHashSetContains(marked, &current.hash) ) continue;

    rc = prollyHashSetAdd(marked, &current.hash);
    if( rc!=SQLITE_OK ) break;

    rc = chunkStoreGet(cs, &current.hash, &data, &nData);
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
        pTrace->hasMissing = 1;
        pTrace->rc = rc;
        memcpy(&pTrace->missing, &current.hash, sizeof(ProllyHash));
        memcpy(&pTrace->parent, &current.parent, sizeof(ProllyHash));
        pTrace->zSource = current.zSource;
        pTrace->zParentType = current.zParentType;
        pTrace->iParentLevel = current.iParentLevel;
        pTrace->nParentItems = current.nParentItems;
        pTrace->iChild = current.iChild;
      }
      break;
    }

    childCtx.q = &queue;
    childCtx.cs = cs;
    childCtx.parent = current.hash;
    childCtx.zSource = "child";
    childCtx.zParentType = gcChunkTypeName(doltliteClassifyChunk(data, nData));
    childCtx.iParentLevel = -1;
    childCtx.nParentItems = -1;
    childCtx.iNextChild = 0;
    if( strcmp(childCtx.zParentType, "prolly-node")==0 ){
      ProllyNode node;
      if( prollyNodeParse(&node, data, nData)==SQLITE_OK ){
        childCtx.iParentLevel = node.level;
        childCtx.nParentItems = (int)node.nItems;
      }
    }
    rc = doltliteEnumerateChunkChildren(data, nData, gcChildCb, &childCtx);

    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;
  }

  gcQueueFree(&queue);
  return rc;
}

static void gcFormatMarkFailure(
  char *zBuf,
  int nBuf,
  const GcMarkTrace *pTrace
){
  char zMissing[PROLLY_HASH_SIZE*2+1];
  char zParent[PROLLY_HASH_SIZE*2+1];
  if( !zBuf || nBuf<=0 || !pTrace || !pTrace->hasMissing ){
    return;
  }
  doltliteHashToHex(&pTrace->missing, zMissing);
  if( prollyHashIsEmpty(&pTrace->parent) ){
    sqlite3_snprintf(nBuf, zBuf,
      "gc mark phase failed: missing chunk %s source=%s rc=%d",
      zMissing, pTrace->zSource ? pTrace->zSource : "unknown", pTrace->rc);
  }else{
    doltliteHashToHex(&pTrace->parent, zParent);
    if( pTrace->zParentType && strcmp(pTrace->zParentType, "prolly-node")==0 ){
      sqlite3_snprintf(nBuf, zBuf,
        "gc mark phase failed: missing chunk %s parent=%s source=%s "
        "parent_type=%s parent_level=%d parent_items=%d child_index=%d rc=%d",
        zMissing, zParent, pTrace->zSource ? pTrace->zSource : "unknown",
        pTrace->zParentType, pTrace->iParentLevel, pTrace->nParentItems,
        pTrace->iChild, pTrace->rc);
    }else{
      sqlite3_snprintf(nBuf, zBuf,
        "gc mark phase failed: missing chunk %s parent=%s source=%s "
        "parent_type=%s child_index=%d rc=%d",
        zMissing, zParent, pTrace->zSource ? pTrace->zSource : "unknown",
        pTrace->zParentType ? pTrace->zParentType : "unknown",
        pTrace->iChild, pTrace->rc);
    }
  }
}

/* DOLTLITE_CRASH_GC_WRITE=N hard-exits on the Nth gc rewrite write. */
#ifdef SQLITE_TEST
static int gcCrashTarget = -2;
static int gcCrashCount = 0;
static void gcCrashResetForRun(void){
  if( gcCrashTarget == -2 ){
    const char *zEnv = getenv("DOLTLITE_CRASH_GC_WRITE");
    gcCrashTarget = zEnv ? atoi(zEnv) : -1;
  }
  if( gcCrashTarget > 0 ) gcCrashCount = 0;
}
#define GC_CRASH_CHECK() do{ \
  if( gcCrashTarget>0 && ++gcCrashCount>=gcCrashTarget ){ \
    _exit(99); \
  } \
}while(0)
#else
#define gcCrashResetForRun() ((void)0)
#define GC_CRASH_CHECK() ((void)0)
#endif

/* Buffered forward-only writer for the gc tmp file; streams so RAM stays
** bounded. */
typedef struct GcFileWriter GcFileWriter;
struct GcFileWriter {
  sqlite3_file *pFile;
  i64 iOff;
  int nBuf;
  u8 aBuf[65536];
};

static int gcWriterFlush(GcFileWriter *pW){
  int rc;
  if( pW->nBuf==0 ) return SQLITE_OK;
  GC_CRASH_CHECK();
  rc = sqlite3OsWrite(pW->pFile, pW->aBuf, pW->nBuf, pW->iOff);
  if( rc!=SQLITE_OK ) return rc;
  pW->iOff += pW->nBuf;
  pW->nBuf = 0;
  return SQLITE_OK;
}

static int gcWriterAppend(GcFileWriter *pW, const u8 *p, int n){
  while( n>0 ){
    int space = (int)sizeof(pW->aBuf) - pW->nBuf;
    int nCopy = n<space ? n : space;
    memcpy(pW->aBuf + pW->nBuf, p, nCopy);
    pW->nBuf += nCopy;
    p += nCopy;
    n -= nCopy;
    if( pW->nBuf==(int)sizeof(pW->aBuf) ){
      int rc = gcWriterFlush(pW);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int gcStreamMarkedChunk(
  ChunkStore *cs,
  const ProllyHash *pHash,
  ProllyHashSet *marked,
  GcFileWriter *pW,
  i64 *piPos,
  ChunkIndexEntry *aNewIndex,
  int *pnNewIndex
){
  u8 *chunkData = 0;
  int nChunkData = 0;
  int rc;

  if( !prollyHashSetContains(marked, pHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, pHash, &chunkData, &nChunkData);
  if( rc!=SQLITE_OK ) return rc;
  if( nChunkData < 0 ){
    sqlite3_free(chunkData);
    return SQLITE_CORRUPT;
  }

  memcpy(&aNewIndex[*pnNewIndex].hash, pHash, sizeof(ProllyHash));
  aNewIndex[*pnNewIndex].offset = *piPos;
  aNewIndex[*pnNewIndex].size = nChunkData;
  (*pnNewIndex)++;
  *piPos += 4 + (i64)nChunkData;

  if( pW ){
    u8 aLen[4];
    CS_WRITE_U32(aLen, nChunkData);
    rc = gcWriterAppend(pW, aLen, 4);
    if( rc==SQLITE_OK ) rc = gcWriterAppend(pW, chunkData, nChunkData);
  }
  sqlite3_free(chunkData);
  return rc;
}

/* Assemble the compacted index without materializing chunk data. Offsets
** describe the compacted layout; a writer streams it. Without a writer,
** fetch-and-discard still hash-validates every kept chunk. */
static int gcBuildCompactedIndex(
  ChunkStore *cs,
  ProllyHashSet *marked,
  GcFileWriter *pW,
  i64 *pnNewData,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex
){
  int i;
  int kept = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  i64 iPos = CHUNK_MANIFEST_SIZE;
  int rc = SQLITE_OK;

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      if( prollyHashSetContains(marked, &aIdx[i].hash) ) kept++;
    }
  }
  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; i<nPend; i++){
      if( prollyHashSetContains(marked, &aPend[i].hash) ) kept++;
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      if( prollyHashSetContains(marked, &aRec[i].hash) ) kept++;
    }
  }

  {
    i64 nEntries = kept ? (i64)kept : 1;
    if( nEntries > (i64)0x7fffffff/(i64)sizeof(ChunkIndexEntry) ){
      return SQLITE_NOMEM;
    }
    aNewIndex = sqlite3_malloc((int)(nEntries * (i64)sizeof(ChunkIndexEntry)));
    if( !aNewIndex ) return SQLITE_NOMEM;
  }

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx && rc==SQLITE_OK; i++){
      rc = gcStreamMarkedChunk(cs, &aIdx[i].hash, marked, pW, &iPos,
                               aNewIndex, &nNewIndex);
    }
  }
  if( rc==SQLITE_OK ){
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; i<nPend && rc==SQLITE_OK; i++){
      rc = gcStreamMarkedChunk(cs, &aPend[i].hash, marked, pW, &iPos,
                               aNewIndex, &nNewIndex);
    }
  }
  if( rc==SQLITE_OK ){
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec && rc==SQLITE_OK; i++){
      rc = gcStreamMarkedChunk(cs, &aRec[i].hash, marked, pW, &iPos,
                               aNewIndex, &nNewIndex);
    }
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(aNewIndex);
    return rc;
  }

  qsort(aNewIndex, nNewIndex, sizeof(aNewIndex[0]), csIndexEntryCmp);

  *pnNewData = iPos - CHUNK_MANIFEST_SIZE;
  *ppNewIndex = aNewIndex;
  *pnNewIndex = nNewIndex;
  return SQLITE_OK;
}

static int gcReopenChunkFile(ChunkStore *cs, sqlite3_file **ppFile){
  int rc;
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
  sqlite3_file *pFile = 0;
  i64 fileSize = 0;

  rc = sqlite3OsOpenMalloc(chunkFileGetVfs(&cs->file),
                           chunkFileGetFilename(&cs->file), &pFile, flags, 0);
  if( rc!=SQLITE_OK ){
    if( pFile ) sqlite3OsCloseFree(pFile);
    return rc;
  }
  rc = sqlite3OsFileSize(pFile, &fileSize);
  if( rc!=SQLITE_OK ){
    sqlite3OsCloseFree(pFile);
    return rc;
  }
  if( ppFile ){
    *ppFile = pFile;
  }else{
    chunkFileSetHandle(&cs->file, pFile);
    chunkFileSetSize(&cs->file, fileSize);
  }
  return SQLITE_OK;
}

static int gcRewriteFile(
  ChunkStore *cs,
  ProllyHashSet *marked,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex,
  i64 *pnNewData,
  int *pReplaced
){
  int i;
  int kept = 0;
  i64 nDataBytes = 0;
  i64 indexSize;
  i64 finalSize;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  i64 iPos = CHUNK_MANIFEST_SIZE;
  u8 manifest[CHUNK_MANIFEST_SIZE];
  ChunkStore manifestCs;
  char *zRaw;
  char *zTmp = 0;
  int rc = SQLITE_OK;
  int retainTmp = 0;

  *pReplaced = 0;
  *ppNewIndex = 0;
  *pnNewIndex = 0;
  *pnNewData = 0;

  gcCrashResetForRun();

  /* Manifest leads the file, so compacted geometry must be known before
  ** streaming. The streaming pass re-checks layout against fetched data. */
  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      if( prollyHashSetContains(marked, &aIdx[i].hash) ){
        kept++;
        nDataBytes += 4 + (i64)aIdx[i].size;
      }
    }
  }
  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; i<nPend; i++){
      if( prollyHashSetContains(marked, &aPend[i].hash) ){
        kept++;
        nDataBytes += 4 + (i64)aPend[i].size;
      }
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      if( prollyHashSetContains(marked, &aRec[i].hash) ){
        kept++;
        nDataBytes += 4 + (i64)aRec[i].size;
      }
    }
  }
  indexSize = (i64)kept * CHUNK_INDEX_ENTRY_SIZE;
  finalSize = CHUNK_MANIFEST_SIZE + nDataBytes + indexSize;

  manifestCs = *cs;
  chunkIndexSetMetadata(&manifestCs.index, kept,
                        CHUNK_MANIFEST_SIZE + nDataBytes, indexSize);
  walStateSetOffset(&manifestCs.wal, finalSize);
  csSerializeManifest(&manifestCs, manifest);
  csManifestSeal(manifest, 0);

  {
    i64 nEntries = kept ? (i64)kept : 1;
    if( nEntries > (i64)0x7fffffff/(i64)sizeof(ChunkIndexEntry) ){
      return SQLITE_NOMEM;
    }
    aNewIndex = sqlite3_malloc((int)(nEntries * (i64)sizeof(ChunkIndexEntry)));
    if( !aNewIndex ) return SQLITE_NOMEM;
  }

  zRaw = sqlite3_mprintf("%s-gc-tmp", chunkFileGetFilename(&cs->file));
  if( !zRaw ){
    sqlite3_free(aNewIndex);
    return SQLITE_NOMEM;
  }
  /* SQLITE_OPEN_MAIN_DB: the name needs the VFS's double-nul terminator. */
  rc = chunkStoreDupFilenameDoubleNul(zRaw, &zTmp);
  sqlite3_free(zRaw);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aNewIndex);
    return rc;
  }

  {
    sqlite3_file *pTmpFile = 0;
    int tmpFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                 | SQLITE_OPEN_MAIN_DB;
    GcFileWriter w;

    /* Best-effort stale-tmp removal; failure here must not fail GC. */
    sqlite3BeginBenignMalloc();
    sqlite3OsDelete(chunkFileGetVfs(&cs->file), zTmp, 0);
    sqlite3EndBenignMalloc();

    rc = sqlite3OsOpenMalloc(chunkFileGetVfs(&cs->file), zTmp, &pTmpFile, tmpFlags, 0);
    if( rc != SQLITE_OK ){
      sqlite3_free(zTmp);
      sqlite3_free(aNewIndex);
      return rc;
    }

    w.pFile = pTmpFile;
    w.iOff = 0;
    w.nBuf = 0;

    rc = gcWriterAppend(&w, manifest, CHUNK_MANIFEST_SIZE);

    if( rc==SQLITE_OK ){
      int nIdx; const ChunkIndexEntry *aIdx;
      chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
      for(i=0; i<nIdx && rc==SQLITE_OK; i++){
        rc = gcStreamMarkedChunk(cs, &aIdx[i].hash, marked, &w, &iPos,
                                 aNewIndex, &nNewIndex);
      }
    }
    if( rc==SQLITE_OK ){
      int nPend; const ChunkIndexEntry *aPend;
      chunkStagingGetPending(&cs->staging, &nPend, &aPend);
      for(i=0; i<nPend && rc==SQLITE_OK; i++){
        rc = gcStreamMarkedChunk(cs, &aPend[i].hash, marked, &w, &iPos,
                                 aNewIndex, &nNewIndex);
      }
    }
    if( rc==SQLITE_OK ){
      int nRec; const ChunkIndexEntry *aRec;
      chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
      for(i=0; i<nRec && rc==SQLITE_OK; i++){
        rc = gcStreamMarkedChunk(cs, &aRec[i].hash, marked, &w, &iPos,
                                 aNewIndex, &nNewIndex);
      }
    }

    /* If streamed layout disagrees with precomputed geometry, do not
    ** replace the live file. */
    if( rc==SQLITE_OK
     && (nNewIndex!=kept || iPos!=CHUNK_MANIFEST_SIZE + nDataBytes) ){
      rc = SQLITE_CORRUPT;
    }

    if( rc==SQLITE_OK ){
      qsort(aNewIndex, nNewIndex, sizeof(aNewIndex[0]), csIndexEntryCmp);
      for(i=0; i<nNewIndex && rc==SQLITE_OK; i++){
        u8 aEntry[CHUNK_INDEX_ENTRY_SIZE];
        u8 *p = aEntry;
        memcpy(p, aNewIndex[i].hash.data, PROLLY_HASH_SIZE);
        p += PROLLY_HASH_SIZE;
        CS_WRITE_I64(p, aNewIndex[i].offset);
        p += 8;
        CS_WRITE_U32(p, (u32)aNewIndex[i].size);
        rc = gcWriterAppend(&w, aEntry, CHUNK_INDEX_ENTRY_SIZE);
      }
    }
    if( rc==SQLITE_OK ) rc = gcWriterFlush(&w);

    if( rc==SQLITE_OK ){
      GC_CRASH_CHECK();
      rc = sqlite3OsTruncate(pTmpFile, finalSize);
    }
    if( rc==SQLITE_OK ){
      GC_CRASH_CHECK();
      rc = sqlite3OsSync(pTmpFile, SQLITE_SYNC_NORMAL);
    }
    if( rc==SQLITE_OK ){
      sqlite3_file *pOldFile = chunkFileGetHandle(&cs->file);
      sqlite3_file *pNewFile = 0;

#if SQLITE_OS_WIN
      /* Windows will not reliably replace an open file. Close both before
      ** the atomic replace; restore the destination handle on failure. */
      sqlite3OsCloseFree(pTmpFile);
      pTmpFile = 0;
      if( pOldFile ){
        sqlite3OsCloseFree(pOldFile);
        pOldFile = 0;
        chunkFileSetHandle(&cs->file, 0);
        chunkFileSetSize(&cs->file, 0);
      }
#endif

      GC_CRASH_CHECK();
      if( rc==SQLITE_OK ){
        rc = sqlite3OsReplaceFile(chunkFileGetVfs(&cs->file), zTmp,
                                  chunkFileGetFilename(&cs->file),
                                  &retainTmp);
      }
      if( rc!=SQLITE_OK ){
#if SQLITE_OS_WIN
        if( chunkFileGetHandle(&cs->file)==0 ){
          int restoreRc = gcReopenChunkFile(cs, 0);
          if( restoreRc!=SQLITE_OK ) rc = restoreRc;
        }
#endif
        if( !retainTmp ){
          sqlite3BeginBenignMalloc();
          sqlite3OsDelete(chunkFileGetVfs(&cs->file), zTmp, 0);
          sqlite3EndBenignMalloc();
        }
      }else{
        *pReplaced = 1;
#if !SQLITE_OS_WIN
        sqlite3OsCloseFree(pTmpFile);
        pTmpFile = 0;
#endif
      }

      if( *pReplaced ){
        if( pOldFile ){
          sqlite3OsCloseFree(pOldFile);
        }
        chunkFileSetHandle(&cs->file, 0);
        chunkFileSetSize(&cs->file, 0);
      }

      if( rc==SQLITE_OK ){
        int reopenAttempt;
        for(reopenAttempt=0; reopenAttempt<3; reopenAttempt++){
          pNewFile = 0;
          rc = gcReopenChunkFile(cs, &pNewFile);
          if( rc==SQLITE_OK ) break;
          if( pNewFile ){
            sqlite3OsCloseFree(pNewFile);
            pNewFile = 0;
          }
          /* Retrying would mask an allocation failure as success. */
          if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) break;
        }
      }

      if( rc==SQLITE_OK ){
        chunkFileSetHandle(&cs->file, pNewFile);
        chunkFileSetSize(&cs->file, finalSize);
        walStateSetDataSize(&cs->wal, 0);
      }else if( pNewFile ){
        sqlite3OsCloseFree(pNewFile);
      }
    }else{
      sqlite3BeginBenignMalloc();
      sqlite3OsDelete(chunkFileGetVfs(&cs->file), zTmp, 0);
      sqlite3EndBenignMalloc();
    }
    if( !*pReplaced && pTmpFile ){
      sqlite3OsCloseFree(pTmpFile);
    }
  }
  sqlite3_free(zTmp);

  *ppNewIndex = aNewIndex;
  *pnNewIndex = nNewIndex;
  *pnNewData = nDataBytes;
  return rc;
}

static int gcSweep(
  ChunkStore *cs,
  ProllyHashSet *marked,
  int *pKept,
  int *pRemoved
){
  int i, kept = 0, removed = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  i64 nNewData = 0;
  int rc = SQLITE_OK;
  int replaced = 0;

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      if( prollyHashSetContains(marked, &aIdx[i].hash) ){
        kept++;
      }else{
        removed++;
      }
    }
  }
  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; i<nPend; i++){
      if( prollyHashSetContains(marked, &aPend[i].hash) ){
        kept++;
      }
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      if( prollyHashSetContains(marked, &aRec[i].hash) ){
        kept++;
      }
    }
  }

  if( removed==0 && chunkStagingRecentCount(&cs->staging)==0 ){
    *pKept = kept;
    *pRemoved = 0;
    return SQLITE_OK;
  }

  if( chunkFileGetFilename(&cs->file)
   && strcmp(chunkFileGetFilename(&cs->file), ":memory:")!=0 ){
    rc = gcRewriteFile(cs, marked, &aNewIndex, &nNewIndex, &nNewData,
                       &replaced);
  }else{
    rc = gcBuildCompactedIndex(cs, marked, 0, &nNewData, &aNewIndex,
                               &nNewIndex);
  }

  if( rc==SQLITE_OK || replaced ){
    i64 indexSize = (i64)nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
    chunkIndexReplaceEntries(&cs->index, aNewIndex, nNewIndex);
    chunkIndexSetMetadata(&cs->index, nNewIndex,
                          CHUNK_MANIFEST_SIZE + nNewData, indexSize);
    walStateSetOffset(&cs->wal, CHUNK_MANIFEST_SIZE + nNewData + indexSize);
    aNewIndex = 0;

    chunkStagingResetAfterSweep(&cs->staging);
  }

  sqlite3_free(aNewIndex);

  *pKept = kept;
  *pRemoved = removed;
  return rc;
}

/* Report a gc phase failure without masking SQLITE_NOMEM / SQLITE_FULL. */
static void gcResultError(sqlite3_context *context, int rc, const char *zMsg){
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
    sqlite3_result_error_nomem(context);
  }else if( rc==SQLITE_FULL ){
    sqlite3_result_error_code(context, rc);
  }else{
    sqlite3_result_error(context, zMsg, -1);
    sqlite3_result_error_code(context, rc);
  }
}

/* Graph lock for gc. bBusyRetry spins (explicit dolt_gc). Checkpoint
** compaction must not: testvfs xWrite can re-enter while this thread
** already holds the graph lock (self-deadlock). */
static int gcLockAndRefresh(sqlite3 *db, ChunkStore *cs, int bBusyRetry){
  int rc;
  assert( sqlite3_mutex_held(db->mutex) );
  do {
    rc = chunkStoreLockAndRefresh(cs);
  }while( rc==SQLITE_BUSY && bBusyRetry && sqlite3InvokeBusyHandler(&db->busyHandler) );
  return rc;
}

/* Lock, mark, sweep, verify. On failure pzPhase names the stage; the lock
** is released before return on every acquired path. */
static int gcRun(
  sqlite3 *db,
  ChunkStore *cs,
  int *pnKept,
  int *pnRemoved,
  const char **pzPhase,
  char *zPhaseBuf,
  int nPhaseBuf,
  int bRequireExclusive,
  int bForceRefresh,
  int bBusyRetry
){
  ProllyHashSet marked;
  GcMarkTrace markTrace;
  int rc;

  *pnKept = 0;
  *pnRemoved = 0;

  if( bRequireExclusive && (!sqlite3_get_autocommit(db) || cs->lockDepth>0) ){
    *pzPhase = "gc requires exclusive access";
    return SQLITE_BUSY;
  }

  rc = gcLockAndRefresh(db, cs, bBusyRetry);
  if( rc!=SQLITE_OK ){
    *pzPhase = "failed to acquire lock for gc";
    return rc;
  }
  /* Sweep republishes by renaming over this path. A non-writable connection,
  ** or one whose file was replaced underneath it, must not. Checked after
  ** refresh, which detects the replacement. */
  if( cs->readOnly || cs->movedReadOnly ){
    chunkStoreUnlock(cs);
    *pzPhase = cs->readOnly
             ? "attempt to write a readonly database"
             : "cannot rewrite a database file that was replaced";
    return SQLITE_READONLY;
  }
  if( bForceRefresh ){
    rc = chunkStoreForceRefresh(cs);
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(cs);
      *pzPhase = "failed to refresh store for gc";
      return rc;
    }
  }

  rc = csMaterializeIndex(cs);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    *pzPhase = "gc index load failed";
    return rc;
  }

  rc = prollyHashSetInit(&marked, chunkIndexCount(&cs->index) > 64 ? chunkIndexCount(&cs->index) : 64);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    *pzPhase = "gc mark phase failed";
    return rc;
  }

  rc = gcMarkReachable(db, cs, &marked, &markTrace);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&marked);
    chunkStoreUnlock(cs);
    if( markTrace.hasMissing && zPhaseBuf && nPhaseBuf>0 ){
      gcFormatMarkFailure(zPhaseBuf, nPhaseBuf, &markTrace);
      *pzPhase = zPhaseBuf;
    }else{
      *pzPhase = "gc mark phase failed";
    }
    return rc;
  }

  rc = gcSweep(cs, &marked, pnKept, pnRemoved);
  prollyHashSetFree(&marked);
#ifdef DOLTLITE_PROLLY_CHECK
  if( rc==SQLITE_OK ) gcVerifySessionResolvable(db, cs);
#endif
  chunkStoreUnlock(cs);
  if( rc!=SQLITE_OK ) *pzPhase = "gc sweep phase failed";
  return rc;
}

static void doltliteGcFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int nKept = 0, nRemoved = 0;
  int rc;
  const char *zPhase = 0;
  char zPhaseBuf[256];
  char result[128];

  (void)argc;
  (void)argv;

  if( !cs ){
    sqlite3_result_error(context, "no database", -1);
    return;
  }

  if( !chunkFileGetFilename(&cs->file) || strcmp(chunkFileGetFilename(&cs->file), ":memory:")==0 ){
    sqlite3_result_text(context, "0 chunks removed, 0 chunks kept (in-memory)", -1, SQLITE_TRANSIENT);
    return;
  }

  rc = gcRun(db, cs, &nKept, &nRemoved, &zPhase, zPhaseBuf,
             sizeof(zPhaseBuf), 1, 1, 1);
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_BUSY ){
      sqlite3_result_error(context,
        zPhase ? zPhase : "database is locked by another connection", -1);
    }else{
      gcResultError(context, rc, zPhase);
    }
    return;
  }

  sqlite3_snprintf(sizeof(result), result,
    "%d chunks removed, %d chunks kept", nRemoved, nKept);
  sqlite3_result_text(context, result, -1, SQLITE_TRANSIENT);
}

/* Compaction targets one store. Mark from cs's own refs and session hashes
** sharing cs, so an ATTACH is marked from its own state. */
int doltliteGcCompactStoreWithPhase(
  sqlite3 *db,
  ChunkStore *cs,
  const char **pzPhase
){
  int nKept = 0, nRemoved = 0;

  if( !cs ) return SQLITE_OK;
  if( !chunkFileGetFilename(&cs->file) || strcmp(chunkFileGetFilename(&cs->file), ":memory:")==0 ){
    return SQLITE_OK;
  }
  if( !sqlite3_get_autocommit(db)
   || cs->lockDepth>0
   || cs->staging.nRecentUncommitted>0 ){
    return SQLITE_OK;
  }

  return gcRun(db, cs, &nKept, &nRemoved, pzPhase, 0, 0, 0, 0, 0);
}

int doltliteGcCompactStore(sqlite3 *db, ChunkStore *cs){
  const char *zPhase = 0;
  return doltliteGcCompactStoreWithPhase(db, cs, &zPhase);
}

int doltliteGcCompactDbWithPhase(sqlite3 *db, int iDb, const char **pzPhase){
  if( !db || iDb<0 || iDb>=db->nDb ) return SQLITE_OK;
  return doltliteGcCompactStoreWithPhase(
      db, doltliteBtreeChunkStore(db->aDb[iDb].pBt), pzPhase);
}

int doltliteGcRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_gc", 0,
                                  DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                  doltliteGcFunc, 0, 0);
}

#endif
