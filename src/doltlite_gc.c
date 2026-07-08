
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
  q->aItems = sqlite3_malloc(q->nAlloc * sizeof(GcQueueItem));
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
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  if( q->nItems >= q->nAlloc ){
    int newAlloc = q->nAlloc * 2;
    GcQueueItem *aNew = sqlite3_realloc(q->aItems, newAlloc * sizeof(GcQueueItem));
    if( !aNew ) return SQLITE_NOMEM;
    q->aItems = aNew;
    q->nAlloc = newAlloc;
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
  /* Only NOTFOUND means the sweep collected a live hash. Other errors
  ** (injected IO faults, OOM) leave the check inconclusive. */
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
  /* Diagnostic-only pass; allocation failures inside it are deliberately
  ** inconclusive (see gcVerifyHashCb), so mark them benign. */
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

static int gcAppendMarkedChunk(
  ChunkStore *cs,
  const ProllyHash *pHash,
  ProllyHashSet *marked,
  u8 **ppBuf,
  int *pnBuf,
  int *pnBufAlloc,
  i64 dataOffset,
  ChunkIndexEntry *aNewIndex,
  int *pnNewIndex
){
  u8 *chunkData = 0;
  int nChunkData = 0;
  i64 need;
  int rc;

  if( !prollyHashSetContains(marked, pHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, pHash, &chunkData, &nChunkData);
  if( rc!=SQLITE_OK ) return rc;

  if( nChunkData < 0 ){
    sqlite3_free(chunkData);
    return SQLITE_CORRUPT;
  }
  need = (i64)*pnBuf + 4 + (i64)nChunkData;
  if( need > (i64)0x7fffffff ){
    sqlite3_free(chunkData);
    return SQLITE_NOMEM;
  }
  if( need > (i64)*pnBufAlloc ){
    i64 newAlloc = *pnBufAlloc ? (i64)*pnBufAlloc * 2 : (i64)65536;
    u8 *pNew;
    while( newAlloc < need ){
      if( newAlloc > (i64)0x7fffffff/2 ){
        newAlloc = (i64)0x7fffffff;
        break;
      }
      newAlloc *= 2;
    }
    if( newAlloc < need || newAlloc > (i64)0x7fffffff ){
      sqlite3_free(chunkData);
      return SQLITE_NOMEM;
    }
    pNew = sqlite3_realloc(*ppBuf, (int)newAlloc);
    if( !pNew ){
      sqlite3_free(chunkData);
      return SQLITE_NOMEM;
    }
    *ppBuf = pNew;
    *pnBufAlloc = (int)newAlloc;
  }

  CS_WRITE_U32(*ppBuf + *pnBuf, nChunkData);

  memcpy(&aNewIndex[*pnNewIndex].hash, pHash, sizeof(ProllyHash));
  aNewIndex[*pnNewIndex].offset = dataOffset + *pnBuf;
  aNewIndex[*pnNewIndex].size = nChunkData;
  (*pnNewIndex)++;

  memcpy(*ppBuf + *pnBuf + 4, chunkData, nChunkData);
  *pnBuf += 4 + nChunkData;

  sqlite3_free(chunkData);
  return SQLITE_OK;
}

static int gcBuildCompactedData(
  ChunkStore *cs,
  ProllyHashSet *marked,
  u8 **ppNewData,
  int *pnNewData,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex
){
  int i;
  int kept = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  u8 *buf = 0;
  int nBuf = 0, nBufAlloc = 0;
  i64 dataOffset = CHUNK_MANIFEST_SIZE;
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

  aNewIndex = sqlite3_malloc((kept ? kept : 1) * (int)sizeof(ChunkIndexEntry));
  if( !aNewIndex ) return SQLITE_NOMEM;

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      rc = gcAppendMarkedChunk(cs, &aIdx[i].hash, marked, &buf, &nBuf,
                               &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aNewIndex);
        sqlite3_free(buf);
        return rc;
      }
    }
  }
  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; i<nPend; i++){
      rc = gcAppendMarkedChunk(cs, &aPend[i].hash, marked, &buf, &nBuf,
                               &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aNewIndex);
        sqlite3_free(buf);
        return rc;
      }
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      rc = gcAppendMarkedChunk(cs, &aRec[i].hash, marked, &buf, &nBuf,
                               &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aNewIndex);
        sqlite3_free(buf);
        return rc;
      }
    }
  }

  qsort(aNewIndex, nNewIndex, sizeof(aNewIndex[0]), csIndexEntryCmp);

  *ppNewData = buf;
  *pnNewData = nBuf;
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
  const u8 *pNewData,
  int nNewData,
  const ChunkIndexEntry *pNewIndex,
  int nNewIndex,
  int *pReplaced
){
  int i;
  int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
  i64 indexOffset = CHUNK_MANIFEST_SIZE + nNewData;
  u8 *indexBuf = 0;
  u8 manifest[CHUNK_MANIFEST_SIZE];
  ChunkStore manifestCs;
  int rc = SQLITE_OK;

  *pReplaced = 0;

#ifdef SQLITE_TEST
  {
    static int crashGcTarget = -2;
    static int crashGcCount = 0;
    if( crashGcTarget == -2 ){
      const char *zEnv = getenv("DOLTLITE_CRASH_GC_WRITE");
      crashGcTarget = zEnv ? atoi(zEnv) : -1;
    }
    if( crashGcTarget > 0 ) crashGcCount = 0;
#define GC_CRASH_CHECK() do{ \
  if( crashGcTarget>0 && ++crashGcCount>=crashGcTarget ){ \
    _exit(99); \
  } \
}while(0)
#else
#define GC_CRASH_CHECK() ((void)0)
#endif

  indexBuf = sqlite3_malloc(indexSize);
  if( !indexBuf ) return SQLITE_NOMEM;
  for(i=0; i<nNewIndex; i++){
    u8 *p = indexBuf + i * CHUNK_INDEX_ENTRY_SIZE;
    memcpy(p, pNewIndex[i].hash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    CS_WRITE_I64(p, pNewIndex[i].offset);
    p += 8;
    CS_WRITE_U32(p, (u32)pNewIndex[i].size);
  }

  manifestCs = *cs;
  chunkIndexSetMetadata(&manifestCs.index, nNewIndex, indexOffset, indexSize);
  walStateSetOffset(&manifestCs.wal, indexOffset + indexSize);

  csSerializeManifest(&manifestCs, manifest);
  csManifestSeal(manifest);

  if( chunkFileGetFilename(&cs->file) && strcmp(chunkFileGetFilename(&cs->file), ":memory:")!=0 ){
    char *zRaw = sqlite3_mprintf("%s-gc-tmp", chunkFileGetFilename(&cs->file));
    char *zTmp = 0;
    if( !zRaw ){
      sqlite3_free(indexBuf);
      return SQLITE_NOMEM;
    }
    /* Opened with SQLITE_OPEN_MAIN_DB below, so the name needs the VFS's
    ** double-nul terminator. */
    rc = chunkStoreDupFilenameDoubleNul(zRaw, &zTmp);
    sqlite3_free(zRaw);
    if( rc!=SQLITE_OK ){
      sqlite3_free(indexBuf);
      return rc;
    }

    {
      sqlite3_file *pTmpFile = 0;
      int tmpFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                   | SQLITE_OPEN_MAIN_DB;
      i64 writeOff = 0;

      /* Best-effort removal of a stale tmp file; a failure here (including
      ** the OS-layer fault probe) must not fail the GC. */
      sqlite3BeginBenignMalloc();
      sqlite3OsDelete(chunkFileGetVfs(&cs->file), zTmp, 0);
      sqlite3EndBenignMalloc();

      rc = sqlite3OsOpenMalloc(chunkFileGetVfs(&cs->file), zTmp, &pTmpFile, tmpFlags, 0);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTmp); sqlite3_free(indexBuf);
        return rc;
      }

      GC_CRASH_CHECK();
      rc = sqlite3OsWrite(pTmpFile, manifest, CHUNK_MANIFEST_SIZE, writeOff);
      writeOff += CHUNK_MANIFEST_SIZE;

      if( rc==SQLITE_OK && nNewData>0 ){
        const u8 *p = pNewData;
        int remaining = nNewData;
        while( remaining > 0 && rc==SQLITE_OK ){
          int toWrite = remaining > 65536 ? 65536 : remaining;
          GC_CRASH_CHECK();
          rc = sqlite3OsWrite(pTmpFile, p, toWrite, writeOff);
          p += toWrite;
          writeOff += toWrite;
          remaining -= toWrite;
        }
      }

      if( rc==SQLITE_OK && indexSize>0 ){
        const u8 *p = indexBuf;
        int remaining = indexSize;
        while( remaining > 0 && rc==SQLITE_OK ){
          int toWrite = remaining > 65536 ? 65536 : remaining;
          GC_CRASH_CHECK();
          rc = sqlite3OsWrite(pTmpFile, p, toWrite, writeOff);
          p += toWrite;
          writeOff += toWrite;
          remaining -= toWrite;
        }
      }

      if( rc==SQLITE_OK ){
        GC_CRASH_CHECK();
        rc = sqlite3OsTruncate(pTmpFile, writeOff);
      }
      if( rc==SQLITE_OK ){
        GC_CRASH_CHECK();
        rc = sqlite3OsSync(pTmpFile, SQLITE_SYNC_NORMAL);
      }
      if( rc==SQLITE_OK ){
        sqlite3_file *pOldFile = chunkFileGetHandle(&cs->file);
        sqlite3_file *pNewFile = 0;

#if SQLITE_OS_WIN
        /* Windows will not reliably replace either an open source file or an
        ** open destination file. Close both before the atomic replace call,
        ** and restore the destination handle if replacement fails. */
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
                                    chunkFileGetFilename(&cs->file));
        }
        if( rc!=SQLITE_OK ){
#if SQLITE_OS_WIN
          if( chunkFileGetHandle(&cs->file)==0 ){
            int restoreRc = gcReopenChunkFile(cs, 0);
            if( restoreRc!=SQLITE_OK ) rc = restoreRc;
          }
#endif
          sqlite3BeginBenignMalloc();
          sqlite3OsDelete(chunkFileGetVfs(&cs->file), zTmp, 0);
          sqlite3EndBenignMalloc();
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
          chunkFileSetSize(&cs->file, CHUNK_MANIFEST_SIZE + nNewData + indexSize);
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
  }

  sqlite3_free(indexBuf);
#ifdef SQLITE_TEST
  }
#undef GC_CRASH_CHECK
#endif
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
  u8 *buf = 0;
  int nBuf = 0;
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

  rc = gcBuildCompactedData(cs, marked, &buf, &nBuf, &aNewIndex, &nNewIndex);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcRewriteFile(cs, buf, nBuf, aNewIndex, nNewIndex, &replaced);

  if( rc==SQLITE_OK || replaced ){
    int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
    chunkIndexReplaceEntries(&cs->index, aNewIndex, nNewIndex);
    chunkIndexSetMetadata(&cs->index, nNewIndex,
                          CHUNK_MANIFEST_SIZE + nBuf, indexSize);
    walStateSetOffset(&cs->wal, CHUNK_MANIFEST_SIZE + nBuf + indexSize);
    aNewIndex = 0;

    chunkStagingResetAfterSweep(&cs->staging);
  }

  sqlite3_free(aNewIndex);
  sqlite3_free(buf);

  *pKept = kept;
  *pRemoved = removed;
  return rc;
}

/* Report a gc phase failure without masking canonical resource errors:
** callers (e.g. the fault harnesses) must see SQLITE_NOMEM / SQLITE_FULL
** when the underlying failure was one of those result codes, not a generic
** SQLITE_ERROR message. */
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

static int gcLockAndRefresh(sqlite3 *db, ChunkStore *cs){
  int rc;
  assert( sqlite3_mutex_held(db->mutex) );
  do {
    rc = chunkStoreLockAndRefresh(cs);
  }while( rc==SQLITE_BUSY && sqlite3InvokeBusyHandler(&db->busyHandler) );
  return rc;
}

/* Lock, mark, sweep and verify the chunk store. On failure pzPhase names the
** stage that failed so callers can report it; the lock is released before
** return on every path that acquired it. */
static int gcRun(
  sqlite3 *db,
  ChunkStore *cs,
  int *pnKept,
  int *pnRemoved,
  const char **pzPhase,
  char *zPhaseBuf,
  int nPhaseBuf,
  int bRequireExclusive,
  int bForceRefresh
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

  rc = gcLockAndRefresh(db, cs);
  if( rc!=SQLITE_OK ){
    *pzPhase = "failed to acquire lock for gc";
    return rc;
  }
  if( bForceRefresh ){
    rc = chunkStoreForceRefresh(cs);
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(cs);
      *pzPhase = "failed to refresh store for gc";
      return rc;
    }
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
             sizeof(zPhaseBuf), 1, 1);
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

int doltliteGcCompactWithPhase(sqlite3 *db, const char **pzPhase){
  ChunkStore *cs = doltliteGetChunkStore(db);
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

  return gcRun(db, cs, &nKept, &nRemoved, pzPhase, 0, 0, 0, 0);
}

int doltliteGcCompact(sqlite3 *db){
  const char *zPhase = 0;
  return doltliteGcCompactWithPhase(db, &zPhase);
}

int doltliteGcRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_gc", 0, SQLITE_UTF8, 0,
                                  doltliteGcFunc, 0, 0);
}

#endif
