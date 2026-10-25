
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "prolly_chunk_walk.h"

#include <string.h>
#include <stdio.h>
#ifdef SQLITE_CRASH_TEST
#include <unistd.h>
#endif

#include "chunk_store_int.h"
#include "doltlite_internal.h"

typedef struct GcSlots GcSlots;
struct GcSlots {
  i64 *a;
  sqlite3_file *pFile;
};

typedef struct GcMarks GcMarks;
struct GcMarks {
  ChunkIndexSpool *pIndex;
  Bitvec *pBits;
  int nEntries;
  int keepAll;
  GcSlots order;
  int nOrder;
};

typedef struct GcQueue GcQueue;
typedef struct GcQueueItem GcQueueItem;
struct GcQueueItem {
  ProllyHash hash;
  ProllyHash parent;
  int eSource;
  int eParentType;
  int iParentLevel;
  int nParentItems;
  int iChild;
};
struct GcQueue {
  GcQueueItem *aItems;
  int nItems;
  int nAlloc;
  int iHead;
  int nLimit;
  sqlite3_vfs *pVfs;
  GcMarks *marked;
  sqlite3_file *pFile;
  GcQueueItem *aWrite;
  int nWrite;
  i64 iRead;
  i64 iWrite;
};

typedef struct GcChildCtx GcChildCtx;
struct GcChildCtx {
  GcQueue *q;
  ChunkStore *cs;
  ProllyHash parent;
  int eSource;
  int eParentType;
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
  int eSource;
  int eParentType;
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

enum {
  GC_REFS, GC_BRANCH_COMMIT, GC_BRANCH_WORKING, GC_TAG, GC_TRACKING,
  GC_PENDING, GC_SESSION, GC_CHILD
};

static const char *gcSourceName(int eSource){
  static const char *azSource[] = {
    "refs-table", "branch-commit", "branch-working-set", "tag-commit",
    "tracking-commit", "pending", "session", "child"
  };
  return azSource[eSource];
}

#define GC_SLOTS_BYTES (32*1024*1024)

static int gcSlotsInit(GcSlots *p, sqlite3_vfs *pVfs, int n){
  i64 nLimit = GC_SLOTS_BYTES;
#ifdef SQLITE_TEST
  {
    const char *zLimit = getenv("DOLTLITE_GC_SLOTS_LIMIT");
    int nEnv;
    if( zLimit && sqlite3GetInt32(zLimit, &nEnv) && nEnv>=0 ) nLimit = nEnv;
  }
#endif
  memset(p, 0, sizeof(*p));
  if( (i64)n*(i64)sizeof(i64)<=nLimit ){
    p->a = sqlite3_malloc64((sqlite3_uint64)MAX(n,1)*sizeof(i64));
    return p->a ? SQLITE_OK : SQLITE_NOMEM;
  }
  return sqlite3OsOpenMalloc(pVfs, 0, &p->pFile,
      SQLITE_OPEN_TEMP_JOURNAL | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
    | SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_DELETEONCLOSE, 0);
}

static void gcSlotsFree(GcSlots *p){
  sqlite3_free(p->a);
  if( p->pFile ) sqlite3OsCloseFree(p->pFile);
  memset(p, 0, sizeof(*p));
}

static int gcSlotsPut(GcSlots *p, int i, i64 v){
  u8 a[8];
  if( p->a ){
    p->a[i] = v;
    return SQLITE_OK;
  }
  CS_WRITE_I64(a, v);
  return sqlite3OsWrite(p->pFile, a, sizeof(a), (i64)i*(i64)sizeof(a));
}

static int gcSlotsGet(GcSlots *p, int i, i64 *pV){
  u8 a[8];
  int rc;
  if( p->a ){
    *pV = p->a[i];
    return SQLITE_OK;
  }
  rc = sqlite3OsRead(p->pFile, a, sizeof(a), (i64)i*(i64)sizeof(a));
  if( rc==SQLITE_OK ) *pV = CS_READ_I64(a);
  return rc;
}

static void gcMarksFree(GcMarks *p){
  gcSlotsFree(&p->order);
  sqlite3BitvecDestroy(p->pBits);
  csIndexSpoolFree(p->pIndex);
  memset(p, 0, sizeof(*p));
}

static int gcMarksInit(GcMarks *p, ChunkStore *cs){
  int rc;
  memset(p, 0, sizeof(*p));
  rc = csIndexSnapshot(cs, 1, &p->pIndex);
  if( rc!=SQLITE_OK ) return rc;
  p->nEntries = csIndexSpoolCount(p->pIndex);
  if( p->nEntries>0 ){
    p->pBits = sqlite3BitvecCreate((u32)p->nEntries);
    if( !p->pBits ){
      gcMarksFree(p);
      return SQLITE_NOMEM;
    }
  }
  rc = gcSlotsInit(&p->order, chunkFileGetVfs(&cs->file), p->nEntries);
  if( rc!=SQLITE_OK ) gcMarksFree(p);
  return rc;
}

static int gcMarksTest(GcMarks *p, int i){
  assert( i>=0 && i<p->nEntries );
  return p->keepAll || sqlite3BitvecTest(p->pBits, (u32)i+1);
}

static int gcMarksAdd(GcMarks *p, const ProllyHash *h, int *pSeen){
  int i;
  int rc = csIndexSpoolFind(p->pIndex, h, &i);
  *pSeen = 0;
  if( rc!=SQLITE_OK || i<0 ) return rc;
  *pSeen = gcMarksTest(p, i);
  if( *pSeen ) return SQLITE_OK;
  rc = sqlite3BitvecSet(p->pBits, (u32)i+1);
  if( rc==SQLITE_OK ) rc = gcSlotsPut(&p->order, p->nOrder++, i);
  return rc;
}

#define GC_QUEUE_BUFFER 256
#define GC_QUEUE_BYTES (64*1024*1024)

static int gcQueueInit(
  GcQueue *q, sqlite3_vfs *pVfs, GcMarks *marked
){
  memset(q, 0, sizeof(*q));
  q->pVfs = pVfs;
  q->marked = marked;
  q->nLimit = GC_QUEUE_BYTES / sizeof(GcQueueItem) - 2*GC_QUEUE_BUFFER;
#ifdef SQLITE_TEST
  {
    const char *zLimit = getenv("DOLTLITE_GC_QUEUE_LIMIT");
    int nLimit;
    if( zLimit && sqlite3GetInt32(zLimit, &nLimit)
     && nLimit>0 && nLimit<q->nLimit ) q->nLimit = nLimit;
  }
#endif
  q->nAlloc = MIN(GC_QUEUE_BUFFER, q->nLimit);
  q->aItems = sqlite3_malloc(q->nAlloc * sizeof(GcQueueItem));
  return q->aItems ? SQLITE_OK : SQLITE_NOMEM;
}

static void gcQueueFree(GcQueue *q){
  if( q->pFile ) sqlite3OsCloseFree(q->pFile);
  sqlite3_free(q->aWrite);
  sqlite3_free(q->aItems);
  memset(q, 0, sizeof(*q));
}

static int gcQueueWrite(GcQueue *q, const GcQueueItem *a, int n){
  while( n>0 ){
    int nWrite = MIN(n, GC_QUEUE_BUFFER);
    int nByte = nWrite * (int)sizeof(GcQueueItem);
    int rc;
    if( q->iWrite>LARGEST_INT64-nByte ) return SQLITE_FULL;
    rc = sqlite3OsWrite(q->pFile, a, nByte, q->iWrite);
    if( rc!=SQLITE_OK ) return rc;
    q->iWrite += nByte;
    a += nWrite;
    n -= nWrite;
  }
  return SQLITE_OK;
}

static int gcQueueFlush(GcQueue *q){
  int rc = SQLITE_OK;
  if( q->nWrite ){
    rc = gcQueueWrite(q, q->aWrite, q->nWrite);
    if( rc==SQLITE_OK ) q->nWrite = 0;
  }
  return rc;
}

static int gcQueueSpill(GcQueue *q){
  int nFirst = MIN(q->nItems, q->nAlloc-q->iHead);
  int rc = sqlite3OsOpenMalloc(q->pVfs, 0, &q->pFile,
      SQLITE_OPEN_TEMP_JOURNAL | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
    | SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_DELETEONCLOSE, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = gcQueueWrite(q, &q->aItems[q->iHead], nFirst);
  if( rc==SQLITE_OK && nFirst<q->nItems ){
    rc = gcQueueWrite(q, q->aItems, q->nItems-nFirst);
  }
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_free(q->aItems);
  q->aItems = sqlite3_malloc(GC_QUEUE_BUFFER * sizeof(GcQueueItem));
  q->aWrite = sqlite3_malloc(GC_QUEUE_BUFFER * sizeof(GcQueueItem));
  if( !q->aItems || !q->aWrite ) return SQLITE_NOMEM;
  q->nAlloc = GC_QUEUE_BUFFER;
  q->nItems = q->iHead = 0;
  return SQLITE_OK;
}

static int gcQueuePush(
  GcQueue *q,
  const ProllyHash *h,
  const ProllyHash *pParent,
  int eSource,
  int eParentType,
  int iParentLevel,
  int nParentItems,
  int iChild
){
  GcQueueItem item;
  int rc, seen;
  assert( q->nItems>=0 && q->nItems<=q->nAlloc );
  assert( q->iHead>=0 && q->iHead<q->nAlloc );
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  /* Marking is complete only when every scheduled chunk has been walked. */
  rc = gcMarksAdd(q->marked, h, &seen);
  if( rc!=SQLITE_OK || seen ) return rc;
  if( !q->pFile && q->nItems==q->nAlloc ){
    int nNew = q->nAlloc * 2;
    /* A moving realloc can hold both rings until the copy completes. */
    if( nNew>q->nLimit-q->nAlloc ){
      rc = gcQueueSpill(q);
      if( rc!=SQLITE_OK ) return rc;
    }else{
      GcQueueItem *aNew = sqlite3_realloc(q->aItems,
                                         nNew * sizeof(GcQueueItem));
      if( !aNew ) return SQLITE_NOMEM;
      q->aItems = aNew;
      memcpy(&q->aItems[q->nAlloc], q->aItems,
             q->iHead * sizeof(GcQueueItem));
      q->nAlloc = nNew;
    }
  }
  memset(&item, 0, sizeof(item));
  item.hash = *h;
  if( pParent ) item.parent = *pParent;
  item.eSource = eSource;
  item.eParentType = eParentType;
  item.iParentLevel = iParentLevel;
  item.nParentItems = nParentItems;
  item.iChild = iChild;
  if( q->pFile ){
    q->aWrite[q->nWrite++] = item;
    return q->nWrite==GC_QUEUE_BUFFER ? gcQueueFlush(q) : SQLITE_OK;
  }
  q->aItems[(q->iHead+q->nItems)%q->nAlloc] = item;
  q->nItems++;
  return SQLITE_OK;
}

static int gcQueuePop(GcQueue *q, GcQueueItem *pItem){
  if( q->nItems==0 && q->pFile ){
    int nByte;
    int rc = gcQueueFlush(q);
    if( rc!=SQLITE_OK ) return rc;
    nByte = (int)MIN(q->iWrite-q->iRead,
                    GC_QUEUE_BUFFER * (i64)sizeof(GcQueueItem));
    if( nByte==0 ) return SQLITE_DONE;
    rc = sqlite3OsRead(q->pFile, q->aItems, nByte, q->iRead);
    if( rc!=SQLITE_OK ) return rc;
    q->iRead += nByte;
    q->iHead = 0;
    q->nItems = nByte / sizeof(GcQueueItem);
  }
  if( q->nItems==0 ) return SQLITE_DONE;
  *pItem = q->aItems[q->iHead];
  q->iHead = (q->iHead+1)%q->nAlloc;
  q->nItems--;
  return SQLITE_OK;
}

static int gcChildCb(void *ctx, const ProllyHash *pHash){
  GcChildCtx *p = (GcChildCtx*)ctx;
  int iChild = p->iNextChild++;
  return gcQueuePush(p->q, pHash, &p->parent, p->eSource,
                     p->eParentType, p->iParentLevel, p->nParentItems,
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
#ifdef SQLITE_TEST
  extern int sqlite3_io_error_benign;
  int savedIoBenign = sqlite3_io_error_benign;
  sqlite3_io_error_benign = 1;
#endif
  v.cs = cs; v.rc = SQLITE_OK;
  /* Diagnostic-only; allocation and I/O failures are inconclusive. */
  sqlite3BeginBenignMalloc();
  (void)doltliteSeedSessionHashes(db, cs, gcVerifySessionHashCb, &v);
  sqlite3EndBenignMalloc();
#ifdef SQLITE_TEST
  sqlite3_io_error_benign = savedIoBenign;
#endif
}
#endif

/* Drain the queue, marking everything reachable. A tolerant drain walks
** this connection's own roots, which may name garbage a peer's gc already
** swept (absent, not damaged); only chunks reachable from the refs must all
** be present. */
static int gcMarkDrain(
  sqlite3 *db,
  ChunkStore *cs,
  GcQueue *q,
  GcMarkTrace *pTrace,
  int localOnly,
  int *pPartial,
  int bTolerant
){
  GcQueueItem current;
  int rc;
  while( (rc = gcQueuePop(q, &current))==SQLITE_OK ){
    u8 *data = 0;
    int nData = 0;
    GcChildCtx childCtx;

    if( sqlite3_is_interrupted(db) ){
      rc = SQLITE_INTERRUPT;
      break;
    }

    if( localOnly ){
      int has = 0;
      rc = chunkStoreHas(cs, &current.hash, &has);
      if( rc!=SQLITE_OK ) break;
      if( !has ){
        *pPartial = 1;
        continue;
      }
    }
    rc = chunkStoreGet(cs, &current.hash, &data, &nData);
    if( bTolerant && rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK;
      continue;
    }
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
        pTrace->hasMissing = 1;
        pTrace->rc = rc;
        memcpy(&pTrace->missing, &current.hash, sizeof(ProllyHash));
        memcpy(&pTrace->parent, &current.parent, sizeof(ProllyHash));
        pTrace->eSource = current.eSource;
        pTrace->eParentType = current.eParentType;
        pTrace->iParentLevel = current.iParentLevel;
        pTrace->nParentItems = current.nParentItems;
        pTrace->iChild = current.iChild;
      }
      break;
    }

    childCtx.q = q;
    childCtx.cs = cs;
    childCtx.parent = current.hash;
    childCtx.eSource = GC_CHILD;
    childCtx.eParentType = doltliteClassifyChunk(data, nData);
    childCtx.iParentLevel = -1;
    childCtx.nParentItems = -1;
    childCtx.iNextChild = 0;
    if( childCtx.eParentType==CHUNK_PROLLY_NODE ){
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

  return rc;
}

static int gcMarkReachable(
  sqlite3 *db,
  ChunkStore *cs,
  GcMarks *marked,
  GcMarkTrace *pTrace
){
  GcQueue queue;
  GcChildCtx seedCtx;
  int localOnly = chunkStoreSourceEnabled(cs);
  int partial = 0;
  int rc, i;

  memset(pTrace, 0, sizeof(*pTrace));
  rc = gcQueueInit(&queue, chunkFileGetVfs(&cs->file), marked);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcQueuePush(&queue, refsTableGetHash(&cs->refs), 0, GC_REFS,
                   0, -1, -1, -1);

  {
    int nBr; const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    for(i=0; rc==SQLITE_OK && i<nBr; i++){
      rc = gcQueuePush(&queue, &aBr[i].commitHash, 0, GC_BRANCH_COMMIT,
                       0, -1, -1, -1);
      if( rc==SQLITE_OK ){
        rc = gcQueuePush(&queue, &aBr[i].workingSetHash, 0,
                         GC_BRANCH_WORKING, 0, -1, -1, -1);
      }
    }
  }

  {
    int nTg; const TagRef *aTg;
    refsTableGetTags(&cs->refs, &nTg, &aTg);
    for(i=0; rc==SQLITE_OK && i<nTg; i++){
      rc = gcQueuePush(&queue, &aTg[i].commitHash, 0, GC_TAG,
                       0, -1, -1, -1);
    }
  }

  {
    int nTk; const TrackingBranch *aTk;
    refsTableGetTracking(&cs->refs, &nTk, &aTk);
    for(i=0; rc==SQLITE_OK && i<nTk; i++){
      rc = gcQueuePush(&queue, &aTk[i].commitHash, 0, GC_TRACKING,
                       0, -1, -1, -1);
    }
  }

  if( rc==SQLITE_OK ){
    rc = gcMarkDrain(db, cs, &queue, pTrace, localOnly, &partial, 0);
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  }

  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; rc==SQLITE_OK && i<nPend; i++){
      rc = gcQueuePush(&queue, &aPend[i].hash, 0, GC_PENDING,
                       0, -1, -1, -1);
    }
    for(i=0; rc==SQLITE_OK && i<cs->nStoredHit; i++){
      rc = gcQueuePush(&queue, &cs->aStoredHit[i], 0, GC_PENDING,
                       0, -1, -1, -1);
    }
  }

  if( rc==SQLITE_OK ){
    memset(&seedCtx, 0, sizeof(seedCtx));
    seedCtx.q = &queue;
    seedCtx.cs = cs;
    seedCtx.eSource = GC_SESSION;
    rc = doltliteSeedSessionHashes(db, cs, gcSessionChildCb, &seedCtx);
  }

  if( rc==SQLITE_OK ){
    rc = gcMarkDrain(db, cs, &queue, pTrace, localOnly, &partial, 1);
  }

  if( rc==SQLITE_DONE && partial ){
    /* Missing parents can hide cached descendants. Without the complete
    ** graph, unmarked local chunks are not provably unreachable. */
    marked->keepAll = 1;
  }
  gcQueueFree(&queue);
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
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
      zMissing, gcSourceName(pTrace->eSource), pTrace->rc);
  }else{
    doltliteHashToHex(&pTrace->parent, zParent);
    if( pTrace->eParentType==CHUNK_PROLLY_NODE ){
      sqlite3_snprintf(nBuf, zBuf,
        "gc mark phase failed: missing chunk %s parent=%s source=%s "
        "parent_type=%s parent_level=%d parent_items=%d child_index=%d rc=%d",
        zMissing, zParent, gcSourceName(pTrace->eSource),
        gcChunkTypeName(pTrace->eParentType), pTrace->iParentLevel, pTrace->nParentItems,
        pTrace->iChild, pTrace->rc);
    }else{
      sqlite3_snprintf(nBuf, zBuf,
        "gc mark phase failed: missing chunk %s parent=%s source=%s "
        "parent_type=%s child_index=%d rc=%d",
        zMissing, zParent, gcSourceName(pTrace->eSource),
        gcChunkTypeName(pTrace->eParentType),
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
#define GC_WRITER_BUF 65536
struct GcFileWriter {
  sqlite3_file *pFile;
  i64 iOff;
  int nBuf;
  u8 *aBuf;
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
    int space = GC_WRITER_BUF - pW->nBuf;
    int nCopy = n<space ? n : space;
    memcpy(pW->aBuf + pW->nBuf, p, nCopy);
    pW->nBuf += nCopy;
    p += nCopy;
    n -= nCopy;
    if( pW->nBuf==GC_WRITER_BUF ){
      int rc = gcWriterFlush(pW);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int gcStreamMarkedChunk(
  ChunkStore *cs,
  const ProllyHash *pHash,
  int isMarked,
  GcFileWriter *pW,
  i64 *piPos,
  ChunkIndexEntry *aNewIndex,
  int *pnNewIndex
){
  u8 *chunkData = 0;
  int nChunkData = 0;
  int rc;

  if( !isMarked ) return SQLITE_OK;

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
  GcMarks *marked,
  GcFileWriter *pW,
  i64 *pnNewData,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex
){
  int i, kept = 0, nNew = 0;
  ChunkIndexEntry *aNew;
  i64 iPos = CHUNK_MANIFEST_SIZE;
  int rc = SQLITE_OK;
  for(i=0; i<marked->nEntries; i++) kept += gcMarksTest(marked, i);
  if( kept>INT_MAX/(int)sizeof(*aNew) ) return SQLITE_NOMEM;
  aNew = sqlite3_malloc64((sqlite3_uint64)MAX(kept,1)*sizeof(*aNew));
  if( !aNew ) return SQLITE_NOMEM;
  for(i=0; rc==SQLITE_OK && i<marked->nEntries; i++){
    ChunkIndexEntry e;
    if( !gcMarksTest(marked, i) ) continue;
    rc = csIndexSpoolGet(marked->pIndex, i, &e);
    if( rc==SQLITE_OK ){
      rc = gcStreamMarkedChunk(cs, &e.hash, 1, pW, &iPos, aNew, &nNew);
    }
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(aNew);
    return rc;
  }
  *pnNewData = iPos-CHUNK_MANIFEST_SIZE;
  *ppNewIndex = aNew;
  *pnNewIndex = nNew;
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

/* Write the compacted image of cs (manifest, marked chunks, sorted index)
** to zPath, which must carry the VFS double-nul terminator. On success the
** open, synced file handle is returned for the caller to swap or close;
** the source store is not touched. */
static int gcCopyChunk(
  ChunkStore *cs,
  GcMarks *marked,
  int i,
  GcFileWriter *pW,
  GcSlots *pOffsets,
  i64 *piPos
){
  ChunkIndexEntry e;
  u8 *data = 0;
  int nData = 0;
  u8 aLen[4];
  int rc = csIndexSpoolGet(marked->pIndex, i, &e);
  if( rc==SQLITE_OK ) rc = chunkStoreGet(cs, &e.hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( nData!=e.size ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  rc = gcSlotsPut(pOffsets, i, *piPos);
  *piPos += 4+(i64)nData;
  CS_WRITE_U32(aLen, nData);
  if( rc==SQLITE_OK ) rc = gcWriterAppend(pW, aLen, sizeof(aLen));
  if( rc==SQLITE_OK ) rc = gcWriterAppend(pW, data, nData);
  sqlite3_free(data);
  return rc;
}

static int gcWriteCompactedTo(
  ChunkStore *cs,
  GcMarks *marked,
  sqlite3_vfs *pVfs,
  const char *zPath,
  int bDeleteExisting,
  int *pbTargetNonEmpty,
  sqlite3_file **ppFileOut,
  i64 *pFinalSize,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex,
  i64 *pnNewData
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
  sqlite3_vfs *pOutVfs;
  int rc = SQLITE_OK;

  *ppFileOut = 0;
  *pFinalSize = 0;
  *ppNewIndex = 0;
  *pnNewIndex = 0;
  *pnNewData = 0;
  /* NULL keeps in-place GC on the source VFS. VACUUM INTO passes the
  ** target VFS so a memdb (or other) destination is not opened on unix. */
  pOutVfs = pVfs ? pVfs : chunkFileGetVfs(&cs->file);

  gcCrashResetForRun();

  /* Manifest leads the file, so compacted geometry must be known before
  ** streaming. The streaming pass re-checks layout against fetched data. */
  for(i=0; i<marked->nEntries; i++){
    ChunkIndexEntry e;
    if( !gcMarksTest(marked, i) ) continue;
    rc = csIndexSpoolGet(marked->pIndex, i, &e);
    if( rc!=SQLITE_OK ) return rc;
    kept++;
    nDataBytes += 4+(i64)e.size;
  }
  indexSize = (i64)kept * CHUNK_INDEX_ENTRY_SIZE;
  finalSize = CHUNK_MANIFEST_SIZE + nDataBytes + indexSize;

  manifestCs = *cs;
  chunkIndexSetMetadata(&manifestCs.index, kept,
                        CHUNK_MANIFEST_SIZE + nDataBytes, indexSize);
  walStateSetOffset(&manifestCs.wal, finalSize);
  csSerializeManifest(&manifestCs, manifest);
  csManifestSeal(manifest, 0);

  if( kept<=8192 ){
    aNewIndex = sqlite3_malloc64((sqlite3_uint64)MAX(kept,1)*sizeof(*aNewIndex));
    if( !aNewIndex ) return SQLITE_NOMEM;
  }

  {
    sqlite3_file *pTmpFile = 0;
    int tmpFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                 | SQLITE_OPEN_MAIN_DB;
    GcFileWriter w, indexWriter;
    GcSlots offsets;

    if( bDeleteExisting ){
      /* Best-effort stale-tmp removal; failure here must not fail GC. */
      sqlite3BeginBenignMalloc();
      sqlite3OsDelete(pOutVfs, zPath, 0);
      sqlite3EndBenignMalloc();
    }

    rc = sqlite3OsOpenMalloc(pOutVfs, zPath, &pTmpFile, tmpFlags, 0);
    if( rc==SQLITE_OK && bDeleteExisting ){
      rc = sqlite3OsDoltliteModeOf(pTmpFile, chunkFileGetFilename(&cs->file), 0);
    }
    if( rc != SQLITE_OK ){
      if( pTmpFile ) sqlite3OsCloseFree(pTmpFile);
      sqlite3_free(aNewIndex);
      return rc;
    }

    /* The pre-open emptiness probe is advisory; only the handle we hold is
    ** authoritative. A writer that filled the target between the probe and
    ** this open must not be clobbered — refuse and leave their file alone. */
    if( pbTargetNonEmpty ){
      i64 szNow = 0;
      rc = csFileLockOutput(pTmpFile);
      if( rc==SQLITE_OK ) rc = sqlite3OsFileSize(pTmpFile, &szNow);
      if( rc==SQLITE_OK && szNow>0 ){
        *pbTargetNonEmpty = 1;
        rc = SQLITE_ERROR;
      }
      if( rc!=SQLITE_OK ){
        sqlite3OsCloseFree(pTmpFile);
        sqlite3_free(aNewIndex);
        return rc;
      }
      sqlite3OsDoltliteRequestDirSync(pTmpFile);
    }

    w.pFile = pTmpFile;
    w.iOff = 0;
    w.nBuf = 0;
    w.aBuf = sqlite3_malloc(2*GC_WRITER_BUF);
    if( !w.aBuf ){
      sqlite3OsCloseFree(pTmpFile);
      sqlite3_free(aNewIndex);
      return SQLITE_NOMEM;
    }

    memset(&offsets, 0, sizeof(offsets));
    rc = gcWriterAppend(&w, manifest, CHUNK_MANIFEST_SIZE);

    indexWriter.pFile = pTmpFile;
    indexWriter.iOff = CHUNK_MANIFEST_SIZE+nDataBytes;
    indexWriter.nBuf = 0;
    indexWriter.aBuf = w.aBuf+GC_WRITER_BUF;
    /* Data goes out in mark order, the index in hash order. Marking is FIFO
    ** and pushes a node's children together, so siblings land adjacent and
    ** readahead fetches them in one read; hash order scatters them. */
    if( rc==SQLITE_OK ){
      rc = gcSlotsInit(&offsets, chunkFileGetVfs(&cs->file), marked->nEntries);
    }
    for(i=0; rc==SQLITE_OK && i<marked->nOrder; i++){
      i64 iSpool;
      rc = gcSlotsGet(&marked->order, i, &iSpool);
      if( rc==SQLITE_OK ){
        rc = gcCopyChunk(cs, marked, (int)iSpool, &w, &offsets, &iPos);
      }
    }
    for(i=0; rc==SQLITE_OK && marked->keepAll && i<marked->nEntries; i++){
      if( marked->pBits && sqlite3BitvecTest(marked->pBits, (u32)i+1) ){
        continue;
      }
      rc = gcCopyChunk(cs, marked, i, &w, &offsets, &iPos);
    }
    for(i=0; rc==SQLITE_OK && i<marked->nEntries; i++){
      ChunkIndexEntry e;
      i64 iOffset = 0;
      u8 aEntry[CHUNK_INDEX_ENTRY_SIZE];
      if( !gcMarksTest(marked, i) ) continue;
      rc = csIndexSpoolGet(marked->pIndex, i, &e);
      if( rc==SQLITE_OK ) rc = gcSlotsGet(&offsets, i, &iOffset);
      if( rc!=SQLITE_OK ) break;
      e.offset = iOffset;
      if( aNewIndex ) aNewIndex[nNewIndex] = e;
      nNewIndex++;
      memcpy(aEntry, e.hash.data, PROLLY_HASH_SIZE);
      CS_WRITE_I64(aEntry+PROLLY_HASH_SIZE, e.offset);
      CS_WRITE_U32(aEntry+PROLLY_HASH_SIZE+8, e.size);
      rc = gcWriterAppend(&indexWriter, aEntry, sizeof(aEntry));
    }
    gcSlotsFree(&offsets);
    if( rc==SQLITE_OK
     && (nNewIndex!=kept || iPos!=CHUNK_MANIFEST_SIZE+nDataBytes) ){
      rc = SQLITE_CORRUPT;
    }
    if( rc==SQLITE_OK ) rc = gcWriterFlush(&w);
    if( rc==SQLITE_OK ) rc = gcWriterFlush(&indexWriter);

    if( rc==SQLITE_OK ){
      GC_CRASH_CHECK();
      rc = sqlite3OsTruncate(pTmpFile, finalSize);
    }
    if( rc==SQLITE_OK ){
      GC_CRASH_CHECK();
      rc = sqlite3OsSync(pTmpFile,
          cs->fullFsync ? SQLITE_SYNC_FULL : SQLITE_SYNC_NORMAL);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(w.aBuf);
#if !SQLITE_OS_UNIX
      sqlite3OsCloseFree(pTmpFile);
#endif
      sqlite3BeginBenignMalloc();
      sqlite3OsDelete(pOutVfs, zPath, 0);
      sqlite3EndBenignMalloc();
#if SQLITE_OS_UNIX
      sqlite3OsCloseFree(pTmpFile);
#endif
      sqlite3_free(aNewIndex);
      return rc;
    }
    sqlite3_free(w.aBuf);
    *ppFileOut = pTmpFile;
  }
  *pFinalSize = finalSize;
  *ppNewIndex = aNewIndex;
  *pnNewIndex = nNewIndex;
  *pnNewData = nDataBytes;
  return SQLITE_OK;
}

static int gcRewriteFile(
  ChunkStore *cs,
  GcMarks *marked,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex,
  i64 *pnNewData,
  int *pReplaced
){
  i64 finalSize = 0;
  char *zRaw;
  char *zTmp = 0;
  sqlite3_file *pTmpFile = 0;
  int rc;
  int retainTmp = 0;

  *pReplaced = 0;
  *ppNewIndex = 0;
  *pnNewIndex = 0;
  *pnNewData = 0;

  zRaw = sqlite3_mprintf("%s-gc-tmp", chunkFileGetFilename(&cs->file));
  if( !zRaw ) return SQLITE_NOMEM;
  /* SQLITE_OPEN_MAIN_DB: the name needs the VFS's double-nul terminator. */
  rc = chunkStoreDupFilenameDoubleNul(zRaw, &zTmp);
  sqlite3_free(zRaw);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcWriteCompactedTo(cs, marked, 0, zTmp, 1, 0, &pTmpFile, &finalSize,
                          ppNewIndex, pnNewIndex, pnNewData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zTmp);
    return rc;
  }

  {
    {
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
      rc = chunkStoreReplaceLockedFile(cs, zTmp, &retainTmp);
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
    }
    if( !*pReplaced && pTmpFile ){
      sqlite3OsCloseFree(pTmpFile);
    }
  }
  sqlite3_free(zTmp);
  return rc;
}

static int gcSweep(
  ChunkStore *cs,
  GcMarks *marked,
  int *pKept,
  int *pRemoved
){
  int i, kept = 0, removed = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  i64 nNewData = 0;
  int rc = SQLITE_OK;
  int replaced = 0;

  for(i=0; i<marked->nEntries; i++){
    if( gcMarksTest(marked, i) ) kept++;
    else removed++;
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
    csIndexCacheFree(cs);
    chunkIndexSetMetadata(&cs->index, nNewIndex,
                          CHUNK_MANIFEST_SIZE + nNewData, indexSize);
    if( aNewIndex ) chunkIndexReplaceEntries(&cs->index, aNewIndex, nNewIndex);
    else csIndexInstallFlat(&cs->index);
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

/* Graph lock for gc. bBusyRetry waits for explicit commands. Checkpoint
** compaction must not: testvfs xWrite can re-enter while this thread
** already holds the graph lock (self-deadlock). */
static int gcLockAndRefresh(
  sqlite3 *db,
  ChunkStore *cs,
  int bBusyRetry,
  int *pChanged
){
  int rc;
  assert( sqlite3_mutex_held(db->mutex) );
  do {
    rc = chunkStoreLockAndRefreshChanged(cs, pChanged);
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
  GcMarks marked;
  GcMarkTrace markTrace;
  u32 reloadGen;
  int bRefreshed = 0;
  int rc;

  *pnKept = 0;
  *pnRemoved = 0;

  if( bRequireExclusive && (!sqlite3_get_autocommit(db) || cs->lockDepth>0) ){
    *pzPhase = "gc requires exclusive access";
    return SQLITE_BUSY;
  }

  chunkStoreSourceCloseWriter(cs);
  reloadGen = cs->reloadGen;
  rc = gcLockAndRefresh(db, cs, bBusyRetry, &bRefreshed);
  if( rc!=SQLITE_OK ){
    *pzPhase = "failed to acquire lock for gc";
    return rc;
  }
  /* Sweep republishes by renaming over this path. A non-writable connection,
  ** or one whose file was replaced underneath it, must not. Checked after
  ** refresh, which detects the replacement. */
  if( chunkStoreWriteRefused(cs) ){
    chunkStoreUnlock(cs);
    *pzPhase = cs->movedReadOnly
             ? "cannot rewrite a database file that was replaced"
             : "attempt to write a readonly database";
    return SQLITE_READONLY;
  }
  if( bForceRefresh ){
    rc = chunkStoreForceRefresh(cs);
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(cs);
      *pzPhase = "failed to refresh store for gc";
      return rc;
    }
  }else if( cs->snapshotPinned ){
    /* Marking from a statement's pinned snapshot would sweep a peer write
    ** made since it began out of the republished store. */
    int bChanged = 0;
    rc = chunkStoreHasExternalChanges(cs, &bChanged);
    if( rc==SQLITE_OK && bChanged ) rc = SQLITE_BUSY;
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(cs);
      *pzPhase = "database changed during gc; retry";
      return rc;
    }
  }

  /* These refreshes consume the store-changed signal the next statement
  ** would reload the session on; without it, it reads the catalog it had,
  ** which may be a peer-superseded one this sweep collects. */
  if( bRefreshed || cs->reloadGen!=reloadGen ){
    doltliteInvalidateSessionWorkingState(db);
  }

  rc = gcMarksInit(&marked, cs);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    *pzPhase = "gc mark phase failed";
    return rc;
  }

  rc = gcMarkReachable(db, cs, &marked, &markTrace);
  if( rc!=SQLITE_OK ){
    gcMarksFree(&marked);
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
  gcMarksFree(&marked);
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

  if( !chunkFileGetFilename(&cs->file) || cs->isSharedMemory
   || strcmp(chunkFileGetFilename(&cs->file), ":memory:")==0 ){
    sqlite3_result_text(context, "0 chunks removed, 0 chunks kept (in-memory)", -1, SQLITE_TRANSIENT);
    return;
  }

  rc = gcRun(db, cs, &nKept, &nRemoved, &zPhase, zPhaseBuf,
             sizeof(zPhaseBuf), 1, 1, 1);
  /* The sweep's refresh adopted a peer tip and spent the change signal. */
  doltliteNoteSessionMissedDiskTip(db);
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_BUSY ){
      sqlite3_result_error(context,
        zPhase ? zPhase : "database is locked by another connection", -1);
      sqlite3_result_error_code(context, SQLITE_BUSY);
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
static int doltliteGcCompactStoreWithPhase(
  sqlite3 *db,
  ChunkStore *cs,
  const char **pzPhase,
  int bBusyRetry
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

  return gcRun(db, cs, &nKept, &nRemoved, pzPhase, 0, 0, 0, 0, bBusyRetry);
}

int doltliteGcCompactStore(sqlite3 *db, ChunkStore *cs){
  const char *zPhase = 0;
  return doltliteGcCompactStoreWithPhase(db, cs, &zPhase, 0);
}

/* mode=ro and immutable=1 are not compacted writes. Stock's ATTACH of the
** target decides the message: a missing mode=ro path cannot be opened, a
** non-empty SQLite database already exists, and every other readonly open
** refuses the write. immutable=1 still creates a zero-byte file. */
static int gcVacuumIntoReadonly(
  sqlite3 *db,
  sqlite3_vfs *pVfs,
  const char *zPath,
  const char *zOriginal,
  unsigned int flags,
  int bImmutable,
  const char **pzPhase
){
  sqlite3_file *pProbe = 0;
  unsigned int probeFlags;
  i64 sz = 0;
  int nHdr = 0;
  int rc;
  char aHdr[16];

  if( bImmutable && (flags & SQLITE_OPEN_READONLY)==0 ){
    probeFlags = flags;
    probeFlags &= ~(unsigned int)SQLITE_OPEN_READONLY;
    probeFlags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                | SQLITE_OPEN_MAIN_DB;
  }else{
    probeFlags = flags | SQLITE_OPEN_MAIN_DB;
  }
  /* zPath is a plain double-NUL name. Leaving URI set would make unixOpen
  ** read option bytes that this copy does not have. */
  probeFlags &= ~(unsigned int)SQLITE_OPEN_URI;

  rc = sqlite3OsOpenMalloc(pVfs, zPath, &pProbe, (int)probeFlags, 0);
  if( rc!=SQLITE_OK || !pProbe ){
    if( pProbe ) sqlite3OsCloseFree(pProbe);
    sqlite3ErrorWithMsg(db, rc==SQLITE_OK ? SQLITE_CANTOPEN : rc,
                        "unable to open database: %s", zOriginal);
    *pzPhase = sqlite3_errmsg(db);
    return rc==SQLITE_OK ? SQLITE_CANTOPEN : rc;
  }
  rc = sqlite3OsFileSize(pProbe, &sz);
  if( rc==SQLITE_OK && sz>=16 ){
    rc = sqlite3OsRead(pProbe, aHdr, 16, 0);
    if( rc==SQLITE_OK ) nHdr = 16;
    else if( rc==SQLITE_IOERR_SHORT_READ ) rc = SQLITE_OK;
  }
  sqlite3OsCloseFree(pProbe);
  if( rc!=SQLITE_OK ){
    sqlite3ErrorWithMsg(db, rc, "unable to open database: %s", zOriginal);
    *pzPhase = sqlite3_errmsg(db);
    return rc;
  }
  if( sz>0 && nHdr==16 && memcmp(aHdr, "SQLite format 3", 16)==0 ){
    *pzPhase = "output file already exists";
    return SQLITE_ERROR;
  }
  *pzPhase = "attempt to write a readonly database";
  return SQLITE_READONLY;
}

/* VACUUM INTO for a doltlite-format database: write a compacted copy of
** the store — the same image gc would swap in, so same committed data and
** current working state, dead chunks dropped — to zOut. The source store
** is only read; nothing is swapped or adopted. Stock's contract for the
** target is kept: it must not exist unless it is an empty file. zOut is
** parsed the same way ATTACH parses a filename. */
int doltliteGcVacuumInto(
  sqlite3 *db,
  int iDb,
  const char *zOut,
  const char **pzPhase
){
  ChunkStore *cs;
  GcMarks marked;
  GcMarkTrace markTrace;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  i64 nNewData = 0;
  i64 finalSize = 0;
  sqlite3_file *pOutFile = 0;
  CsFileLock pDestLock = 0;
  char *zDestLockName = 0;
  char *zPath = 0;
  char *zFile = 0;
  char *zErr = 0;
  sqlite3_vfs *pVfs = 0;
  unsigned int openFlags;
  int bMem = 0;
  int bImmutable = 0;
  int bRefreshed = 0;
  int rc;

  *pzPhase = 0;
  if( !db || iDb<0 || iDb>=db->nDb ) return SQLITE_ERROR;
  cs = doltliteBtreeChunkStore(db->aDb[iDb].pBt);
  if( !cs || !chunkFileGetFilename(&cs->file) || cs->isSharedMemory
   || strcmp(chunkFileGetFilename(&cs->file), ":memory:")==0 ){
    *pzPhase = "cannot VACUUM an in-memory database INTO a file";
    return SQLITE_ERROR;
  }
  if( !zOut || zOut[0]==0 ){
    *pzPhase = "non-text filename";
    return SQLITE_ERROR;
  }
  /* A literal :memory: target has no chunk-store file to write; without
  ** this the VFS would create a file named ":memory:". A file: URI that
  ** selects memdb is a different target and is written below. */
  if( strcmp(zOut, ":memory:")==0 ){
    *pzPhase = "cannot VACUUM a doltlite database INTO an in-memory target";
    return SQLITE_ERROR;
  }

  /* Same flag adjustment stock VACUUM makes before ATTACH parses zOut. */
  openFlags = (unsigned int)db->openFlags;
  openFlags &= ~(unsigned int)SQLITE_OPEN_READONLY;
  openFlags |= SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
  rc = sqlite3ParseUri(db->pVfs ? db->pVfs->zName : 0, zOut, &openFlags,
                       &pVfs, &zFile, &zErr);
  if( rc!=SQLITE_OK ){
    if( zErr ){
      sqlite3ErrorWithMsg(db, rc, "%s", zErr);
      *pzPhase = sqlite3_errmsg(db);
      sqlite3_free(zErr);
    }else{
      *pzPhase = "out of memory";
    }
    return rc;
  }
  bMem = ((openFlags & SQLITE_OPEN_MEMORY)!=0) || sqlite3IsMemdb(pVfs);
  bImmutable = sqlite3_uri_boolean(zFile, "immutable", 0);
  rc = chunkStoreDupFilenameDoubleNul(zFile, &zPath);
  sqlite3_free_filename(zFile);
  zFile = 0;
  if( rc!=SQLITE_OK ){
    *pzPhase = "out of memory";
    return rc;
  }
  if( bMem && !sqlite3IsMemdb(pVfs) ){
    pVfs = sqlite3_vfs_find("memdb");
    if( !pVfs ){
      sqlite3_free(zPath);
      sqlite3ErrorWithMsg(db, SQLITE_ERROR, "no such vfs: memdb");
      *pzPhase = sqlite3_errmsg(db);
      return SQLITE_ERROR;
    }
  }
  if( !bMem
   && ((openFlags & SQLITE_OPEN_READONLY) || bImmutable) ){
    rc = gcVacuumIntoReadonly(db, pVfs, zPath, zOut, openFlags, bImmutable,
                              pzPhase);
    sqlite3_free(zPath);
    return rc;
  }

  /* Reserve the destination before anything is examined or written there.
  ** The source graph lock says nothing about another process committing a
  ** database at this path, and the emptiness check below would otherwise be
  ** a guess that a peer can invalidate before the first byte goes out.
  ** A memdb target has no disk sidecar to lock. */
  if( !bMem ){
    rc = csFileLock(pVfs, zPath, SQLITE_OPEN_READONLY,
                    &pDestLock, &zDestLockName);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zPath);
      *pzPhase = "output file is in use by another connection";
      return rc==SQLITE_BUSY ? SQLITE_BUSY : rc;
    }
  }

  rc = gcLockAndRefresh(db, cs, 1, &bRefreshed);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zPath);
    csFileUnlock(pDestLock, &zDestLockName);
    *pzPhase = "failed to acquire lock for vacuum into";
    return rc;
  }
  if( bRefreshed ) doltliteInvalidateSessionWorkingState(db);
  rc = gcMarksInit(&marked, cs);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zPath);
    chunkStoreUnlock(cs);
    csFileUnlock(pDestLock, &zDestLockName);
    *pzPhase = "vacuum into mark phase failed";
    return rc;
  }
  rc = gcMarkReachable(db, cs, &marked, &markTrace);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zPath);
    gcMarksFree(&marked);
    chunkStoreUnlock(cs);
    csFileUnlock(pDestLock, &zDestLockName);
    *pzPhase = "vacuum into mark phase failed";
    return rc;
  }

  if( rc==SQLITE_OK ){
    int bTargetNonEmpty = 0;
    rc = gcWriteCompactedTo(cs, &marked, pVfs, zPath, 0, &bTargetNonEmpty,
                            &pOutFile, &finalSize,
                            &aNewIndex, &nNewIndex, &nNewData);
    if( bTargetNonEmpty ){
      sqlite3_free(zPath);
      gcMarksFree(&marked);
      chunkStoreUnlock(cs);
      csFileUnlock(pDestLock, &zDestLockName);
      *pzPhase = "output file already exists";
      return SQLITE_ERROR;
    }
  }
  gcMarksFree(&marked);
  chunkStoreUnlock(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zPath);
    csFileUnlock(pDestLock, &zDestLockName);
    *pzPhase = "vacuum into write failed";
    return rc;
  }
  /* The VFS keeps the open name until xClose; free it after. Hold the
  ** destination reservation until the output is closed. */
  sqlite3OsCloseFree(pOutFile);
  csFileUnlock(pDestLock, &zDestLockName);
  sqlite3_free(zPath);
  sqlite3_free(aNewIndex);
  return SQLITE_OK;
}

int doltliteGcCompactDbWithPhase(sqlite3 *db, int iDb, const char **pzPhase){
  if( !db || iDb<0 || iDb>=db->nDb ) return SQLITE_OK;
  return doltliteGcCompactStoreWithPhase(
      db, doltliteBtreeChunkStore(db->aDb[iDb].pBt), pzPhase, 1);
}

int doltliteGcRegister(sqlite3 *db){
  return doltliteCreateCommandFunc(db, "dolt_gc", 0,
                                 doltliteGcFunc);
}

#endif
