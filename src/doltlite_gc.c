/*
** Stop-the-world garbage collection for Doltlite.
**
** SQL interface:
**   SELECT dolt_gc();
**   -- Returns "N chunks removed, M chunks kept"
**
** Algorithm: mark-and-sweep
**   Mark:  BFS from all roots (branches, tags, HEAD, staged, merge state)
**          following commit chains, catalogs, and prolly tree nodes.
**   Sweep: Rewrite the chunk store file with only reachable chunks.
**
** This is a stop-the-world operation: no other connections should be
** active during GC.  The entire file is rewritten atomically.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#ifdef _WIN32
# include <io.h>
# define GC_OPEN(p,f,m) _open(p, f|_O_BINARY, m)
# define GC_WRITE(fd,b,n) _write(fd, b, (unsigned)(n))
# define GC_FSYNC(fd) _commit(fd)
# define GC_CLOSE(fd) _close(fd)
#else
# include <unistd.h>
# define GC_OPEN(p,f,m) open(p, f, m)
# define GC_WRITE(fd,b,n) write(fd, b, n)
# define GC_FSYNC(fd) fsync(fd)
# define GC_CLOSE(fd) close(fd)
#endif

extern void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);

/* Provided by prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);

/* ----------------------------------------------------------------
** Hash set for the mark phase.
** Simple open-addressing hash table keyed by ProllyHash.
** ---------------------------------------------------------------- */

typedef struct GcHashSet GcHashSet;
struct GcHashSet {
  ProllyHash *aSlots;   /* Hash slot array */
  u8 *aUsed;            /* 1 if slot is occupied */
  int nSlots;           /* Total slots (power of 2) */
  int nUsed;            /* Number of entries */
};

static int gcHashSetInit(GcHashSet *hs, int nCapacity){
  int n = 256;
  while( n < nCapacity*2 ) n *= 2;
  hs->aSlots = sqlite3_malloc(n * sizeof(ProllyHash));
  hs->aUsed = sqlite3_malloc(n);
  if( !hs->aSlots || !hs->aUsed ){
    sqlite3_free(hs->aSlots);
    sqlite3_free(hs->aUsed);
    return SQLITE_NOMEM;
  }
  memset(hs->aUsed, 0, n);
  hs->nSlots = n;
  hs->nUsed = 0;
  return SQLITE_OK;
}

static void gcHashSetFree(GcHashSet *hs){
  sqlite3_free(hs->aSlots);
  sqlite3_free(hs->aUsed);
  memset(hs, 0, sizeof(*hs));
}

static u32 gcSlotIndex(const ProllyHash *h, int nSlots){
  u32 v = (u32)h->data[0] | ((u32)h->data[1]<<8) |
          ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24);
  return v & (nSlots - 1);
}

static int gcHashSetContains(GcHashSet *hs, const ProllyHash *h){
  u32 idx = gcSlotIndex(h, hs->nSlots);
  int i;
  for(i=0; i<hs->nSlots; i++){
    u32 slot = (idx + i) & (hs->nSlots - 1);
    if( !hs->aUsed[slot] ) return 0;
    if( memcmp(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE)==0 ) return 1;
  }
  return 0;
}

static int gcHashSetGrow(GcHashSet *hs);

static int gcHashSetAdd(GcHashSet *hs, const ProllyHash *h){
  u32 idx;
  int i;
  if( hs->nUsed >= hs->nSlots / 2 ){
    int rc = gcHashSetGrow(hs);
    if( rc!=SQLITE_OK ) return rc;
  }
  idx = gcSlotIndex(h, hs->nSlots);
  for(i=0; i<hs->nSlots; i++){
    u32 slot = (idx + i) & (hs->nSlots - 1);
    if( !hs->aUsed[slot] ){
      memcpy(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE);
      hs->aUsed[slot] = 1;
      hs->nUsed++;
      return SQLITE_OK;
    }
    if( memcmp(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE)==0 ){
      return SQLITE_OK; /* already present */
    }
  }
  return SQLITE_FULL;
}

static int gcHashSetGrow(GcHashSet *hs){
  GcHashSet newHs;
  int i, rc;
  int newSize = hs->nSlots * 2;
  newHs.aSlots = sqlite3_malloc(newSize * sizeof(ProllyHash));
  newHs.aUsed = sqlite3_malloc(newSize);
  if( !newHs.aSlots || !newHs.aUsed ){
    sqlite3_free(newHs.aSlots);
    sqlite3_free(newHs.aUsed);
    return SQLITE_NOMEM;
  }
  memset(newHs.aUsed, 0, newSize);
  newHs.nSlots = newSize;
  newHs.nUsed = 0;
  for(i=0; i<hs->nSlots; i++){
    if( hs->aUsed[i] ){
      rc = gcHashSetAdd(&newHs, &hs->aSlots[i]);
      if( rc!=SQLITE_OK ){
        sqlite3_free(newHs.aSlots);
        sqlite3_free(newHs.aUsed);
        return rc;
      }
    }
  }
  sqlite3_free(hs->aSlots);
  sqlite3_free(hs->aUsed);
  *hs = newHs;
  return SQLITE_OK;
}

/* ----------------------------------------------------------------
** BFS queue for mark traversal.
** ---------------------------------------------------------------- */

typedef struct GcQueue GcQueue;
struct GcQueue {
  ProllyHash *aItems;
  int nItems;
  int nAlloc;
  int iHead;   /* Next item to dequeue */
};

static int gcQueueInit(GcQueue *q){
  q->nAlloc = 256;
  q->aItems = sqlite3_malloc(q->nAlloc * sizeof(ProllyHash));
  if( !q->aItems ) return SQLITE_NOMEM;
  q->nItems = 0;
  q->iHead = 0;
  return SQLITE_OK;
}

static void gcQueueFree(GcQueue *q){
  sqlite3_free(q->aItems);
  memset(q, 0, sizeof(*q));
}

static int gcQueuePush(GcQueue *q, const ProllyHash *h){
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  if( q->nItems >= q->nAlloc ){
    int newAlloc = q->nAlloc * 2;
    ProllyHash *aNew = sqlite3_realloc(q->aItems, newAlloc * sizeof(ProllyHash));
    if( !aNew ) return SQLITE_NOMEM;
    q->aItems = aNew;
    q->nAlloc = newAlloc;
  }
  memcpy(&q->aItems[q->nItems], h, sizeof(ProllyHash));
  q->nItems++;
  return SQLITE_OK;
}

static int gcQueuePop(GcQueue *q, ProllyHash *h){
  if( q->iHead >= q->nItems ) return 0;
  memcpy(h, &q->aItems[q->iHead], sizeof(ProllyHash));
  q->iHead++;
  return 1;
}

/* ----------------------------------------------------------------
** Chunk type detection helpers.
** ---------------------------------------------------------------- */

#define PROLLY_NODE_MAGIC_VAL 0x504E4F44

static int isCommitChunk(const u8 *data, int nData){
  /* Commits start with version byte (V2=2).
  ** V2: version(1) + nParents(1) + parents(20*N) + catalog(20) + ... min ~30 bytes */
  if( nData < 30 ) return 0;
  if( data[0] != DOLTLITE_COMMIT_V2 ) return 0;
  /* Verify it's NOT a prolly node (magic would be at offset 0) */
  if( nData >= 4 ){
    u32 m = (u32)data[0] | ((u32)data[1]<<8) |
            ((u32)data[2]<<16) | ((u32)data[3]<<24);
    if( m == PROLLY_NODE_MAGIC_VAL ) return 0;
  }
  return 1;
}

static int isProllyNodeChunk(const u8 *data, int nData){
  u32 m;
  if( nData < 8 ) return 0;
  m = (u32)data[0] | ((u32)data[1]<<8) |
      ((u32)data[2]<<16) | ((u32)data[3]<<24);
  return m == PROLLY_NODE_MAGIC_VAL;
}

/* ----------------------------------------------------------------
** Mark phase: BFS from all roots.
** ---------------------------------------------------------------- */

static int gcMarkReachable(
  ChunkStore *cs,
  GcHashSet *marked
){
  GcQueue queue;
  ProllyHash current;
  int rc, i;

  rc = gcQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;

  /* Seed: manifest roots */
  rc = gcQueuePush(&queue, &cs->root);
  if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &cs->catalog);
  if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &cs->headCommit);
  if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &cs->refsHash);

  /* All branch tips + per-branch WorkingSet chunks */
  for(i=0; rc==SQLITE_OK && i<cs->nBranches; i++){
    rc = gcQueuePush(&queue, &cs->aBranches[i].commitHash);
    if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &cs->aBranches[i].workingSetHash);
  }

  /* All tag targets */
  for(i=0; rc==SQLITE_OK && i<cs->nTags; i++){
    rc = gcQueuePush(&queue, &cs->aTags[i].commitHash);
  }
  if( rc!=SQLITE_OK ){
    gcQueueFree(&queue);
    return rc;
  }

  /* BFS */
  while( gcQueuePop(&queue, &current) ){
    u8 *data = 0;
    int nData = 0;

    if( prollyHashIsEmpty(&current) ) continue;
    if( gcHashSetContains(marked, &current) ) continue;

    rc = gcHashSetAdd(marked, &current);
    if( rc!=SQLITE_OK ) break;

    rc = chunkStoreGet(cs, &current, &data, &nData);
    if( rc!=SQLITE_OK ){
      /* Chunk missing — skip, don't fail GC */
      continue;
    }

    if( isProllyNodeChunk(data, nData) ){
      /* Prolly tree node — follow child hashes if internal */
      ProllyNode node;
      int parseRc = prollyNodeParse(&node, data, nData);
      if( parseRc==SQLITE_OK && node.level > 0 ){
        for(i=0; i<(int)node.nItems; i++){
          ProllyHash childHash;
          prollyNodeChildHash(&node, i, &childHash);
          rc = gcQueuePush(&queue, &childHash);
          if( rc!=SQLITE_OK ) break;
        }
      }
    }else if( isCommitChunk(data, nData) ){
      /* Commit — follow ALL parents + catalog (+ rootHash for V1) */
      DoltliteCommit commit;
      memset(&commit, 0, sizeof(commit));
      int drc = doltliteCommitDeserialize(data, nData, &commit);
      if( drc==SQLITE_OK ){
        int pi;
        for(pi=0; pi<commit.nParents; pi++){
          rc = gcQueuePush(&queue, &commit.aParents[pi]);
          if( rc!=SQLITE_OK ) break;
        }
        if( rc==SQLITE_OK && !prollyHashIsEmpty(&commit.rootHash) ){
          rc = gcQueuePush(&queue, &commit.rootHash);  /* V1 compat */
        }
        if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &commit.catalogHash);
        doltliteCommitClear(&commit);
      }
    }else{
      /* Could be a catalog, refs, or conflict chunk.
      ** Catalogs contain table root hashes we need to follow.
      ** Format: version(1=0x02)+iNextTable(4)+nTables(4)+entries */
      /* WorkingSet chunk: version(1=0x01) + staged(20) + merging(1) +
      ** mergeCommit(20) + conflicts(20) = 62 bytes.
      ** Follow staged catalog and conflicts catalog hashes. */
      if( nData == WS_TOTAL_SIZE && data[0] == 1 ){
        ProllyHash stagedCat, conflictsCat;
        memcpy(stagedCat.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
        memcpy(conflictsCat.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
        rc = gcQueuePush(&queue, &stagedCat);
        if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &conflictsCat);
        if( rc==SQLITE_OK && data[WS_MERGING_OFF] ){  /* isMerging */
          ProllyHash mergeCommit;
          memcpy(mergeCommit.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
          rc = gcQueuePush(&queue, &mergeCommit);
        }
      }
      /* Catalog chunk: version(1='C') */
      if( nData >= 9 && data[0] == 0x43 ){
        int nTables = (int)(data[5] | (data[6]<<8) |
                            (data[7]<<16) | (data[8]<<24));
        if( nTables >= 0 && nTables < 10000 ){
          const u8 *p = data + 9;
          for(i=0; i<nTables; i++){
            if( p + 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2 > data + nData ) break;
            {
              ProllyHash tableRoot;
              memcpy(tableRoot.data, p + 5, PROLLY_HASH_SIZE);
              rc = gcQueuePush(&queue, &tableRoot);
              if( rc!=SQLITE_OK ) break;
            }
            {
              int nameLen;
              p += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE;
              nameLen = p[0] | (p[1]<<8);
              p += 2 + nameLen;
            }
          }
        }
      }
    }

    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;
  }

  gcQueueFree(&queue);
  return rc;
}

/* ----------------------------------------------------------------
** Sweep phase: rewrite chunk store with only marked chunks.
**
** Strategy:
**   1. Build list of surviving ChunkIndexEntry from old index
**   2. Write new file: manifest + chunk data + new index
**   3. Atomically replace old file (temp + rename)
**   4. Reload the chunk store index
** ---------------------------------------------------------------- */

/*
** Iterate the existing chunk index, copy surviving (marked) chunks into
** a new contiguous data buffer, and build a new sorted index.
** Caller must sqlite3_free *ppNewData and *ppNewIndex when done.
*/
static int gcBuildCompactedData(
  ChunkStore *cs,
  GcHashSet *marked,
  u8 **ppNewData,
  int *pnNewData,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex
){
  int i, j;
  int kept = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  u8 *buf = 0;
  int nBuf = 0, nBufAlloc = 0;
  i64 dataOffset = CHUNK_MANIFEST_SIZE;
  int rc = SQLITE_OK;

  /* Count survivors to pre-allocate */
  for(i=0; i<cs->nIndex; i++){
    if( gcHashSetContains(marked, &cs->aIndex[i].hash) ) kept++;
  }

  aNewIndex = sqlite3_malloc(kept * (int)sizeof(ChunkIndexEntry));
  if( !aNewIndex ) return SQLITE_NOMEM;

  for(i=0; i<cs->nIndex; i++){
    u8 *chunkData = 0;
    int nChunkData = 0;

    if( !gcHashSetContains(marked, &cs->aIndex[i].hash) ) continue;

    rc = chunkStoreGet(cs, &cs->aIndex[i].hash, &chunkData, &nChunkData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aNewIndex);
      sqlite3_free(buf);
      return rc;
    }

    /* Grow buffer: 4 (length prefix) + nChunkData */
    {
      int need = nBuf + 4 + nChunkData;
      if( need > nBufAlloc ){
        int newAlloc = nBufAlloc ? nBufAlloc * 2 : 65536;
        while( newAlloc < need ) newAlloc *= 2;
        buf = sqlite3_realloc(buf, newAlloc);
        if( !buf ){
          sqlite3_free(chunkData);
          sqlite3_free(aNewIndex);
          return SQLITE_NOMEM;
        }
        nBufAlloc = newAlloc;
      }
    }

    /* Write length prefix + data */
    buf[nBuf]   = (u8)(nChunkData);
    buf[nBuf+1] = (u8)(nChunkData>>8);
    buf[nBuf+2] = (u8)(nChunkData>>16);
    buf[nBuf+3] = (u8)(nChunkData>>24);

    memcpy(&aNewIndex[nNewIndex].hash, &cs->aIndex[i].hash, sizeof(ProllyHash));
    aNewIndex[nNewIndex].offset = dataOffset + nBuf;
    aNewIndex[nNewIndex].size = nChunkData;
    nNewIndex++;

    memcpy(buf + nBuf + 4, chunkData, nChunkData);
    nBuf += 4 + nChunkData;

    sqlite3_free(chunkData);
  }

  /* Sort new index by hash for binary search */
  for(i=1; i<nNewIndex; i++){
    ChunkIndexEntry tmp = aNewIndex[i];
    j = i-1;
    while( j>=0 && memcmp(aNewIndex[j].hash.data, tmp.hash.data, PROLLY_HASH_SIZE)>0 ){
      aNewIndex[j+1] = aNewIndex[j];
      j--;
    }
    aNewIndex[j+1] = tmp;
  }

  *ppNewData = buf;
  *pnNewData = nBuf;
  *ppNewIndex = aNewIndex;
  *pnNewIndex = nNewIndex;
  return SQLITE_OK;
}

/*
** Write the compacted chunk store file: manifest + chunk data + index.
** Fsyncs, renames the temp file into place, and reopens the file handle.
*/
static int gcRewriteFile(
  ChunkStore *cs,
  const u8 *pNewData,
  int nNewData,
  const ChunkIndexEntry *pNewIndex,
  int nNewIndex
){
  int i;
  int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
  i64 indexOffset = CHUNK_MANIFEST_SIZE + nNewData;
  u8 *indexBuf = 0;
  u8 manifest[CHUNK_MANIFEST_SIZE];
  int rc = SQLITE_OK;

  /* Build serialized index */
  indexBuf = sqlite3_malloc(indexSize);
  if( !indexBuf ) return SQLITE_NOMEM;
  for(i=0; i<nNewIndex; i++){
    u8 *p = indexBuf + i * CHUNK_INDEX_ENTRY_SIZE;
    memcpy(p, pNewIndex[i].hash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    {
      i64 off = pNewIndex[i].offset;
      p[0] = (u8)off; p[1] = (u8)(off>>8);
      p[2] = (u8)(off>>16); p[3] = (u8)(off>>24);
      p[4] = (u8)(off>>32); p[5] = (u8)(off>>40);
      p[6] = (u8)(off>>48); p[7] = (u8)(off>>56);
    }
    p += 8;
    {
      u32 sz = (u32)pNewIndex[i].size;
      p[0] = (u8)sz; p[1] = (u8)(sz>>8);
      p[2] = (u8)(sz>>16); p[3] = (u8)(sz>>24);
    }
  }

  /* Update ChunkStore fields so the manifest reflects GC state */
  cs->nChunks = nNewIndex;
  cs->iIndexOffset = indexOffset;
  cs->nIndexSize = indexSize;
  cs->iWalOffset = indexOffset + indexSize;

  csSerializeManifest(cs, manifest);

  /* Write to temp file, then rename.
  ** Uses direct POSIX I/O instead of the SQLite VFS because the VFS
  ** xWrite has a page-size limit that fails on large bulk writes. */
  if( cs->zFilename && strcmp(cs->zFilename, ":memory:")!=0 ){
    char *zTmp = sqlite3_mprintf("%s-gc-tmp", cs->zFilename);
    if( !zTmp ){
      sqlite3_free(indexBuf);
      return SQLITE_NOMEM;
    }

    {
      int fd = GC_OPEN(zTmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if( fd < 0 ){
        sqlite3_free(zTmp); sqlite3_free(indexBuf);
        return SQLITE_CANTOPEN;
      }

      if( GC_WRITE(fd, manifest, CHUNK_MANIFEST_SIZE) != CHUNK_MANIFEST_SIZE ){
        rc = SQLITE_IOERR_WRITE;
      }
      if( rc==SQLITE_OK && nNewData>0 ){
        /* Write chunk data in chunks to handle large buffers */
        const u8 *p = pNewData;
        int remaining = nNewData;
        while( remaining > 0 && rc==SQLITE_OK ){
          int toWrite = remaining > 1048576 ? 1048576 : remaining;
          int written = GC_WRITE(fd, p, toWrite);
          if( written != toWrite ){ rc = SQLITE_IOERR_WRITE; break; }
          p += toWrite;
          remaining -= toWrite;
        }
      }
      if( rc==SQLITE_OK && indexSize>0 ){
        if( GC_WRITE(fd, indexBuf, indexSize) != indexSize ){
          rc = SQLITE_IOERR_WRITE;
        }
      }
      if( rc==SQLITE_OK ){
        if( GC_FSYNC(fd)!=0 ) rc = SQLITE_IOERR_FSYNC;
      }
      GC_CLOSE(fd);

      if( rc==SQLITE_OK ){
        /* Close the old file handle before rename */
        if( cs->pFile && cs->pFile->pMethods ){
          cs->pFile->pMethods->xClose(cs->pFile);
          cs->pFile = 0;
        }

        if( rename(zTmp, cs->zFilename)!=0 ){
          rc = SQLITE_IOERR;
        }

        if( rc==SQLITE_OK ){
          sqlite3_free(cs->pWalData);
          cs->pWalData = 0;
          cs->nWalData = 0;
        }

        /* Reopen via VFS for subsequent operations */
        if( rc==SQLITE_OK ){
          int reopenFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
          int dummy;
          sqlite3_file *pNewFile = sqlite3_malloc(cs->pVfs->szOsFile);
          if( pNewFile ){
            rc = cs->pVfs->xOpen(cs->pVfs, cs->zFilename, pNewFile,
                                 reopenFlags, &dummy);
            if( rc==SQLITE_OK ){
              cs->pFile = pNewFile;
            } else {
              sqlite3_free(pNewFile);
            }
          } else {
            rc = SQLITE_NOMEM;
          }
        }
      }else{
        unlink(zTmp);
      }
    }
    sqlite3_free(zTmp);
  }

  sqlite3_free(indexBuf);
  return rc;
}

static int gcSweep(
  ChunkStore *cs,
  GcHashSet *marked,
  int *pKept,
  int *pRemoved
){
  int i, kept = 0, removed = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  u8 *buf = 0;
  int nBuf = 0;
  int rc = SQLITE_OK;

  /* Count survivors and removals */
  for(i=0; i<cs->nIndex; i++){
    if( gcHashSetContains(marked, &cs->aIndex[i].hash) ){
      kept++;
    }else{
      removed++;
    }
  }
  for(i=0; i<cs->nPending; i++){
    if( gcHashSetContains(marked, &cs->aPending[i].hash) ){
      kept++;
    }
  }

  if( removed==0 ){
    *pKept = kept;
    *pRemoved = 0;
    return SQLITE_OK;
  }

  /* Build compacted data and index */
  rc = gcBuildCompactedData(cs, marked, &buf, &nBuf, &aNewIndex, &nNewIndex);
  if( rc!=SQLITE_OK ) return rc;

  /* Rewrite the file */
  rc = gcRewriteFile(cs, buf, nBuf, aNewIndex, nNewIndex);

  /* Update in-memory state */
  if( rc==SQLITE_OK ){
    int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
    sqlite3_free(cs->aIndex);
    cs->aIndex = aNewIndex;
    cs->nIndex = nNewIndex;
    cs->nIndexAlloc = nNewIndex;
    cs->nChunks = nNewIndex;
    cs->iIndexOffset = CHUNK_MANIFEST_SIZE + nBuf;
    cs->nIndexSize = indexSize;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE + nBuf + indexSize;
    aNewIndex = 0;  /* ownership transferred */

    cs->nPending = 0;
    cs->nWriteBuf = 0;
  }

  sqlite3_free(aNewIndex);
  sqlite3_free(buf);

  *pKept = kept;
  *pRemoved = removed;
  return rc;
}

/* ----------------------------------------------------------------
** dolt_gc() SQL function.
** ---------------------------------------------------------------- */

static void doltliteGcFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  GcHashSet marked;
  int nKept = 0, nRemoved = 0;
  int rc;
  char result[128];

  (void)argc;
  (void)argv;

  if( !cs ){
    sqlite3_result_error(context, "no database", -1);
    return;
  }

  /* In-memory databases don't need GC (no file to compact) */
  if( !cs->zFilename || strcmp(cs->zFilename, ":memory:")==0 ){
    sqlite3_result_text(context, "0 chunks removed, 0 chunks kept (in-memory)", -1, SQLITE_TRANSIENT);
    return;
  }

  rc = gcHashSetInit(&marked, cs->nIndex > 64 ? cs->nIndex : 64);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "out of memory", -1);
    return;
  }

  /* Mark phase */
  rc = gcMarkReachable(cs, &marked);
  if( rc!=SQLITE_OK ){
    gcHashSetFree(&marked);
    sqlite3_result_error(context, "gc mark phase failed", -1);
    return;
  }

  /* Sweep phase */
  rc = gcSweep(cs, &marked, &nKept, &nRemoved);
  gcHashSetFree(&marked);

  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "gc sweep phase failed", -1);
    return;
  }

  sqlite3_snprintf(sizeof(result), result,
    "%d chunks removed, %d chunks kept", nRemoved, nKept);
  sqlite3_result_text(context, result, -1, SQLITE_TRANSIENT);
}

/* ----------------------------------------------------------------
** Registration.
** ---------------------------------------------------------------- */

int doltliteGcRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_gc", 0, SQLITE_UTF8, 0,
                                  doltliteGcFunc, 0, 0);
}

#endif /* DOLTLITE_PROLLY */
