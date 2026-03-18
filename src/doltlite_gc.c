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
  /* Commits start with version byte = DOLTLITE_COMMIT_VERSION (1),
  ** followed by 20-byte parent hash. Minimum size ~63 bytes. */
  if( nData < 63 ) return 0;
  if( data[0] != DOLTLITE_COMMIT_VERSION ) return 0;
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

  /* Seed: all manifest roots */
  gcQueuePush(&queue, &cs->root);
  gcQueuePush(&queue, &cs->catalog);
  gcQueuePush(&queue, &cs->headCommit);
  gcQueuePush(&queue, &cs->stagedCatalog);
  gcQueuePush(&queue, &cs->refsHash);

  /* Merge state */
  if( cs->isMerging ){
    gcQueuePush(&queue, &cs->mergeCommitHash);
    gcQueuePush(&queue, &cs->conflictsCatalogHash);
  }

  /* All branch tips */
  for(i=0; i<cs->nBranches; i++){
    gcQueuePush(&queue, &cs->aBranches[i].commitHash);
  }

  /* All tag targets */
  for(i=0; i<cs->nTags; i++){
    gcQueuePush(&queue, &cs->aTags[i].commitHash);
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
          gcQueuePush(&queue, &childHash);
        }
      }
    }else if( isCommitChunk(data, nData) ){
      /* Commit — follow parent, root, catalog */
      DoltliteCommit commit;
      memset(&commit, 0, sizeof(commit));
      int drc = doltliteCommitDeserialize(data, nData, &commit);
      if( drc==SQLITE_OK ){
        gcQueuePush(&queue, &commit.parentHash);
        gcQueuePush(&queue, &commit.rootHash);
        gcQueuePush(&queue, &commit.catalogHash);
        doltliteCommitClear(&commit);
      }
    }else{
      /* Could be a catalog, refs, or conflict chunk.
      ** Catalogs contain table root hashes we need to follow.
      ** Try parsing as catalog: header is iNextTable(4)+nTables(4)+meta(64). */
      if( nData >= 72 ){
        int nTables = (int)(data[4] | (data[5]<<8) |
                            (data[6]<<16) | (data[7]<<24));
        /* Sanity check: nTables should be reasonable */
        if( nTables >= 0 && nTables < 10000 ){
          const u8 *p = data + 72;
          for(i=0; i<nTables; i++){
            /* Each entry: iTable(4) + flags(1) + root(20) + schemaHash(20) + name_len(2) + name(var) */
            if( p + 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2 > data + nData ) break;
            {
              ProllyHash tableRoot;
              memcpy(tableRoot.data, p + 5, PROLLY_HASH_SIZE);
              gcQueuePush(&queue, &tableRoot);
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
  int nBuf = 0, nBufAlloc = 0;
  i64 dataOffset;
  int rc = SQLITE_OK;

  /* Count survivors */
  for(i=0; i<cs->nIndex; i++){
    if( gcHashSetContains(marked, &cs->aIndex[i].hash) ){
      kept++;
    }else{
      removed++;
    }
  }

  /* Also check pending chunks */
  for(i=0; i<cs->nPending; i++){
    if( gcHashSetContains(marked, &cs->aPending[i].hash) ){
      kept++;
    }
  }

  if( removed==0 ){
    *pKept = kept;
    *pRemoved = 0;
    return SQLITE_OK;  /* Nothing to do */
  }

  /* Build new index and data buffer with surviving chunks */
  aNewIndex = sqlite3_malloc(kept * (int)sizeof(ChunkIndexEntry));
  if( !aNewIndex ) return SQLITE_NOMEM;
  nNewIndex = 0;

  /* Data starts after manifest */
  dataOffset = CHUNK_MANIFEST_SIZE;

  for(i=0; i<cs->nIndex; i++){
    if( !gcHashSetContains(marked, &cs->aIndex[i].hash) ) continue;

    /* Read chunk data from old file */
    {
      u8 *chunkData = 0;
      int nChunkData = 0;
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

      /* Record new index entry */
      memcpy(&aNewIndex[nNewIndex].hash, &cs->aIndex[i].hash, sizeof(ProllyHash));
      aNewIndex[nNewIndex].offset = dataOffset + nBuf;
      aNewIndex[nNewIndex].size = nChunkData;
      nNewIndex++;

      memcpy(buf + nBuf + 4, chunkData, nChunkData);
      nBuf += 4 + nChunkData;

      sqlite3_free(chunkData);
    }
  }

  /* Sort new index by hash for binary search */
  {
    int j;
    for(i=1; i<nNewIndex; i++){
      ChunkIndexEntry tmp = aNewIndex[i];
      j = i-1;
      while( j>=0 && memcmp(aNewIndex[j].hash.data, tmp.hash.data, PROLLY_HASH_SIZE)>0 ){
        aNewIndex[j+1] = aNewIndex[j];
        j--;
      }
      aNewIndex[j+1] = tmp;
    }
  }

  /* Build serialized index */
  {
    int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
    i64 indexOffset = CHUNK_MANIFEST_SIZE + nBuf;
    u8 *indexBuf = sqlite3_malloc(indexSize);
    if( !indexBuf ){
      sqlite3_free(aNewIndex);
      sqlite3_free(buf);
      return SQLITE_NOMEM;
    }
    for(i=0; i<nNewIndex; i++){
      u8 *p = indexBuf + i * CHUNK_INDEX_ENTRY_SIZE;
      memcpy(p, aNewIndex[i].hash.data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      /* offset: i64 little-endian */
      {
        i64 off = aNewIndex[i].offset;
        p[0] = (u8)off; p[1] = (u8)(off>>8);
        p[2] = (u8)(off>>16); p[3] = (u8)(off>>24);
        p[4] = (u8)(off>>32); p[5] = (u8)(off>>40);
        p[6] = (u8)(off>>48); p[7] = (u8)(off>>56);
      }
      p += 8;
      /* size: u32 little-endian */
      {
        u32 sz = (u32)aNewIndex[i].size;
        p[0] = (u8)sz; p[1] = (u8)(sz>>8);
        p[2] = (u8)(sz>>16); p[3] = (u8)(sz>>24);
      }
    }

    /* Build new manifest */
    {
      u8 manifest[CHUNK_MANIFEST_SIZE];
      u8 *m = manifest;
      memset(manifest, 0, CHUNK_MANIFEST_SIZE);
      /* magic */
      m[0]=(u8)CHUNK_STORE_MAGIC; m[1]=(u8)(CHUNK_STORE_MAGIC>>8);
      m[2]=(u8)(CHUNK_STORE_MAGIC>>16); m[3]=(u8)(CHUNK_STORE_MAGIC>>24);
      m+=4;
      /* version */
      m[0]=(u8)CHUNK_STORE_VERSION; m[1]=(u8)(CHUNK_STORE_VERSION>>8);
      m[2]=(u8)(CHUNK_STORE_VERSION>>16); m[3]=(u8)(CHUNK_STORE_VERSION>>24);
      m+=4;
      /* root_hash */
      memcpy(m, cs->root.data, PROLLY_HASH_SIZE); m+=PROLLY_HASH_SIZE;
      /* chunk_count */
      { u32 cc = (u32)nNewIndex;
        m[0]=(u8)cc; m[1]=(u8)(cc>>8); m[2]=(u8)(cc>>16); m[3]=(u8)(cc>>24); }
      m+=4;
      /* index_offset (i64) */
      { i64 io = indexOffset;
        m[0]=(u8)io; m[1]=(u8)(io>>8); m[2]=(u8)(io>>16); m[3]=(u8)(io>>24);
        m[4]=(u8)(io>>32); m[5]=(u8)(io>>40); m[6]=(u8)(io>>48); m[7]=(u8)(io>>56); }
      m+=8;
      /* index_size */
      { u32 is = (u32)indexSize;
        m[0]=(u8)is; m[1]=(u8)(is>>8); m[2]=(u8)(is>>16); m[3]=(u8)(is>>24); }
      m+=4;
      /* catalog_hash */
      memcpy(m, cs->catalog.data, PROLLY_HASH_SIZE); m+=PROLLY_HASH_SIZE;
      /* head_commit */
      memcpy(m, cs->headCommit.data, PROLLY_HASH_SIZE); m+=PROLLY_HASH_SIZE;
      /* staged_catalog */
      memcpy(m, cs->stagedCatalog.data, PROLLY_HASH_SIZE); m+=PROLLY_HASH_SIZE;
      /* refs_hash */
      memcpy(m, cs->refsHash.data, PROLLY_HASH_SIZE); m+=PROLLY_HASH_SIZE;
      /* is_merging */
      { u32 im = (u32)cs->isMerging;
        m[0]=(u8)im; m[1]=(u8)(im>>8); m[2]=(u8)(im>>16); m[3]=(u8)(im>>24); }
      m+=4;
      /* merge_commit_hash */
      memcpy(m, cs->mergeCommitHash.data, PROLLY_HASH_SIZE); m+=PROLLY_HASH_SIZE;
      /* conflicts_catalog_hash */
      memcpy(m, cs->conflictsCatalogHash.data, PROLLY_HASH_SIZE);

      /* Write to temp file, then rename */
      if( cs->zFilename && strcmp(cs->zFilename, ":memory:")!=0 ){
        char *zTmp = sqlite3_mprintf("%s-gc-tmp", cs->zFilename);
        if( !zTmp ){
          sqlite3_free(indexBuf); sqlite3_free(aNewIndex); sqlite3_free(buf);
          return SQLITE_NOMEM;
        }

        {
          sqlite3_file *pTmp = 0;
          int tmpFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                         SQLITE_OPEN_MAIN_DB;
          int dummy;

          pTmp = sqlite3_malloc(cs->pVfs->szOsFile);
          if( !pTmp ){
            sqlite3_free(zTmp); sqlite3_free(indexBuf);
            sqlite3_free(aNewIndex); sqlite3_free(buf);
            return SQLITE_NOMEM;
          }

          rc = cs->pVfs->xOpen(cs->pVfs, zTmp, pTmp, tmpFlags, &dummy);
          if( rc==SQLITE_OK ){
            /* Write manifest */
            rc = pTmp->pMethods->xWrite(pTmp, manifest, CHUNK_MANIFEST_SIZE, 0);
            /* Write chunk data */
            if( rc==SQLITE_OK && nBuf>0 ){
              rc = pTmp->pMethods->xWrite(pTmp, buf, nBuf, CHUNK_MANIFEST_SIZE);
            }
            /* Write index */
            if( rc==SQLITE_OK && indexSize>0 ){
              rc = pTmp->pMethods->xWrite(pTmp, indexBuf, indexSize, indexOffset);
            }
            /* Truncate to exact size */
            if( rc==SQLITE_OK ){
              rc = pTmp->pMethods->xTruncate(pTmp, indexOffset + indexSize);
            }
            /* Sync */
            if( rc==SQLITE_OK ){
              rc = pTmp->pMethods->xSync(pTmp, SQLITE_SYNC_NORMAL);
            }
            pTmp->pMethods->xClose(pTmp);
          }
          sqlite3_free(pTmp);

          if( rc==SQLITE_OK ){
            /* Close our current file handle */
            if( cs->pFile && cs->pFile->pMethods ){
              cs->pFile->pMethods->xClose(cs->pFile);
            }

            /* Rename temp over original */
            rc = cs->pVfs->xDelete(cs->pVfs, cs->zFilename, 0);
            if( rc==SQLITE_OK ){
              /* Use system rename */
              if( rename(zTmp, cs->zFilename)!=0 ){
                rc = SQLITE_IOERR;
              }
            }

            /* Reopen the file */
            if( rc==SQLITE_OK ){
              int reopenFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
              rc = cs->pVfs->xOpen(cs->pVfs, cs->zFilename, cs->pFile,
                                   reopenFlags, &dummy);
            }
          }else{
            /* Clean up temp on failure */
            cs->pVfs->xDelete(cs->pVfs, zTmp, 0);
          }
        }
        sqlite3_free(zTmp);
      }else{
        /* In-memory: just replace the index */
        rc = SQLITE_OK;
      }

      /* Update in-memory index */
      if( rc==SQLITE_OK ){
        sqlite3_free(cs->aIndex);
        cs->aIndex = aNewIndex;
        cs->nIndex = nNewIndex;
        cs->nIndexAlloc = nNewIndex;
        cs->nChunks = nNewIndex;
        cs->iIndexOffset = CHUNK_MANIFEST_SIZE + nBuf;
        cs->nIndexSize = indexSize;
        cs->iAppendOffset = CHUNK_MANIFEST_SIZE + nBuf;
        aNewIndex = 0;  /* ownership transferred */

        /* Clear pending buffer */
        cs->nPending = 0;
        cs->nWriteBuf = 0;
      }
    }

    sqlite3_free(indexBuf);
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
