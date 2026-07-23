
#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "chunk_index.h"
#include "prolly_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

void chunkIndexGetEntries(const ChunkIndex *idx, int *pn, const ChunkIndexEntry **par){
  assert( idx!=0 && pn!=0 && par!=0 );
  assert( idx->nIndex>=0 );
  assert( idx->nIndex==0 || idx->aIndex!=0 );
  *pn = idx->nIndex;
  *par = idx->aIndex;
}

int chunkIndexCount(const ChunkIndex *idx){
  assert( idx!=0 );
  assert( idx->nIndex>=0 );
  return idx->nIndex;
}

void chunkIndexSetMetadata(ChunkIndex *idx, int nChunks, i64 iOffset, i64 nSize){
  assert( idx!=0 );
  assert( nChunks>=0 );
  assert( iOffset>=0 && nSize>=0 );
  idx->nChunks = nChunks;
  idx->iIndexOffset = iOffset;
  idx->nIndexSize = nSize;
}

void chunkIndexReplaceEntries(ChunkIndex *idx, ChunkIndexEntry *aNew, int nNew){
  assert( idx!=0 );
  assert( nNew>=0 );
  assert( nNew==0 || aNew!=0 );
  csReleaseIndexBuf(idx->aIndex, idx->aIndexMmapBase, idx->aIndexMmapSize);
  idx->aIndex = aNew;
  idx->nIndex = nNew;
  idx->aIndexMmapBase = 0;
  idx->aIndexMmapSize = 0;
}

void csReleaseIndexBuf(ChunkIndexEntry *aIndex,
                       void *mmapBase, i64 mmapSize){
  (void)mmapBase;
  (void)mmapSize;
  sqlite3_free(aIndex);
}

int csSearchIndex(
  const ChunkIndexEntry *aIdx,
  int nIdx,
  const ProllyHash *pHash
){
  int lo = 0;
  int hi = nIdx - 1;
  assert( pHash!=0 );
  assert( nIdx>=0 );
  assert( nIdx==0 || aIdx!=0 );
  while( lo <= hi ){
    int mid = lo + (hi - lo) / 2;
    int cmp = prollyHashCompare(&aIdx[mid].hash, pHash);
    if( cmp == 0 ) return mid;
    if( cmp < 0 ){
      lo = mid + 1;
    }else{
      hi = mid - 1;
    }
  }
  return -1;
}

static int csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e){
  u32 sz = CS_READ_U32(aBuf + PROLLY_HASH_SIZE + 8);
  if( sz > (u32)0x7fffffff ) return SQLITE_CORRUPT;
  memcpy(e->hash.data, aBuf, PROLLY_HASH_SIZE);
  e->offset = CS_READ_I64(aBuf + PROLLY_HASH_SIZE);
  e->size = (int)sz;
  return SQLITE_OK;
}

static int csValidateIndexEntries(
  const ChunkIndexEntry *aIndex,
  int nIndex,
  i64 iIndexOffset
){
  int i;
  for( i=0; i<nIndex; i++ ){
    const ChunkIndexEntry *e = &aIndex[i];
    if( e->offset < CHUNK_MANIFEST_SIZE ) return SQLITE_CORRUPT;
    if( e->size < 0 ) return SQLITE_CORRUPT;
    if( e->offset > iIndexOffset - 4 ) return SQLITE_CORRUPT;
    if( (i64)e->size > iIndexOffset - e->offset - 4 ){
      return SQLITE_CORRUPT;
    }
    if( i>0 && prollyHashCompare(&aIndex[i-1].hash, &e->hash) >= 0 ){
      return SQLITE_CORRUPT;
    }
  }
  return SQLITE_OK;
}

int csReadIndex(ChunkStore *cs){
  int rc;
  i64 nEntries64;
  int nEntries;
  u8 *aBuf;
  int i;
  i64 fileSize = 0;

  if( cs->index.nIndexSize == 0 ){
    cs->index.nIndex = 0;
    return SQLITE_OK;
  }
  if( cs->index.nChunks == 0 ){
    return SQLITE_CORRUPT;
  }

  nEntries64 = cs->index.nIndexSize / CHUNK_INDEX_ENTRY_SIZE;
  if( nEntries64 * CHUNK_INDEX_ENTRY_SIZE != cs->index.nIndexSize ){
    return SQLITE_CORRUPT;
  }
  if( nEntries64 > INT_MAX ){
    return SQLITE_TOOBIG;
  }
  if( cs->index.nIndexSize > INT_MAX ){
    return SQLITE_TOOBIG;
  }
  nEntries = (int)nEntries64;
  if( nEntries > INT_MAX/(int)sizeof(ChunkIndexEntry) ){
    return SQLITE_TOOBIG;
  }
  /* nChunks includes WAL chunks; only compacted entries are in this index. */
  if( nEntries > cs->index.nChunks ){
    return SQLITE_CORRUPT;
  }

  if( cs->index.iIndexOffset < 0 || cs->index.nIndexSize < 0 ){
    return SQLITE_CORRUPT;
  }
  if( cs->file.pFile==0 ) return SQLITE_IOERR;
  rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
  if( rc != SQLITE_OK ) return rc;
  if( fileSize > 0
   && (cs->index.iIndexOffset > fileSize
       || cs->index.nIndexSize > fileSize - cs->index.iIndexOffset) ){
    return SQLITE_CORRUPT;
  }

  cs->index.aIndex = (ChunkIndexEntry *)sqlite3_malloc(
    nEntries * (int)sizeof(ChunkIndexEntry)
  );
  if( cs->index.aIndex == 0 ) return SQLITE_NOMEM;
  cs->index.nIndex = nEntries;
  cs->index.aIndexMmapBase = 0;
  cs->index.aIndexMmapSize = 0;

  aBuf = (u8 *)sqlite3_malloc64(cs->index.nIndexSize);
  if( aBuf == 0 ){
    sqlite3_free(cs->index.aIndex);
    cs->index.aIndex = 0;
    cs->index.nIndex = 0;
    return SQLITE_NOMEM;
  }

  rc = sqlite3OsRead(cs->file.pFile, aBuf, cs->index.nIndexSize, cs->index.iIndexOffset);
  if( rc != SQLITE_OK ){
    sqlite3_free(aBuf);
    sqlite3_free(cs->index.aIndex);
    cs->index.aIndex = 0;
    cs->index.nIndex = 0;
    return rc;
  }

  for( i = 0; i < nEntries; i++ ){
    rc = csDeserializeIndexEntry(aBuf + i * CHUNK_INDEX_ENTRY_SIZE,
                                 &cs->index.aIndex[i]);
    if( rc != SQLITE_OK ){
      sqlite3_free(aBuf);
      sqlite3_free(cs->index.aIndex);
      cs->index.aIndex = 0;
      cs->index.nIndex = 0;
      return rc;
    }
  }

  rc = csValidateIndexEntries(cs->index.aIndex, nEntries,
                              cs->index.iIndexOffset);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aBuf);
    sqlite3_free(cs->index.aIndex);
    cs->index.aIndex = 0;
    cs->index.nIndex = 0;
    return rc;
  }

  sqlite3_free(aBuf);
  return SQLITE_OK;
}

int csIndexEntryCmp(const void *a, const void *b){
  const ChunkIndexEntry *ea = (const ChunkIndexEntry *)a;
  const ChunkIndexEntry *eb = (const ChunkIndexEntry *)b;
  return prollyHashCompare(&ea->hash, &eb->hash);
}

static int csIndexLowerBound(
  const ChunkIndexEntry *aIndex,
  int nIndex,
  int lo,
  const ProllyHash *pHash
){
  int hi = nIndex;
  while( lo < hi ){
    int mid = lo + ((hi - lo) >> 1);
    if( prollyHashCompare(&aIndex[mid].hash, pHash) < 0 ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return lo;
}

int csMergeIndex(
  ChunkStore *cs,
  ChunkIndexEntry **ppMerged,
  int *pnMerged
){
  i64 nTotal64 = (i64)cs->index.nIndex + (i64)cs->staging.nPending;
  int nTotal;
  ChunkIndexEntry *aMerged;
  int idxPos, pendPos, outPos;

  *ppMerged = 0;
  *pnMerged = 0;
  if( nTotal64 == 0 ) return SQLITE_OK;
  if( nTotal64 > INT_MAX/(int)sizeof(ChunkIndexEntry) ) return SQLITE_TOOBIG;
  nTotal = (int)nTotal64;

  aMerged = (ChunkIndexEntry *)sqlite3_malloc64(
    (sqlite3_uint64)nTotal * sizeof(ChunkIndexEntry)
  );
  if( aMerged == 0 ) return SQLITE_NOMEM;

  if( cs->staging.nPending > 1 ){
    qsort(cs->staging.aPending, cs->staging.nPending, sizeof(ChunkIndexEntry),
          csIndexEntryCmp);
  }

  if( cs->staging.nPending > 0 && cs->staging.nPending <= 32 ){
    idxPos = 0;
    outPos = 0;
    for( pendPos = 0; pendPos < cs->staging.nPending; pendPos++ ){
      ChunkIndexEntry *pPending = &cs->staging.aPending[pendPos];
      int found;
      int nCopy;
      int pos = csIndexLowerBound(cs->index.aIndex, cs->index.nIndex, idxPos,
                                  &pPending->hash);
      nCopy = pos - idxPos;
      if( nCopy > 0 ){
        memcpy(&aMerged[outPos], &cs->index.aIndex[idxPos],
               nCopy * sizeof(ChunkIndexEntry));
        outPos += nCopy;
      }
      found = pos < cs->index.nIndex
           && prollyHashCompare(&cs->index.aIndex[pos].hash, &pPending->hash)==0;
      aMerged[outPos++] = *pPending;
      idxPos = found ? pos + 1 : pos;
    }
    if( idxPos < cs->index.nIndex ){
      int nCopy = cs->index.nIndex - idxPos;
      memcpy(&aMerged[outPos], &cs->index.aIndex[idxPos],
             nCopy * sizeof(ChunkIndexEntry));
      outPos += nCopy;
    }
    *ppMerged = aMerged;
    *pnMerged = outPos;
    return SQLITE_OK;
  }

  idxPos = 0;
  pendPos = 0;
  outPos = 0;
  while( idxPos < cs->index.nIndex && pendPos < cs->staging.nPending ){
    int cmp = prollyHashCompare(&cs->index.aIndex[idxPos].hash, &cs->staging.aPending[pendPos].hash);
    if( cmp < 0 ){
      aMerged[outPos++] = cs->index.aIndex[idxPos++];
    }else if( cmp > 0 ){
      aMerged[outPos++] = cs->staging.aPending[pendPos++];
    }else{

      aMerged[outPos++] = cs->staging.aPending[pendPos++];
      idxPos++;
    }
  }
  while( idxPos < cs->index.nIndex ) aMerged[outPos++] = cs->index.aIndex[idxPos++];
  while( pendPos < cs->staging.nPending ) aMerged[outPos++] = cs->staging.aPending[pendPos++];

  *ppMerged = aMerged;
  *pnMerged = outPos;
  return SQLITE_OK;
}

#endif
