
#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "chunk_index.h"
#include "prolly_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define CS_INDEX_WINDOW_MIN 4096
#define CS_INDEX_WINDOW_MARGIN_DIV 64

void chunkIndexGetEntries(const ChunkIndex *idx, int *pn, const ChunkIndexEntry **par){
  assert( idx!=0 && pn!=0 && par!=0 );
  assert( idx->nIndex>=0 );
  assert( idx->nIndex==0 || idx->aIndex!=0 );
  *pn = idx->nIndex;
  *par = idx->aIndex;
}

int chunkIndexCount(const ChunkIndex *idx){
  i64 n;
  assert( idx!=0 );
  assert( idx->nIndex>=0 );
  n = (i64)idx->nIndex + idx->lazy.nEntries;
  return n>INT_MAX ? INT_MAX : (int)n;
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
  memset(&idx->lazy, 0, sizeof(idx->lazy));
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
  int outerLo = 0;
  int outerHi = nIdx - 1;
  assert( pHash!=0 );
  assert( nIdx>=0 );
  assert( nIdx==0 || aIdx!=0 );
  if( nIdx>=CS_INDEX_WINDOW_MIN ){
    int margin = nIdx/CS_INDEX_WINDOW_MARGIN_DIV + 1;
    int bucket = pHash->data[0];
    lo = (int)(((i64)bucket * nIdx) >> 8) - margin;
    hi = (int)(((i64)(bucket + 1) * nIdx) >> 8) + margin;
    if( lo<0 ) lo = 0;
    if( hi>=nIdx ) hi = nIdx - 1;
    outerLo = lo;
    outerHi = hi;
  }
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
  if( nIdx<CS_INDEX_WINDOW_MIN ) return -1;
  if( outerLo>0 && prollyHashCompare(pHash, &aIdx[outerLo].hash)<0 ){
    lo = 0;
    hi = outerLo - 1;
  }else if( outerHi<nIdx-1
         && prollyHashCompare(pHash, &aIdx[outerHi].hash)>0 ){
    lo = outerHi + 1;
    hi = nIdx - 1;
  }else{
    return -1;
  }
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

static int csReadLazyPage(
  ChunkStore *cs,
  i64 iOffset,
  int nBody,
  const ProllyHash *pHash,
  i64 iLimit,
  u8 **ppBody
){
  u8 aHeader[CS_WAL_CHUNK_HDR_SIZE];
  ProllyHash actual;
  u8 *aBody;
  int rc;

  *ppBody = 0;
  if( nBody<CS_INDEX_PAGE_HEADER_SIZE || nBody>CS_INDEX_PAGE_SIZE
   || iOffset<cs->index.lazy.iDataEnd
   || iLimit<CS_WAL_CHUNK_HDR_SIZE+nBody
   || iOffset>iLimit-CS_WAL_CHUNK_HDR_SIZE-nBody ){
    return SQLITE_CORRUPT;
  }
  rc = sqlite3OsRead(cs->file.pFile, aHeader, sizeof(aHeader), iOffset);
  if( rc!=SQLITE_OK ) return rc;
  if( aHeader[0]!=CS_WAL_TAG_CHUNK
   || CS_READ_U32(aHeader+CS_WAL_CHUNK_LEN_OFF)!=(u32)nBody
   || memcmp(aHeader+CS_WAL_CHUNK_HASH_OFF, pHash->data,
             PROLLY_HASH_SIZE)!=0 ){
    return SQLITE_CORRUPT;
  }
  aBody = sqlite3_malloc(nBody);
  if( !aBody ) return SQLITE_NOMEM;
  rc = sqlite3OsRead(cs->file.pFile, aBody, nBody,
                     iOffset+CS_WAL_CHUNK_HDR_SIZE);
  if( rc==SQLITE_OK ){
    prollyHashCompute(aBody, nBody, &actual);
    if( prollyHashCompare(&actual, pHash)!=0 ) rc = SQLITE_CORRUPT;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(aBody);
    return rc;
  }
  *ppBody = aBody;
  return SQLITE_OK;
}

static int csLazyPageShape(
  const u8 *aBody,
  int nBody,
  u32 *pMagic,
  int *pnCell,
  int *pnCellSize
){
  u32 magic;
  u32 nCell;
  int nCellSize;
  if( nBody<CS_INDEX_PAGE_HEADER_SIZE ) return SQLITE_CORRUPT;
  magic = CS_READ_U32(aBody);
  nCell = CS_READ_U32(aBody+4);
  nCellSize = magic==CS_INDEX_PAGE_LEAF_MAGIC
            ? CHUNK_INDEX_ENTRY_SIZE
            : magic==CS_INDEX_PAGE_INTERNAL_MAGIC ? CS_INDEX_CHILD_SIZE : 0;
  if( nCellSize==0 || nCell==0 || nCell>(u32)INT_MAX
   || (i64)CS_INDEX_PAGE_HEADER_SIZE+(i64)nCell*nCellSize!=nBody ){
    return SQLITE_CORRUPT;
  }
  *pMagic = magic;
  *pnCell = (int)nCell;
  *pnCellSize = nCellSize;
  return SQLITE_OK;
}

static int csLazyPageMaxHash(
  const u8 *aBody,
  u32 magic,
  int nCell,
  ProllyHash *pMax
){
  const u8 *p;
  if( magic==CS_INDEX_PAGE_LEAF_MAGIC ){
    p = aBody+CS_INDEX_PAGE_HEADER_SIZE
      +(nCell-1)*CHUNK_INDEX_ENTRY_SIZE;
  }else{
    p = aBody+CS_INDEX_PAGE_HEADER_SIZE+(nCell-1)*CS_INDEX_CHILD_SIZE;
  }
  memcpy(pMax->data, p, PROLLY_HASH_SIZE);
  return SQLITE_OK;
}

int csIndexLookup(
  ChunkStore *cs,
  const ProllyHash *pHash,
  ChunkIndexEntry *pEntry,
  int *pFound
){
  ChunkIndexLazy *pLazy = &cs->index.lazy;
  ProllyHash pageHash;
  ProllyHash expectedMax;
  i64 iOffset;
  i64 iLimit;
  int nBody;
  int haveExpectedMax = 0;
  int depth;
  int idx;

  *pFound = 0;
  idx = csSearchIndex(cs->index.aIndex, cs->index.nIndex, pHash);
  if( idx>=0 ){
    *pEntry = cs->index.aIndex[idx];
    *pFound = 1;
    return SQLITE_OK;
  }
  if( !pLazy->active ) return SQLITE_OK;
  iOffset = pLazy->iRootOffset;
  nBody = pLazy->nRootSize;
  pageHash = pLazy->rootHash;
  iLimit = iOffset+CS_WAL_CHUNK_HDR_SIZE+nBody;

  for(depth=0; depth<32; depth++){
    u8 *aBody = 0;
    u32 magic;
    int nCell;
    int nCellSize;
    ProllyHash pageMax;
    int rc = csReadLazyPage(cs, iOffset, nBody, &pageHash, iLimit, &aBody);
    if( rc!=SQLITE_OK ) return rc;
    rc = csLazyPageShape(aBody, nBody, &magic, &nCell, &nCellSize);
    if( rc==SQLITE_OK ){
      csLazyPageMaxHash(aBody, magic, nCell, &pageMax);
      if( haveExpectedMax
       && prollyHashCompare(&pageMax, &expectedMax)!=0 ){
        rc = SQLITE_CORRUPT;
      }
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(aBody);
      return rc;
    }

    if( magic==CS_INDEX_PAGE_LEAF_MAGIC ){
      int lo = 0;
      int hi = nCell-1;
      int i;
      ProllyHash previous;
      for(i=0; i<nCell; i++){
        const u8 *p = aBody+CS_INDEX_PAGE_HEADER_SIZE
                    +i*CHUNK_INDEX_ENTRY_SIZE;
        ChunkIndexEntry e;
        rc = csDeserializeIndexEntry(p, &e);
        if( rc!=SQLITE_OK
         || e.offset<CHUNK_MANIFEST_SIZE
         || e.offset>pLazy->iDataEnd-4
         || (i64)e.size>pLazy->iDataEnd-e.offset-4
         || (i>0 && prollyHashCompare(&previous, &e.hash)>=0) ){
          sqlite3_free(aBody);
          return SQLITE_CORRUPT;
        }
        previous = e.hash;
      }
      while( lo<=hi ){
        int mid = lo+(hi-lo)/2;
        const u8 *p = aBody+CS_INDEX_PAGE_HEADER_SIZE
                    +mid*CHUNK_INDEX_ENTRY_SIZE;
        ProllyHash h;
        int cmp;
        memcpy(h.data, p, PROLLY_HASH_SIZE);
        cmp = prollyHashCompare(&h, pHash);
        if( cmp<0 ) lo = mid+1;
        else if( cmp>0 ) hi = mid-1;
        else{
          rc = csDeserializeIndexEntry(p, pEntry);
          if( rc==SQLITE_OK
           && (pEntry->offset<CHUNK_MANIFEST_SIZE
               || pEntry->offset>pLazy->iDataEnd-4
               || (i64)pEntry->size
                    >pLazy->iDataEnd-pEntry->offset-4) ){
            rc = SQLITE_CORRUPT;
          }
          sqlite3_free(aBody);
          if( rc==SQLITE_OK ) *pFound = 1;
          return rc;
        }
      }
      sqlite3_free(aBody);
      return SQLITE_OK;
    }else{
      int lo = 0;
      int hi = nCell;
      const u8 *p;
      int i;
      ProllyHash previous;
      for(i=0; i<nCell; i++){
        const u8 *q = aBody+CS_INDEX_PAGE_HEADER_SIZE
                    +i*CS_INDEX_CHILD_SIZE;
        ProllyHash h;
        i64 childOffset;
        u32 childSize;
        memcpy(h.data, q, PROLLY_HASH_SIZE);
        q += PROLLY_HASH_SIZE;
        childOffset = CS_READ_I64(q);
        q += 8;
        childSize = CS_READ_U32(q);
        if( (i>0 && prollyHashCompare(&previous, &h)>=0)
         || childSize<CS_INDEX_PAGE_HEADER_SIZE
         || childSize>CS_INDEX_PAGE_SIZE
         || childOffset<pLazy->iDataEnd
         || iOffset<CS_WAL_CHUNK_HDR_SIZE+(i64)childSize
         || childOffset>iOffset-CS_WAL_CHUNK_HDR_SIZE-childSize ){
          sqlite3_free(aBody);
          return SQLITE_CORRUPT;
        }
        previous = h;
      }
      while( lo<hi ){
        int mid = lo+(hi-lo)/2;
        const u8 *q = aBody+CS_INDEX_PAGE_HEADER_SIZE
                    +mid*CS_INDEX_CHILD_SIZE;
        ProllyHash h;
        memcpy(h.data, q, PROLLY_HASH_SIZE);
        if( prollyHashCompare(&h, pHash)<0 ) lo = mid+1;
        else hi = mid;
      }
      if( lo==nCell ){
        sqlite3_free(aBody);
        return SQLITE_OK;
      }
      p = aBody+CS_INDEX_PAGE_HEADER_SIZE+lo*CS_INDEX_CHILD_SIZE;
      memcpy(expectedMax.data, p, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      iLimit = iOffset;
      iOffset = CS_READ_I64(p);
      p += 8;
      nBody = (int)CS_READ_U32(p);
      p += 4;
      memcpy(pageHash.data, p, PROLLY_HASH_SIZE);
      haveExpectedMax = 1;
      sqlite3_free(aBody);
    }
  }
  return SQLITE_CORRUPT;
}

static int csCollectLazyPage(
  ChunkStore *cs,
  i64 iOffset,
  int nBody,
  const ProllyHash *pHash,
  i64 iLimit,
  const ProllyHash *pExpectedMax,
  ChunkIndexEntry *aOut,
  int nOut,
  int *pPos,
  int depth
){
  u8 *aBody = 0;
  u32 magic;
  int nCell;
  int nCellSize;
  ProllyHash pageMax;
  int i;
  int rc;

  if( depth>=32 ) return SQLITE_CORRUPT;
  rc = csReadLazyPage(cs, iOffset, nBody, pHash, iLimit, &aBody);
  if( rc!=SQLITE_OK ) return rc;
  rc = csLazyPageShape(aBody, nBody, &magic, &nCell, &nCellSize);
  if( rc!=SQLITE_OK ) goto collect_done;
  csLazyPageMaxHash(aBody, magic, nCell, &pageMax);
  if( pExpectedMax && prollyHashCompare(&pageMax, pExpectedMax)!=0 ){
    rc = SQLITE_CORRUPT;
    goto collect_done;
  }

  if( magic==CS_INDEX_PAGE_LEAF_MAGIC ){
    for(i=0; i<nCell; i++){
      ChunkIndexEntry e;
      const u8 *p = aBody+CS_INDEX_PAGE_HEADER_SIZE
                  +i*CHUNK_INDEX_ENTRY_SIZE;
      rc = csDeserializeIndexEntry(p, &e);
      if( rc!=SQLITE_OK ) goto collect_done;
      if( e.offset<CHUNK_MANIFEST_SIZE || e.offset>cs->index.lazy.iDataEnd-4
       || (i64)e.size>cs->index.lazy.iDataEnd-e.offset-4
       || *pPos>=nOut
       || (*pPos>0
           && prollyHashCompare(&aOut[*pPos-1].hash, &e.hash)>=0) ){
        rc = SQLITE_CORRUPT;
        goto collect_done;
      }
      aOut[(*pPos)++] = e;
    }
  }else{
    ProllyHash previousMax;
    for(i=0; i<nCell; i++){
      const u8 *p = aBody+CS_INDEX_PAGE_HEADER_SIZE+i*CS_INDEX_CHILD_SIZE;
      ProllyHash childMax;
      ProllyHash childHash;
      i64 childOffset;
      int childSize;
      memcpy(childMax.data, p, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      childOffset = CS_READ_I64(p);
      p += 8;
      childSize = (int)CS_READ_U32(p);
      p += 4;
      memcpy(childHash.data, p, PROLLY_HASH_SIZE);
      if( i>0 && prollyHashCompare(&previousMax, &childMax)>=0 ){
        rc = SQLITE_CORRUPT;
        goto collect_done;
      }
      previousMax = childMax;
      rc = csCollectLazyPage(cs, childOffset, childSize, &childHash,
                             iOffset, &childMax, aOut, nOut, pPos, depth+1);
      if( rc!=SQLITE_OK ) goto collect_done;
    }
  }

collect_done:
  sqlite3_free(aBody);
  return rc;
}

int csMaterializeIndex(ChunkStore *cs){
  ChunkIndexLazy lazy = cs->index.lazy;
  ChunkIndexEntry *aLazy = 0;
  ChunkIndexEntry *aMerged = 0;
  int nTotal;
  int i = 0;
  int j = 0;
  int k = 0;
  int rc;

  if( !lazy.active ) return SQLITE_OK;
  if( lazy.nEntries<=0
   || lazy.nEntries>INT_MAX/(int)sizeof(ChunkIndexEntry)
   || cs->index.nIndex>INT_MAX-lazy.nEntries ){
    return SQLITE_CORRUPT;
  }
  aLazy = sqlite3_malloc64(
      (sqlite3_uint64)lazy.nEntries*sizeof(ChunkIndexEntry));
  if( !aLazy ) return SQLITE_NOMEM;
  rc = csCollectLazyPage(cs, lazy.iRootOffset, lazy.nRootSize,
                         &lazy.rootHash,
                         lazy.iRootOffset+CS_WAL_CHUNK_HDR_SIZE+lazy.nRootSize,
                         0, aLazy, lazy.nEntries, &i, 0);
  if( rc!=SQLITE_OK || i!=lazy.nEntries ){
    sqlite3_free(aLazy);
    return rc==SQLITE_OK ? SQLITE_CORRUPT : rc;
  }
  nTotal = lazy.nEntries+cs->index.nIndex;
  aMerged = sqlite3_malloc64((sqlite3_uint64)nTotal*sizeof(ChunkIndexEntry));
  if( !aMerged ){
    sqlite3_free(aLazy);
    return SQLITE_NOMEM;
  }
  i = 0;
  while( i<lazy.nEntries && j<cs->index.nIndex ){
    int cmp = prollyHashCompare(&aLazy[i].hash, &cs->index.aIndex[j].hash);
    if( cmp<0 ) aMerged[k++] = aLazy[i++];
    else if( cmp>0 ) aMerged[k++] = cs->index.aIndex[j++];
    else{ aMerged[k++] = cs->index.aIndex[j++]; i++; }
  }
  while( i<lazy.nEntries ) aMerged[k++] = aLazy[i++];
  while( j<cs->index.nIndex ) aMerged[k++] = cs->index.aIndex[j++];
  sqlite3_free(aLazy);
  chunkIndexReplaceEntries(&cs->index, aMerged, k);
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

  memset(&cs->index.lazy, 0, sizeof(cs->index.lazy));
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
  /* nChunks includes WAL; index compacted. */
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

  rc = csReadSliced(cs, aBuf, (i64)cs->index.nIndexSize, cs->index.iIndexOffset);
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
