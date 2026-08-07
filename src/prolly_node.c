
#ifdef DOLTLITE_PROLLY

#include "prolly_node.h"
#include "prolly_encoding.h"
#include <string.h>

#define PROLLY_HDR_SIZE       8
#define PROLLY_MAGIC_OFF      0
#define PROLLY_LEVEL_OFF      4
#define PROLLY_COUNT_OFF      5
#define PROLLY_FLAGS_OFF      7

int prollyNodeParseSparse(ProllyNode *pNode, const u8 *pData, int nData,
                          int nDataPhys){
  u32 magic;
  u16 count;
  int nOffsets;
  int minSize;
  i64 expectedSize;
  u32 totalKeyBytes;
  u32 totalValBytes;
  const u8 *pCur;
  int i;
  u8 keyFlag;
  int hasCounts;

  memset(pNode, 0, sizeof(*pNode));

  if( nData<PROLLY_HDR_SIZE || nDataPhys<PROLLY_HDR_SIZE || nDataPhys>nData ){
    return SQLITE_CORRUPT;
  }

  magic = PROLLY_GET_U32(pData + PROLLY_MAGIC_OFF);
  if( magic!=PROLLY_NODE_MAGIC ){
    return SQLITE_CORRUPT;
  }

  pNode->pData = pData;
  pNode->nData = nData;
  pNode->nDataPhys = nDataPhys;
  pNode->level = pData[PROLLY_LEVEL_OFF];
  count = PROLLY_GET_U16(pData + PROLLY_COUNT_OFF);
  if( count > PROLLY_NODE_MAX_ITEMS ){
    return SQLITE_CORRUPT;
  }
  pNode->nItems = count;
  pNode->flags = pData[PROLLY_FLAGS_OFF];
  keyFlag = pNode->flags & (PROLLY_NODE_INTKEY | PROLLY_NODE_BLOBKEY);
  if( keyFlag!=PROLLY_NODE_INTKEY && keyFlag!=PROLLY_NODE_BLOBKEY ){
    return SQLITE_CORRUPT;
  }
  hasCounts = (pNode->flags & PROLLY_NODE_SUBTREE_COUNTS) ? 1 : 0;
  if( hasCounts && pNode->level==0 ){
    return SQLITE_CORRUPT;
  }

  if( count==0 ){
    if( pNode->level>0 ){
      return SQLITE_CORRUPT;
    }

    pNode->aKeyOff = 0;
    pNode->aValOff = 0;
    pNode->pKeyData = pData + PROLLY_HDR_SIZE;
    pNode->pValData = pData + PROLLY_HDR_SIZE;
    return SQLITE_OK;
  }

  nOffsets = (int)(count + 1) * 4 * 2;
  minSize = PROLLY_HDR_SIZE + nOffsets;
  if( nDataPhys<minSize ){
    return SQLITE_CORRUPT;
  }

  pCur = pData + PROLLY_HDR_SIZE;
  pNode->aKeyOff = (const u32*)pCur;
  pCur += (count + 1) * 4;
  pNode->aValOff = (const u32*)pCur;
  pCur += (count + 1) * 4;

  pNode->pKeyData = pCur;

  totalKeyBytes = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[count]);
  totalValBytes = PROLLY_GET_U32((const u8*)&pNode->aValOff[count]);
  if( totalKeyBytes > (u32)nData || totalValBytes > (u32)nData ){
    return SQLITE_CORRUPT;
  }

  pNode->pValData = pCur + totalKeyBytes;

  expectedSize = (i64)minSize + totalKeyBytes + totalValBytes;
  if( hasCounts ){
    expectedSize += (i64)count * 8;
  }
  if( expectedSize != nData ){
    return SQLITE_CORRUPT;
  }
  if( nDataPhys<nData ){
    if( pNode->level!=0 || hasCounts ){
      return SQLITE_CORRUPT;
    }
    if( nDataPhys < minSize + (int)totalKeyBytes ){
      return SQLITE_CORRUPT;
    }
  }else if( nDataPhys!=nData ){
    return SQLITE_CORRUPT;
  }

  if( hasCounts ){
    pNode->pSubtreeCounts = pCur + totalKeyBytes + totalValBytes;
  }

  for(i=0; i<=count; i++){
    u32 keyOff = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
    u32 valOff = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
    if( keyOff > totalKeyBytes || valOff > totalValBytes ){
      return SQLITE_CORRUPT;
    }
    /* Both arrays must start at 0. Readers reach key data two ways -- through
    ** these offsets, and by striding from pKeyData for fixed-width keys, as
    ** prollyNodeIntKey does with i*8 -- and a nonzero first offset makes the
    ** two disagree about the same node while every other check still passes. */
    if( i==0 && (keyOff!=0 || valOff!=0) ){
      return SQLITE_CORRUPT;
    }
    if( i>0 ){
      u32 prevKeyOff = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i-1]);
      u32 prevValOff = PROLLY_GET_U32((const u8*)&pNode->aValOff[i-1]);
      if( keyOff < prevKeyOff || valOff < prevValOff ){
        return SQLITE_CORRUPT;
      }
      if( (pNode->flags & PROLLY_NODE_INTKEY) && keyOff - prevKeyOff != 8 ){
        return SQLITE_CORRUPT;
      }
      if( pNode->level>0 && valOff - prevValOff != PROLLY_HASH_SIZE ){
        return SQLITE_CORRUPT;
      }
    }
  }

  return SQLITE_OK;
}

int prollyNodeParse(ProllyNode *pNode, const u8 *pData, int nData){
  return prollyNodeParseSparse(pNode, pData, nData, nData);
}

void prollyNodeKey(const ProllyNode *pNode, int i, const u8 **ppKey, int *pnKey){
  u32 off0;
  u32 off1;
  assert( i >= 0 && i < (int)pNode->nItems );
  off0 = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
  off1 = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i+1]);
  *ppKey = pNode->pKeyData + off0;
  *pnKey = (int)(off1 - off0);
}

void prollyNodeValue(const ProllyNode *pNode, int i, const u8 **ppVal, int *pnVal){
  int nAvail;
  prollyNodeValueSpan(pNode, i, ppVal, pnVal, &nAvail);
}

void prollyNodeValueSpan(const ProllyNode *pNode, int i, const u8 **ppVal,
                         int *pnVal, int *pnAvail){
  u32 off0;
  u32 off1;
  int nValPhys;
  assert( i >= 0 && i < (int)pNode->nItems );
  off0 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  off1 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i+1]);
  *pnVal = (int)(off1 - off0);
  nValPhys = pNode->nDataPhys - (int)(pNode->pValData - pNode->pData);
  if( nValPhys<0 ) nValPhys = 0;
  if( (int)off0 < nValPhys ){
    *ppVal = pNode->pValData + off0;
    *pnAvail = nValPhys - (int)off0;
    if( *pnAvail > *pnVal ) *pnAvail = *pnVal;
  }else{
    *ppVal = pNode->pValData + nValPhys;
    *pnAvail = 0;
  }
}

void prollyEncodeIntKey(i64 v, u8 buf[8]){
  u64 u = ((u64)v) ^ ((u64)1 << 63);
  buf[0] = (u8)(u >> 56);
  buf[1] = (u8)(u >> 48);
  buf[2] = (u8)(u >> 40);
  buf[3] = (u8)(u >> 32);
  buf[4] = (u8)(u >> 24);
  buf[5] = (u8)(u >> 16);
  buf[6] = (u8)(u >> 8);
  buf[7] = (u8)u;
}

i64 prollyDecodeIntKey(const u8 *p){
  u64 u = ((u64)p[0]<<56) | ((u64)p[1]<<48) | ((u64)p[2]<<40) | ((u64)p[3]<<32)
        | ((u64)p[4]<<24) | ((u64)p[5]<<16) | ((u64)p[6]<<8) | (u64)p[7];
  return (i64)(u ^ ((u64)1 << 63));
}

i64 prollyNodeIntKey(const ProllyNode *pNode, int i){
  const u8 *p;
  assert( i >= 0 && i < (int)pNode->nItems );
  assert( (pNode->flags & PROLLY_NODE_INTKEY)!=0 );
  p = pNode->pKeyData + i*8;
  return prollyDecodeIntKey(p);
}

void prollyNodeChildHash(const ProllyNode *pNode, int i, ProllyHash *pHash){
  u32 off;
  assert( i >= 0 && i < (int)pNode->nItems );
  off = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  memcpy(pHash->data, pNode->pValData + off, PROLLY_HASH_SIZE);
}

int prollyNodeHasSubtreeCounts(const ProllyNode *pNode){
  return (pNode->flags & PROLLY_NODE_SUBTREE_COUNTS) ? 1 : 0;
}

u64 prollyNodeChildSubtreeCount(const ProllyNode *pNode, int i){
  const u8 *p;
  u64 v;
  assert( i >= 0 && i < (int)pNode->nItems );
  assert( pNode->pSubtreeCounts != 0 );
  p = pNode->pSubtreeCounts + (i * 8);
  v = ((u64)p[0])
    | ((u64)p[1] << 8)
    | ((u64)p[2] << 16)
    | ((u64)p[3] << 24)
    | ((u64)p[4] << 32)
    | ((u64)p[5] << 40)
    | ((u64)p[6] << 48)
    | ((u64)p[7] << 56);
  return v;
}

static SQLITE_INLINE int prollyKeyComparePrefix(
  const u8 *pLeft,
  const u8 *pRight,
  int n
){
  /* Blob-key node searches compare many short keys. Check the first word
  ** inline before calling memcmp() for any remaining suffix. */
  if( n>=8 ){
    u64 left = ((u64)pLeft[0]<<56) | ((u64)pLeft[1]<<48)
             | ((u64)pLeft[2]<<40) | ((u64)pLeft[3]<<32)
             | ((u64)pLeft[4]<<24) | ((u64)pLeft[5]<<16)
             | ((u64)pLeft[6]<<8) | (u64)pLeft[7];
    u64 right = ((u64)pRight[0]<<56) | ((u64)pRight[1]<<48)
              | ((u64)pRight[2]<<40) | ((u64)pRight[3]<<32)
              | ((u64)pRight[4]<<24) | ((u64)pRight[5]<<16)
              | ((u64)pRight[6]<<8) | (u64)pRight[7];
    if( left<right ) return -1;
    if( left>right ) return 1;
    pLeft += 8;
    pRight += 8;
    n -= 8;
    if( n==0 ) return 0;
  }
  return memcmp(pLeft, pRight, n);
}

int prollyNodeSearchBlob(
  const ProllyNode *pNode,
  const u8 *pKey,
  int nKey,
  int *pRes
){
  int lo = 0;
  int hi = pNode->nItems - 1;
  int mid;
  int c;
  const u8 *pMidKey;
  int nMidKey;
  int nCmp;

  if( pNode->nItems==0 ){
    *pRes = -1;
    return 0;
  }

  while( lo<=hi ){
    mid = lo + (hi - lo) / 2;
    prollyNodeKey(pNode, mid, &pMidKey, &nMidKey);

    nCmp = nMidKey < nKey ? nMidKey : nKey;
    c = prollyKeyComparePrefix(pKey, pMidKey, nCmp);
    if( c==0 ) c = nKey - nMidKey;

    if( c==0 ){
      *pRes = 0;
      return mid;
    }else if( c<0 ){
      hi = mid - 1;
    }else{
      lo = mid + 1;
    }
  }

  if( lo>=pNode->nItems ){
    *pRes = 1;
    return pNode->nItems - 1;
  }else{
    *pRes = -1;
    return lo;
  }
}

int prollyNodeSearchInt(const ProllyNode *pNode, i64 intKey, int *pRes){
  int lo = 0;
  int hi = pNode->nItems - 1;
  int mid;
  i64 midKey;

  if( pNode->nItems==0 ){
    *pRes = -1;
    return 0;
  }

  while( lo<=hi ){
    mid = lo + (hi - lo) / 2;
    midKey = prollyNodeIntKey(pNode, mid);

    if( intKey==midKey ){
      *pRes = 0;
      return mid;
    }else if( intKey<midKey ){
      hi = mid - 1;
    }else{
      lo = mid + 1;
    }
  }

  if( lo>=pNode->nItems ){
    *pRes = 1;
    return pNode->nItems - 1;
  }else{
    *pRes = -1;
    return lo;
  }
}

#define PROLLY_BUILDER_INIT_CAP  64
#define PROLLY_BUILDER_INIT_BUF  1024

void prollyNodeBuilderInit(ProllyNodeBuilder *b, u8 level, u8 flags){
  memset(b, 0, sizeof(*b));
  b->level = level;
  b->flags = flags;
}

static int builderGrowOffsets(ProllyNodeBuilder *b){
  i64 nNeeded = (i64)b->nItems + 2;
  if( nNeeded > (i64)0x7fffffff/(i64)sizeof(u32) ) return SQLITE_NOMEM;
  if( nNeeded > (i64)b->nAlloc ){
    i64 nNew = b->nAlloc ? (i64)b->nAlloc * 2 : (i64)PROLLY_BUILDER_INIT_CAP;
    u32 *aNew;
    while( nNew < nNeeded ){
      if( nNew > (i64)0x7fffffff/(i64)sizeof(u32)/2 ){
        nNew = (i64)0x7fffffff/(i64)sizeof(u32);
        break;
      }
      nNew *= 2;
    }
    if( nNew < nNeeded ) return SQLITE_NOMEM;

    aNew = (u32*)sqlite3_realloc(b->aKeyOff, (int)(nNew * (i64)sizeof(u32)));
    if( !aNew ) return SQLITE_NOMEM;
    b->aKeyOff = aNew;

    aNew = (u32*)sqlite3_realloc(b->aValOff, (int)(nNew * (i64)sizeof(u32)));
    if( !aNew ) return SQLITE_NOMEM;
    b->aValOff = aNew;

    b->nAlloc = (int)nNew;
  }
  return SQLITE_OK;
}

static int builderGrowSubtreeCounts(ProllyNodeBuilder *b){
  i64 nNeeded = (i64)b->nItems + 1;
  if( nNeeded > (i64)0x7fffffff/(i64)sizeof(u64) ) return SQLITE_NOMEM;
  if( nNeeded > (i64)b->nSubtreeCountAlloc ){
    i64 nNew = b->nSubtreeCountAlloc
                 ? (i64)b->nSubtreeCountAlloc * 2 : (i64)PROLLY_BUILDER_INIT_CAP;
    u64 *aNew;
    while( nNew < nNeeded ){
      if( nNew > (i64)0x7fffffff/(i64)sizeof(u64)/2 ){
        nNew = (i64)0x7fffffff/(i64)sizeof(u64);
        break;
      }
      nNew *= 2;
    }
    if( nNew < nNeeded ) return SQLITE_NOMEM;
    aNew = (u64*)sqlite3_realloc(b->aSubtreeCount,
                                 (int)(nNew * (i64)sizeof(u64)));
    if( !aNew ) return SQLITE_NOMEM;
    b->aSubtreeCount = aNew;
    b->nSubtreeCountAlloc = (int)nNew;
  }
  return SQLITE_OK;
}

static int builderGrowKeyBuf(ProllyNodeBuilder *b, int nAdd){
  i64 nNeeded;
  if( nAdd<0 ) return SQLITE_NOMEM;
  nNeeded = (i64)b->nKeyBytes + (i64)nAdd;
  if( nNeeded > (i64)0x7fffffff ) return SQLITE_NOMEM;
  if( nNeeded > (i64)b->nKeyBufAlloc ){
    i64 nNew = b->nKeyBufAlloc ? (i64)b->nKeyBufAlloc * 2 : (i64)PROLLY_BUILDER_INIT_BUF;
    u8 *pNew;
    while( nNew < nNeeded ){
      if( nNew > (i64)0x7fffffff/2 ){
        nNew = (i64)0x7fffffff;
        break;
      }
      nNew *= 2;
    }
    if( nNew < nNeeded ) return SQLITE_NOMEM;
    pNew = (u8*)sqlite3_realloc(b->pKeyBuf, (int)nNew);
    if( !pNew ) return SQLITE_NOMEM;
    b->pKeyBuf = pNew;
    b->nKeyBufAlloc = (int)nNew;
  }
  return SQLITE_OK;
}

static int builderGrowValBuf(ProllyNodeBuilder *b, int nAdd){
  i64 nNeeded;
  if( nAdd<0 ) return SQLITE_NOMEM;
  nNeeded = (i64)b->nValBytes + (i64)nAdd;
  if( nNeeded > (i64)0x7fffffff ) return SQLITE_NOMEM;
  if( nNeeded > (i64)b->nValBufAlloc ){
    i64 nNew = b->nValBufAlloc ? (i64)b->nValBufAlloc * 2 : (i64)PROLLY_BUILDER_INIT_BUF;
    u8 *pNew;
    while( nNew < nNeeded ){
      if( nNew > (i64)0x7fffffff/2 ){
        nNew = (i64)0x7fffffff;
        break;
      }
      nNew *= 2;
    }
    if( nNew < nNeeded ) return SQLITE_NOMEM;
    /* A single oversize value would leave most of a doubled buffer dead;
    ** size to fit and let the next ordinary add resume doubling. */
    if( nAdd >= 1024*1024 && nNew > nNeeded ) nNew = nNeeded;
    pNew = (u8*)sqlite3_realloc(b->pValBuf, (int)nNew);
    if( !pNew ) return SQLITE_NOMEM;
    b->pValBuf = pNew;
    b->nValBufAlloc = (int)nNew;
  }
  return SQLITE_OK;
}

/* Restore the physical-equals-logical invariant on pValBuf by writing the
** symbolic zero tail of the last value into the buffer. */
static int builderMaterializeZeroTail(ProllyNodeBuilder *b){
  int rc;
  int nTail = (int)b->nValZeroTail;
  int nPhys;
  if( nTail==0 ) return SQLITE_OK;
  nPhys = b->nValBytes - nTail;
  b->nValBytes = nPhys;
  rc = builderGrowValBuf(b, nTail);
  b->nValBytes = nPhys + nTail;
  if( rc ) return rc;
  memset(b->pValBuf + nPhys, 0, (size_t)nTail);
  b->nValZeroTail = 0;
  return SQLITE_OK;
}

static int builderAddCore(
  ProllyNodeBuilder *b,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal,
  u64 subtreeCount, int haveCount
){
  int rc;

  if( b->nItems>=PROLLY_NODE_MAX_ITEMS ){
    return SQLITE_FULL;
  }

  rc = builderMaterializeZeroTail(b);
  if( rc ) return rc;

  rc = builderGrowOffsets(b);
  if( rc ) return rc;

  rc = builderGrowKeyBuf(b, nKey);
  if( rc ) return rc;
  rc = builderGrowValBuf(b, nVal);
  if( rc ) return rc;

  if( haveCount ){
    rc = builderGrowSubtreeCounts(b);
    if( rc ) return rc;
  }

  if( b->nItems==0 ){
    b->aKeyOff[0] = 0;
    b->aValOff[0] = 0;
  }

  if( nKey > 0 ){
    memcpy(b->pKeyBuf + b->nKeyBytes, pKey, nKey);
    b->nKeyBytes += nKey;
  }
  b->aKeyOff[b->nItems + 1] = (u32)b->nKeyBytes;

  if( nVal > 0 ){
    memcpy(b->pValBuf + b->nValBytes, pVal, nVal);
    b->nValBytes += nVal;
  }
  b->aValOff[b->nItems + 1] = (u32)b->nValBytes;

  if( haveCount ){
    b->aSubtreeCount[b->nItems] = subtreeCount;
  }

  b->nItems++;
  return SQLITE_OK;
}

int prollyNodeBuilderAdd(
  ProllyNodeBuilder *b,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal
){
  return builderAddCore(b, pKey, nKey, pVal, nVal, 0, 0);
}

int prollyNodeBuilderAddWithCount(
  ProllyNodeBuilder *b,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal,
  u64 subtreeCount
){
  return builderAddCore(b, pKey, nKey, pVal, nVal, subtreeCount, 1);
}

/* Add an entry whose value is pVal[0..nValPrefix) followed by nZeroTail zero
** bytes. The zeros are recorded symbolically; they materialize only if
** another entry is added afterward (the tail must remain the final bytes of
** the value region) or the node is finished with the flat Finish. */
int prollyNodeBuilderAddZeroTail(
  ProllyNodeBuilder *b,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nValPrefix,
  i64 nZeroTail
){
  int rc;

  if( nZeroTail<=0 ){
    return builderAddCore(b, pKey, nKey, pVal, nValPrefix, 0, 0);
  }
  assert( b->level==0 );

  if( b->nItems>=PROLLY_NODE_MAX_ITEMS ){
    return SQLITE_FULL;
  }
  if( (i64)b->nValBytes + (i64)nValPrefix + nZeroTail > (i64)0x7fffffff ){
    return SQLITE_NOMEM;
  }

  rc = builderMaterializeZeroTail(b);
  if( rc ) return rc;

  rc = builderGrowOffsets(b);
  if( rc ) return rc;
  rc = builderGrowKeyBuf(b, nKey);
  if( rc ) return rc;
  rc = builderGrowValBuf(b, nValPrefix);
  if( rc ) return rc;

  if( b->nItems==0 ){
    b->aKeyOff[0] = 0;
    b->aValOff[0] = 0;
  }

  if( nKey > 0 ){
    memcpy(b->pKeyBuf + b->nKeyBytes, pKey, nKey);
    b->nKeyBytes += nKey;
  }
  b->aKeyOff[b->nItems + 1] = (u32)b->nKeyBytes;

  if( nValPrefix > 0 ){
    memcpy(b->pValBuf + b->nValBytes, pVal, nValPrefix);
  }
  b->nValBytes += nValPrefix + (int)nZeroTail;
  b->nValZeroTail = nZeroTail;
  b->aValOff[b->nItems + 1] = (u32)b->nValBytes;

  b->nItems++;
  return SQLITE_OK;
}

int prollyNodeBuilderFinish(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut){
  i64 nZeroTail = 0;
  int rc = builderMaterializeZeroTail(b);
  if( rc ){
    *ppOut = 0;
    *pnOut = 0;
    return rc;
  }
  rc = prollyNodeBuilderFinishSparse(b, ppOut, pnOut, &nZeroTail);
  assert( rc!=SQLITE_OK || nZeroTail==0 );
  return rc;
}

/* Like prollyNodeBuilderFinish, but a symbolic zero tail stays symbolic:
** *ppOut holds the node minus its final *pnZeroTail zero bytes while *pnOut
** is still the full logical size. Tails only arise on leaves, where the
** value region is the last region of the node, so the omitted bytes are
** always the chunk's final bytes. */
int prollyNodeBuilderFinishSparse(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut,
                                  i64 *pnZeroTail){
  int nOff;
  int nTotal;
  int nPhys;
  u8 *pBuf;
  u8 *pCur;
  int i;
  int writeCounts;
  u8 flagsOut;

  *ppOut = 0;
  *pnOut = 0;
  *pnZeroTail = 0;

  writeCounts = (b->level > 0 && b->aSubtreeCount != 0 && b->nItems > 0) ? 1 : 0;
  flagsOut = b->flags;
  if( writeCounts ){
    flagsOut |= PROLLY_NODE_SUBTREE_COUNTS;
  }else{
    flagsOut &= ~PROLLY_NODE_SUBTREE_COUNTS;
  }

  nOff = (b->nItems + 1) * 4;
  nTotal = PROLLY_HDR_SIZE + nOff * 2 + b->nKeyBytes + b->nValBytes;
  if( writeCounts ){
    nTotal += b->nItems * 8;
  }
  assert( b->nValZeroTail==0 || !writeCounts );
  nPhys = nTotal - (int)b->nValZeroTail;

  pBuf = (u8*)sqlite3_malloc(nPhys);
  if( !pBuf ) return SQLITE_NOMEM;

  PROLLY_PUT_U32(pBuf + PROLLY_MAGIC_OFF, PROLLY_NODE_MAGIC);
  pBuf[PROLLY_LEVEL_OFF] = b->level;
  PROLLY_PUT_U16(pBuf + PROLLY_COUNT_OFF, (u16)b->nItems);
  pBuf[PROLLY_FLAGS_OFF] = flagsOut;

  pCur = pBuf + PROLLY_HDR_SIZE;

  for(i=0; i<=b->nItems; i++){
    PROLLY_PUT_U32(pCur, b->aKeyOff[i]);
    pCur += 4;
  }

  for(i=0; i<=b->nItems; i++){
    PROLLY_PUT_U32(pCur, b->aValOff[i]);
    pCur += 4;
  }

  if( b->nKeyBytes>0 ){
    memcpy(pCur, b->pKeyBuf, b->nKeyBytes);
    pCur += b->nKeyBytes;
  }

  if( b->nValBytes>0 ){
    int nValPhys = b->nValBytes - (int)b->nValZeroTail;
    memcpy(pCur, b->pValBuf, nValPhys);
    pCur += nValPhys;
  }

  if( writeCounts ){
    for(i=0; i<b->nItems; i++){
      u64 v = b->aSubtreeCount[i];
      pCur[0] = (u8)v;
      pCur[1] = (u8)(v >> 8);
      pCur[2] = (u8)(v >> 16);
      pCur[3] = (u8)(v >> 24);
      pCur[4] = (u8)(v >> 32);
      pCur[5] = (u8)(v >> 40);
      pCur[6] = (u8)(v >> 48);
      pCur[7] = (u8)(v >> 56);
      pCur += 8;
    }
  }

  assert( pCur==pBuf+nPhys );

  *ppOut = pBuf;
  *pnOut = nTotal;
  *pnZeroTail = b->nValZeroTail;
  return SQLITE_OK;
}

void prollyNodeBuilderReset(ProllyNodeBuilder *b){
  b->nItems = 0;
  b->nKeyBytes = 0;
  b->nValBytes = 0;
  b->nValZeroTail = 0;
}

void prollyNodeBuilderFree(ProllyNodeBuilder *b){
  sqlite3_free(b->aKeyOff);
  sqlite3_free(b->aValOff);
  sqlite3_free(b->pKeyBuf);
  sqlite3_free(b->pValBuf);
  sqlite3_free(b->aSubtreeCount);
  memset(b, 0, sizeof(*b));
}

int prollyCompareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
){
  if( flags & PROLLY_NODE_INTKEY ){
    if( iKey1 < iKey2 ) return -1;
    if( iKey1 > iKey2 ) return +1;
    return 0;
  }else{
    int n = nKey1 < nKey2 ? nKey1 : nKey2;
    int c = memcmp(pKey1, pKey2, n);
    if( c != 0 ) return c;
    if( nKey1 < nKey2 ) return -1;
    if( nKey1 > nKey2 ) return 1;
    return 0;
  }
}

#endif
