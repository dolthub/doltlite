
#ifdef DOLTLITE_PROLLY

#include "prolly_node.h"
#include "prolly_encoding.h"
#include <string.h>

#define PROLLY_HDR_SIZE       8   
#define PROLLY_MAGIC_OFF      0
#define PROLLY_LEVEL_OFF      4
#define PROLLY_COUNT_OFF      5
#define PROLLY_FLAGS_OFF      7

int prollyNodeParse(ProllyNode *pNode, const u8 *pData, int nData){
  u32 magic;
  u16 count;
  int nOffsets;        
  int minSize;         
  u32 totalKeyBytes;   
  u32 totalValBytes;   
  const u8 *pCur;

  memset(pNode, 0, sizeof(*pNode));

  
  if( nData<PROLLY_HDR_SIZE ){
    return SQLITE_CORRUPT;
  }

  
  magic = PROLLY_GET_U32(pData + PROLLY_MAGIC_OFF);
  if( magic!=PROLLY_NODE_MAGIC ){
    return SQLITE_CORRUPT;
  }

  pNode->pData = pData;
  pNode->nData = nData;
  pNode->level = pData[PROLLY_LEVEL_OFF];
  count = PROLLY_GET_U16(pData + PROLLY_COUNT_OFF);
  pNode->nItems = count;
  pNode->flags = pData[PROLLY_FLAGS_OFF];

  if( count==0 ){
    
    pNode->aKeyOff = 0;
    pNode->aValOff = 0;
    pNode->pKeyData = pData + PROLLY_HDR_SIZE;
    pNode->pValData = pData + PROLLY_HDR_SIZE;
    return SQLITE_OK;
  }

  
  nOffsets = (int)(count + 1) * 4 * 2;
  minSize = PROLLY_HDR_SIZE + nOffsets;
  if( nData<minSize ){
    return SQLITE_CORRUPT;
  }

  
  pCur = pData + PROLLY_HDR_SIZE;
  pNode->aKeyOff = (const u32*)pCur;
  pCur += (count + 1) * 4;
  pNode->aValOff = (const u32*)pCur;
  pCur += (count + 1) * 4;

  
  pNode->pKeyData = pCur;

  
  totalKeyBytes = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[count]);
  pNode->pValData = pCur + totalKeyBytes;

  totalValBytes = PROLLY_GET_U32((const u8*)&pNode->aValOff[count]);
  if( totalKeyBytes > (u32)nData || totalValBytes > (u32)nData
   || minSize + (int)totalKeyBytes + (int)totalValBytes != nData ){
    return SQLITE_CORRUPT;
  }

  return SQLITE_OK;
}

void prollyNodeKey(const ProllyNode *pNode, int i, const u8 **ppKey, int *pnKey){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off0 = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
  u32 off1 = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i+1]);
  *ppKey = pNode->pKeyData + off0;
  *pnKey = (int)(off1 - off0);
}

void prollyNodeValue(const ProllyNode *pNode, int i, const u8 **ppVal, int *pnVal){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off0 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  u32 off1 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i+1]);
  *ppVal = pNode->pValData + off0;
  *pnVal = (int)(off1 - off0);
}

i64 prollyNodeIntKey(const ProllyNode *pNode, int i){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
  const u8 *p = pNode->pKeyData + off;
  
  u64 u = ((u64)p[0]<<56) | ((u64)p[1]<<48) | ((u64)p[2]<<40) | ((u64)p[3]<<32)
         | ((u64)p[4]<<24) | ((u64)p[5]<<16) | ((u64)p[6]<<8) | (u64)p[7];
  return (i64)(u ^ ((u64)1 << 63));  
}

void prollyNodeChildHash(const ProllyNode *pNode, int i, ProllyHash *pHash){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  memcpy(pHash->data, pNode->pValData + off, PROLLY_HASH_SIZE);
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
    c = memcmp(pKey, pMidKey, nCmp);
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
  int nNeeded = b->nItems + 2;  
  if( nNeeded>b->nAlloc ){
    int nNew = b->nAlloc ? b->nAlloc * 2 : PROLLY_BUILDER_INIT_CAP;
    u32 *aNew;
    while( nNew<nNeeded ) nNew *= 2;

    aNew = (u32*)sqlite3_realloc(b->aKeyOff, nNew * sizeof(u32));
    if( !aNew ) return SQLITE_NOMEM;
    b->aKeyOff = aNew;

    aNew = (u32*)sqlite3_realloc(b->aValOff, nNew * sizeof(u32));
    if( !aNew ) return SQLITE_NOMEM;
    b->aValOff = aNew;

    b->nAlloc = nNew;
  }
  return SQLITE_OK;
}

static int builderGrowKeyBuf(ProllyNodeBuilder *b, int nAdd){
  int nNeeded = b->nKeyBytes + nAdd;
  if( nNeeded>b->nKeyBufAlloc ){
    int nNew = b->nKeyBufAlloc ? b->nKeyBufAlloc * 2 : PROLLY_BUILDER_INIT_BUF;
    u8 *pNew;
    while( nNew<nNeeded ) nNew *= 2;
    pNew = (u8*)sqlite3_realloc(b->pKeyBuf, nNew);
    if( !pNew ) return SQLITE_NOMEM;
    b->pKeyBuf = pNew;
    b->nKeyBufAlloc = nNew;
  }
  return SQLITE_OK;
}

static int builderGrowValBuf(ProllyNodeBuilder *b, int nAdd){
  int nNeeded = b->nValBytes + nAdd;
  if( nNeeded>b->nValBufAlloc ){
    int nNew = b->nValBufAlloc ? b->nValBufAlloc * 2 : PROLLY_BUILDER_INIT_BUF;
    u8 *pNew;
    while( nNew<nNeeded ) nNew *= 2;
    pNew = (u8*)sqlite3_realloc(b->pValBuf, nNew);
    if( !pNew ) return SQLITE_NOMEM;
    b->pValBuf = pNew;
    b->nValBufAlloc = nNew;
  }
  return SQLITE_OK;
}

int prollyNodeBuilderAdd(
  ProllyNodeBuilder *b,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal
){
  int rc;

  if( b->nItems>=PROLLY_NODE_MAX_ITEMS ){
    return SQLITE_FULL;
  }

  
  rc = builderGrowOffsets(b);
  if( rc ) return rc;

  
  rc = builderGrowKeyBuf(b, nKey);
  if( rc ) return rc;
  rc = builderGrowValBuf(b, nVal);
  if( rc ) return rc;

  
  if( b->nItems==0 ){
    b->aKeyOff[0] = 0;
    b->aValOff[0] = 0;
  }

  
  memcpy(b->pKeyBuf + b->nKeyBytes, pKey, nKey);
  b->nKeyBytes += nKey;
  b->aKeyOff[b->nItems + 1] = (u32)b->nKeyBytes;

  
  memcpy(b->pValBuf + b->nValBytes, pVal, nVal);
  b->nValBytes += nVal;
  b->aValOff[b->nItems + 1] = (u32)b->nValBytes;

  b->nItems++;
  return SQLITE_OK;
}

int prollyNodeBuilderFinish(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut){
  int nOff;         
  int nTotal;       
  u8 *pBuf;
  u8 *pCur;
  int i;

  *ppOut = 0;
  *pnOut = 0;

  nOff = (b->nItems + 1) * 4;
  nTotal = PROLLY_HDR_SIZE + nOff * 2 + b->nKeyBytes + b->nValBytes;

  pBuf = (u8*)sqlite3_malloc(nTotal);
  if( !pBuf ) return SQLITE_NOMEM;

  
  PROLLY_PUT_U32(pBuf + PROLLY_MAGIC_OFF, PROLLY_NODE_MAGIC);
  pBuf[PROLLY_LEVEL_OFF] = b->level;
  PROLLY_PUT_U16(pBuf + PROLLY_COUNT_OFF, (u16)b->nItems);
  pBuf[PROLLY_FLAGS_OFF] = b->flags;

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
    memcpy(pCur, b->pValBuf, b->nValBytes);
    pCur += b->nValBytes;
  }

  assert( pCur==pBuf+nTotal );

  *ppOut = pBuf;
  *pnOut = nTotal;
  return SQLITE_OK;
}

void prollyNodeBuilderReset(ProllyNodeBuilder *b){
  b->nItems = 0;
  b->nKeyBytes = 0;
  b->nValBytes = 0;
  
}

void prollyNodeBuilderFree(ProllyNodeBuilder *b){
  sqlite3_free(b->aKeyOff);
  sqlite3_free(b->aValOff);
  sqlite3_free(b->pKeyBuf);
  sqlite3_free(b->pValBuf);
  memset(b, 0, sizeof(*b));
}

void prollyNodeComputeHash(const u8 *pData, int nData, ProllyHash *pOut){
  prollyHashCompute(pData, nData, pOut);
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
