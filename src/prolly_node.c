/*
** Prolly tree node: binary serialization and deserialization for prolly
** map nodes. Content-addressed immutable nodes for the Doltite storage engine.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_node.h"
#include <string.h>

/*
** Little-endian encoding helpers.
*/
#define PROLLY_GET_U32(p) \
  ((u32)(p)[0] | ((u32)(p)[1]<<8) | ((u32)(p)[2]<<16) | ((u32)(p)[3]<<24))
#define PROLLY_PUT_U32(p,v) do{ \
  (p)[0]=(u8)(v); (p)[1]=(u8)((v)>>8); \
  (p)[2]=(u8)((v)>>16); (p)[3]=(u8)((v)>>24); \
}while(0)
#define PROLLY_GET_U16(p) \
  ((u16)(p)[0] | ((u16)(p)[1]<<8))
#define PROLLY_PUT_U16(p,v) do{ \
  (p)[0]=(u8)(v); (p)[1]=(u8)((v)>>8); \
}while(0)

/*
** Header layout constants.
*/
#define PROLLY_HDR_SIZE       8   /* magic(4) + level(1) + count(2) + flags(1) */
#define PROLLY_MAGIC_OFF      0
#define PROLLY_LEVEL_OFF      4
#define PROLLY_COUNT_OFF      5
#define PROLLY_FLAGS_OFF      7

/*
** Parse a serialized prolly node from raw bytes.
**
** Layout:
**   [magic:4][level:1][count:2][flags:1]   = 8 bytes header
**   [keyOffsets: u32 * (count+1)]
**   [valOffsets: u32 * (count+1)]
**   [key data: variable]
**   [value data: variable]
**
** Returns SQLITE_OK on success, SQLITE_CORRUPT if the data is malformed.
*/
int prollyNodeParse(ProllyNode *pNode, const u8 *pData, int nData){
  u32 magic;
  u16 count;
  int nOffsets;        /* total bytes consumed by both offset arrays */
  int minSize;         /* minimum valid buffer size */
  u32 totalKeyBytes;   /* last key offset = total key bytes */
  u32 totalValBytes;   /* last val offset = total val bytes */
  const u8 *pCur;

  memset(pNode, 0, sizeof(*pNode));

  /* Need at least the header */
  if( nData<PROLLY_HDR_SIZE ){
    return SQLITE_CORRUPT;
  }

  /* Validate magic */
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
    /* Empty node: no offset arrays or data regions */
    pNode->aKeyOff = 0;
    pNode->aValOff = 0;
    pNode->pKeyData = pData + PROLLY_HDR_SIZE;
    pNode->pValData = pData + PROLLY_HDR_SIZE;
    return SQLITE_OK;
  }

  /* Each offset array has (count+1) u32 entries */
  nOffsets = (int)(count + 1) * 4 * 2;
  minSize = PROLLY_HDR_SIZE + nOffsets;
  if( nData<minSize ){
    return SQLITE_CORRUPT;
  }

  /* Point offset arrays into the buffer. The buffer is packed u32s in
  ** little-endian order. We cast directly since we read them through
  ** the PROLLY_GET_U32 macro when accessing individual entries, but the
  ** header stores them as a contiguous u32 array so we can also cast
  ** when aligned.  We store as const u32* for direct indexed access. */
  pCur = pData + PROLLY_HDR_SIZE;
  pNode->aKeyOff = (const u32*)pCur;
  pCur += (count + 1) * 4;
  pNode->aValOff = (const u32*)pCur;
  pCur += (count + 1) * 4;

  /* Key data starts right after the offset arrays */
  pNode->pKeyData = pCur;

  /* Validate that key and value regions fit in the buffer */
  totalKeyBytes = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[count]);
  pNode->pValData = pCur + totalKeyBytes;

  totalValBytes = PROLLY_GET_U32((const u8*)&pNode->aValOff[count]);
  if( totalKeyBytes > (u32)nData || totalValBytes > (u32)nData
   || minSize + (int)totalKeyBytes + (int)totalValBytes != nData ){
    return SQLITE_CORRUPT;
  }

  return SQLITE_OK;
}

/*
** Return a pointer to key i and its length.
*/
void prollyNodeKey(const ProllyNode *pNode, int i, const u8 **ppKey, int *pnKey){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off0 = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
  u32 off1 = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i+1]);
  *ppKey = pNode->pKeyData + off0;
  *pnKey = (int)(off1 - off0);
}

/*
** Return a pointer to value i and its length.
*/
void prollyNodeValue(const ProllyNode *pNode, int i, const u8 **ppVal, int *pnVal){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off0 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  u32 off1 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i+1]);
  *ppVal = pNode->pValData + off0;
  *pnVal = (int)(off1 - off0);
}

/*
** Read the integer key at index i. The key is stored as 8 bytes
** in little-endian format.
*/
i64 prollyNodeIntKey(const ProllyNode *pNode, int i){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
  const u8 *p = pNode->pKeyData + off;
  /* Decode big-endian with sign-bit flip (matches encodeI64BE) */
  u64 u = ((u64)p[0]<<56) | ((u64)p[1]<<48) | ((u64)p[2]<<40) | ((u64)p[3]<<32)
         | ((u64)p[4]<<24) | ((u64)p[5]<<16) | ((u64)p[6]<<8) | (u64)p[7];
  return (i64)(u ^ ((u64)1 << 63));  /* un-flip sign bit */
}

/*
** Copy the child hash at value position i into *pHash.
** Only valid for internal nodes (level > 0).
*/
void prollyNodeChildHash(const ProllyNode *pNode, int i, ProllyHash *pHash){
  assert( i >= 0 && i < (int)pNode->nItems );
  u32 off = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  memcpy(pHash->data, pNode->pValData + off, PROLLY_HASH_SIZE);
}

/*
** Binary search for a blob key within the node.
** NOTE: keep in sync with prollyNodeSearchInt below.
** Both are hot-path functions; a callback/flag indirection would hurt perf.
**
** Returns the index where the key was found or where it would be inserted.
** *pRes is set to:
**   0   if an exact match was found
**   <0  if the search key is less than the key at the returned index
**   >0  if the search key is greater than the key at the returned index
*/
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

    /* BLOBKEY nodes store sort keys — memcmp gives correct order.
    ** INTKEY fallback also uses memcmp on LE-encoded keys. */
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

  /* No exact match. lo is the insertion point. */
  if( lo>=pNode->nItems ){
    *pRes = 1;
    return pNode->nItems - 1;
  }else{
    *pRes = -1;
    return lo;
  }
}

/*
** Binary search for an integer key within the node.
** NOTE: keep in sync with prollyNodeSearchBlob above.
** Both are hot-path functions; a callback/flag indirection would hurt perf.
**
** Returns the index where the key was found or where it would be inserted.
** *pRes is set to:
**   0   if an exact match was found
**   <0  if the search key is less than the key at the returned index
**   >0  if the search key is greater than the key at the returned index
*/
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

  /* No exact match. lo is the insertion point. */
  if( lo>=pNode->nItems ){
    *pRes = 1;
    return pNode->nItems - 1;
  }else{
    *pRes = -1;
    return lo;
  }
}

/* -----------------------------------------------------------------------
** ProllyNodeBuilder: incremental construction of serialized nodes.
** -----------------------------------------------------------------------*/

/*
** Default initial capacity for offset arrays and data buffers.
*/
#define PROLLY_BUILDER_INIT_CAP  64
#define PROLLY_BUILDER_INIT_BUF  1024

/*
** Initialize a node builder for the given level and flags.
*/
void prollyNodeBuilderInit(ProllyNodeBuilder *b, u8 level, u8 flags){
  memset(b, 0, sizeof(*b));
  b->level = level;
  b->flags = flags;
}

/*
** Ensure the offset arrays can hold at least (nItems+1) entries.
** Both aKeyOff and aValOff grow together.
** Returns SQLITE_OK or SQLITE_NOMEM.
*/
static int builderGrowOffsets(ProllyNodeBuilder *b){
  int nNeeded = b->nItems + 2;  /* need count+1 entries after this add */
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

/*
** Ensure the key data buffer can hold nKeyBytes + nAdd bytes.
** Returns SQLITE_OK or SQLITE_NOMEM.
*/
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

/*
** Ensure the value data buffer can hold nValBytes + nAdd bytes.
** Returns SQLITE_OK or SQLITE_NOMEM.
*/
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

/*
** Add a key-value pair to the builder.
** Returns SQLITE_OK, SQLITE_NOMEM, or SQLITE_FULL if too many items.
*/
int prollyNodeBuilderAdd(
  ProllyNodeBuilder *b,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal
){
  int rc;

  if( b->nItems>=PROLLY_NODE_MAX_ITEMS ){
    return SQLITE_FULL;
  }

  /* Grow offset arrays if needed */
  rc = builderGrowOffsets(b);
  if( rc ) return rc;

  /* Grow data buffers */
  rc = builderGrowKeyBuf(b, nKey);
  if( rc ) return rc;
  rc = builderGrowValBuf(b, nVal);
  if( rc ) return rc;

  /* Record the starting offset for this key (first entry is 0 for item 0) */
  if( b->nItems==0 ){
    b->aKeyOff[0] = 0;
    b->aValOff[0] = 0;
  }

  /* Copy key data */
  memcpy(b->pKeyBuf + b->nKeyBytes, pKey, nKey);
  b->nKeyBytes += nKey;
  b->aKeyOff[b->nItems + 1] = (u32)b->nKeyBytes;

  /* Copy value data */
  memcpy(b->pValBuf + b->nValBytes, pVal, nVal);
  b->nValBytes += nVal;
  b->aValOff[b->nItems + 1] = (u32)b->nValBytes;

  b->nItems++;
  return SQLITE_OK;
}

/*
** Serialize the builder contents into a single allocated buffer.
** The caller must sqlite3_free(*ppOut) when done.
**
** Layout:
**   [magic:4][level:1][count:2][flags:1]
**   [keyOffsets: u32 * (count+1)]
**   [valOffsets: u32 * (count+1)]
**   [key data]
**   [value data]
**
** Returns SQLITE_OK or SQLITE_NOMEM.
*/
int prollyNodeBuilderFinish(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut){
  int nOff;         /* bytes for one offset array */
  int nTotal;       /* total serialized size */
  u8 *pBuf;
  u8 *pCur;
  int i;

  *ppOut = 0;
  *pnOut = 0;

  nOff = (b->nItems + 1) * 4;
  nTotal = PROLLY_HDR_SIZE + nOff * 2 + b->nKeyBytes + b->nValBytes;

  pBuf = (u8*)sqlite3_malloc(nTotal);
  if( !pBuf ) return SQLITE_NOMEM;

  /* Write header */
  PROLLY_PUT_U32(pBuf + PROLLY_MAGIC_OFF, PROLLY_NODE_MAGIC);
  pBuf[PROLLY_LEVEL_OFF] = b->level;
  PROLLY_PUT_U16(pBuf + PROLLY_COUNT_OFF, (u16)b->nItems);
  pBuf[PROLLY_FLAGS_OFF] = b->flags;

  pCur = pBuf + PROLLY_HDR_SIZE;

  /* Write key offsets in little-endian */
  for(i=0; i<=b->nItems; i++){
    PROLLY_PUT_U32(pCur, b->aKeyOff[i]);
    pCur += 4;
  }

  /* Write value offsets in little-endian */
  for(i=0; i<=b->nItems; i++){
    PROLLY_PUT_U32(pCur, b->aValOff[i]);
    pCur += 4;
  }

  /* Write key data */
  if( b->nKeyBytes>0 ){
    memcpy(pCur, b->pKeyBuf, b->nKeyBytes);
    pCur += b->nKeyBytes;
  }

  /* Write value data */
  if( b->nValBytes>0 ){
    memcpy(pCur, b->pValBuf, b->nValBytes);
    pCur += b->nValBytes;
  }

  assert( pCur==pBuf+nTotal );

  *ppOut = pBuf;
  *pnOut = nTotal;
  return SQLITE_OK;
}

/*
** Reset the builder for reuse. Keeps allocated buffers but zeros
** counts and sizes so the next build starts fresh.
*/
void prollyNodeBuilderReset(ProllyNodeBuilder *b){
  b->nItems = 0;
  b->nKeyBytes = 0;
  b->nValBytes = 0;
  /* nAlloc, nKeyBufAlloc, nValBufAlloc and pointers are preserved */
}

/*
** Free all resources held by the builder.
*/
void prollyNodeBuilderFree(ProllyNodeBuilder *b){
  sqlite3_free(b->aKeyOff);
  sqlite3_free(b->aValOff);
  sqlite3_free(b->pKeyBuf);
  sqlite3_free(b->pValBuf);
  memset(b, 0, sizeof(*b));
}

/*
** Compute the content-address hash of a serialized node.
*/
void prollyNodeComputeHash(const u8 *pData, int nData, ProllyHash *pOut){
  prollyHashCompute(pData, nData, pOut);
}

/*
** Compare two keys. For INTKEY tables, compare the i64 values directly.
** For BLOBKEY tables, use memcmp with length comparison (sort key encoding
** ensures memcmp gives correct order).
**
** Returns negative if key1 < key2, zero if equal, positive if key1 > key2.
*/
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

#endif /* DOLTLITE_PROLLY */
