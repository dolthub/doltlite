
#ifndef SQLITE_PROLLY_NODE_H
#define SQLITE_PROLLY_NODE_H

#include "sqliteInt.h"
#include "prolly_encoding.h"
#include "prolly_hash.h"

#define PROLLY_NODE_MAGIC 0x504E4F44

#define PROLLY_NODE_INTKEY        0x01
#define PROLLY_NODE_BLOBKEY       0x02
#define PROLLY_NODE_SUBTREE_COUNTS 0x04

#define PROLLY_NODE_MAX_ITEMS 4096

#define PROLLY_NODE_ENTRY_BYTES(level,nKey,nVal) \
  ((nKey) + (nVal) + 8 + ((level)>0 ? 8 : 0))

static inline int prollyKeyCmp(const u8 *pA, int nA, const u8 *pB, int nB){
  int n = nA < nB ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  return nA - nB;
}

typedef struct ProllyNode ProllyNode;
/* Offsets are little-endian; accessors decode them. aSubtreeCount is set
** only with PROLLY_NODE_SUBTREE_COUNTS. */
struct ProllyNode {
  const u8 *pData;
  int nData;
  int nDataPhys;
  u8 level;
  u16 nItems;
  u8 flags;
  const u32 *aKeyOff;
  const u32 *aValOff;
  const u8 *pKeyData;
  const u8 *pValData;
  const u8 *pSubtreeCounts;
};

int prollyNodeParse(ProllyNode *pNode, const u8 *pData, int nData);
int prollyNodeParseSparse(ProllyNode *pNode, const u8 *pData, int nData,
                          int nDataPhys);

void prollyNodeKey(const ProllyNode *pNode, int i, const u8 **ppKey, int *pnKey);

void prollyNodeValue(const ProllyNode *pNode, int i, const u8 **ppVal, int *pnVal);
static SQLITE_INLINE void prollyNodeValueSpanInline(
  const ProllyNode *pNode,
  int i,
  const u8 **ppVal,
  int *pnVal,
  int *pnAvail
){
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
void prollyNodeValueSpan(const ProllyNode *pNode, int i, const u8 **ppVal,
                         int *pnVal, int *pnAvail);

i64 prollyNodeIntKey(const ProllyNode *pNode, int i);

void prollyNodeChildHash(const ProllyNode *pNode, int i, ProllyHash *pHash);

int prollyNodeHasSubtreeCounts(const ProllyNode *pNode);

u64 prollyNodeChildSubtreeCount(const ProllyNode *pNode, int i);

int prollyNodeSearchBlob(const ProllyNode *pNode,
                         const u8 *pKey, int nKey, int *pRes);

int prollyNodeSearchInt(const ProllyNode *pNode, i64 intKey, int *pRes);

void prollyEncodeIntKey(i64 v, u8 buf[8]);

i64 prollyDecodeIntKey(const u8 *p);

typedef struct ProllyNodeBuilder ProllyNodeBuilder;
struct ProllyNodeBuilder {
  u8 level;
  u8 flags;
  int nItems;
  int nKeyBytes;
  int nValBytes;
  int nAlloc;
  u32 *aKeyOff;
  u32 *aValOff;
  u8 *pKeyBuf;
  int nKeyBufAlloc;
  u8 *pValBuf;
  int nValBufAlloc;
  i64 nValZeroTail;        /* trailing zeros of the last value, not stored in pValBuf */
  u64 *aSubtreeCount;
  int nSubtreeCountAlloc;
};

void prollyNodeBuilderInit(ProllyNodeBuilder *b, u8 level, u8 flags);

int prollyNodeBuilderAdd(ProllyNodeBuilder *b,
                         const u8 *pKey, int nKey,
                         const u8 *pVal, int nVal);

int prollyNodeBuilderAddWithCount(ProllyNodeBuilder *b,
                                  const u8 *pKey, int nKey,
                                  const u8 *pVal, int nVal,
                                  u64 subtreeCount);

int prollyNodeBuilderAddZeroTail(ProllyNodeBuilder *b,
                                 const u8 *pKey, int nKey,
                                 const u8 *pVal, int nValPrefix,
                                 i64 nZeroTail);

int prollyNodeBuilderFinish(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut);

int prollyNodeBuilderFinishSparse(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut,
                                  i64 *pnZeroTail);

void prollyNodeBuilderReset(ProllyNodeBuilder *b);

void prollyNodeBuilderFree(ProllyNodeBuilder *b);

int prollyCompareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
);

#endif
