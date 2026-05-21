
#ifndef SQLITE_SORTKEY_H
#define SQLITE_SORTKEY_H

#include "sqliteInt.h"
#include <string.h>

#define SORTKEY_NULL    0x05
#define SORTKEY_NUM     0x15
#define SORTKEY_TEXT    0x35
#define SORTKEY_BLOB    0x45

int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut);

int sortKeyFromRecordPrefix(const u8 *pRec, int nRec, int nKeyField,
                            u8 **ppOut, int *pnOut);

int sortKeyFromRecordPrefixColl(const u8 *pRec, int nRec, int nKeyField,
                                 const KeyInfo *pKeyInfo,
                                 u8 **ppOut, int *pnOut);
int sortKeyFromRecordPrefixCollBuffer(const u8 *pRec, int nRec, int nKeyField,
                                 const KeyInfo *pKeyInfo,
                                 u8 **ppBuf, int *pnAlloc, int *pnOut);
int sortKeyFromMemPrefixCollBuffer(
  Mem *aMem, int nMem, int nKeyField, const KeyInfo *pKeyInfo,
  u8 **ppBuf, int *pnAlloc, int *pnOut
);

int sortKeyFromInt64(i64 v, u8 *pOut, int *pnOut);
int sortKeyFromInt64Buffer(i64 v, u8 **ppBuf, int *pnAlloc, int *pnOut);

static inline int sortKeyInt64FitsExact(i64 v){
  return v>=-9007199254740992LL && v<=9007199254740992LL;
}

static inline void sortKeyWriteExactInt64(i64 v, u8 *pOut){
  double d = (double)v;
  u64 x;
  int i;
  pOut[0] = SORTKEY_NUM;
  memcpy(&x, &d, 8);
  if( x & ((u64)1 << 63) ){
    x = ~x;
  }else{
    x ^= ((u64)1 << 63);
  }
  for(i=0; i<8; i++){
    pOut[1+i] = (u8)(x >> (56 - i*8));
  }
}

int sortKeySize(const u8 *pRec, int nRec);

int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut);
int recordFromSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
);

static inline int compareSortKeys(
  const u8 *pKey1, int nKey1,
  const u8 *pKey2, int nKey2
){
  int n = nKey1 < nKey2 ? nKey1 : nKey2;
  int c = memcmp(pKey1, pKey2, n);
  if( c!=0 ) return c;
  if( nKey1 < nKey2 ) return -1;
  if( nKey1 > nKey2 ) return 1;
  return 0;
}

#endif
