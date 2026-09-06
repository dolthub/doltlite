
#ifndef SQLITE_SORTKEY_H
#define SQLITE_SORTKEY_H

#include "sqliteInt.h"
#include <string.h>

#define SORTKEY_NULL    0x05
#define SORTKEY_NUM     0x15
#define SORTKEY_TEXT    0x35
#define SORTKEY_BLOB    0x45

/* DESC 9-byte numeric terminator so it is not a prefix of the 18-byte form. */
#define SORTKEY_NUM_DESC_END 0x00

/* True if b can start a field in either direction (DESC inverts every byte).
** Prefix match is equality only on a field boundary. An inexact integer is
** 18 bytes, so the 9-byte IEEE base is a prefix of a smaller number. Non-tags
** are continuations so a later encoding fails onto the real comparator. */
static inline int sortKeyByteStartsField(u8 b){
  switch( b ){
    case SORTKEY_NULL:
    case SORTKEY_NUM:
    case SORTKEY_TEXT:
    case SORTKEY_BLOB:
    case (u8)~SORTKEY_NULL:
    case (u8)~SORTKEY_NUM:
    case (u8)~SORTKEY_TEXT:
    case (u8)~SORTKEY_BLOB:
      return 1;
  }
  return 0;
}

/* Smallest key above every key that starts with pKey; NULL when none exists. */
int sortKeyPrefixSuccessor(const u8 *pKey, int nKey, u8 **ppOut, int *pnOut);

int sortKeyFromRecordPrefixColl(const u8 *pRec, int nRec, int nKeyField,
                                 const KeyInfo *pKeyInfo,
                                 u8 **ppOut, int *pnOut);
static inline int sortKeyFromRecordPrefix(const u8 *pRec, int nRec, int nKeyField,
                            u8 **ppOut, int *pnOut){
  return sortKeyFromRecordPrefixColl(pRec, nRec, nKeyField, NULL, ppOut, pnOut);
}
static inline int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut){
  return sortKeyFromRecordPrefix(pRec, nRec, 0, ppOut, pnOut);
}
int sortKeyFromRecordPrefixCollBuffer(const u8 *pRec, int nRec, int nKeyField,
                                 const KeyInfo *pKeyInfo,
                                 u8 **ppBuf, int *pnAlloc, int *pnOut);
int sortKeyFromMemPrefixCollBuffer(
  Mem *aMem, int nMem, int nKeyField, const KeyInfo *pKeyInfo,
  u8 **ppBuf, int *pnAlloc, int *pnOut
);

int sortKeyRecordNeedsPayload(const u8 *pRec, int nRec, int nKeyField);

int sortKeyFromInt64(i64 v, u8 *pOut, int *pnOut);

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

int recordFromSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
);
static inline int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut){
  int nAlloc = 0;
  *ppOut = 0;
  return recordFromSortKeyBuffer(pSortKey, nSortKey, ppOut, &nAlloc, pnOut);
}
int recordFromSortKeyBufferColl(
  const u8 *pSortKey, int nSortKey, const KeyInfo *pKeyInfo,
  u8 **ppBuf, int *pnAlloc, int *pnOut
);

#endif
