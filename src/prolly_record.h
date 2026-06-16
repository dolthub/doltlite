
#ifndef SQLITE_PROLLY_RECORD_H
#define SQLITE_PROLLY_RECORD_H

#include "sqliteInt.h"

/*
** Read a SQLite-style varint from p (bounded by pEnd) into *pVal.
** Returns the number of bytes consumed, or 0 on short input.
** A return of 0 is the sentinel for "truncated"; callers MUST check
** this to distinguish a parse failure from a legitimate single-byte 0.
*/
static inline int dlReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v;
  int i;
  if( p >= pEnd ){ *pVal = 0; return 0; }
  v = p[0];
  if( !(v & 0x80) ){ *pVal = v; return 1; }
  v &= 0x7f;
  for(i = 1; i < 8 && p+i < pEnd; i++){
    v = (v << 7) | (p[i] & 0x7f);
    if( !(p[i] & 0x80) ){ *pVal = v; return i + 1; }
  }
  if( i == 8 && p+i < pEnd ){
    v = (v << 8) | p[i];
    *pVal = v;
    return 9;
  }
  *pVal = 0;
  return 0;
}

static inline int dlSerialTypeLen(u64 st){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( st <= 6 ) return aLen[st];
  if( st == 7 ) return 8;
  if( st >= 12 ) return (int)(st - 12) / 2;
  return 0;
}

static inline int dlSerialIsInt(int st){
  return st>=1 && st<=9 && st!=7;
}

static inline i64 dlReadIntBytes(const u8 *p, int nBytes){
  /* Accumulate in unsigned: a left shift of a sign-extended negative i64 is
  ** undefined behavior. Two's-complement makes the unsigned result identical. */
  u64 v = (nBytes>0 && (p[0] & 0x80)) ? ~(u64)0 : 0;
  int i;
  for(i=0; i<nBytes; i++) v = (v<<8) | p[i];
  return (i64)v;
}

static inline i64 dlDecodeSerialInt(int st, const u8 *p, int n){
  if( st==9 ) return 1;
  if( st>=1 && st<=6 ){
    int nB = dlSerialTypeLen((u64)st);
    if( nB > n ) return 0;
    return dlReadIntBytes(p, nB);
  }
  return 0;
}

#define DOLTLITE_MAX_RECORD_FIELDS SQLITE_MAX_COLUMN

typedef struct DoltliteRecordInfo DoltliteRecordInfo;
struct DoltliteRecordInfo {
  int nField;
  int aType[DOLTLITE_MAX_RECORD_FIELDS];
  int aOffset[DOLTLITE_MAX_RECORD_FIELDS];
};

int doltliteParseRecordStrict(const u8 *pData, int nData,
                              DoltliteRecordInfo *pInfo);

void doltliteParseRecord(const u8 *pData, int nData, DoltliteRecordInfo *pInfo);

#endif
