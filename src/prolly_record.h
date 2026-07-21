
#ifndef SQLITE_PROLLY_RECORD_H
#define SQLITE_PROLLY_RECORD_H

#include <limits.h>
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
  u64 n;
  if( st <= 6 ) return aLen[st];
  if( st == 7 ) return 8;
  if( st >= 12 ){
    n = (st - 12) / 2;
    if( n > (u64)INT_MAX ) return -1;
    return (int)n;
  }
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

/* Decode field iField of a parsed record as text into a freshly sqlite3_malloc'd
** NUL-terminated string (*pzOut, NULL for a SQL NULL field). Returns SQLITE_OK,
** SQLITE_CORRUPT on a malformed/non-text serial type, or SQLITE_NOMEM. */
static inline int dlRecordTextField(
  const u8 *pVal,
  int nVal,
  const DoltliteRecordInfo *pRi,
  int iField,
  char **pzOut
){
  int st, off, len;
  char *zOut;

  *pzOut = 0;
  if( iField>=pRi->nField ) return SQLITE_CORRUPT;
  st = pRi->aType[iField];
  off = pRi->aOffset[iField];
  if( st==0 ) return SQLITE_OK;
  if( st<13 || (st&1)==0 ) return SQLITE_CORRUPT;
  len = (st-13)/2;
  if( off<0 || off+len>nVal ) return SQLITE_CORRUPT;
  zOut = sqlite3_malloc(len+1);
  if( !zOut ) return SQLITE_NOMEM;
  memcpy(zOut, pVal+off, len);
  zOut[len] = 0;
  *pzOut = zOut;
  return SQLITE_OK;
}

/* Decode field iField of a parsed record as a signed integer. Returns 0 for a
** NULL, non-integer, or out-of-bounds field (serial type 8 is the constant 0,
** type 9 the constant 1). */
static inline i64 dlRecordIntField(
  const u8 *pVal,
  int nVal,
  const DoltliteRecordInfo *pRi,
  int iField
){
  const u8 *pBody;
  int st, off, nByte, i;
  i64 v;

  if( iField>=pRi->nField ) return 0;
  st = pRi->aType[iField];
  off = pRi->aOffset[iField];
  switch( st ){
    case 0:
    case 8: return 0;
    case 9: return 1;
    case 1: nByte = 1; break;
    case 2: nByte = 2; break;
    case 3: nByte = 3; break;
    case 4: nByte = 4; break;
    case 5: nByte = 6; break;
    case 6: nByte = 8; break;
    default: return 0;
  }
  if( off<0 || off+nByte>nVal ) return 0;
  pBody = pVal + off;
  v = (pBody[0] & 0x80) ? -1 : 0;
  for(i=0; i<nByte; i++){
    v = (v << 8) | pBody[i];
  }
  return v;
}

#endif
