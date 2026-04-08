/* SQLite record-format decoding helpers and varint/serial-type utilities. */
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "sqliteInt.h"

char *doltliteDecodeRecord(const u8 *pData, int nData);

void doltliteResultRecord(sqlite3_context *ctx, const u8 *pData, int nData);

typedef struct DoltliteColInfo DoltliteColInfo;
struct DoltliteColInfo {
  char **azName;    
  int nCol;         
  int iPkCol;       
};

int doltliteGetColumnNames(sqlite3 *db, const char *zTable, DoltliteColInfo *ci);

void doltliteFreeColInfo(DoltliteColInfo *ci);

static inline int dlReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v;
  int i;
  if( p >= pEnd ){ *pVal = 0; return 0; }
  v = p[0];
  if( !(v & 0x80) ){ *pVal = v; return 1; }
  v &= 0x7f;
  for(i = 1; i < 9 && p+i < pEnd; i++){
    v = (v << 7) | (p[i] & 0x7f);
    if( !(p[i] & 0x80) ){ *pVal = v; return i + 1; }
  }
  *pVal = v;
  return i;
}

static inline int dlSerialTypeLen(u64 st){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( st <= 6 ) return aLen[st];
  if( st == 7 ) return 8;
  if( st >= 12 ) return (int)(st - 12) / 2;
  return 0;
}

#endif 
