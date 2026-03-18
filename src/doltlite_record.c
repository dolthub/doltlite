/*
** SQLite record format decoder.
**
** Record format:
**   [header_size: varint]
**   [serial_type_1: varint] [serial_type_2: varint] ...
**   [value_1] [value_2] ...
**
** Serial types:
**   0 = NULL
**   1 = 1-byte int
**   2 = 2-byte int (big-endian)
**   3 = 3-byte int
**   4 = 4-byte int
**   5 = 6-byte int
**   6 = 8-byte int
**   7 = 8-byte IEEE float
**   8 = integer 0
**   9 = integer 1
**   >=12, even = blob of (N-12)/2 bytes
**   >=13, odd  = text of (N-13)/2 bytes
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_record.h"
#include <string.h>
#include <stdio.h>

/* Read a big-endian SQLite varint. Returns bytes consumed. */
static int recReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v = 0;
  int i;
  for(i=0; i<9 && p+i<pEnd; i++){
    if( i<8 ){
      v = (v << 7) | (p[i] & 0x7f);
      if( (p[i] & 0x80)==0 ){ *pVal = v; return i+1; }
    }else{
      v = (v << 8) | p[i];
      *pVal = v;
      return 9;
    }
  }
  *pVal = v;
  return i ? i : 1;
}

/* Read a big-endian signed integer of nBytes bytes. */
static i64 recReadInt(const u8 *p, int nBytes){
  i64 v;
  int i;
  /* Sign extend from the first byte */
  v = (p[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++){
    v = (v << 8) | p[i];
  }
  return v;
}

/* Append text to a dynamically growing buffer. */
typedef struct RecBuf RecBuf;
struct RecBuf {
  char *z;
  int n;
  int nAlloc;
};

static void recBufAppend(RecBuf *b, const char *z, int n){
  if( n<0 ) n = (int)strlen(z);
  if( b->n + n + 1 > b->nAlloc ){
    int nNew = b->nAlloc ? b->nAlloc*2 : 128;
    while( nNew < b->n + n + 1 ) nNew *= 2;
    b->z = sqlite3_realloc(b->z, nNew);
    if( !b->z ){ b->nAlloc = 0; return; }
    b->nAlloc = nNew;
  }
  memcpy(b->z + b->n, z, n);
  b->n += n;
  b->z[b->n] = 0;
}

char *doltliteDecodeRecord(const u8 *pData, int nData){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  u64 hdrSize;
  int hdrBytes;
  const u8 *pHdrEnd;
  const u8 *pBody;
  RecBuf buf;
  int fieldIdx = 0;

  if( !pData || nData < 1 ) return 0;

  memset(&buf, 0, sizeof(buf));

  /* Read header size */
  hdrBytes = recReadVarint(p, pEnd, &hdrSize);
  p += hdrBytes;
  pHdrEnd = pData + hdrSize;
  pBody = pData + hdrSize;

  /* Walk serial types in header, decode each field from body */
  while( p < pHdrEnd && p < pEnd ){
    u64 st;
    int stBytes = recReadVarint(p, pHdrEnd, &st);
    p += stBytes;

    if( fieldIdx > 0 ) recBufAppend(&buf, "|", 1);

    if( st==0 ){
      /* NULL */
      recBufAppend(&buf, "NULL", 4);
    }else if( st==8 ){
      recBufAppend(&buf, "0", 1);
    }else if( st==9 ){
      recBufAppend(&buf, "1", 1);
    }else if( st>=1 && st<=6 ){
      /* Integer: 1,2,3,4,6,8 bytes */
      static const int sizes[] = {0,1,2,3,4,6,8};
      int nBytes = sizes[st];
      if( pBody + nBytes <= pEnd ){
        i64 v = recReadInt(pBody, nBytes);
        char tmp[32];
        sqlite3_snprintf(sizeof(tmp), tmp, "%lld", v);
        recBufAppend(&buf, tmp, -1);
      }
      pBody += nBytes;
    }else if( st==7 ){
      /* IEEE 754 float, 8 bytes big-endian */
      if( pBody + 8 <= pEnd ){
        double v;
        u64 bits = 0;
        int i;
        for(i=0; i<8; i++) bits = (bits<<8) | pBody[i];
        memcpy(&v, &bits, 8);
        char tmp[64];
        sqlite3_snprintf(sizeof(tmp), tmp, "%!.15g", v);
        recBufAppend(&buf, tmp, -1);
      }
      pBody += 8;
    }else if( st>=12 && (st&1)==0 ){
      /* Blob */
      int len = ((int)st - 12) / 2;
      recBufAppend(&buf, "x'", 2);
      if( pBody + len <= pEnd ){
        int i;
        for(i=0; i<len; i++){
          char hex[3];
          sqlite3_snprintf(3, hex, "%02x", pBody[i]);
          recBufAppend(&buf, hex, 2);
        }
      }
      recBufAppend(&buf, "'", 1);
      pBody += len;
    }else if( st>=13 && (st&1)==1 ){
      /* Text */
      int len = ((int)st - 13) / 2;
      if( pBody + len <= pEnd ){
        recBufAppend(&buf, (const char*)pBody, len);
      }
      pBody += len;
    }

    fieldIdx++;
  }

  return buf.z;
}

void doltliteResultRecord(sqlite3_context *ctx, const u8 *pData, int nData){
  if( !pData || nData<=0 ){
    sqlite3_result_null(ctx);
    return;
  }
  {
    char *z = doltliteDecodeRecord(pData, nData);
    if( z ){
      sqlite3_result_text(ctx, z, -1, sqlite3_free);
    }else{
      sqlite3_result_null(ctx);
    }
  }
}

#endif /* DOLTLITE_PROLLY */
