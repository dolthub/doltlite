
#ifdef DOLTLITE_PROLLY

#include "sortkey.h"
#include "vdbeInt.h"
#include <limits.h>
#include <string.h>

static int skGetVarint32(const u8 *p, u32 *pVal){
  u32 a;
  a = *p;
  if( !(a & 0x80) ){
    *pVal = a;
    return 1;
  }

  {
    u32 v = a & 0x7f;
    int i = 1;
    do {
      a = p[i];
      v = (v << 7) | (a & 0x7f);
      i++;
    } while( (a & 0x80) && i < 9 );
    *pVal = v;
    return i;
  }
}

static u32 serialTypeLen(u32 serialType){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( serialType <= 6 ) return aLen[serialType];
  if( serialType == 7 ) return 8;
  if( serialType >= 12 ) return (serialType - 12) / 2;
  return 0;
}

static u8 serialTypeTag(u32 serialType){
  if( serialType == 0 ) return SORTKEY_NULL;
  if( serialType <= 6 || serialType == 8 || serialType == 9 ){
    return SORTKEY_NUM;
  }
  if( serialType == 7 ) return SORTKEY_NUM;
  if( serialType >= 13 && (serialType & 1) ) return SORTKEY_TEXT;
  if( serialType >= 12 && !(serialType & 1) ) return SORTKEY_BLOB;
  return SORTKEY_NULL;
}

#define SORTKEY_COLL_BINARY 0
#define SORTKEY_COLL_NOCASE 1
#define SORTKEY_COLL_RTRIM  2

static void intSerialType(i64 v, u32 *pType, u32 *pLen);
static void writeIntBE(u8 *p, i64 v, int nByte);

static int collFromKeyInfo(const KeyInfo *pKeyInfo, int iCol){
  const CollSeq *pColl;
  if( !pKeyInfo ) return SORTKEY_COLL_BINARY;
  if( iCol < 0 || iCol >= pKeyInfo->nAllField ) return SORTKEY_COLL_BINARY;
  pColl = pKeyInfo->aColl[iCol];
  if( !pColl || !pColl->zName ) return SORTKEY_COLL_BINARY;
  if( sqlite3StrICmp(pColl->zName, "NOCASE")==0 ) return SORTKEY_COLL_NOCASE;
  if( sqlite3StrICmp(pColl->zName, "RTRIM")==0 ) return SORTKEY_COLL_RTRIM;
  return SORTKEY_COLL_BINARY;
}

static u32 collTextLen(int coll, const u8 *pData, u32 nData){
  if( coll==SORTKEY_COLL_RTRIM ){
    while( nData > 0 && pData[nData - 1] == 0x20 ) nData--;
  }
  return nData;
}

static sqlite3_int64 encodedFieldSize(
  u32 serialType,
  const u8 *pData,
  u32 nData,
  int coll
){
  u8 tag = serialTypeTag(serialType);
  switch( tag ){
    case SORTKEY_NULL:
      return 1;
    case SORTKEY_NUM:
      if( serialType <= 6 ){
        u64 uv = (pData[0] & 0x80) ? (u64)-1 : 0;
        i64 v;
        double d;
        i64 back;
        int exact;
        u32 i;
        if( serialType == 8 || serialType == 9 ) return 9;
        for(i = 0; i < nData; i++){
          uv = (uv << 8) | pData[i];
        }
        v = (i64)uv;
        d = (double)v;
        exact = d>=-9223372036854775808.0
             && d<9223372036854775808.0
             && (back = (i64)d)==v;
        return exact ? 9 : 18;
      }
      return 9;
    case SORTKEY_TEXT: {
      u32 n = collTextLen(coll, pData, nData);
      sqlite3_int64 extra = 0;
      u32 i;
      for(i = 0; i < n; i++){
        u8 b = pData[i];
        if( coll==SORTKEY_COLL_NOCASE && b >= 'A' && b <= 'Z' ) b += 32;
        if( b == 0x00 ) extra++;
      }
      return 1 + (sqlite3_int64)n + extra + 2;
    }
    case SORTKEY_BLOB: {
      sqlite3_int64 extra = 0;
      u32 i;
      for(i = 0; i < nData; i++){
        if( pData[i] == 0x00 ) extra++;
      }
      return 1 + (sqlite3_int64)nData + extra + 2;
    }
    default:
      return 1;
  }
}

static int encodeNumeric(u8 *pOut, u32 serialType, const u8 *pData, u32 nData){
  u8 buf[8];
  double d;
  i64 intVal = 0;
  int isInt = 0;
  int exactInt = 1;
  int doubleAboveInt = 0;

  pOut[0] = SORTKEY_NUM;

  /* Convert SQLite numeric serial types to sortable IEEE bytes: positive
  ** values flip the sign bit, negative values invert all bits. */
  if( serialType == 7 ){

    memcpy(buf, pData, 8);
  }else{

    i64 v;
    u64 x;
    if( serialType == 8 ){
      v = 0;
    }else if( serialType == 9 ){
      v = 1;
    }else{
      u64 uv = (pData[0] & 0x80) ? (u64)-1 : 0;
      for(u32 i = 0; i < nData; i++){
        uv = (uv << 8) | pData[i];
      }
      v = (i64)uv;
    }
    isInt = 1;
    intVal = v;
    d = (double)v;
    exactInt = d>=-9223372036854775808.0
            && d<9223372036854775808.0
            && ((i64)d)==v;
    if( !exactInt ){
      if( d>=9223372036854775808.0 ){
        doubleAboveInt = 1;
      }else if( d>=-9223372036854775808.0 ){
        doubleAboveInt = ((i64)d)>v;
      }
    }

    memcpy(&x, &d, 8);
    if( doubleAboveInt ){
      if( x & ((u64)1 << 63) ){
        x++;
      }else{
        x--;
      }
    }
    buf[0] = (u8)(x >> 56); buf[1] = (u8)(x >> 48);
    buf[2] = (u8)(x >> 40); buf[3] = (u8)(x >> 32);
    buf[4] = (u8)(x >> 24); buf[5] = (u8)(x >> 16);
    buf[6] = (u8)(x >> 8);  buf[7] = (u8)(x);
  }

  if( buf[0] & 0x80 ){

    for(int i = 0; i < 8; i++) buf[i] = ~buf[i];
  }else{

    buf[0] ^= 0x80;
  }

  memcpy(pOut + 1, buf, 8);
  if( isInt && !exactInt ){
    u64 u = ((u64)intVal) ^ ((u64)1 << 63);
    pOut[9] = 0x80;
    pOut[10] = (u8)(u >> 56);
    pOut[11] = (u8)(u >> 48);
    pOut[12] = (u8)(u >> 40);
    pOut[13] = (u8)(u >> 32);
    pOut[14] = (u8)(u >> 24);
    pOut[15] = (u8)(u >> 16);
    pOut[16] = (u8)(u >> 8);
    pOut[17] = (u8)u;
    return 18;
  }
  return 9;
}

static int numericSortKeyLen(const u8 *pSortKey, int nAvail){
  if( nAvail<9 || pSortKey[0]!=SORTKEY_NUM ) return 0;
  if( nAvail>=18 && (pSortKey[9]==0x01 || pSortKey[9]==0x80) ){
    return 18;
  }
  return 9;
}

static int encodeVarLen(u8 *pOut, u8 tag, const u8 *pData, u32 nData){
  int pos = 0;
  pOut[pos++] = tag;

  for(u32 i = 0; i < nData; i++){
    if( pData[i] == 0x00 ){
      pOut[pos++] = 0x00;
      pOut[pos++] = 0x01;
    }else{
      pOut[pos++] = pData[i];
    }
  }

  pOut[pos++] = 0x00;
  pOut[pos++] = 0x00;

  return pos;
}

static int encodeText(u8 *pOut, const u8 *pData, u32 nData, int coll){
  int pos = 0;
  u32 n = collTextLen(coll, pData, nData);
  pOut[pos++] = SORTKEY_TEXT;
  for(u32 i = 0; i < n; i++){
    u8 b = pData[i];
    if( coll==SORTKEY_COLL_NOCASE && b >= 'A' && b <= 'Z' ) b += 32;
    if( b == 0x00 ){
      pOut[pos++] = 0x00;
      pOut[pos++] = 0x01;
    }else{
      pOut[pos++] = b;
    }
  }
  pOut[pos++] = 0x00;
  pOut[pos++] = 0x00;
  return pos;
}

static int sortKeyEncode(const u8 *pRec, int nRec, u8 *pOut, int nMaxFields,
                         const KeyInfo *pKeyInfo){
  u32 hdrSize;
  u32 hdrOff;
  u32 dataOff;
  int outPos = 0;
  sqlite3_int64 outSize = 0;
  int nField = 0;

  if( nRec <= 0 ) return -1;

  hdrOff = skGetVarint32(pRec, &hdrSize);
  if( hdrSize > (u32)nRec ) return -1;
  dataOff = hdrSize;

  while( hdrOff < hdrSize ){
    u32 serialType;
    u32 fieldLen;
    const u8 *pField;
    int coll;

    if( nMaxFields > 0 && nField >= nMaxFields ) break;

    hdrOff += skGetVarint32(pRec + hdrOff, &serialType);
    fieldLen = serialTypeLen(serialType);

    if( fieldLen > (u32)nRec - dataOff ) return -1;
    pField = pRec + dataOff;
    coll = collFromKeyInfo(pKeyInfo, nField);

    if( pOut ){
      u8 tag = serialTypeTag(serialType);
      switch( tag ){
        case SORTKEY_NULL:
          pOut[outPos++] = SORTKEY_NULL;
          break;
        case SORTKEY_NUM:
          outPos += encodeNumeric(pOut + outPos, serialType, pField, fieldLen);
          break;
        case SORTKEY_TEXT:
          outPos += encodeText(pOut + outPos, pField, fieldLen, coll);
          break;
        case SORTKEY_BLOB:
          outPos += encodeVarLen(pOut + outPos, tag, pField, fieldLen);
          break;
        default:
          pOut[outPos++] = SORTKEY_NULL;
          break;
      }
    }else{
      sqlite3_int64 nFieldSize =
          encodedFieldSize(serialType, pField, fieldLen, coll);
      if( nFieldSize > INT_MAX || outSize > INT_MAX - nFieldSize ){
        return -2;
      }
      outSize += nFieldSize;
    }

    dataOff += fieldLen;
    nField++;
  }

  return pOut ? outPos : (int)outSize;
}

int sortKeySize(const u8 *pRec, int nRec){
  return sortKeyEncode(pRec, nRec, NULL, 0, NULL);
}

int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut){
  return sortKeyFromRecordPrefix(pRec, nRec, 0, ppOut, pnOut);
}

int sortKeyFromRecordPrefix(
  const u8 *pRec, int nRec, int nKeyField, u8 **ppOut, int *pnOut
){
  return sortKeyFromRecordPrefixColl(pRec, nRec, nKeyField, NULL, ppOut, pnOut);
}

int sortKeyFromRecordPrefixCollBuffer(
  const u8 *pRec, int nRec, int nKeyField, const KeyInfo *pKeyInfo,
  u8 **ppBuf, int *pnAlloc, int *pnOut
){
  int nSize;
  int nAlloc;

  *pnOut = 0;

  nSize = sortKeyEncode(pRec, nRec, 0, nKeyField, pKeyInfo);
  if( nSize == -2 ) return SQLITE_TOOBIG;
  if( nSize < 0 ) return SQLITE_CORRUPT;

  nAlloc = nSize < 64 ? 64 : nSize;
  if( *pnAlloc < nAlloc ){
    u8 *pNew = (u8*)sqlite3_realloc64(*ppBuf, (sqlite3_uint64)nAlloc);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nAlloc;
  }

  if( nSize != sortKeyEncode(pRec, nRec, *ppBuf, nKeyField, pKeyInfo) ){
    return SQLITE_CORRUPT;
  }
  *pnOut = nSize;
  return SQLITE_OK;
}

static int sortKeyMemSerialType(Mem *pMem, u32 *pSerialType, u32 *pLen){
  int flags = pMem->flags;
  if( flags & MEM_Null ){
    *pSerialType = 0;
    *pLen = 0;
    return SQLITE_OK;
  }
  if( flags & MEM_IntReal ){
    return SQLITE_NOTFOUND;
  }
  if( flags & MEM_Int ){
    intSerialType(pMem->u.i, pSerialType, pLen);
    return SQLITE_OK;
  }
  if( flags & MEM_Real ){
    *pSerialType = 7;
    *pLen = 8;
    return SQLITE_OK;
  }
  if( flags & (MEM_Str|MEM_Blob) ){
    if( flags & MEM_Zero ){
      return SQLITE_NOTFOUND;
    }
    if( pMem->n < 0 || (pMem->n > 0 && pMem->z==0) ){
      return SQLITE_CORRUPT;
    }
    *pLen = (u32)pMem->n;
    *pSerialType = (*pLen * 2) + 12 + ((flags & MEM_Str)!=0);
    return SQLITE_OK;
  }
  return SQLITE_NOTFOUND;
}

static int sortKeyEncodeMemArray(
  Mem *aMem,
  int nMem,
  int nKeyField,
  const KeyInfo *pKeyInfo,
  u8 *pOut
){
  int nField = nKeyField > 0 && nKeyField < nMem ? nKeyField : nMem;
  sqlite3_int64 outSize = 0;
  int outPos = 0;
  int i;

  if( nField < 0 ) return -1;
  for(i = 0; i < nField; i++){
    Mem *pMem = &aMem[i];
    u32 serialType;
    u32 fieldLen;
    u8 aNum[8];
    const u8 *pField = (const u8*)pMem->z;
    int coll;
    int rc = sortKeyMemSerialType(pMem, &serialType, &fieldLen);
    if( rc==SQLITE_NOTFOUND ) return -3;
    if( rc!=SQLITE_OK ) return -1;

    if( serialType <= 6 || serialType==8 || serialType==9 ){
      writeIntBE(aNum, pMem->u.i, (int)fieldLen);
      pField = aNum;
    }else if( serialType==7 ){
      u64 v;
      memcpy(&v, &pMem->u.r, 8);
      aNum[0] = (u8)(v >> 56); aNum[1] = (u8)(v >> 48);
      aNum[2] = (u8)(v >> 40); aNum[3] = (u8)(v >> 32);
      aNum[4] = (u8)(v >> 24); aNum[5] = (u8)(v >> 16);
      aNum[6] = (u8)(v >> 8);  aNum[7] = (u8)v;
      pField = aNum;
    }

    coll = collFromKeyInfo(pKeyInfo, i);
    if( pOut ){
      u8 tag = serialTypeTag(serialType);
      switch( tag ){
        case SORTKEY_NULL:
          pOut[outPos++] = SORTKEY_NULL;
          break;
        case SORTKEY_NUM:
          outPos += encodeNumeric(pOut + outPos, serialType, pField, fieldLen);
          break;
        case SORTKEY_TEXT:
          outPos += encodeText(pOut + outPos, pField, fieldLen, coll);
          break;
        case SORTKEY_BLOB:
          outPos += encodeVarLen(pOut + outPos, tag, pField, fieldLen);
          break;
        default:
          pOut[outPos++] = SORTKEY_NULL;
          break;
      }
    }else{
      sqlite3_int64 nFieldSize =
          encodedFieldSize(serialType, pField, fieldLen, coll);
      if( nFieldSize > INT_MAX || outSize > INT_MAX - nFieldSize ){
        return -2;
      }
      outSize += nFieldSize;
    }
  }

  return pOut ? outPos : (int)outSize;
}

int sortKeyFromMemPrefixCollBuffer(
  Mem *aMem, int nMem, int nKeyField, const KeyInfo *pKeyInfo,
  u8 **ppBuf, int *pnAlloc, int *pnOut
){
  int nSize;
  int nAlloc;

  *pnOut = 0;
  if( !aMem || nMem<=0 ) return SQLITE_NOTFOUND;

  nSize = sortKeyEncodeMemArray(aMem, nMem, nKeyField, pKeyInfo, 0);
  if( nSize == -3 ) return SQLITE_NOTFOUND;
  if( nSize == -2 ) return SQLITE_TOOBIG;
  if( nSize < 0 ) return SQLITE_CORRUPT;

  nAlloc = nSize < 64 ? 64 : nSize;
  if( *pnAlloc < nAlloc ){
    u8 *pNew = (u8*)sqlite3_realloc64(*ppBuf, (sqlite3_uint64)nAlloc);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nAlloc;
  }

  if( nSize != sortKeyEncodeMemArray(aMem, nMem, nKeyField, pKeyInfo, *ppBuf) ){
    return SQLITE_CORRUPT;
  }
  *pnOut = nSize;
  return SQLITE_OK;
}

int sortKeyFromRecordPrefixColl(
  const u8 *pRec, int nRec, int nKeyField, const KeyInfo *pKeyInfo,
  u8 **ppOut, int *pnOut
){
  *ppOut = 0;
  return sortKeyFromRecordPrefixCollBuffer(
      pRec, nRec, nKeyField, pKeyInfo, ppOut, &(int){0}, pnOut);
}

int sortKeyFromInt64(i64 v, u8 *pOut, int *pnOut){
  u8 aData[8];
  u32 serialType;
  u32 nData;

  intSerialType(v, &serialType, &nData);
  writeIntBE(aData, v, (int)nData);
  *pnOut = encodeNumeric(pOut, serialType, aData, nData);
  return SQLITE_OK;
}

int sortKeyFromInt64Buffer(i64 v, u8 **ppBuf, int *pnAlloc, int *pnOut){
  if( *pnAlloc < 64 ){
    u8 *pNew = (u8*)sqlite3_realloc64(*ppBuf, 64);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = 64;
  }
  return sortKeyFromInt64(v, *ppBuf, pnOut);
}

static void intSerialType(i64 v, u32 *pType, u32 *pLen){
  if( v==0 ){ *pType = 8; *pLen = 0; return; }
  if( v==1 ){ *pType = 9; *pLen = 0; return; }
  if( v>=-128 && v<=127 ){ *pType = 1; *pLen = 1; return; }
  if( v>=-32768 && v<=32767 ){ *pType = 2; *pLen = 2; return; }
  if( v>=-8388608 && v<=8388607 ){ *pType = 3; *pLen = 3; return; }
  if( v>=-2147483648LL && v<=2147483647LL ){ *pType = 4; *pLen = 4; return; }
  if( v>=-140737488355328LL && v<=140737488355327LL ){ *pType = 5; *pLen = 6; return; }
  *pType = 6; *pLen = 8;
}

static void writeIntBE(u8 *p, i64 v, int nByte){
  int j;
  for(j = nByte - 1; j >= 0; j--){
    p[j] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

static SQLITE_INLINE void decodeNumericSortKeyToRecord(
  const u8 *pIn,
  int nIn,
  u32 *pType,
  u32 *pLen,
  u8 *pOut
){
  u8 buf[8];
  double d;
  u64 x;
  int i;

  if( nIn>=17 && (pIn[8]==0x01 || pIn[8]==0x80) ){
    u64 u = ((u64)pIn[9] << 56) | ((u64)pIn[10] << 48)
          | ((u64)pIn[11] << 40) | ((u64)pIn[12] << 32)
          | ((u64)pIn[13] << 24) | ((u64)pIn[14] << 16)
          | ((u64)pIn[15] << 8)  | (u64)pIn[16];
    i64 iv = (i64)(u ^ ((u64)1 << 63));
    intSerialType(iv, pType, pLen);
    writeIntBE(pOut, iv, (int)*pLen);
    return;
  }

  memcpy(buf, pIn, 8);
  if( buf[0] & 0x80 ){
    buf[0] ^= 0x80;
  }else{
    for(i = 0; i < 8; i++) buf[i] = ~buf[i];
  }

  x = ((u64)buf[0] << 56) | ((u64)buf[1] << 48)
    | ((u64)buf[2] << 40) | ((u64)buf[3] << 32)
    | ((u64)buf[4] << 24) | ((u64)buf[5] << 16)
    | ((u64)buf[6] << 8)  | (u64)buf[7];
  memcpy(&d, &x, 8);

  {
    i64 iv = (i64)d;
    if( (double)iv == d && d >= -9.22e18 && d <= 9.22e18 ){
      intSerialType(iv, pType, pLen);
      writeIntBE(pOut, iv, (int)*pLen);
    }else{
      *pType = 7;
      *pLen = 8;
      memcpy(pOut, buf, 8);
    }
  }
}

static int recordFromAllNumericSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
){
  u32 aType[8];
  u32 aLen[8];
  u8 aBuf[8][8];
  int nField;
  int nHdr;
  int nData;
  int nTotal;
  int off;
  int i;
  u8 *pOut;

  if( nSortKey<=0 || (nSortKey % 9)!=0 ){
    return SQLITE_NOTFOUND;
  }
  nField = nSortKey / 9;
  if( nField > 8 ) return SQLITE_NOTFOUND;

  nHdr = 1 + nField;
  nData = 0;
  for(i = 0; i < nField; i++){
    int pos = i * 9;
    if( pSortKey[pos]!=SORTKEY_NUM ){
      return SQLITE_NOTFOUND;
    }
    decodeNumericSortKeyToRecord(pSortKey + pos + 1, 8,
                                 &aType[i], &aLen[i], aBuf[i]);
    nData += (int)aLen[i];
  }

  nTotal = nHdr + nData;
  if( *pnAlloc < nTotal ){
    u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, nTotal);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nTotal;
  }
  pOut = *ppBuf;

  pOut[0] = (u8)nHdr;
  for(i = 0; i < nField; i++){
    pOut[1 + i] = (u8)aType[i];
  }
  off = nHdr;
  for(i = 0; i < nField; i++){
    if( aLen[i]>0 ){
      memcpy(pOut + off, aBuf[i], aLen[i]);
      off += (int)aLen[i];
    }
  }

  *pnOut = nTotal;
  return SQLITE_OK;
}

static int recordFromNumericVarlenSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
){
  u32 aType0;
  u32 aLen0;
  u32 aType1;
  u32 aLen1;
  u8 aBuf0[8];
  u8 tag1;
  const u8 *pData1;
  int nEnc1;
  int hasEscape = 0;
  int nHdr;
  int nTotal;
  int off;
  u8 *pOut;
  int pos;

  if( nSortKey<12
   || pSortKey[0]!=SORTKEY_NUM
   || numericSortKeyLen(pSortKey, nSortKey)!=9
   || pSortKey[nSortKey-2]!=0x00
   || pSortKey[nSortKey-1]!=0x00 ){
    return SQLITE_NOTFOUND;
  }
  tag1 = pSortKey[9];
  if( tag1!=SORTKEY_TEXT && tag1!=SORTKEY_BLOB ){
    return SQLITE_NOTFOUND;
  }

  pData1 = pSortKey + 10;
  aLen1 = 0;
  {
    const u8 *pZero = (const u8*)memchr(pData1, 0x00,
                                        (size_t)(nSortKey - 10));
    if( pZero==0 ) return SQLITE_NOTFOUND;
    if( pZero==pSortKey + nSortKey - 2 ){
      nEnc1 = (int)(pZero - pData1);
      aLen1 = (u32)nEnc1;
      pos = nSortKey;
    }else{
      pos = 10;
      while( pos < nSortKey ){
        u8 b = pSortKey[pos++];
        if( b==0x00 ){
          if( pos >= nSortKey ) return SQLITE_NOTFOUND;
          if( pSortKey[pos]==0x00 ){
            nEnc1 = pos - 1 - 10;
            pos++;
            break;
          }
          if( pSortKey[pos]!=0x01 ){
            return SQLITE_NOTFOUND;
          }
          hasEscape = 1;
          pos++;
        }
        aLen1++;
      }
    }
  }
  if( pos!=nSortKey ){
    return SQLITE_NOTFOUND;
  }

  decodeNumericSortKeyToRecord(pSortKey + 1, 8, &aType0, &aLen0, aBuf0);
  aType1 = aLen1 * 2 + (tag1==SORTKEY_TEXT ? 13 : 12);

  nHdr = 1 + sqlite3VarintLen(aType0) + sqlite3VarintLen(aType1);
  if( nHdr > 126 ) nHdr++;
  nTotal = nHdr + (int)aLen0 + (int)aLen1;
  if( *pnAlloc < nTotal ){
    u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, nTotal);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nTotal;
  }
  pOut = *ppBuf;

  off = putVarint32(pOut, (u32)nHdr);
  off += putVarint32(pOut + off, aType0);
  off += putVarint32(pOut + off, aType1);
  if( aLen0>0 ){
    memcpy(pOut + off, aBuf0, aLen0);
    off += (int)aLen0;
  }
  if( aLen1>0 ){
    if( !hasEscape ){
      memcpy(pOut + off, pData1, aLen1);
    }else{
      const u8 *pSrc = pData1;
      const u8 *pEnd = pData1 + nEnc1;
      u8 *pDst = pOut + off;
      while( pSrc < pEnd ){
        if( pSrc+1 < pEnd && pSrc[0]==0x00 && pSrc[1]==0x01 ){
          *pDst++ = 0x00;
          pSrc += 2;
        }else{
          *pDst++ = *pSrc++;
        }
      }
    }
  }

  *pnOut = nTotal;
  return SQLITE_OK;
}

#define SORTKEY_SMALL_BLOB_FAST_MAX 64

static int recordFromNumericSmallBlobSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
){
  u32 aType0;
  u32 aLen0;
  u32 aType1;
  u8 aBuf0[8];
  u8 aBlob[SORTKEY_SMALL_BLOB_FAST_MAX];
  int pos;
  int nBlob = 0;
  int nHdr;
  int nTotal;
  int off;
  u8 *pOut;

  if( nSortKey<12
   || pSortKey[0]!=SORTKEY_NUM
   || numericSortKeyLen(pSortKey, nSortKey)!=9
   || pSortKey[9]!=SORTKEY_BLOB ){
    return SQLITE_NOTFOUND;
  }

  pos = 10;
  while( pos < nSortKey ){
    u8 b = pSortKey[pos++];
    if( b==0x00 ){
      if( pos >= nSortKey ) return SQLITE_NOTFOUND;
      if( pSortKey[pos]==0x00 ){
        pos++;
        break;
      }
      if( pSortKey[pos]!=0x01 ) return SQLITE_NOTFOUND;
      pos++;
      b = 0x00;
    }
    if( nBlob >= SORTKEY_SMALL_BLOB_FAST_MAX ) return SQLITE_NOTFOUND;
    aBlob[nBlob++] = b;
  }
  if( pos!=nSortKey ){
    return SQLITE_NOTFOUND;
  }

  decodeNumericSortKeyToRecord(pSortKey + 1, 8, &aType0, &aLen0, aBuf0);
  aType1 = (u32)nBlob * 2 + 12;

  if( aType1<128 ){
    nHdr = 3;
  }else{
    nHdr = 1 + sqlite3VarintLen(aType0) + sqlite3VarintLen(aType1);
  }
  if( nHdr > 126 ) nHdr++;
  nTotal = nHdr + (int)aLen0 + nBlob;
  if( *pnAlloc < nTotal ){
    u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, nTotal);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nTotal;
  }
  pOut = *ppBuf;

  if( aType1<128 ){
    pOut[0] = 3;
    pOut[1] = (u8)aType0;
    pOut[2] = (u8)aType1;
    off = 3;
  }else{
    off = putVarint32(pOut, (u32)nHdr);
    off += putVarint32(pOut + off, aType0);
    off += putVarint32(pOut + off, aType1);
  }
  if( aLen0>0 ){
    memcpy(pOut + off, aBuf0, aLen0);
    off += (int)aLen0;
  }
  if( nBlob>0 ){
    memcpy(pOut + off, aBlob, nBlob);
  }

  *pnOut = nTotal;
  return SQLITE_OK;
}

int recordFromSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
){
  u32 aType[64];
  u32 aLen[64];

  const u8 *aFieldPtr[64];
  int aEncLen[64];
  u8 aIntBuf[64][8];
  int nFields = 0;
  int pos = 0;
  u8 *pOut;
  int nHdr, nData, nTotal;
  int i;

  *pnOut = 0;

  if( nSortKey <= 0 ){
    if( *pnAlloc < 1 ){
      u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, 1);
      if( !pNew ) return SQLITE_NOMEM;
      *ppBuf = pNew;
      *pnAlloc = 1;
    }
    pOut = *ppBuf;
    pOut[0] = 1;
    *pnOut = 1;
    return SQLITE_OK;
  }

  {
    int rc = recordFromAllNumericSortKeyBuffer(pSortKey, nSortKey,
                                               ppBuf, pnAlloc, pnOut);
    if( rc!=SQLITE_NOTFOUND ) return rc;
  }
  {
    int rc = recordFromNumericSmallBlobSortKeyBuffer(pSortKey, nSortKey,
                                                     ppBuf, pnAlloc, pnOut);
    if( rc!=SQLITE_NOTFOUND ) return rc;
  }
  {
    int rc = recordFromNumericVarlenSortKeyBuffer(pSortKey, nSortKey,
                                                  ppBuf, pnAlloc, pnOut);
    if( rc!=SQLITE_NOTFOUND ) return rc;
  }

  while( pos < nSortKey && nFields < 64 ){
    u8 tag = pSortKey[pos++];

    if( tag == SORTKEY_NULL ){
      aType[nFields] = 0;
      aLen[nFields] = 0;
      aFieldPtr[nFields] = 0;
      aEncLen[nFields] = 0;
      nFields++;

    }else if( tag == SORTKEY_NUM ){
      int nNum;
      pos--;
      nNum = numericSortKeyLen(pSortKey + pos, nSortKey - pos);
      if( nNum==0 ) return SQLITE_CORRUPT;
      decodeNumericSortKeyToRecord(pSortKey + pos + 1, nNum - 1,
                                   &aType[nFields], &aLen[nFields],
                                   aIntBuf[nFields]);
      pos += nNum;
      aFieldPtr[nFields] = aIntBuf[nFields];
      aEncLen[nFields] = 0;
      nFields++;

    }else if( tag == SORTKEY_TEXT || tag == SORTKEY_BLOB ){

      int start = pos;
      int dataLen = 0;
      while( pos < nSortKey ){
        const u8 *p0 = pSortKey + pos;
        const u8 *pZero = (const u8*)memchr(p0, 0x00, (size_t)(nSortKey - pos));
        if( pZero==0 ) return SQLITE_CORRUPT;
        {
          int gap = (int)(pZero - p0);
          dataLen += gap;
          pos += gap;
        }
        if( pos + 1 >= nSortKey ) return SQLITE_CORRUPT;
        if( pSortKey[pos+1] == 0x00 ){
          pos += 2;
          break;
        }else if( pSortKey[pos+1] == 0x01 ){
          dataLen++;
          pos += 2;
        }else{
          return SQLITE_CORRUPT;
        }
      }
      if( tag == SORTKEY_TEXT ){
        aType[nFields] = (u32)dataLen * 2 + 13;
      }else{
        aType[nFields] = (u32)dataLen * 2 + 12;
      }
      aLen[nFields] = (u32)dataLen;

      aFieldPtr[nFields] = pSortKey + start;
      aEncLen[nFields] = (pos - 2) - start;
      nFields++;

    }else{
      return SQLITE_CORRUPT;
    }
  }

  nHdr = 1;
  for(i = 0; i < nFields; i++){
    nHdr += sqlite3VarintLen(aType[i]);
  }
  if( nHdr > 126 ) nHdr++;

  nData = 0;
  for(i = 0; i < nFields; i++) nData += (int)aLen[i];

  nTotal = nHdr + nData;
  if( *pnAlloc < nTotal ){
    u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, nTotal);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nTotal;
  }
  pOut = *ppBuf;

  {
    int off;

    off = putVarint32(pOut, (u32)nHdr);
    for(i = 0; i < nFields; i++){
      off += putVarint32(pOut + off, aType[i]);
    }

    for(i = 0; i < nFields; i++){
      u32 serialType = aType[i];
      u32 fieldLen = aLen[i];

      if( fieldLen == 0 ){

        continue;
      }

      if( serialType <= 6 || serialType == 7 ){

        memcpy(pOut + off, aIntBuf[i], fieldLen);
        off += (int)fieldLen;
      }else if( (u32)aEncLen[i] == fieldLen ){
        memcpy(pOut + off, aFieldPtr[i], fieldLen);
        off += (int)fieldLen;
      }else{

        const u8 *pSrc = aFieldPtr[i];
        const u8 *pSrcEnd = pSortKey + nSortKey;
        int j = 0;
        u32 written = 0;
        while( written < fieldLen ){
          if( pSrc+j+1 < pSrcEnd
           && pSrc[j] == 0x00 && pSrc[j+1] == 0x01 ){
            pOut[off++] = 0x00;
            j += 2;
          }else if( pSrc+j < pSrcEnd ){
            pOut[off++] = pSrc[j];
            j++;
          }else{

            pOut[off++] = 0x00;
          }
          written++;
        }
      }
    }
  }

  *pnOut = nTotal;
  return SQLITE_OK;
}

int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut){
  int nAlloc = 0;
  *ppOut = 0;
  return recordFromSortKeyBuffer(pSortKey, nSortKey, ppOut, &nAlloc, pnOut);
}

#endif
