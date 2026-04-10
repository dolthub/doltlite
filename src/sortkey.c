
#ifdef DOLTLITE_PROLLY

#include "sortkey.h"
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

static int encodedFieldSize(u32 serialType, const u8 *pData, u32 nData){
  u8 tag = serialTypeTag(serialType);
  switch( tag ){
    case SORTKEY_NULL:
      return 1;
    case SORTKEY_NUM:
      return 9;
    case SORTKEY_TEXT:
    case SORTKEY_BLOB: {
      
      int extra = 0;
      u32 i;
      for(i = 0; i < nData; i++){
        if( pData[i] == 0x00 ) extra++;
      }
      return 1 + (int)nData + extra + 2;  
    }
    default:
      return 1;
  }
}

static void encodeNumeric(u8 *pOut, u32 serialType, const u8 *pData, u32 nData){
  u8 buf[8];
  double d;

  pOut[0] = SORTKEY_NUM;

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
      v = (pData[0] & 0x80) ? -1 : 0;
      for(u32 i = 0; i < nData; i++){
        v = (v << 8) | pData[i];
      }
    }
    d = (double)v;
    
    memcpy(&x, &d, 8);
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

/*
** Encode the first nMaxFields columns of pRec as a binary-comparable
** sort key. Pass nMaxFields = 0 (or any value larger than the field
** count) to encode the whole record. Returns the encoded byte count, or
** -1 on error.
*/
static int sortKeyEncode(const u8 *pRec, int nRec, u8 *pOut, int nMaxFields){
  u32 hdrSize;
  u32 hdrOff;
  u32 dataOff;
  int outPos = 0;
  int nField = 0;

  if( nRec <= 0 ) return -1;


  hdrOff = skGetVarint32(pRec, &hdrSize);
  if( hdrSize > (u32)nRec ) return -1;
  dataOff = hdrSize;


  while( hdrOff < hdrSize ){
    u32 serialType;
    u32 fieldLen;
    const u8 *pField;

    if( nMaxFields > 0 && nField >= nMaxFields ) break;

    hdrOff += skGetVarint32(pRec + hdrOff, &serialType);
    fieldLen = serialTypeLen(serialType);


    if( dataOff + fieldLen > (u32)nRec ) return -1;
    pField = pRec + dataOff;

    if( pOut ){
      u8 tag = serialTypeTag(serialType);
      switch( tag ){
        case SORTKEY_NULL:
          pOut[outPos++] = SORTKEY_NULL;
          break;
        case SORTKEY_NUM:
          encodeNumeric(pOut + outPos, serialType, pField, fieldLen);
          outPos += 9;
          break;
        case SORTKEY_TEXT:
        case SORTKEY_BLOB:
          outPos += encodeVarLen(pOut + outPos, tag, pField, fieldLen);
          break;
        default:
          pOut[outPos++] = SORTKEY_NULL;
          break;
      }
    }else{
      outPos += encodedFieldSize(serialType, pField, fieldLen);
    }

    dataOff += fieldLen;
    nField++;
  }

  return outPos;
}

int sortKeySize(const u8 *pRec, int nRec){
  return sortKeyEncode(pRec, nRec, NULL, 0);
}

int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut){
  return sortKeyFromRecordPrefix(pRec, nRec, 0, ppOut, pnOut);
}

int sortKeyFromRecordPrefix(
  const u8 *pRec, int nRec, int nKeyField, u8 **ppOut, int *pnOut
){
  int nSize;
  u8 *pBuf;

  *ppOut = 0;
  *pnOut = 0;

  nSize = sortKeyEncode(pRec, nRec, NULL, nKeyField);
  if( nSize < 0 ) return SQLITE_CORRUPT;
  if( nSize == 0 ){

    *ppOut = (u8*)sqlite3_malloc(1);
    if( !*ppOut ) return SQLITE_NOMEM;
    *pnOut = 0;
    return SQLITE_OK;
  }

  pBuf = (u8*)sqlite3_malloc(nSize);
  if( !pBuf ) return SQLITE_NOMEM;

  sortKeyEncode(pRec, nRec, pBuf, nKeyField);

  *ppOut = pBuf;
  *pnOut = nSize;
  return SQLITE_OK;
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

int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut){
  
  u32 aType[64];
  u32 aLen[64];
  
  const u8 *aFieldPtr[64];
  u8 aIntBuf[64][8];  
  int nFields = 0;
  int pos = 0;
  u8 *pOut;
  int nHdr, nData, nTotal;
  int i;

  *ppOut = 0;
  *pnOut = 0;

  if( nSortKey <= 0 ){
    
    pOut = (u8*)sqlite3_malloc(1);
    if( !pOut ) return SQLITE_NOMEM;
    pOut[0] = 1;  
    *ppOut = pOut;
    *pnOut = 1;
    return SQLITE_OK;
  }

  
  while( pos < nSortKey && nFields < 64 ){
    u8 tag = pSortKey[pos++];

    if( tag == SORTKEY_NULL ){
      aType[nFields] = 0;
      aLen[nFields] = 0;
      aFieldPtr[nFields] = 0;
      nFields++;

    }else if( tag == SORTKEY_NUM ){
      
      u8 buf[8];
      double d;
      u64 x;
      if( pos + 8 > nSortKey ) return SQLITE_CORRUPT;
      memcpy(buf, pSortKey + pos, 8);
      pos += 8;
      
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
          
          intSerialType(iv, &aType[nFields], &aLen[nFields]);
          writeIntBE(aIntBuf[nFields], iv, (int)aLen[nFields]);
          aFieldPtr[nFields] = aIntBuf[nFields];
        }else{
          
          aType[nFields] = 7;
          aLen[nFields] = 8;
          memcpy(aIntBuf[nFields], buf, 8);
          aFieldPtr[nFields] = aIntBuf[nFields];
        }
      }
      nFields++;

    }else if( tag == SORTKEY_TEXT || tag == SORTKEY_BLOB ){
      
      
      int start = pos;
      int dataLen = 0;
      while( pos < nSortKey ){
        if( pSortKey[pos] == 0x00 ){
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
        }else{
          dataLen++;
          pos++;
        }
      }
      if( tag == SORTKEY_TEXT ){
        aType[nFields] = (u32)dataLen * 2 + 13;  
      }else{
        aType[nFields] = (u32)dataLen * 2 + 12;  
      }
      aLen[nFields] = (u32)dataLen;
      
      
      aFieldPtr[nFields] = pSortKey + start;
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
  pOut = (u8*)sqlite3_malloc(nTotal);
  if( !pOut ) return SQLITE_NOMEM;

  
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

  *ppOut = pOut;
  *pnOut = nTotal;
  return SQLITE_OK;
}

#endif 
