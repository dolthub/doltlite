/*
** Sort key encoding for BLOBKEY (index) entries.
**
** Converts SQLite serialized record format into a memcmp-sortable
** byte string. See sortkey.h for format documentation.
*/
#ifdef DOLTLITE_PROLLY

#include "sortkey.h"
#include <string.h>

/*
** Read a varint from pBuf. Returns the number of bytes consumed.
** Value is stored in *pVal. Handles 1-9 byte varints.
*/
static int skGetVarint32(const u8 *p, u32 *pVal){
  u32 a;
  a = *p;
  if( !(a & 0x80) ){
    *pVal = a;
    return 1;
  }
  /* Multi-byte varint — defer to full parser */
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

/*
** Return the data length for a SQLite serial type.
*/
static u32 serialTypeLen(u32 serialType){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( serialType <= 6 ) return aLen[serialType];
  if( serialType == 7 ) return 8;
  if( serialType >= 12 ) return (serialType - 12) / 2;
  return 0;  /* types 8,9,10,11 are constants */
}

/*
** Classify a serial type into a sort key tag.
** INT and FLOAT both map to SORTKEY_NUM for correct cross-type
** numeric comparison (e.g., integer 0 vs float -1.5).
*/
static u8 serialTypeTag(u32 serialType){
  if( serialType == 0 ) return SORTKEY_NULL;
  if( serialType <= 6 || serialType == 8 || serialType == 9 ){
    return SORTKEY_NUM;
  }
  if( serialType == 7 ) return SORTKEY_NUM;
  if( serialType >= 13 && (serialType & 1) ) return SORTKEY_TEXT;
  if( serialType >= 12 && !(serialType & 1) ) return SORTKEY_BLOB;
  return SORTKEY_NULL;  /* shouldn't reach here */
}

/*
** Compute the encoded size for a single field in the sort key.
**
** NULL:    1 byte (tag only)
** Integer: 9 bytes (tag + 8 bytes)
** Float:   9 bytes (tag + 8 bytes)
** Text:    1 + escaped_length + 2 bytes (tag + escaped data + 0x00 0x00)
** Blob:    1 + escaped_length + 2 bytes (tag + escaped data + 0x00 0x00)
**
** For text/blob, each 0x00 byte in the data expands to 0x00 0x01 (2 bytes).
*/
static int encodedFieldSize(u32 serialType, const u8 *pData, u32 nData){
  u8 tag = serialTypeTag(serialType);
  switch( tag ){
    case SORTKEY_NULL:
      return 1;
    case SORTKEY_NUM:
      return 9;
    case SORTKEY_TEXT:
    case SORTKEY_BLOB: {
      /* Count NUL bytes for escaping */
      int extra = 0;
      u32 i;
      for(i = 0; i < nData; i++){
        if( pData[i] == 0x00 ) extra++;
      }
      return 1 + (int)nData + extra + 2;  /* tag + data + escapes + terminator */
    }
    default:
      return 1;
  }
}

/*
** Encode a numeric field (integer or float) into the sort key buffer.
** All numbers are converted to IEEE 754 double and encoded with the
** float encoding scheme. This ensures correct cross-type comparison
** (e.g., integer 0 vs float -1.5 sorts correctly).
**
** Encoding: big-endian IEEE 754 with sign normalization:
**   Positive (incl. +0): flip sign bit (XOR 0x80 on first byte)
**   Negative: flip ALL bits (reverses order for negatives)
*/
static void encodeNumeric(u8 *pOut, u32 serialType, const u8 *pData, u32 nData){
  u8 buf[8];
  double d;

  pOut[0] = SORTKEY_NUM;

  if( serialType == 7 ){
    /* Already a float — data is 8-byte big-endian IEEE 754 */
    memcpy(buf, pData, 8);
  }else{
    /* Integer — extract value and convert to double */
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
    /* Convert double to 8-byte big-endian */
    memcpy(&x, &d, 8);
    buf[0] = (u8)(x >> 56); buf[1] = (u8)(x >> 48);
    buf[2] = (u8)(x >> 40); buf[3] = (u8)(x >> 32);
    buf[4] = (u8)(x >> 24); buf[5] = (u8)(x >> 16);
    buf[6] = (u8)(x >> 8);  buf[7] = (u8)(x);
  }

  /* Apply sign normalization for correct memcmp ordering */
  if( buf[0] & 0x80 ){
    /* Negative: flip all bits */
    for(int i = 0; i < 8; i++) buf[i] = ~buf[i];
  }else{
    /* Positive (or zero): flip sign bit only */
    buf[0] ^= 0x80;
  }

  memcpy(pOut + 1, buf, 8);
}

/*
** Encode a text or blob field with NUL-byte escaping.
**
** Each 0x00 in the input is encoded as 0x00 0x01.
** The field is terminated by 0x00 0x00.
** This ensures:
**   1. No embedded NUL can be confused with the terminator
**   2. Shorter strings sort before longer strings with the same prefix
**   3. memcmp gives lexicographic ordering
*/
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

  /* Double-NUL terminator */
  pOut[pos++] = 0x00;
  pOut[pos++] = 0x00;

  return pos;
}

/*
** Internal implementation: parse record and either compute size or encode.
** If pOut is NULL, only compute the size.
** Returns the total sort key size, or -1 on error.
*/
static int sortKeyEncode(const u8 *pRec, int nRec, u8 *pOut){
  u32 hdrSize;
  u32 hdrOff;
  u32 dataOff;
  int outPos = 0;

  if( nRec <= 0 ) return -1;

  /* Parse header size */
  hdrOff = skGetVarint32(pRec, &hdrSize);
  if( hdrSize > (u32)nRec ) return -1;
  dataOff = hdrSize;

  /* Process each field */
  while( hdrOff < hdrSize ){
    u32 serialType;
    u32 fieldLen;
    const u8 *pField;

    hdrOff += skGetVarint32(pRec + hdrOff, &serialType);
    fieldLen = serialTypeLen(serialType);

    /* Bounds check */
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
  }

  return outPos;
}

int sortKeySize(const u8 *pRec, int nRec){
  return sortKeyEncode(pRec, nRec, NULL);
}

int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut){
  int nSize;
  u8 *pBuf;

  *ppOut = 0;
  *pnOut = 0;

  nSize = sortKeyEncode(pRec, nRec, NULL);
  if( nSize < 0 ) return SQLITE_CORRUPT;
  if( nSize == 0 ){
    /* Empty record — produce empty sort key */
    *ppOut = (u8*)sqlite3_malloc(1);
    if( !*ppOut ) return SQLITE_NOMEM;
    *pnOut = 0;
    return SQLITE_OK;
  }

  pBuf = (u8*)sqlite3_malloc(nSize);
  if( !pBuf ) return SQLITE_NOMEM;

  sortKeyEncode(pRec, nRec, pBuf);


  *ppOut = pBuf;
  *pnOut = nSize;
  return SQLITE_OK;
}

/*
** Determine the minimum SQLite serial type for an i64 value.
** Mirrors btreeSerialType() logic in prolly_btree.c.
*/
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

/*
** Write an integer value as big-endian bytes (SQLite record format).
*/
static void writeIntBE(u8 *p, i64 v, int nByte){
  int j;
  for(j = nByte - 1; j >= 0; j--){
    p[j] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

/*
** Decode a sort key back into a SQLite serialized record.
**
** First pass: parse sort key fields to determine serial types and data sizes.
** Second pass: write the SQLite record (varint header + field data).
*/
int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut){
  /* Max 64 fields (matching serializeUnpackedRecord limit) */
  u32 aType[64];
  u32 aLen[64];
  /* Temporary storage for decoded field data.
  ** Text/blob fields point into decode buffer; ints/floats are small. */
  const u8 *aFieldPtr[64];
  u8 aIntBuf[64][8];  /* Per-field buffer for int/float data */
  int nFields = 0;
  int pos = 0;
  u8 *pOut;
  int nHdr, nData, nTotal;
  int i;

  *ppOut = 0;
  *pnOut = 0;

  if( nSortKey <= 0 ){
    /* Empty sort key → empty record */
    pOut = (u8*)sqlite3_malloc(1);
    if( !pOut ) return SQLITE_NOMEM;
    pOut[0] = 1;  /* header size = 1 (just the header size varint) */
    *ppOut = pOut;
    *pnOut = 1;
    return SQLITE_OK;
  }

  /* ---- Pass 1: Parse sort key fields ---- */
  while( pos < nSortKey && nFields < 64 ){
    u8 tag = pSortKey[pos++];

    if( tag == SORTKEY_NULL ){
      aType[nFields] = 0;
      aLen[nFields] = 0;
      aFieldPtr[nFields] = 0;
      nFields++;

    }else if( tag == SORTKEY_NUM ){
      /* 8-byte IEEE 754 double with sign normalization → restore.
      ** If the double is an exact integer, emit integer serial type
      ** for canonical SQLite record encoding. */
      u8 buf[8];
      double d;
      u64 x;
      if( pos + 8 > nSortKey ) return SQLITE_CORRUPT;
      memcpy(buf, pSortKey + pos, 8);
      pos += 8;
      /* Undo sign normalization */
      if( buf[0] & 0x80 ){
        /* Was positive: unflip sign bit only */
        buf[0] ^= 0x80;
      }else{
        /* Was negative: unflip all bits */
        for(i = 0; i < 8; i++) buf[i] = ~buf[i];
      }
      /* Convert big-endian bytes to u64 then to double */
      x = ((u64)buf[0] << 56) | ((u64)buf[1] << 48)
        | ((u64)buf[2] << 40) | ((u64)buf[3] << 32)
        | ((u64)buf[4] << 24) | ((u64)buf[5] << 16)
        | ((u64)buf[6] << 8)  | (u64)buf[7];
      memcpy(&d, &x, 8);
      /* Check if this double is an exact integer */
      {
        i64 iv = (i64)d;
        if( (double)iv == d && d >= -9.22e18 && d <= 9.22e18 ){
          /* Emit as integer serial type */
          intSerialType(iv, &aType[nFields], &aLen[nFields]);
          writeIntBE(aIntBuf[nFields], iv, (int)aLen[nFields]);
          aFieldPtr[nFields] = aIntBuf[nFields];
        }else{
          /* Emit as float serial type — store big-endian IEEE 754 */
          aType[nFields] = 7;
          aLen[nFields] = 8;
          memcpy(aIntBuf[nFields], buf, 8);
          aFieldPtr[nFields] = aIntBuf[nFields];
        }
      }
      nFields++;

    }else if( tag == SORTKEY_TEXT || tag == SORTKEY_BLOB ){
      /* NUL-escaped data terminated by 0x00 0x00 */
      /* First, compute unescaped length */
      int start = pos;
      int dataLen = 0;
      while( pos < nSortKey ){
        if( pSortKey[pos] == 0x00 ){
          if( pos + 1 >= nSortKey ) return SQLITE_CORRUPT;
          if( pSortKey[pos+1] == 0x00 ){
            /* Terminator found */
            pos += 2;
            break;
          }else if( pSortKey[pos+1] == 0x01 ){
            /* Escaped NUL */
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
      /* We'll decode the data in pass 2. Store the start offset. */
      /* Use aFieldPtr to store the sort key offset (cast to pointer math) */
      aFieldPtr[nFields] = pSortKey + start;
      nFields++;

    }else{
      return SQLITE_CORRUPT;
    }
  }

  /* ---- Compute SQLite record size ---- */
  nHdr = 1;  /* At least 1 byte for header size varint */
  for(i = 0; i < nFields; i++){
    nHdr += sqlite3VarintLen(aType[i]);
  }
  if( nHdr > 126 ) nHdr++;  /* 2-byte varint for header size */

  nData = 0;
  for(i = 0; i < nFields; i++) nData += (int)aLen[i];

  nTotal = nHdr + nData;
  pOut = (u8*)sqlite3_malloc(nTotal);
  if( !pOut ) return SQLITE_NOMEM;

  /* ---- Pass 2: Write SQLite record ---- */
  {
    int off;

    /* Header: size varint + serial type varints */
    off = putVarint32(pOut, (u32)nHdr);
    for(i = 0; i < nFields; i++){
      off += putVarint32(pOut + off, aType[i]);
    }

    /* Data: field values */
    for(i = 0; i < nFields; i++){
      u32 serialType = aType[i];
      u32 fieldLen = aLen[i];

      if( fieldLen == 0 ){
        /* NULL, or constant int (type 8/9) — no data bytes */
        continue;
      }

      if( serialType <= 6 || serialType == 7 ){
        /* Integer or float — data in aIntBuf */
        memcpy(pOut + off, aIntBuf[i], fieldLen);
        off += (int)fieldLen;
      }else{
        /* Text or blob — decode from sort key NUL-escaped data */
        const u8 *pSrc = aFieldPtr[i];
        int j = 0;
        u32 written = 0;
        while( written < fieldLen ){
          if( pSrc[j] == 0x00 && pSrc[j+1] == 0x01 ){
            pOut[off++] = 0x00;
            j += 2;
          }else{
            pOut[off++] = pSrc[j];
            j++;
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

#endif /* DOLTLITE_PROLLY */
