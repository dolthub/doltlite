
#ifdef DOLTLITE_PROLLY

#include "doltlite_commit.h"
#include <string.h>

#define DLC_PUT_U16(p,v) do{ (p)[0]=(u8)(v); (p)[1]=(u8)((v)>>8); }while(0)
#define DLC_GET_U16(p) ((u16)((p)[0] | ((p)[1]<<8)))
#define DLC_PUT_I64(p,v) do{ \
  (p)[0]=(u8)(v); (p)[1]=(u8)((v)>>8); (p)[2]=(u8)((v)>>16); \
  (p)[3]=(u8)((v)>>24); (p)[4]=(u8)((v)>>32); (p)[5]=(u8)((v)>>40); \
  (p)[6]=(u8)((v)>>48); (p)[7]=(u8)((v)>>56); }while(0)
#define DLC_GET_I64(p) ((i64)( \
  (u64)(p)[0] | ((u64)(p)[1]<<8) | ((u64)(p)[2]<<16) | ((u64)(p)[3]<<24) | \
  ((u64)(p)[4]<<32) | ((u64)(p)[5]<<40) | ((u64)(p)[6]<<48) | ((u64)(p)[7]<<56)))

int doltliteCommitSerialize(const DoltliteCommit *c, u8 **ppOut, int *pnOut){
  int nName = c->zName ? (int)strlen(c->zName) : 0;
  int nEmail = c->zEmail ? (int)strlen(c->zEmail) : 0;
  int nMsg = c->zMessage ? (int)strlen(c->zMessage) : 0;
  int nPar;
  if( nName > 0xFFFF || nEmail > 0xFFFF || nMsg > 0xFFFF ){
    return SQLITE_TOOBIG;
  }
  nPar = c->nParents > 0 ? c->nParents : (prollyHashIsEmpty(&c->parentHash) ? 0 : 1);
  
  int sz = 1 + 1 + PROLLY_HASH_SIZE*nPar + PROLLY_HASH_SIZE + 8
         + 2 + nName + 2 + nEmail + 2 + nMsg;
  u8 *buf = sqlite3_malloc(sz);
  u8 *p;
  int i;
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  
  *p++ = DOLTLITE_COMMIT_V2;

  
  *p++ = (u8)nPar;
  if( nPar > 0 && c->nParents > 0 ){
    for(i=0; i<nPar; i++){
      memcpy(p, c->aParents[i].data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
    }
  }else if( nPar == 1 ){
    
    memcpy(p, c->parentHash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
  }

  
  memcpy(p, c->catalogHash.data, PROLLY_HASH_SIZE); p += PROLLY_HASH_SIZE;

  
  DLC_PUT_I64(p, c->timestamp); p += 8;

  
  DLC_PUT_U16(p, (u16)nName); p += 2;
  if( nName>0 ){ memcpy(p, c->zName, nName); p += nName; }

  
  DLC_PUT_U16(p, (u16)nEmail); p += 2;
  if( nEmail>0 ){ memcpy(p, c->zEmail, nEmail); p += nEmail; }

  
  DLC_PUT_U16(p, (u16)nMsg); p += 2;
  if( nMsg>0 ){ memcpy(p, c->zMessage, nMsg); p += nMsg; }

  *ppOut = buf;
  *pnOut = (int)(p - buf);
  return SQLITE_OK;
}

int doltliteCommitDeserialize(const u8 *data, int nData, DoltliteCommit *c){
  const u8 *p = data;
  int nName, nEmail, nMsg;
  int version;
  int i;

  memset(c, 0, sizeof(*c));
  if( nData < 1 ) return SQLITE_CORRUPT;

  version = *p++;
  if( version != DOLTLITE_COMMIT_V2 ) return SQLITE_CORRUPT;

  {
    
    int nPar;
    if( nData < 3 ) return SQLITE_CORRUPT;
    nPar = *p++;
    if( nPar > DOLTLITE_MAX_PARENTS ) return SQLITE_CORRUPT;
    if( p + nPar*PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 8 + 6 > data + nData ){
      return SQLITE_CORRUPT;
    }
    c->nParents = nPar;
    for(i=0; i<nPar; i++){
      memcpy(c->aParents[i].data, p, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
    }
    
    if( nPar > 0 ) c->parentHash = c->aParents[0];
    
    memcpy(c->catalogHash.data, p, PROLLY_HASH_SIZE); p += PROLLY_HASH_SIZE;
    memset(c->rootHash.data, 0, PROLLY_HASH_SIZE);
  }

  
  c->timestamp = DLC_GET_I64(p); p += 8;

  
  nName = DLC_GET_U16(p); p += 2;
  if( p + nName > data + nData ) return SQLITE_CORRUPT;
  c->zName = sqlite3_malloc(nName + 1);
  if( !c->zName ) return SQLITE_NOMEM;
  if( nName>0 ) memcpy(c->zName, p, nName);
  c->zName[nName] = 0;
  p += nName;

  
  nEmail = DLC_GET_U16(p); p += 2;
  if( p + nEmail > data + nData ) return SQLITE_CORRUPT;
  c->zEmail = sqlite3_malloc(nEmail + 1);
  if( !c->zEmail ){ doltliteCommitClear(c); return SQLITE_NOMEM; }
  if( nEmail>0 ) memcpy(c->zEmail, p, nEmail);
  c->zEmail[nEmail] = 0;
  p += nEmail;

  
  nMsg = DLC_GET_U16(p); p += 2;
  if( p + nMsg > data + nData ){ doltliteCommitClear(c); return SQLITE_CORRUPT; }
  c->zMessage = sqlite3_malloc(nMsg + 1);
  if( !c->zMessage ){ doltliteCommitClear(c); return SQLITE_NOMEM; }
  if( nMsg>0 ) memcpy(c->zMessage, p, nMsg);
  c->zMessage[nMsg] = 0;

  return SQLITE_OK;
}

void doltliteCommitClear(DoltliteCommit *c){
  sqlite3_free(c->zName);
  sqlite3_free(c->zEmail);
  sqlite3_free(c->zMessage);
  c->zName = 0;
  c->zEmail = 0;
  c->zMessage = 0;
}

static const char hexchars[] = "0123456789abcdef";

void doltliteHashToHex(const ProllyHash *h, char *buf){
  int i;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    buf[i*2]   = hexchars[(h->data[i]>>4) & 0xf];
    buf[i*2+1] = hexchars[h->data[i] & 0xf];
  }
  buf[PROLLY_HASH_SIZE*2] = 0;
}

int doltliteHexToHash(const char *hex, ProllyHash *h){
  int i;
  if( !hex || strlen(hex) < PROLLY_HASH_SIZE*2 ) return SQLITE_ERROR;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    int hi, lo;
    char ch = hex[i*2];
    char cl = hex[i*2+1];
    if( ch>='0' && ch<='9' ) hi = ch - '0';
    else if( ch>='a' && ch<='f' ) hi = ch - 'a' + 10;
    else if( ch>='A' && ch<='F' ) hi = ch - 'A' + 10;
    else return SQLITE_ERROR;
    if( cl>='0' && cl<='9' ) lo = cl - '0';
    else if( cl>='a' && cl<='f' ) lo = cl - 'a' + 10;
    else if( cl>='A' && cl<='F' ) lo = cl - 'A' + 10;
    else return SQLITE_ERROR;
    h->data[i] = (u8)((hi<<4) | lo);
  }
  return SQLITE_OK;
}

#endif 
