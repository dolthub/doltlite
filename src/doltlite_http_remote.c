
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_creds.h"
#include "doltlite_net.h"
#ifdef DOLTLITE_HAVE_AUTH
#include "doltlite_tls.h"
#endif
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

typedef struct HttpRemote HttpRemote;
struct HttpRemote {
  DoltliteRemote base;
  char *zHost;
  int port;
  int useTls;
  char *zBasePath;

#ifdef DOLTLITE_HAVE_AUTH
  DoltliteCreds *cred;  /* NULL when unauthenticated */
  char *zAudience;      /* JWT audience: remote host or override */
#endif

  u8 *pUploadBuf;
  i64 nUploadBuf;
  i64 nUploadBufAlloc;

  u8 *pPendingRefs;
  int nPendingRefs;
  ProllyHash expectedRefsHash;
  u8 hasExpectedRefsHash;
  char *zPushBranch;
  int bPushForce;
};

#define HTTP_RESP_MAX_BYTES ((i64)128 * 1024 * 1024)

#ifdef DOLTLITE_HAVE_AUTH
typedef DoltliteConn HttpConn;
#define httpConnOpen(H,P,T) doltliteConnOpen((H),(P),(T))
#define httpConnWriteAll(C,B,N) doltliteConnWriteAll((C),(B),(N))
#define httpConnRead(C,B,N) doltliteConnRead((C),(B),(N))
#define httpConnClose(C) doltliteConnClose((C))
#else
typedef struct HttpConn HttpConn;
struct HttpConn {
  int fd;
};

static HttpConn *httpConnOpen(const char *zHost, int port, int useTls){
  struct addrinfo hints;
  struct addrinfo *pRes = 0;
  struct addrinfo *pAi;
  char zPort[16];
  int fd = -1;
  HttpConn *pConn;

  if( useTls ) return 0;
  if( doltliteNetInit()!=0 ) return 0;
  sqlite3_snprintf(sizeof(zPort), zPort, "%d", port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if( getaddrinfo(zHost, zPort, &hints, &pRes)!=0 ) return 0;

  for(pAi=pRes; pAi; pAi=pAi->ai_next){
    fd = (int)socket(pAi->ai_family, pAi->ai_socktype, pAi->ai_protocol);
    if( fd<0 ) continue;
    if( connect(fd, pAi->ai_addr, (int)pAi->ai_addrlen)==0 ) break;
    doltliteCloseSocket(fd);
    fd = -1;
  }
  freeaddrinfo(pRes);
  if( fd<0 ) return 0;

  pConn = sqlite3_malloc(sizeof(HttpConn));
  if( !pConn ){
    doltliteCloseSocket(fd);
    return 0;
  }
  pConn->fd = fd;
  return pConn;
}

static int httpConnWriteAll(HttpConn *pConn, const void *pBuf, int nBuf){
  const char *z = (const char*)pBuf;
  int nSent = 0;
  while( nSent<nBuf ){
    int n = send(pConn->fd, z+nSent, nBuf-nSent, 0);
    if( n<=0 ) return 1;
    nSent += n;
  }
  return 0;
}

static int httpConnRead(HttpConn *pConn, void *pBuf, int nBuf){
  return recv(pConn->fd, (char*)pBuf, nBuf, 0);
}

static void httpConnClose(HttpConn *pConn){
  if( pConn ){
    doltliteCloseSocket(pConn->fd);
    sqlite3_free(pConn);
  }
}
#endif

static int readUntilEof(HttpConn *conn, u8 **ppOut, int *pnOut){
  i64 nAlloc = 4096;
  i64 nUsed = 0;
  u8 *pBuf = sqlite3_malloc64(nAlloc);
  if( !pBuf ) return SQLITE_NOMEM;

  for(;;){
    int n;
    if( nUsed + 1024 > nAlloc ){
      u8 *pNew;
      i64 nNew = nAlloc * 2;
      if( nNew > HTTP_RESP_MAX_BYTES ) nNew = HTTP_RESP_MAX_BYTES;
      if( nNew <= nAlloc ){
        sqlite3_free(pBuf);
        return SQLITE_TOOBIG;
      }
      pNew = sqlite3_realloc64(pBuf, nNew);
      if( !pNew ){
        sqlite3_free(pBuf);
        return SQLITE_NOMEM;
      }
      pBuf = pNew;
      nAlloc = nNew;
    }
    n = httpConnRead(conn, pBuf + nUsed, (int)(nAlloc - nUsed));
    if( n < 0 ){
      sqlite3_free(pBuf);
      return SQLITE_IOERR;
    }
    if( n == 0 ) break;
    nUsed += (i64)n;
  }

  /* Callers scan the response as a C string (e.g. atoi on Content-Length), so
  ** guarantee a trailing NUL. The growth loop keeps slack in practice, but a
  ** buffer filled exactly to nAlloc would otherwise leave the parse to run off
  ** the end. */
  if( nUsed >= nAlloc ){
    u8 *pNew = sqlite3_realloc64(pBuf, nUsed + 1);
    if( !pNew ){
      sqlite3_free(pBuf);
      return SQLITE_NOMEM;
    }
    pBuf = pNew;
    nAlloc = nUsed + 1;
  }
  pBuf[nUsed] = 0;

  *ppOut = pBuf;
  *pnOut = (int)nUsed;
  return SQLITE_OK;
}

static int httpRequest(
  HttpRemote *p,
  const char *zMethod,
  const char *zPath,
  const u8 *pBody, int nBody,
  int *pStatus,
  u8 **ppResp, int *pnResp
){
  HttpConn *conn;
  char *zAuth = 0;
  char *zHdr;
  u8 *pRaw = 0;
  int nRaw = 0;
  int rc = SQLITE_ERROR;

  *pStatus = 0;
  *ppResp = 0;
  *pnResp = 0;

  conn = httpConnOpen(p->zHost, p->port, p->useTls);
  if( !conn ) return SQLITE_ERROR;

#ifdef DOLTLITE_HAVE_AUTH
  if( p->useTls && p->cred ){
    char *jwt = 0;
    if( doltliteCredsBearerToken(p->cred, p->zAudience, &jwt)==0 && jwt ){
      zAuth = sqlite3_mprintf("Authorization: Bearer %s\r\n", jwt);
    }
    if( jwt ) free(jwt);
  }
#endif

  if( pBody && nBody > 0 ){
    zHdr = sqlite3_mprintf(
      "%s %s HTTP/1.1\r\n"
      "Host: %s\r\n"
      "%s"
      "Content-Length: %d\r\n"
      "Content-Type: application/octet-stream\r\n"
      "Connection: close\r\n"
      "\r\n",
      zMethod, zPath, p->zHost, zAuth ? zAuth : "", nBody);
  }else{
    zHdr = sqlite3_mprintf(
      "%s %s HTTP/1.1\r\n"
      "Host: %s\r\n"
      "%s"
      "%s"
      "Connection: close\r\n"
      "\r\n",
      zMethod, zPath, p->zHost, zAuth ? zAuth : "",
      sqlite3_stricmp(zMethod, "GET")==0 ? "" : "Content-Length: 0\r\n");
  }
  sqlite3_free(zAuth);
  if( !zHdr ){ httpConnClose(conn); return SQLITE_NOMEM; }

  rc = httpConnWriteAll(conn, zHdr, (int)strlen(zHdr)) ? SQLITE_IOERR : SQLITE_OK;
  sqlite3_free(zHdr);
  if( rc != SQLITE_OK ){ httpConnClose(conn); return rc; }
  if( pBody && nBody > 0 ){
    if( httpConnWriteAll(conn, pBody, nBody) ){
      httpConnClose(conn);
      return SQLITE_IOERR;
    }
  }

  rc = readUntilEof(conn, &pRaw, &nRaw);
  httpConnClose(conn);
  if( rc != SQLITE_OK ) return rc;

  {
    int i;
    int statusStart = -1;
    for(i=0; i<nRaw-3; i++){
      if( pRaw[i]==' ' && statusStart<0 ){
        statusStart = i + 1;
      }else if( statusStart>=0 && (pRaw[i]==' ' || pRaw[i]=='\r') ){

        char aBuf[4];
        int len = i - statusStart;
        if( len>=1 && len<=3 ){
          memcpy(aBuf, pRaw+statusStart, len);
          aBuf[len] = 0;
          *pStatus = atoi(aBuf);
        }
        break;
      }
    }
  }

  {
    int i;
    int bodyStart = -1;
    int contentLength = -1;

    for(i=0; i<nRaw-3; i++){

      if( (i==0 || pRaw[i-1]=='\n') &&
          nRaw-i > 16 &&
          (pRaw[i]=='C' || pRaw[i]=='c') ){

        if( sqlite3_strnicmp((const char*)pRaw+i, "Content-Length:", 15)==0 ){
          contentLength = atoi((const char*)pRaw+i+15);
        }
      }
      if( pRaw[i]=='\r' && pRaw[i+1]=='\n' && pRaw[i+2]=='\r' && pRaw[i+3]=='\n' ){
        bodyStart = i + 4;
        break;
      }
    }

    if( bodyStart < 0 ){

      sqlite3_free(pRaw);
      return SQLITE_OK;
    }

    {
      int nAvail = nRaw - bodyStart;
      int nCopy;
      if( contentLength >= 0 && contentLength <= nAvail ){
        nCopy = contentLength;
      }else{
        nCopy = nAvail;
      }
      if( nCopy > 0 ){
        *ppResp = sqlite3_malloc(nCopy);
        if( !*ppResp ){
          sqlite3_free(pRaw);
          return SQLITE_NOMEM;
        }
        memcpy(*ppResp, pRaw + bodyStart, nCopy);
        *pnResp = nCopy;
      }
    }
    sqlite3_free(pRaw);
  }

  return SQLITE_OK;
}

static int uploadBufAppend(HttpRemote *p, const u8 *pData, int nData){
  if( p->nUploadBuf + nData > p->nUploadBufAlloc ){
    i64 nNew = p->nUploadBufAlloc ? p->nUploadBufAlloc * 2 : 4096;
    u8 *pNew;
    while( nNew < p->nUploadBuf + nData ) nNew *= 2;
    pNew = sqlite3_realloc64(p->pUploadBuf, nNew);
    if( !pNew ) return SQLITE_NOMEM;
    p->pUploadBuf = pNew;
    p->nUploadBufAlloc = nNew;
  }
  memcpy(p->pUploadBuf + p->nUploadBuf, pData, nData);
  p->nUploadBuf += nData;
  return SQLITE_OK;
}

static char *buildPath(HttpRemote *p, const char *zSuffix){
  int nBase = (int)strlen(p->zBasePath);
  int nSuffix = (int)strlen(zSuffix);
  char *z = sqlite3_malloc(nBase + nSuffix + 1);
  if( z ){
    memcpy(z, p->zBasePath, nBase);
    memcpy(z + nBase, zSuffix, nSuffix + 1);
  }
  return z;
}

static int httpHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                         int nHash, u8 *aResult){
  HttpRemote *p = (HttpRemote*)pRemote;
  char *zPath;
  u8 *pReqBody;
  int nReqBody;
  int status = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int rc;

  if( nHash <= 0 ) return SQLITE_OK;

  nReqBody = nHash * PROLLY_HASH_SIZE;
  pReqBody = sqlite3_malloc(nReqBody);
  if( !pReqBody ) return SQLITE_NOMEM;
  memcpy(pReqBody, aHash, nReqBody);

  zPath = buildPath(p, "/has-chunks");
  if( !zPath ){
    sqlite3_free(pReqBody);
    return SQLITE_NOMEM;
  }

  rc = httpRequest(p, "POST", zPath,
                   pReqBody, nReqBody, &status, &pResp, &nResp);
  sqlite3_free(pReqBody);
  sqlite3_free(zPath);

  if( rc != SQLITE_OK ) return rc;
  if( status != 200 ){
    sqlite3_free(pResp);
    return SQLITE_ERROR;
  }

  if( nResp >= nHash ){
    memcpy(aResult, pResp, nHash);
  }else{

    int i;
    if( nResp > 0 ) memcpy(aResult, pResp, nResp);
    for(i=nResp; i<nHash; i++) aResult[i] = 0;
  }
  sqlite3_free(pResp);
  return SQLITE_OK;
}

static int httpGetChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                        u8 **ppData, int *pnData){
  HttpRemote *p = (HttpRemote*)pRemote;
  char zHex[PROLLY_HASH_SIZE*2+1];
  char zSuffix[PROLLY_HASH_SIZE*2+10];
  char *zPath;
  int status = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int rc;

  *ppData = 0;
  *pnData = 0;

  doltliteHashToHex(pHash, zHex);
  snprintf(zSuffix, sizeof(zSuffix), "/chunk/%s", zHex);
  zPath = buildPath(p, zSuffix);
  if( !zPath ) return SQLITE_NOMEM;

  rc = httpRequest(p, "GET", zPath,
                   0, 0, &status, &pResp, &nResp);
  sqlite3_free(zPath);

  if( rc != SQLITE_OK ){
    sqlite3_free(pResp);
    return rc;
  }
  if( status == 404 ){
    sqlite3_free(pResp);
    return SQLITE_NOTFOUND;
  }
  if( status != 200 ){
    sqlite3_free(pResp);
    return SQLITE_ERROR;
  }

  *ppData = pResp;
  *pnData = nResp;
  return SQLITE_OK;
}

static int httpPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                        const u8 *pData, int nData){
  HttpRemote *p = (HttpRemote*)pRemote;
  u8 aLen[4];
  int rc;

  rc = uploadBufAppend(p, pHash->data, PROLLY_HASH_SIZE);
  if( rc != SQLITE_OK ) return rc;

  aLen[0] = (u8)(nData & 0xff);
  aLen[1] = (u8)((nData >> 8) & 0xff);
  aLen[2] = (u8)((nData >> 16) & 0xff);
  aLen[3] = (u8)((nData >> 24) & 0xff);
  rc = uploadBufAppend(p, aLen, 4);
  if( rc != SQLITE_OK ) return rc;

  rc = uploadBufAppend(p, pData, nData);
  return rc;
}

/* Batched fetch: POST the concatenated hashes to /get-chunks and parse the
** framed reply (per requested hash: a 4-byte big-endian length then that many
** payload bytes; length 0xFFFFFFFF marks an absent chunk). One round trip
** replaces one GET /chunk per hash. */
static int httpGetChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                         int nHash, u8 **apData, int *anData){
  HttpRemote *p = (HttpRemote*)pRemote;
  char *zPath;
  u8 *pReq = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int status = 0;
  int rc;
  int i;
  i64 poff;

  for(i=0; i<nHash; i++){ apData[i] = 0; anData[i] = 0; }
  if( nHash <= 0 ) return SQLITE_OK;

  pReq = sqlite3_malloc(nHash * PROLLY_HASH_SIZE);
  if( !pReq ) return SQLITE_NOMEM;
  for(i=0; i<nHash; i++){
    memcpy(pReq + (i * PROLLY_HASH_SIZE), aHash[i].data, PROLLY_HASH_SIZE);
  }

  zPath = buildPath(p, "/get-chunks");
  if( !zPath ){ sqlite3_free(pReq); return SQLITE_NOMEM; }

  rc = httpRequest(p, "POST", zPath, pReq, nHash * PROLLY_HASH_SIZE,
                   &status, &pResp, &nResp);
  sqlite3_free(zPath);
  sqlite3_free(pReq);
  if( rc != SQLITE_OK ){ sqlite3_free(pResp); return rc; }
  if( status != 200 ){ sqlite3_free(pResp); return SQLITE_ERROR; }

  poff = 0;
  for(i=0; i<nHash; i++){
    u32 len;
    if( poff + 4 > nResp ){ rc = SQLITE_ERROR; break; }
    len = ((u32)pResp[poff] << 24) | ((u32)pResp[poff+1] << 16)
        | ((u32)pResp[poff+2] << 8) | (u32)pResp[poff+3];
    poff += 4;
    if( len == 0xFFFFFFFFu ) continue;            /* absent */
    if( poff + (i64)len > nResp ){ rc = SQLITE_ERROR; break; }
    apData[i] = sqlite3_malloc(len ? (int)len : 1);
    if( !apData[i] ){ rc = SQLITE_NOMEM; break; }
    memcpy(apData[i], pResp + poff, len);
    anData[i] = (int)len;
    poff += len;
  }

  sqlite3_free(pResp);
  return rc;   /* caller frees any apData[i] set before an error */
}

static int httpGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  HttpRemote *p = (HttpRemote*)pRemote;
  char *zPath;
  int status = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int rc;

  *ppData = 0;
  *pnData = 0;

  zPath = buildPath(p, "/refs");
  if( !zPath ) return SQLITE_NOMEM;

  rc = httpRequest(p, "GET", zPath,
                   0, 0, &status, &pResp, &nResp);
  sqlite3_free(zPath);

  if( rc != SQLITE_OK ){
    sqlite3_free(pResp);
    return rc;
  }
  if( status == 404 ){
    sqlite3_free(pResp);
    return SQLITE_NOTFOUND;
  }
  if( status != 200 ){
    sqlite3_free(pResp);
    return SQLITE_ERROR;
  }

  *ppData = pResp;
  *pnData = nResp;
  return SQLITE_OK;
}

static int httpSetRefs(DoltliteRemote *pRemote, const char *zBranch,
                       int bForce, const u8 *pData, int nData){
  HttpRemote *p = (HttpRemote*)pRemote;

  sqlite3_free(p->pPendingRefs);
  p->pPendingRefs = 0;
  p->nPendingRefs = 0;
  p->hasExpectedRefsHash = 0;
  sqlite3_free(p->zPushBranch);
  p->zPushBranch = zBranch ? sqlite3_mprintf("%s", zBranch) : 0;
  if( zBranch && !p->zPushBranch ) return SQLITE_NOMEM;
  p->bPushForce = bForce;

  if( pData && nData > 0 ){
    p->pPendingRefs = sqlite3_malloc(nData);
    if( !p->pPendingRefs ) return SQLITE_NOMEM;
    memcpy(p->pPendingRefs, pData, nData);
    p->nPendingRefs = nData;
  }
  return SQLITE_OK;
}

static int httpSetRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pData,
  int nData
){
  HttpRemote *p = (HttpRemote*)pRemote;
  int rc = httpSetRefs(pRemote, zBranch, bForce, pData, nData);
  if( rc!=SQLITE_OK ) return rc;
  if( pExpectedRefsHash ){
    memcpy(&p->expectedRefsHash, pExpectedRefsHash, sizeof(ProllyHash));
  }else{
    memset(&p->expectedRefsHash, 0, sizeof(ProllyHash));
  }
  p->hasExpectedRefsHash = 1;
  return SQLITE_OK;
}

static int httpCommit(DoltliteRemote *pRemote){
  HttpRemote *p = (HttpRemote*)pRemote;
  int status = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int rc;
  char *zPath;

  if( p->pUploadBuf && p->nUploadBuf > 0 ){
    zPath = buildPath(p, "/chunks");
    if( !zPath ) return SQLITE_NOMEM;

    rc = httpRequest(p, "POST", zPath,
                     p->pUploadBuf, (int)p->nUploadBuf,
                     &status, &pResp, &nResp);
    sqlite3_free(zPath);
    sqlite3_free(pResp);
    if( rc != SQLITE_OK ) return rc;
    if( status != 200 && status != 204 ) return SQLITE_ERROR;
  }

  if( p->pPendingRefs && p->nPendingRefs > 0 ){
    /* Body: [u16 branchLen][branch][u8 force] then, for refs-if, the expected
    ** refs hash, then the refs blob. The prefix declares the push scope so the
    ** server can reject changes to any other ref. */
    int nBranch = p->zPushBranch ? (int)strlen(p->zPushBranch) : 0;
    int nHashPart = p->hasExpectedRefsHash ? PROLLY_HASH_SIZE : 0;
    int nReq = 2 + nBranch + 1 + nHashPart + p->nPendingRefs;
    u8 *pReq;
    int off = 0;

    pResp = 0; nResp = 0; status = 0;
    zPath = buildPath(p, p->hasExpectedRefsHash ? "/refs-if" : "/refs");
    if( !zPath ) return SQLITE_NOMEM;
    pReq = sqlite3_malloc(nReq);
    if( !pReq ){
      sqlite3_free(zPath);
      return SQLITE_NOMEM;
    }
    pReq[off++] = (u8)(nBranch & 0xff);
    pReq[off++] = (u8)((nBranch >> 8) & 0xff);
    if( nBranch>0 ){ memcpy(pReq+off, p->zPushBranch, nBranch); off += nBranch; }
    pReq[off++] = (u8)(p->bPushForce ? 1 : 0);
    if( nHashPart ){
      memcpy(pReq+off, p->expectedRefsHash.data, PROLLY_HASH_SIZE);
      off += PROLLY_HASH_SIZE;
    }
    memcpy(pReq+off, p->pPendingRefs, p->nPendingRefs);
    rc = httpRequest(p, "PUT", zPath, pReq, nReq, &status, &pResp, &nResp);
    sqlite3_free(pReq);
    sqlite3_free(zPath);
    sqlite3_free(pResp);
    if( rc != SQLITE_OK ) return rc;
    if( status == 409 ) return SQLITE_BUSY;
    if( status != 200 && status != 204 ) return SQLITE_ERROR;
  }

  {
    pResp = 0; nResp = 0; status = 0;
    zPath = buildPath(p, "/commit");
    if( !zPath ) return SQLITE_NOMEM;

    rc = httpRequest(p, "POST", zPath,
                     0, 0, &status, &pResp, &nResp);
    sqlite3_free(zPath);
    sqlite3_free(pResp);
    if( rc != SQLITE_OK ) return rc;
    if( status != 200 && status != 204 ) return SQLITE_ERROR;
  }

  sqlite3_free(p->pUploadBuf);
  p->pUploadBuf = 0;
  p->nUploadBuf = 0;
  p->nUploadBufAlloc = 0;
  p->hasExpectedRefsHash = 0;

  sqlite3_free(p->pPendingRefs);
  p->pPendingRefs = 0;
  p->nPendingRefs = 0;
  sqlite3_free(p->zPushBranch);
  p->zPushBranch = 0;

  return SQLITE_OK;
}

static void httpClose(DoltliteRemote *pRemote){
  HttpRemote *p = (HttpRemote*)pRemote;
#ifdef DOLTLITE_HAVE_AUTH
  doltliteCredsFree(p->cred);
  sqlite3_free(p->zAudience);
#endif
  sqlite3_free(p->zHost);
  sqlite3_free(p->zBasePath);
  sqlite3_free(p->pUploadBuf);
  sqlite3_free(p->pPendingRefs);
  sqlite3_free(p->zPushBranch);
  sqlite3_free(p);
}

#ifdef DOLTLITE_HAVE_AUTH
static void httpResolveCreds(HttpRemote *p){
  char *dir = doltliteCredsDir();
  const char *kid = getenv("DOLTLITE_CREDS_KID");
  DoltliteCreds *cred = 0;
  if( dir ){
    if( kid && *kid ){
      doltliteCredsLoad(dir, kid, &cred);
    }else{
      doltliteCredsLoadDefault(dir, &cred);
    }
    free(dir);
  }
  if( cred ){
    const char *aud = getenv("DOLT_OVERRIDE_GRPC_JWT_AUDIENCE");
    if( !aud || !*aud ) aud = p->zHost;
    p->zAudience = sqlite3_mprintf("%s", aud);
    if( p->zAudience ){
      p->cred = cred;
    }else{
      doltliteCredsFree(cred);
    }
  }
}
#endif

DoltliteRemote *doltliteHttpRemoteOpen(const char *zUrl){
  HttpRemote *p;
  const char *zAfterScheme;
  const char *zHostStart;
  const char *zPortStart;
  const char *zPathStart;
  int nHost;
  int port;
  int useTls;
  int nUserPath;
  int nBasePath;

  if( !zUrl ) return 0;

  if( strncmp(zUrl, "https://", 8) == 0 ){
#ifndef DOLTLITE_HAVE_AUTH
    return 0;
#else
    useTls = 1;
    port = 443;
    zAfterScheme = zUrl + 8;
#endif
  }else if( strncmp(zUrl, "http://", 7) == 0 ){
    useTls = 0;
    port = 80;
    zAfterScheme = zUrl + 7;
  }else{
    return 0;
  }
  zHostStart = zAfterScheme;

  zPortStart = 0;
  zPathStart = 0;
  {
    const char *c = zHostStart;
    while( *c && *c != ':' && *c != '/' ) c++;
    nHost = (int)(c - zHostStart);
    if( *c == ':' ){
      zPortStart = c + 1;
      c = zPortStart;
      while( *c && *c != '/' ) c++;
      port = atoi(zPortStart);
      if( port <= 0 ) port = useTls ? 443 : 80;
      if( *c == '/' ) zPathStart = c;
    }else if( *c == '/' ){
      zPathStart = c;
    }
  }

  if( nHost <= 0 ) return 0;

  nUserPath = 0;
  if( zPathStart ){
    nUserPath = (int)strlen(zPathStart);

    while( nUserPath > 0 && zPathStart[nUserPath-1] == '/' ){
      nUserPath--;
    }
  }

  p = sqlite3_malloc(sizeof(HttpRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(HttpRemote));

  p->zHost = sqlite3_malloc(nHost + 1);
  if( !p->zHost ){
    sqlite3_free(p);
    return 0;
  }
  memcpy(p->zHost, zHostStart, nHost);
  p->zHost[nHost] = 0;

  p->port = port;

  nBasePath = nUserPath;
  if( nBasePath <= 0 ){

    sqlite3_free(p->zHost);
    sqlite3_free(p);
    return 0;
  }
  p->zBasePath = sqlite3_malloc(nBasePath + 1);
  if( !p->zBasePath ){
    sqlite3_free(p->zHost);
    sqlite3_free(p);
    return 0;
  }
  memcpy(p->zBasePath, zPathStart, nBasePath);
  p->zBasePath[nBasePath] = '\0';

  p->useTls = useTls;
#ifdef DOLTLITE_HAVE_AUTH
  if( useTls ){
    httpResolveCreds(p);
  }
#endif

  p->base.xGetChunk = httpGetChunk;
  p->base.xPutChunk = httpPutChunk;
  p->base.xHasChunks = httpHasChunks;
  p->base.xGetChunks = httpGetChunks;
  p->base.xGetRefs = httpGetRefs;
  p->base.xSetRefs = httpSetRefs;
  p->base.xSetRefsIf = httpSetRefsIf;
  p->base.xCommit = httpCommit;
  p->base.xClose = httpClose;

  return &p->base;
}

#endif
