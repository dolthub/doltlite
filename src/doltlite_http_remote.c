/*
** HTTP remote client for Doltlite.
**
** Implements the DoltliteRemote vtable over HTTP, communicating with
** the doltlite HTTP remote server.  Uses raw BSD sockets — no libcurl
** or other external dependencies.
**
** URL format:  http://host:port/dbname
** The dbname becomes the base path (e.g., "/dbname").
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_remote.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#ifdef _WIN32
/* HTTP remote not yet ported to Windows (requires Winsock) */
void doltliteRegisterHttpRemote(sqlite3 *db){ (void)db; }
#else /* POSIX */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>

/* ----------------------------------------------------------------
** HttpRemote structure
** ---------------------------------------------------------------- */

typedef struct HttpRemote HttpRemote;
struct HttpRemote {
  DoltliteRemote base;     /* vtable — must be first */
  char *zHost;             /* e.g., "localhost" */
  int port;                /* e.g., 8080 */
  char *zBasePath;         /* e.g., "/v1" */
  /* Upload buffer: accumulate chunks during push, flush on commit */
  u8 *pUploadBuf;
  i64 nUploadBuf;
  i64 nUploadBufAlloc;
  /* Pending refs update */
  u8 *pPendingRefs;
  int nPendingRefs;
};

/* ----------------------------------------------------------------
** Hash-to-hex helper
** ---------------------------------------------------------------- */

static void hashToHex(const ProllyHash *pHash, char *zOut){
  static const char hex[] = "0123456789abcdef";
  int i;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    zOut[i*2]   = hex[pHash->data[i] >> 4];
    zOut[i*2+1] = hex[pHash->data[i] & 0x0f];
  }
  zOut[PROLLY_HASH_SIZE*2] = 0;
}

/* ----------------------------------------------------------------
** Minimal HTTP client (no libcurl)
** ---------------------------------------------------------------- */

/*
** Read all available data from fd until EOF.
** Caller frees *ppOut with sqlite3_free.
*/
static int readUntilEof(int fd, u8 **ppOut, int *pnOut){
  int nAlloc = 4096;
  int nUsed = 0;
  u8 *pBuf = sqlite3_malloc(nAlloc);
  if( !pBuf ) return SQLITE_NOMEM;

  for(;;){
    ssize_t n;
    if( nUsed + 1024 > nAlloc ){
      nAlloc *= 2;
      u8 *pNew = sqlite3_realloc(pBuf, nAlloc);
      if( !pNew ){
        sqlite3_free(pBuf);
        return SQLITE_NOMEM;
      }
      pBuf = pNew;
    }
    n = read(fd, pBuf + nUsed, nAlloc - nUsed);
    if( n < 0 ){
      sqlite3_free(pBuf);
      return SQLITE_IOERR;
    }
    if( n == 0 ) break;
    nUsed += (int)n;
  }

  *ppOut = pBuf;
  *pnOut = nUsed;
  return SQLITE_OK;
}

/*
** Perform a single HTTP request. Returns SQLITE_OK on success.
** Response body is returned in *ppResp (caller frees), *pnResp.
** *pStatus is the HTTP status code (200, 404, etc.)
*/
static int httpRequest(
  const char *zHost, int port,
  const char *zMethod,        /* "GET", "POST", "PUT" */
  const char *zPath,          /* "/v1/root" */
  const u8 *pBody, int nBody, /* Request body (NULL for GET) */
  int *pStatus,               /* OUT: HTTP status code */
  u8 **ppResp, int *pnResp   /* OUT: Response body */
){
  int fd = -1;
  struct sockaddr_in addr;
  struct hostent *he;
  char aReqHdr[1024];
  int nReqHdr;
  u8 *pRaw = 0;
  int nRaw = 0;
  int rc = SQLITE_ERROR;

  *pStatus = 0;
  *ppResp = 0;
  *pnResp = 0;

  /* Resolve host */
  he = gethostbyname(zHost);
  if( !he ) return SQLITE_ERROR;

  /* Create socket and connect */
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if( fd < 0 ) return SQLITE_ERROR;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((u16)port);
  memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);

  if( connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0 ){
    close(fd);
    return SQLITE_ERROR;
  }

  /* Build request headers */
  if( pBody && nBody > 0 ){
    nReqHdr = snprintf(aReqHdr, sizeof(aReqHdr),
      "%s %s HTTP/1.1\r\n"
      "Host: %s\r\n"
      "Content-Length: %d\r\n"
      "Content-Type: application/octet-stream\r\n"
      "Connection: close\r\n"
      "\r\n",
      zMethod, zPath, zHost, nBody);
  }else{
    nReqHdr = snprintf(aReqHdr, sizeof(aReqHdr),
      "%s %s HTTP/1.1\r\n"
      "Host: %s\r\n"
      "Connection: close\r\n"
      "\r\n",
      zMethod, zPath, zHost);
  }

  /* Send request */
  {
    ssize_t nSent = write(fd, aReqHdr, nReqHdr);
    if( nSent != nReqHdr ){ close(fd); return SQLITE_ERROR; }
  }
  if( pBody && nBody > 0 ){
    int nOff = 0;
    while( nOff < nBody ){
      ssize_t n = write(fd, pBody + nOff, nBody - nOff);
      if( n <= 0 ){ close(fd); return SQLITE_ERROR; }
      nOff += (int)n;
    }
  }

  /* Read entire response */
  rc = readUntilEof(fd, &pRaw, &nRaw);
  close(fd);
  fd = -1;
  if( rc != SQLITE_OK ) return rc;

  /* Parse status line: "HTTP/1.x NNN ...\r\n" */
  {
    int i;
    int statusStart = -1;
    for(i=0; i<nRaw-3; i++){
      if( pRaw[i]==' ' && statusStart<0 ){
        statusStart = i + 1;
      }else if( statusStart>=0 && (pRaw[i]==' ' || pRaw[i]=='\r') ){
        /* Parse 3-digit status code */
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

  /* Find end of headers: \r\n\r\n */
  {
    int i;
    int bodyStart = -1;
    int contentLength = -1;

    for(i=0; i<nRaw-3; i++){
      /* Look for Content-Length header */
      if( (i==0 || pRaw[i-1]=='\n') &&
          nRaw-i > 16 &&
          (pRaw[i]=='C' || pRaw[i]=='c') ){
        /* Case-insensitive check for "Content-Length:" */
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
      /* No headers found — treat entire raw as empty response */
      sqlite3_free(pRaw);
      return SQLITE_OK;
    }

    /* Extract body */
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

/* ----------------------------------------------------------------
** Append to dynamic upload buffer (realloc pattern).
** ---------------------------------------------------------------- */

static int uploadBufAppend(HttpRemote *p, const u8 *pData, int nData){
  if( p->nUploadBuf + nData > p->nUploadBufAlloc ){
    i64 nNew = p->nUploadBufAlloc ? p->nUploadBufAlloc * 2 : 4096;
    while( nNew < p->nUploadBuf + nData ) nNew *= 2;
    u8 *pNew = sqlite3_realloc64(p->pUploadBuf, nNew);
    if( !pNew ) return SQLITE_NOMEM;
    p->pUploadBuf = pNew;
    p->nUploadBufAlloc = nNew;
  }
  memcpy(p->pUploadBuf + p->nUploadBuf, pData, nData);
  p->nUploadBuf += nData;
  return SQLITE_OK;
}

/* ----------------------------------------------------------------
** Build a full path: basePath + suffix
** ---------------------------------------------------------------- */

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

/* ----------------------------------------------------------------
** VTable implementations
** ---------------------------------------------------------------- */

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

  /* Build request body: nHash * PROLLY_HASH_SIZE bytes (concatenated hashes) */
  nReqBody = nHash * PROLLY_HASH_SIZE;
  pReqBody = sqlite3_malloc(nReqBody);
  if( !pReqBody ) return SQLITE_NOMEM;
  memcpy(pReqBody, aHash, nReqBody);

  zPath = buildPath(p, "/has-chunks");
  if( !zPath ){
    sqlite3_free(pReqBody);
    return SQLITE_NOMEM;
  }

  rc = httpRequest(p->zHost, p->port, "POST", zPath,
                   pReqBody, nReqBody, &status, &pResp, &nResp);
  sqlite3_free(pReqBody);
  sqlite3_free(zPath);

  if( rc != SQLITE_OK ) return rc;
  if( status != 200 ){
    sqlite3_free(pResp);
    return SQLITE_ERROR;
  }

  /* Response: nHash bytes, each 0 or 1 */
  if( nResp >= nHash ){
    memcpy(aResult, pResp, nHash);
  }else{
    /* Short response — treat missing as absent */
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

  hashToHex(pHash, zHex);
  snprintf(zSuffix, sizeof(zSuffix), "/chunk/%s", zHex);
  zPath = buildPath(p, zSuffix);
  if( !zPath ) return SQLITE_NOMEM;

  rc = httpRequest(p->zHost, p->port, "GET", zPath,
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

  /* Append to upload buffer: hash(20) + length(4 LE) + data(length) */
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

  rc = httpRequest(p->zHost, p->port, "GET", zPath,
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

static int httpSetRefs(DoltliteRemote *pRemote, const u8 *pData, int nData){
  HttpRemote *p = (HttpRemote*)pRemote;

  /* Free any previous pending refs */
  sqlite3_free(p->pPendingRefs);
  p->pPendingRefs = 0;
  p->nPendingRefs = 0;

  if( pData && nData > 0 ){
    p->pPendingRefs = sqlite3_malloc(nData);
    if( !p->pPendingRefs ) return SQLITE_NOMEM;
    memcpy(p->pPendingRefs, pData, nData);
    p->nPendingRefs = nData;
  }
  return SQLITE_OK;
}

static int httpCommit(DoltliteRemote *pRemote){
  HttpRemote *p = (HttpRemote*)pRemote;
  int status = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int rc;
  char *zPath;

  /* 1. If upload buffer has data: POST /v1/chunks with entire buffer */
  if( p->pUploadBuf && p->nUploadBuf > 0 ){
    zPath = buildPath(p, "/chunks");
    if( !zPath ) return SQLITE_NOMEM;

    rc = httpRequest(p->zHost, p->port, "POST", zPath,
                     p->pUploadBuf, (int)p->nUploadBuf,
                     &status, &pResp, &nResp);
    sqlite3_free(zPath);
    sqlite3_free(pResp);
    if( rc != SQLITE_OK ) return rc;
    if( status != 200 && status != 204 ) return SQLITE_ERROR;
  }

  /* 2. If pending refs: PUT /v1/refs with refs data */
  if( p->pPendingRefs && p->nPendingRefs > 0 ){
    pResp = 0; nResp = 0; status = 0;
    zPath = buildPath(p, "/refs");
    if( !zPath ) return SQLITE_NOMEM;

    rc = httpRequest(p->zHost, p->port, "PUT", zPath,
                     p->pPendingRefs, p->nPendingRefs,
                     &status, &pResp, &nResp);
    sqlite3_free(zPath);
    sqlite3_free(pResp);
    if( rc != SQLITE_OK ) return rc;
    if( status != 200 && status != 204 ) return SQLITE_ERROR;
  }

  /* 3. POST /v1/commit to finalize */
  {
    pResp = 0; nResp = 0; status = 0;
    zPath = buildPath(p, "/commit");
    if( !zPath ) return SQLITE_NOMEM;

    rc = httpRequest(p->zHost, p->port, "POST", zPath,
                     0, 0, &status, &pResp, &nResp);
    sqlite3_free(zPath);
    sqlite3_free(pResp);
    if( rc != SQLITE_OK ) return rc;
    if( status != 200 && status != 204 ) return SQLITE_ERROR;
  }

  /* 4. Clear buffers */
  sqlite3_free(p->pUploadBuf);
  p->pUploadBuf = 0;
  p->nUploadBuf = 0;
  p->nUploadBufAlloc = 0;

  sqlite3_free(p->pPendingRefs);
  p->pPendingRefs = 0;
  p->nPendingRefs = 0;

  return SQLITE_OK;
}

static void httpClose(DoltliteRemote *pRemote){
  HttpRemote *p = (HttpRemote*)pRemote;
  sqlite3_free(p->zHost);
  sqlite3_free(p->zBasePath);
  sqlite3_free(p->pUploadBuf);
  sqlite3_free(p->pPendingRefs);
  sqlite3_free(p);
}

/* ----------------------------------------------------------------
** Constructor: parse URL and create HttpRemote
** ---------------------------------------------------------------- */

/*
** Open an HTTP remote. Parses URL like "http://host:port/path"
**
** Examples:
**   http://localhost:8080/mydb       -> host=localhost, port=8080, path=/mydb
**   http://example.com:3000/myrepo   -> host=example.com, port=3000, path=/myrepo
*/
DoltliteRemote *doltliteHttpRemoteOpen(const char *zUrl){
  HttpRemote *p;
  const char *zAfterScheme;
  const char *zHostStart;
  const char *zPortStart;
  const char *zPathStart;
  int nHost;
  int port = 80;
  int nUserPath;
  int nBasePath;

  if( !zUrl ) return 0;

  /* Skip "http://" */
  if( strncmp(zUrl, "http://", 7) != 0 ) return 0;
  zAfterScheme = zUrl + 7;
  zHostStart = zAfterScheme;

  /* Find end of host (colon for port, slash for path, or end of string) */
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
      if( port <= 0 ) port = 80;
      if( *c == '/' ) zPathStart = c;
    }else if( *c == '/' ){
      zPathStart = c;
    }
  }

  if( nHost <= 0 ) return 0;

  /* Calculate user path length (0 if no path, excludes trailing slash) */
  nUserPath = 0;
  if( zPathStart ){
    nUserPath = (int)strlen(zPathStart);
    /* Strip trailing slash */
    while( nUserPath > 0 && zPathStart[nUserPath-1] == '/' ){
      nUserPath--;
    }
  }

  /* Allocate and populate */
  p = sqlite3_malloc(sizeof(HttpRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(HttpRemote));

  /* Copy host */
  p->zHost = sqlite3_malloc(nHost + 1);
  if( !p->zHost ){
    sqlite3_free(p);
    return 0;
  }
  memcpy(p->zHost, zHostStart, nHost);
  p->zHost[nHost] = 0;

  p->port = port;

  /* Build base path from URL path (e.g., "/dbname") */
  nBasePath = nUserPath;
  if( nBasePath <= 0 ){
    /* No path in URL -- invalid for multi-db server */
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

  /* Wire up vtable */
  p->base.xGetChunk = httpGetChunk;
  p->base.xPutChunk = httpPutChunk;
  p->base.xHasChunks = httpHasChunks;
  p->base.xGetRefs = httpGetRefs;
  p->base.xSetRefs = httpSetRefs;
  p->base.xCommit = httpCommit;
  p->base.xClose = httpClose;

  return &p->base;
}

#endif /* !_WIN32 */
#endif /* DOLTLITE_PROLLY */
