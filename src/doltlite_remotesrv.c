
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "doltlite_remotesrv.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_tls.h"
#include "doltlite_creds.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#ifndef DOLTLITE_HAVE_AUTH

int doltliteServerPort(DoltliteServer *s){ (void)s; return 0; }
#else
#include "doltlite_net.h"
#include <pthread.h>
#include <errno.h>

/* Bound resource use while keeping any one slow client off the accept loop. */
#define SERVER_WORKERS 4
#define SERVER_QUEUE_SIZE 16
#define SERVER_DEFAULT_TIMEOUT_MS 30000

typedef struct DoltliteWorkerArg DoltliteWorkerArg;

struct DoltliteWorkerArg {
  DoltliteServer *pSrv;
  int index;
};

struct DoltliteServer {
  int listenFd;
  int port;
  int running;
  char *zDir;
  pthread_t thread;
  pthread_t workers[SERVER_WORKERS];
  DoltliteWorkerArg workerArgs[SERVER_WORKERS];
  int nWorkers;
  pthread_mutex_t mutex;
  pthread_cond_t workReady;
  int mutexInit;
  int condInit;
  int queue[SERVER_QUEUE_SIZE];
  int queueHead;
  int nQueue;
  int activeFd[SERVER_WORKERS];
  int timeoutMs;

  DoltliteTlsServer *tls;
  char *authKeysDir;
  char *audience;
};

static void sendResponse(DoltliteConn *fd, int status, const char *zStatus,
                         const u8 *pBody, int nBody){
  char zHeader[256];
  int nHeader;
  sqlite3_snprintf(sizeof(zHeader), zHeader,
    "HTTP/1.1 %d %s\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n",
    status, zStatus, nBody);
  nHeader = (int)strlen(zHeader);
  if( doltliteConnWriteAll(fd, zHeader, nHeader)!=0 ) return;
  if( pBody && nBody>0 ){
    doltliteConnWriteAll(fd, pBody, nBody);
  }
}

static void sendOk(DoltliteConn *fd, const u8 *pBody, int nBody){
  sendResponse(fd, 200, "OK", pBody, nBody);
}

static void sendNotFound(DoltliteConn *fd){
  sendResponse(fd, 404, "Not Found", (const u8*)"Not Found", 9);
}

static void sendBadRequest(DoltliteConn *fd){
  sendResponse(fd, 400, "Bad Request", (const u8*)"Bad Request", 11);
}

static void sendError(DoltliteConn *fd){
  sendResponse(fd, 500, "Internal Server Error",
               (const u8*)"Internal Server Error", 21);
}

static void sendPayloadTooLarge(DoltliteConn *fd){
  sendResponse(fd, 413, "Payload Too Large",
               (const u8*)"Payload Too Large", 17);
}

static void sendConflict(DoltliteConn *fd){
  sendResponse(fd, 409, "Conflict", (const u8*)"Conflict", 8);
}

static void sendUnauthorized(DoltliteConn *fd){
  sendResponse(fd, 401, "Unauthorized", (const u8*)"Unauthorized", 12);
}

static int remoteSrvCommitPending(ChunkStore *pStore){
  int rc = chunkStoreCommit(pStore);
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
  }
  return rc;
}

#define MAX_HEADER_SIZE 4096

/* Request caps for an unauthenticated protocol. */
#define MAX_CHUNK_BYTES   (64 * 1024 * 1024)   /* 64 MiB single chunk */
#define MAX_REQUEST_BYTES (128 * 1024 * 1024)  /* 128 MiB total body */

static int readExact(DoltliteConn *fd, u8 *pBuf, int nBytes){
  int nRead = 0;
  while( nRead < nBytes ){
    int n = doltliteConnRead(fd, pBuf + nRead, nBytes - nRead);
    if( n <= 0 ) return -1;
    nRead += n;
  }
  return 0;
}

static int headerNameEquals(
  const char *zName,
  int nName,
  const char *zExpected
){
  int nExpected = (int)strlen(zExpected);
  return nName==nExpected
      && sqlite3_strnicmp(zName, zExpected, nExpected)==0;
}

static int parseContentLength(
  const char *zValue,
  const char *zEnd,
  int *pnValue
){
  i64 nValue = 0;
  const char *p = zValue;

  while( p<zEnd && (*p==' ' || *p=='\t') ) p++;
  while( zEnd>p && (zEnd[-1]==' ' || zEnd[-1]=='\t') ) zEnd--;
  if( p==zEnd ) return -1;

  while( p<zEnd ){
    int digit;
    if( *p<'0' || *p>'9' ) return -1;
    digit = *p - '0';
    if( nValue>(MAX_REQUEST_BYTES-digit)/10 ) return -2;
    nValue = nValue*10 + digit;
    p++;
  }
  *pnValue = (int)nValue;
  return 0;
}

static int parseRequest(
  DoltliteConn *fd,
  char *zMethod, int nMethodMax,
  char *zPath, int nPathMax,
  char *zAuth, int nAuthMax,
  u8 **ppBody, int *pnBody
){
  char aBuf[MAX_HEADER_SIZE];
  int nBuf = 0;
  int headerEnd = 0;
  int contentLength = 0;
  int seenContentLength = 0;
  int seenAuthorization = 0;
  char *p;

  *ppBody = 0;
  *pnBody = 0;
  if( zAuth && nAuthMax>0 ) zAuth[0] = '\0';

  while( nBuf < MAX_HEADER_SIZE-1 ){
    int n = doltliteConnRead(fd, &aBuf[nBuf], 1);
    if( n <= 0 ) return -1;
    nBuf++;
    if( nBuf>=4
     && aBuf[nBuf-4]=='\r' && aBuf[nBuf-3]=='\n'
     && aBuf[nBuf-2]=='\r' && aBuf[nBuf-1]=='\n' ){
      headerEnd = 1;
      break;
    }
  }
  if( !headerEnd ) return -1;
  aBuf[nBuf] = '\0';

  p = aBuf;
  {
    char *pSpace = strchr(p, ' ');
    int len;
    if( !pSpace ) return -1;
    len = (int)(pSpace - p);
    if( len >= nMethodMax ) len = nMethodMax - 1;
    memcpy(zMethod, p, len);
    zMethod[len] = '\0';
    p = pSpace + 1;
  }
  {
    char *pSpace = strchr(p, ' ');
    int len;
    if( !pSpace ) return -1;
    len = (int)(pSpace - p);
    if( len >= nPathMax ) len = nPathMax - 1;
    memcpy(zPath, p, len);
    zPath[len] = '\0';
    p = pSpace + 1;
  }

  p = strstr(aBuf, "\r\n");
  if( !p ) return -1;
  p += 2;
  while( p[0]!='\r' || p[1]!='\n' ){
    char *pEnd = strstr(p, "\r\n");
    char *pColon;
    char *pValue;
    char *pValueEnd;
    int nName;
    int i;

    if( !pEnd ) return -1;
    pColon = (char*)memchr(p, ':', (size_t)(pEnd - p));
    if( !pColon || pColon==p ) return -1;
    nName = (int)(pColon - p);
    for(i=0; i<nName; i++){
      if( p[i]==' ' || p[i]=='\t' ) return -1;
    }
    pValue = pColon + 1;
    pValueEnd = pEnd;

    if( headerNameEquals(p, nName, "Content-Length") ){
      int rc;
      if( seenContentLength ) return -1;
      seenContentLength = 1;
      rc = parseContentLength(pValue, pValueEnd, &contentLength);
      if( rc!=0 ) return rc;
    }else if( headerNameEquals(p, nName, "Transfer-Encoding") ){
      /* Chunked request bodies are not implemented. Reject the request
      ** instead of treating its first chunk as another HTTP request. */
      return -1;
    }else if( zAuth && nAuthMax>0
           && headerNameEquals(p, nName, "Authorization") ){
      int len;
      if( seenAuthorization ) return -1;
      seenAuthorization = 1;
      while( pValue<pValueEnd && (*pValue==' ' || *pValue=='\t') ) pValue++;
      while( pValueEnd>pValue
          && (pValueEnd[-1]==' ' || pValueEnd[-1]=='\t') ) pValueEnd--;
      len = (int)(pValueEnd - pValue);
      if( len >= nAuthMax ) len = nAuthMax - 1;
      memcpy(zAuth, pValue, len);
      zAuth[len] = '\0';
    }
    p = pEnd + 2;
  }

  if( contentLength > 0 ){
    u8 *pBody = (u8*)sqlite3_malloc(contentLength);
    if( !pBody ) return -1;
    if( readExact(fd, pBody, contentLength)!=0 ){
      sqlite3_free(pBody);
      return -1;
    }
    *ppBody = pBody;
    *pnBody = contentLength;
  }

  return 0;
}

static int parsePath(
  const char *zPath,
  char *zDbName, int nDbNameMax,
  char *zEndpoint, int nEndpointMax
){
  const char *p = zPath;
  const char *dbStart;
  const char *epStart;
  int dbLen, epLen;

  if( *p != '/' ) return -1;
  p++;

  dbStart = p;
  while( *p && *p != '/' ) p++;
  dbLen = (int)(p - dbStart);
  if( dbLen <= 0 || dbLen >= nDbNameMax ) return -1;
  memcpy(zDbName, dbStart, dbLen);
  zDbName[dbLen] = '\0';

  if( *p != '/' ) return -1;
  p++;

  epStart = p;
  epLen = (int)strlen(epStart);
  if( epLen <= 0 || epLen >= nEndpointMax ) return -1;
  memcpy(zEndpoint, epStart, epLen);
  zEndpoint[epLen] = '\0';

  return 0;
}

static int isSafeDbName(const char *zDbName){
  int i;
  /* Reject any leading-dot name. This covers "." and ".." plus dotfiles
  ** like ".env", ".bashrc", ".gitignore" that an attacker could otherwise
  ** plant in the served directory.
  */
  if( zDbName[0]=='.' ) return 0;
  for(i=0; zDbName[i]; i++){
    char c = zDbName[i];
    if( (c>='a' && c<='z')
     || (c>='A' && c<='Z')
     || (c>='0' && c<='9')
     || c=='_' || c=='-' || c=='.' ){
      continue;
    }
    return 0;
  }
  return 1;
}

static void handleGetRoot(ChunkStore *pStore, DoltliteConn *fd){
  ProllyHash root;
  const char *zDef = chunkStoreGetDefaultBranch(pStore);
  if( zDef && chunkStoreFindBranch(pStore, zDef, &root)==SQLITE_OK ){
    sendOk(fd, root.data, PROLLY_HASH_SIZE);
  }else{
    memset(&root, 0, sizeof(root));
    sendOk(fd, root.data, PROLLY_HASH_SIZE);
  }
}

static void handleHasChunks(ChunkStore *pStore, DoltliteConn *fd,
                            const u8 *pBody, int nBody){
  int nHashes;
  u8 *aResult;
  int rc;

  if( nBody % PROLLY_HASH_SIZE != 0 ){
    sendBadRequest(fd);
    return;
  }
  nHashes = nBody / PROLLY_HASH_SIZE;
  if( nHashes == 0 ){
    sendOk(fd, 0, 0);
    return;
  }

  aResult = (u8*)sqlite3_malloc(nHashes);
  if( !aResult ){
    sendError(fd);
    return;
  }
  memset(aResult, 0, nHashes);
  if( !pStore ){
    sendOk(fd, aResult, nHashes);
    sqlite3_free(aResult);
    return;
  }

  rc = chunkStoreHasMany(pStore, (const ProllyHash*)pBody,
                         nHashes, aResult);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aResult);
    sendError(fd);
    return;
  }
  sendOk(fd, aResult, nHashes);
  sqlite3_free(aResult);
}

/* Batched read: body is N concatenated hashes; reply is the framed form
** httpGetChunks parses -- per requested hash, a 4-byte big-endian length then
** that many payload bytes, with length 0xFFFFFFFF marking an absent chunk. */
static void handleGetChunks(ChunkStore *pStore, DoltliteConn *fd,
                            const u8 *pBody, int nBody){
  int nHashes, i, rc;
  u8 *pOut = 0;
  i64 nOut = 0, nAlloc = 0;

  if( nBody % PROLLY_HASH_SIZE != 0 ){
    sendBadRequest(fd);
    return;
  }
  nHashes = nBody / PROLLY_HASH_SIZE;

  for(i=0; i<nHashes; i++){
    const ProllyHash *pHash = (const ProllyHash*)(pBody + (i * PROLLY_HASH_SIZE));
    u8 *pData = 0;
    int nData = 0;
    u32 len;
    i64 need;

    rc = pStore ? chunkStoreGet(pStore, pHash, &pData, &nData) : SQLITE_NOTFOUND;
    if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ){
      sqlite3_free(pData);
      sqlite3_free(pOut);
      sendError(fd);
      return;
    }
    len = (rc==SQLITE_NOTFOUND) ? 0xFFFFFFFFu : (u32)nData;

    need = nOut + 4 + (rc==SQLITE_OK ? nData : 0);
    if( need > nAlloc ){
      i64 nNew = nAlloc ? nAlloc*2 : 4096;
      u8 *pTmp;
      while( nNew < need ) nNew *= 2;
      pTmp = sqlite3_realloc(pOut, (int)nNew);
      if( !pTmp ){
        sqlite3_free(pData);
        sqlite3_free(pOut);
        sendError(fd);
        return;
      }
      pOut = pTmp;
      nAlloc = nNew;
    }

    pOut[nOut++] = (u8)(len >> 24);
    pOut[nOut++] = (u8)(len >> 16);
    pOut[nOut++] = (u8)(len >> 8);
    pOut[nOut++] = (u8)len;
    if( rc==SQLITE_OK ){
      memcpy(pOut + nOut, pData, nData);
      nOut += nData;
    }
    sqlite3_free(pData);
  }

  sendOk(fd, pOut, (int)nOut);
  sqlite3_free(pOut);
}

static void handleGetChunk(ChunkStore *pStore, DoltliteConn *fd, const char *zHexHash){
  ProllyHash hash;
  u8 *pData = 0;
  int nData = 0;
  int rc;

  if( (int)strlen(zHexHash) < PROLLY_HASH_SIZE*2 ){
    sendBadRequest(fd);
    return;
  }

  if( doltliteHexToHash(zHexHash, &hash)!=SQLITE_OK ){
    sendBadRequest(fd);
    return;
  }

  rc = chunkStoreGet(pStore, &hash, &pData, &nData);
  if( rc==SQLITE_NOTFOUND ){
    sendNotFound(fd);
    return;
  }
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, pData, nData);
  sqlite3_free(pData);
}

static void handlePostChunks(ChunkStore *pStore, DoltliteConn *fd,
                             const u8 *pBody, int nBody){
  int offset = 0;
  int rc;

  while( offset + PROLLY_HASH_SIZE + 4 <= nBody ){
    u32 len;
    ProllyHash hash;

    offset += PROLLY_HASH_SIZE;

    len = (u32)pBody[offset]
        | ((u32)pBody[offset+1] << 8)
        | ((u32)pBody[offset+2] << 16)
        | ((u32)pBody[offset+3] << 24);
    offset += 4;

    /* Compare as unsigned and cap single-chunk memory use. */
    if( len > (u32)MAX_CHUNK_BYTES
     || len > (u32)(nBody - offset) ){
      sendBadRequest(fd);
      return;
    }

    rc = chunkStorePut(pStore, pBody + offset, (int)len, &hash);
    if( rc!=SQLITE_OK ){
      chunkStoreRollback(pStore);
      sendError(fd);
      return;
    }
    offset += (int)len;
  }

  rc = remoteSrvCommitPending(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handleGetRefs(ChunkStore *pStore, DoltliteConn *fd){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  if( prollyHashIsEmpty(refsTableGetHash(&pStore->refs)) ){
    sendNotFound(fd);
    return;
  }

  rc = chunkStoreGet(pStore, refsTableGetHash(&pStore->refs), &pData, &nData);
  if( rc==SQLITE_NOTFOUND ){
    sendNotFound(fd);
    return;
  }
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, pData, nData);
  sqlite3_free(pData);
}

static int remoteSrvPersistRefs(ChunkStore *pStore){
  int rc = chunkStoreSerializeRefs(pStore);
  if( rc==SQLITE_OK ) rc = remoteSrvCommitPending(pStore);
  else chunkStoreRollback(pStore);
  return rc;
}

static int remoteSrvApplyRefs(ChunkStore *pStore, const char *zBranch,
                              int bForce, const u8 *pBody, int nBody){
  int rc;

  if( nBody<=0 ) return SQLITE_ERROR;
  rc = doltliteValidateScopedRefsUpdate(pStore, pBody, nBody, zBranch, bForce);
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
    return rc;
  }
  rc = chunkStoreInstallRefsBlob(pStore, pBody, nBody);
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
    return rc;
  }
  return remoteSrvCommitPending(pStore);
}

static int remoteSrvApplyRefsIf(
  ChunkStore *pStore,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pBody,
  int nBody
){
  int rc;
  if( nBody<=0 ) return SQLITE_ERROR;
  rc = chunkStoreLockAndRefresh(pStore);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(refsTableGetHash(&pStore->refs), pExpectedRefsHash)!=0 ){
    chunkStoreUnlock(pStore);
    return SQLITE_BUSY;
  }
  rc = remoteSrvApplyRefs(pStore, zBranch, bForce, pBody, nBody);
  chunkStoreUnlock(pStore);
  return rc;
}

/* Parse the [u16 branchLen][branch][u8 force] scope prefix a push prepends to
** the refs body. On success *pzBranch is an owned string the caller frees, and
** the rest output points at the remainder (refs blob, or expectedHash+blob). */
static int remoteSrvParseRefsPrefix(
  const u8 *pBody, int nBody,
  char **pzBranch, int *pbForce, const u8 **ppRest, int *pnRest
){
  int nBranch;
  *pzBranch = 0;
  if( nBody < 3 ) return SQLITE_ERROR;
  nBranch = pBody[0] | (pBody[1] << 8);
  if( 2 + nBranch + 1 > nBody ) return SQLITE_ERROR;
  *pzBranch = sqlite3_mprintf("%.*s", nBranch, (const char*)pBody + 2);
  if( !*pzBranch ) return SQLITE_NOMEM;
  *pbForce = pBody[2 + nBranch] ? 1 : 0;
  *ppRest = pBody + 2 + nBranch + 1;
  *pnRest = nBody - (2 + nBranch + 1);
  return SQLITE_OK;
}

static void handlePutRefs(ChunkStore *pStore, DoltliteConn *fd,
                          const u8 *pBody, int nBody){
  char *zBranch = 0;
  int bForce = 0;
  const u8 *pRest = 0;
  int nRest = 0;
  int rc;

  rc = remoteSrvParseRefsPrefix(pBody, nBody, &zBranch, &bForce,
                                &pRest, &nRest);
  if( rc!=SQLITE_OK || nRest<=0 ){
    sqlite3_free(zBranch);
    sendBadRequest(fd);
    return;
  }

  rc = remoteSrvApplyRefs(pStore, zBranch, bForce, pRest, nRest);
  sqlite3_free(zBranch);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handlePutRefsIf(ChunkStore *pStore, DoltliteConn *fd,
                            const u8 *pBody, int nBody){
  ProllyHash expectedRefsHash;
  char *zBranch = 0;
  int bForce = 0;
  const u8 *pRest = 0;
  int nRest = 0;
  int rc;

  rc = remoteSrvParseRefsPrefix(pBody, nBody, &zBranch, &bForce,
                                &pRest, &nRest);
  if( rc!=SQLITE_OK || nRest<=PROLLY_HASH_SIZE ){
    sqlite3_free(zBranch);
    sendBadRequest(fd);
    return;
  }
  memcpy(expectedRefsHash.data, pRest, PROLLY_HASH_SIZE);

  rc = remoteSrvApplyRefsIf(pStore, &expectedRefsHash, zBranch, bForce,
                            pRest + PROLLY_HASH_SIZE,
                            nRest - PROLLY_HASH_SIZE);
  sqlite3_free(zBranch);
  if( rc==SQLITE_BUSY ){
    sendConflict(fd);
    return;
  }
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

int doltliteRemoteSrvCommitPendingForTest(ChunkStore *pStore){
  return remoteSrvCommitPending(pStore);
}

int doltliteRemoteSrvApplyRefsForTest(
  ChunkStore *pStore, const u8 *pBody, int nBody
){
  /* Exercises the install/rollback path only; scope validation is covered
  ** separately. */
  int rc;
  if( nBody<=0 ) return SQLITE_ERROR;
  rc = chunkStoreInstallRefsBlob(pStore, pBody, nBody);
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
    return rc;
  }
  return remoteSrvCommitPending(pStore);
}

static void handleCommit(ChunkStore *pStore, DoltliteConn *fd){
  int rc;

  rc = remoteSrvPersistRefs(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handleRequest(DoltliteServer *pSrv, DoltliteConn *fd){
  char zMethod[16];
  char zPath[512];
  char zDbName[256];
  char zEndpoint[256];
  char zAuth[1024];
  u8 *pBody = 0;
  int nBody = 0;
  ChunkStore store;
  char zDbPath[1024];
  int rc;
  int flags;
  int isReadOnlyEndpoint = 0;
  int isHasChunksEndpoint = 0;
  int isGetChunksEndpoint = 0;
  int exists = 0;
  sqlite3_vfs *pVfs;

  rc = parseRequest(fd, zMethod, sizeof(zMethod),
                    zPath, sizeof(zPath),
                    zAuth, sizeof(zAuth), &pBody, &nBody);
  if( rc!=0 ){
    if( rc==-2 ){
      sendPayloadTooLarge(fd);
    }else{
      sendBadRequest(fd);
    }
    return;
  }

  if( pSrv->authKeysDir ){
    if( doltliteCredsVerifyBearer(zAuth, pSrv->audience, pSrv->authKeysDir,
                                  (long)time(NULL), 0)!=0 ){
      sendUnauthorized(fd);
      sqlite3_free(pBody);
      return;
    }
  }

  if( parsePath(zPath, zDbName, sizeof(zDbName),
                zEndpoint, sizeof(zEndpoint))!=0 ){
    sendNotFound(fd);
    sqlite3_free(pBody);
    return;
  }
  if( !isSafeDbName(zDbName) ){
    sendNotFound(fd);
    sqlite3_free(pBody);
    return;
  }

  sqlite3_snprintf(sizeof(zDbPath), zDbPath, "%s/%s", pSrv->zDir, zDbName);
  isHasChunksEndpoint = strcmp(zMethod, "POST")==0
                     && strcmp(zEndpoint, "has-chunks")==0;
  isGetChunksEndpoint = strcmp(zMethod, "POST")==0
                     && strcmp(zEndpoint, "get-chunks")==0;
  isReadOnlyEndpoint = strcmp(zMethod, "GET")==0
                     || isHasChunksEndpoint || isGetChunksEndpoint;
  pVfs = sqlite3_vfs_find(0);
  rc = pVfs->xAccess(pVfs, zDbPath, SQLITE_ACCESS_EXISTS, &exists);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    sqlite3_free(pBody);
    return;
  }
  if( !exists && isHasChunksEndpoint ){
    handleHasChunks(0, fd, pBody, nBody);
    sqlite3_free(pBody);
    return;
  }
  if( !exists && isReadOnlyEndpoint ){
    sendNotFound(fd);
    sqlite3_free(pBody);
    return;
  }

  flags = isReadOnlyEndpoint
        ? (SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB)
        : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  memset(&store, 0, sizeof(store));
  rc = chunkStoreOpen(&store, pVfs, zDbPath, flags);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    sqlite3_free(pBody);
    return;
  }

  if( strcmp(zMethod, "GET")==0 ){
    if( strcmp(zEndpoint, "root")==0 ){
      handleGetRoot(&store, fd);
    }else if( strncmp(zEndpoint, "chunk/", 6)==0 ){
      handleGetChunk(&store, fd, zEndpoint + 6);
    }else if( strcmp(zEndpoint, "refs")==0 ){
      handleGetRefs(&store, fd);
    }else{
      sendNotFound(fd);
    }
  }else if( strcmp(zMethod, "POST")==0 ){
    if( strcmp(zEndpoint, "has-chunks")==0 ){
      handleHasChunks(&store, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "get-chunks")==0 ){
      handleGetChunks(&store, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "chunks")==0 ){
      handlePostChunks(&store, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "commit")==0 ){
      handleCommit(&store, fd);
    }else{
      sendNotFound(fd);
    }
  }else if( strcmp(zMethod, "PUT")==0 ){
    if( strcmp(zEndpoint, "refs")==0 ){
      handlePutRefs(&store, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "refs-if")==0 ){
      handlePutRefsIf(&store, fd, pBody, nBody);
    }else{
      sendNotFound(fd);
    }
  }else{
    sendBadRequest(fd);
  }

  chunkStoreClose(&store);
  sqlite3_free(pBody);
}

static void serverCleanup(DoltliteServer *pSrv);

static void *serverWorkerEntry(void *pArg){
  DoltliteWorkerArg *pWorker = (DoltliteWorkerArg*)pArg;
  DoltliteServer *pSrv = pWorker->pSrv;

  for(;;){
    DoltliteConn *conn;
    int clientFd;

    pthread_mutex_lock(&pSrv->mutex);
    while( pSrv->nQueue==0 && pSrv->running ){
      pthread_cond_wait(&pSrv->workReady, &pSrv->mutex);
    }
    if( pSrv->nQueue==0 ){
      pthread_mutex_unlock(&pSrv->mutex);
      break;
    }
    clientFd = pSrv->queue[pSrv->queueHead];
    pSrv->queueHead = (pSrv->queueHead + 1) % SERVER_QUEUE_SIZE;
    pSrv->nQueue--;
    pSrv->activeFd[pWorker->index] = clientFd;
    pthread_mutex_unlock(&pSrv->mutex);

    conn = pSrv->tls ? doltliteConnServerAccept(pSrv->tls, clientFd)
                     : doltliteConnFromFd(clientFd);
    if( conn ){
      handleRequest(pSrv, conn);
      doltliteConnClose(conn);
    }

    pthread_mutex_lock(&pSrv->mutex);
    pSrv->activeFd[pWorker->index] = -1;
    pthread_mutex_unlock(&pSrv->mutex);
  }
  return 0;
}

static int serverStartWorkers(DoltliteServer *pSrv){
  int i;
  for(i=0; i<SERVER_WORKERS; i++){
    pSrv->workerArgs[i].pSrv = pSrv;
    pSrv->workerArgs[i].index = i;
    if( pthread_create(&pSrv->workers[i], 0, serverWorkerEntry,
                       &pSrv->workerArgs[i])!=0 ){
      pthread_mutex_lock(&pSrv->mutex);
      pSrv->running = 0;
      pthread_cond_broadcast(&pSrv->workReady);
      pthread_mutex_unlock(&pSrv->mutex);
      while( pSrv->nWorkers>0 ){
        pthread_join(pSrv->workers[--pSrv->nWorkers], 0);
      }
      return SQLITE_ERROR;
    }
    pSrv->nWorkers++;
  }
  return SQLITE_OK;
}

static void serverRequestStop(DoltliteServer *pSrv){
  int i;
  pthread_mutex_lock(&pSrv->mutex);
  pSrv->running = 0;
  while( pSrv->nQueue>0 ){
    int fd = pSrv->queue[pSrv->queueHead];
    pSrv->queueHead = (pSrv->queueHead + 1) % SERVER_QUEUE_SIZE;
    pSrv->nQueue--;
    doltliteShutdownSocket(fd);
    doltliteCloseSocket(fd);
  }
  /* Workers close active sockets. shutdown() wakes their blocking I/O without
  ** risking descriptor reuse between this thread and the owning worker. */
  for(i=0; i<pSrv->nWorkers; i++){
    if( pSrv->activeFd[i]>=0 ){
      doltliteShutdownSocket(pSrv->activeFd[i]);
    }
  }
  pthread_cond_broadcast(&pSrv->workReady);
  pthread_mutex_unlock(&pSrv->mutex);
}

static void serverJoinWorkers(DoltliteServer *pSrv){
  int i;
  for(i=0; i<pSrv->nWorkers; i++){
    pthread_join(pSrv->workers[i], 0);
  }
  pSrv->nWorkers = 0;
}

static int serverIsRunning(DoltliteServer *pSrv){
  int running;
  pthread_mutex_lock(&pSrv->mutex);
  running = pSrv->running;
  pthread_mutex_unlock(&pSrv->mutex);
  return running;
}

static char *dupStr(const char *z){
  int n;
  char *s;
  if( !z ) return 0;
  n = (int)strlen(z);
  s = sqlite3_malloc(n + 1);
  if( s ) memcpy(s, z, n + 1);
  return s;
}

static int serverInit(DoltliteServer *pSrv, const DoltliteServeOpts *o){
  struct sockaddr_in addr;
  socklen_t addrLen;
  struct in_addr bindIn;
  int opt = 1;
  int i;
  const char *zBindAddr = o->zBindAddr;

  memset(pSrv, 0, sizeof(*pSrv));
  pSrv->listenFd = -1;
  pSrv->timeoutMs = o->timeoutMs>0 ? o->timeoutMs
                                   : SERVER_DEFAULT_TIMEOUT_MS;
  for(i=0; i<SERVER_WORKERS; i++) pSrv->activeFd[i] = -1;
  if( pthread_mutex_init(&pSrv->mutex, 0)!=0 ) return SQLITE_ERROR;
  pSrv->mutexInit = 1;
  if( pthread_cond_init(&pSrv->workReady, 0)!=0 ){
    serverCleanup(pSrv);
    return SQLITE_ERROR;
  }
  pSrv->condInit = 1;

  if( zBindAddr==0 || zBindAddr[0]=='\0' ){
    zBindAddr = "127.0.0.1";
  }
  if( inet_pton(AF_INET, zBindAddr, &bindIn)!=1 ){
    serverCleanup(pSrv);
    return SQLITE_ERROR;
  }

  pSrv->zDir = dupStr(o->zDir);
  if( !pSrv->zDir ){
    serverCleanup(pSrv);
    return SQLITE_NOMEM;
  }

  if( o->authKeysDir && o->authKeysDir[0] ){
    pSrv->authKeysDir = dupStr(o->authKeysDir);
    if( !pSrv->authKeysDir ){ serverCleanup(pSrv); return SQLITE_NOMEM; }
  }
  if( o->audience && o->audience[0] ){
    pSrv->audience = dupStr(o->audience);
    if( !pSrv->audience ){ serverCleanup(pSrv); return SQLITE_NOMEM; }
  }
  if( o->certFile && o->keyFile ){
    pSrv->tls = doltliteTlsServerNew(o->certFile, o->keyFile);
    if( !pSrv->tls ){ serverCleanup(pSrv); return SQLITE_ERROR; }
  }

  if( bindIn.s_addr != htonl(INADDR_LOOPBACK) && pSrv->tls==0 ){
    fprintf(stderr,
      "WARNING: doltlite-remotesrv bound to %s without TLS — traffic is "
      "unencrypted and unauthenticated. Use --cert/--key/--auth-keys, or "
      "only do this on trusted networks or behind a reverse proxy.\n",
      zBindAddr);
  }

  if( doltliteNetInit()!=0 ){ serverCleanup(pSrv); return SQLITE_ERROR; }
  pSrv->listenFd = (int)socket(AF_INET, SOCK_STREAM, 0);
  if( pSrv->listenFd < 0 ){ serverCleanup(pSrv); return SQLITE_ERROR; }

  setsockopt(pSrv->listenFd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr = bindIn;
  addr.sin_port = htons((u16)o->port);

  if( bind(pSrv->listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0 ){
    serverCleanup(pSrv);
    return SQLITE_ERROR;
  }
  if( listen(pSrv->listenFd, 5) < 0 ){
    serverCleanup(pSrv);
    return SQLITE_ERROR;
  }

  addrLen = sizeof(addr);
  if( getsockname(pSrv->listenFd, (struct sockaddr*)&addr, &addrLen)==0 ){
    pSrv->port = ntohs(addr.sin_port);
  }else{
    pSrv->port = o->port;
  }

  pSrv->running = 1;
  if( serverStartWorkers(pSrv)!=SQLITE_OK ){
    serverCleanup(pSrv);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static void serverLoop(DoltliteServer *pSrv){
  while( serverIsRunning(pSrv) ){
    doltlite_pollfd pfd;
    int clientFd;

    pfd.fd = pSrv->listenFd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    if( doltlitePoll(&pfd, 1, 100) <= 0 ) continue;

    clientFd = (int)accept(pSrv->listenFd, NULL, NULL);
    if( clientFd < 0 ) continue;
    if( doltliteSocketSetTimeout(clientFd, pSrv->timeoutMs)!=0 ){
      doltliteCloseSocket(clientFd);
      continue;
    }

    pthread_mutex_lock(&pSrv->mutex);
    if( !pSrv->running || pSrv->nQueue==SERVER_QUEUE_SIZE ){
      pthread_mutex_unlock(&pSrv->mutex);
      doltliteCloseSocket(clientFd);
      continue;
    }
    pSrv->queue[(pSrv->queueHead + pSrv->nQueue) % SERVER_QUEUE_SIZE] = clientFd;
    pSrv->nQueue++;
    pthread_cond_signal(&pSrv->workReady);
    pthread_mutex_unlock(&pSrv->mutex);
  }
}

static void serverCleanup(DoltliteServer *pSrv){
  if( pSrv->listenFd >= 0 ){
    doltliteCloseSocket(pSrv->listenFd);
    pSrv->listenFd = -1;
  }
  if( pSrv->tls ){
    doltliteTlsServerFree(pSrv->tls);
    pSrv->tls = 0;
  }
  sqlite3_free(pSrv->zDir);
  pSrv->zDir = 0;
  sqlite3_free(pSrv->authKeysDir);
  pSrv->authKeysDir = 0;
  sqlite3_free(pSrv->audience);
  pSrv->audience = 0;
  if( pSrv->condInit ){
    pthread_cond_destroy(&pSrv->workReady);
    pSrv->condInit = 0;
  }
  if( pSrv->mutexInit ){
    pthread_mutex_destroy(&pSrv->mutex);
    pSrv->mutexInit = 0;
  }
}

int doltliteRemoteSrvInitForTest(
  const DoltliteServeOpts *o,
  int *pMutexInit,
  int *pCondInit
){
  DoltliteServer server;
  int rc;

  rc = serverInit(&server, o);
  *pMutexInit = server.mutexInit;
  *pCondInit = server.condInit;
  if( rc==SQLITE_OK ){
    serverRequestStop(&server);
    serverJoinWorkers(&server);
  }
  serverCleanup(&server);
  return rc;
}

int doltliteServeOpts(const DoltliteServeOpts *o){
  DoltliteServer server;
  int rc;

  rc = serverInit(&server, o);
  if( rc!=SQLITE_OK ) return rc;

  serverLoop(&server);
  serverRequestStop(&server);
  serverJoinWorkers(&server);
  serverCleanup(&server);
  return SQLITE_OK;
}

int doltliteServe(const char *zDir, int port, const char *zBindAddr){
  DoltliteServeOpts o;
  memset(&o, 0, sizeof(o));
  o.zDir = zDir;
  o.port = port;
  o.zBindAddr = zBindAddr;
  return doltliteServeOpts(&o);
}

static void *serverThreadEntry(void *pArg){
  DoltliteServer *pSrv = (DoltliteServer*)pArg;
  serverLoop(pSrv);
  return 0;
}

DoltliteServer *doltliteServeAsyncOpts(const DoltliteServeOpts *o){
  DoltliteServer *pSrv;
  int rc;

  pSrv = (DoltliteServer*)sqlite3_malloc(sizeof(DoltliteServer));
  if( !pSrv ) return 0;

  rc = serverInit(pSrv, o);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pSrv);
    return 0;
  }

  if( pthread_create(&pSrv->thread, 0, serverThreadEntry, pSrv)!=0 ){
    serverRequestStop(pSrv);
    serverJoinWorkers(pSrv);
    serverCleanup(pSrv);
    sqlite3_free(pSrv);
    return 0;
  }

  return pSrv;
}

DoltliteServer *doltliteServeAsync(const char *zDir, int port,
                                   const char *zBindAddr){
  DoltliteServeOpts o;
  memset(&o, 0, sizeof(o));
  o.zDir = zDir;
  o.port = port;
  o.zBindAddr = zBindAddr;
  return doltliteServeAsyncOpts(&o);
}

void doltliteServerStop(DoltliteServer *pServer){
  if( !pServer ) return;
  serverRequestStop(pServer);
  pthread_join(pServer->thread, 0);
  serverJoinWorkers(pServer);
  serverCleanup(pServer);
  sqlite3_free(pServer);
}

int doltliteServerPort(DoltliteServer *pServer){
  return pServer ? pServer->port : 0;
}

#endif
#endif
