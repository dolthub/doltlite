
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "doltlite_remotesrv.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_tls.h"
#include "doltlite_creds.h"
#include "doltlite_parse.h"

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

/* Workers run only after the request has arrived. Idle connections
** stay on the accept thread, so they cannot fill the pool. */
#define SERVER_WORKERS 4
#define SERVER_QUEUE_SIZE 16
#define SERVER_PENDING_MAX 64
#define SERVER_DEFAULT_TIMEOUT_MS 30000
#define SERVER_HEADER_TIMEOUT_MS 2000
#define SERVER_DB_CACHE_MAX 32
#define MAX_HEADER_SIZE 4096

typedef struct DoltliteWorkerArg DoltliteWorkerArg;
typedef struct RemoteDbHandle RemoteDbHandle;
typedef struct PendingConn PendingConn;

struct ServerJob {
  int fd;
  DoltliteConn *conn;
  char *zPrefix;
  int nPrefix;
  int nHeader;
  u8 *pBody;
  int nBody;
  i64 nBodyAlloc;
  int requestStatus;
};

/* Request bytes collected before a worker is taken. conn is set for TLS. */
struct PendingConn {
  int fd;
  DoltliteConn *conn;
  i64 startMs;
  char aBuf[MAX_HEADER_SIZE];
  int nBuf;
  int bHandshake;
  int bWantWrite;
  int nHeader;
  int contentLength;
  u8 *pBody;
  int nBody;
  i64 nBodyAlloc;
  int requestStatus;
  int bReadAgain;
};

struct DoltliteWorkerArg {
  DoltliteServer *pSrv;
  int index;
};

/* Open ChunkStore cache keyed by db path. Workers serialize store access
** on h->mu. The HTTP write runs after that lock is dropped, so a client
** that does not read stalls only its own worker. */
struct RemoteDbHandle {
  char *zPath;
  ChunkStore store;
  int bOpen;
  int bWritable;
  int nReservations;
  i64 lastUseMs;
  pthread_mutex_t mu;
  int muInit;
  RemoteDbHandle *pNext;
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
  struct ServerJob queue[SERVER_QUEUE_SIZE];
  int queueHead;
  int nQueue;
  int activeFd[SERVER_WORKERS];
  int timeoutMs;
  PendingConn *aPending;
  i64 nBodyAlloc;

  DoltliteTlsServer *tls;
  char *authKeysDir;
  char *audience;

  RemoteDbHandle *pDbCache;
  int nDbCache;
};

/* One assembled HTTP response for the worker that is inside handleRequest.
** Filled while h->mu is held; flushed after remoteDbRelease. */
typedef struct RemoteSrvPending RemoteSrvPending;
struct RemoteSrvPending {
  u8 *pMsg;
  int nMsg;
  int bFail;
};

#if defined(_MSC_VER)
#define REMOTESRV_TLS __declspec(thread)
#else
#define REMOTESRV_TLS __thread
#endif
static REMOTESRV_TLS RemoteSrvPending *remotesrvPending;

/* Copy one slice of the response. Header and body are separate calls.
** A failed copy drops the partial message; the caller sends 500 after
** the database lock is released. */
static int remotesrvPendingAdd(const void *p, int n){
  RemoteSrvPending *pend = remotesrvPending;
  u8 *pNew;
  i64 nNew;

  if( !pend || pend->bFail ) return 1;
  if( n<=0 ) return 0;
  /* Callers cap the body. This only rejects a header+body that cannot fit
  ** in the int length passed to the socket write. */
  if( pend->nMsg > 0x7fffffff - n ){
    sqlite3_free(pend->pMsg);
    pend->pMsg = 0;
    pend->nMsg = 0;
    pend->bFail = 1;
    return 1;
  }
  nNew = (i64)pend->nMsg + (i64)n;
  pNew = (u8*)sqlite3_realloc64(pend->pMsg, (sqlite3_uint64)nNew);
  if( !pNew ){
    sqlite3_free(pend->pMsg);
    pend->pMsg = 0;
    pend->nMsg = 0;
    pend->bFail = 1;
    return 1;
  }
  memcpy(pNew + pend->nMsg, p, (size_t)n);
  pend->pMsg = pNew;
  pend->nMsg = (int)nNew;
  return 0;
}

static void sendResponseEx(
  DoltliteConn *fd,
  int status,
  const char *zStatus,
  const char *zContentType,
  const u8 *pBody,
  int nBody
){
  char zHeader[384];
  int nHeader;
  if( zContentType && zContentType[0] ){
    sqlite3_snprintf(sizeof(zHeader), zHeader,
      "HTTP/1.1 %d %s\r\n"
      "Content-Type: %s\r\n"
      "Content-Length: %d\r\n"
      "Connection: close\r\n"
      "\r\n",
      status, zStatus, zContentType, nBody);
  }else{
    sqlite3_snprintf(sizeof(zHeader), zHeader,
      "HTTP/1.1 %d %s\r\n"
      "Content-Length: %d\r\n"
      "Connection: close\r\n"
      "\r\n",
      status, zStatus, nBody);
  }
  nHeader = (int)strlen(zHeader);
  /* Database handle is still locked. Keep the bytes and let handleRequest
  ** write them after remoteDbRelease. */
  if( remotesrvPending ){
    if( remotesrvPendingAdd(zHeader, nHeader)==0 && pBody && nBody>0 ){
      remotesrvPendingAdd(pBody, nBody);
    }
    return;
  }
  if( doltliteConnWriteAll(fd, zHeader, nHeader)!=0 ) return;
  if( pBody && nBody>0 ){
    doltliteConnWriteAll(fd, pBody, nBody);
  }
}

static void sendResponse(DoltliteConn *fd, int status, const char *zStatus,
                         const u8 *pBody, int nBody){
  sendResponseEx(fd, status, zStatus, 0, pBody, nBody);
}

static void sendOk(DoltliteConn *fd, const u8 *pBody, int nBody){
  sendResponse(fd, 200, "OK", pBody, nBody);
}

/* Structured error body: {"code","sqlite","message"}. */
static void sendStructuredError(
  DoltliteConn *fd,
  int httpStatus,
  const char *zHttpReason,
  const char *zCode,
  int sqliteRc,
  const char *zMessage
){
  char zBody[512];
  int nBody;
  const char *zMsg = zMessage && zMessage[0] ? zMessage : "error";
  const char *zC = zCode && zCode[0] ? zCode : "error";
  /* Plain ASCII so known strings need no JSON escaping. */
  sqlite3_snprintf(sizeof(zBody), zBody,
    "{\"code\":\"%s\",\"sqlite\":%d,\"message\":\"%s\"}",
    zC, sqliteRc, zMsg);
  nBody = (int)strlen(zBody);
  sendResponseEx(fd, httpStatus, zHttpReason, "application/json",
                 (const u8*)zBody, nBody);
}

static void sendNotFound(DoltliteConn *fd){
  sendStructuredError(fd, 404, "Not Found", "not_found", SQLITE_NOTFOUND,
                      "not found");
}

static void sendBadRequest(DoltliteConn *fd){
  sendStructuredError(fd, 400, "Bad Request", "bad_request", SQLITE_ERROR,
                      "bad request");
}

static void sendSqliteError(DoltliteConn *fd, int rc){
  switch( rc ){
    case SQLITE_LOCKED:
      sendStructuredError(fd, 409, "Conflict", "working_set", rc,
                          "remote branch has uncommitted changes and cannot be "
                          "overwritten by push");
      return;
    case SQLITE_BUSY:
      sendStructuredError(fd, 503, "Service Unavailable", "busy", rc,
                          "database is locked by another connection");
      return;
    case SQLITE_BUSY_SNAPSHOT:
      sendStructuredError(fd, 409, "Conflict", "refs_changed", rc,
                          "remote refs changed; pull and retry");
      return;
    case SQLITE_CONSTRAINT:
      sendStructuredError(fd, 400, "Bad Request", "non_ff", rc,
                          "not a fast-forward of the remote branch "
                          "(use force to overwrite)");
      return;
    case SQLITE_NOMEM:
      sendStructuredError(fd, 500, "Internal Server Error", "nomem", rc,
                          "out of memory");
      return;
    case SQLITE_READONLY:
      sendStructuredError(fd, 500, "Internal Server Error", "readonly", rc,
                          "database is read-only");
      return;
    case SQLITE_CORRUPT:
      sendStructuredError(fd, 500, "Internal Server Error", "corrupt", rc,
                          "database is corrupt");
      return;
    case SQLITE_NOTADB:
      sendStructuredError(fd, 500, "Internal Server Error", "notadb", rc,
                          "file is not a database");
      return;
    case SQLITE_IOERR:
      sendStructuredError(fd, 500, "Internal Server Error", "io", rc,
                          "I/O error");
      return;
    case SQLITE_TOOBIG:
      sendStructuredError(fd, 413, "Payload Too Large", "toobig", rc,
                          "payload too large");
      return;
    case SQLITE_AUTH:
      sendStructuredError(fd, 401, "Unauthorized", "unauthorized", rc,
                          "unauthorized");
      return;
    case SQLITE_NOTFOUND:
      sendNotFound(fd);
      return;
    default:
      sendStructuredError(fd, 500, "Internal Server Error", "error",
                          rc ? rc : SQLITE_ERROR, "internal error");
      return;
  }
}

static void sendPayloadTooLarge(DoltliteConn *fd){
  sendStructuredError(fd, 413, "Payload Too Large", "toobig", SQLITE_TOOBIG,
                      "payload too large");
}

static void sendUnauthorized(DoltliteConn *fd){
  sendStructuredError(fd, 401, "Unauthorized", "unauthorized", SQLITE_AUTH,
                      "unauthorized");
}

static int remoteSrvBusyRetry(int nBusy){
  if( nBusy>=20 ) return 0;
  sqlite3_sleep(50);
  return 1;
}

static int remoteSrvCommitPending(ChunkStore *pStore){
  int nBusy = 0;
  int rc;
  do {
    rc = chunkStoreCommit(pStore);
  }while( rc==SQLITE_BUSY && remoteSrvBusyRetry(nBusy++) );
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
  }
  return rc;
}

/* Unauthenticated protocol caps. Response matches HTTP_RESP_MAX_BYTES so
** batch get-chunks cannot materialize an unbounded reply. */
#define MAX_CHUNK_BYTES    ((i64)1024 * 1024 * 1024)
#define MAX_REQUEST_BYTES  MAX_CHUNK_BYTES
#define MAX_RESPONSE_BYTES MAX_REQUEST_BYTES

#define SERVER_BODY_MIN_RATE     (64 * 1024)
#define SERVER_BODY_BUFFER_MAX   (MAX_REQUEST_BYTES * SERVER_WORKERS)

static i64 monotonicMs(void){
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (i64)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Grow *pp to need bytes, capped at MAX_RESPONSE_BYTES (realloc64). */
static int remoteSrvGrowBuf(u8 **pp, i64 *pnAlloc, i64 need){
  i64 nNew;
  u8 *pTmp;

  if( need<0 || need>MAX_RESPONSE_BYTES ) return SQLITE_TOOBIG;
  if( need<=*pnAlloc ) return SQLITE_OK;

  nNew = *pnAlloc>0 ? *pnAlloc : (i64)4096;
  while( nNew<need ){
    if( nNew > MAX_RESPONSE_BYTES/2 ){
      nNew = MAX_RESPONSE_BYTES;
      break;
    }
    nNew *= 2;
  }
  if( nNew<need ) return SQLITE_TOOBIG;

  pTmp = (u8*)sqlite3_realloc64(*pp, (sqlite3_uint64)nNew);
  if( !pTmp ) return SQLITE_NOMEM;
  *pp = pTmp;
  *pnAlloc = nNew;
  return SQLITE_OK;
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
  const char *p = zValue;
  uint64_t nValue;
  int rc;

  while( p<zEnd && (*p==' ' || *p=='\t') ) p++;
  while( zEnd>p && (zEnd[-1]==' ' || zEnd[-1]=='\t') ) zEnd--;
  rc = doltliteParseDecimal(p, zEnd, MAX_REQUEST_BYTES, &nValue);
  if( rc==DOLTLITE_DECIMAL_RANGE ) return -2;
  if( rc!=DOLTLITE_DECIMAL_OK ) return -1;
  *pnValue = (int)nValue;
  return 0;
}

/* zPrefix holds the header block plus any body bytes already read with it.
** nHeader is the index just past the blank line. Body bytes are not
** copied out here: a token check has to be able to reject the request
** before Content-Length is allocated. */
static int parseRequestHeaders(
  const char *zPrefix, int nPrefix, int nHeader,
  char *zMethod, int nMethodMax,
  char *zPath, int nPathMax,
  char *zAuth, int nAuthMax,
  int *pContentLength,
  int *pContentRc
){
  char aBuf[MAX_HEADER_SIZE];
  int contentLength = 0;
  int contentRc = 0;
  int seenContentLength = 0;
  int seenAuthorization = 0;
  char *p;

  *pContentLength = 0;
  *pContentRc = 0;
  if( zAuth && nAuthMax>0 ) zAuth[0] = '\0';
  if( nHeader<=0 || nHeader>nPrefix || nHeader>=MAX_HEADER_SIZE ) return -1;
  memcpy(aBuf, zPrefix, (size_t)nHeader);
  aBuf[nHeader] = '\0';

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
      if( seenContentLength ){
        if( contentRc==0 ) contentRc = -1;
      }else{
        seenContentLength = 1;
        rc = parseContentLength(pValue, pValueEnd, &contentLength);
        if( rc!=0 ) contentRc = rc;
      }
    }else if( headerNameEquals(p, nName, "Transfer-Encoding") ){
      /* Chunked request bodies are not implemented. */
      if( contentRc==0 ) contentRc = -1;
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

  *pContentLength = contentRc==0 ? contentLength : 0;
  *pContentRc = contentRc;
  return 0;
}

static int requestHeaderStatus(
  DoltliteServer *pSrv,
  const char *zPrefix, int nPrefix, int nHeader,
  int *pContentLength
){
  char zMethod[16], zPath[512], zAuth[1024];
  int contentRc;
  int rc;

  rc = parseRequestHeaders(zPrefix, nPrefix, nHeader,
                           zMethod, sizeof(zMethod), zPath, sizeof(zPath),
                           zAuth, sizeof(zAuth), pContentLength, &contentRc);
  if( rc!=0 ) return 400;
  if( pSrv->authKeysDir
   && doltliteCredsVerifyBearer(zAuth, pSrv->audience, pSrv->authKeysDir,
                               (long)time(NULL), 0)!=0 ){
    return 401;
  }
  if( contentRc==-2 ) return 413;
  return contentRc==0 ? 200 : 400;
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
  /* Reject leading-dot names (., .., and dotfiles). */
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

static int remoteSrvResolveRoot(ChunkStore *pStore, ProllyHash *pRoot){
  const char *zDef = chunkStoreGetDefaultBranch(pStore);
  int rc = zDef ? chunkStoreFindBranch(pStore, zDef, pRoot) : SQLITE_NOTFOUND;
  if( rc==SQLITE_NOTFOUND && refsTableBranchCount(&pStore->refs)==0 ){
    memset(pRoot, 0, sizeof(*pRoot));
    return SQLITE_OK;
  }
  return rc==SQLITE_NOTFOUND ? SQLITE_CORRUPT : rc;
}

int doltliteRemoteSrvResolveRootForTest(
  ChunkStore *pStore,
  ProllyHash *pRoot
){
  return remoteSrvResolveRoot(pStore, pRoot);
}

static void handleGetRoot(ChunkStore *pStore, DoltliteConn *fd){
  ProllyHash root;
  int rc = remoteSrvResolveRoot(pStore, &root);
  if( rc==SQLITE_OK ) sendOk(fd, root.data, PROLLY_HASH_SIZE);
  else sendSqliteError(fd, rc);
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

  aResult = (u8*)sqlite3_malloc64((sqlite3_uint64)nHashes);
  if( !aResult ){
    sendSqliteError(fd, SQLITE_NOMEM);
    return;
  }
  memset(aResult, 0, (size_t)nHashes);
  if( !pStore ){
    sendOk(fd, aResult, nHashes);
    sqlite3_free(aResult);
    return;
  }

  rc = chunkStoreHasMany(pStore, (const ProllyHash*)pBody,
                         nHashes, aResult);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aResult);
    sendSqliteError(fd, rc);
    return;
  }
  sendOk(fd, aResult, nHashes);
  sqlite3_free(aResult);
}

/* Body is N concatenated hashes; reply is per-hash [u32be len][bytes],
** 0xFFFFFFFF if absent. Capped at MAX_RESPONSE_BYTES (413). */
static void handleGetChunks(ChunkStore *pStore, DoltliteConn *fd,
                            const u8 *pBody, int nBody){
  int nHashes, i, j, rc = SQLITE_OK;
  int bTooLarge = 0;
  u8 *pOut = 0;
  u8 *apData[CHUNK_READ_BATCH_MAX] = {0};
  int anData[CHUNK_READ_BATCH_MAX];
  i64 nOut = 0, nAlloc = 0;

  if( nBody % PROLLY_HASH_SIZE != 0 ){
    sendBadRequest(fd);
    return;
  }
  nHashes = nBody / PROLLY_HASH_SIZE;
  for(i=0; i<nHashes; ){
    int nRead = MIN(nHashes-i, CHUNK_READ_BATCH_MAX);
    if( pStore ){
      rc = chunkStoreReadBatch(pStore, (const ProllyHash*)pBody+i, nRead,
                               apData, anData, &nRead);
      if( rc!=SQLITE_OK ) goto get_chunks_done;
    }else{
      memset(anData, 0, sizeof(anData));
    }
    for(j=0; j<nRead; j++){
      int nData = anData[j];
      u32 len = apData[j] ? (u32)nData : 0xFFFFFFFFu;
      i64 add = 4 + (apData[j] ? (i64)nData : 0);
      if( add<0 || nOut > MAX_RESPONSE_BYTES-add ){
        bTooLarge = 1;
        goto get_chunks_done;
      }
      rc = remoteSrvGrowBuf(&pOut, &nAlloc, nOut+add);
      if( rc!=SQLITE_OK ){
        bTooLarge = rc==SQLITE_TOOBIG;
        goto get_chunks_done;
      }
      pOut[nOut++] = (u8)(len >> 24);
      pOut[nOut++] = (u8)(len >> 16);
      pOut[nOut++] = (u8)(len >> 8);
      pOut[nOut++] = (u8)len;
      if( apData[j] && nData>0 ){
        memcpy(pOut+nOut, apData[j], nData);
        nOut += nData;
      }
      sqlite3_free(apData[j]);
      apData[j] = 0;
    }
    i += nRead;
  }
get_chunks_done:
  for(j=0; j<CHUNK_READ_BATCH_MAX; j++) sqlite3_free(apData[j]);
  if( bTooLarge ) sendPayloadTooLarge(fd);
  else if( rc!=SQLITE_OK ) sendSqliteError(fd, rc);
  else sendOk(fd, pOut, (int)nOut);
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
    sendSqliteError(fd, rc);
    return;
  }
  if( nData<0 || (i64)nData>MAX_RESPONSE_BYTES ){
    sqlite3_free(pData);
    sendPayloadTooLarge(fd);
    return;
  }

  sendOk(fd, pData, nData);
  sqlite3_free(pData);
}

static int remoteSrvStoreChunkBatch(
  ChunkStore *pStore,
  const u8 *pBody,
  int nBody
){
  int offset = 0;
  int rc;

  while( offset<nBody ){
    u32 len;
    ProllyHash declared;
    ProllyHash computed;

    if( nBody-offset < PROLLY_HASH_SIZE+4 ){
      rc = SQLITE_PROTOCOL;
      goto store_error;
    }
    memcpy(declared.data, pBody+offset, PROLLY_HASH_SIZE);
    offset += PROLLY_HASH_SIZE;

    len = (u32)pBody[offset]
        | ((u32)pBody[offset+1] << 8)
        | ((u32)pBody[offset+2] << 16)
        | ((u32)pBody[offset+3] << 24);
    offset += 4;

    if( (i64)len > MAX_CHUNK_BYTES
     || len > (u32)(nBody - offset) ){
      rc = SQLITE_PROTOCOL;
      goto store_error;
    }

    prollyHashCompute(pBody+offset, (int)len, &computed);
    if( prollyHashCompare(&declared, &computed)!=0 ){
      rc = SQLITE_PROTOCOL;
      goto store_error;
    }
    rc = chunkStorePut(pStore, pBody+offset, (int)len, 0);
    if( rc!=SQLITE_OK ) goto store_error;
    offset += (int)len;
  }

  return remoteSrvCommitPending(pStore);

store_error:
  chunkStoreRollback(pStore);
  return rc;
}

static void handlePostChunks(ChunkStore *pStore, DoltliteConn *fd,
                             const u8 *pBody, int nBody){
  int rc = remoteSrvStoreChunkBatch(pStore, pBody, nBody);
  if( rc==SQLITE_PROTOCOL ){
    sendBadRequest(fd);
    return;
  }
  if( rc!=SQLITE_OK ){
    sendSqliteError(fd, rc);
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
    sendSqliteError(fd, rc);
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

static int remoteSrvApplyRefsLocked(ChunkStore *pStore, const char *zBranch,
                                    int bForce, const u8 *pBody, int nBody,
                                    int *pbMissing){
  ProllyHash refsHash;
  int rc;

  if( nBody<=0 ) return SQLITE_ERROR;
  rc = doltliteValidateScopedRefsUpdate(pStore, pBody, nBody, zBranch,
                                        bForce, pbMissing);
  if( rc==SQLITE_OK ){
    prollyHashCompute(pBody, nBody, &refsHash);
    if( prollyHashCompare(refsTableGetHash(&pStore->refs), &refsHash)==0 ){
      return SQLITE_OK;
    }
  }
  if( rc==SQLITE_OK ){
    rc = doltliteValidateRefsTargetGraph(pStore, pBody, nBody, zBranch,
                                         pbMissing);
    if( rc==SQLITE_CORRUPT && *pbMissing ) rc = SQLITE_BUSY_SNAPSHOT;
  }
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
    return rc;
  }
  rc = doltliteInstallPushedRefsBlob(pStore, pBody, nBody, zBranch);
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(pStore);
    return rc;
  }
  return remoteSrvCommitPending(pStore);
}

static int remoteSrvLockAndForceRefresh(ChunkStore *pStore){
  int nBusy = 0;
  int rc;
  do {
    rc = chunkStoreLockAndRefresh(pStore);
  }while( rc==SQLITE_BUSY && remoteSrvBusyRetry(nBusy++) );
  if( rc==SQLITE_OK ){
    rc = chunkStoreForceRefresh(pStore);
    if( rc==SQLITE_CANTOPEN || rc==SQLITE_NOTADB ) rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) chunkStoreUnlock(pStore);
  return rc;
}

static int remoteSrvApplyRefs(ChunkStore *pStore, const char *zBranch,
                              int bForce, const u8 *pBody, int nBody,
                              int *pbMissing){
  int rc;
  if( nBody<=0 ) return SQLITE_ERROR;
  rc = remoteSrvLockAndForceRefresh(pStore);
  if( rc!=SQLITE_OK ) return rc;
  rc = remoteSrvApplyRefsLocked(pStore, zBranch, bForce, pBody, nBody,
                                pbMissing);
  chunkStoreUnlock(pStore);
  return rc;
}

static int remoteSrvApplyRefsIf(
  ChunkStore *pStore,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pBody,
  int nBody,
  int *pbMissing
){
  u8 *pMerged = 0;
  int nMerged = 0;
  int rc;
  if( nBody<=0 ) return SQLITE_ERROR;
  rc = remoteSrvLockAndForceRefresh(pStore);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(refsTableGetHash(&pStore->refs), pExpectedRefsHash)!=0 ){
    rc = doltliteMergeScopedRefsUpdate(pStore, pExpectedRefsHash, pBody, nBody,
                                      zBranch, bForce, &pMerged, &nMerged,
                                      pbMissing);
    if( rc==SQLITE_OK ){
      rc = remoteSrvApplyRefsLocked(pStore, zBranch, bForce, pMerged, nMerged,
                                    pbMissing);
    }
  }else{
    rc = remoteSrvApplyRefsLocked(pStore, zBranch, bForce, pBody, nBody,
                                  pbMissing);
  }
  sqlite3_free(pMerged);
  chunkStoreUnlock(pStore);
  return rc;
}

/* Parse [u16 branchLen][branch][u8 force]; *pzBranch is owned. */
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

/* 409 + BUSY_SNAPSHOT keeps older clients retrying; the code tells newer
** ones to rescan below chunks this server already holds. */
static void remoteSrvSendRefsError(DoltliteConn *fd, int rc, int bMissing){
  if( rc==SQLITE_BUSY_SNAPSHOT && bMissing ){
    sendStructuredError(fd, 409, "Conflict", "missing_chunks", rc,
                        "remote is missing chunks of the pushed history; "
                        "retry the push");
    return;
  }
  sendSqliteError(fd, rc);
}

static void handlePutRefs(ChunkStore *pStore, DoltliteConn *fd,
                          const u8 *pBody, int nBody){
  int bMissing = 0;
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

  rc = remoteSrvApplyRefs(pStore, zBranch, bForce, pRest, nRest, &bMissing);
  sqlite3_free(zBranch);
  if( rc!=SQLITE_OK ){
    remoteSrvSendRefsError(fd, rc, bMissing);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handlePutRefsIf(ChunkStore *pStore, DoltliteConn *fd,
                            const u8 *pBody, int nBody){
  int bMissing = 0;
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
                            nRest - PROLLY_HASH_SIZE, &bMissing);
  sqlite3_free(zBranch);
  if( rc!=SQLITE_OK ){
    remoteSrvSendRefsError(fd, rc, bMissing);
    return;
  }

  sendOk(fd, 0, 0);
}

int doltliteRemoteSrvCommitPendingForTest(ChunkStore *pStore){
  return remoteSrvCommitPending(pStore);
}

int doltliteRemoteSrvStoreChunkBatchForTest(
  ChunkStore *pStore, const u8 *pBody, int nBody
){
  return remoteSrvStoreChunkBatch(pStore, pBody, nBody);
}

int doltliteRemoteSrvApplyScopedRefsForTest(
  ChunkStore *pStore,
  const char *zBranch,
  int bForce,
  const u8 *pBody,
  int nBody
){
  int bMissing = 0;
  return remoteSrvApplyRefs(pStore, zBranch, bForce, pBody, nBody, &bMissing);
}

int doltliteRemoteSrvApplyRefsForTest(
  ChunkStore *pStore, const u8 *pBody, int nBody
){
  /* Install/rollback path only; scope validation is covered separately. */
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
    sendSqliteError(fd, rc);
    return;
  }

  sendOk(fd, 0, 0);
}

static void remoteDbHandleCloseStore(RemoteDbHandle *h){
  if( h->bOpen ){
    chunkStoreClose(&h->store);
    memset(&h->store, 0, sizeof(h->store));
    h->bOpen = 0;
    h->bWritable = 0;
  }
}

static int remoteDbHandleOpenStore(
  RemoteDbHandle *h,
  const char *zPath,
  int bWritable
){
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  int flags;
  int rc;

  if( !pVfs ) return SQLITE_ERROR;
  flags = bWritable
        ? (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB)
        : (SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  memset(&h->store, 0, sizeof(h->store));
  rc = chunkStoreOpen(&h->store, pVfs, zPath, flags);
  if( rc==SQLITE_OK ){
    h->bOpen = 1;
    h->bWritable = bWritable ? 1 : 0;
  }
  return rc;
}

static void remoteDbHandleFree(RemoteDbHandle *h){
  if( !h ) return;
  remoteDbHandleCloseStore(h);
  if( h->muInit ){
    pthread_mutex_destroy(&h->mu);
    h->muInit = 0;
  }
  sqlite3_free(h->zPath);
  sqlite3_free(h);
}

static void remoteDbCacheClear(DoltliteServer *pSrv){
  RemoteDbHandle *h = pSrv->pDbCache;
  pSrv->pDbCache = 0;
  pSrv->nDbCache = 0;
  while( h ){
    RemoteDbHandle *pNext = h->pNext;
    remoteDbHandleFree(h);
    h = pNext;
  }
}

/* Evict one idle handle; skip busy DBs via non-blocking locks. */
static void remoteDbCacheEvictOne(DoltliteServer *pSrv){
  RemoteDbHandle *h;
  RemoteDbHandle *pPrev = 0;
  RemoteDbHandle *pBestPrev = 0;
  RemoteDbHandle *pBest = 0;
  i64 bestAge = 0;

  for(h=pSrv->pDbCache; h; pPrev=h, h=h->pNext){
    if( h->nReservations>0 ) continue;
    if( pthread_mutex_trylock(&h->mu)!=0 ) continue;
    if( !pBest || h->lastUseMs < bestAge ){
      if( pBest ) pthread_mutex_unlock(&pBest->mu);
      pBest = h;
      pBestPrev = pPrev;
      bestAge = h->lastUseMs;
    }else{
      pthread_mutex_unlock(&h->mu);
    }
  }
  if( !pBest ) return;
  if( pBestPrev ) pBestPrev->pNext = pBest->pNext;
  else pSrv->pDbCache = pBest->pNext;
  pSrv->nDbCache--;
  pthread_mutex_unlock(&pBest->mu);
  remoteDbHandleFree(pBest);
}

/* Look up or create a handle; *ppOut is locked. Caller remoteDbRelease. */
static int remoteDbAcquire(
  DoltliteServer *pSrv,
  const char *zPath,
  int bWritable,
  RemoteDbHandle **ppOut
){
  RemoteDbHandle *h = 0;
  int rc;

  *ppOut = 0;
  pthread_mutex_lock(&pSrv->mutex);
  for(h=pSrv->pDbCache; h; h=h->pNext){
    if( strcmp(h->zPath, zPath)==0 ) break;
  }
  if( !h ){
    while( pSrv->nDbCache >= SERVER_DB_CACHE_MAX ){
      int nBefore = pSrv->nDbCache;
      remoteDbCacheEvictOne(pSrv);
      if( pSrv->nDbCache >= nBefore ) break;
    }
    h = (RemoteDbHandle*)sqlite3_malloc(sizeof(*h));
    if( !h ){
      pthread_mutex_unlock(&pSrv->mutex);
      return SQLITE_NOMEM;
    }
    memset(h, 0, sizeof(*h));
    h->zPath = sqlite3_mprintf("%s", zPath);
    if( !h->zPath || pthread_mutex_init(&h->mu, 0)!=0 ){
      sqlite3_free(h->zPath);
      sqlite3_free(h);
      pthread_mutex_unlock(&pSrv->mutex);
      return SQLITE_NOMEM;
    }
    h->muInit = 1;
    h->pNext = pSrv->pDbCache;
    pSrv->pDbCache = h;
    pSrv->nDbCache++;
  }
  h->nReservations++;
  pthread_mutex_unlock(&pSrv->mutex);

  pthread_mutex_lock(&h->mu);
  h->lastUseMs = monotonicMs();

  /* Drop any leftover staging from a failed prior request before refresh. */
  if( h->bOpen
   && (h->store.staging.nPending>0 || h->store.staging.nRecentUncommitted>0) ){
    chunkStoreRollback(&h->store);
  }

  if( h->bOpen && bWritable && !h->bWritable ){
    remoteDbHandleCloseStore(h);
  }

  if( !h->bOpen ){
    rc = remoteDbHandleOpenStore(h, zPath, bWritable);
    if( rc!=SQLITE_OK ) goto acquire_error;
  }else if( bWritable ){
    rc = remoteSrvLockAndForceRefresh(&h->store);
    if( rc==SQLITE_OK ){
      chunkStoreUnlock(&h->store);
    }else if( h->store.movedReadOnly ){
      remoteDbHandleCloseStore(h);
      rc = remoteDbHandleOpenStore(h, zPath, bWritable);
      if( rc!=SQLITE_OK ) goto acquire_error;
    }else{
      goto acquire_error;
    }
  }else{
    int changed = 0;
    rc = chunkStoreRefreshIfChanged(&h->store, &changed);
    if( rc!=SQLITE_OK || h->store.movedReadOnly ){
      remoteDbHandleCloseStore(h);
      rc = remoteDbHandleOpenStore(h, zPath, bWritable);
      if( rc!=SQLITE_OK ) goto acquire_error;
    }
  }

  *ppOut = h;
  return SQLITE_OK;

acquire_error:
#if SQLITE_OS_WIN
  remoteDbHandleCloseStore(h);
#endif
  pthread_mutex_unlock(&h->mu);
  pthread_mutex_lock(&pSrv->mutex);
  h->nReservations--;
  pthread_mutex_unlock(&pSrv->mutex);
  return rc;
}

static void remoteDbRelease(DoltliteServer *pSrv, RemoteDbHandle *h){
  if( !h ) return;
  if( h->bOpen
   && (h->store.staging.nPending>0 || h->store.staging.nRecentUncommitted>0) ){
    chunkStoreRollback(&h->store);
  }
#if SQLITE_OS_WIN
  /* An idle open handle prevents a Windows peer from replacing the file. */
  remoteDbHandleCloseStore(h);
#endif
  pthread_mutex_unlock(&h->mu);
  pthread_mutex_lock(&pSrv->mutex);
  h->nReservations--;
  pthread_mutex_unlock(&pSrv->mutex);
}

static void handleRequest(
  DoltliteServer *pSrv,
  DoltliteConn *fd,
  char *zPrefix,
  int nPrefix,
  int nHeader,
  u8 *pBody,
  int nBody,
  int requestStatus
){
  char zMethod[16];
  char zPath[512];
  char zDbName[256];
  char zEndpoint[256];
  ChunkStore *pStore = 0;
  RemoteDbHandle *pHandle = 0;
  char zDbPath[1024];
  int rc;
  int isReadOnlyEndpoint = 0;
  int isHasChunksEndpoint = 0;
  int isGetChunksEndpoint = 0;
  int exists = 0;
  int contentLength = 0;
  int contentRc = 0;
  sqlite3_vfs *pVfs;
  RemoteSrvPending pending;

  if( requestStatus!=200 ){
    if( requestStatus==401 ) sendUnauthorized(fd);
    else if( requestStatus==413 ) sendPayloadTooLarge(fd);
    else sendBadRequest(fd);
    return;
  }
  rc = parseRequestHeaders(zPrefix, nPrefix, nHeader,
                           zMethod, sizeof(zMethod),
                           zPath, sizeof(zPath),
                           0, 0,
                           &contentLength, &contentRc);
  if( rc!=0 || contentRc!=0 || contentLength!=nBody ){
    sendBadRequest(fd);
    return;
  }

  if( parsePath(zPath, zDbName, sizeof(zDbName),
                zEndpoint, sizeof(zEndpoint))!=0 ){
    sendNotFound(fd);
    return;
  }
  if( !isSafeDbName(zDbName) ){
    sendNotFound(fd);
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
    sendSqliteError(fd, rc);
    return;
  }
  if( !exists && isHasChunksEndpoint ){
    handleHasChunks(0, fd, pBody, nBody);
    return;
  }
  if( !exists && isReadOnlyEndpoint ){
    sendNotFound(fd);
    return;
  }

  rc = remoteDbAcquire(pSrv, zDbPath, !isReadOnlyEndpoint, &pHandle);
  if( rc!=SQLITE_OK ){
    sendSqliteError(fd, rc);
    return;
  }
  pStore = &pHandle->store;

  memset(&pending, 0, sizeof(pending));
  remotesrvPending = &pending;

  if( strcmp(zMethod, "GET")==0 ){
    if( strcmp(zEndpoint, "root")==0 ){
      handleGetRoot(pStore, fd);
    }else if( strncmp(zEndpoint, "chunk/", 6)==0 ){
      handleGetChunk(pStore, fd, zEndpoint + 6);
    }else if( strcmp(zEndpoint, "refs")==0 ){
      handleGetRefs(pStore, fd);
    }else{
      sendNotFound(fd);
    }
  }else if( strcmp(zMethod, "POST")==0 ){
    if( strcmp(zEndpoint, "has-chunks")==0 ){
      handleHasChunks(pStore, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "get-chunks")==0 ){
      handleGetChunks(pStore, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "chunks")==0 ){
      handlePostChunks(pStore, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "commit")==0 ){
      handleCommit(pStore, fd);
    }else{
      sendNotFound(fd);
    }
  }else if( strcmp(zMethod, "PUT")==0 ){
    if( strcmp(zEndpoint, "refs")==0 ){
      handlePutRefs(pStore, fd, pBody, nBody);
    }else if( strcmp(zEndpoint, "refs-if")==0 ){
      handlePutRefsIf(pStore, fd, pBody, nBody);
    }else{
      sendNotFound(fd);
    }
  }else{
    sendBadRequest(fd);
  }

  remotesrvPending = 0;
  remoteDbRelease(pSrv, pHandle);
  if( pending.bFail ){
    sqlite3_free(pending.pMsg);
    sendSqliteError(fd, SQLITE_NOMEM);
  }else if( pending.pMsg && pending.nMsg>0 ){
    doltliteConnWriteAll(fd, pending.pMsg, pending.nMsg);
    sqlite3_free(pending.pMsg);
  }
}

static void serverCleanup(DoltliteServer *pSrv);

static void *serverWorkerEntry(void *pArg){
  DoltliteWorkerArg *pWorker = (DoltliteWorkerArg*)pArg;
  DoltliteServer *pSrv = pWorker->pSrv;

  for(;;){
    DoltliteConn *conn;
    struct ServerJob job;

    pthread_mutex_lock(&pSrv->mutex);
    while( pSrv->nQueue==0 && pSrv->running ){
      pthread_cond_wait(&pSrv->workReady, &pSrv->mutex);
    }
    if( pSrv->nQueue==0 ){
      pthread_mutex_unlock(&pSrv->mutex);
      break;
    }
    job = pSrv->queue[pSrv->queueHead];
    pSrv->queueHead = (pSrv->queueHead + 1) % SERVER_QUEUE_SIZE;
    pSrv->nQueue--;
    pSrv->activeFd[pWorker->index] = job.fd;
    pthread_mutex_unlock(&pSrv->mutex);

    conn = job.conn ? job.conn : doltliteConnFromFd(job.fd);
    if( conn ){
      handleRequest(pSrv, conn, job.zPrefix, job.nPrefix, job.nHeader,
                    job.pBody, job.nBody, job.requestStatus);
      doltliteConnClose(conn);
    }
    sqlite3_free(job.zPrefix);
    sqlite3_free(job.pBody);

    pthread_mutex_lock(&pSrv->mutex);
    pSrv->nBodyAlloc -= job.nBodyAlloc;
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
    struct ServerJob job;
    job = pSrv->queue[pSrv->queueHead];
    pSrv->queueHead = (pSrv->queueHead + 1) % SERVER_QUEUE_SIZE;
    pSrv->nQueue--;
    if( job.fd>=0 ) doltliteShutdownSocket(job.fd);
    if( job.conn ) doltliteConnClose(job.conn);
    else if( job.fd>=0 ) doltliteCloseSocket(job.fd);
    sqlite3_free(job.zPrefix);
    sqlite3_free(job.pBody);
    pSrv->nBodyAlloc -= job.nBodyAlloc;
  }
  /* shutdown() wakes blocking I/O without descriptor reuse. */
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
  pSrv->aPending = sqlite3_malloc(sizeof(PendingConn) * SERVER_PENDING_MAX);
  if( !pSrv->aPending ){
    serverCleanup(pSrv);
    return SQLITE_NOMEM;
  }
  for(i=0; i<SERVER_PENDING_MAX; i++) pSrv->aPending[i].fd = -1;

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
    if( !o->audience || !o->audience[0] ){
      serverCleanup(pSrv);
      return SQLITE_ERROR;
    }
    if( doltliteCredsValidateAuthDir(o->authKeysDir)!=0 ){
      serverCleanup(pSrv);
      return SQLITE_ERROR;
    }
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

  /* Loopback is 127/8, not just 127.0.0.1. Warn TLS and auth separately;
  ** unauthenticated writes open the store READWRITE|CREATE. */
  if( (ntohl(bindIn.s_addr) >> 24) != 127 ){
    if( pSrv->tls==0 ){
      fprintf(stderr,
        "WARNING: doltlite-remotesrv bound to %s without TLS — traffic is "
        "unencrypted. Use --cert/--key, or only do this on trusted networks "
        "or behind a reverse proxy.\n",
        zBindAddr);
    }
    if( pSrv->authKeysDir==0 ){
      fprintf(stderr,
        "WARNING: doltlite-remotesrv bound to %s without authentication — "
        "anyone who can reach it may read these databases and push to them. "
        "Use --auth-keys DIR, or bind 127.0.0.1 to stay local.\n",
        zBindAddr);
    }
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

static int serverHeaderBudget(const DoltliteServer *pSrv){
  if( pSrv->timeoutMs < SERVER_HEADER_TIMEOUT_MS ) return pSrv->timeoutMs;
  return SERVER_HEADER_TIMEOUT_MS;
}

static i64 pendingBudget(const DoltliteServer *pSrv, const PendingConn *p){
  i64 budget;
  if( p->bHandshake ) return pSrv->timeoutMs;
  if( p->nHeader ){
    budget = serverHeaderBudget(pSrv) + (i64)p->nBody * 1000 / SERVER_BODY_MIN_RATE;
    return budget<pSrv->timeoutMs ? budget : pSrv->timeoutMs;
  }
  return serverHeaderBudget(pSrv);
}

static void pendingClose(DoltliteServer *pSrv, PendingConn *p){
  if( p->conn ){
    doltliteConnClose(p->conn);
    p->conn = 0;
    p->fd = -1;
  }else if( p->fd>=0 ){
    doltliteCloseSocket(p->fd);
    p->fd = -1;
  }
  p->nBuf = 0;
  p->bHandshake = 0;
  p->bWantWrite = 0;
  sqlite3_free(p->pBody);
  p->pBody = 0;
  pthread_mutex_lock(&pSrv->mutex);
  pSrv->nBodyAlloc -= p->nBodyAlloc;
  pthread_mutex_unlock(&pSrv->mutex);
  p->nBodyAlloc = 0;
}

static int pendingAppendBody(DoltliteServer *pSrv, PendingConn *p,
                             const u8 *aData, int nData){
  i64 need = (i64)p->nBody + nData;
  i64 nAlloc = p->nBodyAlloc;
  int rc;

  if( need>nAlloc ){
    if( nAlloc==0 ) nAlloc = 4096;
    while( nAlloc<need ) nAlloc *= 2;
    pthread_mutex_lock(&pSrv->mutex);
    if( nAlloc-p->nBodyAlloc > SERVER_BODY_BUFFER_MAX-pSrv->nBodyAlloc ){
      pthread_mutex_unlock(&pSrv->mutex);
      return SQLITE_NOMEM;
    }
    pSrv->nBodyAlloc += nAlloc-p->nBodyAlloc;
    pthread_mutex_unlock(&pSrv->mutex);
    rc = remoteSrvGrowBuf(&p->pBody, &p->nBodyAlloc, nAlloc);
    if( rc!=SQLITE_OK ){
      pthread_mutex_lock(&pSrv->mutex);
      pSrv->nBodyAlloc -= nAlloc-p->nBodyAlloc;
      pthread_mutex_unlock(&pSrv->mutex);
      return rc;
    }
  }
  if( nData>0 ) memcpy(p->pBody+p->nBody, aData, (size_t)nData);
  p->nBody += nData;
  return SQLITE_OK;
}

static int pendingHeaderEnd(const char *aBuf, int nBuf){
  int i;
  for(i=0; i+3<nBuf; i++){
    if( aBuf[i]=='\r' && aBuf[i+1]=='\n'
     && aBuf[i+2]=='\r' && aBuf[i+3]=='\n' ){
      return i + 4;
    }
  }
  return 0;
}

static int enqueueJob(
  DoltliteServer *pSrv,
  int fd,
  DoltliteConn *conn,
  char *zPrefix,
  int nPrefix,
  int nHeader,
  u8 *pBody,
  int nBody,
  i64 nBodyAlloc,
  int requestStatus
){
  int idx;
  struct ServerJob *job;

  pthread_mutex_lock(&pSrv->mutex);
  if( !pSrv->running || pSrv->nQueue==SERVER_QUEUE_SIZE ){
    pSrv->nBodyAlloc -= nBodyAlloc;
    pthread_mutex_unlock(&pSrv->mutex);
    sqlite3_free(zPrefix);
    sqlite3_free(pBody);
    if( conn ) doltliteConnClose(conn);
    else if( fd>=0 ) doltliteCloseSocket(fd);
    return -1;
  }
  idx = (pSrv->queueHead + pSrv->nQueue) % SERVER_QUEUE_SIZE;
  job = &pSrv->queue[idx];
  job->fd = fd;
  job->conn = conn;
  job->zPrefix = zPrefix;
  job->nPrefix = nPrefix;
  job->nHeader = nHeader;
  job->pBody = pBody;
  job->nBody = nBody;
  job->nBodyAlloc = nBodyAlloc;
  job->requestStatus = requestStatus;
  pSrv->nQueue++;
  pthread_cond_signal(&pSrv->workReady);
  pthread_mutex_unlock(&pSrv->mutex);
  return 0;
}

/* Move a finished request onto the worker queue. 0 means this slot no longer
** owns the connection. -1 leaves it in place for the caller to close. */
static int pendingPromote(
  DoltliteServer *pSrv,
  PendingConn *p,
  int nHeader
){
  char *zCopy;
  int nPrefix;
  int fd;
  DoltliteConn *conn;

  nPrefix = p->nBuf;
  zCopy = sqlite3_malloc(nPrefix + 1);
  if( !zCopy ) return -1;
  memcpy(zCopy, p->aBuf, (size_t)nPrefix);
  zCopy[nPrefix] = 0;
  if( doltliteSocketSetBlocking(p->fd, 1)!=0
   || (p->conn ? doltliteConnSetTimeout(p->conn, pSrv->timeoutMs)
               : doltliteSocketSetTimeout(p->fd, pSrv->timeoutMs))!=0 ){
    sqlite3_free(zCopy);
    return -1;
  }
  fd = p->fd;
  conn = p->conn;
  p->fd = -1;
  p->conn = 0;
  p->nBuf = 0;
  p->bHandshake = 0;
  p->bWantWrite = 0;
  enqueueJob(pSrv, fd, conn, zCopy, nPrefix, nHeader,
             p->pBody, p->nBody, p->nBodyAlloc, p->requestStatus);
  p->pBody = 0;
  p->nBodyAlloc = 0;
  return 0;
}

static int pendingIngest(DoltliteServer *pSrv, PendingConn *p){
  int nHeader = 0;

  if( p->fd<0 ) return 1;
  if( p->bHandshake ){
    int step = doltliteConnHandshakeStep(p->conn);
    if( step<0 ){
      pendingClose(pSrv, p);
      return 1;
    }
    if( step==2 ){
      p->bWantWrite = 1;
      return 0;
    }
    if( step==1 ){
      p->bWantWrite = 0;
      return 0;
    }
    p->bHandshake = 0;
    p->bWantWrite = 0;
    p->startMs = monotonicMs();
  }
  while( p->nHeader==0 && p->nBuf < MAX_HEADER_SIZE-1 ){
    int n;
    int space = MAX_HEADER_SIZE - 1 - p->nBuf;
    if( p->conn ){
      n = doltliteConnReadNonblock(p->conn, p->aBuf + p->nBuf, space);
    }else{
      n = doltliteSocketRecv(p->fd, p->aBuf + p->nBuf, space);
    }
    if( n==-2 ) return 0;
    if( n<=0 ){
      pendingClose(pSrv, p);
      return 1;
    }
    p->nBuf += n;
    nHeader = pendingHeaderEnd(p->aBuf, p->nBuf);
    if( nHeader ){
      int nCopy;
      p->nHeader = nHeader;
      p->requestStatus = requestHeaderStatus(
          pSrv, p->aBuf, p->nBuf, nHeader, &p->contentLength);
      if( p->requestStatus!=200 ){
        if( pendingPromote(pSrv, p, nHeader)!=0 ) pendingClose(pSrv, p);
        return 1;
      }
      nCopy = p->nBuf - nHeader;
      if( nCopy>p->contentLength ) nCopy = p->contentLength;
      if( pendingAppendBody(pSrv, p, (const u8*)p->aBuf+nHeader, nCopy)!=SQLITE_OK ){
        pendingClose(pSrv, p);
        return 1;
      }
      p->nBuf = nHeader;
      p->startMs = monotonicMs();
    }
  }
  if( p->nHeader ){
    u8 aData[8192];
    int n;
    int nRead = p->contentLength-p->nBody;
    p->bReadAgain = 0;
    if( monotonicMs()-p->startMs >= pendingBudget(pSrv, p) ){
      p->requestStatus = 400;
    }else if( nRead>0 ){
      if( nRead>(int)sizeof(aData) ) nRead = sizeof(aData);
      n = p->conn ? doltliteConnReadNonblock(p->conn, aData, nRead)
                  : doltliteSocketRecv(p->fd, aData, nRead);
      if( n==-2 ) return 0;
      if( n<=0 ){
        p->requestStatus = 400;
      }else if( pendingAppendBody(pSrv, p, aData, n)!=SQLITE_OK ){
        pendingClose(pSrv, p);
        return 1;
      }else if( p->nBody<p->contentLength ){
        p->bReadAgain = 1;
        return 0;
      }
    }
    if( pendingPromote(pSrv, p, p->nHeader)!=0 ) pendingClose(pSrv, p);
  }else{
    pendingClose(pSrv, p);
  }
  return 1;
}

static int pendingAdd(
  DoltliteServer *pSrv,
  PendingConn *aPending,
  int *pnPending,
  int clientFd
){
  PendingConn *p;

  if( *pnPending>=SERVER_PENDING_MAX ){
    doltliteCloseSocket(clientFd);
    return -1;
  }
  if( doltliteSocketSetBlocking(clientFd, 0)!=0 ){
    doltliteCloseSocket(clientFd);
    return -1;
  }
  p = &aPending[*pnPending];
  memset(p, 0, sizeof(*p));
  p->fd = clientFd;
  p->startMs = monotonicMs();
  if( pSrv->tls ){
    p->conn = doltliteConnServerAttach(pSrv->tls, clientFd);
    if( !p->conn ){
      p->fd = -1;
      return -1;
    }
    p->bHandshake = 1;
  }
  (*pnPending)++;
  return 0;
}

static void serverLoop(DoltliteServer *pSrv){
  PendingConn *aPending = pSrv->aPending;
  doltlite_pollfd aPoll[SERVER_PENDING_MAX + 1];
  int nPending = 0;
  int i;

  if( !aPending ) return;
  while( serverIsRunning(pSrv) ){
    int nReady;
    int nPolled;
    int clientFd;
    int pollMs = 100;

    nPolled = nPending;
    aPoll[0].fd = pSrv->listenFd;
    aPoll[0].events = POLLIN;
    aPoll[0].revents = 0;
    for(i=0; i<nPending; i++){
      aPoll[i+1].fd = aPending[i].fd;
      aPoll[i+1].events = POLLIN;
      if( aPending[i].bWantWrite ) aPoll[i+1].events |= POLLOUT;
      aPoll[i+1].revents = 0;
      if( aPending[i].bReadAgain ) pollMs = 0;
    }
    nReady = doltlitePoll(aPoll, 1 + nPending, pollMs);
    if( nReady<0 ) continue;

    if( (aPoll[0].revents & POLLIN)!=0 ){
      clientFd = (int)accept(pSrv->listenFd, 0, 0);
      if( clientFd>=0
       && pendingAdd(pSrv, aPending, &nPending, clientFd)==0
       && pendingIngest(pSrv, &aPending[nPending-1]) ){
        nPending--;
      }
    }

    for(i=nPolled-1; i>=0; i--){
      int remove = 0;
      int ready = aPoll[i+1].revents;
      PendingConn *p = &aPending[i];
      if( p->bReadAgain || (ready & (POLLIN|POLLERR|POLLHUP|POLLNVAL))!=0
       || (p->bWantWrite && (ready & POLLOUT)!=0) ){
        remove = pendingIngest(pSrv, p);
      }
      if( !remove && monotonicMs() - p->startMs > pendingBudget(pSrv, p) ){
        p->requestStatus = 400;
        if( !p->nHeader || pendingPromote(pSrv, p, p->nHeader)!=0 ){
          pendingClose(pSrv, p);
        }
        remove = 1;
      }
      if( remove ){
        nPending--;
        aPending[i] = aPending[nPending];
      }
    }
  }
  for(i=0; i<nPending; i++) pendingClose(pSrv, &aPending[i]);
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
  remoteDbCacheClear(pSrv);
  sqlite3_free(pSrv->zDir);
  pSrv->zDir = 0;
  sqlite3_free(pSrv->authKeysDir);
  pSrv->authKeysDir = 0;
  sqlite3_free(pSrv->audience);
  pSrv->audience = 0;
  sqlite3_free(pSrv->aPending);
  pSrv->aPending = 0;
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
