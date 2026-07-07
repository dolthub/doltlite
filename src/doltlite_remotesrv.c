
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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>
#include <errno.h>

struct DoltliteServer {
  int listenFd;
  int port;
  volatile int running;
  char *zDir;
  pthread_t thread;

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

  {
    const char *zCL = "Content-Length:";
    int nCL = (int)strlen(zCL);
    char *pLine = strstr(aBuf, zCL);
    if( !pLine ){

      zCL = "content-length:";
      pLine = strstr(aBuf, zCL);
    }
    if( pLine ){
      pLine += nCL;
      while( *pLine==' ' || *pLine=='\t' ) pLine++;
      contentLength = atoi(pLine);
    }
  }

  if( zAuth && nAuthMax>0 ){
    const char *zH = "Authorization:";
    char *pLine = strstr(aBuf, zH);
    if( !pLine ){
      zH = "authorization:";
      pLine = strstr(aBuf, zH);
    }
    if( pLine ){
      char *pEnd;
      int len;
      pLine += (int)strlen("Authorization:");
      while( *pLine==' ' || *pLine=='\t' ) pLine++;
      pEnd = pLine;
      while( *pEnd && *pEnd!='\r' && *pEnd!='\n' ) pEnd++;
      len = (int)(pEnd - pLine);
      if( len >= nAuthMax ) len = nAuthMax - 1;
      memcpy(zAuth, pLine, len);
      zAuth[len] = '\0';
    }
  }

  /* Reject negative (e.g. integer overflow from atoi) or oversized bodies.
  ** A hostile peer could send Content-Length: 2147483647 and OOM the host;
  ** -2 signals the caller to return HTTP 413.
  */
  if( contentLength < 0 || contentLength > MAX_REQUEST_BYTES ){
    return -2;
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

static int remoteSrvApplyRefs(ChunkStore *pStore, const u8 *pBody, int nBody){
  int rc;

  if( nBody<=0 ) return SQLITE_ERROR;
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
  rc = remoteSrvApplyRefs(pStore, pBody, nBody);
  chunkStoreUnlock(pStore);
  return rc;
}

static void handlePutRefs(ChunkStore *pStore, DoltliteConn *fd,
                          const u8 *pBody, int nBody){
  int rc;

  if( nBody<=0 ){
    sendBadRequest(fd);
    return;
  }

  rc = remoteSrvApplyRefs(pStore, pBody, nBody);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handlePutRefsIf(ChunkStore *pStore, DoltliteConn *fd,
                            const u8 *pBody, int nBody){
  ProllyHash expectedRefsHash;
  int rc;

  if( nBody<=PROLLY_HASH_SIZE ){
    sendBadRequest(fd);
    return;
  }
  memcpy(expectedRefsHash.data, pBody, PROLLY_HASH_SIZE);

  rc = remoteSrvApplyRefsIf(pStore, &expectedRefsHash,
                            pBody + PROLLY_HASH_SIZE,
                            nBody - PROLLY_HASH_SIZE);
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
  return remoteSrvApplyRefs(pStore, pBody, nBody);
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
  isReadOnlyEndpoint = strcmp(zMethod, "GET")==0 || isHasChunksEndpoint;
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
  const char *zBindAddr = o->zBindAddr;

  memset(pSrv, 0, sizeof(*pSrv));
  pSrv->listenFd = -1;

  if( zBindAddr==0 || zBindAddr[0]=='\0' ){
    zBindAddr = "127.0.0.1";
  }
  if( inet_pton(AF_INET, zBindAddr, &bindIn)!=1 ){
    return SQLITE_ERROR;
  }

  pSrv->zDir = dupStr(o->zDir);
  if( !pSrv->zDir ) return SQLITE_NOMEM;

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

  pSrv->listenFd = socket(AF_INET, SOCK_STREAM, 0);
  if( pSrv->listenFd < 0 ){ serverCleanup(pSrv); return SQLITE_ERROR; }

  setsockopt(pSrv->listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

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
  return SQLITE_OK;
}

static void serverLoop(DoltliteServer *pSrv){
  while( pSrv->running ){
    struct pollfd pfd;
    int clientFd;
    DoltliteConn *conn;

    pfd.fd = pSrv->listenFd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    if( poll(&pfd, 1, 1000) <= 0 ) continue;

    clientFd = accept(pSrv->listenFd, NULL, NULL);
    if( clientFd < 0 ) continue;

    conn = pSrv->tls ? doltliteConnServerAccept(pSrv->tls, clientFd)
                     : doltliteConnFromFd(clientFd);
    if( !conn ) continue;

    handleRequest(pSrv, conn);
    doltliteConnClose(conn);
  }
}

static void serverCleanup(DoltliteServer *pSrv){
  if( pSrv->listenFd >= 0 ){
    close(pSrv->listenFd);
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
}

int doltliteServeOpts(const DoltliteServeOpts *o){
  DoltliteServer server;
  int rc;

  rc = serverInit(&server, o);
  if( rc!=SQLITE_OK ) return rc;

  serverLoop(&server);
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
  serverCleanup(pSrv);
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
  pServer->running = 0;

  if( pServer->listenFd >= 0 ){
    close(pServer->listenFd);
    pServer->listenFd = -1;
  }
  pthread_join(pServer->thread, 0);
  sqlite3_free(pServer);
}

int doltliteServerPort(DoltliteServer *pServer){
  return pServer ? pServer->port : 0;
}

#endif
#endif
