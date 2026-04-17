/* Test-only HTTP server side of the remote protocol. Not hardened
** for production: single-threaded per-connection handling, no TLS,
** no auth, no limits beyond MAX_HEADER_SIZE. Used by the fetch/push
** tests and for local integration work against doltlite_http_remote. */

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "doltlite_remotesrv.h"
#include "doltlite_commit.h"

#include <string.h>
#include <stdio.h>

#ifdef _WIN32

DoltliteServer *doltliteServerCreate(const char *z, int p, char **e){
  (void)z;(void)p; if(e) *e=sqlite3_mprintf("remotesrv not available on Windows"); return 0;
}
void doltliteServerDestroy(DoltliteServer *s){ (void)s; }
int doltliteServerPort(DoltliteServer *s){ (void)s; return 0; }
#else
#include <sys/socket.h>
#include <netinet/in.h>
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
};

static int hexVal(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  if( c>='A' && c<='F' ) return c - 'A' + 10;
  return -1;
}

static int hexToHash(const char *zHex, ProllyHash *pHash){
  int i;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    int hi = hexVal(zHex[i*2]);
    int lo = hexVal(zHex[i*2+1]);
    if( hi<0 || lo<0 ) return SQLITE_ERROR;
    pHash->data[i] = (u8)((hi<<4)|lo);
  }
  return SQLITE_OK;
}

static int writeAll(int fd, const void *pBuf, int nBuf){
  int nWritten = 0;
  const u8 *p = (const u8*)pBuf;
  while( nWritten < nBuf ){
    ssize_t n = write(fd, p + nWritten, nBuf - nWritten);
    if( n<0 ){
      if( errno==EINTR ) continue;
      return SQLITE_IOERR_WRITE;
    }
    if( n==0 ) return SQLITE_IOERR_WRITE;
    nWritten += (int)n;
  }
  return SQLITE_OK;
}

static void sendResponse(int fd, int status, const char *zStatus,
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
  if( writeAll(fd, zHeader, nHeader)!=SQLITE_OK ) return;
  if( pBody && nBody>0 ){
    writeAll(fd, pBody, nBody);
  }
}

static void sendOk(int fd, const u8 *pBody, int nBody){
  sendResponse(fd, 200, "OK", pBody, nBody);
}

static void sendNotFound(int fd){
  sendResponse(fd, 404, "Not Found", (const u8*)"Not Found", 9);
}

static void sendBadRequest(int fd){
  sendResponse(fd, 400, "Bad Request", (const u8*)"Bad Request", 11);
}

static void sendError(int fd){
  sendResponse(fd, 500, "Internal Server Error",
               (const u8*)"Internal Server Error", 21);
}

#define MAX_HEADER_SIZE 4096

static int readExact(int fd, u8 *pBuf, int nBytes){
  int nRead = 0;
  while( nRead < nBytes ){
    int n = (int)read(fd, pBuf + nRead, nBytes - nRead);
    if( n <= 0 ) return -1;
    nRead += n;
  }
  return 0;
}

static int parseRequest(
  int fd,
  char *zMethod, int nMethodMax,
  char *zPath, int nPathMax,
  u8 **ppBody, int *pnBody
){
  char aBuf[MAX_HEADER_SIZE];
  int nBuf = 0;
  int headerEnd = 0;
  int contentLength = 0;
  char *p;

  *ppBody = 0;
  *pnBody = 0;


  while( nBuf < MAX_HEADER_SIZE-1 ){
    int n = (int)read(fd, &aBuf[nBuf], 1);
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
  const char *dbEnd;
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
  if( zDbName[0]=='.' && zDbName[1]=='\0' ) return 0;
  if( zDbName[0]=='.' && zDbName[1]=='.' && zDbName[2]=='\0' ) return 0;
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

static void handleGetRoot(ChunkStore *pStore, int fd){
  ProllyHash root;
  const char *zDef = chunkStoreGetDefaultBranch(pStore);
  if( zDef && chunkStoreFindBranch(pStore, zDef, &root)==SQLITE_OK ){
    sendOk(fd, root.data, PROLLY_HASH_SIZE);
  }else{
    memset(&root, 0, sizeof(root));
    sendOk(fd, root.data, PROLLY_HASH_SIZE);
  }
}

static void handleHasChunks(ChunkStore *pStore, int fd,
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

static void handleGetChunk(ChunkStore *pStore, int fd, const char *zHexHash){
  ProllyHash hash;
  u8 *pData = 0;
  int nData = 0;
  int rc;

  if( (int)strlen(zHexHash) < PROLLY_HASH_SIZE*2 ){
    sendBadRequest(fd);
    return;
  }

  if( hexToHash(zHexHash, &hash)!=SQLITE_OK ){
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

static void handlePostChunks(ChunkStore *pStore, int fd,
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

    if( offset + (int)len > nBody ){
      sendBadRequest(fd);
      return;
    }

    rc = chunkStorePut(pStore, pBody + offset, (int)len, &hash);
    if( rc!=SQLITE_OK ){
      sendError(fd);
      return;
    }
    offset += (int)len;
  }


  rc = chunkStoreCommit(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handleGetRefs(ChunkStore *pStore, int fd){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  if( prollyHashIsEmpty(&pStore->refsHash) ){
    sendNotFound(fd);
    return;
  }

  rc = chunkStoreGet(pStore, &pStore->refsHash, &pData, &nData);
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
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(pStore);
  return rc;
}

static void handlePutRefs(ChunkStore *pStore, int fd,
                          const u8 *pBody, int nBody){
  ProllyHash hash;
  int rc;

  if( nBody<=0 ){
    sendBadRequest(fd);
    return;
  }

  rc = chunkStorePut(pStore, pBody, nBody, &hash);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  pStore->refsHash = hash;

  rc = chunkStoreReloadRefs(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }


  rc = chunkStoreCommit(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handleCommit(ChunkStore *pStore, int fd){
  int rc;

  rc = remoteSrvPersistRefs(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

static void handleRequest(DoltliteServer *pSrv, int fd){
  char zMethod[16];
  char zPath[512];
  char zDbName[256];
  char zEndpoint[256];
  u8 *pBody = 0;
  int nBody = 0;
  ChunkStore store;
  char zDbPath[1024];
  int rc;
  int flags;

  if( parseRequest(fd, zMethod, sizeof(zMethod),
                   zPath, sizeof(zPath), &pBody, &nBody)!=0 ){
    sendBadRequest(fd);
    return;
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
  flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
  memset(&store, 0, sizeof(store));
  rc = chunkStoreOpen(&store, sqlite3_vfs_find(0), zDbPath, flags);
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
    }else{
      sendNotFound(fd);
    }
  }else{
    sendBadRequest(fd);
  }

  chunkStoreClose(&store);
  sqlite3_free(pBody);
}

static int serverInit(DoltliteServer *pSrv, const char *zDir, int port){
  struct sockaddr_in addr;
  socklen_t addrLen;
  int opt = 1;
  int nDir;

  memset(pSrv, 0, sizeof(*pSrv));
  pSrv->listenFd = -1;


  nDir = (int)strlen(zDir);
  pSrv->zDir = sqlite3_malloc(nDir + 1);
  if( !pSrv->zDir ) return SQLITE_NOMEM;
  memcpy(pSrv->zDir, zDir, nDir + 1);


  pSrv->listenFd = socket(AF_INET, SOCK_STREAM, 0);
  if( pSrv->listenFd < 0 ){
    sqlite3_free(pSrv->zDir);
    return SQLITE_ERROR;
  }

  setsockopt(pSrv->listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons((u16)port);

  if( bind(pSrv->listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0 ){
    close(pSrv->listenFd);
    sqlite3_free(pSrv->zDir);
    return SQLITE_ERROR;
  }

  if( listen(pSrv->listenFd, 5) < 0 ){
    close(pSrv->listenFd);
    sqlite3_free(pSrv->zDir);
    return SQLITE_ERROR;
  }


  addrLen = sizeof(addr);
  if( getsockname(pSrv->listenFd, (struct sockaddr*)&addr, &addrLen)==0 ){
    pSrv->port = ntohs(addr.sin_port);
  }else{
    pSrv->port = port;
  }

  pSrv->running = 1;
  return SQLITE_OK;
}

static void serverLoop(DoltliteServer *pSrv){
  while( pSrv->running ){
    struct pollfd pfd;
    int clientFd;

    pfd.fd = pSrv->listenFd;
    pfd.events = POLLIN;
    pfd.revents = 0;


    if( poll(&pfd, 1, 1000) <= 0 ) continue;

    clientFd = accept(pSrv->listenFd, NULL, NULL);
    if( clientFd < 0 ) continue;

    handleRequest(pSrv, clientFd);
    close(clientFd);
  }
}

static void serverCleanup(DoltliteServer *pSrv){
  if( pSrv->listenFd >= 0 ){
    close(pSrv->listenFd);
    pSrv->listenFd = -1;
  }
  sqlite3_free(pSrv->zDir);
  pSrv->zDir = 0;
}

int doltliteServe(const char *zDir, int port){
  DoltliteServer server;
  int rc;

  rc = serverInit(&server, zDir, port);
  if( rc!=SQLITE_OK ) return rc;

  serverLoop(&server);
  serverCleanup(&server);
  return SQLITE_OK;
}

static void *serverThreadEntry(void *pArg){
  DoltliteServer *pSrv = (DoltliteServer*)pArg;
  serverLoop(pSrv);
  serverCleanup(pSrv);
  return 0;
}

DoltliteServer *doltliteServeAsync(const char *zDir, int port){
  DoltliteServer *pSrv;
  int rc;

  pSrv = (DoltliteServer*)sqlite3_malloc(sizeof(DoltliteServer));
  if( !pSrv ) return 0;

  rc = serverInit(pSrv, zDir, port);
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
