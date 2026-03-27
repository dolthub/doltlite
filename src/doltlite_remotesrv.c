/*
** Minimal HTTP/1.1 server for doltlite remote access.
** Serves a directory of .doltlite files over HTTP, enabling push/fetch/clone
** from remote clients.
**
** URL routing: /{dbname}/endpoint
**   where dbname maps to {zDir}/{dbname} on disk.
**
** Uses POSIX sockets, no external dependencies.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "doltlite_remotesrv.h"
#include "doltlite_commit.h"

#include <string.h>
#include <stdio.h>

#ifdef _WIN32
/* remotesrv not yet ported to Windows (requires Winsock + Win32 threads) */
DoltliteServer *doltliteServerCreate(const char *z, int p, char **e){
  (void)z;(void)p; if(e) *e=sqlite3_mprintf("remotesrv not available on Windows"); return 0;
}
void doltliteServerDestroy(DoltliteServer *s){ (void)s; }
int doltliteServerPort(DoltliteServer *s){ (void)s; return 0; }
#else /* POSIX */
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>

struct DoltliteServer {
  int listenFd;          /* Listening socket */
  int port;              /* Actual port (may differ from requested if 0) */
  volatile int running;  /* Set to 0 to stop */
  char *zDir;            /* Directory containing .doltlite files */
  pthread_t thread;      /* Background thread (for async mode) */
};

/* ----------------------------------------------------------------
** Hex utilities
** ---------------------------------------------------------------- */

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

/* ----------------------------------------------------------------
** HTTP response helpers
** ---------------------------------------------------------------- */

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
  write(fd, zHeader, nHeader);
  if( pBody && nBody>0 ){
    write(fd, pBody, nBody);
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

/* ----------------------------------------------------------------
** Minimal HTTP request parser
** ---------------------------------------------------------------- */

/* Maximum header size we'll accept */
#define MAX_HEADER_SIZE 4096

/* Read exactly nBytes from fd into pBuf. Returns 0 on success. */
static int readExact(int fd, u8 *pBuf, int nBytes){
  int nRead = 0;
  while( nRead < nBytes ){
    int n = (int)read(fd, pBuf + nRead, nBytes - nRead);
    if( n <= 0 ) return -1;
    nRead += n;
  }
  return 0;
}

/*
** Parse an HTTP request from fd.
** Extracts method, path, content-length, and reads the body if present.
** Returns 0 on success, -1 on error.
*/
static int parseRequest(
  int fd,
  char *zMethod, int nMethodMax,    /* OUT: method (GET/POST/PUT) */
  char *zPath, int nPathMax,        /* OUT: request path */
  u8 **ppBody, int *pnBody          /* OUT: body (caller frees) */
){
  char aBuf[MAX_HEADER_SIZE];
  int nBuf = 0;
  int headerEnd = 0;
  int contentLength = 0;
  char *p;

  *ppBody = 0;
  *pnBody = 0;

  /* Read headers byte by byte until we find \r\n\r\n */
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

  /* Parse request line: METHOD /path HTTP/1.x\r\n */
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

  /* Scan headers for Content-Length */
  {
    const char *zCL = "Content-Length:";
    int nCL = (int)strlen(zCL);
    char *pLine = strstr(aBuf, zCL);
    if( !pLine ){
      /* Try lowercase */
      zCL = "content-length:";
      pLine = strstr(aBuf, zCL);
    }
    if( pLine ){
      pLine += nCL;
      while( *pLine==' ' || *pLine=='\t' ) pLine++;
      contentLength = atoi(pLine);
    }
  }

  /* Read body if present */
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

/* ----------------------------------------------------------------
** Path parsing: extract /{dbname}/{endpoint} from URL path
** ---------------------------------------------------------------- */

/*
** Parse path like "/mydb/root" into dbname="mydb", endpoint="root".
** Also handles "/mydb/chunk/HEXHASH".
** Returns 0 on success, -1 if path doesn't match expected format.
*/
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
  p++; /* skip leading '/' */

  /* dbname is everything up to the next '/' */
  dbStart = p;
  while( *p && *p != '/' ) p++;
  dbLen = (int)(p - dbStart);
  if( dbLen <= 0 || dbLen >= nDbNameMax ) return -1;
  memcpy(zDbName, dbStart, dbLen);
  zDbName[dbLen] = '\0';

  if( *p != '/' ) return -1;
  p++; /* skip '/' */

  /* endpoint is the rest */
  epStart = p;
  epLen = (int)strlen(epStart);
  if( epLen <= 0 || epLen >= nEndpointMax ) return -1;
  memcpy(zEndpoint, epStart, epLen);
  zEndpoint[epLen] = '\0';

  return 0;
}

/* ----------------------------------------------------------------
** Request handlers (now take ChunkStore* instead of DoltliteServer*)
** ---------------------------------------------------------------- */

/* GET /{db}/root -- Return 20-byte raw root hash */
static void handleGetRoot(ChunkStore *pStore, int fd){
  ProllyHash root;
  chunkStoreGetRoot(pStore, &root);
  sendOk(fd, root.data, PROLLY_HASH_SIZE);
}

/* POST /{db}/has-chunks -- Batch existence check */
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

/* GET /{db}/chunk/{40-char-hex-hash} -- Download one chunk */
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

/* POST /{db}/chunks -- Upload batch of chunks */
static void handlePostChunks(ChunkStore *pStore, int fd,
                             const u8 *pBody, int nBody){
  int offset = 0;
  int rc;

  while( offset + PROLLY_HASH_SIZE + 4 <= nBody ){
    u32 len;
    ProllyHash hash;

    /* Skip the client-provided hash (20 bytes) -- chunkStorePut computes it */
    offset += PROLLY_HASH_SIZE;

    /* Read 4-byte little-endian length */
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

  /* Commit so chunks persist across requests (store is opened per-request) */
  rc = chunkStoreCommit(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

/* GET /{db}/refs -- Get serialized refs blob */
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

/* PUT /{db}/refs -- Update refs blob */
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

  /* Commit so refs persist across requests (store is opened per-request) */
  rc = chunkStoreCommit(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

/* POST /{db}/commit -- Commit pending writes */
static void handleCommit(ChunkStore *pStore, int fd){
  int rc;

  rc = chunkStoreSerializeRefs(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  rc = chunkStoreCommit(pStore);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  /* After refs are committed, update head/catalog from default branch */
  {
    const char *zDef = chunkStoreGetDefaultBranch(pStore);
    ProllyHash branchCommit;
    if( zDef && chunkStoreFindBranch(pStore, zDef, &branchCommit)==SQLITE_OK ){
      u8 *cdata = 0; int ncdata = 0;
      if( chunkStoreGet(pStore, &branchCommit, &cdata, &ncdata)==SQLITE_OK && cdata ){
        DoltliteCommit commit;
        if( doltliteCommitDeserialize(cdata, ncdata, &commit)==SQLITE_OK ){
          chunkStoreSetHeadCommit(pStore, &branchCommit);
          chunkStoreSetCatalog(pStore, &commit.catalogHash);
          doltliteCommitClear(&commit);
        }
        sqlite3_free(cdata);
      }
      chunkStoreCommit(pStore);  /* Write updated manifest */
    }
  }

  sendOk(fd, 0, 0);
}

/* ----------------------------------------------------------------
** Request routing
** ---------------------------------------------------------------- */

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

  /* Parse /{dbname}/{endpoint} */
  if( parsePath(zPath, zDbName, sizeof(zDbName),
                zEndpoint, sizeof(zEndpoint))!=0 ){
    sendNotFound(fd);
    sqlite3_free(pBody);
    return;
  }

  /* Open the chunk store for this database */
  sqlite3_snprintf(sizeof(zDbPath), zDbPath, "%s/%s", pSrv->zDir, zDbName);
  flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
  memset(&store, 0, sizeof(store));
  rc = chunkStoreOpen(&store, sqlite3_vfs_find(0), zDbPath, flags);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    sqlite3_free(pBody);
    return;
  }

  /* Route to handler based on method + endpoint */
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

/* ----------------------------------------------------------------
** Server lifecycle
** ---------------------------------------------------------------- */

static int serverInit(DoltliteServer *pSrv, const char *zDir, int port){
  struct sockaddr_in addr;
  socklen_t addrLen;
  int opt = 1;
  int nDir;

  memset(pSrv, 0, sizeof(*pSrv));
  pSrv->listenFd = -1;

  /* Copy directory path */
  nDir = (int)strlen(zDir);
  pSrv->zDir = sqlite3_malloc(nDir + 1);
  if( !pSrv->zDir ) return SQLITE_NOMEM;
  memcpy(pSrv->zDir, zDir, nDir + 1);

  /* Create listening socket */
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

  /* Retrieve actual port (important when port==0) */
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

    /* Poll with 1-second timeout so we can check the running flag */
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

/*
** Start serving doltlite databases in a directory over HTTP.
** Blocks until stopped. Returns SQLITE_OK on clean shutdown.
*/
int doltliteServe(const char *zDir, int port){
  DoltliteServer server;
  int rc;

  rc = serverInit(&server, zDir, port);
  if( rc!=SQLITE_OK ) return rc;

  serverLoop(&server);
  serverCleanup(&server);
  return SQLITE_OK;
}

/* Thread entry point for async mode */
static void *serverThreadEntry(void *pArg){
  DoltliteServer *pSrv = (DoltliteServer*)pArg;
  serverLoop(pSrv);
  serverCleanup(pSrv);
  return 0;
}

/*
** Non-blocking: start server in background thread.
*/
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

/*
** Stop a running async server and free resources.
*/
void doltliteServerStop(DoltliteServer *pServer){
  if( !pServer ) return;
  pServer->running = 0;
  /* Close the listen socket to unblock accept() */
  if( pServer->listenFd >= 0 ){
    close(pServer->listenFd);
    pServer->listenFd = -1;
  }
  pthread_join(pServer->thread, 0);
  sqlite3_free(pServer);
}

/*
** Return the actual port the server is listening on.
*/
int doltliteServerPort(DoltliteServer *pServer){
  return pServer ? pServer->port : 0;
}

#endif /* !_WIN32 */
#endif /* DOLTLITE_PROLLY */
