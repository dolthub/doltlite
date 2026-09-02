
#ifndef DOLTLITE_ENABLE_REMOTES
#define DOLTLITE_ENABLE_REMOTES 1
#endif

#if defined(DOLTLITE_PROLLY) && DOLTLITE_ENABLE_REMOTES

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_creds.h"
#include "doltlite_net.h"
#include "doltlite_parse.h"
#ifdef DOLTLITE_HAVE_AUTH
#include "doltlite_tls.h"
#endif
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct HttpRemote HttpRemote;
struct HttpRemote {
  DoltliteRemote base;
  char *zHost;
  int port;
  int useTls;
  int timeoutMs;
  char *zBasePath;

#ifdef DOLTLITE_HAVE_AUTH
  DoltliteCreds *cred;
  char *zAudience;      /* JWT audience */
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
  char *zLastError;
};

#define HTTP_RESP_MAX_BYTES ((i64)128 * 1024 * 1024)
#define HTTP_UPLOAD_BATCH_MAX ((i64)32 * 1024 * 1024)
#define HTTP_TIMEOUT_MS 30000

static void httpClearLastError(HttpRemote *p){
  sqlite3_free(p->zLastError);
  p->zLastError = 0;
}

static void httpSetLastError(HttpRemote *p, const char *zMsg){
  httpClearLastError(p);
  if( zMsg && zMsg[0] ){
    p->zLastError = sqlite3_mprintf("%s", zMsg);
  }
}

static char *httpJsonStringField(const u8 *p, int n, const char *zKey){
  char zNeedle[64];
  int nNeedle;
  int i;
  if( !p || n<=0 || !zKey ) return 0;
  sqlite3_snprintf(sizeof(zNeedle), zNeedle, "\"%s\"", zKey);
  nNeedle = (int)strlen(zNeedle);
  for(i=0; i+nNeedle<n; i++){
    int j, k, start, end;
    if( memcmp(p+i, zNeedle, (size_t)nNeedle)!=0 ) continue;
    j = i + nNeedle;
    while( j<n && (p[j]==' ' || p[j]=='\t' || p[j]=='\r' || p[j]=='\n') ) j++;
    if( j>=n || p[j]!=':' ) continue;
    j++;
    while( j<n && (p[j]==' ' || p[j]=='\t' || p[j]=='\r' || p[j]=='\n') ) j++;
    if( j>=n || p[j]!='"' ) continue;
    start = ++j;
    while( j<n && p[j]!='"' ){
      if( p[j]=='\\' ) j++;
      j++;
    }
    if( j>=n ) return 0;
    end = j;
    {
      char *z = sqlite3_malloc(end - start + 1);
      if( !z ) return 0;
      /* Emitted bodies are plain ASCII (no escapes). */
      for(k=0; k<end-start; k++) z[k] = (char)p[start+k];
      z[end-start] = 0;
      return z;
    }
  }
  return 0;
}

static int httpJsonIntField(const u8 *p, int n, const char *zKey, int *pOut){
  char zNeedle[64];
  int nNeedle;
  int i;
  if( !p || n<=0 || !zKey || !pOut ) return 0;
  sqlite3_snprintf(sizeof(zNeedle), zNeedle, "\"%s\"", zKey);
  nNeedle = (int)strlen(zNeedle);
  for(i=0; i+nNeedle<n; i++){
    int j;
    uint64_t value;
    int neg = 0;
    if( memcmp(p+i, zNeedle, (size_t)nNeedle)!=0 ) continue;
    j = i + nNeedle;
    while( j<n && (p[j]==' ' || p[j]=='\t' || p[j]=='\r' || p[j]=='\n') ) j++;
    if( j>=n || p[j]!=':' ) continue;
    j++;
    while( j<n && (p[j]==' ' || p[j]=='\t' || p[j]=='\r' || p[j]=='\n') ) j++;
    if( j<n && p[j]=='-' ){ neg = 1; j++; }
    if( j>=n || p[j]<'0' || p[j]>'9' ) continue;
    {
      int k = j;
      while( k<n && p[k]>='0' && p[k]<='9' ) k++;
      if( doltliteParseDecimal((const char*)p+j, (const char*)p+k,
                              0x7fffffff, &value)!=DOLTLITE_DECIMAL_OK ){
        return 0;
      }
      *pOut = neg ? -(int)value : (int)value;
      return 1;
    }
  }
  return 0;
}

/* Map HTTP status to an SQLite code; stash xErrMsg. */
static int httpMapError(
  HttpRemote *p,
  int status,
  const u8 *pResp,
  int nResp
){
  char *zCode = 0;
  char *zMsg = 0;
  int sqliteRc = 0;
  int hasSqlite = 0;
  int rc;

  httpClearLastError(p);
  if( pResp && nResp>0 && pResp[0]=='{' ){
    zCode = httpJsonStringField(pResp, nResp, "code");
    zMsg = httpJsonStringField(pResp, nResp, "message");
    hasSqlite = httpJsonIntField(pResp, nResp, "sqlite", &sqliteRc);
  }

  if( zMsg ){
    httpSetLastError(p, zMsg);
  }else if( pResp && nResp>0 && pResp[0]!='{' ){
    /* Plain-text body from older servers. */
    char *zPlain = sqlite3_malloc(nResp + 1);
    if( zPlain ){
      memcpy(zPlain, pResp, (size_t)nResp);
      zPlain[nResp] = 0;
      while( nResp>0 && (zPlain[nResp-1]=='\n' || zPlain[nResp-1]=='\r') ){
        zPlain[--nResp] = 0;
      }
      if( zPlain[0] ) httpSetLastError(p, zPlain);
      sqlite3_free(zPlain);
    }
  }

  if( status==409
   || (zCode && strcmp(zCode, "refs_changed")==0)
   || (hasSqlite && sqliteRc==SQLITE_BUSY) ){
    rc = SQLITE_BUSY;
    if( !p->zLastError ){
      httpSetLastError(p, "remote refs changed; pull and retry");
    }
  }else if( status==401 || status==403
         || (zCode && (strcmp(zCode, "unauthorized")==0
                    || strcmp(zCode, "forbidden")==0))
         || (hasSqlite && sqliteRc==SQLITE_AUTH) ){
    /* 403 is auth failure; SQLite has no authz code. */
    rc = SQLITE_AUTH;
    if( !p->zLastError ){
      httpSetLastError(p, status==403 ? "forbidden" : "unauthorized");
    }
  }else if( status==404
         || (zCode && strcmp(zCode, "not_found")==0)
         || (hasSqlite && sqliteRc==SQLITE_NOTFOUND) ){
    rc = SQLITE_NOTFOUND;
    if( !p->zLastError ) httpSetLastError(p, "not found");
  }else if( status==413
         || (zCode && strcmp(zCode, "toobig")==0)
         || (hasSqlite && sqliteRc==SQLITE_TOOBIG) ){
    rc = SQLITE_TOOBIG;
    if( !p->zLastError ) httpSetLastError(p, "payload too large");
  }else if( (zCode && strcmp(zCode, "non_ff")==0)
         || (hasSqlite && sqliteRc==SQLITE_CONSTRAINT) ){
    rc = SQLITE_CONSTRAINT;
    if( !p->zLastError ){
      httpSetLastError(p,
        "not a fast-forward of the remote branch (use force to overwrite)");
    }
  }else if( hasSqlite && sqliteRc!=0 ){
    rc = sqliteRc;
  }else if( status>=500 ){
    rc = SQLITE_IOERR;
    if( !p->zLastError ) httpSetLastError(p, "remote server error");
  }else{
    rc = SQLITE_ERROR;
    if( !p->zLastError ) httpSetLastError(p, "remote request failed");
  }

  sqlite3_free(zCode);
  sqlite3_free(zMsg);
  return rc;
}

static int httpHeaderNameEquals(
  const u8 *zName,
  int nName,
  const char *zExpected
){
  int nExpected = (int)strlen(zExpected);
  return nName==nExpected
      && sqlite3_strnicmp((const char*)zName, zExpected, nExpected)==0;
}

#ifdef DOLTLITE_HAVE_AUTH
typedef DoltliteConn HttpConn;
#define httpConnOpen(H,P,T,M) doltliteConnOpenTimeout((H),(P),(T),(M))
#define httpConnWriteAll(C,B,N) doltliteConnWriteAll((C),(B),(N))
#define httpConnRead(C,B,N) doltliteConnRead((C),(B),(N))
#define httpConnClose(C) doltliteConnClose((C))
#else
typedef struct HttpConn HttpConn;
struct HttpConn {
  int fd;
  i64 deadlineMs;
};

static int httpConnApplyDeadline(HttpConn *pConn){
  i64 remaining = pConn->deadlineMs - doltliteMonotonicMs();
  if( remaining<=0 ) return 1;
  if( remaining>0x7fffffff ) remaining = 0x7fffffff;
  return doltliteSocketSetTimeout(pConn->fd, (int)remaining);
}

static HttpConn *httpConnOpen(
  const char *zHost,
  int port,
  int useTls,
  int timeoutMs
){
  char zPort[16];
  i64 deadlineMs = doltliteMonotonicMs() + timeoutMs;
  int fd;
  HttpConn *pConn;

  if( useTls ) return 0;
  sqlite3_snprintf(sizeof(zPort), zPort, "%d", port);
  fd = doltliteTcpConnect(zHost, zPort, timeoutMs);
  if( fd<0 ) return 0;

  pConn = sqlite3_malloc(sizeof(HttpConn));
  if( !pConn ){
    doltliteCloseSocket(fd);
    return 0;
  }
  pConn->fd = fd;
  pConn->deadlineMs = deadlineMs;
  if( httpConnApplyDeadline(pConn)!=0 ){
    doltliteCloseSocket(fd);
    sqlite3_free(pConn);
    return 0;
  }
  return pConn;
}

static int httpConnWriteAll(HttpConn *pConn, const void *pBuf, int nBuf){
  const char *z = (const char*)pBuf;
  int nSent = 0;
  while( nSent<nBuf ){
    int n;
    if( httpConnApplyDeadline(pConn)!=0 ) return 1;
    n = send(pConn->fd, z+nSent, nBuf-nSent, 0);
    if( n<=0 ) return 1;
    nSent += n;
  }
  return 0;
}

static int httpConnRead(HttpConn *pConn, void *pBuf, int nBuf){
  if( httpConnApplyDeadline(pConn)!=0 ) return -1;
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

  /* Callers scan as a C string; NUL-terminate even when nAlloc is filled. */
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

static int httpParseResponse(
  const u8 *pRaw,
  int nRaw,
  int *pStatus,
  u8 **ppResp,
  int *pnResp
){
  int i = 0;
  int bodyStart = -1;
  int contentLength = -1;
  int seenContentLength = 0;
  int seenTransferEncoding = 0;
  int statusStart;
  int statusEnd;
  uint64_t value;

  *pStatus = 0;
  *ppResp = 0;
  *pnResp = 0;

  while( i<nRaw && pRaw[i]!=' ' && pRaw[i]!='\r' && pRaw[i]!='\n' ) i++;
  if( i>=nRaw || pRaw[i]!=' ' ) return SQLITE_PROTOCOL;
  statusStart = ++i;
  while( i<nRaw && pRaw[i]!=' ' && pRaw[i]!='\r' && pRaw[i]!='\n' ) i++;
  statusEnd = i;
  if( statusEnd-statusStart!=3
   || doltliteParseDecimal(
        (const char*)pRaw+statusStart, (const char*)pRaw+statusEnd,
        999, &value)!=DOLTLITE_DECIMAL_OK
   || value<100 ){
    return SQLITE_PROTOCOL;
  }
  *pStatus = (int)value;

  while( i+1<nRaw && !(pRaw[i]=='\r' && pRaw[i+1]=='\n') ) i++;
  if( i+1>=nRaw ) return SQLITE_PROTOCOL;
  i += 2;
  while( i<nRaw ){
    int lineStart = i;
    int lineEnd;
    int colon;
    int valueStart;
    int valueEnd;
    int parseRc;

    if( i+1<nRaw && pRaw[i]=='\r' && pRaw[i+1]=='\n' ){
      bodyStart = i + 2;
      break;
    }
    while( i+1<nRaw && !(pRaw[i]=='\r' && pRaw[i+1]=='\n') ) i++;
    if( i+1>=nRaw ) return SQLITE_PROTOCOL;
    lineEnd = i;
    i += 2;

    colon = lineStart;
    while( colon<lineEnd && pRaw[colon]!=':' ) colon++;
    if( colon==lineStart || colon==lineEnd ) continue;

    valueStart = colon + 1;
    while( valueStart<lineEnd
        && (pRaw[valueStart]==' ' || pRaw[valueStart]=='\t') ){
      valueStart++;
    }
    valueEnd = lineEnd;
    while( valueEnd>valueStart
        && (pRaw[valueEnd-1]==' ' || pRaw[valueEnd-1]=='\t') ){
      valueEnd--;
    }

    if( httpHeaderNameEquals(
          pRaw+lineStart, colon-lineStart, "Content-Length") ){
      if( seenContentLength ) return SQLITE_PROTOCOL;
      seenContentLength = 1;
      parseRc = doltliteParseDecimal(
          (const char*)pRaw+valueStart, (const char*)pRaw+valueEnd,
          (uint64_t)HTTP_RESP_MAX_BYTES, &value);
      if( parseRc!=DOLTLITE_DECIMAL_OK ){
        return parseRc==DOLTLITE_DECIMAL_RANGE
             ? SQLITE_TOOBIG : SQLITE_PROTOCOL;
      }
      contentLength = (int)value;
    }else if( httpHeaderNameEquals(
                 pRaw+lineStart, colon-lineStart, "Transfer-Encoding") ){
      seenTransferEncoding = 1;
    }
  }

  if( bodyStart<0 || seenTransferEncoding ) return SQLITE_PROTOCOL;

  {
    int nAvail = nRaw - bodyStart;
    int nCopy = contentLength>=0 ? contentLength : nAvail;
    if( contentLength>=0 && contentLength!=nAvail ) return SQLITE_PROTOCOL;
    if( nCopy>0 ){
      *ppResp = sqlite3_malloc(nCopy);
      if( !*ppResp ) return SQLITE_NOMEM;
      memcpy(*ppResp, pRaw + bodyStart, nCopy);
      *pnResp = nCopy;
    }
  }

  return SQLITE_OK;
}

/* Emscripten has no usable sockets: connect() is emulated as a WebSocket dial,
** so the socket client above cannot reach an ordinary HTTP server from a
** browser or from node. Requests go through the host's own HTTP stack
** instead, which also hands TLS to the host and makes https remotes work
** without mbedtls. The call has to block, because it runs inside a SQL
** statement: XHR in synchronous mode where it exists, and a worker signalled
** through Atomics.wait under node. */
#if defined(__EMSCRIPTEN__)
#include <emscripten.h>

EM_JS(int, doltliteWasmHttpSend, (const char *zUrl, const char *zMethod,
                                  const char *zAuth,
                                  const unsigned char *pBody, int nBody,
                                  int timeoutMs), {
  var st = Module.__doltliteHttp || (Module.__doltliteHttp = {});
  st.resp = null;
  var url = UTF8ToString(zUrl);
  var method = UTF8ToString(zMethod);
  var auth = zAuth ? UTF8ToString(zAuth) : "";
  var body = null;
  if (nBody > 0) {
    body = new Uint8Array(nBody);
    body.set(HEAPU8.subarray(pBody, pBody + nBody));
  }
  try {
    if (typeof XMLHttpRequest !== "undefined") {
      var xhr = new XMLHttpRequest();
      xhr.open(method, url, false);
      var binaryString = false;
      try {
        xhr.responseType = "arraybuffer";
      } catch (e) {
        binaryString = true;
      }
      if (xhr.responseType !== "arraybuffer") binaryString = true;
      if (binaryString && xhr.overrideMimeType) {
        xhr.overrideMimeType("text/plain; charset=x-user-defined");
      }
      if (auth) xhr.setRequestHeader("Authorization", auth);
      if (body) xhr.setRequestHeader("Content-Type", "application/octet-stream");
      xhr.send(body);
      if (binaryString) {
        var text = xhr.responseText || "";
        var out = new Uint8Array(text.length);
        for (var i = 0; i < text.length; i++) out[i] = text.charCodeAt(i) & 0xff;
        st.resp = out;
      } else {
        st.resp = xhr.response ? new Uint8Array(xhr.response) : new Uint8Array(0);
      }
      return xhr.status;
    }
    if (typeof require === "function" && typeof SharedArrayBuffer !== "undefined") {
      if (!st.node) {
        var wt = require("node:worker_threads");
        var src = [
          "const { workerData } = require('node:worker_threads');",
          "const ctl = new Int32Array(workerData.ctl);",
          "const req = workerData.req;",
          "const data = workerData.data;",
          "const dec = new TextDecoder();",
          "(async function(){ for(;;){",
          "  Atomics.wait(ctl, 0, 0);",
          "  if(Atomics.load(ctl,0) !== 1) continue;",
          "  let status = 0, len = 0, over = 0, bad = 0;",
          "  try {",
          "    const nj = Atomics.load(ctl,4), nb = Atomics.load(ctl,5);",
          "    const spec = JSON.parse(dec.decode(new Uint8Array(req, 0, nj)));",
          "    const opt = { method: spec.method };",
          "    if(spec.timeoutMs > 0 && typeof AbortSignal !== 'undefined'",
          "       && AbortSignal.timeout) opt.signal = AbortSignal.timeout(spec.timeoutMs);",
          "    if(spec.auth) opt.headers = { Authorization: spec.auth };",
          "    if(nb > 0){",
          "      opt.body = Buffer.from(new Uint8Array(req, nj, nb));",
          "      opt.headers = Object.assign({}, opt.headers, {'Content-Type':'application/octet-stream'});",
          "    }",
          "    const r = await fetch(spec.url, opt);",
          "    status = r.status;",
          "    const b = new Uint8Array(await r.arrayBuffer());",
          "    len = b.length;",
          "    if(len > data.byteLength){ over = 1; }",
          "    else { new Uint8Array(data).set(b); }",
          "  } catch(e){ bad = 1; }",
          "  Atomics.store(ctl,1,status); Atomics.store(ctl,2,len);",
          "  Atomics.store(ctl,3,bad); Atomics.store(ctl,6,over);",
          "  Atomics.store(ctl,0,2); Atomics.notify(ctl,0);",
          "} })();"
        ].join("\n");
        var ctlSab = new SharedArrayBuffer(32);
        st.node = {
          ctl: new Int32Array(ctlSab),
          ctlSab: ctlSab,
          req: new SharedArrayBuffer(8 * 1024 * 1024),
          data: new SharedArrayBuffer(8 * 1024 * 1024),
          src: src,
          wt: wt
        };
        st.node.worker = new wt.Worker(src, {
          eval: true,
          workerData: { ctl: ctlSab, req: st.node.req, data: st.node.data }
        });
        st.node.worker.unref();
      }
      var n = st.node;
      for (;;) {
        var spec = JSON.stringify({ url: url, method: method, auth: auth,
                                    timeoutMs: timeoutMs });
        var enc = new TextEncoder().encode(spec);
        if (enc.length + (body ? body.length : 0) > n.req.byteLength) return 0;
        new Uint8Array(n.req).set(enc, 0);
        if (body) new Uint8Array(n.req).set(body, enc.length);
        Atomics.store(n.ctl, 4, enc.length);
        Atomics.store(n.ctl, 5, body ? body.length : 0);
        Atomics.store(n.ctl, 6, 0);
        Atomics.store(n.ctl, 0, 1);
        Atomics.notify(n.ctl, 0);
        Atomics.wait(n.ctl, 0, 1);
        var over = Atomics.load(n.ctl, 6);
        var need = Atomics.load(n.ctl, 2);
        var bad = Atomics.load(n.ctl, 3);
        var status = Atomics.load(n.ctl, 1);
        Atomics.store(n.ctl, 0, 0);
        if (over) {
          var grow = n.data.byteLength;
          while (grow < need) grow *= 2;
          n.data = new SharedArrayBuffer(grow);
          n.worker.terminate();
          n.worker = new n.wt.Worker(n.src, {
            eval: true,
            workerData: { ctl: n.ctlSab, req: n.req, data: n.data }
          });
          n.worker.unref();
          continue;
        }
        if (bad) return 0;
        st.resp = new Uint8Array(need);
        st.resp.set(new Uint8Array(n.data, 0, need));
        return status;
      }
    }
  } catch (e) {
    st.resp = null;
    return 0;
  }
  return 0;
});

EM_JS(int, doltliteWasmHttpRespLen, (void), {
  var st = Module.__doltliteHttp;
  return (st && st.resp) ? st.resp.length : 0;
});

EM_JS(void, doltliteWasmHttpRespTake, (unsigned char *pDest), {
  var st = Module.__doltliteHttp;
  if (st && st.resp) HEAPU8.set(st.resp, pDest);
  if (st) st.resp = null;
});

static int httpRequest(
  HttpRemote *p,
  const char *zMethod,
  const char *zPath,
  const u8 *pBody, i64 nBody,
  int *pStatus,
  u8 **ppResp, int *pnResp
){
  char *zUrl;
  char *zAuth = 0;
  int status;
  int nResp;

  *pStatus = 0;
  *ppResp = 0;
  *pnResp = 0;
  httpClearLastError(p);

  if( nBody > 0x7fffffff ) return SQLITE_TOOBIG;

  zUrl = sqlite3_mprintf("%s%s:%d%s", p->useTls ? "https://" : "http://",
                         p->zHost, p->port, zPath);
  if( !zUrl ) return SQLITE_NOMEM;

#ifdef DOLTLITE_HAVE_AUTH
  if( p->useTls && p->cred ){
    char *jwt = 0;
    if( doltliteCredsBearerToken(p->cred, p->zAudience, &jwt)==0 && jwt ){
      zAuth = sqlite3_mprintf("Bearer %s", jwt);
    }
    if( jwt ) sqlite3_free(jwt);
  }
#endif

  status = doltliteWasmHttpSend(zUrl, zMethod, zAuth, pBody, (int)nBody,
                                p->timeoutMs);
  sqlite3_free(zUrl);
  sqlite3_free(zAuth);
  if( status<=0 ){
    httpSetLastError(p, "could not connect to remote");
    return SQLITE_ERROR;
  }

  nResp = doltliteWasmHttpRespLen();
  if( nResp<0 || (i64)nResp>HTTP_RESP_MAX_BYTES ) return SQLITE_TOOBIG;
  if( nResp>0 ){
    /* One extra byte: callers scan the body as a C string. */
    u8 *pResp = sqlite3_malloc(nResp + 1);
    if( !pResp ) return SQLITE_NOMEM;
    doltliteWasmHttpRespTake(pResp);
    pResp[nResp] = 0;
    *ppResp = pResp;
    *pnResp = nResp;
  }
  *pStatus = status;
  return SQLITE_OK;
}

#else
static int httpRequest(
  HttpRemote *p,
  const char *zMethod,
  const char *zPath,
  const u8 *pBody, i64 nBody,
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
  httpClearLastError(p);

  conn = httpConnOpen(p->zHost, p->port, p->useTls, p->timeoutMs);
  if( !conn ){
    httpSetLastError(p, "could not connect to remote");
    return SQLITE_ERROR;
  }

#ifdef DOLTLITE_HAVE_AUTH
  if( p->useTls && p->cred ){
    char *jwt = 0;
    if( doltliteCredsBearerToken(p->cred, p->zAudience, &jwt)==0 && jwt ){
      zAuth = sqlite3_mprintf("Authorization: Bearer %s\r\n", jwt);
    }
    if( jwt ) sqlite3_free(jwt);
  }
#endif

  if( pBody && nBody > 0 ){
    zHdr = sqlite3_mprintf(
      "%s %s HTTP/1.1\r\n"
      "Host: %s\r\n"
      "%s"
      "Content-Length: %lld\r\n"
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
    i64 off = 0;
    while( off<nBody ){
      i64 nRemain = nBody - off;
      int nWrite = nRemain>0x7fffffff ? 0x7fffffff : (int)nRemain;
      if( httpConnWriteAll(conn, pBody+off, nWrite) ){
        httpConnClose(conn);
        return SQLITE_IOERR;
      }
      off += nWrite;
    }
  }

  rc = readUntilEof(conn, &pRaw, &nRaw);
  httpConnClose(conn);
  if( rc != SQLITE_OK ) return rc;
  rc = httpParseResponse(pRaw, nRaw, pStatus, ppResp, pnResp);
  sqlite3_free(pRaw);
  return rc;
}
#endif /* __EMSCRIPTEN__ */

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

static int httpFlushUploadBatch(HttpRemote *p){
  char *zPath;
  int status = 0;
  u8 *pResp = 0;
  int nResp = 0;
  int rc;

  if( p->nUploadBuf==0 ) return SQLITE_OK;
  zPath = buildPath(p, "/chunks");
  if( !zPath ) return SQLITE_NOMEM;
  rc = httpRequest(p, "POST", zPath, p->pUploadBuf, p->nUploadBuf,
                   &status, &pResp, &nResp);
  sqlite3_free(zPath);
  if( rc==SQLITE_OK && status!=200 && status!=204 ){
    rc = httpMapError(p, status, pResp, nResp);
  }
  sqlite3_free(pResp);
  if( rc==SQLITE_OK ) p->nUploadBuf = 0;
  return rc;
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

  if( rc != SQLITE_OK ){
    sqlite3_free(pResp);
    return rc;
  }
  if( status != 200 ){
    rc = httpMapError(p, status, pResp, nResp);
    sqlite3_free(pResp);
    return rc;
  }

  if( nResp!=nHash ){
    sqlite3_free(pResp);
    return SQLITE_PROTOCOL;
  }
  {
    int i;
    for(i=0; i<nHash; i++){
      if( pResp[i]>1 ){
        sqlite3_free(pResp);
        return SQLITE_PROTOCOL;
      }
    }
  }
  memcpy(aResult, pResp, nHash);
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
  if( status != 200 ){
    rc = httpMapError(p, status, pResp, nResp);
    sqlite3_free(pResp);
    return rc;
  }

  *ppData = pResp;
  *pnData = nResp;
  return SQLITE_OK;
}

static int httpPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                        const u8 *pData, int nData){
  HttpRemote *p = (HttpRemote*)pRemote;
  u8 aLen[4];
  i64 nRecord;
  i64 nSaved;
  int rc;

  if( nData<0 ) return SQLITE_CORRUPT;
  nRecord = PROLLY_HASH_SIZE + 4 + (i64)nData;
  if( p->nUploadBuf>0
   && p->nUploadBuf + nRecord > HTTP_UPLOAD_BATCH_MAX ){
    rc = httpFlushUploadBatch(p);
    if( rc!=SQLITE_OK ) return rc;
  }
  nSaved = p->nUploadBuf;

  rc = uploadBufAppend(p, pHash->data, PROLLY_HASH_SIZE);
  if( rc != SQLITE_OK ) goto put_error;

  aLen[0] = (u8)(nData & 0xff);
  aLen[1] = (u8)((nData >> 8) & 0xff);
  aLen[2] = (u8)((nData >> 16) & 0xff);
  aLen[3] = (u8)((nData >> 24) & 0xff);
  rc = uploadBufAppend(p, aLen, 4);
  if( rc != SQLITE_OK ) goto put_error;

  rc = uploadBufAppend(p, pData, nData);
  if( rc != SQLITE_OK ) goto put_error;
  return SQLITE_OK;

put_error:
  p->nUploadBuf = nSaved;
  return rc;
}

static int httpParseChunkBatch(
  const u8 *pResp,
  int nResp,
  int nHash,
  u8 **apData,
  int *anData
){
  i64 poff = 0;
  int rc = SQLITE_OK;
  int i;

  for(i=0; i<nHash; i++){ apData[i] = 0; anData[i] = 0; }
  for(i=0; i<nHash; i++){
    u32 len;
    if( poff + 4 > nResp ){ rc = SQLITE_ERROR; break; }
    len = ((u32)pResp[poff] << 24) | ((u32)pResp[poff+1] << 16)
        | ((u32)pResp[poff+2] << 8) | (u32)pResp[poff+3];
    poff += 4;
    if( len == 0xFFFFFFFFu ) continue;
    if( poff + (i64)len > nResp ){ rc = SQLITE_ERROR; break; }
    apData[i] = sqlite3_malloc(len ? (int)len : 1);
    if( !apData[i] ){ rc = SQLITE_NOMEM; break; }
    memcpy(apData[i], pResp + poff, len);
    anData[i] = (int)len;
    poff += len;
  }
  return rc;
}

int doltliteHttpParseResponseForTest(
  const u8 *pRaw,
  int nRaw,
  int nHash
){
  u8 *apData[64];
  int anData[64];
  u8 *pResp = 0;
  int nResp = 0;
  int status = 0;
  int rc;
  int i;

  if( nHash<0 || nHash>64 ) return SQLITE_MISUSE;
  rc = httpParseResponse(pRaw, nRaw, &status, &pResp, &nResp);
  if( rc==SQLITE_OK && status==200 ){
    rc = httpParseChunkBatch(pResp, nResp, nHash, apData, anData);
    for(i=0; i<nHash; i++) sqlite3_free(apData[i]);
  }
  sqlite3_free(pResp);
  return rc;
}

/* POST hashes to /get-chunks; reply is per-hash [u32be len][bytes],
** 0xFFFFFFFF if absent. */
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
  if( status != 200 ){
    rc = httpMapError(p, status, pResp, nResp);
    sqlite3_free(pResp);
    return rc;
  }
  rc = httpParseChunkBatch(pResp, nResp, nHash, apData, anData);
  sqlite3_free(pResp);
  return rc;   /* caller frees apData[i] set before an error */
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
  if( status != 200 ){
    rc = httpMapError(p, status, pResp, nResp);
    sqlite3_free(pResp);
    return rc;
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

  rc = httpFlushUploadBatch(p);
  if( rc!=SQLITE_OK ) return rc;

  if( p->pPendingRefs && p->nPendingRefs > 0 ){
    /* [u16 branchLen][branch][u8 force], then refs-if expected hash, then refs. */
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
    if( rc != SQLITE_OK ){
      sqlite3_free(pResp);
      return rc;
    }
    if( status != 200 && status != 204 ){
      rc = httpMapError(p, status, pResp, nResp);
      sqlite3_free(pResp);
      return rc;
    }
    sqlite3_free(pResp);
  }

  {
    pResp = 0; nResp = 0; status = 0;
    zPath = buildPath(p, "/commit");
    if( !zPath ) return SQLITE_NOMEM;

    rc = httpRequest(p, "POST", zPath,
                     0, 0, &status, &pResp, &nResp);
    sqlite3_free(zPath);
    if( rc != SQLITE_OK ){
      sqlite3_free(pResp);
      return rc;
    }
    if( status != 200 && status != 204 ){
      rc = httpMapError(p, status, pResp, nResp);
      sqlite3_free(pResp);
      return rc;
    }
    sqlite3_free(pResp);
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

static const char *httpErrMsg(DoltliteRemote *pRemote){
  HttpRemote *p = (HttpRemote*)pRemote;
  return p->zLastError;
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
  httpClearLastError(p);
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
    sqlite3_free(dir);
  }
  if( cred ){
    const char *aud = getenv("DOLT_OVERRIDE_GRPC_JWT_AUDIENCE");
    if( aud && *aud ){
      p->zAudience = sqlite3_mprintf("%s", aud);
    }else if( strncmp(p->zHost, "doltliteremoteapi.", 18)==0 ){
      /* doltremoteapi audience for dolthub. */
      p->zAudience = sqlite3_mprintf("doltremoteapi.%s", p->zHost + 18);
    }else{
      p->zAudience = sqlite3_mprintf("%s", p->zHost);
    }
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
  const char *zTimeout;
  uint64_t timeoutMs;

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
      uint64_t value;
      zPortStart = c + 1;
      c = zPortStart;
      while( *c && *c != '/' ) c++;
      if( doltliteParseDecimal(zPortStart, c, 65535, &value)
          !=DOLTLITE_DECIMAL_OK
       || value==0 ){
        return 0;
      }
      port = (int)value;
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
  timeoutMs = HTTP_TIMEOUT_MS;
  zTimeout = getenv("DOLTLITE_HTTP_TIMEOUT_MS");
  if( zTimeout && zTimeout[0] ){
    if( doltliteParseDecimal(
          zTimeout, zTimeout+strlen(zTimeout), 0x7fffffff, &timeoutMs)
          !=DOLTLITE_DECIMAL_OK
     || timeoutMs==0 ){
      timeoutMs = HTTP_TIMEOUT_MS;
    }
  }
  p->timeoutMs = (int)timeoutMs;

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
  p->base.xErrMsg = httpErrMsg;
  p->base.bResumePartialPuts = 1;

  return &p->base;
}

#endif
