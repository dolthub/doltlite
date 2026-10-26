#include "sqlite3.h"
#include "doltlite_remote.h"
#include "doltlite_tls.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <errno.h>

static int failures;
static void check(const char *name, int ok){
  if( !ok ){ fprintf(stderr, "FAIL: %s\n", name); failures++; }
}

enum {
  SLOW_READ, STALL_READ, SLOW_UPLOAD, STALL_UPLOAD, STALE_UPLOAD, DROP_UPLOAD, DROP_ACK,
  DROP_ALWAYS, DROP_REFS, DROP_REFS_IF, DROP_COMMIT, REJECT_UPLOAD
};
typedef struct TransferServer TransferServer;
struct TransferServer {
  int fd, port, stop, mode, connections, uploads, commits, refs, reads, stored, bad;
  unsigned char *body;
  int nBody;
  pthread_mutex_t mutex;
  pthread_t thread;
  DoltliteTlsServer *tls;
};
static int stopped(TransferServer *s){
  int stop;
  pthread_mutex_lock(&s->mutex);
  stop = s->stop;
  pthread_mutex_unlock(&s->mutex);
  return stop;
}
static int reply(DoltliteConn *c, int status, const void *body, int size){
  char header[160];
  int n = snprintf(header, sizeof(header),
      "HTTP/1.1 %d Result\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n",
      status, size);
  return doltliteConnWriteAll(c, header, n)
      || (body && size && doltliteConnWriteAll(c, body, size));
}
static void handle(TransferServer *s, DoltliteConn *c){
  while( !stopped(s) ){
    char header[8192], method[16], path[128];
    const char *length;
    unsigned char *body = 0;
    int n = 0, size = 0, offset = 0, isUpload;
    while( n<(int)sizeof(header)-1 ){
      if( doltliteConnRead(c, header+n, 1)!=1 ) return;
      n++;
      if( n>=4 && memcmp(header+n-4, "\r\n\r\n", 4)==0 ) break;
    }
    header[n] = 0;
    if( sscanf(header, "%15s %127s", method, path)!=2 ){ s->bad++; return; }
    length = strstr(header, "Content-Length:");
    if( length ) size = atoi(length+15);
    if( size<0 || size>40*1024*1024 ){ s->bad++; return; }
    isUpload = strcmp(method, "POST")==0 && strcmp(path, "/repo.db/chunks")==0;
    if( isUpload ) s->uploads++;
    if( isUpload && s->mode==STALL_UPLOAD ){
      while( !stopped(s) ) sqlite3_sleep(10);
      return;
    }
    if( isUpload && (s->mode==DROP_ALWAYS || (s->mode==DROP_UPLOAD && s->uploads==1)) ) return;
    if( size ) body = malloc(size);
    if( size && !body ){ s->bad++; return; }
    while( offset<size ){
      int want = size-offset;
      int got;
      if( want>32768 ) want = 32768;
      got = doltliteConnRead(c, body+offset, want);
      if( got<=0 ){ free(body); return; }
      offset += got;
      if( s->mode==SLOW_UPLOAD && size-offset>4*1024*1024 ) sqlite3_sleep(15);
    }
    if( isUpload ){
      ProllyHash computed;
      if( size<PROLLY_HASH_SIZE+4 ){ s->bad++; free(body); return; }
      prollyHashCompute(body+PROLLY_HASH_SIZE+4, size-PROLLY_HASH_SIZE-4, &computed);
      if( memcmp(body, computed.data, PROLLY_HASH_SIZE) ) s->bad++;
      if( s->mode==REJECT_UPLOAD ){
        free(body);
        reply(c, 403, 0, 0);
        return;
      }
      if( s->body ){
        if( size!=s->nBody || memcmp(body, s->body, size) ) s->bad++;
        free(body);
      }else{
        s->body = body;
        s->nBody = size;
        s->stored++;
      }
      if( s->mode==DROP_ACK && s->uploads==1 ) return;
      if( reply(c, 200, 0, 0) ) return;
    }else{
      free(body);
      if( strcmp(method, "PUT")==0 ){
        s->refs++;
        if( s->mode==DROP_REFS || s->mode==DROP_REFS_IF ) return;
        if( reply(c, 200, 0, 0) ) return;
      }else if( strcmp(path, "/repo.db/commit")==0 ){
        s->commits++;
        if( s->mode==DROP_COMMIT ) return;
        if( reply(c, 200, 0, 0) ) return;
      }else if( strcmp(path, "/repo.db/refs")==0 ){
        s->reads++;
        if( s->mode==SLOW_READ || s->mode==STALL_READ ){
          int i;
          if( reply(c, 200, 0, 24) ) return;
          if( s->mode==STALL_READ ){
            char ignored;
            doltliteConnRead(c, &ignored, 1);
            return;
          }
          for(i=0; i<24; i++){
            sqlite3_sleep(150);
            if( doltliteConnWriteAll(c, "x", 1) ) return;
          }
        }else{
          if( reply(c, 200, "x", 1) || s->mode==STALE_UPLOAD ) return;
        }
      }else if( strncmp(path, "/repo.db/chunk/", 15)==0 ){
        if( s->nBody<PROLLY_HASH_SIZE+4 ){ s->bad++; return; }
        if( reply(c, 200, s->body+PROLLY_HASH_SIZE+4, s->nBody-PROLLY_HASH_SIZE-4) ) return;
      }else{
        s->bad++;
        return;
      }
    }
  }
}
static void *serve(void *arg){
  TransferServer *s = arg;
  while( !stopped(s) ){
    fd_set reads;
    struct timeval wait = {0, 100000};
    int fd, small = 32768;
    struct timeval timeout = {5, 0};
    DoltliteConn *c;
    FD_ZERO(&reads);
    FD_SET(s->fd, &reads);
    if( select(s->fd+1, &reads, 0, 0, &wait)<=0 ) continue;
    fd = accept(s->fd, 0, 0);
    if( fd<0 ) continue;
    s->connections++;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if( s->mode==SLOW_UPLOAD || s->mode==STALL_UPLOAD ) setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    c = s->tls ? doltliteConnServerAttach(s->tls, fd) : doltliteConnFromFd(fd);
    if( !c ){ s->bad++; continue; }
    if( !s->tls || doltliteConnHandshakeStep(c)==0 ) handle(s, c);
    doltliteConnClose(c);
  }
  return 0;
}
static int start(TransferServer *s, int mode, DoltliteTlsServer *tls){
  struct sockaddr_in addr;
  socklen_t len = sizeof(addr);
  memset(s, 0, sizeof(*s));
  s->mode = mode;
  s->tls = tls;
  pthread_mutex_init(&s->mutex, 0);
  s->fd = socket(AF_INET, SOCK_STREAM, 0);
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if( s->fd<0 || bind(s->fd, (struct sockaddr*)&addr, sizeof(addr))
   || listen(s->fd, 8) || getsockname(s->fd, (struct sockaddr*)&addr, &len) ) return 0;
  s->port = ntohs(addr.sin_port);
  return pthread_create(&s->thread, 0, serve, s)==0;
}
static void stop(TransferServer *s){
  pthread_mutex_lock(&s->mutex);
  s->stop = 1;
  pthread_mutex_unlock(&s->mutex);
  pthread_join(s->thread, 0);
  close(s->fd);
  pthread_mutex_destroy(&s->mutex);
}
static void runCase(int mode, const char *name, DoltliteTlsServer *tls){
  TransferServer s;
  DoltliteRemote *r;
  char url[128];
  int before = failures, rc = SQLITE_ERROR, n = 0;
  unsigned char *data = 0, *out = 0;
  ProllyHash hash;
  check("start test server", start(&s, mode, tls));
  if( failures!=before ) return;
  snprintf(url, sizeof(url), "%s://localhost:%d/repo.db", tls ? "https" : "http", s.port);
  r = doltliteHttpRemoteOpen(url);
  check("open HTTP remote", r!=0);
  if( !r ){ stop(&s); return; }
  if( mode==SLOW_READ || mode==STALL_READ ){
    rc = r->xGetRefs(r, &out, &n);
    if( mode==SLOW_READ ) check("response progresses beyond total timeout", rc==SQLITE_OK && n==24);
    else check("stalled response names timeout", rc!=SQLITE_OK && r->xErrMsg(r)
        && strstr(r->xErrMsg(r), "timed out")!=0);
    sqlite3_free(out);
  }else if( mode==DROP_REFS || mode==DROP_REFS_IF ){
    memset(&hash, 0, sizeof(hash));
    rc = mode==DROP_REFS
        ? r->xSetRefs(r, "main", 0, (const unsigned char*)"x", 1)
        : r->xSetRefsIf(r, &hash, "main", 0, (const unsigned char*)"x", 1);
    check("queue refs update", rc==SQLITE_OK);
    rc = r->xCommit(r);
    check("ambiguous ref update fails", rc!=SQLITE_OK);
  }else if( mode==DROP_COMMIT ){
    rc = r->xCommit(r);
    check("ambiguous commit fails", rc!=SQLITE_OK);
  }else{
    int size = mode==SLOW_UPLOAD || mode==STALL_UPLOAD || mode==DROP_UPLOAD
             || mode==DROP_ALWAYS ? 16*1024*1024 : 128*1024;
    data = malloc(size);
    check("allocate chunk", data!=0);
    if( data ){
      if( mode==STALE_UPLOAD ){
        check("prime cached connection", r->xGetRefs(r, &out, &n)==SQLITE_OK);
        sqlite3_free(out);
        out = 0;
      }
      memset(data, 'a', size);
      prollyHashCompute(data, size, &hash);
      check("queue upload", r->xPutChunk(r, &hash, data, size)==SQLITE_OK);
      rc = r->xCommit(r);
      if( mode==STALL_UPLOAD ) check("stalled upload names timeout", rc!=SQLITE_OK && r->xErrMsg(r)
          && strstr(r->xErrMsg(r), "upload timed out")!=0);
      else if( mode==DROP_ALWAYS || mode==REJECT_UPLOAD ) check("failed upload is bounded", rc!=SQLITE_OK);
      else{
        check("upload succeeds", rc==SQLITE_OK);
        if( rc==SQLITE_OK ){
          rc = r->xGetChunk(r, &hash, &out, &n);
          check("retried chunk is unchanged", rc==SQLITE_OK && n==size && memcmp(out, data, size)==0);
          sqlite3_free(out);
        }
      }
      free(data);
    }
  }
  if( failures!=before ) fprintf(stderr, "%s: %s\n", name, r->xErrMsg(r) ? r->xErrMsg(r) : "no transport error");
  r->xClose(r);
  stop(&s);
  check("server protocol", s.bad==0);
  if( mode==DROP_REFS || mode==DROP_REFS_IF ) check("refs update is not replayed", s.refs==1 && s.commits==0);
  if( mode==DROP_COMMIT ) check("commit is not replayed", s.commits==1);
  if( mode==STALL_READ ) check("stalled read is not replayed", s.reads==1);
  if( mode==STALL_UPLOAD ) check("stalled upload is not replayed", s.uploads==1);
  if( mode==STALE_UPLOAD ) check("stale upload connection reopened", s.connections==2 && s.uploads==1);
  if( mode==DROP_ALWAYS ) check("upload retry limit", s.uploads==2);
  if( mode==REJECT_UPLOAD ) check("HTTP rejection is not replayed", s.uploads==1);
  if( mode==DROP_UPLOAD || mode==DROP_ACK ) check("upload retried once", s.uploads==2 && s.stored==1 && s.commits==1);
  free(s.body);
  printf("%s %s: %s\n", tls ? "HTTPS" : "HTTP", name, failures==before ? "PASS" : "FAIL");
}
int main(int argc, char **argv){
  DoltliteTlsServer *tls = 0;
  int i;
  const char *names[] = {"slow response", "stalled response", "slow upload", "stalled upload", "stale upload connection",
      "dropped upload", "lost upload acknowledgement", "repeated disconnect",
      "ambiguous refs", "ambiguous refs CAS", "ambiguous commit", "HTTP rejection"};
  if( argc==3 ){
    tls = doltliteTlsServerNew(argv[1], argv[2]);
    if( !tls ){ fprintf(stderr, "cannot initialize test TLS server\n"); return 1; }
  }
  setenv("DOLTLITE_HTTP_TIMEOUT_MS", "2000", 1);
  for(i=SLOW_READ; i<=REJECT_UPLOAD; i++) runCase(i, names[i], tls);
  if( tls ) doltliteTlsServerFree(tls);
  printf("http_transfer_test: %d failures\n", failures);
  return failures!=0;
}
