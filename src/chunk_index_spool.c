#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"

#define CS_SPOOL_BUFFER 8192
#define CS_SPOOL_BLOCK 128
#define CS_SPOOL_CACHE (CS_SPOOL_BUFFER/CS_SPOOL_BLOCK)

struct ChunkIndexSpool {
  sqlite3_vfs *pVfs;
  sqlite3_file *pFile;
  i64 iWrite;
  i64 iRun;
  int nRun;
  int nRunEntry;
  int nBuffer;
  int nEntry;
  int ready;
  ProllyHash last;
  int aCache[CS_SPOOL_CACHE];
  ChunkIndexEntry aBuffer[CS_SPOOL_BUFFER];
};

typedef struct CsSpoolReader CsSpoolReader;
struct CsSpoolReader {
  sqlite3_file *pFile;
  i64 iRead;
  int nLeft;
  int i;
  int n;
  ChunkIndexEntry a[CS_SPOOL_BLOCK];
};

static int csSpoolOpen(sqlite3_vfs *pVfs, sqlite3_file **ppFile){
  return sqlite3OsOpenMalloc(pVfs, 0, ppFile,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_EXCLUSIVE
      | SQLITE_OPEN_DELETEONCLOSE | SQLITE_OPEN_TRANSIENT_DB, 0);
}

void csIndexSpoolFree(ChunkIndexSpool *p){
  if( !p ) return;
  if( p->pFile ) sqlite3OsCloseFree(p->pFile);
  sqlite3_free(p);
}

int csIndexSpoolInit(sqlite3_vfs *pVfs, ChunkIndexSpool **pp){
  ChunkIndexSpool *p = sqlite3_malloc64(sizeof(*p));
  int rc;
  *pp = 0;
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->pVfs = pVfs;
  rc = csSpoolOpen(pVfs, &p->pFile);
  if( rc!=SQLITE_OK ){
    csIndexSpoolFree(p);
    return rc;
  }
  *pp = p;
  return SQLITE_OK;
}

static int csSpoolWrite(sqlite3_file *pFile, const void *data, int n, i64 offset){
  const u8 *a = data;
  while( n>0 ){
    int size = MIN(n, 65536);
    int rc = sqlite3OsWrite(pFile, a, size, offset);
    if( rc!=SQLITE_OK ) return rc;
    a += size;
    offset += size;
    n -= size;
  }
  return SQLITE_OK;
}

static int csSpoolHeader(sqlite3_file *pFile, i64 iOff, int n){
  u8 a[8];
  CS_WRITE_I64(a, n);
  return csSpoolWrite(pFile, a, sizeof(a), iOff);
}

static int csSpoolFlush(ChunkIndexSpool *p){
  int i, n = 0, sorted = 1;
  int rc;
  if( p->nBuffer==0 ) return SQLITE_OK;
  for(i=1; i<p->nBuffer; i++){
    if( csIndexEntryCmp(&p->aBuffer[i-1], &p->aBuffer[i])>0 ){
      sorted = 0;
      break;
    }
  }
  if( !sorted ) qsort(p->aBuffer, p->nBuffer, sizeof(p->aBuffer[0]), csIndexEntryCmp);
  for(i=0; i<p->nBuffer; i++){
    if( n>0 && csIndexEntryCmp(&p->aBuffer[n-1], &p->aBuffer[i])==0 ) n--;
    p->aBuffer[n++] = p->aBuffer[i];
  }
  if( p->nRun==0 || prollyHashCompare(&p->last, &p->aBuffer[0].hash)>=0 ){
    p->iRun = p->iWrite;
    p->iWrite += 8;
    p->nRunEntry = 0;
    p->nRun++;
  }
  rc = csSpoolWrite(p->pFile, p->aBuffer, n*sizeof(p->aBuffer[0]), p->iWrite);
  if( rc!=SQLITE_OK ) return rc;
  p->iWrite += (i64)n*sizeof(p->aBuffer[0]);
  p->nRunEntry += n;
  rc = csSpoolHeader(p->pFile, p->iRun, p->nRunEntry);
  p->last = p->aBuffer[n-1].hash;
  p->nBuffer = 0;
  return rc;
}

int csIndexSpoolAdd(void *pCtx, const ChunkIndexEntry *e){
  ChunkIndexSpool *p = pCtx;
  assert( !p->ready );
  if( p->nEntry==INT_MAX ) return SQLITE_TOOBIG;
  p->nEntry++;
  p->aBuffer[p->nBuffer++] = *e;
  return p->nBuffer==CS_SPOOL_BUFFER ? csSpoolFlush(p) : SQLITE_OK;
}

static int csSpoolReaderInit(CsSpoolReader *p, sqlite3_file *pFile, i64 *piOff){
  u8 a[8];
  i64 n;
  int rc;
  memset(p, 0, sizeof(*p));
  p->pFile = pFile;
  rc = sqlite3OsRead(pFile, a, sizeof(a), *piOff);
  if( rc!=SQLITE_OK ) return rc;
  n = CS_READ_I64(a);
  if( n<=0 || n>INT_MAX ) return SQLITE_CORRUPT;
  p->nLeft = (int)n;
  p->iRead = *piOff+8;
  *piOff = p->iRead+n*sizeof(ChunkIndexEntry);
  return SQLITE_OK;
}

static int csSpoolPeek(CsSpoolReader *p, ChunkIndexEntry **pp){
  int rc;
  *pp = 0;
  if( p->i==p->n ){
    if( p->nLeft==0 ) return SQLITE_OK;
    p->n = MIN(p->nLeft, CS_SPOOL_BLOCK);
    rc = sqlite3OsRead(p->pFile, p->a, p->n*sizeof(p->a[0]), p->iRead);
    if( rc!=SQLITE_OK ) return rc;
    p->iRead += (i64)p->n*sizeof(p->a[0]);
    p->nLeft -= p->n;
    p->i = 0;
  }
  *pp = &p->a[p->i];
  return SQLITE_OK;
}

int csIndexSpoolFinish(ChunkIndexSpool *p){
  int rc = csSpoolFlush(p);
  int i;
  if( rc!=SQLITE_OK ) return rc;
  while( p->nRun>1 ){
    sqlite3_file *pOut = 0;
    i64 iRead = 0, iWrite = 0;
    int nRun = 0;
    rc = csSpoolOpen(p->pVfs, &pOut);
    if( rc!=SQLITE_OK ) return rc;
    for(i=0; rc==SQLITE_OK && i<p->nRun; i+=2){
      CsSpoolReader a, b;
      i64 iHeader = iWrite;
      int nOut = 0, nBuffer = 0;
      memset(&b, 0, sizeof(b));
      rc = csSpoolReaderInit(&a, p->pFile, &iRead);
      if( rc==SQLITE_OK && i+1<p->nRun ){
        rc = csSpoolReaderInit(&b, p->pFile, &iRead);
      }
      iWrite += 8;
      while( rc==SQLITE_OK ){
        ChunkIndexEntry *ea, *eb, *e;
        rc = csSpoolPeek(&a, &ea);
        if( rc==SQLITE_OK ) rc = csSpoolPeek(&b, &eb);
        if( rc!=SQLITE_OK || (!ea && !eb) ) break;
        if( !eb || (ea && csIndexEntryCmp(ea, eb)<0) ){
          e = ea;
          a.i++;
        }else{
          if( ea && csIndexEntryCmp(ea, eb)==0 ) a.i++;
          e = eb;
          b.i++;
        }
        p->aBuffer[nBuffer++] = *e;
        nOut++;
        if( nBuffer==CS_SPOOL_BUFFER ){
          rc = csSpoolWrite(pOut, p->aBuffer,
                              nBuffer*sizeof(*e), iWrite);
          iWrite += (i64)nBuffer*sizeof(*e);
          nBuffer = 0;
        }
      }
      if( rc==SQLITE_OK && nBuffer>0 ){
        rc = csSpoolWrite(pOut, p->aBuffer,
                            nBuffer*sizeof(ChunkIndexEntry), iWrite);
        iWrite += (i64)nBuffer*sizeof(ChunkIndexEntry);
      }
      if( rc==SQLITE_OK ) rc = csSpoolHeader(pOut, iHeader, nOut);
      nRun++;
    }
    if( rc!=SQLITE_OK ){
      sqlite3OsCloseFree(pOut);
      return rc;
    }
    sqlite3OsCloseFree(p->pFile);
    p->pFile = pOut;
    p->iWrite = iWrite;
    p->nRun = nRun;
  }
  p->nEntry = p->nRun ? (int)((p->iWrite-8)/sizeof(ChunkIndexEntry)) : 0;
  for(i=0; i<CS_SPOOL_CACHE; i++) p->aCache[i] = -1;
  p->ready = 1;
  return SQLITE_OK;
}

int csIndexSpoolCount(const ChunkIndexSpool *p){
  return p->nEntry;
}

int csIndexSpoolGet(ChunkIndexSpool *p, int i, ChunkIndexEntry *e){
  int block = i/CS_SPOOL_BLOCK;
  int slot = block%CS_SPOOL_CACHE;
  int first = block*CS_SPOOL_BLOCK;
  ChunkIndexEntry *a;
  assert( p->ready );
  if( i<0 || i>=p->nEntry ) return SQLITE_CORRUPT;
  a = &p->aBuffer[slot*CS_SPOOL_BLOCK];
  if( p->aCache[slot]!=block ){
    int n = MIN(p->nEntry-first, CS_SPOOL_BLOCK);
    int rc = sqlite3OsRead(p->pFile, a, n*sizeof(*a), 8+(i64)first*sizeof(*a));
    if( rc!=SQLITE_OK ) return rc;
    p->aCache[slot] = block;
  }
  *e = a[i-first];
  return SQLITE_OK;
}

int csIndexSpoolFind(ChunkIndexSpool *p, const ProllyHash *h, int *pi){
  int lo = 0, hi = p->nEntry-1;
  *pi = -1;
  while( lo<=hi ){
    int mid = lo+(hi-lo)/2;
    ChunkIndexEntry e;
    int cmp;
    int rc = csIndexSpoolGet(p, mid, &e);
    if( rc!=SQLITE_OK ) return rc;
    cmp = prollyHashCompare(&e.hash, h);
    if( cmp==0 ){
      *pi = mid;
      break;
    }
    if( cmp<0 ) lo = mid+1;
    else hi = mid-1;
  }
  return SQLITE_OK;
}

int csIndexSnapshot(ChunkStore *cs, int pending, ChunkIndexSpool **pp){
  ChunkIndexSpool *p = 0;
  int rc, i;
  *pp = 0;
  rc = csIndexSpoolInit(cs->file.pVfs, &p);
  if( rc==SQLITE_OK ) rc = csVisitIndex(cs, csIndexSpoolAdd, p);
  for(i=0; rc==SQLITE_OK && i<cs->staging.nRecent; i++){
    rc = csIndexSpoolAdd(p, &cs->staging.aRecent[i]);
  }
  for(i=0; pending && rc==SQLITE_OK && i<cs->staging.nPending; i++){
    rc = csIndexSpoolAdd(p, &cs->staging.aPending[i]);
  }
  if( rc==SQLITE_OK ) rc = csIndexSpoolFinish(p);
  if( rc!=SQLITE_OK ) csIndexSpoolFree(p);
  else *pp = p;
  return rc;
}

#endif
