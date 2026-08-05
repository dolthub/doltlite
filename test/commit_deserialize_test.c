/* Bounds checks for the commit-chunk deserializer. Commit chunks are
** content-addressed only by hash, not by internal layout, so a corrupt or
** crafted chunk can carry a length prefix that runs past the buffer. Each
** case places the crafted buffer against a PROT_NONE guard page, so a read
** past the final byte faults instead of silently over-reading: an unfixed
** deserializer crashes here, a fixed one returns SQLITE_CORRUPT. */

#ifdef DOLTLITE_PROLLY

#include "doltlite_commit.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

static int failures = 0;

/* Return a pointer to an nData-byte region whose last byte abuts a
** PROT_NONE page, so any access at data[nData] or beyond faults. */
static u8 *guardedBuffer(int nData, void **ppMap, long *pMapLen){
  long pg = sysconf(_SC_PAGESIZE);
  long mapLen = pg * 2;
  u8 *base = mmap(0, mapLen, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
  if( base==MAP_FAILED ){ perror("mmap"); exit(2); }
  if( mprotect(base+pg, pg, PROT_NONE)!=0 ){ perror("mprotect"); exit(2); }
  *ppMap = base;
  *pMapLen = mapLen;
  return base + pg - nData;
}

static void expectCorrupt(const char *label, const u8 *tmpl, int nData){
  void *pMap; long mapLen;
  u8 *data = guardedBuffer(nData, &pMap, &mapLen);
  DoltliteCommit c;
  int rc;
  memcpy(data, tmpl, nData);
  rc = doltliteCommitDeserialize(data, nData, &c);
  if( rc!=SQLITE_CORRUPT ){
    fprintf(stderr, "FAIL %s: expected SQLITE_CORRUPT(%d), got %d\n",
            label, SQLITE_CORRUPT, rc);
    doltliteCommitClear(&c);
    failures++;
  }else{
    printf("ok %s -> SQLITE_CORRUPT\n", label);
  }
  munmap(pMap, mapLen);
}

/* Header up to and including the timestamp for a 0-parent V2 commit; the
** minimum size the length-prefix bounds check requires is 16+H. */
static int fillHeader(u8 *buf){
  int H = PROLLY_HASH_SIZE;
  int off = 0;
  buf[off++] = DOLTLITE_COMMIT_V2;
  buf[off++] = 0;                    /* nParents */
  memset(buf+off, 0xAB, H); off += H;/* catalogHash */
  memset(buf+off, 0, 8); off += 8;   /* timestamp */
  return off;                        /* 10+H */
}

static void testValidRoundTrip(void){
  DoltliteCommit in, out;
  u8 *buf = 0;
  int n = 0, rc;
  memset(&in, 0, sizeof(in));
  memset(in.catalogHash.data, 0x11, PROLLY_HASH_SIZE);
  in.timestamp = 123456789;
  in.zName = "alice";
  in.zEmail = "alice@example.com";
  in.zMessage = "a commit message";
  rc = doltliteCommitSerialize(&in, &buf, &n);
  if( rc!=SQLITE_OK ){ fprintf(stderr,"FAIL roundtrip: serialize rc=%d\n",rc); failures++; return; }
  rc = doltliteCommitDeserialize(buf, n, &out);
  if( rc!=SQLITE_OK
   || strcmp(out.zName,"alice")!=0
   || strcmp(out.zEmail,"alice@example.com")!=0
   || strcmp(out.zMessage,"a commit message")!=0
   || out.timestamp!=123456789 ){
    fprintf(stderr,"FAIL roundtrip: rc=%d name=%s email=%s msg=%s ts=%lld\n",
            rc, out.zName?out.zName:"(null)", out.zEmail?out.zEmail:"(null)",
            out.zMessage?out.zMessage:"(null)", (long long)out.timestamp);
    failures++;
  }else{
    printf("ok valid round-trip\n");
  }
  doltliteCommitClear(&out);
  sqlite3_free(buf);
}

static void testTrailingByte(void){
  DoltliteCommit in;
  u8 *buf = 0;
  u8 *pNew;
  int n = 0;
  int rc;

  memset(&in, 0, sizeof(in));
  in.zName = "alice";
  in.zEmail = "alice@example.com";
  in.zMessage = "message";
  rc = doltliteCommitSerialize(&in, &buf, &n);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "FAIL trailing_byte: serialize rc=%d\n", rc);
    failures++;
    return;
  }
  pNew = sqlite3_realloc(buf, n + 1);
  if( !pNew ){
    fprintf(stderr, "FAIL trailing_byte: realloc\n");
    failures++;
    sqlite3_free(buf);
    return;
  }
  buf = pNew;
  buf[n] = 0;
  expectCorrupt("trailing_byte", buf, n + 1);
  sqlite3_free(buf);
}

int main(void){
  int H = PROLLY_HASH_SIZE;
  int nData = 16 + H;               /* minimum that clears the header check */
  u8 *tmpl = malloc(nData);
  int off;

  testValidRoundTrip();
  testTrailingByte();

  /* Truncated after a non-empty name: the email length prefix would be read
  ** at data[nData]. */
  off = fillHeader(tmpl);
  tmpl[off++] = 4; tmpl[off++] = 0;  /* nName = 4 */
  memset(tmpl+off, 'x', 4); off += 4;/* name body reaches exactly nData */
  expectCorrupt("trunc_before_email_len", tmpl, nData);

  /* Truncated after name+empty email: the message length prefix would be
  ** read at data[nData]. */
  off = fillHeader(tmpl);
  tmpl[off++] = 2; tmpl[off++] = 0;  /* nName = 2 */
  memset(tmpl+off, 'y', 2); off += 2;/* name body */
  tmpl[off++] = 0; tmpl[off++] = 0;  /* nEmail = 0, reaches exactly nData */
  expectCorrupt("trunc_before_msg_len", tmpl, nData);

  free(tmpl);
  if( failures ){
    fprintf(stderr, "commit_deserialize_test: %d failure(s)\n", failures);
    return 1;
  }
  printf("commit_deserialize_test: all passed\n");
  return 0;
}

#else
int main(void){ return 0; }
#endif
