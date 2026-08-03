#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include "sqliteInt.h"
#include "chunk_store.h"
#include "chunk_refs.h"

static void assertStrictTruncation(void) {
  static int checked = 0;
  ChunkStore source;
  ChunkStore decoded;
  ProllyHash commitHash;
  ProllyHash workingSetHash;
  u8 *blob = 0;
  int nBlob = 0;
  int i;
  int rc;

  if (checked) return;
  checked = 1;
  memset(&source, 0, sizeof(source));
  memset(&commitHash, 0x11, sizeof(commitHash));
  memset(&workingSetHash, 0x22, sizeof(workingSetHash));
  rc = chunkStoreSetDefaultBranch(&source, "main");
  if (rc==SQLITE_OK) rc = chunkStoreAddBranch(&source, "main", &commitHash);
  if (rc==SQLITE_OK) {
    rc = chunkStoreSetBranchWorkingSet(&source, "main", &workingSetHash);
  }
  if (rc==SQLITE_OK) {
    rc = chunkStoreSerializeRefsToBlob(&source, &blob, &nBlob);
  }
  if (rc!=SQLITE_OK || !blob || nBlob<=0) abort();

  for (i=0; i<nBlob; i++) {
    memset(&decoded, 0, sizeof(decoded));
    rc = csDeserializeRefs(&decoded, blob, i);
    if (rc!=SQLITE_CORRUPT) abort();
    csFreeRefsState(&decoded);
  }
  memset(&decoded, 0, sizeof(decoded));
  rc = csDeserializeRefs(&decoded, blob, nBlob);
  if (rc!=SQLITE_OK) abort();
  csFreeRefsState(&decoded);
  sqlite3_free(blob);
  csFreeRefsState(&source);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ChunkStore cs;

  assertStrictTruncation();

  if (size > 1024 * 1024) return 0;
  if (size > (size_t)0x7fffffff) return 0;

  memset(&cs, 0, sizeof(cs));
  cs.pGraphLockFile = 0;

  (void)csDeserializeRefs(&cs, data, (int)size);

  csFreeRefsState(&cs);
  return 0;
}
