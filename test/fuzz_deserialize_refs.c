#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include "sqliteInt.h"
#include "chunk_store.h"
#include "chunk_refs.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ChunkStore cs;

  if (size > 1024 * 1024) return 0;
  if (size > (size_t)0x7fffffff) return 0;

  memset(&cs, 0, sizeof(cs));
  cs.pGraphLockFile = 0;

  (void)csDeserializeRefs(&cs, data, (int)size);

  csFreeRefsState(&cs);
  return 0;
}
