#include <stddef.h>
#include <stdint.h>

#include "doltlite_commit.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  DoltliteCommit commit;

  if (size > 1024 * 1024 || size > (size_t)0x7fffffff) return 0;

  (void)doltliteCommitDeserialize(data, (int)size, &commit);
  doltliteCommitClear(&commit);
  return 0;
}
