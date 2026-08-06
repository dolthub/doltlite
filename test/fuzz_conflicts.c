#include <stddef.h>
#include <stdint.h>

#include "doltlite_internal.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size > 1024 * 1024 || size > (size_t)0x7fffffff) return 0;

  (void)doltliteDeserializeConflictsForTest(data, (int)size);
  return 0;
}
