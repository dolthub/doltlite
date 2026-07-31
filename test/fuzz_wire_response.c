#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include "sqlite3.h"

extern int doltliteHttpParseResponseForTest(
  const unsigned char *pRaw,
  int nRaw,
  int nHash
);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static int initialized = 0;
  uint8_t *normalized;
  size_t i;
  size_t nNormalized;
  int nHash;

  if (size < 1 || size > 1024 * 1024) return 0;
  if (size - 1 > (size_t)0x7fffffff) return 0;
  if (!initialized) {
    sqlite3_initialize();
    initialized = 1;
  }

  nHash = data[0] % 65;
  (void)doltliteHttpParseResponseForTest(
      data + 1, (int)(size - 1), nHash);

  normalized = (uint8_t *)malloc((size - 1) * 2);
  if (!normalized) return 0;
  nNormalized = 0;
  for (i = 1; i < size; i++) {
    if (data[i] == '\n' && (i == 1 || data[i - 1] != '\r')) {
      normalized[nNormalized++] = '\r';
    }
    normalized[nNormalized++] = data[i];
  }
  (void)doltliteHttpParseResponseForTest(
      normalized, (int)nNormalized, nHash);
  free(normalized);
  return 0;
}
