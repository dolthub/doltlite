#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include "sqliteInt.h"
#include "prolly_node.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ProllyNode node;
  int rc;

  if (size > 256 * 1024) return 0;
  if (size > (size_t)0x7fffffff) return 0;

  rc = prollyNodeParse(&node, data, (int)size);
  if (rc != SQLITE_OK) return 0;

  for (int i = 0; i < node.nItems; i++) {
    const u8 *p;
    int n;
    prollyNodeKey(&node, i, &p, &n);
    prollyNodeValue(&node, i, &p, &n);
    if (node.flags & PROLLY_NODE_INTKEY) {
      (void)prollyNodeIntKey(&node, i);
    }
    if (node.level > 0) {
      ProllyHash h;
      prollyNodeChildHash(&node, i, &h);
    }
  }

  if (node.nItems > 0) {
    int res;
    if (node.flags & PROLLY_NODE_INTKEY) {
      i64 k = prollyNodeIntKey(&node, 0);
      (void)prollyNodeSearchInt(&node, k, &res);
      (void)prollyNodeSearchInt(&node, 0, &res);
      (void)prollyNodeSearchInt(&node, INT64_MAX, &res);
      (void)prollyNodeSearchInt(&node, INT64_MIN, &res);
    } else {
      const u8 *p;
      int n;
      prollyNodeKey(&node, 0, &p, &n);
      if (n > 0) {
        (void)prollyNodeSearchBlob(&node, p, n, &res);
      }
      (void)prollyNodeSearchBlob(&node, (const u8 *)"", 0, &res);
    }
  }

  return 0;
}
