/*
** Prolly tree node format: binary serialization for prolly map nodes.
**
** Node binary layout:
**   [magic:4][level:1][count:2][flags:1]
**   [keyOffsets: u32 * (count+1)]   -- offsets into key region
**   [valOffsets: u32 * (count+1)]   -- offsets into value region
**   [key data: variable]
**   [value data: variable]
**
** For internal nodes, values are ProllyHash (child addresses).
** For leaf nodes, values are arbitrary payloads (SQLite record format).
*/
#ifndef SQLITE_PROLLY_NODE_H
#define SQLITE_PROLLY_NODE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

/* Node magic bytes */
#define PROLLY_NODE_MAGIC 0x504E4F44  /* "PNOD" */

/* Node flags */
#define PROLLY_NODE_INTKEY  0x01  /* Keys are 64-bit integers (table btree) */
#define PROLLY_NODE_BLOBKEY 0x02  /* Keys are blobs (index btree) */

/* Maximum items per node (soft limit, actual limit from chunker) */
#define PROLLY_NODE_MAX_ITEMS 4096

/*
** Read-only node handle. Points into chunk data (no copy).
** Valid only while the underlying chunk data is alive.
*/
typedef struct ProllyNode ProllyNode;
struct ProllyNode {
  const u8 *pData;    /* Raw serialized node data */
  int nData;           /* Size of serialized data */
  u8 level;            /* Tree level: 0=leaf, >0=internal */
  u16 nItems;          /* Number of key-value pairs */
  u8 flags;            /* PROLLY_NODE_INTKEY or PROLLY_NODE_BLOBKEY */
  const u32 *aKeyOff;  /* Key offset array (count+1 entries) */
  const u32 *aValOff;  /* Value offset array (count+1 entries) */
  const u8 *pKeyData;  /* Start of key data region */
  const u8 *pValData;  /* Start of value data region */
  ProllyHash addr;     /* Content address of this node */
};

/* Parse a serialized node. Returns SQLITE_OK or SQLITE_CORRUPT. */
int prollyNodeParse(ProllyNode *pNode, const u8 *pData, int nData);

/* Get key at index i. Sets *ppKey and *pnKey. */
void prollyNodeKey(const ProllyNode *pNode, int i, const u8 **ppKey, int *pnKey);

/* Get value at index i. Sets *ppVal and *pnVal. */
void prollyNodeValue(const ProllyNode *pNode, int i, const u8 **ppVal, int *pnVal);

/* Get integer key at index i (only valid if PROLLY_NODE_INTKEY). */
i64 prollyNodeIntKey(const ProllyNode *pNode, int i);

/* Get child hash at index i (only valid for internal nodes, level>0). */
void prollyNodeChildHash(const ProllyNode *pNode, int i, ProllyHash *pHash);

/*
** Binary search for blob key. Returns index of exact match, or
** the index where key would be inserted (for seek operations).
** *pRes: 0 if exact match, <0 if key is less, >0 if key is greater.
*/
int prollyNodeSearchBlob(const ProllyNode *pNode,
                         const u8 *pKey, int nKey, int *pRes);

/*
** Binary search for integer key.
** *pRes: 0 if exact match, <0 if key is less, >0 if key is greater.
*/
int prollyNodeSearchInt(const ProllyNode *pNode, i64 intKey, int *pRes);

/*
** Node builder: construct a new serialized node.
*/
typedef struct ProllyNodeBuilder ProllyNodeBuilder;
struct ProllyNodeBuilder {
  u8 level;
  u8 flags;
  int nItems;
  int nKeyBytes;
  int nValBytes;
  int nAlloc;          /* Allocated capacity for offset arrays */
  u32 *aKeyOff;        /* Key offsets */
  u32 *aValOff;        /* Value offsets */
  u8 *pKeyBuf;         /* Key data buffer */
  int nKeyBufAlloc;
  u8 *pValBuf;         /* Value data buffer */
  int nValBufAlloc;
};

/* Initialize a node builder */
void prollyNodeBuilderInit(ProllyNodeBuilder *b, u8 level, u8 flags);

/* Add a key-value pair to the builder */
int prollyNodeBuilderAdd(ProllyNodeBuilder *b,
                         const u8 *pKey, int nKey,
                         const u8 *pVal, int nVal);

/* Serialize the node into a new allocation. Caller must sqlite3_free(*ppOut). */
int prollyNodeBuilderFinish(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut);

/* Reset builder for reuse (keeps allocated buffers) */
void prollyNodeBuilderReset(ProllyNodeBuilder *b);

/* Free builder resources */
void prollyNodeBuilderFree(ProllyNodeBuilder *b);

/* Compute the content hash of serialized node data */
void prollyNodeComputeHash(const u8 *pData, int nData, ProllyHash *pOut);

/*
** Compare two keys. For INTKEY tables, compare the i64 values directly.
** For BLOBKEY tables, use memcmp with length comparison.
**
** Returns negative if key1 < key2, zero if equal, positive if key1 > key2.
*/
int prollyCompareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
);

#endif /* SQLITE_PROLLY_NODE_H */
