
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_chunk_walk.h"

#include <string.h>

DoltliteChunkType doltliteClassifyChunk(const u8 *data, int nData){
  u32 m;

  if( !data || nData < 4 ) return CHUNK_UNKNOWN;

  /* Check prolly node magic first (4-byte LE) */
  m = (u32)data[0] | ((u32)data[1]<<8) |
      ((u32)data[2]<<16) | ((u32)data[3]<<24);
  if( m == PROLLY_NODE_MAGIC && nData >= 8 ){
    return CHUNK_PROLLY_NODE;
  }

  /* Working set: exactly WS_TOTAL_SIZE bytes, version byte == 1 */
  if( nData == WS_TOTAL_SIZE && data[0] == 1 ){
    return CHUNK_WORKING_SET;
  }

  /* Catalog V2/V3: recognized by catalogParseHeader */
  {
    int nTables; const u8 *pEntries;
    if( catalogParseHeader(data, nData, &nTables, &pEntries) ){
      return CHUNK_CATALOG;
    }
  }

  /* Commit V2: version byte matches, minimum size 30 */
  if( nData >= 30 && data[0] == DOLTLITE_COMMIT_V2 ){
    return CHUNK_COMMIT;
  }

  return CHUNK_UNKNOWN;
}

static int enumerateProllyNodeChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
){
  ProllyNode node;
  int rc;
  int i;

  rc = prollyNodeParse(&node, data, nData);
  if( rc!=SQLITE_OK ) return rc;
  if( node.level == 0 ) return SQLITE_OK; /* leaf: no child hashes */

  for(i=0; i<(int)node.nItems; i++){
    ProllyHash childHash;
    prollyNodeChildHash(&node, i, &childHash);
    rc = xChild(ctx, &childHash);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int enumerateCommitChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
){
  DoltliteCommit commit;
  int drc, rc = SQLITE_OK;
  int pi;

  memset(&commit, 0, sizeof(commit));
  drc = doltliteCommitDeserialize(data, nData, &commit);
  if( drc!=SQLITE_OK ) return drc;

  for(pi=0; pi<commit.nParents && rc==SQLITE_OK; pi++){
    rc = xChild(ctx, &commit.aParents[pi]);
  }
  if( rc==SQLITE_OK ){
    rc = xChild(ctx, &commit.catalogHash);
  }
  doltliteCommitClear(&commit);
  return rc;
}

static int enumerateCatalogChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
){
  int nTables;
  const u8 *p;
  int i;
  int rc = SQLITE_OK;

  if( !catalogParseHeader(data, nData, &nTables, &p) ) return SQLITE_CORRUPT;
  if( nTables < 0 || nTables >= 10000 ) return SQLITE_CORRUPT;
  for(i=0; i<nTables && rc==SQLITE_OK; i++){
    int nameLen;
    ProllyHash tableRoot;
    const u8 *pEnd = data + nData;

    if( p + CAT_ENTRY_FIXED_SIZE > pEnd ) return SQLITE_CORRUPT;

    memcpy(tableRoot.data,
           p + CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE,
           PROLLY_HASH_SIZE);
    rc = xChild(ctx, &tableRoot);
    if( rc!=SQLITE_OK ) break;

    p += CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE
       + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE;
    if( p + 2 > pEnd ) return SQLITE_CORRUPT;
    nameLen = p[0] | (p[1]<<8);
    if( p + 2 + nameLen > pEnd ) return SQLITE_CORRUPT;
    p += 2 + nameLen;
  }
  return rc;
}

static int enumerateWorkingSetChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
){
  ProllyHash h;
  int rc;

  (void)nData;

  /* staged catalog hash */
  memcpy(h.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  rc = xChild(ctx, &h);
  if( rc!=SQLITE_OK ) return rc;

  /* conflicts catalog hash */
  memcpy(h.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
  rc = xChild(ctx, &h);
  if( rc!=SQLITE_OK ) return rc;

  /* merge commit hash (only if merging) */
  if( data[WS_MERGING_OFF] ){
    memcpy(h.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
    rc = xChild(ctx, &h);
  }

  return rc;
}

int doltliteEnumerateChunkChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
){
  DoltliteChunkType type = doltliteClassifyChunk(data, nData);

  switch( type ){
    case CHUNK_PROLLY_NODE:
      return enumerateProllyNodeChildren(data, nData, xChild, ctx);
    case CHUNK_COMMIT:
      return enumerateCommitChildren(data, nData, xChild, ctx);
    case CHUNK_CATALOG:
      return enumerateCatalogChildren(data, nData, xChild, ctx);
    case CHUNK_WORKING_SET:
      return enumerateWorkingSetChildren(data, nData, xChild, ctx);
    case CHUNK_UNKNOWN:
    default:
      return SQLITE_OK;
  }
}

#endif /* DOLTLITE_PROLLY */
