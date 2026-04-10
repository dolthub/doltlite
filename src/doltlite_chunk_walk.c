
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

  /* Refs blob: version byte 5/6 with length-prefixed sections. */
  if( nData >= 5 && (data[0] == 5 || data[0] == 6) ){
    return CHUNK_REFS;
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

static int enumerateRefsChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
){
  const u8 *p = data;
  const u8 *pEnd = data + nData;
  int version;
  int defLen;
  int nBranches;
  int nTags;
  int nRemotes;
  int nTracking;
  int i;
  ProllyHash h;
  int rc = SQLITE_OK;

  if( nData < 5 ) return SQLITE_CORRUPT;
  version = *p++;
  if( version!=5 && version!=6 ) return SQLITE_CORRUPT;
  if( p + 4 > pEnd ) return SQLITE_CORRUPT;
  defLen = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
  p += 4;
  if( defLen < 0 || p + defLen > pEnd ) return SQLITE_CORRUPT;
  p += defLen;

  if( p + 4 > pEnd ) return SQLITE_CORRUPT;
  nBranches = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
  p += 4;
  if( nBranches < 0 || nBranches > 100000 ) return SQLITE_CORRUPT;
  for(i=0; i<nBranches && rc==SQLITE_OK; i++){
    int nameLen;
    if( p + 4 > pEnd ) return SQLITE_CORRUPT;
    nameLen = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
    p += 4;
    if( nameLen < 0 || p + nameLen + PROLLY_HASH_SIZE > pEnd ) return SQLITE_CORRUPT;
    p += nameLen;
    memcpy(h.data, p, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    rc = xChild(ctx, &h);
    if( rc!=SQLITE_OK ) break;
    if( p + PROLLY_HASH_SIZE > pEnd ) return SQLITE_CORRUPT;
    memcpy(h.data, p, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    rc = xChild(ctx, &h);
  }
  if( rc!=SQLITE_OK ) return rc;

  if( p + 4 > pEnd ) return SQLITE_CORRUPT;
  nTags = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
  p += 4;
  if( nTags < 0 || nTags > 100000 ) return SQLITE_CORRUPT;
  for(i=0; i<nTags && rc==SQLITE_OK; i++){
    int nameLen;
    if( p + 4 > pEnd ) return SQLITE_CORRUPT;
    nameLen = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
    p += 4;
    if( nameLen < 0 || p + nameLen + PROLLY_HASH_SIZE > pEnd ) return SQLITE_CORRUPT;
    p += nameLen;
    memcpy(h.data, p, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    rc = xChild(ctx, &h);
    if( rc!=SQLITE_OK ) break;
    if( version>=6 ){
      int n;
      if( p + 4 > pEnd ) return SQLITE_CORRUPT;
      n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
      p += 4;
      if( n < 0 || p + n > pEnd ) return SQLITE_CORRUPT;
      p += n;
      if( p + 4 > pEnd ) return SQLITE_CORRUPT;
      n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
      p += 4;
      if( n < 0 || p + n > pEnd ) return SQLITE_CORRUPT;
      p += n;
      if( p + 8 > pEnd ) return SQLITE_CORRUPT;
      p += 8;
      if( p + 4 > pEnd ) return SQLITE_CORRUPT;
      n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
      p += 4;
      if( n < 0 || p + n > pEnd ) return SQLITE_CORRUPT;
      p += n;
    }
  }
  if( rc!=SQLITE_OK ) return rc;

  if( p + 4 > pEnd ) return SQLITE_CORRUPT;
  nRemotes = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
  p += 4;
  if( nRemotes < 0 || nRemotes > 100000 ) return SQLITE_CORRUPT;
  for(i=0; i<nRemotes; i++){
    int n;
    if( p + 4 > pEnd ) return SQLITE_CORRUPT;
    n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
    p += 4;
    if( n < 0 || p + n > pEnd ) return SQLITE_CORRUPT;
    p += n;
    if( p + 4 > pEnd ) return SQLITE_CORRUPT;
    n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
    p += 4;
    if( n < 0 || p + n > pEnd ) return SQLITE_CORRUPT;
    p += n;
  }

  if( p + 4 > pEnd ) return SQLITE_CORRUPT;
  nTracking = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
  p += 4;
  if( nTracking < 0 || nTracking > 100000 ) return SQLITE_CORRUPT;
  for(i=0; i<nTracking && rc==SQLITE_OK; i++){
    int n;
    if( p + 4 > pEnd ) return SQLITE_CORRUPT;
    n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
    p += 4;
    if( n < 0 || p + n > pEnd ) return SQLITE_CORRUPT;
    p += n;
    if( p + 4 > pEnd ) return SQLITE_CORRUPT;
    n = (int)(p[0] | (p[1]<<8) | (p[2]<<16) | (p[3]<<24));
    p += 4;
    if( n < 0 || p + n + PROLLY_HASH_SIZE > pEnd ) return SQLITE_CORRUPT;
    p += n;
    memcpy(h.data, p, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    rc = xChild(ctx, &h);
  }
  if( rc!=SQLITE_OK ) return rc;
  if( p != pEnd ) return SQLITE_CORRUPT;
  return SQLITE_OK;
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
    case CHUNK_REFS:
      return enumerateRefsChildren(data, nData, xChild, ctx);
    case CHUNK_UNKNOWN:
    default:
      return SQLITE_OK;
  }
}

#endif /* DOLTLITE_PROLLY */
