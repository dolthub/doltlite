
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "chunk_refs.h"
#include "doltlite_commit.h"
#include "doltlite_chunk_walk.h"

#include <string.h>

/* Strict: every length prefix is bounds-checked and the record must end at
** nData. A V2 working set shares this tag and one colliding size. */
static int isCommitChunk(const u8 *data, int nData){
  const u8 *p = data;
  const u8 *pEnd = data + nData;
  int nPar;
  int i;

  if( nData < 1 || *p++ != DOLTLITE_COMMIT_V2 ) return 0;
  if( p >= pEnd ) return 0;
  nPar = *p++;
  if( nPar > DOLTLITE_MAX_PARENTS ) return 0;
  if( pEnd - p < nPar*PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 8 + 6 ){
    return 0;
  }
  p += nPar*PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 8;
  for(i=0; i<3; i++){
    int n;
    if( pEnd - p < 2 ) return 0;
    n = p[0] | (p[1]<<8);
    p += 2;
    if( pEnd - p < n ) return 0;
    p += n;
  }
  return p == pEnd;
}

/* COMMIT_V2 and WS_V2 share tag 2; author+email+message of 46 bytes is
** exactly WS_TOTAL_SIZE_V2. Order is load-bearing: a colliding check must
** run first or reject the impostor. Misreads are not parse errors — the
** walker treats bytes as child hashes, reports held chunks missing, and
** the damage is permanent. Check new formats against every tag and size. */
DoltliteChunkType doltliteClassifyChunk(const u8 *data, int nData){
  u32 m;

  if( !data || nData < 4 ) return CHUNK_UNKNOWN;

  m = (u32)data[0] | ((u32)data[1]<<8) |
      ((u32)data[2]<<16) | ((u32)data[3]<<24);
  if( m == PROLLY_NODE_MAGIC && nData >= 8 ){
    return CHUNK_PROLLY_NODE;
  }

  if( isCommitChunk(data, nData) ){
    return CHUNK_COMMIT;
  }

  /* DLC/DCV framed blobs lead with 'D' == CATALOG_FORMAT_V3; match them
  ** before catalog parse. Both are leaves (no child hashes). */
  if( nData >= 6
   && data[0]=='D' && data[1]=='L' && data[2]=='C' && data[3]==1 ){
    return CHUNK_CONFLICTS;
  }
  if( nData >= 6
   && data[0]=='D' && data[1]=='C' && data[2]=='V' && data[3]==1 ){
    return CHUNK_CONSTRAINT_VIOLATIONS;
  }

  if( chunkStoreValidateWorkingSetBlob(data, nData)==SQLITE_OK ){
    return CHUNK_WORKING_SET;
  }

  {
    int nTables; const u8 *pEntries; int iFormat;
    if( catalogParseHeaderEx(data, nData, &iFormat, &nTables, &pEntries) ){
      return CHUNK_CATALOG;
    }
  }

  if( nData >= 5 && data[0] == 7 ){
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
  if( node.level == 0 ) return SQLITE_OK;

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
  int iFormat;
  const u8 *p;
  int i;
  int rc = SQLITE_OK;

  if( !catalogParseHeaderEx(data, nData, &iFormat, &nTables, &p) ) return SQLITE_CORRUPT;
  if( nTables < 0 || nTables >= 10000 ) return SQLITE_CORRUPT;
  for(i=0; i<nTables && rc==SQLITE_OK; i++){
    ProllyHash tableRoot;
    const u8 *pEnd = data + nData;

    if( p + (iFormat!=CATALOG_FORMAT_V3 ? CAT_ENTRY_FIXED_SIZE_V4 : CAT_ENTRY_FIXED_SIZE_V3) > pEnd ){
      return SQLITE_CORRUPT;
    }

    memcpy(tableRoot.data,
           p + CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE,
           PROLLY_HASH_SIZE);
    rc = xChild(ctx, &tableRoot);
    if( rc!=SQLITE_OK ) break;

    p += CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE
       + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE;
    if( iFormat!=CATALOG_FORMAT_V3 ){
      int nType, nName, nTbl;
      if( p + 6 > pEnd ) return SQLITE_CORRUPT;
      nType = p[0] | (p[1]<<8); p += 2;
      nName = p[0] | (p[1]<<8); p += 2;
      nTbl = p[0] | (p[1]<<8); p += 2;
      if( p + nType + nName + nTbl > pEnd ) return SQLITE_CORRUPT;
      p += nType + nName + nTbl;
    }else{
      int nameLen;
      if( p + 2 > pEnd ) return SQLITE_CORRUPT;
      nameLen = p[0] | (p[1]<<8);
      if( p + 2 + nameLen > pEnd ) return SQLITE_CORRUPT;
      p += 2 + nameLen;
    }
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
  u8 version;

  if( nData < 1 ) return SQLITE_CORRUPT;
  version = data[0];

  memcpy(h.data, data + WS_WORKING_CAT_OFF, PROLLY_HASH_SIZE);
  rc = xChild(ctx, &h);
  if( rc!=SQLITE_OK ) return rc;

  memcpy(h.data, data + WS_WORKING_COMMIT_OFF, PROLLY_HASH_SIZE);
  rc = xChild(ctx, &h);
  if( rc!=SQLITE_OK ) return rc;

  memcpy(h.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  rc = xChild(ctx, &h);
  if( rc!=SQLITE_OK ) return rc;

  memcpy(h.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
  rc = xChild(ctx, &h);
  if( rc!=SQLITE_OK ) return rc;

  if( data[WS_MERGING_OFF] ){
    memcpy(h.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
    rc = xChild(ctx, &h);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( version >= WS_FORMAT_VERSION_V3 && nData >= WS_REBASE_ONTO_OFF + PROLLY_HASH_SIZE
   && data[WS_REBASING_OFF] ){
    memcpy(h.data, data + WS_PRE_REBASE_CAT_OFF, PROLLY_HASH_SIZE);
    rc = xChild(ctx, &h);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(h.data, data + WS_REBASE_ONTO_OFF, PROLLY_HASH_SIZE);
    rc = xChild(ctx, &h);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( version == WS_FORMAT_VERSION_V4 && nData >= WS_TOTAL_SIZE_V4 ){
    memcpy(h.data, data + WS_CONSTRAINT_VIOLATIONS_OFF_V4, PROLLY_HASH_SIZE);
    rc = xChild(ctx, &h);
  }else if( version >= WS_FORMAT_VERSION_V5 && nData >= WS_TOTAL_SIZE ){
    memcpy(h.data, data + WS_CONSTRAINT_VIOLATIONS_OFF, PROLLY_HASH_SIZE);
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
  int rc = csDecodeRefsV7(data, nData, 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  return csDecodeRefsV7(data, nData, 0, xChild, ctx);
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
    case CHUNK_CONFLICTS:
    case CHUNK_CONSTRAINT_VIOLATIONS:
      return SQLITE_OK;
    case CHUNK_UNKNOWN:
    default:
      return SQLITE_CORRUPT;
  }
}

#endif
