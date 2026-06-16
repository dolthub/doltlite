
#ifdef DOLTLITE_PROLLY
#ifdef DOLTLITE_PROLLY_CHECK

#include "sqliteInt.h"
#include "prolly_node.h"
#include "prolly_hash.h"
#include "prolly_check.h"
#include "chunk_store.h"
#include <string.h>

static int checkSubtree(
  ChunkStore *pStore,
  const ProllyHash *pHash,
  u8 flags,
  int expectedLevel,
  u8 **ppLastKey,
  int *pnLastKey,
  i64 *piLastKey,
  char **pzErr
){
  u8 *pData = 0;
  int nData = 0;
  ProllyNode node;
  int rc;
  int i;
  u8 *pPrevKey = 0;
  int nPrevKey = 0;
  i64 iPrevKey = 0;
  u8 isInt = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  rc = chunkStoreGet(pStore, pHash, &pData, &nData);
  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf(
      "chunkStoreGet failed rc=%d hash=%02x%02x%02x%02x%02x%02x%02x%02x",
      rc, pHash->data[0], pHash->data[1], pHash->data[2], pHash->data[3],
      pHash->data[4], pHash->data[5], pHash->data[6], pHash->data[7]);
    return rc;
  }

  /* Re-hash cached chunks too; chunkStoreGet verifies only disk reads. */
  {
    ProllyHash computed;
    prollyHashCompute(pData, nData, &computed);
    if( prollyHashCompare(&computed, pHash) != 0 ){
      *pzErr = sqlite3_mprintf(
        "chunk content does not hash to its store key: "
        "expected=%02x%02x%02x%02x%02x%02x%02x%02x "
        "computed=%02x%02x%02x%02x%02x%02x%02x%02x nData=%d",
        pHash->data[0], pHash->data[1], pHash->data[2], pHash->data[3],
        pHash->data[4], pHash->data[5], pHash->data[6], pHash->data[7],
        computed.data[0], computed.data[1], computed.data[2], computed.data[3],
        computed.data[4], computed.data[5], computed.data[6], computed.data[7],
        nData);
      sqlite3_free(pData);
      return SQLITE_CORRUPT;
    }
  }

  rc = prollyNodeParse(&node, pData, nData);
  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf(
      "prollyNodeParse failed rc=%d nData=%d level=%d count=%u flags=0x%02x"
      " hash=%02x%02x%02x%02x%02x%02x%02x%02x",
      rc, nData,
      nData>=5 ? (int)pData[4] : -1,
      nData>=7 ? (unsigned)(pData[5] | (pData[6]<<8)) : 0u,
      nData>=8 ? (unsigned)pData[7] : 0u,
      pHash->data[0], pHash->data[1], pHash->data[2], pHash->data[3],
      pHash->data[4], pHash->data[5], pHash->data[6], pHash->data[7]);
    sqlite3_free(pData);
    return rc;
  }

  if( (node.flags & (PROLLY_NODE_INTKEY|PROLLY_NODE_BLOBKEY))
        != (flags & (PROLLY_NODE_INTKEY|PROLLY_NODE_BLOBKEY)) ){
    *pzErr = sqlite3_mprintf("flags=0x%x expected=0x%x",
                             (unsigned)node.flags, (unsigned)flags);
    sqlite3_free(pData);
    return SQLITE_CORRUPT;
  }

  if( expectedLevel>=0 && (int)node.level!=expectedLevel ){
    *pzErr = sqlite3_mprintf("level=%d expected=%d",
                             (int)node.level, expectedLevel);
    sqlite3_free(pData);
    return SQLITE_CORRUPT;
  }

  if( node.nItems==0 ){
    *pzErr = sqlite3_mprintf("empty non-root node level=%d", (int)node.level);
    sqlite3_free(pData);
    return SQLITE_CORRUPT;
  }

  for(i=0; i<(int)node.nItems; i++){
    const u8 *pKey;
    int nKey;
    i64 iKey = 0;

    prollyNodeKey(&node, i, &pKey, &nKey);
    if( isInt ){
      iKey = prollyNodeIntKey(&node, i);
    }

    if( i>0 ){
      int c = prollyCompareKeys(flags,
                                pPrevKey, nPrevKey, iPrevKey,
                                (u8*)pKey, nKey, iKey);
      if( c>=0 ){
        *pzErr = sqlite3_mprintf(
          "keys not strictly ascending at level=%d idx=%d cmp=%d",
          (int)node.level, i, c);
        sqlite3_free(pPrevKey);
        sqlite3_free(pData);
        return SQLITE_CORRUPT;
      }
    }

    sqlite3_free(pPrevKey);
    pPrevKey = 0;
    if( nKey>0 ){
      pPrevKey = sqlite3_malloc(nKey);
      if( !pPrevKey ){
        sqlite3_free(pData);
        *pzErr = sqlite3_mprintf("OOM copying key");
        return SQLITE_NOMEM;
      }
      memcpy(pPrevKey, pKey, nKey);
    }
    nPrevKey = nKey;
    iPrevKey = iKey;
  }

  if( node.level>0 ){
    for(i=0; i<(int)node.nItems; i++){
      ProllyHash childHash;
      u8 *pChildLastKey = 0;
      int nChildLastKey = 0;
      i64 iChildLastKey = 0;
      const u8 *pParentKey;
      int nParentKey;
      i64 iParentKey = 0;
      int c;

      prollyNodeChildHash(&node, i, &childHash);
      rc = checkSubtree(pStore, &childHash, flags,
                        (int)node.level - 1,
                        &pChildLastKey, &nChildLastKey, &iChildLastKey,
                        pzErr);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pChildLastKey);
        sqlite3_free(pPrevKey);
        sqlite3_free(pData);
        return rc;
      }

      prollyNodeKey(&node, i, &pParentKey, &nParentKey);
      if( isInt ){
        iParentKey = prollyNodeIntKey(&node, i);
      }
      c = prollyCompareKeys(flags,
                            (u8*)pParentKey, nParentKey, iParentKey,
                            pChildLastKey, nChildLastKey, iChildLastKey);
      sqlite3_free(pChildLastKey);
      if( c!=0 ){
        *pzErr = sqlite3_mprintf(
          "boundary mismatch at level=%d idx=%d cmp=%d",
          (int)node.level, i, c);
        sqlite3_free(pPrevKey);
        sqlite3_free(pData);
        return SQLITE_CORRUPT;
      }
    }
  }

  if( ppLastKey ){
    *ppLastKey = pPrevKey;
    *pnLastKey = nPrevKey;
    if( piLastKey ) *piLastKey = iPrevKey;
    pPrevKey = 0;
  }

  sqlite3_free(pPrevKey);
  sqlite3_free(pData);
  return SQLITE_OK;
}

int prollyCheckTree(
  ChunkStore *pStore,
  const ProllyHash *pRoot,
  u8 flags,
  char **pzErr
){
  u8 *pLastKey = 0;
  int nLastKey = 0;
  i64 iLastKey = 0;
  int rc;

  if( pzErr ) *pzErr = 0;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;

  rc = checkSubtree(pStore, pRoot, flags, -1,
                    &pLastKey, &nLastKey, &iLastKey, pzErr);
  sqlite3_free(pLastKey);
  return rc;
}

#endif
#endif
