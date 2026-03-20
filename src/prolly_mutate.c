/*
** Merge-flush: apply buffered edits from MutMap into an existing prolly tree.
** This is the core mutation algorithm — it merges sorted edits with the
** existing tree's leaf entries, then rebuilds the tree using the chunker.
**
** The algorithm performs a single sorted merge-walk over two streams:
**   1. The existing tree's leaf entries (via cursor)
**   2. The pending edits (via MutMap iterator)
**
** The merged output is fed to the chunker, which produces a new set of
** content-addressed nodes forming the updated tree. Unchanged subtrees
** are structurally shared with the old tree.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include <string.h>

/*
** Compare two keys according to the node flags.
**
** For INTKEY tables: compare the 64-bit integer keys.
** For BLOBKEY tables: memcmp on the shorter length, then compare lengths.
**
** Returns negative if key1 < key2, zero if equal, positive if key1 > key2.
*/
/*
** Compare two SQLite serialized record keys using sqlite3VdbeRecordCompare.
** This is necessary because raw memcmp does NOT produce the correct sort
** order for SQLite record format (serial type encodings in the header
** cause byte-order divergence from logical key order).
**
** We allocate a temporary KeyInfo with BINARY collation, unpack pKey2
** into an UnpackedRecord, and compare pKey1 against it.
*/
/*
** Compare two serialized SQLite record keys by parsing the record
** headers and comparing field by field. This avoids needing KeyInfo
** or the full VdbeRecordCompare machinery.
**
** Returns <0, 0, >0 like strcmp.
*/
int compareBlobKeys(const u8 *pKey1, int nKey1,
                    const u8 *pKey2, int nKey2){
  u32 hdr1, hdr2;
  u32 off1, off2;  /* Offset within header */
  u32 d1, d2;      /* Offset within data */

  if( nKey1==0 && nKey2==0 ) return 0;
  if( nKey1==0 ) return -1;
  if( nKey2==0 ) return 1;

  /* Parse header sizes */
  off1 = getVarint32(pKey1, hdr1);
  off2 = getVarint32(pKey2, hdr2);
  if( hdr1 > (u32)nKey1 || hdr2 > (u32)nKey2 ){
    /* Malformed header — fall back to raw comparison */
    if( nKey1<nKey2 ) return -1;
    if( nKey1>nKey2 ) return 1;
    return memcmp(pKey1, pKey2, nKey1);
  }
  d1 = hdr1;
  d2 = hdr2;

  /* Compare fields one by one */
  while( off1 < hdr1 && off2 < hdr2 ){
    u32 type1, type2;
    u32 len1, len2;

    off1 += getVarint32(pKey1+off1, type1);
    off2 += getVarint32(pKey2+off2, type2);

    /* Compute field lengths from serial types */
    if( type1<=6 ){
      /* Integer types: 0=NULL(0 bytes), 1=1byte, 2=2bytes, 3=3bytes, 4=4bytes, 5=6bytes, 6=8bytes */
      static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
      len1 = aLen[type1];
    }else if( type1==7 ){
      len1 = 8; /* float */
    }else if( type1>=12 ){
      len1 = (type1-12)/2;
    }else{
      len1 = 0; /* types 8,9,10,11 = constants */
    }

    if( type2<=6 ){
      static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
      len2 = aLen[type2];
    }else if( type2==7 ){
      len2 = 8;
    }else if( type2>=12 ){
      len2 = (type2-12)/2;
    }else{
      len2 = 0;
    }

    /* Bounds check: ensure field data doesn't exceed key buffer */
    if( d1+len1 > (u32)nKey1 || d2+len2 > (u32)nKey2 ){
      /* Malformed key — fall back to raw length comparison */
      if( nKey1<nKey2 ) return -1;
      if( nKey1>nKey2 ) return 1;
      return 0;
    }

    /* NULL handling: NULL sorts before everything */
    if( type1==0 && type2==0 ){ d1+=len1; d2+=len2; continue; }
    if( type1==0 ) return -1;
    if( type2==0 ) return 1;

    /* Integer types (1-6, 8, 9) */
    if( (type1>=1 && type1<=6) || type1==8 || type1==9 ){
      i64 v1, v2;
      if( type1==8 ) v1 = 0;
      else if( type1==9 ) v1 = 1;
      else{
        v1 = (pKey1[d1] & 0x80) ? -1 : 0;
        for(u32 i=0; i<len1; i++) v1 = (v1<<8) | pKey1[d1+i];
      }

      /* type2 might be different type; compare by type class */
      if( (type2>=1 && type2<=6) || type2==8 || type2==9 ){
        if( type2==8 ) v2 = 0;
        else if( type2==9 ) v2 = 1;
        else{
          v2 = (pKey2[d2] & 0x80) ? -1 : 0;
          for(u32 i=0; i<len2; i++) v2 = (v2<<8) | pKey2[d2+i];
        }
        if( v1<v2 ){ return -1; }
        if( v1>v2 ){ return 1; }
      }else if( type2==7 ){
        /* Integer vs float: promote to float */
        return -1; /* integers sort before floats with same value; simplified */
      }else{
        /* Integer vs text/blob: integer sorts first */
        return -1;
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Float (type 7) */
    if( type1==7 ){
      if( type2==7 ){
        /* Compare 8-byte big-endian floats */
        int c = memcmp(pKey1+d1, pKey2+d2, 8);
        if( c!=0 ) return c;
      }else if( (type2>=1 && type2<=6) || type2==8 || type2==9 ){
        return 1; /* float after integer */
      }else{
        return -1; /* float before text/blob */
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Text types (odd serial type >= 13) */
    if( type1>=13 && (type1&1) ){
      if( type2>=13 && (type2&1) ){
        /* Both text: compare as strings (BINARY collation) */
        u32 n = len1 < len2 ? len1 : len2;
        int c = memcmp(pKey1+d1, pKey2+d2, n);
        if( c!=0 ) return c;
        if( len1<len2 ) return -1;
        if( len1>len2 ) return 1;
      }else if( type2>=12 && !(type2&1) ){
        /* Text vs blob: text sorts before blob */
        return -1;
      }else{
        /* Text vs numeric: text sorts after numeric */
        return 1;
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Blob types (even serial type >= 12) */
    if( type1>=12 && !(type1&1) ){
      if( type2>=12 && !(type2&1) ){
        u32 n = len1 < len2 ? len1 : len2;
        int c = memcmp(pKey1+d1, pKey2+d2, n);
        if( c!=0 ) return c;
        if( len1<len2 ) return -1;
        if( len1>len2 ) return 1;
      }else{
        return 1; /* blob sorts last */
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Fallback: compare raw bytes */
    {
      u32 n = len1 < len2 ? len1 : len2;
      int c = memcmp(pKey1+d1, pKey2+d2, n);
      if( c!=0 ) return c;
      if( len1<len2 ) return -1;
      if( len1>len2 ) return 1;
    }
    d1 += len1; d2 += len2;
  }

  /* All compared fields are equal. Compare by number of remaining fields. */
  if( off1 < hdr1 ) return 1;  /* key1 has more fields */
  if( off2 < hdr2 ) return -1; /* key2 has more fields */
  return 0;
}

static int compareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
){
  if( flags & PROLLY_NODE_INTKEY ){
    if( iKey1 < iKey2 ) return -1;
    if( iKey1 > iKey2 ) return +1;
    return 0;
  }else{
    return compareBlobKeys(pKey1, nKey1, pKey2, nKey2);
  }
}

/*
** Feed a single key-value pair into the chunker.
**
** For INTKEY tables the key is serialized as an 8-byte little-endian i64
** so that the chunker (which works on raw bytes) receives a sortable key
** representation.  For BLOBKEY tables the key bytes are passed through
** directly.
*/
static int feedChunker(
  ProllyChunker *pCh,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  if( flags & PROLLY_NODE_INTKEY ){
    /* Encode integer key as 8-byte little-endian for the chunker. */
    u8 aKeyBuf[8];
    aKeyBuf[0] = (u8)(intKey);
    aKeyBuf[1] = (u8)(intKey >> 8);
    aKeyBuf[2] = (u8)(intKey >> 16);
    aKeyBuf[3] = (u8)(intKey >> 24);
    aKeyBuf[4] = (u8)(intKey >> 32);
    aKeyBuf[5] = (u8)(intKey >> 40);
    aKeyBuf[6] = (u8)(intKey >> 48);
    aKeyBuf[7] = (u8)(intKey >> 56);
    return prollyChunkerAdd(pCh, aKeyBuf, 8, pVal, nVal);
  }else{
    return prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
  }
}

/*
** Build a new tree from scratch using only the edits in the MutMap.
** This is used when the old tree is empty (oldRoot is all-zeros).
**
** We simply iterate over all edits in sorted order. INSERT edits are
** fed to the chunker; DELETE edits are silently ignored (there is
** nothing to delete in an empty tree).
*/
static int buildFromEdits(
  ProllyMutator *pMut
){
  ProllyChunker chunker;
  ProllyMutMapIter iter;
  int rc;

  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ) return rc;

  prollyMutMapIterFirst(&iter, pMut->pEdits);
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      rc = feedChunker(&chunker, pMut->flags,
                       pEntry->pKey, pEntry->nKey, pEntry->intKey,
                       pEntry->pVal, pEntry->nVal);
      if( rc!=SQLITE_OK ){
        prollyChunkerFree(&chunker);
        return rc;
      }
    }
    /* DELETE edits on an empty tree: nothing to do, skip */
    prollyMutMapIterNext(&iter);
  }

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

  prollyChunkerFree(&chunker);
  return rc;
}

/*
** Clone all entries from a ProllyNode into a ProllyNodeBuilder.
*/
static int cloneNodeToBuilder(
  const ProllyNode *pNode,
  ProllyNodeBuilder *b
){
  int i, rc;
  for(i = 0; i < pNode->nItems; i++){
    const u8 *pK, *pV;
    int nK, nV;
    prollyNodeKey(pNode, i, &pK, &nK);
    prollyNodeValue(pNode, i, &pV, &nV);
    rc = prollyNodeBuilderAdd(b, pK, nK, pV, nV);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/*
** Serialize a node builder, write chunk to store, return hash.
*/
static int builderToChunk(
  ProllyNodeBuilder *b,
  ChunkStore *pStore,
  ProllyHash *pHash
){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  rc = prollyNodeBuilderFinish(b, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStorePut(pStore, pData, nData, pHash);
  sqlite3_free(pData);
  return rc;
}

/*
** Get the key for an entry in a node, handling INTKEY encoding.
** For INTKEY, writes 8-byte LE into aKeyBuf.
*/
static void getNodeKey(
  const ProllyNode *pNode, int idx, u8 flags,
  u8 *aKeyBuf, const u8 **ppKey, int *pnKey
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 ik = prollyNodeIntKey(pNode, idx);
    aKeyBuf[0] = (u8)(ik);      aKeyBuf[1] = (u8)(ik >> 8);
    aKeyBuf[2] = (u8)(ik >> 16); aKeyBuf[3] = (u8)(ik >> 24);
    aKeyBuf[4] = (u8)(ik >> 32); aKeyBuf[5] = (u8)(ik >> 40);
    aKeyBuf[6] = (u8)(ik >> 48); aKeyBuf[7] = (u8)(ik >> 56);
    *ppKey = aKeyBuf;
    *pnKey = 8;
  }else{
    prollyNodeKey(pNode, idx, ppKey, pnKey);
  }
}

/*
** Apply edits to a tree using the Dolt-style cursor-path-stack approach.
**
** Algorithm:
**   1. Iterate edits in sorted order.
**   2. For each edit, seek cursor to the edit key. The cursor's path
**      stack now points from root to the target leaf.
**   3. Clone the leaf into a builder, apply ALL edits that target this
**      leaf (consuming them from the iterator).
**   4. Serialize the new leaf → chunkStorePut → get newHash.
**   5. Walk UP the path stack from leaf-parent to root:
**      - Clone the ancestor node into a builder
**      - Replace the child hash at the cursor's index with newHash
**      - Serialize → chunkStorePut → get newHash
**   6. The final newHash is the new root for this batch of edits.
**      Set it as the cursor's root for the next seek.
**
** Cost: O(M × (log N + L)) where M = edits, L = avg leaf size.
** Unchanged subtrees are never touched — only the path from root
** to the edited leaf is rewritten.
*/
static int applyEdits(
  ProllyMutator *pMut
){
  ProllyCursor cur;
  ProllyMutMapIter iter;
  int rc;
  int seekRes;
  ProllyHash currentRoot;

  memcpy(&currentRoot, &pMut->oldRoot, sizeof(ProllyHash));
  prollyMutMapIterFirst(&iter, pMut->pEdits);

  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);

    /* Step 1: Seek cursor to edit key */
    prollyCursorInit(&cur, pMut->pStore, pMut->pCache,
                     &currentRoot, pMut->flags);
    if( pMut->flags & PROLLY_NODE_INTKEY ){
      rc = prollyCursorSeekInt(&cur, pEd->intKey, &seekRes);
    }else{
      rc = prollyCursorSeekBlob(&cur, pEd->pKey, pEd->nKey, &seekRes);
    }
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }

    int leafLevel = cur.nLevel - 1;
    ProllyNode *pLeaf = &cur.aLevel[leafLevel].pEntry->node;

    /* Step 2: Clone leaf into builder, apply all edits for this leaf */
    ProllyNodeBuilder leafBuilder;
    prollyNodeBuilderInit(&leafBuilder, 0, pMut->flags);

    /* Save iterator position to detect if merge consumed any edits */
    ProllyMutMapEntry *pIterBefore = prollyMutMapIterValid(&iter) ?
                                     prollyMutMapIterEntry(&iter) : 0;

    /* Merge old entries with edits (sorted merge into builder) */
    {
      int j = 0;
      while( j < pLeaf->nItems || prollyMutMapIterValid(&iter) ){
        int haveOld = (j < pLeaf->nItems);
        int haveEdit = prollyMutMapIterValid(&iter);
        ProllyMutMapEntry *pE = haveEdit ?
                                prollyMutMapIterEntry(&iter) : 0;

        if( haveOld && haveEdit ){
          /* Check if edit is still within this leaf's key range */
          const u8 *pLK; int nLK; i64 iLK = 0;
          u8 aLK[8];
          getNodeKey(pLeaf, pLeaf->nItems - 1, pMut->flags,
                     aLK, &pLK, &nLK);

          int editCmp;
          if( pMut->flags & PROLLY_NODE_INTKEY ){
            i64 lastIK = prollyNodeIntKey(pLeaf, pLeaf->nItems - 1);
            if( pE->intKey > lastIK ) editCmp = 1;
            else editCmp = 0;
          }else{
            editCmp = compareKeys(pMut->flags, pLK, nLK, 0,
                                  pE->pKey, pE->nKey, pE->intKey);
            editCmp = (editCmp < 0) ? 1 : 0;  /* 1 = past leaf */
          }

          if( editCmp ){
            /* Edit is past this leaf — copy remaining old entries */
            const u8 *pK; int nK;
            const u8 *pV; int nV;
            u8 aKB[8];
            getNodeKey(pLeaf, j, pMut->flags, aKB, &pK, &nK);
            prollyNodeValue(pLeaf, j, &pV, &nV);
            rc = prollyNodeBuilderAdd(&leafBuilder, pK, nK, pV, nV);
            if( rc!=SQLITE_OK ) goto leaf_err;
            j++;
            continue;
          }

          /* Compare old key with edit key */
          const u8 *pOK; int nOK; i64 iOK = 0;
          u8 aOK[8];
          if( pMut->flags & PROLLY_NODE_INTKEY ){
            iOK = prollyNodeIntKey(pLeaf, j);
          }
          getNodeKey(pLeaf, j, pMut->flags, aOK, &pOK, &nOK);

          int cmp = compareKeys(pMut->flags, pOK, nOK, iOK,
                                pE->pKey, pE->nKey, pE->intKey);
          if( cmp < 0 ){
            /* Old entry first — copy */
            const u8 *pV; int nV;
            prollyNodeValue(pLeaf, j, &pV, &nV);
            rc = prollyNodeBuilderAdd(&leafBuilder, pOK, nOK, pV, nV);
            if( rc!=SQLITE_OK ) goto leaf_err;
            j++;
          }else if( cmp == 0 ){
            /* Replace or delete */
            if( pE->op==PROLLY_EDIT_INSERT ){
              u8 aEK[8];
              const u8 *pEK; int nEK;
              getNodeKey(pLeaf, j, pMut->flags, aEK, &pEK, &nEK);
              /* Use edit's value but preserve key format */
              if( pMut->flags & PROLLY_NODE_INTKEY ){
                rc = prollyNodeBuilderAdd(&leafBuilder, pOK, nOK,
                                          pE->pVal, pE->nVal);
              }else{
                rc = prollyNodeBuilderAdd(&leafBuilder,
                       pE->pKey, pE->nKey, pE->pVal, pE->nVal);
              }
              if( rc!=SQLITE_OK ) goto leaf_err;
            }
            /* DELETE: skip both */
            j++;
            prollyMutMapIterNext(&iter);
          }else{
            /* Insert new entry before old */
            if( pE->op==PROLLY_EDIT_INSERT ){
              u8 aEK[8];
              const u8 *pEK; int nEK;
              if( pMut->flags & PROLLY_NODE_INTKEY ){
                aEK[0] = (u8)(pE->intKey);
                aEK[1] = (u8)(pE->intKey >> 8);
                aEK[2] = (u8)(pE->intKey >> 16);
                aEK[3] = (u8)(pE->intKey >> 24);
                aEK[4] = (u8)(pE->intKey >> 32);
                aEK[5] = (u8)(pE->intKey >> 40);
                aEK[6] = (u8)(pE->intKey >> 48);
                aEK[7] = (u8)(pE->intKey >> 56);
                rc = prollyNodeBuilderAdd(&leafBuilder, aEK, 8,
                                          pE->pVal, pE->nVal);
              }else{
                rc = prollyNodeBuilderAdd(&leafBuilder,
                       pE->pKey, pE->nKey, pE->pVal, pE->nVal);
              }
              if( rc!=SQLITE_OK ) goto leaf_err;
            }
            prollyMutMapIterNext(&iter);
          }
        }else if( haveOld ){
          const u8 *pK; int nK;
          const u8 *pV; int nV;
          u8 aKB[8];
          getNodeKey(pLeaf, j, pMut->flags, aKB, &pK, &nK);
          prollyNodeValue(pLeaf, j, &pV, &nV);
          rc = prollyNodeBuilderAdd(&leafBuilder, pK, nK, pV, nV);
          if( rc!=SQLITE_OK ) goto leaf_err;
          j++;
        }else{
          /* Remaining edits are past this leaf's last key.
          ** If this is the rightmost leaf (cursor would go to EOF
          ** after it), consume them as trailing inserts. Otherwise,
          ** break — the next seek will route them to the right leaf. */
          int isRightmost = 1;
          {
            /* Check if there's a next leaf by looking at parent indices */
            int lv;
            for(lv = leafLevel - 1; lv >= 0; lv--){
              if( cur.aLevel[lv].pEntry &&
                  cur.aLevel[lv].idx < cur.aLevel[lv].pEntry->node.nItems - 1 ){
                isRightmost = 0;
                break;
              }
            }
          }
          if( !isRightmost ) break;

          /* This is the rightmost leaf — consume remaining edits */
          while( prollyMutMapIterValid(&iter) ){
            ProllyMutMapEntry *pT = prollyMutMapIterEntry(&iter);
            if( pT->op==PROLLY_EDIT_INSERT ){
              u8 aEK[8];
              const u8 *pEK; int nEK;
              if( pMut->flags & PROLLY_NODE_INTKEY ){
                aEK[0] = (u8)(pT->intKey);
                aEK[1] = (u8)(pT->intKey >> 8);
                aEK[2] = (u8)(pT->intKey >> 16);
                aEK[3] = (u8)(pT->intKey >> 24);
                aEK[4] = (u8)(pT->intKey >> 32);
                aEK[5] = (u8)(pT->intKey >> 40);
                aEK[6] = (u8)(pT->intKey >> 48);
                aEK[7] = (u8)(pT->intKey >> 56);
                pEK = aEK; nEK = 8;
              }else{
                pEK = pT->pKey; nEK = pT->nKey;
              }
              rc = prollyNodeBuilderAdd(&leafBuilder,
                     pEK, nEK, pT->pVal, pT->nVal);
              if( rc!=SQLITE_OK ) goto leaf_err;
            }
            prollyMutMapIterNext(&iter);
          }
          break;
        }
      }
    }

    /* Safety: if the merge loop didn't consume ANY edits, force-consume
    ** the current one to prevent infinite loops. This happens when the
    ** seek lands on a leaf where the edit key is past the last entry
    ** AND we're not at the rightmost leaf (so the isRightmost path
    ** didn't fire). Force-consume: if it's an INSERT, add it to the
    ** builder; if DELETE, just skip it. */
    {
      ProllyMutMapEntry *pIterAfter = prollyMutMapIterValid(&iter) ?
                                      prollyMutMapIterEntry(&iter) : 0;
      if( pIterAfter == pIterBefore && pIterAfter != 0 ){
        /* Iterator didn't move — force consume */
        if( pIterAfter->op==PROLLY_EDIT_INSERT ){
          u8 aEK[8];
          const u8 *pEK; int nEK;
          if( pMut->flags & PROLLY_NODE_INTKEY ){
            aEK[0] = (u8)(pIterAfter->intKey);
            aEK[1] = (u8)(pIterAfter->intKey >> 8);
            aEK[2] = (u8)(pIterAfter->intKey >> 16);
            aEK[3] = (u8)(pIterAfter->intKey >> 24);
            aEK[4] = (u8)(pIterAfter->intKey >> 32);
            aEK[5] = (u8)(pIterAfter->intKey >> 40);
            aEK[6] = (u8)(pIterAfter->intKey >> 48);
            aEK[7] = (u8)(pIterAfter->intKey >> 56);
            pEK = aEK; nEK = 8;
          }else{
            pEK = pIterAfter->pKey; nEK = pIterAfter->nKey;
          }
          rc = prollyNodeBuilderAdd(&leafBuilder,
                 pEK, nEK, pIterAfter->pVal, pIterAfter->nVal);
          if( rc!=SQLITE_OK ) goto leaf_err;
        }
        prollyMutMapIterNext(&iter);
      }
    }

    /* Step 3: Serialize new leaf */
    ProllyHash newHash;
    if( leafBuilder.nItems == 0 ){
      /* Leaf became empty (all entries deleted).  Set hash to empty.
      ** The ancestor rewrite will handle removing the child ref. */
      memset(&newHash, 0, sizeof(ProllyHash));
    }else{
      rc = builderToChunk(&leafBuilder, pMut->pStore, &newHash);
      if( rc!=SQLITE_OK ) goto leaf_err;
    }
    prollyNodeBuilderFree(&leafBuilder);

    /* Step 4: Walk UP ancestor chain, replacing child hashes */
    {
      int level;
      for(level = leafLevel - 1; level >= 0; level--){
        ProllyNode *pAnc = &cur.aLevel[level].pEntry->node;
        int childIdx = cur.aLevel[level].idx;
        ProllyNodeBuilder ancBuilder;

        prollyNodeBuilderInit(&ancBuilder, (u8)(pAnc->level),
                              pMut->flags);

        /* Copy all entries, replacing the child hash at childIdx */
        int k;
        for(k = 0; k < pAnc->nItems; k++){
          const u8 *pK; int nK;
          const u8 *pV; int nV;
          prollyNodeKey(pAnc, k, &pK, &nK);
          if( k == childIdx ){
            /* Check if child was deleted (empty hash) */
            if( prollyHashIsEmpty(&newHash) ){
              /* Skip this entry — child was deleted */
              continue;
            }
            /* Replace with new child hash */
            rc = prollyNodeBuilderAdd(&ancBuilder, pK, nK,
                                      newHash.data, PROLLY_HASH_SIZE);
          }else{
            prollyNodeValue(pAnc, k, &pV, &nV);
            rc = prollyNodeBuilderAdd(&ancBuilder, pK, nK, pV, nV);
          }
          if( rc!=SQLITE_OK ){
            prollyNodeBuilderFree(&ancBuilder);
            prollyCursorClose(&cur);
            return rc;
          }
        }

        if( ancBuilder.nItems == 0 ){
          /* Ancestor became empty — propagate up */
          memset(&newHash, 0, sizeof(ProllyHash));
          prollyNodeBuilderFree(&ancBuilder);
          continue;
        }

        rc = builderToChunk(&ancBuilder, pMut->pStore, &newHash);
        prollyNodeBuilderFree(&ancBuilder);
        if( rc!=SQLITE_OK ){
          prollyCursorClose(&cur);
          return rc;
        }
      }
    }

    /* newHash is now the new root */
    memcpy(&currentRoot, &newHash, sizeof(ProllyHash));
    prollyCursorClose(&cur);
    continue;

leaf_err:
    prollyNodeBuilderFree(&leafBuilder);
    prollyCursorClose(&cur);
    return rc;
  }

  memcpy(&pMut->newRoot, &currentRoot, sizeof(ProllyHash));
  return SQLITE_OK;
}

/*
** Apply all edits in pMut->pEdits to the tree rooted at pMut->oldRoot.
** Produces a new tree whose root hash is stored in pMut->newRoot.
**
** Three cases:
**   1. No edits: newRoot = oldRoot (no work).
**   2. Old tree is empty: build from edits alone.
**   3. Otherwise: merge-walk old tree + edits.
**
** Returns SQLITE_OK on success with result in pMut->newRoot.
*/
int prollyMutateFlush(ProllyMutator *pMut){
  /* Case 1: no edits — the tree is unchanged */
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  /* Case 2: old tree is empty — build entirely from edits */
  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    return buildFromEdits(pMut);
  }

  /* Case 3: targeted leaf edits */
  return applyEdits(pMut);
}

/*
** Convenience function: apply a single INSERT to a tree.
**
** Creates a temporary 1-entry MutMap, wraps it in a ProllyMutator,
** calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateInsert(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the INSERT */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

/*
** Convenience function: apply a single DELETE to a tree.
**
** Creates a temporary 1-entry MutMap with a DELETE edit, wraps it in a
** ProllyMutator, calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateDelete(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the DELETE */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
