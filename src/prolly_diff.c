/*
** prolly_diff.c — Two-tree diff for prolly trees.
**
** Computes the set of changes (ADD, DELETE, MODIFY) between two prolly
** trees by walking two cursors in parallel, comparing keys in sorted
** order.  When the root hashes are identical the diff is trivially empty.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"

#include <string.h>  /* memcmp */

/*
** Compare two blob keys lexicographically.
** Returns <0, 0, or >0.
*/
static int diffBlobKeyCmp(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  int n = (nA < nB) ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  return nA - nB;
}

/*
** Compare the current keys of two cursors.
** Sets *pCmp to <0, 0, or >0 following the same convention as memcmp.
**
** For INTKEY trees the integer keys are compared directly.
** For BLOBKEY trees the raw key blobs are compared lexicographically.
*/
static void diffCompareKeys(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  int *pCmp
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 iOld = prollyCursorIntKey(pOld);
    i64 iNew = prollyCursorIntKey(pNew);
    if( iOld < iNew )      *pCmp = -1;
    else if( iOld > iNew )  *pCmp =  1;
    else                     *pCmp =  0;
  }else{
    const u8 *pKeyOld; int nKeyOld;
    const u8 *pKeyNew; int nKeyNew;
    prollyCursorKey(pOld, &pKeyOld, &nKeyOld);
    prollyCursorKey(pNew, &pKeyNew, &nKeyNew);
    *pCmp = diffBlobKeyCmp(pKeyOld, nKeyOld, pKeyNew, nKeyNew);
  }
}

/*
** Fill a ProllyDiffChange from a single cursor entry.
**   type     – PROLLY_DIFF_ADD, _DELETE, or _MODIFY
**   pCur     – cursor positioned at the entry
**   flags    – INTKEY / BLOBKEY
**   pChange  – [out] change record to populate
**
** Key fields are always set from pCur.  Caller is responsible for
** setting the value pointers that correspond to the "other" side
** (pOldVal for ADD, pNewVal for DELETE, or both for MODIFY).
*/
static void diffFillKey(
  ProllyDiffChange *pChange,
  ProllyCursor *pCur,
  u8 flags
){
  if( flags & PROLLY_NODE_INTKEY ){
    pChange->intKey = prollyCursorIntKey(pCur);
    pChange->pKey   = 0;
    pChange->nKey   = 0;
  }else{
    const u8 *pKey; int nKey;
    prollyCursorKey(pCur, &pKey, &nKey);
    pChange->pKey   = pKey;
    pChange->nKey   = nKey;
    pChange->intKey = 0;
  }
}

/*
** Emit a DELETE change for the current entry of curOld.
*/
static int diffEmitDelete(
  ProllyCursor *pOld,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pVal; int nVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_DELETE;
  diffFillKey(&change, pOld, flags);
  prollyCursorValue(pOld, &pVal, &nVal);
  change.pOldVal = pVal;
  change.nOldVal = nVal;
  change.pNewVal = 0;
  change.nNewVal = 0;
  return xCallback(pCtx, &change);
}

/*
** Emit an ADD change for the current entry of curNew.
*/
static int diffEmitAdd(
  ProllyCursor *pNew,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pVal; int nVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_ADD;
  diffFillKey(&change, pNew, flags);
  prollyCursorValue(pNew, &pVal, &nVal);
  change.pOldVal = 0;
  change.nOldVal = 0;
  change.pNewVal = pVal;
  change.nNewVal = nVal;
  return xCallback(pCtx, &change);
}

/*
** Emit a MODIFY change when the key matches but the value differs.
*/
static int diffEmitModify(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_MODIFY;
  diffFillKey(&change, pNew, flags);
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  change.pOldVal = pOldVal;
  change.nOldVal = nOldVal;
  change.pNewVal = pNewVal;
  change.nNewVal = nNewVal;
  return xCallback(pCtx, &change);
}

/*
** Read a SQLite-format varint from p, not reading past pEnd.
** Returns the number of bytes consumed, or 0 on error.
*/
static int diffReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v = 0;
  int i;
  if( p >= pEnd ){ *pVal = 0; return 0; }
  for(i=0; i<9 && p+i<pEnd; i++){
    if( i<8 ){
      v = (v << 7) | (p[i] & 0x7f);
      if( (p[i] & 0x80)==0 ){ *pVal = v; return i+1; }
    }else{
      v = (v << 8) | p[i];
      *pVal = v;
      return 9;
    }
  }
  *pVal = v;
  return i ? i : 0;
}

/*
** Return the data size in bytes for a given SQLite serial type.
*/
static int diffSerialTypeSize(u64 st){
  if( st==0 ) return 0;
  if( st==1 ) return 1;
  if( st==2 ) return 2;
  if( st==3 ) return 3;
  if( st==4 ) return 4;
  if( st==5 ) return 6;
  if( st==6 ) return 8;
  if( st==7 ) return 8;
  if( st==8 || st==9 ) return 0;
  if( st>=12 && (st&1)==0 ) return (int)(st-12)/2;
  if( st>=13 && (st&1)==1 ) return (int)(st-13)/2;
  return 0;
}

/*
** Compare two SQLite record-format values field-by-field, treating
** trailing NULL fields (serial type 0) in the longer record as equal
** to missing fields in the shorter record.  This handles the case
** where ALTER TABLE ADD COLUMN causes records to be rewritten with
** extra trailing NULLs.
**
** Returns non-zero if the records are logically equal.
** Returns 0 (not equal) if either record is malformed.
*/
int diffRecordsEqualFieldwise(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  const u8 *pEndA, *pEndB;
  u64 hdrSizeA, hdrSizeB;
  int hdrBytesA, hdrBytesB;
  const u8 *hpA, *hpB;
  const u8 *hdrEndA, *hdrEndB;
  int offA, offB;

  if( nA < 1 || nB < 1 ) return 0;

  pEndA = pA + nA;
  pEndB = pB + nB;

  hdrBytesA = diffReadVarint(pA, pEndA, &hdrSizeA);
  if( hdrBytesA == 0 ) return 0;
  hdrBytesB = diffReadVarint(pB, pEndB, &hdrSizeB);
  if( hdrBytesB == 0 ) return 0;

  if( (int)hdrSizeA < hdrBytesA || (int)hdrSizeA > nA ) return 0;
  if( (int)hdrSizeB < hdrBytesB || (int)hdrSizeB > nB ) return 0;

  hpA = pA + hdrBytesA;
  hpB = pB + hdrBytesB;
  hdrEndA = pA + (int)hdrSizeA;
  hdrEndB = pB + (int)hdrSizeB;
  offA = (int)hdrSizeA;
  offB = (int)hdrSizeB;

  while( hpA < hdrEndA || hpB < hdrEndB ){
    u64 stA = 0, stB = 0;
    int szA, szB;

    if( hpA < hdrEndA ){
      int n = diffReadVarint(hpA, hdrEndA, &stA);
      if( n == 0 ) return 0;
      hpA += n;
    }
    if( hpB < hdrEndB ){
      int n = diffReadVarint(hpB, hdrEndB, &stB);
      if( n == 0 ) return 0;
      hpB += n;
    }

    szA = diffSerialTypeSize(stA);
    szB = diffSerialTypeSize(stB);

    if( stA == 0 && stB == 0 ) continue;
    if( stA != stB ) return 0;

    if( offA + szA > nA || offB + szB > nB ) return 0;
    if( szA > 0 && memcmp(pA + offA, pB + offB, szA) != 0 ) return 0;

    offA += szA;
    offB += szB;
  }

  return 1;
}

/*
** Return non-zero if the two values at the current cursor positions
** are identical.  Fast path: same length and same bytes.  Slow path:
** parse SQLite record format to compare field-by-field, tolerating
** trailing NULL differences from ALTER TABLE ADD COLUMN.
*/
static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  if( nOldVal == nNewVal ){
    if( nOldVal == 0 ) return 1;
    return memcmp(pOldVal, pNewVal, nOldVal) == 0;
  }
  if( nOldVal < 2 || nNewVal < 2 ) return 0;
  return diffRecordsEqualFieldwise(pOldVal, nOldVal, pNewVal, nNewVal);
}

/* --------------------------------------------------------------------------
** Cursor-based merge walk: brute-force diff of two subtrees via cursors.
** O(N+M) where N and M are the entry counts of the two subtrees.
** Used as fallback when structural comparison can't skip subtrees
** (e.g., boundary key mismatches at internal node level).
** -------------------------------------------------------------------------- */
static int diffCursorWalk(
  ChunkStore *pStore, ProllyCache *pCache,
  const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
  u8 flags, ProllyDiffCallback xCb, void *pCtx
){
  ProllyCursor *pCurOld = 0;
  ProllyCursor *pCurNew = 0;
  int rc = SQLITE_OK;
  int emptyOld = 0, emptyNew = 0;

  pCurOld = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  pCurNew = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pCurOld || !pCurNew ){
    sqlite3_free(pCurOld); sqlite3_free(pCurNew);
    return SQLITE_NOMEM;
  }

  prollyCursorInit(pCurOld, pStore, pCache, pOldRoot, flags);
  prollyCursorInit(pCurNew, pStore, pCache, pNewRoot, flags);

  rc = prollyCursorFirst(pCurOld, &emptyOld);
  if( rc==SQLITE_OK ) rc = prollyCursorFirst(pCurNew, &emptyNew);
  if( rc!=SQLITE_OK ) goto walk_done;

  while( prollyCursorIsValid(pCurOld) && prollyCursorIsValid(pCurNew) ){
    int cmp;
    diffCompareKeys(pCurOld, pCurNew, flags, &cmp);
    if( cmp < 0 ){
      rc = diffEmitDelete(pCurOld, flags, xCb, pCtx);
      if( rc==SQLITE_OK ) rc = prollyCursorNext(pCurOld);
    }else if( cmp > 0 ){
      rc = diffEmitAdd(pCurNew, flags, xCb, pCtx);
      if( rc==SQLITE_OK ) rc = prollyCursorNext(pCurNew);
    }else{
      if( !diffValuesEqual(pCurOld, pCurNew) ){
        rc = diffEmitModify(pCurOld, pCurNew, flags, xCb, pCtx);
      }
      if( rc==SQLITE_OK ) rc = prollyCursorNext(pCurOld);
      if( rc==SQLITE_OK ) rc = prollyCursorNext(pCurNew);
    }
    if( rc!=SQLITE_OK ) break;
  }
  while( rc==SQLITE_OK && prollyCursorIsValid(pCurOld) ){
    rc = diffEmitDelete(pCurOld, flags, xCb, pCtx);
    if( rc==SQLITE_OK ) rc = prollyCursorNext(pCurOld);
  }
  while( rc==SQLITE_OK && prollyCursorIsValid(pCurNew) ){
    rc = diffEmitAdd(pCurNew, flags, xCb, pCtx);
    if( rc==SQLITE_OK ) rc = prollyCursorNext(pCurNew);
  }

walk_done:
  prollyCursorClose(pCurOld);
  prollyCursorClose(pCurNew);
  sqlite3_free(pCurOld);
  sqlite3_free(pCurNew);
  return rc;
}

/* --------------------------------------------------------------------------
** Emit all entries under a subtree as ADDs or DELETEs.
** -------------------------------------------------------------------------- */
static int diffEmitSubtree(
  ChunkStore *pStore, ProllyCache *pCache,
  const ProllyHash *pRoot, u8 flags, u8 changeType,
  ProllyDiffCallback xCb, void *pCtx
){
  ProllyCursor cur;
  int rc, empty = 0;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  prollyCursorInit(&cur, pStore, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &empty);
  if( rc!=SQLITE_OK || empty ){ prollyCursorClose(&cur); return rc; }
  while( prollyCursorIsValid(&cur) ){
    if( changeType==PROLLY_DIFF_ADD ){
      rc = diffEmitAdd(&cur, flags, xCb, pCtx);
    }else{
      rc = diffEmitDelete(&cur, flags, xCb, pCtx);
    }
    if( rc!=SQLITE_OK ) break;
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  return rc;
}

/* --------------------------------------------------------------------------
** Compare boundary keys from two internal nodes.
** -------------------------------------------------------------------------- */
static int diffNodeKeyCmp(
  const ProllyNode *pA, int iA,
  const ProllyNode *pB, int iB,
  u8 flags
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 a = prollyNodeIntKey(pA, iA);
    i64 b = prollyNodeIntKey(pB, iB);
    return (a < b) ? -1 : (a > b) ? 1 : 0;
  }else{
    const u8 *pKA; int nKA;
    const u8 *pKB; int nKB;
    prollyNodeKey(pA, iA, &pKA, &nKA);
    prollyNodeKey(pB, iB, &pKB, &nKB);
    return diffBlobKeyCmp(pKA, nKA, pKB, nKB);
  }
}

/* --------------------------------------------------------------------------
** Leaf-level diff: compare entries in two leaf nodes directly.
** -------------------------------------------------------------------------- */
static int diffLeaves(
  const ProllyNode *pOld, const ProllyNode *pNew, u8 flags,
  ProllyDiffCallback xCb, void *pCtx
){
  int i = 0, j = 0, rc = SQLITE_OK;

  while( i < (int)pOld->nItems && j < (int)pNew->nItems ){
    int cmp;
    if( flags & PROLLY_NODE_INTKEY ){
      i64 ik = prollyNodeIntKey(pOld, i);
      i64 jk = prollyNodeIntKey(pNew, j);
      cmp = (ik < jk) ? -1 : (ik > jk) ? 1 : 0;
    }else{
      const u8 *pKA; int nKA; const u8 *pKB; int nKB;
      prollyNodeKey(pOld, i, &pKA, &nKA);
      prollyNodeKey(pNew, j, &pKB, &nKB);
      cmp = diffBlobKeyCmp(pKA, nKA, pKB, nKB);
    }

    if( cmp < 0 ){
      ProllyDiffChange ch; const u8 *pV; int nV;
      memset(&ch, 0, sizeof(ch));
      ch.type = PROLLY_DIFF_DELETE;
      if( flags & PROLLY_NODE_INTKEY ) ch.intKey = prollyNodeIntKey(pOld, i);
      else prollyNodeKey(pOld, i, &ch.pKey, &ch.nKey);
      prollyNodeValue(pOld, i, &pV, &nV);
      ch.pOldVal = pV; ch.nOldVal = nV;
      rc = xCb(pCtx, &ch); i++;
    }else if( cmp > 0 ){
      ProllyDiffChange ch; const u8 *pV; int nV;
      memset(&ch, 0, sizeof(ch));
      ch.type = PROLLY_DIFF_ADD;
      if( flags & PROLLY_NODE_INTKEY ) ch.intKey = prollyNodeIntKey(pNew, j);
      else prollyNodeKey(pNew, j, &ch.pKey, &ch.nKey);
      prollyNodeValue(pNew, j, &pV, &nV);
      ch.pNewVal = pV; ch.nNewVal = nV;
      rc = xCb(pCtx, &ch); j++;
    }else{
      const u8 *pOV; int nOV; const u8 *pNV; int nNV;
      prollyNodeValue(pOld, i, &pOV, &nOV);
      prollyNodeValue(pNew, j, &pNV, &nNV);
      int eq = (nOV==nNV && (nOV==0 || memcmp(pOV, pNV, nOV)==0));
      if( !eq && nOV>=2 && nNV>=2 ) eq = diffRecordsEqualFieldwise(pOV,nOV,pNV,nNV);
      if( !eq ){
        ProllyDiffChange ch;
        memset(&ch, 0, sizeof(ch));
        ch.type = PROLLY_DIFF_MODIFY;
        if( flags & PROLLY_NODE_INTKEY ) ch.intKey = prollyNodeIntKey(pNew, j);
        else prollyNodeKey(pNew, j, &ch.pKey, &ch.nKey);
        ch.pOldVal = pOV; ch.nOldVal = nOV;
        ch.pNewVal = pNV; ch.nNewVal = nNV;
        rc = xCb(pCtx, &ch);
      }
      i++; j++;
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  while( i < (int)pOld->nItems && rc==SQLITE_OK ){
    ProllyDiffChange ch; const u8 *pV; int nV;
    memset(&ch, 0, sizeof(ch));
    ch.type = PROLLY_DIFF_DELETE;
    if( flags & PROLLY_NODE_INTKEY ) ch.intKey = prollyNodeIntKey(pOld, i);
    else prollyNodeKey(pOld, i, &ch.pKey, &ch.nKey);
    prollyNodeValue(pOld, i, &pV, &nV);
    ch.pOldVal = pV; ch.nOldVal = nV;
    rc = xCb(pCtx, &ch); i++;
  }
  while( j < (int)pNew->nItems && rc==SQLITE_OK ){
    ProllyDiffChange ch; const u8 *pV; int nV;
    memset(&ch, 0, sizeof(ch));
    ch.type = PROLLY_DIFF_ADD;
    if( flags & PROLLY_NODE_INTKEY ) ch.intKey = prollyNodeIntKey(pNew, j);
    else prollyNodeKey(pNew, j, &ch.pKey, &ch.nKey);
    prollyNodeValue(pNew, j, &pV, &nV);
    ch.pNewVal = pV; ch.nNewVal = nV;
    rc = xCb(pCtx, &ch); j++;
  }
  return rc;
}

/* --------------------------------------------------------------------------
** Recursive structural diff.
**
** Compares two prolly tree nodes top-down, skipping shared subtrees
** by hash comparison.  For k changes on an N-entry tree of depth d,
** this visits O(k * d) nodes instead of O(N).
**
**   diffNodes(oldHash, newHash):
**     if oldHash == newHash → return (skip entire subtree!)
**     load both nodes
**     if both leaves → compare entries directly
**     if both internal → merge-walk children:
**       matching childHash → SKIP (the key optimization)
**       same boundary key, different childHash → recurse
**       boundary mismatch → fall back to cursor walk for that range
** -------------------------------------------------------------------------- */
static int diffNodes(
  ChunkStore *pStore, ProllyCache *pCache,
  const ProllyHash *pOldHash, const ProllyHash *pNewHash,
  const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
  u8 flags, ProllyDiffCallback xCb, void *pCtx
){
  u8 *oldData = 0, *newData = 0;
  int nOld = 0, nNew = 0;
  ProllyNode oldNode, newNode;
  int rc = SQLITE_OK;

  /* Same hash → identical subtree, zero diff */
  if( prollyHashCompare(pOldHash, pNewHash)==0 ) return SQLITE_OK;

  /* Handle empty hashes */
  if( prollyHashIsEmpty(pOldHash) && prollyHashIsEmpty(pNewHash) ) return SQLITE_OK;
  if( prollyHashIsEmpty(pOldHash) ){
    return diffEmitSubtree(pStore, pCache, pNewHash, flags, PROLLY_DIFF_ADD, xCb, pCtx);
  }
  if( prollyHashIsEmpty(pNewHash) ){
    return diffEmitSubtree(pStore, pCache, pOldHash, flags, PROLLY_DIFF_DELETE, xCb, pCtx);
  }

  /* Load both nodes */
  rc = chunkStoreGet(pStore, pOldHash, &oldData, &nOld);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParse(&oldNode, oldData, nOld);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); return rc; }

  rc = chunkStoreGet(pStore, pNewHash, &newData, &nNew);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); return rc; }
  rc = prollyNodeParse(&newNode, newData, nNew);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); sqlite3_free(newData); return rc; }

  if( oldNode.level==0 && newNode.level==0 ){
    /* ── Both leaves: direct entry comparison ── */
    rc = diffLeaves(&oldNode, &newNode, flags, xCb, pCtx);

  }else if( oldNode.level>0 && newNode.level>0 ){
    /* ── Both internal: compare children, skip shared subtrees ── */
    int i = 0, j = 0;

    while( i < (int)oldNode.nItems && j < (int)newNode.nItems && rc==SQLITE_OK ){
      ProllyHash oldChild, newChild;
      prollyNodeChildHash(&oldNode, i, &oldChild);
      prollyNodeChildHash(&newNode, j, &newChild);

      /* THE KEY OPTIMIZATION: skip shared subtrees */
      if( prollyHashCompare(&oldChild, &newChild)==0 ){
        i++; j++;
        continue;
      }

      int cmp = diffNodeKeyCmp(&oldNode, i, &newNode, j, flags);

      if( cmp==0 ){
        /* Same boundary key, different content → recurse */
        rc = diffNodes(pStore, pCache, &oldChild, &newChild,
                       pOldRoot, pNewRoot, flags, xCb, pCtx);
        i++; j++;
      }else{
        /* Boundary mismatch: the chunker split differently between old
        ** and new. We can't pair children 1:1 by boundary key.
        **
        ** Correct approach: use cursor walk on the FULL tree roots for
        ** the remaining range. The cursors need to be positioned past
        ** the already-processed children (those with matching hashes).
        ** We use the boundary key of the last matched child as the
        ** seek target. */
        {
          ProllyCursor *pCO = sqlite3_malloc(sizeof(ProllyCursor));
          ProllyCursor *pCN = sqlite3_malloc(sizeof(ProllyCursor));
          if( !pCO || !pCN ){
            sqlite3_free(pCO); sqlite3_free(pCN);
            rc = SQLITE_NOMEM;
          }else{
            prollyCursorInit(pCO, pStore, pCache, pOldRoot, flags);
            prollyCursorInit(pCN, pStore, pCache, pNewRoot, flags);

            /* Seek past already-processed children */
            if( i > 0 && (flags & PROLLY_NODE_INTKEY) ){
              i64 seekKey = prollyNodeIntKey(&oldNode, i-1);
              int res;
              rc = prollyCursorSeekInt(pCO, seekKey, &res);
              if( rc==SQLITE_OK && res==0 ) rc = prollyCursorNext(pCO);
              if( rc==SQLITE_OK ){
                rc = prollyCursorSeekInt(pCN, seekKey, &res);
                if( rc==SQLITE_OK && res==0 ) rc = prollyCursorNext(pCN);
              }
            }else if( i > 0 ){
              const u8 *pSK; int nSK;
              int emO=0, emN=0;
              prollyNodeKey(&oldNode, i-1, &pSK, &nSK);
              rc = prollyCursorSeekBlob(pCO, pSK, nSK, &emO);
              if( rc==SQLITE_OK && emO==0 ) rc = prollyCursorNext(pCO);
              if( rc==SQLITE_OK ){
                rc = prollyCursorSeekBlob(pCN, pSK, nSK, &emN);
                if( rc==SQLITE_OK && emN==0 ) rc = prollyCursorNext(pCN);
              }
            }else{
              int emO=0, emN=0;
              rc = prollyCursorFirst(pCO, &emO);
              if( rc==SQLITE_OK ) rc = prollyCursorFirst(pCN, &emN);
            }

            /* Merge walk the rest */
            while( rc==SQLITE_OK && prollyCursorIsValid(pCO) && prollyCursorIsValid(pCN) ){
              int keyCmp;
              diffCompareKeys(pCO, pCN, flags, &keyCmp);
              if( keyCmp < 0 ){
                rc = diffEmitDelete(pCO, flags, xCb, pCtx);
                if( rc==SQLITE_OK ) rc = prollyCursorNext(pCO);
              }else if( keyCmp > 0 ){
                rc = diffEmitAdd(pCN, flags, xCb, pCtx);
                if( rc==SQLITE_OK ) rc = prollyCursorNext(pCN);
              }else{
                if( !diffValuesEqual(pCO, pCN) ){
                  rc = diffEmitModify(pCO, pCN, flags, xCb, pCtx);
                }
                if( rc==SQLITE_OK ) rc = prollyCursorNext(pCO);
                if( rc==SQLITE_OK ) rc = prollyCursorNext(pCN);
              }
            }
            while( rc==SQLITE_OK && prollyCursorIsValid(pCO) ){
              rc = diffEmitDelete(pCO, flags, xCb, pCtx);
              if( rc==SQLITE_OK ) rc = prollyCursorNext(pCO);
            }
            while( rc==SQLITE_OK && prollyCursorIsValid(pCN) ){
              rc = diffEmitAdd(pCN, flags, xCb, pCtx);
              if( rc==SQLITE_OK ) rc = prollyCursorNext(pCN);
            }

            prollyCursorClose(pCO);
            prollyCursorClose(pCN);
            sqlite3_free(pCO);
            sqlite3_free(pCN);
          }
        }
        i = (int)oldNode.nItems;
        j = (int)newNode.nItems;
      }
    }

    /* Remaining old children → all deleted */
    while( i < (int)oldNode.nItems && rc==SQLITE_OK ){
      ProllyHash ch;
      prollyNodeChildHash(&oldNode, i, &ch);
      rc = diffEmitSubtree(pStore, pCache, &ch, flags, PROLLY_DIFF_DELETE, xCb, pCtx);
      i++;
    }
    /* Remaining new children → all added */
    while( j < (int)newNode.nItems && rc==SQLITE_OK ){
      ProllyHash ch;
      prollyNodeChildHash(&newNode, j, &ch);
      rc = diffEmitSubtree(pStore, pCache, &ch, flags, PROLLY_DIFF_ADD, xCb, pCtx);
      j++;
    }

  }else{
    /* Level mismatch — shouldn't happen in well-formed trees. Fallback. */
    rc = diffCursorWalk(pStore, pCache, pOldRoot, pNewRoot, flags, xCb, pCtx);
  }

  sqlite3_free(oldData);
  sqlite3_free(newData);
  return rc;
}

/* --------------------------------------------------------------------------
** Public API: Structural diff between two prolly trees.
**
** Exploits content-addressing to skip shared subtrees.  For k changes
** on an N-entry tree of depth d, this visits O(k * d) nodes instead
** of O(N).  Falls back to cursor-based merge walk when chunk boundaries
** shifted between old and new trees.
** -------------------------------------------------------------------------- */
int prollyDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  return diffNodes(pStore, pCache, pOldRoot, pNewRoot,
                   pOldRoot, pNewRoot, flags, xCallback, pCtx);
}

#endif /* DOLTLITE_PROLLY */
