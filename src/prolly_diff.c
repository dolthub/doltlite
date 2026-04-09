
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"

#include <string.h>  

static int diffBlobKeyCmp(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  int n = (nA < nB) ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  return nA - nB;
}

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

/* Compare two SQLite record-format values field-by-field. Returns 1 if
** equal, 0 otherwise. Needed because records with identical logical content
** can have different total byte lengths due to varint encoding differences. */
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

/* Compare two record values: fast memcmp path, then field-wise fallback
** for records with different varint encodings of the same logical data. */
int prollyValuesEqual(const u8 *pA, int nA, const u8 *pB, int nB){
  if( nA==nB ){
    if( nA==0 ) return 1;
    if( memcmp(pA, pB, nA)==0 ) return 1;
  }
  if( nA < 2 || nB < 2 ) return 0;
  return diffRecordsEqualFieldwise(pA, nA, pB, nB);
}

static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  return prollyValuesEqual(pOldVal, nOldVal, pNewVal, nNewVal);
}

/* Merge-walk two positioned cursors, emitting diffs until both are exhausted. */
static int diffMergeWalk(
  ProllyCursor *pCurOld, ProllyCursor *pCurNew,
  u8 flags, ProllyDiffCallback xCb, void *pCtx
){
  int rc = SQLITE_OK;
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
  return rc;
}

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
  if( rc==SQLITE_OK ) rc = diffMergeWalk(pCurOld, pCurNew, flags, xCb, pCtx);

walk_done:
  prollyCursorClose(pCurOld);
  prollyCursorClose(pCurNew);
  sqlite3_free(pCurOld);
  sqlite3_free(pCurNew);
  return rc;
}

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
      int eq;
      prollyNodeValue(pOld, i, &pOV, &nOV);
      prollyNodeValue(pNew, j, &pNV, &nNV);
      eq = (nOV==nNV && (nOV==0 || memcmp(pOV, pNV, nOV)==0));
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

/* Recursive node-level diff. Identical child hashes are skipped (structural
** sharing). When internal keys align, recurse into changed subtrees. When
** keys diverge (tree shape changed), fall back to a full cursor walk from
** pOldRoot/pNewRoot -- this handles inserts/deletes that shifted boundaries. */
/*
** Process one level of the diff between two internal nodes. For child
** pairs with matching key boundaries but different hashes, push them
** onto the work stack for iterative descent instead of recursing.
*/
static int diffNodesOneLevel(
  ChunkStore *pStore, ProllyCache *pCache,
  const ProllyHash *pOldHash, const ProllyHash *pNewHash,
  const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
  u8 flags, ProllyDiffCallback xCb, void *pCtx,
  ProllyHash **ppStack, int *pnStack, int *pnStackAlloc
){
  u8 *oldData = 0, *newData = 0;
  int nOld = 0, nNew = 0;
  ProllyNode oldNode, newNode;
  int rc = SQLITE_OK;

  if( prollyHashCompare(pOldHash, pNewHash)==0 ) return SQLITE_OK;

  if( prollyHashIsEmpty(pOldHash) && prollyHashIsEmpty(pNewHash) ) return SQLITE_OK;
  if( prollyHashIsEmpty(pOldHash) ){
    return diffEmitSubtree(pStore, pCache, pNewHash, flags, PROLLY_DIFF_ADD, xCb, pCtx);
  }
  if( prollyHashIsEmpty(pNewHash) ){
    return diffEmitSubtree(pStore, pCache, pOldHash, flags, PROLLY_DIFF_DELETE, xCb, pCtx);
  }

  rc = chunkStoreGet(pStore, pOldHash, &oldData, &nOld);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParse(&oldNode, oldData, nOld);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); return rc; }

  rc = chunkStoreGet(pStore, pNewHash, &newData, &nNew);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); return rc; }
  rc = prollyNodeParse(&newNode, newData, nNew);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); sqlite3_free(newData); return rc; }

  if( oldNode.level==0 && newNode.level==0 ){
    rc = diffLeaves(&oldNode, &newNode, flags, xCb, pCtx);

  }else if( oldNode.level>0 && newNode.level>0 ){
    int i = 0, j = 0;

    while( i < (int)oldNode.nItems && j < (int)newNode.nItems && rc==SQLITE_OK ){
      ProllyHash oldChild, newChild;
      int cmp;
      prollyNodeChildHash(&oldNode, i, &oldChild);
      prollyNodeChildHash(&newNode, j, &newChild);

      if( prollyHashCompare(&oldChild, &newChild)==0 ){
        i++; j++;
        continue;
      }

      cmp = diffNodeKeyCmp(&oldNode, i, &newNode, j, flags);

      if( cmp==0 ){
        /* Push child pair onto work stack for iterative processing */
        if( *pnStack + 2 > *pnStackAlloc ){
          int nNew = *pnStackAlloc ? *pnStackAlloc * 2 : 32;
          ProllyHash *pNew = sqlite3_realloc(*ppStack,
                                             nNew * (int)sizeof(ProllyHash));
          if( !pNew ){ rc = SQLITE_NOMEM; break; }
          *ppStack = pNew;
          *pnStackAlloc = nNew;
        }
        (*ppStack)[(*pnStack)++] = oldChild;
        (*ppStack)[(*pnStack)++] = newChild;
        i++; j++;
      }else{
        
        {
          ProllyCursor *pCO = sqlite3_malloc(sizeof(ProllyCursor));
          ProllyCursor *pCN = sqlite3_malloc(sizeof(ProllyCursor));
          if( !pCO || !pCN ){
            sqlite3_free(pCO); sqlite3_free(pCN);
            rc = SQLITE_NOMEM;
          }else{
            prollyCursorInit(pCO, pStore, pCache, pOldRoot, flags);
            prollyCursorInit(pCN, pStore, pCache, pNewRoot, flags);

            
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

            if( rc==SQLITE_OK ) rc = diffMergeWalk(pCO, pCN, flags, xCb, pCtx);

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

    
    while( i < (int)oldNode.nItems && rc==SQLITE_OK ){
      ProllyHash ch;
      prollyNodeChildHash(&oldNode, i, &ch);
      rc = diffEmitSubtree(pStore, pCache, &ch, flags, PROLLY_DIFF_DELETE, xCb, pCtx);
      i++;
    }
    
    while( j < (int)newNode.nItems && rc==SQLITE_OK ){
      ProllyHash ch;
      prollyNodeChildHash(&newNode, j, &ch);
      rc = diffEmitSubtree(pStore, pCache, &ch, flags, PROLLY_DIFF_ADD, xCb, pCtx);
      j++;
    }

  }else{
    
    rc = diffCursorWalk(pStore, pCache, pOldRoot, pNewRoot, flags, xCb, pCtx);
  }

  sqlite3_free(oldData);
  sqlite3_free(newData);
  return rc;
}

int prollyDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyHash *aStack = 0;
  int nStack = 0, nStackAlloc = 0;
  int rc = SQLITE_OK;

  /* Seed the work stack with the root pair */
  aStack = sqlite3_malloc(32 * (int)sizeof(ProllyHash));
  if( !aStack ) return SQLITE_NOMEM;
  nStackAlloc = 32;
  aStack[nStack++] = *pOldRoot;
  aStack[nStack++] = *pNewRoot;

  /* Iterative descent: process each (old, new) hash pair. Each level
  ** may push child pairs back onto the stack instead of recursing. */
  while( nStack >= 2 && rc==SQLITE_OK ){
    ProllyHash newH = aStack[--nStack];
    ProllyHash oldH = aStack[--nStack];
    rc = diffNodesOneLevel(pStore, pCache, &oldH, &newH,
                           pOldRoot, pNewRoot, flags, xCallback, pCtx,
                           &aStack, &nStack, &nStackAlloc);
  }
  sqlite3_free(aStack);
  return rc;
}

#endif 
