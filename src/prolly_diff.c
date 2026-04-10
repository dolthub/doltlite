
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"
#include "doltlite_record.h"

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

/* Compare two SQLite record-format values field-by-field. Returns
** SQLITE_OK and sets *pEqual, or SQLITE_CORRUPT for malformed records. */
static int diffRecordsEqualFieldwise(
  const u8 *pA, int nA,
  const u8 *pB, int nB,
  int *pEqual
){
  DoltliteRecordInfo aInfo;
  DoltliteRecordInfo bInfo;
  int i;
  int rc;

  *pEqual = 0;
  if( nA < 1 || nB < 1 ) return SQLITE_CORRUPT;

  rc = doltliteParseRecordStrict(pA, nA, &aInfo);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteParseRecordStrict(pB, nB, &bInfo);
  if( rc!=SQLITE_OK ) return rc;

  if( aInfo.nField != bInfo.nField ) return SQLITE_OK;

  for(i=0; i<aInfo.nField; i++){
    int stA = aInfo.aType[i];
    int stB = bInfo.aType[i];
    int szA;
    int szB;

    if( stA != stB ) return SQLITE_OK;
    szA = diffSerialTypeSize((u64)stA);
    szB = diffSerialTypeSize((u64)stB);
    if( szA != szB ) return SQLITE_OK;
    if( szA>0 && memcmp(pA + aInfo.aOffset[i], pB + bInfo.aOffset[i], szA)!=0 ){
      return SQLITE_OK;
    }
  }
  *pEqual = 1;
  return SQLITE_OK;
}

/* Compare two record values: fast memcmp path, then field-wise fallback
** for records with different varint encodings of the same logical data. */
int prollyValuesEqual(
  const u8 *pA, int nA,
  const u8 *pB, int nB,
  int *pEqual
){
  int rc;
  if( nA==nB ){
    if( nA==0 ){
      *pEqual = 1;
      return SQLITE_OK;
    }
    if( memcmp(pA, pB, nA)==0 ){
      if( nA < 2 ){
        *pEqual = 0;
        return SQLITE_CORRUPT;
      }
      rc = diffRecordsEqualFieldwise(pA, nA, pB, nB, pEqual);
      if( rc!=SQLITE_OK ) return rc;
      if( *pEqual ) return SQLITE_OK;
      return SQLITE_CORRUPT;
    }
  }
  if( nA < 2 || nB < 2 ){
    *pEqual = 0;
    return SQLITE_CORRUPT;
  }
  return diffRecordsEqualFieldwise(pA, nA, pB, nB, pEqual);
}

static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  int equal = 0;
  int rc;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  rc = prollyValuesEqual(pOldVal, nOldVal, pNewVal, nNewVal, &equal);
  if( rc!=SQLITE_OK ) return rc;
  return equal ? SQLITE_OK : SQLITE_DONE;
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
      rc = diffValuesEqual(pCurOld, pCurNew);
      if( rc==SQLITE_DONE ){
        rc = diffEmitModify(pCurOld, pCurNew, flags, xCb, pCtx);
      }else if( rc!=SQLITE_OK ){
        break;
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
  ProllyCursor *pCur;
  int rc, empty = 0;
  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;
  pCur = sqlite3_malloc(sizeof(ProllyCursor));
  if( !pCur ) return SQLITE_NOMEM;
  prollyCursorInit(pCur, pStore, pCache, pRoot, flags);
  rc = prollyCursorFirst(pCur, &empty);
  if( rc!=SQLITE_OK || empty ){ prollyCursorClose(pCur); sqlite3_free(pCur); return rc; }
  while( prollyCursorIsValid(pCur) ){
    if( changeType==PROLLY_DIFF_ADD ){
      rc = diffEmitAdd(pCur, flags, xCb, pCtx);
    }else{
      rc = diffEmitDelete(pCur, flags, xCb, pCtx);
    }
    if( rc!=SQLITE_OK ) break;
    rc = prollyCursorNext(pCur);
    if( rc!=SQLITE_OK ) break;
  }
  prollyCursorClose(pCur);
  sqlite3_free(pCur);
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
      int eqRc = SQLITE_OK;
      prollyNodeValue(pOld, i, &pOV, &nOV);
      prollyNodeValue(pNew, j, &pNV, &nNV);
      eq = (nOV==nNV && (nOV==0 || memcmp(pOV, pNV, nOV)==0));
      if( !eq && nOV>=2 && nNV>=2 ){
        eqRc = diffRecordsEqualFieldwise(pOV, nOV, pNV, nNV, &eq);
      }
      if( eqRc!=SQLITE_OK ) return eqRc;
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

/*
** Streaming diff iterator implementation.
** Uses the same cursor merge-walk as diffCursorWalk / diffMergeWalk,
** but yields one change at a time instead of calling a callback.
*/

static void diffIterFreeValCopies(ProllyDiffIter *pIter){
  sqlite3_free(pIter->pOldValCopy);
  pIter->pOldValCopy = 0;
  pIter->nOldValCopy = 0;
  sqlite3_free(pIter->pNewValCopy);
  pIter->pNewValCopy = 0;
  pIter->nNewValCopy = 0;
}

int prollyDiffIterOpen(
  ProllyDiffIter *pIter,
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 flags
){
  int rc = SQLITE_OK;
  int emptyOld = 0, emptyNew = 0;

  memset(pIter, 0, sizeof(*pIter));
  pIter->pStore = pStore;
  pIter->pCache = pCache;
  pIter->flags = flags;

  pIter->pCurOld = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  pIter->pCurNew = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pIter->pCurOld || !pIter->pCurNew ){
    sqlite3_free(pIter->pCurOld);
    sqlite3_free(pIter->pCurNew);
    pIter->pCurOld = 0;
    pIter->pCurNew = 0;
    pIter->eof = 1;
    pIter->rc = SQLITE_NOMEM;
    return SQLITE_NOMEM;
  }

  prollyCursorInit(pIter->pCurOld, pStore, pCache, pOldRoot, flags);
  prollyCursorInit(pIter->pCurNew, pStore, pCache, pNewRoot, flags);

  rc = prollyCursorFirst(pIter->pCurOld, &emptyOld);
  if( rc==SQLITE_OK ) rc = prollyCursorFirst(pIter->pCurNew, &emptyNew);

  if( rc!=SQLITE_OK ){
    pIter->eof = 1;
    pIter->rc = rc;
    return rc;
  }

  /* If both cursors are empty, we are done immediately. */
  if( !prollyCursorIsValid(pIter->pCurOld) &&
      !prollyCursorIsValid(pIter->pCurNew) ){
    pIter->eof = 1;
  }

  return SQLITE_OK;
}

/*
** Advance the iterator to produce the next diff change.
** On success, *ppChange points to the change (valid until the next
** Step or Close call). Returns SQLITE_ROW when a change is available,
** SQLITE_DONE when no more changes, or an error code.
*/
int prollyDiffIterStep(ProllyDiffIter *pIter, ProllyDiffChange **ppChange){
  ProllyCursor *pOld;
  ProllyCursor *pNew;
  ProllyDiffChange *pCh;
  int validOld, validNew;

  *ppChange = 0;

  if( pIter->eof ) return SQLITE_DONE;
  if( pIter->rc!=SQLITE_OK ) return pIter->rc;

  /* Free previous value copies */
  diffIterFreeValCopies(pIter);

  pOld = pIter->pCurOld;
  pNew = pIter->pCurNew;
  pCh = &pIter->current;

  /* Loop to skip equal entries without recursion */
  for(;;){
    validOld = prollyCursorIsValid(pOld);
    validNew = prollyCursorIsValid(pNew);

    if( !validOld && !validNew ){
      pIter->eof = 1;
      return SQLITE_DONE;
    }

    memset(pCh, 0, sizeof(*pCh));

    if( validOld && validNew ){
      int cmp;
      diffCompareKeys(pOld, pNew, pIter->flags, &cmp);

      if( cmp < 0 ){
        /* Key only in old tree: DELETE */
        const u8 *pVal; int nVal;
        pCh->type = PROLLY_DIFF_DELETE;
        diffFillKey(pCh, pOld, pIter->flags);
        prollyCursorValue(pOld, &pVal, &nVal);
        if( nVal > 0 ){
          pIter->pOldValCopy = sqlite3_malloc(nVal);
          if( !pIter->pOldValCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
          memcpy(pIter->pOldValCopy, pVal, nVal);
          pIter->nOldValCopy = nVal;
        }
        pCh->pOldVal = pIter->pOldValCopy;
        pCh->nOldVal = pIter->nOldValCopy;
        pIter->rc = prollyCursorNext(pOld);
        break; /* Found a change */
      }else if( cmp > 0 ){
        /* Key only in new tree: ADD */
        const u8 *pVal; int nVal;
        pCh->type = PROLLY_DIFF_ADD;
        diffFillKey(pCh, pNew, pIter->flags);
        prollyCursorValue(pNew, &pVal, &nVal);
        if( nVal > 0 ){
          pIter->pNewValCopy = sqlite3_malloc(nVal);
          if( !pIter->pNewValCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
          memcpy(pIter->pNewValCopy, pVal, nVal);
          pIter->nNewValCopy = nVal;
        }
        pCh->pNewVal = pIter->pNewValCopy;
        pCh->nNewVal = pIter->nNewValCopy;
        pIter->rc = prollyCursorNext(pNew);
        break; /* Found a change */
      }else{
        /* Same key in both trees: check if values differ */
        pIter->rc = diffValuesEqual(pOld, pNew);
        if( pIter->rc==SQLITE_OK ){
          /* Values equal — advance both cursors and loop */
          pIter->rc = prollyCursorNext(pOld);
          if( pIter->rc==SQLITE_OK ) pIter->rc = prollyCursorNext(pNew);
          if( pIter->rc!=SQLITE_OK ) return pIter->rc;
          continue; /* Skip to next pair */
        }else if( pIter->rc==SQLITE_DONE ){
          pIter->rc = SQLITE_OK;
          const u8 *pOV; int nOV;
          const u8 *pNV; int nNV;
          pCh->type = PROLLY_DIFF_MODIFY;
          diffFillKey(pCh, pNew, pIter->flags);
          prollyCursorValue(pOld, &pOV, &nOV);
          prollyCursorValue(pNew, &pNV, &nNV);
          if( nOV > 0 ){
            pIter->pOldValCopy = sqlite3_malloc(nOV);
            if( !pIter->pOldValCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
            memcpy(pIter->pOldValCopy, pOV, nOV);
            pIter->nOldValCopy = nOV;
          }
          if( nNV > 0 ){
            pIter->pNewValCopy = sqlite3_malloc(nNV);
            if( !pIter->pNewValCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
            memcpy(pIter->pNewValCopy, pNV, nNV);
            pIter->nNewValCopy = nNV;
          }
          pCh->pOldVal = pIter->pOldValCopy;
          pCh->nOldVal = pIter->nOldValCopy;
          pCh->pNewVal = pIter->pNewValCopy;
          pCh->nNewVal = pIter->nNewValCopy;
          pIter->rc = prollyCursorNext(pOld);
          if( pIter->rc==SQLITE_OK ) pIter->rc = prollyCursorNext(pNew);
          break; /* Found a change */
        }else{
          return pIter->rc;
        }
      }
    }else if( validOld ){
      /* Only old cursor valid: DELETE */
      const u8 *pVal; int nVal;
      pCh->type = PROLLY_DIFF_DELETE;
      diffFillKey(pCh, pOld, pIter->flags);
      prollyCursorValue(pOld, &pVal, &nVal);
      if( nVal > 0 ){
        pIter->pOldValCopy = sqlite3_malloc(nVal);
        if( !pIter->pOldValCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
        memcpy(pIter->pOldValCopy, pVal, nVal);
        pIter->nOldValCopy = nVal;
      }
      pCh->pOldVal = pIter->pOldValCopy;
      pCh->nOldVal = pIter->nOldValCopy;
      pIter->rc = prollyCursorNext(pOld);
      break; /* Found a change */
    }else{
      /* Only new cursor valid: ADD */
      const u8 *pVal; int nVal;
      pCh->type = PROLLY_DIFF_ADD;
      diffFillKey(pCh, pNew, pIter->flags);
      prollyCursorValue(pNew, &pVal, &nVal);
      if( nVal > 0 ){
        pIter->pNewValCopy = sqlite3_malloc(nVal);
        if( !pIter->pNewValCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
        memcpy(pIter->pNewValCopy, pVal, nVal);
        pIter->nNewValCopy = nVal;
      }
      pCh->pNewVal = pIter->pNewValCopy;
      pCh->nNewVal = pIter->nNewValCopy;
      pIter->rc = prollyCursorNext(pNew);
      break; /* Found a change */
    }
  }

  if( pIter->rc!=SQLITE_OK ){
    return pIter->rc;
  }

  *ppChange = pCh;
  return SQLITE_ROW;
}

void prollyDiffIterClose(ProllyDiffIter *pIter){
  if( pIter->pCurOld ){
    prollyCursorClose(pIter->pCurOld);
    sqlite3_free(pIter->pCurOld);
    pIter->pCurOld = 0;
  }
  if( pIter->pCurNew ){
    prollyCursorClose(pIter->pCurNew);
    sqlite3_free(pIter->pCurNew);
    pIter->pCurNew = 0;
  }
  diffIterFreeValCopies(pIter);
  pIter->eof = 1;
}

#endif
