
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"
#include "prolly_record.h"

#include <string.h>

int prollyFetchNode(ChunkStore *pStore, const ProllyHash *pHash,
                    ProllyNode *pNode, u8 **ppData){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  rc = chunkStoreGet(pStore, pHash, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParse(pNode, pData, nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pData);
    return rc;
  }
  *ppData = pData;
  return SQLITE_OK;
}

static void diffCompareKeys(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  int *pCmp
){
  const u8 *pKeyOld; int nKeyOld;
  const u8 *pKeyNew; int nKeyNew;
  (void)flags;
  prollyCursorKey(pOld, &pKeyOld, &nKeyOld);
  prollyCursorKey(pNew, &pKeyNew, &nKeyNew);
  *pCmp = prollyKeyCmp(pKeyOld, nKeyOld, pKeyNew, nKeyNew);
}

static void diffFillKey(
  ProllyDiffChange *pChange,
  ProllyCursor *pCur,
  u8 flags
){
  const u8 *pKey; int nKey;
  prollyCursorKey(pCur, &pKey, &nKey);
  pChange->pKey   = pKey;
  pChange->nKey   = nKey;
  pChange->keyIsIntKey = (flags & PROLLY_NODE_INTKEY)!=0;
  pChange->intKey = pChange->keyIsIntKey ? prollyCursorIntKey(pCur) : 0;
}

static int diffIterCopyKey(
  ProllyDiffIter *pIter,
  ProllyDiffChange *pChange,
  ProllyCursor *pCur,
  u8 flags
){
  const u8 *pKey;
  int nKey;
  prollyCursorKey(pCur, &pKey, &nKey);
  if( nKey>0 ){
    pIter->pKeyCopy = sqlite3_malloc(nKey);
    if( !pIter->pKeyCopy ) return SQLITE_NOMEM;
    memcpy(pIter->pKeyCopy, pKey, nKey);
    pIter->nKeyCopy = nKey;
  }
  pChange->pKey = pIter->pKeyCopy;
  pChange->nKey = pIter->nKeyCopy;
  pChange->keyIsIntKey = (flags & PROLLY_NODE_INTKEY)!=0;
  pChange->intKey = pChange->keyIsIntKey ? prollyCursorIntKey(pCur) : 0;
  return SQLITE_OK;
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

    if( stA != stB ){
      if( dlSerialIsInt(stA) && dlSerialIsInt(stB) ){
        i64 vA = dlDecodeSerialInt(stA, pA + aInfo.aOffset[i],
                                nA - aInfo.aOffset[i]);
        i64 vB = dlDecodeSerialInt(stB, pB + bInfo.aOffset[i],
                                nB - bInfo.aOffset[i]);
        if( vA != vB ) return SQLITE_OK;
        continue;
      }
      return SQLITE_OK;
    }
    szA = dlSerialTypeLen((u64)stA);
    szB = dlSerialTypeLen((u64)stB);
    if( szA != szB ) return SQLITE_OK;
    if( szA>0 && memcmp(pA + aInfo.aOffset[i], pB + bInfo.aOffset[i], szA)!=0 ){
      return SQLITE_OK;
    }
  }
  *pEqual = 1;
  return SQLITE_OK;
}

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
      rc = diffRecordsEqualFieldwise(pA, nA, pB, nB, pEqual);
      if( rc!=SQLITE_OK ) return rc;
      if( *pEqual ) return SQLITE_OK;
      return SQLITE_CORRUPT;
    }
  }
  return diffRecordsEqualFieldwise(pA, nA, pB, nB, pEqual);
}

static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew,
                           int *pEqual){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  return prollyValuesEqual(pOldVal, nOldVal, pNewVal, nNewVal, pEqual);
}

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
      int equal = 0;
      rc = diffValuesEqual(pCurOld, pCurNew, &equal);
      if( rc!=SQLITE_OK ) break;
      if( !equal ){
        rc = diffEmitModify(pCurOld, pCurNew, flags, xCb, pCtx);
        if( rc!=SQLITE_OK ) break;
      }
      rc = prollyCursorNext(pCurOld);
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
  const u8 *pKA; int nKA;
  const u8 *pKB; int nKB;
  (void)flags;
  prollyNodeKey(pA, iA, &pKA, &nKA);
  prollyNodeKey(pB, iB, &pKB, &nKB);
  return prollyKeyCmp(pKA, nKA, pKB, nKB);
}

static void diffEmitKey(ProllyDiffChange *ch, const ProllyNode *pN, int i, u8 flags){
  prollyNodeKey(pN, i, &ch->pKey, &ch->nKey);
  if( flags & PROLLY_NODE_INTKEY ){
    ch->intKey = prollyNodeIntKey(pN, i);
  }else{
    ch->intKey = 0;
  }
}

static int diffLeaves(
  const ProllyNode *pOld, const ProllyNode *pNew, u8 flags,
  ProllyDiffCallback xCb, void *pCtx
){
  int i = 0, j = 0, rc = SQLITE_OK;

  while( i < (int)pOld->nItems && j < (int)pNew->nItems ){
    int cmp;
    {
      const u8 *pKA; int nKA; const u8 *pKB; int nKB;
      prollyNodeKey(pOld, i, &pKA, &nKA);
      prollyNodeKey(pNew, j, &pKB, &nKB);
      cmp = prollyKeyCmp(pKA, nKA, pKB, nKB);
    }

    if( cmp < 0 ){
      ProllyDiffChange ch; const u8 *pV; int nV;
      memset(&ch, 0, sizeof(ch));
      ch.type = PROLLY_DIFF_DELETE;
      diffEmitKey(&ch, pOld, i, flags);
      prollyNodeValue(pOld, i, &pV, &nV);
      ch.pOldVal = pV; ch.nOldVal = nV;
      rc = xCb(pCtx, &ch); i++;
    }else if( cmp > 0 ){
      ProllyDiffChange ch; const u8 *pV; int nV;
      memset(&ch, 0, sizeof(ch));
      ch.type = PROLLY_DIFF_ADD;
      diffEmitKey(&ch, pNew, j, flags);
      prollyNodeValue(pNew, j, &pV, &nV);
      ch.pNewVal = pV; ch.nNewVal = nV;
      rc = xCb(pCtx, &ch); j++;
    }else{
      const u8 *pOV; int nOV; const u8 *pNV; int nNV;
      int eq;
      int eqRc = SQLITE_OK;
      prollyNodeValue(pOld, i, &pOV, &nOV);
      prollyNodeValue(pNew, j, &pNV, &nNV);
      eqRc = prollyValuesEqual(pOV, nOV, pNV, nNV, &eq);
      if( eqRc!=SQLITE_OK ) return eqRc;
      if( !eq ){
        ProllyDiffChange ch;
        memset(&ch, 0, sizeof(ch));
        ch.type = PROLLY_DIFF_MODIFY;
        diffEmitKey(&ch, pNew, j, flags);
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
    diffEmitKey(&ch, pOld, i, flags);
    prollyNodeValue(pOld, i, &pV, &nV);
    ch.pOldVal = pV; ch.nOldVal = nV;
    rc = xCb(pCtx, &ch); i++;
  }
  while( j < (int)pNew->nItems && rc==SQLITE_OK ){
    ProllyDiffChange ch; const u8 *pV; int nV;
    memset(&ch, 0, sizeof(ch));
    ch.type = PROLLY_DIFF_ADD;
    diffEmitKey(&ch, pNew, j, flags);
    prollyNodeValue(pNew, j, &pV, &nV);
    ch.pNewVal = pV; ch.nNewVal = nV;
    rc = xCb(pCtx, &ch); j++;
  }
  return rc;
}

static int diffNodesOneLevel(
  ChunkStore *pStore, ProllyCache *pCache,
  const ProllyHash *pOldHash, const ProllyHash *pNewHash,
  const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
  u8 flags, ProllyDiffCallback xCb, void *pCtx,
  ProllyHash **ppStack, int *pnStack, int *pnStackAlloc
){
  u8 *oldData = 0, *newData = 0;
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

  rc = prollyFetchNode(pStore, pOldHash, &oldNode, &oldData);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyFetchNode(pStore, pNewHash, &newNode, &newData);
  if( rc!=SQLITE_OK ){ sqlite3_free(oldData); return rc; }

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

        if( *pnStack + 2 > *pnStackAlloc ){
          i64 nNew = *pnStackAlloc ? (i64)*pnStackAlloc * 2 : (i64)32;
          ProllyHash *pNew;
          if( nNew > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ){
            rc = SQLITE_NOMEM;
            break;
          }
          pNew = sqlite3_realloc(*ppStack,
                                 (int)(nNew * (i64)sizeof(ProllyHash)));
          if( !pNew ){ rc = SQLITE_NOMEM; break; }
          *ppStack = pNew;
          *pnStackAlloc = (int)nNew;
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
            /* Walk this subtree only; the driver stack owns siblings. */
            prollyCursorInit(pCO, pStore, pCache, pOldHash, flags);
            prollyCursorInit(pCN, pStore, pCache, pNewHash, flags);

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

    rc = diffCursorWalk(pStore, pCache, pOldHash, pNewHash, flags, xCb, pCtx);
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

  aStack = sqlite3_malloc(32 * (int)sizeof(ProllyHash));
  if( !aStack ) return SQLITE_NOMEM;
  nStackAlloc = 32;
  aStack[nStack++] = *pOldRoot;
  aStack[nStack++] = *pNewRoot;

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

static void diffIterFreeCopies(ProllyDiffIter *pIter){
  sqlite3_free(pIter->pKeyCopy);
  pIter->pKeyCopy = 0;
  pIter->nKeyCopy = 0;
  sqlite3_free(pIter->pOldValCopy);
  pIter->pOldValCopy = 0;
  pIter->nOldValCopy = 0;
  sqlite3_free(pIter->pNewValCopy);
  pIter->pNewValCopy = 0;
  pIter->nNewValCopy = 0;
}

/* Copy a cursor value into the iterator-owned buffer and point the change at
** it. On OOM sets pIter->rc and returns SQLITE_NOMEM; nSrc==0 leaves an empty
** (null) value. */
static int diffSetChangeVal(
  ProllyDiffIter *pIter,
  u8 **ppCopy, int *pnCopy,
  const u8 **ppOut, int *pnOut,
  const u8 *pSrc, int nSrc
){
  if( nSrc > 0 ){
    *ppCopy = sqlite3_malloc(nSrc);
    if( !*ppCopy ){ pIter->rc = SQLITE_NOMEM; return SQLITE_NOMEM; }
    memcpy(*ppCopy, pSrc, nSrc);
    *pnCopy = nSrc;
  }
  *ppOut = *ppCopy;
  *pnOut = *pnCopy;
  return SQLITE_OK;
}

int prollyDiffIterOpen(
  ProllyDiffIter *pIter,
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 oldFlags,
  u8 newFlags
){
  int rc = SQLITE_OK;
  int emptyOld = 0, emptyNew = 0;

  memset(pIter, 0, sizeof(*pIter));
  pIter->pStore = pStore;
  pIter->pCache = pCache;
  pIter->flags = oldFlags;
  pIter->oldFlags = oldFlags;
  pIter->newFlags = newFlags;
  pIter->shapeMismatch =
      ((oldFlags ^ newFlags) & PROLLY_NODE_INTKEY)!=0;

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

  prollyCursorInit(pIter->pCurOld, pStore, pCache, pOldRoot, oldFlags);
  prollyCursorInit(pIter->pCurNew, pStore, pCache, pNewRoot, newFlags);

  rc = prollyCursorFirst(pIter->pCurOld, &emptyOld);
  if( rc==SQLITE_OK ) rc = prollyCursorFirst(pIter->pCurNew, &emptyNew);

  if( rc!=SQLITE_OK ){
    pIter->eof = 1;
    pIter->rc = rc;
    return rc;
  }

  if( !prollyCursorIsValid(pIter->pCurOld) &&
      !prollyCursorIsValid(pIter->pCurNew) ){
    pIter->eof = 1;
  }

  return SQLITE_OK;
}

int prollyDiffIterStep(ProllyDiffIter *pIter, ProllyDiffChange **ppChange){
  ProllyCursor *pOld;
  ProllyCursor *pNew;
  ProllyDiffChange *pCh;
  int validOld, validNew;

  *ppChange = 0;

  if( pIter->eof ) return SQLITE_DONE;
  if( pIter->rc!=SQLITE_OK ) return pIter->rc;

  diffIterFreeCopies(pIter);

  pOld = pIter->pCurOld;
  pNew = pIter->pCurNew;
  pCh = &pIter->current;

  for(;;){
    validOld = prollyCursorIsValid(pOld);
    validNew = prollyCursorIsValid(pNew);

    if( !validOld && !validNew ){
      pIter->eof = 1;
      return SQLITE_DONE;
    }

    memset(pCh, 0, sizeof(*pCh));

    if( validOld && validNew && !pIter->shapeMismatch ){
      int cmp;
      diffCompareKeys(pOld, pNew, pIter->flags, &cmp);

      if( cmp < 0 ){

        const u8 *pVal; int nVal;
        pCh->type = PROLLY_DIFF_DELETE;
        pIter->rc = diffIterCopyKey(pIter, pCh, pOld, pIter->oldFlags);
        if( pIter->rc!=SQLITE_OK ) return pIter->rc;
        prollyCursorValue(pOld, &pVal, &nVal);
        if( diffSetChangeVal(pIter, &pIter->pOldValCopy, &pIter->nOldValCopy,
                             &pCh->pOldVal, &pCh->nOldVal, pVal, nVal) ) return SQLITE_NOMEM;
        pIter->rc = prollyCursorNext(pOld);
        break;
      }else if( cmp > 0 ){

        const u8 *pVal; int nVal;
        pCh->type = PROLLY_DIFF_ADD;
        pIter->rc = diffIterCopyKey(pIter, pCh, pNew, pIter->newFlags);
        if( pIter->rc!=SQLITE_OK ) return pIter->rc;
        prollyCursorValue(pNew, &pVal, &nVal);
        if( diffSetChangeVal(pIter, &pIter->pNewValCopy, &pIter->nNewValCopy,
                             &pCh->pNewVal, &pCh->nNewVal, pVal, nVal) ) return SQLITE_NOMEM;
        pIter->rc = prollyCursorNext(pNew);
        break;
      }else{
        int equal = 0;
        pIter->rc = diffValuesEqual(pOld, pNew, &equal);
        if( pIter->rc!=SQLITE_OK ) return pIter->rc;
        if( equal ){
          pIter->rc = prollyCursorNext(pOld);
          if( pIter->rc==SQLITE_OK ) pIter->rc = prollyCursorNext(pNew);
          if( pIter->rc!=SQLITE_OK ) return pIter->rc;
          continue;
        }else{
          const u8 *pOV; int nOV;
          const u8 *pNV; int nNV;
          pCh->type = PROLLY_DIFF_MODIFY;
          pIter->rc = diffIterCopyKey(pIter, pCh, pNew, pIter->newFlags);
          if( pIter->rc!=SQLITE_OK ) return pIter->rc;
          prollyCursorValue(pOld, &pOV, &nOV);
          prollyCursorValue(pNew, &pNV, &nNV);
          if( diffSetChangeVal(pIter, &pIter->pOldValCopy, &pIter->nOldValCopy,
                               &pCh->pOldVal, &pCh->nOldVal, pOV, nOV) ) return SQLITE_NOMEM;
          if( diffSetChangeVal(pIter, &pIter->pNewValCopy, &pIter->nNewValCopy,
                               &pCh->pNewVal, &pCh->nNewVal, pNV, nNV) ) return SQLITE_NOMEM;
          pIter->rc = prollyCursorNext(pOld);
          if( pIter->rc==SQLITE_OK ) pIter->rc = prollyCursorNext(pNew);
          break;
        }
      }
    }else if( validOld ){

      const u8 *pVal; int nVal;
      pCh->type = PROLLY_DIFF_DELETE;
      pIter->rc = diffIterCopyKey(pIter, pCh, pOld, pIter->oldFlags);
      if( pIter->rc!=SQLITE_OK ) return pIter->rc;
      prollyCursorValue(pOld, &pVal, &nVal);
      if( diffSetChangeVal(pIter, &pIter->pOldValCopy, &pIter->nOldValCopy,
                           &pCh->pOldVal, &pCh->nOldVal, pVal, nVal) ) return SQLITE_NOMEM;
      pIter->rc = prollyCursorNext(pOld);
      break;
    }else{

      const u8 *pVal; int nVal;
      pCh->type = PROLLY_DIFF_ADD;
      pIter->rc = diffIterCopyKey(pIter, pCh, pNew, pIter->newFlags);
      if( pIter->rc!=SQLITE_OK ) return pIter->rc;
      prollyCursorValue(pNew, &pVal, &nVal);
      if( diffSetChangeVal(pIter, &pIter->pNewValCopy, &pIter->nNewValCopy,
                           &pCh->pNewVal, &pCh->nNewVal, pVal, nVal) ) return SQLITE_NOMEM;
      pIter->rc = prollyCursorNext(pNew);
      break;
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
  diffIterFreeCopies(pIter);
  pIter->eof = 1;
}

#endif
