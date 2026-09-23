
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"
#include "prolly_record.h"

#include <string.h>

#define DIFF_PREFETCH_MAX_PAIRS 256
#define DIFF_PREFETCH_BATCH_HASHES 256

typedef struct DiffPrefetchPair DiffPrefetchPair;
struct DiffPrefetchPair {
  ProllyHash oldHash;
  ProllyHash newHash;
};

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

static int diffRecordsEqualFieldwise(
  const u8 *pA, int nA,
  const u8 *pB, int nB,
  int *pEqual,
  int *pnFieldA,
  int *pnFieldB
){
  DoltliteRecordInfo aInfo = {0};
  DoltliteRecordInfo bInfo = {0};
  int nField;
  int i;
  int rc;

  doltliteRecordInfoInit(&aInfo);
  doltliteRecordInfoInit(&bInfo);
  *pEqual = 0;
  if( pnFieldA ) *pnFieldA = 0;
  if( pnFieldB ) *pnFieldB = 0;
  if( nA < 1 || nB < 1 ){
    DoltliteRecordInfo *pInfo;
    const u8 *pRec;
    int nRec;
    if( nA < 1 && nB < 1 ){
      *pEqual = 1;
      return SQLITE_OK;
    }
    pRec = nA < 1 ? pB : pA;
    nRec = nA < 1 ? nB : nA;
    pInfo = nA < 1 ? &bInfo : &aInfo;
    rc = doltliteParseRecordStrict(pRec, nRec, pInfo);
    if( rc!=SQLITE_OK ) return rc;
    if( nA < 1 ){
      if( pnFieldB ) *pnFieldB = pInfo->nField;
    }else if( pnFieldA ){
      *pnFieldA = pInfo->nField;
    }
    for(i=0; i<pInfo->nField; i++){
      if( pInfo->aType[i]!=0 ) goto diff_eq_done;
    }
    *pEqual = 1;
    goto diff_eq_done;
  }

  rc = doltliteParseRecordStrict(pA, nA, &aInfo);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteParseRecordStrict(pB, nB, &bInfo);
  if( rc!=SQLITE_OK ){
    doltliteRecordInfoClear(&aInfo);
    return rc;
  }
  if( pnFieldA ) *pnFieldA = aInfo.nField;
  if( pnFieldB ) *pnFieldB = bInfo.nField;

  nField = aInfo.nField > bInfo.nField ? aInfo.nField : bInfo.nField;
  for(i=0; i<nField; i++){
    int stA = i<aInfo.nField ? aInfo.aType[i] : 0;
    int stB = i<bInfo.nField ? bInfo.aType[i] : 0;
    int szA;
    int szB;

    if( stA != stB ){
      if( dlSerialIsInt(stA) && dlSerialIsInt(stB) ){
        i64 vA = dlDecodeSerialInt(stA, pA + aInfo.aOffset[i],
                                   nA - aInfo.aOffset[i]);
        i64 vB = dlDecodeSerialInt(stB, pB + bInfo.aOffset[i],
                                   nB - bInfo.aOffset[i]);
        if( vA != vB ) goto diff_eq_done;
        continue;
      }
      goto diff_eq_done;
    }
    szA = dlSerialTypeLen((u64)stA);
    szB = dlSerialTypeLen((u64)stB);
    if( szA != szB ) goto diff_eq_done;
    if( szA>0 && memcmp(pA + aInfo.aOffset[i], pB + bInfo.aOffset[i], szA)!=0 ){
      goto diff_eq_done;
    }
  }
  *pEqual = 1;
diff_eq_done:
  doltliteRecordInfoClear(&aInfo);
  doltliteRecordInfoClear(&bInfo);
  return SQLITE_OK;
}

/* True when the stored record may stand in for the new one, so an update that
** changes nothing can keep the old bytes and leave the table hash alone.
** Semantic equality is not enough: a record that drops trailing fields compares
** equal to the longer one it replaces, and keeping those bytes would leave the
** dropped column in storage for the next ADD COLUMN to read back. */
int prollyValuesInterchangeable(
  const u8 *pOld, int nOld,
  const u8 *pNew, int nNew,
  int *pOk
){
  int equal = 0;
  int nFieldOld = 0;
  int nFieldNew = 0;
  int rc;

  *pOk = 0;
  rc = diffRecordsEqualFieldwise(pOld, nOld, pNew, nNew, &equal,
                                 &nFieldOld, &nFieldNew);
  if( rc!=SQLITE_OK ) return rc;
  *pOk = equal && nFieldOld<=nFieldNew;
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
      rc = diffRecordsEqualFieldwise(pA, nA, pB, nB, pEqual, 0, 0);
      if( rc!=SQLITE_OK ) return rc;
      if( *pEqual ) return SQLITE_OK;
      return SQLITE_CORRUPT;
    }
  }
  return diffRecordsEqualFieldwise(pA, nA, pB, nB, pEqual, 0, 0);
}

static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew,
                           int *pEqual){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  return prollyValuesEqual(pOldVal, nOldVal, pNewVal, nNewVal, pEqual);
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

/* Copy into the iterator-owned buffer; nSrc==0 is null. */
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

/* Open dual cursors over a differing range: whole subtrees, or a subtree
** tail past the boundary key a suspended internal walk stopped at. */
static int diffIterActivateRange(
  ProllyDiffIter *pIter,
  const ProllyHash *pOldHash,
  const ProllyHash *pNewHash,
  const ProllyNode *pSeekNode,
  int iSeekItem
){
  int rc = SQLITE_OK;
  int emO = 0, emN = 0;

  pIter->pCurOld = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  pIter->pCurNew = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pIter->pCurOld || !pIter->pCurNew ){
    sqlite3_free(pIter->pCurOld);
    sqlite3_free(pIter->pCurNew);
    pIter->pCurOld = 0;
    pIter->pCurNew = 0;
    return SQLITE_NOMEM;
  }
  prollyCursorInit(pIter->pCurOld, pIter->pStore, pIter->pCache,
                   pOldHash, pIter->oldFlags);
  prollyCursorInit(pIter->pCurNew, pIter->pStore, pIter->pCache,
                   pNewHash, pIter->newFlags);

  if( pSeekNode && (pIter->flags & PROLLY_NODE_INTKEY) ){
    i64 seekKey = prollyNodeIntKey(pSeekNode, iSeekItem);
    rc = prollyCursorSeekInt(pIter->pCurOld, seekKey, &emO);
    if( rc==SQLITE_OK && emO==0 ) rc = prollyCursorNext(pIter->pCurOld);
    if( rc==SQLITE_OK ){
      rc = prollyCursorSeekInt(pIter->pCurNew, seekKey, &emN);
      if( rc==SQLITE_OK && emN==0 ) rc = prollyCursorNext(pIter->pCurNew);
    }
  }else if( pSeekNode ){
    const u8 *pSK; int nSK;
    prollyNodeKey(pSeekNode, iSeekItem, &pSK, &nSK);
    rc = prollyCursorSeekBlob(pIter->pCurOld, pSK, nSK, &emO);
    if( rc==SQLITE_OK && emO==0 ) rc = prollyCursorNext(pIter->pCurOld);
    if( rc==SQLITE_OK ){
      rc = prollyCursorSeekBlob(pIter->pCurNew, pSK, nSK, &emN);
      if( rc==SQLITE_OK && emN==0 ) rc = prollyCursorNext(pIter->pCurNew);
    }
  }else{
    rc = prollyCursorFirst(pIter->pCurOld, &emO);
    if( rc==SQLITE_OK ) rc = prollyCursorFirst(pIter->pCurNew, &emN);
  }

  if( rc!=SQLITE_OK ){
    prollyCursorClose(pIter->pCurOld);
    prollyCursorClose(pIter->pCurNew);
    sqlite3_free(pIter->pCurOld);
    sqlite3_free(pIter->pCurNew);
    pIter->pCurOld = 0;
    pIter->pCurNew = 0;
    return rc;
  }
  pIter->cursorsActive = 1;
  return SQLITE_OK;
}

static void diffIterCloseCursors(ProllyDiffIter *pIter){
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
  pIter->cursorsActive = 0;
}

static void diffIterPopFrame(ProllyDiffIter *pIter){
  DiffIterFrame *pF;
  if( pIter->nFrames<=0 ) return;
  pF = &pIter->aFrames[--pIter->nFrames];
  sqlite3_free(pF->pOldData);
  sqlite3_free(pF->pNewData);
  memset(pF, 0, sizeof(*pF));
}

static int diffIterEnsurePrefetchBuf(ProllyDiffIter *pIter){
  if( pIter->pPrefetchPairs ) return SQLITE_OK;
  pIter->pPrefetchPairs = sqlite3_malloc64(
      (sqlite3_uint64)DIFF_PREFETCH_MAX_PAIRS * sizeof(DiffPrefetchPair)
    + (sqlite3_uint64)DIFF_PREFETCH_BATCH_HASHES * sizeof(ProllyHash));
  if( !pIter->pPrefetchPairs ) return SQLITE_NOMEM;
  pIter->pPrefetchHashes = (DiffPrefetchPair*)pIter->pPrefetchPairs
                         + DIFF_PREFETCH_MAX_PAIRS;
  return SQLITE_OK;
}

static int diffIterPrefetchPairs(
  ProllyDiffIter *pIter,
  const DiffPrefetchPair *aInitial,
  int nInitial
){
  DiffPrefetchPair *aPair;
  ProllyHash *aHash;
  int iLevel = 0;
  int nPair = nInitial;
  int rc = SQLITE_OK;

  if( !pIter->pStore->pChunkSource || nInitial==0 ) return SQLITE_OK;
  rc = diffIterEnsurePrefetchBuf(pIter);
  if( rc!=SQLITE_OK ) return rc;
  aPair = (DiffPrefetchPair*)pIter->pPrefetchPairs;
  aHash = (ProllyHash*)pIter->pPrefetchHashes;
  memcpy(aPair, aInitial, (size_t)nInitial * sizeof(DiffPrefetchPair));

  while( iLevel<nPair ){
    int iLevelEnd = nPair;
    int iBatch = iLevel;
    int k;

    while( iBatch<iLevelEnd ){
      int nBatchPair = iLevelEnd - iBatch;
      int nHash;
      if( nBatchPair>DIFF_PREFETCH_BATCH_HASHES/2 ){
        nBatchPair = DIFF_PREFETCH_BATCH_HASHES/2;
      }
      for(k=0; k<nBatchPair; k++){
        aHash[k*2] = aPair[iBatch+k].oldHash;
        aHash[k*2+1] = aPair[iBatch+k].newHash;
      }
      nHash = nBatchPair * 2;
      rc = chunkStoreSourcePrefetchMany(pIter->pStore, aHash, nHash);
      if( rc!=SQLITE_OK ) return rc;
      iBatch += nBatchPair;
    }

    if( pIter->shapeMismatch ) break;
    for(k=iLevel; k<iLevelEnd && nPair<DIFF_PREFETCH_MAX_PAIRS; k++){
      ProllyNode oldNode, newNode;
      u8 *pOldData = 0;
      u8 *pNewData = 0;
      int i = 0;
      int j = 0;

      rc = prollyFetchNode(pIter->pStore, &aPair[k].oldHash,
                           &oldNode, &pOldData);
      if( rc!=SQLITE_OK ) return rc;
      rc = prollyFetchNode(pIter->pStore, &aPair[k].newHash,
                           &newNode, &pNewData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pOldData);
        return rc;
      }
      if( oldNode.level>0 && newNode.level>0
       && oldNode.level==newNode.level ){
        while( i<(int)oldNode.nItems
            && j<(int)newNode.nItems
            && nPair<DIFF_PREFETCH_MAX_PAIRS ){
          DiffPrefetchPair *pPair = &aPair[nPair];
          prollyNodeChildHash(&oldNode, i, &pPair->oldHash);
          prollyNodeChildHash(&newNode, j, &pPair->newHash);
          if( prollyHashCompare(&pPair->oldHash, &pPair->newHash)==0 ){
            i++;
            j++;
            continue;
          }
          if( diffNodeKeyCmp(&oldNode, i, &newNode, j,
                             pIter->flags)!=0 ){
            break;
          }
          nPair++;
          i++;
          j++;
        }
      }
      sqlite3_free(pOldData);
      sqlite3_free(pNewData);
    }
    iLevel = iLevelEnd;
  }

  return SQLITE_OK;
}

static int diffIterPrefetchFrame(
  ProllyDiffIter *pIter,
  DiffIterFrame *pF
){
  DiffPrefetchPair *aPair;
  int i = pF->i;
  int j = pF->j;
  int nPair = 0;
  int rc;

  if( !pIter->pStore->pChunkSource ) return SQLITE_OK;
  if( i<pF->iPrefetched && j<pF->jPrefetched ) return SQLITE_OK;
  rc = diffIterEnsurePrefetchBuf(pIter);
  if( rc!=SQLITE_OK ) return rc;
  aPair = (DiffPrefetchPair*)pIter->pPrefetchPairs;

  while( i<(int)pF->oldNode.nItems
      && j<(int)pF->newNode.nItems
      && nPair<DIFF_PREFETCH_MAX_PAIRS ){
    DiffPrefetchPair *pPair = &aPair[nPair];
    prollyNodeChildHash(&pF->oldNode, i, &pPair->oldHash);
    prollyNodeChildHash(&pF->newNode, j, &pPair->newHash);
    if( prollyHashCompare(&pPair->oldHash, &pPair->newHash)==0 ){
      i++;
      j++;
      continue;
    }
    if( diffNodeKeyCmp(&pF->oldNode, i, &pF->newNode, j,
                       pIter->flags)!=0 ){
      break;
    }
    nPair++;
    i++;
    j++;
  }

  pF->iPrefetched = i;
  pF->jPrefetched = j;
  return diffIterPrefetchPairs(pIter, aPair, nPair);
}

/* Expand one differing subtree pair: identical hashes vanish, same-level
** internal pairs suspend as a frame, everything else (leaves, mixed
** levels, one empty side) becomes a cursor range. */
static int diffIterDescendPair(
  ProllyDiffIter *pIter,
  const ProllyHash *pOldHash,
  const ProllyHash *pNewHash
){
  ProllyNode oldNode, newNode;
  u8 *pOldData = 0, *pNewData = 0;
  int rc;

  if( prollyHashCompare(pOldHash, pNewHash)==0 ) return SQLITE_OK;
  if( prollyHashIsEmpty(pOldHash) && prollyHashIsEmpty(pNewHash) ){
    return SQLITE_OK;
  }
  if( prollyHashIsEmpty(pOldHash) || prollyHashIsEmpty(pNewHash) ){
    return diffIterActivateRange(pIter, pOldHash, pNewHash, 0, 0);
  }

  rc = prollyFetchNode(pIter->pStore, pOldHash, &oldNode, &pOldData);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyFetchNode(pIter->pStore, pNewHash, &newNode, &pNewData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pOldData);
    return rc;
  }

  if( oldNode.level>0 && newNode.level>0 && oldNode.level==newNode.level ){
    DiffIterFrame *pF;
    if( pIter->nFrames+1 > pIter->nFramesAlloc ){
      int nNew = pIter->nFramesAlloc ? pIter->nFramesAlloc*2 : 8;
      DiffIterFrame *aNew = sqlite3_realloc(pIter->aFrames,
          nNew * (int)sizeof(DiffIterFrame));
      if( !aNew ){
        sqlite3_free(pOldData);
        sqlite3_free(pNewData);
        return SQLITE_NOMEM;
      }
      pIter->aFrames = aNew;
      pIter->nFramesAlloc = nNew;
    }
    pF = &pIter->aFrames[pIter->nFrames++];
    memset(pF, 0, sizeof(*pF));
    pF->oldHash = *pOldHash;
    pF->newHash = *pNewHash;
    pF->oldNode = oldNode;
    pF->newNode = newNode;
    pF->pOldData = pOldData;
    pF->pNewData = pNewData;
    return SQLITE_OK;
  }

  sqlite3_free(pOldData);
  sqlite3_free(pNewData);
  return diffIterActivateRange(pIter, pOldHash, pNewHash, 0, 0);
}

/* Resume the top frame's child walk until it activates a cursor range,
** pushes a deeper frame, or exhausts (pop). */
static int diffIterAdvanceFrame(ProllyDiffIter *pIter){
  DiffIterFrame *pF = &pIter->aFrames[pIter->nFrames-1];
  int rc = SQLITE_OK;

  while( pF->i < (int)pF->oldNode.nItems
      && pF->j < (int)pF->newNode.nItems ){
    ProllyHash oldChild, newChild;
    int cmp;
    prollyNodeChildHash(&pF->oldNode, pF->i, &oldChild);
    prollyNodeChildHash(&pF->newNode, pF->j, &newChild);

    if( prollyHashCompare(&oldChild, &newChild)==0 ){
      pF->i++;
      pF->j++;
      continue;
    }

    cmp = diffNodeKeyCmp(&pF->oldNode, pF->i, &pF->newNode, pF->j,
                         pIter->flags);
    if( cmp==0 ){
      rc = diffIterPrefetchFrame(pIter, pF);
      if( rc!=SQLITE_OK ) return rc;
      pF->i++;
      pF->j++;
      return diffIterDescendPair(pIter, &oldChild, &newChild);
    }

    /* Chunk boundaries diverged; walk the rest of both subtrees as one
    ** range starting past the last shared boundary. */
    {
      int iSeek = pF->i - 1;
      const ProllyNode *pSeekNode = iSeek>=0 ? &pF->oldNode : 0;
      ProllyHash oldHash = pF->oldHash;
      ProllyHash newHash = pF->newHash;
      pF->i = (int)pF->oldNode.nItems;
      pF->j = (int)pF->newNode.nItems;
      rc = diffIterActivateRange(pIter, &oldHash, &newHash,
                                 pSeekNode, iSeek);
      return rc;
    }
  }

  if( pF->i < (int)pF->oldNode.nItems ){
    ProllyHash ch;
    ProllyHash empty;
    memset(&empty, 0, sizeof(empty));
    prollyNodeChildHash(&pF->oldNode, pF->i, &ch);
    pF->i++;
    return diffIterActivateRange(pIter, &ch, &empty, 0, 0);
  }
  if( pF->j < (int)pF->newNode.nItems ){
    ProllyHash ch;
    ProllyHash empty;
    memset(&empty, 0, sizeof(empty));
    prollyNodeChildHash(&pF->newNode, pF->j, &ch);
    pF->j++;
    return diffIterActivateRange(pIter, &empty, &ch, 0, 0);
  }

  diffIterPopFrame(pIter);
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

  memset(pIter, 0, sizeof(*pIter));
  pIter->pStore = pStore;
  pIter->pCache = pCache;
  pIter->flags = oldFlags;
  pIter->oldFlags = oldFlags;
  pIter->newFlags = newFlags;
  pIter->shapeMismatch =
      ((oldFlags ^ newFlags) & PROLLY_NODE_INTKEY)!=0;

  if( pStore->pChunkSource
   && prollyHashCompare(pOldRoot, pNewRoot)!=0 ){
    if( !prollyHashIsEmpty(pOldRoot) && !prollyHashIsEmpty(pNewRoot) ){
      DiffPrefetchPair rootPair;
      rootPair.oldHash = *pOldRoot;
      rootPair.newHash = *pNewRoot;
      rc = diffIterPrefetchPairs(pIter, &rootPair, 1);
    }else{
      ProllyHash rootHash = prollyHashIsEmpty(pOldRoot)
                          ? *pNewRoot : *pOldRoot;
      rc = chunkStoreSourcePrefetchMany(pStore, &rootHash, 1);
    }
    if( rc!=SQLITE_OK ){
      pIter->eof = 1;
      pIter->rc = rc;
      return rc;
    }
  }

  if( pIter->shapeMismatch ){
    rc = diffIterActivateRange(pIter, pOldRoot, pNewRoot, 0, 0);
  }else{
    rc = diffIterDescendPair(pIter, pOldRoot, pNewRoot);
  }
  if( rc!=SQLITE_OK ){
    pIter->eof = 1;
    pIter->rc = rc;
    return rc;
  }

  if( !pIter->cursorsActive && pIter->nFrames==0 ){
    pIter->eof = 1;
  }else if( pIter->cursorsActive
         && !prollyCursorIsValid(pIter->pCurOld)
         && !prollyCursorIsValid(pIter->pCurNew)
         && pIter->nFrames==0 ){
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

  pCh = &pIter->current;

  for(;;){
    if( !pIter->cursorsActive ){
      if( pIter->nFrames==0 ){
        pIter->eof = 1;
        return SQLITE_DONE;
      }
      pIter->rc = diffIterAdvanceFrame(pIter);
      if( pIter->rc!=SQLITE_OK ) return pIter->rc;
      continue;
    }

    pOld = pIter->pCurOld;
    pNew = pIter->pCurNew;
    validOld = prollyCursorIsValid(pOld);
    validNew = prollyCursorIsValid(pNew);

    if( !validOld && !validNew ){
      diffIterCloseCursors(pIter);
      continue;
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
  diffIterCloseCursors(pIter);
  while( pIter->nFrames>0 ){
    diffIterPopFrame(pIter);
  }
  sqlite3_free(pIter->aFrames);
  pIter->aFrames = 0;
  pIter->nFramesAlloc = 0;
  sqlite3_free(pIter->pPrefetchPairs);
  pIter->pPrefetchPairs = 0;
  pIter->pPrefetchHashes = 0;
  diffIterFreeCopies(pIter);
  pIter->eof = 1;
}

#endif
