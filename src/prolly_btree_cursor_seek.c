#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* Cursor seek / moveto paths and related key helpers. */

static int keyInfoHasUnsupportedCollation(
  const KeyInfo *pKeyInfo,
  int nField
){
  int i;
  if( !pKeyInfo ) return 0;
  if( nField<=0 || nField>pKeyInfo->nAllField ){
    nField = pKeyInfo->nAllField;
  }
  for(i=0; i<nField; i++){
    const CollSeq *pColl = pKeyInfo->aColl[i];
    if( !pColl || !pColl->zName ) continue;
    if( sqlite3StrICmp(pColl->zName, "BINARY")!=0
     && sqlite3StrICmp(pColl->zName, "NOCASE")!=0
     && sqlite3StrICmp(pColl->zName, "RTRIM")!=0 ){
      return 1;
    }
  }
  return 0;
}
int prollyBtCursorTableMoveto(
  BtCursor *pCur,
  i64 intKey,
  int bias,
  int *pRes
){
  int rc;
  struct TableEntry *pTE;
  int rootIsEmpty;
  int canUseAppendSeekFloor;
  (void)bias;

  assert( pCur->curIntKey );

  pCur->nSeek++;
  if( pCur->pBtree ) pCur->pBtree->nSeek++;
  clearMergeCursorState(pCur);
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  CLEAR_CACHED_COMPARE_KEY(pCur);

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE && pTE->pPending && pCur->pMutMap!=(ProllyMutMap*)pTE->pPending ){
    pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
  }
  canUseAppendSeekFloor = pCur->pBtree
    && pCur->pBtree->db
    && !pCur->pBtree->db->autoCommit
    && pCur->pBtree->db->pSavepoint==0;
  rootIsEmpty = prollyHashIsEmpty(&pCur->pCur.root);
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapEntry *pEntry = 0;
    if( rootIsEmpty
     && pCur->pMutMap->isIntKey
     && pCur->pMutMap->appendSorted
     && intKey > prollyMutMapEntryIntKey(
                  &pCur->pMutMap->aEntries[pCur->pMutMap->nEntries-1])
    ){
      goto table_moveto_deferred_miss;
    }
    if( canUseAppendSeekFloor
     && pTE
     && pTE->appendSeekFloorValid
     && prollyHashCompare(&pTE->root, &pTE->appendSeekRoot)==0
     && pCur->pMutMap->isIntKey
     && pCur->pMutMap->appendSorted
     && intKey >= pTE->appendSeekFloor
     && intKey > prollyMutMapEntryIntKey(
                  &pCur->pMutMap->aEntries[pCur->pMutMap->nEntries-1])
    ){
      goto table_moveto_deferred_miss;
    }
    rc = prollyMutMapFindRc(pCur->pMutMap, 0, 0, intKey, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( !pEntry ){
      pCur->mmExactMiss = 1;
      pCur->mmMissGeneration = pCur->pMutMap->generation;
      pCur->mmMissIntKey = intKey;
    }
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      if( pCur->isPinned ){
        return SQLITE_CONSTRAINT_PINNED;
      }
      *pRes = 0;
      setCursorToMutMapEntryPhys(pCur, (int)(pEntry - pCur->pMutMap->aEntries));
      pCur->deferredTreeSeek = 1;
      return SQLITE_OK;
    }

    /* intKey is delete-masked or absent from the mut map. Probe the tree
    ** for a live exact hit first so equality lookups stay on the fast
    ** path. */
    if( !pEntry ){
      refreshCursorRoot(pCur);
      if( !prollyHashIsEmpty(&pCur->pCur.root) ){
        int res = 0;
        rc = prollyCursorSeekInt(&pCur->pCur, intKey, &res);
        if( rc!=SQLITE_OK ) return rc;
        if( res==0 ){
          *pRes = 0;
          pCur->eState = CURSOR_VALID;
          pCur->curFlags |= BTCF_ValidNKey;
          pCur->cachedIntKey = intKey;
          CLEAR_CACHED_PAYLOAD(pCur);
          cacheCurrentTreePayloadIfIntKey(pCur);
          return SQLITE_OK;
        }
        if( res<0 && canUseAppendSeekFloor && pTE ){
          if( !pTE->appendSeekFloorValid
           || prollyHashCompare(&pTE->root, &pTE->appendSeekRoot)!=0
           || intKey < pTE->appendSeekFloor
          ){
            pTE->appendSeekFloor = intKey;
          }
          pTE->appendSeekRoot = pTE->root;
          pTE->appendSeekFloorValid = 1;
        }
      }
    }

    /* No live exact match. Report "not found" now but defer the merged
    ** repositioning (which materializes the mut-map order) until something
    ** actually consumes the cursor position: equality probes on write paths
    ** never do, while range seeks consume it immediately via Eof/Next/Prev.
    ** The raw tree position must not leak out of this path: it can sit on a
    ** delete-masked row, skip mut-map-only rows, or see an empty root when
    ** every live row is in the mut map.
    **
    ** The result must be -1, not +1: the VDBE treats res>0 as "the cursor
    ** sits on a row above the key" and reads it with no Eof check, which a
    ** deferred position cannot honor when nothing at or above the key
    ** survives. With res<0 every consumer either steps forward through the
    ** merge machinery or checks Eof before reading. */
table_moveto_deferred_miss:
    pCur->deferredMergedSeek = 1;
    pCur->mmActive = 1;
    pCur->cachedIntKey = intKey;
    pCur->curFlags &= ~BTCF_ValidNKey;
    pCur->eState = CURSOR_VALID;
    *pRes = -1;
    return SQLITE_OK;
  }

  if( rootIsEmpty ){
    refreshCursorRoot(pCur);
    rootIsEmpty = prollyHashIsEmpty(&pCur->pCur.root);
  }
  if( rootIsEmpty ){
    /* Empty tables return res<0 with an invalid cursor, matching stock. */
    *pRes = -1;
    pCur->eState = CURSOR_INVALID;
    return SQLITE_OK;
  }

  refreshCursorRoot(pCur);

  rc = prollyCursorSeekInt(&pCur->pCur, intKey, pRes);
  if( rc==SQLITE_OK ){
    if( *pRes==0 ){
      pCur->eState = CURSOR_VALID;
      pCur->curFlags |= BTCF_ValidNKey;
      pCur->cachedIntKey = intKey;
      CLEAR_CACHED_PAYLOAD(pCur);
      cacheCurrentTreePayloadIfIntKey(pCur);
    } else if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      if( *pRes<0 && canUseAppendSeekFloor ){
        if( pTE ){
          if( !pTE->appendSeekFloorValid
           || prollyHashCompare(&pTE->root, &pTE->appendSeekRoot)!=0
           || intKey < pTE->appendSeekFloor
          ){
            pTE->appendSeekFloor = intKey;
          }
          pTE->appendSeekRoot = pTE->root;
          pTE->appendSeekFloorValid = 1;
        }
      }
      pCur->eState = CURSOR_VALID;
      pCur->curFlags &= ~BTCF_ValidNKey;
      cacheCurrentTreePayloadIfIntKey(pCur);
    } else {
      pCur->eState = CURSOR_INVALID;
    }
  }
  return rc;
}
int sqlite3BtreeTableMoveto(
  BtCursor *pCur,
  i64 intKey,
  int bias,
  int *pRes
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xTableMoveto(pCur, intKey, bias, pRes);
}
u32 btreeSerialType(Mem *pMem, u32 *pLen){
  int flags = pMem->flags;
  if( flags & MEM_Null ){ *pLen = 0; return SERIAL_TYPE_NULL; }
  if( flags & MEM_Int ){
    i64 v = pMem->u.i;
    if( v==0 ){ *pLen = 0; return SERIAL_TYPE_ZERO; }
    if( v==1 ){ *pLen = 0; return SERIAL_TYPE_ONE; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return SERIAL_TYPE_INT8; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return SERIAL_TYPE_INT16; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return SERIAL_TYPE_INT24; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return SERIAL_TYPE_INT32; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return SERIAL_TYPE_INT48; }
    *pLen = 8; return SERIAL_TYPE_INT64;
  }
  if( flags & MEM_Real ){ *pLen = 8; return SERIAL_TYPE_FLOAT64; }
  if( flags & MEM_Str ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + SERIAL_TYPE_TEXT_BASE;
  }
  if( flags & MEM_Blob ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + SERIAL_TYPE_BLOB_BASE;
  }
  *pLen = 0; return SERIAL_TYPE_NULL;
}
static int findMatchingMutMapEntry(
  ProllyMutMap *pMap,
  KeyInfo *pKeyInfo,
  UnpackedRecord *pIdxKey,
  const u8 *pSortKey,
  int nSortKey,
  int bExactKey,
  ProllyMutMapEntry **ppMatch,
  int *pCmp,
  int *pEqSeen
){
  int rc = SQLITE_OK;
  int cmp = 0;
  ProllyMutMapEntry *pMatch = 0;
  u8 *pRecBuf = 0;
  int nRecBufAlloc = 0;
  int lo = 0, found = 0;

  *ppMatch = 0;
  *pCmp = 0;
  *pEqSeen = 0;
  if( !pMap || prollyMutMapIsEmpty(pMap) ){
    return SQLITE_OK;
  }

  if( bExactKey
   || (pKeyInfo && pIdxKey->nField >= pKeyInfo->nAllField) ){
    ProllyMutMapEntry *pEntry = 0;
    rc = prollyMutMapFindRc(pMap, pSortKey, nSortKey, 0, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      *ppMatch = pEntry;
      *pEqSeen = 1;
    }
    return SQLITE_OK;
  }

  rc = prollyMutMapResolveSortedPos(pMap, pSortKey, nSortKey, 0,
                                    &lo, &found);

  while( rc==SQLITE_OK && lo < pMap->nEntries ){
    ProllyMutMapEntry *pEntry;
    const u8 *pRec;
    int nRec;
    int cmpLen;
    int prefixCmp;

    rc = prollyMutMapEntryAt(pMap, lo, &pEntry);
    if( rc!=SQLITE_OK ) break;
    pRec = pEntry->pVal;
    nRec = pEntry->nVal;

    if( pMap->isIntKey ){
      lo++;
      continue;
    }
    cmpLen = pEntry->nKey < nSortKey ? pEntry->nKey : nSortKey;
    prefixCmp = memcmp(pEntry->pKey, pSortKey, cmpLen);
    if( prefixCmp>0 ){
      /* The first pending row above the seek key. That is a landing for any
      ** direction -- the caller reports it as above and its consumer steps
      ** from there. It is emphatically not an equality, so *pEqSeen stays
      ** clear: claiming otherwise tells an equality seek it found a row.
      ** default_rc says how a prefix-equal row compares, which is a different
      ** question, and gating this on it lost the landing for SeekGE. */
      if( pEntry->op==PROLLY_EDIT_INSERT ){
        pMatch = pEntry;
        cmp = 1;
        break;
      }
      lo++;
      continue;
    }
    if( prefixCmp==0 && pEntry->nKey < nSortKey ){
      break;
    }
    if( nRec==0 ){
      rc = recordFromSortKeyBufferColl(pEntry->pKey, pEntry->nKey,
                                    pKeyInfo,
                                    &pRecBuf, &nRecBufAlloc, &nRec);
      if( rc!=SQLITE_OK ) break;
      pRec = pRecBuf;
    }
    pIdxKey->eqSeen = 0;
    cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
    if( cmp<0 && !pIdxKey->eqSeen ){
      break;
    }
    /* cmp>0 with no equality means this row sorts above the seek key even
    ** though its key bytes started with the seek key's. A numeric value that
    ** no double represents exactly is encoded as the neighbouring double plus
    ** the exact integer, so its key literally begins with the key of that
    ** neighbour: seeking to the neighbour lands here with the bytes equal and
    ** the values not. That is the same landing the prefix-above branch reports,
    ** so report it rather than losing the row. *pEqSeen stays clear -- nothing
    ** compared equal, and claiming otherwise would let an equality seek
    ** overwrite the wrong row. */
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      pMatch = pEntry;
      *pEqSeen = (cmp==0 || pIdxKey->eqSeen);
      break;
    }
    lo++;
  }

  sqlite3_free(pRecBuf);
  if( rc==SQLITE_OK && pMatch ){
    *ppMatch = pMatch;
    *pCmp = cmp;
  }
  return rc;
}

static int scanMutMapForCustomCollation(
  BtCursor *pCur,
  ProllyMutMap *pMap,
  UnpackedRecord *pIdxKey,
  ProllyMutMapEntry **ppMatch,
  int *pCmp
){
  int i;
  int rc = SQLITE_OK;
  u8 *pRecBuf = 0;
  int nRecBufAlloc = 0;

  *ppMatch = 0;
  *pCmp = 0;
  if( !pMap || prollyMutMapIsEmpty(pMap) || pMap->isIntKey ){
    return SQLITE_OK;
  }

  for(i=0; i<pMap->nEntries; i++){
    ProllyMutMapEntry *pEntry = &pMap->aEntries[i];
    const u8 *pRec = pEntry->pVal;
    int nRec = pEntry->nVal;
    int cmp;

    if( pEntry->op!=PROLLY_EDIT_INSERT ){
      continue;
    }
    if( nRec==0 ){
      rc = recordFromSortKeyBufferColl(pEntry->pKey, pEntry->nKey,
                                    pCur->pKeyInfo,
                                    &pRecBuf, &nRecBufAlloc, &nRec);
      if( rc!=SQLITE_OK ) break;
      pRec = pRecBuf;
    }
    pIdxKey->eqSeen = 0;
    cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
    if( cmp==0 || pIdxKey->eqSeen ){
      *ppMatch = pEntry;
      *pCmp = cmp;
      break;
    }
  }

  sqlite3_free(pRecBuf);
  return rc;
}

static int scanTreeForCustomCollation(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pFound,
  int *pCmp
){
  int rc;
  int res = 0;
  u8 *pRecBuf = pCur->pMovetoRec;
  int nRecBufAlloc = pCur->nMovetoRecAlloc;

  *pFound = 0;
  *pCmp = 0;
  rc = prollyCursorFirst(&pCur->pCur, &res);
  while( rc==SQLITE_OK && res==0 && prollyCursorIsValid(&pCur->pCur) ){
    const u8 *pKey = 0;
    int nKey = 0;
    const u8 *pRec = 0;
    int nRec = 0;
    int cmp;
    int isDeleted = 0;

    rc = prollyCursorCheckInterrupt(pCur);
    if( rc!=SQLITE_OK ) break;
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      ProllyMutMapEntry *pEntry = 0;
      rc = prollyMutMapFindRc(pCur->pMutMap, pKey, nKey, 0, &pEntry);
      if( rc!=SQLITE_OK ) break;
      if( pEntry && pEntry->op==PROLLY_EDIT_DELETE ) isDeleted = 1;
    }
    if( !isDeleted ){
      prollyCursorValue(&pCur->pCur, &pRec, &nRec);
      if( nRec==0 ){
        rc = recordFromSortKeyBufferColl(pKey, nKey, pCur->pKeyInfo,
                                         &pRecBuf, &nRecBufAlloc, &nRec);
        if( rc!=SQLITE_OK ) break;
        pRec = pRecBuf;
      }
      pIdxKey->eqSeen = 0;
      cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
      if( cmp==0 || pIdxKey->eqSeen ){
        *pFound = 1;
        *pCmp = cmp;
        break;
      }
    }
    rc = prollyCursorNext(&pCur->pCur);
  }

  pCur->pMovetoRec = pRecBuf;
  pCur->nMovetoRecAlloc = nRecBufAlloc;
  return rc;
}

/* Build the sort-key probe for an index moveto; caches on pCur. */
static int indexMovetoBuildSeekKey(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pnSeekKeyField,
  int *pnSortKey,
  u8 **ppSortKey,
  int *pExactMutMapKey
){
  int nSeekKeyField = 0;
  int nSortKey = 0;
  int nSerKey = 0;
  int rc;

  *pExactMutMapKey = 0;
  if( pCur->pKeyInfo && pIdxKey->nField < pCur->pKeyInfo->nAllField ){
    nSeekKeyField = (int)pIdxKey->nField;
  }
  /* Table-root range seeks must ignore probe fields beyond the PK. */
  if( pCur->isTableRoot && pCur->pKeyInfo
   && pIdxKey->default_rc != 0
   && pIdxKey->nField > pCur->pKeyInfo->nKeyField
   && (nSeekKeyField==0 || nSeekKeyField > pCur->pKeyInfo->nKeyField) ){
    nSeekKeyField = pCur->pKeyInfo->nKeyField;
  }
  if( unpackedRecordCanUseIntSortKey(
          pCur, pIdxKey,
          nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField) ){
    int nIntField = nSeekKeyField>0
      ? nSeekKeyField : (int)pIdxKey->nField;
    if( pCur->pSeekSortKey==0
     && nIntField <= (int)sizeof(pCur->aSeekSortKey)/18 ){
      pCur->pSeekSortKey = pCur->aSeekSortKey;
      pCur->nSeekSortKeyAlloc = sizeof(pCur->aSeekSortKey);
    }else if( pCur->pSeekSortKey==pCur->aSeekSortKey
           && nIntField > (int)sizeof(pCur->aSeekSortKey)/18 ){
      pCur->pSeekSortKey = 0;
      pCur->nSeekSortKeyAlloc = 0;
    }
    rc = sortKeyFromUnpackedIntRecordBuffer(
        pIdxKey, nIntField,
        &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
  }else{
    if( pCur->pSeekSortKey==pCur->aSeekSortKey ){
      pCur->pSeekSortKey = 0;
      pCur->nSeekSortKeyAlloc = 0;
    }
    rc = sortKeyFromMemPrefixCollBuffer(
        pIdxKey->aMem, (int)pIdxKey->nField, nSeekKeyField,
        pCur->pKeyInfo,
        &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
    if( rc==SQLITE_NOTFOUND ){
      rc = serializeUnpackedRecordBuffer(
          pIdxKey, &pCur->pSeekRecord, &pCur->nSeekRecordAlloc, &nSerKey);
      if( rc!=SQLITE_OK ) return rc;
      rc = sortKeyFromRecordPrefixCollBuffer(
          pCur->pSeekRecord, nSerKey, nSeekKeyField, pCur->pKeyInfo,
          &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
    }
  }
  if( rc!=SQLITE_OK ) return rc;
  *ppSortKey = pCur->pSeekSortKey;
  *pnSortKey = nSortKey;
  *pnSeekKeyField = nSeekKeyField;
  pCur->nSeekSortKey = nSortKey;
  pCur->nSeekKeyField = nSeekKeyField;
  /* A probe covering a table root's whole primary key names one row, so the
  ** pending map can be searched by exact key -- but only when the probe is an
  ** equality. A range bound names no row, and an exact lookup for it finds
  ** nothing, which would hide every pending row the range should return. */
  if( pCur->pKeyInfo
   && pIdxKey->default_rc == 0
   && nSeekKeyField == pCur->pKeyInfo->nKeyField
   && pCur->isTableRoot ){
    *pExactMutMapKey = 1;
  }
  return SQLITE_OK;
}

/* Exact full-key probe against cursor + pending mutmaps. *pDone is set when
** the seek is fully resolved (hit or hard error already returned). */
static int indexMovetoExactMutMap(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  const u8 *pSortKey,
  int nSortKey,
  int exactMutMapKey,
  int *pRes,
  int *pDone
){
  struct TableEntry *pTE;
  ProllyMutMap *pPending;
  ProllyMutMapEntry *pEntry = 0;
  int cursorMapMiss = 0;
  int rc;

  *pDone = 0;
  if( !pCur->pKeyInfo
   || !(exactMutMapKey || pIdxKey->nField >= pCur->pKeyInfo->nAllField) ){
    return SQLITE_OK;
  }
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    rc = prollyMutMapFindRc(pCur->pMutMap, pSortKey, nSortKey, 0, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    cursorMapMiss = pEntry==0;
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      if( pCur->isPinned ) return SQLITE_CONSTRAINT_PINNED;
      setCursorToMutMapEntryPhys(
          pCur, (int)(pEntry - pCur->pMutMap->aEntries));
      /* The tree side is still wherever the last operation left it. A
      ** later step must re-seek it to this key before merging, or it
      ** feeds stale entries into the merge scan. */
      pCur->deferredTreeSeek = 1;
      *pRes = 0;
      pIdxKey->eqSeen = 1;
      *pDone = 1;
      return SQLITE_OK;
    }
  }
  if( pPending && pPending!=pCur->pMutMap
   && !prollyMutMapIsEmpty(pPending) ){
    rc = prollyMutMapFindRc(pPending, pSortKey, nSortKey, 0, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      if( pCur->isPinned ) return SQLITE_CONSTRAINT_PINNED;
      pCur->pMutMap = pPending;
      setCursorToMutMapEntryPhys(
          pCur, (int)(pEntry - pPending->aEntries));
      pCur->deferredTreeSeek = 1;
      *pRes = 0;
      pIdxKey->eqSeen = 1;
      *pDone = 1;
      return SQLITE_OK;
    }
  }
  if( cursorMapMiss
   && (!pPending || pPending==pCur->pMutMap)
   && nSortKey<=(int)sizeof(pCur->aSeekSortKey) ){
    pCur->mmExactMiss = 1;
    pCur->mmMissGeneration = pCur->pMutMap->generation;
    pCur->nMmMissKey = nSortKey;
    if( pSortKey!=pCur->aSeekSortKey ){
      memcpy(pCur->aSeekSortKey, pSortKey, nSortKey);
    }
  }
  return SQLITE_OK;
}

/* Unsupported-collation full scan of mutmaps + tree. *pDone when resolved. */
static int indexMovetoCustomCollation(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int nSeekKeyField,
  int exactMutMapKey,
  int *pRes,
  int *pDone
){
  struct TableEntry *pTE;
  ProllyMutMap *pPending;
  ProllyMutMapEntry *pEntry = 0;
  int cmp = 0;
  int treeFound = 0;
  int rc;

  *pDone = 0;
  if( !pCur->pKeyInfo
   || !(exactMutMapKey || pIdxKey->nField >= pCur->pKeyInfo->nAllField)
   || !keyInfoHasUnsupportedCollation(
          pCur->pKeyInfo,
          nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField) ){
    return SQLITE_OK;
  }
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;

  rc = scanMutMapForCustomCollation(
      pCur, (ProllyMutMap*)pCur->pMutMap, pIdxKey, &pEntry, &cmp);
  if( rc!=SQLITE_OK ) return rc;
  if( pEntry ){
    setCursorToMutMapEntryPhys(
        pCur, (int)(pEntry - pCur->pMutMap->aEntries));
    /* The tree side is still wherever the last operation left it, the same
    ** as every other mut-map landing after a moveto: a later step must
    ** re-seek it to this key before merging or it feeds stale rows in. */
    pCur->deferredTreeSeek = 1;
    *pRes = cmp;
    pIdxKey->eqSeen = 1;
    *pDone = 1;
    return SQLITE_OK;
  }

  if( pPending && pPending!=pCur->pMutMap ){
    rc = scanMutMapForCustomCollation(
        pCur, pPending, pIdxKey, &pEntry, &cmp);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry ){
      const u8 *pVal = pEntry->pVal;
      int nVal = pEntry->nVal;
      if( nVal==0 ){
        rc = cacheCursorPayloadReconstructed(
            pCur, pEntry->pKey, pEntry->nKey);
      }else{
        rc = cacheCursorPayloadCopy(pCur, pVal, nVal);
      }
      if( rc!=SQLITE_OK ) return rc;
      pCur->eState = CURSOR_VALID;
      *pRes = cmp;
      pIdxKey->eqSeen = 1;
      *pDone = 1;
      return SQLITE_OK;
    }
  }

  rc = scanTreeForCustomCollation(pCur, pIdxKey, &treeFound, &cmp);
  if( rc!=SQLITE_OK ) return rc;
  if( treeFound ){
    pCur->eState = CURSOR_VALID;
    cacheCurrentTreeStoredPayloadNonIntKey(pCur);
    *pRes = cmp;
    pIdxKey->eqSeen = 1;
    *pDone = 1;
  }
  return SQLITE_OK;
}

/* After a non-exact tree seek, scan forward for the first live match /
** successor, skipping mutmap-delete-masked rows. */
static int indexMovetoScanTreeLeaf(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  const u8 *pSortKey,
  int nSortKey,
  int nSeekKeyField,
  int *pTreeFound,
  int *pTreeCmp,
  int *pTreeEqSeen
){
  int rc = SQLITE_OK;
  int iLevel;
  ProllyCacheEntry *pLeaf;
  int lo;
  int nItems;
  int bestIdx = -1;
  int bestCmp = 0;
  int eqLanding = 0;
  u8 *pRecBuf;
  int nRecBufAlloc;
  int i;

  *pTreeEqSeen = 0;
  if( pCur->pCur.eState!=PROLLY_CURSOR_VALID ) return SQLITE_OK;

  iLevel = pCur->pCur.iLevel;
  pLeaf = pCur->pCur.aLevel[iLevel].pEntry;
  lo = pCur->pCur.aLevel[iLevel].idx;
  nItems = pLeaf->node.nItems;
  pRecBuf = pCur->pMovetoRec;
  nRecBufAlloc = pCur->nMovetoRecAlloc;

  for(;;){
    for( i = lo; i < nItems; i++ ){
      const u8 *pSK; int nSK;
      const u8 *pVal; int nVal;
      int recCmp;
      prollyNodeKey(&pLeaf->node, i, &pSK, &nSK);

      {
        int cmpLen = nSK < nSortKey ? nSK : nSortKey;
        int prefixCmp = memcmp(pSK, pSortKey, cmpLen);
        if( prefixCmp > 0 ){
          if( bestIdx < 0 ){
            if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
              ProllyMutMapEntry *mmE = 0;
              rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
              if( rc!=SQLITE_OK ) break;
              if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                /* Delete-masked row past the seek key: the next live row
                ** is still the positioning answer, so keep scanning. */
                continue;
              }
            }
            bestIdx = i;
            if( nSeekKeyField>0 ){
              pIdxKey->eqSeen = 0;
              bestCmp = 1;
              eqLanding = 0;
            }else{
              const u8 *pVal2; int nVal2;
              prollyNodeValue(&pLeaf->node, i, &pVal2, &nVal2);
              if( nVal2==0 ){
                rc = recordFromSortKeyBufferColl(
                    pSK, nSK, pCur->pKeyInfo,
                    &pRecBuf, &nRecBufAlloc, &nVal2);
                if( rc!=SQLITE_OK ) break;
                pVal2 = pRecBuf;
              }
              pIdxKey->eqSeen = 0;
              bestCmp = sqlite3VdbeRecordCompare(nVal2, pVal2, pIdxKey);
              eqLanding = (bestCmp==0 || pIdxKey->eqSeen);
            }
          }
          break;
        }
        if( nSeekKeyField>0 && prefixCmp==0 && nSK>=nSortKey
         && (nSK==nSortKey || sortKeyByteStartsField(pSK[nSortKey])) ){
          if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
            ProllyMutMapEntry *mmE = 0;
            rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
            if( rc!=SQLITE_OK ) break;
            if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
              continue;
            }
          }
          pIdxKey->eqSeen = 1;
          bestIdx = i;
          bestCmp = pIdxKey->default_rc;
          eqLanding = 1;
          *pTreeFound = 1;
          *pTreeCmp = bestCmp;
          if( pIdxKey->default_rc < 0 ){
            continue;
          }
          break;
        }
      }
      prollyNodeValue(&pLeaf->node, i, &pVal, &nVal);
      if( nVal==0 ){
        rc = recordFromSortKeyBufferColl(
            pSK, nSK, pCur->pKeyInfo, &pRecBuf, &nRecBufAlloc, &nVal);
        if( rc!=SQLITE_OK ) break;
        pVal = pRecBuf;
      }
      pIdxKey->eqSeen = 0;
      recCmp = sqlite3VdbeRecordCompare(nVal, pVal, pIdxKey);

      if( recCmp==0 || pIdxKey->eqSeen ){
        if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
          ProllyMutMapEntry *mmE = 0;
          rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
          if( rc!=SQLITE_OK ) break;
          if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
            continue;
          }
        }
        bestIdx = i;
        bestCmp = recCmp;
        eqLanding = 1;
        *pTreeFound = 1;
        *pTreeCmp = recCmp;
        break;
      }else if( recCmp > 0 ){
        if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
          ProllyMutMapEntry *mmE = 0;
          rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
          if( rc!=SQLITE_OK ) break;
          if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
            continue;
          }
        }
        if( bestIdx < 0 ){
          bestIdx = i;
          bestCmp = recCmp;
          eqLanding = 0;
        }
      }
    }
    if( rc!=SQLITE_OK || *pTreeFound || bestIdx>=0 ) break;
    if( !(pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap)) ) break;
    /* Everything from the seek point through the end of this leaf was
    ** delete-masked; the first live row may sit in a later leaf. */
    if( nItems>0 ) pCur->pCur.aLevel[pCur->pCur.iLevel].idx = nItems-1;
    rc = prollyCursorNext(&pCur->pCur);
    if( rc!=SQLITE_OK ) break;
    if( pCur->pCur.eState!=PROLLY_CURSOR_VALID ) break;
    iLevel = pCur->pCur.iLevel;
    pLeaf = pCur->pCur.aLevel[iLevel].pEntry;
    nItems = pLeaf->node.nItems;
    lo = 0;
  }
  pCur->pMovetoRec = pRecBuf;
  pCur->nMovetoRecAlloc = nRecBufAlloc;
  if( rc!=SQLITE_OK ) return rc;

  if( *pTreeFound ){
    pCur->pCur.aLevel[iLevel].idx = bestIdx;
    *pTreeEqSeen = eqLanding;
  }else if( bestIdx >= 0 ){
    pCur->pCur.aLevel[iLevel].idx = bestIdx;
    *pTreeCmp = bestCmp;
    *pTreeFound = 1;
    *pTreeEqSeen = eqLanding;
  }
  return SQLITE_OK;
}

/* Exact tree hit (not delete-masked). *pDone when resolved. */
static int indexMovetoExactTreeHit(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  const u8 *pSortKey,
  int nSortKey,
  int exactMutMapKey,
  int *pRes,
  int *pDone
){
  int seekRes = 0;
  int rc;

  *pDone = 0;
  rc = prollyCursorSeekBlob(&pCur->pCur, pSortKey, nSortKey, &seekRes);
  if( rc!=SQLITE_OK ) return rc;
  if( seekRes==0
   && pCur->pCur.eState==PROLLY_CURSOR_VALID
   && pCur->pKeyInfo
   && (exactMutMapKey || pIdxKey->nField >= pCur->pKeyInfo->nAllField) ){
    int isDeleted = 0;
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      ProllyMutMapEntry *mmE = 0;
      rc = prollyMutMapFindRc(pCur->pMutMap, pSortKey, nSortKey, 0, &mmE);
      if( rc!=SQLITE_OK ) return rc;
      if( mmE && mmE->op==PROLLY_EDIT_DELETE ) isDeleted = 1;
    }
    if( !isDeleted ){
      *pRes = 0;
      pIdxKey->eqSeen = 1;
      pCur->eState = CURSOR_VALID;
      cacheCurrentTreeStoredPayloadNonIntKey(pCur);
      *pDone = 1;
    }
  }
  return SQLITE_OK;
}

int prollyBtCursorIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  int rc;
  int done = 0;

  assert( !pCur->curIntKey );

  if( pCur->pBtree ) pCur->pBtree->nSeek++;

  clearMergeCursorState(pCur);
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  CLEAR_CACHED_COMPARE_KEY(pCur);

  refreshCursorRoot(pCur);

  {
    int treeFound = 0, mutFound = 0, mutEqSeen = 0, treeEqSeen = 0;
    int treeCmp = 0, mutCmp = 0;
    const u8 *mutKey = 0;
    int mutNKey = 0;
    ProllyMutMapEntry *mutE = 0;
    int mutFromCursorMap = 0;
    int exactMutMapKey = 0;
    u8 *pSortKey = 0;
    int nSortKey = 0;
    int nSeekKeyField = 0;

    rc = indexMovetoBuildSeekKey(
        pCur, pIdxKey, &nSeekKeyField, &nSortKey, &pSortKey, &exactMutMapKey);
    if( rc!=SQLITE_OK ) return rc;

    rc = indexMovetoCustomCollation(
        pCur, pIdxKey, nSeekKeyField, exactMutMapKey, pRes, &done);
    if( rc!=SQLITE_OK || done ) return rc;

    rc = indexMovetoExactMutMap(
        pCur, pIdxKey, pSortKey, nSortKey, exactMutMapKey, pRes, &done);
    if( rc!=SQLITE_OK || done ) return rc;

    rc = indexMovetoExactTreeHit(
        pCur, pIdxKey, pSortKey, nSortKey, exactMutMapKey, pRes, &done);
    if( rc!=SQLITE_OK || done ) return rc;
    /* A failed tree seek must not fall through to the mut-map merge below:
    ** that path resets rc and reports "row not found" for what was really
    ** an I/O or allocation failure. */
    rc = indexMovetoScanTreeLeaf(
        pCur, pIdxKey, pSortKey, nSortKey, nSeekKeyField,
        &treeFound, &treeCmp, &treeEqSeen);
    if( rc!=SQLITE_OK ) return rc;

    {
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      ProllyMutMap *pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
      if( ((pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap))
         || (pPending && pPending!=pCur->pMutMap
             && !prollyMutMapIsEmpty(pPending)))
       && !(treeFound && treeCmp==0) ){
      int savedEqSeen = pIdxKey->eqSeen;
      rc = findMatchingMutMapEntry((ProllyMutMap*)pCur->pMutMap,
                                   pCur->pKeyInfo,
                                   pIdxKey, pSortKey, nSortKey,
                                   exactMutMapKey,
                                   &mutE, &mutCmp, &mutEqSeen);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      if( mutE ) mutFromCursorMap = 1;
      if( !mutE && pPending && pPending!=pCur->pMutMap ){
        rc = findMatchingMutMapEntry(pPending,
                                     pCur->pKeyInfo,
                                     pIdxKey, pSortKey, nSortKey,
                                     exactMutMapKey,
                                     &mutE, &mutCmp, &mutEqSeen);
        if( rc!=SQLITE_OK ){
          return rc;
        }
      }
      if( mutE ){

        const u8 *pMutVal = mutE->pVal;
        int nMutVal = mutE->nVal;
        if( mutFromCursorMap && nMutVal==0 ){
          mutFound = 1;
        }else if( nMutVal==0 ){
          rc = recordFromSortKeyBufferColl(
              mutE->pKey, mutE->nKey, pCur->pKeyInfo,
              &pCur->pMovetoRec, &pCur->nMovetoRecAlloc, &nMutVal);
          if( rc!=SQLITE_OK ) return rc;
          pMutVal = pCur->pMovetoRec;
        }
        if( pMutVal ){
          mutKey = pMutVal;
          mutNKey = nMutVal;
          mutFound = 1;
        }
      }
      pIdxKey->eqSeen = savedEqSeen;
    }
    }

    if( mutFound ){
      int useMut = !treeFound;
      if( treeFound ){
        const u8 *pTreeKey = 0;
        int nTreeKey = 0;
        int nCmp;
        int cmp;
        prollyCursorKey(&pCur->pCur, &pTreeKey, &nTreeKey);
        nCmp = nTreeKey < mutE->nKey ? nTreeKey : mutE->nKey;
        cmp = memcmp(pTreeKey, mutE->pKey, nCmp);
        if( cmp>0 || (cmp==0 && nTreeKey>mutE->nKey) ){
          useMut = 1;
        }
      }
      if( useMut ){
        if( mutFromCursorMap ){
          setCursorToMutMapEntryPhys(
              pCur, (int)(mutE - pCur->pMutMap->aEntries));
          pCur->deferredTreeSeek = 1;
        }else{
          rc = cacheCursorPayloadCopy(pCur, mutKey, mutNKey);
          if( rc!=SQLITE_OK ){
            return rc;
          }
          pCur->eState = CURSOR_VALID;
        }
        *pRes = mutCmp;
        /* Only if the row actually compared equal. A landing above the key is
        ** still a landing, but an equality seek must not read it as a hit. */
        if( mutEqSeen ) pIdxKey->eqSeen = 1;
        assert( pIdxKey->default_rc>=0 || mutCmp!=-1 || pIdxKey->eqSeen );
        return SQLITE_OK;
      }
    }
    if( treeFound ){
      *pRes = treeCmp;
      if( treeEqSeen ) pIdxKey->eqSeen = 1;
      assert( pIdxKey->default_rc>=0 || treeCmp!=-1 || pIdxKey->eqSeen );
      /* A prefix seek can land on a tree row whose value was overwritten in
      ** this transaction (full-key seeks catch this in the exact-match fast
      ** path above). Serve the row from the mut map, or the caller reads the
      ** stale tree payload. */
      if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        const u8 *pTreeKey = 0;
        int nTreeKey = 0;
        ProllyMutMapEntry *mmE = 0;
        prollyCursorKey(&pCur->pCur, &pTreeKey, &nTreeKey);
        rc = prollyMutMapFindRc(pCur->pMutMap, pTreeKey, nTreeKey, 0, &mmE);
        if( rc!=SQLITE_OK ) return rc;
        if( mmE && mmE->op==PROLLY_EDIT_INSERT ){
          setCursorToMutMapEntryPhys(
              pCur, (int)(mmE - pCur->pMutMap->aEntries));
          pCur->deferredTreeSeek = 1;
          return SQLITE_OK;
        }
      }
      pCur->eState = CURSOR_VALID;
      cacheCurrentTreeStoredPayloadNonIntKey(pCur);
      return SQLITE_OK;
    }
  }

  {
    int lastRes = 0;
    rc = prollyCursorLast(&pCur->pCur, &lastRes);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      /* No live row at or above the seek key on either side. Land on the
      ** merged last row so delete-masked tree rows are never exposed and
      ** mut-map-only rows are still reachable. */
      pCur->mmActive = 1;
      rc = mergeLast(pCur, &lastRes);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( lastRes==0 ){
      pCur->eState = CURSOR_VALID;
      *pRes = -1;
      if( !pCur->mmActive || pCur->mergeSrc==MERGE_SRC_TREE ){
        /* Cache only on a pure tree landing: getCursorPayload serves the
        ** cache before the merge source, so caching on a BOTH landing
        ** serves the shadowed committed value instead of the pending one
        ** written in this transaction. A BOTH landing needs no deferral
        ** either -- mergeLast leaves the tree cursor ON the row, valid
        ** for a step in either direction. */
        cacheCurrentTreeStoredPayloadNonIntKey(pCur);
      }else if( pCur->mergeSrc==MERGE_SRC_MUT ){
        /* mergeLast scanned backwards to get here, so the tree side sits
        ** below this mut-map row -- it was retreated past any delete-masked
        ** row at the same key. Stepping forward from that would serve a tree
        ** row the scan has already passed. Defer the tree seek so the first
        ** step re-seeks to this row's key and adjusts for its own direction. */
        pCur->deferredTreeSeek = 1;
      }
    } else {
      pCur->eState = CURSOR_INVALID;
      *pRes = -1;
    }
  }
  return SQLITE_OK;
}
int sqlite3BtreeIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xIndexMoveto(pCur, pIdxKey, pRes);
}
int cachedSeekKeyMatchesCurrent(BtCursor *pCur){
  const u8 *pKey = 0;
  int nKey = 0;

  if( !pCur || pCur->curIntKey
   || pCur->nSeekSortKey<=0 || pCur->nSeekKeyField!=0 ){
    return 0;
  }
  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    if( currentMutMapEntry(pCur, &e)!=SQLITE_OK || !e ) return 0;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }
  return pKey && nKey==pCur->nSeekSortKey
      && memcmp(pKey, pCur->pSeekSortKey, nKey)==0;
}

int sqlite3BtreeProllyCachedIndexKeyCompare(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  const u8 *pKey = 0;
  int nKey = 0;
  int nCmp;
  int cmp;

  if( !pCur || pCur->pCurOps!=&prollyCursorOps || pCur->curIntKey ){
    return SQLITE_NOTFOUND;
  }
  if( pCur->eState!=CURSOR_VALID || !pIdxKey || !pCur->pKeyInfo ){
    return SQLITE_NOTFOUND;
  }

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e;
    int rc = currentMutMapEntry(pCur, &e);
    if( rc!=SQLITE_OK ) return rc;
    if( !e ) return SQLITE_NOTFOUND;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }else{
    return SQLITE_NOTFOUND;
  }

  if( prollyBtCursorCursorHasHint(pCur, BTREE_SEEK_EQ)
   && pCur->nSeekSortKey>0
   && pCur->nSeekKeyField==(int)pIdxKey->nField ){
    nCmp = nKey < pCur->nSeekSortKey ? nKey : pCur->nSeekSortKey;
    cmp = memcmp(pKey, pCur->pSeekSortKey, nCmp);
    if( cmp<0 ){
      *pRes = -1;
    }else if( cmp>0 ){
      *pRes = 1;
    }else if( nKey < pCur->nSeekSortKey ){
      *pRes = -1;
    }else if( nKey > pCur->nSeekSortKey
           && !sortKeyByteStartsField(pKey[pCur->nSeekSortKey]) ){
      /* The seek key stops mid-field, so the rows are not equal and their
      ** order depends on the encoding rather than on the bytes compared
      ** here. Decline and let the record comparator answer. */
      return SQLITE_NOTFOUND;
    }else{
      pIdxKey->eqSeen = 1;
      *pRes = pIdxKey->default_rc;
    }
    return SQLITE_OK;
  }

  {
    int rc;
    if( pCur->nCompareSortKey<=0
     || pCur->nCompareKeyField!=(int)pIdxKey->nField ){
      rc = sortKeyFromUnpackedForCount(
          pCur, pIdxKey, &pCur->pCompareSortKey,
          &pCur->nCompareSortKeyAlloc, &pCur->nCompareSortKey);
      if( rc!=SQLITE_OK ){
        CLEAR_CACHED_COMPARE_KEY(pCur);
        return rc;
      }
      pCur->nCompareKeyField = (int)pIdxKey->nField;
    }
    nCmp = nKey < pCur->nCompareSortKey ? nKey : pCur->nCompareSortKey;
    cmp = memcmp(pKey, pCur->pCompareSortKey, nCmp);
    if( cmp<0 ){
      *pRes = -1;
    }else if( cmp>0 ){
      *pRes = 1;
    }else if( nKey < pCur->nCompareSortKey ){
      *pRes = -1;
    }else if( nKey > pCur->nCompareSortKey
           && !sortKeyByteStartsField(pKey[pCur->nCompareSortKey]) ){
      return SQLITE_NOTFOUND;
    }else{
      pIdxKey->eqSeen = 1;
      *pRes = pIdxKey->default_rc;
    }
    return SQLITE_OK;
  }
}

/* Clear cached compare key before OP_SeekScan changes seek targets. */
void sqlite3BtreeProllyClearCompareKey(BtCursor *pCur){
  if( pCur ){
    CLEAR_CACHED_COMPARE_KEY(pCur);
  }
}

#endif
