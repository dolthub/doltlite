#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Index-key construction and three-way row/cell merge. */

/* Serial type + body length for an i64 value, matching the encoding used
** by SQLite's row records and prolly trees. */
void doltliteIpkSerialType(i64 v, u32 *pType, u32 *pLen){
  if( v==0 ){ *pType = 8; *pLen = 0; return; }
  if( v==1 ){ *pType = 9; *pLen = 0; return; }
  if( v>=-128 && v<=127 ){ *pType = 1; *pLen = 1; return; }
  if( v>=-32768 && v<=32767 ){ *pType = 2; *pLen = 2; return; }
  if( v>=-8388608 && v<=8388607 ){ *pType = 3; *pLen = 3; return; }
  if( v>=-2147483648LL && v<=2147483647LL ){ *pType = 4; *pLen = 4; return; }
  if( v>=-140737488355328LL && v<=140737488355327LL ){ *pType = 5; *pLen = 6; return; }
  *pType = 6; *pLen = 8;
}

void doltliteIpkWriteBE(u8 *p, i64 v, int n){
  int i;
  for(i=n-1; i>=0; i--){ p[i] = (u8)(v & 0xff); v >>= 8; }
}

/* KeyInfo for Index without a Parse context. Matches sqlite3KeyInfoOfIndex
** collations/sort flags so VC raw-row paths encode the same sort keys as VDBE. */
KeyInfo *doltliteKeyInfoOfIndex(sqlite3 *db, Index *pIdx){
  int i;
  int nCol;
  int nKey;
  KeyInfo *pKey;

  if( !db || !pIdx ) return 0;
  nCol = pIdx->nColumn;
  nKey = pIdx->nKeyCol;
  if( pIdx->uniqNotNull ){
    pKey = sqlite3KeyInfoAlloc(db, nKey, nCol - nKey);
  }else{
    pKey = sqlite3KeyInfoAlloc(db, nCol, 0);
  }
  if( !pKey ) return 0;
  for(i=0; i<nCol; i++){
    const char *zColl = pIdx->azColl ? pIdx->azColl[i] : 0;
    if( !zColl || zColl==sqlite3StrBINARY
     || sqlite3StrICmp(zColl, "BINARY")==0 ){
      pKey->aColl[i] = 0;
    }else{
      pKey->aColl[i] = sqlite3FindCollSeq(db, ENC(db), zColl, 0);
    }
    pKey->aSortFlags[i] = pIdx->aSortOrder ? pIdx->aSortOrder[i] : 0;
  }
  return pKey;
}

static int indexKeyInfoNeedsPayload(
  const KeyInfo *pKeyInfo,
  const u8 *pIdxRec,
  int nIdxRec
){
  int i;
  if( pKeyInfo ){
    if( pKeyInfo->nKeyField < pKeyInfo->nAllField ) return 1;
    for(i=0; i<pKeyInfo->nAllField; i++){
      const CollSeq *pColl = pKeyInfo->aColl[i];
      if( pColl && pColl->zName
       && (sqlite3StrICmp(pColl->zName, "NOCASE")==0
        || sqlite3StrICmp(pColl->zName, "RTRIM")==0) ){
        return 1;
      }
    }
  }
  return sortKeyRecordNeedsPayload(pIdxRec, nIdxRec, 0);
}

/* Build index sort key (+ optional index-record payload) from a table row. */
static int doltliteBuildIndexEntry(
  const u8 *pRec, int nRec,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  u8 **ppSortKey, int *pnSortKey,
  u8 **ppIdxRec, int *pnIdxRec,
  int *pStorePayload
){
  DoltliteRecordInfo info;
  u8 *pIdxRec = 0;
  int nIdxRec = 0;
  u32 ipkType = 0;
  u32 ipkLen = 0;
  int useIpk = 0;
  int storePayload = 0;
  int rc;

  if( ppSortKey ) *ppSortKey = 0;
  if( pnSortKey ) *pnSortKey = 0;
  if( ppIdxRec ) *ppIdxRec = 0;
  if( pnIdxRec ) *pnIdxRec = 0;
  if( pStorePayload ) *pStorePayload = 0;

  doltliteParseRecord(pRec, nRec, &info);
  if( info.nField==0 ) return SQLITE_CORRUPT;

  if( iPKey>=0 && iPKey<info.nField ){
    int st = info.aType[iPKey];
    if( st==0 || st==8 || st==9 ){
      useIpk = 1;
      doltliteIpkSerialType(intKey, &ipkType, &ipkLen);
    }
  }

  {
    int i, hdrLen = 0, bodyLen = 0;
    int nTotal;
    u8 *p;

    int nOutField = info.nField;
    int *aFieldOrder = sqlite3_malloc(nOutField * sizeof(int));
    u8 *aUsed = sqlite3_malloc(info.nField);
    if( !aFieldOrder || !aUsed ){
      sqlite3_free(aFieldOrder);
      sqlite3_free(aUsed);
      return SQLITE_NOMEM;
    }
    memset(aUsed, 0, info.nField);

    {
      int out = 0;
      for(i=0; i<nIdxCol; i++){
        int col = aiColumn[i];
        if( col>=0 && col<info.nField ){
          aFieldOrder[out++] = col;
          aUsed[col] = 1;
        }
      }
      /* IPK is the secondary-index tie-breaker. */
      if( iPKey>=0 && iPKey<info.nField && !aUsed[iPKey] ){
        aFieldOrder[out++] = iPKey;
        aUsed[iPKey] = 1;
      }
      nOutField = out;
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = (useIpk && col==iPKey) ? (int)ipkType : info.aType[col];
      int flen = st>0 ? dlSerialTypeLen((u64)st) : 0;
      hdrLen += sqlite3VarintLen(st);
      bodyLen += flen;
    }

    {
      int tentative = hdrLen + 1;
      if( tentative > 126 ) tentative++;
      hdrLen = tentative;
    }

    nTotal = hdrLen + bodyLen;
    pIdxRec = sqlite3_malloc(nTotal);
    if( !pIdxRec ){
      sqlite3_free(aFieldOrder);
      sqlite3_free(aUsed);
      return SQLITE_NOMEM;
    }

    p = pIdxRec;
    {
      int hs = hdrLen;
      if( hs <= 0x7f ){ *p++ = (u8)hs; }
      else{ *p++ = (u8)(0x80|(hs>>7)); *p++ = (u8)(hs&0x7f); }
    }
    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = (useIpk && col==iPKey) ? (int)ipkType : info.aType[col];
      p += sqlite3PutVarint(p, st);
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st;
      int flen;
      if( useIpk && col==iPKey ){
        st = (int)ipkType;
        flen = (int)ipkLen;
        if( flen>0 ){
          doltliteIpkWriteBE(p, intKey, flen);
          p += flen;
        }
        continue;
      }
      st = info.aType[col];
      flen = st>0 ? dlSerialTypeLen((u64)st) : 0;
      if( flen>0 ){
        memcpy(p, pRec + info.aOffset[col], flen);
        p += flen;
      }
    }
    nIdxRec = (int)(p - pIdxRec);
    sqlite3_free(aFieldOrder);
    sqlite3_free(aUsed);
  }

  storePayload = indexKeyInfoNeedsPayload(pKeyInfo, pIdxRec, nIdxRec);
  rc = sortKeyFromRecordPrefixColl(pIdxRec, nIdxRec, 0, pKeyInfo,
                                    ppSortKey, pnSortKey);
  /* WITHOUT ROWID secondary indexes suffix the table-tree key. */
  if( rc==SQLITE_OK && iPKey<0 && pTreeKey && nTreeKey>0 ){
    u8 *pCombined = sqlite3_realloc(*ppSortKey, *pnSortKey + nTreeKey);
    if( !pCombined ){
      sqlite3_free(*ppSortKey);
      *ppSortKey = 0;
      *pnSortKey = 0;
      sqlite3_free(pIdxRec);
      return SQLITE_NOMEM;
    }
    memcpy(pCombined + *pnSortKey, pTreeKey, nTreeKey);
    *ppSortKey = pCombined;
    *pnSortKey += nTreeKey;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(pIdxRec);
    return rc;
  }
  if( pStorePayload ) *pStorePayload = storePayload;
  if( storePayload && ppIdxRec ){
    *ppIdxRec = pIdxRec;
    if( pnIdxRec ) *pnIdxRec = nIdxRec;
  }else{
    sqlite3_free(pIdxRec);
  }
  return SQLITE_OK;
}

/* Apply old/new table-row values to one secondary-index mutmap. Shared by
** merge, conflicts resolve, and workspace so NOCASE/RTRIM/DESC match VDBE. */
int doltliteIndexMutMapRowDelta(
  ProllyMutMap *pMap,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal
){
  int rc = SQLITE_OK;

  if( !pMap ) return SQLITE_MISUSE;

  if( pOldVal && nOldVal>0 ){
    u8 *pSK = 0;
    int nSK = 0;
    rc = doltliteBuildIndexEntry(
        pOldVal, nOldVal, aiColumn, nIdxCol, pKeyInfo, iPKey, intKey,
        pTreeKey, nTreeKey, &pSK, &nSK, 0, 0, 0);
    if( rc==SQLITE_OK ){
      rc = prollyMutMapDelete(pMap, pSK, nSK, 0);
    }
    sqlite3_free(pSK);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( pNewVal && nNewVal>0 ){
    u8 *pSK = 0;
    u8 *pRec = 0;
    int nSK = 0, nRec = 0, store = 0;
    rc = doltliteBuildIndexEntry(
        pNewVal, nNewVal, aiColumn, nIdxCol, pKeyInfo, iPKey, intKey,
        pTreeKey, nTreeKey, &pSK, &nSK, &pRec, &nRec, &store);
    if( rc==SQLITE_OK ){
      if( store ){
        rc = prollyMutMapInsert(pMap, pSK, nSK, 0, pRec, nRec);
      }else{
        rc = prollyMutMapInsert(pMap, pSK, nSK, 0, 0, 0);
      }
    }
    sqlite3_free(pSK);
    sqlite3_free(pRec);
  }
  return rc;
}

int doltliteIndexApplyRowDelta(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *cache,
  ProllyHash *pIdxRoot,
  u8 idxFlags,
  Index *pIdx,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal
){
  KeyInfo *pKeyInfo = 0;
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;

  if( !cs || !cache || !pIdxRoot || !pIdx ) return SQLITE_MISUSE;
  if( (!pOldVal || nOldVal<=0) && (!pNewVal || nNewVal<=0) ) return SQLITE_OK;

  pKeyInfo = doltliteKeyInfoOfIndex(db, pIdx);
  if( !pKeyInfo ) return SQLITE_NOMEM;

  rc = prollyMutMapInit(&mm, 0);
  if( rc!=SQLITE_OK ){
    sqlite3KeyInfoUnref(pKeyInfo);
    return rc;
  }

  rc = doltliteIndexMutMapRowDelta(
      &mm, pIdx->aiColumn, pIdx->nKeyCol, pKeyInfo,
      iPKey, intKey, pTreeKey, nTreeKey,
      pOldVal, nOldVal, pNewVal, nNewVal);
  if( rc==SQLITE_OK && !prollyMutMapIsEmpty(&mm) ){
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    mut.oldRoot = *pIdxRoot;
    mut.pEdits = &mm;
    mut.flags = idxFlags ? idxFlags : (u8)PROLLY_NODE_BLOBKEY;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ) *pIdxRoot = mut.newRoot;
  }

  prollyMutMapFree(&mm);
  sqlite3KeyInfoUnref(pKeyInfo);
  return rc;
}

typedef struct RowMergeCtx RowMergeCtx;
struct RowMergeCtx {
  ProllyMutMap *pEdits;
  u8 isIntKey;
  MergeIndexInfo *aIndexes;
  int nIndexes;
  int nConflicts;

  DoltliteConflictRow *aConflicts;
  int nConflictsAlloc;
};

typedef struct RecField RecField;
struct RecField { u64 st; int off; int len; };

static int parseRecordFields(const u8 *pRec, int nRec,
                             RecField **ppFields, int *pnFields){
  const u8 *pPos, *pEnd, *pHdrEnd;
  u64 hdrSize;
  int hdrBytes, nFields = 0, nAlloc = 0;
  i64 bodyOff;
  RecField *aFields = 0;

  if(!pRec || nRec<1) { *ppFields=0; *pnFields=0; return 0; }
  pPos = pRec; pEnd = pRec + nRec;
  hdrBytes = dlReadVarint(pPos, pEnd, &hdrSize);
  if(hdrBytes<=0){ *ppFields=0; *pnFields=0; return -1; }
  pPos += hdrBytes;
  if((u64)hdrBytes > hdrSize || hdrSize > (u64)nRec){
    *ppFields=0; *pnFields=0; return -1;
  }
  pHdrEnd = pRec + (int)hdrSize;
  bodyOff = (i64)hdrSize;

  while(pPos < pHdrEnd && pPos < pEnd){
    u64 st; int stBytes, sz;
    stBytes = dlReadVarint(pPos, pHdrEnd, &st);
    if(stBytes<=0){
      sqlite3_free(aFields);
      *ppFields=0; *pnFields=0;
      return -1;
    }
    pPos += stBytes;
    sz = dlSerialTypeLen(st);
    if(sz < 0 || bodyOff + (i64)sz > (i64)nRec){
      sqlite3_free(aFields);
      *ppFields=0; *pnFields=0;
      return -1;
    }

    if( DOLTLITE_GROW_ARRAY(&aFields, &nAlloc, nFields+1, 16)!=SQLITE_OK ){
      sqlite3_free(aFields);
      *ppFields=0; *pnFields=0;
      return -1;
    }
    aFields[nFields].st = st;
    aFields[nFields].off = (int)bodyOff;
    aFields[nFields].len = sz;
    nFields++;
    bodyOff += sz;
  }

  *ppFields = aFields;
  *pnFields = nFields;
  return nFields;
}

static int fieldEquals(const u8 *pRecA, RecField *fA,
                       const u8 *pRecB, RecField *fB){
  if(fA->st != fB->st) return 1;
  if(fA->len != fB->len) return 1;
  if(fA->len==0) return 0;
  return memcmp(pRecA + fA->off, pRecB + fB->off, fA->len);
}

typedef struct MergeWinner MergeWinner;
struct MergeWinner { const u8 *pRec; RecField *pField; };

static u8 *buildMergedRecord(MergeWinner *aWinners, int nFields, int *pnOut){
  int hdrSize = 0, bodySize = 0, pos, i;
  u8 *result;

  for(i=0; i<nFields; i++){
    u64 st = aWinners[i].pField->st;
    if(st <= 0x7f) hdrSize += 1;
    else if(st <= 0x3fff) hdrSize += 2;
    else if(st <= 0x1fffff) hdrSize += 3;
    else hdrSize += 4;
    bodySize += aWinners[i].pField->len;
  }

  { int tentative = hdrSize + 1;
    if(tentative > 0x7f) tentative++;
    hdrSize = tentative;
  }

  result = sqlite3_malloc(hdrSize + bodySize);
  if(!result){ *pnOut = 0; return 0; }

  pos = 0;
  { u64 hs = (u64)hdrSize;
    if(hs <= 0x7f){ result[pos++] = (u8)hs; }
    else{ result[pos++] = (u8)(0x80 | (hs>>7)); result[pos++] = (u8)(hs&0x7f); }
  }

  for(i=0; i<nFields; i++){
    u64 st = aWinners[i].pField->st;
    if(st <= 0x7f){
      result[pos++] = (u8)st;
    }else if(st <= 0x3fff){
      result[pos++] = (u8)(0x80 | (st>>7));
      result[pos++] = (u8)(st&0x7f);
    }else if(st <= 0x1fffff){
      result[pos++] = (u8)(0x80 | (st>>14));
      result[pos++] = (u8)(0x80 | ((st>>7)&0x7f));
      result[pos++] = (u8)(st&0x7f);
    }else{
      result[pos++] = (u8)(0x80 | (st>>21));
      result[pos++] = (u8)(0x80 | ((st>>14)&0x7f));
      result[pos++] = (u8)(0x80 | ((st>>7)&0x7f));
      result[pos++] = (u8)(st&0x7f);
    }
  }

  for(i=0; i<nFields; i++){
    if(aWinners[i].pField->len > 0){
      memcpy(result + pos, aWinners[i].pRec + aWinners[i].pField->off,
             aWinners[i].pField->len);
      pos += aWinners[i].pField->len;
    }
  }

  *pnOut = pos;

#ifndef NDEBUG
  {
    int nfCheck = 0;
    RecField *aCheck = 0;
    if( parseRecordFields(result, pos, &aCheck, &nfCheck) >= 0 ){
      assert( nfCheck == nFields );
      sqlite3_free(aCheck);
    }
  }
#endif

  return result;
}

static u8 *tryCellMerge(
  const u8 *pBase, int nBase,
  const u8 *pOurs, int nOurs,
  const u8 *pTheirs, int nTheirs,
  int *pnMerged
){
  RecField *aBase=0, *aOurs=0, *aTheirs=0;
  int nfBase=0, nfOurs=0, nfTheirs=0;
  int nfMax, i;
  u8 *result = 0;

  if(parseRecordFields(pBase, nBase, &aBase, &nfBase)<0) goto fail;
  if(parseRecordFields(pOurs, nOurs, &aOurs, &nfOurs)<0) goto fail;
  if(parseRecordFields(pTheirs, nTheirs, &aTheirs, &nfTheirs)<0) goto fail;

  nfMax = nfBase;
  if(nfOurs > nfMax) nfMax = nfOurs;
  if(nfTheirs > nfMax) nfMax = nfTheirs;

  {
    MergeWinner *winners;
    /* A field beyond a record's stored width is a trailing NULL (SQLite omits
    ** them), semantically identical to an explicit NULL field. Treating "absent"
    ** and "explicit NULL" uniformly lets the per-cell merge span records of
    ** different widths -- e.g. a dual ADD COLUMN merge where each side's row
    ** carries a different number of columns. */
    static const RecField kNullField = { 0, 0, 0 };
    int nEmit = 0;

    winners = sqlite3_malloc(nfMax * (int)sizeof(*winners));
    if(!winners) goto fail;

    for(i=0; i<nfMax; i++){
      RecField *fB = (i<nfBase)   ? &aBase[i]   : (RecField*)&kNullField;
      RecField *fO = (i<nfOurs)   ? &aOurs[i]   : (RecField*)&kNullField;
      RecField *fT = (i<nfTheirs) ? &aTheirs[i] : (RecField*)&kNullField;
      int oursChanged   = fieldEquals(pBase, fB, pOurs, fO)!=0;
      int theirsChanged = fieldEquals(pBase, fB, pTheirs, fT)!=0;

      if(!theirsChanged){

        winners[i].pRec = pOurs; winners[i].pField = fO;
      }else if(!oursChanged){

        winners[i].pRec = pTheirs; winners[i].pField = fT;
      }else if(fieldEquals(pOurs, fO, pTheirs, fT)==0){

        winners[i].pRec = pOurs; winners[i].pField = fO;
      }else{
        sqlite3_free(winners); goto fail;
      }
      if( winners[i].pField->st != 0 ) nEmit = i+1;
    }

    /* Drop trailing NULLs so the merged row re-encodes in canonical form. */
    result = buildMergedRecord(winners, nEmit, pnMerged);
    sqlite3_free(winners);
  }

  sqlite3_free(aBase);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  return result;

fail:
  sqlite3_free(aBase);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  *pnMerged = 0;
  return 0;
}

static int rowMergeCallback(void *pCtx, const ThreeWayChange *pChange){
  RowMergeCtx *ctx = (RowMergeCtx*)pCtx;
  int rc = SQLITE_OK;

  switch( pChange->type ){
    case THREE_WAY_LEFT_ADD:
    case THREE_WAY_LEFT_MODIFY:
    case THREE_WAY_LEFT_DELETE:

      break;

    case THREE_WAY_RIGHT_ADD:

      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      if( rc==SQLITE_OK && ctx->nIndexes>0
       && pChange->pTheirVal && pChange->nTheirVal>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              mi->pEdits, mi->aiColumn, mi->nColumn, mi->pKeyInfo,
              mi->iPKey, pChange->intKey, pChange->pKey, pChange->nKey,
              0, 0, pChange->pTheirVal, pChange->nTheirVal);
        }
      }
      break;

    case THREE_WAY_RIGHT_MODIFY:

      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      if( rc==SQLITE_OK && ctx->nIndexes>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              mi->pEdits, mi->aiColumn, mi->nColumn, mi->pKeyInfo,
              mi->iPKey, pChange->intKey, pChange->pKey, pChange->nKey,
              pChange->pBaseVal, pChange->nBaseVal,
              pChange->pTheirVal, pChange->nTheirVal);
        }
      }
      break;

    case THREE_WAY_RIGHT_DELETE:

      rc = prollyMutMapDelete(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey);
      if( rc==SQLITE_OK && ctx->nIndexes>0
       && pChange->pBaseVal && pChange->nBaseVal>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              mi->pEdits, mi->aiColumn, mi->nColumn, mi->pKeyInfo,
              mi->iPKey, pChange->intKey, pChange->pKey, pChange->nKey,
              pChange->pBaseVal, pChange->nBaseVal, 0, 0);
        }
      }
      break;

    case THREE_WAY_CONVERGENT:

      break;

    case THREE_WAY_CONFLICT_MM: {

      u8 *pMerged = 0;
      int nMerged = 0;

      if( pChange->pBaseVal && pChange->nBaseVal>0
       && pChange->pOurVal && pChange->nOurVal>0
       && pChange->pTheirVal && pChange->nTheirVal>0 ){
        pMerged = tryCellMerge(
            pChange->pBaseVal, pChange->nBaseVal,
            pChange->pOurVal, pChange->nOurVal,
            pChange->pTheirVal, pChange->nTheirVal,
            &nMerged);
      }

      if( pMerged ){
        rc = prollyMutMapInsert(ctx->pEdits,
            pChange->pKey, pChange->nKey, pChange->intKey,
            pMerged, nMerged);
        if( rc==SQLITE_OK && ctx->nIndexes>0 ){
          int ix;
          for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
            MergeIndexInfo *mi = &ctx->aIndexes[ix];
            rc = doltliteIndexMutMapRowDelta(
                mi->pEdits, mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                mi->iPKey, pChange->intKey, pChange->pKey, pChange->nKey,
                pChange->pBaseVal, pChange->nBaseVal, pMerged, nMerged);
          }
        }
        sqlite3_free(pMerged);
        break;
      }

    }
    deliberate_fall_through
    case THREE_WAY_CONFLICT_DM: {

      rc = DOLTLITE_GROW_ARRAY(&ctx->aConflicts, &ctx->nConflictsAlloc,
                                ctx->nConflicts + 1, 16);
      if( rc!=SQLITE_OK ) return rc;
      {
        DoltliteConflictRow *cr = &ctx->aConflicts[ctx->nConflicts];
        memset(cr, 0, sizeof(*cr));
        cr->intKey = pChange->intKey;
        if( pChange->pKey && pChange->nKey>0 ){
          rc = doltliteDupBytes(pChange->pKey, pChange->nKey, &cr->pKey);
          if( rc!=SQLITE_OK ) return rc;
          cr->nKey = pChange->nKey;
        }
        if( pChange->pBaseVal && pChange->nBaseVal>0 ){
          rc = doltliteDupBytes(pChange->pBaseVal, pChange->nBaseVal, &cr->pBaseVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nBaseVal = pChange->nBaseVal;
        }
        if( pChange->pOurVal && pChange->nOurVal>0 ){
          rc = doltliteDupBytes(pChange->pOurVal, pChange->nOurVal, &cr->pOurVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nOurVal = pChange->nOurVal;
        }
        if( pChange->pTheirVal && pChange->nTheirVal>0 ){
          rc = doltliteDupBytes(pChange->pTheirVal, pChange->nTheirVal, &cr->pTheirVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            sqlite3_free(cr->pOurVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nTheirVal = pChange->nTheirVal;
        }
        ctx->nConflicts++;
      }
      break;
    }
  }
  return rc;
}

int canFastMerge(
  sqlite3 *db,
  const char *zName,
  int schemaUnchangedBothSides
){
  Table *pTab;
  FKey *pFK;
  int i;

  if( !schemaUnchangedBothSides ) return 0;
  if( !zName || !db ) return 0;

  pTab = sqlite3FindTable(db, zName, 0);
  if( !pTab ) return 0;

  if( pTab->pIndex ) return 0;
  if( pTab->pCheck && pTab->pCheck->nExpr>0 ) return 0;

  for(i=0; i<pTab->nCol; i++){
    Column *pCol = &pTab->aCol[i];
    if( (pCol->colFlags & COLFLAG_PRIMKEY)!=0 ) continue;
    if( pCol->notNull!=OE_None ) return 0;
  }

  for(pFK=pTab->u.tab.pFKey; pFK; pFK=pFK->pNextFrom){
    if( pFK->aAction[0]!=OE_None || pFK->aAction[1]!=OE_None ) return 0;
  }
  for(pFK=sqlite3FkReferences(pTab); pFK; pFK=pFK->pNextTo){
    if( pFK->aAction[0]!=OE_None || pFK->aAction[1]!=OE_None ) return 0;
  }

  return 1;
}

static void freeRowMergeCtx(RowMergeCtx *ctx){
  int i;
  for(i=0; i<ctx->nConflicts; i++){
    doltliteConflictRowFree(&ctx->aConflicts[i]);
  }
  sqlite3_free(ctx->aConflicts);
  if( ctx->pEdits ){
    prollyMutMapFree(ctx->pEdits);
    sqlite3_free(ctx->pEdits);
  }
}

int mergeTableRows(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pnConflicts,
  DoltliteConflictRow **ppConflicts,
  MergeIndexInfo *aIndexes,
  int nIndexes
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);

  RowMergeCtx ctx;
  ProllyMutator mut;
  int rc;
  int i;

  memset(&ctx, 0, sizeof(ctx));
  ctx.isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  ctx.aIndexes = aIndexes;
  ctx.nIndexes = nIndexes;
  ctx.pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !ctx.pEdits ) return SQLITE_NOMEM;
  rc = prollyMutMapInit(ctx.pEdits, ctx.isIntKey);
  if( rc!=SQLITE_OK ){ sqlite3_free(ctx.pEdits); return rc; }

  for(i=0; i<nIndexes; i++){
    aIndexes[i].pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
    if( !aIndexes[i].pEdits ){ rc = SQLITE_NOMEM; goto merge_err; }
    rc = prollyMutMapInit(aIndexes[i].pEdits, 0);
    if( rc!=SQLITE_OK ) goto merge_err;
  }

  rc = prollyThreeWayDiff(cs, cache, pAncRoot, pOursRoot, pTheirsRoot,
                          flags, rowMergeCallback, &ctx);
  if( rc!=SQLITE_OK ) goto merge_err;

  if( !prollyMutMapIsEmpty(ctx.pEdits) ){
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    memcpy(&mut.oldRoot, pOursRoot, sizeof(ProllyHash));
    mut.pEdits = ctx.pEdits;
    mut.flags = flags;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ){
      memcpy(pMergedRoot, &mut.newRoot, sizeof(ProllyHash));
    }
  }else{
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
  }

  for(i=0; i<nIndexes && rc==SQLITE_OK; i++){
    if( !prollyMutMapIsEmpty(aIndexes[i].pEdits) ){
      ProllyMutator idxMut;
      memset(&idxMut, 0, sizeof(idxMut));
      idxMut.pStore = cs;
      idxMut.pCache = cache;
      memcpy(&idxMut.oldRoot, &aIndexes[i].oursRoot, sizeof(ProllyHash));
      idxMut.pEdits = aIndexes[i].pEdits;
      idxMut.flags = PROLLY_NODE_BLOBKEY;
      rc = prollyMutateFlush(&idxMut);
      if( rc==SQLITE_OK ){
        memcpy(&aIndexes[i].mergedRoot, &idxMut.newRoot, sizeof(ProllyHash));
      }
    }else{
      memcpy(&aIndexes[i].mergedRoot, &aIndexes[i].oursRoot, sizeof(ProllyHash));
    }
  }

  *pnConflicts = ctx.nConflicts;
  *ppConflicts = ctx.aConflicts;
  ctx.aConflicts = 0;
  ctx.nConflicts = 0;

merge_err:
  for(i=0; i<nIndexes; i++){
    if( aIndexes[i].pEdits ){
      prollyMutMapFree(aIndexes[i].pEdits);
      sqlite3_free(aIndexes[i].pEdits);
      aIndexes[i].pEdits = 0;
    }
  }
  freeRowMergeCtx(&ctx);
  return rc;
}


#endif
