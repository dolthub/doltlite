#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"
#include "vdbeInt.h"

typedef struct RowMergeCtx RowMergeCtx;
struct RowMergeCtx {
  sqlite3 *db;
  Table *pGeneratedTable;
  sqlite3_stmt *pGeneratedStmt;
  ProllyMutMap *pEdits;
  const MergeRowPolicy *pPolicy;
  u8 isIntKey;
  MergeIndexInfo *aIndexes;
  int nIndexes;
  int nConflicts;

  DoltliteConflictRow *aConflicts;
  int nConflictsAlloc;
};

static int mergeGeneratedField(Table *pTab, int iField){
  int iCol;
  if( !pTab || iField>=pTab->nNVCol ) return 0;
  iCol = HasRowid(pTab)
      ? sqlite3StorageColumnToTable(pTab, iField)
      : sqlite3PrimaryKeyIndex(pTab)->aiColumn[iField];
  return (pTab->aCol[iCol].colFlags & COLFLAG_STORED)!=0;
}


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

/* sqlite_master fields 1/2 are name and tbl_name. Match the base
** row: it carries the ancestor name the policy was built from. */
static int catalogRowNamedInList(
  const char **azNames,
  int nNames,
  const u8 *pBase, int nBase
){
  RecField *aBase = 0;
  int nBaseF = 0;
  int matched = 0;
  int i, f;

  if( !azNames || nNames<=0 ) return 0;
  if( !pBase || nBase<=0 ) return 0;
  /* Field count, or -1; not an SQLITE_ code. */
  if( parseRecordFields(pBase, nBase, &aBase, &nBaseF) < 0 ) return 0;
  for(f=1; f<=2 && !matched; f++){
    if( f>=nBaseF ) break;
    for(i=0; i<nNames && !matched; i++){
      const char *z = azNames[i];
      int n;
      if( !z ) continue;
      n = (int)strlen(z);
      if( aBase[f].len==n
       && sqlite3_strnicmp((const char*)(pBase + aBase[f].off), z, n)==0 ){
        matched = 1;
      }
    }
  }
  sqlite3_free(aBase);
  return matched;
}

static int catalogRowNamedByPolicy(
  const MergeRowPolicy *pPolicy,
  const u8 *pBase, int nBase
){
  if( !pPolicy ) return 0;
  return catalogRowNamedInList(
      pPolicy->azRenameOverDrop, pPolicy->nRenameOverDrop, pBase, nBase);
}

static int catalogRowNamedByDualRename(
  const MergeRowPolicy *pPolicy,
  const u8 *pBase, int nBase
){
  if( !pPolicy ) return 0;
  return catalogRowNamedInList(
      pPolicy->azDualRename, pPolicy->nDualRename, pBase, nBase);
}

static int fieldEquals(const u8 *pRecA, const RecField *fA,
                       const u8 *pRecB, const RecField *fB){
  if(fA->st != fB->st) return 1;
  if(fA->len != fB->len) return 1;
  if(fA->len==0) return 0;
  return memcmp(pRecA + fA->off, pRecB + fB->off, fA->len);
}

static int fieldDecodeInt(const u8 *pRec, const RecField *f, i64 *pOut){
  int st = (int)f->st;
  int nNeed;
  if( !dlSerialIsInt(st) ) return 0;
  if( st==8 ){ *pOut = 0; return 1; }
  if( st==9 ){ *pOut = 1; return 1; }
  nNeed = dlSerialTypeLen((u64)st);
  if( nNeed<0 || f->len<nNeed ) return 0;
  *pOut = dlDecodeSerialInt(st, pRec + f->off, f->len);
  return 1;
}

static int fieldDecodeReal(const u8 *pRec, const RecField *f, double *pOut){
  u64 bits;
  if( f->st!=7 || f->len<8 ) return 0;
  bits = (u64)dlReadIntBytes(pRec + f->off, 8);
  memcpy(pOut, &bits, sizeof(*pOut));
  return 1;
}

/* Untyped columns store 1 and 1.0 as different serial types. SQLite
** compares those numbers as equal, including two widths of one integer. */
static int fieldNumbersEqual(const u8 *pRecA, const RecField *fA,
                             const u8 *pRecB, const RecField *fB){
  int aInt = dlSerialIsInt((int)fA->st);
  int bInt = dlSerialIsInt((int)fB->st);
  i64 iv, iv2;
  double rv;
  if( aInt && bInt ){
    if( !fieldDecodeInt(pRecA, fA, &iv) ) return 0;
    if( !fieldDecodeInt(pRecB, fB, &iv2) ) return 0;
    return iv==iv2;
  }
  if( aInt && fB->st==7 ){
    if( !fieldDecodeInt(pRecA, fA, &iv) ) return 0;
    if( !fieldDecodeReal(pRecB, fB, &rv) ) return 0;
    return sqlite3IntFloatCompare(iv, rv)==0;
  }
  if( bInt && fA->st==7 ){
    if( !fieldDecodeInt(pRecB, fB, &iv) ) return 0;
    if( !fieldDecodeReal(pRecA, fA, &rv) ) return 0;
    return sqlite3IntFloatCompare(iv, rv)==0;
  }
  return 0;
}

/* Every field is byte-identical or the same SQLite number. Trailing
** omitted fields are NULL, matching the cell merge. */
static int recordsNumericallyEqual(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  static const RecField kNullField = { 0, 0, 0 };
  RecField *aA = 0, *aB = 0;
  int nFieldA = 0, nFieldB = 0;
  int nField, i;

  if( parseRecordFields(pA, nA, &aA, &nFieldA)<0 ) return 0;
  if( parseRecordFields(pB, nB, &aB, &nFieldB)<0 ){
    sqlite3_free(aA);
    return 0;
  }
  nField = nFieldA>nFieldB ? nFieldA : nFieldB;
  for(i=0; i<nField; i++){
    const RecField *fA = i<nFieldA ? &aA[i] : &kNullField;
    const RecField *fB = i<nFieldB ? &aB[i] : &kNullField;
    if( fieldEquals(pA, fA, pB, fB)!=0
     && fieldNumbersEqual(pA, fA, pB, fB)==0 ){
      sqlite3_free(aA);
      sqlite3_free(aB);
      return 0;
    }
  }
  sqlite3_free(aA);
  sqlite3_free(aB);
  return 1;
}

/* Index payload for a number: NULL, integer, or IEEE real. Text does not
** qualify, so an empty value is not treated as equal to a text row. */
static int recordIsNumberOrNull(const u8 *pRec, int nRec){
  RecField *a = 0;
  int nField = 0;
  int i;
  if( parseRecordFields(pRec, nRec, &a, &nField)<0 ) return 0;
  for(i=0; i<nField; i++){
    int st = (int)a[i].st;
    if( st!=0 && st!=7 && !dlSerialIsInt(st) ){
      sqlite3_free(a);
      return 0;
    }
  }
  sqlite3_free(a);
  return 1;
}

static int recordsEqualFields(
  const u8 *pA,
  int nA,
  const u8 *pB,
  int nB,
  const int *aiField,
  int nField,
  const int *aiSkip,
  int nSkip,
  int *pbEqual
){
  static const RecField nullField = { 0, 0, 0 };
  RecField *aA = 0, *aB = 0;
  int nFieldA = 0, nFieldB = 0;
  int i, j;

  *pbEqual = 0;
  if( parseRecordFields(pA, nA, &aA, &nFieldA)<0 ) return SQLITE_CORRUPT;
  if( parseRecordFields(pB, nB, &aB, &nFieldB)<0 ){
    sqlite3_free(aA);
    return SQLITE_CORRUPT;
  }
  *pbEqual = 1;
  for(i=0; i<nField; i++){
    int f = aiField[i];
    const RecField *pFieldA = f<nFieldA ? &aA[f] : &nullField;
    const RecField *pFieldB = f<nFieldB ? &aB[f] : &nullField;
    for(j=0; j<nSkip; j++){
      if( aiSkip[j]==f ) break;
    }
    if( j<nSkip ) continue;
    if( fieldEquals(pA, pFieldA, pB, pFieldB)!=0 ){
      *pbEqual = 0;
      break;
    }
  }
  sqlite3_free(aA);
  sqlite3_free(aB);
  return SQLITE_OK;
}

typedef struct MergeWinner MergeWinner;
struct MergeWinner { const u8 *pRec; RecField *pField; };

static u8 *buildMergedRecord(MergeWinner *aWinners, int nFields, int *pnOut){
  static const RecField kNullField = { 0, 0, 0 };
  MergeWinner oneNull;
  int hdrSize = 0, bodySize = 0, pos, i;
  u8 *result;

  /* A header with no serial types is unreadable: column fetch always
  ** parses one type byte and then calls the row corrupt. One explicit
  ** NULL is the empty row. */
  if( nFields<=0 ){
    oneNull.pRec = 0;
    oneNull.pField = (RecField*)&kNullField;
    aWinners = &oneNull;
    nFields = 1;
  }

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

static int fieldIsDualAdd(const MergeRowPolicy *pPolicy, int iField){
  int i;
  if( !pPolicy ) return 0;
  for(i=0; i<pPolicy->nDualAddFields; i++){
    if( pPolicy->aiDualAddFields[i]==iField ) return 1;
  }
  return 0;
}

static u8 *tryCellMerge(
  Table *pGeneratedTable,
  const MergeRowPolicy *pPolicy,
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
    /* Past stored width is a missing field. It compares equal to an
    ** encoded NULL, so an omitted NULL and a stored NULL still merge.
    ** A column added on both sides has no ancestor value; those fields
    ** are changes on each side, then the two values are compared. */
    static const RecField kNullField = { 0, 0, 0 };
    int nEmit = 0;

    winners = sqlite3_malloc(nfMax * (int)sizeof(*winners));
    if(!winners) goto fail;

    for(i=0; i<nfMax; i++){
      RecField *fB = (i<nfBase)   ? &aBase[i]   : (RecField*)&kNullField;
      RecField *fO = (i<nfOurs)   ? &aOurs[i]   : (RecField*)&kNullField;
      RecField *fT = (i<nfTheirs) ? &aTheirs[i] : (RecField*)&kNullField;
      int oursChanged, theirsChanged;
      if( fieldIsDualAdd(pPolicy, i) ){
        oursChanged = 1;
        theirsChanged = 1;
      }else{
        oursChanged   = fieldEquals(pBase, fB, pOurs, fO)!=0;
        theirsChanged = fieldEquals(pBase, fB, pTheirs, fT)!=0;
      }

      if( mergeGeneratedField(pGeneratedTable, i) || !theirsChanged ){

        winners[i].pRec = pOurs; winners[i].pField = fO;
      }else if(!oursChanged){

        winners[i].pRec = pTheirs; winners[i].pField = fT;
      }else if( fieldEquals(pOurs, fO, pTheirs, fT)==0
             || fieldNumbersEqual(pOurs, fO, pTheirs, fT) ){

        winners[i].pRec = pOurs; winners[i].pField = fO;
      }else{
        sqlite3_free(winners); goto fail;
      }
      /* An encoded NULL is stored (serial type 0). Only the synthetic
      ** absent field is left off, so a missing column still takes its
      ** default instead of becoming NULL. */
      if( winners[i].pField!=(RecField*)&kNullField ) nEmit = i+1;
    }

    /* If every winner was absent, keep the full width: a header with no
    ** types does not read, and a shorter row would surface a column
    ** default in place of an explicit NULL. */
    if( nEmit==0 && nfMax>0 ) nEmit = nfMax;
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

static int copyConflictRecord(
  const MergeRowPolicy *pPolicy,
  const u8 *pRecord,
  int nRecord,
  u8 **ppOut,
  int *pnOut
){
  RecField *aFields = 0;
  MergeWinner *aWinners;
  int nFields = 0;
  int i, j, nKeep = 0;

  if( !pPolicy || pPolicy->nDropFields==0 ){
    *pnOut = nRecord;
    return doltliteDupBytes(pRecord, nRecord, ppOut);
  }
  if( parseRecordFields(pRecord, nRecord, &aFields, &nFields)<0 ){
    return SQLITE_CORRUPT;
  }
  aWinners = sqlite3_malloc((nFields+1)*sizeof(*aWinners));
  if( !aWinners ){
    sqlite3_free(aFields);
    return SQLITE_NOMEM;
  }
  for(i=0; i<nFields; i++){
    for(j=0; j<pPolicy->nDropFields; j++){
      if( pPolicy->aiDropFields[j]==i ) break;
    }
    if( j<pPolicy->nDropFields ) continue;
    aWinners[nKeep].pRec = pRecord;
    aWinners[nKeep++].pField = &aFields[i];
  }
  *ppOut = buildMergedRecord(aWinners, nKeep, pnOut);
  sqlite3_free(aWinners);
  sqlite3_free(aFields);
  return *ppOut ? SQLITE_OK : SQLITE_NOMEM;
}

static int rowMergeCallback(void *pCtx, const ThreeWayChange *pChange){
  RowMergeCtx *ctx = (RowMergeCtx*)pCtx;
  int rc = SQLITE_OK;

  switch( pChange->type ){
    case THREE_WAY_LEFT_ADD:
    case THREE_WAY_LEFT_MODIFY: {
      const u8 *pOurs = pChange->pOurVal;
      int nOurs = pChange->nOurVal;
      u8 *pOwned;
      int ix;

      if( !ctx->pGeneratedTable ) break;
      rc = mergeGeneratedSideRow(ctx->db, ctx->pGeneratedTable,
          &ctx->pGeneratedStmt, pChange->intKey, &pOurs, &nOurs, &pOwned);
      if( rc!=SQLITE_OK ) return rc;
      if( nOurs!=pChange->nOurVal
       || memcmp(pOurs, pChange->pOurVal, nOurs)!=0 ){
        rc = prollyMutMapInsert(ctx->pEdits,
            pChange->pKey, pChange->nKey, pChange->intKey, pOurs, nOurs);
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              pChange->pOurVal, pChange->nOurVal, pOurs, nOurs, &mi->part);
        }
      }
      sqlite3_free(pOwned);
      break;
    }

    case THREE_WAY_LEFT_DELETE:
      break;

    case THREE_WAY_RIGHT_ADD: {
      const u8 *pTheirs;
      int nTheirs;
      u8 *pOwned;

      pTheirs = pChange->pTheirVal;
      nTheirs = pChange->nTheirVal;
      rc = mergeGeneratedSideRow(ctx->db, ctx->pGeneratedTable,
          &ctx->pGeneratedStmt, pChange->intKey, &pTheirs, &nTheirs, &pOwned);
      if( rc!=SQLITE_OK ) return rc;
      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pTheirs, nTheirs);
      if( rc==SQLITE_OK && ctx->nIndexes>0
       && pTheirs && nTheirs>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              0, 0, pTheirs, nTheirs, &mi->part);
        }
      }
      sqlite3_free(pOwned);
      break;
    }

    case THREE_WAY_RIGHT_MODIFY: {
      const u8 *pTheirs;
      int nTheirs;
      u8 *pOwned;

      pTheirs = pChange->pTheirVal;
      nTheirs = pChange->nTheirVal;
      rc = mergeGeneratedSideRow(ctx->db, ctx->pGeneratedTable,
          &ctx->pGeneratedStmt, pChange->intKey, &pTheirs, &nTheirs, &pOwned);
      if( rc!=SQLITE_OK ) return rc;
      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pTheirs, nTheirs);
      if( rc==SQLITE_OK && ctx->nIndexes>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              pChange->pBaseVal, pChange->nBaseVal,
              pTheirs, nTheirs, &mi->part);
        }
      }
      sqlite3_free(pOwned);
      break;
    }

    case THREE_WAY_RIGHT_DELETE:
      rc = prollyMutMapDelete(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey);
      if( rc==SQLITE_OK && ctx->nIndexes>0
       && pChange->pBaseVal && pChange->nBaseVal>0 ){
        int ix;
        for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = doltliteIndexMutMapRowDelta(
              ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
              mi->pKeyInfo, mi->iPKey, pChange->intKey,
              pChange->pKey, pChange->nKey,
              pChange->pBaseVal, pChange->nBaseVal, 0, 0, &mi->part);
        }
      }
      break;

    case THREE_WAY_CONVERGENT:
      break;

    case THREE_WAY_CONFLICT_MM: {

      u8 *pMerged = 0;
      int nMerged = 0;

      /* Both sides inserted this key. A unique index sorts 1 and 1.0
      ** together. The integer fits in the key, so its value is empty;
      ** the real is stored beside the key. The merged tree is ours. */
      if( !pChange->pBaseVal || pChange->nBaseVal<=0 ){
        int oursHas = pChange->pOurVal && pChange->nOurVal>0;
        int theirsHas = pChange->pTheirVal && pChange->nTheirVal>0;
        const u8 *pNum = 0;
        int nNum = 0;
        if( oursHas!=theirsHas ){
          pNum = oursHas ? pChange->pOurVal : pChange->pTheirVal;
          nNum = oursHas ? pChange->nOurVal : pChange->nTheirVal;
        }
        if( (!oursHas && !theirsHas)
         || (pNum && recordIsNumberOrNull(pNum, nNum))
         || (oursHas && theirsHas
             && recordsNumericallyEqual(pChange->pOurVal, pChange->nOurVal,
                                         pChange->pTheirVal,
                                         pChange->nTheirVal)) ){
          break;
        }
      }

      if( pChange->pBaseVal && pChange->nBaseVal>0
       && pChange->pOurVal && pChange->nOurVal>0
       && pChange->pTheirVal && pChange->nTheirVal>0 ){
        pMerged = tryCellMerge(
            ctx->pGeneratedTable, ctx->pPolicy,
            pChange->pBaseVal, pChange->nBaseVal,
            pChange->pOurVal, pChange->nOurVal,
            pChange->pTheirVal, pChange->nTheirVal,
            &nMerged);
      }

      if( pMerged ){
        rc = mergeGeneratedRecord(ctx->db, ctx->pGeneratedTable,
             &ctx->pGeneratedStmt, pChange->intKey, &pMerged, &nMerged);
        if( rc!=SQLITE_OK ){
          sqlite3_free(pMerged);
          return rc;
        }
        rc = prollyMutMapInsert(ctx->pEdits,
            pChange->pKey, pChange->nKey, pChange->intKey,
            pMerged, nMerged);
        if( rc==SQLITE_OK && ctx->nIndexes>0 ){
          int ix;
          for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
            MergeIndexInfo *mi = &ctx->aIndexes[ix];
            rc = doltliteIndexMutMapRowDelta(
                ctx->db, mi->pIdx, mi->pEdits, mi->aiColumn, mi->nColumn,
                mi->pKeyInfo, mi->iPKey, pChange->intKey,
                pChange->pKey, pChange->nKey,
                pChange->pOurVal, pChange->nOurVal, pMerged, nMerged, &mi->part);
          }
        }
        sqlite3_free(pMerged);
        break;
      }

      if( catalogRowNamedByDualRename(ctx->pPolicy,
                                      pChange->pBaseVal, pChange->nBaseVal) ){
        break;
      }

    }
    deliberate_fall_through
    case THREE_WAY_CONFLICT_DM: {

      if( pChange->type==THREE_WAY_CONFLICT_DM && ctx->pPolicy
       && ctx->pPolicy->nDeleteCompareFields>0 ){
        const u8 *pSurv = pChange->pOurVal ? pChange->pOurVal
                                           : pChange->pTheirVal;
        int nSurv = pChange->pOurVal ? pChange->nOurVal : pChange->nTheirVal;
        int bSharedEqual = 0;
        rc = recordsEqualFields(
            pChange->pBaseVal, pChange->nBaseVal,
            pSurv, nSurv, ctx->pPolicy->aiDeleteCompareFields,
            ctx->pPolicy->nDeleteCompareFields,
            ctx->pPolicy->aiDropFields,
            (pChange->pOurVal!=0)==ctx->pPolicy->bSchemaIsTheirs
              ? ctx->pPolicy->nDropFields : 0, &bSharedEqual);
        if( rc!=SQLITE_OK ) return rc;
        if( bSharedEqual ){
          if( pChange->pOurVal && !pChange->pTheirVal ){
            rc = prollyMutMapDelete(ctx->pEdits,
                pChange->pKey, pChange->nKey, pChange->intKey);
            if( rc==SQLITE_OK && ctx->nIndexes>0 ){
              int ix;
              for(ix=0; ix<ctx->nIndexes && rc==SQLITE_OK; ix++){
                MergeIndexInfo *mi = &ctx->aIndexes[ix];
                rc = doltliteIndexMutMapRowDelta(
                    ctx->db, mi->pIdx, mi->pEdits,
                    mi->aiColumn, mi->nColumn, mi->pKeyInfo, mi->iPKey,
                    pChange->intKey, pChange->pKey, pChange->nKey,
                    pChange->pOurVal, pChange->nOurVal, 0, 0, &mi->part);
              }
            }
          }
          break;
        }
      }

      /* Policy-named catalog row (rename vs drop): rename wins, as Dolt.
      ** Inferring here would also pick a winner for a dual rename. */
      if( pChange->type==THREE_WAY_CONFLICT_DM
       && catalogRowNamedByPolicy(ctx->pPolicy,
                                  pChange->pBaseVal, pChange->nBaseVal) ){
        const u8 *pSurv = pChange->pOurVal ? pChange->pOurVal
                                           : pChange->pTheirVal;
        int nSurv = pChange->pOurVal ? pChange->nOurVal : pChange->nTheirVal;
        if( pSurv && nSurv>0 ){
          rc = prollyMutMapInsert(ctx->pEdits,
              pChange->pKey, pChange->nKey, pChange->intKey, pSurv, nSurv);
          break;
        }
      }

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
          rc = copyConflictRecord(ctx->pPolicy,
              pChange->pBaseVal, pChange->nBaseVal,
              &cr->pBaseVal, &cr->nBaseVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
        }
        if( pChange->pOurVal && pChange->nOurVal>0 ){
          rc = copyConflictRecord(ctx->pPolicy,
              pChange->pOurVal, pChange->nOurVal,
              &cr->pOurVal, &cr->nOurVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
        }
        if( pChange->pTheirVal && pChange->nTheirVal>0 ){
          rc = copyConflictRecord(ctx->pPolicy,
              pChange->pTheirVal, pChange->nTheirVal,
              &cr->pTheirVal, &cr->nTheirVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            sqlite3_free(cr->pOurVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
        }
        ctx->nConflicts++;
      }
      break;
    }
  }
  return rc;
}

static int layoutCellsMatch(const u8 *pRec, const RecField *aF, int nF,
                            int iA, int iB){
  if( iA<0 || iB<0 || iA>=nF || iB>=nF ) return 1;
  return fieldEquals(pRec, &aF[iA], pRec, &aF[iB])==0;
}

/* Could drops, re-adds and renames to fresh names have produced the side's
** layout while leaving this row's bytes as they are? Drops keep the
** survivors' order and cells, and ADD COLUMN appends, so the side must be
** an in-order run of surviving ancestor columns holding their own cells,
** followed by added columns holding their default or no cell. A column
** whose default is not NULL, or whose cell is not stored, is taken to match. */
static int layoutFitsRow(
  const u8 *pRec, int nRec,
  const MergeLayout *pL,
  u8 *aTailOk
){
  RecField *aF = 0;
  int nF = 0, p, last = -1, bFits = 0;

  if( parseRecordFields(pRec, nRec, &aF, &nF)<0 ) return 1;
  aTailOk[pL->nCol] = 1;
  for(p=pL->nCol-1; p>=0; p--){
    const MergeLayoutCol *pC = &pL->aCol[p];
    aTailOk[p] = aTailOk[p+1]
        && !(pC->bAddsAsNull && pC->iField>=0 && pC->iField<nF
             && aF[pC->iField].st!=0);
  }
  for(p=0; p<=pL->nCol; p++){
    const MergeLayoutCol *pC;
    int s;
    if( aTailOk[p] ){
      bFits = 1;
      break;
    }
    if( p==pL->nCol ) break;
    pC = &pL->aCol[p];
    if( pC->iSrcSlot>=0 ){
      s = pC->iSrcSlot;
      if( s<=last
       || !layoutCellsMatch(pRec, aF, nF, pC->iField, pL->aAncField[s]) ){
        break;
      }
    }else{
      for(s=last+1; s<pL->nAnc; s++){
        if( layoutCellsMatch(pRec, aF, nF, pC->iField, pL->aAncField[s]) ){
          break;
        }
      }
      if( s>=pL->nAnc ) break;
    }
    last = s;
  }
  sqlite3_free(aF);
  return bFits;
}

/* A rename that reuses a name. bOtherUnchanged: the other side kept the
** ancestor schema, so any row this layout cannot explain as a drop and
** re-add is ambiguous even if this side rewrote it. Otherwise only a row
** still byte-for-byte from the ancestor counts. */
static int mergeSideKeptAncestorRow(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pSideRoot,
  u8 ancFlags,
  u8 sideFlags,
  const MergeLayout *pLayout,
  int bOtherUnchanged,
  int *pbKept
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  ProllyCursor cur;
  u8 *aTailOk = 0;
  u64 nAnc = 0, nTouched = 0;
  int res = 0;
  int rc;

  *pbKept = 0;
  if( !cs || !pCache || prollyHashIsEmpty(pAncRoot) ) return SQLITE_OK;
  if( prollyHashIsEmpty(pSideRoot) ) return SQLITE_OK;
  aTailOk = sqlite3_malloc64((u64)pLayout->nCol + 1);
  if( !aTailOk ) return SQLITE_NOMEM;
  prollyCursorInit(&cur, cs, pCache, pAncRoot, ancFlags);
  rc = prollyCursorFirst(&cur, &res);
  while( rc==SQLITE_OK && !res && prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0;
    int nVal = 0;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( !layoutFitsRow(pVal, nVal, pLayout, aTailOk) ) nAnc++;
    rc = prollyCursorNext(&cur);
  }
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK || nAnc==0 ) goto done;
  if( prollyHashCompare(pAncRoot, pSideRoot)==0 || bOtherUnchanged ){
    *pbKept = 1;
    goto done;
  }
  memset(&iter, 0, sizeof(iter));
  rc = prollyDiffIterOpen(&iter, cs, pCache, pAncRoot, pSideRoot,
                          ancFlags, sideFlags);
  if( rc!=SQLITE_OK ) goto done;
  while( nTouched<nAnc
      && (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW ){
    if( (pChange->type==PROLLY_DIFF_MODIFY
      || pChange->type==PROLLY_DIFF_DELETE)
     && !layoutFitsRow(pChange->pOldVal, pChange->nOldVal, pLayout,
                       aTailOk) ){
      nTouched++;
    }
  }
  prollyDiffIterClose(&iter);
  if( rc==SQLITE_ROW || rc==SQLITE_DONE ){
    rc = SQLITE_OK;
    *pbKept = nTouched<nAnc;
  }
done:
  sqlite3_free(aTailOk);
  return rc;
}

/* Record fields skip VIRTUAL columns, so a later column's stored
** index is the count of non-VIRTUAL columns before it, not its
** CREATE TABLE ordinal. A VIRTUAL column has no stored value. */
int mergeStoredFieldIndex(ParsedColumn *aCols, int iCol){
  int i, n = 0;
  if( iCol<0 || parsedColumnIsVirtual(&aCols[iCol]) ) return -1;
  for(i=0; i<iCol; i++){
    if( !parsedColumnIsVirtual(&aCols[i]) ) n++;
  }
  return n;
}

static int colInfoIndex(const DoltliteColInfo *ci, const char *zName){
  int i;
  if( !ci || !zName ) return -1;
  for(i=0; i<ci->nCol; i++){
    if( ci->azName[i] && sqlite3_stricmp(ci->azName[i], zName)==0 ) return i;
  }
  return -1;
}

/* Omitted trailing fields are NULL. VIRTUAL slots are not compared. */
static int storedFieldsEqual(
  const u8 *pA, int nA, const DoltliteRecordInfo *pAi, int iA,
  const u8 *pB, int nB, const DoltliteRecordInfo *pBi, int iB
){
  int aNull, bNull;
  if( iA<0 || iB<0 ) return 1;
  aNull = iA>=pAi->nField || pAi->aType[iA]==0;
  bNull = iB>=pBi->nField || pBi->aType[iB]==0;
  if( aNull || bNull ) return aNull && bNull;
  return doltliteFieldValuesEqual(
      pAi->aType[iA], pA, nA, pAi->aOffset[iA],
      pBi->aType[iB], pB, nB, pBi->aOffset[iB]);
}

static int loadReaderCols(
  const char *zSql, const char *zTable, DoltliteColInfo *ci
){
  sqlite3 *tmp = 0;
  int rc;
  memset(ci, 0, sizeof(*ci));
  if( !zSql || !zTable ) return SQLITE_OK;
  rc = sqlite3_open(":memory:", &tmp);
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, zSql, 0, 0, 0);
  if( rc==SQLITE_OK ) rc = doltliteGetReaderColumnNames(tmp, zTable, ci);
  if( tmp ) sqlite3_close(tmp);
  return rc;
}

/* 1 when reader columns are the parsed declared list, so ancestor slots
** line up with the merge's column indexes. 0 when they do not. -1 on OOM. */
static int colsMatchParsed(const char *zSql, const DoltliteColInfo *ci){
  ParsedColumn *a = 0;
  int n = 0, i, rc;
  rc = parseColumns(zSql, &a, &n);
  if( rc!=SQLITE_OK ){
    freeColumns(a, n);
    return rc==SQLITE_NOMEM ? -1 : 0;
  }
  rc = n==ci->nCol;
  for(i=0; rc==1 && i<n; i++){
    if( !a[i].zName || !ci->azName[i]
     || sqlite3_stricmp(a[i].zName, ci->azName[i])!=0 ) rc = 0;
  }
  freeColumns(a, n);
  return rc;
}
static int recSlot(const DoltliteColInfo *ci, int i){
  if( !ci || i<0 || i>=ci->nCol ) return -1;
  return ci->aColToRec ? ci->aColToRec[i] : i;
}

/* Names say each side column is that ancestor column. */
static int rowFitsColumnNames(
  const DoltliteColInfo *pSide, const DoltliteColInfo *pAnc,
  const u8 *pSideRec, int nSideRec, const DoltliteRecordInfo *pSideInfo,
  const u8 *pAncRec, int nAncRec, const DoltliteRecordInfo *pAncInfo
){
  int i;
  for(i=0; i<pSide->nCol; i++){
    int k = colInfoIndex(pAnc, pSide->azName[i]);
    if( k<0 ) return 0;
    if( !storedFieldsEqual(pSideRec, nSideRec, pSideInfo, recSlot(pSide, i),
                           pAncRec, nAncRec, pAncInfo, recSlot(pAnc, k)) ){
      return 0;
    }
  }
  return 1;
}

/* In-order ancestor column whose cell the side column still holds.
** A value that matches two ancestor columns does not decide. */
static int assignColumnsByCells(
  const DoltliteColInfo *pSide, const DoltliteColInfo *pAnc,
  const u8 *pSideRec, int nSideRec, const DoltliteRecordInfo *pSideInfo,
  const u8 *pAncRec, int nAncRec, const DoltliteRecordInfo *pAncInfo,
  int *aSideAnc
){
  int i, next = 0;
  for(i=0; i<pSide->nCol; i++){
    int iS = recSlot(pSide, i);
    int nHit = 0, hit = -1, k;
    if( iS<0 ){
      aSideAnc[i] = colInfoIndex(pAnc, pSide->azName[i]);
      continue;
    }
    for(k=next; k<pAnc->nCol; k++){
      int iA = recSlot(pAnc, k);
      if( iA<0 ) continue;
      if( storedFieldsEqual(pSideRec, nSideRec, pSideInfo, iS,
                            pAncRec, nAncRec, pAncInfo, iA) ){
        nHit++;
        hit = k;
        if( nHit>1 ) return 0;
      }
    }
    if( nHit!=1 ) return 0;
    aSideAnc[i] = hit;
    next = hit+1;
  }
  return 1;
}

int mergeRenameHoldingDroppedName(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pSideRoot,
  u8 ancFlags,
  u8 sideFlags,
  const char *zAncSql,
  const char *zSideSql,
  const char *zTable,
  int *aSideAnc,
  int nSide,
  char **pzReused
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DoltliteColInfo ancCi, sideCi;
  ProllyCursor ancCur, sideCur;
  DoltliteRecordInfo ancInfo, sideInfo;
  int *aAssign = 0;
  int sawName = 0, sawAssign = 0, bShifted = 0;
  int ancInit = 0, sideInit = 0;
  int i, res = 0, rc = SQLITE_OK;

  if( pzReused ) *pzReused = 0;
  memset(&ancCi, 0, sizeof(ancCi));
  memset(&sideCi, 0, sizeof(sideCi));
  if( !cs || !pCache || !zAncSql || !zSideSql || !zTable ) return SQLITE_OK;
  if( prollyHashIsEmpty(pAncRoot) || prollyHashIsEmpty(pSideRoot) ){
    return SQLITE_OK;
  }
  if( ((ancFlags ^ sideFlags) & PROLLY_NODE_INTKEY)!=0 ) return SQLITE_OK;
  rc = loadReaderCols(zAncSql, zTable, &ancCi);
  if( rc==SQLITE_OK ) rc = loadReaderCols(zSideSql, zTable, &sideCi);
  if( rc!=SQLITE_OK || sideCi.nCol<=0 || sideCi.nCol>=ancCi.nCol ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }
  for(i=0; i<sideCi.nCol; i++){
    int k = colInfoIndex(&ancCi, sideCi.azName[i]);
    if( k>=0 && k!=i ) bShifted = 1;
  }
  if( !bShifted ) goto done;
  if( aSideAnc && nSide!=sideCi.nCol ) goto done;
  i = colsMatchParsed(zAncSql, &ancCi);
  if( i<0 || (i==1 && (i = colsMatchParsed(zSideSql, &sideCi))<0) ){
    rc = SQLITE_NOMEM;
    goto done;
  }
  if( i!=1 ) goto done;
  aAssign = sqlite3_malloc(sideCi.nCol * (int)sizeof(int));
  if( !aAssign ){ rc = SQLITE_NOMEM; goto done; }
  doltliteRecordInfoInit(&ancInfo);
  doltliteRecordInfoInit(&sideInfo);
  prollyCursorInit(&ancCur, cs, pCache, pAncRoot, ancFlags);
  prollyCursorInit(&sideCur, cs, pCache, pSideRoot, sideFlags);
  ancInit = 1;
  sideInit = 1;
  rc = prollyCursorFirst(&ancCur, &res);
  while( rc==SQLITE_OK && !res && prollyCursorIsValid(&ancCur) ){
    const u8 *pAncVal = 0, *pSideVal = 0, *pKey = 0;
    int nAncVal = 0, nSideVal = 0, nKey = 0, sideRes = 0;
    prollyCursorValue(&ancCur, &pAncVal, &nAncVal);
    if( (ancFlags & PROLLY_NODE_INTKEY)!=0 ){
      rc = prollyCursorSeekInt(&sideCur, prollyCursorIntKey(&ancCur), &sideRes);
    }else{
      prollyCursorKey(&ancCur, &pKey, &nKey);
      rc = prollyCursorSeekBlob(&sideCur, pKey, nKey, &sideRes);
    }
    if( rc!=SQLITE_OK ) break;
    if( sideRes==0 && prollyCursorIsValid(&sideCur) ){
      prollyCursorValue(&sideCur, &pSideVal, &nSideVal);
      doltliteParseRecord(pAncVal, nAncVal, &ancInfo);
      doltliteParseRecord(pSideVal, nSideVal, &sideInfo);
      if( rowFitsColumnNames(&sideCi, &ancCi, pSideVal, nSideVal, &sideInfo,
                             pAncVal, nAncVal, &ancInfo) ){
        sawName = 1;
      }else if( !sawAssign ){
        sawAssign = assignColumnsByCells(
            &sideCi, &ancCi, pSideVal, nSideVal, &sideInfo,
            pAncVal, nAncVal, &ancInfo, aAssign);
      }else{
        int *aTmp = sqlite3_malloc(sideCi.nCol * (int)sizeof(int));
        int same = 1;
        if( !aTmp ){ rc = SQLITE_NOMEM; break; }
        if( assignColumnsByCells(&sideCi, &ancCi, pSideVal, nSideVal, &sideInfo,
                                 pAncVal, nAncVal, &ancInfo, aTmp) ){
          for(i=0; i<sideCi.nCol; i++) if( aTmp[i]!=aAssign[i] ) same = 0;
          if( !same ) sawAssign = 0;
        }
        sqlite3_free(aTmp);
        if( !sawAssign ) break;
      }
    }
    rc = prollyCursorNext(&ancCur);
  }
  doltliteRecordInfoClear(&ancInfo);
  doltliteRecordInfoClear(&sideInfo);
  if( rc==SQLITE_OK && !sawName && sawAssign && pzReused ){
    const char *zReuse = 0;
    for(i=0; i<sideCi.nCol; i++){
      int k = colInfoIndex(&ancCi, sideCi.azName[i]);
      if( k>=0 && k!=aAssign[i] ){ zReuse = sideCi.azName[i]; break; }
    }
    if( zReuse ){
      *pzReused = sqlite3_mprintf("%s", zReuse);
      if( !*pzReused ) rc = SQLITE_NOMEM;
      else if( aSideAnc ) memcpy(aSideAnc, aAssign, (size_t)nSide*sizeof(int));
    }
  }
done:
  if( ancInit ) prollyCursorClose(&ancCur);
  if( sideInit ) prollyCursorClose(&sideCur);
  sqlite3_free(aAssign);
  doltliteFreeColInfo(&ancCi);
  doltliteFreeColInfo(&sideCi);
  return rc;
}

/* Without column tags a column is matched by name, so a rename onto a name
** another ancestor column had (a swap, or b->c then a->b) would route the
** other side's cells into the wrong column. A name that moved slots can
** also come from dropping and re-adding a column; refuse only when a row the
** side kept rules that out. Fewer columns are the same trap when the rename
** took a dropped column's name: the kept cells no longer match that name.
** Only the incoming side is refused then. The branch that did the rename
** still accepts the other side's rows, mapped by those cells. */
int mergePass1CheckRenameReusingColumnName(MergePass1Ctx *c){
  int side, i, j;

  for(side=0; side<2; side++){
    SchemaEntry *aRen = side ? c->aTheirsSchema : c->aOursSchema;
    int nRen = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aOth = side ? c->aOursSchema : c->aTheirsSchema;
    int nOth = side ? c->nOursSchema : c->nTheirsSchema;
    struct TableEntry *aRenCat = side ? c->aTheirs : c->aOurs;
    int nRenCat = side ? c->nTheirs : c->nOurs;
    struct TableEntry *aOthCat = side ? c->aOurs : c->aTheirs;
    int nOthCat = side ? c->nOurs : c->nTheirs;

    for(i=0; i<c->nAncSchema; i++){
      const char *zTable = c->aAncSchema[i].zName;
      SchemaEntry *pRenSe, *pOthSe;
      struct TableEntry *pAncCat, *pRenCatEnt, *pOthCatEnt;
      ParsedColumn *aAncCols = 0, *aRenCols = 0;
      int nAncCols = 0, nRenCols = 0;
      const char *zMoved = 0;
      MergeLayoutCol *aLayoutCol = 0;
      int *aAncField = 0;
      int bKept = 0;
      int bReuseDecided = 0;
      int rc;

      if( !zTable || !c->aAncSchema[i].zType ) continue;
      if( strcmp(c->aAncSchema[i].zType, "table")!=0 ) continue;
      pRenSe = findSchemaEntry(aRen, nRen, zTable);
      pOthSe = findSchemaEntry(aOth, nOth, zTable);
      if( !pRenSe || !pOthSe || !pRenSe->zSql || !pOthSe->zSql ) continue;
      if( strcmp(pRenSe->zSql, c->aAncSchema[i].zSql)==0 ) continue;
      if( strcmp(pRenSe->zSql, pOthSe->zSql)==0 ) continue;
      pAncCat = doltliteFindTableByName(c->aAnc, c->nAnc, zTable);
      pRenCatEnt = doltliteFindTableByName(aRenCat, nRenCat, zTable);
      pOthCatEnt = doltliteFindTableByName(aOthCat, nOthCat, zTable);
      if( !pAncCat || !pRenCatEnt || !pOthCatEnt ) continue;
      if( strcmp(pOthSe->zSql, c->aAncSchema[i].zSql)==0
       && prollyHashCompare(&pAncCat->root, &pOthCatEnt->root)==0 ){
        continue;
      }
      if( parseColumns(c->aAncSchema[i].zSql, &aAncCols, &nAncCols)!=SQLITE_OK ){
        continue;
      }
      if( parseColumns(pRenSe->zSql, &aRenCols, &nRenCols)!=SQLITE_OK ){
        freeColumns(aAncCols, nAncCols);
        continue;
      }
      if( nRenCols>=nAncCols ){
        for(j=0; j<nAncCols && !zMoved; j++){
          int k;
          if( sqlite3_stricmp(aRenCols[j].zName, aAncCols[j].zName)==0 ) continue;
          k = parsedColumnIndexByName(aAncCols, nAncCols, aRenCols[j].zName);
          if( k>=0 && k!=j ) zMoved = aRenCols[j].zName;
        }
      }else if( side==1 ){
        char *zReuse = 0;
        rc = mergeRenameHoldingDroppedName(
            c->db, &pAncCat->root, &pRenCatEnt->root,
            pAncCat->flags, pRenCatEnt->flags,
            c->aAncSchema[i].zSql, pRenSe->zSql, zTable,
            0, 0, &zReuse);
        if( rc!=SQLITE_OK ){
          freeColumns(aAncCols, nAncCols);
          freeColumns(aRenCols, nRenCols);
          return rc;
        }
        if( zReuse ){
          int kReuse = parsedColumnIndexByName(aRenCols, nRenCols, zReuse);
          sqlite3_free(zReuse);
          if( kReuse>=0 ){
            zMoved = aRenCols[kReuse].zName;
            bKept = 1;
            bReuseDecided = 1;
          }
        }
      }
      rc = SQLITE_OK;
      if( zMoved && !bReuseDecided ){
        aLayoutCol = sqlite3_malloc64(sizeof(MergeLayoutCol)*(u64)nRenCols);
        aAncField = sqlite3_malloc64(sizeof(int)*(u64)nAncCols);
        if( !aLayoutCol || !aAncField ) rc = SQLITE_NOMEM;
      }
      if( rc==SQLITE_OK && zMoved && !bReuseDecided ){
        MergeLayout layout;
        for(j=0; j<nAncCols; j++){
          aAncField[j] = mergeStoredFieldIndex(aAncCols, j);
        }
        for(j=0; j<nRenCols; j++){
          aLayoutCol[j].iField = mergeStoredFieldIndex(aRenCols, j);
          aLayoutCol[j].iSrcSlot =
              parsedColumnIndexByName(aAncCols, nAncCols, aRenCols[j].zName);
          aLayoutCol[j].bAddsAsNull = (u8)parsedColumnAddsAsNull(&aRenCols[j]);
        }
        layout.aCol = aLayoutCol;
        layout.nCol = nRenCols;
        layout.aAncField = aAncField;
        layout.nAnc = nAncCols;
        rc = mergeSideKeptAncestorRow(c->db, &pAncCat->root, &pRenCatEnt->root,
                                      pAncCat->flags, pRenCatEnt->flags, &layout,
                                      strcmp(pOthSe->zSql, c->aAncSchema[i].zSql)==0,
                                      &bKept);
      }
      if( rc==SQLITE_OK && zMoved && c->pzErrMsg && bKept ){
        sqlite3_free(*c->pzErrMsg);
        *c->pzErrMsg = sqlite3_mprintf(
            "cannot %s: table '%s' renames a column to '%s', a name another "
            "of its columns had, so the column each change belongs to is "
            "ambiguous; make the same renames on both branches first",
            c->bBranchMerge ? "merge" : "apply", zTable, zMoved);
      }
      sqlite3_free(aLayoutCol);
      sqlite3_free(aAncField);
      freeColumns(aAncCols, nAncCols);
      freeColumns(aRenCols, nRenCols);
      if( rc!=SQLITE_OK ) return rc;
      if( zMoved && bKept ) return SQLITE_ERROR;
    }
  }
  return SQLITE_OK;
}

/* Other side changed this field in a pre-existing row. Drop vs that
** edit is a Dolt conflict; adds/removes and other columns are not. */
int mergeRowEditsColumn(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOtherRoot,
  u8 ancFlags,
  u8 otherFlags,
  int iField,
  int bNonNullOnly,
  int *pbEdited
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  int rc;

  *pbEdited = 0;
  if( !cs || !pCache || iField<0 ) return SQLITE_OK;
  if( prollyHashIsEmpty(pAncRoot) || prollyHashIsEmpty(pOtherRoot) ){
    return SQLITE_OK;
  }
  memset(&iter, 0, sizeof(iter));
  rc = prollyDiffIterOpen(&iter, cs, pCache, pAncRoot, pOtherRoot,
                          ancFlags, otherFlags);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW ){
    RecField *aOld = 0;
    RecField *aNew = 0;
    static const RecField kNullField = { 0, 0, 0 };
    int nOld = 0, nNew = 0;
    if( pChange->type!=PROLLY_DIFF_MODIFY ) continue;
    if( !pChange->pOldVal || !pChange->pNewVal ) continue;
    if( parseRecordFields(pChange->pOldVal, pChange->nOldVal, &aOld, &nOld)<0 ){
      continue;
    }
    if( parseRecordFields(pChange->pNewVal, pChange->nNewVal, &aNew, &nNew)<0 ){
      sqlite3_free(aOld);
      continue;
    }
    if( (!bNonNullOnly || (iField<nNew && aNew[iField].st!=0))
     && fieldEquals(pChange->pOldVal,
                    iField<nOld ? &aOld[iField] : (RecField*)&kNullField,
                    pChange->pNewVal,
                    iField<nNew ? &aNew[iField] : (RecField*)&kNullField)!=0 ){
      *pbEdited = 1;
    }
    sqlite3_free(aOld);
    sqlite3_free(aNew);
    if( *pbEdited ) break;
  }
  prollyDiffIterClose(&iter);
  return rc==SQLITE_ROW || rc==SQLITE_DONE ? SQLITE_OK : rc;
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
  if( pTab->tabFlags & TF_HasStored ) return 0;

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
  sqlite3_finalize(ctx->pGeneratedStmt);
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
  Table *pTab,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  u8 ancFlags,
  u8 theirsFlags,
  ProllyHash *pMergedRoot,
  int *pnConflicts,
  DoltliteConflictRow **ppConflicts,
  MergeIndexInfo *aIndexes,
  int nIndexes,
  const MergeRowPolicy *pPolicy
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);

  RowMergeCtx ctx;
  ProllyMutator mut;
  int rc;
  int i;

  if( ((flags ^ theirsFlags) & PROLLY_NODE_INTKEY)!=0 ){
    return SQLITE_ERROR;
  }
  if( !prollyHashIsEmpty(pAncRoot)
   && ((flags ^ ancFlags) & PROLLY_NODE_INTKEY)!=0 ){
    return SQLITE_ERROR;
  }

  memset(&ctx, 0, sizeof(ctx));
  ctx.db = db;
  if( pTab && (pTab->tabFlags & TF_HasStored)!=0 ){
    ctx.pGeneratedTable = pTab;
  }
  ctx.isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  ctx.pPolicy = pPolicy;
  ctx.aIndexes = aIndexes;
  ctx.nIndexes = nIndexes;
  ctx.pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !ctx.pEdits ) return SQLITE_NOMEM;
  rc = prollyMutMapInit(ctx.pEdits, ctx.isIntKey);
  if( rc!=SQLITE_OK ){ sqlite3_free(ctx.pEdits); return rc; }

  for(i=0; i<nIndexes; i++){
    aIndexes[i].pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
    if( !aIndexes[i].pEdits ){ rc = SQLITE_NOMEM; goto merge_err; }
    /* Index edits arrive unordered in index-key space; sort once at flush. */
    rc = prollyMutMapInitMode(aIndexes[i].pEdits, 0, 0);
    if( rc!=SQLITE_OK ) goto merge_err;
  }

  rc = prollyThreeWayDiff(cs, cache, pAncRoot, pOursRoot, pTheirsRoot,
                          ancFlags, flags, theirsFlags,
                          rowMergeCallback, &ctx);
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
