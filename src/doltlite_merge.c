
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "prolly_three_way_diff.h"
#include "prolly_three_way_merge.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include "prolly_cache.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "sortkey.h"
#include <string.h>
#include <ctype.h>

typedef struct ConflictTableInfo ConflictTableInfo;
extern int doltliteSerializeConflicts(ChunkStore *cs, ConflictTableInfo *aTables,
                                       int nTables, ProllyHash *pHash);

static struct TableEntry *findTableEntry(
  struct TableEntry *aEntries,
  int nEntries,
  Pgno iTable
){
  return doltliteFindTableByNumber(aEntries, nEntries, iTable);
}

typedef struct MergeIndexInfo MergeIndexInfo;
struct MergeIndexInfo {
  Pgno iTable;
  ProllyHash oursRoot;
  ProllyHash mergedRoot;
  ProllyMutMap *pEdits;
  int nColumn;
  i16 *aiColumn;
  KeyInfo *pKeyInfo;
};

static int buildIndexSortKey(
  const u8 *pRec, int nRec,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  u8 **ppKey, int *pnKey
){
  DoltliteRecordInfo info;
  u8 *pIdxRec = 0;
  int nIdxRec = 0;
  int rc;

  doltliteParseRecord(pRec, nRec, &info);
  if( info.nField==0 ) return SQLITE_CORRUPT;

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
      for(i=0; i<info.nField; i++){
        if( !aUsed[i] ) aFieldOrder[out++] = i;
      }
      nOutField = out;
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = info.aType[col];
      int flen;
      hdrLen += sqlite3VarintLen(st);
      if( st<=0 ){ flen = 0; }
      else if( st==1 ){ flen = 1; }
      else if( st==2 ){ flen = 2; }
      else if( st==3 ){ flen = 3; }
      else if( st==4 ){ flen = 4; }
      else if( st==5 ){ flen = 6; }
      else if( st==6 || st==7 ){ flen = 8; }
      else if( st==8 || st==9 ){ flen = 0; }
      else if( st>=12 && (st&1)==0 ){ flen = (st-12)/2; }
      else if( st>=13 && (st&1)==1 ){ flen = (st-13)/2; }
      else{ flen = 0; }
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
      int st = info.aType[col];
      p += sqlite3PutVarint(p, st);
    }

    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = info.aType[col];
      int flen;
      if( st<=0 ){ flen = 0; }
      else if( st==1 ){ flen = 1; }
      else if( st==2 ){ flen = 2; }
      else if( st==3 ){ flen = 3; }
      else if( st==4 ){ flen = 4; }
      else if( st==5 ){ flen = 6; }
      else if( st==6 || st==7 ){ flen = 8; }
      else if( st==8 || st==9 ){ flen = 0; }
      else if( st>=12 && (st&1)==0 ){ flen = (st-12)/2; }
      else if( st>=13 && (st&1)==1 ){ flen = (st-13)/2; }
      else{ flen = 0; }
      if( flen>0 ){
        memcpy(p, pRec + info.aOffset[col], flen);
        p += flen;
      }
    }
    nIdxRec = (int)(p - pIdxRec);
    sqlite3_free(aFieldOrder);
    sqlite3_free(aUsed);
  }

  rc = sortKeyFromRecordPrefixColl(pIdxRec, nIdxRec, 0, pKeyInfo,
                                    ppKey, pnKey);
  sqlite3_free(pIdxRec);
  return rc;
}

typedef struct RowMergeCtx RowMergeCtx;
struct RowMergeCtx {
  ProllyMutMap *pEdits;
  u8 isIntKey;
  MergeIndexInfo *aIndexes;
  int nIndexes;
  int nConflicts;

  struct ConflictRow {
    i64 intKey;
    u8 *pKey; int nKey;
    u8 *pBaseVal; int nBaseVal;
    u8 *pOurVal; int nOurVal;
    u8 *pTheirVal; int nTheirVal;
  } *aConflicts;
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

static int mergeDupBytes(const u8 *pIn, int nIn, u8 **ppOut){
  u8 *pCopy;
  *ppOut = 0;
  if( !pIn || nIn<=0 ) return SQLITE_OK;
  pCopy = sqlite3_malloc(nIn);
  if( !pCopy ) return SQLITE_NOMEM;
  memcpy(pCopy, pIn, nIn);
  *ppOut = pCopy;
  return SQLITE_OK;
}

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

    winners = sqlite3_malloc(nfMax * (int)sizeof(*winners));
    if(!winners) goto fail;

    for(i=0; i<nfMax; i++){
      int baseHas = (i < nfBase);
      int oursHas = (i < nfOurs);
      int theirsHas = (i < nfTheirs);

      if(!baseHas && oursHas && !theirsHas){

        winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
      }else if(!baseHas && !oursHas && theirsHas){

        winners[i].pRec = pTheirs; winners[i].pField = &aTheirs[i];
      }else if(!baseHas && oursHas && theirsHas){

        if(fieldEquals(pOurs, &aOurs[i], pTheirs, &aTheirs[i])==0){
          winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
        }else{
          sqlite3_free(winners); goto fail;
        }
      }else if(baseHas && oursHas && theirsHas){

        int oursChanged = fieldEquals(pBase, &aBase[i], pOurs, &aOurs[i]);
        int theirsChanged = fieldEquals(pBase, &aBase[i], pTheirs, &aTheirs[i]);
        if(!oursChanged && !theirsChanged){

          winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
        }else if(oursChanged && !theirsChanged){

          winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
        }else if(!oursChanged && theirsChanged){

          winners[i].pRec = pTheirs; winners[i].pField = &aTheirs[i];
        }else{

          if(fieldEquals(pOurs, &aOurs[i], pTheirs, &aTheirs[i])==0){
            winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
          }else{
            sqlite3_free(winners); goto fail;
          }
        }
      }else if(baseHas && oursHas && !theirsHas){

        winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
      }else if(baseHas && !oursHas && theirsHas){

        winners[i].pRec = pTheirs; winners[i].pField = &aTheirs[i];
      }else{
        sqlite3_free(winners); goto fail;
      }
    }

    result = buildMergedRecord(winners, nfMax, pnMerged);
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
          u8 *pIK = 0; int nIK = 0;
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = buildIndexSortKey(pChange->pTheirVal, pChange->nTheirVal,
                                 mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                                 &pIK, &nIK);
          if( rc==SQLITE_OK ){
            rc = prollyMutMapInsert(mi->pEdits, pIK, nIK, 0,
                                    pChange->pTheirVal, pChange->nTheirVal);
            sqlite3_free(pIK);
          }
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
          if( pChange->pBaseVal && pChange->nBaseVal>0 ){
            u8 *pOK = 0; int nOK = 0;
            rc = buildIndexSortKey(pChange->pBaseVal, pChange->nBaseVal,
                                   mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                                   &pOK, &nOK);
            if( rc==SQLITE_OK ){
              rc = prollyMutMapDelete(mi->pEdits, pOK, nOK, 0);
              sqlite3_free(pOK);
            }
          }
          if( rc==SQLITE_OK
           && pChange->pTheirVal && pChange->nTheirVal>0 ){
            u8 *pNK = 0; int nNK = 0;
            rc = buildIndexSortKey(pChange->pTheirVal, pChange->nTheirVal,
                                   mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                                   &pNK, &nNK);
            if( rc==SQLITE_OK ){
              rc = prollyMutMapInsert(mi->pEdits, pNK, nNK, 0,
                                      pChange->pTheirVal, pChange->nTheirVal);
              sqlite3_free(pNK);
            }
          }
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
          u8 *pIK = 0; int nIK = 0;
          MergeIndexInfo *mi = &ctx->aIndexes[ix];
          rc = buildIndexSortKey(pChange->pBaseVal, pChange->nBaseVal,
                                 mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                                 &pIK, &nIK);
          if( rc==SQLITE_OK ){
            rc = prollyMutMapDelete(mi->pEdits, pIK, nIK, 0);
            sqlite3_free(pIK);
          }
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
            if( pChange->pBaseVal && pChange->nBaseVal>0 ){
              u8 *pOK = 0; int nOK = 0;
              rc = buildIndexSortKey(pChange->pBaseVal, pChange->nBaseVal,
                                     mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                                     &pOK, &nOK);
              if( rc==SQLITE_OK ){
                rc = prollyMutMapDelete(mi->pEdits, pOK, nOK, 0);
                sqlite3_free(pOK);
              }
            }
            if( rc==SQLITE_OK ){
              u8 *pNK = 0; int nNK = 0;
              rc = buildIndexSortKey(pMerged, nMerged,
                                     mi->aiColumn, mi->nColumn, mi->pKeyInfo,
                                     &pNK, &nNK);
              if( rc==SQLITE_OK ){
                rc = prollyMutMapInsert(mi->pEdits, pNK, nNK, 0,
                                        pMerged, nMerged);
                sqlite3_free(pNK);
              }
            }
          }
        }
        sqlite3_free(pMerged);
        break;
      }

    }
    case THREE_WAY_CONFLICT_DM: {

      rc = DOLTLITE_GROW_ARRAY(&ctx->aConflicts, &ctx->nConflictsAlloc,
                                ctx->nConflicts + 1, 16);
      if( rc!=SQLITE_OK ) return rc;
      {
        struct ConflictRow *cr = &ctx->aConflicts[ctx->nConflicts];
        memset(cr, 0, sizeof(*cr));
        cr->intKey = pChange->intKey;
        if( pChange->pKey && pChange->nKey>0 ){
          rc = mergeDupBytes(pChange->pKey, pChange->nKey, &cr->pKey);
          if( rc!=SQLITE_OK ) return rc;
          cr->nKey = pChange->nKey;
        }
        if( pChange->pBaseVal && pChange->nBaseVal>0 ){
          rc = mergeDupBytes(pChange->pBaseVal, pChange->nBaseVal, &cr->pBaseVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nBaseVal = pChange->nBaseVal;
        }
        if( pChange->pOurVal && pChange->nOurVal>0 ){
          rc = mergeDupBytes(pChange->pOurVal, pChange->nOurVal, &cr->pOurVal);
          if( rc!=SQLITE_OK ){
            sqlite3_free(cr->pKey);
            sqlite3_free(cr->pBaseVal);
            memset(cr, 0, sizeof(*cr));
            return rc;
          }
          cr->nOurVal = pChange->nOurVal;
        }
        if( pChange->pTheirVal && pChange->nTheirVal>0 ){
          rc = mergeDupBytes(pChange->pTheirVal, pChange->nTheirVal, &cr->pTheirVal);
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

static int canFastMerge(
  sqlite3 *db,
  const char *zName,
  int schemaUnchangedBothSides
){
  Table *pTab;
  FKey *pFK;

  if( !schemaUnchangedBothSides ) return 0;
  if( !zName || !db ) return 0;

  pTab = sqlite3FindTable(db, zName, 0);
  if( !pTab ) return 0;

  if( pTab->pIndex ) return 0;
  if( pTab->pCheck && pTab->pCheck->nExpr>0 ) return 0;

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
    sqlite3_free(ctx->aConflicts[i].pKey);
    sqlite3_free(ctx->aConflicts[i].pBaseVal);
    sqlite3_free(ctx->aConflicts[i].pOurVal);
    sqlite3_free(ctx->aConflicts[i].pTheirVal);
  }
  sqlite3_free(ctx->aConflicts);
  if( ctx->pEdits ){
    prollyMutMapFree(ctx->pEdits);
    sqlite3_free(ctx->pEdits);
  }
}

static int mergeTableRows(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pnConflicts,
  struct ConflictRow **ppConflicts,
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
      idxMut.flags = 0;
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

static struct TableEntry *findTableByName(
  struct TableEntry *aEntries,
  int nEntries,
  const char *zName
){
  return doltliteFindTableByName(aEntries, nEntries, zName);
}

typedef struct ParsedColumn ParsedColumn;
struct ParsedColumn {
  char *zName;
  char *zDef;
};

static int parseQuotedIdentifier(
  const char *z,
  const char *zEnd,
  const char **ppEnd,
  char **pzName
){
  char cOpen, cClose;
  const char *p;
  int nOut = 0;
  char *zName;

  *ppEnd = z;
  *pzName = 0;
  if( z>=zEnd ) return SQLITE_CORRUPT;

  cOpen = *z;
  cClose = cOpen=='[' ? ']' : cOpen;
  p = z + 1;
  while( p<zEnd ){
    if( *p==cClose ){
      if( p+1<zEnd && p[1]==cClose ){
        nOut++;
        p += 2;
        continue;
      }
      break;
    }
    nOut++;
    p++;
  }
  if( p>=zEnd || *p!=cClose ) return SQLITE_CORRUPT;

  zName = sqlite3_malloc(nOut + 1);
  if( !zName ) return SQLITE_NOMEM;

  p = z + 1;
  nOut = 0;
  while( p<zEnd && *p!=cClose ){
    if( p+1<zEnd && p[0]==cClose && p[1]==cClose ){
      zName[nOut++] = cClose;
      p += 2;
    }else{
      zName[nOut++] = *p++;
    }
  }
  zName[nOut] = 0;
  *ppEnd = p + 1;
  *pzName = zName;
  return SQLITE_OK;
}

static int parseColumns(
  const char *zSql,
  ParsedColumn **ppCols, int *pnCols
){
  const char *p, *pEnd;
  int depth;
  const char *segStart;
  ParsedColumn *aCols = 0;
  int nCols = 0, nAlloc = 0;

  *ppCols = 0;
  *pnCols = 0;

  if( !zSql ) return SQLITE_OK;

  p = zSql;
  while( *p && *p!='(' ) p++;
  if( *p!='(' ) return SQLITE_CORRUPT;
  p++;

  pEnd = p;
  depth = 1;
  while( *pEnd && depth>0 ){
    if( *pEnd=='(' ) depth++;
    else if( *pEnd==')' ) depth--;
    pEnd++;
  }
  if( depth!=0 ) return SQLITE_CORRUPT;
  pEnd--;

  segStart = p;
  depth = 0;
  while( p <= pEnd ){
    if( p==pEnd || (*p==',' && depth==0) ){

      const char *s = segStart;
      const char *e = (p==pEnd) ? p : p;
      char *zTrimmed;
      int len;

      while( s<e && isspace((unsigned char)*s) ) s++;
      while( e>s && isspace((unsigned char)*(e-1)) ) e--;

      len = (int)(e - s);
      if( len > 0 ){

        int isConstraint = 0;
        {
          const char *t = s;

          if( len>=11 && sqlite3_strnicmp(t, "PRIMARY KEY", 11)==0
              && (len==11 || !isalnum((unsigned char)t[11])) ){
            isConstraint = 1;
          }else if( len>=6 && sqlite3_strnicmp(t, "UNIQUE", 6)==0
              && (len==6 || t[6]=='(' || isspace((unsigned char)t[6])) ){
            isConstraint = 1;
          }else if( len>=5 && sqlite3_strnicmp(t, "CHECK", 5)==0
              && (len==5 || t[5]=='(' || isspace((unsigned char)t[5])) ){
            isConstraint = 1;
          }else if( len>=11 && sqlite3_strnicmp(t, "FOREIGN KEY", 11)==0
              && (len==11 || !isalnum((unsigned char)t[11])) ){
            isConstraint = 1;
          }else if( len>=10 && sqlite3_strnicmp(t, "CONSTRAINT", 10)==0
              && (len==10 || isspace((unsigned char)t[10])) ){
            isConstraint = 1;
          }
        }

        if( !isConstraint ){

          zTrimmed = sqlite3_malloc(len + 1);
          if( !zTrimmed ){

            { int ci; for(ci=0;ci<nCols;ci++){
              sqlite3_free(aCols[ci].zName);
              sqlite3_free(aCols[ci].zDef);
            }}
            sqlite3_free(aCols);
            return SQLITE_NOMEM;
          }
          memcpy(zTrimmed, s, len);
          zTrimmed[len] = 0;

          {
            char *zName;
            const char *nameStart = s;
            const char *nameEnd = nameStart;
            int nameLen;
            int rc;

            if( *nameStart=='"' || *nameStart=='`' || *nameStart=='[' ){
              rc = parseQuotedIdentifier(nameStart, e, &nameEnd, &zName);
              if( rc!=SQLITE_OK ){
                sqlite3_free(zTrimmed);
                { int ci; for(ci=0;ci<nCols;ci++){
                  sqlite3_free(aCols[ci].zName);
                  sqlite3_free(aCols[ci].zDef);
                }}
                sqlite3_free(aCols);
                return rc;
              }
            }else{
              while( nameEnd<e && !isspace((unsigned char)*nameEnd)
                  && *nameEnd!='(' && *nameEnd!=',' ) nameEnd++;
              nameLen = (int)(nameEnd - nameStart);
              zName = sqlite3_malloc(nameLen + 1);
              if( !zName ){
                sqlite3_free(zTrimmed);
                { int ci; for(ci=0;ci<nCols;ci++){
                  sqlite3_free(aCols[ci].zName);
                  sqlite3_free(aCols[ci].zDef);
                }}
                sqlite3_free(aCols);
                return SQLITE_NOMEM;
              }
              memcpy(zName, nameStart, nameLen);
              zName[nameLen] = 0;
            }

            { int ci; for(ci=0;zName[ci];ci++) zName[ci]=(char)tolower((unsigned char)zName[ci]); }

            if( nameEnd<=nameStart ){
              sqlite3_free(zName);
              sqlite3_free(zTrimmed);
              { int ci; for(ci=0;ci<nCols;ci++){
                sqlite3_free(aCols[ci].zName);
                sqlite3_free(aCols[ci].zDef);
              }}
              sqlite3_free(aCols);
              return SQLITE_CORRUPT;
            }

            if( DOLTLITE_GROW_ARRAY(&aCols, &nAlloc, nCols+1, 8)!=SQLITE_OK ){
              sqlite3_free(zName);
              sqlite3_free(zTrimmed);
              { int ci; for(ci=0;ci<nCols;ci++){
                sqlite3_free(aCols[ci].zName);
                sqlite3_free(aCols[ci].zDef);
              }}
              sqlite3_free(aCols);
              return SQLITE_NOMEM;
            }
            aCols[nCols].zName = zName;
            aCols[nCols].zDef = zTrimmed;
            nCols++;
          }
        }
      }

      segStart = p + 1;
    }else if( *p=='(' ){
      depth++;
    }else if( *p==')' ){
      depth--;
    }
    p++;
  }

  *ppCols = aCols;
  *pnCols = nCols;
  return SQLITE_OK;
}

static void freeColumns(ParsedColumn *aCols, int nCols){
  int i;
  for(i=0; i<nCols; i++){
    sqlite3_free(aCols[i].zName);
    sqlite3_free(aCols[i].zDef);
  }
  sqlite3_free(aCols);
}

static ParsedColumn *findColumn(ParsedColumn *aCols, int nCols, const char *zName){
  int i;
  for(i=0; i<nCols; i++){
    if( aCols[i].zName && sqlite3_stricmp(aCols[i].zName, zName)==0 ){
      return &aCols[i];
    }
  }
  return 0;
}

static int trySchemaColumnMerge(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  char ***ppAddCols, int *pnAddCols,
  int *pUseTheirSchema,
  char **pzErrDetail
){
  ParsedColumn *aAnc=0, *aOurs=0, *aTheirs=0;
  int nAnc=0, nOurs=0, nTheirs=0;
  int rc;
  char **azAdd = 0;
  int nAdd = 0, nAddAlloc = 0;
  int i;

  *ppAddCols = 0;
  *pnAddCols = 0;
  *pUseTheirSchema = 0;

  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); freeColumns(aOurs, nOurs); return rc; }

  for(i=0; i<nTheirs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aTheirs[i].zName);
    if( !ancCol ){

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){

        if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }else{

        rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
        if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
        azAdd[nAdd] = sqlite3_mprintf("%s", aTheirs[i].zDef);
        nAdd++;
      }
    }else{

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){
        int ancToTheirs = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        int ancToOurs = strcmp(ancCol->zDef, ourCol->zDef)!=0;
        if( ancToTheirs && ancToOurs ){

          if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){

            if( pzErrDetail ){
              *pzErrDetail = sqlite3_mprintf(
                "both branches modified column '%s' differently",
                aTheirs[i].zName);
            }
            rc = SQLITE_ERROR;
            goto schema_merge_cleanup;
          }

        }
      }else{

        int theirsModified = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        if( theirsModified ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }
    }
  }

  for(i=0; i<nOurs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aOurs[i].zName);
    if( ancCol ){

      ParsedColumn *theirCol = findColumn(aTheirs, nTheirs, aOurs[i].zName);
      if( !theirCol ){

        int oursModified = strcmp(ancCol->zDef, aOurs[i].zDef)!=0;
        if( oursModified ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aOurs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }
    }

  }

  if( nAdd > 0 ){
    *pUseTheirSchema = 0;
  }
  *ppAddCols = azAdd;
  *pnAddCols = nAdd;
  azAdd = 0; nAdd = 0;

schema_merge_cleanup:
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  if( rc!=SQLITE_OK ){
    { int j; for(j=0;j<nAdd;j++) sqlite3_free(azAdd[j]); }
    sqlite3_free(azAdd);
  }
  return rc;
}

static int serializeMergedCatalog(
  sqlite3 *db,
  const ProllyHash *oursCatHash,
  struct TableEntry *aMerged,
  int nMerged,
  Pgno iNextTable,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  ProllyHash *pOutHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *buf = 0;
  int nBuf = 0;
  int rc;

  (void)oursCatHash;
  (void)iNextTable;

  rc = doltliteSerializeCatalogEntriesWithFallbackSchema(
      db, aMerged, nMerged, aFallbackSchema, nFallbackSchema, &buf, &nBuf);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStorePut(cs, buf, nBuf, pOutHash);
  sqlite3_free(buf);
  return rc;
}

typedef struct MergeConflictTable MergeConflictTable;
struct MergeConflictTable {
  char *zName;
  int nConflicts;
  struct ConflictRow *aRows;
};

static void freeConflictRows(struct ConflictRow *aRows, int nRows){
  int i;
  for(i=0; i<nRows; i++){
    sqlite3_free(aRows[i].pKey);
    sqlite3_free(aRows[i].pBaseVal);
    sqlite3_free(aRows[i].pOurVal);
    sqlite3_free(aRows[i].pTheirVal);
  }
  sqlite3_free(aRows);
}

static void freeAddedColumns(char **azCols, int nCols){
  int i;
  for(i=0; i<nCols; i++) sqlite3_free(azCols[i]);
  sqlite3_free(azCols);
}

static int appendConflictTable(
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  const char *zName,
  int nConflicts,
  struct ConflictRow *aConflictRows
){
  MergeConflictTable *aNew;
  aNew = sqlite3_realloc(*ppConflictTables,
    (*pnConflictTables+1)*(int)sizeof(MergeConflictTable));
  if( !aNew ){
    freeConflictRows(aConflictRows, nConflicts);
    return SQLITE_NOMEM;
  }
  *ppConflictTables = aNew;
  aNew[*pnConflictTables].zName = sqlite3_mprintf("%s", zName);
  aNew[*pnConflictTables].nConflicts = nConflicts;
  aNew[*pnConflictTables].aRows = aConflictRows;
  (*pnConflictTables)++;
  return SQLITE_OK;
}

static int recordSchemaAddColumns(
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  const char *zName,
  char **azAddCols,
  int nAddCols
){
  SchemaMergeAction *aNew;
  aNew = sqlite3_realloc(*ppSchemaActions,
    (*pnSchemaActions+1)*(int)sizeof(SchemaMergeAction));
  if( !aNew ) return SQLITE_NOMEM;
  *ppSchemaActions = aNew;
  aNew[*pnSchemaActions].zTableName = sqlite3_mprintf("%s", zName);
  if( !aNew[*pnSchemaActions].zTableName ) return SQLITE_NOMEM;
  aNew[*pnSchemaActions].azAddColumns = azAddCols;
  aNew[*pnSchemaActions].nAddColumns = nAddCols;
  (*pnSchemaActions)++;
  return SQLITE_OK;
}

static int schemaEntryChangedByName(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aSide, int nSide,
  const char *zName
){
  SchemaEntry *pAnc = findSchemaEntry(aAnc, nAnc, zName);
  SchemaEntry *pSide = findSchemaEntry(aSide, nSide, zName);
  if( pAnc==0 && pSide==0 ) return 0;
  if( pAnc==0 || pSide==0 ) return 1;
  if( pAnc->zType && pSide->zType
   && strcmp(pAnc->zType, pSide->zType)!=0 ) return 1;
  if( pAnc->zTblName && pSide->zTblName
   && strcmp(pAnc->zTblName, pSide->zTblName)!=0 ) return 1;
  if( (pAnc->zSql==0) != (pSide->zSql==0) ) return 1;
  if( pAnc->zSql && strcmp(pAnc->zSql, pSide->zSql)!=0 ) return 1;
  return 0;
}

static int hasSchemaObject(
  SchemaEntry *aSchema,
  int nSchema,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  int i;
  for(i=0; i<nSchema; i++){
    if( strcmp(aSchema[i].zType ? aSchema[i].zType : "", zType ? zType : "")!=0 ) continue;
    if( strcmp(aSchema[i].zName ? aSchema[i].zName : "", zName ? zName : "")!=0 ) continue;
    if( strcmp(aSchema[i].zTblName ? aSchema[i].zTblName : "",
               zTblName ? zTblName : "")!=0 ) continue;
    return 1;
  }
  return 0;
}

static int replayDropsDisjointSchemaObject(
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema
){
  int i;
  for(i=0; i<nAncSchema; i++){
    const char *zType = aAncSchema[i].zType;
    if( !zType ) continue;
    if( strcmp(zType, "table")!=0 && strcmp(zType, "index")!=0 ) continue;
    if( !hasSchemaObject(aTheirsSchema, nTheirsSchema,
                         aAncSchema[i].zType,
                         aAncSchema[i].zName,
                         aAncSchema[i].zTblName) ){
      return 1;
    }
  }
  return 0;
}

static SchemaEntry *findSchemaEntryByRootpage(
  SchemaEntry *aSchema,
  int nSchema,
  Pgno iRootpage
){
  int i;
  for(i=0; i<nSchema; i++){
    if( aSchema[i].iRootpage==iRootpage ) return &aSchema[i];
  }
  return 0;
}

static struct TableEntry *findCatalogEntryBySchemaObject(
  struct TableEntry *aCat,
  int nCat,
  SchemaEntry *aSchema,
  int nSchema,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  int i;
  for(i=0; i<nSchema; i++){
    if( strcmp(aSchema[i].zType ? aSchema[i].zType : "", zType ? zType : "")!=0 ) continue;
    if( strcmp(aSchema[i].zName ? aSchema[i].zName : "", zName ? zName : "")!=0 ) continue;
    if( strcmp(aSchema[i].zTblName ? aSchema[i].zTblName : "", zTblName ? zTblName : "")!=0 ) continue;
    return findTableEntry(aCat, nCat, aSchema[i].iRootpage);
  }
  return 0;
}

static int schemaChangesOverlapByName(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs
){
  int i;
  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    if( !zName ) continue;
    if( schemaEntryChangedByName(aAnc, nAnc, aOurs, nOurs, zName)
     && schemaEntryChangedByName(aAnc, nAnc, aTheirs, nTheirs, zName) ){
      return 1;
    }
  }
  for(i=0; i<nAnc; i++){
    const char *zName = aAnc[i].zName;
    if( !zName ) continue;
    if( findSchemaEntry(aOurs, nOurs, zName) ) continue;
    if( schemaEntryChangedByName(aAnc, nAnc, aTheirs, nTheirs, zName) ){
      return 1;
    }
  }
  return 0;
}

static int catalogHasDisjointSchemaChanges(
  sqlite3 *db,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry *aAncSchema = 0, *aOursSchema = 0, *aTheirsSchema = 0;
  int nAncSchema = 0, nOursSchema = 0, nTheirsSchema = 0;
  int i, rc;
  int sawOursChanged = 0, sawTheirsChanged = 0;
  int result = 0;

  if( !cs || !pCache ) return 0;
  rc = loadSchemaFromCatalog(db, cs, pCache, pCatAnc, &aAncSchema, &nAncSchema);
  if( rc!=SQLITE_OK ) goto done;
  rc = loadSchemaFromCatalog(db, cs, pCache, pCatOurs, &aOursSchema, &nOursSchema);
  if( rc!=SQLITE_OK ) goto done;
  rc = loadSchemaFromCatalog(db, cs, pCache, pCatTheirs, &aTheirsSchema, &nTheirsSchema);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<nOursSchema; i++){
    if( aOursSchema[i].zName
     && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 aOursSchema[i].zName) ){
      sawOursChanged = 1;
      break;
    }
  }
  if( !sawOursChanged ){
    for(i=0; i<nAncSchema; i++){
      if( aAncSchema[i].zName
       && !findSchemaEntry(aOursSchema, nOursSchema, aAncSchema[i].zName) ){
        sawOursChanged = 1;
        break;
      }
    }
  }

  for(i=0; i<nTheirsSchema; i++){
    if( aTheirsSchema[i].zName
     && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aTheirsSchema, nTheirsSchema,
                                 aTheirsSchema[i].zName) ){
      sawTheirsChanged = 1;
      break;
    }
  }
  if( !sawTheirsChanged ){
    for(i=0; i<nAncSchema; i++){
      if( aAncSchema[i].zName
       && !findSchemaEntry(aTheirsSchema, nTheirsSchema, aAncSchema[i].zName) ){
        sawTheirsChanged = 1;
        break;
      }
    }
  }

  if( sawOursChanged && sawTheirsChanged
   && !schemaChangesOverlapByName(aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema) ){
    result = 1;
  }
done:
  freeSchemaEntries(aAncSchema, nAncSchema);
  freeSchemaEntries(aOursSchema, nOursSchema);
  freeSchemaEntries(aTheirsSchema, nTheirsSchema);
  return result;
}

typedef struct SchemaRootpageRemap SchemaRootpageRemap;
struct SchemaRootpageRemap {
  Pgno oldPg;
  Pgno newPg;
};

static Pgno remapSchemaRootpage(
  SchemaRootpageRemap *aRemap,
  int nRemap,
  Pgno iRootpage
){
  int i;
  for(i=0; i<nRemap; i++){
    if( aRemap[i].oldPg==iRootpage ) return aRemap[i].newPg;
  }
  return iRootpage;
}

typedef struct MergeFieldValue MergeFieldValue;
struct MergeFieldValue {
  int eType;
  i64 i;
  const void *p;
  int n;
};

static u32 mergeCatalogSerialType(const MergeFieldValue *pMem, u32 *pLen){
  if( pMem->eType == SQLITE_NULL ){ *pLen = 0; return 0; }
  if( pMem->eType == SQLITE_INTEGER ){
    i64 v = pMem->i;
    if( v==0 ){ *pLen = 0; return 8; }
    if( v==1 ){ *pLen = 0; return 9; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return 1; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return 2; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return 3; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return 4; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return 5; }
    *pLen = 8; return 6;
  }
  if( pMem->eType == SQLITE_TEXT ){
    *pLen = (u32)pMem->n;
    return (u32)(pMem->n * 2 + 13);
  }
  if( pMem->eType == SQLITE_BLOB ){
    *pLen = (u32)pMem->n;
    return (u32)(pMem->n * 2 + 12);
  }
  *pLen = 0;
  return 0;
}

static void mergeCatalogWriteIntBe(u8 *pOut, i64 v, int nByte){
  int i;
  for(i=nByte-1; i>=0; i--){
    pOut[i] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

static void mergeCatalogSerialPut(u8 *pOut, const MergeFieldValue *pMem, u32 serialType){
  switch( serialType ){
    case 0:
    case 8:
    case 9:
      return;
    case 1: mergeCatalogWriteIntBe(pOut, pMem->i, 1); return;
    case 2: mergeCatalogWriteIntBe(pOut, pMem->i, 2); return;
    case 3: mergeCatalogWriteIntBe(pOut, pMem->i, 3); return;
    case 4: mergeCatalogWriteIntBe(pOut, pMem->i, 4); return;
    case 5: mergeCatalogWriteIntBe(pOut, pMem->i, 6); return;
    case 6: mergeCatalogWriteIntBe(pOut, pMem->i, 8); return;
    default:
      if( serialType>=12 ){
        memcpy(pOut, pMem->p, (size_t)pMem->n);
      }
      return;
  }
}

static u8 *buildSchemaCatalogRecord(
  const char *zType,
  const char *zName,
  const char *zTblName,
  i64 iRootpage,
  const char *zSql,
  int *pnOut
){
  MergeFieldValue aMem[5];
  u32 aType[5];
  u32 aLen[5];
  int i, hdrSize = 0, bodySize = 0, pos;
  u8 *pOut, *pHdr, *pBody;

  memset(aMem, 0, sizeof(aMem));
  *pnOut = 0;

  aMem[0].eType = SQLITE_TEXT;    aMem[0].p = zType;    aMem[0].n = (int)strlen(zType);
  aMem[1].eType = SQLITE_TEXT;    aMem[1].p = zName;    aMem[1].n = (int)strlen(zName);
  aMem[2].eType = SQLITE_TEXT;    aMem[2].p = zTblName; aMem[2].n = (int)strlen(zTblName);
  aMem[3].eType = SQLITE_INTEGER; aMem[3].i = iRootpage;
  if( zSql ){
    aMem[4].eType = SQLITE_TEXT;
    aMem[4].p = zSql;
    aMem[4].n = (int)strlen(zSql);
  }else{
    aMem[4].eType = SQLITE_NULL;
    aMem[4].p = 0;
    aMem[4].n = 0;
  }

  for(i=0; i<5; i++){
    aType[i] = mergeCatalogSerialType(&aMem[i], &aLen[i]);
    hdrSize += sqlite3VarintLen(aType[i]);
    bodySize += (int)aLen[i];
  }
  hdrSize += sqlite3VarintLen(hdrSize);

  pOut = sqlite3_malloc(hdrSize + bodySize);
  if( !pOut ) return 0;

  pos = sqlite3PutVarint(pOut, hdrSize);
  pHdr = pOut + pos;
  pBody = pOut + hdrSize;
  for(i=0; i<5; i++){
    pHdr += sqlite3PutVarint(pHdr, aType[i]);
    if( aLen[i]>0 ){
      mergeCatalogSerialPut(pBody, &aMem[i], aType[i]);
      pBody += aLen[i];
    }
  }
  *pnOut = hdrSize + bodySize;
  return pOut;
}

static SchemaEntry *mergedSchemaChoice(
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  const char *zName
){
  SchemaEntry *pAnc = findSchemaEntry(aAncSchema, nAncSchema, zName);
  SchemaEntry *pOurs = findSchemaEntry(aOursSchema, nOursSchema, zName);
  SchemaEntry *pTheirs = findSchemaEntry(aTheirsSchema, nTheirsSchema, zName);
  int oursChanged = schemaEntryChangedByName(aAncSchema, nAncSchema,
                                             aOursSchema, nOursSchema, zName);
  int theirsChanged = schemaEntryChangedByName(aAncSchema, nAncSchema,
                                               aTheirsSchema, nTheirsSchema, zName);
  if( oursChanged && !theirsChanged ) return pOurs;
  if( theirsChanged && !oursChanged ) return pTheirs;
  if( pOurs ) return pOurs;
  if( pTheirs ) return pTheirs;
  return pAnc;
}

static int appendMergedSchemaCatalogRecord(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 iRowid,
  const SchemaEntry *pSe,
  Pgno iRootpage
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  u8 *pRec = 0;
  int nRec = 0;
  int rc;

  if( !pSe || !pSe->zName || !pSe->zType ) return SQLITE_OK;
  pRec = buildSchemaCatalogRecord(pSe->zType, pSe->zName,
                                  pSe->zTblName ? pSe->zTblName : pSe->zName,
                                  (i64)iRootpage, pSe->zSql, &nRec);
  if( !pRec ) return SQLITE_NOMEM;
  rc = prollyMutateInsert(cs, pCache, pRoot, flags, 0, 0,
                          iRowid, pRec, nRec, pRoot);
  sqlite3_free(pRec);
  return rc;
}

static int appendMergedHiddenIndexRow(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 *piNextRowid,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaRootpageRemap *aRemap, int nRemap,
  const char *zName
){
  SchemaEntry *pSe;
  Pgno iRootpage;

  if( !zName ) return SQLITE_OK;
  pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                           aOursSchema, nOursSchema,
                           aTheirsSchema, nTheirsSchema,
                           zName);
  if( !pSe || !pSe->zType || !pSe->zName ) return SQLITE_OK;
  if( strcmp(pSe->zType, "index")!=0 ) return SQLITE_OK;
  if( pSe->zSql ) return SQLITE_OK;

  iRootpage = pSe->iRootpage;
  if( pSe>=aTheirsSchema && pSe<aTheirsSchema+nTheirsSchema ){
    iRootpage = remapSchemaRootpage(aRemap, nRemap, iRootpage);
  }
  return appendMergedSchemaCatalogRecord(db, pRoot, flags, (*piNextRowid)++, pSe, iRootpage);
}

static int appendMergedAuxSchemaRow(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 *piNextRowid,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaRootpageRemap *aRemap, int nRemap,
  const char *zName
){
  SchemaEntry *pSe;
  Pgno iRootpage;

  if( !zName ) return SQLITE_OK;
  pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                           aOursSchema, nOursSchema,
                           aTheirsSchema, nTheirsSchema,
                           zName);
  if( !pSe || !pSe->zType || !pSe->zName ) return SQLITE_OK;
  if( strcmp(pSe->zType, "table")==0 || strcmp(pSe->zType, "index")==0 ){
    return SQLITE_OK;
  }

  iRootpage = pSe->iRootpage;
  if( pSe>=aTheirsSchema && pSe<aTheirsSchema+nTheirsSchema ){
    iRootpage = remapSchemaRootpage(aRemap, nRemap, iRootpage);
  }
  return appendMergedSchemaCatalogRecord(db, pRoot, flags, (*piNextRowid)++, pSe, iRootpage);
}

static int rebuildDisjointSchemaRows(
  sqlite3 *db,
  struct TableEntry *aMerged, int nMerged,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaRootpageRemap *aRemap, int nRemap
){
  struct TableEntry *pMaster = 0;
  ProllyHash root;
  i64 iNextRowid = 1;
  int i, rc = SQLITE_OK;

  for(i=0; i<nMerged; i++){
    if( aMerged[i].iTable==1 ){
      pMaster = &aMerged[i];
      break;
    }
  }
  if( !pMaster ) return SQLITE_OK;
  memset(&root, 0, sizeof(root));

  for(i=0; i<nMerged; i++){
    const char *zName = aMerged[i].zName;
    SchemaEntry *pSe = 0;

    if( aMerged[i].iTable<=1 || !zName ) continue;
    pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                             aOursSchema, nOursSchema,
                             aTheirsSchema, nTheirsSchema,
                             zName);
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, aMerged[i].iTable);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nOursSchema; i++){
    SchemaEntry *pSe = &aOursSchema[i];
    if( !pSe->zName || !pSe->zType ) continue;
    if( strcmp(pSe->zType, "index")!=0 ) continue;
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  pSe->zName) ){
      continue;
    }
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, pSe->iRootpage);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nTheirsSchema; i++){
    SchemaEntry *pSe = &aTheirsSchema[i];
    Pgno iRootpage;
    if( !pSe->zName || !pSe->zType ) continue;
    if( strcmp(pSe->zType, "index")!=0 ) continue;
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  pSe->zName) ){
      continue;
    }
    if( schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 pSe->zName) ){
      continue;
    }
    iRootpage = remapSchemaRootpage(aRemap, nRemap, pSe->iRootpage);
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, iRootpage);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nAncSchema; i++){
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aRemap, nRemap,
                                    aAncSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nOursSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aOursSchema[i].zName) ) continue;
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aRemap, nRemap,
                                    aOursSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nTheirsSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aTheirsSchema[i].zName) ) continue;
    if( findSchemaEntry(aOursSchema, nOursSchema, aTheirsSchema[i].zName) ) continue;
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aRemap, nRemap,
                                    aTheirsSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nAncSchema; i++){
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aRemap, nRemap,
                                  aAncSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nOursSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aOursSchema[i].zName) ) continue;
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aRemap, nRemap,
                                  aOursSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nTheirsSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aTheirsSchema[i].zName) ) continue;
    if( findSchemaEntry(aOursSchema, nOursSchema, aTheirsSchema[i].zName) ) continue;
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aRemap, nRemap,
                                  aTheirsSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }

  memcpy(&pMaster->root, &root, sizeof(root));
  return rc;
}

static int tryResolveSchemaDivergence(
  sqlite3 *db,
  const char *zName,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  int *pSkipRowMerge,
  char **pzErrMsg
){
  ChunkStore *csLocal;
  ProllyCache *cacheLocal;
  SchemaEntry *aAncSchema = 0;
  SchemaEntry *aOursSchema = 0;
  SchemaEntry *aTheirsSchema = 0;
  int nAncSchema = 0;
  int nOursSchema = 0;
  int nTheirsSchema = 0;
  SchemaEntry *ancSchEntry;
  SchemaEntry *ourSchEntry;
  SchemaEntry *theirSchEntry;
  char **azAddCols = 0;
  int nAddCols = 0;
  int useTheirSchema = 0;
  char *zSchemaErr = 0;
  int rc;

  *pSkipRowMerge = 0;
  csLocal = doltliteGetChunkStore(db);
  cacheLocal = doltliteGetCache(db);
  loadSchemaFromCatalog(db, csLocal, cacheLocal, pCatAnc, &aAncSchema, &nAncSchema);
  loadSchemaFromCatalog(db, csLocal, cacheLocal, pCatOurs, &aOursSchema, &nOursSchema);
  loadSchemaFromCatalog(db, csLocal, cacheLocal, pCatTheirs, &aTheirsSchema, &nTheirsSchema);

  ancSchEntry = findSchemaEntry(aAncSchema, nAncSchema, zName);
  ourSchEntry = findSchemaEntry(aOursSchema, nOursSchema, zName);
  theirSchEntry = findSchemaEntry(aTheirsSchema, nTheirsSchema, zName);

  if( ancSchEntry && ancSchEntry->zSql
   && ourSchEntry && ourSchEntry->zSql
   && theirSchEntry && theirSchEntry->zSql ){
    rc = trySchemaColumnMerge(
      ancSchEntry->zSql, ourSchEntry->zSql, theirSchEntry->zSql,
      &azAddCols, &nAddCols, &useTheirSchema, &zSchemaErr);
  }else{
    rc = SQLITE_ERROR;
    zSchemaErr = sqlite3_mprintf("cannot load schemas for merge");
  }

  freeSchemaEntries(aAncSchema, nAncSchema);
  freeSchemaEntries(aOursSchema, nOursSchema);
  freeSchemaEntries(aTheirsSchema, nTheirsSchema);

  if( rc!=SQLITE_OK ){
    if( pzErrMsg ){
      if( zSchemaErr ){
        *pzErrMsg = sqlite3_mprintf(
          "schema conflict on table '%s' \xe2\x80\x94 %s",
          zName ? zName : "(unknown)", zSchemaErr);
      }else{
        *pzErrMsg = sqlite3_mprintf(
          "schema conflict on table '%s'",
          zName ? zName : "(unknown)");
      }
    }
    sqlite3_free(zSchemaErr);
    freeAddedColumns(azAddCols, nAddCols);
    return SQLITE_ERROR;
  }
  sqlite3_free(zSchemaErr);

  if( nAddCols>0 ){
    if( ppSchemaActions && pnSchemaActions ){
      rc = recordSchemaAddColumns(ppSchemaActions, pnSchemaActions, zName,
                                  azAddCols, nAddCols);
      if( rc!=SQLITE_OK ){
        freeAddedColumns(azAddCols, nAddCols);
        return rc;
      }
      azAddCols = 0;
      nAddCols = 0;
    }
    freeAddedColumns(azAddCols, nAddCols);
    *pSkipRowMerge = 1;
    return SQLITE_OK;
  }

  freeAddedColumns(azAddCols, nAddCols);
  return SQLITE_OK;
}

static int mergeCatalogPass1(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  MergeConflictTable **ppConflictTables, int *pnConflictTables,
  int *pTotalConflicts,
  char **pzErrMsg,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions, int *pnSchemaActions,
  int bDisjointSchemaChanges,
  int bPreferOurMaster
){
  int i, rc = SQLITE_OK;
  int iTable1Idx = -1;

  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    const char *zLogicalName = zName;
    const char *zSchemaMergeName = zName;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;

    if( aOurs[i].iTable==1 ){
      iTable1Idx = i;
      continue;
    }

    if( !zName ){
      SchemaEntry *pOurSe = findSchemaEntryByRootpage(
          aOursSchema, nOursSchema, aOurs[i].iTable);
      if( pOurSe && pOurSe->zName && pOurSe->zType
       && strcmp(pOurSe->zType, "index")==0 ){
        zLogicalName = pOurSe->zName;
        zSchemaMergeName = pOurSe->zName;
        ancEntry = findCatalogEntryBySchemaObject(
            aAnc, nAnc, aAncSchema, nAncSchema,
            pOurSe->zType, pOurSe->zName, pOurSe->zTblName);
        theirsEntry = findCatalogEntryBySchemaObject(
            aTheirs, nTheirs, aTheirsSchema, nTheirsSchema,
            pOurSe->zType, pOurSe->zName, pOurSe->zTblName);
        goto do_merge_entry;
      }
      ancEntry = findTableEntry(aAnc, nAnc, aOurs[i].iTable);
      theirsEntry = findTableEntry(aTheirs, nTheirs, aOurs[i].iTable);
      goto do_merge_entry;
    }

    ancEntry = findTableByName(aAnc, nAnc, zName);
    theirsEntry = findTableByName(aTheirs, nTheirs, zName);

do_merge_entry:

    if( !ancEntry ){

      if( theirsEntry ){

        if( !zName && bDisjointSchemaChanges ){
          aMerged[(*pnMerged)++] = aOurs[i];
          continue;
        }

        if( prollyHashCompare(&aOurs[i].root, &theirsEntry->root)!=0
         || prollyHashCompare(&aOurs[i].schemaHash, &theirsEntry->schemaHash)!=0 ){
          if( pzErrMsg ){
            *pzErrMsg = sqlite3_mprintf(
              "schema conflict: table '%s' added on both branches with "
              "different definitions", zLogicalName ? zLogicalName : "");
          }
          return SQLITE_ERROR;
        }

      }
      aMerged[(*pnMerged)++] = aOurs[i];
    }else{

      int oursChanged = prollyHashCompare(&aOurs[i].root, &ancEntry->root)!=0;

      if( !theirsEntry ){

        if( oursChanged ){
          if( pzErrMsg ){
            *pzErrMsg = sqlite3_mprintf(
              "schema conflict: table '%s' modified on one branch "
              "and deleted on the other", zLogicalName ? zLogicalName : "");
          }
          return SQLITE_ERROR;
        }

      }else{
        int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
        int ourSchemaChanged = prollyHashCompare(
            &aOurs[i].schemaHash, &ancEntry->schemaHash)!=0;
        int theirSchemaChanged = prollyHashCompare(
            &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;
        int bNamedSchemaObject = (!zName && zSchemaMergeName && zSchemaMergeName[0]);
        int skipRowMerge = 0;

        if( zSchemaMergeName && zSchemaMergeName[0] ){
          ourSchemaChanged = ourSchemaChanged || schemaEntryChangedByName(
              aAncSchema, nAncSchema, aOursSchema, nOursSchema, zSchemaMergeName);
          theirSchemaChanged = theirSchemaChanged || schemaEntryChangedByName(
              aAncSchema, nAncSchema, aTheirsSchema, nTheirsSchema, zSchemaMergeName);
        }

        if( ourSchemaChanged && theirSchemaChanged
         && (bNamedSchemaObject
             || prollyHashCompare(&aOurs[i].schemaHash,
                                  &theirsEntry->schemaHash)!=0) ){
          rc = tryResolveSchemaDivergence(
            db, zSchemaMergeName, pCatAnc, pCatOurs, pCatTheirs,
            ppSchemaActions, pnSchemaActions, &skipRowMerge, pzErrMsg);
          if( rc!=SQLITE_OK ) return rc;
          if( skipRowMerge ){
            aMerged[(*pnMerged)++] = aOurs[i];
          }
        }

        if( !skipRowMerge ){
          if( oursChanged && theirsChanged ){

            ProllyHash mergedTableRoot;
            int nConflicts = 0;
            struct ConflictRow *aConflictRows = 0;

            MergeIndexInfo *aIdxInfo = 0;
            int nIdxInfo = 0;
            if( zName && db ){
              Table *pTab = sqlite3FindTable(db, zName, "main");
              if( pTab ){
                Index *pIdx;
                int nIdx = 0;
                for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext) nIdx++;
                if( nIdx>0 ){
                  aIdxInfo = sqlite3_malloc(nIdx * (int)sizeof(MergeIndexInfo));
                  if( aIdxInfo ){
                    memset(aIdxInfo, 0, nIdx*(int)sizeof(MergeIndexInfo));
                    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
                      struct TableEntry *oursIdx = findTableEntry(aOurs, nOurs, pIdx->tnum);
                      if( oursIdx ){
                        MergeIndexInfo *mi = &aIdxInfo[nIdxInfo];
                        mi->iTable = pIdx->tnum;
                        memcpy(&mi->oursRoot, &oursIdx->root, sizeof(ProllyHash));
                        mi->nColumn = pIdx->nKeyCol;
                        mi->aiColumn = pIdx->aiColumn;
                        mi->pKeyInfo = 0;
                        nIdxInfo++;
                      }
                    }
                  }
                }
              }
            }

            if( canFastMerge(db, zName,
                             !ourSchemaChanged && !theirSchemaChanged) ){
              int handled = 0;
              rc = prollyThreeWayMergeFast(
                doltliteGetChunkStore(db), doltliteGetCache(db),
                &ancEntry->root, &aOurs[i].root, &theirsEntry->root,
                aOurs[i].flags, &mergedTableRoot, &handled);
              if( rc != SQLITE_OK ){
                sqlite3_free(aIdxInfo);
                return rc;
              }
              if( handled ){
                nConflicts = 0;
                aConflictRows = 0;
                goto post_merge_table_rows;
              }
            }

            rc = mergeTableRows(db, &ancEntry->root, &aOurs[i].root,
                                &theirsEntry->root, aOurs[i].flags,
                                &mergedTableRoot, &nConflicts, &aConflictRows,
                                aIdxInfo, nIdxInfo);

post_merge_table_rows:;

            if( rc==SQLITE_OK ){
              int ix;
              for(ix=0; ix<nIdxInfo; ix++){
                int k;
                for(k=0; k<*pnMerged; k++){
                  if( aMerged[k].iTable==aIdxInfo[ix].iTable ){
                    memcpy(&aMerged[k].root, &aIdxInfo[ix].mergedRoot,
                           sizeof(ProllyHash));
                    break;
                  }
                }
              }
            }
            sqlite3_free(aIdxInfo);
            if( rc!=SQLITE_OK ) return rc;

            {
              struct TableEntry merged = aOurs[i];
              memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));

              if( theirSchemaChanged
               && !ourSchemaChanged ){
                memcpy(&merged.schemaHash, &theirsEntry->schemaHash,
                       sizeof(ProllyHash));
                merged.flags = theirsEntry->flags;
              }
              aMerged[(*pnMerged)++] = merged;
            }

            if( nConflicts>0 ){
              *pTotalConflicts += nConflicts;
              rc = appendConflictTable(ppConflictTables, pnConflictTables,
                                       zLogicalName ? zLogicalName : "",
                                       nConflicts, aConflictRows);
              if( rc!=SQLITE_OK ) return rc;
            }
          }else if( theirsChanged ){
            struct TableEntry merged = aOurs[i];
            memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
            memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
            merged.flags = theirsEntry->flags;
            aMerged[(*pnMerged)++] = merged;
          }else{
            aMerged[(*pnMerged)++] = aOurs[i];
          }
        }
      }
    }
  }

  if( iTable1Idx >= 0 ){
    struct TableEntry *ancEntry = findTableEntry(aAnc, nAnc, 1);
    struct TableEntry *theirsEntry = findTableEntry(aTheirs, nTheirs, 1);
    int hasSchemaActions = (ppSchemaActions && pnSchemaActions && *pnSchemaActions > 0);
    int bPreferOurMasterHere = bPreferOurMaster
        && replayDropsDisjointSchemaObject(aAncSchema, nAncSchema,
                                           aTheirsSchema, nTheirsSchema);

    if( !ancEntry ){
      if( theirsEntry ){
        if( prollyHashCompare(&aOurs[iTable1Idx].root, &theirsEntry->root)!=0
         || prollyHashCompare(&aOurs[iTable1Idx].schemaHash, &theirsEntry->schemaHash)!=0 ){
          return SQLITE_ERROR;
        }
      }
      aMerged[(*pnMerged)++] = aOurs[iTable1Idx];
    }else if( !theirsEntry ){
      int oursChanged = prollyHashCompare(&aOurs[iTable1Idx].root, &ancEntry->root)!=0;
      if( oursChanged ){
        return SQLITE_ERROR;
      }
    }else{
      int oursChanged = prollyHashCompare(&aOurs[iTable1Idx].root, &ancEntry->root)!=0;
      int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
      if( bPreferOurMasterHere ){
        aMerged[(*pnMerged)++] = aOurs[iTable1Idx];
        return rc;
      }
      if( oursChanged && theirsChanged && hasSchemaActions ){

        aMerged[(*pnMerged)++] = aOurs[iTable1Idx];
      }else if( oursChanged && theirsChanged && bDisjointSchemaChanges ){

        aMerged[(*pnMerged)++] = aOurs[iTable1Idx];
      }else if( oursChanged && theirsChanged ){

        ProllyHash mergedTableRoot;
        int nConflicts = 0;
        struct ConflictRow *aConflictRows = 0;
        int theirSchemaChanged2 = prollyHashCompare(
            &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;

        rc = mergeTableRows(db, &ancEntry->root, &aOurs[iTable1Idx].root,
                            &theirsEntry->root, aOurs[iTable1Idx].flags,
                            &mergedTableRoot, &nConflicts, &aConflictRows,
                            NULL, 0);
        if( rc!=SQLITE_OK ) return rc;

        {
          struct TableEntry merged = aOurs[iTable1Idx];
          memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
          if( theirSchemaChanged2
           && prollyHashCompare(&aOurs[iTable1Idx].schemaHash,
                                &ancEntry->schemaHash)==0 ){
            memcpy(&merged.schemaHash, &theirsEntry->schemaHash,
                   sizeof(ProllyHash));
            merged.flags = theirsEntry->flags;
          }
          aMerged[(*pnMerged)++] = merged;
        }

        if( nConflicts>0 ){
          *pTotalConflicts += nConflicts;
          rc = appendConflictTable(ppConflictTables, pnConflictTables,
                                   "(sqlite_master)", nConflicts,
                                   aConflictRows);
          if( rc!=SQLITE_OK ) return rc;
        }
      }else if( theirsChanged ){
        struct TableEntry merged = aOurs[iTable1Idx];
        memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
        memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
        merged.flags = theirsEntry->flags;
        aMerged[(*pnMerged)++] = merged;
      }else{
        aMerged[(*pnMerged)++] = aOurs[iTable1Idx];
      }
    }
  }

  return rc;
}

static int mergeCatalogPass2(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  Pgno *piNextMerged,
  int bDisjointSchemaChanges,
  SchemaRootpageRemap **ppaRemap,
  int *pnRemap
){
  int i;

  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 ) continue;

    if( !zName ){
      SchemaEntry *pTheirSe = findSchemaEntryByRootpage(
          aTheirsSchema, nTheirsSchema, aTheirs[i].iTable);
      if( pTheirSe && pTheirSe->zName && pTheirSe->zType
       && strcmp(pTheirSe->zType, "index")==0 ){
        oursEntry = findCatalogEntryBySchemaObject(
            aOurs, nOurs, aOursSchema, nOursSchema,
            pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
        if( oursEntry ) continue;

        {
          struct TableEntry *ancEntry = findCatalogEntryBySchemaObject(
              aAnc, nAnc, aAncSchema, nAncSchema,
              pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
          if( !ancEntry ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            int forceRemap = bDisjointSchemaChanges;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict || forceRemap ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                                     (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
            aMerged[(*pnMerged)++] = newEntry;
          }else{
            int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
            if( theirsChanged ) return SQLITE_ERROR;
          }
        }
        continue;
      }

      if( !findTableEntry(aOurs, nOurs, aTheirs[i].iTable) ){
        struct TableEntry newEntry = aTheirs[i];
        int j, conflict = 0;
        Pgno oldPg = newEntry.iTable;
        int forceRemap = bDisjointSchemaChanges;
        for(j=0; j<*pnMerged; j++){
          if( aMerged[j].iTable==newEntry.iTable ){
            conflict = 1;
            break;
          }
        }
        if( conflict || forceRemap ){
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = oldPg;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
        aMerged[(*pnMerged)++] = newEntry;
      }else if( bDisjointSchemaChanges ){
        struct TableEntry *oursIdx = findTableEntry(aOurs, nOurs, aTheirs[i].iTable);
        struct TableEntry *ancIdx = findTableEntry(aAnc, nAnc, aTheirs[i].iTable);
        if( oursIdx && !ancIdx
         && (prollyHashCompare(&oursIdx->root, &aTheirs[i].root)!=0
             || prollyHashCompare(&oursIdx->schemaHash, &aTheirs[i].schemaHash)!=0) ){
          struct TableEntry newEntry = aTheirs[i];
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = aTheirs[i].iTable;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
          aMerged[(*pnMerged)++] = newEntry;
        }
      }
      continue;
    }

    oursEntry = findTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;

    {
      struct TableEntry *ancEntry = findTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){

        struct TableEntry newEntry = aTheirs[i];
        int forceRemap = bDisjointSchemaChanges;
        {
          int j, conflict = 0;
          for(j=0; j<*pnMerged; j++){
            if( aMerged[j].iTable==newEntry.iTable ){ conflict = 1; break; }
          }
          if( conflict || forceRemap ) newEntry.iTable = (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;

        newEntry.zName = sqlite3_mprintf("%s", zName);
        aMerged[(*pnMerged)++] = newEntry;
      }else{

        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          return SQLITE_ERROR;
        }

      }
    }
  }
  return SQLITE_OK;
}

static void freeConflictTables(
  MergeConflictTable *aConflictTables,
  int nConflictTables
){
  int ci;
  for(ci=0; ci<nConflictTables; ci++){
    freeConflictRows(aConflictTables[ci].aRows, aConflictTables[ci].nConflicts);
    sqlite3_free(aConflictTables[ci].zName);
  }
  sqlite3_free(aConflictTables);
}

static int loadMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  struct TableEntry **paAnc, int *pnAnc, Pgno *piNextAnc,
  struct TableEntry **paOurs, int *pnOurs, Pgno *piNextOurs,
  struct TableEntry **paTheirs, int *pnTheirs, Pgno *piNextTheirs
){
  int rc;
  rc = doltliteLoadCatalog(db, ancestor, paAnc, pnAnc, piNextAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, ours, paOurs, pnOurs, piNextOurs);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteLoadCatalog(db, theirs, paTheirs, pnTheirs, piNextTheirs);
}

static int allocMergedCatalogEntries(
  int nOurs,
  int nTheirs,
  struct TableEntry **paMerged
){
  int nMergedAlloc = nOurs + nTheirs;
  if( nMergedAlloc==0 ) nMergedAlloc = 1;
  *paMerged = sqlite3_malloc(nMergedAlloc * (int)sizeof(struct TableEntry));
  return *paMerged ? SQLITE_OK : SQLITE_NOMEM;
}

static void recordMergeConflicts(
  sqlite3 *db,
  MergeConflictTable *aConflictTables,
  int nConflictTables
){
  ProllyHash conflictsHash;
  int rc2;

  rc2 = doltliteSerializeConflicts(
      doltliteGetChunkStore(db),
      (ConflictTableInfo*)aConflictTables, nConflictTables,
      &conflictsHash);
  if( rc2==SQLITE_OK ){
    extern void doltliteSetSessionConflictsCatalog(sqlite3*, const ProllyHash*);
    extern void doltliteSetSessionMergeState(sqlite3*, u8, const ProllyHash*, const ProllyHash*);
    doltliteSetSessionConflictsCatalog(db, &conflictsHash);
    doltliteSetSessionMergeState(db, 1, 0, &conflictsHash);
  }
}

int doltliteMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  ProllyHash *pMergedHash,
  int *pnConflicts,
  char **pzErrMsg,
  SchemaMergeAction **ppActions,
  int *pnActions,
  int bPreferOurMaster
){
  struct TableEntry *aAnc = 0, *aOurs = 0, *aTheirs = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0;
  Pgno iNextAnc = 2, iNextOurs = 2, iNextTheirs = 2;
  struct TableEntry *aMerged = 0;
  int nMerged = 0;
  int nMergedAlloc = 0;
  Pgno iNextMerged;
  int rc;
  int totalConflicts = 0;
  int bDisjointSchemaChanges = 0;
  SchemaRootpageRemap *aRemap = 0;
  int nRemap = 0;
  SchemaEntry *aAncSchema = 0, *aOursSchema = 0, *aTheirsSchema = 0;
  int nAncSchema = 0, nOursSchema = 0, nTheirsSchema = 0;

  MergeConflictTable *aConflictTables = 0;
  int nConflictTables = 0;
  (void)nMergedAlloc;

  rc = loadMergeCatalogs(db, ancestor, ours, theirs,
                         &aAnc, &nAnc, &iNextAnc,
                         &aOurs, &nOurs, &iNextOurs,
                         &aTheirs, &nTheirs, &iNextTheirs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = allocMergedCatalogEntries(nOurs, nTheirs, &aMerged);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    ProllyCache *pCache = doltliteGetCache(db);
    rc = loadSchemaFromCatalog(db, cs, pCache, ancestor, &aAncSchema, &nAncSchema);
    if( rc==SQLITE_OK ) rc = loadSchemaFromCatalog(db, cs, pCache, ours, &aOursSchema, &nOursSchema);
    if( rc==SQLITE_OK ) rc = loadSchemaFromCatalog(db, cs, pCache, theirs, &aTheirsSchema, &nTheirsSchema);
    if( rc!=SQLITE_OK ) goto merge_cleanup;
  }

  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;
  bDisjointSchemaChanges = catalogHasDisjointSchemaChanges(db, ancestor, ours, theirs);

  rc = mergeCatalogPass1(db, aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aAncSchema, nAncSchema,
                          aOursSchema, nOursSchema,
                          aTheirsSchema, nTheirsSchema,
                          aMerged, &nMerged,
                          &aConflictTables, &nConflictTables,
                          &totalConflicts, pzErrMsg,
                          ancestor, ours, theirs,
                          ppActions, pnActions,
                          bDisjointSchemaChanges,
                          bPreferOurMaster);
  if( rc!=SQLITE_OK ){
    int k;
    for(k=0; k<nMerged; k++) aMerged[k].zName = 0;
    goto merge_cleanup;
  }

  {
    int k;
    for(k=0; k<nMerged; k++){
      if( aMerged[k].zName ){
        char *z = sqlite3_mprintf("%s", aMerged[k].zName);
        if( !z ){ rc = SQLITE_NOMEM; goto merge_cleanup; }
        aMerged[k].zName = z;
      }
    }
  }

  rc = mergeCatalogPass2(aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aAncSchema, nAncSchema,
                          aOursSchema, nOursSchema,
                          aTheirsSchema, nTheirsSchema,
                          aMerged, &nMerged, &iNextMerged,
                          bDisjointSchemaChanges,
                          &aRemap, &nRemap);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = rebuildDisjointSchemaRows(db, aMerged, nMerged,
                                 aTheirsSchema, nTheirsSchema,
                                 aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 aRemap, nRemap);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              aTheirsSchema, nTheirsSchema, pMergedHash);
  if( pnConflicts ) *pnConflicts = totalConflicts;

  if( totalConflicts>0 && nConflictTables>0 && rc==SQLITE_OK ){
    recordMergeConflicts(db, aConflictTables, nConflictTables);
  }

merge_cleanup:
  sqlite3_free(aRemap);
  freeSchemaEntries(aAncSchema, nAncSchema);
  freeSchemaEntries(aOursSchema, nOursSchema);
  freeSchemaEntries(aTheirsSchema, nTheirsSchema);
  freeConflictTables(aConflictTables, nConflictTables);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aOurs, nOurs);
  doltliteFreeCatalog(aTheirs, nTheirs);
  doltliteFreeCatalog(aMerged, nMerged);
  return rc;
}

#endif
