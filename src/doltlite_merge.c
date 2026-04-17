
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "prolly_three_way_diff.h"
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

/* Per-index state carried through the merge callback so that
** index edits are applied incrementally from the main table diff
** rather than via independent three-way merge. */
typedef struct MergeIndexInfo MergeIndexInfo;
struct MergeIndexInfo {
  Pgno iTable;           /* catalog iTable for this index */
  ProllyHash oursRoot;   /* ours-side index root */
  ProllyHash mergedRoot; /* result (filled after flush) */
  ProllyMutMap *pEdits;  /* accumulated index edits */
  int nColumn;           /* number of index columns (excl PK tail) */
  i16 *aiColumn;         /* column mapping: index position → table col */
  KeyInfo *pKeyInfo;     /* collation info for sort key encoding */
};

/* Build an index sort key from a table row record.
** Extracts the columns specified by aiColumn, appends the remaining
** columns (the PK tail), builds a new record, and encodes it as a
** sort key with all fields (matching the insert encoding). */
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

  /* Build a new record with fields in index order (index cols +
  ** all remaining cols as the PK tail). The SQLite index record
  ** format is: [header][field1][field2]... where the header
  ** contains varint serial types. */
  {
    int i, hdrLen = 0, bodyLen = 0;
    int nTotal;
    u8 *p;

    /* Compute total number of fields: index cols + all table cols
    ** (the full record is the value in the prolly tree, and all
    ** fields go into the sort key for uniqueness). We reorder:
    ** first the aiColumn fields, then all remaining fields. */
    int nOutField = info.nField;
    int *aFieldOrder = sqlite3_malloc(nOutField * sizeof(int));
    u8 *aUsed = sqlite3_malloc(info.nField);
    if( !aFieldOrder || !aUsed ){
      sqlite3_free(aFieldOrder);
      sqlite3_free(aUsed);
      return SQLITE_NOMEM;
    }
    memset(aUsed, 0, info.nField);

    /* First: index columns in index order */
    {
      int out = 0;
      for(i=0; i<nIdxCol; i++){
        int col = aiColumn[i];
        if( col>=0 && col<info.nField ){
          aFieldOrder[out++] = col;
          aUsed[col] = 1;
        }
      }
      /* Then: remaining columns (PK tail + any others) */
      for(i=0; i<info.nField; i++){
        if( !aUsed[i] ) aFieldOrder[out++] = i;
      }
      nOutField = out;
    }

    /* Measure header and body sizes */
    for(i=0; i<nOutField; i++){
      int col = aFieldOrder[i];
      int st = info.aType[col];
      int flen;
      hdrLen += sqlite3VarintLen(st);
      /* Compute field length from serial type */
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

    /* Header size includes the header-size varint itself */
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

    /* Write header */
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

    /* Write body */
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

  /* Encode the projected record as a sort key (all fields). */
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
  int hdrBytes, nFields = 0, nAlloc = 0, bodyOff;
  RecField *aFields = 0;

  if(!pRec || nRec<1) { *ppFields=0; *pnFields=0; return 0; }
  pPos = pRec; pEnd = pRec + nRec;
  hdrBytes = dlReadVarint(pPos, pEnd, &hdrSize);
  pPos += hdrBytes;
  pHdrEnd = pRec + (int)hdrSize;
  bodyOff = (int)hdrSize;

  while(pPos < pHdrEnd && pPos < pEnd){
    u64 st; int stBytes, sz;
    stBytes = dlReadVarint(pPos, pHdrEnd, &st);
    pPos += stBytes;
    sz = dlSerialTypeLen(st);

    if(nFields >= nAlloc){
      RecField *aNew;
      nAlloc = nAlloc ? nAlloc*2 : 16;
      aNew = sqlite3_realloc(aFields, nAlloc*(int)sizeof(RecField));
      if(!aNew){
        sqlite3_free(aFields);
        return -1;
      }
      aFields = aNew;
    }
    aFields[nFields].st = st;
    aFields[nFields].off = bodyOff;
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

/* Field-level 3-way merge. A row with a modify-modify conflict at
** the row level still merges cleanly IF ours and theirs touched
** different columns. For each field, pick the side that changed
** (vs base); if both changed, merge only if the values match.
** Returns NULL on any unresolvable field, pushing the row into the
** conflict table. */
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
        /* Both sides dropped this field (baseHas && !oursHas &&
        ** !theirsHas), or the impossible (0,0,0) case. Field-level
        ** merge can't represent a dropped field in the output —
        ** fall back to row-level conflict handling. */
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

/* The mutator starts from oursRoot, so LEFT_* (our-side-only)
** changes are already baked into the starting tree and must NOT be
** re-applied. Only RIGHT_* changes (theirs-only) need to be
** inserted/deleted on top. CONVERGENT is a no-op for the same
** reason. CONFLICT_MM first tries field-level merge via
** tryCellMerge — non-overlapping field edits auto-resolve — and
** falls through to CONFLICT_DM (conflict record) if cells overlap. */
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
      /* Apply add to each secondary index. */
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
      /* Update each index: delete old entry, insert new. */
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
      /* Delete from each index. */
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
        /* Cell merge resolved: update indexes with the merged record.
        ** Delete base entry, insert merged entry. */
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

      struct ConflictRow *aNew;
      if( ctx->nConflicts >= ctx->nConflictsAlloc ){
        int nNew = ctx->nConflictsAlloc ? ctx->nConflictsAlloc*2 : 16;
        aNew = sqlite3_realloc(ctx->aConflicts, nNew*(int)sizeof(struct ConflictRow));
        if( !aNew ) return SQLITE_NOMEM;
        ctx->aConflicts = aNew;
        ctx->nConflictsAlloc = nNew;
      }
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

  /* Initialize a mutmap for each index. */
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

  /* Flush each index's accumulated edits. */
  for(i=0; i<nIndexes && rc==SQLITE_OK; i++){
    if( !prollyMutMapIsEmpty(aIndexes[i].pEdits) ){
      ProllyMutator idxMut;
      memset(&idxMut, 0, sizeof(idxMut));
      idxMut.pStore = cs;
      idxMut.pCache = cache;
      memcpy(&idxMut.oldRoot, &aIndexes[i].oursRoot, sizeof(ProllyHash));
      idxMut.pEdits = aIndexes[i].pEdits;
      idxMut.flags = 0;  /* blob keys */
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

            if( nCols >= nAlloc ){
              int nNew = nAlloc ? nAlloc*2 : 8;
              ParsedColumn *aNew = sqlite3_realloc(aCols, nNew*(int)sizeof(ParsedColumn));
              if( !aNew ){
                sqlite3_free(zName);
                sqlite3_free(zTrimmed);
                { int ci; for(ci=0;ci<nCols;ci++){
                  sqlite3_free(aCols[ci].zName);
                  sqlite3_free(aCols[ci].zDef);
                }}
                sqlite3_free(aCols);
                return SQLITE_NOMEM;
              }
              aCols = aNew;
              nAlloc = nNew;
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

        if( nAdd >= nAddAlloc ){
          int nNew = nAddAlloc ? nAddAlloc*2 : 4;
          char **azNew = sqlite3_realloc(azAdd, nNew*(int)sizeof(char*));
          if( !azNew ){ rc = SQLITE_NOMEM; goto schema_merge_cleanup; }
          azAdd = azNew;
          nAddAlloc = nNew;
        }
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
  ProllyHash *pOutHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *buf = 0;
  int nBuf = 0;
  int rc;

  (void)oursCatHash;
  (void)iNextTable;

  rc = doltliteSerializeCatalogEntries(db, aMerged, nMerged, &buf, &nBuf);
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

/* Pass 1: walk OUR side, resolving each table against anc/theirs.
** For every "ours" table, determine whether row-merge is needed and
** invoke mergeTableRows; any rows that can't auto-merge become
** conflict entries. Table iTable==1 (the catalog itself) is deferred
** to the end so schema actions collected earlier can influence its
** merge decision. Pass 2 picks up any tables that exist in theirs
** but not ours. */
static int mergeCatalogPass1(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  struct TableEntry *aMerged, int *pnMerged,
  MergeConflictTable **ppConflictTables, int *pnConflictTables,
  int *pTotalConflicts,
  char **pzErrMsg,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions, int *pnSchemaActions
){
  int i, rc = SQLITE_OK;
  int iTable1Idx = -1;

  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;


    if( aOurs[i].iTable==1 ){
      iTable1Idx = i;
      continue;
    }


    if( !zName ){
      /* Nameless catalog entry — secondary index. Three-way merge
      ** by iTable number. With PK-suffix sort keys, the three-way
      ** merge handles non-conflict cases correctly. For conflicts,
      ** the parent table's mergeTableRows overwrites the root with
      ** the incrementally-computed result. */
      ancEntry = findTableEntry(aAnc, nAnc, aOurs[i].iTable);
      theirsEntry = findTableEntry(aTheirs, nTheirs, aOurs[i].iTable);
      goto do_merge_entry;
    }


    ancEntry = findTableByName(aAnc, nAnc, zName);
    theirsEntry = findTableByName(aTheirs, nTheirs, zName);

do_merge_entry:

    if( !ancEntry ){

      if( theirsEntry ){

        if( prollyHashCompare(&aOurs[i].root, &theirsEntry->root)!=0
         || prollyHashCompare(&aOurs[i].schemaHash, &theirsEntry->schemaHash)!=0 ){
          if( pzErrMsg ){
            *pzErrMsg = sqlite3_mprintf(
              "schema conflict: table '%s' added on both branches with "
              "different definitions", zName);
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
              "and deleted on the other", zName);
          }
          return SQLITE_ERROR;
        }

      }else{
        int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
        int ourSchemaChanged = prollyHashCompare(
            &aOurs[i].schemaHash, &ancEntry->schemaHash)!=0;
        int theirSchemaChanged = prollyHashCompare(
            &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;
        int skipRowMerge = 0;


        if( ourSchemaChanged && theirSchemaChanged
         && prollyHashCompare(&aOurs[i].schemaHash,
                              &theirsEntry->schemaHash)!=0 ){
          rc = tryResolveSchemaDivergence(
            db, zName, pCatAnc, pCatOurs, pCatTheirs,
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

            /* Collect secondary indexes for this table so they can
            ** be updated incrementally during the row merge. */
            MergeIndexInfo *aIdxInfo = 0;
            int nIdxInfo = 0;
            if( zName && db ){
              Table *pTab = sqlite3FindTable(db, zName, 0);
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
                        mi->pKeyInfo = 0; /* BINARY collation; NOCASE TBD */
                        nIdxInfo++;
                      }
                    }
                  }
                }
              }
            }

            rc = mergeTableRows(db, &ancEntry->root, &aOurs[i].root,
                                &theirsEntry->root, aOurs[i].flags,
                                &mergedTableRoot, &nConflicts, &aConflictRows,
                                aIdxInfo, nIdxInfo);

            /* Store merged index roots back into aMerged catalog. */
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
                                       zName, nConflicts, aConflictRows);
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

      if( oursChanged && theirsChanged && hasSchemaActions ){

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
  struct TableEntry *aMerged, int *pnMerged,
  Pgno *piNextMerged
){
  int i;

  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 || !zName ) continue;

    oursEntry = findTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;

    {
      struct TableEntry *ancEntry = findTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){

        struct TableEntry newEntry = aTheirs[i];
        {
          int j, conflict = 0;
          for(j=0; j<*pnMerged; j++){
            if( aMerged[j].iTable==newEntry.iTable ){ conflict = 1; break; }
          }
          if( conflict ) newEntry.iTable = (*piNextMerged)++;
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
  int *pnActions
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


  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;



  rc = mergeCatalogPass1(db, aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aMerged, &nMerged,
                          &aConflictTables, &nConflictTables,
                          &totalConflicts, pzErrMsg,
                          ancestor, ours, theirs,
                          ppActions, pnActions);
  if( rc!=SQLITE_OK ){
    /* pass1 shallow-copies zName pointers from aOurs into aMerged.
    ** The strdup loop below (which breaks the aliasing) hasn't run
    ** yet, so NULL out the shallow copies before merge_cleanup frees
    ** both arrays — otherwise it's a double-free. */
    int k;
    for(k=0; k<nMerged; k++) aMerged[k].zName = 0;
    goto merge_cleanup;
  }

  /* pass1 shallow-copies TableEntry structs from aOurs into aMerged,
  ** which means zName pointers are aliased — aMerged[k].zName ==
  ** aOurs[i].zName for some i. serializeMergedCatalog below frees
  ** those strings via doltliteResolveTableNumber's refresh step,
  ** leaving aOurs with dangling pointers. Break the aliasing here
  ** by strdup'ing every aMerged zName so aMerged owns its own
  ** storage and all four catalogs can be cleaned up independently
  ** via doltliteFreeCatalog. pass2 already strdup's on append, so
  ** this loop only needs to fix pass1's output. */
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
                          aMerged, &nMerged, &iNextMerged);
  if( rc!=SQLITE_OK ) goto merge_cleanup;


  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              pMergedHash);
  if( pnConflicts ) *pnConflicts = totalConflicts;


  if( totalConflicts>0 && nConflictTables>0 && rc==SQLITE_OK ){
    recordMergeConflicts(db, aConflictTables, nConflictTables);
  }

merge_cleanup:
  freeConflictTables(aConflictTables, nConflictTables);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aOurs, nOurs);
  doltliteFreeCatalog(aTheirs, nTheirs);
  doltliteFreeCatalog(aMerged, nMerged);
  return rc;
}

#endif
