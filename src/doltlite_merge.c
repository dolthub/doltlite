
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "prolly_three_way_diff.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>

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

typedef struct RowMergeCtx RowMergeCtx;
struct RowMergeCtx {
  ProllyMutMap *pEdits;      
  u8 isIntKey;
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
      nAlloc = nAlloc ? nAlloc*2 : 16;
      aFields = sqlite3_realloc(aFields, nAlloc*(int)sizeof(RecField));
      if(!aFields) return -1;
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
        
        continue;
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
      break;

    case THREE_WAY_RIGHT_MODIFY:
      
      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      break;

    case THREE_WAY_RIGHT_DELETE:
      
      rc = prollyMutMapDelete(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey);
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
          cr->pKey = sqlite3_malloc(pChange->nKey);
          if(cr->pKey) memcpy(cr->pKey, pChange->pKey, pChange->nKey);
          cr->nKey = pChange->nKey;
        }
        if( pChange->pBaseVal && pChange->nBaseVal>0 ){
          cr->pBaseVal = sqlite3_malloc(pChange->nBaseVal);
          if(cr->pBaseVal) memcpy(cr->pBaseVal, pChange->pBaseVal, pChange->nBaseVal);
          cr->nBaseVal = pChange->nBaseVal;
        }
        if( pChange->pOurVal && pChange->nOurVal>0 ){
          cr->pOurVal = sqlite3_malloc(pChange->nOurVal);
          if(cr->pOurVal) memcpy(cr->pOurVal, pChange->pOurVal, pChange->nOurVal);
          cr->nOurVal = pChange->nOurVal;
        }
        if( pChange->pTheirVal && pChange->nTheirVal>0 ){
          cr->pTheirVal = sqlite3_malloc(pChange->nTheirVal);
          if(cr->pTheirVal) memcpy(cr->pTheirVal, pChange->pTheirVal, pChange->nTheirVal);
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
  struct ConflictRow **ppConflicts
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);

  RowMergeCtx ctx;
  ProllyMutator mut;
  int rc;

  memset(&ctx, 0, sizeof(ctx));
  ctx.isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  ctx.pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !ctx.pEdits ) return SQLITE_NOMEM;
  rc = prollyMutMapInit(ctx.pEdits, ctx.isIntKey);
  if( rc!=SQLITE_OK ){ sqlite3_free(ctx.pEdits); return rc; }

  
  rc = prollyThreeWayDiff(cs, cache, pAncRoot, pOursRoot, pTheirsRoot,
                          flags, rowMergeCallback, &ctx);
  if( rc!=SQLITE_OK ){
    freeRowMergeCtx(&ctx);
    return rc;
  }

  
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

  *pnConflicts = ctx.nConflicts;
  *ppConflicts = ctx.aConflicts;
  ctx.aConflicts = 0; 
  ctx.nConflicts = 0;

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

static int serializeMergedCatalog(
  sqlite3 *db,
  const ProllyHash *oursCatHash,     
  struct TableEntry *aMerged,
  int nMerged,
  Pgno iNextTable,
  ProllyHash *pOutHash               
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int sz = 1 + 4 + 4;  
  { int j; for(j=0;j<nMerged;j++){
    int nl = aMerged[j].zName ? (int)strlen(aMerged[j].zName) : 0;
    sz += 4+1+PROLLY_HASH_SIZE+PROLLY_HASH_SIZE+2+nl;
  }}
  u8 *buf;
  u8 *p;
  int rc;

  (void)oursCatHash;  

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  
  *p++ = 0x43;  
  p[0] = (u8)iNextTable;
  p[1] = (u8)(iNextTable>>8);
  p[2] = (u8)(iNextTable>>16);
  p[3] = (u8)(iNextTable>>24);
  p += 4;
  p[0] = (u8)nMerged;
  p[1] = (u8)(nMerged>>8);
  p[2] = (u8)(nMerged>>16);
  p[3] = (u8)(nMerged>>24);
  p += 4;

  
  {
    int i;
    for(i=0; i<nMerged; i++){
      Pgno pg = aMerged[i].iTable;
      int nl = aMerged[i].zName ? (int)strlen(aMerged[i].zName) : 0;
      p[0] = (u8)pg;
      p[1] = (u8)(pg>>8);
      p[2] = (u8)(pg>>16);
      p[3] = (u8)(pg>>24);
      p += 4;
      *p++ = aMerged[i].flags;
      memcpy(p, aMerged[i].root.data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      memcpy(p, aMerged[i].schemaHash.data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      p[0]=(u8)nl; p[1]=(u8)(nl>>8); p+=2;
      if(nl>0) memcpy(p, aMerged[i].zName, nl);
      p += nl;
    }
  }

  rc = chunkStorePut(cs, buf, (int)(p - buf), pOutHash);
  sqlite3_free(buf);
  return rc;
}

typedef struct MergeConflictTable MergeConflictTable;
struct MergeConflictTable {
  char *zName;
  int nConflicts;
  struct ConflictRow *aRows;
};

static int mergeCatalogPass1(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  struct TableEntry *aMerged, int *pnMerged,
  MergeConflictTable **ppConflictTables, int *pnConflictTables,
  int *pTotalConflicts
){
  int i, rc = SQLITE_OK;

  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;

    
    if( aOurs[i].iTable==1 ){
      ancEntry = findTableEntry(aAnc, nAnc, 1);
      theirsEntry = findTableEntry(aTheirs, nTheirs, 1);
      goto do_merge_entry;
    }

    
    if( !zName ){
      aMerged[(*pnMerged)++] = aOurs[i];
      continue;
    }

    
    ancEntry = findTableByName(aAnc, nAnc, zName);
    theirsEntry = findTableByName(aTheirs, nTheirs, zName);

do_merge_entry:

    if( !ancEntry ){
      
      if( theirsEntry ){
        
        if( prollyHashCompare(&aOurs[i].root, &theirsEntry->root)!=0
         || prollyHashCompare(&aOurs[i].schemaHash, &theirsEntry->schemaHash)!=0 ){
          return SQLITE_ERROR;
        }
        
      }
      aMerged[(*pnMerged)++] = aOurs[i];
    }else{
      
      int oursChanged = prollyHashCompare(&aOurs[i].root, &ancEntry->root)!=0;

      if( !theirsEntry ){
        
        if( oursChanged ){
          return SQLITE_ERROR;  
        }
        
      }else{
        int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
        if( oursChanged && theirsChanged ){
          
          ProllyHash mergedTableRoot;
          int nConflicts = 0;
          struct ConflictRow *aConflictRows = 0;

          rc = mergeTableRows(db, &ancEntry->root, &aOurs[i].root,
                              &theirsEntry->root, aOurs[i].flags,
                              &mergedTableRoot, &nConflicts, &aConflictRows);
          if( rc!=SQLITE_OK ) return rc;

          {
            struct TableEntry merged = aOurs[i];
            memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
            aMerged[(*pnMerged)++] = merged;
          }

          if( nConflicts>0 ){
            *pTotalConflicts += nConflicts;
            {
              MergeConflictTable *aNew = sqlite3_realloc(*ppConflictTables,
                (*pnConflictTables+1)*(int)sizeof(MergeConflictTable));
              if( aNew ){
                *ppConflictTables = aNew;
                aNew[*pnConflictTables].zName = sqlite3_mprintf("%s", zName);
                aNew[*pnConflictTables].nConflicts = nConflicts;
                aNew[*pnConflictTables].aRows = aConflictRows;
                (*pnConflictTables)++;
                aConflictRows = 0; 
              }else{
                { int j; for(j=0;j<nConflicts;j++){
                  sqlite3_free(aConflictRows[j].pKey);
                  sqlite3_free(aConflictRows[j].pBaseVal);
                  sqlite3_free(aConflictRows[j].pTheirVal);
                }}
                sqlite3_free(aConflictRows);
              }
            }
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

int doltliteMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  ProllyHash *pMergedHash,
  int *pnConflicts          
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

  
  rc = doltliteLoadCatalog(db, ancestor, &aAnc, &nAnc, &iNextAnc);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, ours, &aOurs, &nOurs, &iNextOurs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, theirs, &aTheirs, &nTheirs, &iNextTheirs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  
  nMergedAlloc = nOurs + nTheirs;
  if( nMergedAlloc==0 ) nMergedAlloc = 1;
  aMerged = sqlite3_malloc(nMergedAlloc * (int)sizeof(struct TableEntry));
  if( !aMerged ){
    rc = SQLITE_NOMEM;
    goto merge_cleanup;
  }

  
  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;

  

  rc = mergeCatalogPass1(db, aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aMerged, &nMerged,
                          &aConflictTables, &nConflictTables,
                          &totalConflicts);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = mergeCatalogPass2(aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
                          aMerged, &nMerged, &iNextMerged);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  
  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              pMergedHash);
  if( pnConflicts ) *pnConflicts = totalConflicts;

  
  if( totalConflicts>0 && nConflictTables>0 && rc==SQLITE_OK ){
    ProllyHash conflictsHash;
    
    int rc2 = doltliteSerializeConflicts(
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

  
  {
    int ci;
    for(ci=0; ci<nConflictTables; ci++){
      int cj;
      for(cj=0; cj<aConflictTables[ci].nConflicts; cj++){
        sqlite3_free(aConflictTables[ci].aRows[cj].pKey);
        sqlite3_free(aConflictTables[ci].aRows[cj].pBaseVal);
        sqlite3_free(aConflictTables[ci].aRows[cj].pOurVal);
        sqlite3_free(aConflictTables[ci].aRows[cj].pTheirVal);
      }
      sqlite3_free(aConflictTables[ci].aRows);
      sqlite3_free(aConflictTables[ci].zName);
    }
    sqlite3_free(aConflictTables);
  }

merge_cleanup:
  sqlite3_free(aAnc);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  sqlite3_free(aMerged);
  return rc;
}

#endif 
