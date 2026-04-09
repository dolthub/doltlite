/*
** Three-way catalog merge. Algorithm:
**  1. Load ancestor/ours/theirs catalogs (table lists).
**  2. Pass 1 (mergeCatalogPass1): for each table in "ours", compare against
**     ancestor and theirs. If both sides changed the same table, perform a
**     row-level three-way merge via prollyThreeWayDiff, attempting per-field
**     cell merge for modify/modify conflicts before recording true conflicts.
**  3. Pass 2 (mergeCatalogPass2): add tables that exist only in "theirs"
**     (new tables added on the other branch).
**  4. Serialize the merged catalog and any conflict rows.
**
** Row-level merge: LEFT changes (ours) are already in the "ours" tree, so
** only RIGHT changes (theirs) need to be applied as edits. Convergent
** changes (both sides made the same edit) are no-ops.
*/
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
      /* Left (ours) changes are already present in the "ours" tree that
      ** we are mutating, so no edits needed. */
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
      /* Both sides modified the same row. Try per-field cell merge: if each
      ** column was changed by at most one side, merge succeeds. Otherwise
      ** fall through to record a conflict (intentional fallthrough to DM). */
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
      /* FALLTHROUGH: cell merge failed, record as conflict */
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

/*
** Column definition parsed from CREATE TABLE SQL.
*/
typedef struct ParsedColumn ParsedColumn;
struct ParsedColumn {
  char *zName;    /* Column name (lowercased for comparison) */
  char *zDef;     /* Full column definition text */
};

/*
** Parse columns from a CREATE TABLE SQL string.
** Returns an array of ParsedColumn entries.
** Caller must free with freeColumns().
*/
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

  /* Find the opening '(' after CREATE TABLE ... */
  p = zSql;
  while( *p && *p!='(' ) p++;
  if( *p!='(' ) return SQLITE_OK;
  p++; /* skip '(' */

  /* Find the matching closing ')' */
  pEnd = p;
  depth = 1;
  while( *pEnd && depth>0 ){
    if( *pEnd=='(' ) depth++;
    else if( *pEnd==')' ) depth--;
    pEnd++;
  }
  if( depth!=0 ) return SQLITE_OK;
  pEnd--; /* back to the ')' */

  /* Now parse comma-separated segments between p and pEnd */
  segStart = p;
  depth = 0;
  while( p <= pEnd ){
    if( p==pEnd || (*p==',' && depth==0) ){
      /* We have a segment from segStart to p */
      const char *s = segStart;
      const char *e = (p==pEnd) ? p : p;
      char *zTrimmed;
      int len;

      /* Trim whitespace */
      while( s<e && isspace((unsigned char)*s) ) s++;
      while( e>s && isspace((unsigned char)*(e-1)) ) e--;

      len = (int)(e - s);
      if( len > 0 ){
        /* Check if this is a table constraint (not a column definition) */
        /* Table constraints start with: PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY, CONSTRAINT */
        int isConstraint = 0;
        {
          const char *t = s;
          /* Skip leading whitespace (already done) */
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
          /* This is a column definition */
          zTrimmed = sqlite3_malloc(len + 1);
          if( !zTrimmed ){
            /* free what we have */
            { int ci; for(ci=0;ci<nCols;ci++){
              sqlite3_free(aCols[ci].zName);
              sqlite3_free(aCols[ci].zDef);
            }}
            sqlite3_free(aCols);
            return SQLITE_NOMEM;
          }
          memcpy(zTrimmed, s, len);
          zTrimmed[len] = 0;

          /* Extract column name: first token */
          {
            char *zName;
            const char *nameStart = s;
            const char *nameEnd = nameStart;
            int nameLen;
            /* Handle quoted names */
            if( *nameStart=='"' || *nameStart=='`' || *nameStart=='[' ){
              char closeChar = (*nameStart=='"') ? '"' :
                               (*nameStart=='`') ? '`' : ']';
              nameEnd = nameStart + 1;
              while( nameEnd<e && *nameEnd!=closeChar ) nameEnd++;
              if( nameEnd<e ) nameEnd++; /* include close quote */
              nameLen = (int)(nameEnd - nameStart);
              /* Store unquoted name for comparison */
              zName = sqlite3_malloc(nameLen - 1);  /* minus 2 quotes + 1 NUL */
              if( zName ){
                memcpy(zName, nameStart+1, nameLen-2);
                zName[nameLen-2] = 0;
                /* Lowercase for comparison */
                { int ci; for(ci=0;zName[ci];ci++) zName[ci]=(char)tolower((unsigned char)zName[ci]); }
              }
            }else{
              while( nameEnd<e && !isspace((unsigned char)*nameEnd)
                  && *nameEnd!='(' && *nameEnd!=',' ) nameEnd++;
              nameLen = (int)(nameEnd - nameStart);
              zName = sqlite3_malloc(nameLen + 1);
              if( zName ){
                memcpy(zName, nameStart, nameLen);
                zName[nameLen] = 0;
                /* Lowercase for comparison */
                { int ci; for(ci=0;zName[ci];ci++) zName[ci]=(char)tolower((unsigned char)zName[ci]); }
              }
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

/*
** Attempt a three-way schema merge for a single table.
** Returns SQLITE_OK if merge succeeds, SQLITE_ERROR on conflict.
** On success, *ppAddCols / *pnAddCols contain ALTER TABLE ADD COLUMN
** definitions that must be applied to "ours" to complete the merge,
** and *pUseTheirSchema is set if we should use theirs' schema hash.
*/
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

  /* Check for conflicts: columns added/modified/dropped on both sides */

  /* Check their adds: columns in theirs but not in ancestor */
  for(i=0; i<nTheirs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aTheirs[i].zName);
    if( !ancCol ){
      /* theirs added this column */
      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){
        /* Both sides added column with same name - check if same definition */
        if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){
          /* Different definitions -> conflict */
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        /* Same definition -> convergent, no action needed */
      }else{
        /* Only theirs added this column -> record as ADD COLUMN */
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
      /* Column existed in ancestor - check if modified differently */
      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){
        int ancToTheirs = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        int ancToOurs = strcmp(ancCol->zDef, ourCol->zDef)!=0;
        if( ancToTheirs && ancToOurs ){
          /* Both modified the same column */
          if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){
            /* Different modifications -> conflict */
            if( pzErrDetail ){
              *pzErrDetail = sqlite3_mprintf(
                "both branches modified column '%s' differently",
                aTheirs[i].zName);
            }
            rc = SQLITE_ERROR;
            goto schema_merge_cleanup;
          }
          /* Same modification -> convergent, no conflict */
        }
      }else{
        /* Column existed in ancestor, is in theirs, but not in ours -> ours dropped it */
        int theirsModified = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        if( theirsModified ){
          /* Ours dropped, theirs modified -> conflict */
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        /* Ours dropped, theirs didn't modify -> drop wins, no conflict */
      }
    }
  }

  /* Check our adds that might conflict with theirs drops */
  for(i=0; i<nOurs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aOurs[i].zName);
    if( ancCol ){
      /* Column existed in ancestor, is in ours */
      ParsedColumn *theirCol = findColumn(aTheirs, nTheirs, aOurs[i].zName);
      if( !theirCol ){
        /* Theirs dropped this column */
        int oursModified = strcmp(ancCol->zDef, aOurs[i].zDef)!=0;
        if( oursModified ){
          /* Theirs dropped, ours modified -> conflict */
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aOurs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        /* Theirs dropped, ours didn't modify -> drop wins, no conflict */
      }
    }
    /* Our-only adds are already in our schema, no action needed */
  }

  /* If we get here, merge is possible */
  if( nAdd > 0 ){
    *pUseTheirSchema = 0; /* We'll use our schema + ADD COLUMN */
  }
  *ppAddCols = azAdd;
  *pnAddCols = nAdd;
  azAdd = 0; nAdd = 0; /* ownership transferred */

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
  int iTable1Idx = -1;  /* Index of table 1 entry, deferred until end */

  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;

    /* Defer table 1 (sqlite_master) until after user tables, so we know
    ** whether schema merge actions occurred. */
    if( aOurs[i].iTable==1 ){
      iTable1Idx = i;
      continue;
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
        int ourSchemaChanged = prollyHashCompare(
            &aOurs[i].schemaHash, &ancEntry->schemaHash)!=0;
        int theirSchemaChanged = prollyHashCompare(
            &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;
        int skipRowMerge = 0;

        /* Schema divergence check: both sides changed the schema differently.
        ** This must be checked regardless of whether data (root) changed. */
        if( ourSchemaChanged && theirSchemaChanged
         && prollyHashCompare(&aOurs[i].schemaHash,
                              &theirsEntry->schemaHash)!=0 ){
          /* Both sides changed the schema differently. Attempt column-level merge. */
          ChunkStore *csLocal = doltliteGetChunkStore(db);
          ProllyCache *cacheLocal = doltliteGetCache(db);
          SchemaEntry *aAncSchema=0, *aOursSchema=0, *aTheirsSchema=0;
          int nAncSchema=0, nOursSchema=0, nTheirsSchema=0;
          SchemaEntry *ancSchEntry=0, *ourSchEntry=0, *theirSchEntry=0;
          char **azAddCols = 0;
          int nAddCols = 0;
          int useTheirSchema = 0;
          char *zSchemaErr = 0;

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
            zSchemaErr = sqlite3_mprintf(
              "cannot load schemas for merge");
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
            { int j; for(j=0;j<nAddCols;j++) sqlite3_free(azAddCols[j]); }
            sqlite3_free(azAddCols);
            return SQLITE_ERROR;
          }
          sqlite3_free(zSchemaErr);

          /* Schema merge succeeded. When there are columns to add from theirs,
          ** skip the row-level merge (which would produce spurious conflicts due
          ** to column position misalignment) and just use ours' data tree.
          ** The caller will ALTER TABLE ADD COLUMN to add theirs' columns. */
          if( nAddCols > 0 ){
            /* Record schema merge actions */
            if( ppSchemaActions && pnSchemaActions ){
              SchemaMergeAction *aNew = sqlite3_realloc(*ppSchemaActions,
                (*pnSchemaActions+1)*(int)sizeof(SchemaMergeAction));
              if( aNew ){
                *ppSchemaActions = aNew;
                aNew[*pnSchemaActions].zTableName = sqlite3_mprintf("%s", zName);
                aNew[*pnSchemaActions].azAddColumns = azAddCols;
                aNew[*pnSchemaActions].nAddColumns = nAddCols;
                (*pnSchemaActions)++;
                azAddCols = 0; nAddCols = 0; /* ownership transferred */
              }
            }
            { int j; for(j=0;j<nAddCols;j++) sqlite3_free(azAddCols[j]); }
            sqlite3_free(azAddCols);

            /* Use ours' data tree as-is (skip row merge) */
            aMerged[(*pnMerged)++] = aOurs[i];
            skipRowMerge = 1;
          }else{
            /* Schema merge succeeded with no columns to add (e.g., both added
            ** the same column identically). Proceed with normal row merge. */
            { int j; for(j=0;j<nAddCols;j++) sqlite3_free(azAddCols[j]); }
            sqlite3_free(azAddCols);
          }
        }

        if( !skipRowMerge ){
          if( oursChanged && theirsChanged ){
            /* Normal row-level merge. */
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
              /* If only theirs changed the schema, use their schema. */
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
        } /* !skipRowMerge */
      }
    }
  }

  /* Now handle table 1 (sqlite_master), which was deferred. */
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
        /* Schema merge is happening — use ours' table 1 as-is.
        ** The caller's ALTER TABLE ADD COLUMN will update sqlite_master. */
        aMerged[(*pnMerged)++] = aOurs[iTable1Idx];
      }else if( oursChanged && theirsChanged ){
        /* Normal row merge for table 1 */
        ProllyHash mergedTableRoot;
        int nConflicts = 0;
        struct ConflictRow *aConflictRows = 0;
        int theirSchemaChanged2 = prollyHashCompare(
            &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;

        rc = mergeTableRows(db, &ancEntry->root, &aOurs[iTable1Idx].root,
                            &theirsEntry->root, aOurs[iTable1Idx].flags,
                            &mergedTableRoot, &nConflicts, &aConflictRows);
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
          {
            MergeConflictTable *aNew = sqlite3_realloc(*ppConflictTables,
              (*pnConflictTables+1)*(int)sizeof(MergeConflictTable));
            if( aNew ){
              *ppConflictTables = aNew;
              aNew[*pnConflictTables].zName = sqlite3_mprintf("%s", "(sqlite_master)");
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
                          &totalConflicts, pzErrMsg,
                          ancestor, ours, theirs,
                          ppActions, pnActions);
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
