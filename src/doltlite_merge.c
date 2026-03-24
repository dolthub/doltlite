/*
** Three-way catalog merge with conflict detection for Doltlite.
**
** Given three catalog hashes (ancestor, ours, theirs), produce a merged
** catalog. For each table:
**   - If only one side changed it, take that side's root hash.
**   - If both sides changed the same table, return SQLITE_ERROR (conflict).
**   - If a table was added on one side and doesn't exist on the other, include it.
**   - If deleted on one side and unchanged on the other, delete it.
**
** Returns the merged catalog as a serialized chunk stored in the chunk store.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "prolly_three_way_diff.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include <string.h>

/* TableEntry struct — must match prolly_btree.c definition */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  char *zName;
  void *pPending;
};

/* From doltlite_conflicts.c */
typedef struct ConflictTableInfo ConflictTableInfo;
extern int doltliteSerializeConflicts(ChunkStore *cs, ConflictTableInfo *aTables,
                                       int nTables, ProllyHash *pHash);

/* Provided by prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/*
** Find a table entry by table number in an array of entries.
** Returns a pointer to the entry, or NULL if not found.
*/
static struct TableEntry *findTableEntry(
  struct TableEntry *aEntries,
  int nEntries,
  Pgno iTable
){
  int i;
  for(i=0; i<nEntries; i++){
    if( aEntries[i].iTable==iTable ) return &aEntries[i];
  }
  return 0;
}

/*
** Row-level merge context: collects non-conflicting edits into a MutMap
** and conflicting rows into a list.
*/
typedef struct RowMergeCtx RowMergeCtx;
struct RowMergeCtx {
  ProllyMutMap *pEdits;      /* Non-conflicting edits to apply */
  u8 isIntKey;
  int nConflicts;
  /* Conflict entries: stored as a simple array for later serialization */
  struct ConflictRow {
    i64 intKey;
    u8 *pKey; int nKey;
    u8 *pBaseVal; int nBaseVal;
    u8 *pOurVal; int nOurVal;
    u8 *pTheirVal; int nTheirVal;
  } *aConflicts;
  int nConflictsAlloc;
};

/* --------------------------------------------------------------------------
** Cell-level (field-by-field) merge of SQLite record blobs.
**
** When both sides modify the same row, parse the three records and compare
** each field. If changes don't overlap, build a merged record.
**
** Returns: merged record blob (caller frees) or NULL if there's a real
**          field-level conflict.
** -------------------------------------------------------------------------- */

/* Read a varint from a record. Returns bytes consumed. */
static int mergeReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v = 0; int i;
  for(i=0; i<9 && p+i<pEnd; i++){
    if(i<8){ v=(v<<7)|(p[i]&0x7f); if(!(p[i]&0x80)){*pVal=v; return i+1;} }
    else{ v=(v<<8)|p[i]; *pVal=v; return 9; }
  }
  *pVal=v; return i?i:1;
}

/* Write a varint. Returns bytes written. */
static int mergeWriteVarint(u8 *p, u64 v){
  u8 buf[9]; int i, n;
  if( v<=0x7f ){ p[0]=(u8)v; return 1; }
  n=0;
  for(i=0; v>0 && i<9; i++){ buf[i]=(u8)(v&0x7f); v>>=7; n++; }
  /* Actually, use the standard SQLite varint encoding */
  /* Simpler: write big-endian varint */
  if( v==0 && n<=1 ){ p[0]=(u8)buf[0]; return 1; }
  /* Fall back to a simple implementation */
  return 0; /* Will use the approach below instead */
}

/* Get the data size for a serial type. */
static int serialTypeSize(u64 st){
  if(st==0||st==8||st==9) return 0;
  if(st>=1&&st<=6){ static const int s[]={0,1,2,3,4,6,8}; return s[st]; }
  if(st==7) return 8;
  if(st>=12&&(st&1)==0) return ((int)st-12)/2;
  if(st>=13&&(st&1)==1) return ((int)st-13)/2;
  return 0;
}

/* Parse a record's fields into arrays of (serial_type, data_offset, data_len).
** Returns number of fields, or -1 on error. */
typedef struct RecField RecField;
struct RecField { u64 st; int off; int len; };

static int parseRecordFields(const u8 *pRec, int nRec,
                             RecField **ppFields, int *pnFields){
  const u8 *p, *pEnd, *pHdrEnd;
  u64 hdrSize;
  int hdrBytes, nFields = 0, nAlloc = 0, bodyOff;
  RecField *aFields = 0;

  if(!pRec || nRec<1) { *ppFields=0; *pnFields=0; return 0; }
  p = pRec; pEnd = pRec + nRec;
  hdrBytes = mergeReadVarint(p, pEnd, &hdrSize);
  p += hdrBytes;
  pHdrEnd = pRec + (int)hdrSize;
  bodyOff = (int)hdrSize;

  while(p < pHdrEnd && p < pEnd){
    u64 st; int stBytes, sz;
    stBytes = mergeReadVarint(p, pHdrEnd, &st);
    p += stBytes;
    sz = serialTypeSize(st);

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

/* Compare a single field between two records. Returns 0 if identical. */
static int fieldEquals(const u8 *pRecA, RecField *fA,
                       const u8 *pRecB, RecField *fB){
  if(fA->st != fB->st) return 1;
  if(fA->len != fB->len) return 1;
  if(fA->len==0) return 0;
  return memcmp(pRecA + fA->off, pRecB + fB->off, fA->len);
}

/*
** Try to merge three records field-by-field.
** Returns a new merged record blob (caller must sqlite3_free), or NULL
** if there's a real field-level conflict.
*/
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

  /* Parse all three records */
  if(parseRecordFields(pBase, nBase, &aBase, &nfBase)<0) goto fail;
  if(parseRecordFields(pOurs, nOurs, &aOurs, &nfOurs)<0) goto fail;
  if(parseRecordFields(pTheirs, nTheirs, &aTheirs, &nfTheirs)<0) goto fail;

  /* Determine max fields (handles schema changes adding columns) */
  nfMax = nfBase;
  if(nfOurs > nfMax) nfMax = nfOurs;
  if(nfTheirs > nfMax) nfMax = nfTheirs;

  /* For each field, decide which version to take */
  {
    /* We'll collect the winning (record, field) pairs, then build the result */
    struct { const u8 *pRec; RecField *pField; } *winners;
    int hdrSize, bodySize, pos;
    u8 *hdr, *body;

    winners = sqlite3_malloc(nfMax * (int)sizeof(*winners));
    if(!winners) goto fail;

    for(i=0; i<nfMax; i++){
      int baseHas = (i < nfBase);
      int oursHas = (i < nfOurs);
      int theirsHas = (i < nfTheirs);

      if(!baseHas && oursHas && !theirsHas){
        /* New field in ours only (schema change on our side) */
        winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
      }else if(!baseHas && !oursHas && theirsHas){
        /* New field in theirs only */
        winners[i].pRec = pTheirs; winners[i].pField = &aTheirs[i];
      }else if(!baseHas && oursHas && theirsHas){
        /* Both added this field (both did schema change) */
        if(fieldEquals(pOurs, &aOurs[i], pTheirs, &aTheirs[i])==0){
          winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
        }else{
          sqlite3_free(winners); goto fail; /* conflict */
        }
      }else if(baseHas && oursHas && theirsHas){
        /* Field exists in all three — compare */
        int oursChanged = fieldEquals(pBase, &aBase[i], pOurs, &aOurs[i]);
        int theirsChanged = fieldEquals(pBase, &aBase[i], pTheirs, &aTheirs[i]);
        if(!oursChanged && !theirsChanged){
          /* Neither changed */
          winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
        }else if(oursChanged && !theirsChanged){
          /* Only ours changed */
          winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
        }else if(!oursChanged && theirsChanged){
          /* Only theirs changed */
          winners[i].pRec = pTheirs; winners[i].pField = &aTheirs[i];
        }else{
          /* Both changed — check if convergent */
          if(fieldEquals(pOurs, &aOurs[i], pTheirs, &aTheirs[i])==0){
            winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
          }else{
            sqlite3_free(winners); goto fail; /* real conflict */
          }
        }
      }else if(baseHas && oursHas && !theirsHas){
        /* Field dropped in theirs? Take ours (theirs has fewer columns) */
        winners[i].pRec = pOurs; winners[i].pField = &aOurs[i];
      }else if(baseHas && !oursHas && theirsHas){
        /* Field dropped in ours? Take theirs */
        winners[i].pRec = pTheirs; winners[i].pField = &aTheirs[i];
      }else{
        /* Edge case: field only in base, dropped by both */
        continue;
      }
    }

    /* Build the merged record: header (varint sizes) + body (field data) */
    /* First pass: calculate header and body sizes */
    hdrSize = 0; bodySize = 0;
    for(i=0; i<nfMax; i++){
      u64 st = winners[i].pField->st;
      /* Varint size for this serial type */
      if(st <= 0x7f) hdrSize += 1;
      else if(st <= 0x3fff) hdrSize += 2;
      else if(st <= 0x1fffff) hdrSize += 3;
      else hdrSize += 4; /* good enough for practical values */
      bodySize += winners[i].pField->len;
    }
    /* Header size varint (the header size includes the header size varint itself) */
    { int tentative = hdrSize + 1;
      if(tentative > 0x7f) tentative++;
      hdrSize = tentative;
    }

    result = sqlite3_malloc(hdrSize + bodySize);
    if(!result){ sqlite3_free(winners); goto fail; }

    /* Write header size varint */
    pos = 0;
    { u64 hs = (u64)hdrSize;
      if(hs <= 0x7f){ result[pos++] = (u8)hs; }
      else{ result[pos++] = (u8)(0x80 | (hs>>7)); result[pos++] = (u8)(hs&0x7f); }
    }
    /* Write serial types */
    for(i=0; i<nfMax; i++){
      u64 st = winners[i].pField->st;
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
    /* Write body */
    for(i=0; i<nfMax; i++){
      if(winners[i].pField->len > 0){
        memcpy(result + pos, winners[i].pRec + winners[i].pField->off,
               winners[i].pField->len);
        pos += winners[i].pField->len;
      }
    }

    *pnMerged = pos;
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
      /* Our change only — already in our tree, no action needed */
      break;

    case THREE_WAY_RIGHT_ADD:
      /* Theirs added a row — insert it */
      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      break;

    case THREE_WAY_RIGHT_MODIFY:
      /* Theirs modified a row — update it */
      rc = prollyMutMapInsert(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey,
          pChange->pTheirVal, pChange->nTheirVal);
      break;

    case THREE_WAY_RIGHT_DELETE:
      /* Theirs deleted a row — delete it */
      rc = prollyMutMapDelete(ctx->pEdits,
          pChange->pKey, pChange->nKey, pChange->intKey);
      break;

    case THREE_WAY_CONVERGENT:
      /* Both made same change — no action, already in ours */
      break;

    case THREE_WAY_CONFLICT_MM: {
      /* Both sides modified — try cell-level merge first */
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
        /* Cell-level merge succeeded — apply as non-conflicting edit */
        rc = prollyMutMapInsert(ctx->pEdits,
            pChange->pKey, pChange->nKey, pChange->intKey,
            pMerged, nMerged);
        sqlite3_free(pMerged);
        break;
      }
      /* Fall through to record as conflict */
    }
    case THREE_WAY_CONFLICT_DM: {
      /* Real conflict — record it with all three values */
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

/*
** Perform row-level three-way merge on a single table.
** Returns: the new merged root hash + number of conflicts.
*/
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
  ProllyCache *pCache = (ProllyCache*)(((char*)cs) -
    ((char*)&((struct { ChunkStore s; ProllyCache c; }*)0)->s - (char*)0) +
    sizeof(ChunkStore));
  /* Actually, we need the BtShared to get the cache. Use a simpler approach: */
  /* The cache is the field right after ChunkStore in BtShared. */
  extern void *doltliteGetBtShared(sqlite3 *db);
  void *pBt = doltliteGetBtShared(db);
  ProllyCache *cache = (ProllyCache*)((u8*)pBt + sizeof(ChunkStore));

  RowMergeCtx ctx;
  ProllyMutator mut;
  int rc;

  memset(&ctx, 0, sizeof(ctx));
  ctx.isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  ctx.pEdits = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !ctx.pEdits ) return SQLITE_NOMEM;
  rc = prollyMutMapInit(ctx.pEdits, ctx.isIntKey);
  if( rc!=SQLITE_OK ){ sqlite3_free(ctx.pEdits); return rc; }

  /* Run three-way diff */
  rc = prollyThreeWayDiff(cs, cache, pAncRoot, pOursRoot, pTheirsRoot,
                          flags, rowMergeCallback, &ctx);
  if( rc!=SQLITE_OK ){
    freeRowMergeCtx(&ctx);
    return rc;
  }

  /* Apply non-conflicting edits to produce merged tree */
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
    /* No non-conflicting edits — merged root is same as ours */
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
  }

  *pnConflicts = ctx.nConflicts;
  *ppConflicts = ctx.aConflicts;
  ctx.aConflicts = 0; /* ownership transferred */
  ctx.nConflicts = 0;

  freeRowMergeCtx(&ctx);
  return rc;
}

static struct TableEntry *findTableByName(
  struct TableEntry *aEntries,
  int nEntries,
  const char *zName
){
  int i;
  if( !zName ) return 0;
  for(i=0; i<nEntries; i++){
    if( aEntries[i].zName && strcmp(aEntries[i].zName, zName)==0 ){
      return &aEntries[i];
    }
  }
  return 0;
}

/*
** Serialize a merged catalog into a chunk and store it.
**
** Format: iNextTable(4) + nTables(4) + meta(64) + entries(25 each)
**
** Meta is copied from the "ours" catalog (the current branch's metadata
** takes precedence in a merge).
*/
static int serializeMergedCatalog(
  sqlite3 *db,
  const ProllyHash *oursCatHash,     /* For copying meta header */
  struct TableEntry *aMerged,
  int nMerged,
  Pgno iNextTable,
  ProllyHash *pOutHash               /* OUT: hash of stored catalog chunk */
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int sz = 4 + 4 + 64;
  { int j; for(j=0;j<nMerged;j++){
    int nl = aMerged[j].zName ? (int)strlen(aMerged[j].zName) : 0;
    sz += 4+1+PROLLY_HASH_SIZE+PROLLY_HASH_SIZE+2+nl;
  }}
  u8 *buf;
  u8 *p;
  int rc;

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  /* Copy iNextTable and meta from ours catalog */
  {
    u8 *oursData = 0;
    int nOursData = 0;
    rc = chunkStoreGet(cs, oursCatHash, &oursData, &nOursData);
    if( rc==SQLITE_OK && nOursData>=72 ){
      memcpy(p, oursData, 72); /* iNextTable(4) + nTables(4) + meta(64) */
    }else{
      memset(p, 0, 72);
    }
    sqlite3_free(oursData);
  }

  /* Override iNextTable */
  p[0] = (u8)iNextTable;
  p[1] = (u8)(iNextTable>>8);
  p[2] = (u8)(iNextTable>>16);
  p[3] = (u8)(iNextTable>>24);

  /* Override nTables */
  p[4] = (u8)nMerged;
  p[5] = (u8)(nMerged>>8);
  p[6] = (u8)(nMerged>>16);
  p[7] = (u8)(nMerged>>24);

  p += 72;

  /* Write table entries: iTable(4) + flags(1) + root(20) + schemaHash(20) + name_len(2) + name(var) */
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

  rc = chunkStorePut(cs, buf, sz, pOutHash);
  sqlite3_free(buf);
  return rc;
}

/*
** Perform a three-way catalog merge.
**
** ancestor: the common ancestor catalog hash
** ours:     the current branch's catalog hash
** theirs:   the other branch's catalog hash
**
** On success, *pMergedHash contains the hash of the merged catalog chunk.
** Returns SQLITE_OK on success, SQLITE_ERROR on conflict.
*/
int doltliteMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  ProllyHash *pMergedHash,
  int *pnConflicts          /* OUT: total row conflicts across all tables */
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

  /* Collected conflicts for storage */
  struct MergeConflictTable {
    char *zName;
    int nConflicts;
    struct ConflictRow *aRows;
  } *aConflictTables = 0;
  int nConflictTables = 0;
  int i;

  /* Load all three catalogs */
  rc = doltliteLoadCatalog(db, ancestor, &aAnc, &nAnc, &iNextAnc);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, ours, &aOurs, &nOurs, &iNextOurs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, theirs, &aTheirs, &nTheirs, &iNextTheirs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  /* Allocate merged array — worst case is all tables from both sides */
  nMergedAlloc = nOurs + nTheirs;
  if( nMergedAlloc==0 ) nMergedAlloc = 1;
  aMerged = sqlite3_malloc(nMergedAlloc * (int)sizeof(struct TableEntry));
  if( !aMerged ){
    rc = SQLITE_NOMEM;
    goto merge_cleanup;
  }

  /* Use ours' iNextTable as the starting point, take max with theirs */
  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;

  /*
  ** NAME-BASED THREE-WAY MERGE
  **
  ** Tables are matched by NAME (zName), not by iTable number.
  ** This handles the case where two branches independently CREATE TABLE
  ** and get different iTable numbers for tables with different names.
  **
  ** Output uses ours' iTable numbers. Theirs' tables that don't exist
  ** in ours get new iTable numbers from iNextMerged.
  */

  /* Pass 1: Process all tables from "ours" */
  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;

    /* Table 1 (sqlite_master) has no zName but must be row-merged
    ** so that CREATE TABLE statements from theirs appear after merge. */
    if( aOurs[i].iTable==1 ){
      ancEntry = findTableEntry(aAnc, nAnc, 1);
      theirsEntry = findTableEntry(aTheirs, nTheirs, 1);
      goto do_merge_entry;
    }

    /* Skip tables without names (internal bookkeeping, not real tables) */
    if( !zName ){
      aMerged[nMerged++] = aOurs[i];
      continue;
    }

    /* Find by NAME in ancestor and theirs */
    ancEntry = findTableByName(aAnc, nAnc, zName);
    theirsEntry = findTableByName(aTheirs, nTheirs, zName);

do_merge_entry:

    if( !ancEntry ){
      /* Table added in ours (not in ancestor by name) */
      if( theirsEntry ){
        /* Both sides added a table with the same name */
        if( prollyHashCompare(&aOurs[i].root, &theirsEntry->root)!=0 ){
          /* Different content — conflict */
          rc = SQLITE_ERROR;
          goto merge_cleanup;
        }
        /* Identical content — include once (ours' iTable) */
      }
      aMerged[nMerged++] = aOurs[i];
    }else{
      /* Table existed in ancestor */
      int oursChanged = prollyHashCompare(&aOurs[i].root, &ancEntry->root)!=0;

      if( !theirsEntry ){
        /* Deleted in theirs (by name) */
        if( oursChanged ){
          rc = SQLITE_ERROR;  /* Modified in ours, deleted in theirs */
          goto merge_cleanup;
        }
        /* Unchanged in ours, deleted in theirs — delete (skip) */
      }else{
        int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
        if( oursChanged && theirsChanged ){
          /* Both modified same table — do row-level three-way merge */
          ProllyHash mergedTableRoot;
          int nConflicts = 0;
          struct ConflictRow *aConflictRows = 0;

          rc = mergeTableRows(db, &ancEntry->root, &aOurs[i].root,
                              &theirsEntry->root, aOurs[i].flags,
                              &mergedTableRoot, &nConflicts, &aConflictRows);
          if( rc!=SQLITE_OK ) goto merge_cleanup;

          {
            struct TableEntry merged = aOurs[i];
            memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
            aMerged[nMerged++] = merged;
          }

          if( nConflicts>0 ){
            totalConflicts += nConflicts;
            /* Collect conflicts for this table */
            {
              struct MergeConflictTable *aNew = sqlite3_realloc(aConflictTables,
                (nConflictTables+1)*(int)sizeof(struct MergeConflictTable));
              if( aNew ){
                aConflictTables = aNew;
                aNew[nConflictTables].zName = sqlite3_mprintf("%s", zName);
                aNew[nConflictTables].nConflicts = nConflicts;
                aNew[nConflictTables].aRows = aConflictRows;
                nConflictTables++;
                aConflictRows = 0; /* ownership transferred */
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
          /* Take theirs' root but with ours' iTable number */
          struct TableEntry merged = aOurs[i];
          memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
          aMerged[nMerged++] = merged;
        }else{
          aMerged[nMerged++] = aOurs[i];
        }
      }
    }
  }

  /* Pass 2: Add tables from "theirs" that don't exist in "ours" by name */
  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 || !zName ) continue;

    oursEntry = findTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;  /* Already handled in pass 1 */

    {
      struct TableEntry *ancEntry = findTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){
        /* Added in theirs only — keep their iTable if no conflict,
        ** otherwise assign a new one. Keeping the original iTable is
        ** important so sqlite_master rootpage values stay consistent. */
        struct TableEntry newEntry = aTheirs[i];
        {
          int j, conflict = 0;
          for(j=0; j<nMerged; j++){
            if( aMerged[j].iTable==newEntry.iTable ){ conflict = 1; break; }
          }
          if( conflict ) newEntry.iTable = iNextMerged++;
        }
        if( newEntry.iTable >= iNextMerged ) iNextMerged = newEntry.iTable + 1;
        /* Copy name string */
        newEntry.zName = sqlite3_mprintf("%s", zName);
        aMerged[nMerged++] = newEntry;
      }else{
        /* Was in ancestor, deleted in ours */
        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          rc = SQLITE_ERROR;  /* Modified in theirs, deleted in ours */
          goto merge_cleanup;
        }
        /* Unchanged in theirs, deleted in ours — delete (skip) */
      }
    }
  }

  /* Serialize the merged catalog */
  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              pMergedHash);
  if( pnConflicts ) *pnConflicts = totalConflicts;

  /* Store conflicts if any */
  if( totalConflicts>0 && nConflictTables>0 && rc==SQLITE_OK ){
    ProllyHash conflictsHash;
    /* ConflictTableInfo has the same layout as MergeConflictTable */
    int rc2 = doltliteSerializeConflicts(
        doltliteGetChunkStore(db),
        (ConflictTableInfo*)aConflictTables, nConflictTables,
        &conflictsHash);
    if( rc2==SQLITE_OK ){
      ChunkStore *cs2 = doltliteGetChunkStore(db);
      chunkStoreSetConflictsCatalog(cs2, &conflictsHash);
      chunkStoreSetMergeState(cs2, 1, 0, &conflictsHash);
    }
  }

  /* Free collected conflicts */
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

#endif /* DOLTLITE_PROLLY */
