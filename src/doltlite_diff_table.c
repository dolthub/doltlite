
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <assert.h>
#include <string.h>
#include <time.h>

static char *buildDiffSchema(DoltliteColInfo *ci){
  int i;
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  char *zColName;
  if( !pStr ) return 0;

  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  for(i=0; i<ci->nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, ", ");
    zColName = sqlite3_mprintf("to_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !zColName ){
      sqlite3_str_reset(pStr);
      return 0;
    }
    if( doltliteAppendQuotedIdent(pStr, zColName)!=SQLITE_OK ){
      sqlite3_free(zColName);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(zColName);
  }
  sqlite3_str_appendall(pStr, ", to_commit TEXT, to_commit_date TEXT");
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendall(pStr, ", ");
    zColName = sqlite3_mprintf("from_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !zColName ){
      sqlite3_str_reset(pStr);
      return 0;
    }
    if( doltliteAppendQuotedIdent(pStr, zColName)!=SQLITE_OK ){
      sqlite3_free(zColName);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(zColName);
  }
  sqlite3_str_appendall(pStr, ", from_commit TEXT, from_commit_date TEXT"
                              ", diff_type TEXT"
                              /* Hidden TVF arguments: used to expose the
                              ** dolt_diff(from_ref, to_ref, table) shape as
                              ** dolt_diff_<table>(from_ref, to_ref). Both
                              ** must be supplied together; if either is
                              ** absent the vtab falls back to its full-
                              ** history no-arg behavior. */
                              ", from_ref TEXT HIDDEN"
                              ", to_ref TEXT HIDDEN)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct AuditRow AuditRow;
struct AuditRow {
  u8 diffType;
  i64 intKey;
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  char zFromCommit[PROLLY_HASH_SIZE*2+1];
  char zToCommit[PROLLY_HASH_SIZE*2+1];
  i64 fromDate;
  i64 toDate;
};

typedef struct DiffTblVtab DiffTblVtab;
struct DiffTblVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct DiffPair DiffPair;
struct DiffPair {

  ProllyHash fromHash;
  ProllyHash fromTblRoot;
  ProllyHash fromCatHash;
  ProllyHash fromSchemaHash;
  u8         fromFlags;
  i64        fromDate;

  char       zToCommit[PROLLY_HASH_SIZE*2+1];
  ProllyHash toTblRoot;
  ProllyHash toCatHash;
  ProllyHash toSchemaHash;
  u8         toFlags;
  i64        toDate;
};

typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;


  DiffPair *aPairs;
  int nPairs;
  int iPair;
  int pairsDone;


  ProllyDiffIter diffIter;
  int diffIterOpen;


  /* Per-pair schema snapshots at the from- and to-commit catalog
  ** hashes. Populated only when the two commits have a different
  ** schema hash (needFilter==1) so that changeIsSchemaOnly can
  ** compare shared columns and filter out modifications that are
  ** purely schema-level with no data change. Each side carries
  ** aColToRec[] so record fields can be read by declared-column
  ** index even when the WITHOUT-ROWID PK-first permutation moves
  ** columns around relative to the declaration. */
  DoltliteColInfo fromColInfo;
  DoltliteColInfo toColInfo;
  int    needFilter;


  AuditRow row;
  int hasRow;
  i64 iRowid;
};

#define DT_IDX_TO_COMMIT_EQ  0x01
#define DT_IDX_SLICE         0x02

static int diffRecordField(
  const u8 *pData,
  int nData,
  int iField,
  int *pType,
  int *pOff
){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  const u8 *pHdrEnd;
  u64 hdrSize;
  int hdrBytes;
  int off;
  int i;

  if( !pData || nData < 1 || iField < 0 ) return SQLITE_CORRUPT;
  hdrBytes = dlReadVarint(p, pEnd, &hdrSize);
  if( hdrBytes <= 0 || hdrSize < (u64)hdrBytes || hdrSize > (u64)nData ){
    return SQLITE_CORRUPT;
  }
  p += hdrBytes;
  pHdrEnd = pData + (int)hdrSize;
  off = (int)hdrSize;
  for(i=0; p < pHdrEnd; i++){
    u64 st;
    int stBytes = dlReadVarint(p, pHdrEnd, &st);
    int len;
    if( stBytes <= 0 ) return SQLITE_CORRUPT;
    p += stBytes;
    len = dlSerialTypeLen(st);
    if( off < 0 || off + len > nData ) return SQLITE_CORRUPT;
    if( i==iField ){
      *pType = (int)st;
      *pOff = off;
      return SQLITE_OK;
    }
    off += len;
  }
  return SQLITE_NOTFOUND;
}

static void clearAuditRow(AuditRow *r){
  memset(r, 0, sizeof(*r));
}

static void closeDiffIter(DiffTblCursor *pCur){
  if( pCur->diffIterOpen ){
    prollyDiffIterClose(&pCur->diffIter);
    pCur->diffIterOpen = 0;
  }
}

typedef struct CmTblInfo CmTblInfo;
struct CmTblInfo {
  ProllyHash key;
  ProllyHash tblRoot;
  ProllyHash catHash;
  ProllyHash schemaHash;
  u8         flags;
  i64        date;
  char       zHexName[PROLLY_HASH_SIZE*2+1];
};

static int mapFind(const CmTblInfo *aMap, int nMap, const ProllyHash *pKey){
  int i;
  for(i=0; i<nMap; i++){
    if( prollyHashCompare(&aMap[i].key, pKey)==0 ) return i;
  }
  return -1;
}

static int mapPut(CmTblInfo **paMap, int *pnMap, const ProllyHash *pKey,
                  const ProllyHash *pTblRoot,
                  const ProllyHash *pCatHash,
                  const ProllyHash *pSchemaHash,
                  u8 flags,
                  const char *zHexName, i64 date){
  int idx = mapFind(*paMap, *pnMap, pKey);
  CmTblInfo *e;
  if( idx<0 ){
    CmTblInfo *aNew = sqlite3_realloc(*paMap,
                          (*pnMap+1)*(int)sizeof(CmTblInfo));
    if( !aNew ) return SQLITE_NOMEM;
    *paMap = aNew;
    e = &aNew[*pnMap];
    memset(e, 0, sizeof(*e));
    e->key = *pKey;
    (*pnMap)++;
  }else{
    e = &(*paMap)[idx];
  }
  e->tblRoot = *pTblRoot;
  e->catHash = *pCatHash;
  e->schemaHash = *pSchemaHash;
  e->flags = flags;
  e->date = date;
  memcpy(e->zHexName, zHexName, PROLLY_HASH_SIZE*2+1);
  return SQLITE_OK;
}

static int pairsAppend(DiffTblCursor *pCur,
                       const ProllyHash *pFromHash,
                       const ProllyHash *pFromTblRoot,
                       const ProllyHash *pFromCatHash,
                       const ProllyHash *pFromSchemaHash,
                       u8 fromFlags, i64 fromDate,
                       const char *zToHex,
                       const ProllyHash *pToTblRoot,
                       const ProllyHash *pToCatHash,
                       const ProllyHash *pToSchemaHash,
                       u8 toFlags, i64 toDate){
  DiffPair *aNew, *r;
  aNew = sqlite3_realloc(pCur->aPairs,
                         (pCur->nPairs+1)*(int)sizeof(DiffPair));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aPairs = aNew;
  r = &aNew[pCur->nPairs++];
  memset(r, 0, sizeof(*r));
  r->fromHash       = *pFromHash;
  r->fromTblRoot    = *pFromTblRoot;
  r->fromCatHash    = *pFromCatHash;
  r->fromSchemaHash = *pFromSchemaHash;
  r->fromFlags      = fromFlags;
  r->fromDate       = fromDate;
  memcpy(r->zToCommit, zToHex, PROLLY_HASH_SIZE*2+1);
  r->toTblRoot      = *pToTblRoot;
  r->toCatHash      = *pToCatHash;
  r->toSchemaHash   = *pToSchemaHash;
  r->toFlags        = toFlags;
  r->toDate         = toDate;
  return SQLITE_OK;
}

static int stackPushUnique(ProllyHash **paAdded, int *pnAdded,
                           ProllyHash **paStack, int *pnStack,
                           const ProllyHash *pHash){
  ProllyHash *aAddedNew;
  ProllyHash *aStackNew;
  int i;
  for(i=0; i<*pnAdded; i++){
    if( prollyHashCompare(&(*paAdded)[i], pHash)==0 ) return SQLITE_OK;
  }
  aAddedNew = sqlite3_malloc((*pnAdded+1)*(int)sizeof(ProllyHash));
  if( !aAddedNew ) return SQLITE_NOMEM;
  aStackNew = sqlite3_malloc((*pnStack+1)*(int)sizeof(ProllyHash));
  if( !aStackNew ){
    sqlite3_free(aAddedNew);
    return SQLITE_NOMEM;
  }
  if( *pnAdded>0 ) memcpy(aAddedNew, *paAdded, (*pnAdded)*(int)sizeof(ProllyHash));
  if( *pnStack>0 ) memcpy(aStackNew, *paStack, (*pnStack)*(int)sizeof(ProllyHash));
  sqlite3_free(*paAdded);
  sqlite3_free(*paStack);
  *paAdded = aAddedNew;
  *paStack = aStackNew;
  (*paAdded)[*pnAdded] = *pHash;
  (*pnAdded)++;
  (*paStack)[*pnStack] = *pHash;
  (*pnStack)++;
  return SQLITE_OK;
}

static int loadTblRootAtCommit(sqlite3 *db, const ProllyHash *pCatHash,
                               const char *zTableName,
                               ProllyHash *pTblRoot, u8 *pFlags,
                               ProllyHash *pSchemaHash){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int rc;
  struct TableEntry *pEntry;
  memset(pTblRoot, 0, sizeof(*pTblRoot));
  *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(*pSchemaHash));
  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = doltliteFindTableByName(aTables, nTables, zTableName);
  if( pEntry ){
    memcpy(pTblRoot, &pEntry->root, sizeof(ProllyHash));
    *pFlags = pEntry->flags;
    if( pSchemaHash ){
      memcpy(pSchemaHash, &pEntry->schemaHash, sizeof(ProllyHash));
    }
  }
  sqlite3_free(aTables);
  return SQLITE_OK;
}

static int seedWorkingChildInfo(
  sqlite3 *db,
  const ProllyHash *pHeadHash,
  const char *zTableName,
  CmTblInfo **paMap,
  int *pnMap
){
  ProllyHash workingCat;
  ProllyHash workingTblRoot;
  ProllyHash workingSchemaHash;
  u8 workingFlags;
  char zWorking[PROLLY_HASH_SIZE*2+1];
  int rc;

  memset(&workingCat, 0, sizeof(workingCat));
  memset(&workingTblRoot, 0, sizeof(workingTblRoot));
  memset(&workingSchemaHash, 0, sizeof(workingSchemaHash));
  memset(zWorking, 0, sizeof(zWorking));
  memcpy(zWorking, "WORKING", 7);

  rc = doltliteFlushCatalogToHash(db, &workingCat);
  if( rc!=SQLITE_OK ) return rc;
  rc = loadTblRootAtCommit(db, &workingCat, zTableName, &workingTblRoot,
                           &workingFlags, &workingSchemaHash);
  if( rc!=SQLITE_OK ) return rc;
  return mapPut(paMap, pnMap, pHeadHash, &workingTblRoot, &workingCat,
                &workingSchemaHash, workingFlags, zWorking, 0);
}

static int appendCurrentDiffPair(
  DiffTblCursor *pCur,
  const ProllyHash *pCurr,
  const DoltliteCommit *pCommit,
  const ProllyHash *pCurTblRoot,
  const ProllyHash *pCurSchemaHash,
  u8 curFlags,
  CmTblInfo *aMap,
  int nMap
){
  CmTblInfo *pInfo;
  int idx;
  int rootsDiffer;
  int schemasDiffer;
  u8 fromFlags;

  idx = mapFind(aMap, nMap, pCurr);
  if( idx<0 ) return SQLITE_OK;
  pInfo = &aMap[idx];
  rootsDiffer = prollyHashCompare(&pInfo->tblRoot, pCurTblRoot)!=0;
  schemasDiffer = prollyHashCompare(&pInfo->schemaHash, pCurSchemaHash)!=0;
  if( !rootsDiffer && !schemasDiffer ) return SQLITE_OK;

  fromFlags = curFlags;
  if( fromFlags==0 ) fromFlags = pInfo->flags;
  return pairsAppend(pCur, pCurr, pCurTblRoot, &pCommit->catalogHash,
                     pCurSchemaHash, fromFlags, pCommit->timestamp,
                     pInfo->zHexName, &pInfo->tblRoot, &pInfo->catHash,
                     &pInfo->schemaHash, pInfo->flags, pInfo->date);
}

static int registerCommitParents(
  CmTblInfo **paMap,
  int *pnMap,
  ProllyHash **paStack,
  int *pnStack,
  ProllyHash **paAdded,
  int *pnAdded,
  const DoltliteCommit *pCommit,
  const ProllyHash *pCurTblRoot,
  const ProllyHash *pCurSchemaHash,
  u8 curFlags,
  const char *zCurHex
){
  int i;
  int nParents;
  const ProllyHash *pParent;
  int rc;

  nParents = pCommit->nParents>0
               ? pCommit->nParents
               : (prollyHashIsEmpty(&pCommit->parentHash) ? 0 : 1);
  for(i=0; i<nParents; i++){
    pParent = pCommit->nParents>0 ? &pCommit->aParents[i] : &pCommit->parentHash;
    rc = mapPut(paMap, pnMap, pParent, pCurTblRoot, &pCommit->catalogHash,
                pCurSchemaHash, curFlags, zCurHex, pCommit->timestamp);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nParents; i++){
    pParent = pCommit->nParents>0 ? &pCommit->aParents[i] : &pCommit->parentHash;
    rc = stackPushUnique(paAdded, pnAdded, paStack, pnStack, pParent);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/* Walk history toward the root, emitting one DiffPair per commit
** whose table root (or schema) differs from the commit that follows
** it. aMap is keyed by commit hash and stores "what child info do we
** have for this commit?" — when we pop a commit off the stack, we
** compare its table root with the child info already registered
** under its own hash and, if different, emit a pair. Before walking
** further we register the parent(s) in aMap with this commit's data,
** so when a parent gets visited later it already has a child. */
static int buildDiffPairs(DiffTblCursor *pCur, sqlite3 *db,
                          const char *zTableName){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  CmTblInfo *aMap = 0;
  int nMap = 0;
  ProllyHash *aStack = 0;
  int nStack = 0;
  ProllyHash *aAdded = 0;
  int nAdded = 0;
  int currInited = 0;
  ProllyHash curr;
  int rc = SQLITE_OK;
  int i;

  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;
  rc = seedWorkingChildInfo(db, &headHash, zTableName, &aMap, &nMap);
  if( rc!=SQLITE_OK ) goto walk_done;

  rc = stackPushUnique(&aAdded, &nAdded, &aStack, &nStack, &headHash);
  if( rc!=SQLITE_OK ) goto walk_done;
  curr = headHash;
  currInited = 1;
  nStack--;

  while( currInited ){
    DoltliteCommit commit;
    ProllyHash curTblRoot;
    ProllyHash curSchemaHash;
    u8 curFlags = 0;
    char curHex[PROLLY_HASH_SIZE*2+1];

    memset(&commit, 0, sizeof(commit));
    memset(&curTblRoot, 0, sizeof(curTblRoot));
    memset(&curSchemaHash, 0, sizeof(curSchemaHash));

    rc = doltliteLoadCommit(db, &curr, &commit);
    if( rc!=SQLITE_OK ) break;

    rc = loadTblRootAtCommit(db, &commit.catalogHash, zTableName,
                             &curTblRoot, &curFlags, &curSchemaHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      break;
    }

    doltliteHashToHex(&curr, curHex);
    rc = appendCurrentDiffPair(pCur, &curr, &commit, &curTblRoot,
                               &curSchemaHash, curFlags, aMap, nMap);
    if( rc==SQLITE_OK ){
      rc = registerCommitParents(&aMap, &nMap, &aStack, &nStack,
                                 &aAdded, &nAdded, &commit, &curTblRoot,
                                 &curSchemaHash, curFlags, curHex);
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;


    if( nStack==0 ){
      currInited = 0;
    }else{
      curr = aStack[nStack-1];
      nStack--;
    }
  }

walk_done:
  sqlite3_free(aMap);
  sqlite3_free(aStack);
  sqlite3_free(aAdded);
  return rc;
}

static int buildWorkingDiffPair(
  DiffTblCursor *pCur,
  sqlite3 *db,
  const char *zTableName
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  ProllyHash workingCat, workingTblRoot, workingSchemaHash;
  ProllyHash headTblRoot, headSchemaHash;
  DoltliteCommit headCommit;
  u8 workingFlags = 0;
  u8 headFlags = 0;
  u8 fromFlags;
  int rc;
  char zWorking[PROLLY_HASH_SIZE*2+1];

  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;

  memset(&workingCat, 0, sizeof(workingCat));
  memset(&workingTblRoot, 0, sizeof(workingTblRoot));
  memset(&workingSchemaHash, 0, sizeof(workingSchemaHash));
  memset(&headTblRoot, 0, sizeof(headTblRoot));
  memset(&headSchemaHash, 0, sizeof(headSchemaHash));
  memset(&headCommit, 0, sizeof(headCommit));
  memset(zWorking, 0, sizeof(zWorking));
  memcpy(zWorking, "WORKING", 7);

  rc = doltliteGetWorkingTableState(db, zTableName,
                                    &workingTblRoot, &workingFlags,
                                    &workingSchemaHash);
  if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteLoadCommit(db, &headHash, &headCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = loadTblRootAtCommit(db, &headCommit.catalogHash, zTableName,
                           &headTblRoot, &headFlags, &headSchemaHash);
  if( rc==SQLITE_OK ){
    int rootsDiffer = prollyHashCompare(&headTblRoot, &workingTblRoot)!=0;
    int schemasDiffer = prollyHashCompare(&headSchemaHash, &workingSchemaHash)!=0;
    if( rootsDiffer || schemasDiffer ){
      if( schemasDiffer ){
        rc = doltliteFlushCatalogToHash(db, &workingCat);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&headCommit);
          return rc;
        }
      }else{
        memset(&workingCat, 0, sizeof(workingCat));
      }
      fromFlags = headFlags ? headFlags : workingFlags;
      rc = pairsAppend(pCur, &headHash, &headTblRoot, &headCommit.catalogHash,
                       &headSchemaHash, fromFlags, headCommit.timestamp,
                       zWorking, &workingTblRoot, &workingCat, &workingSchemaHash,
                       workingFlags, 0);
    }
  }
  doltliteCommitClear(&headCommit);
  return rc;
}

/* TVF slice builder: produce exactly one DiffPair between the two
** supplied refs. Matches Dolt's dolt_diff(from_ref, to_ref, table)
** shape — one pair, net diff between endpoints, no history walk.
** to_ref == 'WORKING' diffs against the current working catalog
** (staged + pending); from_ref == 'WORKING' is not supported by
** Dolt either and returns an empty slice. */
static int buildSliceDiffPair(
  DiffTblCursor *pCur,
  sqlite3 *db,
  const char *zTableName,
  const char *zFromRef,
  const char *zToRef
){
  ProllyHash fromHash, toHash;
  ProllyHash fromTblRoot, toTblRoot;
  ProllyHash fromCatHash, toCatHash;
  ProllyHash fromSchemaHash, toSchemaHash;
  DoltliteCommit fromCommit;
  u8 fromFlags = 0;
  u8 toFlags = 0;
  i64 fromDate = 0;
  i64 toDate = 0;
  int toIsWorking = 0;
  char zToLabel[PROLLY_HASH_SIZE*2+1];
  int rc;

  memset(&fromHash, 0, sizeof(fromHash));
  memset(&toHash, 0, sizeof(toHash));
  memset(&fromTblRoot, 0, sizeof(fromTblRoot));
  memset(&toTblRoot, 0, sizeof(toTblRoot));
  memset(&fromCatHash, 0, sizeof(fromCatHash));
  memset(&toCatHash, 0, sizeof(toCatHash));
  memset(&fromSchemaHash, 0, sizeof(fromSchemaHash));
  memset(&toSchemaHash, 0, sizeof(toSchemaHash));
  memset(&fromCommit, 0, sizeof(fromCommit));
  memset(zToLabel, 0, sizeof(zToLabel));

  if( !zFromRef || !zToRef ) return SQLITE_OK;

  rc = doltliteResolveRef(db, zFromRef, &fromHash);
  if( rc!=SQLITE_OK ) return SQLITE_OK;
  rc = doltliteLoadCommit(db, &fromHash, &fromCommit);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(&fromCatHash, &fromCommit.catalogHash, sizeof(ProllyHash));
  fromDate = fromCommit.timestamp;
  doltliteCommitClear(&fromCommit);

  rc = loadTblRootAtCommit(db, &fromCatHash, zTableName,
                           &fromTblRoot, &fromFlags, &fromSchemaHash);
  if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;

  if( sqlite3_stricmp(zToRef, "WORKING")==0 ){
    toIsWorking = 1;
    rc = doltliteGetWorkingTableState(db, zTableName,
                                      &toTblRoot, &toFlags,
                                      &toSchemaHash);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK ) return rc;
    /* Flush working catalog only if we need its hash; otherwise
    ** leave toCatHash empty so column resolution falls back to
    ** the table root itself. */
    rc = doltliteFlushCatalogToHash(db, &toCatHash);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(zToLabel, "WORKING", 7);
    toDate = 0;
  }else{
    DoltliteCommit toCommit;
    memset(&toCommit, 0, sizeof(toCommit));
    rc = doltliteResolveRef(db, zToRef, &toHash);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    rc = doltliteLoadCommit(db, &toHash, &toCommit);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(&toCatHash, &toCommit.catalogHash, sizeof(ProllyHash));
    toDate = toCommit.timestamp;
    doltliteCommitClear(&toCommit);
    rc = loadTblRootAtCommit(db, &toCatHash, zTableName,
                             &toTblRoot, &toFlags, &toSchemaHash);
    if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;
    doltliteHashToHex(&toHash, zToLabel);
  }

  /* No-op slice: same endpoint or identical table roots. */
  if( !toIsWorking
   && prollyHashCompare(&fromHash, &toHash)==0 ){
    return SQLITE_OK;
  }
  if( prollyHashCompare(&fromTblRoot, &toTblRoot)==0
   && prollyHashCompare(&fromSchemaHash, &toSchemaHash)==0 ){
    return SQLITE_OK;
  }

  /* Use whichever side has a non-zero flags bitfield as the
  ** diff-iteration flags (both sides should agree for a single
  ** table, but be tolerant when one side is missing). */
  if( !fromFlags ) fromFlags = toFlags;
  if( !toFlags ) toFlags = fromFlags;

  return pairsAppend(pCur, &fromHash, &fromTblRoot, &fromCatHash,
                     &fromSchemaHash, fromFlags, fromDate,
                     zToLabel, &toTblRoot, &toCatHash, &toSchemaHash,
                     toFlags, toDate);
}

static void freePairCols(DiffTblCursor *pCur){
  doltliteFreeColInfo(&pCur->fromColInfo);
  doltliteFreeColInfo(&pCur->toColInfo);
  pCur->needFilter = 0;
}

/* Load a DoltliteColInfo snapshot (including aColToRec[]) for
** zTableName at the schema recorded in pCatHash. Opens a fresh
** in-memory SQLite, replays the recorded CREATE TABLE, then
** runs doltliteGetColumnNames() against that temp db so the
** WITHOUT-ROWID PK-first permutation is computed from the same
** PRAGMA table_info the rest of the engine uses. */
static int loadColInfoAtCatalog(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  DoltliteColInfo *pOut
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry *aSchemas = 0;
  int nSchemas = 0;
  SchemaEntry *pEntry;
  sqlite3 *tmp = 0;
  int rc;

  memset(pOut, 0, sizeof(*pOut));
  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_NOTFOUND;

  rc = loadSchemaFromCatalog(db, cs, pCache, pCatHash, &aSchemas, &nSchemas);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = findSchemaEntry(aSchemas, nSchemas, zTableName);
  if( !pEntry || !pEntry->zSql ){
    freeSchemaEntries(aSchemas, nSchemas);
    return SQLITE_NOTFOUND;
  }

  rc = sqlite3_open(":memory:", &tmp);
  if( rc!=SQLITE_OK ) goto cleanup;
  rc = sqlite3_exec(tmp, pEntry->zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto cleanup;

  rc = doltliteGetColumnNames(tmp, zTableName, pOut);
  if( rc!=SQLITE_OK ) goto cleanup;
  if( pOut->nCol<=0 ){ rc = SQLITE_NOTFOUND; goto cleanup; }

cleanup:
  if( tmp ) sqlite3_close(tmp);
  freeSchemaEntries(aSchemas, nSchemas);
  if( rc!=SQLITE_OK ){
    doltliteFreeColInfo(pOut);
  }
  return rc;
}

static i64 sdReadInt(const u8 *p, int nBytes){
  i64 v;
  int i;
  if( nBytes<=0 ) return 0;
  v = (p[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++) v = (v<<8) | p[i];
  return v;
}

static int fieldValuesEqual(
  int aType, const u8 *pA, int nA, int aOff,
  int bType, const u8 *pB, int nB, int bOff
){
  i64 ai, bi;
  int aLen, bLen;


  if( aType==0 && bType==0 ) return 1;
  if( aType==0 || bType==0 ) return 0;


  {
    int aIsInt = (aType>=1 && aType<=6) || aType==8 || aType==9;
    int bIsInt = (bType>=1 && bType<=6) || bType==8 || bType==9;
    if( aIsInt && bIsInt ){
      if( aType==8 )      ai = 0;
      else if( aType==9 ) ai = 1;
      else{
        aLen = dlSerialTypeLen(aType);
        if( aOff<0 || aOff+aLen>nA ) return 0;
        ai = sdReadInt(pA+aOff, aLen);
      }
      if( bType==8 )      bi = 0;
      else if( bType==9 ) bi = 1;
      else{
        bLen = dlSerialTypeLen(bType);
        if( bOff<0 || bOff+bLen>nB ) return 0;
        bi = sdReadInt(pB+bOff, bLen);
      }
      return ai==bi;
    }
  }


  if( aType != bType ) return 0;
  aLen = dlSerialTypeLen(aType);
  if( aLen<0 ) return 0;
  if( aOff<0 || aOff+aLen>nA ) return 0;
  if( bOff<0 || bOff+aLen>nB ) return 0;
  return memcmp(pA+aOff, pB+bOff, aLen)==0;
}

/* Return true if the pOld→pNew MODIFY is purely a schema-level
** reshape with no data change. Compares shared columns by name
** and reads each field via the side's aColToRec[] permutation so
** WITHOUT-ROWID tables (PK-first record layout) map declared
** indices back to the correct record field. Columns that exist
** on only one side must be NULL on that side to still qualify
** as schema-only. */
static int changeIsSchemaOnly(
  const u8 *pFromRec, int nFromRec,
  const u8 *pToRec,   int nToRec,
  const DoltliteColInfo *pFromCi,
  const DoltliteColInfo *pToCi
){
  DoltliteRecordInfo fromRi, toRi;
  int i;

  if( !pFromRec || nFromRec<=0 || !pToRec || nToRec<=0 ) return 0;
  if( !pFromCi || !pToCi ) return 0;
  doltliteParseRecord(pFromRec, nFromRec, &fromRi);
  doltliteParseRecord(pToRec,   nToRec,   &toRi);


  for(i=0; i<pToCi->nCol; i++){
    int fromIdx;
    int toRec = pToCi->aColToRec ? pToCi->aColToRec[i] : i;
    int fromRec;
    for(fromIdx=0; fromIdx<pFromCi->nCol; fromIdx++){
      if( strcmp(pFromCi->azName[fromIdx], pToCi->azName[i])==0 ) break;
    }
    if( fromIdx>=pFromCi->nCol ){
      if( toRec<toRi.nField && toRi.aType[toRec]!=0 ) return 0;
      continue;
    }
    fromRec = pFromCi->aColToRec ? pFromCi->aColToRec[fromIdx] : fromIdx;
    if( toRec>=toRi.nField ){
      if( fromRec<fromRi.nField && fromRi.aType[fromRec]!=0 ) return 0;
      continue;
    }
    if( fromRec>=fromRi.nField ){
      if( toRi.aType[toRec]!=0 ) return 0;
      continue;
    }
    if( !fieldValuesEqual(
            fromRi.aType[fromRec], pFromRec, nFromRec, fromRi.aOffset[fromRec],
            toRi.aType[toRec],     pToRec,   nToRec,   toRi.aOffset[toRec]) ){
      return 0;
    }
  }

  for(i=0; i<pFromCi->nCol; i++){
    int toIdx;
    int fromRec;
    for(toIdx=0; toIdx<pToCi->nCol; toIdx++){
      if( strcmp(pToCi->azName[toIdx], pFromCi->azName[i])==0 ) break;
    }
    if( toIdx<pToCi->nCol ) continue;
    fromRec = pFromCi->aColToRec ? pFromCi->aColToRec[i] : i;
    if( fromRec>=fromRi.nField ) continue;
    if( fromRi.aType[fromRec]!=0 ) return 0;
  }
  return 1;
}

static int openNextPairIter(DiffTblCursor *pCur, sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DiffTblVtab *pVtab = (DiffTblVtab*)pCur->base.pVtab;
  int rc;


  freePairCols(pCur);

  if( !cs ) return SQLITE_OK;

  if( pCur->iPair >= pCur->nPairs ){
    pCur->pairsDone = 1;
    return SQLITE_OK;
  }

  {
    DiffPair *p;
    u8 flags;
    int rc2;
    p = &pCur->aPairs[pCur->iPair++];
    flags = p->fromFlags ? p->fromFlags : p->toFlags;
    doltliteHashToHex(&p->fromHash, pCur->row.zFromCommit);
    pCur->row.fromDate = p->fromDate;
    memcpy(pCur->row.zToCommit, p->zToCommit, PROLLY_HASH_SIZE*2+1);
    pCur->row.toDate = p->toDate;
    rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                            &p->fromTblRoot, &p->toTblRoot, flags);
    if( rc!=SQLITE_OK ) return rc;
    pCur->diffIterOpen = 1;

    if( prollyHashCompare(&p->fromSchemaHash, &p->toSchemaHash)!=0 ){
      int rc2;
      rc2 = loadColInfoAtCatalog(db, &p->fromCatHash, pVtab->zTableName,
                                 &pCur->fromColInfo);
      if( rc2==SQLITE_OK ){
        rc2 = loadColInfoAtCatalog(db, &p->toCatHash, pVtab->zTableName,
                                   &pCur->toColInfo);
      }
      if( rc2==SQLITE_OK ){
        pCur->needFilter = 1;
      }else{
        freePairCols(pCur);
      }
    }
    return SQLITE_OK;
  }
}

static int advanceToNextRow(DiffTblCursor *pCur, sqlite3 *db,
                            const char *zTableName){
  int rc;

  pCur->hasRow = 0;

  for(;;){
    if( pCur->diffIterOpen ){
      ProllyDiffChange *pChange = 0;
      rc = prollyDiffIterStep(&pCur->diffIter, &pChange);
      if( rc==SQLITE_ROW && pChange ){

        if( pCur->needFilter
         && pChange->type==PROLLY_DIFF_MODIFY
         && changeIsSchemaOnly(pChange->pOldVal, pChange->nOldVal,
                               pChange->pNewVal, pChange->nNewVal,
                               &pCur->fromColInfo, &pCur->toColInfo) ){
          continue;
        }


        pCur->row.pOldVal = 0;
        pCur->row.nOldVal = 0;
        pCur->row.pNewVal = 0;
        pCur->row.nNewVal = 0;

        pCur->row.diffType = pChange->type;
        pCur->row.intKey = pChange->intKey;

        pCur->row.pOldVal = pChange->pOldVal;
        pCur->row.nOldVal = pChange->pOldVal ? pChange->nOldVal : 0;
        pCur->row.pNewVal = pChange->pNewVal;
        pCur->row.nNewVal = pChange->pNewVal ? pChange->nNewVal : 0;

        pCur->hasRow = 1;
        pCur->iRowid++;
        return SQLITE_OK;
      }
      if( rc!=SQLITE_DONE && rc!=SQLITE_ROW ){

        return rc;
      }

      closeDiffIter(pCur);
    }


    if( pCur->pairsDone ){
      return SQLITE_OK;
    }

    rc = openNextPairIter(pCur, db);
    if( rc!=SQLITE_OK ) return rc;
    (void)zTableName;
  }
}

static int dtConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DiffTblVtab *pVtab;
  int rc;
  const char *zModName;
  char *zSchema;
  (void)pAux; (void)pzErr;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;

  zModName = argv[0];
  if( zModName && strncmp(zModName, "dolt_diff_", 10)==0 ){
    pVtab->zTableName = sqlite3_mprintf("%s", zModName + 10);
  }else if( argc > 3 ){
    pVtab->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    pVtab->zTableName = sqlite3_mprintf("");
  }

  rc = doltliteLoadUserTableColumns(db, pVtab->zTableName, &pVtab->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }
  zSchema = buildDiffSchema(&pVtab->cols);

  if( !zSchema ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return SQLITE_NOMEM;
  }

  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dtDisconnect(sqlite3_vtab *pBase){
  DiffTblVtab *pVtab = (DiffTblVtab*)pBase;
  sqlite3_free(pVtab->zTableName);
  doltliteFreeColInfo(&pVtab->cols);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int dtBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  DiffTblVtab *p = (DiffTblVtab*)pVtab;
  int i;
  int iToCommitEq = -1;
  int iFromRefEq = -1;
  int iToRefEq = -1;
  int nUser = p->cols.nCol;
  /* Schema layout after buildDiffSchema():
  **   0..nUser-1       : user cols (to_<col>)
  **   nUser            : to_commit
  **   nUser+1          : to_commit_date
  **   nUser+2..2n+1    : from_<col>
  **   2n+2             : from_commit
  **   2n+3             : from_commit_date
  **   2n+4             : diff_type
  **   2n+5             : from_ref  (HIDDEN — TVF arg 1)
  **   2n+6             : to_ref    (HIDDEN — TVF arg 2)
  */
  int toCommitCol = nUser;
  int fromRefCol  = 2*nUser + 5;
  int toRefCol    = 2*nUser + 6;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn==toCommitCol ){
      iToCommitEq = i;
    }else if( pInfo->aConstraint[i].iColumn==fromRefCol ){
      iFromRefEq = i;
    }else if( pInfo->aConstraint[i].iColumn==toRefCol ){
      iToRefEq = i;
    }
  }

  if( iFromRefEq>=0 && iToRefEq>=0 ){
    /* TVF slice form: dolt_diff_<table>(from_ref, to_ref). Both
    ** hidden args are bound; build exactly one diff pair. */
    pInfo->idxNum = DT_IDX_SLICE;
    pInfo->aConstraintUsage[iFromRefEq].argvIndex = 1;
    pInfo->aConstraintUsage[iFromRefEq].omit = 1;
    pInfo->aConstraintUsage[iToRefEq].argvIndex = 2;
    pInfo->aConstraintUsage[iToRefEq].omit = 1;
    pInfo->estimatedCost = 10.0;
  }else if( iToCommitEq>=0 ){
    pInfo->idxNum = DT_IDX_TO_COMMIT_EQ;
    pInfo->aConstraintUsage[iToCommitEq].argvIndex = 1;
    pInfo->estimatedCost = 100.0;
  }else{
    pInfo->idxNum = 0;
    pInfo->estimatedCost = 10000.0;
  }
  return SQLITE_OK;
}

static int dtOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  DiffTblCursor *c; (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dtClose(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  closeDiffIter(c);
  clearAuditRow(&c->row);
  freePairCols(c);
  sqlite3_free(c->aPairs);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dtFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  sqlite3 *db = pVtab->db;
  int rc;
  (void)idxStr;


  closeDiffIter(c);
  clearAuditRow(&c->row);
  freePairCols(c);
  sqlite3_free(c->aPairs);
  c->aPairs = 0;
  c->nPairs = 0;
  c->iPair = 0;
  c->pairsDone = 0;
  c->hasRow = 0;
  c->iRowid = 0;

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    void *pBt = doltliteGetBtShared(db);
    if( !cs || !pBt ){
      c->pairsDone = 1;
      return SQLITE_OK;
    }
  }

  if( (idxNum & DT_IDX_SLICE)!=0 && argc>=2 ){
    const char *zFromRef = (const char*)sqlite3_value_text(argv[0]);
    const char *zToRef = (const char*)sqlite3_value_text(argv[1]);
    rc = buildSliceDiffPair(c, db, pVtab->zTableName, zFromRef, zToRef);
  }else if( (idxNum & DT_IDX_TO_COMMIT_EQ)!=0 && argc>=1 ){
    const char *zToCommit = (const char*)sqlite3_value_text(argv[0]);
    if( zToCommit && sqlite3_stricmp(zToCommit, "WORKING")==0 ){
      rc = buildWorkingDiffPair(c, db, pVtab->zTableName);
    }else{
      rc = buildDiffPairs(c, db, pVtab->zTableName);
    }
  }else{

    rc = buildDiffPairs(c, db, pVtab->zTableName);
  }
  if( rc!=SQLITE_OK ) return rc;
  if( c->nPairs==0 ){
    c->pairsDone = 1;
    return SQLITE_OK;
  }

  return advanceToNextRow(c, db, pVtab->zTableName);
}

static int dtNext(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  return advanceToNextRow(c, pVtab->db, pVtab->zTableName);
}

static int dtEof(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  return !c->hasRow;
}

static int dtColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  AuditRow *r = &c->row;
  int nCols = pVtab->cols.nCol;


  if( nCols > 0 && col < nCols ){

    int colIdx = col;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pNewVal && r->nNewVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pNewVal && r->nNewVal > 0 ){
        int st, off;
        int recField = pVtab->cols.aColToRec
                          ? pVtab->cols.aColToRec[colIdx] : colIdx;
        int rc = diffRecordField(r->pNewVal, r->nNewVal, recField, &st, &off);
        if( rc==SQLITE_OK ){
          doltliteResultField(ctx, r->pNewVal, r->nNewVal, st, off);
        }else if( rc==SQLITE_NOTFOUND ){
          sqlite3_result_null(ctx);
        }else{
          sqlite3_result_error_code(ctx, rc);
          return rc;
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col == nCols ){

    sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
  }else if( nCols > 0 && col == nCols+1 ){

    time_t t = (time_t)r->toDate;
    struct tm *tm = gmtime(&t);
    if(tm){
      char b[32];
      strftime(b, sizeof(b), "%Y-%m-%d %H:%M:%S", tm);
      sqlite3_result_text(ctx, b, -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_null(ctx);
    }
  }else if( nCols > 0 && col < 2*nCols+2 ){

    int colIdx = col - nCols - 2;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pOldVal && r->nOldVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pOldVal && r->nOldVal > 0 ){
        int st, off;
        int recField = pVtab->cols.aColToRec
                          ? pVtab->cols.aColToRec[colIdx] : colIdx;
        int rc = diffRecordField(r->pOldVal, r->nOldVal, recField, &st, &off);
        if( rc==SQLITE_OK ){
          doltliteResultField(ctx, r->pOldVal, r->nOldVal, st, off);
        }else if( rc==SQLITE_NOTFOUND ){
          sqlite3_result_null(ctx);
        }else{
          sqlite3_result_error_code(ctx, rc);
          return rc;
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col == 2*nCols+2 ){

    sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
  }else if( nCols > 0 && col == 2*nCols+3 ){

    time_t t = (time_t)r->fromDate;
    struct tm *tm = gmtime(&t);
    if(tm){
      char b[32];
      strftime(b, sizeof(b), "%Y-%m-%d %H:%M:%S", tm);
      sqlite3_result_text(ctx, b, -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_null(ctx);
    }
  }else if( nCols > 0 && col == 2*nCols+4 ){
    switch( r->diffType ){
      case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
      case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
      case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
    }
  }else{
    /* Hidden TVF arg columns (from_ref, to_ref) — never materialized
    ** in the result set, but SQLite may still ask for them during
    ** query planning. Always return NULL. */
    sqlite3_result_null(ctx);
  }

  return SQLITE_OK;
}

static int dtRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DiffTblCursor*)cur)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module diffTableModule = {
  0, dtConnect, dtConnect, dtBestIndex, dtDisconnect, dtDisconnect,
  dtOpen, dtClose, dtFilter, dtNext, dtEof, dtColumn, dtRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterDiffTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_diff_", &diffTableModule);
}

#endif
