
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "prolly_hashset.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <assert.h>
#include <string.h>
#include <time.h>

static char *buildDiffSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;

  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol,
                                     "to_", ", ")!=SQLITE_OK ){
    sqlite3_str_reset(pStr);
    return 0;
  }
  sqlite3_str_appendall(pStr, ", to_commit TEXT, to_commit_date TEXT");
  if( ci->nCol>0 ){
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol,
                                       "from_", ", ")!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ", from_commit TEXT, from_commit_date TEXT"
                              ", diff_type TEXT"
                              ", from_ref TEXT HIDDEN"
                              ", to_ref TEXT HIDDEN)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct AuditRow AuditRow;
struct AuditRow {
  u8 diffType;
  i64 intKey;
  u8 keyIsIntKey;
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  /* Owned records rebuilt from a clustered key, one per side's PK definition. */
  u8 *pKeyRec; int nKeyRec;
  u8 *pOldKeyRec; int nOldKeyRec;
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

  char       zFromCommit[PROLLY_HASH_SIZE*2+1];
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
  int nPairsAlloc;
  int iPair;
  int pairsDone;

  ProllyDiffIter diffIter;
  int diffIterOpen;

  /* Invalid side renders with the vtab's declared layout. */
  DoltliteSideCols fromSide;
  DoltliteSideCols toSide;
  int    needFilter;

  AuditRow row;
  int hasRow;
  i64 iRowid;
};

#define DT_IDX_TO_COMMIT_EQ   0x01
#define DT_IDX_SLICE          0x02
#define DT_IDX_RANGE_SPEC     0x04
#define DT_IDX_FROM_TO_COMMIT 0x08
#define DT_IDX_FROM_COMMIT_EQ 0x10

static int dtRefError(DiffTblVtab *pVtab, const char *zRef, int rc){
  const char *zKind;
  if( rc!=SQLITE_NOTFOUND && rc!=SQLITE_ERROR ) return rc;
  zKind = rc==SQLITE_NOTFOUND ? "ref not found" : "invalid ref";
  sqlite3_free(pVtab->base.zErrMsg);
  pVtab->base.zErrMsg = sqlite3_mprintf(
      "dolt_diff_%s: %s: %s", pVtab->zTableName, zKind, zRef ? zRef : "");
  return pVtab->base.zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
}

static void clearAuditRow(AuditRow *r){
  sqlite3_free(r->pKeyRec);
  sqlite3_free(r->pOldKeyRec);
  memset(r, 0, sizeof(*r));
}

static void closeDiffIter(DiffTblCursor *pCur){
  if( pCur->diffIterOpen ){
    prollyDiffIterClose(&pCur->diffIter);
    pCur->diffIterOpen = 0;
  }
}

typedef struct CmTblInfo CmTblInfo;
typedef struct CmTblMap CmTblMap;
struct CmTblInfo {
  ProllyHash key;
  ProllyHash tblRoot;
  ProllyHash catHash;
  ProllyHash schemaHash;
  u8         flags;
  i64        date;
  char       zHexName[PROLLY_HASH_SIZE*2+1];
};

struct CmTblMap {
  CmTblInfo *aEntry;
  int nEntry;
  int nAlloc;
  int *aSlot;
  int nSlot;
};

static u32 cmHashSlot(const ProllyHash *pKey, int nSlot){
  u32 h;
  memcpy(&h, pKey->data, sizeof(h));
  return h & (u32)(nSlot - 1);
}

static void cmMapFree(CmTblMap *pMap){
  sqlite3_free(pMap->aEntry);
  sqlite3_free(pMap->aSlot);
  memset(pMap, 0, sizeof(*pMap));
}

static int cmMapRebuild(CmTblMap *pMap, int nSlot){
  int i;
  int *aSlot;
  aSlot = sqlite3_malloc(nSlot * (int)sizeof(int));
  if( !aSlot ) return SQLITE_NOMEM;
  memset(aSlot, 0, nSlot * (int)sizeof(int));

  for(i=0; i<pMap->nEntry; i++){
    u32 slot = cmHashSlot(&pMap->aEntry[i].key, nSlot);
    while( aSlot[slot]!=0 ){
      slot = (slot + 1) & (u32)(nSlot - 1);
    }
    aSlot[slot] = i + 1;
  }

  sqlite3_free(pMap->aSlot);
  pMap->aSlot = aSlot;
  pMap->nSlot = nSlot;
  return SQLITE_OK;
}

static int cmMapEnsureSlots(CmTblMap *pMap){
  int nSlot;
  if( pMap->nSlot>0 && (pMap->nEntry + 1)*2 <= pMap->nSlot ){
    return SQLITE_OK;
  }
  nSlot = pMap->nSlot ? pMap->nSlot*2 : 16;
  while( nSlot < (pMap->nEntry + 1)*2 ) nSlot *= 2;
  return cmMapRebuild(pMap, nSlot);
}

static CmTblInfo *cmMapFind(CmTblMap *pMap, const ProllyHash *pKey){
  u32 slot;
  int i;
  if( pMap->nSlot==0 ) return 0;
  slot = cmHashSlot(pKey, pMap->nSlot);
  for(i=0; i<pMap->nSlot; i++){
    int idx = pMap->aSlot[slot];
    if( idx==0 ) return 0;
    if( prollyHashCompare(&pMap->aEntry[idx-1].key, pKey)==0 ){
      return &pMap->aEntry[idx-1];
    }
    slot = (slot + 1) & (u32)(pMap->nSlot - 1);
  }
  return 0;
}

static int cmMapPut(CmTblMap *pMap, const ProllyHash *pKey,
                    const ProllyHash *pTblRoot,
                    const ProllyHash *pCatHash,
                    const ProllyHash *pSchemaHash,
                    u8 flags,
                    const char *zHexName, i64 date){
  CmTblInfo *e;
  int rc;

  e = cmMapFind(pMap, pKey);
  if( !e ){
    u32 slot;
    rc = cmMapEnsureSlots(pMap);
    if( rc!=SQLITE_OK ) return rc;
    if( pMap->nEntry >= pMap->nAlloc ){
      int nNew = pMap->nAlloc ? pMap->nAlloc*2 : 16;
      CmTblInfo *aNew = sqlite3_realloc(pMap->aEntry,
                            nNew*(int)sizeof(CmTblInfo));
      if( !aNew ) return SQLITE_NOMEM;
      pMap->aEntry = aNew;
      pMap->nAlloc = nNew;
    }
    e = &pMap->aEntry[pMap->nEntry];
    memset(e, 0, sizeof(*e));
    e->key = *pKey;
    slot = cmHashSlot(pKey, pMap->nSlot);
    while( pMap->aSlot[slot]!=0 ){
      slot = (slot + 1) & (u32)(pMap->nSlot - 1);
    }
    pMap->aSlot[slot] = pMap->nEntry + 1;
    pMap->nEntry++;
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
                       const char *zFromHex,
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
  int nNew;
  if( pCur->nPairs>=pCur->nPairsAlloc ){
    nNew = pCur->nPairsAlloc ? pCur->nPairsAlloc*2 : 16;
    aNew = sqlite3_realloc(pCur->aPairs,
                           nNew*(int)sizeof(DiffPair));
    if( !aNew ) return SQLITE_NOMEM;
    pCur->aPairs = aNew;
    pCur->nPairsAlloc = nNew;
  }
  r = &pCur->aPairs[pCur->nPairs++];
  memset(r, 0, sizeof(*r));
  memcpy(r->zFromCommit, zFromHex, PROLLY_HASH_SIZE*2+1);
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

static int stackPushUnique(ProllyHashSet *pSeen,
                           ProllyHash **paStack, int *pnStack, int *pnStackAlloc,
                           const ProllyHash *pHash){
  int rc;
  if( prollyHashSetContains(pSeen, pHash) ) return SQLITE_OK;
  rc = prollyHashSetAdd(pSeen, pHash);
  if( rc!=SQLITE_OK ) return rc;
  if( *pnStack >= *pnStackAlloc ){
    int nNew = *pnStackAlloc ? (*pnStackAlloc)*2 : 16;
    ProllyHash *tmp = sqlite3_realloc(*paStack, nNew*(int)sizeof(ProllyHash));
    if( !tmp ) return SQLITE_NOMEM;
    *paStack = tmp; *pnStackAlloc = nNew;
  }
  (*paStack)[*pnStack] = *pHash;
  (*pnStack)++;
  return SQLITE_OK;
}

static int seedWorkingChildInfo(
  sqlite3 *db,
  const ProllyHash *pHeadHash,
  const char *zTableName,
  CmTblMap *pMap
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
  rc = doltliteLoadTableRootByNameOrEmpty(db, &workingCat, zTableName,
                                          &workingTblRoot, &workingFlags,
                                          &workingSchemaHash);
  if( rc!=SQLITE_OK ) return rc;
  return cmMapPut(pMap, pHeadHash, &workingTblRoot, &workingCat,
                  &workingSchemaHash, workingFlags, zWorking, 0);
}

static int appendCurrentDiffPair(
  DiffTblCursor *pCur,
  const ProllyHash *pCurr,
  const DoltliteCommit *pCommit,
  const ProllyHash *pCurTblRoot,
  const ProllyHash *pCurSchemaHash,
  u8 curFlags,
  CmTblMap *pMap
){
  CmTblInfo *pInfo;
  int rootsDiffer;
  int schemasDiffer;
  u8 fromFlags;

  pInfo = cmMapFind(pMap, pCurr);
  if( !pInfo ) return SQLITE_OK;
  rootsDiffer = prollyHashCompare(&pInfo->tblRoot, pCurTblRoot)!=0;
  schemasDiffer = prollyHashCompare(&pInfo->schemaHash, pCurSchemaHash)!=0;
  if( !rootsDiffer && !schemasDiffer ) return SQLITE_OK;

  fromFlags = curFlags;
  if( fromFlags==0 ) fromFlags = pInfo->flags;
  {
    char zFromHex[PROLLY_HASH_SIZE*2+1];
    doltliteHashToHex(pCurr, zFromHex);
    return pairsAppend(pCur, zFromHex, pCurTblRoot, &pCommit->catalogHash,
                       pCurSchemaHash, fromFlags, pCommit->timestamp,
                       pInfo->zHexName, &pInfo->tblRoot, &pInfo->catHash,
                       &pInfo->schemaHash, pInfo->flags, pInfo->date);
  }
}

static int registerCommitParents(
  CmTblMap *pMap,
  ProllyHashSet *pSeen,
  ProllyHash **paStack,
  int *pnStack,
  int *pnStackAlloc,
  const DoltliteCommit *pCommit,
  const ProllyHash *pCurTblRoot,
  const ProllyHash *pCurSchemaHash,
  u8 curFlags,
  const char *zCurHex
){
  int i;
  const ProllyHash *pParent;
  int rc;

  for(i=0; i<doltliteCommitParentCount(pCommit); i++){
    pParent = doltliteCommitParentHash(pCommit, i);
    if( !pParent ) continue;
    rc = cmMapPut(pMap, pParent, pCurTblRoot, &pCommit->catalogHash,
                  pCurSchemaHash, curFlags, zCurHex, pCommit->timestamp);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<doltliteCommitParentCount(pCommit); i++){
    pParent = doltliteCommitParentHash(pCommit, i);
    if( !pParent ) continue;
    rc = stackPushUnique(pSeen, paStack, pnStack, pnStackAlloc, pParent);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int buildDiffPairs(DiffTblCursor *pCur, sqlite3 *db,
                          const char *zTableName){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  CmTblMap map;
  ProllyHash *aStack = 0;
  int nStack = 0, nStackAlloc = 0;
  ProllyHashSet seen;
  int seenInit = 0;
  int currInited = 0;
  ProllyHash curr;
  int rc = SQLITE_OK;

  if( !cs ) return SQLITE_OK;
  memset(&map, 0, sizeof(map));

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;
  rc = seedWorkingChildInfo(db, &headHash, zTableName, &map);
  if( rc!=SQLITE_OK ) goto walk_done;

  rc = prollyHashSetInit(&seen, 64);
  if( rc!=SQLITE_OK ) goto walk_done;
  seenInit = 1;

  rc = stackPushUnique(&seen, &aStack, &nStack, &nStackAlloc, &headHash);
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

    rc = doltliteLoadTableRootByNameOrEmpty(db, &commit.catalogHash,
                                            zTableName, &curTblRoot,
                                            &curFlags, &curSchemaHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      break;
    }

    doltliteHashToHex(&curr, curHex);
    rc = appendCurrentDiffPair(pCur, &curr, &commit, &curTblRoot,
                               &curSchemaHash, curFlags, &map);
    if( rc==SQLITE_OK ){
      rc = registerCommitParents(&map, &seen, &aStack, &nStack, &nStackAlloc,
                                 &commit, &curTblRoot,
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
  cmMapFree(&map);
  sqlite3_free(aStack);
  if( seenInit ) prollyHashSetFree(&seen);
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
  char zHeadHex[PROLLY_HASH_SIZE*2+1];

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
  rc = doltliteLoadTableRootByNameOrEmpty(db, &headCommit.catalogHash,
                                          zTableName, &headTblRoot,
                                          &headFlags, &headSchemaHash);
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
      doltliteHashToHex(&headHash, zHeadHex);
      fromFlags = headFlags ? headFlags : workingFlags;
      rc = pairsAppend(pCur, zHeadHex, &headTblRoot, &headCommit.catalogHash,
                       &headSchemaHash, fromFlags, headCommit.timestamp,
                       zWorking, &workingTblRoot, &workingCat, &workingSchemaHash,
                       workingFlags, 0);
    }
  }
  doltliteCommitClear(&headCommit);
  return rc;
}

static int buildCommitDiffPair(
  DiffTblCursor *pCur,
  sqlite3 *db,
  const char *zTableName,
  const char *zToCommit,
  int *pbRefError
){
  ProllyHash toHash;
  ProllyHash fromHash;
  ProllyHash fromTblRoot, toTblRoot;
  ProllyHash fromCatHash, toCatHash;
  ProllyHash fromSchemaHash, toSchemaHash;
  DoltliteCommit toCommit;
  u8 fromFlags = 0;
  u8 toFlags = 0;
  i64 fromDate = 0;
  char zFromLabel[PROLLY_HASH_SIZE*2+1];
  char zToLabel[PROLLY_HASH_SIZE*2+1];
  const ProllyHash *pParent;
  int nParents;
  int nIter;
  int i;
  int rc;

  *pbRefError = 0;
  if( !zToCommit ) return SQLITE_OK;

  memset(&toHash, 0, sizeof(toHash));
  memset(&fromHash, 0, sizeof(fromHash));
  memset(&fromTblRoot, 0, sizeof(fromTblRoot));
  memset(&toTblRoot, 0, sizeof(toTblRoot));
  memset(&fromCatHash, 0, sizeof(fromCatHash));
  memset(&toCatHash, 0, sizeof(toCatHash));
  memset(&fromSchemaHash, 0, sizeof(fromSchemaHash));
  memset(&toSchemaHash, 0, sizeof(toSchemaHash));
  memset(&toCommit, 0, sizeof(toCommit));
  memset(zToLabel, 0, sizeof(zToLabel));

  rc = doltliteResolveRef(db, zToCommit, &toHash);
  if( rc!=SQLITE_OK ){
    *pbRefError = 1;
    return rc;
  }

  /* to_commit filter only walks the session head's ancestry; unreachable commits yield no rows. */
  {
    ProllyHash head, base;
    memset(&head, 0, sizeof(head));
    doltliteGetSessionHead(db, &head);
    if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
    rc = doltliteFindAncestor(db, &head, &toHash, &base);
    if( rc==SQLITE_NOTFOUND
     || (rc==SQLITE_OK && prollyHashCompare(&base, &toHash)!=0) ){
      return SQLITE_OK;
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = doltliteLoadCommit(db, &toHash, &toCommit);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(&toCatHash, &toCommit.catalogHash, sizeof(ProllyHash));
  rc = doltliteLoadTableRootByNameOrEmpty(db, &toCatHash, zTableName,
                                          &toTblRoot, &toFlags,
                                          &toSchemaHash);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&toCommit);
    return rc;
  }

  nParents = doltliteCommitParentCount(&toCommit);
  nIter = nParents>0 ? nParents : 1;
  doltliteHashToHex(&toHash, zToLabel);
  for(i=0; i<nIter; i++){
    u8 pairFromFlags;
    u8 pairToFlags;

    memset(&fromHash, 0, sizeof(fromHash));
    memset(&fromTblRoot, 0, sizeof(fromTblRoot));
    memset(&fromCatHash, 0, sizeof(fromCatHash));
    memset(&fromSchemaHash, 0, sizeof(fromSchemaHash));
    fromFlags = 0;
    fromDate = 0;

    if( nParents>0 ){
      DoltliteCommit fromCommit;
      pParent = doltliteCommitParentHash(&toCommit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      memset(&fromCommit, 0, sizeof(fromCommit));
      fromHash = *pParent;
      rc = doltliteLoadCommit(db, &fromHash, &fromCommit);
      if( rc==SQLITE_OK ){
        memcpy(&fromCatHash, &fromCommit.catalogHash, sizeof(ProllyHash));
        fromDate = fromCommit.timestamp;
        rc = doltliteLoadTableRootByNameOrEmpty(db, &fromCatHash, zTableName,
                                                &fromTblRoot, &fromFlags,
                                                &fromSchemaHash);
      }
      doltliteCommitClear(&fromCommit);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&toCommit);
        return rc;
      }
    }

    if( prollyHashCompare(&fromTblRoot, &toTblRoot)==0
     && prollyHashCompare(&fromSchemaHash, &toSchemaHash)==0 ){
      continue;
    }

    pairFromFlags = fromFlags ? fromFlags : toFlags;
    pairToFlags = toFlags ? toFlags : fromFlags;
    doltliteHashToHex(&fromHash, zFromLabel);
    rc = pairsAppend(pCur, zFromLabel, &fromTblRoot, &fromCatHash,
                     &fromSchemaHash, pairFromFlags, fromDate,
                     zToLabel, &toTblRoot, &toCatHash, &toSchemaHash,
                     pairToFlags, toCommit.timestamp);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&toCommit);
      return rc;
    }
  }
  doltliteCommitClear(&toCommit);
  return SQLITE_OK;
}

typedef struct DtSliceEnd DtSliceEnd;
struct DtSliceEnd {
  ProllyHash catHash;
  ProllyHash tblRoot;
  ProllyHash schemaHash;
  u8 flags;
  i64 date;
  char zLabel[PROLLY_HASH_SIZE*2+1];
};

static int dtResolveSliceEnd(
  sqlite3 *db, const char *zRef, const char *zTableName, DtSliceEnd *pEnd,
  int *pbRefError
){
  int rc;
  *pbRefError = 0;
  memset(pEnd, 0, sizeof(*pEnd));

  rc = doltliteResolveCatalogHashForRef(db, zRef, &pEnd->catHash);
  if( rc!=SQLITE_OK ){
    *pbRefError = 1;
    return rc;
  }

  if( doltliteRefIsWorking(zRef) ){
    rc = doltliteGetWorkingTableState(db, zTableName, &pEnd->tblRoot,
                                      &pEnd->flags, &pEnd->schemaHash);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK ) return rc;
    memcpy(pEnd->zLabel, "WORKING", 8);
    return SQLITE_OK;
  }

  rc = doltliteLoadTableRootByNameOrEmpty(db, &pEnd->catHash, zTableName,
                                          &pEnd->tblRoot, &pEnd->flags,
                                          &pEnd->schemaHash);
  if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;

  if( doltliteRefIsStaged(zRef) ){
    memcpy(pEnd->zLabel, "STAGED", 7);
    return SQLITE_OK;
  }

  {
    ProllyHash commitHash;
    DoltliteCommit commit;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc!=SQLITE_OK ){
      *pbRefError = 1;
      return rc;
    }
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ) return rc;
    pEnd->date = commit.timestamp;
    doltliteCommitClear(&commit);
    doltliteHashToHex(&commitHash, pEnd->zLabel);
  }
  return SQLITE_OK;
}

static int buildSliceDiffPair(
  DiffTblCursor *pCur,
  sqlite3 *db,
  const char *zTableName,
  const char *zFromRef,
  const char *zToRef,
  const char **pzBadRef
){
  DtSliceEnd from, to;
  int bRefError = 0;
  int rc;

  *pzBadRef = 0;
  if( !zFromRef || !zToRef ) return SQLITE_OK;

  rc = dtResolveSliceEnd(db, zFromRef, zTableName, &from, &bRefError);
  if( bRefError ) *pzBadRef = zFromRef;
  if( rc!=SQLITE_OK ) return rc;
  rc = dtResolveSliceEnd(db, zToRef, zTableName, &to, &bRefError);
  if( bRefError ) *pzBadRef = zToRef;
  if( rc!=SQLITE_OK ) return rc;

  if( prollyHashCompare(&from.tblRoot, &to.tblRoot)==0
   && prollyHashCompare(&from.schemaHash, &to.schemaHash)==0 ){
    return SQLITE_OK;
  }

  if( !from.flags ) from.flags = to.flags;
  if( !to.flags ) to.flags = from.flags;
  return pairsAppend(pCur, from.zLabel, &from.tblRoot, &from.catHash,
                     &from.schemaHash, from.flags, from.date,
                     to.zLabel, &to.tblRoot, &to.catHash, &to.schemaHash,
                     to.flags, to.date);
}

static int buildRangeSpecDiffPair(
  DiffTblCursor *pCur,
  sqlite3 *db,
  const char *zTableName,
  const char *zSpec
){
  char *zLeft = 0;
  char *zRight = 0;
  const char *zBadRef = 0;
  int rangeType = DOLTLITE_RANGE_NONE;
  int rc;

  rc = doltliteSplitRevisionRange(zSpec, &zLeft, &zRight, &rangeType);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_MISMATCH;
  if( rc!=SQLITE_OK ) return rc;
  if( rangeType==DOLTLITE_RANGE_THREE_DOT ){
    ProllyHash leftHash, rightHash, ancestor;
    char zAncestor[PROLLY_HASH_SIZE*2+1];
    rc = doltliteResolveRef(db, zLeft, &leftHash);
    if( rc==SQLITE_OK ) rc = doltliteResolveRef(db, zRight, &rightHash);
    if( rc==SQLITE_OK ){
      rc = doltliteFindAncestor(db, &leftHash, &rightHash, &ancestor);
    }
    if( rc==SQLITE_OK ){
      doltliteHashToHex(&ancestor, zAncestor);
      rc = buildSliceDiffPair(
          pCur, db, zTableName, zAncestor, zRight, &zBadRef);
    }
  }else{
    rc = buildSliceDiffPair(
        pCur, db, zTableName, zLeft, zRight, &zBadRef);
  }
  sqlite3_free(zLeft);
  sqlite3_free(zRight);
  return rc;
}

static void freePairCols(DiffTblCursor *pCur){
  doltliteSideColsClear(&pCur->fromSide);
  doltliteSideColsClear(&pCur->toSide);
  pCur->needFilter = 0;
}

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
    if( !doltliteFieldValuesEqual(
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

  pCur->needFilter = 0;

  if( !cs ) return SQLITE_OK;

  if( pCur->iPair >= pCur->nPairs ){
    pCur->pairsDone = 1;
    return SQLITE_OK;
  }

  {
    DiffPair *p;
    u8 fromFlags;
    u8 toFlags;
    p = &pCur->aPairs[pCur->iPair++];
    fromFlags = p->fromFlags ? p->fromFlags : p->toFlags;
    toFlags = p->toFlags ? p->toFlags : p->fromFlags;
    memcpy(pCur->row.zFromCommit, p->zFromCommit, PROLLY_HASH_SIZE*2+1);
    pCur->row.fromDate = p->fromDate;
    memcpy(pCur->row.zToCommit, p->zToCommit, PROLLY_HASH_SIZE*2+1);
    pCur->row.toDate = p->toDate;
    rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                            &p->fromTblRoot, &p->toTblRoot,
                            fromFlags, toFlags);
    if( rc!=SQLITE_OK ) return rc;
    pCur->diffIterOpen = 1;

    rc = doltliteSideColsLoad(db, &p->fromCatHash, &p->fromSchemaHash,
                              pVtab->zTableName, &pVtab->cols,
                              !prollyHashIsEmpty(&p->fromTblRoot),
                              &pCur->fromSide);
    if( rc==SQLITE_OK ){
      rc = doltliteSideColsLoad(db, &p->toCatHash, &p->toSchemaHash,
                                pVtab->zTableName, &pVtab->cols,
                                !prollyHashIsEmpty(&p->toTblRoot),
                                &pCur->toSide);
    }
    if( rc!=SQLITE_OK ) return rc;

    pCur->needFilter = pCur->fromSide.valid && pCur->toSide.valid
        && prollyHashCompare(&p->fromSchemaHash, &p->toSchemaHash)!=0;
    return SQLITE_OK;
  }
}

static int advanceToNextRow(DiffTblCursor *pCur, sqlite3 *db){
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
                               &pCur->fromSide.ci, &pCur->toSide.ci) ){
          continue;
        }

        pCur->row.pOldVal = 0;
        pCur->row.nOldVal = 0;
        pCur->row.pNewVal = 0;
        pCur->row.nNewVal = 0;

        pCur->row.diffType = pChange->type;
        pCur->row.intKey = pChange->intKey;
        pCur->row.keyIsIntKey = pChange->keyIsIntKey;

        pCur->row.pOldVal = pChange->pOldVal;
        pCur->row.nOldVal = pChange->pOldVal ? pChange->nOldVal : 0;
        pCur->row.pNewVal = pChange->pNewVal;
        pCur->row.nNewVal = pChange->pNewVal ? pChange->nNewVal : 0;

        /* Clustered empty value: decode each side with that side's PK definition. */
        if( (pCur->row.nOldVal==0 && pChange->type!=PROLLY_DIFF_ADD)
         || (pCur->row.nNewVal==0 && pChange->type!=PROLLY_DIFF_DELETE) ){
          DiffTblVtab *pV = (DiffTblVtab*)pCur->base.pVtab;
          if( pCur->row.nOldVal==0 && pChange->type!=PROLLY_DIFF_ADD ){
            sqlite3_free(pCur->row.pOldKeyRec);
            pCur->row.pOldKeyRec = 0;
            pCur->row.nOldKeyRec = 0;
            rc = pCur->fromSide.valid
              ? doltliteRecordFromClusteredKeyCols(db, &pCur->fromSide.ci,
                    pChange->pKey, pChange->nKey,
                    &pCur->row.pOldKeyRec, &pCur->row.nOldKeyRec)
              : doltliteRecordFromClusteredKey(db, pV->zTableName,
                    pChange->pKey, pChange->nKey,
                    &pCur->row.pOldKeyRec, &pCur->row.nOldKeyRec);
            if( rc!=SQLITE_OK ) return rc;
            if( pCur->row.pOldKeyRec ){
              pCur->row.pOldVal = pCur->row.pOldKeyRec;
              pCur->row.nOldVal = pCur->row.nOldKeyRec;
            }
          }
          if( pCur->row.nNewVal==0 && pChange->type!=PROLLY_DIFF_DELETE ){
            sqlite3_free(pCur->row.pKeyRec);
            pCur->row.pKeyRec = 0;
            pCur->row.nKeyRec = 0;
            rc = pCur->toSide.valid
              ? doltliteRecordFromClusteredKeyCols(db, &pCur->toSide.ci,
                    pChange->pKey, pChange->nKey,
                    &pCur->row.pKeyRec, &pCur->row.nKeyRec)
              : doltliteRecordFromClusteredKey(db, pV->zTableName,
                    pChange->pKey, pChange->nKey,
                    &pCur->row.pKeyRec, &pCur->row.nKeyRec);
            if( rc!=SQLITE_OK ) return rc;
            if( pCur->row.pKeyRec ){
              pCur->row.pNewVal = pCur->row.pKeyRec;
              pCur->row.nNewVal = pCur->row.nKeyRec;
            }
          }
        }

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
  }
}

static int dtConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  return doltliteVtabConnectHistoricalTable(db, argc, argv, "dolt_diff_",
                                            sizeof(DiffTblVtab),
                                            buildDiffSchema, ppVtab, pzErr);
}

static int dtBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  DiffTblVtab *p = (DiffTblVtab*)pVtab;
  int i;
  int iToCommitEq = -1;
  int iFromCommitEq = -1;
  int iFromRefEq = -1;
  int iToRefEq = -1;
  int nUser = p->cols.nCol;
  int toCommitCol = nUser;
  int fromCommitCol = 2*nUser + 2;
  int fromRefCol  = 2*nUser + 5;
  int toRefCol    = 2*nUser + 6;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn==toCommitCol ){
      iToCommitEq = i;
    }else if( pInfo->aConstraint[i].iColumn==fromCommitCol ){
      iFromCommitEq = i;
    }else if( pInfo->aConstraint[i].iColumn==fromRefCol ){
      iFromRefEq = i;
    }else if( pInfo->aConstraint[i].iColumn==toRefCol ){
      iToRefEq = i;
    }
  }

  if( iFromRefEq>=0 && iToRefEq>=0 ){
    pInfo->idxNum = DT_IDX_SLICE;
    pInfo->aConstraintUsage[iFromRefEq].argvIndex = 1;
    pInfo->aConstraintUsage[iFromRefEq].omit = 1;
    pInfo->aConstraintUsage[iToRefEq].argvIndex = 2;
    pInfo->aConstraintUsage[iToRefEq].omit = 1;
    pInfo->estimatedCost = 10.0;
  }else if( iFromRefEq>=0 ){
    pInfo->idxNum = DT_IDX_RANGE_SPEC;
    pInfo->aConstraintUsage[iFromRefEq].argvIndex = 1;
    pInfo->aConstraintUsage[iFromRefEq].omit = 1;
    pInfo->estimatedCost = 10.0;
  }else if( iFromCommitEq>=0 && iToCommitEq>=0 ){
    /* Both ends named: the arbitrary-pair diff, same as the function
    ** form. Constraints are consumed after ref resolution, so revision
    ** specs never face a text recheck against the rendered hash. */
    pInfo->idxNum = DT_IDX_FROM_TO_COMMIT;
    pInfo->aConstraintUsage[iFromCommitEq].argvIndex = 1;
    pInfo->aConstraintUsage[iFromCommitEq].omit = 1;
    pInfo->aConstraintUsage[iToCommitEq].argvIndex = 2;
    pInfo->aConstraintUsage[iToCommitEq].omit = 1;
    pInfo->estimatedCost = 10.0;
  }else if( iToCommitEq>=0 ){
    pInfo->idxNum = DT_IDX_TO_COMMIT_EQ;
    pInfo->aConstraintUsage[iToCommitEq].argvIndex = 1;
    pInfo->aConstraintUsage[iToCommitEq].omit = 1;
    pInfo->estimatedCost = 100.0;
  }else if( iFromCommitEq>=0 ){
    pInfo->idxNum = DT_IDX_FROM_COMMIT_EQ;
    pInfo->aConstraintUsage[iFromCommitEq].argvIndex = 1;
    pInfo->aConstraintUsage[iFromCommitEq].omit = 1;
    pInfo->estimatedCost = 500.0;
  }else{
    pInfo->idxNum = 0;
    pInfo->estimatedCost = 10000.0;
  }
  return SQLITE_OK;
}

static int dtOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  return doltliteVtabOpenCursor(pp, sizeof(DiffTblCursor));
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
  c->nPairsAlloc = 0;
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
    const char *zBadRef = 0;
    rc = buildSliceDiffPair(
        c, db, pVtab->zTableName, zFromRef, zToRef, &zBadRef);
    if( zBadRef ) rc = dtRefError(pVtab, zBadRef, rc);
  }else if( (idxNum & DT_IDX_RANGE_SPEC)!=0 && argc>=1 ){
    const char *zSpec = (const char*)sqlite3_value_text(argv[0]);
    rc = buildRangeSpecDiffPair(c, db, pVtab->zTableName, zSpec);
    if( rc==SQLITE_MISMATCH ){
      sqlite3_free(pVtab->base.zErrMsg);
      pVtab->base.zErrMsg = sqlite3_mprintf(
          "dolt_diff_%s requires a '..' or '...' revision range",
          pVtab->zTableName);
    }else if( rc==SQLITE_ERROR || rc==SQLITE_NOTFOUND ){
      sqlite3_free(pVtab->base.zErrMsg);
      pVtab->base.zErrMsg = sqlite3_mprintf(
          "dolt_diff_%s: invalid revision range '%s'",
          pVtab->zTableName, zSpec ? zSpec : "");
      rc = pVtab->base.zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
  }else if( (idxNum & DT_IDX_FROM_TO_COMMIT)!=0 && argc>=2 ){
    const char *zFrom = (const char*)sqlite3_value_text(argv[0]);
    const char *zTo = (const char*)sqlite3_value_text(argv[1]);
    const char *zBadRef = 0;
    rc = buildSliceDiffPair(
        c, db, pVtab->zTableName, zFrom, zTo, &zBadRef);
    if( zBadRef ) rc = dtRefError(pVtab, zBadRef, rc);
  }else if( (idxNum & DT_IDX_TO_COMMIT_EQ)!=0 && argc>=1 ){
    const char *zToCommit = (const char*)sqlite3_value_text(argv[0]);
    if( zToCommit && sqlite3_stricmp(zToCommit, "WORKING")==0 ){
      rc = buildWorkingDiffPair(c, db, pVtab->zTableName);
    }else if( zToCommit && sqlite3_stricmp(zToCommit, "STAGED")==0 ){
      const char *zBadRef = 0;
      rc = buildSliceDiffPair(
          c, db, pVtab->zTableName, "HEAD", "STAGED", &zBadRef);
      if( zBadRef ) rc = dtRefError(pVtab, zBadRef, rc);
    }else{
      int bRefError = 0;
      rc = buildCommitDiffPair(
          c, db, pVtab->zTableName, zToCommit, &bRefError);
      if( bRefError ) rc = dtRefError(pVtab, zToCommit, rc);
    }
  }else if( (idxNum & DT_IDX_FROM_COMMIT_EQ)!=0 && argc>=1 ){
    /* Resolve the ref once, then keep only history pairs departing it. */
    const char *zFrom = (const char*)sqlite3_value_text(argv[0]);
    char zLabel[PROLLY_HASH_SIZE*2+1];
    rc = SQLITE_OK;
    zLabel[0] = 0;
    if( zFrom && (sqlite3_stricmp(zFrom, "WORKING")==0
               || sqlite3_stricmp(zFrom, "STAGED")==0) ){
      sqlite3_snprintf(sizeof(zLabel), zLabel, "%s", zFrom);
    }else if( zFrom ){
      ProllyHash fromHash;
      rc = doltliteResolveRef(db, zFrom, &fromHash);
      if( rc==SQLITE_OK ){
        doltliteHashToHex(&fromHash, zLabel);
      }else{
        rc = dtRefError(pVtab, zFrom, rc);
      }
    }
    if( zLabel[0] ){
      rc = buildDiffPairs(c, db, pVtab->zTableName);
      if( rc==SQLITE_OK ){
        int iKeep = 0;
        int iScan;
        for(iScan=0; iScan<c->nPairs; iScan++){
          if( sqlite3_stricmp(c->aPairs[iScan].zFromCommit, zLabel)==0 ){
            c->aPairs[iKeep++] = c->aPairs[iScan];
          }
        }
        c->nPairs = iKeep;
      }
    }
  }else{

    rc = buildDiffPairs(c, db, pVtab->zTableName);
  }
  if( rc!=SQLITE_OK ) return rc;
  if( c->nPairs==0 ){
    c->pairsDone = 1;
    return SQLITE_OK;
  }

  return advanceToNextRow(c, db);
}

static int dtNext(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  return advanceToNextRow(c, pVtab->db);
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
    doltliteResultSideCol(ctx, &c->toSide, &pVtab->cols,
                          r->pNewVal, r->nNewVal,
                          r->intKey, r->keyIsIntKey, col);
  }else if( nCols > 0 && col == nCols ){

    sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
  }else if( nCols > 0 && col == nCols+1 ){
    doltliteResultTimestamp(ctx, r->toDate);
  }else if( nCols > 0 && col < 2*nCols+2 ){
    int colIdx = col - nCols - 2;
    doltliteResultSideCol(ctx, &c->fromSide, &pVtab->cols,
                          r->pOldVal, r->nOldVal,
                          r->intKey, r->keyIsIntKey, colIdx);
  }else if( nCols > 0 && col == 2*nCols+2 ){

    sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
  }else if( nCols > 0 && col == 2*nCols+3 ){
    doltliteResultTimestamp(ctx, r->fromDate);
  }else if( nCols > 0 && col == 2*nCols+4 ){
    const char *zType = prollyDiffTypeName(r->diffType);
    if( zType ) sqlite3_result_text(ctx, zType, -1, SQLITE_STATIC);
    else sqlite3_result_null(ctx);
  }else{
    sqlite3_result_null(ctx);
  }

  return SQLITE_OK;
}

static int dtRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DiffTblCursor*)cur)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module diffTableModule = {
  0, dtConnect, dtConnect, dtBestIndex,
  doltliteVtabCommonDisconnect, doltliteVtabCommonDisconnect,
  dtOpen, dtClose, dtFilter, dtNext, dtEof, dtColumn, dtRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

const sqlite3_module *doltliteDiffTableModule(void){
  return &diffTableModule;
}

#endif
