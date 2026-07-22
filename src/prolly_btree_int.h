#ifndef PROLLY_BTREE_INT_H
#define PROLLY_BTREE_INT_H

/* Private declarations shared by the Prolly B-tree implementation modules. */

#include "sqliteInt.h"
#include "btree.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_encoding.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_hashset.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include "prolly_chunk_walk.h"
#include "pager_shim.h"
#include "doltlite_commit.h"
#include "record_codec.h"
#include "sortkey.h"
#include "btree_orig_api.h"
#include "vdbeInt.h"

#include <string.h>
#include <assert.h>

#define SERIAL_TYPE_NULL      0
#define SERIAL_TYPE_INT8      1
#define SERIAL_TYPE_INT16     2
#define SERIAL_TYPE_INT24     3
#define SERIAL_TYPE_INT32     4
#define SERIAL_TYPE_INT48     5
#define SERIAL_TYPE_INT64     6
#define SERIAL_TYPE_FLOAT64   7
#define SERIAL_TYPE_ZERO      8
#define SERIAL_TYPE_ONE       9
#define SERIAL_TYPE_TEXT_BASE 13
#define SERIAL_TYPE_BLOB_BASE 12
#define MAX_RECORD_FIELDS    256
#define MAX_ONEBYTE_HEADER   126

void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
char *doltliteCanonicalizeSchemaSql(const char *zSql, const char *zName);
int doltliteLoadLiveSchemaSql(sqlite3 *db, const char *zType,
                              const char *zDb,
                              const char *zName, const char *zTblName,
                              char **pzSql);
int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);
struct TableEntry;
typedef struct SchemaEntry SchemaEntry;
#ifndef DOLTLITE_SCHEMAENTRY_DEFINED
#define DOLTLITE_SCHEMAENTRY_DEFINED
struct SchemaEntry {
  char *zName;
  char *zTblName;
  char *zSql;
  char *zType;
  Pgno iRootpage;
};
#endif
int doltliteSerializeCatalogEntriesWithFallbackSchema(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  u8 **ppOut,
  int *pnOut
);
int origBtreeCheckpoint(void *p, int eMode, int *pnLog, int *pnCkpt);

static u32 getU32LE(const u8 *p){
  return ((u32)p[0]) | ((u32)p[1]<<8) | ((u32)p[2]<<16) | ((u32)p[3]<<24);
}

static void putU32LE(u8 *p, u32 v){
  p[0] = (u8)v;
  p[1] = (u8)(v>>8);
  p[2] = (u8)(v>>16);
  p[3] = (u8)(v>>24);
}

#ifndef TRANS_NONE
#define TRANS_NONE  0
#define TRANS_READ  1
#define TRANS_WRITE 2
#endif

#ifndef SAVEPOINT_BEGIN
#define SAVEPOINT_BEGIN    0
#define SAVEPOINT_RELEASE  1
#define SAVEPOINT_ROLLBACK 2
#endif

#define CURSOR_VALID       0
#define CURSOR_INVALID     1
#define CURSOR_SKIPNEXT    2
#define CURSOR_REQUIRESEEK 3
#define CURSOR_FAULT       4

#define BTCF_WriteFlag  0x01
#define BTCF_ValidNKey  0x02
#define BTCF_ValidOvfl  0x04
#define BTCF_AtLast     0x08
#define BTCF_Incrblob   0x10
#define BTCF_Multiple   0x20
#define BTCF_Pinned     0x40
#define BTCF_DeleteKey  0x80

#define BTS_READ_ONLY       0x0001
#define BTS_INITIALLY_EMPTY 0x0010

#define CLEAR_CACHED_PAYLOAD(pCsr) do{ \
  if( (pCsr)->cachedPayloadOwned && (pCsr)->pCachedPayload ){ \
    sqlite3_free((pCsr)->pCachedPayload); \
  } \
  if( (pCsr)->pCachedFrom ){ \
    prollyCacheRelease((pCsr)->pCur.pCache, (pCsr)->pCachedFrom); \
    (pCsr)->pCachedFrom = 0; \
  } \
  (pCsr)->pCachedPayload = 0; \
  (pCsr)->nCachedPayload = 0; \
  (pCsr)->cachedPayloadOwned = 0; \
}while(0)

#define CLEAR_CACHED_SEEK_KEY(pCur) do{ \
  (pCur)->nSeekSortKey = 0; \
  (pCur)->nSeekKeyField = 0; \
}while(0)

#define CLEAR_CACHED_COMPARE_KEY(pCur) do{ \
  (pCur)->nCompareSortKey = 0; \
  (pCur)->nCompareKeyField = 0; \
}while(0)

#define PROLLY_DEFAULT_CACHE_SIZE 16384

#ifdef SQLITE_DEFAULT_PAGE_SIZE
# define PROLLY_DEFAULT_PAGE_SIZE SQLITE_DEFAULT_PAGE_SIZE
#else
# define PROLLY_DEFAULT_PAGE_SIZE 4096
#endif

#define PROLLY_MAX_RECORD_SIZE ((sqlite3_int64)(1024*1024*1024))

#ifndef DOLTLITE_TABLEENTRY_DEFINED
#define DOLTLITE_TABLEENTRY_DEFINED
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
  u8 appendSeekFloorValid;
  i64 appendSeekFloor;
  ProllyHash appendSeekRoot;
  u8 tableRootKnown;
  u8 isTableRoot;
  char *zName;
  ProllyMutMap *pPending;
};
#endif

typedef struct Catalog Catalog;
struct Catalog {
  struct TableEntry *a;
  int n;
  int nAlloc;
  Pgno iNextTable;
};

typedef struct SavepointTableEntry SavepointTableEntry;
struct SavepointTableEntry {
  Pgno iTable;
  u8 pendingFlushSeekEdits;
  ProllyMutMap *pPending;
};

typedef struct SavepointCatalogEntry SavepointCatalogEntry;
struct SavepointCatalogEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
  u8 tableRootKnown;
  u8 isTableRoot;
  char *zName;
};

typedef struct SavepointPendingSnapshot SavepointPendingSnapshot;
struct SavepointPendingSnapshot {
  Pgno iTable;
  ProllyMutMap *pPending;
};

struct BtShared {
  ChunkStore store;
  ProllyCache cache;
  PagerShim *pPagerShim;
  sqlite3 *db;
  BtCursor *pCursor;
  u16 btsFlags;
  u32 pageSize;
  u32 iWorkingStateVersion;
  int nRef;
  u8 inCatalogSerialize;
};

struct BtreeOps {
  int (*xClose)(Btree*);
  int (*xNewDb)(Btree*);
  int (*xSetCacheSize)(Btree*, int);
  int (*xSetSpillSize)(Btree*, int);
  int (*xSetMmapLimit)(Btree*, sqlite3_int64);
  int (*xSetPagerFlags)(Btree*, unsigned);
  int (*xSetPageSize)(Btree*, int, int, int);
  int (*xGetPageSize)(Btree*);
  Pgno (*xMaxPageCount)(Btree*, Pgno);
  Pgno (*xLastPage)(Btree*);
  int (*xSecureDelete)(Btree*, int);
  int (*xGetRequestedReserve)(Btree*);
  int (*xGetReserveNoMutex)(Btree*);
  int (*xSetAutoVacuum)(Btree*, int);
  int (*xGetAutoVacuum)(Btree*);
  int (*xIncrVacuum)(Btree*);
  const char *(*xGetFilename)(Btree*);
  const char *(*xGetJournalname)(Btree*);
  int (*xIsReadonly)(Btree*);
  int (*xBeginTrans)(Btree*, int, int*);
  int (*xCommitPhaseOne)(Btree*, const char*);
  int (*xCommitPhaseTwo)(Btree*, int);
  int (*xCommit)(Btree*);
  int (*xRollback)(Btree*, int, int);
  int (*xBeginStmt)(Btree*, int);
  int (*xSavepoint)(Btree*, int, int);
  int (*xTxnState)(Btree*);
  int (*xCreateTable)(Btree*, Pgno*, int);
  int (*xDropTable)(Btree*, int, int*);
  int (*xClearTable)(Btree*, int, i64*);
  void (*xGetMeta)(Btree*, int, u32*);
  int (*xUpdateMeta)(Btree*, int, u32);
  void *(*xSchema)(Btree*, int, void(*)(void*));
  int (*xSchemaLocked)(Btree*);
  int (*xLockTable)(Btree*, int, u8);
  int (*xCursor)(Btree*, Pgno, int, struct KeyInfo*, BtCursor*);
  void (*xEnter)(Btree*);
  void (*xLeave)(Btree*);
  struct Pager *(*xPager)(Btree*);
#ifdef SQLITE_DEBUG
  int (*xClosesWithCursor)(Btree*, BtCursor*);
#endif
};

struct BtCursorOps {
  int (*xClearTableOfCursor)(BtCursor*);
  int (*xCloseCursor)(BtCursor*);
  int (*xCursorHasMoved)(BtCursor*);
  int (*xCursorRestore)(BtCursor*, int*);
  int (*xFirst)(BtCursor*, int*);
  int (*xLast)(BtCursor*, int*);
  int (*xNext)(BtCursor*, int);
  int (*xPrevious)(BtCursor*, int);
  int (*xEof)(BtCursor*);
  int (*xIsEmpty)(BtCursor*, int*);
  int (*xTableMoveto)(BtCursor*, i64, int, int*);
  int (*xIndexMoveto)(BtCursor*, UnpackedRecord*, int*);
  i64 (*xIntegerKey)(BtCursor*);
  u32 (*xPayloadSize)(BtCursor*);
  int (*xPayload)(BtCursor*, u32, u32, void*);
  const void *(*xPayloadFetch)(BtCursor*, u32*);
  sqlite3_int64 (*xMaxRecordSize)(BtCursor*);
  i64 (*xOffset)(BtCursor*);
  int (*xInsert)(BtCursor*, const BtreePayload*, int, int);
  int (*xDelete)(BtCursor*, u8);
  int (*xTransferRow)(BtCursor*, BtCursor*, i64);
  void (*xClearCursor)(BtCursor*);
  int (*xCount)(sqlite3*, BtCursor*, i64*);
  int (*xCountRange)(sqlite3*, BtCursor*, i64, i64, i64*);
  int (*xCountIndexRange)(sqlite3*, BtCursor*, UnpackedRecord*,
                          UnpackedRecord*, i64*);
  i64 (*xRowCountEst)(BtCursor*);
  void (*xCursorPin)(BtCursor*);
  void (*xCursorUnpin)(BtCursor*);
  void (*xCursorHintFlags)(BtCursor*, unsigned);
  int (*xCursorHasHint)(BtCursor*, unsigned int);
#ifndef SQLITE_OMIT_INCRBLOB
  int (*xPayloadChecked)(BtCursor*, u32, u32, void*);
  int (*xPutData)(BtCursor*, u32, u32, void*);
  void (*xIncrblobCursor)(BtCursor*);
#endif
#ifndef NDEBUG
  int (*xCursorIsValid)(BtCursor*);
#endif
  int (*xCursorIsValidNN)(BtCursor*);
};

struct Btree {
  sqlite3 *db;
  BtShared *pBt;
  u8 inTrans;
  u32 iBDataVersion;
  u32 iLoadedWorkingStateVersion;
  Btree *pNext;
  u64 nSeek;

  Catalog cat;

  u32 aMeta[16];

  void *pSchema;
  void (*xFreeSchema)(void*);

  u8 inTransaction;
  u8 bSchemaChangedTxn;
  u8 bMasterRootChangedTxn;
  u8 bFilterSchemaPlaceholders;
  u8 bCatalogDropped;     /* OOM rollback dropped the catalog; reload before
                          ** treating an empty committedCatalogHash as a
                          ** fresh database */
  Pgno mxPageCount;       /* Synthetic max_page_count limit */

  int nSavepoint;
  int nSavepointAlloc;

  struct SavepointTableState {
    SavepointTableEntry *aTables;
    SavepointCatalogEntry *aCatalogSnapshot;
    u8 bCatalogSnapshot;
    u8 bStatement;
    u8 bTablesCaptured;
    u8 bSchemaChangedTxn;
    u8 bMasterRootChangedTxn;

    SavepointPendingSnapshot *aPendingSnapshot;
    int nPendingSnapshot;
    int nPendingSnapshotAlloc;
    int nTables;
    Pgno iNextTable;
    Pgno iLargestRootPage;
    ProllyHash stagedCatalog;
    u8 isMerging;
    ProllyHash mergeCommitHash;
    ProllyHash conflictsCatalogHash;
    ProllyHash constraintViolationsHash;
    u8 isRebasing;
    ProllyHash preRebaseWorkingCat;
    ProllyHash rebaseOntoCommit;
    char *zRebaseOrigBranch;
    char *zRebaseReturnBranch;
    u32 aMeta[16];
  } *aSavepointTables;

  ProllyHash committedCatalogHash;
  ProllyHash committedStagedCatalog;
  u8 committedIsMerging;
  ProllyHash committedMergeCommitHash;
  ProllyHash committedConflictsCatalogHash;
  ProllyHash committedConstraintViolationsHash;
  u32 committedAMeta[16];
  ProllyHash catalogCacheHash;
  u8 *pCatalogCache;
  int nCatalogCache;

  char *zBranch;
  char *zAuthorName;
  char *zAuthorEmail;
  ProllyHash headCommit;
  ProllyHash stagedCatalog;
  u8 isMerging;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;

  u8 isRebasing;
  ProllyHash preRebaseWorkingCat;
  ProllyHash rebaseOntoCommit;
  char *zRebaseOrigBranch;
  char *zRebaseReturnBranch;

  ProllyHash constraintViolationsHash;
  /* Transient: a constraint-violation batch open across a merge detection
  ** pass, so appends accumulate in memory and persist once. Owned by
  ** doltlite_constraint_violations.c. */
  void *pCvBatch;

  const struct BtreeOps *pOps;
  void *pOrigBtree;
};

struct BtCursor {
  u8 eState;
  u8 curFlags;
  u8 curPagerFlags;
  u8 hints;
  int skipNext;
  Btree *pBtree;
  BtShared *pBt;
  BtCursor *pNext;
  Pgno pgnoRoot;
  u8 curIntKey;
  struct KeyInfo *pKeyInfo;

  ProllyCursor pCur;
  ProllyMutMap *pMutMap;

  u8 *pCachedPayload;
  int nCachedPayload;
  u8 cachedPayloadOwned;
  ProllyCacheEntry *pCachedFrom;
  u8 *pReconPayload;
  int nReconPayloadAlloc;
  u8 *pSeekRecord;
  int nSeekRecordAlloc;
  u8 *pSeekSortKey;
  int nSeekSortKeyAlloc;
  int nSeekSortKey;
  int nSeekKeyField;
  u8 *pCompareSortKey;
  int nCompareSortKeyAlloc;
  int nCompareSortKey;
  int nCompareKeyField;
  u8 *pMovetoRec;
  int nMovetoRecAlloc;
  i64 cachedIntKey;

  u8 isTableRoot;
  u8 isPinned;
  u8 flushSeekEdits;

  int mmIdx;
  int mmPhysIdx;
  u8 mmActive;
  u8 mmPhysActive;
  u8 deferredTreeSeek;
  u8 deferredMergedSeek;
#define MERGE_SRC_TREE  0
#define MERGE_SRC_MUT   1
#define MERGE_SRC_BOTH  2
  u8 mergeSrc;

  i64 nKey;
  void *pKey;
  u64 nSeek;
  void *pOrigCursor;
  const struct BtCursorOps *pCurOps;
};

struct TableEntry *catFind(Catalog *cat, Pgno iTable);
struct TableEntry *catAdd(Catalog *cat, Pgno iTable, u8 flags);
void catRemove(Catalog *cat, Pgno iTable);
void catFree(Catalog *cat);
int btreeLoadBranchHeadCatalog(ChunkStore *cs, const char *zBranch,
                                      ProllyHash *pCatHash,
                                      ProllyHash *pHeadCommit);

static inline struct TableEntry *findTable(Btree *p, Pgno iTable){
  return catFind(&p->cat, iTable);
}
static inline struct TableEntry *addTable(Btree *p, Pgno iTable, u8 flags){
  return catAdd(&p->cat, iTable, flags);
}
static inline void removeTable(Btree *p, Pgno iTable){
  catRemove(&p->cat, iTable);
}
static inline void btreeFreeCatalogTables(Btree *p){
  catFree(&p->cat);
}

static int btreeSchemaIndex(Btree *pBtree){
  sqlite3 *db;
  int i;
  if( !pBtree || !(db = pBtree->db) ) return -1;
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].pBt==pBtree ) return i;
  }
  return -1;
}

static const char *btreeSchemaName(Btree *pBtree){
  sqlite3 *db = pBtree ? pBtree->db : 0;
  int iDb = btreeSchemaIndex(pBtree);
  if( !db || iDb<0 || iDb>=db->nDb ) return "main";
  return db->aDb[iDb].zDbSName ? db->aDb[iDb].zDbSName : "main";
}
static void btreeClearCatalogCache(Btree *p){
  sqlite3_free(p->pCatalogCache);
  p->pCatalogCache = 0;
  p->nCatalogCache = 0;
  memset(&p->catalogCacheHash, 0, sizeof(p->catalogCacheHash));
}
static void btreeTakeCatalogCache(Btree *p, u8 **ppData, int nData,
                                  const ProllyHash *pHash){
  sqlite3_free(p->pCatalogCache);
  p->pCatalogCache = *ppData;
  *ppData = 0;
  p->nCatalogCache = nData;
  if( pHash ){
    p->catalogCacheHash = *pHash;
  }else{
    memset(&p->catalogCacheHash, 0, sizeof(p->catalogCacheHash));
  }
}
void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode);
void invalidateSchema(Btree *pBtree);
int flushMutMap(BtCursor *pCur);
void refreshCursorMutMapAliases(Btree *pBtree, BtShared *pBt,
                                        Pgno iTable, ProllyMutMap *pNewMap);
int getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData);
int flushIfNeeded(BtCursor *pCur);
int flushAllPending(Btree *pBtree, BtShared *pBt, Pgno iTable);
int applyMutMapToTableRoot(BtShared *pBt, struct TableEntry *pTE, ProllyMutMap *pMap);
int flushPendingForTable(Btree *pBtree, BtShared *pBt,
                                struct TableEntry *pTE, int clearInPlace);
int cacheCursorPayloadCopy(BtCursor *pCur, const u8 *pData, int nData);
int serializeUnpackedRecordBuffer(
  UnpackedRecord *pRec, u8 **ppBuf, int *pnAlloc, int *pnOut
);
u32 btreeSerialType(Mem *pMem, u32 *pLen);
int mutMapShouldDrain(BtCursor *pCur);
int prollyBtreeCheckMaxPageCount(Btree *p);
int cacheCursorPayloadZeroTail(BtCursor *pCur, const u8 *pData,
                               int nData, i64 nZeroTail);
u32 prollySerialTypeLen(u32 serialType);
ProllyMutMapEntry *currentMutMapEntry(BtCursor *pCur);
void setCursorToMutMapEntryPhys(BtCursor *pCur, int physIdx);
const char *findTableNumberName(sqlite3 *db, Pgno iTable);
int cachedSeekKeyMatchesCurrent(BtCursor *pCur);
int copyZeroTailPayload(ProllyMutMapEntry *e, u32 offset, u32 amt, void *pBuf);
void btreeMarkWorkingStateChanged(Btree *p, int bLocal);
int restoreFromCommitted(Btree *p);
int rollbackNeedsSchemaReset(Btree *pBtree);
void btreeFillWorkingSetBlob(
  u8 *buf,
  const ProllyHash *pWorkingCat,
  const ProllyHash *pWorkingCommit,
  const ProllyHash *pStaged,
  u8 isMerging,
  const ProllyHash *pMergeCommit,
  const ProllyHash *pConflicts,
  u8 isRebasing,
  const ProllyHash *pPreRebaseCat,
  const ProllyHash *pRebaseOnto,
  const char *zRebaseOrigBranch,
  const char *zRebaseReturnBranch,
  const ProllyHash *pConstraintViolations
);

static int keyInfoHasLossyCollation(const KeyInfo *pKeyInfo){
  int i;
  if( !pKeyInfo ) return 0;
  for(i=0; i<pKeyInfo->nAllField; i++){
    const CollSeq *pColl = pKeyInfo->aColl[i];
    if( !pColl || !pColl->zName ) continue;
    if( sqlite3StrICmp(pColl->zName, "NOCASE")==0
     || sqlite3StrICmp(pColl->zName, "RTRIM")==0 ){
      return 1;
    }
  }
  return 0;
}

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

static SQLITE_INLINE void cursorCurrentTreeValue(
  BtCursor *pCur,
  const u8 **ppData,
  int *pnData
){
  ProllyCursor *pProllyCur = &pCur->pCur;
  ProllyCacheEntry *pLeaf = pProllyCur->aLevel[pProllyCur->iLevel].pEntry;
  ProllyNode *pNode = &pLeaf->node;
  int i = pProllyCur->aLevel[pProllyCur->iLevel].idx;
  u32 off0 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i]);
  u32 off1 = PROLLY_GET_U32((const u8*)&pNode->aValOff[i+1]);
  *ppData = pNode->pValData + off0;
  *pnData = (int)(off1 - off0);
}

static SQLITE_INLINE void cursorCurrentTreeValueSpan(
  BtCursor *pCur,
  const u8 **ppData,
  int *pnData,
  int *pnAvail
){
  ProllyCursor *pProllyCur = &pCur->pCur;
  ProllyCacheEntry *pLeaf = pProllyCur->aLevel[pProllyCur->iLevel].pEntry;
  ProllyNode *pNode = &pLeaf->node;
  int i = pProllyCur->aLevel[pProllyCur->iLevel].idx;
  prollyNodeValueSpan(pNode, i, ppData, pnData, pnAvail);
}

static int cursorCurrentTreeValueCopy(
  BtCursor *pCur,
  u32 offset,
  u32 amt,
  void *pBuf
){
  const u8 *pData;
  int nData;
  int nAvail;
  cursorCurrentTreeValueSpan(pCur, &pData, &nData, &nAvail);
  if( (i64)offset + (i64)amt > (i64)nData ){
    return SQLITE_CORRUPT_BKPT;
  }
  if( amt>0 ){
    u32 nPrefix = offset < (u32)nAvail ? (u32)nAvail - offset : 0;
    if( nPrefix>amt ) nPrefix = amt;
    if( nPrefix>0 ) memcpy(pBuf, pData + offset, nPrefix);
    if( nPrefix<amt ) memset((u8*)pBuf + nPrefix, 0, amt - nPrefix);
  }
  return SQLITE_OK;
}

static SQLITE_INLINE u64 cursorCurrentTreeKeyPrefixInt(BtCursor *pCur){
  ProllyCursor *pProllyCur = &pCur->pCur;
  ProllyCacheEntry *pLeaf = pProllyCur->aLevel[pProllyCur->iLevel].pEntry;
  ProllyNode *pNode = &pLeaf->node;
  int i = pProllyCur->aLevel[pProllyCur->iLevel].idx;
  const u8 *p = pNode->pKeyData + i*8;
  assert( (pNode->flags & PROLLY_NODE_INTKEY)!=0 );
  return ((u64)p[0]<<56) | ((u64)p[1]<<48) | ((u64)p[2]<<40)
       | ((u64)p[3]<<32) | ((u64)p[4]<<24) | ((u64)p[5]<<16)
       | ((u64)p[6]<<8) | (u64)p[7];
}

static SQLITE_INLINE i64 cursorCurrentTreeIntKey(BtCursor *pCur){
  u64 u = cursorCurrentTreeKeyPrefixInt(pCur);
  return (i64)(u ^ ((u64)1 << 63));
}

int cacheCursorPayloadReconstructed(
  BtCursor *pCur, const u8 *pSortKey, int nSortKey
);
int prollyBtCursorNext(BtCursor *pCur, int flags);
int prollyBtCursorPrevious(BtCursor *pCur, int flags);

static SQLITE_INLINE void cacheCurrentTreePayloadIfIntKey(BtCursor *pCur){
  if( pCur->curIntKey ){
    const u8 *pVal; int nVal; int nAvail;
    CLEAR_CACHED_PAYLOAD(pCur);
    cursorCurrentTreeValueSpan(pCur, &pVal, &nVal, &nAvail);
    if( nVal > 0 && nAvail==nVal ){
      pCur->pCachedPayload = (u8*)pVal;
      pCur->nCachedPayload = nVal;
      pCur->cachedPayloadOwned = 0;
    }
  }
}

static int isDegenerateSchemaPayload(const u8 *pVal, int nVal){
  return nVal==6
      && pVal
      && pVal[0]==6
      && pVal[1]==0
      && pVal[2]==0
      && pVal[3]==0
      && pVal[4]==0
      && pVal[5]==0;
}

static int cursorOnDegenerateSchemaRow(BtCursor *pCur, int *pIsDegenerate){
  const u8 *pVal;
  int nVal;
  int rc;
  *pIsDegenerate = 0;
  if( pCur->pgnoRoot!=1 || !pCur->curIntKey ) return SQLITE_OK;
  if( pCur->eState!=CURSOR_VALID ) return SQLITE_OK;
  rc = getCursorPayload(pCur, &pVal, &nVal);
  if( rc!=SQLITE_OK ) return rc;
  if( !pVal || nVal<=0 ){
    *pIsDegenerate = 1;
    return SQLITE_OK;
  }
  if( pCur->pBtree->bFilterSchemaPlaceholders
   && isDegenerateSchemaPayload(pVal, nVal) ){
    *pIsDegenerate = 1;
  }
  return SQLITE_OK;
}

static int skipDegenerateSchemaRows(BtCursor *pCur, int dir, int *pRes){
  int rc = SQLITE_OK;
  int isDegenerate = 0;
  if( pRes ) *pRes = 0;
  while( pCur->eState==CURSOR_VALID ){
    rc = cursorOnDegenerateSchemaRow(pCur, &isDegenerate);
    if( rc!=SQLITE_OK || !isDegenerate ) break;
    CLEAR_CACHED_PAYLOAD(pCur);
    rc = dir>=0 ? prollyBtCursorNext(pCur, 0) : prollyBtCursorPrevious(pCur, 0);
    if( rc!=SQLITE_OK && rc!=SQLITE_DONE ) break;
    if( pCur->eState==CURSOR_INVALID ){
      if( pRes ) *pRes = 1;
      rc = SQLITE_OK;
      break;
    }
  }
  return rc;
}

static void cacheCurrentTreeStoredPayloadNonIntKey(BtCursor *pCur){
  const u8 *pVal; int nVal;
  CLEAR_CACHED_PAYLOAD(pCur);
  cursorCurrentTreeValue(pCur, &pVal, &nVal);
  if( nVal > 0 ){
    pCur->pCachedPayload = (u8*)pVal;
    pCur->nCachedPayload = nVal;
    pCur->cachedPayloadOwned = 0;
  }
}

static SQLITE_INLINE int prollyCursorNextFastLeaf(ProllyCursor *pCur){
  ProllyCursorLevel *pLevel = &pCur->aLevel[pCur->iLevel];
  ProllyCacheEntry *pLeaf = pLevel->pEntry;
  assert( pCur->eState==PROLLY_CURSOR_VALID );
  if( pLevel->idx < pLeaf->node.nItems - 1 ){
    pLevel->idx++;
    return SQLITE_OK;
  }
  return prollyCursorNext(pCur);
}
int prollyBtreeClose(Btree*);
int prollyBtreeNewDb(Btree*);
int prollyBtreeSetCacheSize(Btree*, int);
int prollyBtreeSetSpillSize(Btree*, int);
int prollyBtreeSetMmapLimit(Btree*, sqlite3_int64);
int prollyBtreeSetPagerFlags(Btree*, unsigned);
int prollyBtreeSetPageSize(Btree*, int, int, int);
int prollyBtreeGetPageSize(Btree*);
Pgno prollyBtreeMaxPageCount(Btree*, Pgno);
Pgno prollyBtreeLastPage(Btree*);
int prollyBtreeSecureDelete(Btree*, int);
int prollyBtreeGetRequestedReserve(Btree*);
int prollyBtreeGetReserveNoMutex(Btree*);
int prollyBtreeSetAutoVacuum(Btree*, int);
int prollyBtreeGetAutoVacuum(Btree*);
int prollyBtreeIncrVacuum(Btree*);
const char *prollyBtreeGetFilename(Btree*);
const char *prollyBtreeGetJournalname(Btree*);
int prollyBtreeIsReadonly(Btree*);
int prollyBtreeBeginTrans(Btree*, int, int*);
int prollyBtreeCommitPhaseOne(Btree*, const char*);
int prollyBtreeCommitPhaseTwo(Btree*, int);
int prollyBtreeCommit(Btree*);
int prollyBtreeRollback(Btree*, int, int);
int prollyBtreeBeginStmt(Btree*, int);
int prollyBtreeSavepoint(Btree*, int, int);
int prollyBtreeTxnState(Btree*);
int prollyBtreeCreateTable(Btree*, Pgno*, int);
int prollyBtreeDropTable(Btree*, int, int*);
int prollyBtreeClearTable(Btree*, int, i64*);
void prollyBtreeGetMeta(Btree*, int, u32*);
int prollyBtreeUpdateMeta(Btree*, int, u32);
void *prollyBtreeSchema(Btree*, int, void(*)(void*));
int prollyBtreeSchemaLocked(Btree*);
int prollyBtreeLockTable(Btree*, int, u8);
int prollyBtreeCursor(Btree*, Pgno, int, struct KeyInfo*, BtCursor*);
void prollyBtreeEnter(Btree*);
void prollyBtreeLeave(Btree*);
struct Pager *prollyBtreePager(Btree*);
#ifdef SQLITE_DEBUG
int prollyBtreeClosesWithCursor(Btree*, BtCursor*);
#endif
int doltliteEnsureWriteTxnAndSavepoints(sqlite3 *db);
int origBtreeCloseVt(Btree*);
int origBtreeNewDbVt(Btree*);
int origBtreeSetCacheSizeVt(Btree*, int);
int origBtreeSetSpillSizeVt(Btree*, int);
int origBtreeSetMmapLimitVt(Btree*, sqlite3_int64);
int origBtreeSetPagerFlagsVt(Btree*, unsigned);
int origBtreeSetPageSizeVt(Btree*, int, int, int);
int origBtreeGetPageSizeVt(Btree*);
Pgno origBtreeMaxPageCountVt(Btree*, Pgno);
Pgno origBtreeLastPageVt(Btree*);
int origBtreeSecureDeleteVt(Btree*, int);
int origBtreeGetRequestedReserveVt(Btree*);
int origBtreeGetReserveNoMutexVt(Btree*);
int origBtreeSetAutoVacuumVt(Btree*, int);
int origBtreeGetAutoVacuumVt(Btree*);
int origBtreeIncrVacuumVt(Btree*);
const char *origBtreeGetFilenameVt(Btree*);
const char *origBtreeGetJournalnameVt(Btree*);
int origBtreeIsReadonlyVt(Btree*);
int origBtreeBeginTransVt(Btree*, int, int*);
int origBtreeCommitPhaseOneVt(Btree*, const char*);
int origBtreeCommitPhaseTwoVt(Btree*, int);
int origBtreeCommitVt(Btree*);
int origBtreeRollbackVt(Btree*, int, int);
int origBtreeBeginStmtVt(Btree*, int);
int origBtreeSavepointVt(Btree*, int, int);
int origBtreeTxnStateVt(Btree*);
int origBtreeCreateTableVt(Btree*, Pgno*, int);
int origBtreeDropTableVt(Btree*, int, int*);
int origBtreeClearTableVt(Btree*, int, i64*);
void origBtreeGetMetaVt(Btree*, int, u32*);
int origBtreeUpdateMetaVt(Btree*, int, u32);
void *origBtreeSchemaVt(Btree*, int, void(*)(void*));
int origBtreeSchemaLockedVt(Btree*);
int origBtreeLockTableVt(Btree*, int, u8);
int origBtreeCursorVt(Btree*, Pgno, int, struct KeyInfo*, BtCursor*);
void origBtreeEnterVt(Btree*);
void origBtreeLeaveVt(Btree*);
struct Pager *origBtreePagerVt(Btree*);
#ifdef SQLITE_DEBUG
int origBtreeClosesWithCursorVt(Btree*, BtCursor*);
#endif

int prollyBtCursorClearTableOfCursor(BtCursor*);
int prollyBtCursorCloseCursor(BtCursor*);
int prollyBtCursorCursorHasMoved(BtCursor*);
int prollyBtCursorCursorRestore(BtCursor*, int*);
int prollyBtCursorFirst(BtCursor*, int*);
int prollyBtCursorLast(BtCursor*, int*);
int prollyBtCursorNext(BtCursor*, int);
int prollyBtCursorPrevious(BtCursor*, int);
int prollyBtCursorEof(BtCursor*);
int prollyBtCursorIsEmpty(BtCursor*, int*);
int prollyBtCursorTableMoveto(BtCursor*, i64, int, int*);
int prollyBtCursorIndexMoveto(BtCursor*, UnpackedRecord*, int*);
i64 prollyBtCursorIntegerKey(BtCursor*);
u32 prollyBtCursorPayloadSize(BtCursor*);
int prollyBtCursorPayload(BtCursor*, u32, u32, void*);
const void *prollyBtCursorPayloadFetch(BtCursor*, u32*);
sqlite3_int64 prollyBtCursorMaxRecordSize(BtCursor*);
i64 prollyBtCursorOffset(BtCursor*);
int prollyBtCursorInsert(BtCursor*, const BtreePayload*, int, int);
int prollyBtCursorDelete(BtCursor*, u8);
int prollyBtCursorTransferRow(BtCursor*, BtCursor*, i64);
void prollyBtCursorClearCursor(BtCursor*);
int prollyBtCursorCount(sqlite3*, BtCursor*, i64*);
int prollyBtCursorCountRange(sqlite3*, BtCursor*, i64, i64, i64*);
int prollyBtCursorCountIndexRange(sqlite3*, BtCursor*, UnpackedRecord*,
                                         UnpackedRecord*, i64*);
i64 prollyBtCursorRowCountEst(BtCursor*);
void prollyBtCursorCursorPin(BtCursor*);
void prollyBtCursorCursorUnpin(BtCursor*);
void prollyBtCursorCursorHintFlags(BtCursor*, unsigned);
int prollyBtCursorCursorHasHint(BtCursor*, unsigned int);
#ifndef SQLITE_OMIT_INCRBLOB
int prollyBtCursorPayloadChecked(BtCursor*, u32, u32, void*);
int prollyBtCursorPutData(BtCursor*, u32, u32, void*);
void prollyBtCursorIncrblobCursor(BtCursor*);
#endif
#ifndef NDEBUG
int prollyBtCursorCursorIsValid(BtCursor*);
#endif
int prollyBtCursorCursorIsValidNN(BtCursor*);

int origCursorClearTableOfCursorVt(BtCursor*);
int origCursorCloseCursorVt(BtCursor*);
int origCursorCursorHasMovedVt(BtCursor*);
int origCursorCursorRestoreVt(BtCursor*, int*);
int origCursorFirstVt(BtCursor*, int*);
int origCursorLastVt(BtCursor*, int*);
int origCursorNextVt(BtCursor*, int);
int origCursorPreviousVt(BtCursor*, int);
int origCursorEofVt(BtCursor*);
int origCursorIsEmptyVt(BtCursor*, int*);
int origCursorTableMovetoVt(BtCursor*, i64, int, int*);
int origCursorIndexMovetoVt(BtCursor*, UnpackedRecord*, int*);
i64 origCursorIntegerKeyVt(BtCursor*);
u32 origCursorPayloadSizeVt(BtCursor*);
int origCursorPayloadVt(BtCursor*, u32, u32, void*);
const void *origCursorPayloadFetchVt(BtCursor*, u32*);
sqlite3_int64 origCursorMaxRecordSizeVt(BtCursor*);
i64 origCursorOffsetVt(BtCursor*);
int origCursorInsertVt(BtCursor*, const BtreePayload*, int, int);
int origCursorDeleteVt(BtCursor*, u8);
int origCursorTransferRowVt(BtCursor*, BtCursor*, i64);
void origCursorClearCursorVt(BtCursor*);
int origCursorCountVt(sqlite3*, BtCursor*, i64*);
int origCursorCountRangeVt(sqlite3*, BtCursor*, i64, i64, i64*);
int origCursorCountIndexRangeVt(sqlite3*, BtCursor*, UnpackedRecord*,
                                       UnpackedRecord*, i64*);
i64 origCursorRowCountEstVt(BtCursor*);
void origCursorCursorPinVt(BtCursor*);
void origCursorCursorUnpinVt(BtCursor*);
void origCursorCursorHintFlagsVt(BtCursor*, unsigned);
int origCursorCursorHasHintVt(BtCursor*, unsigned int);
#ifndef SQLITE_OMIT_INCRBLOB
int origCursorPayloadCheckedVt(BtCursor*, u32, u32, void*);
int origCursorPutDataVt(BtCursor*, u32, u32, void*);
void origCursorIncrblobCursorVt(BtCursor*);
#endif
#ifndef NDEBUG
int origCursorCursorIsValidVt(BtCursor*);
#endif
int origCursorCursorIsValidNNVt(BtCursor*);

extern const struct BtreeOps prollyBtreeOps;
extern const struct BtreeOps origBtreeVtOps;
extern const struct BtCursorOps prollyCursorOps;
extern const struct BtCursorOps origCursorVtOps;

int btreeLoadBranchHeadCatalog(ChunkStore*, const char*, ProllyHash*, ProllyHash*);
int btreeLoadWorkingSetBlob(ChunkStore*, const char*, ProllyHash*, ProllyHash*,
                            ProllyHash*, u8*, ProllyHash*, ProllyHash*, u8*,
                            ProllyHash*, ProllyHash*, char**, char**, ProllyHash*);
int btreeStoreWorkingSetBlob(ChunkStore*, const char*, const ProllyHash*,
                             const ProllyHash*, const ProllyHash*, u8,
                             const ProllyHash*, const ProllyHash*, u8,
                             const ProllyHash*, const ProllyHash*, const char*,
                             const char*, const ProllyHash*);
int btreeReadWorkingCatalog(ChunkStore*, const char*, ProllyHash*, ProllyHash*);
int btreeWriteWorkingState(ChunkStore*, const char*, const ProllyHash*,
                           const ProllyHash*);
void btreeBumpLocalDataVersion(Btree*);
int btreeRefreshFromDisk(Btree*);
int btreeRefreshSharedWorkingState(Btree*);
int btreeReloadBranchWorkingStateInto(Btree*, int, ProllyHash*);
void btreeStoreCommittedFromCurrent(Btree*, const ProllyHash*);
int syncBtreeSavepoints(Btree*);
int ensureStatementSavepointsCaptured(Btree*);
int pushSavepoint(Btree*, int);
void freeSavepointTables(struct SavepointTableState*);
int snapshotPendingForFlush(Btree*, Pgno, ProllyMutMap**, ProllyMutMap**, int*);
void btreeDiscardAllSavepoints(Btree*);
int serializeCatalog(Btree*, u8**, int*);
int serializeCatalogForCommit(Btree*, u8**, int*);
int deserializeCatalog(Btree*, const u8*, int);
void initDefaultMeta(Btree*);
void resetConnectionSchema(Btree*);
void prollyInvalidateIncrblobCursors(BtShared*, Pgno, i64, int);
void refreshCursorRoot(BtCursor*);
int applyMutMapToTableRoot(BtShared*, struct TableEntry*, ProllyMutMap*);
int flushAllPending(Btree*, BtShared*, Pgno);
int flushDeferredEdits(Btree*, BtShared*);
int saveAllCursors(Btree*, BtShared*, Pgno, BtCursor*);
int saveCursorPosition(BtCursor*);
int restoreCursorPosition(BtCursor*, int*);
int countTreeEntries(Btree*, Pgno, i64*);
int tableEntryIsTableRoot(Btree*, struct TableEntry*, int*);
void clearMergeCursorState(BtCursor*);
int prollyCursorCheckInterrupt(BtCursor*);
int advanceTreeCursor(BtCursor*, int);
ProllyMutMapEntry *orderedMutMapEntryAt(ProllyMutMap*, int);
int unpackedRecordCanUseIntSortKey(BtCursor*, UnpackedRecord*, int);
int sortKeyFromUnpackedIntRecordBuffer(UnpackedRecord*, int, u8**, int*, int*);
int prollyInvokeBusyHandler(BtShared*);
ChunkStore *doltliteGetChunkStore(sqlite3*);
BtShared *doltliteGetBtShared(sqlite3*);
ProllyCache *doltliteGetCache(sqlite3*);
int doltliteRegister(sqlite3*);

#endif /* PROLLY_BTREE_INT_H */
