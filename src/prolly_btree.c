
#ifdef DOLTLITE_PROLLY

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

static void registerDoltiteFunctions(sqlite3 *db);
void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
char *doltliteCanonicalizeSchemaSql(const char *zSql, const char *zName);
int doltliteLoadLiveSchemaSql(sqlite3 *db, const char *zType,
                              const char *zName, const char *zTblName,
                              char **pzSql);
int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);
struct TableEntry;
typedef struct SchemaEntry SchemaEntry;
struct SchemaEntry {
  char *zName;
  char *zTblName;
  char *zSql;
  char *zType;
  Pgno iRootpage;
};
int doltliteSerializeCatalogEntriesWithFallbackSchema(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  u8 **ppOut,
  int *pnOut
);

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

#define BTS_READ_ONLY       0x0001
#define BTS_INITIALLY_EMPTY 0x0010

#define CLEAR_CACHED_PAYLOAD(pCur) do{ \
  if( (pCur)->cachedPayloadOwned && (pCur)->pCachedPayload ){ \
    sqlite3_free((pCur)->pCachedPayload); \
  } \
  (pCur)->pCachedPayload = 0; \
  (pCur)->nCachedPayload = 0; \
  (pCur)->cachedPayloadOwned = 0; \
}while(0)

#define CLEAR_CACHED_SEEK_KEY(pCur) do{ \
  (pCur)->nSeekSortKey = 0; \
  (pCur)->nSeekKeyField = 0; \
}while(0)

#define PROLLY_DEFAULT_CACHE_SIZE 1024

#define PROLLY_DEFAULT_PAGE_SIZE 4096

#define PROLLY_MAX_RECORD_SIZE ((sqlite3_int64)(1024*1024*1024))

static const ProllyHash emptyHash = {{0}};

struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
  char *zName;
  ProllyMutMap *pPending;
};

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
};

typedef struct SavepointCatalogEntry SavepointCatalogEntry;
struct SavepointCatalogEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
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

  int nSavepoint;
  int nSavepointAlloc;

  struct SavepointTableState {
    ProllyHash catalogHash;
    SavepointTableEntry *aTables;
    SavepointCatalogEntry *aCatalogSnapshot;
    u8 bCatalogSnapshot;

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
  } *aSavepointTables;

  ProllyHash committedCatalogHash;
  ProllyHash committedStagedCatalog;
  u8 committedIsMerging;
  ProllyHash committedMergeCommitHash;
  ProllyHash committedConflictsCatalogHash;
  ProllyHash committedConstraintViolationsHash;

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
  u8 *pReconPayload;
  int nReconPayloadAlloc;
  u8 *pSeekRecord;
  int nSeekRecordAlloc;
  u8 *pSeekSortKey;
  int nSeekSortKeyAlloc;
  int nSeekSortKey;
  int nSeekKeyField;
  u8 *pMovetoRec;
  int nMovetoRecAlloc;
  i64 cachedIntKey;

  u8 isPinned;
  u8 flushSeekEdits;

  int mmIdx;
  int mmPhysIdx;
  u8 mmActive;
  u8 mmPhysActive;
  u8 deferredTreeSeek;
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

static struct TableEntry *catFind(Catalog *cat, Pgno iTable);
static struct TableEntry *catAdd(Catalog *cat, Pgno iTable, u8 flags);
static void catRemove(Catalog *cat, Pgno iTable);
static void catFree(Catalog *cat);
static int btreeLoadBranchHeadCatalog(ChunkStore *cs, const char *zBranch,
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
static void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode);
static void invalidateSchema(Btree *pBtree);
static int flushMutMap(BtCursor *pCur);
static void refreshCursorMutMapAliases(Btree *pBtree, BtShared *pBt,
                                        Pgno iTable, ProllyMutMap *pNewMap);
static void getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData);
static int flushIfNeeded(BtCursor *pCur);
static int flushAllPending(Btree *pBtree, BtShared *pBt, Pgno iTable);
static int applyMutMapToTableRoot(BtShared *pBt, struct TableEntry *pTE, ProllyMutMap *pMap);
static int flushPendingForTable(Btree *pBtree, BtShared *pBt,
                                struct TableEntry *pTE, int clearInPlace);
static int cacheCursorPayloadCopy(BtCursor *pCur, const u8 *pData, int nData);
static int serializeUnpackedRecordBuffer(
  UnpackedRecord *pRec, u8 **ppBuf, int *pnAlloc, int *pnOut
);
static u32 btreeSerialType(Mem *pMem, u32 *pLen);

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

static SQLITE_INLINE u64 cursorCurrentTreeKeyPrefixInt(BtCursor *pCur){
  ProllyCursor *pProllyCur = &pCur->pCur;
  ProllyCacheEntry *pLeaf = pProllyCur->aLevel[pProllyCur->iLevel].pEntry;
  ProllyNode *pNode = &pLeaf->node;
  int i = pProllyCur->aLevel[pProllyCur->iLevel].idx;
  u32 off = PROLLY_GET_U32((const u8*)&pNode->aKeyOff[i]);
  const u8 *p = pNode->pKeyData + off;
  return ((u64)p[0]<<56) | ((u64)p[1]<<48) | ((u64)p[2]<<40)
       | ((u64)p[3]<<32) | ((u64)p[4]<<24) | ((u64)p[5]<<16)
       | ((u64)p[6]<<8) | (u64)p[7];
}

static int cacheCursorPayloadReconstructed(
  BtCursor *pCur, const u8 *pSortKey, int nSortKey
);

static SQLITE_INLINE void cacheCurrentTreePayloadIfIntKey(BtCursor *pCur){
  if( pCur->curIntKey ){
    const u8 *pVal; int nVal;
    cursorCurrentTreeValue(pCur, &pVal, &nVal);
    if( nVal > 0 ){
      pCur->pCachedPayload = (u8*)pVal;
      pCur->nCachedPayload = nVal;
      pCur->cachedPayloadOwned = 0;
    }
  }
}

static void cacheCurrentTreeStoredPayloadNonIntKey(BtCursor *pCur){
  const u8 *pVal; int nVal;
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
static int flushDeferredEdits(Btree *pBtree, BtShared *pBt);
static int ensureMutMap(BtCursor *pCur);
static int saveCursorPosition(BtCursor *pCur);
static int restoreCursorPosition(BtCursor *pCur, int *pDifferentRow);
static int pushSavepoint(Btree *pBtree, int bStatement);
static void btreeDiscardAllSavepoints(Btree *pBtree);
static int findTableIndexInArray(struct TableEntry *aTables, int nTables, Pgno iTable);
static int findSavepointTableIndexInArray(SavepointTableEntry *aTables, int nTables, Pgno iTable);
static int snapshotPendingForFlush(Btree *pBtree, Pgno iTable, ProllyMutMap **ppPending,
                                   ProllyMutMap **ppFlushMap, int *pCaptured);
static int restoreFromCommitted(Btree *p);
static void refreshCursorRoot(BtCursor *pCur);
static int countTreeEntries(Btree *pBtree, Pgno iTable, i64 *pCount);
static int saveAllCursors(Btree *pBtree, BtShared *pBt, Pgno iRoot,
                          BtCursor *pExcept);
static int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut);
static int deserializeCatalog(Btree *pBtree, const u8 *data, int nData);
static int tableEntryIsTableRoot(Btree *pBtree, struct TableEntry *pTE);
static int btreeRefreshFromDisk(Btree *p);
static int btreeReloadBranchWorkingState(Btree *p, int bLoadCatalog);
static int btreeReadWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                   ProllyHash *pCatHash,
                                   ProllyHash *pCommitHash);
static int btreeLoadWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pWorkingCat,
  ProllyHash *pWorkingCommit,
  ProllyHash *pStaged,
  u8 *pIsMerging,
  ProllyHash *pMergeCommit,
  ProllyHash *pConflicts,
  u8 *pIsRebasing,
  ProllyHash *pPreRebaseCat,
  ProllyHash *pRebaseOnto,
  char **pzRebaseOrigBranch,
  char **pzRebaseReturnBranch,
  ProllyHash *pConstraintViolations
);

static int btreeStoreWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
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
static int btreeWriteWorkingState(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pCatHash,
  const ProllyHash *pCommitHash
);
static int btreeDeleteImmediate(BtCursor *pCur);

#define PROLLY_MUTMAP_PENDING_FLUSH_LIMIT 65536

static int mutMapShouldDrain(BtCursor *pCur){
  return pCur && pCur->pMutMap
      && prollyMutMapCount(pCur->pMutMap) >= PROLLY_MUTMAP_PENDING_FLUSH_LIMIT;
}

static int prollyBtreeClose(Btree*);
static int prollyBtreeNewDb(Btree*);
static int prollyBtreeSetCacheSize(Btree*, int);
static int prollyBtreeSetSpillSize(Btree*, int);
static int prollyBtreeSetMmapLimit(Btree*, sqlite3_int64);
static int prollyBtreeSetPagerFlags(Btree*, unsigned);
static int prollyBtreeSetPageSize(Btree*, int, int, int);
static int prollyBtreeGetPageSize(Btree*);
static Pgno prollyBtreeMaxPageCount(Btree*, Pgno);
static Pgno prollyBtreeLastPage(Btree*);
static int prollyBtreeSecureDelete(Btree*, int);
static int prollyBtreeGetRequestedReserve(Btree*);
static int prollyBtreeGetReserveNoMutex(Btree*);
static int prollyBtreeSetAutoVacuum(Btree*, int);
static int prollyBtreeGetAutoVacuum(Btree*);
static int prollyBtreeIncrVacuum(Btree*);
static const char *prollyBtreeGetFilename(Btree*);
static const char *prollyBtreeGetJournalname(Btree*);
static int prollyBtreeIsReadonly(Btree*);
static int prollyBtreeBeginTrans(Btree*, int, int*);
static int prollyBtreeCommitPhaseOne(Btree*, const char*);
static int prollyBtreeCommitPhaseTwo(Btree*, int);
static int prollyBtreeCommit(Btree*);
static int prollyBtreeRollback(Btree*, int, int);
static int prollyBtreeBeginStmt(Btree*, int);
static int prollyBtreeSavepoint(Btree*, int, int);
static int prollyBtreeTxnState(Btree*);
static int prollyBtreeCreateTable(Btree*, Pgno*, int);
static int prollyBtreeDropTable(Btree*, int, int*);
static int prollyBtreeClearTable(Btree*, int, i64*);
static void prollyBtreeGetMeta(Btree*, int, u32*);
static int prollyBtreeUpdateMeta(Btree*, int, u32);
static void *prollyBtreeSchema(Btree*, int, void(*)(void*));
static int prollyBtreeSchemaLocked(Btree*);
static int prollyBtreeLockTable(Btree*, int, u8);
static int prollyBtreeCursor(Btree*, Pgno, int, struct KeyInfo*, BtCursor*);
static void prollyBtreeEnter(Btree*);
static void prollyBtreeLeave(Btree*);
static struct Pager *prollyBtreePager(Btree*);
#ifdef SQLITE_DEBUG
static int prollyBtreeClosesWithCursor(Btree*, BtCursor*);
#endif
int doltliteEnsureWriteTxnAndSavepoints(sqlite3 *db);

static int origBtreeCloseVt(Btree*);
static int origBtreeNewDbVt(Btree*);
static int origBtreeSetCacheSizeVt(Btree*, int);
static int origBtreeSetSpillSizeVt(Btree*, int);
static int origBtreeSetMmapLimitVt(Btree*, sqlite3_int64);
static int origBtreeSetPagerFlagsVt(Btree*, unsigned);
static int origBtreeSetPageSizeVt(Btree*, int, int, int);
static int origBtreeGetPageSizeVt(Btree*);
static Pgno origBtreeMaxPageCountVt(Btree*, Pgno);
static Pgno origBtreeLastPageVt(Btree*);
static int origBtreeSecureDeleteVt(Btree*, int);
static int origBtreeGetRequestedReserveVt(Btree*);
static int origBtreeGetReserveNoMutexVt(Btree*);
static int origBtreeSetAutoVacuumVt(Btree*, int);
static int origBtreeGetAutoVacuumVt(Btree*);
static int origBtreeIncrVacuumVt(Btree*);
static const char *origBtreeGetFilenameVt(Btree*);
static const char *origBtreeGetJournalnameVt(Btree*);
static int origBtreeIsReadonlyVt(Btree*);
static int origBtreeBeginTransVt(Btree*, int, int*);
static int origBtreeCommitPhaseOneVt(Btree*, const char*);
static int origBtreeCommitPhaseTwoVt(Btree*, int);
static int origBtreeCommitVt(Btree*);
static int origBtreeRollbackVt(Btree*, int, int);
static int origBtreeBeginStmtVt(Btree*, int);
static int origBtreeSavepointVt(Btree*, int, int);
static int origBtreeTxnStateVt(Btree*);
static int origBtreeCreateTableVt(Btree*, Pgno*, int);
static int origBtreeDropTableVt(Btree*, int, int*);
static int origBtreeClearTableVt(Btree*, int, i64*);
static void origBtreeGetMetaVt(Btree*, int, u32*);
static int origBtreeUpdateMetaVt(Btree*, int, u32);
static void *origBtreeSchemaVt(Btree*, int, void(*)(void*));
static int origBtreeSchemaLockedVt(Btree*);
static int origBtreeLockTableVt(Btree*, int, u8);
static int origBtreeCursorVt(Btree*, Pgno, int, struct KeyInfo*, BtCursor*);
static void origBtreeEnterVt(Btree*);
static void origBtreeLeaveVt(Btree*);
static struct Pager *origBtreePagerVt(Btree*);
#ifdef SQLITE_DEBUG
static int origBtreeClosesWithCursorVt(Btree*, BtCursor*);
#endif

static const struct BtreeOps prollyBtreeOps = {
  prollyBtreeClose,
  prollyBtreeNewDb,
  prollyBtreeSetCacheSize,
  prollyBtreeSetSpillSize,
  prollyBtreeSetMmapLimit,
  prollyBtreeSetPagerFlags,
  prollyBtreeSetPageSize,
  prollyBtreeGetPageSize,
  prollyBtreeMaxPageCount,
  prollyBtreeLastPage,
  prollyBtreeSecureDelete,
  prollyBtreeGetRequestedReserve,
  prollyBtreeGetReserveNoMutex,
  prollyBtreeSetAutoVacuum,
  prollyBtreeGetAutoVacuum,
  prollyBtreeIncrVacuum,
  prollyBtreeGetFilename,
  prollyBtreeGetJournalname,
  prollyBtreeIsReadonly,
  prollyBtreeBeginTrans,
  prollyBtreeCommitPhaseOne,
  prollyBtreeCommitPhaseTwo,
  prollyBtreeCommit,
  prollyBtreeRollback,
  prollyBtreeBeginStmt,
  prollyBtreeSavepoint,
  prollyBtreeTxnState,
  prollyBtreeCreateTable,
  prollyBtreeDropTable,
  prollyBtreeClearTable,
  prollyBtreeGetMeta,
  prollyBtreeUpdateMeta,
  prollyBtreeSchema,
  prollyBtreeSchemaLocked,
  prollyBtreeLockTable,
  prollyBtreeCursor,
  prollyBtreeEnter,
  prollyBtreeLeave,
  prollyBtreePager,
#ifdef SQLITE_DEBUG
  prollyBtreeClosesWithCursor,
#endif
};

static const struct BtreeOps origBtreeVtOps = {
  origBtreeCloseVt,
  origBtreeNewDbVt,
  origBtreeSetCacheSizeVt,
  origBtreeSetSpillSizeVt,
  origBtreeSetMmapLimitVt,
  origBtreeSetPagerFlagsVt,
  origBtreeSetPageSizeVt,
  origBtreeGetPageSizeVt,
  origBtreeMaxPageCountVt,
  origBtreeLastPageVt,
  origBtreeSecureDeleteVt,
  origBtreeGetRequestedReserveVt,
  origBtreeGetReserveNoMutexVt,
  origBtreeSetAutoVacuumVt,
  origBtreeGetAutoVacuumVt,
  origBtreeIncrVacuumVt,
  origBtreeGetFilenameVt,
  origBtreeGetJournalnameVt,
  origBtreeIsReadonlyVt,
  origBtreeBeginTransVt,
  origBtreeCommitPhaseOneVt,
  origBtreeCommitPhaseTwoVt,
  origBtreeCommitVt,
  origBtreeRollbackVt,
  origBtreeBeginStmtVt,
  origBtreeSavepointVt,
  origBtreeTxnStateVt,
  origBtreeCreateTableVt,
  origBtreeDropTableVt,
  origBtreeClearTableVt,
  origBtreeGetMetaVt,
  origBtreeUpdateMetaVt,
  origBtreeSchemaVt,
  origBtreeSchemaLockedVt,
  origBtreeLockTableVt,
  origBtreeCursorVt,
  origBtreeEnterVt,
  origBtreeLeaveVt,
  origBtreePagerVt,
#ifdef SQLITE_DEBUG
  origBtreeClosesWithCursorVt,
#endif
};

static int prollyBtCursorClearTableOfCursor(BtCursor*);
static int prollyBtCursorCloseCursor(BtCursor*);
static int prollyBtCursorCursorHasMoved(BtCursor*);
static int prollyBtCursorCursorRestore(BtCursor*, int*);
static int prollyBtCursorFirst(BtCursor*, int*);
static int prollyBtCursorLast(BtCursor*, int*);
static int prollyBtCursorNext(BtCursor*, int);
static int prollyBtCursorPrevious(BtCursor*, int);
static int prollyBtCursorEof(BtCursor*);
static int prollyBtCursorIsEmpty(BtCursor*, int*);
static int prollyBtCursorTableMoveto(BtCursor*, i64, int, int*);
static int prollyBtCursorIndexMoveto(BtCursor*, UnpackedRecord*, int*);
static i64 prollyBtCursorIntegerKey(BtCursor*);
static u32 prollyBtCursorPayloadSize(BtCursor*);
static int prollyBtCursorPayload(BtCursor*, u32, u32, void*);
static const void *prollyBtCursorPayloadFetch(BtCursor*, u32*);
static sqlite3_int64 prollyBtCursorMaxRecordSize(BtCursor*);
static i64 prollyBtCursorOffset(BtCursor*);
static int prollyBtCursorInsert(BtCursor*, const BtreePayload*, int, int);
static int prollyBtCursorDelete(BtCursor*, u8);
static int prollyBtCursorTransferRow(BtCursor*, BtCursor*, i64);
static void prollyBtCursorClearCursor(BtCursor*);
static int prollyBtCursorCount(sqlite3*, BtCursor*, i64*);
static i64 prollyBtCursorRowCountEst(BtCursor*);
static void prollyBtCursorCursorPin(BtCursor*);
static void prollyBtCursorCursorUnpin(BtCursor*);
static void prollyBtCursorCursorHintFlags(BtCursor*, unsigned);
static int prollyBtCursorCursorHasHint(BtCursor*, unsigned int);
#ifndef SQLITE_OMIT_INCRBLOB
static int prollyBtCursorPayloadChecked(BtCursor*, u32, u32, void*);
static int prollyBtCursorPutData(BtCursor*, u32, u32, void*);
static void prollyBtCursorIncrblobCursor(BtCursor*);
#endif
#ifndef NDEBUG
static int prollyBtCursorCursorIsValid(BtCursor*);
#endif
static int prollyBtCursorCursorIsValidNN(BtCursor*);

static int origCursorClearTableOfCursorVt(BtCursor*);
static int origCursorCloseCursorVt(BtCursor*);
static int origCursorCursorHasMovedVt(BtCursor*);
static int origCursorCursorRestoreVt(BtCursor*, int*);
static int origCursorFirstVt(BtCursor*, int*);
static int origCursorLastVt(BtCursor*, int*);
static int origCursorNextVt(BtCursor*, int);
static int origCursorPreviousVt(BtCursor*, int);
static int origCursorEofVt(BtCursor*);
static int origCursorIsEmptyVt(BtCursor*, int*);
static int origCursorTableMovetoVt(BtCursor*, i64, int, int*);
static int origCursorIndexMovetoVt(BtCursor*, UnpackedRecord*, int*);
static i64 origCursorIntegerKeyVt(BtCursor*);
static u32 origCursorPayloadSizeVt(BtCursor*);
static int origCursorPayloadVt(BtCursor*, u32, u32, void*);
static const void *origCursorPayloadFetchVt(BtCursor*, u32*);
static sqlite3_int64 origCursorMaxRecordSizeVt(BtCursor*);
static i64 origCursorOffsetVt(BtCursor*);
static int origCursorInsertVt(BtCursor*, const BtreePayload*, int, int);
static int origCursorDeleteVt(BtCursor*, u8);
static int origCursorTransferRowVt(BtCursor*, BtCursor*, i64);
static void origCursorClearCursorVt(BtCursor*);
static int origCursorCountVt(sqlite3*, BtCursor*, i64*);
static i64 origCursorRowCountEstVt(BtCursor*);
static void origCursorCursorPinVt(BtCursor*);
static void origCursorCursorUnpinVt(BtCursor*);
static void origCursorCursorHintFlagsVt(BtCursor*, unsigned);
static int origCursorCursorHasHintVt(BtCursor*, unsigned int);
#ifndef SQLITE_OMIT_INCRBLOB
static int origCursorPayloadCheckedVt(BtCursor*, u32, u32, void*);
static int origCursorPutDataVt(BtCursor*, u32, u32, void*);
static void origCursorIncrblobCursorVt(BtCursor*);
#endif
#ifndef NDEBUG
static int origCursorCursorIsValidVt(BtCursor*);
#endif
static int origCursorCursorIsValidNNVt(BtCursor*);

static const struct BtCursorOps prollyCursorOps = {
  prollyBtCursorClearTableOfCursor,
  prollyBtCursorCloseCursor,
  prollyBtCursorCursorHasMoved,
  prollyBtCursorCursorRestore,
  prollyBtCursorFirst,
  prollyBtCursorLast,
  prollyBtCursorNext,
  prollyBtCursorPrevious,
  prollyBtCursorEof,
  prollyBtCursorIsEmpty,
  prollyBtCursorTableMoveto,
  prollyBtCursorIndexMoveto,
  prollyBtCursorIntegerKey,
  prollyBtCursorPayloadSize,
  prollyBtCursorPayload,
  prollyBtCursorPayloadFetch,
  prollyBtCursorMaxRecordSize,
  prollyBtCursorOffset,
  prollyBtCursorInsert,
  prollyBtCursorDelete,
  prollyBtCursorTransferRow,
  prollyBtCursorClearCursor,
  prollyBtCursorCount,
  prollyBtCursorRowCountEst,
  prollyBtCursorCursorPin,
  prollyBtCursorCursorUnpin,
  prollyBtCursorCursorHintFlags,
  prollyBtCursorCursorHasHint,
#ifndef SQLITE_OMIT_INCRBLOB
  prollyBtCursorPayloadChecked,
  prollyBtCursorPutData,
  prollyBtCursorIncrblobCursor,
#endif
#ifndef NDEBUG
  prollyBtCursorCursorIsValid,
#endif
  prollyBtCursorCursorIsValidNN,
};

static const struct BtCursorOps origCursorVtOps = {
  origCursorClearTableOfCursorVt,
  origCursorCloseCursorVt,
  origCursorCursorHasMovedVt,
  origCursorCursorRestoreVt,
  origCursorFirstVt,
  origCursorLastVt,
  origCursorNextVt,
  origCursorPreviousVt,
  origCursorEofVt,
  origCursorIsEmptyVt,
  origCursorTableMovetoVt,
  origCursorIndexMovetoVt,
  origCursorIntegerKeyVt,
  origCursorPayloadSizeVt,
  origCursorPayloadVt,
  origCursorPayloadFetchVt,
  origCursorMaxRecordSizeVt,
  origCursorOffsetVt,
  origCursorInsertVt,
  origCursorDeleteVt,
  origCursorTransferRowVt,
  origCursorClearCursorVt,
  origCursorCountVt,
  origCursorRowCountEstVt,
  origCursorCursorPinVt,
  origCursorCursorUnpinVt,
  origCursorCursorHintFlagsVt,
  origCursorCursorHasHintVt,
#ifndef SQLITE_OMIT_INCRBLOB
  origCursorPayloadCheckedVt,
  origCursorPutDataVt,
  origCursorIncrblobCursorVt,
#endif
#ifndef NDEBUG
  origCursorCursorIsValidVt,
#endif
  origCursorCursorIsValidNNVt,
};

static struct TableEntry *catFind(Catalog *cat, Pgno iTable){
  int lo = 0, hi = cat->n - 1;
  while( lo<=hi ){
    int mid = lo + (hi - lo) / 2;
    Pgno midTable = cat->a[mid].iTable;
    if( midTable==iTable ){
      return &cat->a[mid];
    } else if( midTable<iTable ){
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return 0;
}

static struct TableEntry *catAdd(Catalog *cat, Pgno iTable, u8 flags){
  struct TableEntry *pEntry;

  pEntry = catFind(cat, iTable);
  if( pEntry ){
    pEntry->flags = flags;
    return pEntry;
  }

  if( cat->n>=cat->nAlloc ){
    i64 nNew = cat->nAlloc ? (i64)cat->nAlloc * 2 : (i64)16;
    struct TableEntry *aNew;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(struct TableEntry) ) return 0;
    aNew = sqlite3_realloc(cat->a, (int)(nNew * (i64)sizeof(struct TableEntry)));
    if( !aNew ) return 0;
    cat->a = aNew;
    cat->nAlloc = (int)nNew;
  }

  {
    int lo = 0, hi = cat->n;
    while( lo<hi ){
      int mid = lo + (hi - lo) / 2;
      if( cat->a[mid].iTable < iTable ){
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    if( lo < cat->n ){
      memmove(&cat->a[lo+1], &cat->a[lo],
              (cat->n - lo) * (int)sizeof(struct TableEntry));
    }
    pEntry = &cat->a[lo];
  }
  memset(pEntry, 0, sizeof(*pEntry));
  pEntry->iTable = iTable;
  pEntry->flags = flags;
  cat->n++;

  return pEntry;
}

static void catRemove(Catalog *cat, Pgno iTable){
  int i;
  for(i=0; i<cat->n; i++){
    if( cat->a[i].iTable==iTable ){
      sqlite3_free(cat->a[i].zName);
      if( i<cat->n-1 ){
        memmove(&cat->a[i], &cat->a[i+1],
                (cat->n-i-1)*(int)sizeof(struct TableEntry));
      }
      cat->n--;
      return;
    }
  }
}

static void catFree(Catalog *cat){
  int k;
  for(k=0; k<cat->n; k++){
    sqlite3_free(cat->a[k].zName);
    if( cat->a[k].pPending ){
      ProllyMutMap *pMap = (ProllyMutMap*)cat->a[k].pPending;
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
      cat->a[k].pPending = 0;
    }
  }
  sqlite3_free(cat->a);
  cat->a = 0;
  cat->n = 0;
  cat->nAlloc = 0;
}

static void invalidateSchema(Btree *pBtree){
  if( pBtree->pSchema && pBtree->xFreeSchema ){
    pBtree->xFreeSchema(pBtree->pSchema);
  }
}

static void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode){
  BtCursor *p;
  for(p=pBt->pCursor; p; p=p->pNext){
    if( iTable==0 || p->pgnoRoot==iTable ){
      p->eState = CURSOR_FAULT;
      p->skipNext = errCode;
      p->mmActive = 0;

      prollyCursorReleaseAll(&p->pCur);
    }
  }
}

static void refreshCursorRoot(BtCursor *pCur){
  struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE ){
    pCur->pCur.root = pTE->root;
  }
}

typedef struct CatalogEntryMeta CatalogEntryMeta;
struct CatalogEntryMeta {
  Pgno iTable;
  Pgno iPersistTable;
  char *zType;
  char *zName;
  char *zTblName;
  ProllyHash schemaHash;
};

typedef struct CatalogSerializeEntry CatalogSerializeEntry;
struct CatalogSerializeEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  const char *zType;
  const char *zName;
  const char *zTblName;
};

static int tableEntryNameCmp(const void *a, const void *b){
  const struct TableEntry *ea = (const struct TableEntry *)a;
  const struct TableEntry *eb = (const struct TableEntry *)b;
  if( !ea->zName && !eb->zName ) return 0;
  if( !ea->zName ) return -1;
  if( !eb->zName ) return 1;
  return strcmp(ea->zName, eb->zName);
}

static void freeCatalogEntryMeta(CatalogEntryMeta *aMeta, int nMeta){
  int i;
  for(i=0; i<nMeta; i++){
    sqlite3_free(aMeta[i].zType);
    sqlite3_free(aMeta[i].zName);
    sqlite3_free(aMeta[i].zTblName);
  }
  sqlite3_free(aMeta);
}

static int catalogEntryMetaCmp(const void *a, const void *b){
  const CatalogEntryMeta *ea = (const CatalogEntryMeta*)a;
  const CatalogEntryMeta *eb = (const CatalogEntryMeta*)b;
  int c = strcmp(ea->zType ? ea->zType : "", eb->zType ? eb->zType : "");
  if( c ) return c;
  c = strcmp(ea->zTblName ? ea->zTblName : "", eb->zTblName ? eb->zTblName : "");
  if( c ) return c;
  c = strcmp(ea->zName ? ea->zName : "", eb->zName ? eb->zName : "");
  if( c ) return c;
  if( ea->iTable < eb->iTable ) return -1;
  if( ea->iTable > eb->iTable ) return 1;
  return 0;
}

static const CatalogEntryMeta *findCatalogEntryMetaByPgno(
  CatalogEntryMeta *aMeta,
  int nMeta,
  Pgno iTable
){
  int i;
  for(i=0; i<nMeta; i++){
    if( aMeta[i].iTable==iTable ) return &aMeta[i];
  }
  return 0;
}

static const CatalogEntryMeta *findCatalogEntryMetaByObject(
  CatalogEntryMeta *aMeta,
  int nMeta,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  int i;
  for(i=0; i<nMeta; i++){
    if( strcmp(aMeta[i].zType ? aMeta[i].zType : "", zType ? zType : "")!=0 ) continue;
    if( strcmp(aMeta[i].zName ? aMeta[i].zName : "", zName ? zName : "")!=0 ) continue;
    if( strcmp(aMeta[i].zTblName ? aMeta[i].zTblName : "", zTblName ? zTblName : "")!=0 ) continue;
    return &aMeta[i];
  }
  return 0;
}

static int addCatalogEntryMeta(
  CatalogEntryMeta **paMeta,
  int *pnMeta,
  int *pnAlloc,
  Pgno iTable,
  const char *zType,
  const char *zName,
  const char *zTblName
){
  CatalogEntryMeta *aMeta = *paMeta;
  CatalogEntryMeta *pNew;
  if( findCatalogEntryMetaByPgno(aMeta, *pnMeta, iTable) ) return SQLITE_OK;
  if( *pnMeta>=*pnAlloc ){
    i64 nNew = *pnAlloc ? (i64)*pnAlloc * 2 : (i64)16;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(CatalogEntryMeta) ){
      return SQLITE_NOMEM;
    }
    pNew = sqlite3_realloc(aMeta, (int)(nNew * (i64)sizeof(CatalogEntryMeta)));
    if( !pNew ) return SQLITE_NOMEM;
    aMeta = pNew;
    *paMeta = aMeta;
    *pnAlloc = (int)nNew;
  }
  memset(&aMeta[*pnMeta], 0, sizeof(CatalogEntryMeta));
  aMeta[*pnMeta].iTable = iTable;
  aMeta[*pnMeta].iPersistTable = iTable;
  aMeta[*pnMeta].zType = sqlite3_mprintf("%s", zType ? zType : "");
  aMeta[*pnMeta].zName = sqlite3_mprintf("%s", zName ? zName : "");
  aMeta[*pnMeta].zTblName = sqlite3_mprintf("%s", zTblName ? zTblName : "");
  if( !aMeta[*pnMeta].zType || !aMeta[*pnMeta].zName || !aMeta[*pnMeta].zTblName ){
    return SQLITE_NOMEM;
  }
  (*pnMeta)++;
  return SQLITE_OK;
}

static int buildLiveCatalogEntryMeta(Btree *pBtree, CatalogEntryMeta **ppMeta, int *pnMeta){
  sqlite3 *db;
  Schema *pSchema;
  HashElem *k;
  CatalogEntryMeta *aMeta = 0;
  int nMeta = 0, nAlloc = 0, rc = SQLITE_OK, i;
  if( !pBtree || !(db = pBtree->db) || db->nDb<=0 || !(pSchema = db->aDb[0].pSchema) ){
    *ppMeta = 0;
    *pnMeta = 0;
    return SQLITE_OK;
  }
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    Index *pIdx;
    Pgno iTable = 0;
    if( !pTab ) continue;
    if( pBtree->cat.a ){
      for(i=0; i<pBtree->cat.n; i++){
        if( pBtree->cat.a[i].zName
         && strcmp(pBtree->cat.a[i].zName, pTab->zName)==0 ){
          iTable = pBtree->cat.a[i].iTable;
          break;
        }
      }
    }
    if( iTable==0 ) iTable = pTab->tnum;
    if( iTable>1 ){
      rc = addCatalogEntryMeta(&aMeta, &nMeta, &nAlloc, iTable,
                               "table", pTab->zName, "");
      if( rc!=SQLITE_OK ) goto done;
    }
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      Pgno iIndexTable = 0;
      if( pBtree->cat.a ){
        for(i=0; i<pBtree->cat.n; i++){
          if( pBtree->cat.a[i].zName
           && pIdx->zName
           && strcmp(pBtree->cat.a[i].zName, pIdx->zName)==0 ){
            iIndexTable = pBtree->cat.a[i].iTable;
            break;
          }
        }
      }
      if( iIndexTable==0 ) iIndexTable = pIdx->tnum;
      if( iIndexTable<=1 ) continue;
      if( iIndexTable==iTable ) continue;
      rc = addCatalogEntryMeta(&aMeta, &nMeta, &nAlloc, iIndexTable, "index",
                               pIdx->zName, pTab->zName);
      if( rc!=SQLITE_OK ) goto done;
    }
  }
  if( nMeta>1 ){
    qsort(aMeta, nMeta, sizeof(CatalogEntryMeta), catalogEntryMetaCmp);
  }
  for(i=0; i<nMeta; i++){
    aMeta[i].iPersistTable = 0;
  }
done:
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  *ppMeta = aMeta;
  *pnMeta = nMeta;
  return SQLITE_OK;
}

static int catalogSerializeEntryCmp(const void *a, const void *b){
  const CatalogSerializeEntry *ea = (const CatalogSerializeEntry*)a;
  const CatalogSerializeEntry *eb = (const CatalogSerializeEntry*)b;
  int c = strcmp(ea->zType ? ea->zType : "", eb->zType ? eb->zType : "");
  if( c ) return c;
  c = strcmp(ea->zTblName ? ea->zTblName : "", eb->zTblName ? eb->zTblName : "");
  if( c ) return c;
  c = strcmp(ea->zName ? ea->zName : "", eb->zName ? eb->zName : "");
  if( c ) return c;
  if( ea->iTable < eb->iTable ) return -1;
  if( ea->iTable > eb->iTable ) return 1;
  return 0;
}

typedef struct SchemaCatalogRow SchemaCatalogRow;
struct SchemaCatalogRow {
  i64 iRowid;
  Pgno oldPg;
  Pgno newPg;
  char *zType;
  char *zName;
  char *zTblName;
  char *zSql;
};

static int schemaCatalogHasPgno(SchemaCatalogRow *aRows, int nRows, Pgno iTable){
  int i;
  for(i=0; i<nRows; i++){
    if( aRows[i].oldPg==iTable ) return 1;
  }
  return 0;
}

static int schemaCatalogTextField(
  const u8 *pVal, int nVal, DoltliteRecordInfo *pRi, int iField, char **pzOut
){
  int st, off, len;
  char *zOut;
  *pzOut = 0;
  if( iField>=pRi->nField ) return SQLITE_CORRUPT;
  st = pRi->aType[iField];
  off = pRi->aOffset[iField];
  if( st==0 ) return SQLITE_OK;
  if( st<13 || (st&1)==0 ) return SQLITE_CORRUPT;
  len = (st-13)/2;
  if( off<0 || off+len>nVal ) return SQLITE_CORRUPT;
  zOut = sqlite3_malloc(len+1);
  if( !zOut ) return SQLITE_NOMEM;
  memcpy(zOut, pVal+off, len);
  zOut[len] = 0;
  *pzOut = zOut;
  return SQLITE_OK;
}

static i64 schemaCatalogIntField(
  const u8 *pVal, int nVal, DoltliteRecordInfo *pRi, int iField
){
  const u8 *pBody;
  int st, off, nByte, i;
  i64 v;
  if( iField>=pRi->nField ) return 0;
  st = pRi->aType[iField];
  off = pRi->aOffset[iField];
  switch( st ){
    case 0:
    case 8: return 0;
    case 9: return 1;
    case 1: nByte = 1; break;
    case 2: nByte = 2; break;
    case 3: nByte = 3; break;
    case 4: nByte = 4; break;
    case 5: nByte = 6; break;
    case 6: nByte = 8; break;
    default: return 0;
  }
  if( off<0 || off+nByte>nVal ) return 0;
  pBody = pVal + off;
  v = (pBody[0] & 0x80) ? -1 : 0;
  for(i=0; i<nByte; i++) v = (v << 8) | pBody[i];
  return v;
}

static void freeSchemaCatalogRows(SchemaCatalogRow *aRows, int nRows){
  int i;
  for(i=0; i<nRows; i++){
    sqlite3_free(aRows[i].zType);
    sqlite3_free(aRows[i].zName);
    sqlite3_free(aRows[i].zTblName);
    sqlite3_free(aRows[i].zSql);
  }
  sqlite3_free(aRows);
}

static int schemaCatalogRowCmp(const void *a, const void *b){
  const SchemaCatalogRow *ra = (const SchemaCatalogRow*)a;
  const SchemaCatalogRow *rb = (const SchemaCatalogRow*)b;
  const char *za = ra->zType ? ra->zType : "";
  const char *zb = rb->zType ? rb->zType : "";
  int raRank = strcmp(za, "table")==0 ? 0 : strcmp(za, "index")==0 ? 1 : 2;
  int rbRank = strcmp(zb, "table")==0 ? 0 : strcmp(zb, "index")==0 ? 1 : 2;
  int c = raRank - rbRank;
  if( c ) return c;
  c = strcmp(ra->zTblName ? ra->zTblName : "", rb->zTblName ? rb->zTblName : "");
  if( c ) return c;
  c = strcmp(ra->zName ? ra->zName : "", rb->zName ? rb->zName : "");
  if( c ) return c;
  if( ra->iRowid < rb->iRowid ) return -1;
  if( ra->iRowid > rb->iRowid ) return 1;
  return 0;
}

static int schemaCatalogRowIsVirtualTable(const SchemaCatalogRow *pRow){
  const char *zSql;
  if( !pRow || !pRow->zType || !pRow->zSql ) return 0;
  if( strcmp(pRow->zType, "table")!=0 ) return 0;
  zSql = pRow->zSql;
  while( zSql[0]==' ' || zSql[0]=='\t' || zSql[0]=='\n' || zSql[0]=='\r' ){
    zSql++;
  }
  return sqlite3_strnicmp(zSql, "CREATE VIRTUAL TABLE ", 21)==0;
}

typedef struct SchemaFieldValue SchemaFieldValue;
struct SchemaFieldValue {
  int eType;
  i64 i;
  const void *p;
  int n;
};

static u32 schemaCatalogSerialType(const SchemaFieldValue *pMem, u32 *pLen){
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
  *pLen = 0;
  return 0;
}

static void schemaCatalogWriteIntBe(u8 *pOut, i64 v, int nByte){
  int i;
  for(i=nByte-1; i>=0; i--){
    pOut[i] = (u8)(v & 0xFF);
    v >>= 8;
  }
}

static void schemaCatalogSerialPut(u8 *pOut, const SchemaFieldValue *pMem, u32 serialType){
  switch( serialType ){
    case 0:
    case 8:
    case 9:
      return;
    case 1: schemaCatalogWriteIntBe(pOut, pMem->i, 1); return;
    case 2: schemaCatalogWriteIntBe(pOut, pMem->i, 2); return;
    case 3: schemaCatalogWriteIntBe(pOut, pMem->i, 3); return;
    case 4: schemaCatalogWriteIntBe(pOut, pMem->i, 4); return;
    case 5: schemaCatalogWriteIntBe(pOut, pMem->i, 6); return;
    case 6: schemaCatalogWriteIntBe(pOut, pMem->i, 8); return;
    default:
      if( serialType>=13 ) memcpy(pOut, pMem->p, (size_t)pMem->n);
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
  SchemaFieldValue aMem[5];
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
  }

  for(i=0; i<5; i++){
    aType[i] = schemaCatalogSerialType(&aMem[i], &aLen[i]);
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
      schemaCatalogSerialPut(pBody, &aMem[i], aType[i]);
      pBody += aLen[i];
    }
  }
  *pnOut = hdrSize + bodySize;
  return pOut;
}

static int loadSchemaCatalogRows(
  Btree *pBtree,
  struct TableEntry *aTables,
  int nTables,
  SchemaCatalogRow **ppRows,
  int *pnRows,
  ProllyHash *pMasterRoot,
  u8 *pMasterFlags
){
  ChunkStore *cs = &pBtree->pBt->store;
  ProllyCache *pCache = &pBtree->pBt->cache;
  ProllyCursor cur;
  SchemaCatalogRow *aRows = 0;
  int nRows = 0, nAlloc = 0, i, rc, res;

  *ppRows = 0;
  *pnRows = 0;
  memset(pMasterRoot, 0, sizeof(*pMasterRoot));
  *pMasterFlags = 0;
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==1 ){
      *pMasterRoot = aTables[i].root;
      *pMasterFlags = aTables[i].flags;
      break;
    }
  }
  if( prollyHashIsEmpty(pMasterRoot) ) return SQLITE_OK;

  prollyCursorInit(&cur, cs, pCache, pMasterRoot, *pMasterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal;
    int nVal;
    DoltliteRecordInfo ri;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && nVal>0 ){
      char *zType = 0, *zName = 0, *zTblName = 0, *zSql = 0;
      i64 iRootpage = 0;
      doltliteParseRecord(pVal, nVal, &ri);
      if( ri.nField<5 ){ rc = SQLITE_CORRUPT; break; }
      rc = schemaCatalogTextField(pVal, nVal, &ri, 0, &zType);
      if( rc!=SQLITE_OK ) break;
      rc = schemaCatalogTextField(pVal, nVal, &ri, 1, &zName);
      if( rc!=SQLITE_OK ){ sqlite3_free(zType); break; }
      rc = schemaCatalogTextField(pVal, nVal, &ri, 2, &zTblName);
      if( rc!=SQLITE_OK ){ sqlite3_free(zType); sqlite3_free(zName); break; }
      iRootpage = schemaCatalogIntField(pVal, nVal, &ri, 3);
      rc = schemaCatalogTextField(pVal, nVal, &ri, 4, &zSql);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTblName); break;
      }
      if( nRows>=nAlloc ){
        i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
        SchemaCatalogRow *aNew;
        if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
          sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTblName); sqlite3_free(zSql);
          rc = SQLITE_NOMEM;
          break;
        }
        aNew = sqlite3_realloc(aRows,
                               (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
        if( !aNew ){
          sqlite3_free(zType); sqlite3_free(zName); sqlite3_free(zTblName); sqlite3_free(zSql);
          rc = SQLITE_NOMEM;
          break;
        }
        aRows = aNew;
        nAlloc = (int)nNew;
      }
      memset(&aRows[nRows], 0, sizeof(SchemaCatalogRow));
      aRows[nRows].iRowid = prollyCursorIntKey(&cur);
      aRows[nRows].oldPg = (Pgno)iRootpage;
      aRows[nRows].zType = zType;
      aRows[nRows].zName = zName;
      aRows[nRows].zTblName = zTblName;
      aRows[nRows].zSql = zSql;
      nRows++;
    }
    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK ){
    freeSchemaCatalogRows(aRows, nRows);
    return rc;
  }
  *ppRows = aRows;
  *pnRows = nRows;
  return SQLITE_OK;
}

static int schemaCatalogRowWanted(
  const SchemaCatalogRow *pRow,
  struct TableEntry *aTables,
  int nTables
){
  int i;
  if( !pRow ) return 0;
  if( !pRow->zType ) return 0;
  if( schemaCatalogRowIsVirtualTable(pRow) ) return 1;
  if( strcmp(pRow->zType, "table")!=0 && strcmp(pRow->zType, "index")!=0 ){
    return 1;
  }
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==pRow->oldPg ) return 1;
  }
  if( pRow->zType && strcmp(pRow->zType, "table")==0 && pRow->zName ){
    for(i=0; i<nTables; i++){
      if( aTables[i].zName && strcmp(aTables[i].zName, pRow->zName)==0 ){
        return 1;
      }
    }
  }
  return 0;
}

static void filterSchemaCatalogRows(
  SchemaCatalogRow *aRows,
  int *pnRows,
  struct TableEntry *aTables,
  int nTables
){
  int i, nOut = 0;
  int nRows = *pnRows;
  for(i=0; i<nRows; i++){
    if( schemaCatalogRowWanted(&aRows[i], aTables, nTables) ){
      if( nOut!=i ){
        aRows[nOut] = aRows[i];
        memset(&aRows[i], 0, sizeof(aRows[i]));
      }
      nOut++;
    }else{
      sqlite3_free(aRows[i].zType);
      sqlite3_free(aRows[i].zName);
      sqlite3_free(aRows[i].zTblName);
      sqlite3_free(aRows[i].zSql);
      memset(&aRows[i], 0, sizeof(aRows[i]));
    }
  }
  *pnRows = nOut;
}

static int appendMissingSchemaCatalogRows(
  sqlite3 *db,
  SchemaCatalogRow **paRows,
  int *pnRows,
  CatalogEntryMeta *aMeta,
  int nMeta,
  struct TableEntry *aTables,
  int nTables
){
  SchemaCatalogRow *aRows = *paRows;
  int nRows = *pnRows;
  int nAlloc = nRows;
  i64 iNextRowid = 1;
  int i, j, rc;

  for(i=0; i<nRows; i++){
    if( aRows[i].iRowid >= iNextRowid ) iNextRowid = aRows[i].iRowid + 1;
  }

  for(i=0; i<nMeta; i++){
    SchemaCatalogRow *pRow;
    int wanted = 0;
    if( schemaCatalogHasPgno(aRows, nRows, aMeta[i].iTable) ) continue;
    if( !aMeta[i].zType || !aMeta[i].zName || !aMeta[i].zTblName ) continue;
    if( strcmp(aMeta[i].zType, "table")!=0 && strcmp(aMeta[i].zType, "index")!=0 ){
      continue;
    }
    for(j=0; j<nTables; j++){
      if( aTables[j].iTable==aMeta[i].iTable ){
        wanted = 1;
        break;
      }
      if( aTables[j].zName==0 ) continue;
      if( strcmp(aMeta[i].zType, "table")==0 ){
        if( strcmp(aTables[j].zName, aMeta[i].zName)==0 ){
          wanted = 1;
          break;
        }
      }else if( strcmp(aMeta[i].zType, "index")==0 ){
        if( strcmp(aTables[j].zName, aMeta[i].zName)==0
         || strcmp(aTables[j].zName, aMeta[i].zTblName)==0 ){
          wanted = 1;
          break;
        }
      }
    }
    if( !wanted ) continue;
    if( nRows>=nAlloc ){
      i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
      SchemaCatalogRow *aNew;
      if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aNew = sqlite3_realloc(aRows,
                             (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
      if( !aNew ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aRows = aNew;
      nAlloc = (int)nNew;
    }
    pRow = &aRows[nRows];
    memset(pRow, 0, sizeof(*pRow));
    pRow->iRowid = iNextRowid++;
    pRow->oldPg = aMeta[i].iTable;
    pRow->zType = sqlite3_mprintf("%s", aMeta[i].zType);
    pRow->zName = sqlite3_mprintf("%s", aMeta[i].zName);
    pRow->zTblName = sqlite3_mprintf("%s", aMeta[i].zTblName);
    pRow->zSql = 0;
    if( !pRow->zType || !pRow->zName || !pRow->zTblName ){
      sqlite3_free(pRow->zType);
      sqlite3_free(pRow->zName);
      sqlite3_free(pRow->zTblName);
      for(j=0; j<nRows; j++){
        sqlite3_free(aRows[j].zType);
        sqlite3_free(aRows[j].zName);
        sqlite3_free(aRows[j].zTblName);
        sqlite3_free(aRows[j].zSql);
      }
      sqlite3_free(aRows);
      return SQLITE_NOMEM;
    }
    rc = doltliteLoadLiveSchemaSql(db, pRow->zType, pRow->zName,
                                   pRow->zTblName, &pRow->zSql);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aRows, nRows+1);
      return rc;
    }
    nRows++;
  }

  *paRows = aRows;
  *pnRows = nRows;
  return SQLITE_OK;
}

static int appendFallbackSchemaCatalogRows(
  SchemaCatalogRow **paRows,
  int *pnRows,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallback,
  int nFallback
){
  SchemaCatalogRow *aRows = *paRows;
  int nRows = *pnRows;
  int nAlloc = nRows;
  i64 iNextRowid = 1;
  int i, j;

  if( !aFallback || nFallback<=0 ) return SQLITE_OK;
  for(i=0; i<nRows; i++){
    if( aRows[i].iRowid >= iNextRowid ) iNextRowid = aRows[i].iRowid + 1;
  }

  for(i=0; i<nTables; i++){
    SchemaEntry *pSe = 0;
    SchemaCatalogRow *pRow;
    if( aTables[i].iTable<=1 || !aTables[i].zName ) continue;
    if( schemaCatalogHasPgno(aRows, nRows, aTables[i].iTable) ) continue;
    for(j=0; j<nFallback; j++){
      if( !aFallback[j].zName || !aFallback[j].zType ) continue;
      if( strcmp(aFallback[j].zName, aTables[i].zName)!=0 ) continue;
      pSe = &aFallback[j];
      break;
    }
    if( !pSe ) continue;
    if( nRows>=nAlloc ){
      i64 nNew = nAlloc ? (i64)nAlloc * 2 : (i64)16;
      SchemaCatalogRow *aNew;
      if( nNew > (i64)0x7fffffff/(i64)sizeof(SchemaCatalogRow) ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aNew = sqlite3_realloc(aRows,
                             (int)(nNew * (i64)sizeof(SchemaCatalogRow)));
      if( !aNew ){
        freeSchemaCatalogRows(aRows, nRows);
        return SQLITE_NOMEM;
      }
      aRows = aNew;
      nAlloc = (int)nNew;
    }
    pRow = &aRows[nRows];
    memset(pRow, 0, sizeof(*pRow));
    pRow->iRowid = iNextRowid++;
    pRow->oldPg = aTables[i].iTable;
    pRow->zType = sqlite3_mprintf("%s", pSe->zType ? pSe->zType : "");
    pRow->zName = sqlite3_mprintf("%s", pSe->zName ? pSe->zName : "");
    pRow->zTblName = sqlite3_mprintf("%s", pSe->zTblName ? pSe->zTblName : "");
    pRow->zSql = pSe->zSql ? sqlite3_mprintf("%s", pSe->zSql) : 0;
    if( !pRow->zType || !pRow->zName || !pRow->zTblName || (pSe->zSql && !pRow->zSql) ){
      freeSchemaCatalogRows(aRows, nRows+1);
      return SQLITE_NOMEM;
    }
    nRows++;
  }

  *paRows = aRows;
  *pnRows = nRows;
  return SQLITE_OK;
}

int doltliteSerializeCatalogEntries(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  u8 **ppOut,
  int *pnOut
){
  return doltliteSerializeCatalogEntriesWithFallbackSchema(
      db, aTables, nTables, 0, 0, ppOut, pnOut);
}

int doltliteSerializeCatalogEntriesWithFallbackSchema(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  SchemaEntry *aFallbackSchema,
  int nFallbackSchema,
  u8 **ppOut,
  int *pnOut
){
  int sz = CAT_HEADER_SIZE_V3;
  u8 *buf, *q;
  Btree *pBtree;
  SchemaCatalogRow *aRows = 0;
  CatalogEntryMeta *aMeta = 0;
  ProllyHash masterRoot;
  u8 masterFlags = 0;
  CatalogSerializeEntry *aSorted = 0;
  int nRows = 0, nMeta = 0;
  int i, j;
  int rc;

  if( !db ) return SQLITE_MISUSE;
  if( db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  rc = buildLiveCatalogEntryMeta(pBtree, &aMeta, &nMeta);
  if( rc!=SQLITE_OK ) return rc;
  rc = loadSchemaCatalogRows(pBtree, aTables, nTables, &aRows, &nRows, &masterRoot, &masterFlags);
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  filterSchemaCatalogRows(aRows, &nRows, aTables, nTables);
  rc = appendMissingSchemaCatalogRows(db, &aRows, &nRows, aMeta, nMeta,
                                      aTables, nTables);
  if( rc==SQLITE_OK ){
    rc = appendFallbackSchemaCatalogRows(&aRows, &nRows, aTables, nTables,
                                         aFallbackSchema, nFallbackSchema);
  }
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  if( nRows>0 ){
    ProllyMutMap mm;
    struct TableEntry masterEntry;
    qsort(aRows, nRows, sizeof(SchemaCatalogRow), schemaCatalogRowCmp);
    for(i=0; i<nRows; i++){
      if( schemaCatalogRowIsVirtualTable(&aRows[i]) ){
        aRows[i].newPg = 0;
      }else if( strcmp(aRows[i].zType, "table")==0 || strcmp(aRows[i].zType, "index")==0 ){
        aRows[i].newPg = (Pgno)(i + 2);
      }else{
        aRows[i].newPg = aRows[i].oldPg;
      }
    }

    memset(&mm, 0, sizeof(mm));
    rc = prollyMutMapInit(&mm, 1);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aRows, nRows);
      return rc;
    }
    for(i=0; i<nRows; i++){
      int nRec = 0;
      u8 *pRec;
      ProllyHash h;
      const char *zRecordSql = aRows[i].zSql;
      if( (strcmp(aRows[i].zType, "table")==0 || strcmp(aRows[i].zType, "index")==0)
       && aRows[i].zSql!=0 && aRows[i].zSql[0]!=0 ){
        char *zCanon = doltliteCanonicalizeSchemaSql(aRows[i].zSql, aRows[i].zName);
        if( !zCanon ){
          prollyMutMapFree(&mm);
          freeSchemaCatalogRows(aRows, nRows);
          freeCatalogEntryMeta(aMeta, nMeta);
          return SQLITE_NOMEM;
        }
        prollyHashCompute((const u8*)zCanon, (int)strlen(zCanon), &h);
        zRecordSql = zCanon;
        pRec = buildSchemaCatalogRecord(aRows[i].zType, aRows[i].zName,
                                        aRows[i].zTblName, aRows[i].newPg,
                                        zRecordSql, &nRec);
        sqlite3_free(zCanon);
      }else{
        memset(&h, 0, sizeof(h));
        pRec = buildSchemaCatalogRecord(aRows[i].zType, aRows[i].zName,
                                        aRows[i].zTblName, aRows[i].newPg,
                                        zRecordSql, &nRec);
      }
      for(j=0; j<nTables; j++){
        if( aTables[j].iTable==aRows[i].oldPg ){
          aTables[j].schemaHash = h;
          break;
        }
      }
      if( aRows[i].newPg==aRows[i].oldPg ){
        aRows[i].newPg = aRows[i].oldPg;
      }
      if( !pRec ){
        prollyMutMapFree(&mm);
        freeSchemaCatalogRows(aRows, nRows);
        freeCatalogEntryMeta(aMeta, nMeta);
        return SQLITE_NOMEM;
      }
      rc = prollyMutMapInsert(&mm, 0, 0, (i64)(i + 1), pRec, nRec);
      sqlite3_free(pRec);
      if( rc!=SQLITE_OK ){
        prollyMutMapFree(&mm);
        freeSchemaCatalogRows(aRows, nRows);
        freeCatalogEntryMeta(aMeta, nMeta);
        return rc;
      }
    }
    memset(&masterEntry, 0, sizeof(masterEntry));
    masterEntry.iTable = 1;
    memset(&masterEntry.root, 0, sizeof(masterEntry.root));
    masterEntry.flags = masterFlags;
    rc = applyMutMapToTableRoot(pBtree->pBt, &masterEntry, &mm);
    prollyMutMapFree(&mm);
    if( rc!=SQLITE_OK ){
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return rc;
    }
    masterRoot = masterEntry.root;
  }

  if( nMeta>0 ){
    Pgno iNextHidden = 2;
    for(i=0; i<nRows; i++){
      if( aRows[i].newPg >= iNextHidden ) iNextHidden = aRows[i].newPg + 1;
    }
    for(i=0; i<nMeta; i++){
      if( schemaCatalogHasPgno(aRows, nRows, aMeta[i].iTable) ) continue;
      aMeta[i].iPersistTable = iNextHidden++;
    }
  }

  if( nTables > 0 ){
    aSorted = sqlite3_malloc(nTables * (int)sizeof(CatalogSerializeEntry));
    if( !aSorted ){
      freeSchemaCatalogRows(aRows, nRows);
      return SQLITE_NOMEM;
    }
    memset(aSorted, 0, nTables * (int)sizeof(CatalogSerializeEntry));
    for(i=0; i<nTables; i++){
      const SchemaCatalogRow *pRow = 0;
      const CatalogEntryMeta *pMeta = 0;
      aSorted[i].iTable = aTables[i].iTable;
      aSorted[i].root = aTables[i].root;
      aSorted[i].schemaHash = aTables[i].schemaHash;
      aSorted[i].flags = aTables[i].flags;
      if( aTables[i].iTable==1 ){
        aSorted[i].root = masterRoot;
        aSorted[i].zType = "catalog";
        aSorted[i].zName = "sqlite_master";
        aSorted[i].zTblName = "";
        continue;
      }
      if( aTables[i].zName ){
        for(j=0; j<nRows; j++){
          if( strcmp(aRows[j].zType, "table")==0
           && aRows[j].zName
           && strcmp(aRows[j].zName, aTables[i].zName)==0 ){
            pRow = &aRows[j];
            break;
          }
        }
      }
      for(j=0; j<nRows; j++){
        if( !pRow && aRows[j].oldPg==aTables[i].iTable ){
          pRow = &aRows[j];
          break;
        }
      }
      if( pRow ){
        if( pRow->oldPg==aTables[i].iTable ){
          aSorted[i].iTable = pRow->newPg;
        }else{
          aSorted[i].iTable = aTables[i].iTable;
        }
        aSorted[i].zType = pRow->zType;
        aSorted[i].zName = pRow->zName;
        aSorted[i].zTblName = pRow->zTblName;
      }else{
        pMeta = findCatalogEntryMetaByPgno(aMeta, nMeta, aTables[i].iTable);
        if( pMeta && pMeta->iPersistTable>0 ){
          aSorted[i].iTable = pMeta->iPersistTable;
          aSorted[i].zType = pMeta->zType;
          aSorted[i].zName = pMeta->zName;
          aSorted[i].zTblName = pMeta->zTblName;
        }else{
          aSorted[i].zType = "unknown";
          aSorted[i].zName = aTables[i].zName ? aTables[i].zName : "";
          aSorted[i].zTblName = "";
        }
      }
    }
    qsort(aSorted, nTables, sizeof(CatalogSerializeEntry), catalogSerializeEntryCmp);
  }

  for(i=0; i<nTables; i++){
    int nType = aSorted[i].zType ? (int)strlen(aSorted[i].zType) : 0;
    int nName = aSorted[i].zName ? (int)strlen(aSorted[i].zName) : 0;
    int nTbl = aSorted[i].zTblName ? (int)strlen(aSorted[i].zTblName) : 0;
    if( sz > 0x7FFFFFFF - (4 + 1 + PROLLY_HASH_SIZE*2 + 6 + nType + nName + nTbl) ){
      sqlite3_free(aSorted);
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return SQLITE_TOOBIG;
    }
    sz += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 6 + nType + nName + nTbl;
  }

  buf = sqlite3_malloc(sz);
  if( !buf ){
    sqlite3_free(aSorted);
    freeSchemaCatalogRows(aRows, nRows);
    freeCatalogEntryMeta(aMeta, nMeta);
    return SQLITE_NOMEM;
  }
  q = buf;

  *q++ = CATALOG_FORMAT_V4;
  q[0]=(u8)nTables; q[1]=(u8)(nTables>>8);
  q[2]=(u8)(nTables>>16); q[3]=(u8)(nTables>>24);
  q += 4;

  for(i=0; i<nTables; i++){
    const CatalogSerializeEntry *e = &aSorted[i];
    u32 pg = e->iTable;
    int nType = e->zType ? (int)strlen(e->zType) : 0;
    int nName = e->zName ? (int)strlen(e->zName) : 0;
    int nTbl = e->zTblName ? (int)strlen(e->zTblName) : 0;
    q[0]=(u8)pg; q[1]=(u8)(pg>>8); q[2]=(u8)(pg>>16); q[3]=(u8)(pg>>24);
    q += 4;
    *q++ = e->flags;
    memcpy(q, e->root.data, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(q, e->schemaHash.data, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    q[0]=(u8)nType; q[1]=(u8)(nType>>8); q+=2;
    q[0]=(u8)nName; q[1]=(u8)(nName>>8); q+=2;
    q[0]=(u8)nTbl; q[1]=(u8)(nTbl>>8); q+=2;
    if( nType>0 ) memcpy(q, e->zType, nType);
    q += nType;
    if( nName>0 ) memcpy(q, e->zName, nName);
    q += nName;
    if( nTbl>0 ) memcpy(q, e->zTblName, nTbl);
    q += nTbl;
  }
  sqlite3_free(aSorted);
  freeSchemaCatalogRows(aRows, nRows);
  freeCatalogEntryMeta(aMeta, nMeta);
  *ppOut = buf;
  *pnOut = (int)(q - buf);
  return SQLITE_OK;
}

static int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut){
  return doltliteSerializeCatalogEntries(
      pBtree->db, pBtree->cat.a, pBtree->cat.n, ppOut, pnOut);
}

static int buildRuntimeMasterRoot(Btree *pBtree, ProllyHash *pMasterRoot){
  SchemaCatalogRow *aRows = 0;
  SchemaCatalogRow *aCanon = 0;
  CatalogEntryMeta *aMeta = 0;
  ProllyHash currentMasterRoot;
  u8 masterFlags = 0;
  ProllyMutMap mm;
  struct TableEntry masterEntry;
  int nRows = 0, nMeta = 0;
  int rc = SQLITE_OK;
  int i, j;

  memset(pMasterRoot, 0, sizeof(*pMasterRoot));
  rc = buildLiveCatalogEntryMeta(pBtree, &aMeta, &nMeta);
  if( rc!=SQLITE_OK ) return rc;
  rc = loadSchemaCatalogRows(pBtree, pBtree->cat.a, pBtree->cat.n,
                             &aRows, &nRows, &currentMasterRoot, &masterFlags);
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  rc = appendMissingSchemaCatalogRows(pBtree->db, &aRows, &nRows, aMeta, nMeta,
                                      pBtree->cat.a, pBtree->cat.n);
  if( rc!=SQLITE_OK ){
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  if( nRows<=0 ){
    freeSchemaCatalogRows(aRows, nRows);
    freeCatalogEntryMeta(aMeta, nMeta);
    return SQLITE_OK;
  }

  aCanon = sqlite3_malloc(nRows * (int)sizeof(SchemaCatalogRow));
  if( !aCanon ){
    freeSchemaCatalogRows(aRows, nRows);
    freeCatalogEntryMeta(aMeta, nMeta);
    return SQLITE_NOMEM;
  }
  memcpy(aCanon, aRows, nRows * (int)sizeof(SchemaCatalogRow));
  qsort(aCanon, nRows, sizeof(SchemaCatalogRow), schemaCatalogRowCmp);
  for(i=0; i<nRows; i++){
    if( schemaCatalogRowIsVirtualTable(&aCanon[i]) ){
      aCanon[i].newPg = 0;
    }else if( strcmp(aCanon[i].zType, "table")==0 || strcmp(aCanon[i].zType, "index")==0 ){
      aCanon[i].newPg = (Pgno)(i + 2);
    }else{
      aCanon[i].newPg = aCanon[i].oldPg;
    }
  }
  for(i=0; i<nRows; i++){
    aRows[i].newPg = aRows[i].oldPg;
    if( schemaCatalogRowIsVirtualTable(&aRows[i]) ){
      aRows[i].newPg = 0;
    }else if( strcmp(aRows[i].zType, "table")==0 || strcmp(aRows[i].zType, "index")==0 ){
      for(j=0; j<nRows; j++){
        if( aCanon[j].oldPg==aRows[i].oldPg ){
          aRows[i].newPg = aCanon[j].newPg;
          break;
        }
      }
    }
  }
  sqlite3_free(aCanon);

  memset(&mm, 0, sizeof(mm));
  rc = prollyMutMapInit(&mm, 1);
  if( rc!=SQLITE_OK ){
    freeSchemaCatalogRows(aRows, nRows);
    freeCatalogEntryMeta(aMeta, nMeta);
    return rc;
  }
  for(i=0; i<nRows; i++){
    int nRec = 0;
    u8 *pRec = buildSchemaCatalogRecord(aRows[i].zType, aRows[i].zName,
                                        aRows[i].zTblName, aRows[i].newPg,
                                        aRows[i].zSql, &nRec);
    if( !pRec ){
      prollyMutMapFree(&mm);
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return SQLITE_NOMEM;
    }
    rc = prollyMutMapInsert(&mm, 0, 0, aRows[i].iRowid, pRec, nRec);
    sqlite3_free(pRec);
    if( rc!=SQLITE_OK ){
      prollyMutMapFree(&mm);
      freeSchemaCatalogRows(aRows, nRows);
      freeCatalogEntryMeta(aMeta, nMeta);
      return rc;
    }
  }
  memset(&masterEntry, 0, sizeof(masterEntry));
  masterEntry.iTable = 1;
  masterEntry.flags = masterFlags;
  masterEntry.root = currentMasterRoot;
  rc = applyMutMapToTableRoot(pBtree->pBt, &masterEntry, &mm);
  prollyMutMapFree(&mm);
  if( rc==SQLITE_OK ){
    *pMasterRoot = masterEntry.root;
  }
  freeSchemaCatalogRows(aRows, nRows);
  freeCatalogEntryMeta(aMeta, nMeta);
  return rc;
}

static void initDefaultMeta(Btree *pBtree){
  memset(pBtree->aMeta, 0, sizeof(pBtree->aMeta));
  pBtree->aMeta[BTREE_FILE_FORMAT] = 4;
  pBtree->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;

}

static int deserializeCatalog(Btree *pBtree, const u8 *data, int nData){
  const u8 *q = data;
  int nTables, i;
  int iFormat = 0;

  {
    const u8 *pEntries;
    if( !catalogParseHeaderEx(data, nData, &iFormat, &nTables, &pEntries) ){
      return SQLITE_CORRUPT;
    }
    q = pEntries;
  }

  btreeFreeCatalogTables(pBtree);
  initDefaultMeta(pBtree);

  {
    u32 schemaHash = 0;
    int j;
    for(j = 0; j < nData; j++){
      schemaHash = schemaHash * 31 + data[j];
    }
    pBtree->aMeta[BTREE_SCHEMA_VERSION] = schemaHash | 1;
  }

  for(i=0; i<nTables; i++){
    Pgno iTable;
    u8 flags;
    struct TableEntry *pTE;
    int nLen;
    if( q+4+1+PROLLY_HASH_SIZE+PROLLY_HASH_SIZE > data+nData ){
      return SQLITE_CORRUPT;
    }
    iTable = (Pgno)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
    q += 4;
    flags = *q++;
    pTE = addTable(pBtree, iTable, flags);
    if( !pTE ) return SQLITE_NOMEM;
    memcpy(pTE->root.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(pTE->schemaHash.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    if( iFormat==CATALOG_FORMAT_V4 ){
      int nType, nName, nTbl;
      const u8 *pType, *pName, *pTbl;
      if( q+6 > data+nData ) return SQLITE_CORRUPT;
      nType = q[0] | (q[1]<<8); q += 2;
      nName = q[0] | (q[1]<<8); q += 2;
      nTbl = q[0] | (q[1]<<8); q += 2;
      if( q+nType+nName+nTbl > data+nData ) return SQLITE_CORRUPT;
      pType = q; q += nType;
      pName = q; q += nName;
      pTbl = q; q += nTbl;
      (void)pTbl;
      if( nType==5 && memcmp(pType, "table", 5)==0 && nName>0 ){
        pTE->zName = sqlite3_malloc(nName+1);
        if( !pTE->zName ) return SQLITE_NOMEM;
        memcpy(pTE->zName, pName, nName);
        pTE->zName[nName] = 0;
      }
    }else{
      if( q+2 > data+nData ) return SQLITE_CORRUPT;
      nLen = q[0] | (q[1]<<8); q += 2;
      if( q+nLen > data+nData ) return SQLITE_CORRUPT;
      if( nLen>0 ){
        pTE->zName = sqlite3_malloc(nLen+1);
        if( pTE->zName ){
          memcpy(pTE->zName, q, nLen);
          pTE->zName[nLen] = 0;
        }else{
          return SQLITE_NOMEM;
        }
      }
      q += nLen;
    }
  }

  if( q!=data+nData ) return SQLITE_CORRUPT;

  {
    Pgno maxPage = 0;
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].iTable > maxPage ){
        maxPage = pBtree->cat.a[i].iTable;
      }
    }
    pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE] = maxPage;

    pBtree->cat.iNextTable = maxPage + 1;
  }

  if( getenv("DOLTLITE_DEBUG_CAT") ){
    fprintf(stderr, "deserializeCatalog: n=%d\n", pBtree->cat.n);
    for(i=0; i<pBtree->cat.n; i++){
      char zRoot[PROLLY_HASH_SIZE*2 + 1];
      int k;
      for(k=0; k<PROLLY_HASH_SIZE; k++){
        static const char zHex[] = "0123456789abcdef";
        zRoot[k*2] = zHex[(pBtree->cat.a[i].root.data[k] >> 4) & 0xF];
        zRoot[k*2 + 1] = zHex[pBtree->cat.a[i].root.data[k] & 0xF];
      }
      zRoot[PROLLY_HASH_SIZE*2] = 0;
      fprintf(stderr, "  cat[%d]: iTable=%u name=%s flags=%u root=%s\n",
              i, (unsigned)pBtree->cat.a[i].iTable,
              pBtree->cat.a[i].zName ? pBtree->cat.a[i].zName : "(null)",
              (unsigned)pBtree->cat.a[i].flags, zRoot);
    }
  }

  return SQLITE_OK;
}

static int applyMutMapToTableRoot(
  BtShared *pBt,
  struct TableEntry *pTE,
  ProllyMutMap *pMap
){
  ProllyMutator mut;
  int rc;

  memset(&mut, 0, sizeof(mut));
  mut.pStore = &pBt->store;
  mut.pCache = &pBt->cache;
  mut.oldRoot = pTE->root;
  mut.pEdits = pMap;
  mut.flags = pTE->flags;

  rc = prollyMutateFlush(&mut);
  if( rc!=SQLITE_OK ) return rc;

  pTE->root = mut.newRoot;
  return SQLITE_OK;
}

static int cacheCursorPayloadCopy(BtCursor *pCur, const u8 *pData, int nData){
  u8 *pCopy = 0;
  if( nData > 0 ){
    pCopy = sqlite3_malloc(nData);
    if( !pCopy ) return SQLITE_NOMEM;
    memcpy(pCopy, pData, nData);
  }
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->pCachedPayload = pCopy;
  pCur->nCachedPayload = nData;
  pCur->cachedPayloadOwned = 1;
  return SQLITE_OK;
}

static int cacheCursorPayloadReconstructed(
  BtCursor *pCur, const u8 *pSortKey, int nSortKey
){
  int nRec = 0;
  int rc = recordFromSortKeyBuffer(
      pSortKey, nSortKey,
      &pCur->pReconPayload, &pCur->nReconPayloadAlloc, &nRec);
  if( rc!=SQLITE_OK ) return rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->pCachedPayload = pCur->pReconPayload;
  pCur->nCachedPayload = nRec;
  pCur->cachedPayloadOwned = 0;
  return SQLITE_OK;
}

static int serializeUnpackedRecordBuffer(
  UnpackedRecord *pRec, u8 **ppBuf, int *pnAlloc, int *pnOut
){
  int nField = pRec->nField;
  Mem *aMem = pRec->aMem;
  u32 nData = 0;
  u32 aType[MAX_RECORD_FIELDS];
  u32 aLen[MAX_RECORD_FIELDS];
  int i;
  u8 *pOut;
  int nHdr, nTotal;

  if( nField > MAX_RECORD_FIELDS ) nField = MAX_RECORD_FIELDS;

  for(i=0; i<nField; i++){
    aType[i] = btreeSerialType(&aMem[i], &aLen[i]);
    nData += aLen[i];
  }

  nHdr = 1;
  for(i=0; i<nField; i++) nHdr += sqlite3VarintLen(aType[i]);
  if( nHdr > MAX_ONEBYTE_HEADER ) nHdr++;

  nTotal = nHdr + (int)nData;
  if( *pnAlloc < nTotal ){
    u8 *pNew = (u8*)sqlite3_realloc(*ppBuf, nTotal);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nTotal;
  }
  pOut = *ppBuf;

  {
    int off = putVarint32(pOut, (u32)nHdr);
    for(i=0; i<nField; i++){
      off += putVarint32(pOut + off, aType[i]);
    }
  }

  {
    u32 off = (u32)nHdr;
    for(i=0; i<nField; i++){
      Mem *p = &aMem[i];
      u32 st = aType[i];
      if( st==SERIAL_TYPE_NULL || st==SERIAL_TYPE_ZERO || st==SERIAL_TYPE_ONE ){
      }else if( st<=SERIAL_TYPE_INT64 ){
        i64 v = p->u.i;
        int nByte = (int)aLen[i];
        int j;
        for(j=nByte-1; j>=0; j--){
          pOut[off+j] = (u8)(v & 0xFF);
          v >>= 8;
        }
        off += nByte;
      }else if( st==SERIAL_TYPE_FLOAT64 ){
        u64 floatBits;
        int j;
        memcpy(&floatBits, &p->u.r, 8);
        for(j=7; j>=0; j--){
          pOut[off+j] = (u8)(floatBits & 0xFF);
          floatBits >>= 8;
        }
        off += 8;
      }else{
        int nByte = (int)aLen[i];
        if( nByte > 0 && p->z ) memcpy(pOut + off, p->z, nByte);
        off += nByte;
      }
    }
  }

  *pnOut = nTotal;
  return SQLITE_OK;
}

static int unpackedRecordCanUseIntSortKey(
  BtCursor *pCur,
  UnpackedRecord *pRec,
  int nField
){
  KeyInfo *pKeyInfo = pCur->pKeyInfo;
  int i;
  if( !pKeyInfo || !pRec || nField<=0 || pRec->nField<nField ) return 0;
  if( nField > pKeyInfo->nAllField ) return 0;
  for(i=0; i<nField; i++){
    CollSeq *pColl;
    if( !(pRec->aMem[i].flags & MEM_Int) ) return 0;
    if( pKeyInfo->aSortFlags && (pKeyInfo->aSortFlags[i] & KEYINFO_ORDER_DESC) ){
      return 0;
    }
    pColl = pKeyInfo->aColl[i];
    if( pColl && pColl->zName && sqlite3StrICmp(pColl->zName, "BINARY")!=0 ){
      return 0;
    }
  }
  return 1;
}

static int sortKeyFromUnpackedIntRecordBuffer(
  UnpackedRecord *pRec,
  int nField,
  u8 **ppBuf,
  int *pnAlloc,
  int *pnOut
){
  int i;
  int nOut = 0;
  int nAlloc = nField * 18;
  if( *pnAlloc < nAlloc ){
    u8 *pNew = (u8*)sqlite3_realloc64(*ppBuf, (sqlite3_uint64)nAlloc);
    if( !pNew ) return SQLITE_NOMEM;
    *ppBuf = pNew;
    *pnAlloc = nAlloc;
  }
  for(i=0; i<nField; i++){
    int n = 0;
    int rc = sortKeyFromInt64(pRec->aMem[i].u.i, *ppBuf + nOut, &n);
    if( rc!=SQLITE_OK ) return rc;
    nOut += n;
  }
  *pnOut = nOut;
  return SQLITE_OK;
}
static void clearMergeCursorState(BtCursor *pCur){
  pCur->mmIdx = -1;
  pCur->mmPhysIdx = -1;
  pCur->mmActive = 0;
  pCur->mmPhysActive = 0;
  pCur->deferredTreeSeek = 0;
  pCur->mergeSrc = MERGE_SRC_TREE;
}

static ProllyMutMapEntry *currentMutMapEntry(BtCursor *pCur){
  if( pCur->mmPhysActive ){
    return &pCur->pMutMap->aEntries[pCur->mmPhysIdx];
  }
  return prollyMutMapEntryAt(pCur->pMutMap, pCur->mmIdx);
}

static SQLITE_INLINE ProllyMutMapEntry *orderedMutMapEntryAt(
  ProllyMutMap *pMap,
  int idx
){
  if( pMap->keepSorted || !pMap->orderDirty ){
    return &pMap->aEntries[pMap->aOrder[idx]];
  }
  return prollyMutMapEntryAt(pMap, idx);
}

static void setCursorToMutMapEntryPhys(BtCursor *pCur, int physIdx){
  ProllyMutMapEntry *pEntry = &pCur->pMutMap->aEntries[physIdx];
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->mmIdx = -1;
  pCur->mmPhysIdx = physIdx;
  pCur->mmActive = 1;
  pCur->mmPhysActive = 1;
  pCur->deferredTreeSeek = 0;
  pCur->mergeSrc = MERGE_SRC_MUT;
  pCur->eState = CURSOR_VALID;
  pCur->curFlags &= ~BTCF_AtLast;
  if( pCur->curIntKey ){
    pCur->cachedIntKey = prollyMutMapEntryIntKey(pEntry);
    pCur->curFlags |= BTCF_ValidNKey;
  }else{
    pCur->curFlags &= ~BTCF_ValidNKey;
  }
}

static int advanceTreeCursor(BtCursor *pCur, int dir){
  if( dir>0 ){
    return prollyCursorNext(&pCur->pCur);
  }else{
    return prollyCursorPrev(&pCur->pCur);
  }
}

static int flushMutMap(BtCursor *pCur){
  struct TableEntry *pTE;
  ProllyMutMap *pFlushMap;
  int captured;
  int rc;

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    return SQLITE_INTERNAL;
  }
  if( !pTE->pPending || prollyMutMapIsEmpty((ProllyMutMap*)pTE->pPending) ){
    return SQLITE_OK;
  }

  pFlushMap = (ProllyMutMap*)pTE->pPending;
  captured = 0;
  rc = snapshotPendingForFlush(pCur->pBtree, pCur->pgnoRoot,
                               (ProllyMutMap**)&pTE->pPending,
                               &pFlushMap, &captured);
  if( rc!=SQLITE_OK ) return rc;
  if( captured ){
    refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot,
                               (ProllyMutMap*)pTE->pPending);
  }
  rc = applyMutMapToTableRoot(pCur->pBt, pTE, pFlushMap);
  if( rc!=SQLITE_OK ) return rc;
  pCur->pCur.root = pTE->root;
  if( captured ){
    pCur->flushSeekEdits = 0;
  }else{
    prollyMutMapClear((ProllyMutMap*)pTE->pPending);
  }

  return SQLITE_OK;
}

static int flushPendingForTable(
  Btree *pBtree,
  BtShared *pBt,
  struct TableEntry *pTE,
  int clearInPlace
){
  ProllyMutMap *pMap;
  ProllyMutMap *pFlushMap;
  int captured = 0;
  int rc;

  if( !pTE || !pTE->pPending ) return SQLITE_OK;
  pMap = (ProllyMutMap*)pTE->pPending;
  if( prollyMutMapIsEmpty(pMap) ) return SQLITE_OK;

  pFlushMap = pMap;
  rc = snapshotPendingForFlush(pBtree, pTE->iTable,
                               (ProllyMutMap**)&pTE->pPending,
                               &pFlushMap, &captured);
  if( rc!=SQLITE_OK ) return rc;
  if( captured ){
    refreshCursorMutMapAliases(pBtree, pBt, pTE->iTable,
                               (ProllyMutMap*)pTE->pPending);
  }

  rc = applyMutMapToTableRoot(pBt, pTE, pFlushMap);
  if( rc!=SQLITE_OK ) return rc;

  if( pTE->pPending==pMap ){
    if( clearInPlace ){
      prollyMutMapClear(pMap);
    }else{
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
      pTE->pPending = 0;
      refreshCursorMutMapAliases(pBtree, pBt, pTE->iTable, 0);
    }
  }
  pTE->pendingFlushSeekEdits = 0;
  return SQLITE_OK;
}
static int syncSavepoints(BtCursor *pCur){
  Btree *pBtree = pCur->pBtree;
  sqlite3 *db = pBtree ? pBtree->db : 0;
  if( db ){
    int target = db->nSavepoint + db->nStatement;
    while( pBtree->nSavepoint < target ){
      int rc = pushSavepoint(pBtree, pBtree->nSavepoint >= db->nSavepoint);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

/* Pending edit maps are shared by every cursor on the same table. When a flush
** swaps the table map, every live cursor must drop iterator state into it. */
static void refreshCursorMutMapAliases(Btree *pBtree, BtShared *pBt,
                                        Pgno iTable, ProllyMutMap *pNewMap){
  BtCursor *p;
  for(p = pBt->pCursor; p; p = p->pNext){
    if( p->pBtree==pBtree && p->pgnoRoot==iTable ){
      p->pMutMap = pNewMap;
      p->mmActive = 0;
      p->mmPhysActive = 0;
      p->deferredTreeSeek = 0;
      p->mmIdx = -1;
      p->mmPhysIdx = -1;
    }
  }
}

static int ensureMutMap(BtCursor *pCur){
  int rc;
  struct TableEntry *pTE;
  ProllyMutMap *pMap;

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ) return SQLITE_INTERNAL;

  if( pTE->pPending ){
    pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
    return SQLITE_OK;
  }

  pMap = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !pMap ) return SQLITE_NOMEM;
  rc = prollyMutMapInitMode(pMap, pCur->curIntKey, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pMap);
    return rc;
  }
  if( pCur->pBtree ){
    pMap->currentSavepointLevel = pCur->pBtree->nSavepoint;
  }
  pTE->pPending = pMap;
  refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot, pMap);
  return SQLITE_OK;
}

static int saveCursorPosition(BtCursor *pCur){
  if( pCur->eState!=CURSOR_VALID && pCur->eState!=CURSOR_SKIPNEXT ){
    return SQLITE_OK;
  }
  if( pCur->isPinned ){
    return SQLITE_OK;
  }

  if( !prollyCursorIsValid(&pCur->pCur) ){
    if( pCur->curIntKey && (pCur->curFlags & BTCF_ValidNKey) ){

      pCur->nKey = pCur->cachedIntKey;
      pCur->pKey = 0;
      pCur->eState = CURSOR_REQUIRESEEK;
      return SQLITE_OK;
    }
    pCur->eState = CURSOR_INVALID;
    return SQLITE_OK;
  }

  if( pCur->curIntKey ){
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      pCur->nKey = prollyMutMapEntryIntKey(currentMutMapEntry(pCur));
    }else{
      pCur->nKey = prollyCursorIntKey(&pCur->pCur);
    }
    pCur->pKey = 0;
  } else {
    const u8 *pKey = 0;
    int nKey = 0;
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      ProllyMutMapEntry *pEntry = currentMutMapEntry(pCur);
      pKey = pEntry->pKey;
      nKey = pEntry->nKey;
    }else{
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    }
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
    if( nKey>0 ){
      pCur->pKey = sqlite3_malloc(nKey);
      if( !pCur->pKey ){
        return SQLITE_NOMEM;
      }
      memcpy(pCur->pKey, pKey, nKey);
      pCur->nKey = nKey;
    } else {
      pCur->nKey = 0;
    }
  }

  prollyCursorReleaseAll(&pCur->pCur);
  pCur->deferredTreeSeek = 0;

  pCur->eState = CURSOR_REQUIRESEEK;
  return SQLITE_OK;
}

static int tableEntryIsTableRoot(Btree *pBtree, struct TableEntry *pTE){
  if( !pTE || pTE->iTable<=1 ) return 0;
  if( !pTE->zName && pBtree && pBtree->db ){
    pTE->zName = doltliteResolveTableNumber(pBtree->db, pTE->iTable);
  }
  return pTE->zName!=0;
}

static int restoreCursorPosition(BtCursor *pCur, int *pDifferentRow){
  int rc = SQLITE_OK;
  int res = 0;

  if( pCur->eState!=CURSOR_REQUIRESEEK ){
    if( pDifferentRow ) *pDifferentRow = 0;
    return SQLITE_OK;
  }

  refreshCursorRoot(pCur);

  if( pCur->curIntKey ){
    rc = prollyCursorSeekInt(&pCur->pCur, pCur->nKey, &res);
  } else {
    if( pCur->pKey && pCur->nKey>0 ){
      rc = prollyCursorSeekBlob(&pCur->pCur,
                                 (const u8*)pCur->pKey, (int)pCur->nKey,
                                 &res);
    } else {
      pCur->eState = CURSOR_INVALID;
      if( pDifferentRow ) *pDifferentRow = 1;
      return SQLITE_OK;
    }
  }

  if( pCur->pKey ){
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
  }

  if( rc==SQLITE_OK ){
    if( res==0 ){
      pCur->eState = CURSOR_VALID;
      if( pDifferentRow ) *pDifferentRow = 0;
    } else if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      pCur->eState = CURSOR_VALID;
      if( pDifferentRow ) *pDifferentRow = 1;
    } else {
      pCur->eState = CURSOR_INVALID;
      if( pDifferentRow ) *pDifferentRow = 1;
    }
  } else {
    pCur->eState = CURSOR_FAULT;
    pCur->skipNext = rc;
    if( pDifferentRow ) *pDifferentRow = 1;
  }

  return rc;
}

static void freeSavepointTables(struct SavepointTableState *pState){
  sqlite3_free(pState->zRebaseOrigBranch);
  pState->zRebaseOrigBranch = 0;
  sqlite3_free(pState->zRebaseReturnBranch);
  pState->zRebaseReturnBranch = 0;
  if( pState->aCatalogSnapshot ){
    int i;
    for(i=0; i<pState->nTables; i++){
      sqlite3_free(pState->aCatalogSnapshot[i].zName);
    }
    sqlite3_free(pState->aCatalogSnapshot);
    pState->aCatalogSnapshot = 0;
    pState->bCatalogSnapshot = 0;
  }
  if( pState->aPendingSnapshot ){
    int i;
    for(i=0; i<pState->nPendingSnapshot; i++){
      if( pState->aPendingSnapshot[i].pPending ){
        prollyMutMapFree(pState->aPendingSnapshot[i].pPending);
        sqlite3_free(pState->aPendingSnapshot[i].pPending);
      }
    }
    sqlite3_free(pState->aPendingSnapshot);
    pState->aPendingSnapshot = 0;
    pState->nPendingSnapshot = 0;
    pState->nPendingSnapshotAlloc = 0;
  }
  if( pState->aTables ){
    sqlite3_free(pState->aTables);
    pState->aTables = 0;
  }
  memset(&pState->catalogHash, 0, sizeof(pState->catalogHash));
  memset(&pState->stagedCatalog, 0, sizeof(pState->stagedCatalog));
  pState->isMerging = 0;
  memset(&pState->mergeCommitHash, 0, sizeof(pState->mergeCommitHash));
  memset(&pState->conflictsCatalogHash, 0, sizeof(pState->conflictsCatalogHash));
  memset(&pState->constraintViolationsHash, 0, sizeof(pState->constraintViolationsHash));
  pState->isRebasing = 0;
  memset(&pState->preRebaseWorkingCat, 0, sizeof(pState->preRebaseWorkingCat));
  memset(&pState->rebaseOntoCommit, 0, sizeof(pState->rebaseOntoCommit));
}

static int captureSavepointCatalogSnapshot(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  int i;
  int n = pBtree->cat.n;
  SavepointCatalogEntry *aSnapshot = 0;

  if( n<=0 ) return SQLITE_OK;
  aSnapshot = sqlite3_malloc64((sqlite3_uint64)n * sizeof(SavepointCatalogEntry));
  if( !aSnapshot ) return SQLITE_NOMEM;
  memset(aSnapshot, 0, (size_t)n * sizeof(SavepointCatalogEntry));

  for(i=0; i<n; i++){
    struct TableEntry *pSrc = &pBtree->cat.a[i];
    SavepointCatalogEntry *pDst = &aSnapshot[i];
    pDst->iTable = pSrc->iTable;
    pDst->root = pSrc->root;
    pDst->schemaHash = pSrc->schemaHash;
    pDst->flags = pSrc->flags;
    pDst->pendingFlushSeekEdits = pSrc->pendingFlushSeekEdits;
    if( pSrc->zName ){
      pDst->zName = sqlite3_mprintf("%s", pSrc->zName);
      if( !pDst->zName ){
        int j;
        for(j=0; j<=i; j++){
          sqlite3_free(aSnapshot[j].zName);
        }
        sqlite3_free(aSnapshot);
        return SQLITE_NOMEM;
      }
    }
  }

  pState->aCatalogSnapshot = aSnapshot;
  pState->bCatalogSnapshot = 1;
  return SQLITE_OK;
}

static void captureSavepointSessionState(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  pState->iNextTable = pBtree->cat.iNextTable;
  pState->iLargestRootPage = pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE];
  pState->stagedCatalog = pBtree->stagedCatalog;
  pState->isMerging = pBtree->isMerging;
  pState->mergeCommitHash = pBtree->mergeCommitHash;
  pState->conflictsCatalogHash = pBtree->conflictsCatalogHash;
  pState->constraintViolationsHash = pBtree->constraintViolationsHash;
  pState->isRebasing = pBtree->isRebasing;
  pState->preRebaseWorkingCat = pBtree->preRebaseWorkingCat;
  pState->rebaseOntoCommit = pBtree->rebaseOntoCommit;
  sqlite3_free(pState->zRebaseOrigBranch);
  pState->zRebaseOrigBranch = pBtree->zRebaseOrigBranch
      ? sqlite3_mprintf("%s", pBtree->zRebaseOrigBranch) : 0;
  sqlite3_free(pState->zRebaseReturnBranch);
  pState->zRebaseReturnBranch = pBtree->zRebaseReturnBranch
      ? sqlite3_mprintf("%s", pBtree->zRebaseReturnBranch) : 0;
}

static void restoreSavepointSessionState(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  pBtree->stagedCatalog = pState->stagedCatalog;
  pBtree->isMerging = pState->isMerging;
  pBtree->mergeCommitHash = pState->mergeCommitHash;
  pBtree->conflictsCatalogHash = pState->conflictsCatalogHash;
  pBtree->constraintViolationsHash = pState->constraintViolationsHash;
  pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE] = pState->iLargestRootPage;
  pBtree->isRebasing = pState->isRebasing;
  pBtree->preRebaseWorkingCat = pState->preRebaseWorkingCat;
  pBtree->rebaseOntoCommit = pState->rebaseOntoCommit;
  sqlite3_free(pBtree->zRebaseOrigBranch);
  pBtree->zRebaseOrigBranch = pState->zRebaseOrigBranch
      ? sqlite3_mprintf("%s", pState->zRebaseOrigBranch) : 0;
  sqlite3_free(pBtree->zRebaseReturnBranch);
  pBtree->zRebaseReturnBranch = pState->zRebaseReturnBranch
      ? sqlite3_mprintf("%s", pState->zRebaseReturnBranch) : 0;
}

static int captureSavepointTables(
  Btree *pBtree,
  struct SavepointTableState *pState,
  int bStatement
){
  int k;
  int rc;
  u8 *catData = 0;
  int nCatData = 0;
  memset(&pState->catalogHash, 0, sizeof(pState->catalogHash));
  if( bStatement ){
    rc = captureSavepointCatalogSnapshot(pBtree, pState);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    rc = serializeCatalog(pBtree, &catData, &nCatData);
    if( rc!=SQLITE_OK ) return rc;
    rc = chunkStorePut(&pBtree->pBt->store, catData, nCatData, &pState->catalogHash);
    sqlite3_free(catData);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pBtree->cat.n<=0 ) return SQLITE_OK;
  pState->aTables = sqlite3_malloc(
      pBtree->cat.n * (int)sizeof(SavepointTableEntry));
  if( !pState->aTables ) return SQLITE_NOMEM;
  for(k=0; k<pBtree->cat.n; k++){
    pState->aTables[k].iTable = pBtree->cat.a[k].iTable;
    pState->aTables[k].pendingFlushSeekEdits =
        pBtree->cat.a[k].pendingFlushSeekEdits;
  }
  pState->nTables = pBtree->cat.n;
  return SQLITE_OK;
}

static void pushSavepointOnMutMaps(Btree *pBtree, int level){
  int k;
  for(k=0; k<pBtree->cat.n; k++){
    ProllyMutMap *pMap = (ProllyMutMap*)pBtree->cat.a[k].pPending;
    if( pMap ) prollyMutMapPushSavepoint(pMap, level);
  }
}

static int findPendingSnapshotIndex(
  struct SavepointTableState *pState,
  Pgno iTable
){
  int i;
  for(i=0; i<pState->nPendingSnapshot; i++){
    if( pState->aPendingSnapshot[i].iTable==iTable ){
      return i;
    }
  }
  return -1;
}

static ProllyMutMap *findPendingSnapshot(Btree *pBtree, int iFromSavepoint,
                                          Pgno iTable, int *piSavepoint,
                                          int *piSnapshot){
  int i;
  if( piSavepoint ) *piSavepoint = -1;
  if( piSnapshot ) *piSnapshot = -1;
  for(i=iFromSavepoint; i<pBtree->nSavepoint; i++){
    struct SavepointTableState *pState = &pBtree->aSavepointTables[i];
    int j;
    if( !pState->aPendingSnapshot ) continue;
    j = findPendingSnapshotIndex(pState, iTable);
    if( j >= 0 ){
      if( piSavepoint ) *piSavepoint = i;
      if( piSnapshot ) *piSnapshot = j;
      return pState->aPendingSnapshot[j].pPending;
    }
  }
  return 0;
}

static int allocEmptyPendingLike(ProllyMutMap *pSrc, ProllyMutMap **ppOut){
  ProllyMutMap *pNew;
  int rc;
  *ppOut = 0;
  pNew = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !pNew ) return SQLITE_NOMEM;
  rc = prollyMutMapInitMode(pNew, pSrc->isIntKey, pSrc->keepSorted);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pNew);
    return rc;
  }
  pNew->currentSavepointLevel = pSrc->currentSavepointLevel;
  *ppOut = pNew;
  return SQLITE_OK;
}

static int appendPendingSnapshot(
  struct SavepointTableState *pState,
  Pgno iTable,
  ProllyMutMap *pPending
){
  if( pState->nPendingSnapshot >= pState->nPendingSnapshotAlloc ){
    i64 nNew = pState->nPendingSnapshotAlloc
                 ? (i64)pState->nPendingSnapshotAlloc * 2 : (i64)4;
    SavepointPendingSnapshot *aNew;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(SavepointPendingSnapshot) ){
      return SQLITE_NOMEM;
    }
    aNew = sqlite3_realloc(pState->aPendingSnapshot,
        (int)(nNew * (i64)sizeof(SavepointPendingSnapshot)));
    if( !aNew ) return SQLITE_NOMEM;
    pState->aPendingSnapshot = aNew;
    pState->nPendingSnapshotAlloc = (int)nNew;
  }
  pState->aPendingSnapshot[pState->nPendingSnapshot].iTable = iTable;
  pState->aPendingSnapshot[pState->nPendingSnapshot].pPending = pPending;
  pState->nPendingSnapshot++;
  return SQLITE_OK;
}

static int inheritPendingSnapshots(
  struct SavepointTableState *pParent,
  struct SavepointTableState *pChild
){
  int i;
  if( !pParent || !pChild ) return SQLITE_OK;
  for(i=0; i<pChild->nPendingSnapshot; i++){
    SavepointPendingSnapshot *pSnap = &pChild->aPendingSnapshot[i];
    if( !pSnap->pPending ) continue;
    if( findSavepointTableIndexInArray(pParent->aTables, pParent->nTables,
                                       pSnap->iTable) < 0 ){
      prollyMutMapFree(pSnap->pPending);
      sqlite3_free(pSnap->pPending);
      pSnap->pPending = 0;
      continue;
    }
    if( findPendingSnapshotIndex(pParent, pSnap->iTable) >= 0 ){
      prollyMutMapFree(pSnap->pPending);
      sqlite3_free(pSnap->pPending);
      pSnap->pPending = 0;
      continue;
    }
    if( appendPendingSnapshot(pParent, pSnap->iTable, pSnap->pPending)!=SQLITE_OK ){
      return SQLITE_NOMEM;
    }
    pSnap->pPending = 0;
  }
  return SQLITE_OK;
}

static int rollbackMutMapsToSavepoint(Btree *pBtree, int level,
                                       int iFromSavepoint){
  int k, rc;
  for(k=0; k<pBtree->cat.n; k++){
    struct TableEntry *pTE = &pBtree->cat.a[k];
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    int iSavepoint = -1;
    int iSnapshot = -1;
    ProllyMutMap *pSnap = findPendingSnapshot(pBtree, iFromSavepoint,
                                               pTE->iTable,
                                               &iSavepoint, &iSnapshot);
    if( pSnap ){
      if( pMap ){
        prollyMutMapFree(pMap);
        sqlite3_free(pMap);
      }
      pTE->pPending = pSnap;
      pBtree->aSavepointTables[iSavepoint].aPendingSnapshot[iSnapshot].pPending = 0;
      rc = prollyMutMapRollbackToSavepoint(pSnap, level);
      if( rc!=SQLITE_OK ) return rc;
    }else if( pMap ){
      rc = prollyMutMapRollbackToSavepoint(pMap, level);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static void releaseMutMapsToSavepoint(Btree *pBtree, int level){
  int k;
  for(k=0; k<pBtree->cat.n; k++){
    ProllyMutMap *pMap = (ProllyMutMap*)pBtree->cat.a[k].pPending;
    if( pMap ) prollyMutMapReleaseSavepoint(pMap, level);
  }
}

/* If a dirty map is flushed while a savepoint is open, move that map into the
** savepoint snapshot and continue with a fresh map. Rollback can then restore
** the exact pre-flush edits even though the table root has already changed. */
static int snapshotPendingForFlush(Btree *pBtree, Pgno iTable,
                                   ProllyMutMap **ppPending,
                                   ProllyMutMap **ppFlushMap,
                                   int *pCaptured){
  int i;
  ProllyMutMap *pPending;
  if( pCaptured ) *pCaptured = 0;
  if( !ppFlushMap ) return SQLITE_MISUSE;
  *ppFlushMap = 0;
  if( !pBtree || !ppPending || !*ppPending ) return SQLITE_OK;
  pPending = *ppPending;
  *ppFlushMap = pPending;
  if( pBtree->nSavepoint <= 0 ) return SQLITE_OK;
  if( prollyMutMapIsEmpty(pPending) ) return SQLITE_OK;

  for(i = pBtree->nSavepoint - 1; i >= 0; i--){
    struct SavepointTableState *pState = &pBtree->aSavepointTables[i];
    int j;
    j = findSavepointTableIndexInArray(pState->aTables, pState->nTables, iTable);
    if( j < 0 ) continue;
    if( findPendingSnapshotIndex(pState, iTable) >= 0 ) return SQLITE_OK;
    {
      ProllyMutMap *pNewPending = 0;
      int rc = allocEmptyPendingLike(pPending, &pNewPending);
      if( rc!=SQLITE_OK ) return rc;
      rc = appendPendingSnapshot(pState, iTable, pPending);
      if( rc!=SQLITE_OK ){
        prollyMutMapFree(pNewPending);
        sqlite3_free(pNewPending);
        return rc;
      }
      *ppPending = pNewPending;
      if( pCaptured ) *pCaptured = 1;
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int findTableIndexInArray(
  struct TableEntry *aTables,
  int nTables,
  Pgno iTable
){
  int lo = 0;
  int hi = nTables;
  while( lo < hi ){
    int mid = lo + ((hi - lo) / 2);
    Pgno midTable = aTables[mid].iTable;
    if( midTable==iTable ){
      return mid;
    }
    if( midTable < iTable ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return -1;
}

static char *resolveLiveSchemaTableNumber(sqlite3 *db, Pgno iTable){
  Schema *pSchema;
  HashElem *k;
  if( !db || db->nDb<=0 ) return 0;
  pSchema = db->aDb[0].pSchema;
  if( !pSchema ) return 0;
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    if( pTab && pTab->tnum==(Pgno)iTable ){
      return sqlite3_mprintf("%s", pTab->zName);
    }
  }
  return 0;
}

static int findSavepointTableIndexInArray(
  SavepointTableEntry *aTables,
  int nTables,
  Pgno iTable
){
  int lo = 0;
  int hi = nTables;
  while( lo < hi ){
    int mid = lo + ((hi - lo) / 2);
    Pgno midTable = aTables[mid].iTable;
    if( midTable==iTable ){
      return mid;
    }
    if( midTable < iTable ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return -1;
}

static int restoreTablesFromSavepoint(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  ProllyMutMap **apPending = 0;
  int k;
  int rc;
  struct TableEntry *aCurrent = pBtree->cat.a;
  int nCurrent = pBtree->cat.n;

  if( pState->nTables>0 ){
    apPending = sqlite3_malloc64(
        pState->nTables * sizeof(ProllyMutMap*));
    if( !apPending ) return SQLITE_NOMEM;
    memset(apPending, 0, pState->nTables * sizeof(ProllyMutMap*));

    if( pBtree->cat.nAlloc < pState->nTables ){
      struct TableEntry *aNew = sqlite3_realloc(
          pBtree->cat.a, pState->nTables * (int)sizeof(struct TableEntry));
      if( !aNew ){
        sqlite3_free(apPending);
        return SQLITE_NOMEM;
      }
      pBtree->cat.a = aNew;
      pBtree->cat.nAlloc = pState->nTables;
    }
  }

  for(k=0; k<nCurrent; k++){
    ProllyMutMap *pMap = aCurrent[k].pPending;
    int iSaved;
    if( !pMap ){
      continue;
    }
    iSaved = findSavepointTableIndexInArray(
        pState->aTables, pState->nTables, aCurrent[k].iTable);
    if( iSaved>=0 ){
      apPending[iSaved] = pMap;
    }else{
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
    }
    aCurrent[k].pPending = 0;
  }

  if( prollyHashIsEmpty(&pState->catalogHash) ){
    if( pState->bCatalogSnapshot ){
      btreeFreeCatalogTables(pBtree);
      initDefaultMeta(pBtree);
      if( pState->nTables>0 ){
        if( pBtree->cat.nAlloc < pState->nTables ){
          struct TableEntry *aNew = sqlite3_realloc(
              pBtree->cat.a, pState->nTables * (int)sizeof(struct TableEntry));
          if( !aNew ){
            sqlite3_free(apPending);
            return SQLITE_NOMEM;
          }
          pBtree->cat.a = aNew;
          pBtree->cat.nAlloc = pState->nTables;
        }
        memset(pBtree->cat.a, 0, pState->nTables * sizeof(struct TableEntry));
        for(k=0; k<pState->nTables; k++){
          struct TableEntry *pDst = &pBtree->cat.a[k];
          SavepointCatalogEntry *pSrc = &pState->aCatalogSnapshot[k];
          pDst->iTable = pSrc->iTable;
          pDst->root = pSrc->root;
          pDst->schemaHash = pSrc->schemaHash;
          pDst->flags = pSrc->flags;
          pDst->pendingFlushSeekEdits = pSrc->pendingFlushSeekEdits;
          if( pSrc->zName ){
            pDst->zName = sqlite3_mprintf("%s", pSrc->zName);
            if( !pDst->zName ){
              sqlite3_free(apPending);
              btreeFreeCatalogTables(pBtree);
              initDefaultMeta(pBtree);
              return SQLITE_NOMEM;
            }
          }
        }
      }
      pBtree->cat.n = pState->nTables;
      pBtree->cat.iNextTable = pState->iNextTable;
    }else{
      btreeFreeCatalogTables(pBtree);
      initDefaultMeta(pBtree);
      pBtree->cat.iNextTable = 2;
    }
  }else{
    u8 *catData = 0;
    int nCatData = 0;
    rc = chunkStoreGet(&pBtree->pBt->store, &pState->catalogHash, &catData, &nCatData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(apPending);
      return rc;
    }
    rc = deserializeCatalog(pBtree, catData, nCatData);
    sqlite3_free(catData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(apPending);
      return rc;
    }
  }

  if( pState->nTables>0 ){
    for(k=0; k<pState->nTables; k++){
      int idx = findTableIndexInArray(
          pBtree->cat.a, pBtree->cat.n, pState->aTables[k].iTable);
      if( idx < 0 ){
        sqlite3_free(apPending);
        return SQLITE_CORRUPT;
      }
      pBtree->cat.a[idx].pendingFlushSeekEdits =
          pState->aTables[k].pendingFlushSeekEdits;
      pBtree->cat.a[idx].pPending = apPending[k];
    }
  }

  pBtree->cat.iNextTable = pState->iNextTable;
  sqlite3_free(apPending);
  return SQLITE_OK;
}

static int pushSavepoint(Btree *pBtree, int bStatement){
  struct SavepointTableState *pState;

  if( pBtree->nSavepoint>=pBtree->nSavepointAlloc ){
    i64 nNew = pBtree->nSavepointAlloc
                 ? (i64)pBtree->nSavepointAlloc * 2 : (i64)8;
    struct SavepointTableState *aNewT;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(struct SavepointTableState) ){
      return SQLITE_NOMEM;
    }
    aNewT = sqlite3_realloc(pBtree->aSavepointTables,
        (int)(nNew * (i64)sizeof(struct SavepointTableState)));
    if( !aNewT ) return SQLITE_NOMEM;
    pBtree->aSavepointTables = aNewT;
    pBtree->nSavepointAlloc = (int)nNew;
  }

  pState = &pBtree->aSavepointTables[pBtree->nSavepoint];
  memset(&pState->catalogHash, 0, sizeof(pState->catalogHash));
  pState->aTables = 0;
  pState->aCatalogSnapshot = 0;
  pState->bCatalogSnapshot = 0;
  pState->aPendingSnapshot = 0;
  pState->nPendingSnapshot = 0;
  pState->nPendingSnapshotAlloc = 0;
  pState->nTables = 0;
  pState->iLargestRootPage = 0;
  pState->isRebasing = 0;
  memset(&pState->preRebaseWorkingCat, 0, sizeof(pState->preRebaseWorkingCat));
  memset(&pState->rebaseOntoCommit, 0, sizeof(pState->rebaseOntoCommit));
  pState->zRebaseOrigBranch = 0;
  pState->zRebaseReturnBranch = 0;
  captureSavepointSessionState(pBtree, pState);
  {
    int rc = captureSavepointTables(pBtree, pState, bStatement);
    if( rc!=SQLITE_OK ) return rc;
  }

  pBtree->nSavepoint++;
  pushSavepointOnMutMaps(pBtree, pBtree->nSavepoint);
  return SQLITE_OK;
}

static int countTreeEntries(Btree *pBtree, Pgno iTable, i64 *pCount){
  int rc;
  int res;
  i64 count = 0;
  struct TableEntry *pTE;
  ProllyCursor tempCur;
  BtShared *pBt = pBtree->pBt;

  pTE = findTable(pBtree, iTable);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ){
    *pCount = 0;
    return SQLITE_OK;
  }

  if( getenv("DOLTLITE_DEBUG_COUNT") && iTable==3 ){
    char zRoot[PROLLY_HASH_SIZE*2 + 1];
    int k;
    static const char zHex[] = "0123456789abcdef";
    for(k=0; k<PROLLY_HASH_SIZE; k++){
      zRoot[k*2] = zHex[(pTE->root.data[k] >> 4) & 0xF];
      zRoot[k*2 + 1] = zHex[pTE->root.data[k] & 0xF];
    }
    zRoot[PROLLY_HASH_SIZE*2] = 0;
    fprintf(stderr, "countTreeEntries: branch=%s iTable=%u flags=%u root=%s\n",
            pBtree->zBranch ? pBtree->zBranch : "(null)",
            (unsigned)iTable, (unsigned)pTE->flags, zRoot);
  }

  prollyCursorInit(&tempCur, &pBt->store, &pBt->cache,
                    &pTE->root, pTE->flags);

  rc = prollyCursorFirst(&tempCur, &res);
  if( getenv("DOLTLITE_DEBUG_COUNT") && iTable==3 ){
    fprintf(stderr, "countTreeEntries: rc=%d res=%d state=%d\n", rc, res, tempCur.eState);
  }
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&tempCur);
    *pCount = 0;
    return rc;
  }

  if( res!=0 ){
    prollyCursorClose(&tempCur);
    *pCount = 0;
    return SQLITE_OK;
  }

  while( tempCur.eState==PROLLY_CURSOR_VALID ){
    count++;
    rc = prollyCursorNext(&tempCur);
    if( rc!=SQLITE_OK ) break;
    if( tempCur.eState!=PROLLY_CURSOR_VALID ) break;
  }

  prollyCursorClose(&tempCur);
  *pCount = count;
  return SQLITE_OK;
}

static int saveAllCursors(Btree *pBtree, BtShared *pBt, Pgno iRoot,
                          BtCursor *pExcept){
  BtCursor *p;
  for(p=pBt->pCursor; p; p=p->pNext){
    if( p->pBtree==pBtree
     && p!=pExcept
     && (iRoot==0 || p->pgnoRoot==iRoot) ){
      if( p->eState==CURSOR_VALID || p->eState==CURSOR_SKIPNEXT ){
        int rc = saveCursorPosition(p);
        if( rc!=SQLITE_OK ) return rc;
      }
    }
  }
  return SQLITE_OK;
}

int sqlite3BtreeOpen(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  sqlite3 *db,
  Btree **ppBtree,
  int flags,
  int vfsFlags
){
  Btree *p = 0;
  BtShared *pBt = 0;
  int rc = SQLITE_OK;

  *ppBtree = 0;

  if( !zFilename || zFilename[0]=='\0'
   || (strcmp(zFilename, ":memory:")==0 && db->aDb[0].pBt!=0)
   || (flags & BTREE_SINGLE)
   || (vfsFlags & SQLITE_OPEN_TEMP_DB)
   || origBtreeIsSqliteFile(zFilename)
  ){
    p = sqlite3_malloc(sizeof(Btree));
    if( !p ) return SQLITE_NOMEM;
    memset(p, 0, sizeof(*p));
    p->db = db;
    p->pOps = &origBtreeVtOps;
    rc = origBtreeOpen(pVfs, zFilename, db, &p->pOrigBtree, flags, vfsFlags);
    if( rc!=SQLITE_OK ){ sqlite3_free(p); return rc; }
    *ppBtree = p;
    return SQLITE_OK;
  }

  p = sqlite3_malloc(sizeof(Btree));
  if( !p ){
    return SQLITE_NOMEM;
  }
  memset(p, 0, sizeof(*p));

  pBt = sqlite3_malloc(sizeof(BtShared));
  if( !pBt ){
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }
  memset(pBt, 0, sizeof(*pBt));

  if( !zFilename || zFilename[0]=='\0' ){
    zFilename = ":memory:";
  }

  rc = chunkStoreOpen(&pBt->store, pVfs, zFilename, vfsFlags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pBt);
    sqlite3_free(p);
    return rc;
  }

  rc = prollyCacheInit(&pBt->cache, PROLLY_DEFAULT_CACHE_SIZE);
  if( rc!=SQLITE_OK ){
    chunkStoreClose(&pBt->store);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return rc;
  }

  pBt->pPagerShim = pagerShimCreate(pVfs, zFilename, chunkFileGetHandle(&pBt->store.file));
  if( !pBt->pPagerShim ){
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }
  /* Bind the shim to the chunk store so sqlite3PagerFile() always resolves
  ** to the current cs->pFile. Without this, csReloadFromDisk (triggered by
  ** concurrent connections or peer processes mutating the chunk store) can
  ** free the old pFile and leave the shim with a dangling pointer, crashing
  ** the next sqlite3OsFileControl on a deref'd id->pMethods. */
  pagerShimSetStore(pBt->pPagerShim, &pBt->store);

  pBt->db = db;
  pBt->pageSize = PROLLY_DEFAULT_PAGE_SIZE;
  pBt->iWorkingStateVersion = 1;
  pBt->nRef = 1;
  p->inTransaction = TRANS_NONE;
  p->bSchemaChangedTxn = 0;

  if( pBt->store.readOnly ){
    pBt->btsFlags |= BTS_READ_ONLY;
  }
  if( chunkStoreIsEmpty(&pBt->store) ){
    pBt->btsFlags |= BTS_INITIALLY_EMPTY;
  }

  p->aMeta[BTREE_FREE_PAGE_COUNT] = 0;
  p->aMeta[BTREE_SCHEMA_VERSION] = 0;
  p->aMeta[BTREE_FILE_FORMAT] = 4;
  p->aMeta[BTREE_DEFAULT_CACHE_SIZE] = 0;
  p->aMeta[BTREE_LARGEST_ROOT_PAGE] = 0;
  p->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;
  p->aMeta[BTREE_USER_VERSION] = 0;
  p->aMeta[BTREE_INCR_VACUUM] = 0;
  p->aMeta[BTREE_APPLICATION_ID] = 0;

  {
    ProllyHash catHash;
    ProllyHash workingCommit;
    ProllyHash stagedCatalog;
    ProllyHash mergeCommitHash;
    ProllyHash conflictsCatalogHash;
    ProllyHash branchCommit;
    ProllyHash preRebaseCat;
    ProllyHash rebaseOnto;
    ProllyHash constraintViolationsHash;
    char *zRebaseOrigBranch = 0;
    char *zRebaseReturnBranch = 0;
    u8 isRebasing = 0;
    const char *zDef = chunkStoreGetDefaultBranch(&pBt->store);
    u8 isMerging = 0;
    if( !zDef ) zDef = "main";
    memset(&catHash, 0, sizeof(catHash));
    memset(&workingCommit, 0, sizeof(workingCommit));
    memset(&stagedCatalog, 0, sizeof(stagedCatalog));
    memset(&mergeCommitHash, 0, sizeof(mergeCommitHash));
    memset(&conflictsCatalogHash, 0, sizeof(conflictsCatalogHash));
    memset(&branchCommit, 0, sizeof(branchCommit));
    memset(&preRebaseCat, 0, sizeof(preRebaseCat));
    memset(&rebaseOnto, 0, sizeof(rebaseOnto));
    memset(&constraintViolationsHash, 0, sizeof(constraintViolationsHash));
    rc = btreeLoadWorkingSetBlob(&pBt->store, zDef, &catHash, &workingCommit,
                                 &stagedCatalog, &isMerging,
                                 &mergeCommitHash, &conflictsCatalogHash,
                                 &isRebasing, &preRebaseCat, &rebaseOnto,
                                 &zRebaseOrigBranch, &zRebaseReturnBranch,
                                 &constraintViolationsHash);
    if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK;
    }
    if( rc!=SQLITE_OK ){
      pagerShimDestroy(pBt->pPagerShim);
      prollyCacheFree(&pBt->cache);
      chunkStoreClose(&pBt->store);
      sqlite3_free(pBt);
      sqlite3_free(p);
      return rc;
    }
    if( prollyHashIsEmpty(&catHash) ){
      rc = btreeLoadBranchHeadCatalog(&pBt->store, zDef, &catHash, 0);
      if( rc==SQLITE_NOTFOUND ){
        rc = SQLITE_OK;
      }
      if( rc!=SQLITE_OK ){
        pagerShimDestroy(pBt->pPagerShim);
        prollyCacheFree(&pBt->cache);
        chunkStoreClose(&pBt->store);
        sqlite3_free(pBt);
        sqlite3_free(p);
        return rc;
      }
    }
    if( !prollyHashIsEmpty(&catHash) ){
      u8 *catData = 0;
      int nCatData = 0;
      rc = chunkStoreGet(&pBt->store, &catHash, &catData, &nCatData);
      if( rc==SQLITE_OK && catData ){
        rc = deserializeCatalog(p, catData, nCatData);
        sqlite3_free(catData);
        if( rc!=SQLITE_OK ){
          pagerShimDestroy(pBt->pPagerShim);
          prollyCacheFree(&pBt->cache);
          chunkStoreClose(&pBt->store);
          sqlite3_free(pBt);
          sqlite3_free(p);
          return rc;
        }
      }else{
        sqlite3_free(catData);
        if( rc!=SQLITE_OK ){
          pagerShimDestroy(pBt->pPagerShim);
          prollyCacheFree(&pBt->cache);
          chunkStoreClose(&pBt->store);
          sqlite3_free(pBt);
          sqlite3_free(p);
          return rc;
        }
      }
    }

    if( chunkStoreFindBranch(&pBt->store, zDef, &branchCommit)==SQLITE_OK ){
      memcpy(&p->headCommit, &branchCommit, sizeof(ProllyHash));
    }else if( !prollyHashIsEmpty(&workingCommit) ){
      memcpy(&p->headCommit, &workingCommit, sizeof(ProllyHash));
    }else{
      memset(&p->headCommit, 0, sizeof(ProllyHash));
    }
    p->stagedCatalog = stagedCatalog;
    p->isMerging = isMerging;
    p->mergeCommitHash = mergeCommitHash;
    p->conflictsCatalogHash = conflictsCatalogHash;
    p->isRebasing = isRebasing;
    p->preRebaseWorkingCat = preRebaseCat;
    p->rebaseOntoCommit = rebaseOnto;
    p->zRebaseOrigBranch = zRebaseOrigBranch;
    p->zRebaseReturnBranch = zRebaseReturnBranch;
    p->constraintViolationsHash = constraintViolationsHash;
  }

  p->cat.iNextTable = 2;
  if( !addTable(p, 1, BTREE_INTKEY) ){
    pagerShimDestroy(pBt->pPagerShim);
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }

  p->db = db;
  p->pBt = pBt;
  p->pOps = &prollyBtreeOps;
  p->inTrans = TRANS_NONE;
  p->iBDataVersion = 1;
  p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion;
  p->nSeek = 0;

  {
    const char *defBranch = chunkStoreGetDefaultBranch(&pBt->store);
    ProllyHash branchCommit;
    p->zBranch = sqlite3_mprintf("%s", defBranch);

    if( prollyHashIsEmpty(&p->headCommit)
     && chunkStoreFindBranch(&pBt->store, defBranch, &branchCommit)==SQLITE_OK ){
      memcpy(&p->headCommit, &branchCommit, sizeof(ProllyHash));
    }
  }

  *ppBtree = p;

  registerDoltiteFunctions(db);

  return SQLITE_OK;
}

static int prollyBtreeClose(Btree *p){
  BtShared *pBt;

  pBt = p->pBt;
  assert( pBt!=0 );

  while( pBt->pCursor ){
    sqlite3BtreeCloseCursor(pBt->pCursor);
  }

  if( p->pSchema ){
    if( p->xFreeSchema ) p->xFreeSchema(p->pSchema);
    sqlite3_free(p->pSchema);
    p->pSchema = 0;
  }
  btreeFreeCatalogTables(p);
  if( p->aSavepointTables ){
    int i;
    for(i=0; i<p->nSavepoint; i++){
      freeSavepointTables(&p->aSavepointTables[i]);
    }
    sqlite3_free(p->aSavepointTables);
  }

  pBt->nRef--;
  if( pBt->nRef<=0 ){
    if( pBt->pPagerShim ){
      pagerShimDestroy(pBt->pPagerShim);
      pBt->pPagerShim = 0;
    }
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(pBt);
  }

  sqlite3_free(p->zBranch);
  sqlite3_free(p->zAuthorName);
  sqlite3_free(p->zAuthorEmail);
  sqlite3_free(p->zRebaseOrigBranch);
  sqlite3_free(p->zRebaseReturnBranch);
  sqlite3_free(p);
  return SQLITE_OK;
}
int sqlite3BtreeClose(Btree *p){
  if( !p ) return SQLITE_OK;
  return p->pOps->xClose(p);
}

static int prollyBtreeNewDb(Btree *p){
  memset(p->aMeta, 0, sizeof(p->aMeta));
  p->aMeta[BTREE_FILE_FORMAT] = 4;
  p->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;

  if( !findTable(p, 1) ){
    if( !addTable(p, 1, BTREE_INTKEY) ){
      return SQLITE_NOMEM;
    }
  } else {
    struct TableEntry *pTE = findTable(p, 1);
    memset(&pTE->root, 0, sizeof(ProllyHash));
  }

  return SQLITE_OK;
}
int sqlite3BtreeNewDb(Btree *p){
  if( !p ) return SQLITE_OK;
  return p->pOps->xNewDb(p);
}

static int prollyBtreeSetCacheSize(Btree *p, int mxPage){
  (void)p; (void)mxPage;
  return SQLITE_OK;
}
int sqlite3BtreeSetCacheSize(Btree *p, int mxPage){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetCacheSize(p, mxPage);
}

static int prollyBtreeSetSpillSize(Btree *p, int mxPage){
  (void)p; (void)mxPage;
  return SQLITE_OK;
}
int sqlite3BtreeSetSpillSize(Btree *p, int mxPage){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetSpillSize(p, mxPage);
}

static int prollyBtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  (void)p; (void)szMmap;
  return SQLITE_OK;
}
int sqlite3BtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetMmapLimit(p, szMmap);
}

static int prollyBtreeSetPagerFlags(Btree *p, unsigned pgFlags){
  (void)p; (void)pgFlags;
  return SQLITE_OK;
}
int sqlite3BtreeSetPagerFlags(Btree *p, unsigned pgFlags){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetPagerFlags(p, pgFlags);
}

static int prollyBtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix){
  (void)nReserve; (void)eFix;
  if( nPagesize>=512 && nPagesize<=65536 ){
    p->pBt->pageSize = (u32)nPagesize;
  }
  return SQLITE_OK;
}
int sqlite3BtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetPageSize(p, nPagesize, nReserve, eFix);
}

static int prollyBtreeGetPageSize(Btree *p){
  return (int)p->pBt->pageSize;
}
int sqlite3BtreeGetPageSize(Btree *p){
  return p->pOps->xGetPageSize(p);
}

static Pgno prollyBtreeMaxPageCount(Btree *p, Pgno mxPage){
  (void)p; (void)mxPage;
  return (Pgno)0x7FFFFFFF;
}
Pgno sqlite3BtreeMaxPageCount(Btree *p, Pgno mxPage){
  if( !p ) return 0;
  return p->pOps->xMaxPageCount(p, mxPage);
}

static Pgno prollyBtreeLastPage(Btree *p){
  return p->cat.iNextTable + 1000;
}
Pgno sqlite3BtreeLastPage(Btree *p){
  return p->pOps->xLastPage(p);
}

static int prollyBtreeSecureDelete(Btree *p, int newFlag){
  (void)p; (void)newFlag;
  return 0;
}
int sqlite3BtreeSecureDelete(Btree *p, int newFlag){
  if( !p ) return 0;
  return p->pOps->xSecureDelete(p, newFlag);
}

static int prollyBtreeGetRequestedReserve(Btree *p){
  (void)p;
  return 0;
}
int sqlite3BtreeGetRequestedReserve(Btree *p){
  if( !p ) return 0;
  return p->pOps->xGetRequestedReserve(p);
}

static int prollyBtreeGetReserveNoMutex(Btree *p){
  (void)p;
  return 0;
}
int sqlite3BtreeGetReserveNoMutex(Btree *p){
  if( !p ) return 0;
  return p->pOps->xGetReserveNoMutex(p);
}

static int prollyBtreeSetAutoVacuum(Btree *p, int autoVacuum){
  (void)p; (void)autoVacuum;
  return SQLITE_OK;
}
int sqlite3BtreeSetAutoVacuum(Btree *p, int autoVacuum){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetAutoVacuum(p, autoVacuum);
}

static int prollyBtreeGetAutoVacuum(Btree *p){
  (void)p;
  return BTREE_AUTOVACUUM_NONE;
}
int sqlite3BtreeGetAutoVacuum(Btree *p){
  if( !p ) return BTREE_AUTOVACUUM_NONE;
  return p->pOps->xGetAutoVacuum(p);
}

static int prollyBtreeIncrVacuum(Btree *p){
  (void)p;
  return SQLITE_DONE;
}
int sqlite3BtreeIncrVacuum(Btree *p){
  if( !p ) return SQLITE_DONE;
  return p->pOps->xIncrVacuum(p);
}

static const char *prollyBtreeGetFilename(Btree *p){
  return chunkStoreFilename(&p->pBt->store);
}
const char *sqlite3BtreeGetFilename(Btree *p){
  if( !p ) return "";
  return p->pOps->xGetFilename(p);
}

static const char *prollyBtreeGetJournalname(Btree *p){
  (void)p;
  return "";
}
const char *sqlite3BtreeGetJournalname(Btree *p){
  if( !p ) return "";
  return p->pOps->xGetJournalname(p);
}

static int prollyBtreeIsReadonly(Btree *p){
  return (p->pBt->btsFlags & BTS_READ_ONLY) ? 1 : 0;
}
int sqlite3BtreeIsReadonly(Btree *p){
  if( !p ) return 0;
  return p->pOps->xIsReadonly(p);
}

static int btreeLoadWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pWorkingCat,
  ProllyHash *pWorkingCommit,
  ProllyHash *pStaged,
  u8 *pIsMerging,
  ProllyHash *pMergeCommit,
  ProllyHash *pConflicts,
  u8 *pIsRebasing,
  ProllyHash *pPreRebaseCat,
  ProllyHash *pRebaseOnto,
  char **pzRebaseOrigBranch,
  char **pzRebaseReturnBranch,
  ProllyHash *pConstraintViolations
){
  ProllyHash wsHash;
  u8 *data = 0;
  int nData = 0;
  int rc;
  u8 version;

  if( pWorkingCat ) memset(pWorkingCat, 0, sizeof(ProllyHash));
  if( pWorkingCommit ) memset(pWorkingCommit, 0, sizeof(ProllyHash));
  if( pStaged ) memset(pStaged, 0, sizeof(ProllyHash));
  if( pIsMerging ) *pIsMerging = 0;
  if( pMergeCommit ) memset(pMergeCommit, 0, sizeof(ProllyHash));
  if( pConflicts ) memset(pConflicts, 0, sizeof(ProllyHash));
  if( pIsRebasing ) *pIsRebasing = 0;
  if( pPreRebaseCat ) memset(pPreRebaseCat, 0, sizeof(ProllyHash));
  if( pRebaseOnto ) memset(pRebaseOnto, 0, sizeof(ProllyHash));
  if( pzRebaseOrigBranch ) *pzRebaseOrigBranch = 0;
  if( pzRebaseReturnBranch ) *pzRebaseReturnBranch = 0;
  if( pConstraintViolations ) memset(pConstraintViolations, 0, sizeof(ProllyHash));

  rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&wsHash) ) return SQLITE_NOTFOUND;

  rc = chunkStoreGet(cs, &wsHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( !data || nData < WS_TOTAL_SIZE_V2 ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }
  version = data[0];
  if( version != WS_FORMAT_VERSION_V2
   && version != WS_FORMAT_VERSION_V3
   && version != WS_FORMAT_VERSION_V4
   && version != WS_FORMAT_VERSION_V5 ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  if( pWorkingCat ) memcpy(pWorkingCat->data, data + WS_WORKING_CAT_OFF, PROLLY_HASH_SIZE);
  if( pWorkingCommit ) memcpy(pWorkingCommit->data, data + WS_WORKING_COMMIT_OFF, PROLLY_HASH_SIZE);
  if( pStaged ) memcpy(pStaged->data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  if( pIsMerging ) *pIsMerging = data[WS_MERGING_OFF];
  if( pMergeCommit ) memcpy(pMergeCommit->data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
  if( pConflicts ) memcpy(pConflicts->data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);

  if( (version == WS_FORMAT_VERSION_V3
    || version == WS_FORMAT_VERSION_V4
    || version == WS_FORMAT_VERSION_V5)
   && nData >= WS_TOTAL_SIZE_V3 ){
    if( pIsRebasing ) *pIsRebasing = data[WS_REBASING_OFF];
    if( pPreRebaseCat ) memcpy(pPreRebaseCat->data,
                                data + WS_PRE_REBASE_CAT_OFF, PROLLY_HASH_SIZE);
    if( pRebaseOnto ) memcpy(pRebaseOnto->data,
                              data + WS_REBASE_ONTO_OFF, PROLLY_HASH_SIZE);
    if( pzRebaseOrigBranch ){
      const char *src = (const char*)(data + WS_REBASE_BRANCH_OFF);
      int n = 0;
      while( n < WS_REBASE_BRANCH_LEN && src[n] ) n++;
      if( n > 0 ){
        char *z = sqlite3_malloc(n + 1);
        if( !z ){ sqlite3_free(data); return SQLITE_NOMEM; }
        memcpy(z, src, n);
        z[n] = 0;
        *pzRebaseOrigBranch = z;
      }
    }
  }
  if( version == WS_FORMAT_VERSION_V4 && nData >= WS_TOTAL_SIZE_V4 ){
    if( pConstraintViolations ){
      memcpy(pConstraintViolations->data,
             data + WS_CONSTRAINT_VIOLATIONS_OFF_V4, PROLLY_HASH_SIZE);
    }
  }else if( version == WS_FORMAT_VERSION_V5 && nData >= WS_TOTAL_SIZE ){
    if( pzRebaseReturnBranch ){
      const char *src = (const char*)(data + WS_REBASE_RETURN_BRANCH_OFF);
      int n = 0;
      while( n < WS_REBASE_BRANCH_LEN && src[n] ) n++;
      if( n > 0 ){
        char *z = sqlite3_malloc(n + 1);
        if( !z ){ sqlite3_free(data); return SQLITE_NOMEM; }
        memcpy(z, src, n);
        z[n] = 0;
        *pzRebaseReturnBranch = z;
      }
    }
    if( pConstraintViolations ){
      memcpy(pConstraintViolations->data,
             data + WS_CONSTRAINT_VIOLATIONS_OFF, PROLLY_HASH_SIZE);
    }
  }
  sqlite3_free(data);
  return SQLITE_OK;
}

static int btreeLoadBranchHeadCatalog(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCatHash,
  ProllyHash *pHeadCommit
){
  ProllyHash headCommit;
  u8 *pData = 0;
  int nData = 0;
  DoltliteCommit c;
  int rc;

  if( pCatHash ) memset(pCatHash, 0, sizeof(*pCatHash));
  if( pHeadCommit ) memset(pHeadCommit, 0, sizeof(*pHeadCommit));
  if( !cs || !zBranch ) return SQLITE_ERROR;

  rc = chunkStoreFindBranch(cs, zBranch, &headCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGet(cs, &headCommit, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  memset(&c, 0, sizeof(c));
  rc = doltliteCommitDeserialize(pData, nData, &c);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&c);
    return rc;
  }

  if( pCatHash ) memcpy(pCatHash, &c.catalogHash, sizeof(*pCatHash));
  if( pHeadCommit ) memcpy(pHeadCommit, &headCommit, sizeof(*pHeadCommit));
  doltliteCommitClear(&c);
  return SQLITE_OK;
}

static void btreeFillWorkingSetBlob(
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
){
  static const ProllyHash emptyHash = {{0}};

  memset(buf, 0, WS_TOTAL_SIZE);
  buf[0] = WS_FORMAT_VERSION;
  memcpy(buf + WS_WORKING_CAT_OFF,
         (pWorkingCat ? pWorkingCat : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_WORKING_COMMIT_OFF,
         (pWorkingCommit ? pWorkingCommit : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_STAGED_OFF,
         (pStaged ? pStaged : &emptyHash)->data, PROLLY_HASH_SIZE);
  buf[WS_MERGING_OFF] = isMerging;
  memcpy(buf + WS_MERGE_COMMIT_OFF,
         (pMergeCommit ? pMergeCommit : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_CONFLICTS_OFF,
         (pConflicts ? pConflicts : &emptyHash)->data, PROLLY_HASH_SIZE);
  buf[WS_REBASING_OFF] = isRebasing;
  memcpy(buf + WS_PRE_REBASE_CAT_OFF,
         (pPreRebaseCat ? pPreRebaseCat : &emptyHash)->data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_REBASE_ONTO_OFF,
         (pRebaseOnto ? pRebaseOnto : &emptyHash)->data, PROLLY_HASH_SIZE);
  if( zRebaseOrigBranch ){
    int n = (int)strlen(zRebaseOrigBranch);
    if( n > WS_REBASE_BRANCH_LEN - 1 ) n = WS_REBASE_BRANCH_LEN - 1;
    memcpy(buf + WS_REBASE_BRANCH_OFF, zRebaseOrigBranch, n);
  }
  if( zRebaseReturnBranch ){
    int n = (int)strlen(zRebaseReturnBranch);
    if( n > WS_REBASE_BRANCH_LEN - 1 ) n = WS_REBASE_BRANCH_LEN - 1;
    memcpy(buf + WS_REBASE_RETURN_BRANCH_OFF, zRebaseReturnBranch, n);
  }
  memcpy(buf + WS_CONSTRAINT_VIOLATIONS_OFF,
         (pConstraintViolations ? pConstraintViolations : &emptyHash)->data,
         PROLLY_HASH_SIZE);
}

static int btreeStoreWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
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
){
  u8 buf[WS_TOTAL_SIZE];
  ProllyHash wsHash;
  int rc;

  btreeFillWorkingSetBlob(buf, pWorkingCat, pWorkingCommit, pStaged,
                          isMerging, pMergeCommit, pConflicts,
                          isRebasing, pPreRebaseCat, pRebaseOnto,
                          zRebaseOrigBranch, zRebaseReturnBranch,
                          pConstraintViolations);

  rc = chunkStorePut(cs, buf, WS_TOTAL_SIZE, &wsHash);
  if( rc != SQLITE_OK ) return rc;
  rc = chunkStoreSetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc == SQLITE_NOTFOUND && chunkStoreIsEmpty(cs) ){
    return SQLITE_OK;
  }
  return rc;
}

static int btreeReadWorkingCatalog(
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCatHash,
  ProllyHash *pCommitHash
){
  return btreeLoadWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
}

static int btreeWriteWorkingState(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pCatHash,
  const ProllyHash *pCommitHash
){
  ProllyHash stagedCatalog;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;
  ProllyHash preRebaseCat;
  ProllyHash rebaseOnto;
  ProllyHash constraintViolationsHash;
  char *zRebaseOrigBranch = 0;
  char *zRebaseReturnBranch = 0;
  u8 isMerging = 0;
  u8 isRebasing = 0;
  int rc;

  rc = btreeLoadWorkingSetBlob(cs, zBranch, 0, 0, &stagedCatalog, &isMerging,
                               &mergeCommitHash, &conflictsCatalogHash,
                               &isRebasing, &preRebaseCat, &rebaseOnto,
                               &zRebaseOrigBranch, &zRebaseReturnBranch,
                               &constraintViolationsHash);
  if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ){
    sqlite3_free(zRebaseOrigBranch);
    sqlite3_free(zRebaseReturnBranch);
    return rc;
  }
  if( rc==SQLITE_NOTFOUND ){
    memset(&stagedCatalog, 0, sizeof(ProllyHash));
    memset(&mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&conflictsCatalogHash, 0, sizeof(ProllyHash));
    memset(&preRebaseCat, 0, sizeof(ProllyHash));
    memset(&rebaseOnto, 0, sizeof(ProllyHash));
    memset(&constraintViolationsHash, 0, sizeof(ProllyHash));
    isMerging = 0;
    isRebasing = 0;
  }

  rc = btreeStoreWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                &stagedCatalog, isMerging,
                                &mergeCommitHash, &conflictsCatalogHash,
                                isRebasing, &preRebaseCat, &rebaseOnto,
                                zRebaseOrigBranch, zRebaseReturnBranch,
                                &constraintViolationsHash);
  sqlite3_free(zRebaseOrigBranch);
  sqlite3_free(zRebaseReturnBranch);
  return rc;
}

static int btreeReloadBranchWorkingState(Btree *p, int bLoadCatalog){
  BtShared *pBt = p->pBt;
  ProllyHash catHash;
  ProllyHash workingCommitHash;
  ProllyHash stagedCatalog;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;
  ProllyHash preRebaseCat;
  ProllyHash rebaseOnto;
  ProllyHash constraintViolationsHash;
  char *zRebaseOrigBranch = 0;
  char *zRebaseReturnBranch = 0;
  const char *zBr = p->zBranch ? p->zBranch : "main";
  u8 isMerging = 0;
  u8 isRebasing = 0;
  int hadUserCatalog = p->cat.n > 1;
  int rc;

  memset(&catHash, 0, sizeof(catHash));
  memset(&workingCommitHash, 0, sizeof(workingCommitHash));
  memset(&stagedCatalog, 0, sizeof(stagedCatalog));
  memset(&mergeCommitHash, 0, sizeof(mergeCommitHash));
  memset(&conflictsCatalogHash, 0, sizeof(conflictsCatalogHash));
  memset(&preRebaseCat, 0, sizeof(preRebaseCat));
  memset(&rebaseOnto, 0, sizeof(rebaseOnto));
  memset(&constraintViolationsHash, 0, sizeof(constraintViolationsHash));

  rc = btreeLoadWorkingSetBlob(
      &pBt->store, zBr, &catHash, &workingCommitHash, &stagedCatalog, &isMerging,
      &mergeCommitHash, &conflictsCatalogHash,
      &isRebasing, &preRebaseCat, &rebaseOnto, &zRebaseOrigBranch,
      &zRebaseReturnBranch,
      &constraintViolationsHash);
  if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(zRebaseOrigBranch);
    sqlite3_free(zRebaseReturnBranch);
    return rc;
  }
  if( prollyHashIsEmpty(&catHash) ){
    rc = btreeLoadBranchHeadCatalog(&pBt->store, zBr, &catHash,
                                    &workingCommitHash);
    if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zRebaseOrigBranch);
      sqlite3_free(zRebaseReturnBranch);
      return rc;
    }
  }

  if( bLoadCatalog
   && !prollyHashIsEmpty(&catHash)
   && (prollyHashIsEmpty(&p->committedCatalogHash)
       || prollyHashCompare(&catHash, &p->committedCatalogHash)!=0) ){
    u8 *catData = 0;
    int nCatData = 0;
    rc = chunkStoreGet(&pBt->store, &catHash, &catData, &nCatData);
    if( rc==SQLITE_OK && catData ){
      rc = deserializeCatalog(p, catData, nCatData);
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zRebaseOrigBranch);
        sqlite3_free(zRebaseReturnBranch);
        return rc;
      }
    }else{
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zRebaseOrigBranch);
        sqlite3_free(zRebaseReturnBranch);
        return rc;
      }
    }
  }
  if( prollyHashIsEmpty(&p->headCommit) || !hadUserCatalog ){
    p->headCommit = workingCommitHash;
  }

  p->stagedCatalog = stagedCatalog;
  p->isMerging = isMerging;
  p->mergeCommitHash = mergeCommitHash;
  p->conflictsCatalogHash = conflictsCatalogHash;
  p->isRebasing = isRebasing;
  p->preRebaseWorkingCat = preRebaseCat;
  p->rebaseOntoCommit = rebaseOnto;
  sqlite3_free(p->zRebaseOrigBranch);
  p->zRebaseOrigBranch = zRebaseOrigBranch;
  sqlite3_free(p->zRebaseReturnBranch);
  p->zRebaseReturnBranch = zRebaseReturnBranch;
  p->constraintViolationsHash = constraintViolationsHash;
  return SQLITE_OK;
}

static void btreeBumpDataVersion(Btree *p){
  p->iBDataVersion++;
  if( p->pBt->pPagerShim ){
    p->pBt->pPagerShim->iDataVersion++;
  }
}

static void btreeMarkWorkingStateChanged(Btree *p){
  BtShared *pBt = p->pBt;
  pBt->iWorkingStateVersion++;
  if( pBt->iWorkingStateVersion==0 ){
    pBt->iWorkingStateVersion = 1;
  }
  p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion;
  btreeBumpDataVersion(p);
}

static int btreeRefreshSharedWorkingState(Btree *p){
  BtShared *pBt = p->pBt;
  int rc;
  if( p->iLoadedWorkingStateVersion==pBt->iWorkingStateVersion ){
    return SQLITE_OK;
  }
  rc = btreeReloadBranchWorkingState(p, 1);
  if( rc!=SQLITE_OK ) return rc;
  p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion;
  btreeBumpDataVersion(p);
  return SQLITE_OK;
}

static int btreeRefreshFromDisk(Btree *p){
  BtShared *pBt = p->pBt;
  int bChanged = 0;
  u8 snapshotPinned = pBt->store.snapshotPinned;
  int bAutocommitBoundary = p->inTrans==TRANS_NONE
    && p->db && p->db->autoCommit && !p->db->pSavepoint;
  int rc;

  if( bAutocommitBoundary ){
    pBt->store.snapshotPinned = 0;
  }
  rc = chunkStoreRefreshIfChanged(&pBt->store, &bChanged);
  if( bAutocommitBoundary ){
    pBt->store.snapshotPinned = snapshotPinned;
  }
  if( rc!=SQLITE_OK ) return rc;
  if( !bChanged ) return SQLITE_OK;

  rc = btreeReloadBranchWorkingState(p, 1);
  if( rc!=SQLITE_OK ) return rc;

  btreeMarkWorkingStateChanged(p);

  return SQLITE_OK;
}

static int prollyBtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  BtShared *pBt = p->pBt;
  int rc;

  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }

  if( p->inTrans==TRANS_WRITE ){
    return SQLITE_OK;
  }

  if( p->inTrans==TRANS_READ && !wrFlag ){
    if( p->db && p->db->autoCommit && !p->db->pSavepoint ){
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      pBt->store.snapshotPinned = 0;
    }else{
      return SQLITE_OK;
    }
  }

  if( p->inTrans==TRANS_READ
   && wrFlag
   && p->iLoadedWorkingStateVersion!=pBt->iWorkingStateVersion ){
    return SQLITE_BUSY_SNAPSHOT;
  }

  rc = btreeRefreshFromDisk(p);
  if( rc!=SQLITE_OK ) return rc;
  if( p->inTrans==TRANS_NONE ){
    rc = btreeRefreshSharedWorkingState(p);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }

  if( wrFlag ){
    int nSavepointStart = p->nSavepoint;
    if( pBt->btsFlags & BTS_READ_ONLY ){
      return SQLITE_READONLY;
    }

    rc = chunkStoreLockAndRefresh(&pBt->store);
    if( rc!=SQLITE_OK ) return rc;

    if( p->inTrans==TRANS_READ ){
      int bChanged = 0;
      rc = chunkStoreHasExternalChanges(&pBt->store, &bChanged);
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        return rc;
      }
      if( bChanged ){
        chunkStoreUnlock(&pBt->store);
        return SQLITE_BUSY_SNAPSHOT;
      }
    }

    rc = btreeReloadBranchWorkingState(p, 1);
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(&pBt->store);
      return rc;
    }

    memset(&p->committedCatalogHash, 0, sizeof(ProllyHash));
    {
      const char *zBr = p->zBranch ? p->zBranch : "main";
      int rc2 = btreeReadWorkingCatalog(&pBt->store, zBr,
                                        &p->committedCatalogHash, 0);
      if( rc2!=SQLITE_OK && rc2!=SQLITE_NOTFOUND ){
        chunkStoreUnlock(&pBt->store);
        return rc2;
      }
      if( rc2==SQLITE_NOTFOUND || prollyHashIsEmpty(&p->committedCatalogHash) ){
        rc2 = btreeLoadBranchHeadCatalog(&pBt->store, zBr,
                                         &p->committedCatalogHash, 0);
      }
      if( rc2==SQLITE_NOTFOUND ){
        memset(&p->committedCatalogHash, 0, sizeof(ProllyHash));
      }else if( rc2!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        return rc2;
      }
    }
    p->committedStagedCatalog = p->stagedCatalog;
    p->committedIsMerging = p->isMerging;
    p->committedMergeCommitHash = p->mergeCommitHash;
    p->committedConflictsCatalogHash = p->conflictsCatalogHash;
    p->committedConstraintViolationsHash = p->constraintViolationsHash;

    if( p->db ){
      while( p->nSavepoint < p->db->nSavepoint ){
        int rc2 = pushSavepoint(p, 0);
        if( rc2!=SQLITE_OK ){
          while( p->nSavepoint > nSavepointStart ){
            p->nSavepoint--;
            freeSavepointTables(&p->aSavepointTables[p->nSavepoint]);
          }
          chunkStoreUnlock(&pBt->store);
          return rc2;
        }
      }
    }
    p->inTrans = TRANS_WRITE;
    p->inTransaction = TRANS_WRITE;
  } else {
    if( p->inTrans==TRANS_NONE ){
      p->inTrans = TRANS_READ;
      if( p->inTransaction==TRANS_NONE ){
        p->inTransaction = TRANS_READ;
      }
    }
  }

  pBt->store.snapshotPinned = 1;

  return SQLITE_OK;
}
int sqlite3BtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginTrans(p, wrFlag, pSchemaVersion);
}

static int prollyBtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  (void)p; (void)zSuperJrnl;
  return SQLITE_OK;
}
int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommitPhaseOne(p, zSuperJrnl);
}

static int prollyBtreeCommitPhaseTwo(Btree *p, int bCleanup){
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  int rcRollback = SQLITE_OK;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash catHash;
  (void)bCleanup;

  if( p->inTrans==TRANS_WRITE ){
    rc = flushAllPending(p, pBt, 0);
    if( rc!=SQLITE_OK ){
      chunkStoreRollback(&pBt->store);
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
      return rc;
    }

    {
      rc = serializeCatalog(p, &catData, &nCatData);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(catData);
        chunkStoreRollback(&pBt->store);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }

      {
        const char *zBr = p->zBranch ? p->zBranch : "main";
        rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                      &p->headCommit,
                                      &p->stagedCatalog, p->isMerging,
                                      &p->mergeCommitHash,
                                      &p->conflictsCatalogHash,
                                      p->isRebasing,
                                      &p->preRebaseWorkingCat,
                                      &p->rebaseOntoCommit,
                                      p->zRebaseOrigBranch,
                                      p->zRebaseReturnBranch,
                                      &p->constraintViolationsHash);
        if( rc!=SQLITE_OK ){
          sqlite3_free(catData);
          chunkStoreRollback(&pBt->store);
          chunkStoreUnlock(&pBt->store);
          pBt->store.snapshotPinned = 0;
          return rc;
        }
        rc = chunkStoreSerializeRefs(&pBt->store);
        if( rc!=SQLITE_OK ){
          sqlite3_free(catData);
          chunkStoreRollback(&pBt->store);
          chunkStoreUnlock(&pBt->store);
          pBt->store.snapshotPinned = 0;
          return rc;
        }
      }
    }

    rc = chunkStoreCommit(&pBt->store);
    if( rc==SQLITE_OK ){
      int bReloadSchema = p->bSchemaChangedTxn;
      ProllyHash runtimeMasterRoot;
      memset(&runtimeMasterRoot, 0, sizeof(runtimeMasterRoot));
      p->committedCatalogHash = catHash;
      p->committedStagedCatalog = p->stagedCatalog;
      p->committedIsMerging = p->isMerging;
      p->committedMergeCommitHash = p->mergeCommitHash;
      p->committedConflictsCatalogHash = p->conflictsCatalogHash;
      p->committedConstraintViolationsHash = p->constraintViolationsHash;
      btreeMarkWorkingStateChanged(p);
      if( bReloadSchema ){
        rc = buildRuntimeMasterRoot(p, &runtimeMasterRoot);
        if( rc!=SQLITE_OK ){
          sqlite3_free(catData);
          chunkStoreUnlock(&pBt->store);
          pBt->store.snapshotPinned = 0;
          return rc;
        }
        invalidateCursors(pBt, 0, SQLITE_ABORT);
        rc = deserializeCatalog(p, catData, nCatData);
        if( rc!=SQLITE_OK ){
          sqlite3_free(catData);
          chunkStoreUnlock(&pBt->store);
          pBt->store.snapshotPinned = 0;
          return rc;
        }
        {
          struct TableEntry *pMaster = findTable(p, 1);
          if( pMaster ) pMaster->root = runtimeMasterRoot;
        }
        invalidateSchema(p);
        if( p->db ){
          sqlite3ExpirePreparedStatements(p->db, 0);
          sqlite3ResetAllSchemasOfConnection(p->db);
        }
      }
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      p->nSavepoint = 0;
      p->bSchemaChangedTxn = 0;

      sqlite3_free(catData);
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
    }else{
      int rc2;
      sqlite3_free(catData);
      rc2 = restoreFromCommitted(p);
      if( rc2!=SQLITE_OK ){
        chunkStoreRollback(&pBt->store);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc2;
      }
      {
        BtCursor *pC;
        for(pC = pBt->pCursor; pC; pC = pC->pNext){
          if( pC->pBtree==p && pC->pMutMap ) prollyMutMapClear(pC->pMutMap);
        }
      }
      invalidateCursors(pBt, 0, rc);
      invalidateSchema(p);
      chunkStoreRollback(&pBt->store);
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      p->nSavepoint = 0;
      p->bSchemaChangedTxn = 0;
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
    }
    return rc;
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  p->bSchemaChangedTxn = 0;
  p->nSavepoint = 0;

  chunkStoreUnlock(&pBt->store);
  pBt->store.snapshotPinned = 0;

  return rc;
}
int sqlite3BtreeCommitPhaseTwo(Btree *p, int bCleanup){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommitPhaseTwo(p, bCleanup);
}

static int prollyBtreeCommit(Btree *p){
  int rc;
  rc = p->pOps->xCommitPhaseOne(p, 0);
  if( rc==SQLITE_OK ){
    rc = p->pOps->xCommitPhaseTwo(p, 0);
  }
  return rc;
}
int sqlite3BtreeCommit(Btree *p){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommit(p);
}

static int restoreFromCommitted(Btree *p){
  if( prollyHashIsEmpty(&p->committedCatalogHash) ){
    btreeFreeCatalogTables(p);
    initDefaultMeta(p);
    p->cat.iNextTable = 2;
  }else{
    u8 *catData = 0;
    int nCatData = 0;
    int rc = chunkStoreGet(&p->pBt->store, &p->committedCatalogHash,
                           &catData, &nCatData);
    if( rc!=SQLITE_OK ) return rc;
    rc = deserializeCatalog(p, catData, nCatData);
    sqlite3_free(catData);
    if( rc!=SQLITE_OK ) return rc;
  }
  p->stagedCatalog = p->committedStagedCatalog;
  p->isMerging = p->committedIsMerging;
  p->mergeCommitHash = p->committedMergeCommitHash;
  p->conflictsCatalogHash = p->committedConflictsCatalogHash;
  p->constraintViolationsHash = p->committedConstraintViolationsHash;
  return SQLITE_OK;
}

static int prollyBtreeRollback(Btree *p, int tripCode, int writeOnly){
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  (void)writeOnly;

  if( p->inTrans==TRANS_WRITE ){
    rc = restoreFromCommitted(p);
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
      return rc;
    }
    {
      BtCursor *pC;
      for(pC = pBt->pCursor; pC; pC = pC->pNext){
        if( pC->pBtree==p && pC->pMutMap ) prollyMutMapClear(pC->pMutMap);
      }
    }
    invalidateCursors(pBt, 0, tripCode ? tripCode : SQLITE_ABORT);
    invalidateSchema(p);
    chunkStoreRollback(&pBt->store);
    {
      u8 *catData = 0;
      int nCatData = 0;
      ProllyHash catHash;
      ProllyHash wsHashWouldBe;
      ProllyHash wsHashOnDisk;
      u8 wsBuf[WS_TOTAL_SIZE];
      int bMatchesDisk = 0;
      const char *zBr = p->zBranch ? p->zBranch : "main";

      rc = serializeCatalog(p, &catData, &nCatData);
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }
      prollyHashCompute(catData, nCatData, &catHash);

      btreeFillWorkingSetBlob(wsBuf, &catHash, &p->headCommit,
                              &p->stagedCatalog, p->isMerging,
                              &p->mergeCommitHash, &p->conflictsCatalogHash,
                              p->isRebasing, &p->preRebaseWorkingCat,
                              &p->rebaseOntoCommit,
                              p->zRebaseOrigBranch, p->zRebaseReturnBranch,
                              &p->constraintViolationsHash);
      prollyHashCompute(wsBuf, WS_TOTAL_SIZE, &wsHashWouldBe);

      if( chunkStoreGetBranchWorkingSet(&pBt->store, zBr, &wsHashOnDisk)
          ==SQLITE_OK
       && prollyHashCompare(&wsHashWouldBe, &wsHashOnDisk)==0 ){
        bMatchesDisk = 1;
      }

      if( !bMatchesDisk ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
        if( rc==SQLITE_OK ){
          rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                        &p->headCommit, &p->stagedCatalog,
                                        p->isMerging, &p->mergeCommitHash,
                                        &p->conflictsCatalogHash,
                                        p->isRebasing,
                                        &p->preRebaseWorkingCat,
                                        &p->rebaseOntoCommit,
                                        p->zRebaseOrigBranch,
                                        p->zRebaseReturnBranch,
                                        &p->constraintViolationsHash);
        }
        if( rc==SQLITE_OK ){
          rc = chunkStoreSerializeRefs(&pBt->store);
        }
        if( rc==SQLITE_OK ){
          rc = chunkStoreCommit(&pBt->store);
        }
      }
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }
    }
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  btreeDiscardAllSavepoints(p);

  chunkStoreUnlock(&pBt->store);
  pBt->store.snapshotPinned = 0;

  return rc;
}
int sqlite3BtreeRollback(Btree *p, int tripCode, int writeOnly){
  if( !p ) return SQLITE_OK;
  return p->pOps->xRollback(p, tripCode, writeOnly);
}

static int prollyBtreeBeginStmt(Btree *p, int iStatement){
  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  while( p->nSavepoint < iStatement ){
    int rc = pushSavepoint(p, 1);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}
int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginStmt(p, iStatement);
}

static int rollbackCommittedState(Btree *p, BtShared *pBt){
  int rc = restoreFromCommitted(p);
  if( rc!=SQLITE_OK ) return rc;
  invalidateCursors(pBt, 0, SQLITE_ABORT);
  invalidateSchema(p);
  return SQLITE_OK;
}

static int persistRolledBackSessionState(Btree *p, BtShared *pBt){
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash catHash;
  const char *zBr = p->zBranch ? p->zBranch : "main";
  int rc;

  rc = serializeCatalog(p, &catData, &nCatData);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
  }
  sqlite3_free(catData);
  if( rc!=SQLITE_OK ) return rc;
  rc = btreeStoreWorkingSetBlob(&pBt->store, zBr, &catHash,
                                &p->headCommit, &p->stagedCatalog,
                                p->isMerging, &p->mergeCommitHash,
                                &p->conflictsCatalogHash,
                                p->isRebasing,
                                &p->preRebaseWorkingCat,
                                &p->rebaseOntoCommit,
                                p->zRebaseOrigBranch,
                                p->zRebaseReturnBranch,
                                &p->constraintViolationsHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(&pBt->store);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(&pBt->store);
}

static void btreeDiscardAllSavepoints(Btree *p){
  int j;
  for(j=0; j<p->nSavepoint; j++){
    freeSavepointTables(&p->aSavepointTables[j]);
  }
  p->nSavepoint = 0;
}

static int rollbackAllSavepoints(Btree *p, BtShared *pBt){
  int rc;
  btreeDiscardAllSavepoints(p);
  rc = rollbackCommittedState(p, pBt);
  if( rc!=SQLITE_OK ) return rc;
  if( p->db && p->db->isTransactionSavepoint ){
    return persistRolledBackSessionState(p, pBt);
  }
  return SQLITE_OK;
}

static int rollbackNamedSavepoint(Btree *p, BtShared *pBt, int iSavepoint){
  struct SavepointTableState *pState = &p->aSavepointTables[iSavepoint];
  int j;
  int rc = rollbackMutMapsToSavepoint(p, iSavepoint + 1, iSavepoint);
  if( rc!=SQLITE_OK ) return rc;
  if( pState->aTables ){
    rc = restoreTablesFromSavepoint(p, pState);
    if( rc!=SQLITE_OK ) return rc;
  }
  restoreSavepointSessionState(p, pState);
  freeSavepointTables(pState);
  for(j=iSavepoint+1; j<p->nSavepoint; j++){
    freeSavepointTables(&p->aSavepointTables[j]);
  }
  p->nSavepoint = iSavepoint;
  invalidateCursors(pBt, 0, SQLITE_ABORT);
  invalidateSchema(p);
  return SQLITE_OK;
}

static int releaseSavepointsFrom(Btree *p, int iSavepoint){
  int j;
  releaseMutMapsToSavepoint(p, iSavepoint + 1);
  if( iSavepoint > 0 ){
    for(j=iSavepoint; j<p->nSavepoint; j++){
      int rc = inheritPendingSnapshots(&p->aSavepointTables[iSavepoint-1],
                                       &p->aSavepointTables[j]);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  for(j=iSavepoint; j<p->nSavepoint; j++){
    freeSavepointTables(&p->aSavepointTables[j]);
  }
  p->nSavepoint = iSavepoint;
  return SQLITE_OK;
}

static int prollyBtreeSavepoint(Btree *p, int op, int iSavepoint){
  BtShared *pBt;

  pBt = p->pBt;
  if( pBt==0 || p->inTrans!=TRANS_WRITE ){
    return SQLITE_OK;
  }

  if( op==SAVEPOINT_BEGIN ){
    while( p->nSavepoint < iSavepoint ){
      int rc = pushSavepoint(p, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
    return SQLITE_OK;
  }

  if( op==SAVEPOINT_ROLLBACK ){

    if( iSavepoint>=0 && iSavepoint<p->nSavepoint
     && p->aSavepointTables ){
      return rollbackNamedSavepoint(p, pBt, iSavepoint);
    } else if( iSavepoint>=0 && iSavepoint>=p->nSavepoint ){
      return rollbackCommittedState(p, pBt);
    } else if( iSavepoint<0 ){
      return rollbackAllSavepoints(p, pBt);
    }
  } else {

    if( iSavepoint>=0 && iSavepoint<p->nSavepoint ){
      return releaseSavepointsFrom(p, iSavepoint);
    }
  }

  return SQLITE_OK;
}
int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSavepoint(p, op, iSavepoint);
}

static int prollyBtreeTxnState(Btree *p){
  return (int)p->inTrans;
}
SQLITE_NOINLINE int sqlite3BtreeTxnState(Btree *p){
  if( p==0 ) return TRANS_NONE;
  return p->pOps->xTxnState(p);
}

static int prollyBtreeCreateTable(Btree *p, Pgno *piTable, int flags){
  struct TableEntry *pTE;
  Pgno iTable;

  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  iTable = p->cat.iNextTable;
  p->cat.iNextTable++;

  if( iTable > p->aMeta[BTREE_LARGEST_ROOT_PAGE] ){
    p->aMeta[BTREE_LARGEST_ROOT_PAGE] = iTable;
  }

  pTE = addTable(p, iTable, (u8)(flags & (BTREE_INTKEY|BTREE_BLOBKEY)));
  if( !pTE ){
    return SQLITE_NOMEM;
  }

  *piTable = iTable;
  return SQLITE_OK;
}
int sqlite3BtreeCreateTable(Btree *p, Pgno *piTable, int flags){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCreateTable(p, piTable, flags);
}

static int prollyBtreeDropTable(Btree *p, int iTable, int *piMoved){
  BtShared *pBt = p->pBt;

  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  if( iTable==1 ){
    struct TableEntry *pTE = findTable(p, 1);
    if( pTE ){
      memset(&pTE->root, 0, sizeof(ProllyHash));
    }
    if( piMoved ) *piMoved = 0;
    return SQLITE_OK;
  }

  invalidateCursors(pBt, (Pgno)iTable, SQLITE_ABORT);
  removeTable(p, (Pgno)iTable);

  if( piMoved ) *piMoved = 0;
  return SQLITE_OK;
}
int sqlite3BtreeDropTable(Btree *p, int iTable, int *piMoved){
  if( !p ) return SQLITE_OK;
  return p->pOps->xDropTable(p, iTable, piMoved);
}

static int prollyBtreeClearTable(Btree *p, int iTable, i64 *pnChange){
  BtShared *pBt = p->pBt;
  struct TableEntry *pTE;

  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  pTE = findTable(p, (Pgno)iTable);
  if( !pTE ){
    if( pnChange ) *pnChange = 0;
    return SQLITE_OK;
  }

  if( pnChange ){
    int rc = countTreeEntries(p, (Pgno)iTable, pnChange);
    if( rc!=SQLITE_OK ) return rc;
  }

  invalidateCursors(pBt, (Pgno)iTable, SQLITE_ABORT);
  memset(&pTE->root, 0, sizeof(ProllyHash));

  return SQLITE_OK;
}
int sqlite3BtreeClearTable(Btree *p, int iTable, i64 *pnChange){
  if( !p ) return SQLITE_OK;
  return p->pOps->xClearTable(p, iTable, pnChange);
}

static int prollyBtCursorClearTableOfCursor(BtCursor *pCur){
  return sqlite3BtreeClearTable(pCur->pBtree, (int)pCur->pgnoRoot, 0);
}
int sqlite3BtreeClearTableOfCursor(BtCursor *pCur){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xClearTableOfCursor(pCur);
}

static void prollyBtreeGetMeta(Btree *p, int idx, u32 *pValue){
  BtShared *pBt = p->pBt;
  assert( idx>=0 && idx<SQLITE_N_BTREE_META );

  if( idx==BTREE_DATA_VERSION ){
    if( pBt->pPagerShim ){
      *pValue = pBt->pPagerShim->iDataVersion;
    } else {
      *pValue = p->iBDataVersion;
    }
  } else {
    *pValue = p->aMeta[idx];
  }
}
void sqlite3BtreeGetMeta(Btree *p, int idx, u32 *pValue){
  if( !p ){ *pValue = 0; return; }
  p->pOps->xGetMeta(p, idx, pValue);
}

static int prollyBtreeUpdateMeta(Btree *p, int idx, u32 value){
  BtShared *pBt = p->pBt;

  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }
  if( idx<1 || idx>=SQLITE_N_BTREE_META ){
    return SQLITE_ERROR;
  }

  p->aMeta[idx] = value;

  if( idx==BTREE_SCHEMA_VERSION ){
    p->bSchemaChangedTxn = 1;
    p->iBDataVersion++;
    if( pBt->pPagerShim ){
      pBt->pPagerShim->iDataVersion++;
    }
  }

  return SQLITE_OK;
}
int sqlite3BtreeUpdateMeta(Btree *p, int idx, u32 value){
  if( !p ) return SQLITE_OK;
  return p->pOps->xUpdateMeta(p, idx, value);
}

static void *prollyBtreeSchema(Btree *p, int nBytes, void (*xFree)(void*)){
  if( !p->pSchema && nBytes>0 ){
    p->pSchema = sqlite3_malloc(nBytes);
    if( p->pSchema ){
      memset(p->pSchema, 0, nBytes);
      p->xFreeSchema = xFree;
    }
  }
  return p->pSchema;
}
void *sqlite3BtreeSchema(Btree *p, int nBytes, void (*xFree)(void*)){
  if( !p ) return 0;
  return p->pOps->xSchema(p, nBytes, xFree);
}

static int prollyBtreeSchemaLocked(Btree *p){
  (void)p;
  return 0;
}
int sqlite3BtreeSchemaLocked(Btree *p){
  if( !p ) return 0;
  return p->pOps->xSchemaLocked(p);
}

#ifndef SQLITE_OMIT_SHARED_CACHE
static int prollyBtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  (void)p; (void)iTab; (void)isWriteLock;
  return SQLITE_OK;
}
int sqlite3BtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  if( !p ) return SQLITE_OK;
  return p->pOps->xLockTable(p, iTab, isWriteLock);
}
#endif

int sqlite3BtreeCursorSize(void){
  return (int)sizeof(BtCursor);
}

void sqlite3BtreeCursorZero(BtCursor *p){
  memset(p, 0, sizeof(BtCursor));
  p->pCurOps = &prollyCursorOps;
}

static int prollyBtreeCursor(
  Btree *p,
  Pgno iTable,
  int wrFlag,
  struct KeyInfo *pKeyInfo,
  BtCursor *pCur
){
  BtShared *pBt = p->pBt;
  struct TableEntry *pTE;
  int rc;

  assert( p->inTrans>=TRANS_READ );

  if( p->db && p->db->autoCommit && !p->db->pSavepoint
   && p->inTrans!=TRANS_WRITE ){
    u8 oldSnapshotPinned = pBt->store.snapshotPinned;
    pBt->store.snapshotPinned = 0;
    rc = btreeRefreshFromDisk(p);
    pBt->store.snapshotPinned = oldSnapshotPinned;
    if( rc!=SQLITE_OK ) return rc;
    rc = btreeRefreshSharedWorkingState(p);
    if( rc!=SQLITE_OK ) return rc;
  }

  memset(pCur, 0, sizeof(BtCursor));
  pCur->pBtree = p;
  pCur->pBt = pBt;
  pCur->pgnoRoot = iTable;
  pCur->pKeyInfo = pKeyInfo;
  pCur->eState = CURSOR_INVALID;
  pCur->pCurOps = &prollyCursorOps;

  pTE = findTable(p, iTable);
  if( !pTE ){
    u8 flags = pKeyInfo ? BTREE_BLOBKEY : BTREE_INTKEY;
    pTE = addTable(p, iTable, flags);
    if( !pTE ) return SQLITE_NOMEM;
  }

  pCur->curIntKey = (pTE->flags & BTREE_INTKEY) ? 1 : 0;

  if( wrFlag & BTREE_WRCSR ){
    pCur->curFlags = BTCF_WriteFlag;
  }

  if( pTE->pPending ){
    pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
    if( wrFlag & BTREE_WRCSR ){
      pCur->flushSeekEdits = pTE->pendingFlushSeekEdits;
      if( !pCur->curIntKey
       && tableEntryIsTableRoot(p, pTE)
       && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        pCur->flushSeekEdits = 1;
      }
    }else{
      pCur->flushSeekEdits = pTE->pendingFlushSeekEdits;
    }
  }

  prollyCursorInit(&pCur->pCur, &pBt->store, &pBt->cache,
                    &pTE->root, pTE->flags);

  pCur->pNext = pBt->pCursor;
  pBt->pCursor = pCur;

  return SQLITE_OK;
}
int sqlite3BtreeCursor(
  Btree *p,
  Pgno iTable,
  int wrFlag,
  struct KeyInfo *pKeyInfo,
  BtCursor *pCur
){
  if( !p ) return SQLITE_MISUSE;
  return p->pOps->xCursor(p, iTable, wrFlag, pKeyInfo, pCur);
}

static int prollyBtCursorCloseCursor(BtCursor *pCur){
  BtShared *pBt;
  BtCursor **pp;
  if( !pCur ) return SQLITE_OK;
  pBt = pCur->pBt;
  if( !pBt ) return SQLITE_OK;

  if( pCur->pMutMap && pCur->flushSeekEdits ){
    struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE ){
      pTE->pendingFlushSeekEdits |= pCur->flushSeekEdits;
    }
  }
  pCur->pMutMap = 0;

  prollyCursorClose(&pCur->pCur);

  CLEAR_CACHED_PAYLOAD(pCur);
  if( pCur->pReconPayload ){
    sqlite3_free(pCur->pReconPayload);
    pCur->pReconPayload = 0;
    pCur->nReconPayloadAlloc = 0;
  }
  if( pCur->pSeekRecord ){
    sqlite3_free(pCur->pSeekRecord);
    pCur->pSeekRecord = 0;
    pCur->nSeekRecordAlloc = 0;
  }
  if( pCur->pSeekSortKey ){
    sqlite3_free(pCur->pSeekSortKey);
    pCur->pSeekSortKey = 0;
    pCur->nSeekSortKeyAlloc = 0;
  }
  if( pCur->pMovetoRec ){
    sqlite3_free(pCur->pMovetoRec);
    pCur->pMovetoRec = 0;
    pCur->nMovetoRecAlloc = 0;
  }

  if( pCur->pKey ){
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
  }

  for(pp=&pBt->pCursor; *pp; pp=&(*pp)->pNext){
    if( *pp==pCur ){
      *pp = pCur->pNext;
      break;
    }
  }

  pCur->pBt = 0;
  pCur->pBtree = 0;
  pCur->eState = CURSOR_INVALID;

  return SQLITE_OK;
}
int sqlite3BtreeCloseCursor(BtCursor *pCur){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCloseCursor(pCur);
}

static int prollyBtCursorCursorHasMoved(BtCursor *pCur){
  return (pCur->eState!=CURSOR_VALID);
}
int sqlite3BtreeCursorHasMoved(BtCursor *pCur){
  if( !pCur ) return 0;
  if( !pCur->pCurOps ) return (pCur->eState!=CURSOR_VALID);
  return pCur->pCurOps->xCursorHasMoved(pCur);
}

static int prollyBtCursorCursorRestore(BtCursor *pCur, int *pDifferentRow){
  int rc = SQLITE_OK;

  if( pCur->eState==CURSOR_VALID ){
    if( pDifferentRow ) *pDifferentRow = 0;
    return SQLITE_OK;
  }

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, pDifferentRow);
  } else if( pCur->eState==CURSOR_FAULT ){
    rc = pCur->skipNext;
    if( pDifferentRow ) *pDifferentRow = 1;
  } else {
    if( pDifferentRow ) *pDifferentRow = 1;
  }

  return rc;
}
int sqlite3BtreeCursorRestore(BtCursor *pCur, int *pDifferentRow){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCursorRestore(pCur, pDifferentRow);
}

#ifdef SQLITE_DEBUG
static int prollyBtreeClosesWithCursor(Btree *p, BtCursor *pCur){
  BtCursor *pX;
  if( !p || !p->pBt ) return 0;
  for(pX=p->pBt->pCursor; pX; pX=pX->pNext){
    if( pX==pCur ) return 1;
  }
  return 0;
}
int sqlite3BtreeClosesWithCursor(Btree *p, BtCursor *pCur){
  if( !p ) return 0;
  return p->pOps->xClosesWithCursor(p, pCur);
}
#endif

static int mergeCompare(BtCursor *pCur, ProllyMutMapEntry *e){
  if( pCur->curIntKey ){
    u64 tk = cursorCurrentTreeKeyPrefixInt(pCur);
    u64 ek = e->keyPrefix;
    if( tk < ek ) return -1;
    if( tk > ek ) return 1;
    return 0;
  }else{
    const u8 *pK; int nK;
    int n; int c;
    prollyCursorKey(&pCur->pCur, &pK, &nK);
    n = nK < e->nKey ? nK : e->nKey;
    c = memcmp(pK, e->pKey, n);
    if( c ) return c;
    return (nK < e->nKey) ? -1 : (nK > e->nKey) ? 1 : 0;
  }
}

static int mergeScan(BtCursor *pCur, int dir, int *pRes){
  if( pCur->mmPhysActive ){
    pCur->mmIdx = prollyMutMapOrderIndexFromEntry(
        pCur->pMutMap, &pCur->pMutMap->aEntries[pCur->mmPhysIdx]);
    pCur->mmPhysIdx = -1;
    pCur->mmPhysActive = 0;
  }
  for(;;){
    int treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
    int mutOk  = (pCur->mmIdx >= 0 && pCur->mmIdx < pCur->pMutMap->nEntries);
    ProllyMutMapEntry *e;
    int cmp;

    if( !treeOk && !mutOk ){
      if( pRes ){ *pRes = 1; return SQLITE_OK; }
      return SQLITE_DONE;
    }
    if( !mutOk ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
    e = orderedMutMapEntryAt(pCur->pMutMap, pCur->mmIdx);
    if( !treeOk ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx += dir; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
    cmp = mergeCompare(pCur, e);
    if( cmp*dir < 0 ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }else if( cmp*dir > 0 ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx += dir; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }else{
      if( e->op==PROLLY_EDIT_DELETE ){
        pCur->mmIdx += dir;
        cmp = advanceTreeCursor(pCur, dir);
        if( cmp!=SQLITE_OK ) return cmp;
        continue;
      }
      pCur->mergeSrc = MERGE_SRC_BOTH;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
  }
}

static void cursorNormalizeMmPhys(BtCursor *pCur){
  if( pCur->mmPhysActive ){
    pCur->mmIdx = prollyMutMapOrderIndexFromEntry(
        pCur->pMutMap, &pCur->pMutMap->aEntries[pCur->mmPhysIdx]);
    pCur->mmPhysIdx = -1;
    pCur->mmPhysActive = 0;
  }
}

static int materializeDeferredTreeSeek(BtCursor *pCur, int dir){
  int rc;
  int res = 0;
  if( !pCur->deferredTreeSeek ) return SQLITE_OK;
  pCur->deferredTreeSeek = 0;
  refreshCursorRoot(pCur);
  rc = prollyCursorSeekInt(&pCur->pCur, pCur->cachedIntKey, &res);
  if( rc!=SQLITE_OK ) return rc;
  if( res==0 ){
    pCur->mergeSrc = MERGE_SRC_BOTH;
  }else{
    pCur->mergeSrc = MERGE_SRC_MUT;
    if( dir>0 && res<0 && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyCursorNext(&pCur->pCur);
    }else if( dir<0 && res>0 && prollyCursorIsValid(&pCur->pCur) ){
      rc = prollyCursorPrev(&pCur->pCur);
    }
  }
  return rc;
}

static int mergeStepForward(BtCursor *pCur){
  int rc = SQLITE_OK;
  cursorNormalizeMmPhys(pCur);
  rc = materializeDeferredTreeSeek(pCur, 1);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    rc = advanceTreeCursor(pCur, 1);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH )
    pCur->mmIdx++;
  return mergeScan(pCur, 1, 0);
}

static int mergeStepBackward(BtCursor *pCur){
  int rc = SQLITE_OK;
  cursorNormalizeMmPhys(pCur);
  rc = materializeDeferredTreeSeek(pCur, -1);
  if( rc!=SQLITE_OK ) return rc;
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    rc = advanceTreeCursor(pCur, -1);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH )
    pCur->mmIdx--;
  return mergeScan(pCur, -1, 0);
}

static int seedMutMapIterFromCursor(
  BtCursor *pCur,
  ProllyMutMapIter *pIt
){
  if( pCur->curIntKey ){
    if( pCur->curFlags & BTCF_ValidNKey ){
      prollyMutMapIterSeek(pIt, pCur->pMutMap, 0, 0, pCur->cachedIntKey);
      return SQLITE_OK;
    }
  }else{
    if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
      u8 *pSortKey = 0;
      int nSortKey = 0;
      int nMutKeyField = 0;
      int rc;
      if( pCur->pKeyInfo
       && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
        nMutKeyField = (int)pCur->pKeyInfo->nKeyField;
      }
      rc = sortKeyFromRecordPrefixColl(pCur->pCachedPayload, pCur->nCachedPayload,
                                        nMutKeyField, pCur->pKeyInfo,
                                        &pSortKey, &nSortKey);
      if( rc!=SQLITE_OK ) return rc;
      prollyMutMapIterSeek(pIt, pCur->pMutMap, pSortKey, nSortKey, 0);
      sqlite3_free(pSortKey);
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

static int mergeFirst(BtCursor *pCur, int *pRes){
  pCur->mergeSrc = MERGE_SRC_TREE;
  pCur->mmIdx = 0;
  return mergeScan(pCur, 1, pRes);
}

static int mergeLast(BtCursor *pCur, int *pRes){
  pCur->mmIdx = pCur->pMutMap->nEntries - 1;
  pCur->mergeSrc = MERGE_SRC_TREE;
  return mergeScan(pCur, -1, pRes);
}

static int prollyBtCursorFirst(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  refreshCursorRoot(pCur);
  rc = prollyCursorFirst(&pCur->pCur, pRes);
  if( rc!=SQLITE_OK ) return rc;
  clearMergeCursorState(pCur);

  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    pCur->mmActive = 1;
    rc = mergeFirst(pCur, pRes);
  }else{
    pCur->mmActive = 0;
  }
  pCur->eState = (*pRes==0) ? CURSOR_VALID : CURSOR_INVALID;
  pCur->curFlags &= ~BTCF_AtLast;
  return rc;
}
int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xFirst(pCur, pRes);
}

static int prollyBtCursorLast(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);
  refreshCursorRoot(pCur);
  rc = prollyCursorLast(&pCur->pCur, pRes);
  if( rc!=SQLITE_OK ) return rc;
  clearMergeCursorState(pCur);

  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    pCur->mmActive = 1;
    rc = mergeLast(pCur, pRes);
  }else{
    pCur->mmActive = 0;
  }
  if( *pRes==0 ){
    pCur->eState = CURSOR_VALID;
    pCur->curFlags |= BTCF_AtLast;
  } else {
    pCur->eState = CURSOR_INVALID;
  }
  return rc;
}
int sqlite3BtreeLast(BtCursor *pCur, int *pRes){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xLast(pCur, pRes);
}

static int prollyBtCursorNext(BtCursor *pCur, int flags){
  int rc;
  (void)flags;
  CLEAR_CACHED_PAYLOAD(pCur);

  if( pCur->eState==CURSOR_INVALID ){
    return SQLITE_DONE;
  }

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, 0);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->eState==CURSOR_INVALID ){
      return SQLITE_DONE;
    }
  }

  if( pCur->eState==CURSOR_SKIPNEXT ){
    pCur->eState = CURSOR_VALID;
    if( pCur->skipNext>0 ){
      pCur->skipNext = 0;
      return SQLITE_OK;
    }
    pCur->skipNext = 0;
  }

  if( !pCur->mmActive && pCur->pMutMap==0 ){
    rc = prollyCursorNextFastLeaf(&pCur->pCur);
    if( rc==SQLITE_OK ){
      if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
        pCur->eState = CURSOR_VALID;
        if( pCur->curIntKey ){
          cacheCurrentTreePayloadIfIntKey(pCur);
        }else{
          cacheCurrentTreeStoredPayloadNonIntKey(pCur);
        }
      } else {
        pCur->eState = CURSOR_INVALID;
        return SQLITE_DONE;
      }
    }
    pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey);
    return rc;
  }

  if( pCur->mmActive ){
    rc = mergeStepForward(pCur);
    if( rc==SQLITE_DONE ){
      pCur->eState = CURSOR_INVALID;
    }else if( rc==SQLITE_OK ){
      pCur->eState = CURSOR_VALID;
    }
  }else{

    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      ProllyMutMapIter it;
      rc = SQLITE_OK;
      if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                             prollyCursorIntKey(&pCur->pCur));
      }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pK; int nK;
        prollyCursorKey(&pCur->pCur, &pK, &nK);
        prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
      }else if( pCur->eState==CURSOR_VALID ){
        rc = seedMutMapIterFromCursor(pCur, &it);
        if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;
        if( rc==SQLITE_NOTFOUND ){
          prollyMutMapIterFirst(&it, pCur->pMutMap);
        }
      }else{
        prollyMutMapIterFirst(&it, pCur->pMutMap);
      }
      pCur->mmIdx = it.idx;
      pCur->mmActive = 1;

      if( it.idx >= 0 && it.idx < pCur->pMutMap->nEntries
       && prollyCursorIsValid(&pCur->pCur)
       && mergeCompare(pCur, prollyMutMapEntryAt(pCur->pMutMap, it.idx))==0 ){
        pCur->mergeSrc = MERGE_SRC_BOTH;
      }else if( !prollyCursorIsValid(&pCur->pCur) ){
        pCur->mergeSrc = MERGE_SRC_MUT;
      }else{
        pCur->mergeSrc = MERGE_SRC_TREE;
      }
      rc = mergeStepForward(pCur);
      if( rc==SQLITE_DONE ){
        pCur->eState = CURSOR_INVALID;
      }else if( rc==SQLITE_OK ){
        pCur->eState = CURSOR_VALID;
      }
    }else{
      rc = prollyCursorNextFastLeaf(&pCur->pCur);
      if( rc==SQLITE_OK ){
        if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
          pCur->eState = CURSOR_VALID;
          if( pCur->curIntKey ){
            cacheCurrentTreePayloadIfIntKey(pCur);
          }else{
            cacheCurrentTreeStoredPayloadNonIntKey(pCur);
          }
        } else {
          pCur->eState = CURSOR_INVALID;
          return SQLITE_DONE;
        }
      }
    }
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey);
  return rc;
}
int sqlite3BtreeNext(BtCursor *pCur, int flags){
  if( !pCur ) return SQLITE_DONE;
  return pCur->pCurOps->xNext(pCur, flags);
}

static int prollyBtCursorPrevious(BtCursor *pCur, int flags){
  int rc;
  (void)flags;
  CLEAR_CACHED_PAYLOAD(pCur);

  if( pCur->eState==CURSOR_INVALID ){
    return SQLITE_DONE;
  }

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, 0);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->eState==CURSOR_INVALID ){
      return SQLITE_DONE;
    }
  }

  if( pCur->eState==CURSOR_SKIPNEXT ){
    pCur->eState = CURSOR_VALID;
    if( pCur->skipNext<0 ){
      pCur->skipNext = 0;
      return SQLITE_OK;
    }
    pCur->skipNext = 0;
  }

  if( pCur->mmActive ){
    rc = mergeStepBackward(pCur);
    if( rc==SQLITE_DONE ){
      pCur->eState = CURSOR_INVALID;
    }else if( rc==SQLITE_OK ){
      pCur->eState = CURSOR_VALID;
    }
  }else{
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      ProllyMutMapIter it;
      rc = SQLITE_OK;
      if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                             prollyCursorIntKey(&pCur->pCur));
      }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pK; int nK;
        prollyCursorKey(&pCur->pCur, &pK, &nK);
        prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
      }else if( pCur->eState==CURSOR_VALID ){
        rc = seedMutMapIterFromCursor(pCur, &it);
        if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;
        if( rc==SQLITE_NOTFOUND ){
          prollyMutMapIterLast(&it, pCur->pMutMap);
        }
      }else{
        prollyMutMapIterLast(&it, pCur->pMutMap);
      }
      pCur->mmIdx = it.idx;
      pCur->mmActive = 1;

      if( it.idx >= 0 && it.idx < pCur->pMutMap->nEntries
       && prollyCursorIsValid(&pCur->pCur)
       && mergeCompare(pCur, prollyMutMapEntryAt(pCur->pMutMap, it.idx))==0 ){
        pCur->mergeSrc = MERGE_SRC_BOTH;
      }else if( !prollyCursorIsValid(&pCur->pCur) ){
        pCur->mergeSrc = MERGE_SRC_MUT;
      }else{
        pCur->mergeSrc = MERGE_SRC_TREE;
      }
      rc = mergeStepBackward(pCur);
      if( rc==SQLITE_DONE ){
        pCur->eState = CURSOR_INVALID;
      }else if( rc==SQLITE_OK ){
        pCur->eState = CURSOR_VALID;
      }
    }else{
      rc = prollyCursorPrev(&pCur->pCur);
      if( rc==SQLITE_OK ){
        if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
          pCur->eState = CURSOR_VALID;
        } else {
          pCur->eState = CURSOR_INVALID;
          return SQLITE_DONE;
        }
      }
    }
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey);
  return rc;
}
int sqlite3BtreePrevious(BtCursor *pCur, int flags){
  if( !pCur ) return SQLITE_DONE;
  return pCur->pCurOps->xPrevious(pCur, flags);
}

static int prollyBtCursorEof(BtCursor *pCur){
  return (pCur->eState!=CURSOR_VALID);
}
int sqlite3BtreeEof(BtCursor *pCur){
  if( !pCur ) return 1;
  return pCur->pCurOps->xEof(pCur);
}

static int prollyBtCursorIsEmpty(BtCursor *pCur, int *pRes){
  struct TableEntry *pTE;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    *pRes = 1;
  } else {
    *pRes = prollyHashIsEmpty(&pTE->root) ? 1 : 0;
  }
  return SQLITE_OK;
}
int sqlite3BtreeIsEmpty(BtCursor *pCur, int *pRes){
  if( !pCur ) { *pRes = 1; return SQLITE_OK; }
  return pCur->pCurOps->xIsEmpty(pCur, pRes);
}

static int prollyBtCursorTableMoveto(
  BtCursor *pCur,
  i64 intKey,
  int bias,
  int *pRes
){
  int rc;
  (void)bias;

  assert( pCur->curIntKey );

  pCur->nSeek++;
  if( pCur->pBtree ) pCur->pBtree->nSeek++;
  clearMergeCursorState(pCur);
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);

  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapEntry *pEntry = 0;
    rc = prollyMutMapFindRc(pCur->pMutMap, 0, 0, intKey, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry ){
      if( pEntry->op == PROLLY_EDIT_INSERT ){
        *pRes = 0;
        setCursorToMutMapEntryPhys(pCur, (int)(pEntry - pCur->pMutMap->aEntries));
        pCur->deferredTreeSeek = 1;
        return SQLITE_OK;
      } else {

        *pRes = 1;
        pCur->eState = CURSOR_INVALID;
        return SQLITE_OK;
      }
    }

  }

  refreshCursorRoot(pCur);

  rc = prollyCursorSeekInt(&pCur->pCur, intKey, pRes);
  if( rc==SQLITE_OK ){
    if( *pRes==0 ){
      pCur->eState = CURSOR_VALID;
      pCur->curFlags |= BTCF_ValidNKey;
      pCur->cachedIntKey = intKey;
      CLEAR_CACHED_PAYLOAD(pCur);
      cacheCurrentTreePayloadIfIntKey(pCur);
    } else if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      pCur->eState = CURSOR_VALID;
      pCur->curFlags &= ~BTCF_ValidNKey;
      cacheCurrentTreePayloadIfIntKey(pCur);
    } else {
      pCur->eState = CURSOR_INVALID;
    }
  }
  return rc;
}
int sqlite3BtreeTableMoveto(
  BtCursor *pCur,
  i64 intKey,
  int bias,
  int *pRes
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xTableMoveto(pCur, intKey, bias, pRes);
}

static u32 btreeSerialType(Mem *pMem, u32 *pLen){
  int flags = pMem->flags;
  if( flags & MEM_Null ){ *pLen = 0; return SERIAL_TYPE_NULL; }
  if( flags & MEM_Int ){
    i64 v = pMem->u.i;
    if( v==0 ){ *pLen = 0; return SERIAL_TYPE_ZERO; }
    if( v==1 ){ *pLen = 0; return SERIAL_TYPE_ONE; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return SERIAL_TYPE_INT8; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return SERIAL_TYPE_INT16; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return SERIAL_TYPE_INT24; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return SERIAL_TYPE_INT32; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return SERIAL_TYPE_INT48; }
    *pLen = 8; return SERIAL_TYPE_INT64;
  }
  if( flags & MEM_Real ){ *pLen = 8; return SERIAL_TYPE_FLOAT64; }
  if( flags & MEM_Str ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + SERIAL_TYPE_TEXT_BASE;
  }
  if( flags & MEM_Blob ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + SERIAL_TYPE_BLOB_BASE;
  }
  *pLen = 0; return SERIAL_TYPE_NULL;
}

static int findMatchingMutMapEntry(
  ProllyMutMap *pMap,
  KeyInfo *pKeyInfo,
  UnpackedRecord *pIdxKey,
  const u8 *pSortKey,
  int nSortKey,
  ProllyMutMapEntry **ppMatch,
  int *pCmp
){
  int rc = SQLITE_OK;
  int cmp = 0;
  ProllyMutMapEntry *pMatch = 0;
  u8 *pRecBuf = 0;
  int nRecBufAlloc = 0;
  int lo = 0, found = 0;

  *ppMatch = 0;
  *pCmp = 0;
  if( !pMap || prollyMutMapIsEmpty(pMap) ){
    return SQLITE_OK;
  }

  if( pKeyInfo
   && pIdxKey->nField >= pKeyInfo->nAllField ){
    ProllyMutMapEntry *pEntry = 0;
    rc = prollyMutMapFindRc(pMap, pSortKey, nSortKey, 0, &pEntry);
    if( rc!=SQLITE_OK ) return rc;
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      *ppMatch = pEntry;
    }
    return SQLITE_OK;
  }

  rc = prollyMutMapResolveSortedPos(pMap, pSortKey, nSortKey, 0,
                                    &lo, &found);

  while( rc==SQLITE_OK && lo < pMap->nEntries ){
    ProllyMutMapEntry *pEntry = prollyMutMapEntryAt(pMap, lo);
    const u8 *pRec = pEntry->pVal;
    int nRec = pEntry->nVal;
    int cmpLen;
    int prefixCmp;

    if( pMap->isIntKey ){
      lo++;
      continue;
    }
    cmpLen = pEntry->nKey < nSortKey ? pEntry->nKey : nSortKey;
    prefixCmp = memcmp(pEntry->pKey, pSortKey, cmpLen);
    if( prefixCmp>0 || (prefixCmp==0 && pEntry->nKey < nSortKey) ){
      break;
    }
    if( nRec==0 ){
      rc = recordFromSortKeyBuffer(pEntry->pKey, pEntry->nKey,
                                    &pRecBuf, &nRecBufAlloc, &nRec);
      if( rc!=SQLITE_OK ) break;
      pRec = pRecBuf;
    }
    pIdxKey->eqSeen = 0;
    cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
    if( cmp!=0 && !pIdxKey->eqSeen ){
      break;
    }
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      pMatch = pEntry;
      break;
    }
    lo++;
  }

  sqlite3_free(pRecBuf);
  if( rc==SQLITE_OK && pMatch ){
    *ppMatch = pMatch;
    *pCmp = cmp;
  }
  return rc;
}

static int prollyBtCursorIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  int rc;

  assert( !pCur->curIntKey );

  if( pCur->pBtree ) pCur->pBtree->nSeek++;

  clearMergeCursorState(pCur);
  CLEAR_CACHED_PAYLOAD(pCur);
  CLEAR_CACHED_SEEK_KEY(pCur);

  refreshCursorRoot(pCur);

  {
    int treeFound = 0, mutFound = 0;
    int treeCmp = 0, mutCmp = 0;
    const u8 *mutKey = 0;
    int mutNKey = 0;
    ProllyMutMapEntry *mutE = 0;
    int mutFromCursorMap = 0;

    u8 *pSerKey = 0;
    int nSerKey = 0;
    u8 *pSortKey = 0;
    int nSortKey = 0;
    int nSeekKeyField = 0;
    if( pCur->pKeyInfo && pIdxKey->nField < pCur->pKeyInfo->nAllField ){
      nSeekKeyField = (int)pIdxKey->nField;
    }
    if( unpackedRecordCanUseIntSortKey(
            pCur, pIdxKey,
            nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField) ){
      rc = sortKeyFromUnpackedIntRecordBuffer(
          pIdxKey, nSeekKeyField>0 ? nSeekKeyField : (int)pIdxKey->nField,
          &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
    }else{
      rc = sortKeyFromMemPrefixCollBuffer(
          pIdxKey->aMem, (int)pIdxKey->nField, nSeekKeyField,
          pCur->pKeyInfo,
          &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
      if( rc==SQLITE_NOTFOUND ){
        rc = serializeUnpackedRecordBuffer(
            pIdxKey, &pCur->pSeekRecord, &pCur->nSeekRecordAlloc, &nSerKey);
        if( rc!=SQLITE_OK ) return rc;
        pSerKey = pCur->pSeekRecord;
        rc = sortKeyFromRecordPrefixCollBuffer(
            pSerKey, nSerKey, nSeekKeyField, pCur->pKeyInfo,
            &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
      }
    }
    if( rc!=SQLITE_OK ) return rc;
    pSortKey = pCur->pSeekSortKey;
    pCur->nSeekSortKey = nSortKey;
    pCur->nSeekKeyField = nSeekKeyField;

    if( pCur->pKeyInfo
     && pIdxKey->nField >= pCur->pKeyInfo->nAllField ){
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      ProllyMutMap *pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
      ProllyMutMapEntry *pEntry = 0;
      if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        rc = prollyMutMapFindRc(pCur->pMutMap, pSortKey, nSortKey, 0, &pEntry);
        if( rc!=SQLITE_OK ) return rc;
        if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
          setCursorToMutMapEntryPhys(
              pCur, (int)(pEntry - pCur->pMutMap->aEntries));
          *pRes = 0;
          pIdxKey->eqSeen = 1;
          return SQLITE_OK;
        }
      }
      if( pPending && pPending!=pCur->pMutMap
       && !prollyMutMapIsEmpty(pPending) ){
        rc = prollyMutMapFindRc(pPending, pSortKey, nSortKey, 0, &pEntry);
        if( rc!=SQLITE_OK ) return rc;
        if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
          if( pEntry->nVal>0 && pEntry->pVal ){
            rc = cacheCursorPayloadCopy(pCur, pEntry->pVal, pEntry->nVal);
          }else{
            rc = cacheCursorPayloadReconstructed(
                pCur, pEntry->pKey, pEntry->nKey);
          }
          if( rc!=SQLITE_OK ) return rc;
          pCur->eState = CURSOR_VALID;
          *pRes = 0;
          pIdxKey->eqSeen = 1;
          return SQLITE_OK;
        }
      }
    }

    rc = prollyCursorSeekBlob(&pCur->pCur, pSortKey, nSortKey, &(int){0});
    if( rc==SQLITE_OK && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      int iLevel = pCur->pCur.iLevel;
      ProllyCacheEntry *pLeaf = pCur->pCur.aLevel[iLevel].pEntry;
      int seekIdx = pCur->pCur.aLevel[iLevel].idx;
      int nItems = pLeaf->node.nItems;

      int bestIdx = -1;
      int bestCmp = 0;
      {

        int lo = seekIdx;
        u8 *pRecBuf = pCur->pMovetoRec;
        int nRecBufAlloc = pCur->nMovetoRecAlloc;
        int i;

        for( i = lo; i < nItems; i++ ){
          const u8 *pSK; int nSK;
          const u8 *pVal; int nVal;
          int recCmp;
          prollyNodeKey(&pLeaf->node, i, &pSK, &nSK);

          {
            int cmpLen = nSK < nSortKey ? nSK : nSortKey;
            int prefixCmp = memcmp(pSK, pSortKey, cmpLen);
            if( prefixCmp > 0 ){
              if( bestIdx < 0 ){
                int isDeleted = 0;
                if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
                  ProllyMutMapEntry *mmE = 0;
                  rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
                  if( rc!=SQLITE_OK ) break;
                  if( mmE && mmE->op==PROLLY_EDIT_DELETE ) isDeleted = 1;
                }
                if( !isDeleted ){
                  bestIdx = i;
                  if( nSeekKeyField>0 ){
                    pIdxKey->eqSeen = 0;
                    bestCmp = 1;
                  }else{
                    const u8 *pVal2; int nVal2;
                    prollyNodeValue(&pLeaf->node, i, &pVal2, &nVal2);
                    if( nVal2==0 ){
                      rc = recordFromSortKeyBuffer(
                          pSK, nSK, &pRecBuf, &nRecBufAlloc, &nVal2);
                      if( rc!=SQLITE_OK ) break;
                      pVal2 = pRecBuf;
                    }
                    pIdxKey->eqSeen = 0;
                    bestCmp = sqlite3VdbeRecordCompare(nVal2, pVal2, pIdxKey);
                  }
                }
              }
              break;
            }
            if( nSeekKeyField>0 && prefixCmp==0 && nSK>=nSortKey ){
              if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
                ProllyMutMapEntry *mmE = 0;
                rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
                if( rc!=SQLITE_OK ) break;
                if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                  continue;
                }
              }
              pIdxKey->eqSeen = 1;
              bestIdx = i;
              bestCmp = pIdxKey->default_rc;
              treeFound = 1;
              treeCmp = bestCmp;
              if( pIdxKey->default_rc < 0 ){
                continue;
              }
              break;
            }
          }
          prollyNodeValue(&pLeaf->node, i, &pVal, &nVal);
          if( nVal==0 ){
            rc = recordFromSortKeyBuffer(
                pSK, nSK, &pRecBuf, &nRecBufAlloc, &nVal);
            if( rc!=SQLITE_OK ) break;
            pVal = pRecBuf;
          }
          pIdxKey->eqSeen = 0;
          recCmp = sqlite3VdbeRecordCompare(nVal, pVal, pIdxKey);

          if( recCmp==0 || pIdxKey->eqSeen ){
            if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
              ProllyMutMapEntry *mmE = 0;
              rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
              if( rc!=SQLITE_OK ) break;
              if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                continue;
              }
            }
            bestIdx = i;
            bestCmp = recCmp;
            treeFound = 1;
            treeCmp = recCmp;
            break;
          } else if( recCmp > 0 ){
            if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
              ProllyMutMapEntry *mmE = 0;
              rc = prollyMutMapFindRc(pCur->pMutMap, pSK, nSK, 0, &mmE);
              if( rc!=SQLITE_OK ) break;
              if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
                continue;
              }
            }
            if( bestIdx < 0 ){
              bestIdx = i;
              bestCmp = recCmp;
            }
          }
        }
        pCur->pMovetoRec = pRecBuf;
        pCur->nMovetoRecAlloc = nRecBufAlloc;
        if( rc!=SQLITE_OK ) return rc;
      }

      if( treeFound ){

        pCur->pCur.aLevel[iLevel].idx = bestIdx;
      } else if( bestIdx >= 0 ){

        pCur->pCur.aLevel[iLevel].idx = bestIdx;
        treeCmp = bestCmp;
        treeFound = 1;
      }

    }

    {
      struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
      ProllyMutMap *pPending = pTE ? (ProllyMutMap*)pTE->pPending : 0;
      if( ((pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap))
         || (pPending && pPending!=pCur->pMutMap
             && !prollyMutMapIsEmpty(pPending)))
       && !(treeFound && treeCmp==0) ){
      int savedEqSeen = pIdxKey->eqSeen;
      rc = findMatchingMutMapEntry((ProllyMutMap*)pCur->pMutMap,
                                   pCur->pKeyInfo,
                                   pIdxKey, pSortKey, nSortKey,
                                   &mutE, &mutCmp);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      if( mutE ) mutFromCursorMap = 1;
      if( !mutE && pPending && pPending!=pCur->pMutMap ){
        rc = findMatchingMutMapEntry(pPending,
                                     pCur->pKeyInfo,
                                     pIdxKey, pSortKey, nSortKey,
                                     &mutE, &mutCmp);
        if( rc!=SQLITE_OK ){
          return rc;
        }
      }
      if( mutE ){

        const u8 *pMutVal = mutE->pVal;
        int nMutVal = mutE->nVal;
        if( mutFromCursorMap && nMutVal==0 ){
          mutFound = 1;
        }else if( nMutVal==0 ){
          rc = recordFromSortKeyBuffer(
              mutE->pKey, mutE->nKey,
              &pCur->pMovetoRec, &pCur->nMovetoRecAlloc, &nMutVal);
          if( rc!=SQLITE_OK ) return rc;
          pMutVal = pCur->pMovetoRec;
        }
        if( pMutVal ){
          mutKey = pMutVal;
          mutNKey = nMutVal;
          mutFound = 1;
        }
      }
      pIdxKey->eqSeen = savedEqSeen;
    }
    }

      if( mutFound && (!treeFound || treeCmp!=0) ){
      if( mutFromCursorMap ){
        setCursorToMutMapEntryPhys(
            pCur, (int)(mutE - pCur->pMutMap->aEntries));
      }else{
        rc = cacheCursorPayloadCopy(pCur, mutKey, mutNKey);
        if( rc!=SQLITE_OK ){
          return rc;
        }
        pCur->eState = CURSOR_VALID;
      }
      *pRes = mutCmp;
      pIdxKey->eqSeen = 1;
      return SQLITE_OK;
    }
    if( treeFound ){
      *pRes = treeCmp;
      pCur->eState = CURSOR_VALID;
      cacheCurrentTreeStoredPayloadNonIntKey(pCur);
      return SQLITE_OK;
    }
  }

no_match:

  {
    int lastRes = 0;
    rc = prollyCursorLast(&pCur->pCur, &lastRes);
    if( rc!=SQLITE_OK ) return rc;
    if( lastRes==0 ){
      pCur->eState = CURSOR_VALID;
      *pRes = -1;
      cacheCurrentTreeStoredPayloadNonIntKey(pCur);
    } else {
      pCur->eState = CURSOR_INVALID;
      *pRes = -1;
    }
  }
  return SQLITE_OK;
}
int sqlite3BtreeIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xIndexMoveto(pCur, pIdxKey, pRes);
}

static int cachedSeekKeyMatchesCurrent(BtCursor *pCur){
  const u8 *pKey = 0;
  int nKey = 0;

  if( !pCur || pCur->curIntKey
   || pCur->nSeekSortKey<=0 || pCur->nSeekKeyField!=0 ){
    return 0;
  }
  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
    if( !e ) return 0;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }
  return pKey && nKey==pCur->nSeekSortKey
      && memcmp(pKey, pCur->pSeekSortKey, nKey)==0;
}

int sqlite3BtreeProllyCachedIndexKeyCompare(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  const u8 *pKey = 0;
  int nKey = 0;
  int nCmp;
  int cmp;

  if( !pCur || pCur->pCurOps!=&prollyCursorOps || pCur->curIntKey ){
    return SQLITE_NOTFOUND;
  }
  if( pCur->eState!=CURSOR_VALID || !pIdxKey || !pCur->pKeyInfo ){
    return SQLITE_NOTFOUND;
  }

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
    if( !e ) return SQLITE_NOTFOUND;
    pKey = e->pKey;
    nKey = e->nKey;
  }else if( prollyCursorIsValid(&pCur->pCur) ){
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
  }else{
    return SQLITE_NOTFOUND;
  }

  if( prollyBtCursorCursorHasHint(pCur, BTREE_SEEK_EQ)
   && pCur->nSeekSortKey>0
   && pCur->nSeekKeyField==(int)pIdxKey->nField ){
    nCmp = nKey < pCur->nSeekSortKey ? nKey : pCur->nSeekSortKey;
    cmp = memcmp(pKey, pCur->pSeekSortKey, nCmp);
    if( cmp<0 ){
      *pRes = -1;
    }else if( cmp>0 ){
      *pRes = 1;
    }else if( nKey < pCur->nSeekSortKey ){
      *pRes = -1;
    }else{
      pIdxKey->eqSeen = 1;
      *pRes = pIdxKey->default_rc;
    }
    return SQLITE_OK;
  }

  if( unpackedRecordCanUseIntSortKey(pCur, pIdxKey, (int)pIdxKey->nField) ){
    u8 aSortKey[64];
    u8 *pSortKey = aSortKey;
    int nSortKey = 0;
    int nSortKeyAlloc = (int)sizeof(aSortKey);
    int rc;
    if( pIdxKey->nField * 18 > (int)sizeof(aSortKey) ){
      nSortKeyAlloc = (int)pIdxKey->nField * 18;
      pSortKey = (u8*)sqlite3_malloc64((sqlite3_uint64)nSortKeyAlloc);
      if( !pSortKey ) return SQLITE_NOMEM;
    }
    {
      u8 *pBuf = pSortKey;
      rc = sortKeyFromUnpackedIntRecordBuffer(
          pIdxKey, (int)pIdxKey->nField, &pBuf, &nSortKeyAlloc, &nSortKey);
      assert( pBuf==pSortKey );
    }
    if( rc!=SQLITE_OK ){
      if( pSortKey!=aSortKey ) sqlite3_free(pSortKey);
      return rc;
    }
    nCmp = nKey < nSortKey ? nKey : nSortKey;
    cmp = memcmp(pKey, pSortKey, nCmp);
    if( cmp<0 ){
      *pRes = -1;
    }else if( cmp>0 ){
      *pRes = 1;
    }else if( nKey < nSortKey ){
      *pRes = -1;
    }else{
      pIdxKey->eqSeen = 1;
      *pRes = pIdxKey->default_rc;
    }
    if( pSortKey!=aSortKey ) sqlite3_free(pSortKey);
    return SQLITE_OK;
  }

  return SQLITE_NOTFOUND;
}

static i64 prollyBtCursorIntegerKey(BtCursor *pCur){
  assert( pCur->eState==CURSOR_VALID );
  assert( pCur->curIntKey );

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    return prollyMutMapEntryIntKey(currentMutMapEntry(pCur));
  }
  if( !prollyCursorIsValid(&pCur->pCur)
   && (pCur->curFlags & BTCF_ValidNKey) ){
    return pCur->cachedIntKey;
  }
  return prollyCursorIntKey(&pCur->pCur);
}
i64 sqlite3BtreeIntegerKey(BtCursor *pCur){
  return pCur->pCurOps->xIntegerKey(pCur);
}

static void getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData){

  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    *ppData = pCur->pCachedPayload;
    *pnData = pCur->nCachedPayload;
    return;
  }

  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = currentMutMapEntry(pCur);
    if( pCur->curIntKey ){

      *ppData = e->pVal;
      *pnData = e->nVal;
    }else{
      if( e->nVal > 0 && e->pVal ){
        *ppData = e->pVal;
        *pnData = e->nVal;
      }else{
        if( cacheCursorPayloadReconstructed(pCur, e->pKey, e->nKey)==SQLITE_OK ){
          *ppData = pCur->pCachedPayload;
          *pnData = pCur->nCachedPayload;
        }else{
          *ppData = 0;
          *pnData = 0;
        }
      }
    }
    return;
  }

  if( pCur->curIntKey ){
    cursorCurrentTreeValue(pCur, ppData, pnData);
  }else{

    const u8 *pVal; int nVal;
    cursorCurrentTreeValue(pCur, &pVal, &nVal);
    if( nVal > 0 ){
      pCur->pCachedPayload = (u8*)pVal;
      pCur->nCachedPayload = nVal;
      pCur->cachedPayloadOwned = 0;
      *ppData = pVal;
      *pnData = nVal;
    }else{
      const u8 *pKey; int nKey;
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
      if( cacheCursorPayloadReconstructed(pCur, pKey, nKey)==SQLITE_OK ){
        *ppData = pCur->pCachedPayload;
        *pnData = pCur->nCachedPayload;
      }else{
        *ppData = pVal;
        *pnData = 0;
      }
    }
  }
}

static u32 prollyBtCursorPayloadSize(BtCursor *pCur){
  const u8 *pData;
  int nData;
  assert( pCur->eState==CURSOR_VALID );
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    return (u32)pCur->nCachedPayload;
  }
  getCursorPayload(pCur, &pData, &nData);
  return (u32)nData;
}
u32 sqlite3BtreePayloadSize(BtCursor *pCur){
  return pCur->pCurOps->xPayloadSize(pCur);
}

static int prollyBtCursorPayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  const u8 *pData;
  int nData;

  assert( pCur->eState==CURSOR_VALID );
  getCursorPayload(pCur, &pData, &nData);

  if( (i64)offset + (i64)amt > (i64)nData ){
    return SQLITE_CORRUPT_BKPT;
  }

  memcpy(pBuf, pData + offset, amt);
  return SQLITE_OK;
}
int sqlite3BtreePayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xPayload(pCur, offset, amt, pBuf);
}

static const void *prollyBtCursorPayloadFetch(BtCursor *pCur, u32 *pAmt){
  const u8 *pData;
  int nData;

  assert( pCur->eState==CURSOR_VALID );
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    if( pAmt ) *pAmt = (u32)pCur->nCachedPayload;
    return (const void*)pCur->pCachedPayload;
  }
  getCursorPayload(pCur, &pData, &nData);

  if( pAmt ) *pAmt = (u32)nData;
  return (const void*)pData;
}
const void *sqlite3BtreePayloadFetch(BtCursor *pCur, u32 *pAmt){
  if( !pCur ) return 0;
  return pCur->pCurOps->xPayloadFetch(pCur, pAmt);
}

static sqlite3_int64 prollyBtCursorMaxRecordSize(BtCursor *pCur){
  (void)pCur;
  return PROLLY_MAX_RECORD_SIZE;
}
sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor *pCur){
  return pCur->pCurOps->xMaxRecordSize(pCur);
}

static i64 prollyBtCursorOffset(BtCursor *pCur){
  (void)pCur;
  return 0;
}
i64 sqlite3BtreeOffset(BtCursor *pCur){
  return pCur->pCurOps->xOffset(pCur);
}

static int prollyBtCursorInsert(
  BtCursor *pCur,
  const BtreePayload *pPayload,
  int flags,
  int seekResult
){
  int rc;
  const u8 *pInsertedPayload = 0;
  int nInsertedPayload = 0;
  u8 *pIntKeyBuf = 0;
  (void)seekResult;

  if( flags & BTREE_PREFORMAT ){
    return SQLITE_OK;
  }

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = saveAllCursors(pCur->pBtree, pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->curIntKey ){
    const u8 *pData = (const u8*)pPayload->pData;
    int nData = pPayload->nData;
    i64 nTotal64 = (i64)nData + (i64)pPayload->nZero;
    int nTotal;

    if( nData<0 || pPayload->nZero<0 || nTotal64 > 0x7fffffff ){
      return SQLITE_TOOBIG;
    }
    nTotal = (int)nTotal64;
    if( pPayload->nZero > 0 ){
      pIntKeyBuf = sqlite3_malloc(nTotal);
      if( !pIntKeyBuf ) return SQLITE_NOMEM;
      if( nData > 0 ){
        memcpy(pIntKeyBuf, pData, nData);
      }
      memset(pIntKeyBuf + nData, 0, pPayload->nZero);
      pData = pIntKeyBuf;
      nData = nTotal;
    }
    pInsertedPayload = pData;
    nInsertedPayload = nData;

    rc = prollyMutMapInsert(pCur->pMutMap,
                             NULL, 0, pPayload->nKey,
                             pData, nData);
  } else {

    int nSortKey = 0;
    int nKeyField = 0;
    int splitKey = 0;
    int isIndex = 0;
    if( pCur->pKeyInfo
     && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
      nKeyField = (int)pCur->pKeyInfo->nKeyField;
      splitKey = 1;
      {
        struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
        isIndex = (pTE && !tableEntryIsTableRoot(pCur->pBtree, pTE));
      }
    }
    rc = sortKeyFromRecordPrefixCollBuffer(
        (const u8*)pPayload->pKey, (int)pPayload->nKey,
        isIndex ? 0 : (splitKey ? nKeyField : 0),
        pCur->pKeyInfo,
        &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc, &nSortKey);
    if( rc==SQLITE_OK ){
      if( splitKey ){
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 pCur->pSeekSortKey, nSortKey, 0,
                                 (const u8*)pPayload->pKey, (int)pPayload->nKey);
      }else{
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 pCur->pSeekSortKey, nSortKey, 0,
                                 NULL, 0);
      }
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(pIntKeyBuf);
    return rc;
  }

  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer && mutMapShouldDrain(pCur) ){
      canDefer = 0;
    }
    if( canDefer ){
      if( (flags & BTREE_SAVEPOSITION) && pCur->curIntKey ){
        pCur->eState = CURSOR_VALID;
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = pPayload->nKey;
        rc = cacheCursorPayloadCopy(pCur, pInsertedPayload, nInsertedPayload);
        sqlite3_free(pIntKeyBuf);
        if( rc!=SQLITE_OK ) return rc;

        pCur->mmActive = 0;
        pCur->flushSeekEdits = 0;
      } else if( (flags & BTREE_SAVEPOSITION) && !pCur->curIntKey ){
        sqlite3_free(pIntKeyBuf);
        CLEAR_CACHED_PAYLOAD(pCur);
        if( prollyCursorIsValid(&pCur->pCur) ){
          int trc = prollyCursorNext(&pCur->pCur);
          if( trc==SQLITE_OK
           && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
            pCur->eState = CURSOR_SKIPNEXT;
            pCur->skipNext = 1;
          } else {
            pCur->eState = CURSOR_INVALID;
          }
        } else {
          pCur->eState = CURSOR_INVALID;
        }
        pCur->mmActive = 0;
        pCur->flushSeekEdits = 0;
      } else {
        sqlite3_free(pIntKeyBuf);
        pCur->eState = CURSOR_INVALID;
        pCur->flushSeekEdits = 0;
      }
      return SQLITE_OK;
    }
  }

  sqlite3_free(pIntKeyBuf);
  rc = flushMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;
  {
    struct TableEntry *pTE2 = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE2 ){
      prollyCursorClose(&pCur->pCur);
      prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                       &pTE2->root, pTE2->flags);
    }
  }
  if( pCur->curIntKey ){
    int res = 0;
    rc = prollyCursorSeekInt(&pCur->pCur, pPayload->nKey, &res);
    if( rc==SQLITE_OK ){
      pCur->eState = CURSOR_VALID;
      if( res==0 ){
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = pPayload->nKey;
      }
    }
  } else {
    int res = 0;
    rc = prollyCursorSeekBlob(&pCur->pCur,
                               (const u8*)pPayload->pKey,
                               (int)pPayload->nKey, &res);
    if( rc==SQLITE_OK ) pCur->eState = CURSOR_VALID;
  }

  return rc;
}

static int flushIfNeeded(BtCursor *pCur){
  int rc;
  struct TableEntry *pTE;

  if( !pCur->pMutMap || prollyMutMapIsEmpty(pCur->pMutMap) ){
    return SQLITE_OK;
  }

  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p->pBtree==pCur->pBtree
       && p!=pCur
       && p->pgnoRoot==pCur->pgnoRoot ){
        if( !p->isPinned
         && (p->eState==CURSOR_VALID || p->eState==CURSOR_SKIPNEXT) ){
          p->isPinned = 1;
          rc = saveCursorPosition(p);
          p->isPinned = 0;
          if( rc!=SQLITE_OK ) return rc;
        } else if( p->eState!=CURSOR_REQUIRESEEK
                && p->eState!=CURSOR_INVALID ){
          prollyCursorReleaseAll(&p->pCur);
        }
      }
    }
  }

  rc = flushMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE ){
    prollyCursorClose(&pCur->pCur);
    prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                     &pTE->root, pTE->flags);
  }
  pCur->eState = CURSOR_INVALID;
  return SQLITE_OK;
}

static int flushAllPending(Btree *pBtree, BtShared *pBt, Pgno iTable){
  BtCursor *p;
  int rc;

  for(p = pBt->pCursor; p; p = p->pNext){
    if( p->pBtree==pBtree && (iTable==0 || p->pgnoRoot==iTable) ){
      rc = flushIfNeeded(p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  rc = flushDeferredEdits(pBtree, pBt);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

static int flushDeferredEdits(Btree *pBtree, BtShared *pBt){
  int rc = SQLITE_OK;
  if( pBtree ){
    int i;
    for(i=0; i<pBtree->cat.n; i++){
      struct TableEntry *pTE = &pBtree->cat.a[i];
      rc = flushPendingForTable(pBtree, pBt, pTE, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return rc;
}

static int btreeDeleteImmediate(BtCursor *pCur){
  int rc;

  rc = flushMutMap(pCur);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  {
    struct TableEntry *pTE2 = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE2 ){
      prollyCursorClose(&pCur->pCur);
      prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                       &pTE2->root, pTE2->flags);
    }
  }

  pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
  return rc;
}
int sqlite3BtreeInsert(
  BtCursor *pCur,
  const BtreePayload *pPayload,
  int flags,
  int seekResult
){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xInsert(pCur, pPayload, flags, seekResult);
}

static int prollyBtCursorDelete(BtCursor *pCur, u8 flags){
  int rc;
  const u8 *pKey = 0;
  int nKey = 0;
  i64 iKey = 0;

  u8 *pSavedDelKey = 0;
  int nSavedDelKey = 0;
  int savedDelKeyOwned = 0;
  i64 savedIntKey = 0;
  int hasSavedKey = 0;

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, 0);
    if( rc!=SQLITE_OK || pCur->eState!=CURSOR_VALID ) return rc;
  }else if( pCur->eState==CURSOR_SKIPNEXT ){
    pCur->eState = CURSOR_VALID;
    pCur->skipNext = 0;
  }else if( pCur->eState==CURSOR_INVALID ){

  }else if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_CORRUPT_BKPT;
  }

  if( pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_INVALID ){
    if( pCur->curIntKey ){
      if( pCur->mmActive
       && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
        savedIntKey = prollyMutMapEntryIntKey(currentMutMapEntry(pCur));
        hasSavedKey = 1;
      }else if( !prollyCursorIsValid(&pCur->pCur)
       && (pCur->curFlags & BTCF_ValidNKey) ){
        savedIntKey = pCur->cachedIntKey;
        hasSavedKey = 1;
      }else if( prollyCursorIsValid(&pCur->pCur) ){
        savedIntKey = prollyCursorIntKey(&pCur->pCur);
        hasSavedKey = 1;
      }
    } else {
      if( pCur->nSeekSortKey>0
       && ((flags & BTREE_AUXDELETE) || cachedSeekKeyMatchesCurrent(pCur)) ){
        pSavedDelKey = pCur->pSeekSortKey;
        nSavedDelKey = pCur->nSeekSortKey;
        hasSavedKey = 1;
      }else if( pCur->mmActive
       && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
        ProllyMutMapEntry *e = currentMutMapEntry(pCur);
        pSavedDelKey = sqlite3_malloc(e->nKey);
        if( !pSavedDelKey ) return SQLITE_NOMEM;
        memcpy(pSavedDelKey, e->pKey, e->nKey);
        nSavedDelKey = e->nKey;
        savedDelKeyOwned = 1;
        hasSavedKey = 1;
      }else if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
        int nDelKeyField = 0;
        if( pCur->pKeyInfo
         && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
          struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
          if( pTE && !tableEntryIsTableRoot(pCur->pBtree, pTE) ){
            nDelKeyField = 0;
          }else{
            nDelKeyField = (int)pCur->pKeyInfo->nKeyField;
          }
        }
        rc = sortKeyFromRecordPrefixCollBuffer(
            pCur->pCachedPayload, pCur->nCachedPayload,
            nDelKeyField, pCur->pKeyInfo,
            &pCur->pSeekSortKey, &pCur->nSeekSortKeyAlloc,
            &nSavedDelKey);
        if( rc!=SQLITE_OK ) return rc;
        pSavedDelKey = pCur->pSeekSortKey;
        hasSavedKey = 1;
      }else if( prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pTmp; int nTmp;
        prollyCursorKey(&pCur->pCur, &pTmp, &nTmp);
        pSavedDelKey = sqlite3_malloc(nTmp);
        if( !pSavedDelKey ) return SQLITE_NOMEM;
        memcpy(pSavedDelKey, pTmp, nTmp);
        nSavedDelKey = nTmp;
        savedDelKeyOwned = 1;
        hasSavedKey = 1;
      }
    }
  }

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ){
    if( savedDelKeyOwned ) sqlite3_free(pSavedDelKey);
    return rc;
  }

  rc = saveAllCursors(pCur->pBtree, pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ){
    if( savedDelKeyOwned ) sqlite3_free(pSavedDelKey);
    return rc;
  }

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ){
    if( savedDelKeyOwned ) sqlite3_free(pSavedDelKey);
    return rc;
  }

  if( pCur->curIntKey ){
    if( hasSavedKey ){
      iKey = savedIntKey;
    }
    rc = prollyMutMapDelete(pCur->pMutMap, NULL, 0, iKey);
  } else {
    if( hasSavedKey ){
      pKey = pSavedDelKey;
      nKey = nSavedDelKey;
    }
    rc = prollyMutMapDelete(pCur->pMutMap, pKey, nKey, 0);
    if( savedDelKeyOwned ) sqlite3_free(pSavedDelKey);
    pSavedDelKey = 0;
  }

  if( rc!=SQLITE_OK ) return rc;

  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer && mutMapShouldDrain(pCur) ){
      canDefer = 0;
    }
    if( canDefer ){
      CLEAR_CACHED_PAYLOAD(pCur);
      pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
      pCur->mmActive = 0;
      if( flags & (BTREE_SAVEPOSITION | BTREE_AUXDELETE) ){
        pCur->flushSeekEdits = 1;
        pCur->eState = CURSOR_SKIPNEXT;
        pCur->skipNext = 0;
      } else {
        pCur->eState = CURSOR_INVALID;
      }
      return SQLITE_OK;
    }
  }

  rc = btreeDeleteImmediate(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( flags & BTREE_SAVEPOSITION ){
    int res = 0;
    if( pCur->curIntKey ){
      rc = prollyCursorSeekInt(&pCur->pCur, iKey, &res);
    } else if( pKey && nKey > 0 ){

      u8 *pReseek = sqlite3_malloc(nKey);
      if( pReseek ){
        memcpy(pReseek, pKey, nKey);
        rc = prollyCursorSeekBlob(&pCur->pCur, pReseek, nKey, &res);
        sqlite3_free(pReseek);
      } else {
        rc = SQLITE_NOMEM;
      }
    } else {
      rc = SQLITE_OK;
      res = -1;
    }
    if( rc==SQLITE_OK && prollyCursorIsValid(&pCur->pCur) ){
      pCur->eState = CURSOR_SKIPNEXT;
      pCur->skipNext = (res>=0) ? 1 : -1;
    } else {
      pCur->eState = CURSOR_INVALID;
    }
  } else {
    pCur->eState = CURSOR_INVALID;
  }

  return SQLITE_OK;
}
int sqlite3BtreeDelete(BtCursor *pCur, u8 flags){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xDelete(pCur, flags);
}

static int prollyBtCursorTransferRow(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  int rc;
  const u8 *pVal;
  int nVal;
  BtreePayload payload;

  assert( pSrc->eState==CURSOR_VALID );

  prollyCursorValue(&pSrc->pCur, &pVal, &nVal);

  memset(&payload, 0, sizeof(payload));

  if( pDest->curIntKey ){
    payload.nKey = iKey;
    payload.pData = pVal;
    payload.nData = nVal;
  } else {
    const u8 *pKey;
    int nKey;
    prollyCursorKey(&pSrc->pCur, &pKey, &nKey);
    payload.pKey = pKey;
    payload.nKey = nKey;
  }

  rc = sqlite3BtreeInsert(pDest, &payload, 0, 0);
  return rc;
}

#ifndef SQLITE_OMIT_SHARED_CACHE
static void prollyBtreeEnter(Btree *p){ (void)p; }
void sqlite3BtreeEnter(Btree *p){
  if( p ) p->pOps->xEnter(p);
}
void sqlite3BtreeEnterAll(sqlite3 *db){
  if( db ){ int i; for(i=0; i<db->nDb; i++){
    Btree *p = db->aDb[i].pBt;
    if( p ) p->pOps->xEnter(p);
  }}
}
int sqlite3BtreeSharable(Btree *p){ (void)p; return 0; }
void sqlite3BtreeEnterCursor(BtCursor *pCur){ (void)pCur; }
int sqlite3BtreeConnectionCount(Btree *p){ (void)p; return 1; }
#endif

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE
static void prollyBtreeLeave(Btree *p){ (void)p; }
void sqlite3BtreeLeave(Btree *p){
  if( p ) p->pOps->xLeave(p);
}
void sqlite3BtreeLeaveCursor(BtCursor *pCur){ (void)pCur; }
void sqlite3BtreeLeaveAll(sqlite3 *db){
  if( db ){ int i; for(i=0; i<db->nDb; i++){
    Btree *p = db->aDb[i].pBt;
    if( p ) p->pOps->xLeave(p);
  }}
}
#ifndef NDEBUG
int sqlite3BtreeHoldsMutex(Btree *p){ (void)p; return 1; }
int sqlite3BtreeHoldsAllMutexes(sqlite3 *db){ (void)db; return 1; }
int sqlite3SchemaMutexHeld(sqlite3 *db, int iDb, Schema *pSchema){
  (void)db; (void)iDb; (void)pSchema;
  return 1;
}
#endif
#elif !defined(SQLITE_OMIT_SHARED_CACHE)

static void prollyBtreeLeave(Btree *p){ (void)p; }
#endif

int sqlite3BtreeTripAllCursors(Btree *p, int errCode, int writeOnly){
  BtCursor *pCur;
  BtShared *pBt;

  if( !p ) return SQLITE_OK;
  if( p->pOrigBtree ){
    origBtreeTripAllCursors(p->pOrigBtree, errCode, writeOnly);
    return SQLITE_OK;
  }
  pBt = p->pBt;
  if( !pBt ) return SQLITE_OK;

  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    if( writeOnly && !(pCur->curFlags & BTCF_WriteFlag) ){
      continue;
    }
    if( pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_SKIPNEXT ){
      int rc = saveCursorPosition(pCur);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( errCode ){
      pCur->eState = CURSOR_FAULT;
      pCur->skipNext = errCode;
    }
  }
  return SQLITE_OK;
}
int sqlite3BtreeTransferRow(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  return pDest->pCurOps->xTransferRow(pDest, pSrc, iKey);
}

static void prollyBtCursorClearCursor(BtCursor *pCur){
  if( pCur->pKey ){
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
    pCur->nKey = 0;
  }
  CLEAR_CACHED_PAYLOAD(pCur);
  clearMergeCursorState(pCur);
  pCur->eState = CURSOR_INVALID;
  pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_ValidOvfl|BTCF_AtLast);
  pCur->skipNext = 0;
}
void sqlite3BtreeClearCursor(BtCursor *pCur){
  pCur->pCurOps->xClearCursor(pCur);
}

void sqlite3BtreeClearCache(Btree *p){
  (void)p;
}

static struct Pager *prollyBtreePager(Btree *p){
  return (struct Pager*)(p->pBt->pPagerShim);
}
struct Pager *sqlite3BtreePager(Btree *p){
  if( !p ) return 0;
  return p->pOps->xPager(p);
}

static int prollyBtCursorCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  struct TableEntry *pTE;
  (void)db;

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE && pTE->pPending ){
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    if( !prollyMutMapIsEmpty(pMap) ){
      ProllyMutMap *pFlushMap = pMap;
      int captured = 0;
      int rc = snapshotPendingForFlush(pCur->pBtree, pCur->pgnoRoot,
                                       (ProllyMutMap**)&pTE->pPending,
                                       &pFlushMap, &captured);
      if( rc!=SQLITE_OK ) return rc;
      if( captured ){
        refreshCursorMutMapAliases(pCur->pBtree, pCur->pBt, pCur->pgnoRoot,
                                   (ProllyMutMap*)pTE->pPending);
      }
      rc = applyMutMapToTableRoot(pCur->pBt, pTE, pFlushMap);
      if( rc!=SQLITE_OK ) return rc;
      if( pTE->pPending==pMap ){
        prollyMutMapClear(pMap);
      }
    }
  }
  flushIfNeeded(pCur);
  return countTreeEntries(pCur->pBtree, pCur->pgnoRoot, pnEntry);
}
int sqlite3BtreeCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCount(db, pCur, pnEntry);
}

static i64 prollyBtCursorRowCountEst(BtCursor *pCur){
  struct TableEntry *pTE;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ){
    return 0;
  }
  return 1000000;
}
i64 sqlite3BtreeRowCountEst(BtCursor *pCur){
  return pCur->pCurOps->xRowCountEst(pCur);
}

typedef struct IntegrityCheckCtx IntegrityCheckCtx;
struct IntegrityCheckCtx {
  BtShared *pBt;
  ProllyHashSet seen;
  int mxErr;
  int *pnErr;
};

static int integrityCheckChunkGraph(IntegrityCheckCtx *pCtx, const ProllyHash *pHash);

static int integrityCheckChildCb(void *pArg, const ProllyHash *pHash){
  return integrityCheckChunkGraph((IntegrityCheckCtx*)pArg, pHash);
}

static int integrityCheckChunkGraph(
  IntegrityCheckCtx *pCtx,
  const ProllyHash *pHash
){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  if( pCtx->mxErr>0 && *pCtx->pnErr>=pCtx->mxErr ) return SQLITE_OK;
  if( prollyHashSetContains(&pCtx->seen, pHash) ) return SQLITE_OK;

  rc = prollyHashSetAdd(&pCtx->seen, pHash);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreGet(&pCtx->pBt->store, pHash, &pData, &nData);
  if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
    (*pCtx->pnErr)++;
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteEnumerateChunkChildren(pData, nData, integrityCheckChildCb, pCtx);
  sqlite3_free(pData);
  if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
    (*pCtx->pnErr)++;
    return SQLITE_OK;
  }
  return rc;
}

int doltliteCheckRepoGraphIntegrity(Btree *p, int mxErr, int *pnErr){
  BtShared *pBt;
  IntegrityCheckCtx ctx;
  int i;
  int nErr = 0;
  int rc;

  if( pnErr ) *pnErr = 0;
  if( !p || !p->pBt ) return SQLITE_OK;
  if( p->pOrigBtree ) return SQLITE_OK;

  pBt = p->pBt;
  memset(&ctx, 0, sizeof(ctx));
  ctx.pBt = pBt;
  ctx.mxErr = mxErr;
  ctx.pnErr = &nErr;
  rc = prollyHashSetInit(&ctx.seen, 256);
  if( rc!=SQLITE_OK ) return rc;

  rc = integrityCheckChunkGraph(&ctx, refsTableGetHash(&pBt->store.refs));
  {
    int nBr; const BranchRef *aBr;
    refsTableGetBranches(&pBt->store.refs, &nBr, &aBr);
    for(i=0; rc==SQLITE_OK && i<nBr; i++){
      rc = integrityCheckChunkGraph(&ctx, &aBr[i].commitHash);
      if( rc==SQLITE_OK ){
        rc = integrityCheckChunkGraph(&ctx, &aBr[i].workingSetHash);
      }
    }
  }
  {
    int nTg; const TagRef *aTg;
    refsTableGetTags(&pBt->store.refs, &nTg, &aTg);
    for(i=0; rc==SQLITE_OK && i<nTg; i++){
      rc = integrityCheckChunkGraph(&ctx, &aTg[i].commitHash);
    }
  }
  {
    int nTk; const TrackingBranch *aTk;
    refsTableGetTracking(&pBt->store.refs, &nTk, &aTk);
    for(i=0; rc==SQLITE_OK && i<nTk; i++){
      rc = integrityCheckChunkGraph(&ctx, &aTk[i].commitHash);
    }
  }
  if( rc==SQLITE_OK && p->isMerging ){
    rc = integrityCheckChunkGraph(&ctx, &p->mergeCommitHash);
  }
  if( rc==SQLITE_OK ){
    rc = integrityCheckChunkGraph(&ctx, &p->conflictsCatalogHash);
  }

  prollyHashSetFree(&ctx.seen);
  if( pnErr ) *pnErr = nErr;
  return rc;
}

int sqlite3BtreeSetVersion(Btree *p, int iVersion){
  if( p->inTrans!=TRANS_WRITE ){
    int rc = sqlite3BtreeBeginTrans(p, 2, 0);
    if( rc!=SQLITE_OK ) return rc;
  }

  p->aMeta[BTREE_FILE_FORMAT] = (u32)iVersion;
  return SQLITE_OK;
}

int sqlite3HeaderSizeBtree(void){
  return 100;
}

int sqlite3BtreeIntegrityCheck(
  sqlite3 *db,
  Btree *p,
  Pgno *aRoot,
  sqlite3_value *aCnt,
  int nRoot,
  int mxErr,
  int *pnErr,
  char **pzOut
){
  BtShared *pBt;
  IntegrityCheckCtx ctx;
  int i;
  int nErr = 0;
  int rc;

  if( !p ){
    if( pnErr ) *pnErr = 0;
    if( pzOut ) *pzOut = 0;
    return SQLITE_OK;
  }

  if( p->pOrigBtree ){
    return origBtreeIntegrityCheck(db, p->pOrigBtree, aRoot, aCnt,
                                   nRoot, mxErr, pnErr, pzOut);
  }

  (void)aCnt;

  if( !p->pBt ){
    if( pnErr ) *pnErr = 0;
    if( pzOut ) *pzOut = 0;
    return SQLITE_OK;
  }
  pBt = p->pBt;
  memset(&ctx, 0, sizeof(ctx));
  ctx.pBt = pBt;
  ctx.mxErr = mxErr;
  ctx.pnErr = &nErr;
  rc = prollyHashSetInit(&ctx.seen, 256);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nRoot; i++){

    if( aCnt ){
      sqlite3VdbeMemSetInt64(&aCnt[i], 0);
    }
    if( nErr>=mxErr ) continue;
    {
      struct TableEntry *pTE = findTable(p, aRoot[i]);
      if( !pTE ) continue;
      if( !prollyHashIsEmpty(&pTE->root) ){
        rc = integrityCheckChunkGraph(&ctx, &pTE->root);
        if( rc!=SQLITE_OK ) goto integrity_done;
      }
    }
  }

  rc = doltliteCheckRepoGraphIntegrity(p, mxErr, &i);
  if( rc!=SQLITE_OK ) goto integrity_done;
  nErr += i;

integrity_done:
  prollyHashSetFree(&ctx.seen);
  if( rc!=SQLITE_OK ) return rc;

  if( pnErr ) *pnErr = nErr;
  if( pzOut ){
    if( nErr>0 ){
      *pzOut = sqlite3_mprintf("integrity check failed");
      if( !*pzOut ) return SQLITE_NOMEM;
    }else{
      *pzOut = 0;
    }
  }

  return SQLITE_OK;
}

static void prollyBtCursorCursorPin(BtCursor *pCur){
  pCur->isPinned = 1;
  pCur->curFlags |= BTCF_Pinned;
}
void sqlite3BtreeCursorPin(BtCursor *pCur){
  if( !pCur ) return;
  pCur->pCurOps->xCursorPin(pCur);
}

static void prollyBtCursorCursorUnpin(BtCursor *pCur){
  pCur->isPinned = 0;
  pCur->curFlags &= ~BTCF_Pinned;
}
void sqlite3BtreeCursorUnpin(BtCursor *pCur){
  if( !pCur ) return;
  pCur->pCurOps->xCursorUnpin(pCur);
}

static void prollyBtCursorCursorHintFlags(BtCursor *pCur, unsigned x){
  pCur->hints = (u8)(x & 0xFF);
}
void sqlite3BtreeCursorHintFlags(BtCursor *pCur, unsigned x){
  pCur->pCurOps->xCursorHintFlags(pCur, x);
}

#ifdef SQLITE_ENABLE_CURSOR_HINTS
void sqlite3BtreeCursorHint(BtCursor *pCur, int eHintType, ...){
  (void)pCur; (void)eHintType;
}
#endif

static int prollyBtCursorCursorHasHint(BtCursor *pCur, unsigned int mask){
  return (pCur->hints & mask) != 0;
}
int sqlite3BtreeCursorHasHint(BtCursor *pCur, unsigned int mask){
  return pCur->pCurOps->xCursorHasHint(pCur, mask);
}

BtCursor *sqlite3BtreeFakeValidCursor(void){
  static BtCursor fakeCursor;
  static int initialized = 0;
  if( !initialized ){
    memset(&fakeCursor, 0, sizeof(fakeCursor));
    fakeCursor.eState = CURSOR_VALID;
    initialized = 1;
  }
  return &fakeCursor;
}

int sqlite3BtreeCopyFile(Btree *pTo, Btree *pFrom){
  BtShared *pBtTo = pTo->pBt;
  int i;

  invalidateCursors(pBtTo, 0, SQLITE_ABORT);

  catFree(&pTo->cat);

  for(i=0; i<pFrom->cat.n; i++){
    struct TableEntry *pTE = addTable(pTo,
                                       pFrom->cat.a[i].iTable,
                                       pFrom->cat.a[i].flags);
    if( !pTE ) return SQLITE_NOMEM;
    pTE->root = pFrom->cat.a[i].root;
  }

  memcpy(pTo->aMeta, pFrom->aMeta, sizeof(pTo->aMeta));
  pTo->cat.iNextTable = pFrom->cat.iNextTable;

  pTo->iBDataVersion++;
  if( pBtTo->pPagerShim ){
    pBtTo->pPagerShim->iDataVersion++;
  }

  return SQLITE_OK;
}

int sqlite3BtreeIsInBackup(Btree *p){
  (void)p;
  return 0;
}

#ifndef SQLITE_OMIT_WAL
int sqlite3BtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  (void)p; (void)eMode;
  if( pnLog ) *pnLog = 0;
  if( pnCkpt ) *pnCkpt = 0;
  return SQLITE_OK;
}
#endif

#ifndef SQLITE_OMIT_INCRBLOB

static int prollyBtCursorPayloadChecked(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  const u8 *pVal;
  int nVal;

  if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_ABORT;
  }

  getCursorPayload(pCur, &pVal, &nVal);

  if( (i64)offset + (i64)amt > (i64)nVal ){
    return SQLITE_CORRUPT_BKPT;
  }

  memcpy(pBuf, pVal + offset, amt);
  return SQLITE_OK;
}
int sqlite3BtreePayloadChecked(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return pCur->pCurOps->xPayloadChecked(pCur, offset, amt, pBuf);
}

static int prollyBtCursorPutData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  int rc;
  const u8 *pVal;
  int nVal;
  u8 *pNew;
  BtreePayload payload;

  if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_ABORT;
  }
  if( !(pCur->curFlags & BTCF_WriteFlag) ){
    return SQLITE_READONLY;
  }
  assert( pCur->curFlags & BTCF_Incrblob );

  prollyCursorValue(&pCur->pCur, &pVal, &nVal);

  if( (i64)offset + (i64)amt > (i64)nVal ){
    return SQLITE_CORRUPT_BKPT;
  }

  pNew = sqlite3_malloc(nVal);
  if( !pNew ) return SQLITE_NOMEM;
  memcpy(pNew, pVal, nVal);

  memcpy(pNew + offset, pBuf, amt);

  memset(&payload, 0, sizeof(payload));

  if( pCur->curIntKey ){
    payload.nKey = prollyCursorIntKey(&pCur->pCur);
    payload.pData = pNew;
    payload.nData = nVal;
  } else {
    const u8 *pKey;
    int nKey;
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    payload.pKey = pKey;
    payload.nKey = nKey;
    payload.pData = pNew;
    payload.nData = nVal;
  }

  rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
  sqlite3_free(pNew);
  return rc;
}
int sqlite3BtreePutData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return pCur->pCurOps->xPutData(pCur, offset, amt, pBuf);
}

static void prollyBtCursorIncrblobCursor(BtCursor *pCur){
  pCur->curFlags |= BTCF_Incrblob;
}
void sqlite3BtreeIncrblobCursor(BtCursor *pCur){
  pCur->pCurOps->xIncrblobCursor(pCur);
}

#endif

#ifndef NDEBUG
static int prollyBtCursorCursorIsValid(BtCursor *pCur){
  return pCur && pCur->eState==CURSOR_VALID;
}
int sqlite3BtreeCursorIsValid(BtCursor *pCur){
  return pCur->pCurOps->xCursorIsValid(pCur);
}
#endif

static int prollyBtCursorCursorIsValidNN(BtCursor *pCur){
  assert( pCur!=0 );
  return pCur->eState==CURSOR_VALID;
}
int sqlite3BtreeCursorIsValidNN(BtCursor *pCur){
  return pCur->pCurOps->xCursorIsValidNN(pCur);
}

#ifdef SQLITE_DEBUG
sqlite3_uint64 sqlite3BtreeSeekCount(Btree *p){
  return p ? p->nSeek : 0;
}
#endif

#ifdef SQLITE_TEST
int sqlite3BtreeCursorInfo(BtCursor *pCur, int *aResult, int upCnt){
  (void)pCur;
  if( aResult ){
    aResult[0] = 0;
    aResult[1] = 0;
    aResult[2] = 0;
    aResult[3] = 0;
    aResult[4] = 0;
    if( upCnt >= 6 ){
      aResult[5] = 0;
    }
    if( upCnt >= 10 ){
      aResult[6] = 0;
      aResult[7] = 0;
      aResult[8] = 0;
      aResult[9] = 0;
    }
  }
  return SQLITE_OK;
}

void sqlite3BtreeCursorList(Btree *p){
#ifndef SQLITE_OMIT_TRACE
  BtCursor *pCur;
  BtShared *pBt;

  if( !p || !p->pBt ) return;
  pBt = p->pBt;

  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    const char *zState;
    switch( pCur->eState ){
      case CURSOR_VALID:       zState = "VALID";       break;
      case CURSOR_INVALID:     zState = "INVALID";     break;
      case CURSOR_SKIPNEXT:    zState = "SKIPNEXT";    break;
      case CURSOR_REQUIRESEEK: zState = "REQUIRESEEK"; break;
      case CURSOR_FAULT:       zState = "FAULT";       break;
      default:                 zState = "UNKNOWN";     break;
    }
    sqlite3DebugPrintf(
      "CURSOR %p: table=%d wrFlag=%d state=%s intKey=%d\n",
      (void*)pCur,
      (int)pCur->pgnoRoot,
      (pCur->curFlags & BTCF_WriteFlag) ? 1 : 0,
      zState,
      (int)pCur->curIntKey
    );
  }
#else
  (void)p;
#endif
}
#endif

static void doltiteEngineFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  (void)argc; (void)argv;
  sqlite3_result_text(context, "prolly", -1, SQLITE_STATIC);
}

ChunkStore *doltliteGetChunkStore(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *pBt = db->aDb[0].pBt;
    return &pBt->pBt->store;
  }
  return 0;
}

BtShared *doltliteGetBtShared(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    return db->aDb[0].pBt->pBt;
  }
  return 0;
}

ProllyCache *doltliteGetCache(sqlite3 *db){
  BtShared *pBt = doltliteGetBtShared(db);
  if( pBt ) return &pBt->cache;
  return 0;
}

void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH){
  Btree *pBtree;
  int i;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return;
  pBtree = db->aDb[0].pBt;
  for(i=0; i<pBtree->cat.n; i++){
    if( pBtree->cat.a[i].iTable==iTable ){
      memcpy(&pBtree->cat.a[i].schemaHash, pH, sizeof(ProllyHash));
      return;
    }
  }
}

int doltliteApplyRawRowMutation(
  sqlite3 *db,
  const char *zTable,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  Btree *pBtree;
  BtShared *pBt;
  struct TableEntry *pTE;
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey;

  if( !db || !zTable ) return SQLITE_MISUSE;
  if( db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  pBt = pBtree->pBt;
  if( !pBt ) return SQLITE_ERROR;
  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ) return rc;

  {
    int i;
    pTE = 0;
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].zName
       && strcmp(pBtree->cat.a[i].zName, zTable)==0 ){
        pTE = &pBtree->cat.a[i];
        break;
      }
    }
  }
  if( !pTE ) return SQLITE_NOTFOUND;

  rc = flushPendingForTable(pBtree, pBt, pTE, 0);
  if( rc!=SQLITE_OK ) return rc;

  isIntKey = (pTE->flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  if( pVal ){
    rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  }else{
    rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  }
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  memset(&mut, 0, sizeof(mut));
  mut.pStore = &pBt->store;
  mut.pCache = &pBt->cache;
  memcpy(&mut.oldRoot, &pTE->root, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = pTE->flags;

  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(&pTE->root, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

int doltliteEnsureWriteTxnAndSavepoints(sqlite3 *db){
  Btree *pBtree;
  int rc = SQLITE_OK;
  int target;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( pBtree->inTrans!=TRANS_WRITE ){
    rc = sqlite3BtreeBeginTrans(pBtree, 2, 0);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* VC functions can write through this btree layer without first touching a
  ** SQL table, so mirror SQLite's active savepoint stack before mutating it. */
  target = db->nSavepoint;
  while( pBtree->nSavepoint < target ){
    rc = pushSavepoint(pBtree, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

const char *doltliteNextTableForSchema(sqlite3 *db, int *pIdx, Pgno *piTable){
  Btree *pBtree;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return 0;
  pBtree = db->aDb[0].pBt;
  while( *pIdx < pBtree->cat.n ){
    int i = (*pIdx)++;
    if( pBtree->cat.a[i].iTable>1 && pBtree->cat.a[i].zName ){
      *piTable = pBtree->cat.a[i].iTable;
      return pBtree->cat.a[i].zName;
    }
  }
  return 0;
}

int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut){
  BtShared *pBt = doltliteGetBtShared(db);
  Btree *pBtree;
  int rc;
  if( !pBt ) return SQLITE_ERROR;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  rc = flushAllPending(pBtree, pBt, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = flushDeferredEdits(pBtree, pBt);
  if( rc!=SQLITE_OK ) return rc;

  {
    extern void doltliteUpdateSchemaHashes(sqlite3 *db);
    doltliteUpdateSchemaHashes(db);
  }
  return serializeCatalog(db->aDb[0].pBt, ppOut, pnOut);
}

int doltliteDeserializeCatalogForTest(sqlite3 *db, const u8 *data, int nData){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  return deserializeCatalog(db->aDb[0].pBt, data, nData);
}

int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                        struct TableEntry **ppTables, int *pnTables,
                        Pgno *piNextTable){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  int rc;
  Btree temp;

  if( !cs ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(catHash) ){
    *ppTables = 0;
    *pnTables = 0;
    if( piNextTable ) *piNextTable = 2;
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, catHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  memset(&temp, 0, sizeof(temp));
  rc = deserializeCatalog(&temp, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  *ppTables = temp.cat.a;
  *pnTables = temp.cat.n;
  if( piNextTable ) *piNextTable = temp.cat.iNextTable;
  return SQLITE_OK;
}

void doltliteFreeCatalog(struct TableEntry *a, int n){
  int i;
  if( !a ) return;
  for(i=0; i<n; i++) sqlite3_free(a[i].zName);
  sqlite3_free(a);
}

int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  u8 *data = 0;
  int nData = 0;
  int rc;
  DoltliteCommit commit;

  if( !cs ) return SQLITE_ERROR;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    memset(pCatHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, &headHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  memcpy(pCatHash, &commit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&commit);
  return SQLITE_OK;
}

int doltliteGetWorkingTableState(sqlite3 *db, const char *zTable,
                                 ProllyHash *pRoot, u8 *pFlags,
                                 ProllyHash *pSchemaHash){
  Btree *pBtree;
  Pgno iTable;
  struct TableEntry *pEntry;

  if( pRoot ) memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(ProllyHash));

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( doltliteResolveTableName(db, zTable, &iTable)!=SQLITE_OK ){
    return SQLITE_NOTFOUND;
  }
  pEntry = findTable(pBtree, iTable);
  if( !pEntry ) return SQLITE_NOTFOUND;

  if( pRoot ) memcpy(pRoot, &pEntry->root, sizeof(ProllyHash));
  if( pFlags ) *pFlags = pEntry->flags;
  if( pSchemaHash ) memcpy(pSchemaHash, &pEntry->schemaHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable){
  Btree *pBtree;
  Schema *pSchema;
  HashElem *k;
  int i;
  if( !db || db->nDb<=0 ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( pBtree && pBtree->cat.a ){
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].zName
       && strcmp(pBtree->cat.a[i].zName, zTable)==0 ){
        *piTable = pBtree->cat.a[i].iTable;
        return SQLITE_OK;
      }
    }
  }
  pSchema = db->aDb[0].pSchema;
  if( !pSchema ) return SQLITE_ERROR;
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    if( pTab && strcmp(pTab->zName, zTable)==0 ){
      *piTable = pTab->tnum;
      return SQLITE_OK;
    }
  }
  return SQLITE_ERROR;
}

char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable){
  Btree *pBtree;
  int i;
  if( !db || db->nDb<=0 ) return 0;
  pBtree = db->aDb[0].pBt;
  if( pBtree && pBtree->cat.a ){
    for(i=0; i<pBtree->cat.n; i++){
      if( pBtree->cat.a[i].iTable==iTable && pBtree->cat.a[i].zName ){
        return sqlite3_mprintf("%s", pBtree->cat.a[i].zName);
      }
    }
  }
  {
    char *zLive = resolveLiveSchemaTableNumber(db, iTable);
    if( zLive ) return zLive;
  }
  return 0;
}

int doltliteSwitchCatalog(sqlite3 *db, const ProllyHash *catHash){
  BtShared *pBt = doltliteGetBtShared(db);
  Btree *pBtree;
  ChunkStore *cs;
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !pBt ) return SQLITE_ERROR;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  cs = &pBt->store;

  if( prollyHashIsEmpty(catHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, catHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  invalidateCursors(pBt, 0, SQLITE_ABORT);

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
  pBtree->iBDataVersion++;
  if( pBt->pPagerShim ){
    pBt->pPagerShim->iDataVersion++;
  }

  if( pBtree->db ){
    sqlite3ExpirePreparedStatements(pBtree->db, 0);
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }else{
    invalidateSchema(pBtree);
  }

  return SQLITE_OK;
}

int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash){
  BtShared *pBt = doltliteGetBtShared(db);
  Btree *pBtree;
  ChunkStore *cs;
  u8 *oldCatData = 0;
  int nOldCatData = 0;
  ProllyHash oldStagedCatalog;
  u8 oldIsMerging;
  ProllyHash oldMergeCommitHash;
  ProllyHash oldConflictsCatalogHash;
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !pBt ) return SQLITE_ERROR;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  cs = &pBt->store;

  if( prollyHashIsEmpty(catHash) ) return SQLITE_OK;

  rc = serializeCatalog(pBtree, &oldCatData, &nOldCatData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGet(cs, catHash, &data, &nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(oldCatData);
    return rc;
  }

  oldStagedCatalog = pBtree->stagedCatalog;
  oldIsMerging = pBtree->isMerging;
  oldMergeCommitHash = pBtree->mergeCommitHash;
  oldConflictsCatalogHash = pBtree->conflictsCatalogHash;

  invalidateCursors(pBt, 0, SQLITE_ABORT);

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ){
    btreeFreeCatalogTables(pBtree);
    if( oldCatData ){
      rc = deserializeCatalog(pBtree, oldCatData, nOldCatData);
    }
    sqlite3_free(oldCatData);
    pBtree->stagedCatalog = oldStagedCatalog;
    pBtree->isMerging = oldIsMerging;
    pBtree->mergeCommitHash = oldMergeCommitHash;
    pBtree->conflictsCatalogHash = oldConflictsCatalogHash;
    return rc;
  }

  pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
  pBtree->iBDataVersion++;
  if( pBt->pPagerShim ){
    pBt->pPagerShim->iDataVersion++;
  }

  if( pBtree->db ){
    sqlite3ExpirePreparedStatements(pBtree->db, 0);
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }else{
    invalidateSchema(pBtree);
  }

  memcpy(&pBtree->stagedCatalog, catHash, sizeof(ProllyHash));

  {
    const char *zBr = pBtree->zBranch ? pBtree->zBranch : "main";
    rc = btreeWriteWorkingState(cs, zBr, catHash, NULL);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefs(cs);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreCommit(cs);
  }
  if( rc!=SQLITE_OK ){
    btreeFreeCatalogTables(pBtree);
    if( oldCatData ){
      int rc2 = deserializeCatalog(pBtree, oldCatData, nOldCatData);
      if( rc2!=SQLITE_OK ){
        sqlite3_free(oldCatData);
        chunkStoreRollback(cs);
        return rc;
      }
    }
    pBtree->stagedCatalog = oldStagedCatalog;
    pBtree->isMerging = oldIsMerging;
    pBtree->mergeCommitHash = oldMergeCommitHash;
    pBtree->conflictsCatalogHash = oldConflictsCatalogHash;
    if( pBtree->db ){
      sqlite3ExpirePreparedStatements(pBtree->db, 0);
      sqlite3ResetAllSchemasOfConnection(pBtree->db);
    }else{
      invalidateSchema(pBtree);
    }
    pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
    pBtree->iBDataVersion++;
    if( pBt->pPagerShim ){
      pBt->pPagerShim->iDataVersion++;
    }
    chunkStoreRollback(cs);
    sqlite3_free(oldCatData);
    return rc;
  }

  sqlite3_free(oldCatData);
  return SQLITE_OK;
}

int doltliteUpdateBranchWorkingState(sqlite3 *db, const char *zBranch,
                                     const ProllyHash *pCatHash,
                                     const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  if( !cs ) return SQLITE_ERROR;
  return btreeWriteWorkingState(cs, zBranch, pCatHash, pCommitHash);
}

int doltliteWriteBranchCleanWorkingState(sqlite3 *db, const char *zBranch,
                                         const ProllyHash *pCatHash,
                                         const ProllyHash *pCommitHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash emptyHash;
  if( !cs ) return SQLITE_ERROR;
  memset(&emptyHash, 0, sizeof(emptyHash));
  return btreeStoreWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                  &emptyHash, 0,
                                  &emptyHash, &emptyHash,
                                  0, &emptyHash, &emptyHash,
                                  0, 0, &emptyHash);
}

int chunkStoreWriteBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                        const ProllyHash *pCatHash,
                                        const ProllyHash *pCommitHash){
  return btreeWriteWorkingState(cs, zBranch, pCatHash, pCommitHash);
}

int chunkStoreReadBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                       ProllyHash *pCatHash,
                                       ProllyHash *pCommitHash){
  return btreeReadWorkingCatalog(cs, zBranch, pCatHash, pCommitHash);
}

const char *doltliteGetSessionBranch(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zBranch ? p->zBranch : "main";
  }
  return "main";
}

void doltliteSetSessionBranch(sqlite3 *db, const char *zBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sqlite3_free(p->zBranch);
    p->zBranch = sqlite3_mprintf("%s", zBranch);
  }
}

const char *doltliteGetAuthorName(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zAuthorName ? p->zAuthorName : "doltlite";
  }
  return "doltlite";
}

void doltliteSetAuthorName(sqlite3 *db, const char *zName){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sqlite3_free(p->zAuthorName);
    p->zAuthorName = zName ? sqlite3_mprintf("%s", zName) : 0;
  }
}

const char *doltliteGetAuthorEmail(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    return p->zAuthorEmail ? p->zAuthorEmail : "";
  }
  return "";
}

void doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    sqlite3_free(p->zAuthorEmail);
    p->zAuthorEmail = zEmail ? sqlite3_mprintf("%s", zEmail) : 0;
  }
}

void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(pHead, &db->aDb[0].pBt->headCommit, sizeof(ProllyHash));
  }else{
    memset(pHead, 0, sizeof(ProllyHash));
  }
}

void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->headCommit, pHead, sizeof(ProllyHash));
  }
}

void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(pStaged, &db->aDb[0].pBt->stagedCatalog, sizeof(ProllyHash));
  }else{
    memset(pStaged, 0, sizeof(ProllyHash));
  }
}

void doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->stagedCatalog, pStaged, sizeof(ProllyHash));
  }
}

void doltliteGetSessionMergeState(sqlite3 *db, u8 *pIsMerging,
                                   ProllyHash *pMergeCommit,
                                   ProllyHash *pConflictsCatalog){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    if( pIsMerging ) *pIsMerging = p->isMerging;
    if( pMergeCommit ) memcpy(pMergeCommit, &p->mergeCommitHash, sizeof(ProllyHash));
    if( pConflictsCatalog ) memcpy(pConflictsCatalog, &p->conflictsCatalogHash, sizeof(ProllyHash));
  }else{
    if( pIsMerging ) *pIsMerging = 0;
    if( pMergeCommit ) memset(pMergeCommit, 0, sizeof(ProllyHash));
    if( pConflictsCatalog ) memset(pConflictsCatalog, 0, sizeof(ProllyHash));
  }
}

void doltliteSetSessionMergeState(sqlite3 *db, u8 isMerging,
                                   const ProllyHash *pMergeCommit,
                                   const ProllyHash *pConflictsCatalog){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    p->isMerging = isMerging;
    if( pMergeCommit ) memcpy(&p->mergeCommitHash, pMergeCommit, sizeof(ProllyHash));
    else memset(&p->mergeCommitHash, 0, sizeof(ProllyHash));
    if( pConflictsCatalog ) memcpy(&p->conflictsCatalogHash, pConflictsCatalog, sizeof(ProllyHash));
    else memset(&p->conflictsCatalogHash, 0, sizeof(ProllyHash));
  }
}

void doltliteClearSessionMergeState(sqlite3 *db){
  doltliteSetSessionMergeState(db, 0, 0, 0);
}

void doltliteGetSessionRebaseState(sqlite3 *db, u8 *pIsRebasing,
                                    ProllyHash *pPreRebaseCat,
                                    ProllyHash *pRebaseOnto,
                                    const char **pzOrigBranch,
                                    const char **pzReturnBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    if( pIsRebasing ) *pIsRebasing = p->isRebasing;
    if( pPreRebaseCat ) memcpy(pPreRebaseCat, &p->preRebaseWorkingCat, sizeof(ProllyHash));
    if( pRebaseOnto ) memcpy(pRebaseOnto, &p->rebaseOntoCommit, sizeof(ProllyHash));
    if( pzOrigBranch ) *pzOrigBranch = p->zRebaseOrigBranch;
    if( pzReturnBranch ) *pzReturnBranch = p->zRebaseReturnBranch;
  }else{
    if( pIsRebasing ) *pIsRebasing = 0;
    if( pPreRebaseCat ) memset(pPreRebaseCat, 0, sizeof(ProllyHash));
    if( pRebaseOnto ) memset(pRebaseOnto, 0, sizeof(ProllyHash));
    if( pzOrigBranch ) *pzOrigBranch = 0;
    if( pzReturnBranch ) *pzReturnBranch = 0;
  }
}

void doltliteSetSessionRebaseState(sqlite3 *db, u8 isRebasing,
                                    const ProllyHash *pPreRebaseCat,
                                    const ProllyHash *pRebaseOnto,
                                    const char *zOrigBranch,
                                    const char *zReturnBranch){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *p = db->aDb[0].pBt;
    p->isRebasing = isRebasing;
    if( pPreRebaseCat ) memcpy(&p->preRebaseWorkingCat, pPreRebaseCat, sizeof(ProllyHash));
    else memset(&p->preRebaseWorkingCat, 0, sizeof(ProllyHash));
    if( pRebaseOnto ) memcpy(&p->rebaseOntoCommit, pRebaseOnto, sizeof(ProllyHash));
    else memset(&p->rebaseOntoCommit, 0, sizeof(ProllyHash));
    sqlite3_free(p->zRebaseOrigBranch);
    p->zRebaseOrigBranch = zOrigBranch ? sqlite3_mprintf("%s", zOrigBranch) : 0;
    sqlite3_free(p->zRebaseReturnBranch);
    p->zRebaseReturnBranch = zReturnBranch ? sqlite3_mprintf("%s", zReturnBranch) : 0;
  }
}

void doltliteClearSessionRebaseState(sqlite3 *db){
  doltliteSetSessionRebaseState(db, 0, 0, 0, 0, 0);
}

void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash){
  u8 isMerging = 0;
  if( pHash ) memset(pHash, 0, sizeof(*pHash));
  if( !db || db->nDb<=0 || !db->aDb[0].pBt || !pHash ) return;
  {
    Btree *p = db->aDb[0].pBt;
    if( !db->autoCommit || sqlite3_txn_state(db, "main")!=SQLITE_TXN_NONE || db->pSavepoint ){
      if( p->isMerging ){
        memcpy(pHash, &p->conflictsCatalogHash, sizeof(*pHash));
      }
      return;
    }
  }
  if( db->autoCommit && sqlite3_txn_state(db, "main")==SQLITE_TXN_NONE ){
    sqlite3 *db2 = 0;
    const char *zFilename = sqlite3_db_filename(db, "main");
    if( zFilename && sqlite3_open_v2(zFilename, &db2, SQLITE_OPEN_READONLY, 0)==SQLITE_OK
        && db2 && db2->nDb>0 && db2->aDb[0].pBt ){
      Btree *p2 = db2->aDb[0].pBt;
      Btree *p = db->aDb[0].pBt;
      const char *zBr = p->zBranch ? p->zBranch : "main";
      int rc = btreeLoadWorkingSetBlob(&p2->pBt->store, zBr,
                                       0, 0, 0, &isMerging,
                                       0, pHash, 0, 0, 0, 0, 0, 0);
      sqlite3_close(db2);
      if( rc!=SQLITE_OK || !isMerging ){
        memset(pHash, 0, sizeof(*pHash));
      }
      return;
    }
    if( db2 ) sqlite3_close(db2);
  }
  {
    Btree *p = db->aDb[0].pBt;
    const char *zBr = p->zBranch ? p->zBranch : "main";
    int rc = btreeLoadWorkingSetBlob(&p->pBt->store, zBr,
                                     0, 0, 0, &isMerging,
                                     0, pHash, 0, 0, 0, 0, 0, 0);
    if( rc!=SQLITE_OK || !isMerging ){
      memset(pHash, 0, sizeof(*pHash));
    }
  }
}

void doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->conflictsCatalogHash, pHash, sizeof(ProllyHash));
  }
}

void doltliteGetSessionConstraintViolationsCatalog(sqlite3 *db, ProllyHash *pHash){
  if( pHash ) memset(pHash, 0, sizeof(*pHash));
  if( db && db->nDb>0 && db->aDb[0].pBt && pHash ){
    memcpy(pHash, &db->aDb[0].pBt->constraintViolationsHash, sizeof(ProllyHash));
  }
}

int doltliteGetSessionTableRoot(
  sqlite3 *db, Pgno iTable, ProllyHash *pRoot, u8 *pFlags
){
  Btree *pBtree;
  struct TableEntry *pTE;
  if( pRoot ) memset(pRoot, 0, sizeof(*pRoot));
  if( pFlags ) *pFlags = 0;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  pTE = catFind(&pBtree->cat, iTable);
  if( !pTE ) return SQLITE_NOTFOUND;
  if( pRoot ) memcpy(pRoot, &pTE->root, sizeof(ProllyHash));
  if( pFlags ) *pFlags = pTE->flags;
  return SQLITE_OK;
}

void doltliteSetSessionConstraintViolationsCatalog(sqlite3 *db, const ProllyHash *pHash){
  static const ProllyHash emptyHash = {{0}};
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->constraintViolationsHash,
           pHash ? pHash : &emptyHash, sizeof(ProllyHash));
  }
}

int doltliteSessionHasConstraintViolations(sqlite3 *db){
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return 0;
  return !prollyHashIsEmpty(&db->aDb[0].pBt->constraintViolationsHash);
}

int doltliteSaveWorkingSetWithHash(sqlite3 *db, const ProllyHash *pWorkingCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash workingCatHash;
  ProllyHash wsHash;
  const char *zBranch;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  zBranch = pBtree->zBranch ? pBtree->zBranch : "main";

  if( pWorkingCatHash ){
    workingCatHash = *pWorkingCatHash;
  }else{
    rc = serializeCatalog(pBtree, &catData, &nCatData);
    if( rc != SQLITE_OK ) return rc;
    rc = chunkStorePut(cs, catData, nCatData, &workingCatHash);
    sqlite3_free(catData);
    if( rc != SQLITE_OK ) return rc;
  }

  rc = btreeStoreWorkingSetBlob(cs, zBranch, &workingCatHash,
                                &pBtree->headCommit, &pBtree->stagedCatalog,
                                pBtree->isMerging, &pBtree->mergeCommitHash,
                                &pBtree->conflictsCatalogHash,
                                pBtree->isRebasing,
                                &pBtree->preRebaseWorkingCat,
                                &pBtree->rebaseOntoCommit,
                                pBtree->zRebaseOrigBranch,
                                pBtree->zRebaseReturnBranch,
                                &pBtree->constraintViolationsHash);
  if( rc!=SQLITE_OK ) return rc;

  if( pBtree->isRebasing
   && pBtree->zRebaseReturnBranch
   && pBtree->zRebaseReturnBranch[0]
   && sqlite3_stricmp(zBranch, pBtree->zRebaseReturnBranch)!=0 ){
    rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
    if( rc!=SQLITE_OK ) return rc;
    rc = chunkStoreSetBranchWorkingSet(cs, pBtree->zRebaseReturnBranch, &wsHash);
    if( rc!=SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
}

int doltliteSaveWorkingSet(sqlite3 *db){
  return doltliteSaveWorkingSetWithHash(db, 0);
}

int doltlitePersistWorkingSetWithHash(sqlite3 *db, const ProllyHash *pWorkingCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = doltliteSaveWorkingSetWithHash(db, pWorkingCatHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(cs);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(cs);
}

int doltlitePersistWorkingSet(sqlite3 *db){
  return doltlitePersistWorkingSetWithHash(db, 0);
}

int doltliteGetPersistedWorkingCatalogHash(sqlite3 *db, ProllyHash *pCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  const char *zBranch;

  if( pCatHash ) memset(pCatHash, 0, sizeof(*pCatHash));
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;
  zBranch = pBtree->zBranch ? pBtree->zBranch : "main";
  return btreeReadWorkingCatalog(cs, zBranch, pCatHash, 0);
}

int doltliteLoadWorkingSet(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  char *zNewRebaseOrigBranch = 0;
  char *zNewRebaseReturnBranch = 0;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  rc = btreeLoadWorkingSetBlob(cs, zBranch, 0, 0,
                               &pBtree->stagedCatalog,
                               &pBtree->isMerging, &pBtree->mergeCommitHash,
                               &pBtree->conflictsCatalogHash,
                               &pBtree->isRebasing,
                               &pBtree->preRebaseWorkingCat,
                               &pBtree->rebaseOntoCommit,
                               &zNewRebaseOrigBranch,
                               &zNewRebaseReturnBranch,
                               &pBtree->constraintViolationsHash);
  if( rc == SQLITE_NOTFOUND ){
    memset(&pBtree->stagedCatalog, 0, sizeof(ProllyHash));
    pBtree->isMerging = 0;
    memset(&pBtree->mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&pBtree->conflictsCatalogHash, 0, sizeof(ProllyHash));
    pBtree->isRebasing = 0;
    memset(&pBtree->preRebaseWorkingCat, 0, sizeof(ProllyHash));
    memset(&pBtree->rebaseOntoCommit, 0, sizeof(ProllyHash));
    sqlite3_free(pBtree->zRebaseOrigBranch);
    pBtree->zRebaseOrigBranch = 0;
    sqlite3_free(pBtree->zRebaseReturnBranch);
    pBtree->zRebaseReturnBranch = 0;
    memset(&pBtree->constraintViolationsHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    sqlite3_free(pBtree->zRebaseOrigBranch);
    pBtree->zRebaseOrigBranch = zNewRebaseOrigBranch;
    sqlite3_free(pBtree->zRebaseReturnBranch);
    pBtree->zRebaseReturnBranch = zNewRebaseReturnBranch;
  }else{
    sqlite3_free(zNewRebaseOrigBranch);
    sqlite3_free(zNewRebaseReturnBranch);
  }
  return rc;
}

static int origBtreeCloseVt(Btree *p){
  int rc = origBtreeClose(p->pOrigBtree);
  p->pOrigBtree = 0;

  if( p->pSchema && p->xFreeSchema ) p->xFreeSchema(p->pSchema);
  sqlite3_free(p);
  return rc;
}
static int origBtreeNewDbVt(Btree *p){
  return origBtreeNewDb(p->pOrigBtree);
}
static int origBtreeSetCacheSizeVt(Btree *p, int mxPage){
  return origBtreeSetCacheSize(p->pOrigBtree, mxPage);
}
static int origBtreeSetSpillSizeVt(Btree *p, int mxPage){
  return origBtreeSetSpillSize(p->pOrigBtree, mxPage);
}
static int origBtreeSetMmapLimitVt(Btree *p, sqlite3_int64 szMmap){
  return origBtreeSetMmapLimit(p->pOrigBtree, szMmap);
}
static int origBtreeSetPagerFlagsVt(Btree *p, unsigned pgFlags){
  return origBtreeSetPagerFlags(p->pOrigBtree, pgFlags);
}
static int origBtreeSetPageSizeVt(Btree *p, int nPagesize, int nReserve, int eFix){
  return origBtreeSetPageSize(p->pOrigBtree, nPagesize, nReserve, eFix);
}
static int origBtreeGetPageSizeVt(Btree *p){
  return origBtreeGetPageSize(p->pOrigBtree);
}
static Pgno origBtreeMaxPageCountVt(Btree *p, Pgno mxPage){
  return origBtreeMaxPageCount(p->pOrigBtree, mxPage);
}
static Pgno origBtreeLastPageVt(Btree *p){
  return origBtreeLastPage(p->pOrigBtree);
}
static int origBtreeSecureDeleteVt(Btree *p, int newFlag){
  return origBtreeSecureDelete(p->pOrigBtree, newFlag);
}
static int origBtreeGetRequestedReserveVt(Btree *p){
  return origBtreeGetRequestedReserve(p->pOrigBtree);
}
static int origBtreeGetReserveNoMutexVt(Btree *p){
  return origBtreeGetReserveNoMutex(p->pOrigBtree);
}
static int origBtreeSetAutoVacuumVt(Btree *p, int autoVacuum){
  return origBtreeSetAutoVacuum(p->pOrigBtree, autoVacuum);
}
static int origBtreeGetAutoVacuumVt(Btree *p){
  return origBtreeGetAutoVacuum(p->pOrigBtree);
}
static int origBtreeIncrVacuumVt(Btree *p){
  return origBtreeIncrVacuum(p->pOrigBtree);
}
static const char *origBtreeGetFilenameVt(Btree *p){
  return origBtreeGetFilename(p->pOrigBtree);
}
static const char *origBtreeGetJournalnameVt(Btree *p){
  return origBtreeGetJournalname(p->pOrigBtree);
}
static int origBtreeIsReadonlyVt(Btree *p){
  return origBtreeIsReadonly(p->pOrigBtree);
}
static int origBtreeBeginTransVt(Btree *p, int wrFlag, int *pSchemaVersion){
  return origBtreeBeginTrans(p->pOrigBtree, wrFlag, pSchemaVersion);
}
static int origBtreeCommitPhaseOneVt(Btree *p, const char *zSuperJrnl){
  return origBtreeCommitPhaseOne(p->pOrigBtree, zSuperJrnl);
}
static int origBtreeCommitPhaseTwoVt(Btree *p, int bCleanup){
  return origBtreeCommitPhaseTwo(p->pOrigBtree, bCleanup);
}
static int origBtreeCommitVt(Btree *p){
  return origBtreeCommit(p->pOrigBtree);
}
static int origBtreeRollbackVt(Btree *p, int tripCode, int writeOnly){
  return origBtreeRollback(p->pOrigBtree, tripCode, writeOnly);
}
static int origBtreeBeginStmtVt(Btree *p, int iStatement){
  return origBtreeBeginStmt(p->pOrigBtree, iStatement);
}
static int origBtreeSavepointVt(Btree *p, int op, int iSavepoint){
  return origBtreeSavepoint(p->pOrigBtree, op, iSavepoint);
}
static int origBtreeTxnStateVt(Btree *p){
  return origBtreeTxnState(p->pOrigBtree);
}
static int origBtreeCreateTableVt(Btree *p, Pgno *piTable, int flags){
  return origBtreeCreateTable(p->pOrigBtree, piTable, flags);
}
static int origBtreeDropTableVt(Btree *p, int iTable, int *piMoved){
  return origBtreeDropTable(p->pOrigBtree, iTable, piMoved);
}
static int origBtreeClearTableVt(Btree *p, int iTable, i64 *pnChange){
  return origBtreeClearTable(p->pOrigBtree, iTable, pnChange);
}
static void origBtreeGetMetaVt(Btree *p, int idx, u32 *pValue){
  origBtreeGetMeta(p->pOrigBtree, idx, pValue);
}
static int origBtreeUpdateMetaVt(Btree *p, int idx, u32 value){
  return origBtreeUpdateMeta(p->pOrigBtree, idx, value);
}
static void *origBtreeSchemaVt(Btree *p, int nBytes, void (*xFree)(void*)){
  return (void*)origBtreeSchema(p->pOrigBtree, nBytes, xFree);
}
static int origBtreeSchemaLockedVt(Btree *p){
  return origBtreeSchemaLocked(p->pOrigBtree);
}
static int origBtreeLockTableVt(Btree *p, int iTab, u8 isWriteLock){
  return origBtreeLockTable(p->pOrigBtree, iTab, isWriteLock);
}
static int origBtreeCursorVt(Btree *p, Pgno iTable, int wrFlag,
                             struct KeyInfo *pKeyInfo, BtCursor *pCur){
  void *pOC = sqlite3_malloc(origBtreeCursorSize());
  if( !pOC ) return SQLITE_NOMEM;
  memset(pOC, 0, origBtreeCursorSize());
  pCur->pOrigCursor = pOC;
  pCur->pCurOps = &origCursorVtOps;
  pCur->pBtree = p;
  return origBtreeCursor(p->pOrigBtree, iTable, wrFlag, pKeyInfo, pOC);
}
static void origBtreeEnterVt(Btree *p){
  origBtreeEnter(p->pOrigBtree);
}
static void origBtreeLeaveVt(Btree *p){
  origBtreeLeave(p->pOrigBtree);
}
static struct Pager *origBtreePagerVt(Btree *p){
  return (struct Pager*)origBtreePager(p->pOrigBtree);
}
#ifdef SQLITE_DEBUG
static int origBtreeClosesWithCursorVt(Btree *p, BtCursor *pCur){
  (void)p; (void)pCur;
  return 1;
}
#endif

static int origCursorClearTableOfCursorVt(BtCursor *pCur){
  return origBtreeClearTableOfCursor(pCur->pOrigCursor);
}
static int origCursorCloseCursorVt(BtCursor *pCur){
  Btree *pWrapper = pCur->pBtree;
  int willAutoCloseInner =
      origBtreeCursorIsLastOnSingle(pCur->pOrigCursor);
  int rc = origBtreeCloseCursor(pCur->pOrigCursor);
  sqlite3_free(pCur->pOrigCursor);
  pCur->pOrigCursor = 0;
  if( willAutoCloseInner && pWrapper ){
    pWrapper->pOrigBtree = 0;
    sqlite3_free(pWrapper);
  }
  return rc;
}
static int origCursorCursorHasMovedVt(BtCursor *pCur){
  return origBtreeCursorHasMoved(pCur->pOrigCursor);
}
static int origCursorCursorRestoreVt(BtCursor *pCur, int *pDifferentRow){
  return origBtreeCursorRestore(pCur->pOrigCursor, pDifferentRow);
}
static int origCursorFirstVt(BtCursor *pCur, int *pRes){
  return origBtreeFirst(pCur->pOrigCursor, pRes);
}
static int origCursorLastVt(BtCursor *pCur, int *pRes){
  return origBtreeLast(pCur->pOrigCursor, pRes);
}
static int origCursorNextVt(BtCursor *pCur, int flags){
  return origBtreeNext(pCur->pOrigCursor, flags);
}
static int origCursorPreviousVt(BtCursor *pCur, int flags){
  return origBtreePrevious(pCur->pOrigCursor, flags);
}
static int origCursorEofVt(BtCursor *pCur){
  return origBtreeEof(pCur->pOrigCursor);
}
static int origCursorIsEmptyVt(BtCursor *pCur, int *pRes){
  return origBtreeIsEmpty(pCur->pOrigCursor, pRes);
}
static int origCursorTableMovetoVt(BtCursor *pCur, i64 intKey, int bias, int *pRes){
  return origBtreeTableMoveto(pCur->pOrigCursor, intKey, bias, pRes);
}
static int origCursorIndexMovetoVt(BtCursor *pCur, UnpackedRecord *pIdxKey, int *pRes){
  return origBtreeIndexMoveto(pCur->pOrigCursor, pIdxKey, pRes);
}
static i64 origCursorIntegerKeyVt(BtCursor *pCur){
  return origBtreeIntegerKey(pCur->pOrigCursor);
}
static u32 origCursorPayloadSizeVt(BtCursor *pCur){
  return origBtreePayloadSize(pCur->pOrigCursor);
}
static int origCursorPayloadVt(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return origBtreePayload(pCur->pOrigCursor, offset, amt, pBuf);
}
static const void *origCursorPayloadFetchVt(BtCursor *pCur, u32 *pAmt){
  return origBtreePayloadFetch(pCur->pOrigCursor, pAmt);
}
static sqlite3_int64 origCursorMaxRecordSizeVt(BtCursor *pCur){
  return origBtreeMaxRecordSize(pCur->pOrigCursor);
}
static i64 origCursorOffsetVt(BtCursor *pCur){
  (void)pCur;
  return -1;
}
static int origCursorInsertVt(BtCursor *pCur, const BtreePayload *pPayload, int flags, int seekResult){
  return origBtreeInsert(pCur->pOrigCursor, pPayload, flags, seekResult);
}
static int origCursorDeleteVt(BtCursor *pCur, u8 flags){
  return origBtreeDelete(pCur->pOrigCursor, flags);
}
static int origCursorTransferRowVt(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  return origBtreeTransferRow(pDest->pOrigCursor, pSrc->pOrigCursor, iKey);
}
static void origCursorClearCursorVt(BtCursor *pCur){
  (void)pCur;

}
static int origCursorCountVt(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  return origBtreeCount(db, pCur->pOrigCursor, pnEntry);
}
static i64 origCursorRowCountEstVt(BtCursor *pCur){
  (void)pCur;
  return -1;
}
static void origCursorCursorPinVt(BtCursor *pCur){
  origBtreeCursorPin(pCur->pOrigCursor);
}
static void origCursorCursorUnpinVt(BtCursor *pCur){
  origBtreeCursorUnpin(pCur->pOrigCursor);
}
static void origCursorCursorHintFlagsVt(BtCursor *pCur, unsigned x){
  (void)pCur; (void)x;

}
static int origCursorCursorHasHintVt(BtCursor *pCur, unsigned int mask){
  (void)pCur; (void)mask;
  return 0;
}
#ifndef SQLITE_OMIT_INCRBLOB
static int origCursorPayloadCheckedVt(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  return origBtreePayloadChecked(pCur->pOrigCursor, offset, amt, pBuf);
}
static int origCursorPutDataVt(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  (void)pCur; (void)offset; (void)amt; (void)pBuf;
  return SQLITE_OK;
}
static void origCursorIncrblobCursorVt(BtCursor *pCur){
  (void)pCur;

}
#endif
#ifndef NDEBUG
static int origCursorCursorIsValidVt(BtCursor *pCur){
  (void)pCur;
  return 1;
}
#endif
static int origCursorCursorIsValidNNVt(BtCursor *pCur){
  (void)pCur;
  return 1;
}

extern void doltliteRegister(sqlite3 *db);

static void registerDoltiteFunctions(sqlite3 *db){
  sqlite3_create_function(db, "doltlite_engine", 0, SQLITE_UTF8, 0,
                          doltiteEngineFunc, 0, 0);
  doltliteRegister(db);
}

static int doltliteExtInit(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  (void)pzErrMsg;
  (void)pApi;
  registerDoltiteFunctions(db);
  return SQLITE_OK;
}

int doltliteInstallAutoExt(void){
  return sqlite3_auto_extension((void(*)(void))doltliteExtInit);
}

#endif
