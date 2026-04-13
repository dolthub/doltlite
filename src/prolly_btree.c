
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "btree.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_hashset.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include "prolly_chunk_walk.h"
#include "pager_shim.h"
#include "doltlite_commit.h"
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
#define MAX_RECORD_FIELDS     64  
#define MAX_ONEBYTE_HEADER   126  

static void registerDoltiteFunctions(sqlite3 *db);
void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);

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
#define BTS_PAGESIZE_FIXED  0x0002  
#define BTS_SECURE_DELETE   0x0004  
#define BTS_OVERWRITE       0x0008  
#define BTS_INITIALLY_EMPTY 0x0010  
#define BTS_NO_WAL          0x0020  

#define CLEAR_CACHED_PAYLOAD(pCur) do{ \
  if( (pCur)->cachedPayloadOwned && (pCur)->pCachedPayload ){ \
    sqlite3_free((pCur)->pCachedPayload); \
  } \
  (pCur)->pCachedPayload = 0; \
  (pCur)->nCachedPayload = 0; \
  (pCur)->cachedPayloadOwned = 0; \
}while(0)

typedef struct BtLock BtLock;
struct BtLock {
  Btree *pBtree;        
  Pgno iTable;           
  u8 eLock;              
  BtLock *pNext;         
};

#define PROLLY_DEFAULT_CACHE_SIZE 1024

#define PROLLY_DEFAULT_PAGE_SIZE 4096

#define PROLLY_MAX_RECORD_SIZE ((sqlite3_int64)(1024*1024*1024))

static const ProllyHash emptyHash = {{0}};

/* Maps a SQLite table number (Pgno) to its prolly tree root hash.
** pPending holds deferred MutMap edits not yet flushed to the tree.
** MUST be flushed before any cursor reads, or the cursor sees stale data. */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
  char *zName;
  ProllyMutMap *pPending;
};

/* Shared backend: owns the content-addressed chunk store and node cache.
** pPagerShim exists solely to satisfy SQLite's pager-level queries (e.g.,
** iDataVersion for schema change detection) -- no actual paging occurs. */
struct BtShared {
  ChunkStore store;
  ProllyCache cache;
  PagerShim *pPagerShim;
  sqlite3 *db;
  BtCursor *pCursor;         /* Linked list of all open cursors */
  u8 openFlags;
  u16 btsFlags;
  u32 pageSize;
  int nRef;
};

/* Vtable that dispatches every SQLite Btree API call to either the prolly
** tree implementation or the original SQLite b-tree (for non-DoltLite files
** like temp databases). This is how a single binary supports both engines. */
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

/* Per-connection Btree handle implementing the SQLite Btree interface.
** 'aTables': sorted catalog mapping table numbers to prolly tree roots.
** 'aSavepointTables': savepoint stack for ROLLBACK TO.
** 'aCommittedTables': catalog snapshot at BEGIN for full transaction rollback.
** 'pOps': dispatches to prolly or original SQLite btree implementation. */
struct Btree {
  sqlite3 *db;
  BtShared *pBt;
  u8 inTrans;
  u8 sharable;
  int wantToLock;
  int nBackup;
  u32 iBDataVersion;
  Btree *pNext;
  Btree *pPrev;
  BtLock lock;
  u64 nSeek;

  struct TableEntry *aTables; /* Sorted by iTable for binary search */
  int nTables;
  int nTablesAlloc;
  Pgno iNextTable;           /* Monotonically increasing table number allocator */

  u32 aMeta[16];

  void *pSchema;
  void (*xFreeSchema)(void*);

  u8 inTransaction;

  /* Savepoint stack: aSavepointTables[i] = snapshot of aTables + pending
  ** edit counts. On rollback, roots are restored and MutMap entries truncated. */
  int nSavepoint;
  int nSavepointAlloc;

  struct SavepointTableState {
    struct TableEntry *aTables;
    int *aPendingCount;     /* MutMap nEntries per table at savepoint time */
    int nTables;
    Pgno iNextTable;
    ProllyHash stagedCatalog;
    u8 isMerging;
    ProllyHash mergeCommitHash;
    ProllyHash conflictsCatalogHash;
  } *aSavepointTables;

  /* Snapshot of aTables at BEGIN WRITE -- for full transaction rollback */
  struct TableEntry *aCommittedTables;
  int nCommittedTables;
  Pgno iCommittedNextTable;
  ProllyHash committedStagedCatalog;
  u8 committedIsMerging;
  ProllyHash committedMergeCommitHash;
  ProllyHash committedConflictsCatalogHash;

  /* DoltLite versioning state */
  char *zBranch;
  char *zAuthorName;
  char *zAuthorEmail;
  ProllyHash headCommit;
  ProllyHash stagedCatalog;
  u8 isMerging;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;
  const struct BtreeOps *pOps;
  void *pOrigBtree;   /* Non-NULL only for original SQLite btree files */
};

/* Cursor that merges two sources: the immutable prolly tree (pCur) and an
** in-memory edit buffer (pMutMap). When mmActive is true, Next/Prev
** interleave both sources in key order (see mergeStepForward/Backward).
** pCachedPayload caches the current row -- MUST be cleared via
** CLEAR_CACHED_PAYLOAD on any cursor movement or caller sees stale data. */
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

  ProllyCursor pCur;      /* Cursor into the immutable prolly tree */
  ProllyMutMap *pMutMap;  /* In-memory edit buffer (inserts + deletes) */

  u8 *pCachedPayload;
  int nCachedPayload;
  u8 cachedPayloadOwned;  /* 1 if pCachedPayload was malloc'd by us */
  i64 cachedIntKey;

  u8 isPinned;            /* When pinned, saveCursorPosition is a no-op */
  u8 flushSeekEdits;      /* Flush pending deletes before next IndexMoveto */

  /* Merge iteration state: mmIdx indexes into pMutMap->aEntries.
  ** mergeSrc says where the current row comes from. */
  int mmIdx;
  u8 mmActive;
#define MERGE_SRC_TREE  0
#define MERGE_SRC_MUT   1
#define MERGE_SRC_BOTH  2   /* Same key in tree and MutMap; MutMap wins */
  u8 mergeSrc;

  /* Saved position for CURSOR_REQUIRESEEK restore */
  i64 nKey;
  void *pKey;
  u64 nSeek;
  void *pOrigCursor;
  const struct BtCursorOps *pCurOps;
};

static struct TableEntry *findTable(Btree *pBtree, Pgno iTable);
static struct TableEntry *addTable(Btree *pBtree, Pgno iTable, u8 flags);
static void removeTable(Btree *pBtree, Pgno iTable);
static void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode);
static void invalidateSchema(Btree *pBtree);
static int flushMutMap(BtCursor *pCur);
static void getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData);
static int flushIfNeeded(BtCursor *pCur);
static int flushAllPending(BtShared *pBt, Pgno iTable);
static int applyMutMapToTableRoot(BtShared *pBt, struct TableEntry *pTE, ProllyMutMap *pMap);
static int cacheCursorPayloadCopy(BtCursor *pCur, const u8 *pData, int nData);
static int flushDeferredEdits(BtShared *pBt);
static int ensureMutMap(BtCursor *pCur);
static int saveCursorPosition(BtCursor *pCur);
static int restoreCursorPosition(BtCursor *pCur, int *pDifferentRow);
static int pushSavepoint(Btree *pBtree);
static int restoreFromCommitted(Btree *p);
static void refreshCursorRoot(BtCursor *pCur);
static int countTreeEntries(Btree *pBtree, Pgno iTable, i64 *pCount);
static int saveAllCursors(BtShared *pBt, Pgno iRoot, BtCursor *pExcept);
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
  ProllyHash *pConflicts
);
static int btreeStoreWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pWorkingCat,
  const ProllyHash *pWorkingCommit,
  const ProllyHash *pStaged,
  u8 isMerging,
  const ProllyHash *pMergeCommit,
  const ProllyHash *pConflicts
);
static int btreeWriteWorkingState(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pCatHash,
  const ProllyHash *pCommitHash
);
static int btreeDeleteImmediate(BtCursor *pCur, const u8 *pKey, int nKey, i64 iKey);

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

static struct TableEntry *findTable(Btree *pBtree, Pgno iTable){
  int lo = 0, hi = pBtree->nTables - 1;
  while( lo<=hi ){
    int mid = lo + (hi - lo) / 2;
    Pgno midTable = pBtree->aTables[mid].iTable;
    if( midTable==iTable ){
      return &pBtree->aTables[mid];
    } else if( midTable<iTable ){
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return 0;
}

static struct TableEntry *addTable(Btree *pBtree, Pgno iTable, u8 flags){
  struct TableEntry *pEntry;

  
  pEntry = findTable(pBtree, iTable);
  if( pEntry ){
    pEntry->flags = flags;
    return pEntry;
  }

  
  if( pBtree->nTables>=pBtree->nTablesAlloc ){
    int nNew = pBtree->nTablesAlloc ? pBtree->nTablesAlloc*2 : 16;
    struct TableEntry *aNew;
    aNew = sqlite3_realloc(pBtree->aTables, nNew*(int)sizeof(struct TableEntry));
    if( !aNew ) return 0;
    pBtree->aTables = aNew;
    pBtree->nTablesAlloc = nNew;
  }

  
  {
    int lo = 0, hi = pBtree->nTables;
    while( lo<hi ){
      int mid = lo + (hi - lo) / 2;
      if( pBtree->aTables[mid].iTable < iTable ){
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    
    if( lo < pBtree->nTables ){
      memmove(&pBtree->aTables[lo+1], &pBtree->aTables[lo],
              (pBtree->nTables - lo) * (int)sizeof(struct TableEntry));
    }
    pEntry = &pBtree->aTables[lo];
  }
  memset(pEntry, 0, sizeof(*pEntry));
  pEntry->iTable = iTable;
  pEntry->flags = flags;
  pBtree->nTables++;

  return pEntry;
}

static void removeTable(Btree *pBtree, Pgno iTable){
  int i;
  for(i=0; i<pBtree->nTables; i++){
    if( pBtree->aTables[i].iTable==iTable ){
      if( i<pBtree->nTables-1 ){
        memmove(&pBtree->aTables[i], &pBtree->aTables[i+1],
                (pBtree->nTables-i-1)*(int)sizeof(struct TableEntry));
      }
      pBtree->nTables--;
      return;
    }
  }
}

static void invalidateSchema(Btree *pBtree){
  if( pBtree->pSchema && pBtree->xFreeSchema ){
    pBtree->xFreeSchema(pBtree->pSchema);
    pBtree->pSchema = 0;
  }
}

static void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode){
  BtCursor *p;
  for(p=pBt->pCursor; p; p=p->pNext){
    if( iTable==0 || p->pgnoRoot==iTable ){
      p->eState = CURSOR_FAULT;
      p->skipNext = errCode;
      p->mmActive = 0;
      /* Release cached tree positions but do NOT clear pMutMap — it may
      ** be shared with the table's pPending, and the savepoint rollback
      ** path has already truncated it to the correct saved count.
      ** Clearing it would destroy changes from outer savepoints. */
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

/* Catalog serialization — see chunk_store.h for format docs. */
/*
** Compare two TableEntry structs by name for deterministic catalog ordering.
** NULL names sort before all others (table 1 / sqlite_master has no name).
*/
static int tableEntryNameCmp(const void *a, const void *b){
  const struct TableEntry *ea = (const struct TableEntry *)a;
  const struct TableEntry *eb = (const struct TableEntry *)b;
  if( !ea->zName && !eb->zName ) return 0;
  if( !ea->zName ) return -1;
  if( !eb->zName ) return 1;
  return strcmp(ea->zName, eb->zName);
}

int doltliteSerializeCatalogEntries(
  sqlite3 *db,
  struct TableEntry *aTables,
  int nTables,
  u8 **ppOut,
  int *pnOut
){
  int sz = CAT_HEADER_SIZE_V3;
  u8 *buf, *q;
  struct TableEntry *aSorted;  /* name-sorted copy for deterministic output */
  int i;

  /* Refresh table names from sqlite_master so serialization is deterministic
  ** and reflects the current schema. A simple "resolve only when null" check
  ** would leave zName stale after ALTER TABLE ... RENAME, which would in turn
  ** make dolt_status invisible to the rename. */
  if( db ){
    for(i=0; i<nTables; i++){
      if( aTables[i].iTable>1 ){
        char *zCur = doltliteResolveTableNumber(db, aTables[i].iTable);
        if( zCur ){
          sqlite3_free(aTables[i].zName);
          aTables[i].zName = zCur;
        }
      }
    }
  }

  /* Create a shallow copy sorted by name. The in-memory aTables array
  ** stays sorted by iTable (binary search by page number depends on it).
  ** Empty catalogs are legal (e.g. dolt_add('-A') with no working tables);
  ** sqlite3_malloc(0) returns NULL, which would otherwise be misread as
  ** SQLITE_NOMEM. */
  if( nTables > 0 ){
    aSorted = sqlite3_malloc(nTables * (int)sizeof(struct TableEntry));
    if( !aSorted ) return SQLITE_NOMEM;
    memcpy(aSorted, aTables, nTables * (int)sizeof(struct TableEntry));
    qsort(aSorted, nTables, sizeof(struct TableEntry), tableEntryNameCmp);
  }else{
    aSorted = 0;
  }

  for(i=0; i<nTables; i++){
    int nLen = aSorted[i].zName ? (int)strlen(aSorted[i].zName) : 0;
    if( sz > 0x7FFFFFFF - (4 + 1 + PROLLY_HASH_SIZE*2 + 2 + nLen) ){
      sqlite3_free(aSorted);
      return SQLITE_TOOBIG;
    }
    sz += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2 + nLen;
  }

  buf = sqlite3_malloc(sz);
  if( !buf ){
    sqlite3_free(aSorted);
    return SQLITE_NOMEM;
  }
  q = buf;

  *q++ = CATALOG_FORMAT_V3;
  q[0]=(u8)nTables; q[1]=(u8)(nTables>>8);
  q[2]=(u8)(nTables>>16); q[3]=(u8)(nTables>>24);
  q += 4;

  for(i=0; i<nTables; i++){
    struct TableEntry *t = &aSorted[i];
    u32 pg = t->iTable;
    int nLen = t->zName ? (int)strlen(t->zName) : 0;
    q[0]=(u8)pg; q[1]=(u8)(pg>>8); q[2]=(u8)(pg>>16); q[3]=(u8)(pg>>24);
    q += 4;
    *q++ = t->flags;
    memcpy(q, t->root.data, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    memcpy(q, t->schemaHash.data, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    q[0]=(u8)nLen; q[1]=(u8)(nLen>>8); q+=2;
    if( nLen>0 ) memcpy(q, t->zName, nLen);
    q += nLen;
  }
  sqlite3_free(aSorted);
  *ppOut = buf;
  *pnOut = (int)(q - buf);
  return SQLITE_OK;
}

static int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut){
  return doltliteSerializeCatalogEntries(
      pBtree->db, pBtree->aTables, pBtree->nTables, ppOut, pnOut);
}

static void initDefaultMeta(Btree *pBtree){
  memset(pBtree->aMeta, 0, sizeof(pBtree->aMeta));
  pBtree->aMeta[BTREE_FILE_FORMAT] = 4;
  pBtree->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;
  
}

static int deserializeCatalog(Btree *pBtree, const u8 *data, int nData){
  const u8 *q = data;
  int nTables, i;

  {
    const u8 *pEntries;
    if( !catalogParseHeader(data, nData, &nTables, &pEntries) ){
      return SQLITE_CORRUPT;
    }
    q = pEntries;
  }

  sqlite3_free(pBtree->aTables);
  pBtree->aTables = 0;
  pBtree->nTables = 0;
  pBtree->nTablesAlloc = 0;
  initDefaultMeta(pBtree);

  /* Derive BTREE_SCHEMA_VERSION from a hash of the entire catalog blob.
  ** SQLite uses this value to detect schema changes and invalidate prepared
  ** statements. The |1 ensures it is never zero (zero means "no schema").
  ** This is a content-derived substitute for SQLite's incrementing counter. */
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
    if( q+4+1+PROLLY_HASH_SIZE > data+nData ) return SQLITE_CORRUPT;
    iTable = (Pgno)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
    q += 4;
    flags = *q++;
    pTE = addTable(pBtree, iTable, flags);
    if( !pTE ) return SQLITE_NOMEM;
    memcpy(pTE->root.data, q, PROLLY_HASH_SIZE);
    q += PROLLY_HASH_SIZE;
    if( q + PROLLY_HASH_SIZE <= data+nData ){
      memcpy(pTE->schemaHash.data, q, PROLLY_HASH_SIZE);
      q += PROLLY_HASH_SIZE;
    }
    if( q+2 <= data+nData ){
      nLen = q[0] | (q[1]<<8); q += 2;
      if( nLen>0 && q+nLen<=data+nData ){
        pTE->zName = sqlite3_malloc(nLen+1);
        if( pTE->zName ){
          memcpy(pTE->zName, q, nLen);
          pTE->zName[nLen] = 0;
        }else{
          return SQLITE_NOMEM;
        }
        q += nLen;
      }
    }
  }

  
  {
    Pgno maxPage = 0;
    for(i=0; i<pBtree->nTables; i++){
      if( pBtree->aTables[i].iTable > maxPage ){
        maxPage = pBtree->aTables[i].iTable;
      }
    }
    pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE] = maxPage;
    /* Derive iNextTable from the data — the stored value was zeroed
    ** in the catalog format so the content hash is deterministic. */
    pBtree->iNextTable = maxPage + 1;
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

static void clearMergeCursorState(BtCursor *pCur){
  pCur->mmIdx = -1;
  pCur->mmActive = 0;
  pCur->mergeSrc = MERGE_SRC_TREE;
}

static void setCursorToMutMapEntry(BtCursor *pCur, int idx){
  ProllyMutMapEntry *pEntry = &pCur->pMutMap->aEntries[idx];
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->mmIdx = idx;
  pCur->mmActive = 1;
  pCur->mergeSrc = MERGE_SRC_MUT;
  pCur->eState = CURSOR_VALID;
  pCur->curFlags &= ~BTCF_AtLast;
  if( pCur->curIntKey ){
    pCur->cachedIntKey = pEntry->intKey;
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

  if( !pCur->pMutMap || prollyMutMapIsEmpty(pCur->pMutMap) ){
    return SQLITE_OK;
  }

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    return SQLITE_INTERNAL;
  }

  {
    int rc = applyMutMapToTableRoot(pCur->pBt, pTE, pCur->pMutMap);
    if( rc!=SQLITE_OK ) return rc;
  }
  pCur->pCur.root = pTE->root;
  prollyMutMapClear(pCur->pMutMap);

  return SQLITE_OK;
}

/* SQLite may open implicit statement savepoints (nStatement) in addition to
** explicit user savepoints (nSavepoint). Our savepoint stack must match
** their combined count, or ROLLBACK TO will index out of bounds.
** This is called before any write to ensure we're caught up. */
static int syncSavepoints(BtCursor *pCur){
  Btree *pBtree = pCur->pBtree;
  sqlite3 *db = pBtree ? pBtree->db : 0;
  if( db ){
    int target = db->nSavepoint + db->nStatement;
    while( pBtree->nSavepoint < target ){
      int rc = pushSavepoint(pBtree);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int ensureMutMap(BtCursor *pCur){
  int rc;
  if( pCur->pMutMap ){
    return SQLITE_OK;
  }
  pCur->pMutMap = sqlite3_malloc(sizeof(ProllyMutMap));
  if( !pCur->pMutMap ){
    return SQLITE_NOMEM;
  }
  rc = prollyMutMapInit(pCur->pMutMap, pCur->curIntKey);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pCur->pMutMap);
    pCur->pMutMap = 0;
    return rc;
  }
  return SQLITE_OK;
}

static int saveCursorPosition(BtCursor *pCur){
  int rc = SQLITE_OK;

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
      pCur->nKey = pCur->pMutMap->aEntries[pCur->mmIdx].intKey;
    }else{
      pCur->nKey = prollyCursorIntKey(&pCur->pCur);
    }
    pCur->pKey = 0;
  } else {
    const u8 *pKey = 0;
    int nKey = 0;
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      pKey = pCur->pMutMap->aEntries[pCur->mmIdx].pKey;
      nKey = pCur->pMutMap->aEntries[pCur->mmIdx].nKey;
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

  rc = prollyCursorSave(&pCur->pCur);
  if( rc!=SQLITE_OK ){
    return rc;
  }

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
  if( pState->aTables ){
    sqlite3_free(pState->aTables);
    pState->aTables = 0;
  }
  if( pState->aPendingCount ){
    sqlite3_free(pState->aPendingCount);
    pState->aPendingCount = 0;
  }
  memset(&pState->stagedCatalog, 0, sizeof(pState->stagedCatalog));
  pState->isMerging = 0;
  memset(&pState->mergeCommitHash, 0, sizeof(pState->mergeCommitHash));
  memset(&pState->conflictsCatalogHash, 0, sizeof(pState->conflictsCatalogHash));
}

static void truncatePendingToCount(ProllyMutMap *pMap, int savedCount){
  while( pMap && pMap->nEntries > savedCount ){
    ProllyMutMapEntry *e;
    pMap->nEntries--;
    e = &pMap->aEntries[pMap->nEntries];
    sqlite3_free(e->pKey);
    e->pKey = 0;
    sqlite3_free(e->pVal);
    e->pVal = 0;
  }
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

static int restoreTablesFromSavepoint(
  Btree *pBtree,
  struct SavepointTableState *pState
){
  ProllyMutMap **apPending = 0;
  int k;

  if( pState->nTables>0 ){
    apPending = sqlite3_malloc64(
        pState->nTables * sizeof(ProllyMutMap*));
    if( !apPending ) return SQLITE_NOMEM;
    memset(apPending, 0, pState->nTables * sizeof(ProllyMutMap*));

    if( pBtree->nTablesAlloc < pState->nTables ){
      struct TableEntry *aNew = sqlite3_realloc(
          pBtree->aTables, pState->nTables * (int)sizeof(struct TableEntry));
      if( !aNew ){
        sqlite3_free(apPending);
        return SQLITE_NOMEM;
      }
      pBtree->aTables = aNew;
      pBtree->nTablesAlloc = pState->nTables;
    }
  }

  for(k=0; k<pBtree->nTables; k++){
    ProllyMutMap *pMap = pBtree->aTables[k].pPending;
    int iSaved;
    if( !pMap ) continue;
    iSaved = findTableIndexInArray(
        pState->aTables, pState->nTables, pBtree->aTables[k].iTable);
    if( iSaved>=0 ){
      truncatePendingToCount(pMap, pState->aPendingCount[iSaved]);
      apPending[iSaved] = pMap;
    }else{
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
    }
  }

  if( pState->nTables>0 ){
    memcpy(pBtree->aTables, pState->aTables,
           pState->nTables * sizeof(struct TableEntry));
    for(k=0; k<pState->nTables; k++){
      pBtree->aTables[k].pPending = apPending[k];
    }
  }else{
    sqlite3_free(pBtree->aTables);
    pBtree->aTables = 0;
    pBtree->nTablesAlloc = 0;
  }

  pBtree->nTables = pState->nTables;
  pBtree->iNextTable = pState->iNextTable;
  sqlite3_free(apPending);
  return SQLITE_OK;
}

/* Snapshot the current state so ROLLBACK TO can restore it.
** Saves: (1) the current prolly root hash, (2) each table's root hash,
** (3) the nEntries count of each table's pending MutMap.
** On rollback, table roots are restored and MutMap entries are truncated
** back to the saved count -- this works because MutMap is append-only.
** IMPORTANT: all cursor MutMaps must be flushed first, because the
** savepoint only records table-level root hashes and MutMap counts. */
static int pushSavepoint(Btree *pBtree){
  struct SavepointTableState *pState;
  int rc;

  /* Flush all cursor edits before snapshotting */
  {
    BtCursor *p;
    for(p = pBtree->pBt->pCursor; p; p = p->pNext){
      rc = flushIfNeeded(p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  if( pBtree->nSavepoint>=pBtree->nSavepointAlloc ){
    int nNew = pBtree->nSavepointAlloc ? pBtree->nSavepointAlloc*2 : 8;
    struct SavepointTableState *aNewT;
    aNewT = sqlite3_realloc(pBtree->aSavepointTables, nNew*(int)sizeof(struct SavepointTableState));
    if( !aNewT ) return SQLITE_NOMEM;
    pBtree->aSavepointTables = aNewT;
    pBtree->nSavepointAlloc = nNew;
  }

  pState = &pBtree->aSavepointTables[pBtree->nSavepoint];
  pState->aTables = 0;
  pState->aPendingCount = 0;
  pState->nTables = 0;
  pState->iNextTable = pBtree->iNextTable;
  pState->stagedCatalog = pBtree->stagedCatalog;
  pState->isMerging = pBtree->isMerging;
  pState->mergeCommitHash = pBtree->mergeCommitHash;
  pState->conflictsCatalogHash = pBtree->conflictsCatalogHash;
  if( pBtree->nTables > 0 ){
    pState->aTables = sqlite3_malloc(pBtree->nTables * (int)sizeof(struct TableEntry));
    if( !pState->aTables ) return SQLITE_NOMEM;
    pState->aPendingCount = sqlite3_malloc(pBtree->nTables * (int)sizeof(int));
    if( !pState->aPendingCount ){
      sqlite3_free(pState->aTables);
      pState->aTables = 0;
      return SQLITE_NOMEM;
    }
    memcpy(pState->aTables, pBtree->aTables,
           pBtree->nTables * sizeof(struct TableEntry));
    pState->nTables = pBtree->nTables;
    {
      int k;
      for(k=0; k<pState->nTables; k++){
        ProllyMutMap *pMap = (ProllyMutMap*)pState->aTables[k].pPending;
        pState->aPendingCount[k] = pMap ? pMap->nEntries : 0;
        pState->aTables[k].pPending = 0;  
      }
    }
  }

  pBtree->nSavepoint++;
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

  prollyCursorInit(&tempCur, &pBt->store, &pBt->cache,
                    &pTE->root, pTE->flags);

  rc = prollyCursorFirst(&tempCur, &res);
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

static int saveAllCursors(BtShared *pBt, Pgno iRoot, BtCursor *pExcept){
  BtCursor *p;
  for(p=pBt->pCursor; p; p=p->pNext){
    if( p!=pExcept && (iRoot==0 || p->pgnoRoot==iRoot) ){
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

  /* Delegate to the original SQLite btree implementation for:
  ** - NULL/empty filenames
  ** - attached explicit in-memory databases
  ** - BTREE_SINGLE flag (transient/ephemeral btrees)
  ** - Temp database (SQLITE_OPEN_TEMP_DB in vfsFlags)
  ** - Existing files with standard SQLite headers
  **
  ** Keep the main ":memory:" database on the Doltlite path so the shell and
  ** SQL functions/modules are registered normally. */
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

  pBt->pPagerShim = pagerShimCreate(pVfs, zFilename, pBt->store.pFile);
  if( !pBt->pPagerShim ){
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }

  pBt->db = db;
  pBt->pageSize = PROLLY_DEFAULT_PAGE_SIZE;
  pBt->nRef = 1;
  pBt->openFlags = (u8)flags;
  p->inTransaction = TRANS_NONE;

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

  
  /* Load catalog from the default branch working-set blob. Use the default branch
  ** since p->zBranch hasn't been set yet (checkout happens later). */
  {
    ProllyHash catHash;
    ProllyHash workingCommit;
    ProllyHash stagedCatalog;
    ProllyHash mergeCommitHash;
    ProllyHash conflictsCatalogHash;
    ProllyHash branchCommit;
    const char *zDef = chunkStoreGetDefaultBranch(&pBt->store);
    u8 isMerging = 0;
    if( !zDef ) zDef = "main";
    memset(&catHash, 0, sizeof(catHash));
    memset(&workingCommit, 0, sizeof(workingCommit));
    memset(&stagedCatalog, 0, sizeof(stagedCatalog));
    memset(&mergeCommitHash, 0, sizeof(mergeCommitHash));
    memset(&conflictsCatalogHash, 0, sizeof(conflictsCatalogHash));
    memset(&branchCommit, 0, sizeof(branchCommit));
    rc = btreeLoadWorkingSetBlob(&pBt->store, zDef, &catHash, &workingCommit,
                                 &stagedCatalog, &isMerging,
                                 &mergeCommitHash, &conflictsCatalogHash);
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
  }

  
  p->iNextTable = 2;
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
  p->sharable = 0;
  p->wantToLock = 0;
  p->nBackup = 0;
  p->iBDataVersion = 1;
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

  
  if( p->pSchema && p->xFreeSchema ){
    p->xFreeSchema(p->pSchema);
    p->pSchema = 0;
  }
  sqlite3_free(p->aTables);
  if( p->aSavepointTables ){
    int i;
    for(i=0; i<p->nSavepoint; i++){
      freeSavepointTables(&p->aSavepointTables[i]);
    }
    sqlite3_free(p->aSavepointTables);
  }
  sqlite3_free(p->aCommittedTables);

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

/* No-op: prolly trees manage their own cache (ProllyCache), not SQLite's
** page cache. SetCacheSize, SetSpillSize, SetMmapLimit, SetPagerFlags,
** SecureDelete, AutoVacuum, IncrVacuum, LockTable, and SchemaLocked are
** all no-ops because the underlying concepts (page cache tuning, WAL spill,
** mmap, secure overwrite, vacuuming, shared-cache locks) don't apply to
** content-addressed immutable trees. They must still return SQLITE_OK to
** satisfy callers that expect success. */
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

#if SQLITE_MAX_MMAP_SIZE>0
static int prollyBtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  (void)p; (void)szMmap;
  return SQLITE_OK;
}
int sqlite3BtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetMmapLimit(p, szMmap);
}
#endif

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

/* Prolly trees have no pages, but SQLite queries this to determine if the
** database is empty (LastPage==0 means empty). Return a synthetic value
** larger than any table number to prevent SQLite from treating the db
** as empty. The +1000 provides headroom for new tables within a txn. */
static Pgno prollyBtreeLastPage(Btree *p){
  return p->iNextTable + 1000;
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
  ProllyHash *pConflicts
){
  ProllyHash wsHash;
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( pWorkingCat ) memset(pWorkingCat, 0, sizeof(ProllyHash));
  if( pWorkingCommit ) memset(pWorkingCommit, 0, sizeof(ProllyHash));
  if( pStaged ) memset(pStaged, 0, sizeof(ProllyHash));
  if( pIsMerging ) *pIsMerging = 0;
  if( pMergeCommit ) memset(pMergeCommit, 0, sizeof(ProllyHash));
  if( pConflicts ) memset(pConflicts, 0, sizeof(ProllyHash));

  rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&wsHash) ) return SQLITE_NOTFOUND;

  rc = chunkStoreGet(cs, &wsHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( !data || nData < WS_TOTAL_SIZE || data[0] != WS_FORMAT_VERSION ){
    sqlite3_free(data);
    return SQLITE_CORRUPT;
  }

  if( pWorkingCat ) memcpy(pWorkingCat->data, data + WS_WORKING_CAT_OFF, PROLLY_HASH_SIZE);
  if( pWorkingCommit ) memcpy(pWorkingCommit->data, data + WS_WORKING_COMMIT_OFF, PROLLY_HASH_SIZE);
  if( pStaged ) memcpy(pStaged->data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  if( pIsMerging ) *pIsMerging = data[WS_MERGING_OFF];
  if( pMergeCommit ) memcpy(pMergeCommit->data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
  if( pConflicts ) memcpy(pConflicts->data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
  sqlite3_free(data);
  return SQLITE_OK;
}

static int btreeStoreWorkingSetBlob(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pWorkingCat,
  const ProllyHash *pWorkingCommit,
  const ProllyHash *pStaged,
  u8 isMerging,
  const ProllyHash *pMergeCommit,
  const ProllyHash *pConflicts
){
  u8 buf[WS_TOTAL_SIZE];
  ProllyHash wsHash;
  static const ProllyHash emptyHash = {{0}};
  int rc;

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
                                 0, 0, 0, 0);
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
  u8 isMerging = 0;
  int rc;

  rc = btreeLoadWorkingSetBlob(cs, zBranch, 0, 0, &stagedCatalog, &isMerging,
                               &mergeCommitHash, &conflictsCatalogHash);
  if( rc!=SQLITE_OK && rc!=SQLITE_NOTFOUND ) return rc;
  if( rc==SQLITE_NOTFOUND ){
    memset(&stagedCatalog, 0, sizeof(ProllyHash));
    memset(&mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&conflictsCatalogHash, 0, sizeof(ProllyHash));
    isMerging = 0;
  }

  return btreeStoreWorkingSetBlob(cs, zBranch, pCatHash, pCommitHash,
                                  &stagedCatalog, isMerging,
                                  &mergeCommitHash, &conflictsCatalogHash);
}

static int btreeReloadBranchWorkingState(Btree *p, int bLoadCatalog){
  BtShared *pBt = p->pBt;
  ProllyHash catHash;
  ProllyHash stagedCatalog;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;
  const char *zBr = p->zBranch ? p->zBranch : "main";
  u8 isMerging = 0;
  int rc;

  memset(&catHash, 0, sizeof(catHash));
  memset(&stagedCatalog, 0, sizeof(stagedCatalog));
  memset(&mergeCommitHash, 0, sizeof(mergeCommitHash));
  memset(&conflictsCatalogHash, 0, sizeof(conflictsCatalogHash));

  rc = btreeLoadWorkingSetBlob(
      &pBt->store, zBr, &catHash, 0, &stagedCatalog, &isMerging,
      &mergeCommitHash, &conflictsCatalogHash);
  if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) return rc;

  if( bLoadCatalog && !prollyHashIsEmpty(&catHash) ){
    u8 *catData = 0;
    int nCatData = 0;
    rc = chunkStoreGet(&pBt->store, &catHash, &catData, &nCatData);
    if( rc==SQLITE_OK && catData ){
      rc = deserializeCatalog(p, catData, nCatData);
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ) return rc;
    }else{
      sqlite3_free(catData);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  p->stagedCatalog = stagedCatalog;
  p->isMerging = isMerging;
  p->mergeCommitHash = mergeCommitHash;
  p->conflictsCatalogHash = conflictsCatalogHash;
  return SQLITE_OK;
}

static int btreeRefreshFromDisk(Btree *p){
  BtShared *pBt = p->pBt;
  int bChanged = 0;
  int rc = chunkStoreRefreshIfChanged(&pBt->store, &bChanged);
  if( rc!=SQLITE_OK ) return rc;
  if( !bChanged ) return SQLITE_OK;

  rc = btreeReloadBranchWorkingState(p, 1);
  if( rc!=SQLITE_OK ) return rc;

  p->iBDataVersion++;
  if( pBt->pPagerShim ){
    pBt->pPagerShim->iDataVersion++;
  }

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

  
  rc = btreeRefreshFromDisk(p);
  if( rc!=SQLITE_OK ) return rc;
  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }

  if( wrFlag ){
    int nSavepointStart = p->nSavepoint;
    if( pBt->btsFlags & BTS_READ_ONLY ){
      return SQLITE_READONLY;
    }
    /* Acquire exclusive file lock for the duration of this write transaction.
    ** This serializes write transactions across connections, matching SQLite's
    ** standard behavior. The lock also refreshes from disk so we see the
    ** latest committed state. */
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

    sqlite3_free(p->aCommittedTables);
    p->aCommittedTables = 0;
    p->nCommittedTables = 0;
    if( p->nTables > 0 ){
      p->aCommittedTables = sqlite3_malloc(
          p->nTables * (int)sizeof(struct TableEntry));
      if( p->aCommittedTables ){
        memcpy(p->aCommittedTables, p->aTables,
               p->nTables * sizeof(struct TableEntry));
        p->nCommittedTables = p->nTables;
      }
    }
    p->iCommittedNextTable = p->iNextTable;
    p->committedStagedCatalog = p->stagedCatalog;
    p->committedIsMerging = p->isMerging;
    p->committedMergeCommitHash = p->mergeCommitHash;
    p->committedConflictsCatalogHash = p->conflictsCatalogHash;
    
    if( p->db ){
      while( p->nSavepoint < p->db->nSavepoint ){
        int rc2 = pushSavepoint(p);
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

  /* Pin the chunk store snapshot for the duration of this transaction.
  ** This prevents garbage collection from reclaiming chunks that our
  ** in-flight cursors reference. Unpinned at commit/rollback. */
  pBt->store.snapshotPinned = 1;

  return SQLITE_OK;
}
int sqlite3BtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginTrans(p, wrFlag, pSchemaVersion);
}

/* Phase one is a no-op for prolly trees. All work happens in phase two.
** SQLite's two-phase commit exists for journal/WAL coordination, which
** prolly trees don't use -- the chunk store commit is atomic. */
static int prollyBtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  (void)p; (void)zSuperJrnl;
  return SQLITE_OK;
}
int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  if( !p ) return SQLITE_OK;
  return p->pOps->xCommitPhaseOne(p, zSuperJrnl);
}

/* Commit protocol for prolly trees:
**   1. Flush all pending MutMap edits into their respective prolly trees
**      (this produces new immutable tree roots in the chunk store).
**   2. Serialize the catalog (table-number-to-root-hash mapping) and store
**      it as a content-addressed chunk.
**   3. Update the manifest's catalog hash and root hash.
**   4. Atomically commit the manifest to disk via chunkStoreCommit.
** If any step fails, the on-disk state is unchanged (the manifest hasn't
** been rewritten), so the old data is still valid on next open. */
static int prollyBtreeCommitPhaseTwo(Btree *p, int bCleanup){
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  int rcRollback = SQLITE_OK;
  (void)bCleanup;

  if( p->inTrans==TRANS_WRITE ){
    /* Step 1: flush all in-memory edits to prolly trees */
    rc = flushAllPending(pBt, 0);
    if( rc!=SQLITE_OK ) return rc;
    /* Steps 2-3: serialize catalog, store as chunk, update manifest */
    {
      u8 *catData = 0;
      int nCatData = 0;
      ProllyHash catHash;
      rc = serializeCatalog(p, &catData, &nCatData);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
        sqlite3_free(catData);
      }
      if( rc!=SQLITE_OK ) return rc;
      /* Save per-branch working catalog so other connections on this branch
      ** (and cross-branch connections) find the correct catalog on refresh. */
      {
        const char *zBr = p->zBranch ? p->zBranch : "main";
        rc = btreeWriteWorkingState(&pBt->store, zBr, &catHash, NULL);
        if( rc!=SQLITE_OK ) return rc;
        rc = chunkStoreSerializeRefs(&pBt->store);
        if( rc!=SQLITE_OK ) return rc;
      }
    }
    /* Step 4: atomic manifest write */
    rc = chunkStoreCommit(&pBt->store);
    if( rc==SQLITE_OK ){
      p->iBDataVersion++;
      if( pBt->pPagerShim ){
        pBt->pPagerShim->iDataVersion++;
      }
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      p->nSavepoint = 0;

      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
    }else{
      /* Commit failed after we had already materialized in-memory roots.
      ** Restore the pre-transaction in-memory state, but do not try to
      ** durably persist the rollback result here. The durable commit did
      ** not happen, so the correct outcome is simply "old durable state
      ** remains on disk". */
      int rc2 = restoreFromCommitted(p);
      if( rc2!=SQLITE_OK ){
        chunkStoreRollback(&pBt->store);
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc2;
      }
      {
        BtCursor *pC;
        for(pC = pBt->pCursor; pC; pC = pC->pNext){
          if( pC->pMutMap ) prollyMutMapClear(pC->pMutMap);
        }
      }
      invalidateCursors(pBt, 0, rc);
      invalidateSchema(p);
      chunkStoreRollback(&pBt->store);
      p->inTrans = TRANS_NONE;
      p->inTransaction = TRANS_NONE;
      p->nSavepoint = 0;
      chunkStoreUnlock(&pBt->store);
      pBt->store.snapshotPinned = 0;
    }
    return rc;
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
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
  if( p->aCommittedTables ){
    sqlite3_free(p->aTables);
    if( p->nCommittedTables > 0 ){
      p->aTables = sqlite3_malloc(
          p->nCommittedTables * (int)sizeof(struct TableEntry));
      if( !p->aTables ){
        p->nTables = 0;
        p->nTablesAlloc = 0;
        return SQLITE_NOMEM;
      }
      memcpy(p->aTables, p->aCommittedTables,
             p->nCommittedTables * sizeof(struct TableEntry));
    } else {
      p->aTables = 0;
    }
    p->nTables = p->nCommittedTables;
    p->nTablesAlloc = p->nCommittedTables;
    p->iNextTable = p->iCommittedNextTable;
  }
  p->stagedCatalog = p->committedStagedCatalog;
  p->isMerging = p->committedIsMerging;
  p->mergeCommitHash = p->committedMergeCommitHash;
  p->conflictsCatalogHash = p->committedConflictsCatalogHash;
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
        if( pC->pMutMap ) prollyMutMapClear(pC->pMutMap);
      }
    }
    invalidateCursors(pBt, 0, tripCode ? tripCode : SQLITE_ABORT);
    invalidateSchema(p);
    chunkStoreRollback(&pBt->store);
    {
      u8 *catData = 0;
      int nCatData = 0;
      ProllyHash catHash;
      const char *zBr = p->zBranch ? p->zBranch : "main";

      rc = serializeCatalog(p, &catData, &nCatData);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
      }
      sqlite3_free(catData);
      if( rc==SQLITE_OK ){
        rc = btreeWriteWorkingState(&pBt->store, zBr, &catHash, &p->headCommit);
      }
      if( rc==SQLITE_OK ){
        rc = chunkStoreSerializeRefs(&pBt->store);
      }
      if( rc==SQLITE_OK ){
        rc = chunkStoreCommit(&pBt->store);
      }
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(&pBt->store);
        pBt->store.snapshotPinned = 0;
        return rc;
      }
    }
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  p->nSavepoint = 0;

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
    int rc = pushSavepoint(p);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}
int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
  if( !p ) return SQLITE_OK;
  return p->pOps->xBeginStmt(p, iStatement);
}

static int prollyBtreeSavepoint(Btree *p, int op, int iSavepoint){
  BtShared *pBt;

  pBt = p->pBt;
  if( pBt==0 || p->inTrans!=TRANS_WRITE ){
    return SQLITE_OK;
  }

  if( op==SAVEPOINT_BEGIN ){
    while( p->nSavepoint < iSavepoint ){
      int rc = pushSavepoint(p);
      if( rc!=SQLITE_OK ) return rc;
    }
    return SQLITE_OK;
  }

  if( op==SAVEPOINT_ROLLBACK ){
    /* Rollback: restore the prolly root and each table's root to the
    ** values captured at pushSavepoint time. For MutMap entries, we
    ** truncate back to the saved nEntries count (MutMap is append-only,
    ** so entries after savedCount are exactly the post-savepoint writes).
    ** Tables created after the savepoint are removed entirely. */
    if( iSavepoint>=0 && iSavepoint<p->nSavepoint
     && p->aSavepointTables ){
      struct SavepointTableState *pState = &p->aSavepointTables[iSavepoint];
      if( pState->aTables ){
        int rc = restoreTablesFromSavepoint(p, pState);
        if( rc!=SQLITE_OK ) return rc;
      }
      p->stagedCatalog = pState->stagedCatalog;
      p->isMerging = pState->isMerging;
      p->mergeCommitHash = pState->mergeCommitHash;
      p->conflictsCatalogHash = pState->conflictsCatalogHash;
      freeSavepointTables(pState);
      
      {
        int j;
        for(j=iSavepoint+1; j<p->nSavepoint; j++){
          freeSavepointTables(&p->aSavepointTables[j]);
        }
      }
      p->nSavepoint = iSavepoint;
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    } else if( iSavepoint>=0 && iSavepoint>=p->nSavepoint ){
      { int rc2 = restoreFromCommitted(p); if( rc2 ) return rc2; }
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    } else if( iSavepoint<0 ){
      int j;
      for(j=0; j<p->nSavepoint; j++){
        freeSavepointTables(&p->aSavepointTables[j]);
      }
      { int rc2 = restoreFromCommitted(p); if( rc2 ) return rc2; }
      p->nSavepoint = 0;
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    }
  } else {
    
    if( iSavepoint>=0 && iSavepoint<p->nSavepoint ){
      int j;
      for(j=iSavepoint; j<p->nSavepoint; j++){
        freeSavepointTables(&p->aSavepointTables[j]);
      }
      p->nSavepoint = iSavepoint;
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

  iTable = p->iNextTable;
  p->iNextTable++;

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

  /* Table 1 (sqlite_master) cannot be removed from the catalog -- SQLite
  ** expects it to always exist. Instead, clear its root to make it empty. */
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

  assert( p->inTrans>=TRANS_READ );

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

  
  {
    BtCursor *pOther;
    for(pOther = pBt->pCursor; pOther; pOther = pOther->pNext){
      if( pOther->pgnoRoot==iTable && pOther->pMutMap
          && !prollyMutMapIsEmpty(pOther->pMutMap) ){
        int rc = flushMutMap(pOther);
        if( rc!=SQLITE_OK ) return rc;
      }
    }
  }
  if( pTE->pPending ){
    if( wrFlag & BTREE_WRCSR ){
      pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
      pTE->pPending = 0;
      pCur->flushSeekEdits = pTE->pendingFlushSeekEdits;
      if( !pCur->curIntKey
       && tableEntryIsTableRoot(p, pTE)
       && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        pCur->flushSeekEdits = 1;
      }
      pTE->pendingFlushSeekEdits = 0;
    }else{
      
      ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
      if( !prollyMutMapIsEmpty(pMap) ){
        int rc2 = applyMutMapToTableRoot(pBt, pTE, pMap);
        prollyMutMapFree(pMap);
        sqlite3_free(pMap);
        pTE->pPending = 0;
        pTE->pendingFlushSeekEdits = 0;
        if( rc2!=SQLITE_OK ) return rc2;
      }
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

/* On cursor close, pending edits in the cursor's MutMap must not be lost.
** They are handed off to the TableEntry's pPending field so a future cursor
** or commit will flush them. If pPending already has edits (from another
** closed cursor), the two MutMaps are merged. This is critical: without
** this handoff, inserts made through a cursor that is closed before commit
** would silently disappear. */
static int prollyBtCursorCloseCursor(BtCursor *pCur){
  BtShared *pBt;
  BtCursor **pp;
  if( !pCur ) return SQLITE_OK;
  pBt = pCur->pBt;
  if( !pBt ) return SQLITE_OK;

  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE && !pTE->pPending ){
      pTE->pPending = pCur->pMutMap;
      pTE->pendingFlushSeekEdits = pCur->flushSeekEdits;
      pCur->pMutMap = 0;
    }else if( pTE && pTE->pPending ){
      int rc = prollyMutMapMerge((ProllyMutMap*)pTE->pPending, pCur->pMutMap);
      if( rc!=SQLITE_OK ) return rc;
      pTE->pendingFlushSeekEdits |= pCur->flushSeekEdits;
      prollyMutMapFree(pCur->pMutMap);
      sqlite3_free(pCur->pMutMap);
      pCur->pMutMap = 0;
    }else{
      int rc = flushMutMap(pCur);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  prollyCursorClose(&pCur->pCur);

  if( pCur->pMutMap ){
    prollyMutMapFree(pCur->pMutMap);
    sqlite3_free(pCur->pMutMap);
    pCur->pMutMap = 0;
  }

  CLEAR_CACHED_PAYLOAD(pCur);

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

/*
** Merge iteration: the cursor presents a unified view of the immutable prolly
** tree and the in-memory MutMap (pending inserts/deletes). Both sources are
** sorted by key. mergeStepForward/Backward advance through both in sorted
** order, skipping MutMap delete entries and preferring MutMap data when keys
** match (MERGE_SRC_BOTH). This is conceptually a sorted merge-join where
** deletes act as tombstones that suppress matching tree entries.
*/
static int mergeCompare(BtCursor *pCur, ProllyMutMapEntry *e){
  if( pCur->curIntKey ){
    i64 tk = prollyCursorIntKey(&pCur->pCur);
    if( tk < e->intKey ) return -1;
    if( tk > e->intKey ) return 1;
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

/* Scan for the next visible merged entry in the given direction.
** dir=+1 for forward, dir=-1 for backward.
** If pRes is non-NULL (mergeFirst/mergeLast), sets *pRes=1 if empty.
** If pRes is NULL (mergeStep), returns SQLITE_DONE if exhausted. */
static int mergeScan(BtCursor *pCur, int dir, int *pRes){
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
    e = &pCur->pMutMap->aEntries[pCur->mmIdx];
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

static int mergeStepForward(BtCursor *pCur){
  int rc = SQLITE_OK;
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
      rc = sortKeyFromRecordPrefix(pCur->pCachedPayload, pCur->nCachedPayload,
                                   nMutKeyField, &pSortKey, &nSortKey);
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

static int flushTablePending(BtCursor *pCur){
  struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE && pTE->pPending ){
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    if( !prollyMutMapIsEmpty(pMap) ){
      int rc = applyMutMapToTableRoot(pCur->pBt, pTE, pMap);
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
      pTE->pPending = 0;
      return rc;
    }
    prollyMutMapFree(pMap);
    sqlite3_free(pMap);
    pTE->pPending = 0;
  }
  return SQLITE_OK;
}

static int prollyBtCursorFirst(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  rc = flushTablePending(pCur);
  if( rc!=SQLITE_OK ) return rc;

  
  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p!=pCur && p->pgnoRoot==pCur->pgnoRoot
       && p->pMutMap && !prollyMutMapIsEmpty(p->pMutMap) ){
        rc = flushMutMap(p);
        if( rc!=SQLITE_OK ) return rc;
        p->eState = CURSOR_INVALID;
      }
    }
  }
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
  rc = flushTablePending(pCur);
  if( rc!=SQLITE_OK ) return rc;

  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p!=pCur && p->pgnoRoot==pCur->pgnoRoot
       && p->pMutMap && !prollyMutMapIsEmpty(p->pMutMap) ){
        rc = flushMutMap(p);
        if( rc!=SQLITE_OK ) return rc;
        p->eState = CURSOR_INVALID;
      }
    }
  }
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
      /* If the MutMap entry matches the current tree key (e.g. an UPDATE
      ** just wrote to the same key), both sources must advance past it.
      ** Otherwise only the tree advances. Without this, mergeStepForward
      ** leaves the MutMap positioned at the current key, causing the
      ** merged scan to re-visit the row (double-apply UPDATE bug).
      ** If the tree cursor is already at EOF, only the MutMap source
      ** should advance; advancing a tree at EOF would deref a null node. */
      if( it.idx >= 0 && it.idx < pCur->pMutMap->nEntries
       && prollyCursorIsValid(&pCur->pCur)
       && mergeCompare(pCur, &pCur->pMutMap->aEntries[it.idx])==0 ){
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
      rc = prollyCursorNext(&pCur->pCur);
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
      /* Same fix as in Next: if MutMap entry matches the current tree
      ** key, advance both sources to avoid re-visiting the row.
      ** If the tree cursor is at EOF, only the MutMap should retreat. */
      if( it.idx >= 0 && it.idx < pCur->pMutMap->nEntries
       && prollyCursorIsValid(&pCur->pCur)
       && mergeCompare(pCur, &pCur->pMutMap->aEntries[it.idx])==0 ){
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

  
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapEntry *pEntry = prollyMutMapFind(pCur->pMutMap, 0, 0, intKey);
    if( pEntry ){
      if( pEntry->op == PROLLY_EDIT_INSERT ){
        int idx = (int)(pEntry - pCur->pMutMap->aEntries);
        
        *pRes = 0;
        refreshCursorRoot(pCur);
        {
          int seekRes = 0;
          rc = prollyCursorSeekInt(&pCur->pCur, intKey, &seekRes);
          if( rc!=SQLITE_OK ) return rc;
        }
        setCursorToMutMapEntry(pCur, idx);
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
      pCur->cachedPayloadOwned = 0;
    } else if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      pCur->eState = CURSOR_VALID;
      pCur->curFlags &= ~BTCF_ValidNKey;
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

static int serializeUnpackedRecord(UnpackedRecord *pRec, u8 **ppOut, int *pnOut){
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
  pOut = (u8*)sqlite3_malloc(nTotal);
  if( !pOut ) return SQLITE_NOMEM;

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

  *ppOut = pOut;
  *pnOut = nTotal;
  return SQLITE_OK;
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
  int lo, hi;

  *ppMatch = 0;
  *pCmp = 0;
  if( !pMap || prollyMutMapIsEmpty(pMap) ){
    return SQLITE_OK;
  }

  if( pKeyInfo
   && pIdxKey->nField >= pKeyInfo->nAllField ){
    ProllyMutMapEntry *pEntry = prollyMutMapFind(pMap, pSortKey, nSortKey, 0);
    if( pEntry && pEntry->op==PROLLY_EDIT_INSERT ){
      *ppMatch = pEntry;
    }
    return SQLITE_OK;
  }

  lo = 0;
  hi = pMap->nEntries;
  while( lo < hi ){
    int mid = lo + (hi - lo) / 2;
    ProllyMutMapEntry *pEntry = &pMap->aEntries[mid];
    const u8 *pRec = pEntry->pVal;
    int nRec = pEntry->nVal;
    int isLess;

    if( pEntry->isIntKey ){
      lo = mid + 1;
      continue;
    }
    if( nRec==0 ){
      sqlite3_free(pRecBuf);
      pRecBuf = 0;
      rc = recordFromSortKey(pEntry->pKey, pEntry->nKey, &pRecBuf, &nRec);
      if( rc!=SQLITE_OK ) break;
      pRec = pRecBuf;
    }
    pIdxKey->eqSeen = 0;
    cmp = sqlite3VdbeRecordCompare(nRec, pRec, pIdxKey);
    isLess = (cmp<0 && !pIdxKey->eqSeen);
    if( isLess ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }

  while( rc==SQLITE_OK && lo < pMap->nEntries ){
    ProllyMutMapEntry *pEntry = &pMap->aEntries[lo];
    const u8 *pRec = pEntry->pVal;
    int nRec = pEntry->nVal;

    if( pEntry->isIntKey ){
      lo++;
      continue;
    }
    if( nRec==0 ){
      sqlite3_free(pRecBuf);
      pRecBuf = 0;
      rc = recordFromSortKey(pEntry->pKey, pEntry->nKey, &pRecBuf, &nRec);
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

  if( pCur->flushSeekEdits
   && (pCur->curFlags & BTCF_WriteFlag)
   && pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    rc = flushMutMap(pCur);
    if( rc!=SQLITE_OK ) return rc;
    pCur->mmActive = 0;
    pCur->flushSeekEdits = 0;
  }

  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p->pgnoRoot==pCur->pgnoRoot
       && p->pMutMap && !prollyMutMapIsEmpty(p->pMutMap) ){
        if( p==pCur ){
          
          struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
          if( pTE && prollyHashIsEmpty(&pTE->root) ){
            rc = flushMutMap(p);
            if( rc!=SQLITE_OK ) return rc;
          }
        }else{
          rc = flushMutMap(p);
          if( rc!=SQLITE_OK ) return rc;
          p->eState = CURSOR_INVALID;
        }
      }
    }
  }

  refreshCursorRoot(pCur);

  
  {
    int treeFound = 0, mutFound = 0;
    int treeCmp = 0, mutCmp = 0;
    const u8 *mutKey = 0;
    int mutNKey = 0;
    ProllyMutMapEntry *mutE = 0;
    int mutFromCursorMap = 0;

    /* Build the sort-key prefix used to seek the prolly tree. For unique
    ** indexes (and the PK index of WITHOUT ROWID tables), nKeyField <
    ** nAllField and the stored prolly key is the sort-key encoding of
    ** only the leading nKeyField columns; encode the seek key the same
    ** way so it matches. For non-unique indexes the entire record is
    ** the prolly key, so we encode all columns. */
    u8 *pSerKey = 0;
    int nSerKey = 0;
    u8 *pSortKey = 0;
    int nSortKey = 0;
    int nSeekKeyField = 0;
    if( pCur->pKeyInfo
     && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
      nSeekKeyField = (int)pCur->pKeyInfo->nKeyField;
    }
    rc = serializeUnpackedRecord(pIdxKey, &pSerKey, &nSerKey);
    if( rc!=SQLITE_OK ) return rc;
    rc = sortKeyFromRecordPrefix(pSerKey, nSerKey, nSeekKeyField,
                                 &pSortKey, &nSortKey);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pSerKey);
      return rc;
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

        int lo = 0, hi = nItems;
        u8 *pRecBuf = 0;
        int i;
        while( lo < hi ){
          int mid = lo + (hi - lo) / 2;
          const u8 *pSK; int nSK;
          int cmpLen; int keyCmp;
          prollyNodeKey(&pLeaf->node, mid, &pSK, &nSK);
          cmpLen = nSK < nSortKey ? nSK : nSortKey;
          keyCmp = memcmp(pSK, pSortKey, cmpLen);
          if( keyCmp < 0 || (keyCmp==0 && nSK < nSortKey) ){
            lo = mid + 1;
          }else{
            hi = mid;
          }
        }


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
                  ProllyMutMapEntry *mmE = prollyMutMapFind(
                      pCur->pMutMap, pSK, nSK, 0);
                  if( mmE && mmE->op==PROLLY_EDIT_DELETE ) isDeleted = 1;
                }
                if( !isDeleted ){
                  const u8 *pVal2; int nVal2;
                  prollyNodeValue(&pLeaf->node, i, &pVal2, &nVal2);
                  if( nVal2==0 ){
                    recordFromSortKey(pSK, nSK, &pRecBuf, &nVal2);
                    pVal2 = pRecBuf;
                  }
                  pIdxKey->eqSeen = 0;
                  bestIdx = i;
                  bestCmp = sqlite3VdbeRecordCompare(nVal2, pVal2, pIdxKey);
                }
              }
              break;
            }
          }
          prollyNodeValue(&pLeaf->node, i, &pVal, &nVal);
          if( nVal==0 ){
            sqlite3_free(pRecBuf); pRecBuf = 0;
            recordFromSortKey(pSK, nSK, &pRecBuf, &nVal);
            pVal = pRecBuf;
          }
          pIdxKey->eqSeen = 0;
          recCmp = sqlite3VdbeRecordCompare(nVal, pVal, pIdxKey);

          if( recCmp==0 || pIdxKey->eqSeen ){
            if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
              ProllyMutMapEntry *mmE = prollyMutMapFind(
                  pCur->pMutMap, pSK, nSK, 0);
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
              ProllyMutMapEntry *mmE = prollyMutMapFind(
                  pCur->pMutMap, pSK, nSK, 0);
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
        sqlite3_free(pRecBuf);
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
      rc = findMatchingMutMapEntry((ProllyMutMap*)pCur->pMutMap,
                                   pCur->pKeyInfo,
                                   pIdxKey, pSortKey, nSortKey,
                                   &mutE, &mutCmp);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pSerKey);
        sqlite3_free(pSortKey);
        return rc;
      }
      if( mutE ) mutFromCursorMap = 1;
      if( !mutE && pPending && pPending!=pCur->pMutMap ){
        rc = findMatchingMutMapEntry(pPending,
                                     pCur->pKeyInfo,
                                     pIdxKey, pSortKey, nSortKey,
                                     &mutE, &mutCmp);
        if( rc!=SQLITE_OK ){
          sqlite3_free(pSerKey);
          sqlite3_free(pSortKey);
          return rc;
        }
      }
      if( mutE ){
        
        const u8 *pMutVal = mutE->pVal;
        int nMutVal = mutE->nVal;
        u8 *pRecon = 0;
        if( nMutVal==0 ){
          recordFromSortKey(mutE->pKey, mutE->nKey, &pRecon, &nMutVal);
          pMutVal = pRecon;
        }
        if( pMutVal ){
          mutKey = pMutVal;
          mutNKey = nMutVal;
          mutFound = 1;
        }
        if( !mutFound ) sqlite3_free(pRecon);
      }
    }
    }
    sqlite3_free(pSerKey);
    sqlite3_free(pSortKey);

    
    if( mutFound && (!treeFound || treeCmp!=0) ){
      if( mutFromCursorMap ){
        setCursorToMutMapEntry(pCur, (int)(mutE - pCur->pMutMap->aEntries));
      }else{
        rc = cacheCursorPayloadCopy(pCur, mutKey, mutNKey);
        if( rc!=SQLITE_OK ){
          return rc;
        }
        pCur->eState = CURSOR_VALID;
      }
      *pRes = mutCmp;
      return SQLITE_OK;
    }
    if( treeFound ){
      *pRes = treeCmp;
      pCur->eState = CURSOR_VALID;
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

static i64 prollyBtCursorIntegerKey(BtCursor *pCur){
  assert( pCur->eState==CURSOR_VALID );
  assert( pCur->curIntKey );
  
  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    return pCur->pMutMap->aEntries[pCur->mmIdx].intKey;
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

/* Returns the current row's payload. Priority order:
** 1. Cached payload (set by TableMoveto/IndexMoveto from MutMap lookups)
** 2. MutMap entry (when merge iteration sourced the row from MutMap)
** 3. Prolly tree entry (the normal immutable tree path)
** For index tables, the value may be empty (stored as sort key only),
** in which case we reconstruct the original record via recordFromSortKey. */
static void getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData){
  
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    *ppData = pCur->pCachedPayload;
    *pnData = pCur->nCachedPayload;
    return;
  }

  
  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
    if( pCur->curIntKey ){
      
      *ppData = e->pVal;
      *pnData = e->nVal;
    }else{
      
      u8 *pRec = 0; int nRec = 0;
      recordFromSortKey(e->pKey, e->nKey, &pRec, &nRec);
      if( pRec ){
        if( pCur->cachedPayloadOwned && pCur->pCachedPayload ){
          sqlite3_free(pCur->pCachedPayload);
        }
        pCur->pCachedPayload = pRec;
        pCur->nCachedPayload = nRec;
        pCur->cachedPayloadOwned = 1;
      }
      *ppData = pRec;
      *pnData = nRec;
    }
    return;
  }

  if( pCur->curIntKey ){
    prollyCursorValue(&pCur->pCur, ppData, pnData);
  }else{
    
    const u8 *pVal; int nVal;
    prollyCursorValue(&pCur->pCur, &pVal, &nVal);
    if( nVal > 0 ){
      *ppData = pVal;
      *pnData = nVal;
    }else{
      const u8 *pKey; int nKey;
      u8 *pRec = 0; int nRec = 0;
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
      recordFromSortKey(pKey, nKey, &pRec, &nRec);
      if( pRec ){
        if( pCur->cachedPayloadOwned && pCur->pCachedPayload ){
          sqlite3_free(pCur->pCachedPayload);
        }
        pCur->pCachedPayload = pRec;
        pCur->nCachedPayload = nRec;
        pCur->cachedPayloadOwned = 1;
        *ppData = pRec;
        *pnData = nRec;
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
  (void)seekResult;

  /* BTREE_PREFORMAT is used by SQLite's bulk-insert optimization where data
  ** is pre-formatted into page images. Prolly trees don't use pages, so
  ** this is a no-op. The actual insert happens in a subsequent non-preformat call. */
  if( flags & BTREE_PREFORMAT ){
    return SQLITE_OK;
  }

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = saveAllCursors(pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->curIntKey ){
    const u8 *pData = (const u8*)pPayload->pData;
    int nData = pPayload->nData;
    int nTotal = nData + pPayload->nZero;
    u8 *pBuf = 0;

    if( pPayload->nZero > 0 && nTotal > nData ){
      pBuf = sqlite3_malloc(nTotal);
      if( !pBuf ) return SQLITE_NOMEM;
      if( nData > 0 ){
        memcpy(pBuf, pData, nData);
      }
      memset(pBuf + nData, 0, pPayload->nZero);
      pData = pBuf;
      nData = nTotal;
    }

    rc = prollyMutMapInsert(pCur->pMutMap,
                             NULL, 0, pPayload->nKey,
                             pData, nData);
    sqlite3_free(pBuf);
  } else {
    /* BLOBKEY storage. Two flavors of cursor land here:
    **
    ** (1) The PK index of a WITHOUT ROWID table (or any UNIQUE index).
    **     KeyInfo has nKeyField < nAllField: the leading nKeyField
    **     columns are unique, the trailing columns are non-key data
    **     (table columns for the PK index, rowid/PK suffix for unique
    **     indexes). For these we encode only the first nKeyField
    **     columns as the prolly key and store the full original record
    **     in the value side. Same prolly key on both sides of an UPDATE
    **     means the diff walker classifies it as MODIFY rather than
    **     DELETE+ADD. The full record in the value lets the read path
    **     use it directly and skip recordFromSortKey().
    **
    ** (2) Non-UNIQUE indexes. KeyInfo has nKeyField == nAllField: the
    **     entire entry (indexed columns plus rowid suffix) participates
    **     in the uniqueness of the index entry. Two rows can share the
    **     first nKeyField bytes (same indexed column value) and the
    **     rowid suffix is what distinguishes them. For these we encode
    **     the full record as the prolly key with an empty value side,
    **     which is the existing behavior — modifications produce
    **     different prolly keys and the diff correctly emits DELETE+ADD. */
    u8 *pSortKey = 0;
    int nSortKey = 0;
    int nKeyField = 0;
    int splitKey = 0;
    if( pCur->pKeyInfo
     && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
      nKeyField = (int)pCur->pKeyInfo->nKeyField;
      splitKey = 1;
    }
    rc = sortKeyFromRecordPrefix((const u8*)pPayload->pKey,
                                 (int)pPayload->nKey,
                                 splitKey ? nKeyField : 0,
                                 &pSortKey, &nSortKey);
    if( rc==SQLITE_OK ){
      if( splitKey ){
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 pSortKey, nSortKey, 0,
                                 (const u8*)pPayload->pKey, (int)pPayload->nKey);
      }else{
        rc = prollyMutMapInsert(pCur->pMutMap,
                                 pSortKey, nSortKey, 0,
                                 NULL, 0);
      }
      sqlite3_free(pSortKey);
    }
  }

  if( rc!=SQLITE_OK ) return rc;

  /* Edits to user tables (pgnoRoot > 1) are deferred in the MutMap and
  ** flushed lazily. Table 1 (sqlite_master) is flushed immediately because
  ** SQLite reads it back within the same transaction to verify schema changes,
  ** and the merge iteration path is not active for schema table reads. */
  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer ){
      if( (flags & BTREE_SAVEPOSITION) && pCur->curIntKey ){
        
        ProllyMutMapEntry *pEntry = prollyMutMapFind(
            pCur->pMutMap, NULL, 0, pPayload->nKey);
        pCur->eState = CURSOR_VALID;
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = pPayload->nKey;
        rc = cacheCursorPayloadCopy(
            pCur,
            (pEntry && pEntry->nVal > 0 && pEntry->pVal) ? pEntry->pVal : 0,
            (pEntry && pEntry->nVal > 0 && pEntry->pVal) ? pEntry->nVal : 0);
        if( rc!=SQLITE_OK ) return rc;
        /* The MutMap was modified — the merge iteration state (mmIdx,
        ** mergeSrc) is stale. Reset so the next Next/Previous call
        ** re-synchronizes the merge cursor with the updated MutMap. */
        pCur->mmActive = 0;
        pCur->flushSeekEdits = 0;
      } else if( (flags & BTREE_SAVEPOSITION) && !pCur->curIntKey ){
        /* For BLOBKEY cursors (WITHOUT ROWID PK), after a deferred insert
        ** following a deferred delete with SAVEPOSITION, advance the tree
        ** cursor past the old (deleted) entry. This prevents OP_Next from
        ** re-syncing with MutMap at the old position and picking up the
        ** newly inserted entry (whose sort key differs because non-PK
        ** columns changed). Set skipNext=1 so OP_Next returns immediately
        ** with the cursor at the next tree entry. */
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
        pCur->eState = CURSOR_INVALID;
        pCur->flushSeekEdits = 0;
      }
      return SQLITE_OK;
    }
  }


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
  int anyFlushed = 0;
  int needFlush = 0;

  
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    needFlush = 1;
  }
  if( !needFlush ){
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p!=pCur && p->pgnoRoot==pCur->pgnoRoot
       && p->pMutMap && !prollyMutMapIsEmpty(p->pMutMap) ){
        needFlush = 1;
        break;
      }
    }
  }
  if( !needFlush ) return SQLITE_OK;

  
  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p!=pCur && p->pgnoRoot==pCur->pgnoRoot ){
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

  
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    rc = flushMutMap(pCur);
    if( rc!=SQLITE_OK ) return rc;
    anyFlushed = 1;
  }

  
  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p!=pCur && p->pgnoRoot==pCur->pgnoRoot
       && p->pMutMap && !prollyMutMapIsEmpty(p->pMutMap) ){
        rc = flushMutMap(p);
        if( rc!=SQLITE_OK ) return rc;
        p->eState = CURSOR_INVALID;
        anyFlushed = 1;
      }
    }
  }

  if( anyFlushed ){
    pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE ){
      prollyCursorClose(&pCur->pCur);
      prollyCursorInit(&pCur->pCur, &pCur->pBt->store, &pCur->pBt->cache,
                       &pTE->root, pTE->flags);
    }
    pCur->eState = CURSOR_INVALID;
  }
  return SQLITE_OK;
}

static int flushAllPending(BtShared *pBt, Pgno iTable){
  BtCursor *p;
  int rc;

  
  for(p = pBt->pCursor; p; p = p->pNext){
    if( iTable==0 || p->pgnoRoot==iTable ){
      rc = flushIfNeeded(p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  
  rc = flushDeferredEdits(pBt);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

static int flushDeferredEdits(BtShared *pBt){
  int rc = SQLITE_OK;
  if( pBt->db && pBt->db->nDb>0 && pBt->db->aDb[0].pBt ){
    Btree *pBtree = pBt->db->aDb[0].pBt;
    int i;
    for(i=0; i<pBtree->nTables; i++){
      struct TableEntry *pTE = &pBtree->aTables[i];
      if( pTE->pPending && !prollyMutMapIsEmpty((ProllyMutMap*)pTE->pPending) ){
        rc = applyMutMapToTableRoot(pBt, pTE, (ProllyMutMap*)pTE->pPending);
        prollyMutMapFree((ProllyMutMap*)pTE->pPending);
        sqlite3_free(pTE->pPending);
        pTE->pPending = 0;
        if( rc!=SQLITE_OK ) return rc;
      }
    }
  }
  return rc;
}

static int btreeDeleteImmediate(BtCursor *pCur, const u8 *pKey, int nKey, i64 iKey){
  int rc;
  (void)pKey;
  (void)nKey;
  (void)iKey;

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
  /* Save the delete key BEFORE syncSavepoints, which may flush the cursor's
  ** MutMap and reinitialize its tree position. Without this, the key
  ** extraction below would read from an invalidated cursor. */
  u8 *pSavedDelKey = 0;
  int nSavedDelKey = 0;
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

  /* Extract the key to delete now, while the cursor is still positioned.
  ** syncSavepoints below may push implicit savepoints which flush pending
  ** MutMap edits and reinitialize the tree cursor. */
  if( pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_INVALID ){
    if( pCur->curIntKey ){
      if( pCur->mmActive
       && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
        savedIntKey = pCur->pMutMap->aEntries[pCur->mmIdx].intKey;
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
      if( pCur->mmActive
       && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
        ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
        pSavedDelKey = sqlite3_malloc(e->nKey);
        if( !pSavedDelKey ) return SQLITE_NOMEM;
        memcpy(pSavedDelKey, e->pKey, e->nKey);
        nSavedDelKey = e->nKey;
        hasSavedKey = 1;
      }else if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
        int nDelKeyField = 0;
        if( pCur->pKeyInfo
         && pCur->pKeyInfo->nKeyField < pCur->pKeyInfo->nAllField ){
          nDelKeyField = (int)pCur->pKeyInfo->nKeyField;
        }
        rc = sortKeyFromRecordPrefix(pCur->pCachedPayload, pCur->nCachedPayload,
                                     nDelKeyField, &pSavedDelKey, &nSavedDelKey);
        if( rc!=SQLITE_OK ) return rc;
        hasSavedKey = 1;
      }else if( prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pTmp; int nTmp;
        prollyCursorKey(&pCur->pCur, &pTmp, &nTmp);
        pSavedDelKey = sqlite3_malloc(nTmp);
        if( !pSavedDelKey ) return SQLITE_NOMEM;
        memcpy(pSavedDelKey, pTmp, nTmp);
        nSavedDelKey = nTmp;
        hasSavedKey = 1;
      }
    }
  }

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ){ sqlite3_free(pSavedDelKey); return rc; }

  rc = saveAllCursors(pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ){ sqlite3_free(pSavedDelKey); return rc; }

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ){ sqlite3_free(pSavedDelKey); return rc; }

  /* Use the saved key for the MutMap delete */
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
    sqlite3_free(pSavedDelKey);
    pSavedDelKey = 0;
  }

  if( rc!=SQLITE_OK ) return rc;

  
  {
    int canDefer = (pCur->pgnoRoot > 1);
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

  
  rc = btreeDeleteImmediate(pCur, pKey, nKey, iKey);
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
static void prollyBtreeEnter(Btree *p){
  p->wantToLock++;
}
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
static void prollyBtreeLeave(Btree *p){
  p->wantToLock--;
}
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
  pBt = p->pBt;

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

/* Returns a PagerShim cast to Pager*. SQLite reads iDataVersion from this
** "pager" to detect schema changes. The shim has no real paging -- it only
** exists to provide the data version counter that SQLite checks. */
static struct Pager *prollyBtreePager(Btree *p){
  return (struct Pager*)(p->pBt->pPagerShim);
}
struct Pager *sqlite3BtreePager(Btree *p){
  if( !p ) return 0;
  return p->pOps->xPager(p);
}

static int prollyBtCursorCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  (void)db;
  
  flushTablePending(pCur);
  flushIfNeeded(pCur);
  return countTreeEntries(pCur->pBtree, pCur->pgnoRoot, pnEntry);
}
int sqlite3BtreeCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xCount(db, pCur, pnEntry);
}

/* Returns a rough row count estimate for the query planner. Prolly trees
** don't track exact counts cheaply, so return 1M as a placeholder to
** avoid the planner choosing bad plans for "empty" tables. TODO: store
** actual row counts in the catalog for better query planning. */
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

  rc = integrityCheckChunkGraph(&ctx, &pBt->store.refsHash);
  for(i=0; rc==SQLITE_OK && i<pBt->store.nBranches; i++){
    rc = integrityCheckChunkGraph(&ctx, &pBt->store.aBranches[i].commitHash);
    if( rc==SQLITE_OK ){
      rc = integrityCheckChunkGraph(&ctx, &pBt->store.aBranches[i].workingSetHash);
    }
  }
  for(i=0; rc==SQLITE_OK && i<pBt->store.nTags; i++){
    rc = integrityCheckChunkGraph(&ctx, &pBt->store.aTags[i].commitHash);
  }
  for(i=0; rc==SQLITE_OK && i<pBt->store.nTracking; i++){
    rc = integrityCheckChunkGraph(&ctx, &pBt->store.aTracking[i].commitHash);
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

  /* Attached plain SQLite databases: delegate to the original btree. */
  if( p->pOrigBtree ){
    return origBtreeIntegrityCheck(db, p->pOrigBtree, aRoot, aCnt,
                                   nRoot, mxErr, pnErr, pzOut);
  }

  (void)aCnt;
  (void)mxErr;

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

  sqlite3_free(pTo->aTables);
  pTo->aTables = 0;
  pTo->nTables = 0;
  pTo->nTablesAlloc = 0;

  for(i=0; i<pFrom->nTables; i++){
    struct TableEntry *pTE = addTable(pTo,
                                       pFrom->aTables[i].iTable,
                                       pFrom->aTables[i].flags);
    if( !pTE ) return SQLITE_NOMEM;
    pTE->root = pFrom->aTables[i].root;
  }

  memcpy(pTo->aMeta, pFrom->aMeta, sizeof(pTo->aMeta));
  pTo->iNextTable = pFrom->iNextTable;

  pTo->iBDataVersion++;
  if( pBtTo->pPagerShim ){
    pBtTo->pPagerShim->iDataVersion++;
  }

  return SQLITE_OK;
}

int sqlite3BtreeIsInBackup(Btree *p){
  return p->nBackup > 0;
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

  prollyCursorValue(&pCur->pCur, &pVal, &nVal);

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
  (void)pCur; (void)upCnt;
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
  for(i=0; i<pBtree->nTables; i++){
    if( pBtree->aTables[i].iTable==iTable ){
      memcpy(&pBtree->aTables[i].schemaHash, pH, sizeof(ProllyHash));
      return;
    }
  }
}

const char *doltliteNextTableForSchema(sqlite3 *db, int *pIdx, Pgno *piTable){
  Btree *pBtree;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return 0;
  pBtree = db->aDb[0].pBt;
  while( *pIdx < pBtree->nTables ){
    int i = (*pIdx)++;
    if( pBtree->aTables[i].iTable>1 && pBtree->aTables[i].zName ){
      *piTable = pBtree->aTables[i].iTable;
      return pBtree->aTables[i].zName;
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
  rc = flushAllPending(pBt, 0);
  if( rc!=SQLITE_OK ) return rc;
  
  rc = flushDeferredEdits(pBt);
  if( rc!=SQLITE_OK ) return rc;
  
  {
    extern void doltliteUpdateSchemaHashes(sqlite3 *db);
    doltliteUpdateSchemaHashes(db);
  }
  return serializeCatalog(pBtree, ppOut, pnOut);
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

  *ppTables = temp.aTables;
  *pnTables = temp.nTables;
  if( piNextTable ) *piNextTable = temp.iNextTable;
  return SQLITE_OK;
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
  Schema *pSchema;
  HashElem *k;
  if( !db || db->nDb<=0 ) return SQLITE_ERROR;
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

/* Load a catalog into the Btree without committing to disk.
** Used by checkout to switch the in-memory table registry. */
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

  sqlite3_free(pBtree->aTables);
  pBtree->aTables = 0;
  pBtree->nTables = 0;
  pBtree->nTablesAlloc = 0;

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
  pBtree->iBDataVersion++;
  if( pBt->pPagerShim ){
    pBt->pPagerShim->iDataVersion++;
  }

  if( pBtree->db ){
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

  
  pBtree->aTables = 0;
  pBtree->nTables = 0;
  pBtree->nTablesAlloc = 0;

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pBtree->aTables);
    pBtree->aTables = 0;
    pBtree->nTables = 0;
    pBtree->nTablesAlloc = 0;
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
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }else{
    invalidateSchema(pBtree);
  }

  
  memcpy(&pBtree->stagedCatalog, catHash, sizeof(ProllyHash));

  
  /* Update per-branch working state for the current session branch.
  ** Callers that change branches (checkout) must save the old branch's
  ** working catalog BEFORE calling hardReset. */
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
    sqlite3_free(pBtree->aTables);
    pBtree->aTables = 0;
    pBtree->nTables = 0;
    pBtree->nTablesAlloc = 0;
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

void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash){
  doltliteGetSessionMergeState(db, 0, 0, pHash);
}

void doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    memcpy(&db->aDb[0].pBt->conflictsCatalogHash, pHash, sizeof(ProllyHash));
  }
}

int doltliteSaveWorkingSet(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash workingCatHash;
  const char *zBranch;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  zBranch = pBtree->zBranch ? pBtree->zBranch : "main";

  rc = serializeCatalog(pBtree, &catData, &nCatData);
  if( rc != SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, catData, nCatData, &workingCatHash);
  sqlite3_free(catData);
  if( rc != SQLITE_OK ) return rc;

  return btreeStoreWorkingSetBlob(cs, zBranch, &workingCatHash,
                                  &pBtree->headCommit, &pBtree->stagedCatalog,
                                  pBtree->isMerging, &pBtree->mergeCommitHash,
                                  &pBtree->conflictsCatalogHash);
}

int doltlitePersistWorkingSet(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = doltliteSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(cs);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(cs);
}

int doltliteLoadWorkingSet(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  rc = btreeLoadWorkingSetBlob(cs, zBranch, 0, 0,
                               &pBtree->stagedCatalog,
                               &pBtree->isMerging, &pBtree->mergeCommitHash,
                               &pBtree->conflictsCatalogHash);
  if( rc == SQLITE_NOTFOUND ){
    memset(&pBtree->stagedCatalog, 0, sizeof(ProllyHash));
    pBtree->isMerging = 0;
    memset(&pBtree->mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&pBtree->conflictsCatalogHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
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
  p->wantToLock++;
}
static void origBtreeLeaveVt(Btree *p){
  origBtreeLeave(p->pOrigBtree);
  p->wantToLock--;
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
  int rc = origBtreeCloseCursor(pCur->pOrigCursor);
  sqlite3_free(pCur->pOrigCursor);
  pCur->pOrigCursor = 0;
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

#endif 
