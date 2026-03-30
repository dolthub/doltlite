/*
** 2024-01-01
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Prolly tree implementation of the btree.h API.
**
** This file replaces SQLite's traditional B-tree page-based storage with
** content-addressed prolly trees. Instead of managing database pages via
** the Pager, we store key-value entries in a prolly tree structure backed
** by a ChunkStore.
**
** Architecture overview:
**
**   Btree       -- Per-connection handle (one per attached database)
**   BtShared    -- Shared state: chunk store, cache, table registry
**   BtCursor    -- Cursor over a single table's prolly tree
**
** Each "table" in the SQLite schema corresponds to a separate prolly tree
** identified by a ProllyHash root. The BtShared.aTables registry maps
** table numbers (Pgno) to their current root hashes.
**
** Write operations buffer edits in a ProllyMutMap (a skip list), then
** flush them by merging with the existing tree to produce a new root.
** This is the copy-on-write / structural sharing model used by Dolt.
**
** Transactions are lightweight: beginning a write transaction snapshots
** the current root hash. Commit writes pending chunks and updates the
** manifest. Rollback restores the snapshot.
**
** Part of the Doltite storage engine.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "btree.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include "pager_shim.h"
#include "doltlite_commit.h"
#include "sortkey.h"
#include "btree_orig_api.h"
#include "vdbeInt.h"

#include <string.h>
#include <assert.h>

/* SQLite serial type constants */
#define SERIAL_TYPE_NULL      0
#define SERIAL_TYPE_INT8      1
#define SERIAL_TYPE_INT16     2
#define SERIAL_TYPE_INT24     3
#define SERIAL_TYPE_INT32     4
#define SERIAL_TYPE_INT48     5
#define SERIAL_TYPE_INT64     6
#define SERIAL_TYPE_FLOAT64   7
#define SERIAL_TYPE_ZERO      8   /* Integer constant 0 (no payload) */
#define SERIAL_TYPE_ONE       9   /* Integer constant 1 (no payload) */
#define SERIAL_TYPE_TEXT_BASE 13  /* Text: serial_type = len*2 + 13 */
#define SERIAL_TYPE_BLOB_BASE 12  /* Blob: serial_type = len*2 + 12 */
#define MAX_RECORD_FIELDS     64  /* Max fields in stack-allocated arrays */
#define MAX_ONEBYTE_HEADER   126  /* Max header size for single-byte varint */

/* Forward declarations */
static void registerDoltiteFunctions(sqlite3 *db);
void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);

/* --------------------------------------------------------------------------
** Constants and macros
** -------------------------------------------------------------------------- */

/*
** Transaction states.  These must match the TRANS_* values used by
** the rest of the SQLite codebase (defined in sqliteInt.h for the
** standard btree, but we define them here for the prolly tree build).
*/
#ifndef TRANS_NONE
#define TRANS_NONE  0
#define TRANS_READ  1
#define TRANS_WRITE 2
#endif

/*
** Savepoint operation codes.  These are the values passed as the first
** argument to sqlite3BtreeSavepoint().
*/
#ifndef SAVEPOINT_BEGIN
#define SAVEPOINT_BEGIN    0
#define SAVEPOINT_RELEASE  1
#define SAVEPOINT_ROLLBACK 2
#endif

/*
** Cursor states.  A cursor is in one of these states at all times.
**
** CURSOR_VALID:       Cursor points to a valid entry.
** CURSOR_INVALID:     Cursor does not point to a valid entry.
** CURSOR_SKIPNEXT:    Next call to Next or Previous should be skipped.
** CURSOR_REQUIRESEEK: Cursor position was saved and needs to be restored.
** CURSOR_FAULT:       An error has occurred; cursor is unusable.
*/
#define CURSOR_VALID       0
#define CURSOR_INVALID     1
#define CURSOR_SKIPNEXT    2
#define CURSOR_REQUIRESEEK 3
#define CURSOR_FAULT       4

/*
** Cursor flags.  These are stored in BtCursor.curFlags.
*/
#define BTCF_WriteFlag  0x01   /* Cursor is open for writing */
#define BTCF_ValidNKey  0x02   /* Cache of BtCursor.nKey is valid */
#define BTCF_ValidOvfl  0x04   /* Overflow page cache is valid */
#define BTCF_AtLast     0x08   /* Cursor is pointing at the last entry */
#define BTCF_Incrblob   0x10   /* This is an incremental blob cursor */
#define BTCF_Multiple   0x20   /* Multiple cursors on same btree */
#define BTCF_Pinned     0x40   /* Cursor is pinned (cannot be moved by save) */

/*
** BtShared flags (stored in BtShared.btsFlags).
*/
#define BTS_READ_ONLY       0x0001  /* Database is read-only */
#define BTS_PAGESIZE_FIXED  0x0002  /* Page size cannot be changed */
#define BTS_SECURE_DELETE   0x0004  /* Secure delete is enabled */
#define BTS_OVERWRITE       0x0008  /* Overwrite deleted content with zeros */
#define BTS_INITIALLY_EMPTY 0x0010  /* Database was empty at open time */
#define BTS_NO_WAL          0x0020  /* WAL mode is not used */

/* WS_* constants now defined in chunk_store.h */

/* Clear the cached payload, freeing owned copies */
#define CLEAR_CACHED_PAYLOAD(pCur) do{ \
  if( (pCur)->cachedPayloadOwned && (pCur)->pCachedPayload ){ \
    sqlite3_free((pCur)->pCachedPayload); \
  } \
  (pCur)->pCachedPayload = 0; \
  (pCur)->nCachedPayload = 0; \
  (pCur)->cachedPayloadOwned = 0; \
}while(0)

/*
** BtLock structure.  Not used by prolly trees (no shared cache) but
** needed for struct compatibility with code that references Btree.lock.
*/
typedef struct BtLock BtLock;
struct BtLock {
  Btree *pBtree;        /* Btree handle holding this lock */
  Pgno iTable;           /* Root page of the table being locked */
  u8 eLock;              /* READ_LOCK or WRITE_LOCK */
  BtLock *pNext;         /* Next in linked list of locks */
};

/* Default cache capacity (number of nodes) */
#define PROLLY_DEFAULT_CACHE_SIZE 1024

/* Default page size for compatibility with SQLite callers */
#define PROLLY_DEFAULT_PAGE_SIZE 4096

/* Maximum record size (1 GB) */
#define PROLLY_MAX_RECORD_SIZE ((sqlite3_int64)(1024*1024*1024))

/* The empty hash (all zeros) represents an empty tree */
static const ProllyHash emptyHash = {{0}};

/* --------------------------------------------------------------------------
** Table entry in BtShared table registry.
**
** Each entry maps a table number (Pgno, used as a logical identifier)
** to its current prolly tree root hash and table flags.
** -------------------------------------------------------------------------- */
struct TableEntry {
  Pgno iTable;           /* Logical table number (like SQLite's root page) */
  ProllyHash root;       /* Current root hash of this table's prolly tree */
  ProllyHash schemaHash; /* Hash of this table's CREATE TABLE SQL (schema) */
  u8 flags;              /* BTREE_INTKEY or BTREE_BLOBKEY */
  char *zName;           /* Table name (owned, NULL for internal tables) */
  ProllyMutMap *pPending; /* Deferred edits from closed write cursors.
                          ** Transferred here on cursor close, flushed at
                          ** BtreeCommitPhaseTwo or BtreeFirst/BtreeLast. */
};

/* --------------------------------------------------------------------------
** Core structure definitions.
**
** These are the prolly-tree versions of SQLite's Btree, BtShared, and
** BtCursor structures. They are forward-declared in btree.h and fully
** defined here.
** -------------------------------------------------------------------------- */

/*
** BtShared: Shared state for a single database file.
** Contains the chunk store, node cache, table registry, and meta values.
**
** In standard SQLite, BtShared can be shared among multiple connections
** (shared cache mode). In the prolly tree implementation, shared cache
** is not supported, so there is always a 1:1 relationship between
** Btree and BtShared.
*/
struct BtShared {
  ChunkStore store;          /* Content-addressed chunk store */
  ProllyCache cache;         /* LRU cache for deserialized prolly nodes */

  /* Pager shim: provides the Pager* interface for code that calls
  ** sqlite3BtreePager() and then uses the result. */
  PagerShim *pPagerShim;

  sqlite3 *db;              /* Database connection (for error reporting) */
  BtCursor *pCursor;        /* Linked list of all open cursors */
  u8 openFlags;             /* Flags from sqlite3BtreeOpen() */
  u16 btsFlags;             /* BTS_* flags */
  u32 pageSize;             /* Dummy page size for compatibility (4096) */
  int nRef;                 /* Reference count from Btree handles */
};

/*
** BtreeOps: vtable for dispatching Btree operations.
** Prolly-tree and original-SQLite implementations each provide a static
** instance of this struct.  Every Btree handle stores a pointer to the
** appropriate table, eliminating per-call if(pOrigBtree) dispatch checks.
*/
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

/* --------------------------------------------------------------------------
** BtCursorOps vtable: function-pointer table for cursor-level dispatch.
** -------------------------------------------------------------------------- */
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

/*
** Btree: Per-connection handle for an open database.
*/
struct Btree {
  sqlite3 *db;              /* The database connection holding this btree */
  BtShared *pBt;            /* The underlying shared state */
  u8 inTrans;               /* TRANS_NONE, TRANS_READ, TRANS_WRITE */
  u8 sharable;              /* Always 0 for prolly tree (no shared cache) */
  int wantToLock;            /* Number of nested calls to sqlite3BtreeEnter() */
  int nBackup;               /* Number of backup operations reading this btree */
  u32 iBDataVersion;         /* Combines with Pager iDataVersion */
  Btree *pNext;              /* List linkage for db->aDb */
  Btree *pPrev;
  BtLock lock;               /* Unused but needed for struct compat */
  u64 nSeek;                 /* Debug: count of seek operations */

  /* Per-session state (moved from BtShared for per-connection branching) */
  ProllyHash root;           /* Current root hash (may include uncommitted) */
  ProllyHash committedRoot;  /* Root hash at last commit (for rollback) */

  /*
  ** Table registry: maps table number (Pgno) to prolly tree root hash.
  ** Table 1 is always the master schema table (sqlite_master).
  */
  struct TableEntry *aTables;
  int nTables;               /* Number of tables in registry */
  int nTablesAlloc;          /* Allocated capacity of aTables */
  Pgno iNextTable;           /* Next table number to assign on CREATE */

  /* Meta values (SQLITE_N_BTREE_META = 16). */
  u32 aMeta[16];

  /* Schema management */
  void *pSchema;             /* Pointer to Schema object */
  void (*xFreeSchema)(void*); /* Destructor for pSchema */

  u8 inTransaction;         /* TRANS_NONE, TRANS_READ, or TRANS_WRITE */

  /*
  ** Savepoint stack.
  */
  ProllyHash *aSavepoint;   /* Array of root hash snapshots */
  int nSavepoint;            /* Number of active savepoints */
  int nSavepointAlloc;       /* Allocated capacity of aSavepoint */

  struct SavepointTableState {
    struct TableEntry *aTables;
    int *aPendingCount;    /* nEntries for each table's pPending at save time */
    int nTables;
    Pgno iNextTable;
  } *aSavepointTables;
  int nSavepointTablesAlloc;

  /* Committed table registry for transaction rollback */
  struct TableEntry *aCommittedTables;
  int nCommittedTables;
  Pgno iCommittedNextTable;

  /* Per-session branch state (authoritative in-memory state).
  ** Persisted to per-branch WorkingSet chunks, NOT in the manifest. */
  char *zBranch;             /* Current branch name (owned, NULL = "main") */
  char *zAuthorName;         /* Configured author name (owned, NULL = "doltlite") */
  char *zAuthorEmail;        /* Configured author email (owned, NULL = "") */
  ProllyHash headCommit;     /* This session's HEAD commit hash */
  ProllyHash stagedCatalog;  /* This session's staged catalog hash */
  u8 isMerging;              /* 1 if a merge is in progress */
  ProllyHash mergeCommitHash;     /* Commit hash being merged in */
  ProllyHash conflictsCatalogHash; /* Conflicts catalog hash */
  const struct BtreeOps *pOps;  /* Vtable: prolly or orig dispatch */
  void *pOrigBtree;  /* Non-NULL → delegate to original SQLite btree */
};

/*
** BtCursor: Cursor for iterating over entries in a single table.
**
** Each cursor references a specific table by pgnoRoot (the logical
** table number) and maintains a ProllyCursor for tree traversal.
** Write cursors also have a ProllyMutMap for buffering edits.
*/
struct BtCursor {
  u8 eState;                 /* CURSOR_VALID, CURSOR_INVALID, etc. */
  u8 curFlags;               /* BTCF_* flags */
  u8 curPagerFlags;          /* Copy of BtShared.btsFlags for cursor */
  u8 hints;                  /* Hint flags from CursorHintFlags */
  int skipNext;              /* Skip direction for CURSOR_SKIPNEXT, or error */
  Btree *pBtree;             /* The Btree handle that opened this cursor */
  BtShared *pBt;             /* The shared state */
  BtCursor *pNext;           /* Linked list of all cursors on pBt */
  Pgno pgnoRoot;             /* Table number (logical root page) */
  u8 curIntKey;              /* True if this is an INTKEY table */
  struct KeyInfo *pKeyInfo;  /* Key comparison info for index btrees */

  /* Prolly cursor: actual tree traversal state */
  ProllyCursor pCur;

  /* Pending writes for this table */
  ProllyMutMap *pMutMap;

  /* Cached payload — may point to MutMap data (borrowed, not owned) */
  u8 *pCachedPayload;
  int nCachedPayload;
  u8 cachedPayloadOwned;   /* 1 if pCachedPayload was sqlite3_malloc'd */
  i64 cachedIntKey;

  /* Pinned state */
  u8 isPinned;

  /* Merge iteration state: when mmActive, BtreeNext/Prev merges the
  ** tree cursor with pending MutMap entries in sorted order. */
  int mmIdx;                 /* Current MutMap index for merge (-1 = none) */
  u8 mmActive;               /* 1 if merge iteration is active */
#define MERGE_SRC_TREE  0
#define MERGE_SRC_MUT   1
#define MERGE_SRC_BOTH  2    /* Same key in tree and MutMap — MutMap wins */
  u8 mergeSrc;               /* Which source provided the current entry */

  /* Save/restore state */
  i64 nKey;                  /* Saved integer key or blob key length */
  void *pKey;                /* Saved blob key (malloc'd) */
  u64 nSeek;                 /* Debug seek counter (per-cursor) */
  void *pOrigCursor; /* Non-NULL → delegate to original SQLite cursor */
  const struct BtCursorOps *pCurOps; /* Vtable: prolly or orig cursor dispatch */
};

/* --------------------------------------------------------------------------
** Internal helper function prototypes
** -------------------------------------------------------------------------- */
static struct TableEntry *findTable(Btree *pBtree, Pgno iTable);
static struct TableEntry *addTable(Btree *pBtree, Pgno iTable, u8 flags);
static void removeTable(Btree *pBtree, Pgno iTable);
static void invalidateCursors(BtShared *pBt, Pgno iTable, int errCode);
static void invalidateSchema(Btree *pBtree);
static int flushMutMap(BtCursor *pCur);
static int flushIfNeeded(BtCursor *pCur);
static int flushAllPending(BtShared *pBt, Pgno iTable);
static int flushDeferredEdits(BtShared *pBt);
static int ensureMutMap(BtCursor *pCur);
static int saveCursorPosition(BtCursor *pCur);
static int restoreCursorPosition(BtCursor *pCur, int *pDifferentRow);
static int pushSavepoint(Btree *pBtree);
static void refreshCursorRoot(BtCursor *pCur);
static int countTreeEntries(Btree *pBtree, Pgno iTable, i64 *pCount);
static int saveAllCursors(BtShared *pBt, Pgno iRoot, BtCursor *pExcept);
static int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut);
static int deserializeCatalog(Btree *pBtree, const u8 *data, int nData);
static int btreeRefreshFromDisk(Btree *p);
static int btreeDeleteDeferred(BtCursor *pCur, const u8 *pKey, int nKey, i64 iKey);
static int btreeDeleteImmediate(BtCursor *pCur, const u8 *pKey, int nKey, i64 iKey);

/* --------------------------------------------------------------------------
** BtreeOps vtable: forward declarations and static instances.
**
** Each Btree-level public function is split into a prolly implementation
** (prollyBtreeXxx) and an orig-SQLite wrapper (origBtreeXxxVt).  The two
** static vtable instances below let sqlite3BtreeOpen install the right
** dispatch table once, eliminating per-call if(pOrigBtree) checks.
** -------------------------------------------------------------------------- */

/* --- prolly implementation forward declarations --- */
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

/* --- orig-SQLite wrapper forward declarations --- */
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

/* --- vtable instances --- */
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

/* --------------------------------------------------------------------------
** BtCursorOps vtable: forward declarations and static instances.
**
** Each cursor-level public function is split into a prolly implementation
** (prollyCursorXxx) and an orig-SQLite wrapper (origCursorXxxVt).  The two
** static vtable instances below let cursor creation install the right
** dispatch table once, eliminating per-call if(pOrigCursor) checks.
** -------------------------------------------------------------------------- */

/* --- prolly cursor implementation forward declarations --- */
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

/* --- orig-cursor wrapper forward declarations --- */
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

/* --- cursor vtable instances --- */
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

/* --------------------------------------------------------------------------
** Internal helper implementations
** -------------------------------------------------------------------------- */

/*
** Find a table entry by table number in the BtShared table registry.
** Returns a pointer to the entry, or NULL if not found.
*/
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

/*
** Add a new table entry to the registry.  The new table starts with
** an empty root hash (all zeros), meaning an empty tree.
** Returns a pointer to the new entry, or NULL on allocation failure.
*/
static struct TableEntry *addTable(Btree *pBtree, Pgno iTable, u8 flags){
  struct TableEntry *pEntry;

  /* Check if table already exists */
  pEntry = findTable(pBtree, iTable);
  if( pEntry ){
    pEntry->flags = flags;
    return pEntry;
  }

  /* Grow the array if needed */
  if( pBtree->nTables>=pBtree->nTablesAlloc ){
    int nNew = pBtree->nTablesAlloc ? pBtree->nTablesAlloc*2 : 16;
    struct TableEntry *aNew;
    aNew = sqlite3_realloc(pBtree->aTables, nNew*(int)sizeof(struct TableEntry));
    if( !aNew ) return 0;
    pBtree->aTables = aNew;
    pBtree->nTablesAlloc = nNew;
  }

  /* Find sorted insertion point using binary search */
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
    /* Insert at position lo, shifting elements right */
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

/*
** Remove a table entry from the registry by table number.
*/
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

/*
** Invalidate all cursors on a given table (or all if iTable==0).
*/
/*
** Invalidate the schema cache. Called on rollback/savepoint rollback
** so that SQLite re-reads sqlite_master on the next operation.
*/
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
      /* Discard pending mutations — tree is being rolled back */
      if( p->pMutMap ) prollyMutMapClear(p->pMutMap);
    }
  }
}

/*
** Refresh a cursor's prolly cursor root from the table registry.
*/
static void refreshCursorRoot(BtCursor *pCur){
  struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE ){
    pCur->pCur.root = pTE->root;
  }
}

/*
** Serialize the table registry + meta values into a catalog chunk.
** Format: [iNextTable:4][nTables:4][meta[0..15]: 64 bytes]
**         per table: [iTable:4][flags:1][root:20]
*/
/*
** Serialize the catalog (table registry) to a content-addressed chunk.
**
** Following Dolt's RootValue model, the catalog is purely data-derived:
** table entries with their root hashes, schema hashes, names, and flags.
** aMeta[0..15] (SQLite runtime state) is NOT included — it is reconstructed
** from constants on catalog load. This ensures the catalog hash changes ONLY
** when actual table data or schema changes, enabling O(1) dirty checks.
**
** Format V2 (clean catalog):
**   version(1) = 0x02
**   iNextTable(4)
**   nTables(4)
**   table_entries[]:
**     iTable(4) + flags(1) + root(20) + schemaHash(20) + name_len(2) + name(var)
**
** Format:
**   version(1=0x02) + iNextTable(4) + nTables(4) + table_entries[...]
*/
/* Catalog version tag — must NOT collide with DOLTLITE_COMMIT_V2 (0x02)
** or PROLLY_NODE_MAGIC first byte. Using 0x43 = 'C' for Catalog. */
#define CATALOG_FORMAT_V2 0x43

static int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut){
  int nTables = pBtree->nTables;
  int sz = 1 + 4 + 4;  /* version(1) + iNextTable(4) + nTables(4) */
  u8 *buf, *q;
  int i;

  /* Populate table names from sqlite_master if missing */
  if( pBtree->db ){
    for(i=0; i<nTables; i++){
      if( !pBtree->aTables[i].zName && pBtree->aTables[i].iTable>1 ){
        pBtree->aTables[i].zName = doltliteResolveTableNumber(
            pBtree->db, pBtree->aTables[i].iTable);
      }
    }
  }
  /* Note: schemaHash is computed by doltliteUpdateSchemaHashes() which is
  ** called from dolt_commit/dolt_add at the SQL function level — NOT here,
  ** because serializeCatalog can be called during BtreeCommitPhaseTwo where
  ** re-entrant SQL queries would crash. */

  /* Calculate variable size: per table = 4+1+20+20+2+name_len */
  for(i=0; i<nTables; i++){
    int nLen = pBtree->aTables[i].zName ? (int)strlen(pBtree->aTables[i].zName) : 0;
    if( sz > 0x7FFFFFFF - (4 + 1 + PROLLY_HASH_SIZE*2 + 2 + nLen) ){
      return SQLITE_TOOBIG;
    }
    sz += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2 + nLen;
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  q = buf;
  /* Version tag — distinguishes V2 (no meta) from V1 (has meta) */
  *q++ = CATALOG_FORMAT_V2;
  q[0]=(u8)(pBtree->iNextTable); q[1]=(u8)(pBtree->iNextTable>>8);
  q[2]=(u8)(pBtree->iNextTable>>16); q[3]=(u8)(pBtree->iNextTable>>24);
  q += 4;
  q[0]=(u8)nTables; q[1]=(u8)(nTables>>8);
  q[2]=(u8)(nTables>>16); q[3]=(u8)(nTables>>24);
  q += 4;
  /* Table entries only — no aMeta. Hash covers purely data content. */
  for(i=0; i<nTables; i++){
    struct TableEntry *t = &pBtree->aTables[i];
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
  *ppOut = buf;
  *pnOut = (int)(q - buf);
  return SQLITE_OK;
}

/*
** Initialize aMeta with default constant values.
** Called after deserializing a V2 catalog (which has no meta) or
** during a fresh database open.
*/
static void initDefaultMeta(Btree *pBtree){
  memset(pBtree->aMeta, 0, sizeof(pBtree->aMeta));
  pBtree->aMeta[BTREE_FILE_FORMAT] = 4;
  pBtree->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;
  /* BTREE_SCHEMA_VERSION starts at 0, bumped by doltliteHardReset.
  ** BTREE_LARGEST_ROOT_PAGE is set after table entries are loaded. */
}

/*
** Deserialize a V2 catalog chunk into the table registry.
** V2 format: version(1='C') + iNextTable(4) + nTables(4) + entries.
** No aMeta — runtime meta is initialized from constants.
** BTREE_SCHEMA_VERSION is derived from a hash of the catalog data so
** that different catalogs produce different versions (needed for
** multi-connection schema change detection).
*/
static int deserializeCatalog(Btree *pBtree, const u8 *data, int nData){
  const u8 *q = data;
  int nTables, i;

  if( nData < 9 ) return SQLITE_CORRUPT;
  if( data[0] != CATALOG_FORMAT_V2 ) return SQLITE_CORRUPT;

  /* Clear table registry */
  sqlite3_free(pBtree->aTables);
  pBtree->aTables = 0;
  pBtree->nTables = 0;
  pBtree->nTablesAlloc = 0;

  /* V2 format: version(1) + iNextTable(4) + nTables(4) + entries */
  q++;  /* skip version byte */
  pBtree->iNextTable = (Pgno)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  q += 4;
  nTables = (int)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  q += 4;
  initDefaultMeta(pBtree);

  /* Derive BTREE_SCHEMA_VERSION from catalog content so multiple
  ** connections on the same database agree on the version number,
  ** and schema changes are detected across connections. */
  {
    u32 schemaHash = 0;
    int j;
    for(j = 0; j < nData; j++){
      schemaHash = schemaHash * 31 + data[j];
    }
    pBtree->aMeta[BTREE_SCHEMA_VERSION] = schemaHash | 1;  /* ensure non-zero */
  }

  /* Table entries */
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

  /* Derive LARGEST_ROOT_PAGE from loaded tables */
  {
    Pgno maxPage = 0;
    for(i=0; i<pBtree->nTables; i++){
      if( pBtree->aTables[i].iTable > maxPage ){
        maxPage = pBtree->aTables[i].iTable;
      }
    }
    pBtree->aMeta[BTREE_LARGEST_ROOT_PAGE] = maxPage;
  }

  return SQLITE_OK;
}

/*
** Flush pending mutations from a cursor's MutMap into the table's
** prolly tree.  Produces a new root hash and updates the table registry.
*/
static int flushMutMap(BtCursor *pCur){
  int rc;
  struct TableEntry *pTE;
  ProllyMutator mut;

  if( !pCur->pMutMap || prollyMutMapIsEmpty(pCur->pMutMap) ){
    return SQLITE_OK;
  }

  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    return SQLITE_INTERNAL;
  }

  memset(&mut, 0, sizeof(mut));
  mut.pStore = &pCur->pBt->store;
  mut.pCache = &pCur->pBt->cache;
  mut.oldRoot = pTE->root;
  mut.pEdits = pCur->pMutMap;
  mut.flags = pTE->flags;

  rc = prollyMutateFlush(&mut);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  pTE->root = mut.newRoot;
  pCur->pCur.root = mut.newRoot;
  prollyMutMapClear(pCur->pMutMap);

  return SQLITE_OK;
}

/*
** Sync savepoint stack with db->nSavepoint.
** SQLite creates user savepoints without notifying the btree layer,
** so we lazily create savepoint snapshots before each write operation.
*/
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

/*
** Ensure the cursor has a MutMap allocated for buffering writes.
*/
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

/*
** Save the current cursor position so it can be restored later.
*/
static int saveCursorPosition(BtCursor *pCur){
  int rc = SQLITE_OK;

  if( pCur->eState!=CURSOR_VALID && pCur->eState!=CURSOR_SKIPNEXT ){
    return SQLITE_OK;
  }
  if( pCur->isPinned ){
    return SQLITE_OK;
  }

  /* If the prolly cursor isn't actually valid (e.g. empty tree, or
  ** cursor was set to CURSOR_VALID without a successful seek via MutMap),
  ** use cached key data if available, otherwise invalidate. */
  if( !prollyCursorIsValid(&pCur->pCur) ){
    if( pCur->curIntKey && (pCur->curFlags & BTCF_ValidNKey) ){
      /* MutMap-satisfied seek: save the cached integer key */
      pCur->nKey = pCur->cachedIntKey;
      pCur->pKey = 0;
      pCur->eState = CURSOR_REQUIRESEEK;
      return SQLITE_OK;
    }
    pCur->eState = CURSOR_INVALID;
    return SQLITE_OK;
  }

  /* Save key BEFORE prollyCursorSave releases node references */
  if( pCur->curIntKey ){
    pCur->nKey = prollyCursorIntKey(&pCur->pCur);
    pCur->pKey = 0;
  } else {
    const u8 *pKey;
    int nKey;
    prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    if( nKey>0 ){
      sqlite3_free(pCur->pKey);
      pCur->pKey = sqlite3_malloc(nKey);
      if( !pCur->pKey ){
        return SQLITE_NOMEM;
      }
      memcpy(pCur->pKey, pKey, nKey);
      pCur->nKey = nKey;
    } else {
      pCur->pKey = 0;
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

/*
** Restore cursor to its previously saved position.
*/
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

/*
** Push the current state onto the savepoint stack.
*/
/* Free a savepoint's table snapshot.  pPending pointers in the snapshot
** are always NULL (we don't clone MutMaps), so no MutMap freeing needed. */
static void freeSavepointTables(struct SavepointTableState *pState){
  if( pState->aTables ){
    sqlite3_free(pState->aTables);
    pState->aTables = 0;
  }
  if( pState->aPendingCount ){
    sqlite3_free(pState->aPendingCount);
    pState->aPendingCount = 0;
  }
}

static int pushSavepoint(Btree *pBtree){
  struct SavepointTableState *pState;
  int rc;

  /* Flush cursor-level MutMaps so all pending edits land in table-level
  ** pPending.  This is cheap (MutMap merge, no tree rebuild). */
  {
    BtCursor *p;
    for(p = pBtree->pBt->pCursor; p; p = p->pNext){
      rc = flushIfNeeded(p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  if( pBtree->nSavepoint>=pBtree->nSavepointAlloc ){
    int nNew = pBtree->nSavepointAlloc ? pBtree->nSavepointAlloc*2 : 8;
    ProllyHash *aNewH;
    struct SavepointTableState *aNewT;
    aNewH = sqlite3_realloc(pBtree->aSavepoint, nNew*(int)sizeof(ProllyHash));
    if( !aNewH ) return SQLITE_NOMEM;
    pBtree->aSavepoint = aNewH;
    aNewT = sqlite3_realloc(pBtree->aSavepointTables, nNew*(int)sizeof(struct SavepointTableState));
    if( !aNewT ) return SQLITE_NOMEM;
    pBtree->aSavepointTables = aNewT;
    pBtree->nSavepointAlloc = nNew;
    pBtree->nSavepointTablesAlloc = nNew;
  }

  pBtree->aSavepoint[pBtree->nSavepoint] = pBtree->root;

  /* Snapshot table roots + pPending high-water marks.  No MutMap cloning.
  ** On rollback, we restore the root hashes and truncate each pPending
  ** to its saved size, discarding only post-savepoint edits. */
  pState = &pBtree->aSavepointTables[pBtree->nSavepoint];
  pState->aTables = 0;
  pState->aPendingCount = 0;
  pState->nTables = 0;
  pState->iNextTable = pBtree->iNextTable;
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
        pState->aTables[k].pPending = 0;  /* Don't own live MutMap */
      }
    }
  }

  pBtree->nSavepoint++;
  return SQLITE_OK;
}

/*
** Count all entries in a table's prolly tree by walking the tree.
*/
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

/*
** Save all cursor positions on a particular table before modifying it.
*/
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

/* --------------------------------------------------------------------------
** Open / Close / NewDb
** -------------------------------------------------------------------------- */

/*
** Open a prolly-tree backed database.
*/
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

  /* Detect standard SQLite file → delegate to original btree */
  if( origBtreeIsSqliteFile(zFilename) ){
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

  chunkStoreGetRoot(&pBt->store, &p->root);
  p->committedRoot = p->root;

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

  /* Initialize default meta values */
  p->aMeta[BTREE_FREE_PAGE_COUNT] = 0;
  p->aMeta[BTREE_SCHEMA_VERSION] = 0;
  p->aMeta[BTREE_FILE_FORMAT] = 4;
  p->aMeta[BTREE_DEFAULT_CACHE_SIZE] = 0;
  p->aMeta[BTREE_LARGEST_ROOT_PAGE] = 0;
  p->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;
  p->aMeta[BTREE_USER_VERSION] = 0;
  p->aMeta[BTREE_INCR_VACUUM] = 0;
  p->aMeta[BTREE_APPLICATION_ID] = 0;

  /* Try to load catalog from existing store. If no catalog exists
  ** (new database), initialize defaults. */
  {
    ProllyHash catHash;
    chunkStoreGetCatalog(&pBt->store, &catHash);
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
        goto catalog_loaded;
      }
    }
  }

  /* No catalog found — initialize fresh database */
  p->iNextTable = 2;
  if( !addTable(p, 1, BTREE_INTKEY) ){
    pagerShimDestroy(pBt->pPagerShim);
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }

catalog_loaded:
  p->db = db;
  p->pBt = pBt;
  p->pOps = &prollyBtreeOps;
  p->inTrans = TRANS_NONE;
  p->sharable = 0;
  p->wantToLock = 0;
  p->nBackup = 0;
  p->iBDataVersion = 1;
  p->nSeek = 0;

  /* Initialize per-session branch state from the default branch */
  {
    const char *defBranch = chunkStoreGetDefaultBranch(&pBt->store);
    ProllyHash branchCommit;
    p->zBranch = sqlite3_mprintf("%s", defBranch);
    /* Try to resolve branch → commit from refs */
    if( chunkStoreFindBranch(&pBt->store, defBranch, &branchCommit)==SQLITE_OK ){
      memcpy(&p->headCommit, &branchCommit, sizeof(ProllyHash));
    }else{
      /* No refs yet — use manifest headCommit directly */
      chunkStoreGetHeadCommit(&pBt->store, &p->headCommit);
    }
    /* Load per-branch WorkingSet (staged + merge state).
    ** Falls back to manifest stagedCatalog for migration from pre-WorkingSet format. */
    {
      ProllyHash wsHash;
      if( chunkStoreGetBranchWorkingSet(&pBt->store, defBranch, &wsHash)==SQLITE_OK
       && !prollyHashIsEmpty(&wsHash) ){
        /* WorkingSet exists — load it */
        u8 *wsData = 0; int nWsData = 0;
        if( chunkStoreGet(&pBt->store, &wsHash, &wsData, &nWsData)==SQLITE_OK
         && nWsData >= WS_TOTAL_SIZE ){
          memcpy(p->stagedCatalog.data, wsData + WS_STAGED_OFF, PROLLY_HASH_SIZE);
          p->isMerging = wsData[WS_MERGING_OFF];
          memcpy(p->mergeCommitHash.data, wsData + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
          memcpy(p->conflictsCatalogHash.data, wsData + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
        }
        sqlite3_free(wsData);
      }else{
        /* Migration: load from manifest (legacy) */
        chunkStoreGetStagedCatalog(&pBt->store, &p->stagedCatalog);
        chunkStoreGetMergeState(&pBt->store, &p->isMerging,
                                &p->mergeCommitHash, &p->conflictsCatalogHash);
      }
    }
  }

  *ppBtree = p;

  /* Register doltite_engine() SQL function for runtime detection.
  ** Must happen AFTER *ppBtree is set so that db->aDb[0].pBt is
  ** accessible for functions that need the chunk store. */
  registerDoltiteFunctions(db);

  return SQLITE_OK;
}

/*
** Close a database connection and free all associated resources.
*/
static int prollyBtreeClose(Btree *p){
  BtShared *pBt;

  pBt = p->pBt;
  assert( pBt!=0 );

  while( pBt->pCursor ){
    sqlite3BtreeCloseCursor(pBt->pCursor);
  }

  /* Free per-session state from Btree */
  if( p->pSchema && p->xFreeSchema ){
    p->xFreeSchema(p->pSchema);
    p->pSchema = 0;
  }
  sqlite3_free(p->aTables);
  sqlite3_free(p->aSavepoint);
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

/*
** Initialize a fresh (empty) database with default meta values.
*/
static int prollyBtreeNewDb(Btree *p){
  memset(p->aMeta, 0, sizeof(p->aMeta));
  p->aMeta[BTREE_FILE_FORMAT] = 4;
  p->aMeta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;

  memset(&p->root, 0, sizeof(ProllyHash));
  memset(&p->committedRoot, 0, sizeof(ProllyHash));

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

/* --------------------------------------------------------------------------
** Configuration (mostly no-ops)
** -------------------------------------------------------------------------- */

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

static Pgno prollyBtreeLastPage(Btree *p){
  /* Must be >= iNextTable so rootpage validation in prepare.c passes */
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

/* --------------------------------------------------------------------------
** Transactions
** -------------------------------------------------------------------------- */

/*
** Check whether the on-disk manifest has changed since the last refresh
** and, if so, reload the catalog and root hash from the chunk store.
** Updates committedRoot, iBDataVersion, and the pager shim version.
*/
static int btreeRefreshFromDisk(Btree *p){
  BtShared *pBt = p->pBt;
  int bChanged = 0;
  int rc = chunkStoreRefreshIfChanged(&pBt->store, &bChanged);
  if( rc!=SQLITE_OK ) return rc;
  if( !bChanged ) return SQLITE_OK;

  /* After refresh, decide whether the manifest state belongs to THIS
  ** connection's branch.  The manifest headCommit is updated only by
  ** dolt_commit / dolt_checkout.  If it matches our branch HEAD, the
  ** manifest catalog/root may include uncommitted working changes and
  ** we should use them.  If it doesn't match, a different branch
  ** committed and we must reload from our branch HEAD commit. */
  {
    ProllyHash catHash;
    ProllyHash manifestHead;
    const char *zBr = p->zBranch ? p->zBranch : "main";
    ProllyHash branchHead;
    int useBranchCommit = 0;

    memset(&catHash, 0, sizeof(catHash));
    chunkStoreGetHeadCommit(&pBt->store, &manifestHead);

    if( chunkStoreFindBranch(&pBt->store, zBr, &branchHead)==SQLITE_OK
     && !prollyHashIsEmpty(&branchHead)
     && prollyHashCompare(&manifestHead, &branchHead)!=0 ){
      /* Manifest was written by a different branch — load our branch's
      ** committed state instead of the manifest catalog. */
      u8 *commitData = 0;
      int nCommitData = 0;
      rc = chunkStoreGet(&pBt->store, &branchHead, &commitData, &nCommitData);
      if( rc==SQLITE_OK && commitData ){
        DoltliteCommit commit;
        rc = doltliteCommitDeserialize(commitData, nCommitData, &commit);
        sqlite3_free(commitData);
        if( rc==SQLITE_OK ){
          memcpy(&catHash, &commit.catalogHash, sizeof(ProllyHash));
          memcpy(&p->root, &commit.rootHash, sizeof(ProllyHash));
          memcpy(&p->headCommit, &branchHead, sizeof(ProllyHash));
          doltliteCommitClear(&commit);
          useBranchCommit = 1;
        }
      }
    }

    if( !useBranchCommit ){
      /* Manifest belongs to our branch — use manifest catalog/root
      ** (may include uncommitted working changes). */
      chunkStoreGetCatalog(&pBt->store, &catHash);
      chunkStoreGetRoot(&pBt->store, &p->root);
    }

    if( !prollyHashIsEmpty(&catHash) ){
      u8 *catData = 0;
      int nCatData = 0;
      rc = chunkStoreGet(&pBt->store, &catHash, &catData, &nCatData);
      if( rc==SQLITE_OK && catData ){
        rc = deserializeCatalog(p, catData, nCatData);
        sqlite3_free(catData);
        if( rc!=SQLITE_OK ) return rc;
      }
    }
    p->committedRoot = p->root;
    p->iBDataVersion++;
    if( pBt->pPagerShim ){
      pBt->pPagerShim->iDataVersion++;
    }
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

  /* Detect if another connection replaced the database file */
  rc = btreeRefreshFromDisk(p);
  if( rc!=SQLITE_OK ) return rc;
  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }

  if( wrFlag ){
    if( pBt->btsFlags & BTS_READ_ONLY ){
      return SQLITE_READONLY;
    }
    /* Ensure aTables includes all persistent tables from the catalog.
    ** Tables are lazily added to aTables when cursors open, so after a
    ** transaction commits and cursors close, aTables may only contain
    ** the master table. Reload from the catalog so the snapshot used
    ** for deferred-write eligibility includes all persistent tables. */
    {
      ProllyHash catHash;
      chunkStoreGetCatalog(&pBt->store, &catHash);
      if( !prollyHashIsEmpty(&catHash) ){
        u8 *catData = 0;
        int nCatData = 0;
        int rc2 = chunkStoreGet(&pBt->store, &catHash, &catData, &nCatData);
        if( rc2==SQLITE_OK && catData ){
          rc2 = deserializeCatalog(p, catData, nCatData);
          sqlite3_free(catData);
          if( rc2!=SQLITE_OK ) return rc2;
        }
      }
    }
    /* Snapshot table registry for rollback */
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
    p->committedRoot = p->root;
    p->inTrans = TRANS_WRITE;
    p->inTransaction = TRANS_WRITE;
    /* Ensure btree savepoint stack matches db->nSavepoint.
    ** This mimics what sqlite3PagerOpenSavepoint does in stock SQLite. */
    if( p->db ){
      while( p->nSavepoint < p->db->nSavepoint ){
        int rc2 = pushSavepoint(p);
        if( rc2!=SQLITE_OK ) return rc2;
      }
    }
  } else {
    if( p->inTrans==TRANS_NONE ){
      p->inTrans = TRANS_READ;
      if( p->inTransaction==TRANS_NONE ){
        p->inTransaction = TRANS_READ;
      }
    }
  }

  /* Pin the chunk store snapshot so concurrent commits from other
  ** connections don't change our view mid-transaction. */
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
  (void)bCleanup;

  if( p->inTrans==TRANS_WRITE ){
    /* Flush all pending mutations before committing */
    rc = flushAllPending(pBt, 0);
    if( rc!=SQLITE_OK ) return rc;
    /* Serialize and store the catalog (table registry + meta) */
    {
      u8 *catData = 0;
      int nCatData = 0;
      ProllyHash catHash;
      rc = serializeCatalog(p, &catData, &nCatData);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(&pBt->store, catData, nCatData, &catHash);
        sqlite3_free(catData);
        if( rc==SQLITE_OK ){
          chunkStoreSetCatalog(&pBt->store, &catHash);
        }
      }
      if( rc!=SQLITE_OK ) return rc;
    }
    chunkStoreSetRoot(&pBt->store, &p->root);
    rc = chunkStoreCommit(&pBt->store);
    if( rc==SQLITE_OK ){
      p->committedRoot = p->root;
      p->iBDataVersion++;
      if( pBt->pPagerShim ){
        pBt->pPagerShim->iDataVersion++;
      }
    }
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  p->nSavepoint = 0;

  /* Unpin snapshot so next transaction sees latest state */
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

/*
** Restore the table registry from the committed snapshot.
** Used by both rollback and savepoint rollback to transaction start.
*/
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
  return SQLITE_OK;
}

static int prollyBtreeRollback(Btree *p, int tripCode, int writeOnly){
  BtShared *pBt = p->pBt;
  (void)writeOnly;

  if( p->inTrans==TRANS_WRITE ){
    /* Restore table registry from committed snapshot */
    if( p->aCommittedTables ){
      sqlite3_free(p->aTables);
      p->aTables = p->aCommittedTables;
      p->nTables = p->nCommittedTables;
      p->nTablesAlloc = p->nCommittedTables;
      p->iNextTable = p->iCommittedNextTable;
      p->aCommittedTables = 0;
      p->nCommittedTables = 0;
    }
    p->root = p->committedRoot;
    /* Clear all pending mutations (they're being rolled back) */
    {
      BtCursor *pC;
      for(pC = pBt->pCursor; pC; pC = pC->pNext){
        if( pC->pMutMap ) prollyMutMapClear(pC->pMutMap);
      }
    }
    invalidateCursors(pBt, 0, tripCode ? tripCode : SQLITE_ABORT);
    invalidateSchema(p);
    chunkStoreRollback(&pBt->store);
  }

  p->inTrans = TRANS_NONE;
  p->inTransaction = TRANS_NONE;
  p->nSavepoint = 0;

  /* Unpin snapshot so next transaction sees latest state */
  pBt->store.snapshotPinned = 0;

  return SQLITE_OK;
}
int sqlite3BtreeRollback(Btree *p, int tripCode, int writeOnly){
  if( !p ) return SQLITE_OK;
  return p->pOps->xRollback(p, tripCode, writeOnly);
}

static int prollyBtreeBeginStmt(Btree *p, int iStatement){
  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  /* Push savepoints until we reach iStatement level */
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
    if( iSavepoint>=0 && iSavepoint<p->nSavepoint
     && p->aSavepointTables ){
      struct SavepointTableState *pState = &p->aSavepointTables[iSavepoint];
      p->root = p->aSavepoint[iSavepoint];
      /* Restore table registry: restore root hashes from snapshot and
      ** truncate each pPending to its pre-savepoint size.  Post-savepoint
      ** edits in pPending are freed; pre-savepoint edits are preserved. */
      if( pState->aTables ){
        /* Restore roots and truncate pPending for tables that existed
        ** at savepoint time.  Tables added after the savepoint are removed. */
        {
          int k;
          for(k=0; k<pState->nTables && k<p->nTables; k++){
            p->aTables[k].root = pState->aTables[k].root;
            /* Truncate pPending to saved size, freeing post-savepoint entries */
            ProllyMutMap *pMap = (ProllyMutMap*)p->aTables[k].pPending;
            if( pMap ){
              int savedCount = pState->aPendingCount[k];
              while( pMap->nEntries > savedCount ){
                pMap->nEntries--;
                ProllyMutMapEntry *e = &pMap->aEntries[pMap->nEntries];
                sqlite3_free(e->pKey); e->pKey = 0;
                sqlite3_free(e->pVal); e->pVal = 0;
              }
            }
          }
          /* Free pPending on tables added after the savepoint */
          for(k=pState->nTables; k<p->nTables; k++){
            if( p->aTables[k].pPending ){
              prollyMutMapFree((ProllyMutMap*)p->aTables[k].pPending);
              sqlite3_free(p->aTables[k].pPending);
            }
          }
        }
        /* Restore table count */
        p->nTables = pState->nTables;
        p->nTablesAlloc = p->nTables;
        p->iNextTable = pState->iNextTable;
        /* Free the snapshot aTables (we restored roots in-place) */
        sqlite3_free(pState->aTables);
        sqlite3_free(pState->aPendingCount);
        pState->aTables = 0;
        pState->aPendingCount = 0;
      }
      /* Free savepoints above this one */
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
      p->root = p->committedRoot;
      { int rc2 = restoreFromCommitted(p); if( rc2 ) return rc2; }
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    } else if( iSavepoint<0 ){
      int j;
      for(j=0; j<p->nSavepoint; j++){
        freeSavepointTables(&p->aSavepointTables[j]);
      }
      p->root = p->committedRoot;
      { int rc2 = restoreFromCommitted(p); if( rc2 ) return rc2; }
      p->nSavepoint = 0;
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    }
  } else {
    /* SAVEPOINT_RELEASE: free this savepoint and all above it */
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

/* --------------------------------------------------------------------------
** Table operations
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** Meta values
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** Schema
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** Cursor operations
** -------------------------------------------------------------------------- */

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

  /* Piece 2: pick up deferred edits from a previous cursor on this table.
  **
  ** Bug #240: When a read cursor opens while a write cursor is still active
  ** on the same table, the MutMap is in the write cursor (not pPending).
  ** The read cursor would see a stale tree.  Fix: before any cursor opens
  ** on a table, flush ALL live MutMaps for that table to the tree. This
  ** ensures every cursor sees consistent data.
  */
  {
    BtCursor *pOther;
    for(pOther = pBt->pCursor; pOther; pOther = pOther->pNext){
      if( pOther->pgnoRoot==iTable && pOther->pMutMap
          && !prollyMutMapIsEmpty(pOther->pMutMap) ){
        flushMutMap(pOther);
      }
    }
  }
  if( pTE->pPending ){
    if( wrFlag & BTREE_WRCSR ){
      pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
      pTE->pPending = 0;
    }else{
      /* Flush pPending too so read cursor sees consistent tree */
      ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
      if( !prollyMutMapIsEmpty(pMap) ){
        ProllyMutator mut;
        int rc2;
        memset(&mut, 0, sizeof(mut));
        mut.pStore = &pBt->store;
        mut.pCache = &pBt->cache;
        memcpy(&mut.oldRoot, &pTE->root, sizeof(ProllyHash));
        mut.pEdits = pMap;
        mut.flags = pTE->flags;
        rc2 = prollyMutateFlush(&mut);
        if( rc2==SQLITE_OK ){
          memcpy(&pTE->root, &mut.newRoot, sizeof(ProllyHash));
        }
        prollyMutMapFree(pMap);
        sqlite3_free(pMap);
        pTE->pPending = 0;
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

static int prollyBtCursorCloseCursor(BtCursor *pCur){
  BtShared *pBt;
  BtCursor **pp;

  if( !pCur ) return SQLITE_OK;
  pBt = pCur->pBt;
  if( !pBt ) return SQLITE_OK;

  /* Piece 4: Transfer MutMap to table entry for deferred flush.
  ** The MutMap will be picked up by the next cursor on this table,
  ** or flushed at BtreeCommitPhaseTwo / BtreeFirst / BtreeLast. */
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pTE && !pTE->pPending ){
      pTE->pPending = pCur->pMutMap;
      pCur->pMutMap = 0;
    }else if( pTE && pTE->pPending ){
      /* Merge into existing pending instead of flushing (tree rebuild).
      ** This happens when multiple cursors on the same table close
      ** during the same transaction. */
      prollyMutMapMerge((ProllyMutMap*)pTE->pPending, pCur->pMutMap);
      prollyMutMapFree(pCur->pMutMap);
      sqlite3_free(pCur->pMutMap);
      pCur->pMutMap = 0;
    }else{
      flushMutMap(pCur);
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

/* --------------------------------------------------------------------------
** Merge iteration: two-pointer merge over tree cursor + MutMap.
** Both sources are sorted by the same key ordering.  On each step,
** we pick the smaller (Next) or larger (Prev) key, handling DELETEs
** by skipping entries present in both sources.
** -------------------------------------------------------------------------- */

/* Compare the tree cursor's current key with a MutMap entry. */
static int mergeCompare(BtCursor *pCur, ProllyMutMapEntry *e){
  if( pCur->curIntKey ){
    i64 tk = prollyCursorIntKey(&pCur->pCur);
    if( tk < e->intKey ) return -1;
    if( tk > e->intKey ) return 1;
    return 0;
  }else{
    const u8 *pK; int nK;
    prollyCursorKey(&pCur->pCur, &pK, &nK);
    int n = nK < e->nKey ? nK : e->nKey;
    int c = memcmp(pK, e->pKey, n);
    if( c ) return c;
    return (nK < e->nKey) ? -1 : (nK > e->nKey) ? 1 : 0;
  }
}

/* Advance to the next merged entry (forward). Returns SQLITE_OK or
** SQLITE_DONE.  Skips DELETE entries and tree entries overridden by
** MutMap. */
static int mergeStepForward(BtCursor *pCur){
  int treeOk, mutOk;

  /* Advance the source(s) that produced the current entry */
  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    prollyCursorNext(&pCur->pCur);
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH ){
    pCur->mmIdx++;
  }

  for(;;){
    treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
    mutOk  = (pCur->mmIdx >= 0 && pCur->mmIdx < pCur->pMutMap->nEntries);

    if( !treeOk && !mutOk ) return SQLITE_DONE;

    if( !mutOk ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      return SQLITE_OK;
    }
    ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
    if( !treeOk ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx++; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      return SQLITE_OK;
    }
    int cmp = mergeCompare(pCur, e);
    if( cmp < 0 ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      return SQLITE_OK;
    }else if( cmp > 0 ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx++; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      return SQLITE_OK;
    }else{
      /* Same key — MutMap overrides tree */
      if( e->op==PROLLY_EDIT_DELETE ){
        pCur->mmIdx++;
        prollyCursorNext(&pCur->pCur);
        continue;
      }
      pCur->mergeSrc = MERGE_SRC_BOTH;
      return SQLITE_OK;
    }
  }
}

/* Advance to the previous merged entry (backward). */
static int mergeStepBackward(BtCursor *pCur){
  int treeOk, mutOk;

  if( pCur->mergeSrc==MERGE_SRC_TREE || pCur->mergeSrc==MERGE_SRC_BOTH ){
    prollyCursorPrev(&pCur->pCur);
  }
  if( pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH ){
    pCur->mmIdx--;
  }

  for(;;){
    treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
    mutOk  = (pCur->mmIdx >= 0 && pCur->mmIdx < pCur->pMutMap->nEntries);

    if( !treeOk && !mutOk ) return SQLITE_DONE;

    if( !mutOk ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      return SQLITE_OK;
    }
    ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
    if( !treeOk ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx--; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      return SQLITE_OK;
    }
    int cmp = mergeCompare(pCur, e);
    if( cmp > 0 ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      return SQLITE_OK;
    }else if( cmp < 0 ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx--; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      return SQLITE_OK;
    }else{
      if( e->op==PROLLY_EDIT_DELETE ){
        pCur->mmIdx--;
        prollyCursorPrev(&pCur->pCur);
        continue;
      }
      pCur->mergeSrc = MERGE_SRC_BOTH;
      return SQLITE_OK;
    }
  }
}

/* Find the first merged entry (for BtreeFirst). */
static int mergeFirst(BtCursor *pCur, int *pRes){
  pCur->mergeSrc = MERGE_SRC_TREE;  /* Will be corrected below */
  pCur->mmIdx = 0;

  int treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
  int mutOk  = (pCur->mmIdx < pCur->pMutMap->nEntries);

  /* Position: we haven't advanced yet, so this IS the first entry.
  ** Use the same scan logic as mergeStepForward but without advancing first. */
  for(;;){
    treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
    mutOk  = (pCur->mmIdx >= 0 && pCur->mmIdx < pCur->pMutMap->nEntries);

    if( !treeOk && !mutOk ){ *pRes = 1; return SQLITE_OK; }

    if( !mutOk ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      *pRes = 0; return SQLITE_OK;
    }
    ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
    if( !treeOk ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx++; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      *pRes = 0; return SQLITE_OK;
    }
    int cmp = mergeCompare(pCur, e);
    if( cmp < 0 ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      *pRes = 0; return SQLITE_OK;
    }else if( cmp > 0 ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx++; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      *pRes = 0; return SQLITE_OK;
    }else{
      if( e->op==PROLLY_EDIT_DELETE ){
        pCur->mmIdx++;
        prollyCursorNext(&pCur->pCur);
        continue;
      }
      pCur->mergeSrc = MERGE_SRC_BOTH;
      *pRes = 0; return SQLITE_OK;
    }
  }
}

/* Find the last merged entry (for BtreeLast). */
static int mergeLast(BtCursor *pCur, int *pRes){
  pCur->mmIdx = pCur->pMutMap->nEntries - 1;
  pCur->mergeSrc = MERGE_SRC_TREE;

  for(;;){
    int treeOk = (pCur->pCur.eState==PROLLY_CURSOR_VALID);
    int mutOk  = (pCur->mmIdx >= 0 && pCur->mmIdx < pCur->pMutMap->nEntries);

    if( !treeOk && !mutOk ){ *pRes = 1; return SQLITE_OK; }

    if( !mutOk ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      *pRes = 0; return SQLITE_OK;
    }
    ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
    if( !treeOk ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx--; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      *pRes = 0; return SQLITE_OK;
    }
    int cmp = mergeCompare(pCur, e);
    if( cmp > 0 ){
      pCur->mergeSrc = MERGE_SRC_TREE;
      *pRes = 0; return SQLITE_OK;
    }else if( cmp < 0 ){
      if( e->op==PROLLY_EDIT_DELETE ){ pCur->mmIdx--; continue; }
      pCur->mergeSrc = MERGE_SRC_MUT;
      *pRes = 0; return SQLITE_OK;
    }else{
      if( e->op==PROLLY_EDIT_DELETE ){
        pCur->mmIdx--;
        prollyCursorPrev(&pCur->pCur);
        continue;
      }
      pCur->mergeSrc = MERGE_SRC_BOTH;
      *pRes = 0; return SQLITE_OK;
    }
  }
}

/* --------------------------------------------------------------------------
** Cursor navigation
** -------------------------------------------------------------------------- */

static int flushTablePending(BtCursor *pCur){
  struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( pTE && pTE->pPending ){
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    if( !prollyMutMapIsEmpty(pMap) ){
      ProllyMutator mut;
      int rc;
      memset(&mut, 0, sizeof(mut));
      mut.pStore = &pCur->pBt->store;
      mut.pCache = &pCur->pBt->cache;
      memcpy(&mut.oldRoot, &pTE->root, sizeof(ProllyHash));
      mut.pEdits = pMap;
      mut.flags = pTE->flags;
      rc = prollyMutateFlush(&mut);
      if( rc==SQLITE_OK ){
        memcpy(&pTE->root, &mut.newRoot, sizeof(ProllyHash));
      }
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

  /* Flush OTHER cursors' MutMaps but NOT our own — merge iteration
  ** handles our pending edits without tree rebuild. */
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

  /* Handle cursors invalidated by delete */
  if( pCur->eState==CURSOR_INVALID ){
    return SQLITE_DONE;
  }

  /* Restore cursor position if it was saved (e.g. by saveAllCursors during
  ** an insert on another cursor sharing the same ephemeral table). Without
  ** this, window functions crash because the prolly cursor's internal node
  ** pointers are NULL after a save. */
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
    /* If the MutMap acquired entries mid-scan (e.g. deferred deletes),
    ** activate merge iteration so we skip deleted entries and see inserts. */
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      ProllyMutMapIter it;
      if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                             prollyCursorIntKey(&pCur->pCur));
      }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pK; int nK;
        prollyCursorKey(&pCur->pCur, &pK, &nK);
        prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
      }else{
        prollyMutMapIterFirst(&it, pCur->pMutMap);
      }
      pCur->mmIdx = it.idx;
      pCur->mmActive = 1;
      pCur->mergeSrc = MERGE_SRC_TREE;
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
      if( pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        prollyMutMapIterSeek(&it, pCur->pMutMap, 0, 0,
                             prollyCursorIntKey(&pCur->pCur));
      }else if( !pCur->curIntKey && prollyCursorIsValid(&pCur->pCur) ){
        const u8 *pK; int nK;
        prollyCursorKey(&pCur->pCur, &pK, &nK);
        prollyMutMapIterSeek(&it, pCur->pMutMap, pK, nK, 0);
      }else{
        prollyMutMapIterLast(&it, pCur->pMutMap);
      }
      pCur->mmIdx = it.idx;
      pCur->mmActive = 1;
      pCur->mergeSrc = MERGE_SRC_TREE;
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

/* --------------------------------------------------------------------------
** Cursor seek
** -------------------------------------------------------------------------- */

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

  /*
  ** Optimization: check the MutMap first before flushing.
  ** If the key is in the MutMap as an INSERT, it exists — no flush needed.
  ** If the key is in the MutMap as a DELETE, it doesn't exist.
  ** If not in MutMap, search the existing tree (also no flush needed).
  ** Only flush when we have pending edits from OTHER cursors.
  */
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    ProllyMutMapEntry *pEntry = prollyMutMapFind(pCur->pMutMap, 0, 0, intKey);
    if( pEntry ){
      if( pEntry->op == PROLLY_EDIT_INSERT ){
        /* Key exists in pending inserts — exact match */
        *pRes = 0;
        pCur->eState = CURSOR_VALID;
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = intKey;
        /* Copy the MutMap value so it's safe after MutMap changes */
        if( pCur->cachedPayloadOwned && pCur->pCachedPayload ){
          sqlite3_free(pCur->pCachedPayload);
        }
        if( pEntry->nVal > 0 && pEntry->pVal ){
          pCur->pCachedPayload = sqlite3_malloc(pEntry->nVal);
          if( pCur->pCachedPayload ){
            memcpy(pCur->pCachedPayload, pEntry->pVal, pEntry->nVal);
            pCur->nCachedPayload = pEntry->nVal;
          } else {
            pCur->nCachedPayload = 0;
          }
        } else {
          CLEAR_CACHED_PAYLOAD(pCur);
        }
        pCur->cachedPayloadOwned = 1;
        /* Also position the prolly cursor for Next/Prev support.
        ** Without this, BtreeNext crashes on an unpositioned cursor
        ** when pPending edits are carried across savepoints. */
        refreshCursorRoot(pCur);
        {
          int seekRes = 0;
          rc = prollyCursorSeekInt(&pCur->pCur, intKey, &seekRes);
          if( rc!=SQLITE_OK ) return rc;
        }
        return SQLITE_OK;
      } else {
        /* Key is pending DELETE — doesn't exist */
        *pRes = 1;
        pCur->eState = CURSOR_INVALID;
        return SQLITE_OK;
      }
    }
    /* Not in MutMap — fall through to check the tree without flushing */
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

/*
** Serialize an UnpackedRecord to a SQLite record blob, then convert
** to sort key for prolly tree navigation.
*/
/*
** Compute the SQLite serial type for an in-memory value (Mem).
**
** The serial type encodes both the data type and its storage size in the
** record format. NULL=0, integers 1-6 use 1/2/3/4/6/8 bytes, float=7
** uses 8 bytes, constants 0 and 1 use types 8/9 with no payload,
** text = len*2+13, blob = len*2+12.
**
** Returns the serial type code and sets *pLen to the payload byte count.
*/
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

/*
** Serialize an UnpackedRecord into a standard SQLite record blob.
**
** The record format is: varint(header_size) followed by serial type varints
** for each field, then the concatenated field data. This is the same binary
** format used by SQLite's OP_MakeRecord. The resulting blob can be compared
** with sqlite3VdbeRecordCompare() or stored as a BLOBKEY table value.
**
** On success, *ppOut is set to a sqlite3_malloc'd buffer (caller frees)
** and *pnOut to its length. Returns SQLITE_OK or SQLITE_NOMEM.
*/
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
        /* no data */
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
        memcpy(&floatBits, &p->u.r, 8);
        int j;
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

static int prollyBtCursorIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  int rc;

  assert( !pCur->curIntKey );

  if( pCur->pBtree ) pCur->pBtree->nSeek++;

  CLEAR_CACHED_PAYLOAD(pCur);

  /* Flush pending mutations from other cursors on this table.
  ** For our own MutMap: flush only if the tree is empty (all data in
  ** MutMap — e.g. freshly populated auto-index).  When the tree already
  ** has data, our MutMap is checked inline during the leaf scan and via
  ** the MutMap seek section below, avoiding O(N^2) flushes during
  ** scan-based DELETE. */
  {
    BtCursor *p;
    for(p = pCur->pBt->pCursor; p; p = p->pNext){
      if( p->pgnoRoot==pCur->pgnoRoot
       && p->pMutMap && !prollyMutMapIsEmpty(p->pMutMap) ){
        if( p==pCur ){
          /* Self-flush only when tree is empty (no prior data to seek) */
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

  /*
  ** Parallel seek: O(log N) tree seek + O(log M) MutMap seek.
  ** Both the tree and MutMap are sorted by sort key.
  ** Compute sort key from search key, seek both, pick the best match.
  ** Verify with VdbeRecordCompare for eqSeen/default_rc correctness.
  */
  {
    int treeFound = 0, mutFound = 0;
    int treeCmp = 0, mutCmp = 0;
    const u8 *mutKey = 0;
    int mutNKey = 0;

    /* Serialize the UnpackedRecord to a blob, then compute sort key */
    u8 *pSerKey = 0;
    int nSerKey = 0;
    u8 *pSortKey = 0;
    int nSortKey = 0;
    rc = serializeUnpackedRecord(pIdxKey, &pSerKey, &nSerKey);
    if( rc!=SQLITE_OK ) return rc;
    rc = sortKeyFromRecord(pSerKey, nSerKey, &pSortKey, &nSortKey);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pSerKey);
      return rc;
    }

    /* ---- Tree seek: O(log N) sort key descent + bounded leaf scan ----
    ** Use sort key blob seek for O(log N) tree descent to the correct leaf.
    ** Then scan the leaf node's VALUES using VdbeRecordCompare.
    ** Leaf nodes are bounded in size (typically ~100 entries), so the
    ** leaf scan is O(K) where K is a small constant.
    ** If not found in the current leaf, check adjacent leaves (prev/next).
    ** Total: O(log N) + O(K) = O(log N). */
    rc = prollyCursorSeekBlob(&pCur->pCur, pSortKey, nSortKey, &(int){0});
    if( rc==SQLITE_OK && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      int iLevel = pCur->pCur.iLevel;
      ProllyCacheEntry *pLeaf = pCur->pCur.aLevel[iLevel].pEntry;
      int seekIdx = pCur->pCur.aLevel[iLevel].idx;
      int nItems = pLeaf->node.nItems;

      /* Binary search on sort key prefix to find matching range, then
      ** VdbeRecordCompare within that range. O(log L + K) where K is
      ** the number of entries matching the sort key prefix.
      ** Entries are sorted by sort key (memcmp). The search sort key
      ** may be a prefix of stored sort keys (fewer index fields). */
      int bestIdx = -1;
      int bestCmp = 0;
      {
        /* Phase 1: Binary search for first entry whose sort key prefix
        ** >= pSortKey. Uses memcmp on the shorter of the two keys. */
        int lo = 0, hi = nItems;
        while( lo < hi ){
          int mid = lo + (hi - lo) / 2;
          const u8 *pSK; int nSK;
          prollyNodeKey(&pLeaf->node, mid, &pSK, &nSK);
          int cmpLen = nSK < nSortKey ? nSK : nSortKey;
          int keyCmp = memcmp(pSK, pSortKey, cmpLen);
          if( keyCmp < 0 || (keyCmp==0 && nSK < nSortKey) ){
            lo = mid + 1;
          }else{
            hi = mid;
          }
        }

        /* Phase 2: Scan from 'lo' using VdbeRecordCompare. Stop when
        ** we pass the prefix range (sort key prefix no longer matches).
        ** The SQLite record is either stored as the prolly value (old
        ** format) or reconstructed from the sort key (new format). */
        u8 *pRecBuf = 0;  /* Temp buffer for reconstructed records */
        int i;
        for( i = lo; i < nItems; i++ ){
          const u8 *pSK; int nSK;
          prollyNodeKey(&pLeaf->node, i, &pSK, &nSK);
          /* Check if past prefix range */
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
          const u8 *pVal; int nVal;
          prollyNodeValue(&pLeaf->node, i, &pVal, &nVal);
          if( nVal==0 ){
            sqlite3_free(pRecBuf); pRecBuf = 0;
            recordFromSortKey(pSK, nSK, &pRecBuf, &nVal);
            pVal = pRecBuf;
          }
          pIdxKey->eqSeen = 0;
          int recCmp = sqlite3VdbeRecordCompare(nVal, pVal, pIdxKey);

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
        /* Exact match found in leaf */
        pCur->pCur.aLevel[iLevel].idx = bestIdx;
      } else if( bestIdx >= 0 ){
        /* No exact match but found a larger entry */
        pCur->pCur.aLevel[iLevel].idx = bestIdx;
        treeCmp = bestCmp;
        treeFound = 1;
      }

      /* The sort-key tree descent via prollyCursorSeekBlob lands on the
      ** correct leaf — adjacent leaf scanning is not needed.  Internal
      ** node boundary keys are the max key of each child, so the binary
      ** search at each level guarantees we reach the leaf containing the
      ** target key (or its immediate neighbor for range queries). */
    }

    /* ---- MutMap seek: O(log M) using prollyMutMapFind ---- */
    /* Only if tree didn't find exact match */
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap)
     && !(treeFound && treeCmp==0) ){
      /* Use sort key for exact match lookup in MutMap */
      ProllyMutMapEntry *mutE = prollyMutMapFind(
          pCur->pMutMap, pSortKey, nSortKey, 0);
      if( mutE && mutE->op==PROLLY_EDIT_INSERT ){
        /* MutMap value is the original SQLite record, or empty for
        ** BLOBKEY entries (reconstructed from sort key via #216). */
        const u8 *pMutVal = mutE->pVal;
        int nMutVal = mutE->nVal;
        u8 *pRecon = 0;
        if( nMutVal==0 ){
          recordFromSortKey(mutE->pKey, mutE->nKey, &pRecon, &nMutVal);
          pMutVal = pRecon;
        }
        if( pMutVal ){
          pIdxKey->eqSeen = 0;
          mutCmp = sqlite3VdbeRecordCompare(nMutVal, pMutVal, pIdxKey);
          if( mutCmp==0 || pIdxKey->eqSeen ){
            mutKey = pMutVal;
            mutNKey = nMutVal;
            mutFound = 1;
          }
        }
        if( !mutFound ) sqlite3_free(pRecon);
      }
    }
    sqlite3_free(pSerKey);
    sqlite3_free(pSortKey);

    /* ---- Pick best result ---- */
    if( mutFound && (!treeFound || treeCmp!=0) ){
      /* MutMap has exact match — cache reconstructed or original record */
      if( pCur->cachedPayloadOwned && pCur->pCachedPayload ){
        sqlite3_free(pCur->pCachedPayload);
      }
      if( mutNKey > 0 ){
        /* Check if mutKey is already an owned buffer (from recordFromSortKey) */
        u8 *pCopy = sqlite3_malloc(mutNKey);
        if( pCopy ){
          memcpy(pCopy, mutKey, mutNKey);
          pCur->pCachedPayload = pCopy;
          pCur->nCachedPayload = mutNKey;
        } else {
          pCur->nCachedPayload = 0;
          pCur->pCachedPayload = 0;
        }
      } else {
        pCur->pCachedPayload = 0;
        pCur->nCachedPayload = 0;
      }
      pCur->cachedPayloadOwned = 1;
      *pRes = mutCmp;
      pCur->eState = CURSOR_VALID;
      return SQLITE_OK;
    }
    if( treeFound ){
      *pRes = treeCmp;
      pCur->eState = CURSOR_VALID;
      return SQLITE_OK;
    }
  }

no_match:
  /* Ran off the end — all stored keys < search key.
  ** Position at the last entry if possible. */
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

/* --------------------------------------------------------------------------
** Cursor read
** -------------------------------------------------------------------------- */

static i64 prollyBtCursorIntegerKey(BtCursor *pCur){
  assert( pCur->eState==CURSOR_VALID );
  assert( pCur->curIntKey );
  /* Merge iteration: current entry may come from MutMap */
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

/*
** For INTKEY tables: payload = the value (data stored in leaf)
** For BLOBKEY (index) tables: payload = the key (entire record IS the key)
*/
static void getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData){
  /* If we have cached payload, return that. */
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    *ppData = pCur->pCachedPayload;
    *pnData = pCur->nCachedPayload;
    return;
  }

  /* Merge iteration: current entry may come from MutMap, not tree. */
  if( pCur->mmActive
   && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
    ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
    if( pCur->curIntKey ){
      /* INTKEY: MutMap value IS the payload */
      *ppData = e->pVal;
      *pnData = e->nVal;
    }else{
      /* BLOBKEY: reconstruct record from MutMap sort key */
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
    /* BLOBKEY: the prolly value may be empty (new format) or contain
    ** the original SQLite record (old format, backward compat).
    ** If empty, reconstruct losslessly from the sort key. */
    const u8 *pVal; int nVal;
    prollyCursorValue(&pCur->pCur, &pVal, &nVal);
    if( nVal > 0 ){
      *ppData = pVal;
      *pnData = nVal;
    }else{
      const u8 *pKey; int nKey;
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
      u8 *pRec = 0; int nRec = 0;
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

/* --------------------------------------------------------------------------
** Cursor write
** -------------------------------------------------------------------------- */

static int prollyBtCursorInsert(
  BtCursor *pCur,
  const BtreePayload *pPayload,
  int flags,
  int seekResult
){
  int rc;
  (void)seekResult;

  /*
  ** BTREE_PREFORMAT means the data was already inserted by
  ** sqlite3BtreeTransferRow() during OP_RowCell processing.
  ** In stock SQLite's page-based btree, PREFORMAT means the cell
  ** is already formatted in the page buffer and OP_Insert just
  ** finalizes it. In the prolly tree, TransferRow calls BtreeInsert
  ** directly, so the row is already stored. Skip the duplicate insert.
  */
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
    /* BLOBKEY (index): compute sort key for memcmp-sortable ordering.
    ** Store sort key as prolly key with empty value — the original
    ** SQLite record is reconstructed from the sort key on read via
    ** recordFromSortKey() (the encoding is lossless). */
    u8 *pSortKey = 0;
    int nSortKey = 0;
    rc = sortKeyFromRecord((const u8*)pPayload->pKey,
                           (int)pPayload->nKey,
                           &pSortKey, &nSortKey);
    if( rc==SQLITE_OK ){
      rc = prollyMutMapInsert(pCur->pMutMap,
                               pSortKey, nSortKey, 0,
                               NULL, 0);
      sqlite3_free(pSortKey);
    }
  }

  if( rc!=SQLITE_OK ) return rc;

  /* Defer flush for ALL non-master tables.  BtreeFirst/Last initialize
  ** merge iteration over the tree cursor + MutMap, and BtreeNext/Prev
  ** advance through both in sorted order.  This eliminates O(N^2)
  ** per-row flushes for ephemeral tables (CTEs, DISTINCT, window
  ** functions) and persistent tables alike. */
  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer ){
      if( (flags & BTREE_SAVEPOSITION) && pCur->curIntKey ){
        /* INTKEY SAVEPOSITION: cache the inserted data so cursor reads work.
        ** The cursor appears positioned on the newly inserted row. */
        ProllyMutMapEntry *pEntry = prollyMutMapFind(
            pCur->pMutMap, NULL, 0, pPayload->nKey);
        pCur->eState = CURSOR_VALID;
        pCur->curFlags |= BTCF_ValidNKey;
        pCur->cachedIntKey = pPayload->nKey;
        if( pCur->cachedPayloadOwned && pCur->pCachedPayload ){
          sqlite3_free(pCur->pCachedPayload);
        }
        if( pEntry && pEntry->nVal > 0 && pEntry->pVal ){
          pCur->pCachedPayload = sqlite3_malloc(pEntry->nVal);
          if( pCur->pCachedPayload ){
            memcpy(pCur->pCachedPayload, pEntry->pVal, pEntry->nVal);
            pCur->nCachedPayload = pEntry->nVal;
          } else {
            pCur->nCachedPayload = 0;
          }
        } else {
          CLEAR_CACHED_PAYLOAD(pCur);
        }
        pCur->cachedPayloadOwned = 1;
      } else {
        /* Non-SAVEPOSITION or BLOBKEY: cursor is not positioned */
        pCur->eState = CURSOR_INVALID;
      }
      return SQLITE_OK;
    }
  }

  /* Master table (pgnoRoot==1) and ephemeral tables: flush immediately */
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

/*
** Flush pending mutations for a cursor's table if needed.
** Called before any read operation that needs consistent tree data.
*/
static int flushIfNeeded(BtCursor *pCur){
  int rc;
  struct TableEntry *pTE;
  int anyFlushed = 0;
  int needFlush = 0;

  /* Check if any flush is needed before doing the expensive saveAllCursors */
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

  /* Save or invalidate ALL other cursors on this table before rebuilding
  ** the tree. Tree rebuild creates new nodes, invalidating any cursor's
  ** cached prolly node pointers. */
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
          /* Release any stale prolly node references */
          prollyCursorReleaseAll(&p->pCur);
        }
      }
    }
  }

  /* Flush THIS cursor's pending mutations */
  if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
    rc = flushMutMap(pCur);
    if( rc!=SQLITE_OK ) return rc;
    anyFlushed = 1;
  }

  /* Also flush any OTHER cursor on the same table with pending mutations */
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
    /* Reinitialize this cursor on the updated tree root */
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

/*
** Flush ALL cursors' pending mutations on a given table.
** Called before reads or when we need consistent data.
*/
static int flushAllPending(BtShared *pBt, Pgno iTable){
  BtCursor *p;
  int rc;

  /* Flush cursor-level MutMaps */
  for(p = pBt->pCursor; p; p = p->pNext){
    if( iTable==0 || p->pgnoRoot==iTable ){
      rc = flushIfNeeded(p);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  /* Also flush table-level deferred edits so data persists to disk.
  ** This is called by both BtreeCommitPhaseTwo (SQLite autocommit)
  ** and FlushAndSerializeCatalog (dolt functions). */
  rc = flushDeferredEdits(pBt);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

/* Flush table-level deferred edits. Called from FlushAndSerializeCatalog. */
static int flushDeferredEdits(BtShared *pBt){
  int rc = SQLITE_OK;
  if( pBt->db && pBt->db->nDb>0 && pBt->db->aDb[0].pBt ){
    Btree *pBtree = pBt->db->aDb[0].pBt;
    int i;
    for(i=0; i<pBtree->nTables; i++){
      struct TableEntry *pTE = &pBtree->aTables[i];
      if( pTE->pPending && !prollyMutMapIsEmpty((ProllyMutMap*)pTE->pPending) ){
        ProllyMutator mut;
        memset(&mut, 0, sizeof(mut));
        mut.pStore = &pBt->store;
        mut.pCache = &pBt->cache;
        memcpy(&mut.oldRoot, &pTE->root, sizeof(ProllyHash));
        mut.pEdits = (ProllyMutMap*)pTE->pPending;
        mut.flags = pTE->flags;
        rc = prollyMutateFlush(&mut);
        if( rc==SQLITE_OK ){
          memcpy(&pTE->root, &mut.newRoot, sizeof(ProllyHash));
        }
        prollyMutMapFree((ProllyMutMap*)pTE->pPending);
        sqlite3_free(pTE->pPending);
        pTE->pPending = 0;
        if( rc!=SQLITE_OK ) return rc;
      }
    }
  }
  return rc;
}

/*
** Handle the deferred-write path for BtreeDelete: the MutMap entry has
** already been marked as deleted, so we just update cursor state without
** flushing to disk.
*/
static int btreeDeleteDeferred(BtCursor *pCur, const u8 *pKey, int nKey, i64 iKey){
  (void)pKey; (void)nKey; (void)iKey;
  CLEAR_CACHED_PAYLOAD(pCur);
  pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
  return SQLITE_OK;
}

/*
** Handle the immediate-flush path for BtreeDelete: flush the MutMap to
** produce a new tree root, reinitialize the cursor, and optionally
** re-seek for BTREE_SAVEPOSITION.
*/
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

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  /* Restore or validate cursor position.  CURSOR_REQUIRESEEK means the
  ** position was saved and must be re-established.  CURSOR_SKIPNEXT means
  ** a deferred delete just happened and the cursor is still positioned.
  ** CURSOR_INVALID is allowed when the delete key is provided by the
  ** caller (OP_IdxDelete) — no cursor position needed. */
  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = restoreCursorPosition(pCur, 0);
    if( rc!=SQLITE_OK || pCur->eState!=CURSOR_VALID ) return rc;
  }else if( pCur->eState==CURSOR_SKIPNEXT ){
    pCur->eState = CURSOR_VALID;
    pCur->skipNext = 0;
  }else if( pCur->eState==CURSOR_INVALID ){
    /* Allow INVALID for index deletes where the key comes from registers,
    ** not from cursor position (OP_IdxDelete). The MutMap delete below
    ** uses the extracted key, not the cursor position. */
  }else if( pCur->eState!=CURSOR_VALID ){
    return SQLITE_CORRUPT_BKPT;
  }

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = saveAllCursors(pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  /* Extract key and record the MutMap deletion.
  ** In merge mode, the current entry may come from MutMap (not the tree
  ** cursor), so read the key from the correct source. */
  if( pCur->curIntKey ){
    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      iKey = pCur->pMutMap->aEntries[pCur->mmIdx].intKey;
    }else if( !prollyCursorIsValid(&pCur->pCur)
     && (pCur->curFlags & BTCF_ValidNKey) ){
      iKey = pCur->cachedIntKey;
    }else{
      iKey = prollyCursorIntKey(&pCur->pCur);
    }
    rc = prollyMutMapDelete(pCur->pMutMap, NULL, 0, iKey);
  } else {
    u8 *pDelSortKey = 0;
    int nDelSortKey = 0;

    if( pCur->mmActive
     && (pCur->mergeSrc==MERGE_SRC_MUT || pCur->mergeSrc==MERGE_SRC_BOTH) ){
      /* Key is the sort key from the MutMap entry */
      ProllyMutMapEntry *e = &pCur->pMutMap->aEntries[pCur->mmIdx];
      pKey = e->pKey;
      nKey = e->nKey;
    }else if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
      rc = sortKeyFromRecord(pCur->pCachedPayload, pCur->nCachedPayload,
                             &pDelSortKey, &nDelSortKey);
      if( rc!=SQLITE_OK ) return rc;
      pKey = pDelSortKey;
      nKey = nDelSortKey;
    } else {
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    }
    rc = prollyMutMapDelete(pCur->pMutMap, pKey, nKey, 0);
    sqlite3_free(pDelSortKey);
  }

  if( rc!=SQLITE_OK ) return rc;

  /* Dispatch to deferred or immediate path — must match BtreeInsert's
  ** canDefer logic so INSERT and DELETE use the same deferral strategy. */
  {
    int canDefer = (pCur->pgnoRoot > 1);
    if( canDefer ){
      rc = btreeDeleteDeferred(pCur, pKey, nKey, iKey);
      if( rc!=SQLITE_OK ) return rc;
      if( flags & BTREE_SAVEPOSITION ){
        /* Cursor is still positioned at the deleted entry in the tree
        ** (deletion is deferred in MutMap). CURSOR_SKIPNEXT with
        ** skipNext=0 causes the next BtreeNext to advance normally. */
        pCur->eState = CURSOR_SKIPNEXT;
        pCur->skipNext = 0;
      } else {
        pCur->eState = CURSOR_INVALID;
      }
      return SQLITE_OK;
    }
  }

  /* Immediate flush path */
  rc = btreeDeleteImmediate(pCur, pKey, nKey, iKey);
  if( rc!=SQLITE_OK ) return rc;

  if( flags & BTREE_SAVEPOSITION ){
    int res = 0;
    if( pCur->curIntKey ){
      rc = prollyCursorSeekInt(&pCur->pCur, iKey, &res);
    } else if( pKey && nKey > 0 ){
      /* Re-seek with the saved blob key */
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

/* --------------------------------------------------------------------------
** TransferRow
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** Shared cache (no-ops)
** -------------------------------------------------------------------------- */

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
/* Non-threadsafe build: provide a no-op so the vtable always has a valid
** function pointer matching prollyBtreeEnter above. */
static void prollyBtreeLeave(Btree *p){ (void)p; }
#endif

/* --------------------------------------------------------------------------
** TripAllCursors
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** ClearCursor / ClearCache
** -------------------------------------------------------------------------- */

static void prollyBtCursorClearCursor(BtCursor *pCur){
  if( pCur->pKey ){
    sqlite3_free(pCur->pKey);
    pCur->pKey = 0;
    pCur->nKey = 0;
  }
  CLEAR_CACHED_PAYLOAD(pCur);
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

/* --------------------------------------------------------------------------
** Pager
** -------------------------------------------------------------------------- */

static struct Pager *prollyBtreePager(Btree *p){
  return (struct Pager*)(p->pBt->pPagerShim);
}
struct Pager *sqlite3BtreePager(Btree *p){
  if( !p ) return 0;
  return p->pOps->xPager(p);
}

/* --------------------------------------------------------------------------
** Count / RowCountEst
** -------------------------------------------------------------------------- */

static int prollyBtCursorCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  (void)db;
  /* Flush any deferred edits so the count reflects pending inserts/deletes */
  flushTablePending(pCur);
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
  /* Default estimate for the query planner */
  return 1000000;
}
i64 sqlite3BtreeRowCountEst(BtCursor *pCur){
  return pCur->pCurOps->xRowCountEst(pCur);
}

/* --------------------------------------------------------------------------
** SetVersion / HeaderSize
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** IntegrityCheck
** -------------------------------------------------------------------------- */

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
  int i;
  int nErr = 0;

  (void)db;
  (void)aCnt;
  (void)mxErr;

  if( !p || !p->pBt ){
    if( pnErr ) *pnErr = 0;
    if( pzOut ) *pzOut = 0;
    return SQLITE_OK;
  }
  pBt = p->pBt;

  for(i=0; i<nRoot; i++){
    /* Set row count to 0 for each table (we don't count precisely) */
    if( aCnt ){
      sqlite3VdbeMemSetInt64(&aCnt[i], 0);
    }
    if( nErr>=mxErr ) continue;
    {
      struct TableEntry *pTE = findTable(p, aRoot[i]);
      if( !pTE ) continue;
      if( !prollyHashIsEmpty(&pTE->root) ){
        if( !chunkStoreHas(&pBt->store, &pTE->root) ){
          nErr++;
        }
      }
    }
  }

  if( pnErr ) *pnErr = nErr;
  if( pzOut ) *pzOut = 0;

  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** CursorPin / CursorUnpin
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** CursorHintFlags / CursorHasHint
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** FakeValidCursor
** -------------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------------
** CopyFile
** -------------------------------------------------------------------------- */

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
  pTo->root = pFrom->root;
  pTo->committedRoot = pFrom->committedRoot;
  pTo->iNextTable = pFrom->iNextTable;

  pTo->iBDataVersion++;
  if( pBtTo->pPagerShim ){
    pBtTo->pPagerShim->iDataVersion++;
  }

  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** IsInBackup
** -------------------------------------------------------------------------- */

int sqlite3BtreeIsInBackup(Btree *p){
  return p->nBackup > 0;
}

/* --------------------------------------------------------------------------
** Checkpoint (WAL compat no-op)
** -------------------------------------------------------------------------- */

#ifndef SQLITE_OMIT_WAL
int sqlite3BtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  (void)p; (void)eMode;
  if( pnLog ) *pnLog = 0;
  if( pnCkpt ) *pnCkpt = 0;
  return SQLITE_OK;
}
#endif

/* --------------------------------------------------------------------------
** Incremental blob
** -------------------------------------------------------------------------- */

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

#endif /* SQLITE_OMIT_INCRBLOB */

/* --------------------------------------------------------------------------
** Debug / Test
** -------------------------------------------------------------------------- */

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
#endif /* SQLITE_TEST */

/*
** Register doltite_engine() SQL function for runtime engine detection.
**   SELECT doltite_engine();  -->  'prolly'
*/
static void doltiteEngineFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  (void)argc; (void)argv;
  sqlite3_result_text(context, "prolly", -1, SQLITE_STATIC);
}

/*
** Get the ChunkStore for a database connection.
** Used by doltlite_commit.c and doltlite_log.c.
*/
ChunkStore *doltliteGetChunkStore(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *pBt = db->aDb[0].pBt;
    return &pBt->pBt->store;
  }
  return 0;
}

/*
** Get the BtShared for a database connection.
** Used by doltlite_commit.c for flushing and catalog serialization.
*/
BtShared *doltliteGetBtShared(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    return db->aDb[0].pBt->pBt;
  }
  return 0;
}

/*
** Flush all pending mutations and serialize catalog.
** Called by dolt_commit before snapshotting state.
*/
/*
** Compute schemaHash for each table by querying sqlite_master.
** Must be called from SQL function context (NOT from BtreeCommit).
*/
void doltliteUpdateSchemaHashes(sqlite3 *db){
  Btree *pBtree;
  int i;
  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return;
  pBtree = db->aDb[0].pBt;
  for(i=0; i<pBtree->nTables; i++){
    if( pBtree->aTables[i].iTable>1 && pBtree->aTables[i].zName ){
      sqlite3_stmt *pStmt = 0;
      char *zSql = sqlite3_mprintf(
        "SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name='%q'",
        pBtree->aTables[i].zName);
      if( zSql ){
        if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK ){
          if( sqlite3_step(pStmt)==SQLITE_ROW ){
            const char *zCreate = (const char*)sqlite3_column_text(pStmt, 0);
            if( zCreate ){
              prollyHashCompute(zCreate, (int)strlen(zCreate),
                                &pBtree->aTables[i].schemaHash);
            }
          }
          sqlite3_finalize(pStmt);
        }
        sqlite3_free(zSql);
      }
    }
  }
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
  /* Flush table-level deferred edits (pPending from closed cursors) */
  rc = flushDeferredEdits(pBt);
  if( rc!=SQLITE_OK ) return rc;
  /* Update schema hashes */
  doltliteUpdateSchemaHashes(db);
  return serializeCatalog(pBtree, ppOut, pnOut);
}

/*
** Load a catalog snapshot from a catalog hash.
** Returns an array of TableEntry and count. Caller must sqlite3_free(*ppTables).
*/
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

/*
** Get the HEAD commit's catalog hash.
*/
int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  u8 *data = 0;
  int nData = 0;
  int rc;
  DoltliteCommit commit;

  if( !cs ) return SQLITE_ERROR;
  /* Use session HEAD, not shared store */
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

/*
** Resolve a table name to its rootpage (iTable number).
*/
int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable){
  sqlite3_stmt *pStmt = 0;
  int rc;
  rc = sqlite3_prepare_v2(db,
    "SELECT rootpage FROM sqlite_master WHERE type='table' AND name=?",
    -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_bind_text(pStmt, 1, zTable, -1, SQLITE_STATIC);
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    *piTable = (Pgno)sqlite3_column_int(pStmt, 0);
    rc = SQLITE_OK;
  }else{
    rc = SQLITE_ERROR;
  }
  sqlite3_finalize(pStmt);
  return rc;
}

/*
** Resolve a rootpage (iTable) to its table name by scanning the in-memory
** schema hash table directly. This avoids calling sqlite3_prepare_v2 which
** would trigger the full SQL parser and query planner, causing stack overflow
** when called from serializeCatalog during BtreeCommitPhaseTwo.
** Returns sqlite3_malloc'd string. Caller must sqlite3_free.
*/
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

/*
** Hard reset: reload a catalog into the live BtShared table registry.
** This replaces the working state with the given catalog's state.
** Also invalidates all cursors and clears the schema cache.
*/
int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash){
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

  /* Invalidate all cursors first */
  invalidateCursors(pBt, 0, SQLITE_ABORT);

  /* Replace table registry */
  sqlite3_free(pBtree->aTables);
  pBtree->aTables = 0;
  pBtree->nTables = 0;
  pBtree->nTablesAlloc = 0;

  rc = deserializeCatalog(pBtree, data, nData);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  /* Bump the schema version so SQLite detects the change and re-reads
  ** sqlite_master on the next statement. Also bump data version. */
  pBtree->aMeta[BTREE_SCHEMA_VERSION]++;
  pBtree->iBDataVersion++;
  if( pBt->pPagerShim ){
    pBt->pPagerShim->iDataVersion++;
  }

  /* Use SQLite's proper schema reset to clear all cached schema objects.
  ** This ensures db->aDb[0].pSchema is properly cleaned up, not just
  ** the Btree's copy. */
  if( pBtree->db ){
    sqlite3ResetAllSchemasOfConnection(pBtree->db);
  }else{
    invalidateSchema(pBtree);
  }

  /* Hard reset: staged = target catalog (merge state is caller's responsibility) */
  memcpy(&pBtree->stagedCatalog, catHash, sizeof(ProllyHash));

  /* Persist the new working state */
  chunkStoreSetCatalog(cs, catHash);
  rc = chunkStoreCommit(cs);
  return rc;
}

/*
** Per-session branch accessors for dolt functions.
*/
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

/* Per-session merge state accessors */
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

/*
** Save session working state to a per-branch WorkingSet chunk.
** Format: version(1) + staged(20) + isMerging(1) + mergeCommit(20) + conflicts(20)
*/
int doltliteSaveWorkingSet(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  u8 buf[WS_TOTAL_SIZE];
  ProllyHash wsHash;
  const char *zBranch;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  zBranch = pBtree->zBranch ? pBtree->zBranch : "main";

  buf[0] = 1;  /* WorkingSet version */
  memcpy(buf + WS_STAGED_OFF, pBtree->stagedCatalog.data, PROLLY_HASH_SIZE);
  buf[WS_MERGING_OFF] = pBtree->isMerging;
  memcpy(buf + WS_MERGE_COMMIT_OFF, pBtree->mergeCommitHash.data, PROLLY_HASH_SIZE);
  memcpy(buf + WS_CONFLICTS_OFF, pBtree->conflictsCatalogHash.data, PROLLY_HASH_SIZE);

  rc = chunkStorePut(cs, buf, WS_TOTAL_SIZE, &wsHash);
  if( rc != SQLITE_OK ) return rc;

  /* Store the WorkingSet hash in the branch ref */
  return chunkStoreSetBranchWorkingSet(cs, zBranch, &wsHash);
}

/*
** Load a branch's WorkingSet into the session state.
*/
int doltliteLoadWorkingSet(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  Btree *pBtree;
  ProllyHash wsHash;
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !db || db->nDb<=0 || !db->aDb[0].pBt ) return SQLITE_ERROR;
  pBtree = db->aDb[0].pBt;
  if( !cs ) return SQLITE_ERROR;

  rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc != SQLITE_OK || prollyHashIsEmpty(&wsHash) ){
    /* No saved WorkingSet — clear to defaults */
    memset(&pBtree->stagedCatalog, 0, sizeof(ProllyHash));
    pBtree->isMerging = 0;
    memset(&pBtree->mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&pBtree->conflictsCatalogHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, &wsHash, &data, &nData);
  if( rc != SQLITE_OK || nData < WS_TOTAL_SIZE ){
    sqlite3_free(data);
    memset(&pBtree->stagedCatalog, 0, sizeof(ProllyHash));
    pBtree->isMerging = 0;
    memset(&pBtree->mergeCommitHash, 0, sizeof(ProllyHash));
    memset(&pBtree->conflictsCatalogHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  memcpy(pBtree->stagedCatalog.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  pBtree->isMerging = data[WS_MERGING_OFF];
  memcpy(pBtree->mergeCommitHash.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
  memcpy(pBtree->conflictsCatalogHash.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);

  sqlite3_free(data);
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** origBtreeXxxVt wrapper implementations.
**
** Each wrapper extracts p->pOrigBtree and calls the corresponding
** origBtreeXxx() function from btree_orig_api.h.
** -------------------------------------------------------------------------- */

static int origBtreeCloseVt(Btree *p){
  int rc = origBtreeClose(p->pOrigBtree);
  p->pOrigBtree = 0;
  /* Free the Btree shell itself */
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

/* --------------------------------------------------------------------------
** BtCursorOps origCursorVtOps wrappers.
**
** Each wrapper extracts pCur->pOrigCursor and calls the corresponding
** origBtreeXxx() function from btree_orig_api.h.
** -------------------------------------------------------------------------- */
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
  /* orig cursors: no-op, original ClearCursor had early return */
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
  /* orig cursors: no-op, original had early return */
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
  /* orig cursors: no-op, original had early return */
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

/* External registration for all dolt features */
extern void doltliteRegister(sqlite3 *db);

static void registerDoltiteFunctions(sqlite3 *db){
  sqlite3_create_function(db, "doltlite_engine", 0, SQLITE_UTF8, 0,
                          doltiteEngineFunc, 0, 0);
  doltliteRegister(db);
}

#endif /* DOLTLITE_PROLLY */
