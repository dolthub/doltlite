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
#include "vdbeInt.h"

#include <string.h>
#include <assert.h>

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
    int nTables;
    Pgno iNextTable;
  } *aSavepointTables;
  int nSavepointTablesAlloc;

  /* Committed table registry for transaction rollback */
  struct TableEntry *aCommittedTables;
  int nCommittedTables;
  Pgno iCommittedNextTable;

  /* Per-session branch state. */
  char *zBranch;             /* Current branch name (owned, NULL = "main") */
  ProllyHash headCommit;     /* This session's HEAD commit hash */
  ProllyHash stagedCatalog;  /* This session's staged catalog hash */
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

  /* Save/restore state */
  i64 nKey;                  /* Saved integer key or blob key length */
  void *pKey;                /* Saved blob key (malloc'd) */
  u64 nSeek;                 /* Debug seek counter (per-cursor) */
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

/* --------------------------------------------------------------------------
** Internal helper implementations
** -------------------------------------------------------------------------- */

/*
** Find a table entry by table number in the BtShared table registry.
** Returns a pointer to the entry, or NULL if not found.
*/
static struct TableEntry *findTable(Btree *pBtree, Pgno iTable){
  int i;
  for(i=0; i<pBtree->nTables; i++){
    if( pBtree->aTables[i].iTable==iTable ){
      return &pBtree->aTables[i];
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

  pEntry = &pBtree->aTables[pBtree->nTables];
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
static int serializeCatalog(Btree *pBtree, u8 **ppOut, int *pnOut){
  int nTables = pBtree->nTables;
  int sz = 4 + 4 + 64;  /* header */
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
    sz += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2 + nLen;
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  q = buf;
  /* iNextTable */
  q[0]=(u8)(pBtree->iNextTable); q[1]=(u8)(pBtree->iNextTable>>8);
  q[2]=(u8)(pBtree->iNextTable>>16); q[3]=(u8)(pBtree->iNextTable>>24);
  q += 4;
  /* nTables */
  q[0]=(u8)nTables; q[1]=(u8)(nTables>>8);
  q[2]=(u8)(nTables>>16); q[3]=(u8)(nTables>>24);
  q += 4;
  /* meta values */
  for(i=0; i<16; i++){
    u32 v = pBtree->aMeta[i];
    q[0]=(u8)v; q[1]=(u8)(v>>8); q[2]=(u8)(v>>16); q[3]=(u8)(v>>24);
    q += 4;
  }
  /* table entries: iTable(4) + flags(1) + root(20) + schemaHash(20) + name_len(2) + name(var) */
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
** Deserialize catalog chunk into the table registry + meta values.
*/
static int deserializeCatalog(Btree *pBtree, const u8 *data, int nData){
  const u8 *q = data;
  int nTables, i;
  if( nData < 72 ) return SQLITE_CORRUPT; /* minimum: 4+4+64 */
  /* iNextTable */
  pBtree->iNextTable = (Pgno)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  q += 4;
  /* nTables */
  nTables = (int)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  q += 4;
  /* meta values */
  for(i=0; i<16; i++){
    pBtree->aMeta[i] = (u32)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
    q += 4;
  }
  /* table entries */
  sqlite3_free(pBtree->aTables);
  pBtree->aTables = 0;
  pBtree->nTables = 0;
  pBtree->nTablesAlloc = 0;
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
    /* schemaHash(20) */
    if( q + PROLLY_HASH_SIZE <= data+nData ){
      memcpy(pTE->schemaHash.data, q, PROLLY_HASH_SIZE);
      q += PROLLY_HASH_SIZE;
    }
    /* name_len(2) + name(var) */
    if( q+2 <= data+nData ){
      nLen = q[0] | (q[1]<<8); q += 2;
      if( nLen>0 && q+nLen<=data+nData ){
        pTE->zName = sqlite3_malloc(nLen+1);
        if( pTE->zName ){
          memcpy(pTE->zName, q, nLen);
          pTE->zName[nLen] = 0;
        }
        q += nLen;
      }
    }
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
static int pushSavepoint(Btree *pBtree){
  struct SavepointTableState *pState;
  int rc;

  /* Flush all pending mutations (both cursor-level and table-level deferred
  ** edits) so that the table-entry root hashes reflect the current state.
  ** Without this, data inserted before the savepoint but not yet flushed
  ** would be lost on ROLLBACK TO because the snapshot would capture stale
  ** root hashes that don't include the pending edits. */
  rc = flushAllPending(pBtree->pBt, 0);
  if( rc!=SQLITE_OK ) return rc;

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

  /* Also snapshot the table registry */
  pState = &pBtree->aSavepointTables[pBtree->nSavepoint];
  pState->aTables = 0;
  pState->nTables = 0;
  pState->iNextTable = pBtree->iNextTable;
  if( pBtree->nTables > 0 ){
    pState->aTables = sqlite3_malloc(pBtree->nTables * (int)sizeof(struct TableEntry));
    if( !pState->aTables ) return SQLITE_NOMEM;
    memcpy(pState->aTables, pBtree->aTables,
           pBtree->nTables * sizeof(struct TableEntry));
    pState->nTables = pBtree->nTables;
    /* Clear pPending pointers in the snapshot — the flush above ensures
    ** all edits are reflected in the root hashes.  Leaving stale pointers
    ** in the snapshot would risk double-free on rollback. */
    {
      int k;
      for(k=0; k<pState->nTables; k++){
        pState->aTables[k].pPending = 0;
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
    chunkStoreGetStagedCatalog(&pBt->store, &p->stagedCatalog);
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
int sqlite3BtreeClose(Btree *p){
  BtShared *pBt;

  if( !p ) return SQLITE_OK;
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
      sqlite3_free(p->aSavepointTables[i].aTables);
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
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Initialize a fresh (empty) database with default meta values.
*/
int sqlite3BtreeNewDb(Btree *p){
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

/* --------------------------------------------------------------------------
** Configuration (mostly no-ops)
** -------------------------------------------------------------------------- */

int sqlite3BtreeSetCacheSize(Btree *p, int mxPage){
  (void)p; (void)mxPage;
  return SQLITE_OK;
}

int sqlite3BtreeSetSpillSize(Btree *p, int mxPage){
  (void)p; (void)mxPage;
  return SQLITE_OK;
}

#if SQLITE_MAX_MMAP_SIZE>0
int sqlite3BtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  (void)p; (void)szMmap;
  return SQLITE_OK;
}
#endif

int sqlite3BtreeSetPagerFlags(Btree *p, unsigned pgFlags){
  (void)p; (void)pgFlags;
  return SQLITE_OK;
}

int sqlite3BtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix){
  (void)nReserve; (void)eFix;
  if( nPagesize>=512 && nPagesize<=65536 ){
    p->pBt->pageSize = (u32)nPagesize;
  }
  return SQLITE_OK;
}

int sqlite3BtreeGetPageSize(Btree *p){
  return (int)p->pBt->pageSize;
}

Pgno sqlite3BtreeMaxPageCount(Btree *p, Pgno mxPage){
  (void)p; (void)mxPage;
  return (Pgno)0x7FFFFFFF;
}

Pgno sqlite3BtreeLastPage(Btree *p){
  /* Must be >= iNextTable so rootpage validation in prepare.c passes */
  return p->iNextTable + 1000;
}

int sqlite3BtreeSecureDelete(Btree *p, int newFlag){
  (void)p; (void)newFlag;
  return 0;
}

int sqlite3BtreeGetRequestedReserve(Btree *p){
  (void)p;
  return 0;
}

int sqlite3BtreeGetReserveNoMutex(Btree *p){
  (void)p;
  return 0;
}

int sqlite3BtreeSetAutoVacuum(Btree *p, int autoVacuum){
  (void)p; (void)autoVacuum;
  return SQLITE_OK;
}

int sqlite3BtreeGetAutoVacuum(Btree *p){
  (void)p;
  return BTREE_AUTOVACUUM_NONE;
}

int sqlite3BtreeIncrVacuum(Btree *p){
  (void)p;
  return SQLITE_DONE;
}

const char *sqlite3BtreeGetFilename(Btree *p){
  return chunkStoreFilename(&p->pBt->store);
}

const char *sqlite3BtreeGetJournalname(Btree *p){
  (void)p;
  return "";
}

int sqlite3BtreeIsReadonly(Btree *p){
  return (p->pBt->btsFlags & BTS_READ_ONLY) ? 1 : 0;
}

/* --------------------------------------------------------------------------
** Transactions
** -------------------------------------------------------------------------- */

int sqlite3BtreeBeginTrans(Btree *p, int wrFlag, int *pSchemaVersion){
  BtShared *pBt = p->pBt;

  if( pSchemaVersion ){
    *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
  }

  if( p->inTrans==TRANS_WRITE ){
    return SQLITE_OK;
  }

  /* Detect if another connection replaced the database file */
  {
    int bChanged = 0;
    int rc = chunkStoreRefreshIfChanged(&pBt->store, &bChanged);
    if( rc!=SQLITE_OK ) return rc;
    if( bChanged ){
      /* After refresh, decide whether the manifest state belongs to THIS
      ** connection's branch.  The manifest headCommit is updated only by
      ** dolt_commit / dolt_checkout.  If it matches our branch HEAD, the
      ** manifest catalog/root may include uncommitted working changes and
      ** we should use them.  If it doesn't match, a different branch
      ** committed and we must reload from our branch HEAD commit. */
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
      if( pSchemaVersion ){
        *pSchemaVersion = (int)p->aMeta[BTREE_SCHEMA_VERSION];
      }
    }
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

  return SQLITE_OK;
}

int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  (void)p; (void)zSuperJrnl;
  return SQLITE_OK;
}

int sqlite3BtreeCommitPhaseTwo(Btree *p, int bCleanup){
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

  return rc;
}

int sqlite3BtreeCommit(Btree *p){
  int rc;
  rc = sqlite3BtreeCommitPhaseOne(p, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3BtreeCommitPhaseTwo(p, 0);
  }
  return rc;
}

int sqlite3BtreeRollback(Btree *p, int tripCode, int writeOnly){
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

  return SQLITE_OK;
}

int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
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

int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint){
  BtShared *pBt;

  if( p==0 ) return SQLITE_OK;
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
      /* Restore table registry */
      if( pState->aTables ){
        /* Free pPending maps on current entries — these hold post-savepoint
        ** edits that are being discarded by the rollback. */
        {
          int k;
          for(k=0; k<p->nTables; k++){
            if( p->aTables[k].pPending ){
              prollyMutMapFree((ProllyMutMap*)p->aTables[k].pPending);
              sqlite3_free(p->aTables[k].pPending);
            }
          }
        }
        sqlite3_free(p->aTables);
        p->aTables = pState->aTables;
        p->nTables = pState->nTables;
        p->nTablesAlloc = pState->nTables;
        p->iNextTable = pState->iNextTable;
        pState->aTables = 0; /* Ownership transferred */
      }
      /* Free savepoints above this one */
      {
        int j;
        for(j=iSavepoint+1; j<p->nSavepoint; j++){
          sqlite3_free(p->aSavepointTables[j].aTables);
        }
      }
      p->nSavepoint = iSavepoint;
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    } else if( iSavepoint>=0 && iSavepoint>=p->nSavepoint ){
      if( p->aCommittedTables ){
        sqlite3_free(p->aTables);
        p->aTables = sqlite3_malloc(
            p->nCommittedTables * (int)sizeof(struct TableEntry));
        if( p->aTables ){
          memcpy(p->aTables, p->aCommittedTables,
                 p->nCommittedTables * sizeof(struct TableEntry));
          p->nTables = p->nCommittedTables;
          p->nTablesAlloc = p->nCommittedTables;
          p->iNextTable = p->iCommittedNextTable;
        } else {
          p->nTables = 0;
          p->nTablesAlloc = 0;
        }
      }
      p->root = p->committedRoot;
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    } else if( iSavepoint<0 ){
      int j;
      for(j=0; j<p->nSavepoint; j++){
        sqlite3_free(p->aSavepointTables[j].aTables);
      }
      p->root = p->committedRoot;
      if( p->aCommittedTables ){
        sqlite3_free(p->aTables);
        p->aTables = sqlite3_malloc(p->nCommittedTables * (int)sizeof(struct TableEntry));
        if( p->aTables ){
          memcpy(p->aTables, p->aCommittedTables,
                 p->nCommittedTables * sizeof(struct TableEntry));
          p->nTables = p->nCommittedTables;
          p->nTablesAlloc = p->nCommittedTables;
          p->iNextTable = p->iCommittedNextTable;
        } else {
          p->nTables = 0;
          p->nTablesAlloc = 0;
        }
      }
      p->nSavepoint = 0;
      invalidateCursors(pBt, 0, SQLITE_ABORT);
      invalidateSchema(p);
    }
  } else {
    /* SAVEPOINT_RELEASE: free savepoints above this one */
    if( iSavepoint>=0 && iSavepoint<p->nSavepoint ){
      int j;
      for(j=iSavepoint; j<p->nSavepoint; j++){
        sqlite3_free(p->aSavepointTables[j].aTables);
      }
      p->nSavepoint = iSavepoint;
    }
  }

  return SQLITE_OK;
}

int sqlite3BtreeTxnState(Btree *p){
  return p ? (int)p->inTrans : TRANS_NONE;
}

/* --------------------------------------------------------------------------
** Table operations
** -------------------------------------------------------------------------- */

int sqlite3BtreeCreateTable(Btree *p, Pgno *piTable, int flags){
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

int sqlite3BtreeDropTable(Btree *p, int iTable, int *piMoved){
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

int sqlite3BtreeClearTable(Btree *p, int iTable, i64 *pnChange){
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

int sqlite3BtreeClearTableOfCursor(BtCursor *pCur){
  return sqlite3BtreeClearTable(pCur->pBtree, (int)pCur->pgnoRoot, 0);
}

/* --------------------------------------------------------------------------
** Meta values
** -------------------------------------------------------------------------- */

void sqlite3BtreeGetMeta(Btree *p, int idx, u32 *pValue){
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

int sqlite3BtreeUpdateMeta(Btree *p, int idx, u32 value){
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

/* --------------------------------------------------------------------------
** Schema
** -------------------------------------------------------------------------- */

void *sqlite3BtreeSchema(Btree *p, int nBytes, void (*xFree)(void*)){
  if( !p->pSchema && nBytes>0 ){
    p->pSchema = sqlite3_malloc(nBytes);
    if( p->pSchema ){
      memset(p->pSchema, 0, nBytes);
      p->xFreeSchema = xFree;
    }
  }
  return p->pSchema;
}

int sqlite3BtreeSchemaLocked(Btree *p){
  (void)p;
  return 0;
}

#ifndef SQLITE_OMIT_SHARED_CACHE
int sqlite3BtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  (void)p; (void)iTab; (void)isWriteLock;
  return SQLITE_OK;
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
}

int sqlite3BtreeCursor(
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

  /* Piece 2: pick up deferred edits from a previous cursor on this table */
  if( pTE->pPending ){
    pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
    pTE->pPending = 0;
  }

  prollyCursorInit(&pCur->pCur, &pBt->store, &pBt->cache,
                    &pTE->root, pTE->flags);

  pCur->pNext = pBt->pCursor;
  pBt->pCursor = pCur;

  return SQLITE_OK;
}

int sqlite3BtreeCloseCursor(BtCursor *pCur){
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

int sqlite3BtreeCursorHasMoved(BtCursor *pCur){
  return (pCur->eState!=CURSOR_VALID);
}

int sqlite3BtreeCursorRestore(BtCursor *pCur, int *pDifferentRow){
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

#ifdef SQLITE_DEBUG
int sqlite3BtreeClosesWithCursor(Btree *p, BtCursor *pCur){
  BtCursor *pX;
  if( !p || !p->pBt ) return 0;
  for(pX=p->pBt->pCursor; pX; pX=pX->pNext){
    if( pX==pCur ) return 1;
  }
  return 0;
}
#endif

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

int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  rc = flushTablePending(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = flushIfNeeded(pCur);
  if( rc!=SQLITE_OK ) return rc;
  refreshCursorRoot(pCur);
  rc = prollyCursorFirst(&pCur->pCur, pRes);
  if( rc==SQLITE_OK ){
    pCur->eState = (*pRes==0) ? CURSOR_VALID : CURSOR_INVALID;
    pCur->curFlags &= ~BTCF_AtLast;
  }
  return rc;
}

int sqlite3BtreeLast(BtCursor *pCur, int *pRes){
  int rc;
  CLEAR_CACHED_PAYLOAD(pCur);
  rc = flushTablePending(pCur);
  if( rc!=SQLITE_OK ) return rc;
  rc = flushIfNeeded(pCur);
  if( rc!=SQLITE_OK ) return rc;
  refreshCursorRoot(pCur);
  rc = prollyCursorLast(&pCur->pCur, pRes);
  if( rc==SQLITE_OK ){
    if( *pRes==0 ){
      pCur->eState = CURSOR_VALID;
      pCur->curFlags |= BTCF_AtLast;
    } else {
      pCur->eState = CURSOR_INVALID;
    }
  }
  return rc;
}

int sqlite3BtreeNext(BtCursor *pCur, int flags){
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

  rc = prollyCursorNext(&pCur->pCur);
  if( rc==SQLITE_OK ){
    if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      pCur->eState = CURSOR_VALID;
    } else {
      pCur->eState = CURSOR_INVALID;
      return SQLITE_DONE;
    }
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey);
  return rc;
}

int sqlite3BtreePrevious(BtCursor *pCur, int flags){
  int rc;
  (void)flags;
  CLEAR_CACHED_PAYLOAD(pCur);

  if( pCur->eState==CURSOR_INVALID ){
    return SQLITE_DONE;
  }

  /* Restore cursor position if it was saved. See sqlite3BtreeNext for the
  ** full explanation — this is the symmetric fix for backward traversal. */
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

  rc = prollyCursorPrev(&pCur->pCur);
  if( rc==SQLITE_OK ){
    if( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      pCur->eState = CURSOR_VALID;
    } else {
      pCur->eState = CURSOR_INVALID;
      return SQLITE_DONE;
    }
  }
  pCur->curFlags &= ~(BTCF_AtLast|BTCF_ValidNKey);
  return rc;
}

int sqlite3BtreeEof(BtCursor *pCur){
  return (pCur->eState!=CURSOR_VALID);
}

int sqlite3BtreeIsEmpty(BtCursor *pCur, int *pRes){
  struct TableEntry *pTE;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE ){
    *pRes = 1;
  } else {
    *pRes = prollyHashIsEmpty(&pTE->root) ? 1 : 0;
  }
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Cursor seek
** -------------------------------------------------------------------------- */

int sqlite3BtreeTableMoveto(
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

/*
** Serialize an UnpackedRecord to a blob for use with compareBlobKeys-based
** tree navigation. Produces a standard SQLite record: varint header size,
** varint serial types, then field data.
*/
/*
** Compute the serial type for a Mem value (same logic as sqlite3VdbeSerialType).
** Returns the serial type and sets *pLen to the data length.
*/
static u32 btreeSerialType(Mem *pMem, u32 *pLen){
  int flags = pMem->flags;
  if( flags & MEM_Null ){ *pLen = 0; return 0; }
  if( flags & MEM_Int ){
    i64 v = pMem->u.i;
    if( v==0 ){ *pLen = 0; return 8; }
    if( v==1 ){ *pLen = 0; return 9; }
    if( v>=-128 && v<=127 ){ *pLen = 1; return 1; }
    if( v>=-32768 && v<=32767 ){ *pLen = 2; return 2; }
    if( v>=-8388608 && v<=8388607 ){ *pLen = 3; return 3; }
    if( v>=-2147483648LL && v<=2147483647LL ){ *pLen = 4; return 4; }
    if( v>=-140737488355328LL && v<=140737488355327LL ){ *pLen = 6; return 5; }
    *pLen = 8; return 6;
  }
  if( flags & MEM_Real ){ *pLen = 8; return 7; }
  if( flags & MEM_Str ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + 13;
  }
  if( flags & MEM_Blob ){
    u32 n = (u32)pMem->n;
    *pLen = n;
    return n*2 + 12;
  }
  *pLen = 0; return 0;
}

/*
** Serialize an UnpackedRecord to a blob for use with compareBlobKeys-based
** tree navigation. Produces a standard SQLite record.
*/
static int serializeUnpackedRecord(UnpackedRecord *pRec, u8 **ppOut, int *pnOut){
  int nField = pRec->nField;
  Mem *aMem = pRec->aMem;
  u32 nData = 0;
  u32 aType[64];
  u32 aLen[64];
  int i;
  u8 *pOut;
  int nHdr, nTotal;

  if( nField > 64 ) nField = 64;

  for(i=0; i<nField; i++){
    aType[i] = btreeSerialType(&aMem[i], &aLen[i]);
    nData += aLen[i];
  }

  nHdr = 1;
  for(i=0; i<nField; i++) nHdr += sqlite3VarintLen(aType[i]);
  if( nHdr > 126 ) nHdr++;

  nTotal = nHdr + (int)nData;
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
      if( st==0 || st==8 || st==9 ){
        /* no data */
      }else if( st<=6 ){
        i64 v = p->u.i;
        int nByte = (int)aLen[i];
        int j;
        for(j=nByte-1; j>=0; j--){
          pOut[off+j] = (u8)(v & 0xFF);
          v >>= 8;
        }
        off += nByte;
      }else if( st==7 ){
        u64 x;
        memcpy(&x, &p->u.r, 8);
        int j;
        for(j=7; j>=0; j--){
          pOut[off+j] = (u8)(x & 0xFF);
          x >>= 8;
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

int sqlite3BtreeIndexMoveto(
  BtCursor *pCur,
  UnpackedRecord *pIdxKey,
  int *pRes
){
  int rc;
  int res;
  int cmp;

  assert( !pCur->curIntKey );

  if( pCur->pBtree ) pCur->pBtree->nSeek++;

  CLEAR_CACHED_PAYLOAD(pCur);

  /* Flush OTHER cursors' pending mutations on this table.
  ** Our own MutMap is handled inline via two-pass scan. */
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

  /*
  ** Parallel seek: O(log N) tree seek + O(log M) MutMap seek.
  ** Both the tree and MutMap are sorted by compareBlobKeys.
  ** Serialize the search key, seek both, pick the best match.
  ** Verify with VdbeRecordCompare for eqSeen/default_rc correctness.
  */
  {
    int treeFound = 0, mutFound = 0;
    int treeCmp = 0, mutCmp = 0;
    const u8 *mutKey = 0;
    int mutNKey = 0;

    /* Serialize the UnpackedRecord to a blob */
    u8 *pSerKey = 0;
    int nSerKey = 0;
    rc = serializeUnpackedRecord(pIdxKey, &pSerKey, &nSerKey);
    if( rc!=SQLITE_OK ) return rc;

    /* ---- Tree seek: O(log N) ---- */
    rc = prollyCursorSeekBlob(&pCur->pCur, pSerKey, nSerKey, &res);
    if( rc==SQLITE_OK && pCur->pCur.eState==PROLLY_CURSOR_VALID ){
      const u8 *pKey; int nKey;

      /* Correction: scan backward then forward for VdbeRecordCompare match.
      ** Skip entries deleted in MutMap. Limited to a few steps. */
      while( 1 ){
        prollyCursorKey(&pCur->pCur, &pKey, &nKey);
        pIdxKey->eqSeen = 0;
        cmp = sqlite3VdbeRecordCompare(nKey, pKey, pIdxKey);
        if( cmp >= 0 || pIdxKey->eqSeen ) break;
        rc = prollyCursorPrev(&pCur->pCur);
        if( rc!=SQLITE_OK || pCur->pCur.eState!=PROLLY_CURSOR_VALID ){
          rc = prollyCursorFirst(&pCur->pCur, &(int){0});
          break;
        }
      }
      /* Forward scan: skip deleted entries, find first cmp >= 0 */
      while( pCur->pCur.eState==PROLLY_CURSOR_VALID ){
        prollyCursorKey(&pCur->pCur, &pKey, &nKey);
        if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
          ProllyMutMapEntry *mmE = prollyMutMapFind(
              pCur->pMutMap, pKey, nKey, 0);
          if( mmE && mmE->op==PROLLY_EDIT_DELETE ){
            rc = prollyCursorNext(&pCur->pCur);
            if( rc!=SQLITE_OK ) break;
            continue;
          }
        }
        pIdxKey->eqSeen = 0;
        treeCmp = sqlite3VdbeRecordCompare(nKey, pKey, pIdxKey);
        if( treeCmp==0 || pIdxKey->eqSeen || treeCmp>0 ){
          treeFound = 1;
          break;
        }
        rc = prollyCursorNext(&pCur->pCur);
        if( rc!=SQLITE_OK ) break;
      }
    }

    /* ---- MutMap seek: O(log M) using prollyMutMapFind ---- */
    /* Only if tree didn't find exact match */
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap)
     && !(treeFound && treeCmp==0) ){
      /* Use compareBlobKeys-based lookup for exact match */
      ProllyMutMapEntry *mutE = prollyMutMapFind(
          pCur->pMutMap, pSerKey, nSerKey, 0);
      if( mutE && mutE->op==PROLLY_EDIT_INSERT ){
        pIdxKey->eqSeen = 0;
        mutCmp = sqlite3VdbeRecordCompare(mutE->nKey, mutE->pKey, pIdxKey);
        if( mutCmp==0 || pIdxKey->eqSeen ){
          mutKey = mutE->pKey;
          mutNKey = mutE->nKey;
          mutFound = 1;
        }
      }
    }
    sqlite3_free(pSerKey);

    /* ---- Pick best result ---- */
    if( mutFound && (!treeFound || treeCmp!=0) ){
      /* MutMap has exact match — use it */
      if( pCur->cachedPayloadOwned && pCur->pCachedPayload ){
        sqlite3_free(pCur->pCachedPayload);
      }
      pCur->pCachedPayload = sqlite3_malloc(mutNKey);
      if( pCur->pCachedPayload ){
        memcpy(pCur->pCachedPayload, mutKey, mutNKey);
        pCur->nCachedPayload = mutNKey;
      } else {
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
    int rc2 = prollyCursorLast(&pCur->pCur, &lastRes);
    if( rc2==SQLITE_OK && lastRes==0 ){
      pCur->eState = CURSOR_VALID;
      *pRes = -1;
    } else {
      pCur->eState = CURSOR_INVALID;
      *pRes = -1;
    }
  }
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Cursor read
** -------------------------------------------------------------------------- */

i64 sqlite3BtreeIntegerKey(BtCursor *pCur){
  assert( pCur->eState==CURSOR_VALID );
  assert( pCur->curIntKey );
  /* When the seek was satisfied from the MutMap the prolly cursor may
  ** not be positioned. Return the cached key only in that case.
  ** If the prolly cursor IS valid, always read from it because
  ** cachedIntKey may be stale after Next()/Previous(). */
  if( !prollyCursorIsValid(&pCur->pCur)
   && (pCur->curFlags & BTCF_ValidNKey) ){
    return pCur->cachedIntKey;
  }
  return prollyCursorIntKey(&pCur->pCur);
}

/*
** For INTKEY tables: payload = the value (data stored in leaf)
** For BLOBKEY (index) tables: payload = the key (entire record IS the key)
*/
static void getCursorPayload(BtCursor *pCur, const u8 **ppData, int *pnData){
  /* If we have cached payload from a MutMap lookup, return that */
  if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
    *ppData = pCur->pCachedPayload;
    *pnData = pCur->nCachedPayload;
    return;
  }
  if( pCur->curIntKey ){
    prollyCursorValue(&pCur->pCur, ppData, pnData);
  }else{
    prollyCursorKey(&pCur->pCur, ppData, pnData);
  }
}

u32 sqlite3BtreePayloadSize(BtCursor *pCur){
  const u8 *pData;
  int nData;
  assert( pCur->eState==CURSOR_VALID );
  getCursorPayload(pCur, &pData, &nData);
  return (u32)nData;
}

int sqlite3BtreePayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
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

const void *sqlite3BtreePayloadFetch(BtCursor *pCur, u32 *pAmt){
  const u8 *pData;
  int nData;

  assert( pCur->eState==CURSOR_VALID );
  getCursorPayload(pCur, &pData, &nData);

  if( pAmt ) *pAmt = (u32)nData;
  return (const void*)pData;
}

sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor *pCur){
  (void)pCur;
  return PROLLY_MAX_RECORD_SIZE;
}

i64 sqlite3BtreeOffset(BtCursor *pCur){
  (void)pCur;
  return 0;
}

/* --------------------------------------------------------------------------
** Cursor write
** -------------------------------------------------------------------------- */

int sqlite3BtreeInsert(
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
    rc = prollyMutMapInsert(pCur->pMutMap,
                             (const u8*)pPayload->pKey,
                             (int)pPayload->nKey,
                             0, NULL, 0);
  }

  if( rc!=SQLITE_OK ) return rc;

  /* Defer flush for persistent non-master tables. The MutMap accumulates edits
  ** and they are flushed at commit time via flushAllPending. TableMoveto
  ** and IndexMoveto check MutMap so reads see pending edits.
  ** Only defer for tables in aCommittedTables — ephemeral tables (CTE working
  ** tables, autoindexes) need immediate writes. */
  {
    int canDefer = 0;
    if( pCur->pgnoRoot > 1 ){
      Btree *pBtree = pCur->pBtree;
      if( pBtree->aCommittedTables ){
        int i;
        for(i = 0; i < pBtree->nCommittedTables; i++){
          if( pBtree->aCommittedTables[i].iTable == pCur->pgnoRoot ){
            canDefer = 1;
            break;
          }
        }
      }
    }
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

int sqlite3BtreeDelete(BtCursor *pCur, u8 flags){
  int rc;

  assert( pCur->pBtree->inTrans==TRANS_WRITE );
  assert( pCur->curFlags & BTCF_WriteFlag );

  /* Restore cursor position if it was saved by saveAllCursors during a
  ** concurrent insert on another cursor sharing the same ephemeral table.
  ** This matches stock SQLite's btree.c which handles CURSOR_REQUIRESEEK
  ** at the top of sqlite3BtreeDelete. Without this, window functions like
  ** PERCENT_RANK crash because the prolly cursor has NULL node pointers. */
  if( pCur->eState!=CURSOR_VALID ){
    if( pCur->eState>=CURSOR_REQUIRESEEK ){
      rc = restoreCursorPosition(pCur, 0);
      if( rc!=SQLITE_OK || pCur->eState!=CURSOR_VALID ) return rc;
    }else{
      return SQLITE_CORRUPT_BKPT;
    }
  }
  assert( pCur->eState==CURSOR_VALID );

  rc = syncSavepoints(pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = saveAllCursors(pCur->pBt, pCur->pgnoRoot, pCur);
  if( rc!=SQLITE_OK ) return rc;

  rc = ensureMutMap(pCur);
  if( rc!=SQLITE_OK ) return rc;

  if( pCur->curIntKey ){
    /* Use the cached integer key when available — the prolly cursor may
    ** not be positioned if the seek was satisfied from the MutMap. */
    i64 intKey;
    if( !prollyCursorIsValid(&pCur->pCur)
     && (pCur->curFlags & BTCF_ValidNKey) ){
      intKey = pCur->cachedIntKey;
    }else{
      intKey = prollyCursorIntKey(&pCur->pCur);
    }
    rc = prollyMutMapDelete(pCur->pMutMap, NULL, 0, intKey);
  } else {
    const u8 *pKey;
    int nKey;
    /* Use cached payload if cursor is positioned on a MutMap entry
    ** (set by IndexMoveto when the match came from the MutMap). */
    if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
      pKey = pCur->pCachedPayload;
      nKey = pCur->nCachedPayload;
    } else {
      prollyCursorKey(&pCur->pCur, &pKey, &nKey);
    }
    rc = prollyMutMapDelete(pCur->pMutMap, pKey, nKey, 0);
  }

  if( rc!=SQLITE_OK ) return rc;

  /* Defer flush for persistent non-master tables, same as Insert. */
  {
    int canDefer = 0;
    if( pCur->pgnoRoot > 1 ){
      Btree *pBtree = pCur->pBtree;
      if( pBtree->aCommittedTables ){
        int i;
        for(i = 0; i < pBtree->nCommittedTables; i++){
          if( pBtree->aCommittedTables[i].iTable == pCur->pgnoRoot ){
            canDefer = 1;
            break;
          }
        }
      }
    }
    if( canDefer ){
      CLEAR_CACHED_PAYLOAD(pCur);
      if( (flags & BTREE_SAVEPOSITION) && pCur->curIntKey ){
        /* The cursor is positioned on the deleted entry in the tree.
        ** The entry still exists in the tree (not flushed) but is marked
        ** DELETE in MutMap. Set SKIPNEXT so Next() advances past it. */
        pCur->eState = CURSOR_SKIPNEXT;
        pCur->skipNext = 0;
      } else {
        pCur->eState = CURSOR_INVALID;
      }
      pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
      return SQLITE_OK;
    }
  }

  /* Master table (pgnoRoot==1) and ephemeral tables: flush immediately */
  {
    i64 savedIntKey = 0;
    u8 *savedBlobKey = 0;
    int savedBlobKeyLen = 0;
    if( pCur->curIntKey ){
      if( !prollyCursorIsValid(&pCur->pCur)
       && (pCur->curFlags & BTCF_ValidNKey) ){
        savedIntKey = pCur->cachedIntKey;
      } else {
        savedIntKey = prollyCursorIntKey(&pCur->pCur);
      }
    } else {
      const u8 *pk; int nk;
      if( pCur->pCachedPayload && pCur->nCachedPayload > 0 ){
        pk = pCur->pCachedPayload;
        nk = pCur->nCachedPayload;
      } else {
        prollyCursorKey(&pCur->pCur, &pk, &nk);
      }
      if( nk > 0 ){
        savedBlobKey = sqlite3_malloc(nk);
        if( savedBlobKey ){ memcpy(savedBlobKey, pk, nk); savedBlobKeyLen = nk; }
      }
    }

    rc = flushMutMap(pCur);
    if( rc!=SQLITE_OK ){
      sqlite3_free(savedBlobKey);
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

    if( flags & BTREE_SAVEPOSITION ){
      int res = 0;
      if( pCur->curIntKey ){
        rc = prollyCursorSeekInt(&pCur->pCur, savedIntKey, &res);
      } else if( savedBlobKey ){
        rc = prollyCursorSeekBlob(&pCur->pCur, savedBlobKey, savedBlobKeyLen, &res);
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
    sqlite3_free(savedBlobKey);
  }

  pCur->curFlags &= ~(BTCF_ValidNKey|BTCF_AtLast);
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** TransferRow
** -------------------------------------------------------------------------- */

int sqlite3BtreeTransferRow(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
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
void sqlite3BtreeEnter(Btree *p){ (void)p; }
void sqlite3BtreeEnterAll(sqlite3 *db){ (void)db; }
int sqlite3BtreeSharable(Btree *p){ (void)p; return 0; }
void sqlite3BtreeEnterCursor(BtCursor *pCur){ (void)pCur; }
int sqlite3BtreeConnectionCount(Btree *p){ (void)p; return 1; }
#endif

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE
void sqlite3BtreeLeave(Btree *p){ (void)p; }
void sqlite3BtreeLeaveCursor(BtCursor *pCur){ (void)pCur; }
void sqlite3BtreeLeaveAll(sqlite3 *db){ (void)db; }
#ifndef NDEBUG
int sqlite3BtreeHoldsMutex(Btree *p){ (void)p; return 1; }
int sqlite3BtreeHoldsAllMutexes(sqlite3 *db){ (void)db; return 1; }
int sqlite3SchemaMutexHeld(sqlite3 *db, int iDb, Schema *pSchema){
  (void)db; (void)iDb; (void)pSchema;
  return 1;
}
#endif
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

/* --------------------------------------------------------------------------
** ClearCursor / ClearCache
** -------------------------------------------------------------------------- */

void sqlite3BtreeClearCursor(BtCursor *pCur){
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

void sqlite3BtreeClearCache(Btree *p){
  (void)p;
}

/* --------------------------------------------------------------------------
** Pager
** -------------------------------------------------------------------------- */

struct Pager *sqlite3BtreePager(Btree *p){
  return (struct Pager*)(p->pBt->pPagerShim);
}

/* --------------------------------------------------------------------------
** Count / RowCountEst
** -------------------------------------------------------------------------- */

int sqlite3BtreeCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  (void)db;
  /* Flush any deferred edits so the count reflects pending inserts/deletes */
  flushTablePending(pCur);
  flushIfNeeded(pCur);
  return countTreeEntries(pCur->pBtree, pCur->pgnoRoot, pnEntry);
}

i64 sqlite3BtreeRowCountEst(BtCursor *pCur){
  struct TableEntry *pTE;
  pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
  if( !pTE || prollyHashIsEmpty(&pTE->root) ){
    return 0;
  }
  /* Default estimate for the query planner */
  return 1000000;
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

void sqlite3BtreeCursorPin(BtCursor *pCur){
  pCur->isPinned = 1;
  pCur->curFlags |= BTCF_Pinned;
}

void sqlite3BtreeCursorUnpin(BtCursor *pCur){
  pCur->isPinned = 0;
  pCur->curFlags &= ~BTCF_Pinned;
}

/* --------------------------------------------------------------------------
** CursorHintFlags / CursorHasHint
** -------------------------------------------------------------------------- */

void sqlite3BtreeCursorHintFlags(BtCursor *pCur, unsigned x){
  pCur->hints = (u8)(x & 0xFF);
}

#ifdef SQLITE_ENABLE_CURSOR_HINTS
void sqlite3BtreeCursorHint(BtCursor *pCur, int eHintType, ...){
  (void)pCur; (void)eHintType;
}
#endif

int sqlite3BtreeCursorHasHint(BtCursor *pCur, unsigned int mask){
  return (pCur->hints & mask) != 0;
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

int sqlite3BtreePayloadChecked(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
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

int sqlite3BtreePutData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
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

void sqlite3BtreeIncrblobCursor(BtCursor *pCur){
  pCur->curFlags |= BTCF_Incrblob;
}

#endif /* SQLITE_OMIT_INCRBLOB */

/* --------------------------------------------------------------------------
** Debug / Test
** -------------------------------------------------------------------------- */

#ifndef NDEBUG
int sqlite3BtreeCursorIsValid(BtCursor *pCur){
  return pCur && pCur->eState==CURSOR_VALID;
}
#endif

int sqlite3BtreeCursorIsValidNN(BtCursor *pCur){
  assert( pCur!=0 );
  return pCur->eState==CURSOR_VALID;
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

/* External registration for all dolt features */
extern void doltliteRegister(sqlite3 *db);

static void registerDoltiteFunctions(sqlite3 *db){
  sqlite3_create_function(db, "doltlite_engine", 0, SQLITE_UTF8, 0,
                          doltiteEngineFunc, 0, 0);
  doltliteRegister(db);
}

#endif /* DOLTLITE_PROLLY */
