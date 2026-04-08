/*
** Shared declarations for the doltlite_*.c integration layer.
**
** This header consolidates struct definitions and function declarations
** used across multiple doltlite_*.c files. Include this instead of
** repeating extern declarations in each file.
*/
#ifndef DOLTLITE_INTERNAL_H
#define DOLTLITE_INTERNAL_H

/*
** This header assumes the consumer has already included sqliteInt.h,
** prolly_hash.h, and chunk_store.h (all doltlite_*.c files do).
*/
typedef struct BtShared BtShared;
typedef struct ProllyCache ProllyCache;

/*
** A single table entry from the catalog.  Returned by doltliteLoadCatalog().
** The pPending field is opaque to the doltlite layer (it is a ProllyMutMap*
** managed by prolly_btree.c).
*/
struct TableEntry {
  Pgno iTable;           /* Logical table number */
  ProllyHash root;       /* Current root hash of this table's prolly tree */
  ProllyHash schemaHash; /* Hash of this table's CREATE TABLE SQL */
  u8 flags;              /* BTREE_INTKEY or BTREE_BLOBKEY */
  char *zName;           /* Table name (owned, NULL for internal tables) */
  void *pPending;        /* Deferred edits (opaque to doltlite layer) */
};

/* --- Catalog lookup helpers --- */

static SQLITE_INLINE struct TableEntry *doltliteFindTableByNumber(
  struct TableEntry *a, int n, Pgno iTable
){
  int i;
  for(i=0; i<n; i++){
    if( a[i].iTable==iTable ) return &a[i];
  }
  return 0;
}

static SQLITE_INLINE struct TableEntry *doltliteFindTableByName(
  struct TableEntry *a, int n, const char *zName
){
  int i;
  if( !zName ) return 0;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ) return &a[i];
  }
  return 0;
}

/* --- Data access (defined in prolly_btree.c) --- */

ChunkStore *doltliteGetChunkStore(sqlite3 *db);
BtShared *doltliteGetBtShared(sqlite3 *db);
ProllyCache *doltliteGetCache(sqlite3 *db);
int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                        struct TableEntry **ppTables, int *pnTables,
                        Pgno *piNextTable);
int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);
int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);

/* --- Per-connection session state (defined in prolly_btree.c) --- */

const char *doltliteGetSessionBranch(sqlite3 *db);
void doltliteSetSessionBranch(sqlite3 *db, const char *zBranch);
void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead);
void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged);
void doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged);
void doltliteGetSessionMergeState(sqlite3 *db, u8 *pIsMerging,
                                  ProllyHash *pMergeCommit,
                                  ProllyHash *pConflictsCatalog);
void doltliteSetSessionMergeState(sqlite3 *db, u8 isMerging,
                                  const ProllyHash *pMergeCommit,
                                  const ProllyHash *pConflictsCatalog);
void doltliteClearSessionMergeState(sqlite3 *db);
void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash);
void doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash);
int doltliteSaveWorkingSet(sqlite3 *db);

/* --- Author info (defined in prolly_btree.c) --- */

const char *doltliteGetAuthorName(sqlite3 *db);
void doltliteSetAuthorName(sqlite3 *db, const char *zName);
const char *doltliteGetAuthorEmail(sqlite3 *db);
void doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail);

#endif /* DOLTLITE_INTERNAL_H */
