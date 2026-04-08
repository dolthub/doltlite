
#ifndef DOLTLITE_INTERNAL_H
#define DOLTLITE_INTERNAL_H

typedef struct BtShared BtShared;
typedef struct ProllyCache ProllyCache;


struct TableEntry {
  Pgno iTable;           
  ProllyHash root;       
  ProllyHash schemaHash; 
  u8 flags;              
  char *zName;           
  void *pPending;        
};

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
int doltliteSwitchCatalog(sqlite3 *db, const ProllyHash *catHash);
int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);
int doltliteUpdateBranchWorkingState(sqlite3 *db, const char *zBranch,
                                     const ProllyHash *pCatHash);

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

const char *doltliteGetAuthorName(sqlite3 *db);
void doltliteSetAuthorName(sqlite3 *db, const char *zName);
const char *doltliteGetAuthorEmail(sqlite3 *db);
void doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail);

#endif 
