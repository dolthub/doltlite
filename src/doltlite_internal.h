
#ifndef DOLTLITE_INTERNAL_H
#define DOLTLITE_INTERNAL_H

typedef struct BtShared BtShared;
typedef struct ProllyCache ProllyCache;

/* iTable is the rowid-alias of the table in sqlite_master and
** survives renames (used by dolt_status to detect renames as same
** iTable + different name). pPending is the in-memory ProllyMutMap
** of uncommitted edits for this table — NULL until the first
** mutation flushes a table entry into the working catalog. */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
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

static SQLITE_INLINE int tableEntryNameCmp(const void *a, const void *b){
  const struct TableEntry *ea = (const struct TableEntry *)a;
  const struct TableEntry *eb = (const struct TableEntry *)b;
  if( !ea->zName && !eb->zName ) return 0;
  if( !ea->zName ) return -1;
  if( !eb->zName ) return 1;
  return strcmp(ea->zName, eb->zName);
}

static SQLITE_INLINE int doltliteFindTableRoot(
  struct TableEntry *a, int n, Pgno iTable,
  ProllyHash *pRoot, u8 *pFlags
){
  struct TableEntry *e = doltliteFindTableByNumber(a, n, iTable);
  if( e ){
    memcpy(pRoot, &e->root, sizeof(ProllyHash));
    if( pFlags ) *pFlags = e->flags;
    return SQLITE_OK;
  }
  memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  return SQLITE_NOTFOUND;
}

static SQLITE_INLINE int doltliteFindTableRootByName(
  struct TableEntry *a, int n, const char *zName,
  ProllyHash *pRoot, u8 *pFlags
){
  struct TableEntry *e = doltliteFindTableByName(a, n, zName);
  if( e ){
    memcpy(pRoot, &e->root, sizeof(ProllyHash));
    if( pFlags ) *pFlags = e->flags;
    return SQLITE_OK;
  }
  memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  return SQLITE_NOTFOUND;
}

ChunkStore *doltliteGetChunkStore(sqlite3 *db);
BtShared *doltliteGetBtShared(sqlite3 *db);
ProllyCache *doltliteGetCache(sqlite3 *db);
int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                        struct TableEntry **ppTables, int *pnTables,
                        Pgno *piNextTable);
void doltliteFreeCatalog(struct TableEntry *a, int n);
int doltliteSerializeCatalogEntries(sqlite3 *db, struct TableEntry *aTables,
                                    int nTables, u8 **ppOut, int *pnOut);
int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash);
int doltliteGetWorkingTableState(sqlite3 *db, const char *zTable,
                                 ProllyHash *pRoot, u8 *pFlags,
                                 ProllyHash *pSchemaHash);
int doltliteHasUncommittedChanges(sqlite3 *db);

int doltliteResolveRef(sqlite3 *db, const char *zRef, ProllyHash *pCommit);

typedef struct DoltliteCommit DoltliteCommit;
int doltliteLoadCommit(sqlite3 *db, const ProllyHash *pHash,
                       DoltliteCommit *pCommit);

int doltliteForEachUserTable(sqlite3 *db, const char *zPrefix,
                             const sqlite3_module *pModule);

int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);
int doltliteApplyRawRowMutation(sqlite3 *db, const char *zTable,
                                const u8 *pKey, int nKey, i64 intKey,
                                const u8 *pVal, int nVal);
int doltliteSwitchCatalog(sqlite3 *db, const ProllyHash *catHash);
int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);
int doltliteUpdateBranchWorkingState(sqlite3 *db, const char *zBranch,
                                     const ProllyHash *pCatHash,
                                     const ProllyHash *pCommitHash);
int doltliteCheckRepoGraphIntegrity(Btree *p, int mxErr, int *pnErr);

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
void doltliteGetSessionRebaseState(sqlite3 *db, u8 *pIsRebasing,
                                   ProllyHash *pPreRebaseCat,
                                   ProllyHash *pRebaseOnto,
                                   const char **pzOrigBranch);
void doltliteSetSessionRebaseState(sqlite3 *db, u8 isRebasing,
                                   const ProllyHash *pPreRebaseCat,
                                   const ProllyHash *pRebaseOnto,
                                   const char *zOrigBranch);
void doltliteClearSessionRebaseState(sqlite3 *db);
void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash);
void doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash);
int doltliteSaveWorkingSet(sqlite3 *db);
int doltlitePersistWorkingSet(sqlite3 *db);
int doltliteLoadWorkingSet(sqlite3 *db, const char *zBranch);

typedef int (*DoltliteRefsMutation)(sqlite3 *db, ChunkStore *cs, void *pArg);
int doltliteMutateRefs(sqlite3 *db, DoltliteRefsMutation xMutate, void *pArg);

const char *doltliteGetAuthorName(sqlite3 *db);
void doltliteSetAuthorName(sqlite3 *db, const char *zName);
const char *doltliteGetAuthorEmail(sqlite3 *db);
void doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail);

typedef struct SchemaEntry SchemaEntry;
struct SchemaEntry {
  char *zName;
  char *zSql;
  char *zType;
};

int loadSchemaFromCatalog(sqlite3 *db, ChunkStore *cs, ProllyCache *pCache,
                          const ProllyHash *pCatHash,
                          SchemaEntry **ppEntries, int *pnEntries);
SchemaEntry *findSchemaEntry(SchemaEntry *a, int n, const char *zName);
void freeSchemaEntries(SchemaEntry *a, int n);

typedef struct SchemaMergeAction SchemaMergeAction;
struct SchemaMergeAction {
  char *zTableName;
  char **azAddColumns;
  int nAddColumns;
};

void freeSchemaMergeActions(SchemaMergeAction *a, int n);

int doltliteMergeCatalogs(sqlite3 *db,
  const ProllyHash *ancestor, const ProllyHash *ours,
  const ProllyHash *theirs, ProllyHash *pMergedHash,
  int *pnConflicts, char **pzErrMsg,
  SchemaMergeAction **ppActions, int *pnActions);

struct ProllyDiffChange;

struct MigrateDiffCtx {
  sqlite3_stmt *pUpd;
  int *aiColIdx;
  char **azColNames;
  int nCols;
};

char *extractColNameFromDef(const char *zDef);
int migrateDiffCb(void *pArg, const struct ProllyDiffChange *pChange);
int migrateSchemaRowData(sqlite3 *db, const ProllyHash *pAncCatHash,
                         const ProllyHash *pTheirCatHash,
                         SchemaMergeAction *aActions, int nActions);

static SQLITE_INLINE int doltliteAppendQuotedIdent(sqlite3_str *pStr,
                                                   const char *zName){
  sqlite3_str_appendf(pStr, "\"%w\"", zName ? zName : "");
  return sqlite3_str_errcode(pStr);
}

#endif
