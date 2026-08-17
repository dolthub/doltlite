#ifdef DOLTLITE_PROLLY

#include "prolly_btree_int.h"

/* B-tree lifecycle, dispatch tables, metadata, and public API shims. */

static void btreeClearCatalogCache(Btree *p){
  sqlite3_free(p->pCatalogCache);
  p->pCatalogCache = 0;
  p->nCatalogCache = 0;
  memset(&p->catalogCacheHash, 0, sizeof(p->catalogCacheHash));
}

static int registerDoltiteFunctions(sqlite3 *db);


#define PROLLY_MUTMAP_PENDING_FLUSH_LIMIT 65536

int mutMapShouldDrain(BtCursor *pCur){
  return pCur && pCur->pMutMap
      && prollyMutMapCount(pCur->pMutMap) >= PROLLY_MUTMAP_PENDING_FLUSH_LIMIT;
}

i64 prollyBtreeSyntheticPageCount(Btree *p){
  ChunkStore *cs;
  i64 n;
  if( !p || !p->pBt ) return 0;
  cs = &p->pBt->store;
  n = (i64)chunkIndexCount(&cs->index)
    + (i64)chunkStagingPendingCount(&cs->staging)
    + (i64)chunkStagingRecentCount(&cs->staging);
  if( n < 0 ) n = 0;
  if( n > (i64)0xfffffffe ) n = (i64)0xfffffffe;
  if( p->aMeta[BTREE_LARGEST_ROOT_PAGE]>(Pgno)n ){
    n = p->aMeta[BTREE_LARGEST_ROOT_PAGE];
  }
  return n;
}

static i64 prollyBtreePendingPageEstimate(Btree *p, i64 nLimit){
  i64 nBytes = 0;
  i64 nInsert = 0;
  i64 nBytePages;
  i64 nRowPages;
  i64 nPageSize;
  int i;
  if( !p || !p->pBt ) return 0;
  nPageSize = p->pBt->pageSize>0 ? (i64)p->pBt->pageSize : 1024;
  for(i=0; i<p->cat.n; i++){
    if( p->cat.a[i].pPending ){
      ProllyMutMap *pMap = (ProllyMutMap*)p->cat.a[i].pPending;
      int j;
      for(j=0; j<pMap->nEntries; j++){
        ProllyMutMapEntry *pEntry = &pMap->aEntries[j];
        if( pEntry->op==PROLLY_EDIT_INSERT ){
          nInsert++;
          nBytes += (i64)pEntry->nKey + (i64)pEntry->nVal
                  + pEntry->nZeroTail + 16;
          if( nLimit>0 && nInsert/16 + nBytes/(2*nPageSize) > nLimit ){
            return nLimit + 1;
          }
        }
      }
    }
  }
  nBytePages = (nBytes + 2*nPageSize - 1)/(2*nPageSize);
  nRowPages = (nInsert + 15)/16;
  return nBytePages > nRowPages ? nBytePages : nRowPages;
}

int prollyBtreeCheckMaxPageCount(Btree *p){
  i64 nCurrent;
  i64 nPending;
  if( !p ) return SQLITE_OK;
  if( p->mxPageCount==0 || p->mxPageCount>=SQLITE_MAX_PAGE_COUNT ){
    return SQLITE_OK;
  }
  nCurrent = prollyBtreeSyntheticPageCount(p);
  if( nCurrent > (i64)p->mxPageCount ){
    return SQLITE_FULL;
  }
  nPending = prollyBtreePendingPageEstimate(
      p, (i64)p->mxPageCount - nCurrent);
  if( nCurrent + nPending > (i64)p->mxPageCount ){
    return SQLITE_FULL;
  }
  return SQLITE_OK;
}


int prollyInvokeBusyHandler(BtShared *pBt){
  if( !pBt || !pBt->db ) return 0;
  assert( sqlite3_mutex_held(pBt->db->mutex) );
  return sqlite3InvokeBusyHandler(&pBt->db->busyHandler);
}

const struct BtreeOps prollyBtreeOps = {
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

const struct BtreeOps origBtreeVtOps = {
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


const struct BtCursorOps prollyCursorOps = {
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
  prollyBtCursorCountRange,
  prollyBtCursorCountIndexRange,
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

const struct BtCursorOps origCursorVtOps = {
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
  origCursorCountRangeVt,
  origCursorCountIndexRangeVt,
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

struct TableEntry *catFind(Catalog *cat, Pgno iTable){
  int lo = 0, hi;
  assert( cat!=0 );
  assert( cat->n==0 || cat->a!=0 );
  hi = cat->n - 1;
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

struct TableEntry *catAdd(Catalog *cat, Pgno iTable, u8 flags){
  struct TableEntry *pEntry;

  assert( cat!=0 );
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
  assert( cat->n<=cat->nAlloc );
#ifndef NDEBUG
  {
    int i;
    for(i=1; i<cat->n; i++){
      assert( cat->a[i-1].iTable < cat->a[i].iTable );
    }
  }
#endif

  return pEntry;
}


int saveAllCursors(Btree *pBtree, BtShared *pBt, Pgno iRoot,
                          BtCursor *pExcept){
  BtCursor *p;
  assert( pBtree!=0 && pBt!=0 );
  assert( pBtree->pBt==pBt );
  for(p=pBt->pCursor; p; p=p->pNext){
    if( p->pBtree==pBtree
     && p!=pExcept
     && (iRoot==0 || p->pgnoRoot==iRoot) ){
      if( p->eState==CURSOR_VALID || p->eState==CURSOR_SKIPNEXT ){
        int rc = saveCursorPosition(p);
        if( rc!=SQLITE_OK ) return rc;
        assert( p->eState==CURSOR_REQUIRESEEK
             || p->eState==CURSOR_INVALID
             || p->eState==CURSOR_FAULT );
      }
    }
  }
  return SQLITE_OK;
}

static int doltliteLooksLikeDbPath(const char *zFilename){
  const char *z;
  const char *zBase = zFilename;
  if( !zFilename ) return 0;
  for(z=zFilename; *z; z++){
    if( *z=='/' || *z=='\\' ) zBase = z + 1;
  }
  return strstr(zBase, ".db")!=0 || strstr(zBase, ".sqlite")!=0;
}

static int doltliteFileExists(sqlite3_vfs *pVfs, const char *zFilename,
                              int *pExists){
  int rc;
  *pExists = 0;
  if( !zFilename || zFilename[0]=='\0' || strcmp(zFilename, ":memory:")==0 ){
    return SQLITE_OK;
  }
  if( !pVfs ){
    pVfs = sqlite3_vfs_find(0);
    if( !pVfs ) return SQLITE_CANTOPEN;
  }
  rc = sqlite3OsAccess(pVfs, zFilename, SQLITE_ACCESS_EXISTS, pExists);
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
  if( rc!=SQLITE_OK ) *pExists = 0;
  return SQLITE_OK;
}

static int doltliteReadableFile(sqlite3_vfs *pVfs, const char *zFilename,
                                int *pIsFile){
  sqlite3_file *pFile = 0;
  int outFlags = 0;
  int rc;
  i64 nSize = 0;
  u8 b;

  *pIsFile = 0;
  rc = sqlite3OsOpenMalloc(pVfs, zFilename, &pFile,
                           SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB,
                           &outFlags);
  if( rc!=SQLITE_OK ){
    if( pFile ) sqlite3OsCloseFree(pFile);
    if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
    return SQLITE_OK;
  }
  rc = sqlite3OsFileSize(pFile, &nSize);
  if( rc==SQLITE_OK && nSize>0 ){
    rc = sqlite3OsRead(pFile, &b, 1, 0);
  }
  sqlite3OsCloseFree(pFile);
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
  if( rc==SQLITE_OK && nSize>0 ) *pIsFile = 1;
  return SQLITE_OK;
}

static int doltliteResolveOpenBranchPath(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  char **pzStoreFilename,
  const char **pzBranch
){
  const char *z;
  const char *zSep = 0;
  int parentExists = 0;
  int rc;
  char *zParent;
  int nParent;
  int parentIsFile = 0;
  int parentIsSqlite = 0;

  *pzStoreFilename = 0;
  *pzBranch = 0;
  if( !zFilename || zFilename[0]=='\0' || strcmp(zFilename, ":memory:")==0 ){
    return SQLITE_OK;
  }

  for(z=zFilename; *z; z++){
    if( *z=='/' || *z=='\\' || *z=='@' ) zSep = z;
  }
  if( !zSep || zSep==zFilename || zSep[1]=='\0' ) return SQLITE_OK;

  nParent = (int)(zSep - zFilename);
  zParent = sqlite3_mprintf("%.*s", nParent, zFilename);
  if( !zParent ) return SQLITE_NOMEM;
  if( !doltliteLooksLikeDbPath(zParent) ){
    sqlite3_free(zParent);
    return SQLITE_OK;
  }

  rc = doltliteFileExists(pVfs, zParent, &parentExists);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zParent);
    return rc;
  }
  if( !parentExists ){
    sqlite3_free(zParent);
    return SQLITE_OK;
  }

  rc = doltliteReadableFile(pVfs, zParent, &parentIsFile);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zParent);
    return rc;
  }
  if( !parentIsFile ){
    sqlite3_free(zParent);
    return SQLITE_OK;
  }

  rc = origBtreeIsSqliteFile(pVfs, zParent, &parentIsSqlite);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zParent);
    return rc;
  }
  if( parentIsSqlite ){
    sqlite3_free(zParent);
    return SQLITE_OK;
  }

  *pzStoreFilename = zParent;
  *pzBranch = zSep + 1;
  return SQLITE_OK;
}

/* True when zFilename names a file that already holds bytes. Used to keep the
** doltlite_engine URI knob from reinterpreting an existing database. */
static int doltliteFileHasContent(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  int *pHasContent
){
  sqlite3_file *pFile = 0;
  int exists = 0;
  int outFlags = 0;
  i64 sz = 0;
  char *zDup = 0;
  int rc;

  *pHasContent = 0;
  if( !pVfs ){
    pVfs = sqlite3_vfs_find(0);
    if( !pVfs ) return SQLITE_OK;
  }
  rc = sqlite3OsAccess(pVfs, zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc!=SQLITE_OK ) return rc;
  if( !exists ) return SQLITE_OK;

  /* zFilename is the ParseUri name, so its parameters live past the first nul
  ** and only the double nul ends it. SQLITE_OPEN_MAIN_DB puts it under that
  ** contract, so hand the VFS a plain copy rather than the parameter list. */
  rc = chunkStoreDupFilenameDoubleNul(zFilename, &zDup);
  if( rc!=SQLITE_OK ) return rc;
  /* The handle keeps the name, not a copy of it, so zDup has to outlive the
  ** close: unixClose logs pFile->zPath on the way out. */
  rc = sqlite3OsOpenMalloc(pVfs, zDup, &pFile,
                           SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB,
                           &outFlags);
  if( rc!=SQLITE_OK ){
    if( pFile ) sqlite3OsCloseFree(pFile);
    sqlite3_free(zDup);
    /* Unreadable is not "empty": leave the knob inactive. */
    *pHasContent = 1;
    return SQLITE_OK;
  }
  rc = sqlite3OsFileSize(pFile, &sz);
  sqlite3OsCloseFree(pFile);
  sqlite3_free(zDup);
  if( rc!=SQLITE_OK ) return rc;
  *pHasContent = sz>0;
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
  int useOrig;
  int hasMainBtree;
  u8 poisonAfterOpen = 0;
  char *zStoreFilename = 0;
  const char *zBranchFromPath = 0;
  const char *zOpenFilename = zFilename;

  *ppBtree = 0;
  hasMainBtree = db && db->nDb>0 && db->aDb[0].pBt!=0;

  useOrig = !zFilename || zFilename[0]=='\0'
   || (strcmp(zFilename, ":memory:")==0 && db->aDb[0].pBt!=0)
   || (flags & BTREE_SINGLE)
   || (vfsFlags & SQLITE_OPEN_TEMP_DB)
   /* VACUUM copies pages, so its target must use the same engine as the legacy
   ** database being vacuumed rather than defaulting to prolly. */
   || (db && (db->mDbFlags & DBFLAG_VacuumOrig)!=0);
  /* A caller creating a file it intends to hold stock pages -- a backup or
  ** VACUUM INTO target for a legacy database -- asks for the stock engine with
  ** doltlite_engine=sqlite, since a new file otherwise becomes a chunk store.
  ** It selects the engine for a database being created and is ignored once the
  ** file has content, so it can never reinterpret an existing chunk store; a
  ** file that already holds stock pages is detected below anyway.
  **
  ** Gated on SQLITE_OPEN_MAIN_DB because sqlite3_uri_parameter() is only
  ** defined on a name sqlite3ParseUri() built: it resolves the name through
  ** databaseName(), which reads zName[-1] through zName[-4] to walk back over
  ** the URI's key/value pairs. Handed a plain string that is merely nul
  ** terminated, it reads before the buffer. Main and ATTACH are the only opens
  ** whose names come from ParseUri; every other caller reaches here with a name
  ** it built itself. */
  if( !useOrig && (vfsFlags & SQLITE_OPEN_MAIN_DB)!=0 ){
    const char *zEngine = sqlite3_uri_parameter(zFilename, "doltlite_engine");
    if( zEngine && sqlite3StrICmp(zEngine, "sqlite")==0 ){
      int hasContent = 1;
      rc = doltliteFileHasContent(pVfs, zFilename, &hasContent);
      if( rc!=SQLITE_OK ) return rc;
      if( !hasContent ) useOrig = 1;
    }
  }
  if( !useOrig ){
    rc = doltliteResolveOpenBranchPath(pVfs, zFilename, &zStoreFilename,
                                       &zBranchFromPath);
    if( rc!=SQLITE_OK ) return rc;
    if( zStoreFilename ) zOpenFilename = zStoreFilename;
    rc = origBtreeIsSqliteFile(pVfs, zOpenFilename, &useOrig);
    if( useOrig && zStoreFilename ){
      useOrig = 0;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zStoreFilename);
      return rc;
    }
  }
  if( useOrig ){
    p = sqlite3_malloc(sizeof(Btree));
    if( !p ){
      sqlite3_free(zStoreFilename);
      return SQLITE_NOMEM;
    }
    memset(p, 0, sizeof(*p));
    p->db = db;
    p->pOps = &origBtreeVtOps;
    rc = origBtreeOpen(pVfs, zFilename, db, &p->pOrigBtree, flags, vfsFlags);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zStoreFilename);
      sqlite3_free(p);
      return rc;
    }
    /* Registration resolves through db->aDb[0].pBt, so assign first. */
    *ppBtree = p;
    rc = registerDoltiteFunctions(db);
    if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
      *ppBtree = 0;
      sqlite3BtreeClose(p);
      sqlite3_free(zStoreFilename);
      return rc;
    }
    sqlite3_free(zStoreFilename);
    return SQLITE_OK;
  }

  p = sqlite3_malloc(sizeof(Btree));
  if( !p ){
    sqlite3_free(zStoreFilename);
    return SQLITE_NOMEM;
  }
  memset(p, 0, sizeof(*p));

  pBt = sqlite3_malloc(sizeof(BtShared));
  if( !pBt ){
    sqlite3_free(zStoreFilename);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }
  memset(pBt, 0, sizeof(*pBt));

  if( !zFilename || zFilename[0]=='\0' ){
    zFilename = ":memory:";
    zOpenFilename = zFilename;
  }

  rc = chunkStoreOpen(&pBt->store, pVfs, zOpenFilename, vfsFlags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zStoreFilename);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return rc;
  }
  /* Serve the recovered prefix to the connection-open catalog reads below,
  ** then re-arm so the first data access fails SQLITE_CORRUPT instead. */
  poisonAfterOpen = pBt->store.corruptMidStream;
  pBt->store.corruptMidStream = 0;

  rc = prollyCacheInit(&pBt->cache, PROLLY_DEFAULT_CACHE_SIZE);
  if( rc!=SQLITE_OK ){
    chunkStoreClose(&pBt->store);
    sqlite3_free(zStoreFilename);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return rc;
  }

  pBt->pPagerShim = pagerShimCreate(pVfs, zOpenFilename, chunkFileGetHandle(&pBt->store.file));
  if( !pBt->pPagerShim ){
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(zStoreFilename);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }
  /* Resolve shim file handles through the chunk store after reloads. */
  pagerShimSetStore(pBt->pPagerShim, &pBt->store);

  pBt->db = db;
  pBt->pageSize = PROLLY_DEFAULT_PAGE_SIZE;
  pBt->iWorkingStateVersion = 1;
  pBt->nRef = 1;
  p->inTransaction = TRANS_NONE;
  p->bSchemaChangedTxn = 0;
  p->bMasterRootChangedTxn = 0;
  p->bFilterSchemaPlaceholders = 0;
  p->mxPageCount = SQLITE_MAX_PAGE_COUNT;

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
    BtreeBranchState state;
    ProllyHash branchCommit;
    ProllyHash revisionCatalog;
    u8 isBranchRevision = 0;
    const char *zDef = zBranchFromPath ? zBranchFromPath :
      chunkStoreGetDefaultBranch(&pBt->store);
    if( !zDef ) zDef = "main";
    memset(&branchCommit, 0, sizeof(branchCommit));
    memset(&revisionCatalog, 0, sizeof(revisionCatalog));
    if( zBranchFromPath ){
      rc = doltliteResolveOpenRevision(&pBt->store, zDef, &branchCommit,
                                       &revisionCatalog, &isBranchRevision);
    }
    if( zBranchFromPath && rc!=SQLITE_OK ){
      int openRc = rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM
                 ? rc : SQLITE_ERROR;
      if( openRc==SQLITE_ERROR ){
        sqlite3ErrorWithMsg(db, SQLITE_ERROR,
                            "unable to select branch \"%s\"", zDef);
      }
      pagerShimDestroy(pBt->pPagerShim);
      prollyCacheFree(&pBt->cache);
      chunkStoreClose(&pBt->store);
      sqlite3_free(zStoreFilename);
      sqlite3_free(pBt);
      sqlite3_free(p);
      return openRc;
    }
    if( zBranchFromPath && !isBranchRevision ){
      memset(&state, 0, sizeof(state));
      state.catalog = revisionCatalog;
      state.stagedCatalog = revisionCatalog;
      state.headCommit = branchCommit;
      p->isDetached = 1;
      rc = SQLITE_OK;
    }else{
      rc = btreeLoadBranchState(&pBt->store, zDef, 1, &state);
    }
    if( rc!=SQLITE_OK ){
      pagerShimDestroy(pBt->pPagerShim);
      prollyCacheFree(&pBt->cache);
      chunkStoreClose(&pBt->store);
      sqlite3_free(zStoreFilename);
      sqlite3_free(pBt);
      sqlite3_free(p);
      return rc;
    }
    if( !prollyHashIsEmpty(&state.catalog) ){
      u8 *catData = 0;
      int nCatData = 0;
      rc = chunkStoreGet(&pBt->store, &state.catalog, &catData, &nCatData);
      if( rc==SQLITE_OK && catData ){
        rc = deserializeCatalog(p, catData, nCatData);
        sqlite3_free(catData);
        if( rc!=SQLITE_OK ){
          btreeClearBranchState(&state);
          pagerShimDestroy(pBt->pPagerShim);
          prollyCacheFree(&pBt->cache);
          chunkStoreClose(&pBt->store);
          sqlite3_free(zStoreFilename);
          sqlite3_free(pBt);
          sqlite3_free(p);
          return rc;
        }
      }else{
        sqlite3_free(catData);
        if( rc!=SQLITE_OK ){
          btreeClearBranchState(&state);
          pagerShimDestroy(pBt->pPagerShim);
          prollyCacheFree(&pBt->cache);
          chunkStoreClose(&pBt->store);
          sqlite3_free(zStoreFilename);
          sqlite3_free(pBt);
          sqlite3_free(p);
          return rc;
        }
      }
    }

    p->headCommit = state.headCommit;
    p->vc.stagedCatalog = state.stagedCatalog;
    p->vc.isMerging = state.isMerging;
    p->vc.mergeCommitHash = state.mergeCommit;
    p->vc.conflictsCatalogHash = state.conflictsCatalog;
    p->isRebasing = state.isRebasing;
    p->preRebaseWorkingCat = state.preRebaseCatalog;
    p->rebaseOntoCommit = state.rebaseOnto;
    p->zRebaseOrigBranch = state.zRebaseOrigBranch;
    p->zRebaseReturnBranch = state.zRebaseReturnBranch;
    p->vc.constraintViolationsHash = state.constraintViolations;
  }

  p->cat.iNextTable = 2;
  if( !addTable(p, 1, BTREE_INTKEY) ){
    pagerShimDestroy(pBt->pPagerShim);
    prollyCacheFree(&pBt->cache);
    chunkStoreClose(&pBt->store);
    sqlite3_free(zStoreFilename);
    sqlite3_free(pBt);
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }

  p->db = db;
  p->pBt = pBt;
  p->pOps = &prollyBtreeOps;
  p->inTrans = TRANS_NONE;
  p->iBDataVersion = pBt->pPagerShim ? 0 : 1;
  p->iLoadedWorkingStateVersion = pBt->iWorkingStateVersion;
  p->nSeek = 0;

  {
    const char *defBranch = zBranchFromPath ? zBranchFromPath :
      chunkStoreGetDefaultBranch(&pBt->store);
    ProllyHash branchCommit;
    if( !defBranch ) defBranch = "main";
    p->zBranch = sqlite3_mprintf("%s", defBranch);
    if( p->zBranch==0 ){
      sqlite3_free(zStoreFilename);
      sqlite3BtreeClose(p);
      return SQLITE_NOMEM;
    }

    if( prollyHashIsEmpty(&p->headCommit)
     && chunkStoreFindBranch(&pBt->store, defBranch, &branchCommit)==SQLITE_OK ){
      memcpy(&p->headCommit, &branchCommit, sizeof(ProllyHash));
    }
  }

  pBt->store.corruptMidStream = poisonAfterOpen;
  if( hasMainBtree
   && !p->isDetached
   && !pBt->store.notADatabase
   && !pBt->store.corruptMidStream ){
    ProllyHash seedHash;
    int seeded = 0;
    rc = doltliteSeedStoreIfNeeded(db, &pBt->store, p->zBranch,
                                    &seedHash, &seeded);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zStoreFilename);
      sqlite3BtreeClose(p);
      return rc;
    }
    if( seeded ) p->headCommit = seedHash;
  }

  *ppBtree = p;

  rc = registerDoltiteFunctions(db);
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
    *ppBtree = 0;
    sqlite3_free(zStoreFilename);
    sqlite3BtreeClose(p);
    return rc;
  }

  sqlite3_free(zStoreFilename);
  return SQLITE_OK;
}

int sqlite3BtreeUsesOrig(Btree *p){
  return p && p->pOps==&origBtreeVtOps;
}

/* db->aDb[].pBt always holds a doltlite Btree; for a legacy database that
** wrapper delegates to a stock btree. The orig page copier is compiled against
** the stock layout and needs that inner pointer, so it resolves schema names
** through here rather than reading Db.pBt directly. Returns 0 for a
** doltlite-format database, which the copier reports as "not a database". */
void *doltliteBtreeOrigPtr(void *pBtree){
  Btree *p = (Btree*)pBtree;
  if( !sqlite3BtreeUsesOrig(p) ) return 0;
  return p->pOrigBtree;
}

int prollyBtreeClose(Btree *p){
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
  btreeClearCatalogCache(p);
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
  sqlite3_free(p->zMergeSourceSpec);
  sqlite3_free(p);
  return SQLITE_OK;
}
int sqlite3BtreeClose(Btree *p){
  if( !p ) return SQLITE_OK;
  return p->pOps->xClose(p);
}

int prollyBtreeNewDb(Btree *p){
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

int prollyBtreeSetCacheSize(Btree *p, int mxPage){
  i64 nEntry;
  if( !p || !p->pBt ) return SQLITE_OK;
  if( mxPage>0 ){
    nEntry = mxPage;
  }else{
    /* Negative cache_size is a KiB budget. Map it to node-cache entries via
    ** the page size, mirroring how the stock pager turns KiB into pages. */
    u32 pgsz = p->pBt->pageSize>0 ? p->pBt->pageSize : PROLLY_DEFAULT_PAGE_SIZE;
    nEntry = (-(i64)mxPage * 1024) / pgsz;
  }
  /* cache_size only grows the node cache. The stock default (-2000) and
  ** memory-shrink requests must not drop it below the engine's baseline,
  ** which the prolly backend relies on for its aggressive node caching. */
  if( nEntry < PROLLY_DEFAULT_CACHE_SIZE ) nEntry = PROLLY_DEFAULT_CACHE_SIZE;
  p->pBt->cache.nCapacity = (int)nEntry;
  return SQLITE_OK;
}
int sqlite3BtreeSetCacheSize(Btree *p, int mxPage){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetCacheSize(p, mxPage);
}

int prollyBtreeSetSpillSize(Btree *p, int mxPage){
  (void)p; (void)mxPage;
  return SQLITE_OK;
}
int sqlite3BtreeSetSpillSize(Btree *p, int mxPage){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetSpillSize(p, mxPage);
}

int prollyBtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  (void)p; (void)szMmap;
  return SQLITE_OK;
}
int sqlite3BtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetMmapLimit(p, szMmap);
}

int prollyBtreeSetPagerFlags(Btree *p, unsigned pgFlags){
  if( p && p->pBt ){
    p->pBt->store.fullFsync = (pgFlags & PAGER_FULLFSYNC) ? 1 : 0;
    p->pBt->store.noSync =
        (pgFlags & PAGER_SYNCHRONOUS_MASK)==PAGER_SYNCHRONOUS_OFF;
  }
  return SQLITE_OK;
}
int sqlite3BtreeSetPagerFlags(Btree *p, unsigned pgFlags){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetPagerFlags(p, pgFlags);
}

int prollyBtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix){
  (void)nReserve; (void)eFix;
  /* Same validation as the stock btree: ignore values that are not a power
  ** of two in [512,65536]. The value is layout-inert either way. */
  if( nPagesize>=512 && nPagesize<=65536 && ((nPagesize-1)&nPagesize)==0 ){
    p->pBt->pageSize = (u32)nPagesize;
  }
  return SQLITE_OK;
}
int sqlite3BtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetPageSize(p, nPagesize, nReserve, eFix);
}

int prollyBtreeGetPageSize(Btree *p){
  return (int)p->pBt->pageSize;
}
int sqlite3BtreeGetPageSize(Btree *p){
  return p->pOps->xGetPageSize(p);
}

Pgno prollyBtreeMaxPageCount(Btree *p, Pgno mxPage){
  Pgno nCurrent;
  if( !p ) return 0;
  nCurrent = (Pgno)prollyBtreeSyntheticPageCount(p);
  if( mxPage>0 ){
    if( mxPage<nCurrent ){
      mxPage = nCurrent;
    }
    p->mxPageCount = mxPage;
  }else if( p->mxPageCount<nCurrent ){
    p->mxPageCount = nCurrent;
  }
  return p->mxPageCount;
}
Pgno sqlite3BtreeMaxPageCount(Btree *p, Pgno mxPage){
  if( !p ) return 0;
  return p->pOps->xMaxPageCount(p, mxPage);
}

Pgno prollyBtreeLastPage(Btree *p){
  return (Pgno)prollyBtreeSyntheticPageCount(p);
}
Pgno sqlite3BtreeLastPage(Btree *p){
  return p->pOps->xLastPage(p);
}

int prollyBtreeSecureDelete(Btree *p, int newFlag){
  (void)p; (void)newFlag;
  return 0;
}
int sqlite3BtreeSecureDelete(Btree *p, int newFlag){
  if( !p ) return 0;
  return p->pOps->xSecureDelete(p, newFlag);
}

int prollyBtreeGetRequestedReserve(Btree *p){
  (void)p;
  return 0;
}
int sqlite3BtreeGetRequestedReserve(Btree *p){
  if( !p ) return 0;
  return p->pOps->xGetRequestedReserve(p);
}

int prollyBtreeGetReserveNoMutex(Btree *p){
  (void)p;
  return 0;
}
int sqlite3BtreeGetReserveNoMutex(Btree *p){
  if( !p ) return 0;
  return p->pOps->xGetReserveNoMutex(p);
}

int sqlite3BtreeIsDoltliteFormat(Btree *p){
  return p && p->pOps==&prollyBtreeOps;
}

int prollyBtreeSetAutoVacuum(Btree *p, int autoVacuum){
  /* Prolly storage has no page freelist; accept auto_vacuum as a no-op. */
  (void)p; (void)autoVacuum;
  return SQLITE_OK;
}
int sqlite3BtreeSetAutoVacuum(Btree *p, int autoVacuum){
  if( !p ) return SQLITE_OK;
  return p->pOps->xSetAutoVacuum(p, autoVacuum);
}

int prollyBtreeGetAutoVacuum(Btree *p){
  (void)p;
  return BTREE_AUTOVACUUM_NONE;
}
int sqlite3BtreeGetAutoVacuum(Btree *p){
  if( !p ) return BTREE_AUTOVACUUM_NONE;
  return p->pOps->xGetAutoVacuum(p);
}

int prollyBtreeIncrVacuum(Btree *p){
  /* No freelist pages to reclaim: report completion immediately, exactly
  ** like stock on a database whose auto_vacuum is none. */
  (void)p;
  return SQLITE_DONE;
}
int sqlite3BtreeIncrVacuum(Btree *p){
  if( !p ) return SQLITE_DONE;
  return p->pOps->xIncrVacuum(p);
}

const char *prollyBtreeGetFilename(Btree *p){
  return chunkStoreFilename(&p->pBt->store);
}
const char *sqlite3BtreeGetFilename(Btree *p){
  if( !p ) return "";
  return p->pOps->xGetFilename(p);
}

const char *prollyBtreeGetJournalname(Btree *p){
  (void)p;
  return "";
}
const char *sqlite3BtreeGetJournalname(Btree *p){
  if( !p ) return "";
  return p->pOps->xGetJournalname(p);
}

int prollyBtreeIsReadonly(Btree *p){
  return p->isDetached || (p->pBt->btsFlags & BTS_READ_ONLY) ? 1 : 0;
}
int sqlite3BtreeIsReadonly(Btree *p){
  if( !p ) return 0;
  return p->pOps->xIsReadonly(p);
}


int prollyBtreeTxnState(Btree *p){
  return (int)p->inTrans;
}
SQLITE_NOINLINE int sqlite3BtreeTxnState(Btree *p){
  if( p==0 ) return TRANS_NONE;
  return p->pOps->xTxnState(p);
}

int prollyBtreeCreateTable(Btree *p, Pgno *piTable, int flags){
  struct TableEntry *pTE;
  Pgno iTable;

  assert( p!=0 && piTable!=0 );
  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }
  PROLLY_ASSERT_GRAPH_LOCKED(p->pBt);

  {
    int rc = ensureStatementSavepointsCaptured(p);
    if( rc!=SQLITE_OK ) return rc;
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

int prollyBtreeDropTable(Btree *p, int iTable, int *piMoved){
  BtShared *pBt = p->pBt;

  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  {
    int rc = ensureStatementSavepointsCaptured(p);
    if( rc!=SQLITE_OK ) return rc;
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

  /* Hand the dropped table's pending edits to any open savepoint that captured
  ** it (the path flushPendingForTable/clearTable use), so ROLLBACK TO can
  ** restore a table created and dropped within the same transaction. Otherwise
  ** catRemove frees the map outright and the pre-drop rows are lost. */
  {
    int rc = syncBtreeSavepoints(p);
    if( rc!=SQLITE_OK ) return rc;
  }
  {
    struct TableEntry *pTE = findTable(p, (Pgno)iTable);
    if( pTE && pTE->pPending ){
      ProllyMutMap *pFlushMap = 0;
      int captured = 0;
      int rc = snapshotPendingForFlush(p, (Pgno)iTable,
                                       (ProllyMutMap**)&pTE->pPending,
                                       &pFlushMap, &captured);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  /* Clear any cursor aliases of the dropped table's pending mutmap before
  ** catRemove frees it; otherwise rollback (prollyBtreeRollback) would
  ** later iterate live cursors and dereference a freed map. */
  refreshCursorMutMapAliases(p, pBt, (Pgno)iTable, 0);
  removeTable(p, (Pgno)iTable);

  if( piMoved ) *piMoved = 0;
  return SQLITE_OK;
}
int sqlite3BtreeDropTable(Btree *p, int iTable, int *piMoved){
  if( !p ) return SQLITE_OK;
  return p->pOps->xDropTable(p, iTable, piMoved);
}

int prollyBtreeClearTable(Btree *p, int iTable, i64 *pnChange){
  BtShared *pBt = p->pBt;
  struct TableEntry *pTE;

  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }

  {
    int rc = ensureStatementSavepointsCaptured(p);
    if( rc!=SQLITE_OK ) return rc;
  }

  pTE = findTable(p, (Pgno)iTable);
  if( !pTE ){
    if( pnChange ) *pnChange = 0;
    return SQLITE_OK;
  }
  if( iTable==1 ){
    if( pnChange ) *pnChange = 0;
    p->bMasterRootChangedTxn = 0;
    return SQLITE_OK;
  }

  /* Save, don't abort, cursors on this table before mutating it: the truncate
  ** optimization (DELETE with no WHERE) can fire while another statement scans
  ** the table, and that scan must restore against the now-empty tree and
  ** continue rather than fail with SQLITE_ABORT. */
  {
    int rc = saveAllCursors(p, pBt, (Pgno)iTable, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  prollyInvalidateIncrblobCursors(pBt, (Pgno)iTable, 0, 1);
  {
    int rc = syncBtreeSavepoints(p);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( pnChange ){
    /* Count pending rows too; same-transaction writes may not be flushed. */
    int rc = flushPendingForTable(p, pBt, pTE, 1);
    if( rc!=SQLITE_OK ) return rc;
    rc = countTreeEntries(p, (Pgno)iTable, pnChange);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* TRUNCATE discards pending rows, preserving savepoint rollback state. */
  if( pTE->pPending ){
    ProllyMutMap *pMap = (ProllyMutMap*)pTE->pPending;
    ProllyMutMap *pFlushMap = pMap;
    int captured = 0;
    int rc = snapshotPendingForFlush(p, (Pgno)iTable,
                                     (ProllyMutMap**)&pTE->pPending,
                                     &pFlushMap, &captured);
    if( rc!=SQLITE_OK ) return rc;
    if( captured ){
      refreshCursorMutMapAliases(p, pBt, (Pgno)iTable,
                                 (ProllyMutMap*)pTE->pPending);
    }else{
      prollyMutMapFree(pMap);
      sqlite3_free(pMap);
      pTE->pPending = 0;
      refreshCursorMutMapAliases(p, pBt, (Pgno)iTable, 0);
    }
  }
  memset(&pTE->root, 0, sizeof(ProllyHash));

  return SQLITE_OK;
}
int sqlite3BtreeClearTable(Btree *p, int iTable, i64 *pnChange){
  if( !p ) return SQLITE_OK;
  return p->pOps->xClearTable(p, iTable, pnChange);
}

int prollyBtCursorClearTableOfCursor(BtCursor *pCur){
  return sqlite3BtreeClearTable(pCur->pBtree, (int)pCur->pgnoRoot, 0);
}
int sqlite3BtreeClearTableOfCursor(BtCursor *pCur){
  if( !pCur ) return SQLITE_OK;
  return pCur->pCurOps->xClearTableOfCursor(pCur);
}

void prollyBtreeGetMeta(Btree *p, int idx, u32 *pValue){
  BtShared *pBt = p->pBt;
  assert( idx>=0 && idx<SQLITE_N_BTREE_META );

  if( idx==BTREE_DATA_VERSION ){
    if( pBt->pPagerShim ){
      *pValue = pBt->pPagerShim->iDataVersion + p->iBDataVersion;
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

int prollyBtreeUpdateMeta(Btree *p, int idx, u32 value){
  if( p->inTrans!=TRANS_WRITE ){
    return SQLITE_ERROR;
  }
  if( idx<1 || idx>=SQLITE_N_BTREE_META ){
    return SQLITE_ERROR;
  }

  {
    /* Btree savepoints push lazily at write time; a meta write is a write,
    ** so push them here or ROLLBACK TO cannot restore the old value. */
    int rc = syncBtreeSavepoints(p);
    if( rc==SQLITE_OK ) rc = ensureStatementSavepointsCaptured(p);
    if( rc!=SQLITE_OK ) return rc;
  }

  p->aMeta[idx] = value;

  if( idx==BTREE_SCHEMA_VERSION ){
    p->bSchemaChangedTxn = 1;
    p->bMasterRootChangedTxn = 1;
    btreeBumpLocalDataVersion(p);
  }

  return SQLITE_OK;
}
int sqlite3BtreeUpdateMeta(Btree *p, int idx, u32 value){
  if( !p ) return SQLITE_OK;
  return p->pOps->xUpdateMeta(p, idx, value);
}

void sqlite3BtreeMarkMasterRootChanged(Btree *p){
  if( p && p->pOps==&prollyBtreeOps ){
    p->bSchemaChangedTxn = 1;
    p->bMasterRootChangedTxn = 1;
  }
}

void *prollyBtreeSchema(Btree *p, int nBytes, void (*xFree)(void*)){
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

int prollyBtreeSchemaLocked(Btree *p){
  (void)p;
  return 0;
}
int sqlite3BtreeSchemaLocked(Btree *p){
  if( !p ) return 0;
  return p->pOps->xSchemaLocked(p);
}

int prollyBtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  (void)p; (void)iTab; (void)isWriteLock;
  return SQLITE_OK;
}
#ifndef SQLITE_OMIT_SHARED_CACHE
int sqlite3BtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  if( !p ) return SQLITE_OK;
  return p->pOps->xLockTable(p, iTab, isWriteLock);
}
#endif

int sqlite3BtreeCursorSize(void){
  return (int)sizeof(BtCursor);
}

void sqlite3BtreeCursorZero(BtCursor *p){
  memset(p, 0, offsetof(BtCursor, pCur));
  memset(&p->pMutMap, 0,
         sizeof(BtCursor) - offsetof(BtCursor, pMutMap));
  p->pCurOps = &prollyCursorOps;
}

int prollyBtreeCursor(
  Btree *p,
  Pgno iTable,
  int wrFlag,
  struct KeyInfo *pKeyInfo,
  BtCursor *pCur
){
  BtShared *pBt = p->pBt;
  struct TableEntry *pTE;

  assert( p->inTrans>=TRANS_READ );

  pCur->pgnoRoot = iTable;
  pCur->pKeyInfo = pKeyInfo;
  pCur->eState = CURSOR_INVALID;
  pCur->pCurOps = &prollyCursorOps;

  pTE = findTable(p, iTable);
  if( !pTE ){
    /* Missing old roots are stale-schema errors; future roots are imports. */
    if( iTable!=1 && iTable < p->cat.iNextTable ){
      sqlite3_log(SQLITE_CORRUPT,
        "doltlite: cursor open on iTable=%u not in catalog "
        "(iNextTable=%u, %s requested); rejecting as CORRUPT. "
        "Likely cause: stale schema cache after a branch switch, "
        "DROP, or concurrent catalog rewrite.",
        (unsigned)iTable, (unsigned)p->cat.iNextTable,
        pKeyInfo ? "BLOBKEY" : "INTKEY");
      return SQLITE_CORRUPT_PGNO(iTable);
    }
    /* The schema root (page 1) is never a dropped user table; an absent
    ** entry just means an empty schema, so synthesize rather than reject. */
    {
      u8 flags = pKeyInfo ? BTREE_BLOBKEY : BTREE_INTKEY;
      pTE = addTable(p, iTable, flags);
      if( !pTE ) return SQLITE_NOMEM;
    }
  }

  pCur->curIntKey = (pTE->flags & BTREE_INTKEY) ? 1 : 0;
  if( !pCur->curIntKey ){
    int rcRoot = SQLITE_OK;
    pCur->isTableRoot = tableEntryIsTableRoot(p, pTE, &rcRoot) ? 1 : 0;
    if( rcRoot!=SQLITE_OK ) return rcRoot;
  }

  pCur->pBtree = p;
  pCur->pBt = pBt;

  if( wrFlag & BTREE_WRCSR ){
    pCur->curFlags = BTCF_WriteFlag;
  }

  if( pTE->pPending ){
    pCur->pMutMap = (ProllyMutMap*)pTE->pPending;
    if( wrFlag & BTREE_WRCSR ){
      pCur->flushSeekEdits = pTE->pendingFlushSeekEdits;
      if( !pCur->curIntKey
       && pCur->isTableRoot
       && !prollyMutMapIsEmpty(pCur->pMutMap) ){
        pCur->flushSeekEdits = 1;
      }
    }else{
      pCur->flushSeekEdits = pTE->pendingFlushSeekEdits;
    }
  }

  prollyCursorInit(&pCur->pCur, &pBt->store, &pBt->cache,
                    &pTE->root, pTE->flags);
  prollyCursorAllowSparse(&pCur->pCur, 1);

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

int prollyBtCursorCloseCursor(BtCursor *pCur){
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
  if( pCur->pSeekSortKey && pCur->pSeekSortKey!=pCur->aSeekSortKey ){
    sqlite3_free(pCur->pSeekSortKey);
  }
  pCur->pSeekSortKey = 0;
  pCur->nSeekSortKeyAlloc = 0;
  if( pCur->pCompareSortKey ){
    sqlite3_free(pCur->pCompareSortKey);
    pCur->pCompareSortKey = 0;
    pCur->nCompareSortKeyAlloc = 0;
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

int prollyBtCursorCursorHasMoved(BtCursor *pCur){
  return (pCur->eState!=CURSOR_VALID);
}
/* -1: handle invalidated (SQL write to its row, or a cursor trip) — abort.
**  1: cursor saved by an unrelated write — caller must re-seek.
**  0: nothing to do; also all orig-engine cursors, whose incrblob cursors
**     never move and restore themselves in accessPayloadChecked. */
int sqlite3BtreeIncrblobCursorReseek(BtCursor *pCur){
  if( pCur->pCurOps!=&prollyCursorOps ) return 0;
  if( pCur->eState==CURSOR_VALID ){
    struct TableEntry *pTE = findTable(pCur->pBtree, pCur->pgnoRoot);
    if( pCur->pMutMap && !prollyMutMapIsEmpty(pCur->pMutMap) ){
      return 1;
    }
    if( pTE && pTE->pPending
     && !prollyMutMapIsEmpty((ProllyMutMap*)pTE->pPending) ){
      return 1;
    }
    return 0;
  }
  if( pCur->eState==CURSOR_FAULT ) return -1;
  return 1;
}

int sqlite3BtreeCursorHasMoved(BtCursor *pCur){
  if( !pCur ) return 0;
  if( pCur->pCurOps==&prollyCursorOps ){
    return pCur->eState!=CURSOR_VALID;
  }
  if( !pCur->pCurOps ) return (pCur->eState!=CURSOR_VALID);
  return pCur->pCurOps->xCursorHasMoved(pCur);
}

int prollyBtCursorCursorRestore(BtCursor *pCur, int *pDifferentRow){
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
int prollyBtreeClosesWithCursor(Btree *p, BtCursor *pCur){
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


void prollyBtreeEnter(Btree *p){ (void)p; }
#ifndef SQLITE_OMIT_SHARED_CACHE
void sqlite3BtreeEnter(Btree *p){
  if( p ) p->pOps->xEnter(p);
}
void sqlite3BtreeEnterAll(sqlite3 *db){
  if( db ){ int i; for(i=0; i<db->nDb; i++){
    Btree *p = db->aDb[i].pBt;
    if( p ) p->pOps->xEnter(p);
  }}
}
/* A prolly btree is never shared, so these answered for that case alone. A
** legacy database delegates to a stock btree that can be sharable, and the
** stock code then expects the whole protocol: answering 0 here suppressed the
** OP_TableLock the code generator would otherwise emit, and the no-op cursor
** mutex left that btree's cursors unguarded. */
int sqlite3BtreeSharable(Btree *p){
  void *pOrig = doltliteBtreeOrigPtr(p);
  return pOrig ? origBtreeSharable(pOrig) : 0;
}
void sqlite3BtreeEnterCursor(BtCursor *pCur){
  if( pCur && pCur->pOrigCursor ) origBtreeEnterCursor(pCur->pOrigCursor);
}
int sqlite3BtreeConnectionCount(Btree *p){
  void *pOrig = doltliteBtreeOrigPtr(p);
  return pOrig ? origBtreeConnectionCount(pOrig) : 1;
}
#endif

void prollyBtreeLeave(Btree *p){ (void)p; }
#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE
void sqlite3BtreeLeave(Btree *p){
  if( p ) p->pOps->xLeave(p);
}
void sqlite3BtreeLeaveCursor(BtCursor *pCur){
  if( pCur && pCur->pOrigCursor ) origBtreeLeaveCursor(pCur->pOrigCursor);
}
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
/* Cross-backend row transfer must use public cursor accessors. */
static int btreeGenericTransferRow(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  int rc;
  BtreePayload x;
  u32 nPayload;
  u8 *pBuf;

  nPayload = sqlite3BtreePayloadSize(pSrc);
  pBuf = (u8*)sqlite3_malloc(nPayload>0 ? (int)nPayload : 1);
  if( pBuf==0 ) return SQLITE_NOMEM;
  rc = sqlite3BtreePayload(pSrc, 0, nPayload, pBuf);
  if( rc!=SQLITE_OK ){ sqlite3_free(pBuf); return rc; }

  memset(&x, 0, sizeof(x));
  if( pDest->curIntKey ){
    x.nKey = iKey;
    x.pData = pBuf;
    x.nData = (int)nPayload;
  }else{
    x.pKey = pBuf;
    x.nKey = (int)nPayload;
  }
  rc = sqlite3BtreeInsert(pDest, &x, 0, 0);
  sqlite3_free(pBuf);
  return rc;
}

int sqlite3BtreeTransferRow(BtCursor *pDest, BtCursor *pSrc, i64 iKey){
  if( pDest->pCurOps != pSrc->pCurOps ){
    return btreeGenericTransferRow(pDest, pSrc, iKey);
  }
  return pDest->pCurOps->xTransferRow(pDest, pSrc, iKey);
}

void prollyBtCursorClearCursor(BtCursor *pCur){
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

struct Pager *prollyBtreePager(Btree *p){
  return (struct Pager*)(p->pBt->pPagerShim);
}
struct Pager *sqlite3BtreePager(Btree *p){
  if( !p ) return 0;
  return p->pOps->xPager(p);
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
  if( pBt->store.corruptMidStream ){
    if( pnErr ) *pnErr = 1;
    return SQLITE_OK;
  }
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
  if( rc==SQLITE_OK && p->vc.isMerging ){
    rc = integrityCheckChunkGraph(&ctx, &p->vc.mergeCommitHash);
  }
  if( rc==SQLITE_OK ){
    rc = integrityCheckChunkGraph(&ctx, &p->vc.conflictsCatalogHash);
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
  if( rc!=SQLITE_OK ){
    if( pnErr ) *pnErr = 1;
    if( pzOut ) *pzOut = 0;
    return rc;
  }

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
  if( rc!=SQLITE_OK ){
    /* OP_IntegrityCk reads *pnErr and frees *pzOut even when an error is
    ** returned, so the outputs must be set on every path.  Count the failed
    ** check itself as an error, as the stock btree does for OOM. */
    if( pnErr ) *pnErr = nErr+1;
    if( pzOut ) *pzOut = 0;
    return rc;
  }

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


int sqlite3BtreeCopyFile(Btree *pTo, Btree *pFrom){
  int i;

  /* A legacy VACUUM copies stock pages between two orig btrees; only the prolly
  ** path below has a catalog to rebuild. The bridge lives in backup.c, which is
  ** compiled out with the rest of VACUUM, so the delegation goes with it. */
#ifndef SQLITE_OMIT_VACUUM
  if( pTo->pOrigBtree && pFrom->pOrigBtree ){
    return origBtreeCopyFile(pTo->pOrigBtree, pFrom->pOrigBtree);
  }
#endif

  invalidateCursors(pTo->pBt, 0, SQLITE_ABORT);

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

  btreeBumpLocalDataVersion(pTo);

  return SQLITE_OK;
}

int sqlite3BtreeIsInBackup(Btree *p){
  assert( p );
  assert( sqlite3_mutex_held(p->db->mutex) );
  return p->nBackup!=0;
}

#ifndef SQLITE_OMIT_WAL
int sqlite3BtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  if( !p ) return SQLITE_OK;
  if( p->pOrigBtree ){
    return origBtreeCheckpoint(p->pOrigBtree, eMode, pnLog, pnCkpt);
  }
  if( pnLog ) *pnLog = 0;
  if( pnCkpt ) *pnCkpt = 0;
  if( p->db && AtomicLoad(&p->db->u1.isInterrupted) ){
    return SQLITE_INTERRUPT;
  }
  return sqlite3PagerCheckpoint(sqlite3BtreePager(p), p->db,
                                eMode, pnLog, pnCkpt);
}
#endif

static void doltiteEngineFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zEngine = "prolly";
  (void)argc; (void)argv;
  if( db && db->nDb>0 && db->aDb[0].pBt
   && db->aDb[0].pBt->pOps==&origBtreeVtOps ){
    zEngine = "orig";
  }
  sqlite3_result_text(context, zEngine, -1, SQLITE_STATIC);
}

/* The store behind one specific btree. Maintenance operations act on the
** database they were given, so they resolve their store from its Btree rather
** than from db->aDb[0]. */
ChunkStore *doltliteBtreeChunkStore(Btree *p){
  if( !p || !sqlite3BtreeIsDoltliteFormat(p) || !p->pBt ) return 0;
  return &p->pBt->store;
}

void doltliteBtreeBackupStart(Btree *p){
  if( p ) p->nBackup++;
}

void doltliteBtreeBackupFinish(Btree *p){
  assert( p && p->nBackup>0 );
  p->nBackup--;
}

void doltliteInvalidateBtreeWorkingState(Btree *p){
  BtShared *pBt = p ? p->pBt : 0;
  if( !pBt ) return;
  pBt->iWorkingStateVersion++;
  if( pBt->iWorkingStateVersion==0 ) pBt->iWorkingStateVersion = 1;
}

ChunkStore *doltliteGetChunkStore(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    Btree *pBt = db->aDb[0].pBt;
    if( sqlite3BtreeUsesOrig(pBt) ) return 0;
    return &pBt->pBt->store;
  }
  return 0;
}

BtShared *doltliteGetBtShared(sqlite3 *db){
  if( db && db->nDb>0 && db->aDb[0].pBt ){
    if( sqlite3BtreeUsesOrig(db->aDb[0].pBt) ) return 0;
    return db->aDb[0].pBt->pBt;
  }
  return 0;
}

int doltliteIsStockSqliteDb(sqlite3 *db){
  return db && db->nDb>0 && db->aDb[0].pBt
      && sqlite3BtreeUsesOrig(db->aDb[0].pBt);
}

ProllyCache *doltliteGetCache(sqlite3 *db){
  BtShared *pBt = doltliteGetBtShared(db);
  if( pBt ) return &pBt->cache;
  return 0;
}


static int registerDoltiteFunctions(sqlite3 *db){
  int rc = sqlite3_create_function(db, "doltlite_engine", 0, SQLITE_UTF8, 0,
                                   doltiteEngineFunc, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteRegister(db);
}

static int doltliteExtInit(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int rc;
  (void)pzErrMsg;
  (void)pApi;
  rc = registerDoltiteFunctions(db);
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
  return SQLITE_OK;
}

int doltliteInstallAutoExt(void){
  return sqlite3_auto_extension((void(*)(void))doltliteExtInit);
}

#endif
