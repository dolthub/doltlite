
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <assert.h>
#include <string.h>
#include <time.h>



static char *buildDiffSchema(DoltliteColInfo *ci){
  int i;
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  char *zColName;
  if( !pStr ) return 0;
  /* Column order matches Dolt's dolt_diff_<table>: all to_* columns
  ** first, then all from_* columns, then commit metadata, then
  ** diff_type. The opposite order would still be byte-equivalent
  ** but produces visibly different `SELECT *` output, which is the
  ** primary user-facing surface. */
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  for(i=0; i<ci->nCol; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, ", ");
    zColName = sqlite3_mprintf("to_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !zColName ){
      sqlite3_str_reset(pStr);
      return 0;
    }
    if( doltliteAppendQuotedIdent(pStr, zColName)!=SQLITE_OK ){
      sqlite3_free(zColName);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(zColName);
  }
  sqlite3_str_appendall(pStr, ", to_commit TEXT, to_commit_date TEXT");
  for(i=0; i<ci->nCol; i++){
    sqlite3_str_appendall(pStr, ", ");
    zColName = sqlite3_mprintf("from_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !zColName ){
      sqlite3_str_reset(pStr);
      return 0;
    }
    if( doltliteAppendQuotedIdent(pStr, zColName)!=SQLITE_OK ){
      sqlite3_free(zColName);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(zColName);
  }
  sqlite3_str_appendall(pStr, ", from_commit TEXT, from_commit_date TEXT"
                              ", diff_type TEXT)");
  z = sqlite3_str_finish(pStr);
  return z;
}

/* A single row of diff output — holds copies of value data so it survives
** cursor movement within the prolly tree. */
typedef struct AuditRow AuditRow;
struct AuditRow {
  u8 diffType;
  i64 intKey;
  u8 *pOldVal; int nOldVal;
  u8 *pNewVal; int nNewVal;
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

/*
** Streaming cursor: walks commit history lazily and yields one diff row
** at a time via a ProllyDiffIter for each commit pair.
*/
typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;

  /* Commit walk state. Instead of a linear first-parent advance,
  ** the cursor pre-builds the full set of commits reachable from
  ** HEAD via BFS through all parent edges (so merge commits
  ** contribute both branches' history). dtFilter populates aWalk;
  ** the cursor walks through it sequentially. */
  ProllyHash *aWalk;            /* Commits to visit, in walk order */
  int nWalk;                    /* Number of commits in aWalk */
  int iWalk;                    /* Index of the commit currently being diffed */
  int commitWalkDone;           /* 1 when iWalk has passed the end */
  int workingPhaseDone;         /* 1 once the working-vs-HEAD diff has been emitted */

  /* Diff iterator for the current commit pair */
  ProllyDiffIter diffIter;
  int diffIterOpen;             /* 1 if diffIter is currently open */

  /* Current row data */
  AuditRow row;
  int hasRow;                   /* 1 if row contains valid data */
  i64 iRowid;                   /* Monotonically increasing rowid */
};

static int diffRecordField(
  const u8 *pData,
  int nData,
  int iField,
  int *pType,
  int *pOff
){
  const u8 *p = pData;
  const u8 *pEnd = pData + nData;
  const u8 *pHdrEnd;
  u64 hdrSize;
  int hdrBytes;
  int off;
  int i;

  if( !pData || nData < 1 || iField < 0 ) return SQLITE_CORRUPT;
  hdrBytes = dlReadVarint(p, pEnd, &hdrSize);
  if( hdrBytes <= 0 || hdrSize < (u64)hdrBytes || hdrSize > (u64)nData ){
    return SQLITE_CORRUPT;
  }
  p += hdrBytes;
  pHdrEnd = pData + (int)hdrSize;
  off = (int)hdrSize;
  for(i=0; p < pHdrEnd; i++){
    u64 st;
    int stBytes = dlReadVarint(p, pHdrEnd, &st);
    int len;
    if( stBytes <= 0 ) return SQLITE_CORRUPT;
    p += stBytes;
    len = dlSerialTypeLen(st);
    if( off < 0 || off + len > nData ) return SQLITE_CORRUPT;
    if( i==iField ){
      *pType = (int)st;
      *pOff = off;
      return SQLITE_OK;
    }
    off += len;
  }
  return SQLITE_NOTFOUND;
}

static void clearAuditRow(AuditRow *r){
  sqlite3_free(r->pOldVal);
  sqlite3_free(r->pNewVal);
  memset(r, 0, sizeof(*r));
}

static void closeDiffIter(DiffTblCursor *pCur){
  if( pCur->diffIterOpen ){
    prollyDiffIterClose(&pCur->diffIter);
    pCur->diffIterOpen = 0;
  }
}

/* Open a working-set vs HEAD diff iterator for the named table.
** The "to" side is the live working catalog; the "from" side is
** HEAD's catalog. Sets row.zToCommit to "WORKING" and row.zFromCommit
** to HEAD's commit hash so the user can filter on either. Returns
** SQLITE_OK with diffIterOpen=1 on success, or SQLITE_OK with
** diffIterOpen=0 if there's nothing to diff (empty working or
** identical to HEAD). */
static int openWorkingDiffIter(DiffTblCursor *pCur, sqlite3 *db,
                               const char *zTableName){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash workingCatHash, headCatHash;
  ProllyHash workingRoot, headRoot;
  ProllyHash headHash;
  u8 flags = 0, headFlags = 0;
  int rc;

  if( !cs ) return SQLITE_OK;

  memset(&workingCatHash, 0, sizeof(workingCatHash));
  memset(&headCatHash, 0, sizeof(headCatHash));
  memset(&workingRoot, 0, sizeof(workingRoot));
  memset(&headRoot, 0, sizeof(headRoot));

  rc = doltliteFlushCatalogToHash(db, &workingCatHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return rc;

  /* Find the table's root in working and in HEAD. */
  {
    struct TableEntry *aTables = 0;
    int nTables = 0;
    rc = doltliteLoadCatalog(db, &workingCatHash, &aTables, &nTables, 0);
    if( rc==SQLITE_OK ){
      doltliteFindTableRootByName(aTables, nTables, zTableName,
                                  &workingRoot, &flags);
    }
    sqlite3_free(aTables);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( !prollyHashIsEmpty(&headCatHash) ){
    struct TableEntry *aTables = 0;
    int nTables = 0;
    rc = doltliteLoadCatalog(db, &headCatHash, &aTables, &nTables, 0);
    if( rc==SQLITE_OK ){
      doltliteFindTableRootByName(aTables, nTables, zTableName,
                                  &headRoot, &headFlags);
    }
    sqlite3_free(aTables);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( flags==0 ) flags = headFlags;

  /* Same root → no working diff to emit. */
  if( prollyHashCompare(&workingRoot, &headRoot)==0 ) return SQLITE_OK;

  /* Set up commit metadata: to_commit="WORKING", from_commit=HEAD hash. */
  memcpy(pCur->row.zToCommit, "WORKING", 7);
  pCur->row.zToCommit[7] = 0;
  pCur->row.toDate = 0;
  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    memset(pCur->row.zFromCommit, '0', PROLLY_HASH_SIZE*2);
    pCur->row.zFromCommit[PROLLY_HASH_SIZE*2] = 0;
    pCur->row.fromDate = 0;
  }else{
    DoltliteCommit headCommit;
    memset(&headCommit, 0, sizeof(headCommit));
    doltliteHashToHex(&headHash, pCur->row.zFromCommit);
    if( doltliteLoadCommit(db, &headHash, &headCommit)==SQLITE_OK ){
      pCur->row.fromDate = headCommit.timestamp;
    }
    doltliteCommitClear(&headCommit);
  }

  rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                          &headRoot, &workingRoot, flags);
  if( rc==SQLITE_OK ) pCur->diffIterOpen = 1;
  return rc;
}

/* BFS-walk all commits reachable from HEAD via every parent edge,
** populating aWalk with the visit order. Used by dtFilter to build
** the full per-commit walk before iteration starts. The walk
** matches what dolt_log shows so dolt_diff_<table> visits every
** commit in the branch's history, not just the first-parent
** chain. */
static int buildWalkList(DiffTblCursor *pCur, sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  ProllyHash *aQueue = 0;
  int nQueue = 0, qHead = 0;
  ProllyHash *aSeen = 0;
  int nSeen = 0;
  int rc = SQLITE_OK;

  if( !cs ) return SQLITE_OK;
  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;

  aQueue = sqlite3_malloc((int)sizeof(ProllyHash));
  if( !aQueue ) return SQLITE_NOMEM;
  aQueue[0] = headHash;
  nQueue = 1;

  while( qHead<nQueue ){
    ProllyHash hash = aQueue[qHead++];
    DoltliteCommit commit;
    int seen = 0;
    int i;

    for(i=0; i<nSeen; i++){
      if( prollyHashCompare(&aSeen[i], &hash)==0 ){ seen = 1; break; }
    }
    if( seen ) continue;
    {
      ProllyHash *aNewSeen = sqlite3_realloc(aSeen,
          (nSeen+1)*(int)sizeof(ProllyHash));
      if( !aNewSeen ){ rc = SQLITE_NOMEM; break; }
      aSeen = aNewSeen;
      aSeen[nSeen++] = hash;
    }
    {
      ProllyHash *aNewWalk = sqlite3_realloc(pCur->aWalk,
          (pCur->nWalk+1)*(int)sizeof(ProllyHash));
      if( !aNewWalk ){ rc = SQLITE_NOMEM; break; }
      pCur->aWalk = aNewWalk;
      pCur->aWalk[pCur->nWalk++] = hash;
    }

    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &hash, &commit);
    if( rc!=SQLITE_OK ) break;

    {
      int nParents;
      int p;
      nParents = commit.nParents>0
                   ? commit.nParents
                   : (prollyHashIsEmpty(&commit.parentHash) ? 0 : 1);
      for(p=0; p<nParents; p++){
        ProllyHash *aNewQ = sqlite3_realloc(aQueue,
            (nQueue+1)*(int)sizeof(ProllyHash));
        if( !aNewQ ){ rc = SQLITE_NOMEM; break; }
        aQueue = aNewQ;
        if( commit.nParents>0 ){
          aQueue[nQueue++] = commit.aParents[p];
        }else{
          aQueue[nQueue++] = commit.parentHash;
        }
      }
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
  }

  sqlite3_free(aQueue);
  sqlite3_free(aSeen);
  return rc;
}

/*
** Open a ProllyDiffIter for the next commit in aWalk[iWalk]
** compared against its first parent. Advances iWalk; sets
** commitWalkDone=1 when the walk is exhausted. Returns SQLITE_OK
** on success.
*/
static int openNextCommitPairIter(DiffTblCursor *pCur, sqlite3 *db,
                                  const char *zTableName){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  int rc;

  if( !cs ) return SQLITE_OK;

  for(;;){
    DoltliteCommit commit, parentCommit;
    ProllyHash curRoot, parentRoot;
    ProllyHash firstParent;
    u8 flags = 0;
    int hasParent = 0;

    if( pCur->iWalk >= pCur->nWalk ){
      pCur->commitWalkDone = 1;
      return SQLITE_OK;
    }

    memset(&commit, 0, sizeof(commit));
    memset(&parentCommit, 0, sizeof(parentCommit));
    memset(&curRoot, 0, sizeof(curRoot));
    memset(&parentRoot, 0, sizeof(parentRoot));
    memset(&firstParent, 0, sizeof(firstParent));

    rc = doltliteLoadCommit(db, &pCur->aWalk[pCur->iWalk], &commit);
    if( rc!=SQLITE_OK ) return rc;

    /* Load current commit's catalog and find this table's root. */
    {
      struct TableEntry *aTables = 0; int nTables = 0;
      rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }
      doltliteFindTableRootByName(aTables, nTables, zTableName, &curRoot, &flags);
      sqlite3_free(aTables);
    }

    doltliteHashToHex(&pCur->aWalk[pCur->iWalk], pCur->row.zToCommit);
    pCur->row.toDate = commit.timestamp;

    /* First parent: aParents[0] for merge commits, parentHash for
    ** legacy single-parent commits, none for root commits. */
    if( commit.nParents>0 ){
      memcpy(&firstParent, &commit.aParents[0], sizeof(ProllyHash));
      hasParent = 1;
    }else if( !prollyHashIsEmpty(&commit.parentHash) ){
      memcpy(&firstParent, &commit.parentHash, sizeof(ProllyHash));
      hasParent = 1;
    }

    pCur->iWalk++;

    if( hasParent ){
      rc = doltliteLoadCommit(db, &firstParent, &parentCommit);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }

      doltliteHashToHex(&firstParent, pCur->row.zFromCommit);
      pCur->row.fromDate = parentCommit.timestamp;

      {
        struct TableEntry *aPT = 0; int nPT = 0;
        rc = doltliteLoadCatalog(db, &parentCommit.catalogHash, &aPT, &nPT, 0);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&parentCommit);
          doltliteCommitClear(&commit);
          return rc;
        }
        doltliteFindTableRootByName(aPT, nPT, zTableName, &parentRoot, 0);
        sqlite3_free(aPT);
      }
      doltliteCommitClear(&parentCommit);

      if( prollyHashCompare(&parentRoot, &curRoot)==0 ){
        /* No diff for this table at this commit — skip and try
        ** the next entry in the walk list. */
        doltliteCommitClear(&commit);
        continue;
      }

      rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                              &parentRoot, &curRoot, flags);
      if( rc==SQLITE_OK ) pCur->diffIterOpen = 1;
      doltliteCommitClear(&commit);
      return rc;
    }

    /* Root commit (no parent) — diff against an empty root so
    ** every row appears as added. */
    if( !prollyHashIsEmpty(&curRoot) ){
      ProllyHash emptyRoot;
      memset(&emptyRoot, 0, sizeof(emptyRoot));
      memset(pCur->row.zFromCommit, '0', PROLLY_HASH_SIZE*2);
      pCur->row.zFromCommit[PROLLY_HASH_SIZE*2] = 0;
      pCur->row.fromDate = 0;

      rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                              &emptyRoot, &curRoot, flags);
      if( rc==SQLITE_OK ) pCur->diffIterOpen = 1;
      doltliteCommitClear(&commit);
      return rc;
    }

    /* Root commit with no table — nothing to diff, skip. */
    doltliteCommitClear(&commit);
    continue;
  }
}

/*
** Try to produce the next row. Steps the current diff iterator; if
** exhausted, moves to the next commit pair. Sets hasRow=1 if a row
** is available, or leaves hasRow=0 if all diffs are exhausted.
*/
static int advanceToNextRow(DiffTblCursor *pCur, sqlite3 *db,
                            const char *zTableName){
  int rc;

  pCur->hasRow = 0;

  for(;;){
    if( pCur->diffIterOpen ){
      ProllyDiffChange *pChange = 0;
      rc = prollyDiffIterStep(&pCur->diffIter, &pChange);
      if( rc==SQLITE_ROW && pChange ){
        u8 *pOldVal = 0;
        u8 *pNewVal = 0;
        /* Free previous value copies (preserves commit-pair metadata) */
        sqlite3_free(pCur->row.pOldVal);
        sqlite3_free(pCur->row.pNewVal);
        pCur->row.pOldVal = 0;
        pCur->row.nOldVal = 0;
        pCur->row.pNewVal = 0;
        pCur->row.nNewVal = 0;

        pCur->row.diffType = pChange->type;
        pCur->row.intKey = pChange->intKey;

        /* Copy value data — the iterator's copies are valid until next Step */
        if( pChange->pOldVal && pChange->nOldVal > 0 ){
          pOldVal = sqlite3_malloc(pChange->nOldVal);
          if( !pOldVal ) return SQLITE_NOMEM;
          memcpy(pOldVal, pChange->pOldVal, pChange->nOldVal);
        }

        if( pChange->pNewVal && pChange->nNewVal > 0 ){
          pNewVal = sqlite3_malloc(pChange->nNewVal);
          if( !pNewVal ){
            sqlite3_free(pOldVal);
            return SQLITE_NOMEM;
          }
          memcpy(pNewVal, pChange->pNewVal, pChange->nNewVal);
        }

        pCur->row.pOldVal = pOldVal;
        pCur->row.nOldVal = pChange->pOldVal ? pChange->nOldVal : 0;
        pCur->row.pNewVal = pNewVal;
        pCur->row.nNewVal = pChange->pNewVal ? pChange->nNewVal : 0;

        pCur->hasRow = 1;
        pCur->iRowid++;
        return SQLITE_OK;
      }
      if( rc!=SQLITE_DONE && rc!=SQLITE_ROW ){
        /* Error from the iterator */
        return rc;
      }
      /* This iter is exhausted */
      closeDiffIter(pCur);

      /* If the working-phase iter just finished, transition to the
      ** commit walk by starting at HEAD. */
      if( !pCur->workingPhaseDone ){
        pCur->workingPhaseDone = 1;
        rc = openNextCommitPairIter(pCur, db, zTableName);
        if( rc!=SQLITE_OK ) return rc;
        continue;
      }
    }

    /* Try the next commit pair from the BFS walk list. */
    if( pCur->commitWalkDone ){
      return SQLITE_OK;
    }

    rc = openNextCommitPairIter(pCur, db, zTableName);
    if( rc!=SQLITE_OK ) return rc;
  }
}

static int dtConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DiffTblVtab *pVtab;
  int rc;
  const char *zModName;
  char *zSchema;
  (void)pAux; (void)pzErr;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;


  zModName = argv[0];
  if( zModName && strncmp(zModName, "dolt_diff_", 10)==0 ){
    pVtab->zTableName = sqlite3_mprintf("%s", zModName + 10);
  }else if( argc > 3 ){
    pVtab->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    pVtab->zTableName = sqlite3_mprintf("");
  }


  rc = doltliteLoadUserTableColumns(db, pVtab->zTableName, &pVtab->cols, pzErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }
  zSchema = buildDiffSchema(&pVtab->cols);

  if( !zSchema ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return SQLITE_NOMEM;
  }

  rc = sqlite3_declare_vtab(db, zSchema);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zTableName);
    doltliteFreeColInfo(&pVtab->cols);
    sqlite3_free(pVtab);
    return rc;
  }

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int dtDisconnect(sqlite3_vtab *pBase){
  DiffTblVtab *pVtab = (DiffTblVtab*)pBase;
  sqlite3_free(pVtab->zTableName);
  doltliteFreeColInfo(&pVtab->cols);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int dtBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10000.0;
  return SQLITE_OK;
}

static int dtOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  DiffTblCursor *c; (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dtClose(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  closeDiffIter(c);
  clearAuditRow(&c->row);
  sqlite3_free(c->aWalk);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dtFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  sqlite3 *db = pVtab->db;
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  /* Reset state */
  closeDiffIter(c);
  clearAuditRow(&c->row);
  sqlite3_free(c->aWalk);
  c->aWalk = 0;
  c->nWalk = 0;
  c->iWalk = 0;
  c->commitWalkDone = 0;
  c->workingPhaseDone = 0;
  c->hasRow = 0;
  c->iRowid = 0;

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    void *pBt = doltliteGetBtShared(db);
    if( !cs || !pBt ){
      c->commitWalkDone = 1;
      c->workingPhaseDone = 1;
      return SQLITE_OK;
    }
  }

  /* Pre-build the BFS walk list of all reachable commits. */
  rc = buildWalkList(c, db);
  if( rc!=SQLITE_OK ) return rc;
  if( c->nWalk==0 ){
    c->commitWalkDone = 1;
    c->workingPhaseDone = 1;
    return SQLITE_OK;
  }

  /* Phase 1: working-set diff vs HEAD. Emits rows with
  ** to_commit="WORKING" first, matching Dolt's dolt_diff_<table>
  ** behavior of including uncommitted changes at the head. If
  ** there are no working changes, openWorkingDiffIter is a no-op
  ** and we fall through to the commit walk. */
  rc = openWorkingDiffIter(c, db, pVtab->zTableName);
  if( rc!=SQLITE_OK ) return rc;
  if( !c->diffIterOpen ){
    c->workingPhaseDone = 1;
    rc = openNextCommitPairIter(c, db, pVtab->zTableName);
    if( rc!=SQLITE_OK ) return rc;
  }

  return advanceToNextRow(c, db, pVtab->zTableName);
}

static int dtNext(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *pVtab = (DiffTblVtab*)cur->pVtab;
  return advanceToNextRow(c, pVtab->db, pVtab->zTableName);
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

  /* Schema layout (matches Dolt):
  **   [0      .. nCols-1   ]  to_<col_i>      (from pNewVal)
  **   [nCols              ]  to_commit
  **   [nCols+1            ]  to_commit_date
  **   [nCols+2 .. 2*nCols+1]  from_<col_i>    (from pOldVal)
  **   [2*nCols+2          ]  from_commit
  **   [2*nCols+3          ]  from_commit_date
  **   [2*nCols+4          ]  diff_type
  */
  if( nCols > 0 && col < nCols ){
    /* to_<col>: from pNewVal */
    int colIdx = col;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pNewVal && r->nNewVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pNewVal && r->nNewVal > 0 ){
        int st, off;
        int rc = diffRecordField(r->pNewVal, r->nNewVal, colIdx, &st, &off);
        if( rc==SQLITE_OK ){
          doltliteResultField(ctx, r->pNewVal, r->nNewVal, st, off);
        }else if( rc==SQLITE_NOTFOUND ){
          sqlite3_result_null(ctx);
        }else{
          sqlite3_result_error_code(ctx, rc);
          return rc;
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col == nCols ){
    /* to_commit */
    sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
  }else if( nCols > 0 && col == nCols+1 ){
    /* to_commit_date */
    time_t t = (time_t)r->toDate;
    struct tm *tm = gmtime(&t);
    if(tm){
      char b[32];
      strftime(b, sizeof(b), "%Y-%m-%d %H:%M:%S", tm);
      sqlite3_result_text(ctx, b, -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_null(ctx);
    }
  }else if( nCols > 0 && col < 2*nCols+2 ){
    /* from_<col>: from pOldVal */
    int colIdx = col - nCols - 2;
    if( colIdx == pVtab->cols.iPkCol ){
      if( r->pOldVal && r->nOldVal > 0 ){
        sqlite3_result_int64(ctx, r->intKey);
      }else{
        sqlite3_result_null(ctx);
      }
    }else{
      if( r->pOldVal && r->nOldVal > 0 ){
        int st, off;
        int rc = diffRecordField(r->pOldVal, r->nOldVal, colIdx, &st, &off);
        if( rc==SQLITE_OK ){
          doltliteResultField(ctx, r->pOldVal, r->nOldVal, st, off);
        }else if( rc==SQLITE_NOTFOUND ){
          sqlite3_result_null(ctx);
        }else{
          sqlite3_result_error_code(ctx, rc);
          return rc;
        }
      }else{
        sqlite3_result_null(ctx);
      }
    }
  }else if( nCols > 0 && col == 2*nCols+2 ){
    /* from_commit */
    sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
  }else if( nCols > 0 && col == 2*nCols+3 ){
    /* from_commit_date */
    time_t t = (time_t)r->fromDate;
    struct tm *tm = gmtime(&t);
    if(tm){
      char b[32];
      strftime(b, sizeof(b), "%Y-%m-%d %H:%M:%S", tm);
      sqlite3_result_text(ctx, b, -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_null(ctx);
    }
  }else{
    /* diff_type */
    switch( r->diffType ){
      case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
      case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
      case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
    }
  }

  return SQLITE_OK;
}

static int dtRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DiffTblCursor*)cur)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module diffTableModule = {
  0, dtConnect, dtConnect, dtBestIndex, dtDisconnect, dtDisconnect,
  dtOpen, dtClose, dtFilter, dtNext, dtEof, dtColumn, dtRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterDiffTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_diff_", &diffTableModule);
}

#endif
