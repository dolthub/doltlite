
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

/* A (from, to) commit pair eligible for diffing. Built up-front in
** dtFilter from a Dolt-matching walk of the commit graph (see
** buildDiffPairs); the cursor iterates through this list, opening one
** ProllyDiffIter per pair. */
typedef struct DiffPair DiffPair;
struct DiffPair {
  /* From side = the older commit being processed. */
  ProllyHash fromHash;
  ProllyHash fromTblRoot;
  ProllyHash fromCatHash;      /* catalog hash at the from commit */
  ProllyHash fromSchemaHash;   /* table's schemaHash at the from commit */
  u8         fromFlags;
  i64        fromDate;
  /* To side = the descendant assigned to this commit by the walk.
  ** zToCommit is either the descendant's hex hash or the literal
  ** "WORKING" for the working-set entry. */
  char       zToCommit[PROLLY_HASH_SIZE*2+1];
  ProllyHash toTblRoot;
  ProllyHash toCatHash;        /* catalog hash for the "to" side */
  ProllyHash toSchemaHash;     /* table's schemaHash on the "to" side */
  u8         toFlags;
  i64        toDate;
};

/*
** Streaming cursor: pre-builds the list of (from,to) diff pairs from a
** Dolt-matching graph walk in dtFilter, then opens one ProllyDiffIter
** per pair lazily and yields rows from each in sequence.
*/
typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;

  /* Pairs to diff, in iteration order. */
  DiffPair *aPairs;
  int nPairs;
  int iPair;                    /* Index of the pair currently being diffed */
  int pairsDone;                /* 1 when iPair has passed the end */

  /* Diff iterator for the current commit pair */
  ProllyDiffIter diffIter;
  int diffIterOpen;             /* 1 if diffIter is currently open */

  /* Cached column-name lists for the current pair's from/to sides.
  ** Loaded in openNextPairIter when the pair's schemas differ, used
  ** by advanceToNextRow to filter out MODIFY changes that only touch
  ** columns outside the intersection of the two schemas (e.g. a
  ** DROP COLUMN commit where the remaining user-visible columns are
  ** unchanged). NULL when schemas are identical. */
  char **azFromCols;
  int    nFromCols;
  char **azToCols;
  int    nToCols;
  int    needFilter;            /* 1 when azFromCols/azToCols are live */

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

/* Working-set vs HEAD diff is no longer a separate phase: it's the
** first entry produced by buildDiffPairs (cmHashToTblInfo[HEAD] is
** seeded with the working catalog as the "to" side, and HEAD's own
** processCommit step emits the (HEAD, WORKING) pair if they differ). */

/* Per-commit "to-side" info recorded in the cmHashToTblInfo map
** during the graph walk. Each commit, when reached, looks up its
** entry to know which descendant commit it should be diffed against
** (and against which version of the table). Mirrors Dolt's
** TblInfoAtCommit struct. */
typedef struct CmTblInfo CmTblInfo;
struct CmTblInfo {
  ProllyHash key;
  ProllyHash tblRoot;
  ProllyHash catHash;       /* catalog hash at the commit that wrote this entry */
  ProllyHash schemaHash;    /* table's schemaHash at that commit */
  u8         flags;
  i64        date;
  char       zHexName[PROLLY_HASH_SIZE*2+1];
};

/* Linear-search map operations. The walk visits each commit once,
** so n is bounded by the size of the commit graph; for that range
** linear search beats hash-table overhead. Returns the index of the
** existing entry if found, or -1 otherwise. */
static int mapFind(const CmTblInfo *aMap, int nMap, const ProllyHash *pKey){
  int i;
  for(i=0; i<nMap; i++){
    if( prollyHashCompare(&aMap[i].key, pKey)==0 ) return i;
  }
  return -1;
}

/* Insert or overwrite an entry in the cmHashToTblInfo map. Mirrors
** Dolt's `dps.cmHashToTblInfo[h] = newInfo` behavior, where the most
** recent processCommit call wins. */
static int mapPut(CmTblInfo **paMap, int *pnMap, const ProllyHash *pKey,
                  const ProllyHash *pTblRoot,
                  const ProllyHash *pCatHash,
                  const ProllyHash *pSchemaHash,
                  u8 flags,
                  const char *zHexName, i64 date){
  int idx = mapFind(*paMap, *pnMap, pKey);
  CmTblInfo *e;
  if( idx<0 ){
    CmTblInfo *aNew = sqlite3_realloc(*paMap,
                          (*pnMap+1)*(int)sizeof(CmTblInfo));
    if( !aNew ) return SQLITE_NOMEM;
    *paMap = aNew;
    e = &aNew[*pnMap];
    memset(e, 0, sizeof(*e));
    e->key = *pKey;
    (*pnMap)++;
  }else{
    e = &(*paMap)[idx];
  }
  e->tblRoot = *pTblRoot;
  e->catHash = *pCatHash;
  e->schemaHash = *pSchemaHash;
  e->flags = flags;
  e->date = date;
  memcpy(e->zHexName, zHexName, PROLLY_HASH_SIZE*2+1);
  return SQLITE_OK;
}

/* Append a (from,to) pair to the cursor's diff-pair list. */
static int pairsAppend(DiffTblCursor *pCur,
                       const ProllyHash *pFromHash,
                       const ProllyHash *pFromTblRoot,
                       const ProllyHash *pFromCatHash,
                       const ProllyHash *pFromSchemaHash,
                       u8 fromFlags, i64 fromDate,
                       const char *zToHex,
                       const ProllyHash *pToTblRoot,
                       const ProllyHash *pToCatHash,
                       const ProllyHash *pToSchemaHash,
                       u8 toFlags, i64 toDate){
  DiffPair *aNew, *r;
  aNew = sqlite3_realloc(pCur->aPairs,
                         (pCur->nPairs+1)*(int)sizeof(DiffPair));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aPairs = aNew;
  r = &aNew[pCur->nPairs++];
  memset(r, 0, sizeof(*r));
  r->fromHash       = *pFromHash;
  r->fromTblRoot    = *pFromTblRoot;
  r->fromCatHash    = *pFromCatHash;
  r->fromSchemaHash = *pFromSchemaHash;
  r->fromFlags      = fromFlags;
  r->fromDate       = fromDate;
  memcpy(r->zToCommit, zToHex, PROLLY_HASH_SIZE*2+1);
  r->toTblRoot      = *pToTblRoot;
  r->toCatHash      = *pToCatHash;
  r->toSchemaHash   = *pToSchemaHash;
  r->toFlags        = toFlags;
  r->toDate         = toDate;
  return SQLITE_OK;
}

/* Helper: load a commit's catalog and look up the named table's root,
** flags, and schemaHash. Sets outputs to zero if the table doesn't
** exist at this commit. */
static int loadTblRootAtCommit(sqlite3 *db, const ProllyHash *pCatHash,
                               const char *zTableName,
                               ProllyHash *pTblRoot, u8 *pFlags,
                               ProllyHash *pSchemaHash){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int rc;
  struct TableEntry *pEntry;
  memset(pTblRoot, 0, sizeof(*pTblRoot));
  *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(*pSchemaHash));
  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = doltliteFindTableByName(aTables, nTables, zTableName);
  if( pEntry ){
    memcpy(pTblRoot, &pEntry->root, sizeof(ProllyHash));
    *pFlags = pEntry->flags;
    if( pSchemaHash ){
      memcpy(pSchemaHash, &pEntry->schemaHash, sizeof(ProllyHash));
    }
  }
  sqlite3_free(aTables);
  return SQLITE_OK;
}

/* Walk the commit graph from HEAD using the same algorithm as Dolt's
** dolt_diff_<table>:
**
**   1. Initialize cmHashToTblInfo[HEAD] = {name: "WORKING", tblRoot:
**      working catalog's root for this table}.
**   2. DFS-LIFO walk via a stack: push parents in forward order, pop
**      from the END (so the LAST parent is visited first).
**   3. For each visited commit C: read cmHashToTblInfo[C] (its assigned
**      descendant info, set by some prior step). If the descendant's
**      tblRoot differs from C's tblRoot, record a (from=C, to=desc)
**      diff pair. Then overwrite cmHashToTblInfo[parent] = C's info
**      for every parent of C (last writer wins).
**
** This produces the exact (commit, parent) attribution Dolt produces
** for merge commits — the merge edge "absorbs" first-parent diffs that
** would otherwise be redundant. See diff_table.go:processCommit /
** commit_itr.go:Next in the dolt repo for the canonical algorithm. */
static int buildDiffPairs(DiffTblCursor *pCur, sqlite3 *db,
                          const char *zTableName){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  ProllyHash workingCat, workingTblRoot;
  u8 workingFlags = 0;
  CmTblInfo *aMap = 0;
  int nMap = 0;
  ProllyHash *aStack = 0;
  int nStack = 0;
  ProllyHash *aAdded = 0;  /* commits already pushed to stack */
  int nAdded = 0;
  int currInited = 0;
  ProllyHash curr;
  int rc = SQLITE_OK;
  int i;

  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;

  /* Initialize cmHashToTblInfo[HEAD] = {name:"WORKING", working root}. */
  memset(&workingCat, 0, sizeof(workingCat));
  memset(&workingTblRoot, 0, sizeof(workingTblRoot));
  rc = doltliteFlushCatalogToHash(db, &workingCat);
  if( rc!=SQLITE_OK ) return rc;
  {
    ProllyHash workingSchemaHash;
    rc = loadTblRootAtCommit(db, &workingCat, zTableName,
                             &workingTblRoot, &workingFlags,
                             &workingSchemaHash);
    if( rc!=SQLITE_OK ) return rc;

    {
      char zWorking[PROLLY_HASH_SIZE*2+1];
      memset(zWorking, 0, sizeof(zWorking));
      memcpy(zWorking, "WORKING", 7);
      rc = mapPut(&aMap, &nMap, &headHash, &workingTblRoot,
                  &workingCat, &workingSchemaHash,
                  workingFlags, zWorking, 0);
      if( rc!=SQLITE_OK ) goto walk_done;
    }
  }

  /* Mark HEAD as added so we don't push it twice if a parent edge
  ** ever cycles back. Initial curr = HEAD; the iter equivalent
  ** returns it before pushing any parents. */
  {
    ProllyHash *aN = sqlite3_realloc(aAdded, (nAdded+1)*(int)sizeof(ProllyHash));
    if( !aN ){ rc = SQLITE_NOMEM; goto walk_done; }
    aAdded = aN;
    aAdded[nAdded++] = headHash;
  }
  curr = headHash;
  currInited = 1;

  while( currInited ){
    DoltliteCommit commit;
    ProllyHash curTblRoot;
    ProllyHash curSchemaHash;
    u8 curFlags = 0;
    char curHex[PROLLY_HASH_SIZE*2+1];
    int idx;

    memset(&commit, 0, sizeof(commit));
    memset(&curTblRoot, 0, sizeof(curTblRoot));
    memset(&curSchemaHash, 0, sizeof(curSchemaHash));

    rc = doltliteLoadCommit(db, &curr, &commit);
    if( rc!=SQLITE_OK ) break;

    rc = loadTblRootAtCommit(db, &commit.catalogHash, zTableName,
                             &curTblRoot, &curFlags, &curSchemaHash);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      break;
    }

    doltliteHashToHex(&curr, curHex);

    /* processCommit: compare against to-info, emit pair if either
    ** the table's data root OR its schema DDL hash differs. Relying
    ** on rootsDiffer alone misses schema-only changes (e.g. ALTER
    ** TABLE DROP COLUMN on an empty table) where the prolly tree
    ** root is unchanged but the user-visible schema isn't. */
    idx = mapFind(aMap, nMap, &curr);
    if( idx>=0 ){
      CmTblInfo *info = &aMap[idx];
      int rootsDiffer = (prollyHashCompare(&info->tblRoot, &curTblRoot)!=0);
      int schemasDiffer = (prollyHashCompare(&info->schemaHash, &curSchemaHash)!=0);
      if( rootsDiffer || schemasDiffer ){
        u8 fromFlags = curFlags;
        if( fromFlags==0 ) fromFlags = info->flags;
        rc = pairsAppend(pCur, &curr, &curTblRoot, &commit.catalogHash,
                         &curSchemaHash, fromFlags, commit.timestamp,
                         info->zHexName, &info->tblRoot, &info->catHash,
                         &info->schemaHash, info->flags, info->date);
        if( rc!=SQLITE_OK ){
          doltliteCommitClear(&commit);
          break;
        }
      }
    }
    /* If idx<0 the commit was reached without an assigned to-info;
    ** that shouldn't happen for a connected graph rooted at HEAD,
    ** so silently skip without emitting. */

    /* For each parent: overwrite cmHashToTblInfo[parent] with curr's
    ** info (last writer wins). */
    {
      int nParents = commit.nParents>0
                       ? commit.nParents
                       : (prollyHashIsEmpty(&commit.parentHash) ? 0 : 1);
      for(i=0; i<nParents; i++){
        const ProllyHash *pParent = commit.nParents>0
                                       ? &commit.aParents[i]
                                       : &commit.parentHash;
        rc = mapPut(&aMap, &nMap, pParent, &curTblRoot, &commit.catalogHash,
                    &curSchemaHash, curFlags, curHex, commit.timestamp);
        if( rc!=SQLITE_OK ) break;
      }
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        break;
      }

      /* Push parents (those not yet added) onto the stack in forward
      ** order so popping from the END visits the LAST parent first.
      ** This matches Dolt's CommitItrForRoots LIFO ordering. */
      for(i=0; i<nParents; i++){
        const ProllyHash *pParent = commit.nParents>0
                                       ? &commit.aParents[i]
                                       : &commit.parentHash;
        int already = 0;
        int j;
        for(j=0; j<nAdded; j++){
          if( prollyHashCompare(&aAdded[j], pParent)==0 ){ already = 1; break; }
        }
        if( already ) continue;
        {
          ProllyHash *aN = sqlite3_realloc(aAdded,
                              (nAdded+1)*(int)sizeof(ProllyHash));
          if( !aN ){ rc = SQLITE_NOMEM; break; }
          aAdded = aN;
          aAdded[nAdded++] = *pParent;
        }
        {
          ProllyHash *aN = sqlite3_realloc(aStack,
                              (nStack+1)*(int)sizeof(ProllyHash));
          if( !aN ){ rc = SQLITE_NOMEM; break; }
          aStack = aN;
          aStack[nStack++] = *pParent;
        }
      }
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;

    /* Pop the next commit from the END of the stack. */
    if( nStack==0 ){
      currInited = 0;
    }else{
      curr = aStack[nStack-1];
      nStack--;
    }
  }

walk_done:
  sqlite3_free(aMap);
  sqlite3_free(aStack);
  sqlite3_free(aAdded);
  return rc;
}

/* Free the cached per-pair column name lists. */
static void freePairCols(DiffTblCursor *pCur){
  int i;
  if( pCur->azFromCols ){
    for(i=0; i<pCur->nFromCols; i++) sqlite3_free(pCur->azFromCols[i]);
    sqlite3_free(pCur->azFromCols);
    pCur->azFromCols = 0;
  }
  pCur->nFromCols = 0;
  if( pCur->azToCols ){
    for(i=0; i<pCur->nToCols; i++) sqlite3_free(pCur->azToCols[i]);
    sqlite3_free(pCur->azToCols);
    pCur->azToCols = 0;
  }
  pCur->nToCols = 0;
  pCur->needFilter = 0;
}

/* Load the column names for zTableName at the commit whose catalog
** hash is pCatHash. Returns the names in *pazOut / *pnOut. The
** caller owns the returned array. Implementation:
**   1. loadSchemaFromCatalog → find the CREATE TABLE DDL for zTableName
**   2. exec the DDL in a throwaway in-memory sqlite db
**   3. PRAGMA table_info to extract column names in declaration order
** Returns SQLITE_NOTFOUND if the table doesn't exist at this commit. */
static int loadColNamesAtCatalog(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  char ***pazOut,
  int *pnOut
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  SchemaEntry *aSchemas = 0;
  int nSchemas = 0;
  SchemaEntry *pEntry;
  sqlite3 *tmp = 0;
  sqlite3_stmt *pStmt = 0;
  char *zPragma = 0;
  char **az = 0;
  int n = 0, alloc = 0;
  int rc;

  *pazOut = 0;
  *pnOut = 0;
  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_NOTFOUND;

  rc = loadSchemaFromCatalog(db, cs, pCache, pCatHash, &aSchemas, &nSchemas);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = findSchemaEntry(aSchemas, nSchemas, zTableName);
  if( !pEntry || !pEntry->zSql ){
    freeSchemaEntries(aSchemas, nSchemas);
    return SQLITE_NOTFOUND;
  }

  rc = sqlite3_open(":memory:", &tmp);
  if( rc!=SQLITE_OK ) goto cleanup;
  rc = sqlite3_exec(tmp, pEntry->zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto cleanup;

  zPragma = sqlite3_mprintf("PRAGMA table_info(\"%w\")", zTableName);
  if( !zPragma ){ rc = SQLITE_NOMEM; goto cleanup; }
  rc = sqlite3_prepare_v2(tmp, zPragma, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) goto cleanup;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    if( n>=alloc ){
      int newAlloc = alloc ? alloc*2 : 8;
      char **aNew = sqlite3_realloc(az, newAlloc*(int)sizeof(char*));
      if( !aNew ){ rc = SQLITE_NOMEM; break; }
      az = aNew;
      alloc = newAlloc;
    }
    az[n] = sqlite3_mprintf("%s", zName ? zName : "");
    if( !az[n] ){ rc = SQLITE_NOMEM; break; }
    n++;
  }

cleanup:
  if( pStmt ) sqlite3_finalize(pStmt);
  sqlite3_free(zPragma);
  if( tmp ) sqlite3_close(tmp);
  freeSchemaEntries(aSchemas, nSchemas);
  if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
    int k;
    for(k=0; k<n; k++) sqlite3_free(az[k]);
    sqlite3_free(az);
    return rc;
  }
  *pazOut = az;
  *pnOut = n;
  return SQLITE_OK;
}

/* Decode a signed integer from `nBytes` bytes of a SQLite serial-type
** body (serial types 1..6). Sign-extends from the top bit. */
static i64 sdReadInt(const u8 *p, int nBytes){
  i64 v;
  int i;
  if( nBytes<=0 ) return 0;
  v = (p[0] & 0x80) ? -1 : 0;
  for(i=0; i<nBytes; i++) v = (v<<8) | p[i];
  return v;
}

/* Compare two record fields by semantic value rather than raw bytes.
** Returns 1 if equal, 0 otherwise. Handles the case where the same
** integer value is encoded with different serial types on each side
** (e.g. literal 1 = type 9 with 0 body bytes vs 1-byte int = type 1
** with 1 body byte). That happens naturally when doltlite re-encodes
** records after an ALTER TABLE, so two semantically-equal rows can
** have byte-different records. */
static int fieldValuesEqual(
  int aType, const u8 *pA, int nA, int aOff,
  int bType, const u8 *pB, int nB, int bOff
){
  i64 ai, bi;
  int aLen, bLen;

  /* NULL vs NULL */
  if( aType==0 && bType==0 ) return 1;
  if( aType==0 || bType==0 ) return 0;

  /* Both sides are integer-valued (1..6, 8, 9) */
  {
    int aIsInt = (aType>=1 && aType<=6) || aType==8 || aType==9;
    int bIsInt = (bType>=1 && bType<=6) || bType==8 || bType==9;
    if( aIsInt && bIsInt ){
      if( aType==8 )      ai = 0;
      else if( aType==9 ) ai = 1;
      else{
        aLen = dlSerialTypeLen(aType);
        if( aOff<0 || aOff+aLen>nA ) return 0;
        ai = sdReadInt(pA+aOff, aLen);
      }
      if( bType==8 )      bi = 0;
      else if( bType==9 ) bi = 1;
      else{
        bLen = dlSerialTypeLen(bType);
        if( bOff<0 || bOff+bLen>nB ) return 0;
        bi = sdReadInt(pB+bOff, bLen);
      }
      return ai==bi;
    }
  }

  /* Everything else: require matching serial type and byte-identical
  ** body bytes. This covers floats (type 7), blobs (>=12 even), and
  ** text (>=13 odd). */
  if( aType != bType ) return 0;
  aLen = dlSerialTypeLen(aType);
  if( aLen<0 ) return 0;
  if( aOff<0 || aOff+aLen>nA ) return 0;
  if( bOff<0 || bOff+aLen>nB ) return 0;
  return memcmp(pA+aOff, pB+bOff, aLen)==0;
}

static int changeIsSchemaOnly(
  const u8 *pFromRec, int nFromRec,
  const u8 *pToRec,   int nToRec,
  char **azFromCols,  int nFromCols,
  char **azToCols,    int nToCols
){
  DoltliteRecordInfo fromRi, toRi;
  int i;

  if( !pFromRec || nFromRec<=0 || !pToRec || nToRec<=0 ) return 0;
  doltliteParseRecord(pFromRec, nFromRec, &fromRi);
  doltliteParseRecord(pToRec,   nToRec,   &toRi);

  /* 1. Every column in TO that's shared with FROM (by name) must
  **    have the same value semantically.
  ** 2. Every column in TO that's NEW (not in FROM) must be NULL in
  **    the to-side record. A non-NULL value in a new column means
  **    the row was actually modified after the ADD COLUMN.
  ** 3. Every column in FROM that's DROPPED (not in TO) must have
  **    been NULL on the from side. A non-NULL value that the user
  **    subsequently dropped is a real data change — the row's
  **    user-visible state went from "has column X = v" to "no
  **    column X at all". Dolt treats that as modified; we must too. */
  for(i=0; i<nToCols; i++){
    int fromIdx;
    for(fromIdx=0; fromIdx<nFromCols; fromIdx++){
      if( strcmp(azFromCols[fromIdx], azToCols[i])==0 ) break;
    }
    if( fromIdx>=nFromCols ){
      /* column only in to (new column) — must be NULL or the change
      ** is real. */
      if( i<toRi.nField && toRi.aType[i]!=0 ) return 0;
      continue;
    }
    if( i>=toRi.nField ){
      if( fromIdx<fromRi.nField && fromRi.aType[fromIdx]!=0 ) return 0;
      continue;
    }
    if( fromIdx>=fromRi.nField ){
      if( toRi.aType[i]!=0 ) return 0;
      continue;
    }
    if( !fieldValuesEqual(
            fromRi.aType[fromIdx], pFromRec, nFromRec, fromRi.aOffset[fromIdx],
            toRi.aType[i],         pToRec,   nToRec,   toRi.aOffset[i]) ){
      return 0;
    }
  }
  /* Check dropped columns: any column in FROM but not in TO, with
  ** a non-NULL value, means real data was lost. */
  for(i=0; i<nFromCols; i++){
    int toIdx;
    for(toIdx=0; toIdx<nToCols; toIdx++){
      if( strcmp(azToCols[toIdx], azFromCols[i])==0 ) break;
    }
    if( toIdx<nToCols ) continue;   /* shared — already handled */
    if( i>=fromRi.nField ) continue; /* implicitly NULL */
    if( fromRi.aType[i]!=0 ) return 0; /* dropped non-NULL value */
  }
  return 1;
}

/* Open a ProllyDiffIter for the next pair in aPairs[iPair] and
** advance iPair. Sets pairsDone=1 when the list is exhausted.
** If the pair's from/to schemaHash differ, also loads the column
** name lists for both sides so advanceToNextRow can filter
** schema-only record-encoding changes. */
static int openNextPairIter(DiffTblCursor *pCur, sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  DiffTblVtab *pVtab = (DiffTblVtab*)pCur->base.pVtab;
  int rc;

  /* Reset per-pair cached column lists from the previous pair. */
  freePairCols(pCur);

  if( !cs ) return SQLITE_OK;

  if( pCur->iPair >= pCur->nPairs ){
    pCur->pairsDone = 1;
    return SQLITE_OK;
  }

  {
    DiffPair *p = &pCur->aPairs[pCur->iPair++];
    u8 flags = p->fromFlags ? p->fromFlags : p->toFlags;
    doltliteHashToHex(&p->fromHash, pCur->row.zFromCommit);
    pCur->row.fromDate = p->fromDate;
    memcpy(pCur->row.zToCommit, p->zToCommit, PROLLY_HASH_SIZE*2+1);
    pCur->row.toDate = p->toDate;
    rc = prollyDiffIterOpen(&pCur->diffIter, cs, pCache,
                            &p->fromTblRoot, &p->toTblRoot, flags);
    if( rc!=SQLITE_OK ) return rc;
    pCur->diffIterOpen = 1;

    /* If the table's schema changed between from and to, load both
    ** column-name lists so advanceToNextRow can filter out MODIFY
    ** changes that only reflect the schema's record encoding.
    ** Failure to load column lists is non-fatal — we fall back to
    ** emitting all changes, which may produce false positives for
    ** DROP COLUMN but is strictly safer than a query error. */
    if( prollyHashCompare(&p->fromSchemaHash, &p->toSchemaHash)!=0 ){
      int rc2;
      rc2 = loadColNamesAtCatalog(db, &p->fromCatHash, pVtab->zTableName,
                                  &pCur->azFromCols, &pCur->nFromCols);
      if( rc2==SQLITE_OK ){
        rc2 = loadColNamesAtCatalog(db, &p->toCatHash, pVtab->zTableName,
                                    &pCur->azToCols, &pCur->nToCols);
      }
      if( rc2==SQLITE_OK ){
        pCur->needFilter = 1;
      }else{
        freePairCols(pCur);
      }
    }
    return SQLITE_OK;
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

        /* When the pair's schemas differ (ADD/DROP/RENAME column
        ** between these commits), a MODIFY change may be a pure
        ** schema-encoding artifact: the record bytes differ but
        ** every user-visible column shared by both schemas has the
        ** same value. Filter those out so they don't show up as
        ** spurious modifications. */
        if( pCur->needFilter
         && pChange->type==PROLLY_DIFF_MODIFY
         && changeIsSchemaOnly(pChange->pOldVal, pChange->nOldVal,
                               pChange->pNewVal, pChange->nNewVal,
                               pCur->azFromCols, pCur->nFromCols,
                               pCur->azToCols,   pCur->nToCols) ){
          continue;
        }

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
    }

    /* Open the next pair, if any. */
    if( pCur->pairsDone ){
      return SQLITE_OK;
    }

    rc = openNextPairIter(pCur, db);
    if( rc!=SQLITE_OK ) return rc;
    (void)zTableName;
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
  freePairCols(c);
  sqlite3_free(c->aPairs);
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
  freePairCols(c);
  sqlite3_free(c->aPairs);
  c->aPairs = 0;
  c->nPairs = 0;
  c->iPair = 0;
  c->pairsDone = 0;
  c->hasRow = 0;
  c->iRowid = 0;

  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    void *pBt = doltliteGetBtShared(db);
    if( !cs || !pBt ){
      c->pairsDone = 1;
      return SQLITE_OK;
    }
  }

  /* Pre-build the (from,to) diff pairs from a Dolt-matching graph
  ** walk. The first pair (when present) is the working-set diff
  ** against HEAD; subsequent pairs are commits walked DFS-LIFO with
  ** descendant attribution overwritten on each step. */
  rc = buildDiffPairs(c, db, pVtab->zTableName);
  if( rc!=SQLITE_OK ) return rc;
  if( c->nPairs==0 ){
    c->pairsDone = 1;
    return SQLITE_OK;
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
