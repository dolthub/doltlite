
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

#ifdef _WIN32
static const char *dlWinStrptime(
  const char *zDate,
  const char *zFmt,
  struct tm *pTm
){
  int year = 0, month = 0, day = 0;
  int hour = 0, minute = 0, second = 0;
  char sep = 0;
  int n = 0;

  if( strcmp(zFmt, "%Y-%m-%dT%H:%M:%S")==0 ){
    n = sscanf(zDate, "%d-%d-%dT%d:%d:%d%c",
               &year, &month, &day, &hour, &minute, &second, &sep);
    if( n!=6 ) return 0;
  }else if( strcmp(zFmt, "%Y-%m-%d %H:%M:%S")==0 ){
    n = sscanf(zDate, "%d-%d-%d %d:%d:%d%c",
               &year, &month, &day, &hour, &minute, &second, &sep);
    if( n!=6 ) return 0;
  }else{
    return 0;
  }

  memset(pTm, 0, sizeof(*pTm));
  pTm->tm_year = year - 1900;
  pTm->tm_mon = month - 1;
  pTm->tm_mday = day;
  pTm->tm_hour = hour;
  pTm->tm_min = minute;
  pTm->tm_sec = second;
  pTm->tm_isdst = 0;
  return zDate + sqlite3Strlen30(zDate);
}

static time_t dlWinTimegm(struct tm *pTm){
  return _mkgmtime(pTm);
}
#define strptime dlWinStrptime
#define timegm dlWinTimegm
#endif

extern int doltliteLogRegister(sqlite3 *db);
extern int doltliteStatusRegister(sqlite3 *db);
extern int doltliteDiffRegister(sqlite3 *db);
extern int doltliteSchemasRegister(sqlite3 *db);
extern int doltliteDiffStatRegister(sqlite3 *db);
extern int doltliteBranchRegister(sqlite3 *db);
extern int doltliteConflictsRegister(sqlite3 *db);
extern int doltliteRegisterConflictTables(sqlite3 *db);
extern int doltliteTagRegister(sqlite3 *db);
extern int doltliteGcRegister(sqlite3 *db);
extern int doltliteRegisterDiffTables(sqlite3 *db);
extern int doltliteAncestorRegister(sqlite3 *db);
extern int doltliteAtRegister(sqlite3 *db);
extern int doltliteRegisterAtTables(sqlite3 *db);
extern int doltliteRegisterHistoryTables(sqlite3 *db);
extern int doltliteSchemaDiffRegister(sqlite3 *db);
extern int doltliteRemoteSqlRegister(sqlite3 *db);

extern int doltliteFindAncestor(sqlite3 *db, const ProllyHash *h1,
                                 const ProllyHash *h2, ProllyHash *pAnc);

extern const char *doltliteNextTableForSchema(sqlite3 *db, int *pIdx, Pgno *piTable);
extern void doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH);

int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash);

typedef struct DoltliteTxnState DoltliteTxnState;
struct DoltliteTxnState {
  u8 *pRefsBlob;
  int nRefsBlob;
  ProllyHash refsHash;
  char *zSessionBranch;
  ProllyHash sessionHead;
  ProllyHash sessionStaged;
  ProllyHash sessionMergeCommit;
  ProllyHash sessionConflictsCatalog;
  ProllyHash sessionCatalogHash;
  u8 sessionIsMerging;
};

static void doltliteTxnStateClear(DoltliteTxnState *p){
  sqlite3_free(p->pRefsBlob);
  sqlite3_free(p->zSessionBranch);
  memset(p, 0, sizeof(*p));
}

static int doltliteSaveTxnState(sqlite3 *db, DoltliteTxnState *p){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *pCatalog = 0;
  int nCatalog = 0;
  int rc;

  memset(p, 0, sizeof(*p));
  if( !cs ) return SQLITE_ERROR;

  rc = chunkStoreSerializeRefsToBlob(cs, &p->pRefsBlob, &p->nRefsBlob);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(&p->refsHash, &cs->refsHash, sizeof(ProllyHash));

  p->zSessionBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !p->zSessionBranch ){
    doltliteTxnStateClear(p);
    return SQLITE_NOMEM;
  }
  doltliteGetSessionHead(db, &p->sessionHead);
  doltliteGetSessionStaged(db, &p->sessionStaged);
  doltliteGetSessionMergeState(db, &p->sessionIsMerging,
                               &p->sessionMergeCommit,
                               &p->sessionConflictsCatalog);

  rc = doltliteFlushAndSerializeCatalog(db, &pCatalog, &nCatalog);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(p);
    return rc;
  }
  rc = chunkStorePut(cs, pCatalog, nCatalog, &p->sessionCatalogHash);
  sqlite3_free(pCatalog);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(p);
  }
  return rc;
}

static int doltliteRestoreTxnState(sqlite3 *db, DoltliteTxnState *p){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;

  rc = chunkStoreLoadRefsFromBlob(cs, p->pRefsBlob, p->nRefsBlob);
  if( rc!=SQLITE_OK ) return rc;
  memcpy(&cs->refsHash, &p->refsHash, sizeof(ProllyHash));

  rc = doltliteSwitchCatalog(db, &p->sessionCatalogHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteSetSessionBranch(db, p->zSessionBranch);
  doltliteSetSessionHead(db, &p->sessionHead);
  doltliteSetSessionStaged(db, &p->sessionStaged);
  doltliteSetSessionMergeState(db, p->sessionIsMerging,
                               &p->sessionMergeCommit,
                               &p->sessionConflictsCatalog);
  return SQLITE_OK;
}

/* Commit-race guard. Takes the graph lock, refreshes ref state from
** disk, then verifies the session's head still matches the branch
** tip. If another connection advanced the branch between the start
** of this commit and the lock acquisition, we return SQLITE_BUSY
** and the caller aborts — otherwise we'd silently overwrite the
** concurrent commit. */
static int doltliteRefreshAndConfirmHead(
  sqlite3 *db,
  ChunkStore *cs,
  const ProllyHash *pExpectedHead
){
  const char *zBranch;
  ProllyHash branchTip;
  int rc;

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  if( cs->snapshotPinned ){
    int changed = 0;
    cs->snapshotPinned = 0;
    rc = chunkStoreRefreshIfChanged(cs, &changed);
    cs->snapshotPinned = 1;
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(cs);
      return rc;
    }
  }

  zBranch = doltliteGetSessionBranch(db);
  if( chunkStoreFindBranch(cs, zBranch, &branchTip)==SQLITE_OK
   && prollyHashCompare(&branchTip, pExpectedHead)!=0 ){
    chunkStoreUnlock(cs);
    return SQLITE_BUSY;
  }
  return SQLITE_OK;
}

int doltliteHasUncommittedChanges(sqlite3 *db){
  ProllyHash headCatHash, stagedHash, workingCatHash;
  u8 *wCatData = 0; int nWCat = 0;
  int rc;

  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&headCatHash) ) return 0;


  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash)
   && prollyHashCompare(&headCatHash, &stagedHash)!=0 ){
    return 1;
  }


  {
    ChunkStore *cs = doltliteGetChunkStore(db);
    if( cs ){
      rc = doltliteFlushAndSerializeCatalog(db, &wCatData, &nWCat);
      if( rc==SQLITE_OK ){
        chunkStorePut(cs, wCatData, nWCat, &workingCatHash);
        sqlite3_free(wCatData);
        if( prollyHashCompare(&headCatHash, &workingCatHash)!=0 ){
          return 1;
        }
      }
    }
    return 0;
  }
}

void doltliteUpdateSchemaHashes(sqlite3 *db){
  int idx = 0;
  Pgno iTable;
  const char *zName;
  while( (zName = doltliteNextTableForSchema(db, &idx, &iTable)) != 0 ){
    sqlite3_stmt *pStmt = 0;
    char *zSql = sqlite3_mprintf(
      "SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name='%q'", zName);
    if( zSql ){
      if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK ){
        if( sqlite3_step(pStmt)==SQLITE_ROW ){
          const char *zCreate = (const char*)sqlite3_column_text(pStmt, 0);
          if( zCreate ){
            ProllyHash h;
            prollyHashCompute(zCreate, (int)strlen(zCreate), &h);
            doltliteSetTableSchemaHash(db, iTable, &h);
          }
        }
        sqlite3_finalize(pStmt);
      }
      sqlite3_free(zSql);
    }
  }
}

int doltliteMutateRefs(sqlite3 *db, DoltliteRefsMutation xMutate, void *pArg){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;
  if( cs->snapshotPinned ){
    int changed = 0;
    cs->snapshotPinned = 0;
    rc = chunkStoreRefreshIfChanged(cs, &changed);
    cs->snapshotPinned = 1;
    if( rc!=SQLITE_OK ){
      chunkStoreUnlock(cs);
      return rc;
    }
  }

  rc = xMutate(db, cs, pArg);
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefs(cs);
    if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  }

  chunkStoreUnlock(cs);
  return rc;
}

int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *catData = 0;
  int nCatData = 0;
  int rc;
  rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, catData, nCatData, pHash);
  sqlite3_free(catData);
  return rc;
}

void freeSchemaMergeActions(SchemaMergeAction *a, int n){
  int i, j;
  for(i=0; i<n; i++){
    for(j=0; j<a[i].nAddColumns; j++){
      sqlite3_free(a[i].azAddColumns[j]);
    }
    sqlite3_free(a[i].azAddColumns);
    sqlite3_free(a[i].zTableName);
  }
  sqlite3_free(a);
}

static int doltliteCreateAndStoreCommitWithTime(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
);

static int doltliteCreateAndStoreCommit(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  ProllyHash *pCommitHash
){
  return doltliteCreateAndStoreCommitWithTime(db, pParent, pCatalog, zMessage,
      zAuthorName, zAuthorEmail, aExtraParents, nExtraParents, 0, pCommitHash);
}

static int doltliteCreateAndStoreCommitWithTime(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit c;
  u8 *commitData = 0;
  int nCommitData = 0;
  int rc, i;

  memset(&c, 0, sizeof(c));
  memcpy(&c.parentHash, pParent, sizeof(ProllyHash));
  memcpy(&c.catalogHash, pCatalog, sizeof(ProllyHash));
  c.timestamp = explicitTimestamp ? explicitTimestamp : (i64)time(0);
  c.zName  = sqlite3_mprintf("%s", zAuthorName  ? zAuthorName  : doltliteGetAuthorName(db));
  c.zEmail = sqlite3_mprintf("%s", zAuthorEmail ? zAuthorEmail : doltliteGetAuthorEmail(db));
  c.zMessage = sqlite3_mprintf("%s", zMessage);

  if( nExtraParents > 0 && aExtraParents ){
    c.aParents[0] = *pParent;
    for(i=0; i<nExtraParents && (i+1)<DOLTLITE_MAX_PARENTS; i++){
      c.aParents[i+1] = aExtraParents[i];
    }
    c.nParents = 1 + (nExtraParents < DOLTLITE_MAX_PARENTS-1
                       ? nExtraParents : DOLTLITE_MAX_PARENTS-1);
  }

  rc = doltliteCommitSerialize(&c, &commitData, &nCommitData);
  if( rc==SQLITE_OK ) rc = chunkStorePut(cs, commitData, nCommitData, pCommitHash);
  sqlite3_free(commitData);
  doltliteCommitClear(&c);
  return rc;
}

static int doltliteAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *branch = doltliteGetSessionBranch(db);
  DoltliteTxnState saved;
  int rc;

  rc = doltliteSaveTxnState(db, &saved);
  if( rc!=SQLITE_OK ) return rc;

  if( cs->nBranches==0 ){
    rc = chunkStoreAddBranch(cs, branch, pNewHead);
    if( rc==SQLITE_OK ) rc = chunkStoreSetDefaultBranch(cs, branch);
  }else{
    rc = chunkStoreUpdateBranch(cs, branch, pNewHead);
  }
  if( rc!=SQLITE_OK ){
    int rc2 = doltliteRestoreTxnState(db, &saved);
    doltliteTxnStateClear(&saved);
    return rc2==SQLITE_OK ? rc : rc2;
  }

  doltliteSetSessionHead(db, pNewHead);
  doltliteSetSessionStaged(db, pCatalogHash);

  rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ){
    int rc2 = doltliteRestoreTxnState(db, &saved);
    doltliteTxnStateClear(&saved);
    return rc2==SQLITE_OK ? rc : rc2;
  }

  doltliteTxnStateClear(&saved);
  return SQLITE_OK;
}

static int doltliteReportConflicts(
  sqlite3 *db,
  sqlite3_context *ctx,
  int nConflicts,
  const char *zOp
){
  char msg[256];
  int rc;
  rc = doltliteRegisterConflictTables(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_snprintf(sizeof(msg), msg,
    "%s has %d conflict(s). Resolve and then commit with dolt_commit.",
    zOp, nConflicts);
  sqlite3_result_text(ctx, msg, -1, SQLITE_TRANSIENT);
  return SQLITE_OK;
}

static void doltliteAddFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;
  int i;
  int stageAll = 0;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }
  if( argc==0 ){
    sqlite3_result_error(context, "dolt_add requires table name or '-A'", -1);
    return;
  }



  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-A")==0
     || strcmp(arg, "--all")==0
     || strcmp(arg, ".")==0 ){
      stageAll = 1;
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      return;
    }
  }

  {
    ProllyHash workingHash;
    ProllyHash savedStaged;

    doltliteGetSessionStaged(db, &savedStaged);

    rc = doltliteFlushCatalogToHash(db, &workingHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }

    if( stageAll ){

      doltliteSetSessionStaged(db, &workingHash);
    }else{

      struct TableEntry *aWorking = 0, *aStaged = 0;
      int nWorking = 0, nStaged = 0;
      ProllyHash stagedHash;


      rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(context, "failed to load working catalog", -1);
        return;
      }

      doltliteGetSessionStaged(db, &stagedHash);
      if( prollyHashIsEmpty(&stagedHash) ){

        ProllyHash headCat;
        rc = doltliteGetHeadCatalogHash(db, &headCat);
        if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCat) ){
          rc = doltliteLoadCatalog(db, &headCat, &aStaged, &nStaged, 0);
        }
      }else{
        rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(aWorking);
        sqlite3_result_error(context, "failed to load staged catalog", -1);
        return;
      }

      for(i=0; i<argc; i++){
        const char *zTable = (const char*)sqlite3_value_text(argv[i]);
        Pgno iTable = 0;
        int j;

        if( !zTable || zTable[0]=='-' || strcmp(zTable, ".")==0 ) continue;
        rc = doltliteResolveTableName(db, zTable, &iTable);
        if( rc!=SQLITE_OK ){

          int found = 0;
          for(j=0; j<nStaged; j++){
            if( aStaged[j].zName && strcmp(aStaged[j].zName, zTable)==0 ){
              if( j+1 < nStaged ){
                memmove(&aStaged[j], &aStaged[j+1],
                        (nStaged-j-1) * (int)sizeof(struct TableEntry));
              }
              nStaged--;
              found = 1;
              break;
            }
          }
          if( !found ){
            char *zErr = sqlite3_mprintf(
                "table not found: %s", zTable);
            sqlite3_free(aWorking);
            sqlite3_free(aStaged);
            if( zErr ){
              sqlite3_result_error(context, zErr, -1);
              sqlite3_free(zErr);
            }else{
              sqlite3_result_error_nomem(context);
            }
            return;
          }
          continue;
        }


        for(j=0; j<nWorking; j++){
          if( aWorking[j].iTable==iTable ){

            int k;
            int updated = 0;
            for(k=0; k<nStaged; k++){
              if( aStaged[k].iTable==iTable ){
                aStaged[k] = aWorking[j];
                updated = 1;
                break;
              }
            }
            if( !updated ){

              struct TableEntry *aNew = sqlite3_realloc(aStaged,
                  (nStaged+1)*(int)sizeof(struct TableEntry));
              if( !aNew ){
                sqlite3_free(aWorking);
                sqlite3_free(aStaged);
                sqlite3_result_error_nomem(context);
                return;
              }
              aStaged = aNew;
              aStaged[nStaged] = aWorking[j];
              nStaged++;
            }
            break;
          }
        }
      }


      {
        u8 *buf = 0;
        int nBuf = 0;
        ProllyHash newStagedHash;
        rc = doltliteSerializeCatalogEntries(db, aStaged, nStaged, &buf, &nBuf);
        if( rc!=SQLITE_OK ){
          sqlite3_free(aWorking);
          sqlite3_free(aStaged);
          sqlite3_result_error_code(context, rc);
          return;
        }
        rc = chunkStorePut(cs, buf, nBuf, &newStagedHash);
        sqlite3_free(buf);
        if( rc==SQLITE_OK ){
          doltliteSetSessionStaged(db, &newStagedHash);
        }
      }

      sqlite3_free(aWorking);
      sqlite3_free(aStaged);
    }

    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      doltliteSetSessionStaged(db, &savedStaged);
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  sqlite3_result_int(context, 0);
}

static void doltliteCommitFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zMessage = 0;
  const char *zAuthor = 0;
  const char *zDate = 0;
  int addAll = 0;
  int addModifiedOnly = 0;
  int amend = 0;
  int allowEmpty = 0;
  int skipEmpty = 0;
  ProllyHash commitHash;
  ProllyHash catalogHash;
  ProllyHash sessionHeadBeforeLock;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  i64 explicitTimestamp = 0;
  int rc;
  int i;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }


  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( arg[0]=='-' && arg[1]!='-' && arg[1]!=0 && arg[2]!=0 ){
      int j;
      for(j=1; arg[j]; j++){
        if( arg[j]=='A' ){
          addAll = 1;
        }else if( arg[j]=='a' ){
          addModifiedOnly = 1;
        }else if( arg[j]=='m' ){
          if( arg[j+1]!=0 ){
            zMessage = &arg[j+1];
          }else if( i+1<argc ){
            zMessage = (const char*)sqlite3_value_text(argv[++i]);
          }
          break;
        }
      }
    }else if( strcmp(arg, "-m")==0 && i+1<argc ){
      zMessage = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "--message")==0 && i+1<argc ){
      zMessage = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "--author")==0 && i+1<argc ){
      zAuthor = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "--date")==0 && i+1<argc ){
      zDate = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "--amend")==0 ){
      amend = 1;
    }else if( strcmp(arg, "--allow-empty")==0 ){
      allowEmpty = 1;
    }else if( strcmp(arg, "--skip-empty")==0 ){
      skipEmpty = 1;
    }else if( strcmp(arg, "-A")==0 ){
      addAll = 1;
    }else if( strcmp(arg, "-a")==0 || strcmp(arg, "--all")==0 ){

      addModifiedOnly = 1;
    }
  }

  if( zDate ){
    struct tm tm;
    const char *p;
    memset(&tm, 0, sizeof(tm));
    p = strptime(zDate, "%Y-%m-%dT%H:%M:%S", &tm);
    if( !p ){
      memset(&tm, 0, sizeof(tm));
      p = strptime(zDate, "%Y-%m-%d %H:%M:%S", &tm);
    }
    if( !p ){
      char *zErr = sqlite3_mprintf(
          "could not parse --date `%s` (expected YYYY-MM-DDTHH:MM:SS)", zDate);
      sqlite3_result_error(context, zErr ? zErr : "bad --date", -1);
      sqlite3_free(zErr);
      return;
    }
    explicitTimestamp = (i64)timegm(&tm);
  }

  if( !zMessage || zMessage[0]==0 ){
    sqlite3_result_error(context,
      "dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')", -1);
    return;
  }

  if( addAll ){

    rc = doltliteFlushCatalogToHash(db, &catalogHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }
    doltliteSetSessionStaged(db, &catalogHash);
  }else if( addModifiedOnly ){

    ProllyHash workingHash, headCatHash, stagedHash;
    struct TableEntry *aWorking = 0, *aHead = 0, *aStaged = 0;
    int nWorking = 0, nHead = 0, nStaged = 0;
    int j, k;

    rc = doltliteFlushCatalogToHash(db, &workingHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }
    rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to load working catalog", -1);
      return;
    }
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash) ){
      rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aWorking);
        sqlite3_result_error(context, "failed to load HEAD catalog", -1);
        return;
      }
    }

    doltliteGetSessionStaged(db, &stagedHash);
    if( !prollyHashIsEmpty(&stagedHash) ){
      rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
    }else if( !prollyHashIsEmpty(&headCatHash) ){
      rc = doltliteLoadCatalog(db, &headCatHash, &aStaged, &nStaged, 0);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(aWorking);
      sqlite3_free(aHead);
      sqlite3_result_error(context, "failed to load staged catalog", -1);
      return;
    }


    for(j=0; j<nWorking; j++){
      const char *zName = aWorking[j].zName;
      int inHead = 0;
      int updated = 0;
      for(k=0; k<nHead; k++){
        if( aHead[k].zName && zName && strcmp(aHead[k].zName, zName)==0 ){
          inHead = 1; break;
        }
      }
      if( !inHead ) continue;

      for(k=0; k<nStaged; k++){
        if( aStaged[k].zName && zName && strcmp(aStaged[k].zName, zName)==0 ){
          aStaged[k] = aWorking[j];
          updated = 1;
          break;
        }
      }
      if( !updated ){
        struct TableEntry *aNew = sqlite3_realloc(aStaged,
            (nStaged+1)*(int)sizeof(struct TableEntry));
        if( !aNew ){
          sqlite3_free(aWorking); sqlite3_free(aHead); sqlite3_free(aStaged);
          sqlite3_result_error_nomem(context);
          return;
        }
        aStaged = aNew;
        aStaged[nStaged++] = aWorking[j];
      }
    }


    for(k=0; k<nHead; k++){
      const char *zName = aHead[k].zName;
      int inWorking = 0;
      int j2;
      for(j2=0; j2<nWorking; j2++){
        if( aWorking[j2].zName && zName && strcmp(aWorking[j2].zName, zName)==0 ){
          inWorking = 1; break;
        }
      }
      if( inWorking ) continue;

      for(j2=0; j2<nStaged; j2++){
        if( aStaged[j2].zName && zName && strcmp(aStaged[j2].zName, zName)==0 ){
          if( j2+1<nStaged ){
            memmove(&aStaged[j2], &aStaged[j2+1],
                    (nStaged-j2-1)*(int)sizeof(struct TableEntry));
          }
          nStaged--;
          break;
        }
      }
    }


    if( nStaged==0 ){
      sqlite3_free(aWorking);
      sqlite3_free(aHead);
      sqlite3_free(aStaged);
      sqlite3_result_error(context,
        "nothing to commit, working tree clean (use dolt_add to stage changes)", -1);
      return;
    }

    {
      u8 *buf = 0;
      int nBuf = 0;
      ProllyHash newStagedHash;
      rc = doltliteSerializeCatalogEntries(db, aStaged, nStaged, &buf, &nBuf);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(cs, buf, nBuf, &newStagedHash);
      }
      sqlite3_free(buf);
      if( rc==SQLITE_OK ){
        doltliteSetSessionStaged(db, &newStagedHash);
      }
    }

    sqlite3_free(aWorking);
    sqlite3_free(aHead);
    sqlite3_free(aStaged);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }


  {
    ProllyHash cfHash;
    doltliteGetSessionConflictsCatalog(db, &cfHash);
    if( !prollyHashIsEmpty(&cfHash) ){
      sqlite3_result_error(context,
        "cannot commit: unresolved merge conflicts. Use dolt_conflicts_resolve() first.", -1);
      return;
    }
  }


  doltliteGetSessionStaged(db, &catalogHash);
  if( prollyHashIsEmpty(&catalogHash) ){
    if( allowEmpty ){
      ProllyHash headCatHash;
      rc = doltliteGetHeadCatalogHash(db, &headCatHash);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash) ){
        memcpy(&catalogHash, &headCatHash, sizeof(ProllyHash));
      }else{
        sqlite3_result_error(context,
          "nothing to commit (use dolt_add first, or dolt_commit('-A', '-m', 'msg'))", -1);
        return;
      }
    }else{
      sqlite3_result_error(context,
        "nothing to commit (use dolt_add first, or dolt_commit('-A', '-m', 'msg'))", -1);
      return;
    }
  }


  {
    u8 isMerging = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( !isMerging && !amend ){
      ProllyHash headCatHash;
      rc = doltliteGetHeadCatalogHash(db, &headCatHash);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash)
       && prollyHashCompare(&catalogHash, &headCatHash)==0 ){
        if( allowEmpty ){

        }else if( skipEmpty ){
          sqlite3_result_int(context, 0);
          return;
        }else{
          sqlite3_result_error(context,
            "nothing to commit, working tree clean (use dolt_add to stage changes)", -1);
          return;
        }
      }
    }
  }


  {
    ProllyHash parentHash;
    char *zParsedName = 0, *zParsedEmail = 0;
    char *zTrimmedMessage = 0;
    doltliteGetSessionHead(db, &parentHash);

    if( amend ){
      DoltliteCommit headCommit;
      ProllyHash headHash;
      memset(&headCommit, 0, sizeof(headCommit));
      doltliteGetSessionHead(db, &headHash);
      if( prollyHashIsEmpty(&headHash) ){
        sqlite3_result_error(context,
          "cannot --amend: branch has no commits", -1);
        return;
      }
      rc = doltliteLoadCommit(db, &headHash, &headCommit);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(context,
          "cannot --amend: failed to load HEAD commit", -1);
        return;
      }
      if( headCommit.nParents == 0 ){
        doltliteCommitClear(&headCommit);
        sqlite3_result_error(context,
          "cannot --amend: HEAD has no parent (initial commit)", -1);
        return;
      }

      memcpy(&parentHash, &headCommit.aParents[0], sizeof(ProllyHash));
      if( !zMessage || !*zMessage ){
        zMessage = sqlite3_mprintf("%s",
            headCommit.zMessage ? headCommit.zMessage : "");
      }
      doltliteCommitClear(&headCommit);
    }

    if( zAuthor ){
      const char *lt = strchr(zAuthor, '<');
      const char *gt = lt ? strchr(lt, '>') : 0;
      if( lt && gt ){
        int nameLen = (int)(lt - zAuthor);
        while( nameLen>0 && zAuthor[nameLen-1]==' ' ) nameLen--;
        zParsedName = sqlite3_mprintf("%.*s", nameLen, zAuthor);
        zParsedEmail = sqlite3_mprintf("%.*s", (int)(gt-lt-1), lt+1);
      }else{
        zParsedName = sqlite3_mprintf("%s", zAuthor);
        zParsedEmail = sqlite3_mprintf("");
      }
    }


    {
      const char *pStart = zMessage;
      const char *pEnd;
      int len;
      while( *pStart==' ' || *pStart=='\t'
          || *pStart=='\n' || *pStart=='\r' ) pStart++;
      pEnd = pStart + strlen(pStart);
      while( pEnd>pStart
          && (pEnd[-1]==' ' || pEnd[-1]=='\t'
           || pEnd[-1]=='\n' || pEnd[-1]=='\r') ) pEnd--;
      if( pEnd==pStart ){
        sqlite3_free(zParsedName);
        sqlite3_free(zParsedEmail);
        sqlite3_result_error(context,
          "dolt_commit requires a non-empty message", -1);
        return;
      }
      len = (int)(pEnd - pStart);
      zTrimmedMessage = sqlite3_malloc(len+1);
      if( !zTrimmedMessage ){
        sqlite3_free(zParsedName);
        sqlite3_free(zParsedEmail);
        sqlite3_result_error_code(context, SQLITE_NOMEM);
        return;
      }
      memcpy(zTrimmedMessage, pStart, len);
      zTrimmedMessage[len] = 0;
      zMessage = zTrimmedMessage;
    }

    rc = doltliteCreateAndStoreCommitWithTime(db, &parentHash, &catalogHash,
        zMessage, zParsedName, zParsedEmail, 0, 0, explicitTimestamp, &commitHash);
    sqlite3_free(zTrimmedMessage);
    sqlite3_free(zParsedName);
    sqlite3_free(zParsedEmail);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  doltliteGetSessionHead(db, &sessionHeadBeforeLock);


  rc = doltliteRefreshAndConfirmHead(db, cs, &sessionHeadBeforeLock);
  if( rc==SQLITE_BUSY ){
    sqlite3_result_error(context,
      "commit conflict: another connection committed to this branch. "
      "Please retry your transaction.", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }


  {
    u8 wasMerging = 0;
    doltliteGetSessionMergeState(db, &wasMerging, 0, 0);
    if( wasMerging ){
      doltliteClearSessionMergeState(db);
    }
  }

  rc = doltliteAdvanceBranch(db, &commitHash, &catalogHash);
  chunkStoreUnlock(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  doltliteHashToHex(&commitHash, hexBuf);

  rc = doltliteRegisterDiffTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  rc = doltliteRegisterHistoryTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  rc = doltliteRegisterAtTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}

static void doltliteResetFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash targetCatHash;
  ProllyHash targetCommit;
  ProllyHash preResetHeadCatHash;
  ProllyHash sessionHeadBeforeLock;
  int havePreResetHead = 0;
  int isHard = 0;
  int isSoft = 0;
  int isMixed = 0;
  const char *zRef = 0;
  const char **azPaths = 0;
  int nPaths = 0;
  int rc;
  int i;
  int graphLocked = 0;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }


  if( doltliteGetHeadCatalogHash(db, &preResetHeadCatHash)==SQLITE_OK
   && !prollyHashIsEmpty(&preResetHeadCatHash) ){
    havePreResetHead = 1;
  }


  azPaths = (const char**)sqlite3_malloc(sizeof(char*) * (argc>0?argc:1));
  if( !azPaths ){ sqlite3_result_error_nomem(context); return; }
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--hard")==0 ){ isHard = 1; }
    else if( strcmp(arg, "--soft")==0 ){ isSoft = 1; }
    else if( strcmp(arg, "--mixed")==0 ){ isMixed = 1; }
    else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      sqlite3_result_error(context, zErr ? zErr : "unknown option", -1);
      sqlite3_free(zErr);
      sqlite3_free(azPaths);
      return;
    }
    else if( !zRef ){

      if( isHard || isSoft || isMixed ){
        zRef = arg;
      }else{
        ProllyHash probe;
        if( doltliteResolveRef(db, arg, &probe)==SQLITE_OK ){
          zRef = arg;
        }else{
          azPaths[nPaths++] = arg;
        }
      }
    }
    else{

      azPaths[nPaths++] = arg;
    }
  }


  if( nPaths>0 ){
    struct TableEntry *aHead = 0, *aStaged = 0;
    int nHead = 0, nStaged = 0;
    ProllyHash headCatHash, stagedHash;
    int p;

    if( isHard || isSoft || zRef ){
      sqlite3_result_error(context,
        "table paths cannot be combined with --hard / --soft or a target ref", -1);
      sqlite3_free(azPaths);
      return;
    }

    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_free(azPaths);
      sqlite3_result_error(context, "failed to read HEAD", -1);
      return;
    }
    if( !prollyHashIsEmpty(&headCatHash) ){
      rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_free(azPaths);
        sqlite3_result_error(context, "failed to load HEAD catalog", -1);
        return;
      }
    }

    doltliteGetSessionStaged(db, &stagedHash);
    if( !prollyHashIsEmpty(&stagedHash) ){
      rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aHead);
        sqlite3_free(azPaths);
        sqlite3_result_error(context, "failed to load staged catalog", -1);
        return;
      }
    }

    for(p=0; p<nPaths; p++){
      const char *zTable = azPaths[p];
      int iH;
      int iS = -1;
      int j;


      for(j=0; j<nStaged; j++){
        if( aStaged[j].zName && strcmp(aStaged[j].zName, zTable)==0 ){
          iS = j; break;
        }
      }
      iH = -1;
      for(j=0; j<nHead; j++){
        if( aHead[j].zName && strcmp(aHead[j].zName, zTable)==0 ){
          iH = j; break;
        }
      }

      if( iH<0 && iS<0 ){
        char *zErr = sqlite3_mprintf("table not found: %s", zTable);
        sqlite3_free(aHead); sqlite3_free(aStaged); sqlite3_free(azPaths);
        sqlite3_result_error(context, zErr ? zErr : "table not found", -1);
        sqlite3_free(zErr);
        return;
      }

      if( iH<0 ){

        if( iS+1<nStaged ){
          memmove(&aStaged[iS], &aStaged[iS+1],
                  (nStaged-iS-1)*(int)sizeof(struct TableEntry));
        }
        nStaged--;
      }else if( iS<0 ){

        struct TableEntry *aNew = sqlite3_realloc(aStaged,
            (nStaged+1)*(int)sizeof(struct TableEntry));
        if( !aNew ){
          sqlite3_free(aHead); sqlite3_free(aStaged); sqlite3_free(azPaths);
          sqlite3_result_error_nomem(context);
          return;
        }
        aStaged = aNew;
        aStaged[nStaged] = aHead[iH];
        nStaged++;
      }else{

        aStaged[iS] = aHead[iH];
      }
    }

    {
      u8 *buf = 0;
      int nBuf = 0;
      ProllyHash newStagedHash;
      rc = doltliteSerializeCatalogEntries(db, aStaged, nStaged, &buf, &nBuf);
      if( rc==SQLITE_OK ){
        rc = chunkStorePut(cs, buf, nBuf, &newStagedHash);
      }
      sqlite3_free(buf);
      if( rc==SQLITE_OK ){
        doltliteSetSessionStaged(db, &newStagedHash);
      }
    }

    sqlite3_free(aHead);
    sqlite3_free(aStaged);
    sqlite3_free(azPaths);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    sqlite3_result_int(context, 0);
    return;
  }
  sqlite3_free(azPaths);


  if( isSoft && !zRef ){
    sqlite3_result_int(context, 0);
    return;
  }
  (void)isMixed;

  if( zRef ){
    DoltliteCommit commit;

    rc = doltliteResolveRef(db,zRef, &targetCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "commit not found", -1);
      return;
    }

    rc = doltliteLoadCommit(db, &targetCommit, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to load commit", -1);
      return;
    }
    memcpy(&targetCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);

    doltliteGetSessionHead(db, &sessionHeadBeforeLock);
    rc = doltliteRefreshAndConfirmHead(db, cs, &sessionHeadBeforeLock);
    if( rc==SQLITE_BUSY ){
      sqlite3_result_error(context,
        "reset conflict: another connection moved this branch. "
        "Please retry your transaction.", -1);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    graphLocked = 1;

    doltliteSetSessionHead(db, &targetCommit);
    rc = chunkStoreUpdateBranch(cs, doltliteGetSessionBranch(db), &targetCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }

    doltliteClearSessionMergeState(db);
  }else{
    rc = doltliteGetHeadCatalogHash(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to read HEAD", -1);
      return;
    }
  }

  doltliteSetSessionStaged(db, &targetCatHash);

  if( isHard ){

    ProllyHash origStagedAfterReset;
    memcpy(&origStagedAfterReset, &targetCatHash, sizeof(ProllyHash));

    if( prollyHashIsEmpty(&targetCatHash) ){
      sqlite3_result_error(context, "no commit to reset to", -1);
      goto reset_cleanup;
    }


    /* Preserve tables that exist in working but not in pre-reset HEAD
    ** (user-created since HEAD). dolt_reset --hard targets HEAD's
    ** catalog, but the user probably doesn't want their in-progress
    ** CREATE TABLE silently deleted. Merge those tables into the
    ** target catalog before applying the reset. */
    if( havePreResetHead ){
      struct TableEntry *aHead = 0;
      int nHead = 0;
      int nUntracked = 0;
      char **azUntracked = 0;
      sqlite3_stmt *pStmt = 0;
      int j, k;

      rc = doltliteLoadCatalog(db, &preResetHeadCatHash, &aHead, &nHead, 0);
      if( rc==SQLITE_OK ){
        rc = sqlite3_prepare_v2(db,
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
            -1, &pStmt, 0);
      }
      if( rc==SQLITE_OK ){
        while( sqlite3_step(pStmt)==SQLITE_ROW ){
          const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
          int inHead = 0;
          if( !zName ) continue;
          for(k=0; k<nHead; k++){
            if( aHead[k].zName && strcmp(aHead[k].zName, zName)==0 ){
              inHead = 1; break;
            }
          }
          if( !inHead ){
            char **aNew = sqlite3_realloc(azUntracked,
                (nUntracked+1)*(int)sizeof(char*));
            if( !aNew ){ rc = SQLITE_NOMEM; break; }
            azUntracked = aNew;
            azUntracked[nUntracked++] = sqlite3_mprintf("%s", zName);
          }
        }
        sqlite3_finalize(pStmt);
      }


      if( rc==SQLITE_OK && nUntracked>0 ){
        ProllyHash workingHash;
        struct TableEntry *aWorking = 0, *aTarget = 0;
        int nWorking = 0, nTarget = 0;

        rc = doltliteFlushCatalogToHash(db, &workingHash);
        if( rc==SQLITE_OK ){
          rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
        }
        if( rc==SQLITE_OK ){
          rc = doltliteLoadCatalog(db, &targetCatHash, &aTarget, &nTarget, 0);
        }
        if( rc==SQLITE_OK ){

          for(j=0; j<nWorking; j++){
            int tgtIdx = -1;
            if( aWorking[j].iTable==1 ){

              continue;
            }
            for(k=0; k<nTarget; k++){
              if( aTarget[k].zName && aWorking[j].zName
               && strcmp(aTarget[k].zName, aWorking[j].zName)==0 ){
                tgtIdx = k; break;
              }
            }
            if( tgtIdx>=0 ){
              aWorking[j] = aTarget[tgtIdx];
            }
          }
        }
        if( rc==SQLITE_OK ){
          u8 *buf = 0;
          int nBuf = 0;
          ProllyHash mergedHash;
          rc = doltliteSerializeCatalogEntries(db, aWorking, nWorking, &buf, &nBuf);
          if( rc==SQLITE_OK ){
            rc = chunkStorePut(cs, buf, nBuf, &mergedHash);
          }
          sqlite3_free(buf);
          if( rc==SQLITE_OK ){
            memcpy(&targetCatHash, &mergedHash, sizeof(ProllyHash));
          }
        }
        sqlite3_free(aWorking);
        sqlite3_free(aTarget);
      }

      for(j=0; j<nUntracked; j++) sqlite3_free(azUntracked[j]);
      sqlite3_free(azUntracked);
      sqlite3_free(aHead);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
        goto reset_cleanup;
      }
    }

    rc = doltliteSaveWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
    rc = doltliteHardReset(db, &targetCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "hard reset failed", -1);
      goto reset_cleanup;
    }

    doltliteSetSessionStaged(db, &origStagedAfterReset);
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
  }else{
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto reset_cleanup;
    }
  }

  sqlite3_result_int(context, 0);
reset_cleanup:
  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
}

static void doltliteMergeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch = 0;
  const char *zMessage = 0;
  int isAbort = 0;
  int noFastForward = 0;
  ProllyHash ourHead, theirHead, ancestorHash;
  ProllyHash ourCatHash, theirCatHash, ancCatHash, mergedCatHash;
  DoltliteTxnState savedState;
  int nMergeConflicts = 0;
  DoltliteCommit ourCommit, theirCommit, ancCommit;
  int graphLocked = 0;
  int rc, i;

  memset(&ourCommit, 0, sizeof(ourCommit));
  memset(&theirCommit, 0, sizeof(theirCommit));
  memset(&ancCommit, 0, sizeof(ancCommit));
  memset(&savedState, 0, sizeof(savedState));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(context, "usage: dolt_merge('branch')", -1); return; }


  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "--abort")==0 ){
      isAbort = 1;
    }else if( strcmp(arg, "--no-ff")==0 ){
      noFastForward = 1;
    }else if( strcmp(arg, "-m")==0 || strcmp(arg, "--message")==0 ){
      if( i+1<argc ){
        zMessage = (const char*)sqlite3_value_text(argv[++i]);
      }else{
        sqlite3_result_error(context, "-m requires a message", -1);
        return;
      }
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      return;
    }else if( !zBranch ){
      zBranch = arg;
    }else{
      sqlite3_result_error(context, "too many positional arguments to dolt_merge", -1);
      return;
    }
  }

  if( isAbort ){
    u8 isMerging = 0;
    ProllyHash headCatHash;

    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( !isMerging ){
      sqlite3_result_error(context, "no merge in progress", -1);
      return;
    }


    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to read HEAD", -1);
      return;
    }
    rc = doltliteHardReset(db, &headCatHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "abort reset failed", -1);
      return;
    }


    doltliteSetSessionStaged(db, &headCatHash);


    doltliteClearSessionMergeState(db);
    rc = doltlitePersistWorkingSet(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }

    sqlite3_result_int(context, 0);
    return;
  }

  if( !zBranch ){
    sqlite3_result_error(context, "branch name required", -1);
    return;
  }

  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }


  rc = doltliteResolveRef(db, zBranch, &theirHead);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&theirHead) ){
    sqlite3_result_error(context, "merge source not found", -1);
    return;
  }


  if( prollyHashCompare(&ourHead, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }


  if( doltliteHasUncommittedChanges(db) ){
    sqlite3_result_error(context,
      "uncommitted changes \xe2\x80\x94 commit or reset before merging", -1);
    return;
  }


  rc = doltliteFindAncestor(db, &ourHead, &theirHead, &ancestorHash);
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&ancestorHash) ){
    sqlite3_result_error(context, "no common ancestor found", -1);
    return;
  }


  if( prollyHashCompare(&ancestorHash, &theirHead)==0 ){
    sqlite3_result_text(context, "Already up to date", -1, SQLITE_STATIC);
    return;
  }


  /* Fast-forward merge: ours is an ancestor of theirs, so the merge
  ** reduces to advancing our branch pointer to theirHead without
  ** creating a merge commit. --no-ff forces a merge commit even
  ** when ff would be possible (matches git behavior). */
  if( prollyHashCompare(&ancestorHash, &ourHead)==0 && !noFastForward ){

    char hx[PROLLY_HASH_SIZE*2+1];

    rc = doltliteLoadCommit(db, &theirHead, &theirCommit);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load commit", -1); return; }

    rc = doltliteSaveTxnState(db, &savedState);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&theirCommit);
      sqlite3_result_error_code(context, rc);
      return;
    }

    rc = doltliteRefreshAndConfirmHead(db, cs, &ourHead);
    if( rc==SQLITE_BUSY ){
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&theirCommit);
      sqlite3_result_error(context,
        "merge conflict: another connection committed to this branch. Please retry your transaction.",
        -1);
      return;
    }
    if( rc!=SQLITE_OK ){
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&theirCommit);
      sqlite3_result_error_code(context, rc);
      return;
    }
    graphLocked = 1;

    rc = doltliteSwitchCatalog(db, &theirCommit.catalogHash);
    if( rc==SQLITE_OK ){
      rc = doltliteUpdateBranchWorkingState(db,
          doltliteGetSessionBranch(db), &theirCommit.catalogHash, NULL);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteAdvanceBranch(db, &theirHead, &theirCommit.catalogHash);
    }
    if( graphLocked ){
      chunkStoreUnlock(cs);
      graphLocked = 0;
    }
    if( rc!=SQLITE_OK ){
      int rc2 = doltliteRestoreTxnState(db, &savedState);
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&theirCommit);
      sqlite3_result_error_code(context, rc2==SQLITE_OK ? rc : rc2);
      return;
    }
    doltliteTxnStateClear(&savedState);

    doltliteCommitClear(&theirCommit);

    doltliteHashToHex(&theirHead, hx);
    sqlite3_result_text(context, hx, -1, SQLITE_TRANSIENT);
    return;
  }

  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){ sqlite3_result_error(context, "failed to load our commit", -1); return; }
  memcpy(&ourCatHash, &ourCommit.catalogHash, sizeof(ProllyHash));

  rc = doltliteLoadCommit(db, &theirHead, &theirCommit);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); sqlite3_result_error(context, "failed to load their commit", -1); return; }
  memcpy(&theirCatHash, &theirCommit.catalogHash, sizeof(ProllyHash));

  rc = doltliteLoadCommit(db, &ancestorHash, &ancCommit);
  if( rc!=SQLITE_OK ){ doltliteCommitClear(&ourCommit); doltliteCommitClear(&theirCommit); sqlite3_result_error(context, "failed to load ancestor", -1); return; }
  memcpy(&ancCatHash, &ancCommit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&ancCommit);

  {
    char *zMergeErr = 0;
    SchemaMergeAction *aSchemaActions = 0;
    int nSchemaActions = 0;
    rc = doltliteMergeCatalogs(db, &ancCatHash, &ourCatHash, &theirCatHash,
                                &mergedCatHash, &nMergeConflicts, &zMergeErr,
                                &aSchemaActions, &nSchemaActions);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      if( zMergeErr ){
        sqlite3_result_error(context, zMergeErr, -1);
        sqlite3_free(zMergeErr);
      }else{
        sqlite3_result_error(context, "merge failed", -1);
      }
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      return;
    }
    sqlite3_free(zMergeErr);

    rc = doltliteSaveTxnState(db, &savedState);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      sqlite3_result_error_code(context, rc);
      return;
    }

    rc = doltliteRefreshAndConfirmHead(db, cs, &ourHead);
    if( rc==SQLITE_BUSY ){
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      sqlite3_result_error(context,
        "merge conflict: another connection committed to this branch. Please retry your transaction.",
        -1);
      return;
    }
    if( rc!=SQLITE_OK ){
      doltliteTxnStateClear(&savedState);
      doltliteCommitClear(&ourCommit);
      doltliteCommitClear(&theirCommit);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      sqlite3_result_error_code(context, rc);
      return;
    }
    graphLocked = 1;


    rc = doltliteSwitchCatalog(db, &mergedCatHash);
    if( rc==SQLITE_OK ){
      doltliteSetSessionStaged(db, &mergedCatHash);
      rc = doltliteUpdateBranchWorkingState(db,
          doltliteGetSessionBranch(db), &mergedCatHash, NULL);
    }
    doltliteCommitClear(&ourCommit);
    doltliteCommitClear(&theirCommit);
    if( rc!=SQLITE_OK ){
      int rc2;
      if( graphLocked ){
        chunkStoreUnlock(cs);
        graphLocked = 0;
      }
      rc2 = doltliteRestoreTxnState(db, &savedState);
      doltliteTxnStateClear(&savedState);
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      sqlite3_result_error_code(context, rc2==SQLITE_OK ? rc : rc2);
      return;
    }


    if( nSchemaActions > 0 ){
      int si;
      for(si=0; si<nSchemaActions; si++){
        int sj;
        for(sj=0; sj<aSchemaActions[si].nAddColumns; sj++){
          char *zAlter = sqlite3_mprintf("ALTER TABLE \"%w\" ADD COLUMN %s",
                                          aSchemaActions[si].zTableName,
                                          aSchemaActions[si].azAddColumns[sj]);
          if( zAlter ){
            rc = sqlite3_exec(db, zAlter, 0, 0, 0);
            sqlite3_free(zAlter);
            if( rc!=SQLITE_OK ){
              break;
            }
          }
        }
        if( rc!=SQLITE_OK ) break;
      }
      if( rc==SQLITE_OK ){

        rc = migrateSchemaRowData(db, &ancCatHash, &theirCatHash, aSchemaActions, nSchemaActions);
      }


      if( rc==SQLITE_OK ){
        rc = doltliteFlushCatalogToHash(db, &mergedCatHash);
      }
      freeSchemaMergeActions(aSchemaActions, nSchemaActions);
      if( rc!=SQLITE_OK ){
        int rc2;
        if( graphLocked ){
          chunkStoreUnlock(cs);
          graphLocked = 0;
        }
        rc2 = doltliteRestoreTxnState(db, &savedState);
        doltliteTxnStateClear(&savedState);
        sqlite3_result_error_code(context, rc2==SQLITE_OK ? rc : rc2);
        return;
      }
    }
  }

  if( nMergeConflicts > 0 ){
    rc = doltliteReportConflicts(db, context, nMergeConflicts, "Merge");
    if( graphLocked ){
      chunkStoreUnlock(cs);
      graphLocked = 0;
    }
    if( rc!=SQLITE_OK ){
      int rc2 = doltliteRestoreTxnState(db, &savedState);
      doltliteTxnStateClear(&savedState);
      sqlite3_result_error_code(context, rc2==SQLITE_OK ? rc : rc2);
      return;
    }
    doltliteTxnStateClear(&savedState);
  }else{
    ProllyHash commitHash;
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    char msg[256];

    doltliteSetSessionStaged(db, &mergedCatHash);

    if( zMessage && zMessage[0] ){
      sqlite3_snprintf(sizeof(msg), msg, "%s", zMessage);
    }else{
      snprintf(msg, sizeof(msg), "Merge branch '%s' into %s",
               zBranch, doltliteGetSessionBranch(db));
    }
    rc = doltliteCreateAndStoreCommit(db, &ourHead, &mergedCatHash,
        msg, NULL, NULL, &theirHead, 1, &commitHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to create merge commit", -1);
      return;
    }

    rc = doltliteAdvanceBranch(db, &commitHash, &mergedCatHash);
    if( graphLocked ){
      chunkStoreUnlock(cs);
      graphLocked = 0;
    }
    if( rc!=SQLITE_OK ){
      int rc2 = doltliteRestoreTxnState(db, &savedState);
      doltliteTxnStateClear(&savedState);
      sqlite3_result_error_code(context, rc2==SQLITE_OK ? rc : rc2);
      return;
    }
    doltliteTxnStateClear(&savedState);

    doltliteHashToHex(&commitHash, hexBuf);
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

/* Shared tail for cherry-pick and revert: treat both as degenerate
** merges and reuse doltliteMergeCatalogs with synthesized anc/our/
** theirs triples. Cherry-pick uses (parent, HEAD, pick), revert
** uses (pick, HEAD, parent) — swapping their-vs-ancestor inverts
** the changes. Conflicts surface the same way a real merge would. */
static int applyMergedCatalogAndCommit(
  sqlite3 *db,
  sqlite3_context *context,
  const ProllyHash *ancCatHash,
  const ProllyHash *ourCatHash,
  const ProllyHash *theirCatHash,
  const ProllyHash *ourHead,
  const char *zMessage,
  int *pnConflicts,
  char *hexBuf
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteTxnState savedState;
  ProllyHash mergedCatHash;
  ProllyHash commitHash;
  int graphLocked = 0;
  int rc;

  memset(&savedState, 0, sizeof(savedState));

  rc = doltliteMergeCatalogs(db, ancCatHash, ourCatHash, theirCatHash,
                              &mergedCatHash, pnConflicts, 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteSaveTxnState(db, &savedState);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteRefreshAndConfirmHead(db, cs, ourHead);
  if( rc!=SQLITE_OK ){
    doltliteTxnStateClear(&savedState);
    return rc;
  }
  graphLocked = 1;

  rc = doltliteSwitchCatalog(db, &mergedCatHash);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  doltliteSetSessionStaged(db, &mergedCatHash);
  rc = doltliteUpdateBranchWorkingState(db,
      doltliteGetSessionBranch(db), &mergedCatHash, NULL);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  if( *pnConflicts > 0 ){
    rc = doltliteReportConflicts(db, context, *pnConflicts,
                                 sqlite3_strnicmp(zMessage, "Revert", 6)==0
                                   ? "Revert" : "Cherry-pick");
    if( rc!=SQLITE_OK ) goto apply_rollback;
    chunkStoreUnlock(cs);
    doltliteTxnStateClear(&savedState);
    return SQLITE_OK;
  }

  rc = doltliteCreateAndStoreCommit(db, ourHead, &mergedCatHash,
      zMessage, NULL, NULL, NULL, 0, &commitHash);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  rc = doltliteAdvanceBranch(db, &commitHash, &mergedCatHash);
  if( rc!=SQLITE_OK ) goto apply_rollback;

  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
  doltliteTxnStateClear(&savedState);
  doltliteHashToHex(&commitHash, hexBuf);
  return SQLITE_OK;

apply_rollback:
  if( graphLocked ){
    chunkStoreUnlock(cs);
  }
  {
    int rc2 = doltliteRestoreTxnState(db, &savedState);
    doltliteTxnStateClear(&savedState);
    return rc2==SQLITE_OK ? rc : rc2;
  }
}

static void doltliteCherryPickFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zRef;
  ProllyHash pickHash, ourHead;
  DoltliteCommit pickCommit, parentCommit, ourCommit;
  int nConflicts = 0;
  int rc;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&pickCommit, 0, sizeof(pickCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_cherry_pick('commit_hash')", -1);
    return;
  }

  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_error(context, "commit hash required", -1);
    return;
  }


  rc = doltliteResolveRef(db,zRef, &pickHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }


  rc = doltliteLoadCommit(db, &pickHash, &pickCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }


  if( prollyHashIsEmpty(&pickCommit.parentHash) ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "cannot cherry-pick the initial commit", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &pickCommit.parentHash, &parentCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&pickCommit);
    sqlite3_result_error(context, "parent commit not found", -1);
    return;
  }


  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&pickCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
  }


  {
    const char *zMsg = pickCommit.zMessage;
    char fallback[256];
    if( !zMsg || !*zMsg ){
      sqlite3_snprintf(sizeof(fallback), fallback, "cherry-pick of %s", zRef);
      zMsg = fallback;
    }

    rc = applyMergedCatalogAndCommit(db, context,
        &parentCommit.catalogHash, &ourCommit.catalogHash,
        &pickCommit.catalogHash, &ourHead, zMsg, &nConflicts, hexBuf);
  }

  doltliteCommitClear(&pickCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  if( rc==SQLITE_BUSY ){
    sqlite3_result_error(context,
      "cherry-pick conflict: another connection committed to this branch. Please retry your transaction.",
      -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "cherry-pick failed", -1);
    return;
  }

  if( nConflicts > 0 ){
    return;
  }else{
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

static void doltliteRevertFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zRef;
  ProllyHash revertHash, ourHead;
  DoltliteCommit revertCommit, parentCommit, ourCommit;
  int nConflicts = 0;
  int rc;
  char hexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&revertCommit, 0, sizeof(revertCommit));
  memset(&parentCommit, 0, sizeof(parentCommit));
  memset(&ourCommit, 0, sizeof(ourCommit));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }

  if( argc<1 ){
    sqlite3_result_int(context, 0);
    return;
  }

  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_int(context, 0);
    return;
  }

  rc = doltliteResolveRef(db,zRef, &revertHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "invalid commit hash", -1);
    return;
  }


  rc = doltliteLoadCommit(db, &revertHash, &revertCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "commit not found", -1);
    return;
  }


  if( prollyHashIsEmpty(&revertCommit.parentHash) ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "cannot revert the initial commit", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &revertCommit.parentHash, &parentCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&revertCommit);
    sqlite3_result_error(context, "parent commit not found", -1);
    return;
  }


  doltliteGetSessionHead(db, &ourHead);
  if( prollyHashIsEmpty(&ourHead) ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteLoadCommit(db, &ourHead, &ourCommit);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&revertCommit);
    doltliteCommitClear(&parentCommit);
    sqlite3_result_error(context, "failed to load HEAD commit", -1);
    return;
  }


  {
    char msg[512];
    sqlite3_snprintf(sizeof(msg), msg, "Revert \"%s\"",
                     revertCommit.zMessage ? revertCommit.zMessage : zRef);

    rc = applyMergedCatalogAndCommit(db, context,
        &revertCommit.catalogHash, &ourCommit.catalogHash,
        &parentCommit.catalogHash, &ourHead, msg, &nConflicts, hexBuf);
  }

  doltliteCommitClear(&revertCommit);
  doltliteCommitClear(&parentCommit);
  doltliteCommitClear(&ourCommit);

  if( rc==SQLITE_BUSY ){
    sqlite3_result_error(context,
      "revert conflict: another connection committed to this branch. Please retry your transaction.",
      -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "revert failed", -1);
    return;
  }

  if( nConflicts > 0 ){
    return;
  }else{
    sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
  }
}

/* Collect the set of commits reachable from pHeadHash but NOT from
** pUpstreamHash. Walks upstream's full ancestor graph into a hash
** set (BFS), then walks head first-parent backward and records every
** commit that isn't already in that set. Emits the replay list in
** oldest-first order (so iteration N's parent is N-1). */
static int doltliteRebaseCollectReplaySet(
  sqlite3 *db,
  const ProllyHash *pHeadHash,
  const ProllyHash *pUpstreamHash,
  ProllyHash **paReplay,
  int *pnReplay
){
  ProllyHashSet upstreamAncestors;
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 0;
  ProllyHash *aReplay = 0;
  int nReplay = 0, nAllocReplay = 0;
  int rc;
  int upstreamInit = 0;
  ProllyHash walk;
  int i;

  *paReplay = 0;
  *pnReplay = 0;

  rc = prollyHashSetInit(&upstreamAncestors, 256);
  if( rc!=SQLITE_OK ) return rc;
  upstreamInit = 1;

  qAlloc = 64;
  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ){ rc = SQLITE_NOMEM; goto cleanup; }
  queue[qTail++] = *pUpstreamHash;

  while( qHead < qTail ){
    ProllyHash cur = queue[qHead++];
    DoltliteCommit c;

    if( prollyHashIsEmpty(&cur) ) continue;
    if( prollyHashSetContains(&upstreamAncestors, &cur) ) continue;
    rc = prollyHashSetAdd(&upstreamAncestors, &cur);
    if( rc!=SQLITE_OK ) goto cleanup;

    memset(&c, 0, sizeof(c));
    if( doltliteLoadCommit(db, &cur, &c)!=SQLITE_OK ) continue;
    for(i=0; i<doltliteCommitParentCount(&c); i++){
      const ProllyHash *pp = doltliteCommitParentHash(&c, i);
      if( !pp || prollyHashIsEmpty(pp) ) continue;
      if( prollyHashSetContains(&upstreamAncestors, pp) ) continue;
      if( qTail >= qAlloc ){
        int nNew = qAlloc * 2;
        ProllyHash *tmp = sqlite3_realloc(queue, nNew*(int)sizeof(ProllyHash));
        if( !tmp ){ doltliteCommitClear(&c); rc = SQLITE_NOMEM; goto cleanup; }
        queue = tmp;
        qAlloc = nNew;
      }
      queue[qTail++] = *pp;
    }
    doltliteCommitClear(&c);
  }

  sqlite3_free(queue);
  queue = 0;

  /* Walk HEAD backward via first-parent, stopping at the first
  ** commit that's already in upstream's ancestry. */
  walk = *pHeadHash;
  while( !prollyHashIsEmpty(&walk) && !prollyHashSetContains(&upstreamAncestors, &walk) ){
    DoltliteCommit c;
    const ProllyHash *pParent;

    if( nReplay >= nAllocReplay ){
      int nNew = nAllocReplay ? nAllocReplay*2 : 16;
      ProllyHash *tmp = sqlite3_realloc(aReplay, nNew*(int)sizeof(ProllyHash));
      if( !tmp ){ rc = SQLITE_NOMEM; goto cleanup; }
      aReplay = tmp;
      nAllocReplay = nNew;
    }
    aReplay[nReplay++] = walk;

    memset(&c, 0, sizeof(c));
    rc = doltliteLoadCommit(db, &walk, &c);
    if( rc!=SQLITE_OK ) goto cleanup;
    pParent = doltliteCommitParentHash(&c, 0);
    if( pParent && !prollyHashIsEmpty(pParent) ){
      walk = *pParent;
    }else{
      memset(&walk, 0, sizeof(walk));
    }
    doltliteCommitClear(&c);
  }

  /* Reverse so the output is oldest-first (ancestor-first). */
  for(i=0; i<nReplay/2; i++){
    ProllyHash tmp = aReplay[i];
    aReplay[i] = aReplay[nReplay-1-i];
    aReplay[nReplay-1-i] = tmp;
  }

  *paReplay = aReplay;
  *pnReplay = nReplay;
  aReplay = 0;
  rc = SQLITE_OK;

cleanup:
  sqlite3_free(queue);
  sqlite3_free(aReplay);
  if( upstreamInit ) prollyHashSetFree(&upstreamAncestors);
  return rc;
}

/* dolt_rebase: replay commits reachable from HEAD but not from the
** upstream ref on top of the upstream. Shares the per-commit apply
** logic with cherry-pick/revert via applyMergedCatalogAndCommit.
** Atomic: if any replay step fails or produces conflicts, restore
** the pre-rebase state and error out. Interactive mode and
** --continue / --abort (beyond "no rebase in progress") are not
** supported in this initial implementation. */
static void doltliteRebaseFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zUpstream;
  ProllyHash upstreamHash, headHash;
  ProllyHash *aReplay = 0;
  int nReplay = 0;
  DoltliteTxnState saved;
  DoltliteCommit upstreamCommit;
  int savedInit = 0;
  int rc;
  int i;

  memset(&saved, 0, sizeof(saved));
  memset(&upstreamCommit, 0, sizeof(upstreamCommit));

  if( !cs ){ sqlite3_result_error(context, "no database", -1); return; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_rebase('upstream_branch')", -1);
    return;
  }

  zUpstream = (const char*)sqlite3_value_text(argv[0]);
  if( !zUpstream ){
    sqlite3_result_error(context, "upstream ref required", -1);
    return;
  }

  if( strcmp(zUpstream, "--abort")==0 || strcmp(zUpstream, "--continue")==0 ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }

  if( doltliteHasUncommittedChanges(db) ){
    sqlite3_result_error(context,
      "cannot start a rebase with uncommitted changes", -1);
    return;
  }

  rc = doltliteResolveRef(db, zUpstream, &upstreamHash);
  if( rc!=SQLITE_OK ){
    char *zErr = sqlite3_mprintf("branch not found: %s", zUpstream);
    sqlite3_result_error(context, zErr ? zErr : "branch not found", -1);
    sqlite3_free(zErr);
    return;
  }

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteRebaseCollectReplaySet(db, &headHash, &upstreamHash,
                                      &aReplay, &nReplay);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( nReplay==0 ){
    sqlite3_free(aReplay);
    sqlite3_result_error(context, "didn't identify any commits!", -1);
    return;
  }

  rc = doltliteSaveTxnState(db, &saved);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aReplay);
    sqlite3_result_error_code(context, rc);
    return;
  }
  savedInit = 1;

  /* Reset the branch to the upstream tip so applyMergedCatalog sees
  ** a consistent session HEAD for the first replay iteration. The
  ** saved txn state will undo this on rollback. */
  rc = doltliteLoadCommit(db, &upstreamHash, &upstreamCommit);
  if( rc!=SQLITE_OK ) goto rollback;

  rc = doltliteSwitchCatalog(db, &upstreamCommit.catalogHash);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&upstreamCommit);
    goto rollback;
  }
  doltliteSetSessionHead(db, &upstreamHash);
  doltliteSetSessionStaged(db, &upstreamCommit.catalogHash);
  rc = doltliteAdvanceBranch(db, &upstreamHash, &upstreamCommit.catalogHash);
  doltliteCommitClear(&upstreamCommit);
  memset(&upstreamCommit, 0, sizeof(upstreamCommit));
  if( rc!=SQLITE_OK ) goto rollback;

  for(i=0; i<nReplay; i++){
    DoltliteCommit replayCommit, parentCommit, curHeadCommit;
    ProllyHash curHead;
    int nConflicts = 0;
    char hexBuf[PROLLY_HASH_SIZE*2+1];

    memset(&replayCommit, 0, sizeof(replayCommit));
    memset(&parentCommit, 0, sizeof(parentCommit));
    memset(&curHeadCommit, 0, sizeof(curHeadCommit));

    rc = doltliteLoadCommit(db, &aReplay[i], &replayCommit);
    if( rc!=SQLITE_OK ) goto rollback;

    if( replayCommit.nParents==0 || prollyHashIsEmpty(&replayCommit.aParents[0]) ){
      /* Root commit can't be replayed — no parent to diff against. */
      doltliteCommitClear(&replayCommit);
      rc = SQLITE_ERROR;
      goto rollback;
    }
    rc = doltliteLoadCommit(db, &replayCommit.aParents[0], &parentCommit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&replayCommit);
      goto rollback;
    }

    doltliteGetSessionHead(db, &curHead);
    rc = doltliteLoadCommit(db, &curHead, &curHeadCommit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&replayCommit);
      doltliteCommitClear(&parentCommit);
      goto rollback;
    }

    rc = applyMergedCatalogAndCommit(db, context,
        &parentCommit.catalogHash,
        &curHeadCommit.catalogHash,
        &replayCommit.catalogHash,
        &curHead,
        replayCommit.zMessage ? replayCommit.zMessage : "",
        &nConflicts, hexBuf);

    doltliteCommitClear(&replayCommit);
    doltliteCommitClear(&parentCommit);
    doltliteCommitClear(&curHeadCommit);

    if( rc!=SQLITE_OK ){
      goto rollback;
    }
    if( nConflicts>0 ){
      /* Conflict during a replay. applyMerged has staged the
      ** conflict and set the session to "merging" state; undo all
      ** of that by restoring the saved pre-rebase state. */
      rc = SQLITE_ERROR;
      goto rollback;
    }
  }

  doltliteTxnStateClear(&saved);
  sqlite3_free(aReplay);
  {
    char *zMsg = sqlite3_mprintf("Successfully rebased and updated refs/heads/%s",
                                 doltliteGetSessionBranch(db));
    if( zMsg ){
      sqlite3_result_text(context, zMsg, -1, sqlite3_free);
    }else{
      sqlite3_result_text(context, "Successfully rebased", -1, SQLITE_STATIC);
    }
  }
  return;

rollback:
  if( savedInit ){
    int rc2 = doltliteRestoreTxnState(db, &saved);
    doltliteTxnStateClear(&saved);
    (void)rc2;
  }
  sqlite3_free(aReplay);
  {
    char *zErr = sqlite3_mprintf(
      "rebase failed (conflict or error during replay) — branch restored to pre-rebase state");
    if( zErr ){
      sqlite3_result_error(context, zErr, -1);
      sqlite3_free(zErr);
    }else{
      sqlite3_result_error_code(context, rc);
    }
  }
}

static void doltliteConfigFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zKey;

  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_config(key [, value])", -1);
    return;
  }
  zKey = (const char*)sqlite3_value_text(argv[0]);
  if( !zKey ){
    sqlite3_result_error(context, "key required", -1);
    return;
  }

  if( argc==1 ){

    if( strcmp(zKey, "user.name")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorName(db), -1, SQLITE_TRANSIENT);
    }else if( strcmp(zKey, "user.email")==0 ){
      sqlite3_result_text(context, doltliteGetAuthorEmail(db), -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
    }
  }else{

    const char *zVal = (const char*)sqlite3_value_text(argv[1]);
    if( strcmp(zKey, "user.name")==0 ){
      doltliteSetAuthorName(db, zVal);
      sqlite3_result_int(context, 0);
    }else if( strcmp(zKey, "user.email")==0 ){
      doltliteSetAuthorEmail(db, zVal);
      sqlite3_result_int(context, 0);
    }else{
      sqlite3_result_error(context, "unknown config key (valid: user.name, user.email)", -1);
    }
  }
}

/* On first open of a writable chunk store with no branches, create
** an empty initial commit on "main" so a fresh database has a valid
** HEAD to commit against. Skipped on read-only or in-memory stores
** and when branches already exist (a previous seed ran, or the file
** was cloned from a remote). */
static void doltliteMaybeSeedRepo(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash emptyParent;
  ProllyHash emptyCatalog;
  ProllyHash seedHash;
  int rc;

  if( !cs ) return;
  if( cs->nBranches > 0 ) return;
  if( sqlite3_db_readonly(db, "main")==1 ) return;

  memset(&emptyParent, 0, sizeof(emptyParent));
  memset(&emptyCatalog, 0, sizeof(emptyCatalog));

  rc = doltliteCreateAndStoreCommit(db, &emptyParent, &emptyCatalog,
      "Initialize data repository", NULL, NULL, 0, 0, &seedHash);
  if( rc!=SQLITE_OK ) return;

  (void)doltliteAdvanceBranch(db, &seedHash, &emptyCatalog);
}

void doltliteRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_commit", -1, SQLITE_UTF8, 0,
                               doltliteCommitFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_add", -1, SQLITE_UTF8, 0,
                                                   doltliteAddFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_reset", -1, SQLITE_UTF8, 0,
                                                   doltliteResetFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_merge", -1, SQLITE_UTF8, 0,
                                                   doltliteMergeFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_cherry_pick", -1, SQLITE_UTF8, 0,
                                                   doltliteCherryPickFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_revert", -1, SQLITE_UTF8, 0,
                                                   doltliteRevertFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_rebase", -1, SQLITE_UTF8, 0,
                                                   doltliteRebaseFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_function(db, "dolt_config", -1, SQLITE_UTF8, 0,
                                                   doltliteConfigFunc, 0, 0);
  if( rc!=SQLITE_OK ) return;
  if( doltliteLogRegister(db)!=SQLITE_OK ) return;
  if( doltliteStatusRegister(db)!=SQLITE_OK ) return;
  if( doltliteDiffRegister(db)!=SQLITE_OK ) return;
  if( doltliteBranchRegister(db)!=SQLITE_OK ) return;
  if( doltliteTagRegister(db)!=SQLITE_OK ) return;
  if( doltliteConflictsRegister(db)!=SQLITE_OK ) return;
  if( doltliteGcRegister(db)!=SQLITE_OK ) return;
  if( doltliteRegisterDiffTables(db)!=SQLITE_OK ) return;
  if( doltliteAncestorRegister(db)!=SQLITE_OK ) return;
  if( doltliteAtRegister(db)!=SQLITE_OK ) return;
  if( doltliteRegisterHistoryTables(db)!=SQLITE_OK ) return;
  if( doltliteSchemaDiffRegister(db)!=SQLITE_OK ) return;
  if( doltliteSchemasRegister(db)!=SQLITE_OK ) return;
  if( doltliteDiffStatRegister(db)!=SQLITE_OK ) return;
  if( doltliteRemoteSqlRegister(db)!=SQLITE_OK ) return;

  {
    extern int doltliteDbpageInstallAutoExt(void);
    doltliteDbpageInstallAutoExt();
  }
  doltliteMaybeSeedRepo(db);
}

#endif
