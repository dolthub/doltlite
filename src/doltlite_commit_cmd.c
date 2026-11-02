#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "vdbeInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <stddef.h>
#include "doltlite_ignore.h"
#include "doltlite_constraint_violations.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

#ifdef _WIN32
static time_t dlWinTimegm(struct tm *pTm){
  return _mkgmtime(pTm);
}
#define timegm dlWinTimegm
#endif

static int parseFixedInt(const char **pz, int nDigits, int *pOut){
  const char *z = *pz;
  int i, v = 0;
  for(i=0; i<nDigits; i++){
    if( z[i]<'0' || z[i]>'9' ) return 0;
    v = v*10 + (z[i]-'0');
  }
  *pz = z + nDigits;
  *pOut = v;
  return 1;
}

/* YYYY-MM-DD, or YYYY-MM-DDTHH:MM:SS with optional frac and Z/±HH:MM.
** Fractional seconds are accepted and dropped; commit timestamps are
** whole seconds. */
static int doltliteParseCommitDate(const char *zDate, i64 *pUnix){
  const char *p = zDate;
  int y, mo, d, h = 0, mi = 0, s = 0;
  int offSign = 0, offH = 0, offM = 0;
  struct tm tm;
  time_t t;
  struct tm *pBack;

  if( !zDate || !pUnix ) return SQLITE_ERROR;
  if( !parseFixedInt(&p, 4, &y) || *p++!='-'
   || !parseFixedInt(&p, 2, &mo) || *p++!='-'
   || !parseFixedInt(&p, 2, &d) ){
    return SQLITE_ERROR;
  }
  if( *p=='T' ){
    p++;
    if( !parseFixedInt(&p, 2, &h) || *p++!=':'
     || !parseFixedInt(&p, 2, &mi) || *p++!=':'
     || !parseFixedInt(&p, 2, &s) ){
      return SQLITE_ERROR;
    }
    if( *p=='.' || *p==',' ){
      p++;
      if( *p<'0' || *p>'9' ) return SQLITE_ERROR;
      while( *p>='0' && *p<='9' ) p++;
    }
    if( *p=='Z' ){
      p++;
    }else if( *p=='+' || *p=='-' ){
      offSign = (*p=='+') ? 1 : -1;
      p++;
      if( !parseFixedInt(&p, 2, &offH) || *p++!=':'
       || !parseFixedInt(&p, 2, &offM) ){
        return SQLITE_ERROR;
      }
    }
  }
  if( *p!=0 ) return SQLITE_ERROR;
  if( y<1 || y>9999 || mo<1 || mo>12 || d<1 || d>31 ) return SQLITE_ERROR;
  if( h>23 || mi>59 || s>59 || offH>23 || offM>59 ) return SQLITE_ERROR;

  memset(&tm, 0, sizeof(tm));
  tm.tm_year = y - 1900;
  tm.tm_mon = mo - 1;
  tm.tm_mday = d;
  tm.tm_hour = h;
  tm.tm_min = mi;
  tm.tm_sec = s;
  tm.tm_isdst = 0;
  t = timegm(&tm);
  pBack = gmtime(&t);
  if( !pBack
   || pBack->tm_year!=y-1900 || pBack->tm_mon!=mo-1
   || pBack->tm_mday!=d || pBack->tm_hour!=h
   || pBack->tm_min!=mi || pBack->tm_sec!=s ){
    return SQLITE_ERROR;
  }
  *pUnix = (i64)t - (i64)offSign*((i64)offH*3600 + (i64)offM*60);
  return SQLITE_OK;
}

/* Parse dolt_commit argv into opts. Malformed options set the context error.
** explicitTimestamp stays 0 for the caller to fill from --date. */
typedef struct DoltliteCommitOptions {
  const char *zMessage;
  const char *zAuthor;
  const char *zDate;
  const char *zBranch;
  int addAll;
  int addModifiedOnly;
  int amend;
  int allowEmpty;
  int skipEmpty;
  int skipVerification;
  int force;
  i64 explicitTimestamp;
} DoltliteCommitOptions;

static int doltliteCommitParseOptions(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv,
  DoltliteCommitOptions *opts
){
  DoltliteCmdArgs args;
  DoltliteCmdOption aOption[] = {
    { "ALL", 'A', DOLTLITE_CMD_OPTION_FLAG, &opts->addAll, 0 },
    { "all", 'a', DOLTLITE_CMD_OPTION_FLAG, &opts->addModifiedOnly, 0 },
    { "force", 'f', DOLTLITE_CMD_OPTION_FLAG, &opts->force, 0 },
    { "message", 'm', DOLTLITE_CMD_OPTION_VALUE, 0, &opts->zMessage },
    { "author", 0, DOLTLITE_CMD_OPTION_VALUE, 0, &opts->zAuthor },
    { "date", 0, DOLTLITE_CMD_OPTION_VALUE, 0, &opts->zDate },
    { "branch", 0, DOLTLITE_CMD_OPTION_VALUE, 0, &opts->zBranch },
    { "amend", 0, DOLTLITE_CMD_OPTION_FLAG, &opts->amend, 0 },
    { "allow-empty", 0, DOLTLITE_CMD_OPTION_FLAG, &opts->allowEmpty, 0 },
    { "skip-empty", 0, DOLTLITE_CMD_OPTION_FLAG, &opts->skipEmpty, 0 },
    { "skip-verification", 0, DOLTLITE_CMD_OPTION_FLAG, &opts->skipVerification, 0 }
  };
  int rc;
  memset(opts, 0, sizeof(*opts));
  rc = doltliteCmdParseArgs(context, argc, argv, aOption, ArraySize(aOption),
                            DOLTLITE_CMD_PARSE_SHORT_GROUPS, &args);
  if( rc!=SQLITE_OK ) return rc;
  if( args.nPositional>0 ){
    const char *arg = args.azPositional[0];
    char *zErr = sqlite3_mprintf(
        "commit does not take positional arguments, but found 1: %s", arg);
    if( zErr ){
      sqlite3_result_error(context, zErr, -1);
      sqlite3_free(zErr);
    }else{
      sqlite3_result_error_nomem(context);
    }
    doltliteCmdArgsClear(&args);
    return SQLITE_ERROR;
  }
  doltliteCmdArgsClear(&args);
  if( opts->allowEmpty && opts->skipEmpty ){
    sqlite3_result_error(context,
      "cannot use both --allow-empty and --skip-empty", -1);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int doltliteCommitValidateAuthor(
  sqlite3_context *context,
  const char *zAuthor
){
  char *zName = 0;
  char *zEmail = 0;
  int rc = doltliteCmdParseAuthor(context, zAuthor, &zName, &zEmail);
  if( rc==SQLITE_OK && zEmail[0]==0 ){
    sqlite3_result_error(context,
      "Aborting commit due to empty author email. Is your config set?", -1);
    rc = SQLITE_ERROR;
  }
  sqlite3_free(zName);
  sqlite3_free(zEmail);
  return rc;
}

/* Rebuild the staged catalog for dolt_commit -a: overlay working-tree
** changes for tables that already exist in HEAD. */
static int doltliteCommitStageModifiedOnly(
  sqlite3 *db,
  sqlite3_context *context,
  int amend,
  const DoltliteBranchCommitState *pBranch,
  ProllyHash *pStagedOut
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash workingHash, headCatHash, stagedHash;
  struct TableEntry *aWorking = 0, *aHead = 0, *aStaged = 0;
  int nWorking = 0, nHead = 0, nStaged = 0;
  int nStagedAlloc = 0;
  int j, k;
  int rc;
  AddNameIndex workingIdx;
  AddNameIndex headIdx;
  AddNameIndex stagedIdx;
  u8 *aRemoveStaged = 0;
  const char **azTouched = 0;
  int nTouched = 0;
  Pgno stagedOffset = 0;

  memset(&workingIdx, 0, sizeof(workingIdx));
  memset(&headIdx, 0, sizeof(headIdx));
  memset(&stagedIdx, 0, sizeof(stagedIdx));

  #define FREE_ADD_MODIFIED_CATALOGS() do { \
    sqlite3_free((void*)azTouched); \
    sqlite3_free(aRemoveStaged); \
    addNameIndexFree(&workingIdx); \
    addNameIndexFree(&headIdx); \
    addNameIndexFree(&stagedIdx); \
    doltliteFreeCatalog(aWorking, nWorking); \
    doltliteFreeCatalog(aHead, nHead); \
    doltliteFreeCatalog(aStaged, nStaged); \
  } while(0)


  if( pBranch ){
    workingHash = pBranch->working;
    rc = SQLITE_OK;
  }else{
    rc = doltliteFlushCatalogToHash(db, &workingHash);
  }
  if( rc!=SQLITE_OK ){
    doltliteVcResultErrorCode(context, sqlite3_context_db_handle(context),
        "failed to flush", rc);
    return rc;
  }
  rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
  if( rc!=SQLITE_OK ){
    doltliteVcResultErrorCode(context, db, "failed to load working catalog", rc);
    FREE_ADD_MODIFIED_CATALOGS();
    return rc;
  }
  if( pBranch ){
    headCatHash = pBranch->headCatalog;
    rc = SQLITE_OK;
  }else{
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  }
  if( rc!=SQLITE_OK ){
    /* The staged load below overwrites rc. Losing this error makes -a
    ** stage nothing, and --amend then reports success. */
    sqlite3_result_error_code(context, rc);
    FREE_ADD_MODIFIED_CATALOGS();
    return rc;
  }
  if( !prollyHashIsEmpty(&headCatHash) ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
    if( rc!=SQLITE_OK ){
      doltliteVcResultErrorCode(context, db, "failed to load HEAD catalog", rc);
      FREE_ADD_MODIFIED_CATALOGS();
      return rc;
    }
  }

  if( pBranch ) stagedHash = pBranch->staged;
  else doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash) ){
    rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
  }else if( !prollyHashIsEmpty(&headCatHash) ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aStaged, &nStaged, 0);
  }
  if( rc!=SQLITE_OK ){
    doltliteVcResultErrorCode(context, db, "failed to load staged catalog", rc);
    FREE_ADD_MODIFIED_CATALOGS();
    return rc;
  }

  rc = doltliteDisjoinCatalogEntries(db, aWorking, nWorking,
                                     aStaged, nStaged, &stagedOffset);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    FREE_ADD_MODIFIED_CATALOGS();
    return rc;
  }

  nStagedAlloc = nStaged + nWorking + 1;
  if( nStagedAlloc>0 ){
    struct TableEntry *aNewStaged = sqlite3_realloc(
        aStaged, nStagedAlloc*(int)sizeof(struct TableEntry));
    if( !aNewStaged ){
      sqlite3_result_error_nomem(context);
      FREE_ADD_MODIFIED_CATALOGS();
      return SQLITE_NOMEM;
    }
    aStaged = aNewStaged;
  }
  aRemoveStaged = sqlite3_malloc(nStagedAlloc>0 ? nStagedAlloc : 1);
  if( !aRemoveStaged ){
    sqlite3_result_error_nomem(context);
    FREE_ADD_MODIFIED_CATALOGS();
    return SQLITE_NOMEM;
  }
  memset(aRemoveStaged, 0, nStagedAlloc>0 ? nStagedAlloc : 1);

  rc = addNameIndexInit(&workingIdx, aWorking, nWorking);
  if( rc==SQLITE_OK ) rc = addNameIndexInit(&headIdx, aHead, nHead);
  if( rc==SQLITE_OK ) rc = addNameIndexInit(&stagedIdx, aStaged, nStaged);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_nomem(context);
    FREE_ADD_MODIFIED_CATALOGS();
    return rc;
  }

  /* Master rows that follow WORKING: tables overlaid from working and
  ** staged deletions. Rename-kept and staged-only keep prior rows. */
  azTouched = sqlite3_malloc((nWorking+nHead+1)*(int)sizeof(char*));
  if( !azTouched ){
    sqlite3_result_error_nomem(context);
    FREE_ADD_MODIFIED_CATALOGS();
    return SQLITE_NOMEM;
  }

  for(j=0; j<nWorking; j++){
    const char *zName = aWorking[j].zName;
    int updated = 0;
    char *zDup;
    struct TableEntry *pStaged;
    if( aWorking[j].iTable==1 ) continue;
    if( !addNameIndexFind(&headIdx, zName) ) continue;

    azTouched[nTouched++] = zName;
    pStaged = addNameIndexFind(&stagedIdx, zName);
    if( pStaged ){
        k = (int)(pStaged - aStaged);
        zDup = zName ? sqlite3_mprintf("%s", zName) : 0;
        if( zName && !zDup ){
          sqlite3_result_error_nomem(context);
          FREE_ADD_MODIFIED_CATALOGS();
          return SQLITE_NOMEM;
        }
        sqlite3_free(aStaged[k].zName);
        aStaged[k] = aWorking[j];
        aStaged[k].zName = zDup;
        updated = 1;
    }
    if( !updated ){
      zDup = zName ? sqlite3_mprintf("%s", zName) : 0;
      if( zName && !zDup ){
        sqlite3_result_error_nomem(context);
        FREE_ADD_MODIFIED_CATALOGS();
        return SQLITE_NOMEM;
      }
      aStaged[nStaged] = aWorking[j];
      aStaged[nStaged].zName = zDup;
      nStaged++;
    }
  }

  for(k=0; k<nHead; k++){
    const char *zName = aHead[k].zName;
    struct TableEntry *pStaged;
    struct TableEntry *pMate = 0;
    if( addNameIndexFind(&workingIdx, zName) ) continue;
    /* A HEAD table missing from working is a deletion unless it is the old
    ** name of a working-tree rename. -a must not stage that half (bare DROP
    ** would drop the table's history). */
    rc = doltliteCatalogRenameMate(db, aHead, nHead, aHead, nHead,
                                   aWorking, nWorking,
                                   &aHead[k], 1, &pMate);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      FREE_ADD_MODIFIED_CATALOGS();
      return rc;
    }
    if( pMate ) continue;
    azTouched[nTouched++] = zName;
    pStaged = addNameIndexFind(&stagedIdx, zName);
    if( pStaged ){
      int j2 = (int)(pStaged - aStaged);
      if( j2>=0 && j2<nStaged ) aRemoveStaged[j2] = 1;
    }
  }
  for(k=0; k<nStaged; ){
    if( aRemoveStaged[k] ){
      sqlite3_free(aStaged[k].zName);
      if( k+1<nStaged ){
        memmove(&aStaged[k], &aStaged[k+1],
                (nStaged-k-1)*(int)sizeof(struct TableEntry));
        memmove(&aRemoveStaged[k], &aRemoveStaged[k+1],
                (nStaged-k-1)*(int)sizeof(u8));
      }
      nStaged--;
      continue;
    }
    k++;
  }

  /* Index entries are unnamed, so by-name overlays never refresh them.
  ** Rebuild: working-sourced tables take working indexes, staged-only keep
  ** theirs, deleted tables drop them. Parents resolve through each catalog's
  ** own schema rows. */
  {
    SchemaEntry *aWorkSchema = 0, *aOldSchema = 0;
    int nWorkSchema = 0, nOldSchema = 0;
    const ProllyHash *pOldSrcHash =
        !prollyHashIsEmpty(&stagedHash) ? &stagedHash : &headCatHash;
    int i2, k2, nAppend = 0;

    rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &workingHash,
                               &aWorkSchema, &nWorkSchema);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(pOldSrcHash) ){
      rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), pOldSrcHash,
                                 &aOldSchema, &nOldSchema);
    }
    if( rc!=SQLITE_OK ){
      freeSchemaEntries(aWorkSchema, nWorkSchema);
      freeSchemaEntries(aOldSchema, nOldSchema);
      doltliteVcResultErrorCode(context, db, "failed to load schema for staging", rc);
      FREE_ADD_MODIFIED_CATALOGS();
      return rc;
    }

    for(i2=0; i2<nOldSchema; i2++){
      if( aOldSchema[i2].iRootpage>1 ){
        aOldSchema[i2].iRootpage += stagedOffset;
      }
    }

    for(k2=0; k2<nStaged; ){
      const char *zParent = 0;
      if( aStaged[k2].iTable<=1 || aStaged[k2].zName ){ k2++; continue; }
      for(i2=0; i2<nOldSchema; i2++){
        if( aOldSchema[i2].zType
         && strcmp(aOldSchema[i2].zType, "index")==0
         && aOldSchema[i2].iRootpage==aStaged[k2].iTable ){
          zParent = aOldSchema[i2].zTblName;
          break;
        }
      }
      if( zParent
       && amTableStagedByName(aStaged, nStaged, zParent)
       && !(addNameIndexFind(&workingIdx, zParent)
            && addNameIndexFind(&headIdx, zParent)) ){
        /* Staged-sourced index: table was staged explicitly (or a rename
        ** kept out of -a). Keep its previous number so it pairs as stored. */
        k2++;
        continue;
      }
      sqlite3_free(aStaged[k2].zName);
      if( k2+1<nStaged ){
        memmove(&aStaged[k2], &aStaged[k2+1],
                (nStaged-k2-1)*(int)sizeof(struct TableEntry));
      }
      nStaged--;
    }

    for(i2=0; i2<nWorkSchema; i2++){
      if( aWorkSchema[i2].zType
       && strcmp(aWorkSchema[i2].zType, "index")==0
       && aWorkSchema[i2].iRootpage>1
       && aWorkSchema[i2].zTblName
       && addNameIndexFind(&workingIdx, aWorkSchema[i2].zTblName)
       && addNameIndexFind(&headIdx, aWorkSchema[i2].zTblName)
       && amTableStagedByName(aStaged, nStaged, aWorkSchema[i2].zTblName) ){
        nAppend++;
      }
    }
    if( nAppend>0 && nStaged+nAppend>nStagedAlloc ){
      struct TableEntry *aGrown = sqlite3_realloc(
          aStaged, (nStaged+nAppend)*(int)sizeof(struct TableEntry));
      if( !aGrown ){
        freeSchemaEntries(aWorkSchema, nWorkSchema);
        freeSchemaEntries(aOldSchema, nOldSchema);
        sqlite3_result_error_nomem(context);
        FREE_ADD_MODIFIED_CATALOGS();
        return SQLITE_NOMEM;
      }
      aStaged = aGrown;
      nStagedAlloc = nStaged + nAppend;
    }
    for(i2=0; i2<nWorkSchema; i2++){
      if( !aWorkSchema[i2].zType
       || strcmp(aWorkSchema[i2].zType, "index")!=0
       || aWorkSchema[i2].iRootpage<=1
       || !aWorkSchema[i2].zTblName
       || !addNameIndexFind(&workingIdx, aWorkSchema[i2].zTblName)
       || !addNameIndexFind(&headIdx, aWorkSchema[i2].zTblName)
       || !amTableStagedByName(aStaged, nStaged, aWorkSchema[i2].zTblName) ){
        continue;
      }
      for(j=0; j<nWorking; j++){
        if( aWorking[j].iTable==aWorkSchema[i2].iRootpage
         && aWorking[j].zName==0 ){
          aStaged[nStaged] = aWorking[j];
          aStaged[nStaged].zName = 0;
          nStaged++;
          break;
        }
      }
    }

    freeSchemaEntries(aWorkSchema, nWorkSchema);
    freeSchemaEntries(aOldSchema, nOldSchema);
  }

  /* Settle numbering, then compose the staged master: touched tables from
  ** working, everything else keeps prior rows. Wholesale working-master
  ** adoption dropped rename-kept tables from the commit. */
  doltliteAlignStagedEntriesToWorking(aWorking, nWorking, aStaged, nStaged);
  doltliteRenumberStaleStagedEntries(aStaged, nStaged, aWorking, nWorking);
  {
    struct TableEntry *pWorkingMaster =
        doltliteFindTableByNumber(aWorking, nWorking, 1);
    struct TableEntry *pStagedMaster =
        doltliteFindTableByNumber(aStaged, nStaged, 1);
    if( pWorkingMaster && pStagedMaster ){
      ProllyHash composedRoot;
      rc = doltliteBuildNamedStageMasterRoot(db,
              &pWorkingMaster->root, pWorkingMaster->flags,
              &pStagedMaster->root, pStagedMaster->flags,
              azTouched, nTouched,
              aStaged, nStaged, 1,
              &composedRoot);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
        FREE_ADD_MODIFIED_CATALOGS();
        return rc;
      }
      pStagedMaster->root = composedRoot;
      pStagedMaster->schemaHash = pWorkingMaster->schemaHash;
      pStagedMaster->flags = pWorkingMaster->flags;
    }
  }

  if( nStaged==0 ){
    /* --amend of the initial commit still rewrites that commit when -a
    ** finds no tracked table. */
    if( amend ){
      FREE_ADD_MODIFIED_CATALOGS();
      return SQLITE_OK;
    }
    sqlite3_result_error(context,
      "nothing to commit, working tree clean (use dolt_add to stage changes)", -1);
    FREE_ADD_MODIFIED_CATALOGS();
    return SQLITE_ERROR;
  }

  {
    u8 *buf = 0;
    int nBuf = 0;
    ProllyHash newStagedHash;
    rc = pBranch
        ? doltliteSerializeCatalogEntriesFromCatalog(db, aStaged, nStaged,
            &pBranch->working, &buf, &nBuf)
        : doltliteSerializeCatalogEntries(db, aStaged, nStaged, &buf, &nBuf);
    if( rc==SQLITE_OK ){
      rc = chunkStorePut(cs, buf, nBuf, &newStagedHash);
    }
    sqlite3_free(buf);
    if( rc==SQLITE_OK ){
      if( pStagedOut ) *pStagedOut = newStagedHash;
      else rc = doltliteSetSessionStaged(db, &newStagedHash);
    }
  }

  FREE_ADD_MODIFIED_CATALOGS();
  #undef FREE_ADD_MODIFIED_CATALOGS
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  return SQLITE_OK;
}

/* Resolve parent, author, and message (--amend/--author) and store the
** commit; hash in pCommitHashOut. */
static int doltliteCommitCreateObject(
  sqlite3 *db,
  sqlite3_context *context,
  const DoltliteCommitOptions *opts,
  const ProllyHash *pCatalogHash,
  ProllyHash *pCommitHashOut,
  const DoltliteBranchCommitState *pBranch
){
  ProllyHash parentHash;
  ProllyHash aExtraParents[DOLTLITE_MAX_PARENTS];
  int nExtraParents = 0;
  char *zParsedName = 0, *zParsedEmail = 0;
  char *zAmendMessage = 0;
  const char *zMessage;
  const char *zAuthor;
  int rc;

  assert( db!=0 && context!=0 && opts!=0 );
  assert( pCatalogHash!=0 && pCommitHashOut!=0 );
  zMessage = opts->zMessage;
  zAuthor = opts->zAuthor;
  if( pBranch ) parentHash = pBranch->head;
  else doltliteGetSessionHead(db, &parentHash);

  if( opts->amend ){
    DoltliteCommit headCommit;
    ProllyHash headHash;
    memset(&headCommit, 0, sizeof(headCommit));
    if( pBranch ) headHash = pBranch->head;
    else doltliteGetSessionHead(db, &headHash);
    if( prollyHashIsEmpty(&headHash) ){
      sqlite3_result_error(context,
        "cannot --amend: branch has no commits", -1);
      return SQLITE_ERROR;
    }
    rc = doltliteLoadCommit(db, &headHash, &headCommit);
    if( rc!=SQLITE_OK ){
      doltliteVcResultErrorCode(context, db,
        "cannot --amend: failed to load HEAD commit", rc);
      return rc;
    }
    if( doltliteCommitParentCount(&headCommit)==0 ){
      /* Rewrite the initial commit in place. It stays parentless. */
      memset(&parentHash, 0, sizeof(parentHash));
    }else{
      const ProllyHash *pParent = doltliteCommitParentHash(&headCommit, 0);
      if( !pParent || prollyHashIsEmpty(pParent) ){
        doltliteCommitClear(&headCommit);
        sqlite3_result_error(context,
          "cannot --amend: HEAD has no parent (initial commit)", -1);
        return SQLITE_ERROR;
      }
      memcpy(&parentHash, pParent, sizeof(ProllyHash));
    }
    /* Carry remaining parents. Amending a merge with only its first parent
    ** drops the merged branch out of ancestry. */
    {
      int nParent = doltliteCommitParentCount(&headCommit);
      int i;
      for(i=1; i<nParent && nExtraParents<DOLTLITE_MAX_PARENTS-1; i++){
        const ProllyHash *pExtra = doltliteCommitParentHash(&headCommit, i);
        if( pExtra && !prollyHashIsEmpty(pExtra) ){
          memcpy(&aExtraParents[nExtraParents++], pExtra, sizeof(ProllyHash));
        }
      }
    }
    if( !zMessage || !*zMessage ){
      zAmendMessage = sqlite3_mprintf("%s",
          headCommit.zMessage ? headCommit.zMessage : "");
      if( !zAmendMessage ){
        doltliteCommitClear(&headCommit);
        sqlite3_result_error_code(context, SQLITE_NOMEM);
        return SQLITE_NOMEM;
      }
      zMessage = zAmendMessage;
    }
    doltliteCommitClear(&headCommit);
  }

  /* A merge commit needs a second parent. Cherry-pick, revert, and rebase
  ** replay leave the source empty (single-parent result). */
  {
    u8 isMerging = 0;
    ProllyHash mergeCommit;
    if( pBranch ){
      isMerging = pBranch->merging;
      mergeCommit = pBranch->merge;
    }else{
      doltliteGetSessionMergeState(db, &isMerging, &mergeCommit, 0);
    }
    if( isMerging && !prollyHashIsEmpty(&mergeCommit)
     && prollyHashCompare(&mergeCommit, &parentHash)!=0
     && nExtraParents < DOLTLITE_MAX_PARENTS-1 ){
      int i;
      for(i=0; i<nExtraParents; i++){
        if( prollyHashCompare(&aExtraParents[i], &mergeCommit)==0 ) break;
      }
      if( i>=nExtraParents ){
        memcpy(&aExtraParents[nExtraParents++], &mergeCommit,
               sizeof(ProllyHash));
      }
    }
  }

  if( zAuthor ){
    rc = doltliteCmdParseAuthor(context, zAuthor,
                                &zParsedName, &zParsedEmail);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zAmendMessage);
      return rc;
    }
  }

  {
    const char *p = zMessage;
    if( !p ){
      sqlite3_free(zAmendMessage);
      sqlite3_free(zParsedName);
      sqlite3_free(zParsedEmail);
      sqlite3_result_error_code(context, SQLITE_NOMEM);
      return SQLITE_NOMEM;
    }
    while( *p==' ' || *p=='\t' || *p=='\n' || *p=='\r' ) p++;
    if( *p==0 ){
      sqlite3_free(zAmendMessage);
      sqlite3_free(zParsedName);
      sqlite3_free(zParsedEmail);
      sqlite3_result_error(context,
        "dolt_commit requires a non-empty message", -1);
      return SQLITE_ERROR;
    }
  }

  rc = doltliteCreateAndStoreCommitWithTime(db, &parentHash, pCatalogHash,
      zMessage, zParsedName, zParsedEmail, aExtraParents, nExtraParents,
      opts->zDate!=0, opts->explicitTimestamp, pCommitHashOut);
  sqlite3_free(zAmendMessage);
  sqlite3_free(zParsedName);
  sqlite3_free(zParsedEmail);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  return SQLITE_OK;
}

static int doltliteCommitNextForeignKeyParent(
  const char **pzSql,
  char **pzParent
){
  const char *z = *pzSql;
  int type;
  int n;

  *pzParent = 0;
  while( *z ){
    n = sqlite3GetToken((const u8*)z, &type);
    if( n<=0 || type==TK_ILLEGAL ) return SQLITE_CORRUPT;
    if( type!=TK_STRING && type!=TK_BLOB && type!=TK_COMMENT
     && type!=TK_SPACE && n==10 && sqlite3_strnicmp(z, "REFERENCES", 10)==0 ){
      z += n;
      do{
        if( !*z ) return SQLITE_CORRUPT;
        n = sqlite3GetToken((const u8*)z, &type);
        if( n<=0 || type==TK_ILLEGAL ) return SQLITE_CORRUPT;
        if( type!=TK_SPACE && type!=TK_COMMENT ) break;
        z += n;
      }while( 1 );
      *pzParent = sqlite3_mprintf("%.*s", n, z);
      if( !*pzParent ) return SQLITE_NOMEM;
      sqlite3Dequote(*pzParent);
      *pzSql = z + n;
      return SQLITE_ROW;
    }
    z += n;
  }
  *pzSql = z;
  return SQLITE_DONE;
}

static int doltliteCommitFindMissingForeignKeyParent(
  sqlite3 *db,
  const ProllyHash *pCatalogHash,
  SchemaEntry *aSchema,
  int nSchema,
  char **pzChild,
  char **pzParent
){
  int i;
  *pzChild = 0;
  *pzParent = 0;
  for(i=0; i<nSchema; i++){
    const char *z;
    char *zParent = 0;
    int rc;
    if( !aSchema[i].zType || sqlite3_stricmp(aSchema[i].zType, "table")!=0
     || !aSchema[i].zSql || !aSchema[i].zName ){
      continue;
    }
    z = aSchema[i].zSql;
    while( (rc = doltliteCommitNextForeignKeyParent(&z, &zParent))==SQLITE_ROW ){
      int j;
      int found = 0;
      for(j=0; j<nSchema; j++){
        if( aSchema[j].zType
         && sqlite3_stricmp(aSchema[j].zType, "table")==0
         && aSchema[j].zName
         && sqlite3_stricmp(aSchema[j].zName, zParent)==0 ){
          found = 1;
          break;
        }
      }
      if( !found ){
        rc = doltliteNonlocalCatalogClaims(db, pCatalogHash, zParent, &found);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zParent);
          return rc;
        }
      }
      if( !found ){
        *pzChild = sqlite3_mprintf("%s", aSchema[i].zName);
        if( !*pzChild ){
          sqlite3_free(zParent);
          return SQLITE_NOMEM;
        }
        *pzParent = zParent;
        return SQLITE_OK;
      }
      sqlite3_free(zParent);
      zParent = 0;
    }
    if( rc!=SQLITE_DONE ) return rc;
  }
  return SQLITE_OK;
}

int doltliteCommitValidateForeignKeyParents(
  sqlite3 *db,
  sqlite3_context *context,
  const ProllyHash *pCatalogHash
){
  SchemaEntry *aSchema = 0;
  int nSchema = 0;
  char *zChild = 0;
  char *zParent = 0;
  char *zErr;
  int rc;

  rc = loadSchemaFromCatalog(db, doltliteGetChunkStore(db),
      doltliteGetCache(db), pCatalogHash, &aSchema, &nSchema);
  if( rc==SQLITE_OK ){
    rc = doltliteCommitFindMissingForeignKeyParent(
        db, pCatalogHash, aSchema, nSchema, &zChild, &zParent);
  }
  freeSchemaEntries(aSchema, nSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zChild);
    sqlite3_free(zParent);
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  if( !zParent ) return SQLITE_OK;

  zErr = sqlite3_mprintf(
      "foreign key on table `%q` requires the referenced table `%q`",
      zChild, zParent);
  sqlite3_free(zChild);
  sqlite3_free(zParent);
  if( !zErr ){
    sqlite3_result_error_code(context, SQLITE_NOMEM);
    return SQLITE_NOMEM;
  }
  sqlite3_result_error(context, zErr, -1);
  sqlite3_free(zErr);
  return SQLITE_ERROR;
}

/* The seed commit records the empty catalog hash. A catalog holding only
** an empty schema table with no user_version or application_id is that
** same empty database. */
static int commitCatalogIsPristine(
  sqlite3 *db,
  const ProllyHash *pCat,
  int *pPristine
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  struct TableEntry *aTables = 0;
  const u8 *pEntries = 0;
  u8 *pData = 0;
  int nData = 0, nTables = 0, iFormat = 0;
  int rc;

  *pPristine = prollyHashIsEmpty(pCat);
  if( *pPristine || !cs ) return SQLITE_OK;
  rc = chunkStoreGet(cs, pCat, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( catalogParseHeaderEx(pData, nData, &iFormat, &nTables, &pEntries)
   && nTables==1
   && (iFormat!=CATALOG_FORMAT_V5
       || memcmp(pData + CAT_HEADER_SIZE_V3, "\0\0\0\0\0\0\0\0", 8)==0) ){
    rc = doltliteLoadCatalog(db, pCat, &aTables, &nTables, 0);
    if( rc==SQLITE_OK ){
      *pPristine = nTables==1 && aTables[0].iTable==1
                && prollyHashIsEmpty(&aTables[0].root);
      doltliteFreeCatalog(aTables, nTables);
    }
  }
  sqlite3_free(pData);
  return rc;
}

typedef struct DoltliteNamedCommit DoltliteNamedCommit;
struct DoltliteNamedCommit {
  const char *zBranch;
  DoltliteBranchCommitState state;
  ProllyHash commit;
};

static int doltliteCommitNamedRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  DoltliteNamedCommit *p = pArg;
  ProllyHash workingSet;
  int rc = chunkStoreGetBranchWorkingSet(cs, p->zBranch, &workingSet);
  if( rc==SQLITE_NOTFOUND ){
    memset(&workingSet, 0, sizeof(workingSet));
    rc = SQLITE_OK;
  }
  if( rc==SQLITE_OK
   && prollyHashCompare(&workingSet, &p->state.workingSet)!=0 ) rc = SQLITE_BUSY;
  if( rc==SQLITE_OK ) rc = chunkStoreUpdateBranch(cs, p->zBranch, &p->commit);
  if( rc==SQLITE_OK ){
    rc = doltliteWriteBranchCommitState(db, p->zBranch, &p->state, &p->commit);
  }
  return rc;
}

static void doltliteCommitNamedBranch(
  sqlite3_context *context,
  DoltliteCommitOptions *opts
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  DoltliteNamedCommit m;
  DoltliteBranchExpectation expected;
  ProllyHash catalog;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;
  memset(&m, 0, sizeof(m));
  m.zBranch = opts->zBranch;
  if( chunkStoreFindBranch(doltliteGetChunkStore(db),
      doltliteGetSessionBranch(db), 0)==SQLITE_NOTFOUND ){
    doltliteCmdResultWriteTxnError(context, db, SQLITE_BUSY);
    return;
  }
  if( opts->zAuthor
   && doltliteCommitValidateAuthor(context, opts->zAuthor)!=SQLITE_OK ) return;
  if( opts->zDate
   && doltliteParseCommitDate(opts->zDate, &opts->explicitTimestamp)!=SQLITE_OK ){
    sqlite3_result_error(context, "could not parse --date", -1);
    return;
  }
  if( (!opts->zMessage || !opts->zMessage[0]) && !opts->amend ){
    sqlite3_result_error(context, "dolt_commit requires a message", -1);
    return;
  }
  rc = doltliteReadBranchCommitState(db, m.zBranch, &m.state);
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_error(context, "commit target branch not found", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( !prollyHashIsEmpty(&m.state.conflicts)
   || doltliteSessionHasUnresolvedConflicts(db) ){
    sqlite3_result_error(context, "cannot commit: unresolved merge conflicts", -1);
    return;
  }
  if( !opts->force && !prollyHashIsEmpty(&m.state.violations) ){
    sqlite3_result_error(context, "cannot commit: unresolved constraint violations", -1);
    return;
  }
  if( opts->amend && (m.state.merging || (m.state.rebaseFlags
   && (m.state.rebaseFlags & WS_REBASE_FLAG_EDIT)==0)) ){
    sqlite3_result_error(context, "cannot amend during a merge or rebase", -1);
    return;
  }
  catalog = m.state.staged;
  if( opts->addAll ){
    rc = doltliteComposeStageAll(db, context, &m.state.working,
        &m.state.headCatalog, &m.state.staged, m.state.onRebasePlan, &catalog);
  }else if( opts->addModifiedOnly ){
    rc = doltliteCommitStageModifiedOnly(db, context, opts->amend,
        &m.state, &catalog);
  }
  if( rc!=SQLITE_OK ) return;
  if( prollyHashIsEmpty(&catalog) ) catalog = m.state.headCatalog;
  if( !m.state.merging && (!opts->amend || opts->skipEmpty)
   && prollyHashCompare(&catalog, &m.state.headCatalog)==0 ){
    if( opts->skipEmpty ){
      sqlite3_result_int(context, 0);
      return;
    }
    if( !opts->allowEmpty ){
      sqlite3_result_error(context, "nothing to commit, working tree clean", -1);
      return;
    }
  }
  if( !opts->force
   && doltliteCommitValidateForeignKeyParents(db, context, &catalog)!=SQLITE_OK ){
    return;
  }
  rc = doltliteCommitCreateObject(db, context, opts, &catalog, &m.commit, &m.state);
  if( rc!=SQLITE_OK ) return;
  if( !db->autoCommit || db->pSavepoint ){
    rc = doltlitePrepareJoinedCommit(db);
    if( rc==SQLITE_DONE ) rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }
  expected.zBranch = m.zBranch;
  expected.pTip = &m.state.head;
  doltliteTestRunBeforeRefInstallHook();
  rc = doltliteMutateRefsExpected(db, &expected, 1, doltliteCommitNamedRef, &m);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  doltliteStoreBranchMoved(db, m.zBranch);
  doltliteHashToHex(&m.commit, hex);
  doltliteHistoricalModulesReset(db);
  sqlite3_result_text(context, hex, -1, SQLITE_TRANSIENT);
}

static void doltliteCommitFuncImpl(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv,
  ProllyHash *pSavedStaged,
  int *pDidStage
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommitOptions opts;
  DoltliteTxnState mutationState;
  const char *zMessage, *zDate;
  int addAll, addModifiedOnly, amend, allowEmpty, skipEmpty, force;
  ProllyHash commitHash;
  ProllyHash catalogHash;
  ProllyHash sessionHeadBeforeLock;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  int sealTopLevel = doltliteSavepointIsTopLevelTxn(db)
                  && !doltliteInAtomicCommand(db);
  int pinnedRead = !db->autoCommit
                && sqlite3_txn_state(db, "main")==SQLITE_TXN_READ;
  int rc;
  int parseRc;

  memset(&mutationState, 0, sizeof(mutationState));

  if( db->nVdbeWrite>0 ){
    sqlite3_result_error(context,
        "cannot commit while a write statement is active", -1);
    return;
  }
  if( doltliteCmdRejectDetached(context) ) return;
  if( doltliteCmdRejectReadOnly(context) ) return;
  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    return;
  }

  parseRc = doltliteCommitParseOptions(context, argc, argv, &opts);
  if( parseRc==SQLITE_OK && opts.zBranch
   && strcmp(opts.zBranch, doltliteGetSessionBranch(db))!=0 ){
    doltliteCommitNamedBranch(context, &opts);
    return;
  }

  if( pinnedRead ){
    doltliteGetSessionHead(db, &sessionHeadBeforeLock);
    rc = doltliteRefreshAndConfirmHead(db, cs, &sessionHeadBeforeLock);
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_BUSY ){
        doltliteCmdResultBranchBusyOrGone(context, db, "commit");
      }
      sqlite3_result_error_code(context, rc);
      return;
    }
    chunkStoreUnlock(cs);
    doltliteInvalidateSessionWorkingState(db);
  }

  /* Seal top-level SAVEPOINT before option validation: Dolt keeps that
  ** SQL txn durable even when dolt_commit later errors. */
  if( sealTopLevel ){
    rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, sqlite3_errmsg(db), -1);
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  if( parseRc!=SQLITE_OK ) return;
  if( opts.zAuthor
   && doltliteCommitValidateAuthor(context, opts.zAuthor)!=SQLITE_OK ){
    return;
  }
  zMessage = opts.zMessage;
  zDate = opts.zDate;
  addAll = opts.addAll;
  addModifiedOnly = opts.addModifiedOnly;
  amend = opts.amend;
  allowEmpty = opts.allowEmpty;
  skipEmpty = opts.skipEmpty;
  force = opts.force;

  if( cs->refs.nBranches>0
   && chunkStoreFindBranch(cs, doltliteGetSessionBranch(db), 0)==SQLITE_NOTFOUND ){
    doltliteCmdResultWriteTxnError(context, db, SQLITE_BUSY);
    return;
  }

  if( amend ){
    u8 isMerging = 0;
    u8 isRebasing = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( isMerging ){
      sqlite3_result_error(context,
        "you are in the middle of a merge -- cannot amend", -1);
      return;
    }
    doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
    if( isRebasing
     && (doltliteGetSessionRebaseFlags(db) & WS_REBASE_FLAG_EDIT)==0 ){
      sqlite3_result_error(context,
        "you are in the middle of a rebase -- cannot amend", -1);
      return;
    }
    if( doltliteSessionHasPendingReplayCommit(db) ){
      sqlite3_result_error(context,
        "you are in the middle of a cherry-pick -- cannot amend", -1);
      return;
    }
  }

  if( !force && doltliteSessionHasConstraintViolations(db) ){
    sqlite3_result_error(context,
      "cannot commit: unresolved entries in dolt_constraint_violations. "
      "Resolve them (DELETE from the per-table vtable) then retry, or "
      "pass --force to commit anyway.",
      -1);
    return;
  }

  if( zDate ){
    if( doltliteParseCommitDate(zDate, &opts.explicitTimestamp)!=SQLITE_OK ){
      char *zErr = sqlite3_mprintf(
          "could not parse --date `%s` (expected YYYY-MM-DD or "
          "YYYY-MM-DDTHH:MM:SS[Z|±HH:MM])", zDate);
      sqlite3_result_error(context, zErr ? zErr : "bad --date", -1);
      sqlite3_free(zErr);
      return;
    }
  }

  if( (!zMessage || zMessage[0]==0) && !amend ){
    sqlite3_result_error(context,
      "dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')", -1);
    return;
  }

  {
    int hasSchemaConflicts = 0;
    rc = doltliteSessionHasSchemaConflicts(db, &hasSchemaConflicts);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    if( hasSchemaConflicts ){
      sqlite3_result_error(context,
        "cannot commit: unresolved schema conflicts. Abort the merge, align "
        "the schemas on one side, then rerun the merge.", -1);
      return;
    }
  }

  {
    ProllyHash cfHash;
    rc = doltliteGetSessionConflictsCatalog(db, &cfHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    if( !prollyHashIsEmpty(&cfHash) ){
      sqlite3_result_error(context,
        "cannot commit: unresolved merge conflicts. Use dolt_conflicts_resolve() first.", -1);
      return;
    }
  }

  if( !sealTopLevel
   && (!db->autoCommit
       || db->pSavepoint) ){
    /* Plain BEGIN and nested SAVEPOINT stay rollbackable until validation. */
    rc = doltlitePrepareJoinedCommit(db);
    if( rc==SQLITE_DONE ) rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, sqlite3_errmsg(db), -1);
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  if( addAll || addModifiedOnly ){
    doltliteGetSessionStaged(db, pSavedStaged);
    *pDidStage = 1;
  }

  if( addAll ){
    rc = doltliteAddStageAll(db, context, 0);
    if( rc!=SQLITE_OK ) return;
  }else if( addModifiedOnly ){
    rc = doltliteCommitStageModifiedOnly(db, context, amend, 0, 0);
    if( rc!=SQLITE_OK ) return;
  }

  /* Final merge commits refresh derived sqlite_stat rows. */
  {
    u8 isMergingCommit = 0;
    doltliteGetSessionMergeState(db, &isMergingCommit, 0, 0);
    if( isMergingCommit ){
      sqlite3_stmt *pProbe = 0;
      int hasStat1 = 0;
      if( sqlite3_prepare_v2(db,
          "SELECT 1 FROM main.sqlite_master "
          "WHERE type='table' AND name='sqlite_stat1' LIMIT 1",
          -1, &pProbe, 0)==SQLITE_OK ){
        if( sqlite3_step(pProbe)==SQLITE_ROW ) hasStat1 = 1;
        sqlite3_finalize(pProbe);
      }
      if( hasStat1 ){
        ProllyHash refreshedCat;
        (void)sqlite3_exec(db, "ANALYZE", 0, 0, 0);
        if( doltliteFlushCatalogToHash(db, &refreshedCat)==SQLITE_OK ){
          rc = doltliteSetSessionStaged(db, &refreshedCat);
          if( rc!=SQLITE_OK ){
            sqlite3_result_error_code(context, rc);
            return;
          }
        }
      }
    }
  }

  doltliteGetSessionStaged(db, &catalogHash);
  if( prollyHashIsEmpty(&catalogHash) ){
    if( skipEmpty ){
      sqlite3_result_int(context, 0);
      return;
    }
    /* Nothing staged: --amend reuses HEAD's tree, as --allow-empty does. */
    if( allowEmpty || amend ){
      ProllyHash headCatHash;
      rc = doltliteGetHeadCatalogHash(db, &headCatHash);
      if( rc==SQLITE_OK ){
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
    if( !isMerging && (!amend || skipEmpty)
     && !doltliteSessionHasPendingReplayCommit(db) ){
      ProllyHash headCatHash;
      int bSame = 0;
      rc = doltliteGetHeadCatalogHash(db, &headCatHash);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash) ){
        bSame = prollyHashCompare(&catalogHash, &headCatHash)==0;
      }else if( rc==SQLITE_OK ){
        rc = commitCatalogIsPristine(db, &catalogHash, &bSame);
        if( rc!=SQLITE_OK ){
          sqlite3_result_error_code(context, rc);
          return;
        }
      }
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
        return;
      }
      if( bSame ){
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

  if( !force ){
    rc = doltliteCommitValidateForeignKeyParents(db, context, &catalogHash);
    if( rc!=SQLITE_OK ) return;
  }

  rc = doltliteCommitCreateObject(db, context, &opts, &catalogHash, &commitHash, 0);
  if( rc!=SQLITE_OK ) return;

  rc = doltliteForEachUserTableInCatalog(db, "dolt_workspace_",
      doltliteWorkspaceTableModule(), &catalogHash);
  if( rc==SQLITE_OK ){
    rc = doltliteForEachUserTableInCatalog(db, "dolt_blame_",
        doltliteBlameTableModule(), &catalogHash);
  }
  if( rc==SQLITE_OK ){
    rc = doltliteForEachUserTableInCatalog(db, "dolt_constraint_violations_",
        doltliteConstraintViolationTableModule(), &catalogHash);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  if( !pinnedRead ) doltliteGetSessionHead(db, &sessionHeadBeforeLock);
  rc = doltliteSaveTxnState(db, &mutationState);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  {
    u8 wasMerging = 0;
    doltliteGetSessionMergeState(db, &wasMerging, 0, 0);
    if( wasMerging ){
      rc = doltliteClearSessionMergeState(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context,
            doltliteRestoreTxnStateOnFailure(db, &mutationState, rc));
        return;
      }
    }
  }

  /* --force is the only way to reach here with CVs present. Leave them on
  ** the working set so they follow the branch, matching Dolt. */

  {
    ProllyHash workingCatHash;
    rc = doltliteFlushCatalogToHash(db, &workingCatHash);
    /* Test hook: stop after the catalog hash exists and before it is stored,
    ** so a peer insert can land in that gap. */
    if( rc==SQLITE_OK ) doltliteTestPause("DOLTLITE_PAUSE_BEFORE_BRANCH_ADVANCE");
    if( rc==SQLITE_OK ){
      rc = doltliteCompareAndAdvanceBranchCurrentCatalog(
          db, &sessionHeadBeforeLock, &commitHash, &catalogHash,
          &workingCatHash);
    }
  }
  if( rc==SQLITE_BUSY ){
    rc = doltliteRestoreTxnStateOnFailure(db, &mutationState, rc);
    if( rc==SQLITE_BUSY ){
      sqlite3_result_error(context,
          "cannot commit: database is busy or branch HEAD changed. "
          "Please retry your transaction.", -1);
    }
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context,
        doltliteRestoreTxnStateOnFailure(db, &mutationState, rc));
    return;
  }
  doltliteTxnStateClear(&mutationState);
  (void)doltliteSetSessionPendingReplayCommit(db, 0);

  doltliteHashToHex(&commitHash, hexBuf);

  doltliteHistoricalModulesReset(db);

  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}


static void doltliteCommitFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ProllyHash savedStaged;
  int didStage = 0;
  doltliteCommitFuncImpl(context, argc, argv, &savedStaged, &didStage);
  if( didStage && !doltliteVcInterruptDeferred(db, 0)
   && (context->isError==SQLITE_INTERRUPT
       || AtomicLoad(&db->u1.isInterrupted)) ){
    int rc = doltliteSetSessionStaged(db, &savedStaged);
    sqlite3_result_error_code(context, rc==SQLITE_OK ? SQLITE_INTERRUPT : rc);
  }
}

typedef struct CommitAllBranch CommitAllBranch;
struct CommitAllBranch {
  const char *zBranch;
  Btree *pBt;
  DoltliteBranchCommitState state;
  ProllyHash commit;
  int bCommitted;
};

typedef struct CommitAll CommitAll;
struct CommitAll {
  CommitAllBranch *a;
  int n;
};

static int commitAllRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  CommitAll *p = pArg;
  int i, rc = SQLITE_OK;
  (void)db;
  for(i=0; rc==SQLITE_OK && i<p->n; i++){
    if( p->a[i].bCommitted ){
      rc = chunkStoreUpdateBranch(cs, p->a[i].zBranch, &p->a[i].commit);
    }
  }
  return rc;
}

static int commitAllCompare(const void *a, const void *b){
  return strcmp(((const CommitAllBranch*)a)->zBranch,
                ((const CommitAllBranch*)b)->zBranch);
}

static int commitAllComposeCatalog(
  sqlite3_context *context,
  DoltliteCommitOptions *opts,
  DoltliteBranchCommitState *st,
  ProllyHash *pCatalog
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  int rc = SQLITE_OK;
  *pCatalog = st->staged;
  if( opts->addAll ){
    rc = doltliteComposeStageAll(db, context, &st->working, &st->headCatalog,
        &st->staged, st->onRebasePlan, pCatalog);
  }else if( opts->addModifiedOnly ){
    rc = doltliteCommitStageModifiedOnly(db, context, opts->amend, st, pCatalog);
  }
  if( rc==SQLITE_OK && prollyHashIsEmpty(pCatalog) ) *pCatalog = st->headCatalog;
  return rc;
}

/* dolt_commit seals the transaction before it finds nothing to commit;
** dolt_commit_all must refuse an empty session branch with the
** transaction still open. */
static int commitAllSessionEmpty(
  sqlite3_context *context,
  DoltliteCommitOptions *opts,
  int *pbEmpty
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  DoltliteBranchCommitState st;
  ProllyHash head, catalog;
  u8 merging = 0;
  int rc;
  *pbEmpty = 0;
  if( opts->amend || opts->allowEmpty || opts->skipEmpty ) return SQLITE_OK;
  doltliteGetSessionMergeState(db, &merging, 0, 0);
  if( merging ) return SQLITE_OK;
  rc = doltliteReadBranchCommitState(db, doltliteGetSessionBranch(db), &st);
  if( rc==SQLITE_OK ){
    rc = doltliteBtreeBranchSnapshot(db->aDb[0].pBt, &head, &st.staged,
                                     &st.working);
  }
  if( rc==SQLITE_OK && prollyHashCompare(&head, &st.head)!=0 ) return SQLITE_OK;
  if( rc==SQLITE_OK ) rc = commitAllComposeCatalog(context, opts, &st, &catalog);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  *pbEmpty = prollyHashCompare(&catalog, &st.headCatalog)==0;
  return SQLITE_OK;
}

/* Another branch of this store written in the transaction: build its
** commit from the transaction's working and staged catalogs. Nothing is
** installed here. */
static int commitAllPrepareBranch(
  sqlite3_context *context,
  DoltliteCommitOptions *opts,
  CommitAllBranch *b
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  DoltliteBranchCommitState *st = &b->state;
  ProllyHash head, staged, working, catalog;
  int rc;
  rc = doltliteReadBranchCommitState(db, b->zBranch, st);
  if( rc==SQLITE_OK ) rc = doltliteBtreeBranchSnapshot(b->pBt, &head, &staged, &working);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  if( prollyHashCompare(&head, &st->head)!=0 ){
    sqlite3_result_error(context, "cannot commit: database is busy or branch "
        "HEAD changed. Please retry your transaction.", -1);
    sqlite3_result_error_code(context, SQLITE_BUSY);
    return SQLITE_BUSY;
  }
  st->working = working;
  st->staged = staged;
  if( !prollyHashIsEmpty(&st->conflicts) ){
    sqlite3_result_error(context, "cannot commit: unresolved merge conflicts", -1);
    return SQLITE_ERROR;
  }
  if( !opts->force && !prollyHashIsEmpty(&st->violations) ){
    sqlite3_result_error(context,
        "cannot commit: unresolved constraint violations", -1);
    return SQLITE_ERROR;
  }
  if( opts->amend && (st->merging || st->rebaseFlags) ){
    sqlite3_result_error(context, "cannot amend during a merge or rebase", -1);
    return SQLITE_ERROR;
  }
  rc = commitAllComposeCatalog(context, opts, st, &catalog);
  if( rc!=SQLITE_OK ) return rc;
  if( !st->merging && (!opts->amend || opts->skipEmpty)
   && prollyHashCompare(&catalog, &st->headCatalog)==0 ){
    if( opts->skipEmpty ) return SQLITE_OK;
    if( !opts->allowEmpty ){
      char *zErr = sqlite3_mprintf("nothing to commit on %s", b->zBranch);
      sqlite3_result_error(context, zErr ? zErr : "nothing to commit", -1);
      sqlite3_free(zErr);
      return SQLITE_ERROR;
    }
  }
  if( !opts->force
   && doltliteCommitValidateForeignKeyParents(db, context, &catalog)!=SQLITE_OK ){
    return SQLITE_ERROR;
  }
  rc = doltliteCommitCreateObject(db, context, opts, &catalog, &b->commit, st);
  if( rc==SQLITE_OK ) b->bCommitted = 1;
  return rc;
}

static void commitAllAppendJson(sqlite3_str *pStr, const char *zBranch,
                                const char *zHash){
  const char *z;
  if( sqlite3_str_length(pStr)>1 ) sqlite3_str_appendchar(pStr, 1, ',');
  sqlite3_str_appendchar(pStr, 1, '"');
  for(z=zBranch; *z; z++){
    if( *z=='"' || *z=='\\' ) sqlite3_str_appendchar(pStr, 1, '\\');
    sqlite3_str_appendchar(pStr, 1, *z);
  }
  sqlite3_str_appendf(pStr, "\":\"%s\"", zHash);
}

/* Commits every branch of this database the transaction wrote, or the
** session's branch when it wrote none, and publishes them together. The
** result maps each branch committed to its new commit hash. */
static void doltliteCommitAllFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommitOptions opts;
  CommitAll all;
  DoltliteBranchExpectation *aExpected = 0;
  sqlite3_str *pStr = 0;
  const char *zSession = doltliteGetSessionBranch(db);
  char *zSessionHash = 0;
  int bSessionDirty;
  int nExpected = 0;
  int i, rc;

  memset(&all, 0, sizeof(all));
  if( db->nVdbeWrite>0 ){
    sqlite3_result_error(context,
        "cannot commit while a write statement is active", -1);
    return;
  }
  if( doltliteCmdRejectDetached(context) ) return;
  if( doltliteCmdRejectReadOnly(context) ) return;
  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    return;
  }
  if( doltliteCommitParseOptions(context, argc, argv, &opts)!=SQLITE_OK ) return;
  if( opts.zBranch ){
    sqlite3_result_error(context,
        "--branch is not supported by dolt_commit_all", -1);
    return;
  }

  all.a = sqlite3_malloc64(sizeof(CommitAllBranch)*(sqlite3_uint64)db->nDb);
  if( !all.a ){
    sqlite3_result_error_nomem(context);
    return;
  }
  for(i=2; i<db->nDb; i++){
    Btree *q = db->aDb[i].pBt;
    if( !doltliteBtreeSameStore(db->aDb[0].pBt, q)
     || !doltliteBtreeTxnWrote(q) ) continue;
    memset(&all.a[all.n], 0, sizeof(all.a[0]));
    all.a[all.n].zBranch = doltliteBtreeBranchName(q);
    all.a[all.n].pBt = q;
    all.n++;
  }
  qsort(all.a, all.n, sizeof(all.a[0]), commitAllCompare);

  bSessionDirty = !db->autoCommit && doltliteBtreeTxnWrote(db->aDb[0].pBt);
  for(i=0; i<all.n; i++){
    rc = commitAllPrepareBranch(context, &opts, &all.a[i]);
    if( rc!=SQLITE_OK ) goto commit_all_done;
  }
  if( bSessionDirty ){
    int bEmpty;
    if( commitAllSessionEmpty(context, &opts, &bEmpty)!=SQLITE_OK ){
      goto commit_all_done;
    }
    if( bEmpty ){
      char *zMsg = sqlite3_mprintf("nothing to commit on %s", zSession);
      sqlite3_result_error(context, zMsg ? zMsg : "nothing to commit", -1);
      sqlite3_free(zMsg);
      goto commit_all_done;
    }
  }

  if( bSessionDirty || all.n==0 ){
    ProllyHash savedStaged;
    int didStage = 0;
    doltliteCommitFuncImpl(context, argc, argv, &savedStaged, &didStage);
    if( context->isError ){
      const char *zErr = (const char*)sqlite3_value_text(context->pOut);
      if( zErr && strncmp(zErr, "nothing to commit", 17)==0 ){
        char *zMsg = sqlite3_mprintf("nothing to commit on %s", zSession);
        sqlite3_result_error(context, zMsg ? zMsg : zErr, -1);
        sqlite3_free(zMsg);
      }
      goto commit_all_done;
    }
    if( sqlite3_value_type(context->pOut)==SQLITE_TEXT ){
      zSessionHash = sqlite3_mprintf("%s",
          (const char*)sqlite3_value_text(context->pOut));
      if( !zSessionHash ){
        sqlite3_result_error_nomem(context);
        goto commit_all_done;
      }
    }
  }else if( !db->autoCommit || db->pSavepoint ){
    rc = doltlitePrepareJoinedCommit(db);
    if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
      sqlite3_result_error_code(context, rc);
      goto commit_all_done;
    }
  }

  aExpected = sqlite3_malloc64(sizeof(*aExpected)*(sqlite3_uint64)(all.n+1));
  if( !aExpected ){
    sqlite3_result_error_nomem(context);
    goto commit_all_done;
  }
  for(i=0; i<all.n; i++){
    if( !all.a[i].bCommitted ) continue;
    aExpected[nExpected].zBranch = all.a[i].zBranch;
    aExpected[nExpected].pTip = &all.a[i].state.head;
    nExpected++;
  }
  if( nExpected>0 ){
    rc = doltliteMutateRefsExpected(db, aExpected, nExpected,
                                    commitAllRefs, &all);
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_BUSY ){
        sqlite3_result_error(context, "cannot commit: database is busy or "
            "branch HEAD changed. Please retry your transaction.", -1);
      }
      sqlite3_result_error_code(context, rc);
      goto commit_all_done;
    }
    for(i=0; i<all.n; i++){
      if( all.a[i].bCommitted ){
        doltliteBtreeBranchCommitted(all.a[i].pBt, &all.a[i].commit);
      }
    }
  }

  pStr = sqlite3_str_new(db);
  sqlite3_str_appendchar(pStr, 1, '{');
  for(i=0; i<=all.n; i++){
    const char *zNext = i<all.n ? all.a[i].zBranch : 0;
    if( zSessionHash && (!zNext || strcmp(zSession, zNext)<0) ){
      commitAllAppendJson(pStr, zSession, zSessionHash);
      sqlite3_free(zSessionHash);
      zSessionHash = 0;
    }
    if( zNext && all.a[i].bCommitted ){
      char hex[PROLLY_HASH_SIZE*2+1];
      doltliteHashToHex(&all.a[i].commit, hex);
      commitAllAppendJson(pStr, zNext, hex);
    }
  }
  sqlite3_str_appendchar(pStr, 1, '}');
  rc = sqlite3_str_errcode(pStr);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    sqlite3_str_reset(pStr);
  }else{
    sqlite3_result_text(context, sqlite3_str_value(pStr), -1, SQLITE_TRANSIENT);
  }
  doltliteHistoricalModulesReset(db);

commit_all_done:
  if( pStr ) sqlite3_free(sqlite3_str_finish(pStr));
  sqlite3_free(aExpected);
  sqlite3_free(zSessionHash);
  sqlite3_free(all.a);
}

int doltliteCommitCmdRegister(sqlite3 *db){
  int rc = doltliteCreateCommandFunc(db, "dolt_commit", -1,
                                     doltliteCommitFunc);
  if( rc==SQLITE_OK ){
    rc = doltliteCreateCommandFunc(db, "dolt_commit_all", -1,
                                   doltliteCommitAllFunc);
  }
  return rc;
}

#endif
