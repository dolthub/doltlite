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
#include "doltlite_name_index.h"
#include <stddef.h>
#include "doltlite_ignore.h"

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
/* Parse dolt_commit's argv into opts. On a malformed option the context error
** is set and a non-OK code returned; explicitTimestamp is left zero for the
** caller to fill from --date after its own validation ordering. */
typedef struct DoltliteCommitOptions {
  const char *zMessage;
  const char *zAuthor;
  const char *zDate;
  int addAll;
  int addModifiedOnly;
  int amend;
  int allowEmpty;
  int skipEmpty;
  int force;
  i64 explicitTimestamp;
} DoltliteCommitOptions;

static int doltliteCommitParseOptions(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv,
  DoltliteCommitOptions *opts
){
  int i;
  memset(opts, 0, sizeof(*opts));
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( arg[0]=='-' && arg[1]!='-' && arg[1]!=0 && arg[2]!=0 ){
      int j;
      for(j=1; arg[j]; j++){
        if( arg[j]=='A' ){
          opts->addAll = 1;
        }else if( arg[j]=='a' ){
          opts->addModifiedOnly = 1;
        }else if( arg[j]=='f' ){
          opts->force = 1;
        }else if( arg[j]=='m' ){
          if( arg[j+1]!=0 ){
            opts->zMessage = &arg[j+1];
          }else if( i+1<argc ){
            opts->zMessage = (const char*)sqlite3_value_text(argv[++i]);
          }else{
            sqlite3_result_error(context, "no value for option `message'", -1);
            return SQLITE_ERROR;
          }
          break;
        }else{
          char *zErr = sqlite3_mprintf("unknown option `-%c'", arg[j]);
          if( zErr ){
            sqlite3_result_error(context, zErr, -1);
            sqlite3_free(zErr);
          }else{
            sqlite3_result_error_nomem(context);
          }
          return SQLITE_ERROR;
        }
      }
    }else if( strcmp(arg, "-m")==0 ){
      if( i+1<argc ){
        opts->zMessage = (const char*)sqlite3_value_text(argv[++i]);
      }else{
        sqlite3_result_error(context, "no value for option `message'", -1);
        return SQLITE_ERROR;
      }
    }else if( strcmp(arg, "--message")==0 ){
      if( i+1<argc ){
        opts->zMessage = (const char*)sqlite3_value_text(argv[++i]);
      }else{
        sqlite3_result_error(context, "no value for option `message'", -1);
        return SQLITE_ERROR;
      }
    }else if( strcmp(arg, "--author")==0 ){
      if( i+1<argc ){
        opts->zAuthor = (const char*)sqlite3_value_text(argv[++i]);
      }else{
        sqlite3_result_error(context, "no value for option `author'", -1);
        return SQLITE_ERROR;
      }
    }else if( strcmp(arg, "--date")==0 ){
      if( i+1<argc ){
        opts->zDate = (const char*)sqlite3_value_text(argv[++i]);
      }else{
        sqlite3_result_error(context, "no value for option `date'", -1);
        return SQLITE_ERROR;
      }
    }else if( strcmp(arg, "--amend")==0 ){
      opts->amend = 1;
    }else if( strcmp(arg, "--allow-empty")==0 ){
      opts->allowEmpty = 1;
    }else if( strcmp(arg, "--skip-empty")==0 ){
      opts->skipEmpty = 1;
    }else if( strcmp(arg, "-f")==0 || strcmp(arg, "--force")==0 ){
      opts->force = 1;
    }else if( strcmp(arg, "-A")==0 ){
      opts->addAll = 1;
    }else if( strcmp(arg, "-a")==0 || strcmp(arg, "--all")==0 ){
      opts->addModifiedOnly = 1;
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      return SQLITE_ERROR;
    }else{
      char *zErr = sqlite3_mprintf(
          "commit does not take positional arguments, but found 1: %s", arg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      return SQLITE_ERROR;
    }
  }
  return SQLITE_OK;
}

/* Rebuild the staged catalog for `dolt_commit -a`, overlaying working-tree
** changes for tables that already exist in HEAD onto the staged catalog, and
** publish it as the session's staged catalog. On failure the context error is
** set and a non-OK code returned. */
static int doltliteCommitStageModifiedOnly(sqlite3 *db, sqlite3_context *context){
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

  memset(&workingIdx, 0, sizeof(workingIdx));
  memset(&headIdx, 0, sizeof(headIdx));
  memset(&stagedIdx, 0, sizeof(stagedIdx));

  #define FREE_ADD_MODIFIED_CATALOGS() do { \
    sqlite3_free(aRemoveStaged); \
    addNameIndexFree(&workingIdx); \
    addNameIndexFree(&headIdx); \
    addNameIndexFree(&stagedIdx); \
    doltliteFreeCatalog(aWorking, nWorking); \
    doltliteFreeCatalog(aHead, nHead); \
    doltliteFreeCatalog(aStaged, nStaged); \
  } while(0)

  rc = doltliteFlushCatalogToHash(db, &workingHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to flush", -1);
    return rc;
  }
  rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to load working catalog", -1);
    FREE_ADD_MODIFIED_CATALOGS();
    return rc;
  }
  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash) ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aHead, &nHead, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to load HEAD catalog", -1);
      FREE_ADD_MODIFIED_CATALOGS();
      return rc;
    }
  }

  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash) ){
    rc = doltliteLoadCatalog(db, &stagedHash, &aStaged, &nStaged, 0);
  }else if( !prollyHashIsEmpty(&headCatHash) ){
    rc = doltliteLoadCatalog(db, &headCatHash, &aStaged, &nStaged, 0);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to load staged catalog", -1);
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

  /* The master entry carries no name and pairs by table number 1. The
  ** staged catalog must use the WORKING master root so the schema rows
  ** and the overlaid entries share one numbering domain; keeping HEAD's
  ** master under working-numbered entries makes the serializer pair
  ** entries with the wrong rows. */
  for(j=0; j<nWorking; j++){
    if( aWorking[j].iTable!=1 ) continue;
    for(k=0; k<nStaged; k++){
      if( aStaged[k].iTable==1 ){
        sqlite3_free(aStaged[k].zName);
        aStaged[k] = aWorking[j];
        aStaged[k].zName = 0;
        break;
      }
    }
    break;
  }

  for(j=0; j<nWorking; j++){
    const char *zName = aWorking[j].zName;
    int updated = 0;
    char *zDup;
    struct TableEntry *pStaged;
    if( !addNameIndexFind(&headIdx, zName) ) continue;

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
    if( addNameIndexFind(&workingIdx, zName) ) continue;
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

  /* Index entries carry no name, so the by-name overlays above never
  ** refresh them: the staged list still holds the old index roots under
  ** tables whose data was just staged from working, committing a catalog
  ** whose indexes disagree with their tables. Rebuild the unnamed
  ** entries so each index follows its table's source: indexes of
  ** working-sourced tables adopt the working entries, indexes kept for
  ** staged-only tables stay, and indexes of deleted tables go away.
  ** Parents resolve through each catalog's own schema rows because entry
  ** numbers are meaningless across catalogs. */
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
      sqlite3_result_error(context, "failed to load schema for staging", -1);
      FREE_ADD_MODIFIED_CATALOGS();
      return rc;
    }

    for(k2=0; k2<nStaged; ){
      const char *zParent = 0;
      const char *zIdxName = 0;
      if( aStaged[k2].iTable<=1 || aStaged[k2].zName ){ k2++; continue; }
      for(i2=0; i2<nOldSchema; i2++){
        if( aOldSchema[i2].zType
         && strcmp(aOldSchema[i2].zType, "index")==0
         && aOldSchema[i2].iRootpage==aStaged[k2].iTable ){
          zParent = aOldSchema[i2].zTblName;
          zIdxName = aOldSchema[i2].zName;
          break;
        }
      }
      if( zParent
       && amTableStagedByName(aStaged, nStaged, zParent)
       && !(addNameIndexFind(&workingIdx, zParent)
            && addNameIndexFind(&headIdx, zParent)) ){
        /* Staged-sourced index: its table was staged explicitly and is
        ** not refreshed from working, so the entry keeps the staged data
        ** root — but its number is from the add-time catalog's domain,
        ** which the serializer resolves against working-domain schema
        ** rows. Renumber by index name so the row pairs; an index that
        ** no longer exists in working falls through to the drop. */
        int renumbered = 0;
        for(i2=0; i2<nWorkSchema; i2++){
          if( aWorkSchema[i2].zType
           && strcmp(aWorkSchema[i2].zType, "index")==0
           && aWorkSchema[i2].zName && zIdxName
           && strcmp(aWorkSchema[i2].zName, zIdxName)==0 ){
            aStaged[k2].iTable = aWorkSchema[i2].iRootpage;
            renumbered = 1;
            break;
          }
        }
        if( renumbered ){
          k2++;
          continue;
        }
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

  if( nStaged==0 ){
    sqlite3_result_error(context,
      "nothing to commit, working tree clean (use dolt_add to stage changes)", -1);
    FREE_ADD_MODIFIED_CATALOGS();
    return SQLITE_ERROR;
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
      rc = doltliteSetSessionStaged(db, &newStagedHash);
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

/* Resolve the parent, author, and message (handling --amend and --author
** parsing) and store the new commit object, returning its hash via
** pCommitHashOut. On failure the context error is set and non-OK returned. */
static int doltliteCommitCreateObject(
  sqlite3 *db,
  sqlite3_context *context,
  const DoltliteCommitOptions *opts,
  const ProllyHash *pCatalogHash,
  ProllyHash *pCommitHashOut
){
  ProllyHash parentHash;
  char *zParsedName = 0, *zParsedEmail = 0;
  const char *zMessage;
  const char *zAuthor;
  int rc;

  assert( db!=0 && context!=0 && opts!=0 );
  assert( pCatalogHash!=0 && pCommitHashOut!=0 );
  zMessage = opts->zMessage;
  zAuthor = opts->zAuthor;
  doltliteGetSessionHead(db, &parentHash);

  if( opts->amend ){
    DoltliteCommit headCommit;
    ProllyHash headHash;
    memset(&headCommit, 0, sizeof(headCommit));
    doltliteGetSessionHead(db, &headHash);
    if( prollyHashIsEmpty(&headHash) ){
      sqlite3_result_error(context,
        "cannot --amend: branch has no commits", -1);
      return SQLITE_ERROR;
    }
    rc = doltliteLoadCommit(db, &headHash, &headCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context,
        "cannot --amend: failed to load HEAD commit", -1);
      return rc;
    }
    if( doltliteCommitParentCount(&headCommit)==0 ){
      doltliteCommitClear(&headCommit);
      sqlite3_result_error(context,
        "cannot --amend: HEAD has no parent (initial commit)", -1);
      return SQLITE_ERROR;
    }

    {
      const ProllyHash *pParent = doltliteCommitParentHash(&headCommit, 0);
      if( !pParent || prollyHashIsEmpty(pParent) ){
        doltliteCommitClear(&headCommit);
        sqlite3_result_error(context,
          "cannot --amend: HEAD has no parent (initial commit)", -1);
        return SQLITE_ERROR;
      }
      memcpy(&parentHash, pParent, sizeof(ProllyHash));
    }
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
    const char *p = zMessage;
    while( *p==' ' || *p=='\t' || *p=='\n' || *p=='\r' ) p++;
    if( *p==0 ){
      sqlite3_free(zParsedName);
      sqlite3_free(zParsedEmail);
      sqlite3_result_error(context,
        "dolt_commit requires a non-empty message", -1);
      return SQLITE_ERROR;
    }
  }

  rc = doltliteCreateAndStoreCommitWithTime(db, &parentHash, pCatalogHash,
      zMessage, zParsedName, zParsedEmail, 0, 0, opts->explicitTimestamp,
      pCommitHashOut);
  sqlite3_free(zParsedName);
  sqlite3_free(zParsedEmail);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  return SQLITE_OK;
}

static void doltliteCommitFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommitOptions opts;
  const char *zMessage, *zDate;
  int addAll, addModifiedOnly, amend, allowEmpty, skipEmpty, force;
  ProllyHash commitHash;
  ProllyHash catalogHash;
  ProllyHash sessionHeadBeforeLock;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  int sealTopLevel = doltliteSavepointIsTopLevelTxn(db);
  int rc;

  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    return;
  }

  /* Top-level SAVEPOINT is sealed before option validation because Dolt keeps
  ** that SQL transaction boundary durable even when dolt_commit later errors. */
  if( sealTopLevel ){
    rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, sqlite3_errmsg(db), -1);
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  if( doltliteCommitParseOptions(context, argc, argv, &opts)!=SQLITE_OK ){
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

  if( !force && doltliteSessionHasConstraintViolations(db) ){
    sqlite3_result_error(context,
      "cannot commit: unresolved entries in dolt_constraint_violations. "
      "Resolve them (DELETE from the per-table vtable) then retry, or "
      "pass --force to commit anyway.",
      -1);
    return;
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
    opts.explicitTimestamp = (i64)timegm(&tm);
  }

  if( !zMessage || zMessage[0]==0 ){
    sqlite3_result_error(context,
      "dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')", -1);
    return;
  }

  if( !sealTopLevel
   && (!db->autoCommit
       || sqlite3_txn_state(db, "main")!=SQLITE_TXN_NONE
       || db->pSavepoint) ){
    /* Plain BEGIN and nested SAVEPOINT cases stay rollbackable until argument
    ** validation and basic commit guards have succeeded. */
    rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, sqlite3_errmsg(db), -1);
      sqlite3_result_error_code(context, rc);
      return;
    }
  }

  rc = doltlitePrepareCatalogForPersistence(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to prepare catalog", -1);
    return;
  }

  if( addAll ){

    rc = doltliteFlushCatalogToHash(db, &catalogHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(context, "failed to flush", -1);
      return;
    }
    rc = doltliteSetSessionStaged(db, &catalogHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
  }else if( addModifiedOnly ){
    rc = doltliteCommitStageModifiedOnly(db, context);
    if( rc!=SQLITE_OK ) return;
  }

  if( doltliteSessionHasSchemaConflicts(db) ){
    sqlite3_result_error(context,
      "cannot commit: unresolved schema conflicts. Abort the merge, align "
      "the schemas on one side, then rerun the merge.", -1);
    return;
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

  rc = doltliteCommitCreateObject(db, context, &opts, &catalogHash, &commitHash);
  if( rc!=SQLITE_OK ) return;

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
      rc = doltliteClearSessionMergeState(db);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(context, rc);
        return;
      }
    }
  }

  {
    extern int doltliteClearAllConstraintViolations(sqlite3*);
    if( doltliteSessionHasConstraintViolations(db) ){
      doltliteClearAllConstraintViolations(db);
    }
  }

  {
    ProllyHash workingCatHash;
    rc = doltliteFlushCatalogToHash(db, &workingCatHash);
    if( rc==SQLITE_OK ){
      rc = doltliteAdvanceBranch(db, &commitHash, &catalogHash, &workingCatHash);
    }
  }
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
  rc = doltliteRegisterWorkspaceTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  rc = doltliteRegisterHistoryTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  rc = doltliteRegisterAtTablesForCatalog(db, &catalogHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  rc = doltliteRegisterBlameTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  rc = doltliteRefreshConstraintViolationTables(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}


int doltliteCommitCmdRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_commit", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteCommitFunc, 0, 0);
}

#endif
