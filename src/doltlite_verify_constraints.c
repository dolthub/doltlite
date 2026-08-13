
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

/*
** SELECT dolt_verify_constraints([--all] [--output-only] [table...]);
**
** Mirrors Dolt's dolt_verify_constraints stored procedure:
**   - Default: only tables that differ from HEAD are scanned, but every
**     violating row in those tables is reported (not just rows new since HEAD).
**   - --all: scan every table against an empty ancestor catalog.
**   - Optional table names restrict which tables are scanned.
**   - --output-only computes violations but does not persist them.
** Returns 0 if no violations, 1 if any were found.
*/

static int tableExists(sqlite3 *db, const char *zName){
  sqlite3_stmt *pStmt = 0;
  char *zSql;
  int found = 0;
  int rc;

  zSql = sqlite3_mprintf(
      "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=%Q",
      zName);
  if( !zSql ) return -1;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return -1;
  if( sqlite3_step(pStmt)==SQLITE_ROW ) found = 1;
  sqlite3_finalize(pStmt);
  return found;
}

/* Collect user table names that differ between two catalogs. */
static int collectChangedTables(
  sqlite3 *db,
  const ProllyHash *pAncCat,
  char ***pazOut,
  int *pnOut
){
  struct TableEntry *aAnc = 0, *aCur = 0;
  int nAnc = 0, nCur = 0;
  char **az = 0;
  int n = 0, nAlloc = 0;
  sqlite3_stmt *pTbls = 0;
  int rc, stepRc;

  *pazOut = 0;
  *pnOut = 0;

  if( pAncCat && !prollyHashIsEmpty(pAncCat) ){
    rc = doltliteLoadCatalog(db, pAncCat, &aAnc, &nAnc, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  {
    ProllyHash curHash;
    rc = doltliteFlushCatalogToHash(db, &curHash);
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aAnc, nAnc);
      return rc;
    }
    rc = doltliteLoadCatalog(db, &curHash, &aCur, &nCur, 0);
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aAnc, nAnc);
      return rc;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM main.sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aAnc, nAnc);
    doltliteFreeCatalog(aCur, nCur);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pTbls, 0);
    struct TableEntry *pA, *pC;
    if( !zName ) continue;
    pA = doltliteFindTableByName(aAnc, nAnc, zName);
    pC = doltliteFindTableByName(aCur, nCur, zName);
    if( pA && pC
     && prollyHashCompare(&pA->root, &pC->root)==0
     && prollyHashCompare(&pA->schemaHash, &pC->schemaHash)==0 ){
      continue;
    }
    if( !pA && !pC ) continue;
    if( n >= nAlloc ){
      int nNew = nAlloc ? nAlloc*2 : 8;
      char **azNew = sqlite3_realloc(az, nNew * (int)sizeof(char*));
      if( !azNew ){ rc = SQLITE_NOMEM; break; }
      az = azNew;
      nAlloc = nNew;
    }
    az[n] = sqlite3_mprintf("%s", zName);
    if( !az[n] ){ rc = SQLITE_NOMEM; break; }
    n++;
  }
  if( rc==SQLITE_OK && stepRc!=SQLITE_DONE && stepRc!=SQLITE_ROW ){
    rc = stepRc;
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pTbls);
  doltliteFreeCatalog(aAnc, nAnc);
  doltliteFreeCatalog(aCur, nCur);
  if( rc!=SQLITE_OK ){
    int i;
    for(i=0; i<n; i++) sqlite3_free(az[i]);
    sqlite3_free(az);
    return rc;
  }
  *pazOut = az;
  *pnOut = n;
  return SQLITE_OK;
}

static void freeStringList(char **az, int n){
  int i;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

static int hasRecordedViolations(
  sqlite3 *db,
  const char **azTables,
  int nTables,
  int *pFound
){
  sqlite3_stmt *pStmt = 0;
  int rc;

  *pFound = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT \"table\" FROM dolt_constraint_violations "
      "WHERE num_violations>0", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zTable = (const char*)sqlite3_column_text(pStmt, 0);
    int i;
    if( nTables==0 ){
      *pFound = 1;
      break;
    }
    for(i=0; zTable && i<nTables; i++){
      if( sqlite3_stricmp(zTable, azTables[i])==0 ){
        *pFound = 1;
        break;
      }
    }
    if( *pFound ) break;
  }
  if( rc==SQLITE_ROW ) rc = SQLITE_OK;
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  {
    int finalizeRc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = finalizeRc;
  }
  return rc;
}

static void doltVerifyConstraintsFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  int bAll = 0;
  int bOutputOnly = 0;
  const char **azArgTables = 0;
  int nArgTables = 0;
  int nArgAlloc = 0;
  char **azChanged = 0;
  int nChanged = 0;
  const char **azScan = 0;
  int nScan = 0;
  int i;
  int rc;
  int nViolations = 0;
  ProllyHash headCat;
  ProllyHash emptyCat;
  DoltliteCommit headCommit;
  ProllyHash headHash;
  const ProllyHash *pDetectAnc = 0;

  memset(&headCat, 0, sizeof(headCat));
  memset(&emptyCat, 0, sizeof(emptyCat));
  memset(&headCommit, 0, sizeof(headCommit));
  memset(&headHash, 0, sizeof(headHash));

  if( doltliteCmdRejectDetached(context) ) return;
  for(i=0; i<argc; i++){
    const char *zArg = (const char*)sqlite3_value_text(argv[i]);
    int exists;
    if( !zArg || !zArg[0] ){
      sqlite3_result_error(context, "invalid empty argument", -1);
      goto cleanup;
    }
    if( strcmp(zArg, "--all")==0 || strcmp(zArg, "-a")==0 ){
      bAll = 1;
      continue;
    }
    if( strcmp(zArg, "--output-only")==0 ){
      bOutputOnly = 1;
      continue;
    }
    if( zArg[0]=='-' ){
      char *zErr = sqlite3_mprintf(
          "unknown flag '%s' (supported: --all, --output-only)", zArg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      goto cleanup;
    }
    exists = tableExists(db, zArg);
    if( exists<0 ){
      sqlite3_result_error_nomem(context);
      goto cleanup;
    }
    if( !exists ){
      char *zErr = sqlite3_mprintf("table not found: %s", zArg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      goto cleanup;
    }
    if( nArgTables >= nArgAlloc ){
      int nNew = nArgAlloc ? nArgAlloc*2 : 4;
      const char **azNew = sqlite3_realloc(
          (void*)azArgTables, nNew * (int)sizeof(const char*));
      if( !azNew ){
        sqlite3_result_error_nomem(context);
        goto cleanup;
      }
      azArgTables = azNew;
      nArgAlloc = nNew;
    }
    azArgTables[nArgTables++] = zArg;
  }

  /* Only the tables about to be re-checked lose their recorded findings.
  ** A scoped verify says nothing about the tables it does not scan, and the
  ** commit gate reads this catalog. */
  if( !bOutputOnly ){
    if( nArgTables>0 ){
      rc = doltliteClearConstraintViolationsForTables(
          db, (const char *const *)azArgTables, nArgTables);
    }else{
      rc = doltliteClearAllConstraintViolations(db);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto cleanup;
    }
  }

  doltliteGetSessionHead(db, &headHash);
  if( !prollyHashIsEmpty(&headHash) ){
    rc = doltliteLoadCommit(db, &headHash, &headCommit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto cleanup;
    }
    memcpy(&headCat, &headCommit.catalogHash, sizeof(headCat));
    doltliteCommitClear(&headCommit);
  }

  /*
  ** Detection always uses an empty ancestor so every violating row in a
  ** scanned table is reported (matching Dolt, which records both sides of a
  ** unique-index collision). The default mode instead restricts the *set* of
  ** tables to those that differ from HEAD.
  */
  pDetectAnc = &emptyCat;

  if( bAll ){
    azScan = azArgTables;
    nScan = nArgTables;
  }else{
    rc = collectChangedTables(db, &headCat, &azChanged, &nChanged);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto cleanup;
    }
    if( nArgTables>0 ){
      /* Intersection of named tables and changed tables. */
      char **azInter = 0;
      int nInter = 0, nInterAlloc = 0;
      int j;
      for(i=0; i<nArgTables; i++){
        int hit = 0;
        for(j=0; j<nChanged; j++){
          if( sqlite3_stricmp(azArgTables[i], azChanged[j])==0 ){
            hit = 1;
            break;
          }
        }
        if( !hit ) continue;
        if( nInter >= nInterAlloc ){
          int nNew = nInterAlloc ? nInterAlloc*2 : 4;
          char **azNew = sqlite3_realloc(azInter, nNew * (int)sizeof(char*));
          if( !azNew ){
            freeStringList(azInter, nInter);
            sqlite3_result_error_nomem(context);
            goto cleanup;
          }
          azInter = azNew;
          nInterAlloc = nNew;
        }
        azInter[nInter] = sqlite3_mprintf("%s", azArgTables[i]);
        if( !azInter[nInter] ){
          freeStringList(azInter, nInter);
          sqlite3_result_error_nomem(context);
          goto cleanup;
        }
        nInter++;
      }
      freeStringList(azChanged, nChanged);
      azChanged = azInter;
      nChanged = nInter;
    }
    if( nChanged==0 ){
      /* No changed tables: nothing to scan. */
      goto detection_done;
    }
    azScan = (const char**)azChanged;
    nScan = nChanged;
  }

  rc = doltliteDetectConstraintViolationsFiltered(
      db, pDetectAnc, azScan, nScan, !bOutputOnly, &nViolations);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    goto cleanup;
  }

detection_done:
  if( bOutputOnly && nViolations==0 ){
    int found = 0;
    rc = hasRecordedViolations(db, azArgTables, nArgTables, &found);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto cleanup;
    }
    nViolations = found;
  }

  if( !bOutputOnly ){
    rc = doltliteRefreshConstraintViolationTables(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      goto cleanup;
    }
  }

  sqlite3_result_int(context, nViolations>0 ? 1 : 0);

cleanup:
  freeStringList(azChanged, nChanged);
  sqlite3_free((void*)azArgTables);
}

int doltliteVerifyConstraintsRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_verify_constraints", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltVerifyConstraintsFunc, 0, 0);
}

#endif
