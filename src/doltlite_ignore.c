
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_ignore.h"
#include <string.h>

/* Match zStr against zPat. '*' and '%' match zero or more characters,
** '?' matches exactly one, everything else is literal. Returns 1 on
** match, 0 otherwise. Recursive on wildcard to keep the code short;
** table names are bounded by sqlite_master so pathological nesting
** isn't a concern. */
static int ignorePatternMatch(const char *zPat, const char *zStr){
  while( *zPat ){
    char c = *zPat;
    if( c=='*' || c=='%' ){
      while( *(zPat+1)=='*' || *(zPat+1)=='%' ) zPat++;
      zPat++;
      if( !*zPat ) return 1;
      while( *zStr ){
        if( ignorePatternMatch(zPat, zStr) ) return 1;
        zStr++;
      }
      return 0;
    }else if( c=='?' ){
      if( !*zStr ) return 0;
      zPat++;
      zStr++;
    }else{
      if( *zStr != c ) return 0;
      zPat++;
      zStr++;
    }
  }
  return *zStr == 0;
}

/* Specificity score = count of literal (non-wildcard) chars. Higher
** wins. Exact-literal patterns beat any wildcard pattern that also
** matches the same string because they necessarily have more literals
** (the entire name vs some proper prefix/suffix). */
static int ignoreSpecificity(const char *zPat){
  int n = 0;
  while( *zPat ){
    if( *zPat != '*' && *zPat != '%' && *zPat != '?' ) n++;
    zPat++;
  }
  return n;
}

enum DoltliteIgnoreSchemaState {
  DOLTLITE_IGNORE_SCHEMA_ABSENT = 0,
  DOLTLITE_IGNORE_SCHEMA_OK = 1,
  DOLTLITE_IGNORE_SCHEMA_BAD = 2
};

/* Read-time schema guard. The parse-time check in build.c catches
** new bad CREATE TABLE statements, but on-disk repos created before
** the guard landed (or via an older binary) can still have a
** wrong-shape dolt_ignore. Detect it here and raise an error so the
** user gets a clear signal instead of a silent "no filtering". */
static int doltliteIgnoreSchemaState(sqlite3 *db, int *pState){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int nCol;
  *pState = DOLTLITE_IGNORE_SCHEMA_ABSENT;
  rc = sqlite3_prepare_v2(db, "PRAGMA main.table_info(\"dolt_ignore\")",
                          -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  nCol = 0;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
    char aff;
    int notNull = sqlite3_column_int(pStmt, 3);
    int pkPos = sqlite3_column_int(pStmt, 5);
    if( !zName ) goto bad;
    aff = sqlite3AffinityType(zType ? zType : "", 0);
    if( nCol==0 ){
      if( sqlite3_stricmp(zName, "pattern")!=0 ) goto bad;
      if( aff!=SQLITE_AFF_TEXT && aff!=SQLITE_AFF_BLOB ) goto bad;
      if( !notNull ) goto bad;
      if( pkPos!=1 ) goto bad;
    }else if( nCol==1 ){
      if( sqlite3_stricmp(zName, "ignored")!=0 ) goto bad;
      if( aff!=SQLITE_AFF_INTEGER && aff!=SQLITE_AFF_NUMERIC ) goto bad;
      if( !notNull ) goto bad;
      if( pkPos!=0 ) goto bad;
    }else{
      goto bad;
    }
    nCol++;
  }
  if( rc!=SQLITE_DONE ){
    sqlite3_finalize(pStmt);
    return rc;
  }
  if( nCol==0 ){
    *pState = DOLTLITE_IGNORE_SCHEMA_ABSENT;
  }else if( nCol==2 ){
    *pState = DOLTLITE_IGNORE_SCHEMA_OK;
  }else{
    *pState = DOLTLITE_IGNORE_SCHEMA_BAD;
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;

bad:
  *pState = DOLTLITE_IGNORE_SCHEMA_BAD;
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

int doltliteCheckIgnore(
  sqlite3 *db,
  const char *zTable,
  int *pIgnored,
  char **pzErr
){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int schemaState;
  int bestSpec = -1;
  int bestIgnored = 0;
  char *zBestPat = 0;
  int tieDisagrees = 0;
  char *zTiePat = 0;

  *pIgnored = 0;
  if( pzErr ) *pzErr = 0;

  rc = doltliteIgnoreSchemaState(db, &schemaState);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  if( schemaState==DOLTLITE_IGNORE_SCHEMA_ABSENT ){
    return SQLITE_OK;
  }
  if( schemaState!=DOLTLITE_IGNORE_SCHEMA_OK ){
    if( pzErr ){
      *pzErr = sqlite3_mprintf(
          "dolt_ignore has an unexpected schema; expected: "
          "CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, "
          "ignored TINYINT NOT NULL, PRIMARY KEY(pattern))");
    }
    return SQLITE_CONSTRAINT;
  }
  rc = sqlite3_prepare_v2(db,
      "SELECT pattern, ignored FROM main.dolt_ignore", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zPat = (const char*)sqlite3_column_text(pStmt, 0);
    int ign = sqlite3_column_int(pStmt, 1);
    int spec;
    if( !zPat ) continue;
    if( !ignorePatternMatch(zPat, zTable) ) continue;
    spec = ignoreSpecificity(zPat);
    if( spec > bestSpec ){
      bestSpec = spec;
      bestIgnored = ign;
      sqlite3_free(zBestPat);
      zBestPat = sqlite3_mprintf("%s", zPat);
      tieDisagrees = 0;
      sqlite3_free(zTiePat);
      zTiePat = 0;
    }else if( spec==bestSpec && ign!=bestIgnored ){
      tieDisagrees = 1;
      sqlite3_free(zTiePat);
      zTiePat = sqlite3_mprintf("%s", zPat);
    }
  }
  sqlite3_finalize(pStmt);

  if( rc!=SQLITE_DONE && rc!=SQLITE_ROW && rc!=SQLITE_OK ){
    sqlite3_free(zBestPat);
    sqlite3_free(zTiePat);
    return rc;
  }

  if( tieDisagrees ){
    if( pzErr ){
      const char *zIgn = bestIgnored ? zBestPat : zTiePat;
      const char *zKeep = bestIgnored ? zTiePat : zBestPat;
      *pzErr = sqlite3_mprintf(
          "the table %s matches conflicting patterns in dolt_ignore:\n"
          "ignored:     %s\nnot ignored: %s",
          zTable, zIgn, zKeep);
    }
    sqlite3_free(zBestPat);
    sqlite3_free(zTiePat);
    return SQLITE_CONSTRAINT;
  }

  if( bestSpec >= 0 ){
    *pIgnored = bestIgnored;
  }

  sqlite3_free(zBestPat);
  sqlite3_free(zTiePat);
  return SQLITE_OK;
}

#endif
