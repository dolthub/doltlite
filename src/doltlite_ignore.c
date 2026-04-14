
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

int doltliteEnsureIgnoreTable(sqlite3 *db){
  return sqlite3_exec(db,
      "CREATE TABLE IF NOT EXISTS dolt_ignore("
      "pattern TEXT NOT NULL,"
      "ignored TINYINT NOT NULL,"
      "PRIMARY KEY(pattern))",
      0, 0, 0);
}

int doltliteCheckIgnore(
  sqlite3 *db,
  const char *zTable,
  int *pIgnored,
  char **pzErr
){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int bestSpec = -1;
  int bestIgnored = 0;
  char *zBestPat = 0;
  int tieDisagrees = 0;
  char *zTiePat = 0;
  int tieIgnored = 0;

  *pIgnored = 0;
  if( pzErr ) *pzErr = 0;

  rc = sqlite3_prepare_v2(db,
      "SELECT pattern, ignored FROM dolt_ignore", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    /* Table missing — e.g. legacy repo or pre-seed call. No patterns
    ** means no filtering. */
    return SQLITE_OK;
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
      tieIgnored = ign;
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
    (void)tieIgnored;
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
