
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include "doltlite_ignore.h"
#include <string.h>

typedef struct IgnoreMatch IgnoreMatch;
struct IgnoreMatch {
  char *zPattern;
  u8 ignored;
  u8 removed;
};

static int ignorePatternMatchMode(
  const char *zPat,
  const char *zStr,
  int patternMode
){
  const char *pStar = 0;
  const char *sStar = 0;

  while( *zStr ){
    unsigned char c = (unsigned char)*zPat;
    if( c=='*' || c=='%' ){
      while( *(zPat+1)=='*' || *(zPat+1)=='%' ) zPat++;
      pStar = zPat;
      sStar = zStr;
      zPat++;
    }else if( c=='?' && (!patternMode || (*zStr!='*' && *zStr!='%')) ){
      zPat++;
      zStr++;
    }else if( c==(unsigned char)*zStr ){
      zPat++;
      zStr++;
    }else if( pStar ){
      zPat = pStar + 1;
      sStar++;
      zStr = sStar;
    }else{
      return 0;
    }
  }
  while( *zPat=='*' || *zPat=='%' ) zPat++;
  return *zPat == 0;
}

static int ignorePatternMatch(const char *zPat, const char *zStr){
  return ignorePatternMatchMode(zPat, zStr, 0);
}

static int ignorePatternCovers(const char *zLess, const char *zMore){
  return ignorePatternMatchMode(zLess, zMore, 1);
}

static unsigned char ignorePatternNext(const char **pzPattern){
  unsigned char c = (unsigned char)**pzPattern;
  if( !c ) return 0;
  (*pzPattern)++;
  if( c=='*' || c=='%' ){
    while( **pzPattern=='*' || **pzPattern=='%' ) (*pzPattern)++;
    return '%';
  }
  return c;
}

static int ignorePatternsEquivalent(const char *zA, const char *zB){
  unsigned char a, b;
  do{
    a = ignorePatternNext(&zA);
    b = ignorePatternNext(&zB);
    if( a!=b ) return 0;
  }while( a );
  return 1;
}

static void ignoreMatchesClear(IgnoreMatch *aMatch, int nMatch){
  int i;
  for(i=0; i<nMatch; i++) sqlite3_free(aMatch[i].zPattern);
  sqlite3_free(aMatch);
}

static int ignoreMatchAppend(
  IgnoreMatch **paMatch,
  int *pnMatch,
  int *pnAlloc,
  const char *zPattern,
  int ignored
){
  IgnoreMatch *p;
  int rc;
  rc = DOLTLITE_GROW_ARRAY(paMatch, pnAlloc, *pnMatch+1, 8);
  if( rc!=SQLITE_OK ) return rc;
  p = &(*paMatch)[*pnMatch];
  memset(p, 0, sizeof(*p));
  p->zPattern = sqlite3_mprintf("%s", zPattern);
  if( !p->zPattern ) return SQLITE_NOMEM;
  p->ignored = ignored!=0;
  (*pnMatch)++;
  return SQLITE_OK;
}

static int ignoreConflict(
  const char *zTable,
  const char *zIgnored,
  const char *zKept,
  char **pzErr
){
  if( pzErr ){
    *pzErr = sqlite3_mprintf(
        "the table %s matches conflicting patterns in dolt_ignore:\n"
        "ignored:     %s\nnot ignored: %s",
        zTable, zIgnored, zKept);
    if( !*pzErr ) return SQLITE_NOMEM;
  }
  return SQLITE_CONSTRAINT;
}

static int ignoreResolveMatches(
  IgnoreMatch *aMatch,
  int nMatch,
  const char *zTable,
  int *pIgnored,
  char **pzErr
){
  int i, j;
  int nIgnored = 0;
  int nKept = 0;
  int nIgnoredRemoved = 0;
  int nKeptRemoved = 0;
  const char *zIgnored = 0;
  const char *zKept = 0;

  for(i=0; i<nMatch; i++){
    if( aMatch[i].ignored ) nIgnored++;
    else nKept++;
  }
  if( nIgnored==0 ) return SQLITE_OK;
  if( nKept==0 ){
    *pIgnored = 1;
    return SQLITE_OK;
  }

  for(i=0; i<nMatch; i++){
    if( !aMatch[i].ignored ) continue;
    for(j=0; j<nMatch; j++){
      if( aMatch[j].ignored ) continue;
      if( ignorePatternsEquivalent(aMatch[i].zPattern, aMatch[j].zPattern) ){
        return ignoreConflict(zTable, aMatch[i].zPattern,
                              aMatch[j].zPattern, pzErr);
      }
      if( ignorePatternCovers(aMatch[i].zPattern, aMatch[j].zPattern) ){
        aMatch[i].removed = 1;
      }
      if( ignorePatternCovers(aMatch[j].zPattern, aMatch[i].zPattern) ){
        aMatch[j].removed = 1;
      }
    }
  }

  for(i=0; i<nMatch; i++){
    if( aMatch[i].ignored ){
      if( aMatch[i].removed ) nIgnoredRemoved++;
      else if( !zIgnored ) zIgnored = aMatch[i].zPattern;
    }else{
      if( aMatch[i].removed ) nKeptRemoved++;
      else if( !zKept ) zKept = aMatch[i].zPattern;
    }
  }
  if( nIgnoredRemoved==nIgnored ) return SQLITE_OK;
  if( nKeptRemoved==nKept ){
    *pIgnored = 1;
    return SQLITE_OK;
  }
  return ignoreConflict(zTable, zIgnored, zKept, pzErr);
}

enum DoltliteIgnoreSchemaState {
  DOLTLITE_IGNORE_SCHEMA_ABSENT = 0,
  DOLTLITE_IGNORE_SCHEMA_OK = 1,
  DOLTLITE_IGNORE_SCHEMA_BAD = 2
};

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
  IgnoreMatch *aMatch = 0;
  int rc;
  int rc2;
  int schemaState;
  int nMatch = 0;
  int nAlloc = 0;

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
    if( !zPat ) continue;
    if( !ignorePatternMatch(zPat, zTable) ) continue;
    rc = ignoreMatchAppend(&aMatch, &nMatch, &nAlloc, zPat, ign);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  rc2 = sqlite3_finalize(pStmt);
  if( rc==SQLITE_OK && rc2!=SQLITE_OK ) rc = rc2;
  if( rc!=SQLITE_OK ){
    ignoreMatchesClear(aMatch, nMatch);
    return rc;
  }
  rc = ignoreResolveMatches(aMatch, nMatch, zTable, pIgnored, pzErr);
  ignoreMatchesClear(aMatch, nMatch);
  return rc;
}

typedef struct IgnoreVtab IgnoreVtab;
struct IgnoreVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct IgnoreCursor IgnoreCursor;
struct IgnoreCursor {
  sqlite3_vtab_cursor base;
};

static const char *zIgnoreVtabSchema =
  "CREATE TABLE x(pattern TEXT NOT NULL PRIMARY KEY, "
  "ignored TINYINT NOT NULL) WITHOUT ROWID";

static const char *zIgnoreCreate =
  "CREATE TABLE main.dolt_ignore("
  "pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern))";

static int ignoreSetErr(IgnoreVtab *p, int rc){
  sqlite3_free(p->base.zErrMsg);
  p->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
  return rc;
}

static int ignoreConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  IgnoreVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zIgnoreVtabSchema,
      sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (IgnoreVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int ignoreBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10.0;
  pInfo->estimatedRows = 0;
  return SQLITE_OK;
}

static int ignoreOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(IgnoreCursor));
}

static int ignoreFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  (void)pCursor; (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  return SQLITE_OK;
}

static int ignoreNext(sqlite3_vtab_cursor *pCursor){
  (void)pCursor;
  return SQLITE_OK;
}

static int ignoreEof(sqlite3_vtab_cursor *pCursor){
  (void)pCursor;
  return 1;
}

static int ignoreColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  (void)pCursor; (void)ctx; (void)iCol;
  return SQLITE_OK;
}

static int ignoreRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  (void)pCursor;
  *pRowid = 0;
  return SQLITE_OK;
}

static int ignoreMaterialize(IgnoreVtab *p){
  char *zErr = 0;
  int rc;
  if( sqlite3FindTable(p->db, "dolt_ignore", "main") ) return SQLITE_OK;
  rc = sqlite3_exec(p->db, zIgnoreCreate, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(p->base.zErrMsg);
    p->base.zErrMsg = sqlite3_mprintf("%s",
        zErr ? zErr : sqlite3_errstr(rc));
    sqlite3_free(zErr);
  }
  return rc;
}

static int ignoreBegin(sqlite3_vtab *pBase){
  return ignoreMaterialize((IgnoreVtab*)pBase);
}

static int ignoreExecBound(IgnoreVtab *p, const char *zSql,
                           sqlite3_value *pArg1, sqlite3_value *pArg2,
                           sqlite3_value *pArg3){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v3(p->db, zSql, -1,
                              SQLITE_PREPARE_NO_VTAB, &pStmt, 0);
  if( rc!=SQLITE_OK ) return ignoreSetErr(p, rc);
  if( pArg1 ) sqlite3_bind_value(pStmt, 1, pArg1);
  if( pArg2 ) sqlite3_bind_value(pStmt, 2, pArg2);
  if( pArg3 ) sqlite3_bind_value(pStmt, 3, pArg3);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return ignoreSetErr(p, rc);
  return SQLITE_OK;
}

static const char *ignoreInsertSql(sqlite3 *db){
  switch( sqlite3_vtab_on_conflict(db) ){
    case SQLITE_REPLACE:
      return "INSERT OR REPLACE INTO main.dolt_ignore(pattern, ignored) "
             "VALUES(?1, ?2)";
    case SQLITE_IGNORE:
      return "INSERT OR IGNORE INTO main.dolt_ignore(pattern, ignored) "
             "VALUES(?1, ?2)";
    case SQLITE_FAIL:
      return "INSERT OR FAIL INTO main.dolt_ignore(pattern, ignored) "
             "VALUES(?1, ?2)";
    case SQLITE_ROLLBACK:
      return "INSERT OR ROLLBACK INTO main.dolt_ignore(pattern, ignored) "
             "VALUES(?1, ?2)";
    default:
      return "INSERT INTO main.dolt_ignore(pattern, ignored) VALUES(?1, ?2)";
  }
}

static const char *ignoreUpdateSql(sqlite3 *db){
  switch( sqlite3_vtab_on_conflict(db) ){
    case SQLITE_REPLACE:
      return "UPDATE OR REPLACE main.dolt_ignore SET pattern=?1, ignored=?2 "
             "WHERE pattern=?3";
    case SQLITE_IGNORE:
      return "UPDATE OR IGNORE main.dolt_ignore SET pattern=?1, ignored=?2 "
             "WHERE pattern=?3";
    case SQLITE_FAIL:
      return "UPDATE OR FAIL main.dolt_ignore SET pattern=?1, ignored=?2 "
             "WHERE pattern=?3";
    case SQLITE_ROLLBACK:
      return "UPDATE OR ROLLBACK main.dolt_ignore SET pattern=?1, ignored=?2 "
             "WHERE pattern=?3";
    default:
      return "UPDATE main.dolt_ignore SET pattern=?1, ignored=?2 "
             "WHERE pattern=?3";
  }
}

static int ignoreUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                        sqlite3_int64 *pRowid){
  IgnoreVtab *p = (IgnoreVtab*)pBase;
  int rc;

  rc = ignoreMaterialize(p);
  if( rc!=SQLITE_OK ) return rc;

  if( argc==1 ){
    return ignoreExecBound(p,
        "DELETE FROM main.dolt_ignore WHERE pattern=?1", argv[0], 0, 0);
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    return ignoreExecBound(p,
        ignoreUpdateSql(p->db), argv[2], argv[3], argv[0]);
  }

  rc = ignoreExecBound(p, ignoreInsertSql(p->db), argv[2], argv[3], 0);
  if( rc!=SQLITE_OK ) return rc;
  if( pRowid ) *pRowid = sqlite3_last_insert_rowid(p->db);
  return SQLITE_OK;
}

static sqlite3_module doltliteIgnoreModule = {
  0, 0, ignoreConnect, ignoreBestIndex, doltliteVtabDisconnect, 0,
  ignoreOpen, doltliteVtabClose, ignoreFilter, ignoreNext, ignoreEof,
  ignoreColumn, ignoreRowid,
  ignoreUpdate, ignoreBegin, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteIgnoreRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_ignore", &doltliteIgnoreModule, 0);
}

#endif
