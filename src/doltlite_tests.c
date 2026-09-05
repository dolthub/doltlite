#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include <errno.h> /* amalgamator: keep */
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct TestsVtab TestsVtab;
struct TestsVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct TestsCursor TestsCursor;
struct TestsCursor {
  sqlite3_vtab_cursor base;
};

static const char *zTestsVtabSchema =
  "CREATE TABLE x("
  "test_name TEXT NOT NULL PRIMARY KEY,"
  "test_group TEXT,"
  "test_query TEXT NOT NULL,"
  "assertion_type TEXT NOT NULL "
    "CHECK(assertion_type IN "
      "('expected_rows','expected_columns','expected_single_value')),"
  "assertion_comparator TEXT NOT NULL "
    "CHECK(assertion_comparator IN ('==','!=','<','>','<=','>=')),"
  "assertion_value TEXT)";

static const char *zTestsCreate =
  "CREATE TABLE main.dolt_tests("
  "test_name TEXT NOT NULL,"
  "test_group TEXT,"
  "test_query TEXT NOT NULL,"
  "assertion_type TEXT NOT NULL,"
  "assertion_comparator TEXT NOT NULL,"
  "assertion_value TEXT,"
  "PRIMARY KEY(test_name),"
  "CONSTRAINT assertion_type_check CHECK(assertion_type IN "
    "('expected_rows','expected_columns','expected_single_value')),"
  "CONSTRAINT assertion_comparator_check CHECK(assertion_comparator IN "
    "('==','!=','<','>','<=','>=')))";

static int testsSetErr(TestsVtab *p, int rc){
  sqlite3_free(p->base.zErrMsg);
  p->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
  return rc;
}

static int testsConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  TestsVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zTestsVtabSchema,
      sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (TestsVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int testsBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10.0;
  pInfo->estimatedRows = 0;
  return SQLITE_OK;
}

static int testsOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(TestsCursor));
}

static int testsFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  (void)pCursor; (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  return SQLITE_OK;
}

static int testsNext(sqlite3_vtab_cursor *pCursor){
  (void)pCursor;
  return SQLITE_OK;
}

static int testsEof(sqlite3_vtab_cursor *pCursor){
  (void)pCursor;
  return 1;
}

static int testsColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  (void)pCursor; (void)ctx; (void)iCol;
  return SQLITE_OK;
}

static int testsRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  (void)pCursor;
  *pRowid = 0;
  return SQLITE_OK;
}

static int testsMaterialize(TestsVtab *p){
  char *zErr = 0;
  int rc;
  if( sqlite3FindTable(p->db, "dolt_tests", "main") ) return SQLITE_OK;
  rc = sqlite3_exec(p->db, zTestsCreate, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(p->base.zErrMsg);
    p->base.zErrMsg = sqlite3_mprintf("%s",
        zErr ? zErr : sqlite3_errstr(rc));
    sqlite3_free(zErr);
  }
  return rc;
}

static int testsBegin(sqlite3_vtab *pBase){
  return testsMaterialize((TestsVtab*)pBase);
}

static int testsUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                       sqlite3_int64 *pRowid){
  TestsVtab *p = (TestsVtab*)pBase;
  sqlite3_stmt *pStmt = 0;
  const char *zSql;
  int i;
  int rc = testsMaterialize(p);
  if( rc!=SQLITE_OK ) return rc;
  if( argc==1 ) return SQLITE_OK;
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ) return SQLITE_OK;
  switch( sqlite3_vtab_on_conflict(p->db) ){
    case SQLITE_REPLACE:
      zSql = "INSERT OR REPLACE INTO main.dolt_tests VALUES(?,?,?,?,?,?)";
      break;
    case SQLITE_IGNORE:
      zSql = "INSERT OR IGNORE INTO main.dolt_tests VALUES(?,?,?,?,?,?)";
      break;
    default:
      zSql = "INSERT INTO main.dolt_tests VALUES(?,?,?,?,?,?)";
      break;
  }
  rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return testsSetErr(p, rc);
  for(i=0; i<6; i++) sqlite3_bind_value(pStmt, i+1, argv[i+2]);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return testsSetErr(p, rc);
  if( pRowid ) *pRowid = sqlite3_last_insert_rowid(p->db);
  return SQLITE_OK;
}

static sqlite3_module doltliteTestsModule = {
  0, 0, testsConnect, testsBestIndex, doltliteVtabDisconnect, 0,
  testsOpen, doltliteVtabClose, testsFilter, testsNext, testsEof,
  testsColumn, testsRowid,
  testsUpdate, testsBegin, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

typedef struct TestDef TestDef;
struct TestDef {
  char *zName;
  char *zGroup;
  char *zQuery;
  char *zAssertion;
  char *zComparator;
  char *zValue;
};

typedef struct TestResult TestResult;
struct TestResult {
  char *zName;
  char *zGroup;
  char *zQuery;
  char *zStatus;
  char *zMessage;
};

typedef struct TestRunVtab TestRunVtab;
struct TestRunVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct TestRunCursor TestRunCursor;
struct TestRunCursor {
  sqlite3_vtab_cursor base;
  TestResult *aResult;
  int nResult;
  int iRow;
};

#define TEST_RUN_FIRST_ARG 5
#define TEST_RUN_MAX_ARGS 127

static void testDefClear(TestDef *p){
  sqlite3_free(p->zName);
  sqlite3_free(p->zGroup);
  sqlite3_free(p->zQuery);
  sqlite3_free(p->zAssertion);
  sqlite3_free(p->zComparator);
  sqlite3_free(p->zValue);
  memset(p, 0, sizeof(*p));
}

static void testResultClear(TestResult *p){
  sqlite3_free(p->zName);
  sqlite3_free(p->zGroup);
  sqlite3_free(p->zQuery);
  sqlite3_free(p->zStatus);
  sqlite3_free(p->zMessage);
  memset(p, 0, sizeof(*p));
}

static void testRunReset(TestRunCursor *p){
  int i;
  for(i=0; i<p->nResult; i++) testResultClear(&p->aResult[i]);
  sqlite3_free(p->aResult);
  p->aResult = 0;
  p->nResult = 0;
  p->iRow = 0;
}

static char *testColumnDup(sqlite3_stmt *pStmt, int iCol){
  const unsigned char *z;
  if( sqlite3_column_type(pStmt, iCol)==SQLITE_NULL ) return 0;
  z = sqlite3_column_text(pStmt, iCol);
  return z ? sqlite3_mprintf("%s", z) : 0;
}

static int testDefAppend(TestDef **paDef, int *pnDef, sqlite3_stmt *pStmt){
  TestDef *aNew;
  TestDef *p;
  int i;
  aNew = sqlite3_realloc64(*paDef,
      (sqlite3_uint64)(*pnDef + 1) * sizeof(**paDef));
  if( !aNew ) return SQLITE_NOMEM;
  *paDef = aNew;
  p = &aNew[*pnDef];
  memset(p, 0, sizeof(*p));
  p->zName = testColumnDup(pStmt, 0);
  p->zGroup = testColumnDup(pStmt, 1);
  p->zQuery = testColumnDup(pStmt, 2);
  p->zAssertion = testColumnDup(pStmt, 3);
  p->zComparator = testColumnDup(pStmt, 4);
  p->zValue = testColumnDup(pStmt, 5);
  if( !p->zName || !p->zQuery || !p->zAssertion || !p->zComparator ){
    testDefClear(p);
    return SQLITE_NOMEM;
  }
  for(i=0; i<6; i++){
    if( sqlite3_column_type(pStmt, i)!=SQLITE_NULL ){
      char *z = i==0 ? p->zName : i==1 ? p->zGroup : i==2 ? p->zQuery
          : i==3 ? p->zAssertion : i==4 ? p->zComparator : p->zValue;
      if( !z ){
        testDefClear(p);
        return SQLITE_NOMEM;
      }
    }
  }
  (*pnDef)++;
  return SQLITE_OK;
}

static int testLoadQuery(sqlite3 *db, const char *zSql, const char *zArg,
    TestDef **paDef, int *pnDef, int *pnAdded){
  sqlite3_stmt *pStmt = 0;
  int nBefore = *pnDef;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  if( zArg ) sqlite3_bind_text(pStmt, 1, zArg, -1, SQLITE_TRANSIENT);
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    rc = testDefAppend(paDef, pnDef, pStmt);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( sqlite3_finalize(pStmt)!=SQLITE_OK && rc==SQLITE_OK ){
    rc = sqlite3_errcode(db);
  }
  *pnAdded = *pnDef - nBefore;
  return rc;
}

static int testLoadArgument(sqlite3 *db, const char *zArg,
    TestDef **paDef, int *pnDef){
  int nAdded = 0;
  int rc;
  if( strcmp(zArg, "*")==0 ){
    return testLoadQuery(db,
        "SELECT * FROM main.dolt_tests ORDER BY test_name",
        0, paDef, pnDef, &nAdded);
  }
  rc = testLoadQuery(db,
      "SELECT * FROM main.dolt_tests WHERE test_name=?1 ORDER BY test_name",
      zArg, paDef, pnDef, &nAdded);
  if( rc!=SQLITE_OK || nAdded ) return rc;
  return testLoadQuery(db,
      "SELECT * FROM main.dolt_tests WHERE test_group=?1 ORDER BY test_name",
      zArg, paDef, pnDef, &nAdded);
}

static const char *testComparePhrase(const char *zComparator){
  if( strcmp(zComparator, "==")==0 ) return "equal to";
  if( strcmp(zComparator, "!=")==0 ) return "not equal to";
  if( strcmp(zComparator, "<")==0 ) return "less than";
  if( strcmp(zComparator, "<=")==0 ) return "less than or equal to";
  if( strcmp(zComparator, ">")==0 ) return "greater than";
  if( strcmp(zComparator, ">=")==0 ) return "greater than or equal to";
  return 0;
}

static int testComparisonPassed(const char *zComparator, int c){
  if( strcmp(zComparator, "==")==0 ) return c==0;
  if( strcmp(zComparator, "!=")==0 ) return c!=0;
  if( strcmp(zComparator, "<")==0 ) return c<0;
  if( strcmp(zComparator, "<=")==0 ) return c<=0;
  if( strcmp(zComparator, ">")==0 ) return c>0;
  if( strcmp(zComparator, ">=")==0 ) return c>=0;
  return 0;
}

static char *testCompareMessage(const char *zAssertion,
    const char *zComparator, const char *zExpected,
    const char *zActual, int c){
  const char *zPhrase = testComparePhrase(zComparator);
  if( !zPhrase ){
    return sqlite3_mprintf("%s is not a valid comparison type", zComparator);
  }
  if( testComparisonPassed(zComparator, c) ) return sqlite3_mprintf("");
  return sqlite3_mprintf("Assertion failed: %s %s %s, got %s",
      zAssertion, zPhrase, zExpected, zActual);
}

static int testParseInteger(const char *z, sqlite3_int64 *pValue){
  char *zEnd = 0;
  long long v;
  if( !z || !z[0] ) return 0;
  errno = 0;
  v = strtoll(z, &zEnd, 10);
  if( errno==ERANGE || zEnd==z || *zEnd!=0 ) return 0;
  *pValue = (sqlite3_int64)v;
  return 1;
}

static int testParseReal(const char *z, double *pValue){
  char *zEnd = 0;
  double v;
  if( !z || !z[0] ) return 0;
  errno = 0;
  v = strtod(z, &zEnd);
  if( errno==ERANGE || zEnd==z || *zEnd!=0 ) return 0;
  *pValue = v;
  return 1;
}

static int testParseBool(const char *z, int *pValue){
  if( sqlite3StrICmp(z, "true")==0 || sqlite3StrICmp(z, "t")==0 ){
    *pValue = 1;
    return 1;
  }
  if( sqlite3StrICmp(z, "false")==0 || sqlite3StrICmp(z, "f")==0 ){
    *pValue = 0;
    return 1;
  }
  return 0;
}

static int testDecimalScale(sqlite3_stmt *pStmt){
#ifdef SQLITE_OMIT_DECLTYPE
  UNUSED_PARAMETER(pStmt);
  return -1;
#else
  const char *zType = sqlite3_column_decltype(pStmt, 0);
  const char *zComma;
  char *zEnd = 0;
  long scale;
  if( !zType || sqlite3_strnicmp(zType, "decimal(", 8)!=0 ) return -1;
  zComma = strchr(zType+8, ',');
  if( !zComma ) return -1;
  scale = strtol(zComma+1, &zEnd, 10);
  if( zEnd==zComma+1 || scale<0 || scale>30 ) return -1;
  while( sqlite3Isspace(*zEnd) ) zEnd++;
  return *zEnd==')' ? (int)scale : -1;
#endif
}

static char *testCompareNull(const char *zComparator,
    sqlite3_stmt *pStmt, int iCol){
  int isNull = sqlite3_column_type(pStmt, iCol)==SQLITE_NULL;
  if( strcmp(zComparator, "==")==0 ){
    if( isNull ) return sqlite3_mprintf("");
    return sqlite3_mprintf(
        "Assertion failed: expected_single_value equal to NULL, got %s",
        sqlite3_column_text(pStmt, iCol));
  }
  if( strcmp(zComparator, "!=")==0 ){
    if( !isNull ) return sqlite3_mprintf("");
    return sqlite3_mprintf(
        "Assertion failed: expected_single_value not equal to NULL, got NULL");
  }
  return sqlite3_mprintf(
      "%s is not a valid comparison for NULL values", zComparator);
}

static char *testCompareSingle(sqlite3_stmt *pStmt,
    const char *zComparator, const char *zExpected){
  int eType = sqlite3_column_type(pStmt, 0);
  char *zActual;
  char *zMessage;
  int expectedBool;
  if( !zExpected ) return testCompareNull(zComparator, pStmt, 0);
  if( strcmp(zExpected, "0")!=0 && strcmp(zExpected, "1")!=0
   && testParseBool(zExpected, &expectedBool) ){
    int actualBool;
    if( eType==SQLITE_INTEGER ){
      actualBool = sqlite3_column_int64(pStmt, 0)==1;
    }else if( eType==SQLITE_TEXT ){
      actualBool = strcmp((const char*)sqlite3_column_text(pStmt, 0), "1")==0;
    }else{
      return sqlite3_mprintf("Could not convert value to boolean: "
          "unexpected SQLite type %d", eType);
    }
    if( strcmp(zComparator, "==")!=0 && strcmp(zComparator, "!=")!=0 ){
      return sqlite3_mprintf("%s is not a valid comparison for boolean "
          "values. Only '==' and '!=' are supported", zComparator);
    }
    if( (strcmp(zComparator, "==")==0 && expectedBool==actualBool)
     || (strcmp(zComparator, "!=")==0 && expectedBool!=actualBool) ){
      return sqlite3_mprintf("");
    }
    return sqlite3_mprintf("Assertion failed: expected_single_value %s %s, "
        "got %s", strcmp(zComparator, "==")==0 ? "equal to" : "not equal to",
        expectedBool ? "true" : "false", actualBool ? "true" : "false");
  }
  if( eType==SQLITE_NULL ){
    return sqlite3_mprintf("Type <nil> is not supported. Open an issue at "
        "https://github.com/dolthub/dolt/issues to see it added");
  }
  if( eType==SQLITE_INTEGER ){
    sqlite3_int64 expected;
    sqlite3_int64 actual = sqlite3_column_int64(pStmt, 0);
    if( !testParseInteger(zExpected, &expected) ){
      return sqlite3_mprintf("Could not compare non integer value '%s', "
          "with %lld", zExpected, actual);
    }
    zActual = sqlite3_mprintf("%lld", actual);
    zMessage = testCompareMessage("expected_single_value", zComparator,
        zExpected, zActual, actual<expected ? -1 : actual>expected ? 1 : 0);
    sqlite3_free(zActual);
    return zMessage;
  }
  if( eType==SQLITE_FLOAT ){
    double expected;
    double actual = sqlite3_column_double(pStmt, 0);
    int scale;
    if( !testParseReal(zExpected, &expected) ){
      return sqlite3_mprintf("Could not compare non float value '%s', "
          "with %f", zExpected, actual);
    }
    scale = testDecimalScale(pStmt);
    zActual = scale>=0 ? sqlite3_mprintf("%.*f", scale, actual)
                       : sqlite3_mprintf("%g", actual);
    zMessage = testCompareMessage("expected_single_value", zComparator,
        zExpected, zActual, actual<expected ? -1 : actual>expected ? 1 : 0);
    sqlite3_free(zActual);
    return zMessage;
  }
  if( eType==SQLITE_TEXT ){
    const char *z = (const char*)sqlite3_column_text(pStmt, 0);
    int c = strcmp(z, zExpected);
    return testCompareMessage("expected_single_value", zComparator,
        zExpected, z, c<0 ? -1 : c>0 ? 1 : 0);
  }
  return sqlite3_mprintf("Type BLOB is not supported. Open an issue at "
      "https://github.com/dolthub/dolt/issues to see it added");
}

static char *testAssertRows(sqlite3_stmt *pStmt, const TestDef *pDef,
    int *pRc){
  sqlite3_int64 expected;
  sqlite3_int64 actual = 0;
  int rc;
  char *zActual;
  char *zMessage;
  if( !pDef->zValue ){
    return sqlite3_mprintf("null is not a valid assertion for expected_rows");
  }
  if( !testParseInteger(pDef->zValue, &expected) ){
    return sqlite3_mprintf("cannot run assertion on non integer value: %s",
        pDef->zValue);
  }
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ) actual++;
  if( rc!=SQLITE_DONE ){
    *pRc = rc;
    return 0;
  }
  zActual = sqlite3_mprintf("%lld", actual);
  zMessage = testCompareMessage("expected_rows", pDef->zComparator,
      pDef->zValue, zActual, actual<expected ? -1 : actual>expected ? 1 : 0);
  sqlite3_free(zActual);
  return zMessage;
}

static char *testAssertColumns(sqlite3_stmt *pStmt, const TestDef *pDef,
    int *pRc){
  sqlite3_int64 expected;
  sqlite3_int64 actual = 0;
  int rc;
  char *zActual;
  char *zMessage;
  if( !pDef->zValue ){
    return sqlite3_mprintf("null is not a valid assertion for expected_rows");
  }
  if( !testParseInteger(pDef->zValue, &expected) ){
    return sqlite3_mprintf("cannot run assertion on non integer value: %s",
        pDef->zValue);
  }
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ) actual = sqlite3_column_count(pStmt);
  else if( rc!=SQLITE_DONE ){
    *pRc = rc;
    return 0;
  }
  zActual = sqlite3_mprintf("%lld", actual);
  zMessage = testCompareMessage("expected_columns", pDef->zComparator,
      pDef->zValue, zActual, actual<expected ? -1 : actual>expected ? 1 : 0);
  sqlite3_free(zActual);
  return zMessage;
}

static char *testAssertSingle(sqlite3_stmt *pStmt, const TestDef *pDef,
    int *pRc){
  int rc = sqlite3_step(pStmt);
  char *zMessage;
  if( rc==SQLITE_DONE ){
    return sqlite3_mprintf("expected_single_value expects exactly one cell. "
        "Received 0 rows");
  }
  if( rc!=SQLITE_ROW ){
    *pRc = rc;
    return 0;
  }
  if( sqlite3_column_count(pStmt)!=1 ){
    return sqlite3_mprintf("expected_single_value expects exactly one cell. "
        "Received multiple columns");
  }
  zMessage = testCompareSingle(pStmt, pDef->zComparator, pDef->zValue);
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    sqlite3_free(zMessage);
    return sqlite3_mprintf("expected_single_value expects exactly one cell. "
        "Received multiple rows");
  }
  if( rc!=SQLITE_DONE ){
    sqlite3_free(zMessage);
    *pRc = rc;
    return 0;
  }
  return zMessage;
}

static char *testPrepareError(sqlite3 *db){
  const char *z = sqlite3_errmsg(db);
  if( sqlite3_strnicmp(z, "no such table: ", 15)==0 ){
    return sqlite3_mprintf("query error: table not found: %s", z+15);
  }
  return sqlite3_mprintf("query error: %s", z);
}

typedef struct TestAuth TestAuth;
struct TestAuth {
  sqlite3 *db;
  sqlite3_xauth xAuth;
  void *pAuthArg;
  int hasCommand;
  int hasPragma;
  int hasTestRun;
};

static int testAuthorizer(void *pArg, int action, const char *z1,
    const char *z2, const char *z3, const char *z4){
  TestAuth *p = (TestAuth*)pArg;
  int rc = SQLITE_OK;
  if( p->xAuth ) rc = p->xAuth(p->pAuthArg, action, z1, z2, z3, z4);
  if( rc==SQLITE_OK && action==SQLITE_FUNCTION && z2 ){
    FuncDef *pFunc = sqlite3FindFunction(p->db, z2, -2, ENC(p->db), 0);
    if( pFunc && (pFunc->funcFlags & SQLITE_FUNC_DIRECT)!=0 ){
      p->hasCommand = 1;
    }
  }
  if( rc==SQLITE_OK && (action==SQLITE_ATTACH
      || action==SQLITE_DETACH
      || action==SQLITE_TRANSACTION
      || action==SQLITE_SAVEPOINT) ){
    p->hasCommand = 1;
  }
  if( rc==SQLITE_OK && action==SQLITE_PRAGMA ){
    p->hasPragma = 1;
    rc = SQLITE_DENY;
  }
  if( rc==SQLITE_OK && action==SQLITE_READ && z1
      && sqlite3_stricmp(z1, "dolt_test_run")==0 ){
    p->hasTestRun = 1;
  }
  return rc;
}

static int testPrepare(sqlite3 *db, const char *zQuery,
    sqlite3_stmt **ppStmt, char **pzMessage){
  sqlite3_stmt *pStmt = 0;
  sqlite3_stmt *pExtra = 0;
  TestAuth auth;
  const char *zTail = 0;
  const char *zNext = 0;
  int rc;
  auth.db = db;
  auth.xAuth = db->xAuth;
  auth.pAuthArg = db->pAuthArg;
  auth.hasCommand = 0;
  auth.hasPragma = 0;
  auth.hasTestRun = 0;
  db->xAuth = testAuthorizer;
  db->pAuthArg = &auth;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, &zTail);
  db->xAuth = auth.xAuth;
  db->pAuthArg = auth.pAuthArg;
  if( auth.hasPragma ){
    sqlite3_finalize(pStmt);
    *pzMessage = sqlite3_mprintf("Cannot execute PRAGMA queries");
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    *pzMessage = testPrepareError(db);
    return SQLITE_OK;
  }
  if( auth.hasTestRun ){
    sqlite3_finalize(pStmt);
    *pzMessage = sqlite3_mprintf("Cannot call dolt_test_run in dolt_tests");
    return SQLITE_OK;
  }
  if( !pStmt ){
    *pzMessage = sqlite3_mprintf("Can only run exactly one query");
    return SQLITE_OK;
  }
  if( auth.hasCommand ){
    sqlite3_finalize(pStmt);
    *pzMessage = sqlite3_mprintf("Cannot execute write queries");
    return SQLITE_OK;
  }
  while( zTail && zTail[0] ){
    auth.hasCommand = 0;
    auth.hasPragma = 0;
    auth.hasTestRun = 0;
    db->xAuth = testAuthorizer;
    db->pAuthArg = &auth;
    rc = sqlite3_prepare_v2(db, zTail, -1, &pExtra, &zNext);
    db->xAuth = auth.xAuth;
    db->pAuthArg = auth.pAuthArg;
    if( auth.hasPragma ){
      sqlite3_finalize(pExtra);
      sqlite3_finalize(pStmt);
      *pzMessage = sqlite3_mprintf("Cannot execute PRAGMA queries");
      return SQLITE_OK;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_finalize(pStmt);
      *pzMessage = sqlite3_mprintf("Can only run exactly one query");
      return SQLITE_OK;
    }
    if( auth.hasTestRun ){
      sqlite3_finalize(pExtra);
      sqlite3_finalize(pStmt);
      *pzMessage = sqlite3_mprintf(
          "Cannot call dolt_test_run in dolt_tests");
      return SQLITE_OK;
    }
    if( pExtra ){
      sqlite3_finalize(pExtra);
      sqlite3_finalize(pStmt);
      *pzMessage = sqlite3_mprintf("Can only run exactly one query");
      return SQLITE_OK;
    }
    if( !zNext || zNext==zTail ) break;
    zTail = zNext;
  }
  if( !sqlite3_stmt_readonly(pStmt) ){
    sqlite3_finalize(pStmt);
    *pzMessage = sqlite3_mprintf("Cannot execute write queries");
    return SQLITE_OK;
  }
  *ppStmt = pStmt;
  return SQLITE_OK;
}

static int testRunOne(sqlite3 *db, const TestDef *pDef,
    TestResult *pResult){
  sqlite3_stmt *pStmt = 0;
  char *zMessage = 0;
  int rc = SQLITE_OK;
  memset(pResult, 0, sizeof(*pResult));
  pResult->zName = sqlite3_mprintf("%s", pDef->zName);
  pResult->zGroup = sqlite3_mprintf("%s", pDef->zGroup ? pDef->zGroup : "");
  pResult->zQuery = sqlite3_mprintf("%s", pDef->zQuery);
  if( !pResult->zName || !pResult->zGroup || !pResult->zQuery ){
    testResultClear(pResult);
    return SQLITE_NOMEM;
  }
  rc = testPrepare(db, pDef->zQuery, &pStmt, &zMessage);
  if( rc!=SQLITE_OK ) goto test_run_done;
  if( pStmt ){
    if( strcmp(pDef->zAssertion, "expected_rows")==0 ){
      zMessage = testAssertRows(pStmt, pDef, &rc);
    }else if( strcmp(pDef->zAssertion, "expected_columns")==0 ){
      zMessage = testAssertColumns(pStmt, pDef, &rc);
    }else if( strcmp(pDef->zAssertion, "expected_single_value")==0 ){
      zMessage = testAssertSingle(pStmt, pDef, &rc);
    }else{
      zMessage = sqlite3_mprintf("%s is not a valid assertion type",
          pDef->zAssertion);
    }
  }
test_run_done:
  if( pStmt ){
    int rcFinal = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK && rcFinal!=SQLITE_OK ) rc = rcFinal;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(zMessage);
    zMessage = sqlite3_mprintf("Query error: %s", sqlite3_errmsg(db));
    rc = zMessage ? SQLITE_OK : SQLITE_NOMEM;
  }
  if( rc!=SQLITE_OK ){
    testResultClear(pResult);
    return rc;
  }
  if( !zMessage ) zMessage = sqlite3_mprintf("");
  pResult->zStatus = sqlite3_mprintf("%s", zMessage[0] ? "FAIL" : "PASS");
  pResult->zMessage = zMessage;
  if( !pResult->zStatus || !pResult->zMessage ){
    testResultClear(pResult);
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int testRunConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  sqlite3_str *pSchema;
  char *zSchema;
  TestRunVtab *pVtab;
  int i;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  pSchema = sqlite3_str_new(db);
  sqlite3_str_appendall(pSchema,
      "CREATE TABLE x(test_name TEXT,test_group_name TEXT,query TEXT,"
      "status TEXT,message TEXT");
  for(i=0; i<TEST_RUN_MAX_ARGS; i++){
    sqlite3_str_appendf(pSchema, ",arg%d HIDDEN", i);
  }
  sqlite3_str_appendchar(pSchema, 1, ')');
  zSchema = sqlite3_str_finish(pSchema);
  if( !zSchema ) return SQLITE_NOMEM;
  rc = doltliteVtabConnectSimple(db, zSchema, sizeof(*pVtab), ppVtab);
  sqlite3_free(zSchema);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (TestRunVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int testRunBestIndex(sqlite3_vtab *pVtab,
    sqlite3_index_info *pInfo){
  int iCol;
  int iCons;
  int nArg = 0;
  (void)pVtab;
  for(iCol=TEST_RUN_FIRST_ARG;
      iCol<TEST_RUN_FIRST_ARG+TEST_RUN_MAX_ARGS; iCol++){
    for(iCons=0; iCons<pInfo->nConstraint; iCons++){
      struct sqlite3_index_constraint *p = &pInfo->aConstraint[iCons];
      if( p->usable && p->iColumn==iCol
       && p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
        sqlite3_value *pRhs = 0;
        if( sqlite3_vtab_rhs_value(pInfo, iCons, &pRhs)!=SQLITE_OK || !pRhs ){
          sqlite3_free(pVtab->zErrMsg);
          pVtab->zErrMsg = sqlite3_mprintf(
              "dolt_test_run requires literal arguments");
          return SQLITE_ERROR;
        }
        pInfo->aConstraintUsage[iCons].argvIndex = ++nArg;
        pInfo->aConstraintUsage[iCons].omit = 1;
        break;
      }
    }
  }
  pInfo->idxNum = nArg;
  pInfo->estimatedCost = 10.0;
  pInfo->estimatedRows = 10;
  return SQLITE_OK;
}

static int testRunOpen(sqlite3_vtab *pVtab,
    sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(TestRunCursor));
}

static int testRunClose(sqlite3_vtab_cursor *pCursor){
  testRunReset((TestRunCursor*)pCursor);
  sqlite3_free(pCursor);
  return SQLITE_OK;
}

static int testRunFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  TestRunCursor *p = (TestRunCursor*)pCursor;
  TestRunVtab *pVtab = (TestRunVtab*)pCursor->pVtab;
  TestDef *aDef = 0;
  int nDef = 0;
  int i;
  int rc = SQLITE_OK;
  (void)idxNum; (void)idxStr;
  testRunReset(p);
  if( argc==0 ){
    rc = testLoadArgument(pVtab->db, "*", &aDef, &nDef);
  }else{
    for(i=0; i<argc && rc==SQLITE_OK; i++){
      const unsigned char *z = sqlite3_value_text(argv[i]);
      int nBefore = nDef;
      rc = testLoadArgument(pVtab->db, z ? (const char*)z : "NULL",
          &aDef, &nDef);
      if( rc==SQLITE_OK && nDef==nBefore ){
        sqlite3_free(pVtab->base.zErrMsg);
        pVtab->base.zErrMsg = sqlite3_mprintf(
            "could not find tests for argument: %s", z ? z : (const unsigned char*)"NULL");
        rc = SQLITE_ERROR;
      }
    }
  }
  if( rc==SQLITE_OK && nDef==0 ){
    sqlite3_free(pVtab->base.zErrMsg);
    pVtab->base.zErrMsg = sqlite3_mprintf(
        "could not find tests for argument: *");
    rc = SQLITE_ERROR;
  }
  if( rc==SQLITE_OK ){
    p->aResult = sqlite3_malloc64((sqlite3_uint64)nDef * sizeof(*p->aResult));
    if( !p->aResult ) rc = SQLITE_NOMEM;
  }
  if( rc==SQLITE_OK ){
    memset(p->aResult, 0, (size_t)nDef * sizeof(*p->aResult));
    for(i=0; i<nDef; i++){
      rc = testRunOne(pVtab->db, &aDef[i], &p->aResult[i]);
      if( rc!=SQLITE_OK ) break;
      p->nResult++;
    }
  }
  for(i=0; i<nDef; i++) testDefClear(&aDef[i]);
  sqlite3_free(aDef);
  if( rc!=SQLITE_OK ) testRunReset(p);
  return rc;
}

static int testRunNext(sqlite3_vtab_cursor *pCursor){
  ((TestRunCursor*)pCursor)->iRow++;
  return SQLITE_OK;
}

static int testRunEof(sqlite3_vtab_cursor *pCursor){
  TestRunCursor *p = (TestRunCursor*)pCursor;
  return p->iRow>=p->nResult;
}

static int testRunColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  TestRunCursor *p = (TestRunCursor*)pCursor;
  TestResult *pResult = &p->aResult[p->iRow];
  const char *z = iCol==0 ? pResult->zName
      : iCol==1 ? pResult->zGroup
      : iCol==2 ? pResult->zQuery
      : iCol==3 ? pResult->zStatus
      : iCol==4 ? pResult->zMessage : 0;
  if( z ) sqlite3_result_text(ctx, z, -1, SQLITE_TRANSIENT);
  else sqlite3_result_null(ctx);
  return SQLITE_OK;
}

static int testRunRowid(sqlite3_vtab_cursor *pCursor,
    sqlite3_int64 *pRowid){
  TestRunCursor *p = (TestRunCursor*)pCursor;
  TestResult *r = &p->aResult[p->iRow];
  u64 h = doltliteFnv1aStr(DOLTLITE_FNV1A_OFFSET, r->zGroup);
  h = doltliteFnv1aSep(h);
  h = doltliteFnv1aStr(h, r->zName);
  *pRowid = doltliteFnv1aRowid(h);
  return SQLITE_OK;
}

static sqlite3_module doltliteTestRunModule = {
  0, 0, testRunConnect, testRunBestIndex, doltliteVtabDisconnect, 0,
  testRunOpen, testRunClose, testRunFilter, testRunNext, testRunEof,
  testRunColumn, testRunRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteTestsRegister(sqlite3 *db){
  int rc = sqlite3_create_module(db, "dolt_tests", &doltliteTestsModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "dolt_test_run",
        &doltliteTestRunModule, 0);
  }
  return rc;
}

#endif
