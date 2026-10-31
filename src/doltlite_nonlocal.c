#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include "doltlite_vtab_util.h"
#include "prolly_cursor.h"
#include "doltlite_record.h"
#include <string.h>

#define NONLOCAL_SCHEMA_PREFIX "dolt_nonlocal:"

typedef struct NonlocalRule NonlocalRule;
struct NonlocalRule {
  char *zPattern;
  char *zRef;
  char *zRefTable;
  char *zOptions;
};

typedef struct NonlocalAttach NonlocalAttach;
struct NonlocalAttach {
  char *zRef;
  char *zSchema;
};

struct DoltliteNonlocal {
  NonlocalRule *aRule;
  int nRule;
  char *zBranch;
  ProllyHash stamp;
  int bLoaded;
  int bStale;
  NonlocalAttach *aAttach;
  int nAttach;
  char **azMissing;
  int nMissing;
  ProllyHash refsSeen;
  u8 bSyncing;
};

static void nonlocalRulesClear(DoltliteNonlocal *p){
  int i;
  for(i=0; i<p->nRule; i++){
    sqlite3_free(p->aRule[i].zPattern);
    sqlite3_free(p->aRule[i].zRef);
    sqlite3_free(p->aRule[i].zRefTable);
    sqlite3_free(p->aRule[i].zOptions);
  }
  sqlite3_free(p->aRule);
  p->aRule = 0;
  p->nRule = 0;
  sqlite3_free(p->zBranch);
  p->zBranch = 0;
  p->bLoaded = 0;
}

static void nonlocalMissingClear(DoltliteNonlocal *p){
  int i;
  for(i=0; i<p->nMissing; i++) sqlite3_free(p->azMissing[i]);
  sqlite3_free(p->azMissing);
  p->azMissing = 0;
  p->nMissing = 0;
}

static int nonlocalIsMissing(DoltliteNonlocal *p, const char *zRef){
  int i;
  for(i=0; i<p->nMissing; i++){
    if( strcmp(p->azMissing[i], zRef)==0 ) return 1;
  }
  return 0;
}

void doltliteNonlocalFree(DoltliteNonlocal *p){
  int i;
  if( !p ) return;
  nonlocalRulesClear(p);
  nonlocalMissingClear(p);
  for(i=0; i<p->nAttach; i++){
    sqlite3_free(p->aAttach[i].zRef);
    sqlite3_free(p->aAttach[i].zSchema);
  }
  sqlite3_free(p->aAttach);
  sqlite3_free(p);
}

static DoltliteNonlocal *nonlocalState(sqlite3 *db, int bCreate){
  DoltliteNonlocal **pp = doltliteNonlocalSlot(db);
  if( !pp ) return 0;
  if( !*pp && bCreate ){
    *pp = sqlite3_malloc(sizeof(**pp));
    if( *pp ) memset(*pp, 0, sizeof(**pp));
  }
  return *pp;
}

static const char *nonlocalRuleRef(DoltliteNonlocal *p, const NonlocalRule *r){
  return r->zRef && r->zRef[0] ? r->zRef : p->zBranch;
}

static void nonlocalToLower(char *z){
  for(; *z; z++) *z = sqlite3Tolower(*z);
}

static int nonlocalIsSystemName(const char *zName){
  return sqlite3_strnicmp(zName, "dolt_", 5)==0
      || sqlite3_strnicmp(zName, "sqlite_", 7)==0;
}

/* The first rule, in table_name order, whose pattern matches zName. */
static const NonlocalRule *nonlocalMatch(DoltliteNonlocal *p, const char *zName){
  char *zLower;
  int i;
  const NonlocalRule *pMatch = 0;
  if( !p || p->nRule==0 || !zName || nonlocalIsSystemName(zName) ) return 0;
  zLower = sqlite3_mprintf("%s", zName);
  if( !zLower ) return 0;
  nonlocalToLower(zLower);
  for(i=0; i<p->nRule && !pMatch; i++){
    char *zPat = sqlite3_mprintf("%s", p->aRule[i].zPattern);
    if( zPat ){
      nonlocalToLower(zPat);
      if( doltliteTablePatternMatch(zPat, zLower) ) pMatch = &p->aRule[i];
      sqlite3_free(zPat);
    }
  }
  sqlite3_free(zLower);
  return pMatch;
}

static int nonlocalLoadRules(sqlite3 *db, DoltliteNonlocal *p){
  sqlite3_stmt *pStmt = 0;
  int rc;
  nonlocalRulesClear(p);
  p->zBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  if( !p->zBranch ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db,
      "SELECT table_name, target_ref, ref_table, options "
      "FROM main.dolt_nonlocal_tables ORDER BY table_name", -1, &pStmt, 0);
  while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
    NonlocalRule *aNew;
    NonlocalRule *r;
    int i;
    const char *az[4];
    if( !sqlite3_column_text(pStmt, 0) ) continue;
    aNew = sqlite3_realloc64(p->aRule, sizeof(*aNew)*(p->nRule+1));
    if( !aNew ){ rc = SQLITE_NOMEM; break; }
    p->aRule = aNew;
    r = &aNew[p->nRule++];
    memset(r, 0, sizeof(*r));
    for(i=0; i<4; i++){
      const char *z = (const char*)sqlite3_column_text(pStmt, i);
      az[i] = z ? z : "";
    }
    r->zPattern = sqlite3_mprintf("%s", az[0]);
    r->zRef = sqlite3_mprintf("%s", az[1]);
    r->zRefTable = sqlite3_mprintf("%s", az[2]);
    r->zOptions = sqlite3_mprintf("%s", az[3]);
    if( !r->zPattern || !r->zRef || !r->zRefTable || !r->zOptions ){
      rc = SQLITE_NOMEM;
    }
  }
  if( pStmt ){
    int rc2 = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  if( rc==SQLITE_OK ){
    p->bLoaded = 1;
  }else{
    nonlocalRulesClear(p);
  }
  return rc;
}

/* Schema of this connection that holds ref on the main store: an ATTACH
** the user made, or one made here. */
static const char *nonlocalSchemaFor(sqlite3 *db, DoltliteNonlocal *p,
                                     const char *zRef){
  int i;
  for(i=2; i<db->nDb; i++){
    Btree *q = db->aDb[i].pBt;
    if( q && doltliteBtreeSameStore(db->aDb[0].pBt, q)
     && strcmp(doltliteBtreeBranchName(q), zRef)==0 ){
      return db->aDb[i].zDbSName;
    }
  }
  for(i=0; p && i<p->nAttach; i++){
    if( strcmp(p->aAttach[i].zRef, zRef)==0
     && sqlite3FindDbName(db, p->aAttach[i].zSchema)>=0 ){
      return p->aAttach[i].zSchema;
    }
  }
  return 0;
}

static int nonlocalAttach(sqlite3 *db, DoltliteNonlocal *p, const char *zRef){
  const char *zFile = sqlite3_db_filename(db, "main");
  NonlocalAttach *aNew;
  char *zSchema;
  char *zSql;
  int rc;
  if( !zFile || !zFile[0] ) return SQLITE_OK;
  zSchema = sqlite3_mprintf(NONLOCAL_SCHEMA_PREFIX "%s", zRef);
  if( !zSchema ) return SQLITE_NOMEM;
  zSql = sqlite3_mprintf("ATTACH '%q/%q' AS \"%w\"", zFile, zRef, zSchema);
  if( !zSql ){
    sqlite3_free(zSchema);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zSchema);
    return rc;
  }
  aNew = sqlite3_realloc64(p->aAttach, sizeof(*aNew)*(p->nAttach+1));
  if( !aNew ){
    sqlite3_free(zSchema);
    return SQLITE_NOMEM;
  }
  p->aAttach = aNew;
  aNew[p->nAttach].zRef = sqlite3_mprintf("%s", zRef);
  aNew[p->nAttach].zSchema = zSchema;
  if( !aNew[p->nAttach].zRef ) return SQLITE_NOMEM;
  p->nAttach++;
  return SQLITE_OK;
}

static int nonlocalNeedsRef(DoltliteNonlocal *p, const char *zRef){
  int i;
  for(i=0; i<p->nRule; i++){
    if( strcmp(nonlocalRuleRef(p, &p->aRule[i]), zRef)==0 ) return 1;
  }
  return 0;
}

/* Attachments made here for refs no rule names any more, or for the
** session's own branch: release them while nothing uses them. */
static void nonlocalDetachUnused(sqlite3 *db, DoltliteNonlocal *p){
  int i = 0;
  while( i<p->nAttach ){
    NonlocalAttach *a = &p->aAttach[i];
    int iDb = sqlite3FindDbName(db, a->zSchema);
    int bKeep = nonlocalNeedsRef(p, a->zRef)
             && strcmp(a->zRef, p->zBranch)!=0
             && !nonlocalIsMissing(p, a->zRef);
    if( iDb>=0 && !bKeep ){
      char *zSql;
      if( db->nVdbeActive>0
       || sqlite3BtreeTxnState(db->aDb[iDb].pBt)!=SQLITE_TXN_NONE ){
        i++;
        continue;
      }
      zSql = sqlite3_mprintf("DETACH \"%w\"", a->zSchema);
      if( !zSql || sqlite3_exec(db, zSql, 0, 0, 0)!=SQLITE_OK ){
        sqlite3_free(zSql);
        i++;
        continue;
      }
      sqlite3_free(zSql);
      iDb = -1;
    }
    if( iDb<0 ){
      sqlite3_free(a->zRef);
      sqlite3_free(a->zSchema);
      p->aAttach[i] = p->aAttach[--p->nAttach];
      continue;
    }
    i++;
  }
}

static int nonlocalCheckRefs(sqlite3 *db, DoltliteNonlocal *p){
  int i;
  nonlocalMissingClear(p);
  for(i=0; i<p->nRule; i++){
    const char *zRef = nonlocalRuleRef(p, &p->aRule[i]);
    ProllyHash h;
    char **az;
    int rc;
    if( strcmp(zRef, p->zBranch)==0 || nonlocalIsMissing(p, zRef) ) continue;
    rc = doltliteResolveRef(db, zRef, &h);
    if( rc==SQLITE_NOMEM ) return rc;
    if( rc==SQLITE_OK ) continue;
    az = sqlite3_realloc64(p->azMissing, sizeof(*az)*(p->nMissing+1));
    if( !az ) return SQLITE_NOMEM;
    p->azMissing = az;
    az[p->nMissing] = sqlite3_mprintf("%s", zRef);
    if( !az[p->nMissing] ) return SQLITE_NOMEM;
    p->nMissing++;
  }
  return SQLITE_OK;
}

/* Around a top-level statement's compile: reload the rules when the
** rules table changed, check the refs they target when the refs moved,
** and attach each target that resolves. Returns nonzero when the
** redirects changed, so a statement compiled before should compile
** again. A failure here leaves names unredirected; the statement reports
** what it cannot resolve. */
int doltliteNonlocalSync(sqlite3 *db){
  DoltliteNonlocal *p;
  Table *pTab;
  ChunkStore *cs;
  ProllyHash stamp;
  const ProllyHash *pRefs;
  int bPending = 0;
  int bReload;
  int nAttach;
  int nMissing;
  int i;

  if( db->nVdbeExec>0 || db->nDb<1 || !doltliteNonlocalSlot(db) ) return 0;
  if( !DbHasProperty(db, 0, DB_SchemaLoaded) ) return 0;
  p = nonlocalState(db, 0);
  if( p && p->bSyncing ) return 0;
  pTab = sqlite3FindTable(db, "dolt_nonlocal_tables", "main");
  if( !pTab || IsVirtual(pTab) ){
    if( p && (p->nRule>0 || p->nAttach>0) ){
      p->bSyncing = 1;
      nonlocalRulesClear(p);
      nonlocalMissingClear(p);
      nonlocalDetachUnused(db, p);
      p->bSyncing = 0;
      return 1;
    }
    return 0;
  }
  if( doltliteBtreeTableStamp(db->aDb[0].pBt, pTab->tnum, &stamp, &bPending)
      !=SQLITE_OK ){
    return 0;
  }
  cs = doltliteGetChunkStore(db);
  if( !cs ) return 0;
  if( !p ) p = nonlocalState(db, 1);
  if( !p ) return 0;
  pRefs = refsTableGetHash(&cs->refs);
  bReload = !p->bLoaded || p->bStale || bPending
         || prollyHashCompare(&p->stamp, &stamp)!=0
         || !p->zBranch || strcmp(p->zBranch, doltliteGetSessionBranch(db))!=0;
  if( !bReload
   && (p->nRule==0 || prollyHashCompare(&p->refsSeen, pRefs)==0) ){
    return 0;
  }
  p->bSyncing = 1;
  nAttach = db->nDb;
  nMissing = p->nMissing;
  if( !bReload || nonlocalLoadRules(db, p)==SQLITE_OK ){
    if( bReload ){
      p->stamp = stamp;
      p->bStale = bPending;
    }
    p->refsSeen = *pRefs;
    if( nonlocalCheckRefs(db, p)==SQLITE_OK ){
      for(i=0; i<p->nRule; i++){
        const char *zRef = nonlocalRuleRef(p, &p->aRule[i]);
        if( strcmp(zRef, p->zBranch)==0 || nonlocalIsMissing(p, zRef)
         || nonlocalSchemaFor(db, p, zRef) ){
          continue;
        }
        (void)nonlocalAttach(db, p, zRef);
      }
    }
    nonlocalDetachUnused(db, p);
  }
  p->bSyncing = 0;
  if( bReload ) return !bPending || db->nDb!=nAttach;
  return db->nDb!=nAttach || nMissing!=0 || p->nMissing!=0;
}

static void nonlocalListName(
  Parse *pParse,
  DoltliteNonlocal *p,
  const NonlocalRule *r,
  const char *zName,
  const char *zSchema,
  const char *zRight
){
  sqlite3 *db = pParse->db;
  Table *pTab;
  const char *zType;
  if( zRight && sqlite3StrICmp(zRight, zName)!=0 ) return;
  if( nonlocalMatch(p, zName)!=r || sqlite3FindTable(db, zName, "main") ){
    return;
  }
  pTab = sqlite3FindTable(db, r->zRefTable[0] ? r->zRefTable : zName, zSchema);
  if( !pTab ) return;
  if( IsView(pTab) ){
    zType = "view";
  }else if( IsVirtual(pTab) ){
    zType = "virtual";
  }else{
    zType = "table";
  }
  sqlite3VdbeMultiLoad(sqlite3GetVdbe(pParse), 1, "sssiii", "main", zName,
      zType, pTab->nCol, (pTab->tabFlags & TF_WithoutRowid)!=0,
      (pTab->tabFlags & TF_Strict)!=0);
}

/* Whether a dolt_nonlocal_tables rule stored in pCatalog claims zName. */
int doltliteNonlocalCatalogClaims(
  sqlite3 *db,
  const ProllyHash *pCatalog,
  const char *zName,
  int *pClaimed
){
  struct TableEntry *aTables = 0, *pTable;
  ProllyCursor cur;
  char *zLower;
  int nTables = 0, res = 0, rc;
  *pClaimed = 0;
  if( nonlocalIsSystemName(zName) ) return SQLITE_OK;
  rc = doltliteLoadCatalog(db, pCatalog, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pTable = doltliteFindTableByName(aTables, nTables, "dolt_nonlocal_tables");
  if( !pTable ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_OK;
  }
  zLower = sqlite3_mprintf("%s", zName);
  if( !zLower ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_NOMEM;
  }
  nonlocalToLower(zLower);
  prollyCursorInit(&cur, doltliteGetChunkStore(db), doltliteGetCache(db),
      &pTable->root, pTable->flags);
  rc = prollyCursorFirst(&cur, &res);
  while( rc==SQLITE_OK && !res && !*pClaimed && prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0;
    int nVal = 0;
    DoltliteRecordInfo info = {0};
    char *zPattern = 0;
    prollyCursorValue(&cur, &pVal, &nVal);
    rc = doltliteParseRecordStrict(pVal, nVal, &info);
    if( rc==SQLITE_OK ) rc = dlRecordTextField(pVal, nVal, &info, 0, &zPattern);
    if( rc==SQLITE_OK && zPattern ){
      nonlocalToLower(zPattern);
      *pClaimed = doltliteTablePatternMatch(zPattern, zLower);
    }
    sqlite3_free(zPattern);
    doltliteRecordInfoClear(&info);
    if( rc==SQLITE_OK ) rc = prollyCursorNext(&cur);
  }
  prollyCursorClose(&cur);
  sqlite3_free(zLower);
  doltliteFreeCatalog(aTables, nTables);
  return rc;
}

/* PRAGMA table_list rows for the names this branch's rules resolve, as
** Dolt lists them in SHOW TABLES: a plain name when its target table
** exists, and each target table a wildcard rule claims. */
void doltliteNonlocalTableList(Parse *pParse, const char *zRight){
  sqlite3 *db = pParse->db;
  DoltliteNonlocal *p = nonlocalState(db, 0);
  int i;
  if( !p || !p->bLoaded ) return;
  for(i=0; i<p->nRule; i++){
    const NonlocalRule *r = &p->aRule[i];
    const char *zRef = nonlocalRuleRef(p, r);
    const char *zSchema;
    int iDb;
    HashElem *k;
    if( sqlite3StrICmp(r->zOptions, "immediate")!=0 ) continue;
    zSchema = strcmp(zRef, p->zBranch)==0 ? "main"
            : nonlocalSchemaFor(db, p, zRef);
    if( !zSchema || (iDb = sqlite3FindDbName(db, zSchema))<0 ) continue;
    if( !strpbrk(r->zPattern, "*%?") ){
      nonlocalListName(pParse, p, r, r->zPattern, zSchema, zRight);
      continue;
    }
    if( r->zRefTable[0] ) continue;
    for(k=sqliteHashFirst(&db->aDb[iDb].pSchema->tblHash); k;
        k=sqliteHashNext(k)){
      Table *pTab = sqliteHashData(k);
      nonlocalListName(pParse, p, r, pTab->zName, zSchema, zRight);
    }
  }
}

/* Redirects a name a rule on the session branch claims: *pzDb and *pzName
** become the schema and table it resolves to. Returns 0 when no rule
** applies, 1 when redirected, -1 with an error left in pParse. */
int doltliteNonlocalResolve(
  Parse *pParse,
  const char **pzName,
  const char **pzDb
){
  sqlite3 *db = pParse->db;
  DoltliteNonlocal *p;
  const NonlocalRule *r;
  const char *zRef;
  const char *zSchema;

  if( *pzDb && sqlite3StrICmp(*pzDb, "main")!=0 ) return 0;
  p = nonlocalState(db, 0);
  if( !p || !p->bLoaded ) return 0;
  r = nonlocalMatch(p, *pzName);
  if( !r ) return 0;
  if( sqlite3StrICmp(r->zOptions, "immediate")!=0 ){
    sqlite3ErrorMsg(pParse, "Invalid nonlocal table options %s: only valid "
                    "value is 'immediate'.", r->zOptions);
    return -1;
  }
  zRef = nonlocalRuleRef(p, r);
  if( nonlocalIsMissing(p, zRef) ){
    sqlite3ErrorMsg(pParse, "branch not found: %s", zRef);
    return -1;
  }
  if( strcmp(zRef, p->zBranch)==0 ){
    zSchema = "main";
  }else{
    zSchema = nonlocalSchemaFor(db, p, zRef);
    if( !zSchema ) return 0;
  }
  if( r->zRefTable[0] ) *pzName = r->zRefTable;
  *pzDb = zSchema;
  return 1;
}

static int nonlocalResolveName(
  sqlite3 *db,
  const char **pzName,
  const char **pzDb
){
  DoltliteNonlocal *p = nonlocalState(db, 0);
  const NonlocalRule *r;
  const char *zRef;
  const char *zSchema;
  if( *pzDb && sqlite3StrICmp(*pzDb, "main")!=0 ) return 0;
  if( !p || !p->bLoaded ) return 0;
  r = nonlocalMatch(p, *pzName);
  if( !r || sqlite3StrICmp(r->zOptions, "immediate")!=0 ) return 0;
  zRef = nonlocalRuleRef(p, r);
  if( strcmp(zRef, p->zBranch)==0 ){
    zSchema = "main";
  }else{
    zSchema = nonlocalSchemaFor(db, p, zRef);
    if( !zSchema ) return 0;
  }
  if( r->zRefTable[0] ) *pzName = r->zRefTable;
  *pzDb = zSchema;
  return 1;
}

Table *doltliteFkParentTable(sqlite3 *db, const char *zTo, const char *zDb){
  const char *zName = zTo;
  const char *zSchema = zDb;
  if( nonlocalResolveName(db, &zName, &zSchema)>0 ){
    Table *pTab = sqlite3FindTable(db, zName, zSchema);
    if( pTab ) return pTab;
  }
  return sqlite3FindTable(db, zTo, zDb);
}

int doltliteNonlocalFkParent(
  sqlite3 *db,
  const char *zTo,
  const char **pzSchema,
  const char **pzTable
){
  *pzTable = zTo;
  *pzSchema = "main";
  return nonlocalResolveName(db, pzTable, pzSchema)>0
      && sqlite3StrICmp(*pzSchema, "main")!=0;
}

/* CREATE TABLE of a name a rule claims is refused, except creating the
** rule's own backing table on its target branch. */
int doltliteNonlocalBlocksCreate(sqlite3 *db, const char *zName){
  DoltliteNonlocal *p = nonlocalState(db, 0);
  const NonlocalRule *r;
  if( !p || !p->bLoaded ) return 0;
  r = nonlocalMatch(p, zName);
  if( !r ) return 0;
  if( strcmp(nonlocalRuleRef(p, r), p->zBranch)==0
   && (!r->zRefTable[0] || sqlite3StrICmp(r->zRefTable, zName)==0) ){
    return 0;
  }
  return 1;
}

int doltliteNonlocalMatches(sqlite3 *db, const char *zName){
  return nonlocalMatch(nonlocalState(db, 0), zName)!=0;
}

int doltliteNonlocalIsSchema(const char *zSchema){
  return zSchema && sqlite3_strnicmp(zSchema, NONLOCAL_SCHEMA_PREFIX,
                                     sizeof(NONLOCAL_SCHEMA_PREFIX)-1)==0;
}

typedef struct NonlocalVtab NonlocalVtab;
struct NonlocalVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

static const char *zNonlocalVtabSchema =
  "CREATE TABLE x(table_name TEXT NOT NULL PRIMARY KEY, target_ref TEXT, "
  "ref_table TEXT, options TEXT)";

static const char *zNonlocalCreate =
  "CREATE TABLE main.dolt_nonlocal_tables("
  "table_name TEXT NOT NULL, target_ref TEXT, ref_table TEXT, options TEXT, "
  "PRIMARY KEY(table_name))";

static int nonlocalConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, zNonlocalVtabSchema,
      sizeof(NonlocalVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  ((NonlocalVtab*)*ppVtab)->db = db;
  return SQLITE_OK;
}

static int nonlocalOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(sqlite3_vtab_cursor));
}

static int nonlocalMaterialize(NonlocalVtab *p){
  return doltliteLazyCreateTable(&p->base, p->db, "dolt_nonlocal_tables",
                                 zNonlocalCreate);
}

static int nonlocalBegin(sqlite3_vtab *pBase){
  return nonlocalMaterialize((NonlocalVtab*)pBase);
}

static int nonlocalUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                          sqlite3_int64 *pRowid){
  NonlocalVtab *p = (NonlocalVtab*)pBase;
  sqlite3_stmt *pStmt = 0;
  const char *zSql;
  int i;
  int rc = nonlocalMaterialize(p);
  if( rc!=SQLITE_OK ) return rc;
  if( argc==1 ) return SQLITE_OK;
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ) return SQLITE_OK;
  switch( sqlite3_vtab_on_conflict(p->db) ){
    case SQLITE_REPLACE:
      zSql = "INSERT OR REPLACE INTO main.dolt_nonlocal_tables VALUES(?,?,?,?)";
      break;
    case SQLITE_IGNORE:
      zSql = "INSERT OR IGNORE INTO main.dolt_nonlocal_tables VALUES(?,?,?,?)";
      break;
    default:
      zSql = "INSERT INTO main.dolt_nonlocal_tables VALUES(?,?,?,?)";
      break;
  }
  rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return doltliteVtabSetErrmsgFromDb(&p->base, p->db, rc);
  for(i=0; i<4; i++) sqlite3_bind_value(pStmt, i+1, argv[i+2]);
  sqlite3_step(pStmt);
  rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK ) return doltliteVtabSetErrmsgFromDb(&p->base, p->db, rc);
  if( pRowid ) *pRowid = sqlite3_last_insert_rowid(p->db);
  return SQLITE_OK;
}

static sqlite3_module doltliteNonlocalModule = {
  0, 0, nonlocalConnect, doltliteVtabBestIndexEmpty, doltliteVtabDisconnect, 0,
  nonlocalOpen, doltliteVtabClose, doltliteVtabEmptyFilter,
  doltliteVtabEmptyNext, doltliteVtabEofAlways, doltliteVtabEmptyColumn,
  doltliteVtabZeroRowid, nonlocalUpdate, nonlocalBegin,
  0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteNonlocalRegister(sqlite3 *db){
  return doltliteCreateShieldedModule(db, "dolt_nonlocal_tables",
                                      &doltliteNonlocalModule, 0);
}

#endif
