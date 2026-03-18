/*
** dolt_schema_diff('from_ref', 'to_ref') — schema-level diff between commits.
**
** Shows tables added, dropped, or modified (schema changed) between
** two commits:
**
**   SELECT * FROM dolt_schema_diff('abc123...', 'def456...');
**   -- table_name | from_create_stmt | to_create_stmt | diff_type
**
** Also accepts branch and tag names.
** With no arguments, diffs HEAD vs working state.
**
** Works by reading sqlite_master (table 1) at each commit and
** comparing the CREATE TABLE statements.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void *doltliteGetBtShared(sqlite3 *db);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);

struct TableEntry { Pgno iTable; ProllyHash root; ProllyHash schemaHash; u8 flags; char *zName; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* --------------------------------------------------------------------------
** Schema entry: name + CREATE statement extracted from sqlite_master rows
** -------------------------------------------------------------------------- */

typedef struct SchemaEntry SchemaEntry;
struct SchemaEntry {
  char *zName;
  char *zSql;       /* CREATE TABLE ... statement */
  char *zType;      /* "table", "index", "view", etc. */
};

/* --------------------------------------------------------------------------
** Schema diff row
** -------------------------------------------------------------------------- */

typedef struct SchemaDiffRow SchemaDiffRow;
struct SchemaDiffRow {
  char *zName;
  char *zFromSql;
  char *zToSql;
  char *zDiffType;    /* "added", "dropped", "modified" */
};

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct SdVtab SdVtab;
struct SdVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct SdCursor SdCursor;
struct SdCursor {
  sqlite3_vtab_cursor base;
  SchemaDiffRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

static void freeSchemaDiffRows(SdCursor *c){
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aRows[i].zName);
    sqlite3_free(c->aRows[i].zFromSql);
    sqlite3_free(c->aRows[i].zToSql);
  }
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
  c->nAlloc = 0;
}

/* --------------------------------------------------------------------------
** Resolve a ref to a commit hash (hash, branch, or tag)
** -------------------------------------------------------------------------- */

static int sdResolveRef(ChunkStore *cs, const char *zRef, ProllyHash *pCommit){
  int rc;
  if( zRef && strlen(zRef)==40 ){
    rc = doltliteHexToHash(zRef, pCommit);
    if( rc==SQLITE_OK && chunkStoreHas(cs, pCommit) ) return SQLITE_OK;
  }
  rc = chunkStoreFindBranch(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ) return SQLITE_OK;
  rc = chunkStoreFindTag(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ) return SQLITE_OK;
  return SQLITE_NOTFOUND;
}

/* --------------------------------------------------------------------------
** Extract schema entries from sqlite_master (table 1) at a given catalog.
**
** sqlite_master rows are SQLite records with fields:
**   type(TEXT), name(TEXT), tbl_name(TEXT), rootpage(INT), sql(TEXT)
**
** We use ProllyCursor to scan table 1's prolly tree, then parse each
** row's record to extract name and sql.
** -------------------------------------------------------------------------- */

/*
** Parse a SQLite record header to find field offsets.
** Returns the number of fields, fills aOffset with byte offsets
** and aType with serial types.
*/
/*
** Read a SQLite varint from p. Returns bytes consumed.
*/
static int readVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v = 0;
  int i;
  for(i=0; i<9 && p+i<pEnd; i++){
    if( i<8 ){
      v = (v << 7) | (p[i] & 0x7f);
      if( (p[i] & 0x80)==0 ){ *pVal = v; return i+1; }
    }else{
      v = (v << 8) | p[i];
      *pVal = v;
      return 9;
    }
  }
  *pVal = v;
  return i;
}

static int parseRecordHeader(
  const u8 *pData, int nData,
  int *aType, int *aOffset, int maxFields
){
  int iField = 0;
  const u8 *p = pData;
  const u8 *pDataEnd = pData + nData;
  const u8 *pEnd;
  int off;
  u64 hdrSize;
  int hdrBytes;

  if( nData < 1 ) return 0;

  /* Header size varint */
  hdrBytes = readVarint(p, pDataEnd, &hdrSize);
  p += hdrBytes;

  pEnd = pData + (int)hdrSize;
  off = (int)hdrSize;

  while( p < pEnd && p < pDataEnd && iField < maxFields ){
    u64 st;
    int stBytes = readVarint(p, pEnd, &st);
    p += stBytes;

    aType[iField] = (int)st;
    aOffset[iField] = off;

    /* Compute field size from serial type */
    if( st==0 ) { /* NULL */ }
    else if( st==1 ) off += 1;
    else if( st==2 ) off += 2;
    else if( st==3 ) off += 3;
    else if( st==4 ) off += 4;
    else if( st==5 ) off += 6;
    else if( st==6 ) off += 8;
    else if( st==7 ) off += 8;
    else if( st==8 || st==9 ) { /* 0 or 1, 0 bytes */ }
    else if( st>=12 && (st&1)==0 ) off += ((int)st-12)/2;  /* blob */
    else if( st>=13 && (st&1)==1 ) off += ((int)st-13)/2;  /* text */
    iField++;
  }

  return iField;
}

static int loadSchemaFromCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pCatHash,
  SchemaEntry **ppEntries, int *pnEntries
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  ProllyHash masterRoot;
  u8 masterFlags = 0;
  ProllyCursor cur;
  int res, rc, i;
  SchemaEntry *aEntries = 0;
  int nEntries = 0, nAlloc = 0;

  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ){ *ppEntries = 0; *pnEntries = 0; return rc; }

  /* Find table 1 (sqlite_master) */
  memset(&masterRoot, 0, sizeof(masterRoot));
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==1 ){
      memcpy(&masterRoot, &aTables[i].root, sizeof(ProllyHash));
      masterFlags = aTables[i].flags;
      break;
    }
  }
  sqlite3_free(aTables);

  if( prollyHashIsEmpty(&masterRoot) ){
    *ppEntries = 0; *pnEntries = 0;
    return SQLITE_OK;
  }

  /* Scan sqlite_master rows */
  prollyCursorInit(&cur, cs, pCache, &masterRoot, masterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){ prollyCursorClose(&cur); *ppEntries = 0; *pnEntries = 0; return rc; }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    int aType[5], aOffset[5];
    int nFields;

    prollyCursorValue(&cur, &pVal, &nVal);

    if( pVal && nVal > 0 ){
      nFields = parseRecordHeader(pVal, nVal, aType, aOffset, 5);

      /* We need: field 0 = type, field 1 = name, field 4 = sql */
      if( nFields >= 5 ){
        char *zType = 0, *zName = 0, *zSql = 0;

        /* Extract text fields */
        if( aType[0] >= 13 && (aType[0]&1)==1 ){
          int len = (aType[0]-13)/2;
          if( aOffset[0]+len <= nVal ){
            zType = sqlite3_malloc(len+1);
            if(zType){ memcpy(zType, pVal+aOffset[0], len); zType[len]=0; }
          }
        }
        if( aType[1] >= 13 && (aType[1]&1)==1 ){
          int len = (aType[1]-13)/2;
          if( aOffset[1]+len <= nVal ){
            zName = sqlite3_malloc(len+1);
            if(zName){ memcpy(zName, pVal+aOffset[1], len); zName[len]=0; }
          }
        }
        if( aType[4] >= 13 && (aType[4]&1)==1 ){
          int len = (aType[4]-13)/2;
          if( aOffset[4]+len <= nVal ){
            zSql = sqlite3_malloc(len+1);
            if(zSql){ memcpy(zSql, pVal+aOffset[4], len); zSql[len]=0; }
          }
        }

        if( zName ){
          if( nEntries >= nAlloc ){
            int nNew = nAlloc ? nAlloc*2 : 16;
            SchemaEntry *aNew = sqlite3_realloc(aEntries, nNew*(int)sizeof(SchemaEntry));
            if( aNew ){ aEntries = aNew; nAlloc = nNew; }
          }
          if( nEntries < nAlloc ){
            aEntries[nEntries].zName = zName; zName = 0;
            aEntries[nEntries].zSql = zSql; zSql = 0;
            aEntries[nEntries].zType = zType; zType = 0;
            nEntries++;
          }
        }
        sqlite3_free(zType);
        sqlite3_free(zName);
        sqlite3_free(zSql);
      }
    }

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }

  prollyCursorClose(&cur);
  *ppEntries = aEntries;
  *pnEntries = nEntries;
  return SQLITE_OK;
}

static void freeSchemaEntries(SchemaEntry *a, int n){
  int i;
  for(i=0; i<n; i++){
    sqlite3_free(a[i].zName);
    sqlite3_free(a[i].zSql);
    sqlite3_free(a[i].zType);
  }
  sqlite3_free(a);
}

/* --------------------------------------------------------------------------
** Find a schema entry by name
** -------------------------------------------------------------------------- */

static SchemaEntry *findSchemaEntry(SchemaEntry *a, int n, const char *zName){
  int i;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ) return &a[i];
  }
  return 0;
}

/* --------------------------------------------------------------------------
** Compute schema diff between two sets of entries
** -------------------------------------------------------------------------- */

static int computeSchemaDiff(
  SdCursor *pCur,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo
){
  int i;

  /* Check each "to" entry: added or modified */
  for(i=0; i<nTo; i++){
    SchemaEntry *fromEntry = findSchemaEntry(aFrom, nFrom, aTo[i].zName);
    SchemaDiffRow *r;

    if( !fromEntry ){
      /* Added */
      if( pCur->nRows >= pCur->nAlloc ){
        int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 16;
        SchemaDiffRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(SchemaDiffRow));
        if( !aNew ) return SQLITE_NOMEM;
        pCur->aRows = aNew; pCur->nAlloc = nNew;
      }
      r = &pCur->aRows[pCur->nRows++];
      memset(r, 0, sizeof(*r));
      r->zName = sqlite3_mprintf("%s", aTo[i].zName);
      r->zFromSql = 0;
      r->zToSql = sqlite3_mprintf("%s", aTo[i].zSql ? aTo[i].zSql : "");
      r->zDiffType = "added";
    }else if( fromEntry->zSql && aTo[i].zSql
           && strcmp(fromEntry->zSql, aTo[i].zSql)!=0 ){
      /* Modified */
      if( pCur->nRows >= pCur->nAlloc ){
        int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 16;
        SchemaDiffRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(SchemaDiffRow));
        if( !aNew ) return SQLITE_NOMEM;
        pCur->aRows = aNew; pCur->nAlloc = nNew;
      }
      r = &pCur->aRows[pCur->nRows++];
      memset(r, 0, sizeof(*r));
      r->zName = sqlite3_mprintf("%s", aTo[i].zName);
      r->zFromSql = sqlite3_mprintf("%s", fromEntry->zSql);
      r->zToSql = sqlite3_mprintf("%s", aTo[i].zSql);
      r->zDiffType = "modified";
    }
  }

  /* Check each "from" entry: dropped if not in "to" */
  for(i=0; i<nFrom; i++){
    SchemaEntry *toEntry = findSchemaEntry(aTo, nTo, aFrom[i].zName);
    if( !toEntry ){
      SchemaDiffRow *r;
      if( pCur->nRows >= pCur->nAlloc ){
        int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 16;
        SchemaDiffRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(SchemaDiffRow));
        if( !aNew ) return SQLITE_NOMEM;
        pCur->aRows = aNew; pCur->nAlloc = nNew;
      }
      r = &pCur->aRows[pCur->nRows++];
      memset(r, 0, sizeof(*r));
      r->zName = sqlite3_mprintf("%s", aFrom[i].zName);
      r->zFromSql = sqlite3_mprintf("%s", aFrom[i].zSql ? aFrom[i].zSql : "");
      r->zToSql = 0;
      r->zDiffType = "dropped";
    }
  }

  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static const char *sdSchema =
  "CREATE TABLE x("
  "  table_name TEXT,"
  "  from_create_stmt TEXT,"
  "  to_create_stmt TEXT,"
  "  diff_type TEXT,"
  "  from_ref TEXT HIDDEN,"
  "  to_ref TEXT HIDDEN"
  ")";

static int sdConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  SdVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, sdSchema);
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int sdDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }

static int sdBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iFrom = -1, iTo = -1;
  int i, argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case 4: iFrom = i; break;
      case 5: iTo = i; break;
    }
  }

  if( iFrom>=0 ){
    pInfo->aConstraintUsage[iFrom].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFrom].omit = 1;
  }
  if( iTo>=0 ){
    pInfo->aConstraintUsage[iTo].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTo].omit = 1;
  }

  pInfo->idxNum = (iFrom>=0 ? 1 : 0) | (iTo>=0 ? 2 : 0);
  pInfo->estimatedCost = 1000.0;
  return SQLITE_OK;
}

static int sdOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  SdCursor *c; (void)v;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int sdClose(sqlite3_vtab_cursor *cur){
  SdCursor *c = (SdCursor*)cur;
  freeSchemaDiffRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int sdFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  SdCursor *c = (SdCursor*)cur;
  SdVtab *v = (SdVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt;
  ProllyCache *pCache;
  const char *zFromRef = 0, *zToRef = 0;
  ProllyHash fromCommit, toCommit;
  DoltliteCommit commit;
  ProllyHash fromCatHash, toCatHash;
  SchemaEntry *aFrom = 0, *aTo = 0;
  int nFrom = 0, nTo = 0;
  u8 *data = 0; int nData = 0;
  int rc, argIdx = 0;
  (void)idxStr;

  freeSchemaDiffRows(c);
  c->iRow = 0;

  if( !cs ) return SQLITE_OK;
  pBt = doltliteGetBtShared(db);
  if( !pBt ) return SQLITE_OK;
  pCache = (ProllyCache*)(((char*)pBt) + sizeof(ChunkStore));

  if( (idxNum & 1) && argIdx<argc ){
    zFromRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 2) && argIdx<argc ){
    zToRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }

  /* Single argument that's not a valid ref = table name filter.
  ** In this mode, diff HEAD vs working for that specific table. */
  {
    const char *zTableFilter = 0;
    if( zFromRef && !zToRef ){
      ProllyHash testHash;
      int resolved = sdResolveRef(cs, zFromRef, &testHash);
      if( resolved!=SQLITE_OK ){
        /* Not a valid commit/branch/tag — treat as table name */
        zTableFilter = zFromRef;
        zFromRef = 0;
      }
    }

    /* Resolve "from" ref → catalog hash */
    if( zFromRef ){
    rc = sdResolveRef(cs, zFromRef, &fromCommit);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    memset(&commit, 0, sizeof(commit));
    rc = chunkStoreGet(cs, &fromCommit, &data, &nData);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data); data = 0;
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    memcpy(&fromCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);
  }else{
    /* Default: HEAD */
    rc = doltliteGetHeadCatalogHash(db, &fromCatHash);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
  }

  /* Resolve "to" ref → catalog hash */
  if( zToRef ){
    rc = sdResolveRef(cs, zToRef, &toCommit);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    memset(&commit, 0, sizeof(commit));
    rc = chunkStoreGet(cs, &toCommit, &data, &nData);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data); data = 0;
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    memcpy(&toCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);
  }else{
    /* Default: working state */
    u8 *catData = 0; int nCatData = 0;
    rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
    if( rc==SQLITE_OK ){
      rc = chunkStorePut(cs, catData, nCatData, &toCatHash);
      sqlite3_free(catData);
    }
    if( rc!=SQLITE_OK ) return SQLITE_OK;
  }

  /* Load schema at both points */
  loadSchemaFromCatalog(db, cs, pCache, &fromCatHash, &aFrom, &nFrom);
  loadSchemaFromCatalog(db, cs, pCache, &toCatHash, &aTo, &nTo);

  /* Compute diff */
  computeSchemaDiff(c, aFrom, nFrom, aTo, nTo);

  /* If filtering by table name, remove rows that don't match */
  if( zTableFilter ){
    int j, k=0;
    for(j=0; j<c->nRows; j++){
      if( c->aRows[j].zName && strcmp(c->aRows[j].zName, zTableFilter)==0 ){
        if( k!=j ) c->aRows[k] = c->aRows[j];
        k++;
      }else{
        sqlite3_free(c->aRows[j].zName);
        sqlite3_free(c->aRows[j].zFromSql);
        sqlite3_free(c->aRows[j].zToSql);
      }
    }
    c->nRows = k;
  }

  } /* end of zTableFilter scope */

  freeSchemaEntries(aFrom, nFrom);
  freeSchemaEntries(aTo, nTo);
  return SQLITE_OK;
}

static int sdNext(sqlite3_vtab_cursor *cur){ ((SdCursor*)cur)->iRow++; return SQLITE_OK; }
static int sdEof(sqlite3_vtab_cursor *cur){ return ((SdCursor*)cur)->iRow >= ((SdCursor*)cur)->nRows; }

static int sdColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  SdCursor *c = (SdCursor*)cur;
  SchemaDiffRow *r = &c->aRows[c->iRow];
  switch( col ){
    case 0: sqlite3_result_text(ctx, r->zName, -1, SQLITE_TRANSIENT); break;
    case 1:
      if(r->zFromSql) sqlite3_result_text(ctx, r->zFromSql, -1, SQLITE_TRANSIENT);
      else sqlite3_result_null(ctx);
      break;
    case 2:
      if(r->zToSql) sqlite3_result_text(ctx, r->zToSql, -1, SQLITE_TRANSIENT);
      else sqlite3_result_null(ctx);
      break;
    case 3: sqlite3_result_text(ctx, r->zDiffType, -1, SQLITE_STATIC); break;
  }
  return SQLITE_OK;
}

static int sdRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((SdCursor*)cur)->iRow; return SQLITE_OK;
}

static sqlite3_module schemaDiffModule = {
  0, 0, sdConnect, sdBestIndex, sdDisconnect, 0,
  sdOpen, sdClose, sdFilter, sdNext, sdEof,
  sdColumn, sdRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteSchemaDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_schema_diff", &schemaDiffModule, 0);
}

#endif /* DOLTLITE_PROLLY */
