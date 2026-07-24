
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "doltlite_name_index.h"
#include <stddef.h>
#include <string.h>

typedef struct SchemaDiffRow SchemaDiffRow;
struct SchemaDiffRow {
  char *zFromName;
  char *zToName;
  char *zFromSql;
  char *zToSql;
};

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
    sqlite3_free(c->aRows[i].zFromName);
    sqlite3_free(c->aRows[i].zToName);
    sqlite3_free(c->aRows[i].zFromSql);
    sqlite3_free(c->aRows[i].zToSql);
  }
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
  c->nAlloc = 0;
}

static int appendSchemaEntry(
  SchemaEntry **paEntries,
  int *pnEntries,
  int *pnAlloc,
  char *zName,
  char *zTblName,
  char *zSql,
  char *zType,
  Pgno iRootpage
){
  SchemaEntry *aEntries;
  int nEntries = *pnEntries;
  int rc = DOLTLITE_GROW_ARRAY(paEntries, pnAlloc, nEntries+1, 16);
  if( rc!=SQLITE_OK ) return rc;
  aEntries = *paEntries;
  aEntries[nEntries].zName = zName;
  aEntries[nEntries].zTblName = zTblName;
  aEntries[nEntries].zSql = zSql;
  aEntries[nEntries].zType = zType;
  aEntries[nEntries].iRootpage = iRootpage;
  *pnEntries = nEntries + 1;
  return SQLITE_OK;
}

static int appendSchemaDiffRow(
  SdCursor *pCur,
  const char *zFromName,
  const char *zToName,
  const char *zFromSql,
  const char *zToSql
){
  SchemaDiffRow *r;
  int rc = DOLTLITE_GROW_ARRAY(&pCur->aRows, &pCur->nAlloc, pCur->nRows+1, 16);
  if( rc!=SQLITE_OK ) return rc;
  r = &pCur->aRows[pCur->nRows];
  memset(r, 0, sizeof(*r));
  r->zFromName = sqlite3_mprintf("%s", zFromName ? zFromName : "");
  r->zToName   = sqlite3_mprintf("%s", zToName   ? zToName   : "");
  r->zFromSql  = sqlite3_mprintf("%s", zFromSql  ? zFromSql  : "");
  r->zToSql    = sqlite3_mprintf("%s", zToSql    ? zToSql    : "");
  if( !r->zFromName || !r->zToName || !r->zFromSql || !r->zToSql ){
    sqlite3_free(r->zFromName);
    sqlite3_free(r->zToName);
    sqlite3_free(r->zFromSql);
    sqlite3_free(r->zToSql);
    memset(r, 0, sizeof(*r));
    return SQLITE_NOMEM;
  }
  pCur->nRows++;
  return SQLITE_OK;
}

int loadSchemaFromCatalog(
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

  memset(&masterRoot, 0, sizeof(masterRoot));
  for(i=0; i<nTables; i++){
    if( aTables[i].iTable==1 ){
      memcpy(&masterRoot, &aTables[i].root, sizeof(ProllyHash));
      masterFlags = aTables[i].flags;
      break;
    }
  }
  doltliteFreeCatalog(aTables, nTables);

  if( prollyHashIsEmpty(&masterRoot) ){
    *ppEntries = 0; *pnEntries = 0;
    return SQLITE_OK;
  }

  prollyCursorInit(&cur, cs, pCache, &masterRoot, masterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){ prollyCursorClose(&cur); *ppEntries = 0; *pnEntries = 0; return rc; }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal;
    int nVal;
    DoltliteRecordInfo ri;

    prollyCursorValue(&cur, &pVal, &nVal);

    if( pVal && nVal > 0 ){
      doltliteParseRecord(pVal, nVal, &ri);

      if( ri.nField < 5 ){
        rc = SQLITE_CORRUPT;
        goto load_schema_done;
      }else{
        char *zType = 0, *zName = 0, *zTblName = 0, *zSql = 0;
        i64 iRootpage = 0;

        rc = dlRecordTextField(pVal, nVal, &ri, 0, &zType);
        if( rc!=SQLITE_OK ) goto load_schema_done;
        rc = dlRecordTextField(pVal, nVal, &ri, 1, &zName);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zType);
          goto load_schema_done;
        }
        rc = dlRecordTextField(pVal, nVal, &ri, 2, &zTblName);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zType);
          sqlite3_free(zName);
          goto load_schema_done;
        }
        iRootpage = dlRecordIntField(pVal, nVal, &ri, 3);
        rc = dlRecordTextField(pVal, nVal, &ri, 4, &zSql);
        if( rc!=SQLITE_OK ){
          sqlite3_free(zType);
          sqlite3_free(zName);
          sqlite3_free(zTblName);
          goto load_schema_done;
        }

        if( zName ){
          rc = appendSchemaEntry(&aEntries, &nEntries, &nAlloc,
                                 zName, zTblName, zSql, zType,
                                 (Pgno)iRootpage);
          if( rc!=SQLITE_OK ) goto load_schema_done;
          zName = 0;
          zTblName = 0;
          zSql = 0;
          zType = 0;
        }
        sqlite3_free(zType);
        sqlite3_free(zName);
        sqlite3_free(zTblName);
        sqlite3_free(zSql);
      }
    }

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }

load_schema_done:
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK ){
    freeSchemaEntries(aEntries, nEntries);
    *ppEntries = 0;
    *pnEntries = 0;
    return rc;
  }
  *ppEntries = aEntries;
  *pnEntries = nEntries;
  return rc;
}

int loadSchemaEntryFromCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pCatHash,
  const char *zName,
  SchemaEntry *pEntry,
  int *pFound
){
  ProllyHash masterRoot;
  u8 masterFlags = 0;
  ProllyCursor cur;
  int res, rc;

  memset(pEntry, 0, sizeof(*pEntry));
  *pFound = 0;
  if( !zName || prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;

  rc = doltliteLoadTableRootById(db, pCatHash, 1, &masterRoot, &masterFlags, 0);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&masterRoot) ) return SQLITE_OK;

  prollyCursorInit(&cur, cs, pCache, &masterRoot, masterFlags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc;
  }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    DoltliteRecordInfo ri;

    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && nVal > 0 ){
      char *zType = 0, *zEntryName = 0, *zTblName = 0, *zSql = 0;
      i64 iRootpage = 0;

      doltliteParseRecord(pVal, nVal, &ri);
      if( ri.nField < 5 ){
        rc = SQLITE_CORRUPT;
        goto load_schema_entry_done;
      }

      rc = dlRecordTextField(pVal, nVal, &ri, 1, &zEntryName);
      if( rc!=SQLITE_OK ) goto load_schema_entry_done;
      if( zEntryName && strcmp(zEntryName, zName)==0 ){
        rc = dlRecordTextField(pVal, nVal, &ri, 0, &zType);
        if( rc==SQLITE_OK ){
          rc = dlRecordTextField(pVal, nVal, &ri, 2, &zTblName);
        }
        if( rc==SQLITE_OK ){
          iRootpage = dlRecordIntField(pVal, nVal, &ri, 3);
          rc = dlRecordTextField(pVal, nVal, &ri, 4, &zSql);
        }
        if( rc!=SQLITE_OK ){
          sqlite3_free(zType);
          sqlite3_free(zEntryName);
          sqlite3_free(zTblName);
          sqlite3_free(zSql);
          goto load_schema_entry_done;
        }
        pEntry->zName = zEntryName;
        pEntry->zTblName = zTblName;
        pEntry->zSql = zSql;
        pEntry->zType = zType;
        pEntry->iRootpage = (Pgno)iRootpage;
        *pFound = 1;
        break;
      }
      sqlite3_free(zEntryName);
    }

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }

load_schema_entry_done:
  prollyCursorClose(&cur);
  if( rc!=SQLITE_OK ){
    clearSchemaEntry(pEntry);
    *pFound = 0;
  }
  return rc;
}

void clearSchemaEntry(SchemaEntry *pEntry){
  sqlite3_free(pEntry->zName);
  sqlite3_free(pEntry->zTblName);
  sqlite3_free(pEntry->zSql);
  sqlite3_free(pEntry->zType);
  memset(pEntry, 0, sizeof(*pEntry));
}

void freeSchemaEntries(SchemaEntry *a, int n){
  int i;
  for(i=0; i<n; i++){
    clearSchemaEntry(&a[i]);
  }
  sqlite3_free(a);
}

SchemaEntry *findSchemaEntry(SchemaEntry *a, int n, const char *zName){
  int i;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ) return &a[i];
  }
  return 0;
}

typedef DoltliteNameIndex SdSchemaIndex;
typedef DoltliteNameIndex SdTableIndex;

static void sdSchemaIndexClear(SdSchemaIndex *pIdx){
  doltliteNameIndexFree(pIdx);
}

static int sdSchemaIndexInit(
  SdSchemaIndex *pIdx,
  SchemaEntry *aSchema,
  int nSchema
){
  return doltliteNameIndexInit(pIdx, aSchema, nSchema,
                               (int)sizeof(SchemaEntry),
                               (int)offsetof(SchemaEntry, zName));
}

static SchemaEntry *sdSchemaIndexFind(
  const SdSchemaIndex *pIdx,
  const char *zName
){
  int r = doltliteNameIndexFind(pIdx, zName);
  return r<0 ? 0 : (SchemaEntry*)(pIdx->aBase + (size_t)r*pIdx->stride);
}

static void sdTableIndexClear(SdTableIndex *pIdx){
  doltliteNameIndexFree(pIdx);
}

static int sdTableIndexInit(
  SdTableIndex *pIdx,
  struct TableEntry *aTable,
  int nTable
){
  return doltliteNameIndexInit(pIdx, aTable, nTable,
                               (int)sizeof(struct TableEntry),
                               (int)offsetof(struct TableEntry, zName));
}

static struct TableEntry *sdTableIndexFind(
  const SdTableIndex *pIdx,
  const char *zName
){
  int r = doltliteNameIndexFind(pIdx, zName);
  return r<0 ? 0 : (struct TableEntry*)(pIdx->aBase + (size_t)r*pIdx->stride);
}

static int computeSchemaDiff(
  SdCursor *pCur,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo,
  struct TableEntry *aFromTables, int nFromTables,
  struct TableEntry *aToTables, int nToTables
){
  int i;
  u8 *fromConsumed = 0, *toConsumed = 0;
  SdSchemaIndex fromSchemaIdx, toSchemaIdx;
  SdTableIndex fromTableIdx, toTableIdx;
  int rc = SQLITE_OK;

  memset(&fromSchemaIdx, 0, sizeof(fromSchemaIdx));
  memset(&toSchemaIdx, 0, sizeof(toSchemaIdx));
  memset(&fromTableIdx, 0, sizeof(fromTableIdx));
  memset(&toTableIdx, 0, sizeof(toTableIdx));

  if( nFrom > 0 ){
    fromConsumed = sqlite3_malloc(nFrom);
    if( !fromConsumed ) return SQLITE_NOMEM;
    memset(fromConsumed, 0, nFrom);
  }
  if( nTo > 0 ){
    toConsumed = sqlite3_malloc(nTo);
    if( !toConsumed ){
      sqlite3_free(fromConsumed);
      return SQLITE_NOMEM;
    }
    memset(toConsumed, 0, nTo);
  }
  rc = sdSchemaIndexInit(&fromSchemaIdx, aFrom, nFrom);
  if( rc!=SQLITE_OK ) goto done;
  rc = sdSchemaIndexInit(&toSchemaIdx, aTo, nTo);
  if( rc!=SQLITE_OK ) goto done;
  rc = sdTableIndexInit(&fromTableIdx, aFromTables, nFromTables);
  if( rc!=SQLITE_OK ) goto done;
  rc = sdTableIndexInit(&toTableIdx, aToTables, nToTables);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<nTo; i++){
    SchemaEntry *fromEntry;
    struct TableEntry *toTE;
    int j;

    fromEntry = sdSchemaIndexFind(&fromSchemaIdx, aTo[i].zName);
    if( fromEntry ) continue;

    toTE = sdTableIndexFind(&toTableIdx, aTo[i].zName);
    if( !toTE || toTE->iTable==0 ) continue;

    for(j=0; j<nFromTables; j++){
      SchemaEntry *dropped;
      int k;
      if( aFromTables[j].iTable != toTE->iTable ) continue;
      if( !aFromTables[j].zName ) continue;
      if( prollyHashCompare(&aFromTables[j].root, &toTE->root)!=0 ) break;

      if( sdTableIndexFind(&toTableIdx, aFromTables[j].zName) ) break;

      dropped = sdSchemaIndexFind(&fromSchemaIdx, aFromTables[j].zName);
      if( !dropped ) break;

      rc = appendSchemaDiffRow(pCur, dropped->zName, aTo[i].zName,
                               dropped->zSql, aTo[i].zSql);
      if( rc!=SQLITE_OK ) goto done;

      toConsumed[i] = 1;
      for(k=0; k<nFrom; k++){
        if( &aFrom[k] == dropped ){ fromConsumed[k] = 1; break; }
      }
      break;
    }
  }

  for(i=0; i<nTo; i++){
    SchemaEntry *fromEntry;
    if( toConsumed && toConsumed[i] ) continue;
    fromEntry = sdSchemaIndexFind(&fromSchemaIdx, aTo[i].zName);

    if( !fromEntry ){

      rc = appendSchemaDiffRow(pCur, "", aTo[i].zName,
                               "", aTo[i].zSql);
      if( rc!=SQLITE_OK ) goto done;
    }else if( fromEntry->zSql && aTo[i].zSql
           && strcmp(fromEntry->zSql, aTo[i].zSql)!=0 ){

      rc = appendSchemaDiffRow(pCur, aTo[i].zName, aTo[i].zName,
                               fromEntry->zSql, aTo[i].zSql);
      if( rc!=SQLITE_OK ) goto done;
    }
  }

  for(i=0; i<nFrom; i++){
    SchemaEntry *toEntry;
    if( fromConsumed && fromConsumed[i] ) continue;
    toEntry = sdSchemaIndexFind(&toSchemaIdx, aFrom[i].zName);
    if( !toEntry ){

      rc = appendSchemaDiffRow(pCur, aFrom[i].zName, "",
                               aFrom[i].zSql, "");
      if( rc!=SQLITE_OK ) goto done;
    }
  }

done:
  sdSchemaIndexClear(&fromSchemaIdx);
  sdSchemaIndexClear(&toSchemaIdx);
  sdTableIndexClear(&fromTableIdx);
  sdTableIndexClear(&toTableIdx);
  sqlite3_free(fromConsumed);
  sqlite3_free(toConsumed);
  return rc;
}

static int sdAppendRenameIfMatches(
  SdCursor *pCur,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo,
  struct TableEntry *aFromTables, int nFromTables,
  struct TableEntry *aToTables, int nToTables,
  const char *zFilter,
  int *pRenamed
){
  struct TableEntry *pTo;
  struct TableEntry *pFrom;
  SchemaEntry *pDropped;
  SchemaEntry *pAdded;
  int rc;

  *pRenamed = 0;
  if( !zFilter ) return SQLITE_OK;

  pTo = doltliteFindTableByName(aToTables, nToTables, zFilter);
  if( pTo && pTo->iTable!=0 ){
    pFrom = doltliteFindTableByNumber(aFromTables, nFromTables, pTo->iTable);
    if( pFrom && pFrom->zName
     && strcmp(pFrom->zName, zFilter)!=0
     && prollyHashCompare(&pFrom->root, &pTo->root)==0
     && !doltliteFindTableByName(aToTables, nToTables, pFrom->zName) ){
      pDropped = findSchemaEntry(aFrom, nFrom, pFrom->zName);
      pAdded = findSchemaEntry(aTo, nTo, zFilter);
      if( pDropped && pAdded && !findSchemaEntry(aFrom, nFrom, zFilter) ){
        rc = appendSchemaDiffRow(pCur, pDropped->zName, pAdded->zName,
                                 pDropped->zSql, pAdded->zSql);
        if( rc!=SQLITE_OK ) return rc;
        *pRenamed = 1;
        return SQLITE_OK;
      }
    }
  }

  pFrom = doltliteFindTableByName(aFromTables, nFromTables, zFilter);
  if( pFrom && pFrom->iTable!=0 ){
    pTo = doltliteFindTableByNumber(aToTables, nToTables, pFrom->iTable);
    if( pTo && pTo->zName
     && strcmp(pTo->zName, zFilter)!=0
     && prollyHashCompare(&pFrom->root, &pTo->root)==0
     && !doltliteFindTableByName(aToTables, nToTables, zFilter) ){
      pDropped = findSchemaEntry(aFrom, nFrom, zFilter);
      pAdded = findSchemaEntry(aTo, nTo, pTo->zName);
      if( pDropped && pAdded && !findSchemaEntry(aFrom, nFrom, pTo->zName) ){
        rc = appendSchemaDiffRow(pCur, pDropped->zName, pAdded->zName,
                                 pDropped->zSql, pAdded->zSql);
        if( rc!=SQLITE_OK ) return rc;
        *pRenamed = 1;
      }
    }
  }

  return SQLITE_OK;
}

static int computeSchemaDiffFiltered(
  SdCursor *pCur,
  SchemaEntry *aFrom, int nFrom,
  SchemaEntry *aTo, int nTo,
  struct TableEntry *aFromTables, int nFromTables,
  struct TableEntry *aToTables, int nToTables,
  const char *zFilter
){
  SchemaEntry *pFrom;
  SchemaEntry *pTo;
  int renamed = 0;
  int rc;

  if( !zFilter ){
    return computeSchemaDiff(pCur, aFrom, nFrom, aTo, nTo,
                             aFromTables, nFromTables,
                             aToTables, nToTables);
  }

  rc = sdAppendRenameIfMatches(pCur, aFrom, nFrom, aTo, nTo,
                               aFromTables, nFromTables,
                               aToTables, nToTables, zFilter, &renamed);
  if( rc!=SQLITE_OK ) return rc;

  pFrom = findSchemaEntry(aFrom, nFrom, zFilter);
  pTo = findSchemaEntry(aTo, nTo, zFilter);

  if( pTo ){
    if( !pFrom ){
      if( !renamed ){
        return appendSchemaDiffRow(pCur, "", pTo->zName, "", pTo->zSql);
      }
    }else if( pFrom->zSql && pTo->zSql && strcmp(pFrom->zSql, pTo->zSql)!=0 ){
      return appendSchemaDiffRow(pCur, pTo->zName, pTo->zName,
                                 pFrom->zSql, pTo->zSql);
    }
  }else if( pFrom && !renamed ){
    return appendSchemaDiffRow(pCur, pFrom->zName, "", pFrom->zSql, "");
  }

  return SQLITE_OK;
}

static int sdAppendSchemaEntryByName(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pCatHash,
  const char *zName,
  SchemaEntry **paEntry,
  int *pnEntry
){
  SchemaEntry entry;
  SchemaEntry *aNew;
  int found = 0;
  int i, rc;

  if( !zName ) return SQLITE_OK;
  for(i=0; i<*pnEntry; i++){
    if( (*paEntry)[i].zName && strcmp((*paEntry)[i].zName, zName)==0 ){
      return SQLITE_OK;
    }
  }

  rc = loadSchemaEntryFromCatalog(db, cs, pCache, pCatHash, zName,
                                  &entry, &found);
  if( rc!=SQLITE_OK ) return rc;
  if( !found ) return SQLITE_OK;

  aNew = sqlite3_realloc(*paEntry,
                         (*pnEntry + 1) * (int)sizeof(SchemaEntry));
  if( !aNew ){
    clearSchemaEntry(&entry);
    return SQLITE_NOMEM;
  }
  *paEntry = aNew;
  (*paEntry)[*pnEntry] = entry;
  (*pnEntry)++;
  return SQLITE_OK;
}

static int sdLoadFilteredSchemas(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pFromCatHash,
  const ProllyHash *pToCatHash,
  struct TableEntry *aFromTables,
  int nFromTables,
  struct TableEntry *aToTables,
  int nToTables,
  const char *zFilter,
  SchemaEntry **paFrom,
  int *pnFrom,
  SchemaEntry **paTo,
  int *pnTo
){
  struct TableEntry *pFrom;
  struct TableEntry *pTo;
  int rc;

  *paFrom = 0;
  *pnFrom = 0;
  *paTo = 0;
  *pnTo = 0;

  rc = sdAppendSchemaEntryByName(db, cs, pCache, pFromCatHash, zFilter,
                                 paFrom, pnFrom);
  if( rc!=SQLITE_OK ) return rc;
  rc = sdAppendSchemaEntryByName(db, cs, pCache, pToCatHash, zFilter,
                                 paTo, pnTo);
  if( rc!=SQLITE_OK ) return rc;

  pTo = doltliteFindTableByName(aToTables, nToTables, zFilter);
  if( pTo && pTo->iTable!=0 ){
    pFrom = doltliteFindTableByNumber(aFromTables, nFromTables, pTo->iTable);
    if( pFrom && pFrom->zName && strcmp(pFrom->zName, zFilter)!=0 ){
      rc = sdAppendSchemaEntryByName(db, cs, pCache, pFromCatHash,
                                     pFrom->zName, paFrom, pnFrom);
      if( rc!=SQLITE_OK ) return rc;
      rc = sdAppendSchemaEntryByName(db, cs, pCache, pToCatHash,
                                     pFrom->zName, paTo, pnTo);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  pFrom = doltliteFindTableByName(aFromTables, nFromTables, zFilter);
  if( pFrom && pFrom->iTable!=0 ){
    pTo = doltliteFindTableByNumber(aToTables, nToTables, pFrom->iTable);
    if( pTo && pTo->zName && strcmp(pTo->zName, zFilter)!=0 ){
      rc = sdAppendSchemaEntryByName(db, cs, pCache, pFromCatHash,
                                     pTo->zName, paFrom, pnFrom);
      if( rc!=SQLITE_OK ) return rc;
      rc = sdAppendSchemaEntryByName(db, cs, pCache, pToCatHash,
                                     pTo->zName, paTo, pnTo);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

static const char *sdSchema =
  "CREATE TABLE x("
  "  from_table_name TEXT,"
  "  to_table_name TEXT,"
  "  from_create_statement TEXT,"
  "  to_create_statement TEXT,"
  "  from_ref TEXT HIDDEN,"
  "  to_ref TEXT HIDDEN,"
  "  table_name TEXT HIDDEN"
  ")";

static int sdConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  SdVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = doltliteVtabConnectSimple(db, sdSchema, sizeof(*v), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  v = (SdVtab*)*ppVtab;
  v->db = db;
  return SQLITE_OK;
}

static int sdBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  return doltliteBestIndexRefs(pInfo, 4, 5, 6);
}

static int sdOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  (void)v;
  return doltliteVtabOpenCursor(pp, sizeof(SdCursor));
}

static int sdClose(sqlite3_vtab_cursor *cur){
  SdCursor *c = (SdCursor*)cur;
  freeSchemaDiffRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int sdResolveOne(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  const char *zRef,
  const char *zWhich,
  ProllyHash *pCatHash
){
  int rc = doltliteResolveCatalogHashForRef(db, zRef, pCatHash);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf(
      "dolt_schema_diff: %s '%s' could not be resolved", zWhich, zRef);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int sdResolveRefs(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  const char *zFromRef,
  const char *zToRef,
  ProllyHash *pFromCatHash,
  ProllyHash *pToCatHash
){
  int rc;

  if( !zFromRef || !zToRef ){
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf(
      "dolt_schema_diff requires from_ref and to_ref"
    );
    return SQLITE_ERROR;
  }

  rc = sdResolveOne(db, pVtab, zFromRef, "from_ref", pFromCatHash);
  if( rc!=SQLITE_OK ) return rc;
  return sdResolveOne(db, pVtab, zToRef, "to_ref", pToCatHash);
}

static int sdParseArgs(
  sqlite3 *db,
  sqlite3_vtab *pVtab,
  int idxNum,
  int argc,
  sqlite3_value **argv,
  const char **pzFromRef,
  const char **pzToRef,
  const char **pzTableFilter
){
  int argIdx = 0;
  const char *zFromRef = 0;
  const char *zToRef = 0;
  const char *zTableFilter = 0;

  if( (idxNum & 1) && argIdx<argc ){
    zFromRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 2) && argIdx<argc ){
    zToRef = (const char*)sqlite3_value_text(argv[argIdx++]);
  }
  if( (idxNum & 4) && argIdx<argc ){
    zTableFilter = (const char*)sqlite3_value_text(argv[argIdx++]);
  }

  if( zFromRef && !zToRef ){
    const char *zDots = strstr(zFromRef, "..");
    if( zDots ){
      int nFrom = (int)(zDots - zFromRef);
      int nTo = (int)strlen(zDots + 2);
      char *zRangeFrom = 0;
      char *zRangeTo = 0;
      ProllyHash probe;
      int rc;

      if( nFrom<=0 || nTo<=0 ){
        sqlite3_free(pVtab->zErrMsg);
        pVtab->zErrMsg = sqlite3_mprintf(
          "Invalid argument to dolt_schema_diff: %s",
          zFromRef
        );
        return SQLITE_ERROR;
      }

      zRangeFrom = sqlite3_mprintf("%.*s", nFrom, zFromRef);
      zRangeTo = sqlite3_mprintf("%s", zDots + 2);
      if( !zRangeFrom || !zRangeTo ){
        sqlite3_free(zRangeFrom);
        sqlite3_free(zRangeTo);
        return SQLITE_NOMEM;
      }

      rc = doltliteResolveCatalogHashForRef(db, zRangeFrom, &probe);
      if( rc==SQLITE_OK ) rc = doltliteResolveCatalogHashForRef(db, zRangeTo, &probe);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zRangeFrom);
        sqlite3_free(zRangeTo);
        return rc;
      }

      zFromRef = zRangeFrom;
      zToRef = zRangeTo;
    }else{
      sqlite3_free(pVtab->zErrMsg);
      pVtab->zErrMsg = sqlite3_mprintf(
        "Invalid argument to dolt_schema_diff: There are less than 2 arguments present, and the first does not contain '..'"
      );
      return SQLITE_ERROR;
    }
  }

  *pzFromRef = zFromRef;
  *pzToRef = zToRef;
  *pzTableFilter = zTableFilter;
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
  const char *zTableFilter = 0;
  ProllyHash fromCatHash, toCatHash;
  SchemaEntry *aFrom = 0, *aTo = 0;
  int nFrom = 0, nTo = 0;
  struct TableEntry *aFromTables = 0, *aToTables = 0;
  int nFromTables = 0, nToTables = 0, freeRangeRefs = 0;
  int rc;
  (void)idxStr;

  freeSchemaDiffRows(c);
  c->iRow = 0;

  if( !cs ) return SQLITE_OK;
  pBt = doltliteGetBtShared(db);
  if( !pBt ) return SQLITE_OK;
  pCache = doltliteGetCache(db);

  rc = sdParseArgs(db, &v->base, idxNum, argc, argv,
                   &zFromRef, &zToRef, &zTableFilter);
  if( rc!=SQLITE_OK ) return rc;
  freeRangeRefs = (zFromRef && zToRef && !(idxNum & 2));

  rc = sdResolveRefs(db, &v->base, zFromRef, zToRef, &fromCatHash, &toCatHash);
  if( rc!=SQLITE_OK ) goto sd_filter_done;

  rc = doltliteLoadCatalog(db, &fromCatHash, &aFromTables, &nFromTables, 0);
  if( rc!=SQLITE_OK ) goto sd_filter_done;
  rc = doltliteLoadCatalog(db, &toCatHash, &aToTables, &nToTables, 0);
  if( rc!=SQLITE_OK ) goto sd_filter_done;

  if( zTableFilter ){
    rc = sdLoadFilteredSchemas(db, cs, pCache, &fromCatHash, &toCatHash,
                               aFromTables, nFromTables, aToTables, nToTables,
                               zTableFilter, &aFrom, &nFrom, &aTo, &nTo);
  }else{
    rc = loadSchemaFromCatalog(db, cs, pCache, &fromCatHash, &aFrom, &nFrom);
    if( rc==SQLITE_OK ){
      rc = loadSchemaFromCatalog(db, cs, pCache, &toCatHash, &aTo, &nTo);
    }
  }
  if( rc!=SQLITE_OK ) goto sd_filter_done;

  rc = computeSchemaDiffFiltered(c, aFrom, nFrom, aTo, nTo,
                                 aFromTables, nFromTables,
                                 aToTables, nToTables,
                                 zTableFilter);
  if( rc!=SQLITE_OK ) goto sd_filter_done;

sd_filter_done:
  freeSchemaEntries(aFrom, nFrom);
  freeSchemaEntries(aTo, nTo);
  doltliteFreeCatalog(aFromTables, nFromTables);
  doltliteFreeCatalog(aToTables, nToTables);
  if( freeRangeRefs ){
    sqlite3_free((char*)zFromRef);
    sqlite3_free((char*)zToRef);
  }
  return rc;
}

static int sdNext(sqlite3_vtab_cursor *cur){ ((SdCursor*)cur)->iRow++; return SQLITE_OK; }
static int sdEof(sqlite3_vtab_cursor *cur){ return ((SdCursor*)cur)->iRow >= ((SdCursor*)cur)->nRows; }

static int sdColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  SdCursor *c = (SdCursor*)cur;
  SchemaDiffRow *r = &c->aRows[c->iRow];

  switch( col ){
    case 0: sqlite3_result_text(ctx, r->zFromName, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, r->zToName,   -1, SQLITE_TRANSIENT); break;
    case 2: sqlite3_result_text(ctx, r->zFromSql,  -1, SQLITE_TRANSIENT); break;
    case 3: sqlite3_result_text(ctx, r->zToSql,    -1, SQLITE_TRANSIENT); break;
  }
  return SQLITE_OK;
}

static int sdRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((SdCursor*)cur)->iRow; return SQLITE_OK;
}

static sqlite3_module schemaDiffModule = {
  0, 0, sdConnect, sdBestIndex, doltliteVtabDisconnect, 0,
  sdOpen, sdClose, sdFilter, sdNext, sdEof,
  sdColumn, sdRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteSchemaDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_schema_diff", &schemaDiffModule, 0);
}

#endif
