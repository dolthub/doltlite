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

static void addFreeEntries(
  struct TableEntry *aWorking, int nWorking,
  struct TableEntry *aStaged,  int nStaged,
  struct TableEntry *aNew,     int nNew
){
  doltliteFreeCatalog(aWorking, nWorking);
  doltliteFreeCatalog(aStaged, nStaged);
  doltliteFreeCatalog(aNew, nNew);
}

static void addResultIgnoreConflict(sqlite3_context *context, char *zIgnErr){
  if( zIgnErr ){
    sqlite3_result_error(context, zIgnErr, -1);
    sqlite3_free(zIgnErr);
  }else{
    sqlite3_result_error(context, "dolt_ignore conflict", -1);
  }
}

static int addCheckIgnore(
  sqlite3 *db,
  sqlite3_context *context,
  const char *zName,
  int *pIgnored
){
  char *zIgnErr = 0;
  int rc;
  *pIgnored = 0;
  rc = doltliteCheckIgnore(db, zName, pIgnored, &zIgnErr);
  if( rc==SQLITE_CONSTRAINT ){
    addResultIgnoreConflict(context, zIgnErr);
  }else if( rc!=SQLITE_OK ){
    sqlite3_free(zIgnErr);
    sqlite3_result_error_code(context, rc);
  }
  return rc;
}

static int addAppendTableEntry(
  sqlite3_context *context,
  struct TableEntry **paEntries,
  int *pnEntries,
  const struct TableEntry *pEntry
){
  struct TableEntry *aNew;
  char *zDup = 0;
  if( pEntry->zName ){
    zDup = sqlite3_mprintf("%s", pEntry->zName);
    if( !zDup ){
      if( context ) sqlite3_result_error_nomem(context);
      return SQLITE_NOMEM;
    }
  }
  aNew = sqlite3_realloc(
      *paEntries, (*pnEntries + 1) * (int)sizeof(struct TableEntry));
  if( !aNew ){
    sqlite3_free(zDup);
    if( context ) sqlite3_result_error_nomem(context);
    return SQLITE_NOMEM;
  }
  *paEntries = aNew;
  (*paEntries)[*pnEntries] = *pEntry;
  (*paEntries)[*pnEntries].zName = zDup;
  (*pnEntries)++;
  return SQLITE_OK;
}

static struct TableEntry *addFindEntryByName(
  struct TableEntry *aEntries,
  int nEntries,
  const char *zName
){
  int i;
  if( !zName ) return 0;
  for(i=0; i<nEntries; i++){
    if( aEntries[i].zName && strcmp(aEntries[i].zName, zName)==0 ){
      return &aEntries[i];
    }
  }
  return 0;
}

/* Catalog name->entry index over the shared DoltliteNameIndex, which reads
** names through the live array so it survives dolt_commit -A rewriting entry
** names in place. */
int addNameIndexInit(
  AddNameIndex *pIdx,
  struct TableEntry *aEntry,
  int nEntry
){
  return doltliteNameIndexInit(pIdx, aEntry, nEntry,
                               (int)sizeof(struct TableEntry),
                               (int)offsetof(struct TableEntry, zName));
}

void addNameIndexFree(AddNameIndex *pIdx){
  doltliteNameIndexFree(pIdx);
}

struct TableEntry *addNameIndexFind(
  const AddNameIndex *pIdx,
  const char *zName
){
  int r = doltliteNameIndexFind(pIdx, zName);
  return r<0 ? 0 : (struct TableEntry*)(pIdx->aBase + (size_t)r*pIdx->stride);
}

static void addAlignStagedEntriesToWorking(
  struct TableEntry *aWorking,
  int nWorking,
  struct TableEntry *aStaged,
  int nStaged
){
  int i;
  AddNameIndex workingIdx;
  if( addNameIndexInit(&workingIdx, aWorking, nWorking)!=SQLITE_OK ){
    for(i=0; i<nStaged; i++){
      struct TableEntry *pWorking;
      if( !aStaged[i].zName ) continue;
      pWorking = addFindEntryByName(aWorking, nWorking, aStaged[i].zName);
      if( pWorking ){
        aStaged[i].iTable = pWorking->iTable;
      }
    }
    return;
  }
  for(i=0; i<nStaged; i++){
    struct TableEntry *pWorking;
    if( !aStaged[i].zName ) continue;
    pWorking = addNameIndexFind(&workingIdx, aStaged[i].zName);
    if( pWorking ){
      aStaged[i].iTable = pWorking->iTable;
    }
  }
  addNameIndexFree(&workingIdx);
}

static int addLoadWorkingAndStagedCatalogs(
  sqlite3 *db,
  const ProllyHash *pWorkingHash,
  struct TableEntry **paWorking,
  int *pnWorking,
  struct TableEntry **paStaged,
  int *pnStaged
){
  ProllyHash stagedHash;
  int rc;

  *paWorking = 0;
  *pnWorking = 0;
  *paStaged = 0;
  *pnStaged = 0;

  rc = doltliteLoadCatalog(db, pWorkingHash, paWorking, pnWorking, 0);
  if( rc!=SQLITE_OK ) return rc;

  doltliteGetSessionStaged(db, &stagedHash);
  if( prollyHashIsEmpty(&stagedHash) ){
    ProllyHash headCat;
    rc = doltliteGetHeadCatalogHash(db, &headCat);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCat) ){
      rc = doltliteLoadCatalog(db, &headCat, paStaged, pnStaged, 0);
    }
  }else{
    rc = doltliteLoadCatalog(db, &stagedHash, paStaged, pnStaged, 0);
  }
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(*paWorking, *pnWorking);
    *paWorking = 0;
    *pnWorking = 0;
  }
  return rc;
}

static int addWriteStagedCatalog(
  sqlite3 *db,
  ChunkStore *cs,
  struct TableEntry *aEntries,
  int nEntries
){
  u8 *buf = 0;
  int nBuf = 0;
  ProllyHash newStagedHash;
  int rc = doltliteSerializeCatalogEntries(db, aEntries, nEntries, &buf, &nBuf);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(cs, buf, nBuf, &newStagedHash);
  }
  sqlite3_free(buf);
  if( rc==SQLITE_OK ){
    doltliteSetSessionStaged(db, &newStagedHash);
  }
  return rc;
}

/* True when the two catalogs' index schema rows for zTable differ: any
** index added, dropped, re-targeted, or redefined. Index changes carry no
** named catalog entry, so every surface that reports per-table change
** (status, the diff summary) attributes them through this comparison. */
int doltliteIndexSchemaRowsDifferForTable(
  SchemaEntry *aA, int nA,
  SchemaEntry *aB, int nB,
  const char *zTable
){
  int i, j, nMatchA = 0, nBForTable = 0;
  for(i=0; i<nA; i++){
    int found = 0;
    if( !aA[i].zType || strcmp(aA[i].zType, "index")!=0
     || !aA[i].zTblName || strcmp(aA[i].zTblName, zTable)!=0 ){
      continue;
    }
    for(j=0; j<nB; j++){
      if( aB[j].zType && strcmp(aB[j].zType, "index")==0
       && aB[j].zTblName && strcmp(aB[j].zTblName, zTable)==0
       && aB[j].zName && aA[i].zName
       && strcmp(aB[j].zName, aA[i].zName)==0
       && ((aB[j].zSql==0)==(aA[i].zSql==0))
       && (aB[j].zSql==0 || strcmp(aB[j].zSql, aA[i].zSql)==0) ){
        found = 1;
        break;
      }
    }
    if( !found ) return 1;
    nMatchA++;
  }
  for(j=0; j<nB; j++){
    if( aB[j].zType && strcmp(aB[j].zType, "index")==0
     && aB[j].zTblName && strcmp(aB[j].zTblName, zTable)==0 ){
      nBForTable++;
    }
  }
  return nMatchA!=nBForTable;
}

/* True when the final staged entry list contains a table entry named
** zTbl. Index entries follow their parent table through -a staging, and
** the parent's presence decides whether an index entry belongs at all. */
int amTableStagedByName(struct TableEntry *aStaged, int nStaged,
                               const char *zTbl){
  int i;
  if( !zTbl ) return 0;
  for(i=0; i<nStaged; i++){
    if( aStaged[i].zName && strcmp(aStaged[i].zName, zTbl)==0 ) return 1;
  }
  return 0;
}

static int addStageAllTables(
  sqlite3 *db,
  sqlite3_context *context,
  ChunkStore *cs,
  const ProllyHash *pWorkingHash
){
  struct TableEntry *aWorking = 0;
  struct TableEntry *aStaged = 0;
  struct TableEntry *aNew = 0;
  int nWorking = 0;
  int nStaged = 0;
  int nNew = 0;
  int k;
  int useWorkingHash = 1;
  int rc;
  AddNameIndex stagedIdx;
  AddNameIndex workingIdx;
  int stagedIdxInit = 0;
  int workingIdxInit = 0;

  rc = addLoadWorkingAndStagedCatalogs(db, pWorkingHash,
                                       &aWorking, &nWorking,
                                       &aStaged, &nStaged);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to load staged catalog", -1);
    return rc;
  }
  rc = addNameIndexInit(&stagedIdx, aStaged, nStaged);
  if( rc!=SQLITE_OK ){
    addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
    sqlite3_result_error_nomem(context);
    return rc;
  }
  stagedIdxInit = 1;
  rc = addNameIndexInit(&workingIdx, aWorking, nWorking);
  if( rc!=SQLITE_OK ){
    addNameIndexFree(&stagedIdx);
    addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
    sqlite3_result_error_nomem(context);
    return rc;
  }
  workingIdxInit = 1;

  for(k=0; k<nWorking; k++){
    const char *zName = aWorking[k].zName;
    struct TableEntry *pUse = &aWorking[k];
    if( aWorking[k].iTable>1 && zName ){
      int ignored = 0;
      rc = addCheckIgnore(db, context, zName, &ignored);
      if( rc!=SQLITE_OK ){
        if( workingIdxInit ) addNameIndexFree(&workingIdx);
        if( stagedIdxInit ) addNameIndexFree(&stagedIdx);
        addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
        return rc;
      }
      if( ignored ){
        useWorkingHash = 0;
        pUse = addNameIndexFind(&stagedIdx, zName);
        if( !pUse ) continue;
      }
    }
    rc = addAppendTableEntry(context, &aNew, &nNew, pUse);
    if( rc!=SQLITE_OK ){
      if( workingIdxInit ) addNameIndexFree(&workingIdx);
      if( stagedIdxInit ) addNameIndexFree(&stagedIdx);
      addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
      return rc;
    }
  }

  for(k=0; k<nStaged; k++){
    const char *zName = aStaged[k].zName;
    if( aStaged[k].iTable<=1 || !zName ) continue;
    if( addNameIndexFind(&workingIdx, zName) ) continue;
    {
      int ignored = 0;
      rc = addCheckIgnore(db, context, zName, &ignored);
      if( rc!=SQLITE_OK ){
        if( workingIdxInit ) addNameIndexFree(&workingIdx);
        if( stagedIdxInit ) addNameIndexFree(&stagedIdx);
        addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
        return rc;
      }
      if( !ignored ) continue;
    }
    useWorkingHash = 0;
    rc = addAppendTableEntry(context, &aNew, &nNew, &aStaged[k]);
    if( rc!=SQLITE_OK ){
      if( workingIdxInit ) addNameIndexFree(&workingIdx);
      if( stagedIdxInit ) addNameIndexFree(&stagedIdx);
      addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
      return rc;
    }
  }

  if( useWorkingHash ){
    doltliteSetSessionStaged(db, pWorkingHash);
    rc = SQLITE_OK;
  }else{
    addAlignStagedEntriesToWorking(aWorking, nWorking, aNew, nNew);
    rc = addWriteStagedCatalog(db, cs, aNew, nNew);
  }
  if( workingIdxInit ) addNameIndexFree(&workingIdx);
  if( stagedIdxInit ) addNameIndexFree(&stagedIdx);
  addFreeEntries(aWorking, nWorking, aStaged, nStaged, aNew, nNew);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
  }
  return rc;
}

/* Remove unnamed (index) staged entries whose parent table is zTable,
** resolving parents through the staged catalog's own schema rows — entry
** numbers do not carry across catalogs. */
void addRemoveIndexEntriesOfTable(
  struct TableEntry *aStaged,
  int *pnStaged,
  SchemaEntry *aStagedSchema,
  int nStagedSchema,
  const char *zTable
){
  int j, i;
  for(j=0; j<*pnStaged; ){
    const char *zParent = 0;
    if( aStaged[j].iTable<=1 || aStaged[j].zName ){ j++; continue; }
    for(i=0; i<nStagedSchema; i++){
      if( aStagedSchema[i].zType
       && strcmp(aStagedSchema[i].zType, "index")==0
       && aStagedSchema[i].iRootpage==aStaged[j].iTable ){
        zParent = aStagedSchema[i].zTblName;
        break;
      }
    }
    if( !zParent || strcmp(zParent, zTable)!=0 ){ j++; continue; }
    sqlite3_free(aStaged[j].zName);
    if( j+1<*pnStaged ){
      memmove(&aStaged[j], &aStaged[j+1],
              (*pnStaged-j-1)*(int)sizeof(struct TableEntry));
    }
    (*pnStaged)--;
  }
}

/* Stage zTable's index entries from the working catalog alongside its
** table entry: an index travels with its table through staging. */
int addAppendIndexEntriesOfTable(
  sqlite3_context *context,
  struct TableEntry **paStaged,
  int *pnStaged,
  struct TableEntry *aWorking,
  int nWorking,
  SchemaEntry *aWorkSchema,
  int nWorkSchema,
  const char *zTable
){
  int i, j, rc;
  for(i=0; i<nWorkSchema; i++){
    if( !aWorkSchema[i].zType
     || strcmp(aWorkSchema[i].zType, "index")!=0
     || aWorkSchema[i].iRootpage<=1
     || !aWorkSchema[i].zTblName
     || strcmp(aWorkSchema[i].zTblName, zTable)!=0 ){
      continue;
    }
    for(j=0; j<nWorking; j++){
      if( aWorking[j].iTable==aWorkSchema[i].iRootpage
       && aWorking[j].zName==0 ){
        rc = addAppendTableEntry(context, paStaged, pnStaged, &aWorking[j]);
        if( rc!=SQLITE_OK ) return rc;
        break;
      }
    }
  }
  return SQLITE_OK;
}

/* Virtual tables persist through their shadow tables: staging a vtab by
** name must carry the shadows (and their indexes) along, exactly as index
** entries travel with ordinary tables. */
static int addStageShadowTablesOf(
  sqlite3 *db,
  sqlite3_context *context,
  struct TableEntry **paStaged,
  int *pnStaged,
  struct TableEntry *aWorking,
  int nWorking,
  SchemaEntry *aWorkSchema,
  int nWorkSchema,
  SchemaEntry *aStagedSchema,
  int nStagedSchema,
  const char *zTable
){
  Table *pTab;
  int i, k, rc;

  pTab = sqlite3FindTable(db, zTable, "main");
  if( !pTab || !IsVirtual(pTab) ) return SQLITE_OK;

  for(i=0; i<nWorkSchema; i++){
    struct TableEntry *pWork;
    int updated = 0;
    if( !aWorkSchema[i].zType
     || strcmp(aWorkSchema[i].zType, "table")!=0
     || !aWorkSchema[i].zName
     || strcmp(aWorkSchema[i].zName, zTable)==0
     || !sqlite3IsShadowTableOf(db, pTab, aWorkSchema[i].zName) ){
      continue;
    }
    pWork = doltliteFindTableByName(aWorking, nWorking, aWorkSchema[i].zName);
    if( !pWork ) continue;
    for(k=0; k<*pnStaged; k++){
      if( (*paStaged)[k].zName
       && strcmp((*paStaged)[k].zName, pWork->zName)==0 ){
        char *zDup = sqlite3_mprintf("%s", pWork->zName);
        if( !zDup ) return SQLITE_NOMEM;
        sqlite3_free((*paStaged)[k].zName);
        (*paStaged)[k] = *pWork;
        (*paStaged)[k].zName = zDup;
        updated = 1;
        break;
      }
    }
    if( !updated ){
      rc = addAppendTableEntry(context, paStaged, pnStaged, pWork);
      if( rc!=SQLITE_OK ) return rc;
    }
    addRemoveIndexEntriesOfTable(*paStaged, pnStaged,
                                 aStagedSchema, nStagedSchema,
                                 pWork->zName);
    rc = addAppendIndexEntriesOfTable(context, paStaged, pnStaged,
                                      aWorking, nWorking,
                                      aWorkSchema, nWorkSchema,
                                      pWork->zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/* Remove staged entries for shadow tables of a dropped virtual table. The
** vtab is gone from the live schema, so shadows are identified through the
** staged catalog's own rows: table rows prefixed "<zTable>_" whose parent
** row is a CREATE VIRTUAL TABLE statement and which no longer exist in
** working. */
static void addRemoveShadowEntriesOfDroppedVtab(
  struct TableEntry *aStaged,
  int *pnStaged,
  SchemaEntry *aStagedSchema,
  int nStagedSchema,
  struct TableEntry *aWorking,
  int nWorking,
  const char *zTable
){
  SchemaEntry *pParent;
  size_t nPrefix;
  int i, k;

  pParent = findSchemaEntry(aStagedSchema, nStagedSchema, zTable);
  if( !pParent || !pParent->zSql
   || sqlite3_strnicmp(pParent->zSql, "CREATE VIRTUAL", 14)!=0 ){
    return;
  }
  nPrefix = strlen(zTable);
  for(i=0; i<nStagedSchema; i++){
    if( !aStagedSchema[i].zType
     || strcmp(aStagedSchema[i].zType, "table")!=0
     || !aStagedSchema[i].zName
     || sqlite3_strnicmp(aStagedSchema[i].zName, zTable, (int)nPrefix)!=0
     || aStagedSchema[i].zName[nPrefix]!='_'
     || doltliteFindTableByName(aWorking, nWorking, aStagedSchema[i].zName) ){
      continue;
    }
    for(k=0; k<*pnStaged; ){
      if( aStaged[k].zName
       && strcmp(aStaged[k].zName, aStagedSchema[i].zName)==0 ){
        sqlite3_free(aStaged[k].zName);
        if( k+1<*pnStaged ){
          memmove(&aStaged[k], &aStaged[k+1],
                  (*pnStaged-k-1)*(int)sizeof(struct TableEntry));
        }
        (*pnStaged)--;
        continue;
      }
      k++;
    }
  }
}

static int addStageNamedTables(
  sqlite3 *db,
  sqlite3_context *context,
  ChunkStore *cs,
  const ProllyHash *pWorkingHash,
  int argc,
  sqlite3_value **argv
){
  struct TableEntry *aWorking = 0;
  struct TableEntry *aStaged = 0;
  int nWorking = 0;
  int nStaged = 0;
  int i;
  int updateMaster = 0;
  int rc;

  SchemaEntry *aWorkSchema = 0;
  SchemaEntry *aStagedSchema = 0;
  int nWorkSchema = 0;
  int nStagedSchema = 0;
  /* Objects whose master rows follow WORKING in this operation: the named
  ** tables (adds and staged drops) plus a staged vtab's shadows. Names are
  ** borrowed from the argv values and schema arrays, which outlive use. */
  char **azTouched = 0;
  int nTouched = 0;

  #define ADDNAMED_FREE_ALL() do { \
    int ft_; \
    for(ft_=0; ft_<nTouched; ft_++) sqlite3_free(azTouched[ft_]); \
    sqlite3_free((void*)azTouched); \
    freeSchemaEntries(aWorkSchema, nWorkSchema); \
    freeSchemaEntries(aStagedSchema, nStagedSchema); \
    doltliteFreeCatalog(aWorking, nWorking); \
    doltliteFreeCatalog(aStaged, nStaged); \
  } while(0)

  #define ADDNAMED_TOUCH(zN) do { \
    char **azNew = sqlite3_realloc((void*)azTouched, \
        (nTouched+1)*(int)sizeof(char*)); \
    char *zOwn_ = azNew ? sqlite3_mprintf("%s", (zN)) : 0; \
    if( azNew ) azTouched = azNew; \
    if( !azNew || !zOwn_ ){ \
      ADDNAMED_FREE_ALL(); \
      sqlite3_result_error_nomem(context); \
      return SQLITE_NOMEM; \
    } \
    azTouched[nTouched++] = zOwn_; \
  } while(0)

  rc = addLoadWorkingAndStagedCatalogs(db, pWorkingHash,
                                       &aWorking, &nWorking,
                                       &aStaged, &nStaged);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to load staged catalog", -1);
    return rc;
  }

  {
    ProllyHash stagedSrc;
    doltliteGetSessionStaged(db, &stagedSrc);
    if( prollyHashIsEmpty(&stagedSrc) ){
      (void)doltliteGetHeadCatalogHash(db, &stagedSrc);
    }
    rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), pWorkingHash,
                               &aWorkSchema, &nWorkSchema);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(&stagedSrc) ){
      rc = loadSchemaFromCatalog(db, cs, doltliteGetCache(db), &stagedSrc,
                                 &aStagedSchema, &nStagedSchema);
    }
    if( rc!=SQLITE_OK ){
      ADDNAMED_FREE_ALL();
      sqlite3_result_error(context, "failed to load schema for staging", -1);
      return rc;
    }
  }

  for(i=0; i<argc; i++){
    const char *zTable = (const char*)sqlite3_value_text(argv[i]);
    Pgno iTable = 0;
    int j;
    if( !zTable || zTable[0]=='-' || strcmp(zTable, ".")==0 ) continue;

    {
      int ignored = 0;
      rc = addCheckIgnore(db, context, zTable, &ignored);
      if( rc!=SQLITE_OK ){
        ADDNAMED_FREE_ALL();
        return rc;
      }
      if( ignored ) continue;
    }

    /* Virtual tables carry no catalog entry of their own (schema row
    ** only), so the entry-based flow below cannot see them: their commit
    ** content IS the shadow tables. Stage those, and adopt the working
    ** master so the vtab's schema row travels too. */
    {
      Table *pLive = sqlite3FindTable(db, zTable, "main");
      if( pLive && IsVirtual(pLive) ){
        int w;
        rc = addStageShadowTablesOf(db, context, &aStaged, &nStaged,
                                    aWorking, nWorking,
                                    aWorkSchema, nWorkSchema,
                                    aStagedSchema, nStagedSchema, zTable);
        if( rc!=SQLITE_OK ){
          ADDNAMED_FREE_ALL();
          return rc;
        }
        ADDNAMED_TOUCH(zTable);
        for(w=0; w<nWorkSchema; w++){
          if( aWorkSchema[w].zType
           && strcmp(aWorkSchema[w].zType, "table")==0
           && aWorkSchema[w].zName
           && strcmp(aWorkSchema[w].zName, zTable)!=0
           && sqlite3IsShadowTableOf(db, pLive, aWorkSchema[w].zName) ){
            ADDNAMED_TOUCH(aWorkSchema[w].zName);
          }
        }
        updateMaster = 1;
        continue;
      }
    }

    rc = doltliteResolveTableName(db, zTable, &iTable);
    if( rc!=SQLITE_OK ){
      int found = 0;
      Pgno iDroppedTable = 0;
      for(j=0; j<nStaged; j++){
        if( aStaged[j].zName && strcmp(aStaged[j].zName, zTable)==0 ){
          iDroppedTable = aStaged[j].iTable;
          found = 1;
          break;
        }
      }
      if( !found && findSchemaEntry(aStagedSchema, nStagedSchema, zTable) ){
        /* A dropped virtual table has a staged schema row but no entry;
        ** the master adoption below stages the row's removal. */
        found = 1;
      }
      if( found ){
        for(j=0; j<nStaged; ){
          int removeEntry = 0;
          if( iDroppedTable!=0 && aStaged[j].iTable==iDroppedTable ){
            removeEntry = 1;
          }else if( aStaged[j].zName && strcmp(aStaged[j].zName, zTable)==0 ){
            removeEntry = 1;
          }
          if( removeEntry ){
            sqlite3_free(aStaged[j].zName);
            if( j+1 < nStaged ){
              memmove(&aStaged[j], &aStaged[j+1],
                      (nStaged-j-1) * (int)sizeof(struct TableEntry));
            }
            nStaged--;
            continue;
          }
          j++;
        }
        addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                     aStagedSchema, nStagedSchema, zTable);
        addRemoveShadowEntriesOfDroppedVtab(aStaged, &nStaged,
                                            aStagedSchema, nStagedSchema,
                                            aWorking, nWorking, zTable);
        ADDNAMED_TOUCH(zTable);
        updateMaster = 1;
      }
      if( !found ){
        char *zErr = sqlite3_mprintf("table not found: %s", zTable);
        ADDNAMED_FREE_ALL();
        if( zErr ){
          sqlite3_result_error(context, zErr, -1);
          sqlite3_free(zErr);
        }else{
          sqlite3_result_error_nomem(context);
        }
        return SQLITE_ERROR;
      }
      continue;
    }

    for(j=0; j<nWorking; j++){
      if( aWorking[j].iTable==iTable ){
        int k;
        int updated = 0;
        for(k=0; k<nStaged; k++){
          int nameMatch = aStaged[k].zName && aWorking[j].zName
            && strcmp(aStaged[k].zName, aWorking[j].zName)==0;
          int rootMatch = aStaged[k].iTable==iTable;
          /* An unnamed staged entry on a bare number match can be a
          ** cross-domain collision with an index entry; pair only when
          ** the staged catalog's own schema rows say the entry at this
          ** number is this very table. */
          int unnamedRootMatch = 0;
          int renameRootMatch = rootMatch
            && aStaged[k].zName && aWorking[j].zName
            && strcmp(aStaged[k].zName, aWorking[j].zName)!=0
            && !addFindEntryByName(aWorking, nWorking, aStaged[k].zName)
            && !addFindEntryByName(aStaged, nStaged, aWorking[j].zName);
          if( rootMatch && !aStaged[k].zName && aWorking[j].zName ){
            int r;
            for(r=0; r<nStagedSchema; r++){
              if( aStagedSchema[r].iRootpage==aStaged[k].iTable
               && aStagedSchema[r].zType
               && strcmp(aStagedSchema[r].zType, "table")==0
               && aStagedSchema[r].zName
               && strcmp(aStagedSchema[r].zName, aWorking[j].zName)==0 ){
                unnamedRootMatch = 1;
                break;
              }
            }
          }
          if( nameMatch || unnamedRootMatch || renameRootMatch ){
            int schemaChanged =
              prollyHashCompare(&aStaged[k].schemaHash, &aWorking[j].schemaHash)!=0;
            int nameChanged =
              (!aStaged[k].zName) != (!aWorking[j].zName)
              || (aStaged[k].zName && aWorking[j].zName
                  && strcmp(aStaged[k].zName, aWorking[j].zName)!=0);
            char *zDup;
            /* A rename retires the old name: its rows (and its indexes'
            ** rows, keyed by the old tbl_name) must follow WORKING too,
            ** where they no longer exist. */
            if( nameChanged && aStaged[k].zName ){
              ADDNAMED_TOUCH(aStaged[k].zName);
            }
            zDup = aWorking[j].zName
                           ? sqlite3_mprintf("%s", aWorking[j].zName) : 0;
            if( aWorking[j].zName && !zDup ){
              ADDNAMED_FREE_ALL();
              sqlite3_result_error_nomem(context);
              return SQLITE_NOMEM;
            }
            sqlite3_free(aStaged[k].zName);
            aStaged[k] = aWorking[j];
            aStaged[k].zName = zDup;
            if( schemaChanged || nameChanged ){
              updateMaster = 1;
            }
            updated = 1;
            break;
          }
        }
        if( !updated ){
          updateMaster = 1;
          rc = addAppendTableEntry(context, &aStaged, &nStaged, &aWorking[j]);
          if( rc!=SQLITE_OK ){
            ADDNAMED_FREE_ALL();
            return rc;
          }
        }
        /* An index travels with its table: replace whatever index entries
        ** the staged catalog carried for this table with the working ones. */
        addRemoveIndexEntriesOfTable(aStaged, &nStaged,
                                     aStagedSchema, nStagedSchema, zTable);
        rc = addAppendIndexEntriesOfTable(context, &aStaged, &nStaged,
                                          aWorking, nWorking,
                                          aWorkSchema, nWorkSchema, zTable);
        if( rc!=SQLITE_OK ){
          ADDNAMED_FREE_ALL();
          return rc;
        }
        rc = addStageShadowTablesOf(db, context, &aStaged, &nStaged,
                                    aWorking, nWorking,
                                    aWorkSchema, nWorkSchema,
                                    aStagedSchema, nStagedSchema, zTable);
        if( rc!=SQLITE_OK ){
          ADDNAMED_FREE_ALL();
          return rc;
        }
        ADDNAMED_TOUCH(zTable);
        break;
      }
    }
  }

  if( updateMaster ){
    struct TableEntry *pWorkingMaster = doltliteFindTableByNumber(aWorking, nWorking, 1);
    struct TableEntry *pStagedMaster = doltliteFindTableByNumber(aStaged, nStaged, 1);
    if( pWorkingMaster ){
      /* The staged master must share the working numbering domain, but a
      ** wholesale adoption would carry unstaged view and trigger changes
      ** into the commit (Dolt keeps them out of a named add). Compose the
      ** master: table and index rows from working, view and trigger rows
      ** from the previously staged state. */
      ProllyHash composedRoot;
      rc = doltliteBuildNamedStageMasterRoot(db,
              &pWorkingMaster->root, pWorkingMaster->flags,
              pStagedMaster ? &pStagedMaster->root : 0,
              pStagedMaster ? pStagedMaster->flags : 0,
              (const char**)azTouched, nTouched,
              &composedRoot);
      if( rc!=SQLITE_OK ){
        ADDNAMED_FREE_ALL();
        sqlite3_result_error_code(context, rc);
        return rc;
      }
      if( pStagedMaster ){
        pStagedMaster->root = composedRoot;
        pStagedMaster->schemaHash = pWorkingMaster->schemaHash;
        pStagedMaster->flags = pWorkingMaster->flags;
      }else{
        struct TableEntry composed = *pWorkingMaster;
        composed.zName = 0;
        composed.root = composedRoot;
        rc = addAppendTableEntry(context, &aStaged, &nStaged, &composed);
        if( rc!=SQLITE_OK ){
          ADDNAMED_FREE_ALL();
          return rc;
        }
      }
    }
  }

  addAlignStagedEntriesToWorking(aWorking, nWorking, aStaged, nStaged);
  rc = addWriteStagedCatalog(db, cs, aStaged, nStaged);
  ADDNAMED_FREE_ALL();
  #undef ADDNAMED_TOUCH
  #undef ADDNAMED_FREE_ALL
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
  }
  return rc;
}

static int doltliteStageArgsAndPersist(
  sqlite3 *db,
  sqlite3_context *context,
  ChunkStore *cs,
  int argc,
  sqlite3_value **argv,
  int stageAll
){
  ProllyHash workingHash;
  ProllyHash savedStaged;
  int rc;

  doltliteGetSessionStaged(db, &savedStaged);

  rc = doltlitePrepareCatalogForPersistence(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to prepare catalog", -1);
    return rc;
  }

  rc = doltliteFlushCatalogToHash(db, &workingHash);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to flush", -1);
    return rc;
  }

  if( stageAll ){
    rc = addStageAllTables(db, context, cs, &workingHash);
  }else{
    rc = addStageNamedTables(db, context, cs, &workingHash, argc, argv);
  }
  if( rc!=SQLITE_OK ) return rc;

  rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ){
    doltliteSetSessionStaged(db, &savedStaged);
    sqlite3_result_error_code(context, rc);
    return rc;
  }
  return SQLITE_OK;
}

static void doltliteAddFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int sealTopLevel = doltliteSavepointIsTopLevelTxn(db);
  int rc;
  int i;
  int stageAll = 0;
  int opRc = SQLITE_OK;

  if( !cs ){
    sqlite3_result_error(context, doltliteVcUnavailableMessage(db), -1);
    goto add_cleanup;
  }
  if( argc==0 ){
    sqlite3_result_error(context, "dolt_add requires table name or '-A'", -1);
    goto add_cleanup;
  }

  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-A")==0
     || strcmp(arg, "--all")==0
     || strcmp(arg, ".")==0 ){
      stageAll = 1;
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(context, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(context);
      }
      goto add_cleanup;
    }
  }

  opRc = doltliteStageArgsAndPersist(db, context, cs, argc, argv, stageAll);
  if( opRc!=SQLITE_OK ) goto add_cleanup;

  sqlite3_result_int(context, 0);

add_cleanup:
  if( sealTopLevel ){
    rc = doltliteVcSealTopLevelSavepointTxn(db);
    if( rc==SQLITE_OK && opRc==SQLITE_OK ){
      rc = doltliteStageArgsAndPersist(db, context, cs, argc, argv, stageAll);
      if( rc!=SQLITE_OK ){
        opRc = rc;
      }
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
    }else if( opRc!=SQLITE_OK ){
      sqlite3_result_error_code(context, opRc);
    }
  }else if( opRc!=SQLITE_OK ){
    sqlite3_result_error_code(context, opRc);
  }
}



int doltliteAddRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_add", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteAddFunc, 0, 0);
}

#endif
