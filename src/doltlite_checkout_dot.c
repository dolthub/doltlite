#ifdef DOLTLITE_PROLLY

#include "doltlite_internal.h"
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include <string.h>

/* A lone "." names every table in the primary catalog and the other catalog.
** Primary is the staged catalog, or HEAD when nothing is staged. The other
** catalog is HEAD, so a table staged as deleted is still restored. Checking
** "." out of a commit uses that commit as primary and the staged catalog
** (HEAD, when nothing is staged) as the other. */
#define DOT_PRIMARY 1
#define DOT_OTHER   2

typedef struct DotName DotName;
struct DotName {
  char *z;
  u8 bits;
};

static int dotAdd(DotName **pa, int *pn, const char *zName, int bit){
  int i;
  char *zOwn;
  DotName *aNew;
  for(i=0; i<*pn; i++){
    if( sqlite3_stricmp((*pa)[i].z, zName)==0 ){
      (*pa)[i].bits = (u8)((*pa)[i].bits | bit);
      return SQLITE_OK;
    }
  }
  aNew = sqlite3_realloc64(*pa, (sqlite3_uint64)(*pn + 1)*sizeof(DotName));
  if( !aNew ) return SQLITE_NOMEM;
  *pa = aNew;
  zOwn = sqlite3_mprintf("%s", zName);
  if( !zOwn ) return SQLITE_NOMEM;
  (*pa)[*pn].z = zOwn;
  (*pa)[*pn].bits = (u8)bit;
  (*pn)++;
  return SQLITE_OK;
}

static int dotCollectSchema(
  SchemaEntry *aEntry,
  int nEntry,
  int bit,
  DotName **pa,
  int *pn
){
  int i;
  int schemas = 0;
  int rc = SQLITE_OK;
  for(i=0; i<nEntry; i++){
    const char *zType = aEntry[i].zType;
    const char *zName = aEntry[i].zName;
    if( !zType ) continue;
    if( strcmp(zType, "view")==0 || strcmp(zType, "trigger")==0 ){
      schemas = 1;
      continue;
    }
    if( strcmp(zType, "table")!=0 || !zName ) continue;
    if( sqlite3_strnicmp(zName, "sqlite_", 7)==0 ) continue;
    rc = dotAdd(pa, pn, zName, bit);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( schemas ) rc = dotAdd(pa, pn, "dolt_schemas", bit);
  return rc;
}

static int dotLoad(
  sqlite3 *db,
  const ProllyHash *pHash,
  int bit,
  DotName **pa,
  int *pn
){
  SchemaEntry *aEntry = 0;
  int nEntry = 0;
  int rc;
  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  rc = loadSchemaFromCatalog(db, doltliteGetChunkStore(db),
                             doltliteGetCache(db), pHash, &aEntry, &nEntry);
  if( rc==SQLITE_OK ) rc = dotCollectSchema(aEntry, nEntry, bit, pa, pn);
  freeSchemaEntries(aEntry, nEntry);
  return rc;
}

static int dotFill(
  DotName *aName,
  int nName,
  int wantBit,
  int excludeBit,
  char ***paz,
  int *pn
){
  int i;
  int n = 0;
  char **az;
  *paz = 0;
  *pn = 0;
  for(i=0; i<nName; i++){
    if( (aName[i].bits & wantBit)==0 ) continue;
    if( excludeBit && (aName[i].bits & excludeBit) ) continue;
    n++;
  }
  if( n==0 ) return SQLITE_OK;
  az = sqlite3_malloc64((sqlite3_uint64)n * sizeof(char*));
  if( !az ) return SQLITE_NOMEM;
  n = 0;
  for(i=0; i<nName; i++){
    if( (aName[i].bits & wantBit)==0 ) continue;
    if( excludeBit && (aName[i].bits & excludeBit) ) continue;
    az[n++] = aName[i].z;
  }
  *paz = az;
  *pn = n;
  return SQLITE_OK;
}

static void dotFreeValues(sqlite3_value **apVal, int nVal){
  int i;
  if( !apVal ) return;
  for(i=0; i<nVal; i++) sqlite3ValueFree(apVal[i]);
  sqlite3_free(apVal);
}

static int dotRun(
  sqlite3 *db,
  sqlite3_context *context,
  const char *zSourceRef,
  char **azName,
  int nName,
  int bSourceHead,
  int bDropIfAbsent,
  const char **pzMissing
){
  sqlite3_value **apVal;
  int i;
  int rc;
  if( nName<=0 ) return SQLITE_OK;
  apVal = sqlite3_malloc64((sqlite3_uint64)nName * sizeof(sqlite3_value*));
  if( !apVal ) return SQLITE_NOMEM;
  memset(apVal, 0, (size_t)nName * sizeof(sqlite3_value*));
  for(i=0; i<nName; i++){
    apVal[i] = sqlite3ValueNew(db);
    if( !apVal[i] ){
      dotFreeValues(apVal, nName);
      return SQLITE_NOMEM;
    }
    sqlite3ValueSetStr(apVal[i], -1, azName[i], SQLITE_UTF8,
                       SQLITE_TRANSIENT);
  }
  rc = doltliteCheckoutTables(db, context, zSourceRef, apVal, 0, nName,
                              pzMissing, bSourceHead, bDropIfAbsent);
  if( rc==SQLITE_NOTFOUND && pzMissing && *pzMissing ){
    char *zKeep = sqlite3_mprintf("%s", *pzMissing);
    if( zKeep ){
      sqlite3_set_auxdata(context, 0, zKeep, sqlite3_free);
      *pzMissing = (const char*)sqlite3_get_auxdata(context, 0);
    }
  }
  dotFreeValues(apVal, nName);
  return rc;
}

static void dotFreeNames(DotName *aName, int nName){
  int i;
  for(i=0; i<nName; i++) sqlite3_free(aName[i].z);
  sqlite3_free(aName);
}

int doltliteCheckoutDot(
  sqlite3 *db,
  sqlite3_context *context,
  const char *zSourceRef,
  const char **pzMissing
){
  DotName *aName = 0;
  char **azPrimary = 0;
  char **azFallback = 0;
  ProllyHash staged;
  ProllyHash head;
  ProllyHash commit;
  ProllyHash commitCat;
  int nName = 0;
  int nPrimary = 0;
  int nFallback = 0;
  int rc;

  memset(&staged, 0, sizeof(staged));
  memset(&head, 0, sizeof(head));
  memset(&commit, 0, sizeof(commit));
  memset(&commitCat, 0, sizeof(commitCat));
  doltliteGetSessionStaged(db, &staged);

  if( zSourceRef ){
    rc = doltliteResolveRef(db, zSourceRef, &commit);
    if( rc!=SQLITE_OK ) return SQLITE_NOTFOUND;
    rc = doltliteCommitCatalogHash(db, &commit, &commitCat);
    if( rc!=SQLITE_OK ) return rc;
    rc = dotLoad(db, &commitCat, DOT_PRIMARY, &aName, &nName);
    if( rc==SQLITE_OK ){
      if( prollyHashIsEmpty(&staged) ){
        rc = doltliteGetHeadCatalogHash(db, &head);
        if( rc==SQLITE_OK ) rc = dotLoad(db, &head, DOT_OTHER, &aName, &nName);
      }else{
        rc = dotLoad(db, &staged, DOT_OTHER, &aName, &nName);
      }
    }
  }else if( prollyHashIsEmpty(&staged) ){
    rc = doltliteGetHeadCatalogHash(db, &head);
    if( rc==SQLITE_OK ) rc = dotLoad(db, &head, DOT_PRIMARY, &aName, &nName);
  }else{
    rc = dotLoad(db, &staged, DOT_PRIMARY, &aName, &nName);
    if( rc==SQLITE_OK ){
      rc = doltliteGetHeadCatalogHash(db, &head);
      if( rc==SQLITE_OK ) rc = dotLoad(db, &head, DOT_OTHER, &aName, &nName);
    }
  }

  if( rc==SQLITE_OK && zSourceRef ){
    rc = dotFill(aName, nName, DOT_PRIMARY|DOT_OTHER, 0,
                 &azPrimary, &nPrimary);
    if( rc==SQLITE_OK ){
      rc = dotRun(db, context, zSourceRef, azPrimary, nPrimary, 0, 1,
                  pzMissing);
    }
  }else if( rc==SQLITE_OK ){
    rc = dotFill(aName, nName, DOT_PRIMARY, 0, &azPrimary, &nPrimary);
    if( rc==SQLITE_OK ){
      rc = dotRun(db, context, 0, azPrimary, nPrimary, 0, 0, pzMissing);
    }
    if( rc==SQLITE_OK ){
      rc = dotFill(aName, nName, DOT_OTHER, DOT_PRIMARY,
                   &azFallback, &nFallback);
    }
    if( rc==SQLITE_OK ){
      rc = dotRun(db, context, 0, azFallback, nFallback, 1, 0, pzMissing);
    }
  }

  sqlite3_free(azPrimary);
  sqlite3_free(azFallback);
  dotFreeNames(aName, nName);
  return rc;
}

#endif
