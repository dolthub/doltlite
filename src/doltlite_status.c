
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

typedef struct StatusRow StatusRow;
struct StatusRow {
  char *zName;
  int staged;
  const char *zStatus;
};

typedef struct DoltliteStatusVtab DoltliteStatusVtab;
struct DoltliteStatusVtab { sqlite3_vtab base; sqlite3 *db; };

typedef struct DoltliteStatusCursor DoltliteStatusCursor;
struct DoltliteStatusCursor {
  sqlite3_vtab_cursor base;
  StatusRow *aRows; int nRows; int iRow;
};

static const char *statusSchema =
  "CREATE TABLE x(table_name TEXT, staged INTEGER, status TEXT)";

static int statusTableName(sqlite3 *db, const struct TableEntry *pEntry, char **pzName){
  *pzName = 0;
  if( pEntry->zName ){
    *pzName = sqlite3_mprintf("%s", pEntry->zName);
    return *pzName ? SQLITE_OK : SQLITE_NOMEM;
  }

  if( pEntry->flags & BTREE_BLOBKEY ){
    return SQLITE_NOTFOUND;
  }
  *pzName = doltliteResolveTableNumber(db, pEntry->iTable);
  return *pzName ? SQLITE_OK : SQLITE_NOMEM;
}

static struct TableEntry *findCatalogEntry(
  struct TableEntry *a, int n, const struct TableEntry *pNeedle
){
  if( pNeedle->zName ){
    return doltliteFindTableByName(a, n, pNeedle->zName);
  }
  return doltliteFindTableByNumber(a, n, pNeedle->iTable);
}

static int addRow(DoltliteStatusCursor *pCur, const char *zName,
                  int staged, const char *zStatus){
  StatusRow *aNew = sqlite3_realloc(pCur->aRows,
      (pCur->nRows+1)*(int)sizeof(StatusRow));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aRows = aNew;
  aNew[pCur->nRows].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[pCur->nRows].zName ) return SQLITE_NOMEM;
  aNew[pCur->nRows].staged = staged;
  aNew[pCur->nRows].zStatus = zStatus;
  pCur->nRows++;
  return SQLITE_OK;
}

/* Detect a rename by iTable identity: a table that keeps the same
** rootpage number and data hash but gains a new name is the same
** table renamed. Without this detection, a rename would show up as
** "deleted <old> + new table <new>" which is noisy and loses the
** continuity git status gets from rename heuristics. */
static int isRenamePair(const struct TableEntry *pA, const struct TableEntry *pB){
  if( pA->iTable != pB->iTable ) return 0;
  if( !pA->zName || !pB->zName ) return 0;
  if( strcmp(pA->zName, pB->zName)==0 ) return 0;
  return prollyHashCompare(&pA->root, &pB->root)==0;
}

static int compareCatalogs(
  DoltliteStatusCursor *pCur, sqlite3 *db,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  int staged
){
  int i, j, rc;

  #define DOLT_STATUS_RENAME_CAP 4096
  unsigned char fromHandled[DOLT_STATUS_RENAME_CAP] = {0};
  unsigned char toHandled[DOLT_STATUS_RENAME_CAP] = {0};
  int useRename = (nFrom <= DOLT_STATUS_RENAME_CAP && nTo <= DOLT_STATUS_RENAME_CAP);

  if( useRename ){
    for(i=0; i<nFrom; i++){
      if( aFrom[i].iTable<=1 || fromHandled[i] ) continue;
      for(j=0; j<nTo; j++){
        if( aTo[j].iTable<=1 || toHandled[j] ) continue;
        if( isRenamePair(&aFrom[i], &aTo[j]) ){
          char *zCompound = sqlite3_mprintf("%s -> %s", aFrom[i].zName, aTo[j].zName);
          if( !zCompound ) return SQLITE_NOMEM;
          rc = addRow(pCur, zCompound, staged, "renamed");
          sqlite3_free(zCompound);
          if( rc!=SQLITE_OK ) return rc;
          fromHandled[i] = 1;
          toHandled[j] = 1;
          break;
        }
      }
    }
  }

  for(i=0; i<nTo; i++){
    struct TableEntry *pFrom;
    char *zName;
    if(aTo[i].iTable<=1) continue;
    if( useRename && toHandled[i] ) continue;
    pFrom = findCatalogEntry(aFrom, nFrom, &aTo[i]);
    rc = statusTableName(db, &aTo[i], &zName);
    if( rc==SQLITE_NOTFOUND ) continue;
    if( rc!=SQLITE_OK ) return rc;
    if(!pFrom){
      rc = addRow(pCur, zName, staged, "new table");
    }else{
      rc = SQLITE_OK;
      if(prollyHashCompare(&pFrom->root, &aTo[i].root)!=0){
        rc = addRow(pCur, zName, staged, "modified");
      }
      if(rc==SQLITE_OK
       && !prollyHashIsEmpty(&pFrom->schemaHash)
       && !prollyHashIsEmpty(&aTo[i].schemaHash)
       && prollyHashCompare(&pFrom->schemaHash, &aTo[i].schemaHash)!=0){
        rc = addRow(pCur, zName, staged, "schema modified");
      }
    }
    sqlite3_free(zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nFrom; i++){
    char *zName;
    if(aFrom[i].iTable<=1) continue;
    if( useRename && fromHandled[i] ) continue;
    if(!findCatalogEntry(aTo, nTo, &aFrom[i])){
      rc = statusTableName(db, &aFrom[i], &zName);
      if( rc==SQLITE_NOTFOUND ) continue;
      if( rc!=SQLITE_OK ) return rc;
      rc = addRow(pCur, zName, staged, "deleted");
      sqlite3_free(zName);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
  #undef DOLT_STATUS_RENAME_CAP
}

static int statusConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DoltliteStatusVtab *pVtab; int rc;
  (void)pAux;(void)argc;(void)argv;(void)pzErr;
  rc = sqlite3_declare_vtab(db, statusSchema);
  if(rc!=SQLITE_OK) return rc;
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if(!pVtab) return SQLITE_NOMEM;
  memset(pVtab,0,sizeof(*pVtab)); pVtab->db=db;
  *ppVtab=&pVtab->base; return SQLITE_OK;
}
static int statusDisconnect(sqlite3_vtab *v){sqlite3_free(v);return SQLITE_OK;}
static int statusOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  DoltliteStatusCursor *c;(void)v;
  c=sqlite3_malloc(sizeof(*c));if(!c)return SQLITE_NOMEM;
  memset(c,0,sizeof(*c));*pp=&c->base;return SQLITE_OK;
}
static int statusClose(sqlite3_vtab_cursor *p){
  DoltliteStatusCursor *c=(DoltliteStatusCursor*)p;
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aRows[i].zName);
  }
  sqlite3_free(c->aRows);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int statusFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DoltliteStatusCursor *pCur=(DoltliteStatusCursor*)pCursor;
  DoltliteStatusVtab *pVtab=(DoltliteStatusVtab*)pCursor->pVtab;
  sqlite3 *db=pVtab->db;
  ChunkStore *cs=doltliteGetChunkStore(db);
  ProllyHash headCatHash,stagedCatHash,workingCatHash;
  struct TableEntry *aHead=0,*aStaged=0,*aWorking=0;
  int nHead=0,nStaged=0,nWorking=0,rc;
  (void)idxNum;(void)idxStr;(void)argc;(void)argv;

  {
    int i;
    for(i=0; i<pCur->nRows; i++){
      sqlite3_free(pCur->aRows[i].zName);
    }
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
  pCur->iRow = 0;
  if(!cs) return SQLITE_OK;

  rc=doltliteGetHeadCatalogHash(db,&headCatHash);
  if(rc!=SQLITE_OK) goto status_done;
  rc = doltliteLoadCatalog(db,&headCatHash,&aHead,&nHead,0);
  if(rc!=SQLITE_OK) goto status_done;

  {extern void doltliteGetSessionStaged(sqlite3*,ProllyHash*);
   doltliteGetSessionStaged(db,&stagedCatHash);}
  if(!prollyHashIsEmpty(&stagedCatHash)){
    rc = doltliteLoadCatalog(db,&stagedCatHash,&aStaged,&nStaged,0);
    if( rc!=SQLITE_OK ) goto status_done;
  }

  {
    rc = doltliteFlushCatalogToHash(db, &workingCatHash);
    if(rc==SQLITE_OK){
      rc = doltliteLoadCatalog(db,&workingCatHash,&aWorking,&nWorking,0);
    }
    if( rc!=SQLITE_OK ) goto status_done;
  }


  if(aStaged){
    rc = compareCatalogs(pCur,db,aHead,nHead,aStaged,nStaged,1);
    if( rc!=SQLITE_OK ) goto status_done;
  }
  {struct TableEntry *aBase=aStaged?aStaged:aHead;
    int nBase=aStaged?nStaged:nHead;
    if(aWorking&&aBase) rc = compareCatalogs(pCur,db,aBase,nBase,aWorking,nWorking,0);
    else if(aWorking&&!aBase) rc = compareCatalogs(pCur,db,0,0,aWorking,nWorking,0);
    if( rc!=SQLITE_OK ) goto status_done;
  }

status_done:
  sqlite3_free(aHead);sqlite3_free(aStaged);sqlite3_free(aWorking);
  return rc;
}

static int statusNext(sqlite3_vtab_cursor *p){
  ((DoltliteStatusCursor*)p)->iRow++;
  return SQLITE_OK;
}
static int statusEof(sqlite3_vtab_cursor *p){
  return ((DoltliteStatusCursor*)p)->iRow >= ((DoltliteStatusCursor*)p)->nRows;
}
static int statusColumn(sqlite3_vtab_cursor *p,sqlite3_context *ctx,int c){
  DoltliteStatusCursor *pCur=(DoltliteStatusCursor*)p;
  StatusRow *r;
  if( pCur->iRow>=pCur->nRows ) return SQLITE_OK;
  r=&pCur->aRows[pCur->iRow];
  switch(c){
    case 0:sqlite3_result_text(ctx,r->zName,-1,SQLITE_TRANSIENT);break;
    case 1:sqlite3_result_int(ctx,r->staged);break;
    case 2:sqlite3_result_text(ctx,r->zStatus,-1,SQLITE_STATIC);break;
  }
  return SQLITE_OK;
}
static int statusRowid(sqlite3_vtab_cursor *p, sqlite3_int64 *r){
  *r = ((DoltliteStatusCursor*)p)->iRow;
  return SQLITE_OK;
}
static int statusBestIndex(sqlite3_vtab *v,sqlite3_index_info *p){(void)v;p->estimatedCost=100.0;return SQLITE_OK;}

static sqlite3_module doltliteStatusModule = {
  0,0,statusConnect,statusBestIndex,statusDisconnect,0,
  statusOpen,statusClose,statusFilter,statusNext,statusEof,
  statusColumn,statusRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteStatusRegister(sqlite3 *db){
  return sqlite3_create_module(db,"dolt_status",&doltliteStatusModule,0);
}

#endif
