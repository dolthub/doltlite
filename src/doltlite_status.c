/*
** dolt_status virtual table: shows working and staged changes.
**
** Usage:
**   SELECT * FROM dolt_status;
**   -- table_name | staged | status
**   -- users      | 0      | modified
**   -- users      | 0      | schema modified
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);

struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  char *zName;
};
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* -------------------------------------------------------------------------- */

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

static struct TableEntry *findInCatalog(struct TableEntry *a, int n, Pgno iTable){
  int i; for(i=0;i<n;i++) if(a[i].iTable==iTable) return &a[i]; return 0;
}

static void addRow(DoltliteStatusCursor *pCur, const char *zName,
                   int staged, const char *zStatus){
  StatusRow *aNew = sqlite3_realloc(pCur->aRows,
      (pCur->nRows+1)*(int)sizeof(StatusRow));
  if(aNew){
    pCur->aRows = aNew;
    aNew[pCur->nRows].zName = sqlite3_mprintf("%s", zName);
    aNew[pCur->nRows].staged = staged;
    aNew[pCur->nRows].zStatus = zStatus;
    pCur->nRows++;
  }
}

static void compareCatalogs(
  DoltliteStatusCursor *pCur, sqlite3 *db,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  int staged
){
  int i;
  for(i=0; i<nTo; i++){
    struct TableEntry *pFrom;
    char *zName;
    if(aTo[i].iTable<=1) continue;
    pFrom = findInCatalog(aFrom, nFrom, aTo[i].iTable);
    zName = aTo[i].zName ? sqlite3_mprintf("%s",aTo[i].zName)
                         : doltliteResolveTableNumber(db, aTo[i].iTable);
    if(!zName) continue;
    if(!pFrom){
      addRow(pCur, zName, staged, "new table");
    }else{
      /* Check data changes */
      if(prollyHashCompare(&pFrom->root, &aTo[i].root)!=0){
        addRow(pCur, zName, staged, "modified");
      }
      /* Check schema changes (schemaHash differs) */
      if(!prollyHashIsEmpty(&pFrom->schemaHash)
       && !prollyHashIsEmpty(&aTo[i].schemaHash)
       && prollyHashCompare(&pFrom->schemaHash, &aTo[i].schemaHash)!=0){
        addRow(pCur, zName, staged, "schema modified");
      }
    }
    sqlite3_free(zName);
  }
  for(i=0; i<nFrom; i++){
    char *zName;
    if(aFrom[i].iTable<=1) continue;
    if(!findInCatalog(aTo, nTo, aFrom[i].iTable)){
      zName = aFrom[i].zName ? sqlite3_mprintf("%s",aFrom[i].zName)
                              : doltliteResolveTableNumber(db, aFrom[i].iTable);
      if(!zName) zName = sqlite3_mprintf("table_%d", aFrom[i].iTable);
      addRow(pCur, zName, staged, "deleted");
      sqlite3_free(zName);
    }
  }
}

/* -------------------------------------------------------------------------- */

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
  int i;for(i=0;i<c->nRows;i++)sqlite3_free(c->aRows[i].zName);
  sqlite3_free(c->aRows);sqlite3_free(c);return SQLITE_OK;
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

  {int i;for(i=0;i<pCur->nRows;i++)sqlite3_free(pCur->aRows[i].zName);}
  sqlite3_free(pCur->aRows);pCur->aRows=0;pCur->nRows=0;pCur->iRow=0;
  if(!cs) return SQLITE_OK;

  rc=doltliteGetHeadCatalogHash(db,&headCatHash);
  if(rc==SQLITE_OK) doltliteLoadCatalog(db,&headCatHash,&aHead,&nHead,0);

  chunkStoreGetStagedCatalog(cs,&stagedCatHash);
  if(!prollyHashIsEmpty(&stagedCatHash))
    doltliteLoadCatalog(db,&stagedCatHash,&aStaged,&nStaged,0);

  {u8 *catData=0;int nCatData=0;
    rc=doltliteFlushAndSerializeCatalog(db,&catData,&nCatData);
    if(rc==SQLITE_OK){
      rc=chunkStorePut(cs,catData,nCatData,&workingCatHash);
      sqlite3_free(catData);
      if(rc==SQLITE_OK) doltliteLoadCatalog(db,&workingCatHash,&aWorking,&nWorking,0);
    }
  }

  if(aStaged&&aHead) compareCatalogs(pCur,db,aHead,nHead,aStaged,nStaged,1);
  {struct TableEntry *aBase=aStaged?aStaged:aHead;
    int nBase=aStaged?nStaged:nHead;
    if(aWorking&&aBase) compareCatalogs(pCur,db,aBase,nBase,aWorking,nWorking,0);
    else if(aWorking&&!aBase) compareCatalogs(pCur,db,0,0,aWorking,nWorking,0);
  }

  sqlite3_free(aHead);sqlite3_free(aStaged);sqlite3_free(aWorking);
  return SQLITE_OK;
}

static int statusNext(sqlite3_vtab_cursor *p){((DoltliteStatusCursor*)p)->iRow++;return SQLITE_OK;}
static int statusEof(sqlite3_vtab_cursor *p){return((DoltliteStatusCursor*)p)->iRow>=((DoltliteStatusCursor*)p)->nRows;}
static int statusColumn(sqlite3_vtab_cursor *p,sqlite3_context *ctx,int c){
  DoltliteStatusCursor *pCur=(DoltliteStatusCursor*)p;
  StatusRow *r=&pCur->aRows[pCur->iRow];
  switch(c){
    case 0:sqlite3_result_text(ctx,r->zName,-1,SQLITE_TRANSIENT);break;
    case 1:sqlite3_result_int(ctx,r->staged);break;
    case 2:sqlite3_result_text(ctx,r->zStatus,-1,SQLITE_STATIC);break;
  }
  return SQLITE_OK;
}
static int statusRowid(sqlite3_vtab_cursor *p,sqlite3_int64 *r){*r=((DoltliteStatusCursor*)p)->iRow;return SQLITE_OK;}
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

#endif /* DOLTLITE_PROLLY */
