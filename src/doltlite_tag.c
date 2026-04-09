
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include <string.h>

typedef struct TagMutationCtx TagMutationCtx;
struct TagMutationCtx {
  const char *zName;
  ProllyHash commitHash;
  int isDelete;
};

static int mutateTagRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  TagMutationCtx *p = (TagMutationCtx*)pArg;
  (void)db;
  if( p->isDelete ) return chunkStoreDeleteTag(cs, p->zName);
  return chunkStoreAddTag(cs, p->zName, &p->commitHash);
}

static void doltTagFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  TagMutationCtx m;
  const char *arg0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "tag name required", -1); return; }

  memset(&m, 0, sizeof(m));

  arg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !arg0 ){ sqlite3_result_error(ctx, "tag name required", -1); return; }

  if( strcmp(arg0, "-d")==0 || strcmp(arg0, "--delete")==0 ){
    const char *zName;
    if( argc<2 ){ sqlite3_result_error(ctx, "tag name required for delete", -1); return; }
    zName = (const char*)sqlite3_value_text(argv[1]);
    if( !zName ){ sqlite3_result_error(ctx, "tag name required", -1); return; }
    m.zName = zName;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateTagRef, &m);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "tag not found", -1);
      return;
    }
  }else{
    
    if( argc>=2 ){
      
      const char *zHash = (const char*)sqlite3_value_text(argv[1]);
      if( !zHash ){ sqlite3_result_error(ctx, "invalid commit hash", -1); return; }
      rc = doltliteHexToHash(zHash, &m.commitHash);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error(ctx, "invalid commit hash format", -1);
        return;
      }
    }else{
      
      doltliteGetSessionHead(db, &m.commitHash);
      if( prollyHashIsEmpty(&m.commitHash) ){
        sqlite3_result_error(ctx, "no commits to tag", -1);
        return;
      }
    }
    m.zName = arg0;
    rc = doltliteMutateRefs(db, mutateTagRef, &m);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "tag already exists", -1);
      return;
    }
  }

  sqlite3_result_int(ctx, 0);
}

typedef struct TagVtab TagVtab;
struct TagVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct TagCur TagCur;
struct TagCur { sqlite3_vtab_cursor base; int iRow; };

static int tagConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  TagVtab *v; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(name TEXT, hash TEXT, commit_message TEXT)");
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v)); v->db = db;
  *ppVtab = &v->base;
  return SQLITE_OK;
}
static int tagDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int tagOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  TagCur *c = sqlite3_malloc(sizeof(*c)); (void)v;
  if(!c) return SQLITE_NOMEM; memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}
static int tagClose(sqlite3_vtab_cursor *c){ sqlite3_free(c); return SQLITE_OK; }
static int tagFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  (void)n;(void)s;(void)a;(void)v;
  ((TagCur*)c)->iRow=0; return SQLITE_OK;
}
static int tagNext(sqlite3_vtab_cursor *c){ ((TagCur*)c)->iRow++; return SQLITE_OK; }
static int tagEof(sqlite3_vtab_cursor *c){
  TagVtab *v = (TagVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  return !cs || ((TagCur*)c)->iRow >= cs->nTags;
}
static int tagColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  TagVtab *v = (TagVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  struct TagRef *t;
  if(!cs) return SQLITE_OK;
  t = &cs->aTags[((TagCur*)c)->iRow];
  switch(col){
    case 0: sqlite3_result_text(ctx, t->zName, -1, SQLITE_TRANSIENT); break;
    case 1: {
      char h[PROLLY_HASH_SIZE*2+1];
      doltliteHashToHex(&t->commitHash, h);
      sqlite3_result_text(ctx, h, -1, SQLITE_TRANSIENT);
      break;
    }
    case 2: {
      DoltliteCommit commit;
      if( doltliteLoadCommit(v->db, &t->commitHash, &commit)==SQLITE_OK ){
        sqlite3_result_text(ctx, commit.zMessage?commit.zMessage:"", -1, SQLITE_TRANSIENT);
        doltliteCommitClear(&commit);
      }
      break;
    }
  }
  return SQLITE_OK;
}
static int tagRowid(sqlite3_vtab_cursor *c, sqlite3_int64 *r){
  *r=((TagCur*)c)->iRow; return SQLITE_OK;
}
static int tagBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v; p->estimatedCost=10; p->estimatedRows=5; return SQLITE_OK;
}

static sqlite3_module tagModule = {
  0,0,tagConnect,tagBestIndex,tagDisconnect,0,
  tagOpen,tagClose,tagFilter,tagNext,tagEof,tagColumn,tagRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteTagRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_tag", -1, SQLITE_UTF8, 0, doltTagFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_module(db, "dolt_tags", &tagModule, 0);
  return rc;
}

#endif 
