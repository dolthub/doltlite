
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

typedef struct TagMutationCtx TagMutationCtx;
struct TagMutationCtx {
  const char *zName;
  ProllyHash commitHash;
  int isDelete;
  const char *zTagger;
  const char *zEmail;
  i64 timestamp;
  const char *zMessage;
};

static void tagResultError(
  sqlite3_context *ctx,
  int rc,
  const char *zNotFound,
  const char *zExists
){
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_error(ctx, zNotFound, -1);
  }else if( rc==SQLITE_ERROR && zExists ){
    sqlite3_result_error(ctx, zExists, -1);
  }else{
    sqlite3_result_error(ctx, sqlite3_errstr(rc), -1);
  }
}

static int mutateTagRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  TagMutationCtx *p = (TagMutationCtx*)pArg;
  (void)db;
  if( p->isDelete ) return chunkStoreDeleteTag(cs, p->zName);
  return chunkStoreAddTagFull(cs, p->zName, &p->commitHash,
                              p->zTagger, p->zEmail,
                              p->timestamp, p->zMessage);
}

static void doltTagFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  TagMutationCtx m;
  const char *arg0;
  const char *zMessage = 0;
  const char *zAuthor = 0;
  const char *zCommitRef = 0;
  char *zParsedTagger = 0;
  char *zParsedEmail = 0;
  int rc, i;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "tag name required", -1); return; }

  memset(&m, 0, sizeof(m));

  arg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !arg0 ){ sqlite3_result_error(ctx, "tag name required", -1); return; }

  /* Delete: dolt_tag('-d', name) — same shape as before. */
  if( strcmp(arg0, "-d")==0 || strcmp(arg0, "--delete")==0 ){
    const char *zName;
    if( argc<2 ){ sqlite3_result_error(ctx, "tag name required for delete", -1); return; }
    zName = (const char*)sqlite3_value_text(argv[1]);
    if( !zName ){ sqlite3_result_error(ctx, "tag name required", -1); return; }
    m.zName = zName;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateTagRef, &m);
    if( rc!=SQLITE_OK ){
      tagResultError(ctx, rc, "tag not found", 0);
      return;
    }
    sqlite3_result_int(ctx, 0);
    return;
  }

  /* Create: dolt_tag(name [, commit_ref] [, '-m', msg] [, '--author', 'name <email>'])
  ** The first non-flag positional argument after the name is treated as
  ** the commit ref to tag; if absent, the session HEAD is used. */
  for(i=1; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-m")==0 || strcmp(arg, "--message")==0 ){
      if( i+1<argc ) zMessage = (const char*)sqlite3_value_text(argv[++i]);
      else{ sqlite3_result_error(ctx, "-m requires a message", -1); return; }
    }else if( strcmp(arg, "--author")==0 ){
      if( i+1<argc ) zAuthor = (const char*)sqlite3_value_text(argv[++i]);
      else{ sqlite3_result_error(ctx, "--author requires 'name <email>'", -1); return; }
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(ctx, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(ctx);
      }
      return;
    }else if( !zCommitRef ){
      zCommitRef = arg;
    }else{
      sqlite3_result_error(ctx, "too many positional arguments to dolt_tag", -1);
      return;
    }
  }

  if( zCommitRef ){
    rc = doltliteResolveRef(db, zCommitRef, &m.commitHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "commit not found", -1);
      return;
    }
  }else{
    doltliteGetSessionHead(db, &m.commitHash);
    if( prollyHashIsEmpty(&m.commitHash) ){
      sqlite3_result_error(ctx, "no commits to tag", -1);
      return;
    }
  }

  if( zAuthor ){
    const char *lt = strchr(zAuthor, '<');
    const char *gt = lt ? strchr(lt, '>') : 0;
    if( lt && gt ){
      int nameLen = (int)(lt - zAuthor);
      while( nameLen>0 && zAuthor[nameLen-1]==' ' ) nameLen--;
      zParsedTagger = sqlite3_mprintf("%.*s", nameLen, zAuthor);
      zParsedEmail  = sqlite3_mprintf("%.*s", (int)(gt-lt-1), lt+1);
    }else{
      zParsedTagger = sqlite3_mprintf("%s", zAuthor);
      zParsedEmail  = sqlite3_mprintf("");
    }
    if( !zParsedTagger || !zParsedEmail ){
      sqlite3_free(zParsedTagger);
      sqlite3_free(zParsedEmail);
      sqlite3_result_error_nomem(ctx);
      return;
    }
    m.zTagger = zParsedTagger;
    m.zEmail  = zParsedEmail;
  }else{
    m.zTagger = doltliteGetAuthorName(db);
    m.zEmail  = doltliteGetAuthorEmail(db);
  }
  m.timestamp = (i64)time(0);
  m.zMessage = zMessage ? zMessage : "";
  m.zName = arg0;

  rc = doltliteMutateRefs(db, mutateTagRef, &m);
  sqlite3_free(zParsedTagger);
  sqlite3_free(zParsedEmail);
  if( rc!=SQLITE_OK ){
    tagResultError(ctx, rc, "tag not found", "tag already exists");
    return;
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
  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x("
      "tag_name TEXT, "
      "tag_hash TEXT, "
      "tagger TEXT, "
      "email TEXT, "
      "date TEXT, "
      "message TEXT"
    ")");
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
    case 0:
      sqlite3_result_text(ctx, t->zName, -1, SQLITE_TRANSIENT);
      break;
    case 1: {
      char h[PROLLY_HASH_SIZE*2+1];
      doltliteHashToHex(&t->commitHash, h);
      sqlite3_result_text(ctx, h, -1, SQLITE_TRANSIENT);
      break;
    }
    case 2:
      sqlite3_result_text(ctx, t->zTagger ? t->zTagger : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 3:
      sqlite3_result_text(ctx, t->zEmail ? t->zEmail : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 4: {
      time_t secs = (time_t)t->timestamp;
      struct tm *tm = gmtime(&secs);
      if( tm ){
        char buf[32];
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
        sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 5:
      sqlite3_result_text(ctx, t->zMessage ? t->zMessage : "",
                          -1, SQLITE_TRANSIENT);
      break;
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
