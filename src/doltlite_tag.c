
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

static void tagSealSavepointError(sqlite3_context *ctx){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  (void)doltliteVcSealSavepointError(db);
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

  if( !cs ){ doltliteVcResultError(ctx, db, "no database"); return; }
  if( argc<1 ){ doltliteVcResultError(ctx, db, "tag name required"); return; }

  memset(&m, 0, sizeof(m));

  arg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !arg0 ){ doltliteVcResultError(ctx, db, "tag name required"); return; }


  if( strcmp(arg0, "-d")==0 || strcmp(arg0, "--delete")==0 ){
    const char *zName;
    if( argc<2 ){ doltliteVcResultError(ctx, db, "tag name required for delete"); return; }
    if( argc!=2 ){
      doltliteVcResultError(ctx, db, "too many positional arguments to dolt_tag");
      return;
    }
    zName = (const char*)sqlite3_value_text(argv[1]);
    if( !zName ){ doltliteVcResultError(ctx, db, "tag name required"); return; }
    m.zName = zName;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateTagRef, &m);
    if( rc!=SQLITE_OK ){
      tagSealSavepointError(ctx);
      doltliteRefResultError(ctx, rc, "tag not found", 0);
      return;
    }
    rc = doltliteVcSealActiveSavepoints(db);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);
    return;
  }


  for(i=1; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-m")==0 || strcmp(arg, "--message")==0 ){
      if( i+1<argc ) zMessage = (const char*)sqlite3_value_text(argv[++i]);
      else{ doltliteVcResultError(ctx, db, "-m requires a message"); return; }
    }else if( strcmp(arg, "--author")==0 ){
      if( i+1<argc ) zAuthor = (const char*)sqlite3_value_text(argv[++i]);
      else{ doltliteVcResultError(ctx, db, "--author requires 'name <email>'"); return; }
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        tagSealSavepointError(ctx);
        sqlite3_result_error(ctx, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(ctx);
      }
      return;
    }else if( !zCommitRef ){
      zCommitRef = arg;
    }else{
      doltliteVcResultError(ctx, db, "too many positional arguments to dolt_tag");
      return;
    }
  }

  if( zCommitRef ){
    rc = doltliteResolveRef(db, zCommitRef, &m.commitHash);
    if( rc!=SQLITE_OK ){
      doltliteVcResultError(ctx, db, "commit not found");
      return;
    }
  }else{
    doltliteGetSessionHead(db, &m.commitHash);
    if( prollyHashIsEmpty(&m.commitHash) ){
      doltliteVcResultError(ctx, db, "no commits to tag");
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
      tagSealSavepointError(ctx);
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
    tagSealSavepointError(ctx);
    doltliteRefResultError(ctx, rc, "tag not found", "tag already exists");
    return;
  }
  rc = doltliteVcSealActiveSavepoints(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

typedef struct TagVtab TagVtab;
struct TagVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct TagCur TagCur;
struct TagCur { sqlite3_vtab_cursor base; int iRow; int bSingle; int iSingle; };

static int tagConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  TagVtab *pVtab;
  int rc;
  (void)pAux;
  (void)argc;
  (void)argv;
  (void)pzErr;
  rc = doltliteVtabConnectSimple(db,
    "CREATE TABLE x("
      "tag_name TEXT, "
      "tag_hash TEXT, "
      "tagger TEXT, "
      "email TEXT, "
      "date TEXT, "
      "message TEXT"
    ")",
    sizeof(*pVtab), ppVtab);
  if( rc != SQLITE_OK ) return rc;
  pVtab = (TagVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}
static int tagOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(TagCur));
}
static int tagFilter(sqlite3_vtab_cursor *pCursor, int idxNum,
    const char *idxStr, int argc, sqlite3_value **argv){
  TagVtab *pVtab = (TagVtab*)pCursor->pVtab;
  TagCur *pCur = (TagCur*)pCursor;
  (void)idxStr;
  pCur->iRow = 0;
  pCur->bSingle = 0;
  pCur->iSingle = -1;
  if( idxNum==1 && argc==1 ){
    ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
    const char *zName = (const char*)sqlite3_value_text(argv[0]);
    pCur->bSingle = 1;
    pCur->iSingle = cs ? csFindNamedRef(cs->refs.aTags, cs->refs.nTags,
                                        (int)sizeof(TagRef), zName) : -1;
    pCur->iRow = pCur->iSingle;
  }
  return SQLITE_OK;
}
static int tagNext(sqlite3_vtab_cursor *pCursor){
  ((TagCur*)pCursor)->iRow++;
  return SQLITE_OK;
}
static int tagEof(sqlite3_vtab_cursor *pCursor){
  TagVtab *pVtab = (TagVtab*)pCursor->pVtab;
  TagCur *pCur = (TagCur*)pCursor;
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  if( !cs ) return 1;
  if( pCur->bSingle ) return pCur->iSingle<0 || pCur->iRow!=pCur->iSingle;
  return pCur->iRow >= refsTableTagCount(&cs->refs);
}
static int tagColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col){
  TagVtab *pVtab = (TagVtab*)pCursor->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  const TagRef *t;
  int nTg;
  const TagRef *aTg;
  if( !cs ) return SQLITE_OK;
  refsTableGetTags(&cs->refs, &nTg, &aTg);
  t = &aTg[((TagCur*)pCursor)->iRow];
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
      doltliteResultTimestamp(ctx, t->timestamp);
      break;
    }
    case 5:
      sqlite3_result_text(ctx, t->zMessage ? t->zMessage : "",
                          -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}
static int tagRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((TagCur*)pCursor)->iRow;
  return SQLITE_OK;
}
static int tagBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10;
  pInfo->estimatedRows = 5;
  return doltliteBestIndexEq(pInfo, 0);
}

static sqlite3_module tagModule = {
  0,0,tagConnect,tagBestIndex,doltliteVtabDisconnect,0,
  tagOpen,doltliteVtabClose,tagFilter,tagNext,tagEof,tagColumn,tagRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteTagRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_tag", -1,
                               DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                               doltTagFunc, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3_create_module(db, "dolt_tags", &tagModule, 0);
  return rc;
}

#endif
