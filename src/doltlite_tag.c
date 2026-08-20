
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
  if( !doltliteUserRefNameIsValid(p->zName) ) return SQLITE_CONSTRAINT;
  return chunkStoreAddTagFull(cs, p->zName, &p->commitHash,
                              p->zTagger, p->zEmail,
                              p->timestamp, p->zMessage);
}

static void doltTagParsedFunc(
  sqlite3_context *ctx,
  int isDelete,
  int nPositional,
  const char **azPositional,
  const char *zMessage,
  const char *zAuthor
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  TagMutationCtx m;
  const char *arg0 = nPositional>0 ? azPositional[0] : 0;
  const char *zCommitRef = nPositional>1 ? azPositional[1] : 0;
  char *zParsedTagger = 0;
  char *zParsedEmail = 0;
  int rc;

  if( !cs ){ doltliteVcResultError(ctx, db, "no database"); return; }
  if( nPositional<1 ){ doltliteVcResultError(ctx, db, "tag name required"); return; }

  memset(&m, 0, sizeof(m));

  if( isDelete ){
    if( nPositional!=1 ){
      doltliteVcResultError(ctx, db, "too many positional arguments to dolt_tag");
      return;
    }
    m.zName = arg0;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateTagRef, &m);
    if( rc==SQLITE_CONSTRAINT && doltliteSessionHasUnresolvedConflicts(db) ){
      doltliteVcResultError(ctx, db,
        "cannot update refs: unresolved merge conflicts");
      return;
    }
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

  if( !doltliteUserRefNameIsValid(arg0) ){
    doltliteVcResultError(ctx, db, "invalid tag name");
    return;
  }


  if( nPositional>2 ){
    doltliteVcResultError(ctx, db, "too many positional arguments to dolt_tag");
    return;
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
  if( rc==SQLITE_CONSTRAINT && doltliteSessionHasUnresolvedConflicts(db) ){
    doltliteVcResultError(ctx, db,
      "cannot update refs: unresolved merge conflicts");
    return;
  }
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

static void doltTagFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  DoltliteCmdArgs args;
  int isDelete = 0;
  const char *zMessage = 0;
  const char *zAuthor = 0;
  DoltliteCmdOption aOption[] = {
    { "delete", 'd', DOLTLITE_CMD_OPTION_FLAG, &isDelete, 0 },
    { "message", 'm', DOLTLITE_CMD_OPTION_VALUE, 0, &zMessage },
    { "author", 0, DOLTLITE_CMD_OPTION_VALUE, 0, &zAuthor }
  };
  int rc;

  if( doltliteCmdRejectDetached(ctx) ) return;
  if( argc<1 ){
    doltliteVcResultError(ctx, sqlite3_context_db_handle(ctx),
                          "tag name required");
    return;
  }
  rc = doltliteCmdParseArgs(ctx, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ){
    tagSealSavepointError(ctx);
    return;
  }
  if( isDelete && (zMessage || zAuthor) ){
    doltliteCmdArgsClear(&args);
    doltliteVcResultError(ctx, sqlite3_context_db_handle(ctx),
                          "too many positional arguments to dolt_tag");
    return;
  }
  doltTagParsedFunc(ctx, isDelete, args.nPositional, args.azPositional,
                    zMessage, zAuthor);
  doltliteCmdArgsClear(&args);
}

typedef struct TagVtab TagVtab;
struct TagVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct TagCur TagCur;
struct TagCur {
  sqlite3_vtab_cursor base;
  int iRow;
  /* Cursor-owned copy of the tags visible at xFilter time. A dolt_tag()
  ** call evaluated in the same statement mutates and reallocates the live
  ** cs->refs arrays mid-scan, so rows must never be served from them. */
  int nRows;
  TagRef *aSnap;
};

static void tagCurClearSnapshot(TagCur *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aSnap[i].zName);
    sqlite3_free(pCur->aSnap[i].zTagger);
    sqlite3_free(pCur->aSnap[i].zEmail);
    sqlite3_free(pCur->aSnap[i].zMessage);
  }
  sqlite3_free(pCur->aSnap);
  pCur->aSnap = 0;
  pCur->nRows = 0;
  pCur->iRow = 0;
}

static int tagCurSnapshot(TagCur *pCur, const TagRef *aTg, int nTg){
  int i;
  tagCurClearSnapshot(pCur);
  if( nTg<=0 ) return SQLITE_OK;
  pCur->aSnap = (TagRef*)sqlite3_malloc64((sqlite3_uint64)nTg*sizeof(TagRef));
  if( !pCur->aSnap ) return SQLITE_NOMEM;
  memset(pCur->aSnap, 0, (size_t)nTg*sizeof(TagRef));
  for(i=0; i<nTg; i++){
    TagRef *pDst = &pCur->aSnap[i];
    const TagRef *pSrc = &aTg[i];
    pCur->nRows = i+1;
    pDst->commitHash = pSrc->commitHash;
    pDst->timestamp = pSrc->timestamp;
    pDst->zName = sqlite3_mprintf("%s", pSrc->zName ? pSrc->zName : "");
    pDst->zTagger = pSrc->zTagger ? sqlite3_mprintf("%s", pSrc->zTagger) : 0;
    pDst->zEmail = pSrc->zEmail ? sqlite3_mprintf("%s", pSrc->zEmail) : 0;
    pDst->zMessage = pSrc->zMessage ? sqlite3_mprintf("%s", pSrc->zMessage) : 0;
    if( !pDst->zName
     || (pSrc->zTagger && !pDst->zTagger)
     || (pSrc->zEmail && !pDst->zEmail)
     || (pSrc->zMessage && !pDst->zMessage) ){
      tagCurClearSnapshot(pCur);
      return SQLITE_NOMEM;
    }
  }
  return SQLITE_OK;
}

static int tagClose(sqlite3_vtab_cursor *pCursor){
  TagCur *pCur = (TagCur*)pCursor;
  tagCurClearSnapshot(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

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
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  int nTg = 0;
  const TagRef *aTg = 0;
  (void)idxStr;
  tagCurClearSnapshot(pCur);
  if( !cs ) return SQLITE_OK;
  refsTableGetTags(&cs->refs, &nTg, &aTg);
  if( idxNum==1 && argc==1 ){
    const char *zName = (const char*)sqlite3_value_text(argv[0]);
    int i = csFindNamedRef(cs->refs.aTags, cs->refs.nTags,
                           (int)sizeof(TagRef), zName);
    if( i<0 ) return SQLITE_OK;
    aTg += i;
    nTg = 1;
  }
  return tagCurSnapshot(pCur, aTg, nTg);
}
static int tagNext(sqlite3_vtab_cursor *pCursor){
  ((TagCur*)pCursor)->iRow++;
  return SQLITE_OK;
}
static int tagEof(sqlite3_vtab_cursor *pCursor){
  TagCur *pCur = (TagCur*)pCursor;
  return pCur->iRow >= pCur->nRows;
}
static int tagColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col){
  TagCur *pCur = (TagCur*)pCursor;
  const TagRef *t;
  if( pCur->iRow >= pCur->nRows ) return SQLITE_OK;
  t = &pCur->aSnap[pCur->iRow];
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
  tagOpen,tagClose,tagFilter,tagNext,tagEof,tagColumn,tagRowid,
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
