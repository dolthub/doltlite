#ifdef DOLTLITE_PROLLY
#include "sqliteInt.h"
#include "btree.h"
#include "chunk_store.h"
#include "prolly_node.h"
#include "doltlite_internal.h"
#include <math.h>

typedef struct DlDump DlDump;
struct DlDump {
  sqlite3 *db;
  sqlite3 *out;
  SchemaEntry *aSchema;
  int nSchema;
  const char *zRef;
  char zHash[PROLLY_HASH_SIZE*2+1];
  int working;
  ProllyHash catalog;
  int (*xWrite)(void*, const void*, int);
  void *pCtx;
  char **pzErr;
};

static int dumpError(DlDump *p, int rc, const char *zObject, const char *zWhy){
  if( p->pzErr && !*p->pzErr ){
    *p->pzErr = sqlite3_mprintf("dolt_dump: %s%s%s", zObject ? zObject : "",
        zObject ? ": " : "", zWhy ? zWhy : sqlite3_errstr(rc));
    if( !*p->pzErr ) return SQLITE_NOMEM;
  }
  return rc;
}

static int dumpBytes(DlDump *p, const void *bytes, int n){
  const char *z = bytes;
  if( p->db->mallocFailed ) return SQLITE_NOMEM;
  while( n>0 && p->xWrite ){
    int count = MIN(n,16384);
    int rc = p->xWrite(p->pCtx,z,count);
    if( rc!=SQLITE_OK ) return dumpError(p,rc,0,"output callback failed");
    z += count;
    n -= count;
  }
  return SQLITE_OK;
}

static int dumpWrite(DlDump *p, const char *z){
  if( !z ) return dumpError(p,SQLITE_NOMEM,0,0);
  return dumpBytes(p,z,sqlite3Strlen30(z));
}

static int dumpExec(DlDump *p, const char *zObject, const char *zSql){
  int rc = sqlite3_exec(p->out, zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) return dumpError(p, rc, zObject, sqlite3_errmsg(p->out));
  return rc;
}

static int dumpSkip(const char *z){
  return !z || sqlite3_strnicmp(z, "sqlite_", 7)==0
      || sqlite3_stricmp(z, "dolt_rebase")==0
      || sqlite3_stricmp(z, "dolt_nonlocal_tables")==0;
}

static int dumpVirtual(const SchemaEntry *e){
  return e->zSql && sqlite3_strnicmp(e->zSql, "CREATE VIRTUAL TABLE", 20)==0;
}

static const char *dumpToken(const char *z, int *type, int *length){
  do{
    *length = sqlite3GetToken((const u8*)z,type);
    if( *type!=TK_SPACE ) return z;
    z += *length;
  }while( *z );
  *length = 0;
  return z;
}

static char *dumpMainSql(const SchemaEntry *e){
  const char *z = e->zSql, *name;
  int type = 0, n = 0, kind;
  if( !strcmp(e->zType,"table") ) kind = TK_TABLE;
  else if( !strcmp(e->zType,"index") ) kind = TK_INDEX;
  else if( !strcmp(e->zType,"view") ) kind = TK_VIEW;
  else if( !strcmp(e->zType,"trigger") ) kind = TK_TRIGGER;
  else return sqlite3_mprintf("%s",z);
  do{
    z = dumpToken(z,&type,&n);
    z += n;
  }while( n && type!=kind );
  if( !n ) return 0;
  name = dumpToken(z,&type,&n);
  if( type==TK_IF ){
    do{
      z = dumpToken(name+n,&type,&n);
      name = z;
    }while( n && type!=TK_EXISTS );
    name = dumpToken(name+n,&type,&n);
  }
  if( !n ) return 0;
  dumpToken(name+n,&type,&kind);
  if( type==TK_DOT ){
    return sqlite3_mprintf("%.*smain%s",(int)(name-e->zSql),e->zSql,name+n);
  }
  return sqlite3_mprintf("%.*smain.%s",(int)(name-e->zSql),e->zSql,name);
}

static int dumpCreate(DlDump *p, SchemaEntry *e){
  Table *t;
  int rc = dumpExec(p, e->zName, e->zSql);
  if( rc!=SQLITE_OK ) return rc;
  t = sqlite3FindTable(p->out, e->zName, "main");
  if( t && IsVirtual(t) ){
    const char *z = t->u.vtab.azArg[0];
    if( sqlite3_stricmp(z,"fts3") && sqlite3_stricmp(z,"fts4")
     && sqlite3_stricmp(z,"fts5") && sqlite3_stricmp(z,"rtree")
     && sqlite3_stricmp(z,"rtree_i32") ){
      return dumpError(p, SQLITE_ERROR, e->zName,
                       "virtual-table module is not part of ordinary SQLite");
    }
  }
  return rc;
}

static int dumpSchema(DlDump *p){
  int pass, i, rc = SQLITE_OK;
  for(pass=0; pass<2 && rc==SQLITE_OK; pass++){
    for(i=0; i<p->nSchema && rc==SQLITE_OK; i++){
      SchemaEntry *e = &p->aSchema[i];
      Table *t;
      if( dumpSkip(e->zName) || !e->zSql || !e->zSql[0]
       || strcmp(e->zType,"table") || dumpVirtual(e)!=(pass==0) ) continue;
      t = sqlite3FindTable(p->out, e->zName, "main");
      if( t && (t->tabFlags & TF_Shadow)!=0 ) continue;
      rc = dumpCreate(p, e);
    }
  }
  return rc;
}

static int dumpValue(DlDump *p, sqlite3_stmt *q, int i){
  int type = sqlite3_column_type(q,i);
  char number[64];
  if( type==SQLITE_NULL ) return dumpWrite(p,"NULL");
  if( type==SQLITE_INTEGER ){
    sqlite3_snprintf(sizeof(number),number,"%lld",sqlite3_column_int64(q,i));
    return dumpWrite(p,number);
  }
  if( type==SQLITE_FLOAT ){
    double value = sqlite3_column_double(q,i);
    if( value==0.0 && signbit(value) ) return dumpWrite(p,"-0.0");
    sqlite3_snprintf(sizeof(number),number,"%!0.17g",value);
    return dumpWrite(p,number);
  }
  {
    const unsigned char *bytes = type==SQLITE_TEXT
        ? sqlite3_column_text(q,i) : sqlite3_column_blob(q,i);
    int n = sqlite3_column_bytes(q,i), j, rc, plain = type==SQLITE_TEXT;
    if( p->db->mallocFailed ) return SQLITE_NOMEM;
    for(j=0; plain && j<n; j++){
      if( bytes[j]<32 || bytes[j]>=127 ) plain = 0;
    }
    if( plain ){
      int begin = 0;
      rc = dumpWrite(p,"'");
      for(j=0; j<n && rc==SQLITE_OK; j++){
        if( bytes[j]=='\'' ){
          rc = dumpBytes(p,bytes+begin,j-begin);
          if( rc==SQLITE_OK ) rc = dumpWrite(p,"''");
          begin = j+1;
        }
      }
      if( rc==SQLITE_OK ) rc = dumpBytes(p,bytes+begin,n-begin);
      if( rc==SQLITE_OK ) rc = dumpWrite(p,"'");
      return rc;
    }
    rc = dumpWrite(p,type==SQLITE_TEXT ? "CAST(X'" : "X'");
    for(j=0; j<n && rc==SQLITE_OK; ){
      static const char hex[] = "0123456789ABCDEF";
      char buf[8192];
      int k = 0;
      while( j<n && k<(int)sizeof(buf) ){
        buf[k++] = hex[bytes[j]>>4];
        buf[k++] = hex[bytes[j++]&15];
      }
      rc = dumpBytes(p,buf,k);
    }
    if( rc==SQLITE_OK ) rc = dumpWrite(p,type==SQLITE_TEXT ? "' AS TEXT)" : "'");
    return rc;
  }
}

static int dumpTable(DlDump *p, const char *zName){
  sqlite3_stmt *cols = 0, *read = 0, *insert = 0;
  sqlite3_str *names = sqlite3_str_new(p->db);
  sqlite3_str *params = sqlite3_str_new(p->db);
  sqlite3_str *readNames = sqlite3_str_new(p->db);
  char *zNames = 0, *zParams = 0, *zRead = 0, *zInsert = 0, *zReadNames = 0;
  int nCol = 0, rc, i;
  {
    Table *t = sqlite3FindTable(p->out,zName,"main");
    ProllyHash root;
    u8 flags = 0;
    rc = t ? doltliteLoadTableRootByName(p->db,&p->catalog,zName,&root,&flags,0)
           : SQLITE_CORRUPT;
    if( rc==SQLITE_OK && HasRowid(t) && t->iPKey<0 && (flags & PROLLY_NODE_INTKEY) ){
      const char *aliases[] = {"rowid","_rowid_","oid"};
      for(i=0; i<3; i++){
        if( sqlite3ColumnIndex(t,aliases[i])<0 ){
          sqlite3_str_appendf(names,"\"%w\"",aliases[i]);
          sqlite3_str_appendf(readNames,"src.\"%w\"",aliases[i]);
          sqlite3_str_appendall(params,"?");
          nCol++;
          break;
        }
      }
    }
  }
  if( rc==SQLITE_OK ){
    char *pragma = sqlite3_mprintf("PRAGMA main.table_xinfo(%Q)",zName);
    rc = pragma ? sqlite3_prepare_v2(p->out,pragma,-1,&cols,0) : SQLITE_NOMEM;
    sqlite3_free(pragma);
  }
  while( rc==SQLITE_OK && sqlite3_step(cols)==SQLITE_ROW ){
    if( sqlite3_column_int(cols,6)!=0 ) continue;
    sqlite3_str_appendf(names,"%s\"%w\"",nCol ? "," : "",
        sqlite3_column_text(cols,1));
    sqlite3_str_appendf(readNames,"%ssrc.\"%w\"",nCol ? "," : "",
        sqlite3_column_text(cols,1));
    sqlite3_str_appendf(params,"%s?",nCol ? "," : "");
    nCol++;
  }
  if( rc==SQLITE_OK ) rc = sqlite3_finalize(cols);
  else sqlite3_finalize(cols);
  zNames = sqlite3_str_finish(names);
  zParams = sqlite3_str_finish(params);
  zReadNames = sqlite3_str_finish(readNames);
  if( rc==SQLITE_OK && (!zNames || !zParams || !zReadNames) ) rc = SQLITE_NOMEM;
  if( rc==SQLITE_OK ){
    if( p->working ) zRead = sqlite3_mprintf("SELECT %s FROM main.\"%w\" AS src",zReadNames,zName);
    else zRead = sqlite3_mprintf("SELECT %s FROM \"dolt_at_%w\"(%Q) AS src",zReadNames,zName,p->zRef);
    zInsert = sqlite3_mprintf("INSERT INTO main.\"%w\"(%s) VALUES(%s)",zName,zNames,zParams);
    if( !zRead || !zInsert ) rc = SQLITE_NOMEM;
  }
  if( rc==SQLITE_OK ) rc = sqlite3_prepare_v2(p->db,zRead,-1,&read,0);
  if( rc!=SQLITE_OK ){
    dumpError(p,rc,zName,sqlite3_errmsg(p->db));
    goto table_done;
  }
  rc = sqlite3_prepare_v2(p->out,zInsert,-1,&insert,0);
  while( rc==SQLITE_OK ){
    int step = sqlite3_step(read);
    if( step==SQLITE_DONE ) break;
    if( step!=SQLITE_ROW ){
      rc = dumpError(p,step,zName,sqlite3_errmsg(p->db));
      break;
    }
    for(i=0; i<nCol && rc==SQLITE_OK; i++){
      rc = sqlite3_bind_value(insert,i+1,sqlite3_column_value(read,i));
    }
    if( rc==SQLITE_OK ) rc = sqlite3_step(insert);
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK ) dumpError(p,rc,zName,sqlite3_errmsg(p->out));
    sqlite3_reset(insert);
    if( rc==SQLITE_OK && p->xWrite ){
      char *prefix = sqlite3_mprintf("INSERT INTO main.\"%w\"(%s) VALUES(",zName,zNames);
      rc = dumpWrite(p,prefix);
      sqlite3_free(prefix);
      for(i=0; i<nCol && rc==SQLITE_OK; i++){
        if( i ) rc = dumpWrite(p,",");
        if( rc==SQLITE_OK ) rc = dumpValue(p,read,i);
      }
      if( rc==SQLITE_OK ) rc = dumpWrite(p,");\n");
    }
  }
table_done:
  sqlite3_finalize(read);
  sqlite3_finalize(insert);
  sqlite3_free(zRead);
  sqlite3_free(zInsert);
  sqlite3_free(zNames);
  sqlite3_free(zParams);
  sqlite3_free(zReadNames);
  return rc;
}

static int dumpObjects(DlDump *p){
  int i, rc = SQLITE_OK;
  for(i=0; i<p->nSchema && rc==SQLITE_OK; i++){
    SchemaEntry *e = &p->aSchema[i];
    char *z;
    if( dumpSkip(e->zName) || dumpSkip(e->zTblName)
     || !e->zSql || !e->zSql[0] || !strcmp(e->zType,"table") ) continue;
    rc = dumpExec(p,e->zName,e->zSql);
    z = sqlite3_mprintf("%s;\n",e->zSql);
    if( rc==SQLITE_OK ) rc = dumpWrite(p,z);
    sqlite3_free(z);
  }
  return rc;
}

static int dumpMeta(DlDump *p, const ProllyHash *cat){
  u32 userVersion = 0, applicationId = 0;
  char sql[128];
  int rc = doltliteCatalogReadMeta(p->db,cat,&userVersion,&applicationId);
  sqlite3_snprintf(sizeof(sql),sql,
      "PRAGMA user_version=%d;\nPRAGMA application_id=%d;\n",
      (int)userVersion,(int)applicationId);
  if( rc==SQLITE_OK ) rc = dumpExec(p,0,sql);
  if( rc==SQLITE_OK ) rc = dumpWrite(p,sql);
  return rc;
}

static int dumpDoltliteFunction(const char *z){
  return !sqlite3_strnicmp(z,"dolt_",5) || !sqlite3_strnicmp(z,"doltlite_",9)
      || !sqlite3_stricmp(z,"active_branch");
}

static int dumpDependencyExpr(Walker *w, Expr *e){
  Parse *parse = w->pParse;
  if( parse->rc!=SQLITE_OK ) return WRC_Abort;
  if( e->op==TK_FUNCTION ){
    const char *z = e->u.zToken;
    if( dumpDoltliteFunction(z) ){
      sqlite3ErrorMsg(parse,"not authorized to use function: %s",z);
    }else if( !sqlite3FindFunction(parse->db,z,-2,SQLITE_UTF8,0) ){
      sqlite3ErrorMsg(parse,"no such function: %s",z);
    }else{
      sqlite3AuthCheck(parse,SQLITE_FUNCTION,0,z,0);
    }
  }else if( e->op==TK_COLLATE ){
    sqlite3GetCollSeq(parse,SQLITE_UTF8,0,e->u.zToken);
  }
  return parse->rc==SQLITE_OK ? WRC_Continue : WRC_Abort;
}

static int dumpDependencySelect(Walker *w, Select *s){
  int i;
  if( w->pParse->rc!=SQLITE_OK ) return WRC_Abort;
#ifndef SQLITE_OMIT_CTE
  if( s->pWith ){
    for(i=0; i<s->pWith->nCte; i++){
      if( sqlite3WalkSelect(w,s->pWith->a[i].pSelect) ) return WRC_Abort;
    }
  }
#endif
  for(i=0; s->pSrc && i<s->pSrc->nSrc; i++){
    SrcItem *item = &s->pSrc->a[i];
    if( item->fg.isTabFunc ){
      if( dumpDoltliteFunction(item->zName) ){
        sqlite3ErrorMsg(w->pParse,"not authorized to use function: %s",item->zName);
      }else{
        sqlite3LocateTable(w->pParse,0,item->zName,"main");
      }
      if( w->pParse->rc!=SQLITE_OK ) return WRC_Abort;
    }
    if( !item->fg.isUsing && sqlite3WalkExpr(w,item->u3.pOn) ) return WRC_Abort;
  }
  return WRC_Continue;
}

static int dumpValidateDependencies(DlDump *p, Table *table){
  Parse parse;
  Walker w;
  Trigger *trigger;
  int rc;
  sqlite3_mutex_enter(p->out->mutex);
  sqlite3ParseObjectInit(&parse,p->out);
  memset(&w,0,sizeof(w));
  w.pParse = &parse;
  w.xExprCallback = dumpDependencyExpr;
  w.xSelectCallback = dumpDependencySelect;
  w.xSelectCallback2 = sqlite3WalkWinDefnDummyCallback;
  parse.zAuthContext = table->zName;
  if( IsView(table) ) sqlite3WalkSelect(&w,table->u.view.pSelect);
  for(trigger=table->pTrigger; trigger && !parse.rc; trigger=trigger->pNext){
    TriggerStep *step;
    parse.zAuthContext = trigger->zName;
    sqlite3WalkExpr(&w,trigger->pWhen);
    for(step=trigger->step_list; step && !parse.rc; step=step->pNext){
      Upsert *upsert;
      Select select;
      memset(&select,0,sizeof(select));
      select.pSrc = step->pSrc;
      sqlite3WalkSelect(&w,&select);
      sqlite3WalkSelect(&w,step->pSelect);
      sqlite3WalkExpr(&w,step->pWhere);
      sqlite3WalkExprList(&w,step->pExprList);
      for(upsert=step->pUpsert; upsert && !parse.rc; upsert=upsert->pNextUpsert){
        sqlite3WalkExprList(&w,upsert->pUpsertTarget);
        sqlite3WalkExpr(&w,upsert->pUpsertTargetWhere);
        sqlite3WalkExprList(&w,upsert->pUpsertSet);
        sqlite3WalkExpr(&w,upsert->pUpsertWhere);
      }
    }
  }
  rc = p->out->mallocFailed ? SQLITE_NOMEM : parse.rc;
  if( rc!=SQLITE_OK ) dumpError(p,rc,table->zName,parse.zErrMsg);
  sqlite3DbFree(p->out,parse.zErrMsg);
  sqlite3ParseObjectReset(&parse);
  sqlite3_mutex_leave(p->out->mutex);
  return rc;
}

static int dumpValidatePrepare(DlDump *p, Table *table, const char *sql){
  sqlite3_stmt *q = 0;
  int rc = sqlite3_prepare_v2(p->out,sql,-1,&q,0);
  sqlite3_finalize(q);
  if( rc==SQLITE_ERROR
   && sqlite3_strnicmp(sqlite3_errmsg(p->out),"no such table:",14)==0 ){
    rc = dumpValidateDependencies(p,table);
  }
  return rc;
}

static int dumpValidate(DlDump *p){
  int i, rc = SQLITE_OK;
  for(i=0; i<p->nSchema && rc==SQLITE_OK; i++){
    SchemaEntry *e = &p->aSchema[i];
    Table *table;
    Trigger *trigger;
    if( dumpSkip(e->zName) ) continue;
    table = sqlite3FindTable(p->out,e->zName,"main");
    if( !table ) continue;
    if( IsView(table) ){
      char *sql = sqlite3_mprintf("SELECT * FROM main.\"%w\" LIMIT 0",e->zName);
      if( !sql ) return SQLITE_NOMEM;
      rc = dumpValidatePrepare(p,table,sql);
      sqlite3_free(sql);
    }
    for(trigger=table->pTrigger; trigger && rc==SQLITE_OK; trigger=trigger->pNext){
      sqlite3_str *text = sqlite3_str_new(p->out);
      char *sql;
      if( trigger->op==TK_INSERT ){
        sqlite3_str_appendf(text,"EXPLAIN INSERT INTO main.\"%w\" DEFAULT VALUES",e->zName);
      }else if( trigger->op==TK_DELETE ){
        sqlite3_str_appendf(text,"EXPLAIN DELETE FROM main.\"%w\"",e->zName);
      }else{
        int j, n = 0;
        sqlite3_str_appendf(text,"EXPLAIN UPDATE main.\"%w\" SET ",e->zName);
        for(j=0; j<table->nCol; j++){
          if( table->aCol[j].colFlags & COLFLAG_GENERATED ) continue;
          sqlite3_str_appendf(text,"%s\"%w\"=\"%w\"",n++ ? "," : "",
              table->aCol[j].zCnName,table->aCol[j].zCnName);
        }
      }
      sql = sqlite3_str_finish(text);
      if( !sql ) return SQLITE_NOMEM;
      rc = dumpValidatePrepare(p,table,sql);
      sqlite3_free(sql);
    }
    if( rc!=SQLITE_OK ) dumpError(p,rc,e->zName,sqlite3_errmsg(p->out));
  }
  return rc;
}

#ifndef SQLITE_OMIT_AUTHORIZATION
typedef struct DumpAuth DumpAuth;
struct DumpAuth {
  int (*callback)(void*,int,const char*,const char*,const char*,const char*);
  void *ctx;
};
static int dumpAuthorize(void *ctx, int op, const char *a, const char *b,
                         const char *db, const char *trigger){
  DumpAuth *saved = ctx;
  if( op==SQLITE_FUNCTION && b
   && dumpDoltliteFunction(b) ) return SQLITE_DENY;
  return saved->callback ? saved->callback(saved->ctx,op,a,b,db,trigger) : SQLITE_OK;
}
#endif

static int dumpCheckBindings(DlDump *p){
  sqlite3_stmt *q = 0;
  char *sql;
  int rc;
  if( !findSchemaEntry(p->aSchema,p->nSchema,"dolt_nonlocal_tables") ) return SQLITE_OK;
  sql = p->working
      ? sqlite3_mprintf("SELECT 1 FROM main.dolt_nonlocal_tables LIMIT 1")
      : sqlite3_mprintf("SELECT 1 FROM dolt_at_dolt_nonlocal_tables(%Q) LIMIT 1",p->zRef);
  if( !sql ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(p->db,sql,-1,&q,0);
  sqlite3_free(sql);
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(q);
    if( rc==SQLITE_ROW ) rc = dumpError(p,SQLITE_ERROR,"dolt_nonlocal_tables",
        "nonlocal table bindings require DoltLite");
    else if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  }
  sqlite3_finalize(q);
  return rc;
}

static int dumpRun(DlDump *p){
  ProllyHash cat;
  Btree *bt = p->db->aDb[0].pBt;
  int i, pass, rc, pinned = 0, ownedRead, fk = 0;
  int transaction = 0, haveFk = 0;
#ifndef SQLITE_OMIT_AUTHORIZATION
  DumpAuth auth;
  int haveAuth = 0;
#endif
  if( !doltliteGetChunkStore(p->db) ){
    return dumpError(p,SQLITE_ERROR,0,"source must be a DoltLite database");
  }
  ownedRead = sqlite3BtreeTxnState(bt)==SQLITE_TXN_NONE;
  rc = doltliteBeginReadSnapshot(p->db);
  if( rc!=SQLITE_OK ) goto dump_done;
  pinned = 1;
  p->working = doltliteRefIsWorking(p->zRef);
  rc = doltliteResolveCatalogHashForRef(p->db,p->zRef,&cat);
  if( rc!=SQLITE_OK ){
    dumpError(p,rc,p->zRef,"revision could not be resolved");
    goto dump_done;
  }
  p->catalog = cat;
  if( !p->working && !doltliteRefIsStaged(p->zRef) ){
    ProllyHash hash;
    rc = doltliteResolveRef(p->db,p->zRef,&hash);
    if( rc!=SQLITE_OK ) goto dump_done;
    doltliteHashToHex(&hash,p->zHash);
    p->zRef = p->zHash;
  }
  rc = loadSchemaFromCatalog(p->db,doltliteGetChunkStore(p->db),
      doltliteGetCache(p->db),&cat,&p->aSchema,&p->nSchema);
  if( rc==SQLITE_OK ) rc = dumpCheckBindings(p);
  if( rc!=SQLITE_OK ) goto dump_done;
  if( !p->out ){
    rc = sqlite3_open_v2("",&p->out,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,0);
  }else if( !sqlite3BtreeUsesOrig(p->out->aDb[0].pBt) || !p->out->autoCommit ){
    rc = dumpError(p,SQLITE_MISUSE,0,
        "destination must be an ordinary SQLite connection outside a transaction");
  }else{
    sqlite3_stmt *q = 0;
    rc = sqlite3_prepare_v2(p->out,"SELECT count(*) FROM main.sqlite_schema",-1,&q,0);
    if( rc==SQLITE_OK ){
      int step = sqlite3_step(q);
      if( step!=SQLITE_ROW ) rc = step;
      else if( sqlite3_column_int(q,0)!=0 ){
        rc = dumpError(p,SQLITE_ERROR,0,"destination database must be empty");
      }
    }
    sqlite3_finalize(q);
  }
  if( rc!=SQLITE_OK ) goto dump_done;
  sqlite3_db_config(p->out,SQLITE_DBCONFIG_ENABLE_FKEY,-1,&fk);
  haveFk = 1;
  rc = sqlite3_db_config(p->out,SQLITE_DBCONFIG_ENABLE_FKEY,0,0);
  if( rc==SQLITE_OK ) rc = dumpExec(p,0,"SAVEPOINT doltlite_export");
  if( rc!=SQLITE_OK ) goto dump_done;
  transaction = 1;
#ifndef SQLITE_OMIT_AUTHORIZATION
  auth.callback = p->out->xAuth; auth.ctx = p->out->pAuthArg;
  rc = sqlite3_set_authorizer(p->out,dumpAuthorize,&auth);
  haveAuth = rc==SQLITE_OK;
#endif
  for(i=0; i<p->nSchema && rc==SQLITE_OK; i++){
    SchemaEntry *e = &p->aSchema[i];
    char *sql;
    if( !e->zSql || !e->zSql[0] ) continue;
    sql = dumpMainSql(e);
    if( !sql ) rc = SQLITE_NOMEM;
    else{
      sqlite3_free(e->zSql);
      e->zSql = sql;
    }
  }
  if( rc==SQLITE_OK ) rc = dumpSchema(p);
  if( rc!=SQLITE_OK ) goto dump_done;
  rc = dumpWrite(p,"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n");
  if( rc==SQLITE_OK ) rc = dumpMeta(p,&cat);
  for(pass=0; pass<2 && rc==SQLITE_OK; pass++){
    for(i=0; i<p->nSchema && rc==SQLITE_OK; i++){
      SchemaEntry *e = &p->aSchema[i];
      Table *t;
      char *z = 0;
      if( dumpSkip(e->zName) || strcmp(e->zType,"table") || !e->zSql
       || !e->zSql[0] || dumpVirtual(e)!=(pass==0) ) continue;
      t = sqlite3FindTable(p->out,e->zName,"main");
      if( !t ) { rc = SQLITE_CORRUPT; break; }
      if( (t->tabFlags & TF_Shadow)!=0 ){
        z = sqlite3_mprintf("DELETE FROM main.\"%w\";\n",e->zName);
        if( z ) rc = dumpExec(p,e->zName,z);
      }else z = sqlite3_mprintf("%s;\n",e->zSql);
      if( rc==SQLITE_OK ) rc = dumpWrite(p,z);
      sqlite3_free(z);
      if( rc==SQLITE_OK && !IsVirtual(t) ) rc = dumpTable(p,e->zName);
    }
  }
  if( rc==SQLITE_OK ){
    SchemaEntry *seq = findSchemaEntry(p->aSchema,p->nSchema,"sqlite_sequence");
    if( seq ){
      if( !sqlite3FindTable(p->out,"sqlite_sequence","main") ){
        char *name = 0, *sql;
        int suffix = 0;
        do{
          sqlite3_free(name);
          name = sqlite3_mprintf("doltlite_export_sequence_%d",suffix++);
        }while( name && (sqlite3FindTable(p->out,name,"main")
                       || sqlite3FindIndex(p->out,name,"main")) );
        sql = name ? sqlite3_mprintf("CREATE TABLE main.\"%w\"(id INTEGER PRIMARY KEY AUTOINCREMENT);\n"
                                    "DROP TABLE main.\"%w\";\n",name,name) : 0;
        sqlite3_free(name);
        if( !sql ) rc = SQLITE_NOMEM;
        else rc = dumpExec(p,"sqlite_sequence",sql);
        if( rc==SQLITE_OK ) rc = dumpWrite(p,sql);
        sqlite3_free(sql);
      }
      if( rc==SQLITE_OK ) rc = dumpExec(p,"sqlite_sequence","DELETE FROM main.sqlite_sequence");
      if( rc==SQLITE_OK ) rc = dumpWrite(p,"DELETE FROM main.sqlite_sequence;\n");
      if( rc==SQLITE_OK ) rc = dumpTable(p,"sqlite_sequence");
    }
  }
  if( rc==SQLITE_OK ) rc = dumpObjects(p);
  if( rc==SQLITE_OK ) rc = dumpValidate(p);
  if( rc==SQLITE_OK ) rc = dumpWrite(p,"COMMIT;\n");
  if( rc==SQLITE_OK ) rc = dumpExec(p,0,"RELEASE doltlite_export");
  if( rc==SQLITE_OK ) transaction = 0;
dump_done:
#ifndef SQLITE_OMIT_AUTHORIZATION
  if( transaction && haveAuth ) sqlite3_set_authorizer(p->out,0,0);
#endif
  if( transaction ) sqlite3_exec(p->out,
      "ROLLBACK TO doltlite_export; RELEASE doltlite_export",0,0,0);
#ifndef SQLITE_OMIT_AUTHORIZATION
  if( haveAuth ) sqlite3_set_authorizer(p->out,auth.callback,auth.ctx);
#endif
  if( haveFk ) sqlite3_db_config(p->out,SQLITE_DBCONFIG_ENABLE_FKEY,fk,0);
  freeSchemaEntries(p->aSchema,p->nSchema);
  if( pinned ){
    doltliteEndReadSnapshot(p->db);
    if( ownedRead ) sqlite3BtreeCommit(bt);
  }
  return rc==SQLITE_OK ? rc : dumpError(p,rc,0,0);
}

SQLITE_API int SQLITE_APICALL doltlite_dump_sql(sqlite3 *db, const char *zRef,
    int (SQLITE_CALLBACK *xWrite)(void*,const void*,int), void *pCtx, char **pzErr){
  DlDump p;
  char *zError = 0;
  int rc;
  if( pzErr ) *pzErr = 0;
  if( !sqlite3SafetyCheckOk(db) || !xWrite ) return SQLITE_MISUSE;
  memset(&p,0,sizeof(p));
  p.db = db;
  p.zRef = zRef ? zRef : "WORKING";
  p.xWrite = xWrite;
  p.pCtx = pCtx;
  p.pzErr = &zError;
  sqlite3_mutex_enter(db->mutex);
  rc = dumpRun(&p);
  sqlite3_close(p.out);
  if( rc!=SQLITE_OK ) sqlite3ErrorWithMsg(db,rc,"%s",zError ? zError : sqlite3_errstr(rc));
  else sqlite3Error(db,SQLITE_OK);
  rc = sqlite3ApiExit(db,rc);
  sqlite3_mutex_leave(db->mutex);
  if( pzErr ) *pzErr = zError;
  else sqlite3_free(zError);
  return rc;
}

SQLITE_API int SQLITE_APICALL doltlite_export_sqlite(sqlite3 *db, const char *zRef,
    sqlite3 *out, char **pzErr){
  DlDump p;
  char *zError = 0;
  int rc;
  if( pzErr ) *pzErr = 0;
  if( !sqlite3SafetyCheckOk(db) || !sqlite3SafetyCheckOk(out) || db==out ){
    return SQLITE_MISUSE;
  }
  memset(&p,0,sizeof(p));
  p.db = db;
  p.out = out;
  p.zRef = zRef ? zRef : "WORKING";
  p.pzErr = &zError;
  sqlite3_mutex_enter(db->mutex);
  sqlite3_mutex_enter(out->mutex);
  rc = dumpRun(&p);
  if( rc!=SQLITE_OK ) sqlite3ErrorWithMsg(db,rc,"%s",zError ? zError : sqlite3_errstr(rc));
  else sqlite3Error(db,SQLITE_OK);
  rc = sqlite3ApiExit(db,rc);
  sqlite3_mutex_leave(out->mutex);
  sqlite3_mutex_leave(db->mutex);
  if( pzErr ) *pzErr = zError;
  else sqlite3_free(zError);
  return rc;
}
#endif

#ifdef DOLTLITE_PROLLY
typedef struct DumpFile DumpFile;
struct DumpFile { sqlite3_file *file; i64 offset; };

static int dumpFileWrite(void *ctx, const void *data, int n){
  DumpFile *f = ctx;
  int rc = sqlite3OsWrite(f->file,data,n,f->offset);
  if( rc==SQLITE_OK ) f->offset += n;
  return rc;
}

static char *dumpFileUri(const char *z){
  sqlite3_str *s = sqlite3_str_new(0);
  const unsigned char *p = (const unsigned char*)z;
  sqlite3_str_appendall(s,"file:");
  for(; *p; p++){
    if( (*p>='a' && *p<='z') || (*p>='A' && *p<='Z')
     || (*p>='0' && *p<='9') || strchr("/:-._~",*p) ){
      sqlite3_str_appendchar(s,1,*p);
    }else sqlite3_str_appendf(s,"%%%02X",*p);
  }
  sqlite3_str_appendall(s,"?doltlite_engine=sqlite");
  return sqlite3_str_finish(s);
}

static int dumpDestination(sqlite3_vfs *vfs, const char *path, char **result){
  const char *sep = strrchr(path,'/');
  const char *base;
  char *parent, *full;
  int rc, nParent = 0;
#if SQLITE_OS_WIN
  const char *back = strrchr(path,'\\');
  if( back && (!sep || back>sep) ) sep=back;
  if( !sep && path[0] && path[1]==':' ) sep=path+1;
#endif
  base = sep ? sep+1 : path;
  if( !base[0] ) return SQLITE_CANTOPEN;
  if( sep ){
    nParent = (int)(sep-path)+(sep==path || *sep==':');
#if SQLITE_OS_WIN
    if( sep==path+2 && path[1]==':' ) nParent++;
#endif
  }
  parent = sep ? sqlite3_mprintf("%.*s",nParent,path) : sqlite3_mprintf(".");
  full = sqlite3_malloc64(vfs->mxPathname+1);
  if( !parent || !full ){
    sqlite3_free(parent); sqlite3_free(full);
    return SQLITE_NOMEM;
  }
  rc = sqlite3OsFullPathname(vfs,parent,vfs->mxPathname+1,full);
  if( rc==SQLITE_OK_SYMLINK ) rc=SQLITE_OK;
  if( rc==SQLITE_OK ){
    int n = sqlite3Strlen30(full);
    int hasSep = n>0 && full[n-1]=='/';
#if SQLITE_OS_WIN
    hasSep |= n>0 && full[n-1]=='\\';
#endif
    *result = sqlite3_mprintf("%s%s%s",full,hasSep ? "" : "/",base);
    if( !*result ) rc=SQLITE_NOMEM;
    else if( sqlite3Strlen30(*result)>vfs->mxPathname ) rc=SQLITE_CANTOPEN;
  }
  sqlite3_free(parent); sqlite3_free(full);
  return rc;
}

static int dumpToFile(sqlite3 *db, const char *zRef, const char *zPath,
                      int sqliteFormat, char **pzErr){
  sqlite3_vfs *vfs = sqlite3_vfs_find(0);
  sqlite3 *out = 0;
  sqlite3_file *file = 0;
  sqlite3_filename filename = 0;
  char *tmp = 0, *dest = 0, *uri = 0;
  unsigned char random[16];
  char hex[33];
  int rc, i, exists = 0, created = 0;
  DumpFile stream;
  memset(&stream,0,sizeof(stream));
  rc = dumpDestination(vfs,zPath,&dest);
  if( rc==SQLITE_OK ) rc = sqlite3OsAccess(vfs,dest,SQLITE_ACCESS_EXISTS,&exists);
  if( rc!=SQLITE_OK ) goto file_done;
  if( exists ){
    *pzErr = sqlite3_mprintf("dolt_dump: destination already exists: %s",zPath);
    rc = SQLITE_CANTOPEN;
    goto file_done;
  }
  sqlite3_randomness(sizeof(random),random);
  for(i=0; i<16; i++) sqlite3_snprintf(3,hex+2*i,"%02x",random[i]);
  tmp = sqlite3_mprintf("%s-dump-%s",dest,hex);
  if( !tmp ){ rc = SQLITE_NOMEM; goto file_done; }
  filename = sqlite3_create_filename(tmp,"","",0,0);
  if( !filename ){ rc=SQLITE_NOMEM; goto file_done; }
  rc = sqlite3OsOpenMalloc(vfs,filename,&file,
      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_EXCLUSIVE
      |SQLITE_OPEN_MAIN_DB,0);
  if( rc!=SQLITE_OK ) goto file_done;
  created = 1;
  if( sqliteFormat ){
    sqlite3OsCloseFree(file);
    file = 0;
    uri = dumpFileUri(tmp);
    if( !uri ){ rc = SQLITE_NOMEM; goto file_done; }
    rc = sqlite3_open_v2(uri,&out,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE
        |SQLITE_OPEN_URI|SQLITE_OPEN_NOFOLLOW,0);
    if( rc==SQLITE_OK ) rc = doltlite_export_sqlite(db,zRef,out,pzErr);
    if( out ){
      int closeRc = sqlite3_close(out);
      out = 0;
      if( rc==SQLITE_OK ) rc = closeRc;
    }
    if( rc==SQLITE_OK ) rc = sqlite3OsOpenMalloc(vfs,filename,&file,
        SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB,0);
  }else{
    stream.file = file;
    rc = doltlite_dump_sql(db,zRef,dumpFileWrite,&stream,pzErr);
  }
  if( rc==SQLITE_OK ) rc = sqlite3OsSync(file,SQLITE_SYNC_FULL);
  if( file ){
    sqlite3OsCloseFree(file);
    file = 0;
  }
  if( rc==SQLITE_OK ) rc = sqlite3OsPublishFile(vfs,tmp,dest,&exists);
file_done:
  if( out ) sqlite3_close(out);
  if( file ) sqlite3OsCloseFree(file);
  if( created ) sqlite3OsDelete(vfs,tmp,0);
  if( rc!=SQLITE_OK && !*pzErr ){
    if( exists ){
      *pzErr = sqlite3_mprintf("dolt_dump: destination already exists: %s",zPath);
    }else{
      *pzErr = sqlite3_mprintf("dolt_dump: cannot export %s: %s",zPath,
          sqlite3_errstr(rc));
    }
  }
  sqlite3_free_filename(filename);
  sqlite3_free(uri);
  sqlite3_free(tmp);
  sqlite3_free(dest);
  return rc;
}

static void dumpFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  DoltliteCmdArgs args;
  const char *zPath = 0, *zFormat = 0, *zRef = 0;
  DoltliteCmdOption opts[] = {
    {"file-name",0,DOLTLITE_CMD_OPTION_VALUE,0,&zPath},
    {"result-format",'r',DOLTLITE_CMD_OPTION_VALUE,0,&zFormat},
    {"ref",0,DOLTLITE_CMD_OPTION_VALUE,0,&zRef}
  };
  char *zErr = 0;
  int rc;
  rc = doltliteCmdParseArgs(ctx,argc,argv,opts,ArraySize(opts),0,&args);
  if( rc!=SQLITE_OK ) return;
  if( args.nPositional || !zPath || !zPath[0]
   || (zFormat && sqlite3_stricmp(zFormat,"sql")
                    && sqlite3_stricmp(zFormat,"sqlite")) ){
    sqlite3_result_error(ctx,
        "dolt_dump requires --file-name and --result-format sql or sqlite",-1);
    doltliteCmdArgsClear(&args);
    return;
  }
  rc = dumpToFile(db,zRef ? zRef : "WORKING",zPath,
      zFormat && sqlite3_stricmp(zFormat,"sqlite")==0,&zErr);
  if( rc==SQLITE_OK ) sqlite3_result_int(ctx,0);
  else{
    sqlite3_result_error(ctx,zErr ? zErr : sqlite3_errstr(rc),-1);
    sqlite3_result_error_code(ctx,rc);
  }
  sqlite3_free(zErr);
  doltliteCmdArgsClear(&args);
}

int doltliteDumpRegister(sqlite3 *db){
  return sqlite3_create_function(db,"dolt_dump",-1,
      SQLITE_UTF8|SQLITE_DIRECTONLY,0,dumpFunc,0,0);
}
#endif
