#include "prolly_btree_int.h"
#include "doltlite_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lib/test_tmpdir.h"

static int passed, failed;
static void check(const char *name, int ok){
  if( ok ) passed++;
  else { failed++; fprintf(stderr,"FAIL: %s\n",name); }
}
static int sql(sqlite3 *db, const char *text){
  char *err = 0;
  int rc = sqlite3_exec(db,text,0,0,&err);
  if( rc ) fprintf(stderr,"SQL %d: %s [%s]\n",rc,err ? err : "",text);
  sqlite3_free(err);
  return rc;
}
static int number(sqlite3 *db, const char *text){
  sqlite3_stmt *q = 0;
  int n = -1;
  if( sqlite3_prepare_v2(db,text,-1,&q,0)==SQLITE_OK
   && sqlite3_step(q)==SQLITE_ROW ) n = sqlite3_column_int(q,0);
  sqlite3_finalize(q);
  return n;
}
static sqlite3 *openStock(void){
  sqlite3 *out = 0;
  check("open SQLite",sqlite3_open_v2("file::memory:?doltlite_engine=sqlite",
      &out,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI,0)==SQLITE_OK);
  return out;
}
static void compare(sqlite3 *source, sqlite3 *out){
  sqlite3_stmt *a = 0, *b = 0;
  int ra, rb, i;
  check("source query",sqlite3_prepare_v2(source,"SELECT id,v FROM t ORDER BY id",-1,&a,0)==SQLITE_OK);
  check("output query",sqlite3_prepare_v2(out,"SELECT id,v FROM t ORDER BY id",-1,&b,0)==SQLITE_OK);
  if( !a || !b ) goto done;
  for(;;){
    ra = sqlite3_step(a); rb = sqlite3_step(b);
    check("row result",ra==rb);
    if( ra!=SQLITE_ROW || rb!=SQLITE_ROW ) break;
    for(i=0; i<2; i++){
      int ta = sqlite3_column_type(a,i), tb = sqlite3_column_type(b,i);
      check("value type",ta==tb);
      if( ta==SQLITE_INTEGER ) check("integer",sqlite3_column_int64(a,i)==sqlite3_column_int64(b,i));
      else if( ta==SQLITE_FLOAT ){
        double da = sqlite3_column_double(a,i), db = sqlite3_column_double(b,i);
        if( memcmp(&da,&db,sizeof(da)) ) fprintf(stderr,"real mismatch id=%lld source=%a dest=%a\n",sqlite3_column_int64(a,0),da,db);
        check("real bits",memcmp(&da,&db,sizeof(da))==0);
      }else if( ta==SQLITE_TEXT || ta==SQLITE_BLOB ){
        const void *pa = sqlite3_column_blob(a,i), *pb = sqlite3_column_blob(b,i);
        int na = sqlite3_column_bytes(a,i), nb = sqlite3_column_bytes(b,i);
        check("bytes",na==nb && (na==0 || (pa && pb && !memcmp(pa,pb,na))));
      }
    }
  }
done:
  sqlite3_finalize(a); sqlite3_finalize(b);
}

typedef struct Sink Sink;
struct Sink {
  char *data;
  int n, cap, calls, max;
  int fail;
  sqlite3 *peer;
  int advanced;
  const char *action;
  int limit, errorCode;
};
static int writer(void *ctx, const void *data, int n){
  Sink *s = ctx;
  s->calls++;
  if( n>s->max ) s->max=n;
  if( s->fail && s->calls>=s->fail ) return SQLITE_FULL;
  if( s->limit && s->n+n>s->limit ) return s->errorCode;
  if( s->peer && !s->advanced ){
    s->advanced=1;
    check("peer advances during snapshot",sql(s->peer,s->action ? s->action :
        "UPDATE t SET v='peer'; INSERT INTO other VALUES(2);"
        "SELECT dolt_commit('-am','peer');")==SQLITE_OK);
  }
  if( s->n+n+1>s->cap ){
    int cap = (s->n+n+1)*2;
    char *next = realloc(s->data,cap);
    if( !next ) return SQLITE_NOMEM;
    s->data=next; s->cap=cap;
  }
  memcpy(s->data+s->n,data,n); s->n+=n; s->data[s->n]=0;
  return SQLITE_OK;
}

static void values(void){
  const char *ddl[] = {
    "CREATE TABLE t(id INTEGER PRIMARY KEY,v BLOB)",
    "CREATE TABLE t(id INT PRIMARY KEY,v BLOB)",
    "CREATE TABLE t(id INT PRIMARY KEY,v BLOB) WITHOUT ROWID"
  };
  int layout, txn, format;
  for(layout=0; layout<3; layout++) for(txn=0; txn<2; txn++){
    sqlite3 *source = 0;
    check("open source",sqlite3_open(":memory:",&source)==SQLITE_OK);
    check("create",sql(source,ddl[layout])==SQLITE_OK);
    check("values",sql(source,
        "INSERT INTO t VALUES(1,NULL),(2,X''),(3,X'0027FF'),"
        "(4,CAST(X'610062' AS TEXT)),(5,'a''b'),(6,char(13,10)),"
        "(7,CAST(X'80FF' AS TEXT)),(8,1.2345678901234567),"
        "(9,9e999),(10,-9e999),(11,5e-324),(12,-0.0),"
        "(13,-9223372036854775808),(14,9223372036854775807),"
        "(15,zeroblob(1048576)); SELECT dolt_commit('-Am','seed');")==SQLITE_OK);
    if( txn ) check("begin SQL write",sql(source,"BEGIN; INSERT INTO t VALUES(16,'pending')")==SQLITE_OK);
    for(format=0; format<2; format++){
      sqlite3 *out = openStock();
      char *err = 0;
      int rc;
      if( format ){
        Sink s = {0};
        rc = doltlite_dump_sql(source,0,writer,&s,&err);
        check("stream success",rc==SQLITE_OK && err==0);
        check("bounded output chunks",s.calls>100 && s.max<=16384);
        if( rc==SQLITE_OK ) check("replay stream",sql(out,s.data)==SQLITE_OK);
        free(s.data);
      }else{
        rc = doltlite_export_sqlite(source,0,out,&err);
        if( rc ) fprintf(stderr,"export error: %s\n",err ? err : "");
        check("direct export",rc==SQLITE_OK && err==0);
      }
      check("source SQL boundary",sqlite3_get_autocommit(source)==!txn);
      compare(source,out);
      check("stock integrity",number(out,"SELECT integrity_check='ok' FROM pragma_integrity_check")==1);
      sqlite3_free(err); sqlite3_close(out);
    }
    if( txn ) sql(source,"ROLLBACK");
    sqlite3_close(source);
  }
}

static void danglingSchema(void){
  const char *refs[] = {"WORKING","STAGED","HEAD","dangling"};
  sqlite3 *db = 0;
  int ref, format;
  check("dangling source opens",sqlite3_open(":memory:",&db)==SQLITE_OK);
  check("dangling schema setup",sql(db,
      "CREATE TABLE x(a); CREATE TABLE y(a); INSERT INTO y VALUES(1);"
      "CREATE VIEW v AS SELECT * FROM x;"
      "CREATE TRIGGER tr AFTER INSERT ON y BEGIN INSERT INTO x VALUES(new.a); END;"
      "DROP TABLE x; SELECT dolt_commit('-Am','dangling'); SELECT dolt_tag('dangling')")==SQLITE_OK);
  for(ref=0; ref<4; ref++) for(format=0; format<2; format++){
    sqlite3 *out = openStock();
    char *error = 0;
    int rc;
    if( format ){
      Sink sink = {0};
      rc = doltlite_dump_sql(db,refs[ref],writer,&sink,&error);
      check("dangling SQL export",rc==SQLITE_OK && !error);
      if( rc==SQLITE_OK ) check("dangling SQL replay",sql(out,sink.data)==SQLITE_OK);
      free(sink.data);
    }else{
      rc = doltlite_export_sqlite(db,refs[ref],out,&error);
      check("dangling SQLite export",rc==SQLITE_OK && !error);
    }
    check("dangling definitions retained",number(out,
        "SELECT count(*) FROM main.sqlite_schema WHERE name IN ('v','tr') AND instr(sql,'x')>0")==2);
    check("dangling table data retained",number(out,"SELECT a FROM y")==1);
    if( rc==SQLITE_OK ){
      check("dangling dependencies restored",sql(out,"CREATE TABLE x(a); INSERT INTO y VALUES(2)")==SQLITE_OK);
      check("restored view and trigger work",number(out,"SELECT a FROM v")==2);
    }
    check("dangling source remains unchanged",number(db,"SELECT count(*) FROM y")==1
        && number(db,"SELECT count(*) FROM dolt_status")==0);
    sqlite3_free(error); sqlite3_close(out);
  }
  sqlite3_close(db);
}

static void appIdentity(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3_result_value(ctx,argv[0]);
}
static int appCompare(void *ctx, int na, const void *a, int nb, const void *b){
  int result = memcmp(a,b,na<nb ? na : nb);
  return result ? result : na-nb;
}
static void appCollation(void *ctx, sqlite3 *db, int enc, const char *name){
  (*(int*)ctx)++;
  sqlite3_create_collation(db,name,enc,0,appCompare);
}
static void danglingApplications(void){
  sqlite3 *db = 0, *out = openStock();
  char *error = 0;
  int rc, needed = 0;
  sqlite3_open(":memory:",&db);
  check("application schema setup",sql(db,
      "CREATE VIEW v AS SELECT app_identity(a) FROM gone")==SQLITE_OK);
  rc = doltlite_export_sqlite(db,0,out,&error);
  check("missing application function rejected",rc==SQLITE_ERROR
      && error && strstr(error,"app_identity"));
  check("missing function rolls back destination",number(out,"SELECT count(*) FROM sqlite_schema")==0);
  sqlite3_free(error); error = 0;
  check("application collation schema setup",sql(db,
      "DROP VIEW v; CREATE VIEW v AS SELECT app_identity(a) COLLATE app_collation FROM gone")==SQLITE_OK);
  check("application function registered",sqlite3_create_function(out,"app_identity",1,
      SQLITE_UTF8,0,appIdentity,0,0)==SQLITE_OK);
  rc = doltlite_export_sqlite(db,0,out,&error);
  check("missing application collation rejected",rc==SQLITE_ERROR
      && error && strstr(error,"app_collation"));
  check("missing collation rolls back destination",number(out,"SELECT count(*) FROM sqlite_schema")==0);
  sqlite3_free(error); error = 0;
  check("collation factory registered",sqlite3_collation_needed(out,&needed,appCollation)==SQLITE_OK);
  rc = doltlite_export_sqlite(db,0,out,&error);
  check("application dependencies accepted",rc==SQLITE_OK && !error && needed>0);
  check("application schema restored",sql(out,"CREATE TABLE gone(a); INSERT INTO gone VALUES(7)")==SQLITE_OK
      && number(out,"SELECT * FROM v")==7);
  sqlite3_free(error); sqlite3_close(out); sqlite3_close(db);
}

static int memoryAccessCalls;
static int memoryAccess(sqlite3_vfs *vfs, const char *path, int flags, int *exists){
  memoryAccessCalls++;
  return SQLITE_IOERR_ACCESS;
}
static void memoryDestinations(void){
  sqlite3_vfs vfs = *sqlite3_vfs_find(0);
  const char *uris[] = {"file::memory:?doltlite_engine=sqlite",
                       "file:dump-memory?mode=memory&doltlite_engine=sqlite",
                       "file:dump-memory?doltlite_engine=sqlite"};
  int i;
  vfs.zName = "dump-memory";
  vfs.xAccess = memoryAccess;
  check("memory VFS registered",sqlite3_vfs_register(&vfs,0)==SQLITE_OK);
  for(i=0; i<3; i++){
    sqlite3 *db=0;
    int flags=SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI;
    int rc;
    if( i==2 ) flags |= SQLITE_OPEN_MEMORY;
    memoryAccessCalls=0;
    rc=sqlite3_open_v2(uris[i],&db,flags,vfs.zName);
    check("SQLite memory destination opens",rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      check("SQLite memory destination engine",number(db,"SELECT doltlite_engine()='orig'")==1);
      check("SQLite memory destination writes",sql(db,"CREATE TABLE t(id INT); INSERT INTO t VALUES(1)")==SQLITE_OK);
    }
    check("memory destination avoids filesystem",memoryAccessCalls==0);
    sqlite3_close(db);
  }
  check("memory VFS unregistered",sqlite3_vfs_unregister(&vfs)==SQLITE_OK);
}

static void virtualTables(void){
  const char *ddl[] = {
    "CREATE VIRTUAL TABLE v USING fts3(body)",
    "CREATE VIRTUAL TABLE v USING fts4(body)",
    "CREATE VIRTUAL TABLE v USING fts5(body)",
    "CREATE TABLE content(id INTEGER PRIMARY KEY,body TEXT);"
    "CREATE VIRTUAL TABLE v USING fts5(body,content='content',content_rowid='id')",
    "CREATE VIRTUAL TABLE v USING fts5(body,content='')",
    "CREATE VIRTUAL TABLE v USING rtree(id,x1,x2,y1,y2)",
    "CREATE VIRTUAL TABLE v USING rtree_i32(id,x1,x2,y1,y2)"
  };
  const char *options[] = {"ENABLE_FTS3","ENABLE_FTS4","ENABLE_FTS5",
                          "ENABLE_FTS5","ENABLE_FTS5","ENABLE_RTREE","ENABLE_RTREE"};
  int module, historical, format;
  for(module=0; module<(int)(sizeof(ddl)/sizeof(ddl[0])); module++){
    sqlite3 *db=0;
    sqlite3_open(":memory:",&db);
    if( !sqlite3_compileoption_used(options[module]) ){
      sqlite3_close(db);
      continue;
    }
    check("virtual schema",sql(db,ddl[module])==SQLITE_OK);
    if( module>=5 ){
      check("multi-level R-tree seed",sql(db,
          "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1500)"
          "INSERT INTO v SELECT i,i-1,i,0,1 FROM n")==SQLITE_OK);
    }else{
      if( module==3 ) check("external content seed",sql(db,
          "INSERT INTO content VALUES(1,'hello export')")==SQLITE_OK);
      check("virtual seed",sql(db,"INSERT INTO v(rowid,body) VALUES(1,'hello export')")==SQLITE_OK);
    }
    check("virtual commit",sql(db,"SELECT dolt_commit('-Am','virtual')")==SQLITE_OK);
    if( module>=5 ){
      check("working R-tree seed",sql(db,"INSERT INTO v VALUES(2000,0,1,0,1)")==SQLITE_OK);
    }else{
      if( module==3 ) check("working external content seed",sql(db,
          "INSERT INTO content VALUES(2,'hello working')")==SQLITE_OK);
      check("working virtual seed",sql(db,"INSERT INTO v(rowid,body) VALUES(2,'hello working')")==SQLITE_OK);
    }
    for(historical=0; historical<2; historical++) for(format=0; format<2; format++){
      sqlite3 *out=openStock();
      char *error=0;
      const char *ref=historical ? "HEAD" : "WORKING";
      int rc;
      if( format ){
        Sink sink={0};
        rc=doltlite_dump_sql(db,ref,writer,&sink,&error);
        check("virtual stream",rc==SQLITE_OK);
        if( rc==SQLITE_OK ) check("virtual replay",sql(out,sink.data)==SQLITE_OK);
        free(sink.data);
      }else{
        rc=doltlite_export_sqlite(db,ref,out,&error);
        check("virtual direct",rc==SQLITE_OK);
      }
      if( rc ) fprintf(stderr,"virtual export module %d ref %s: %s\n",module,ref,error ? error : "");
      check("virtual immediate query",number(out,module>=5
          ? "SELECT count(*) FROM main.v WHERE x1<=0.5 AND x2>=0.5"
          : "SELECT count(*) FROM main.v WHERE v MATCH 'hello'")==2-historical);
      if( module>=5 ) check("R-tree integrity",number(out,"SELECT rtreecheck('v')='ok'")==1);
      else if( module>=2 ) check("FTS5 integrity",sql(out,"INSERT INTO main.v(v) VALUES('integrity-check')")==SQLITE_OK);
      sqlite3_free(error);sqlite3_close(out);
    }
    sqlite3_close(db);
  }
}

static void destinationScope(void){
  sqlite3 *source=0;
  int format;
  sqlite3_open(":memory:",&source);
  sql(source,"CREATE TABLE audit(id INTEGER PRIMARY KEY);"
      "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT);"
      "CREATE INDEX ix ON t(v); CREATE VIEW vv AS SELECT * FROM t;"
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;"
      "INSERT INTO t VALUES(1,'one'),(100,'deleted'); DELETE FROM t WHERE id=100;"
      "CREATE TABLE pragma_table_xinfo(v TEXT); INSERT INTO pragma_table_xinfo VALUES('ordinary');"
      "CREATE TABLE \"quote \"\" name\"(\"strange \"\" column\" TEXT);"
      "INSERT INTO \"quote \"\" name\" VALUES('quoted');");
  for(format=0; format<2; format++){
    sqlite3 *out=openStock();
    char *error=0;
    int rc;
    sql(out,"CREATE TEMP TABLE audit(id INTEGER PRIMARY KEY); INSERT INTO temp.audit VALUES(999);"
        "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT);"
        "INSERT INTO temp.t VALUES(999,'sentinel');"
        "CREATE INDEX temp.ix ON t(v); CREATE TEMP VIEW vv AS SELECT 'sentinel' AS v;"
        "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;");
    if( format ){
      Sink sink={0};
      rc=doltlite_dump_sql(source,0,writer,&sink,&error);
      check("scoped SQL export",rc==SQLITE_OK && !error);
      if( rc==SQLITE_OK ){
        rc=sqlite3_exec(out,sink.data,0,0,&error);
        check("scoped SQL replay",rc==SQLITE_OK && !error);
      }
      free(sink.data);
    }else{
      rc=doltlite_export_sqlite(source,0,out,&error);
      check("scoped SQLite export",rc==SQLITE_OK && !error);
    }
    if( rc ) fprintf(stderr,"scoped export: %s\n",error ? error : "");
    check("main rows exported",number(out,"SELECT count(*) FROM main.t WHERE id=1 AND v='one'")==1);
    check("main audit preserved",number(out,"SELECT count(*) FROM main.audit")==2);
    check("main index present",number(out,"SELECT count(*) FROM main.sqlite_schema WHERE name='ix'")==1);
    check("main view resolves main",number(out,"SELECT count(*) FROM main.vv WHERE id=1")==1);
    check("main sequence preserved",number(out,"SELECT seq FROM main.sqlite_sequence WHERE name='t'")==100);
    check("TEMP rows unchanged",number(out,"SELECT count(*) FROM temp.t")==1
        && number(out,"SELECT count(*) FROM temp.t WHERE id=999 AND v='sentinel'")==1);
    check("TEMP sequence unchanged",number(out,"SELECT seq FROM temp.sqlite_sequence WHERE name='t'")==999);
    check("TEMP audit unchanged",number(out,"SELECT count(*) FROM temp.audit")==1);
    check("TEMP index unchanged",number(out,"SELECT count(*) FROM temp.sqlite_schema WHERE name='ix'")==1);
    check("pragma-named table exported",number(out,"SELECT count(*) FROM main.pragma_table_xinfo WHERE v='ordinary'")==1);
    check("quoted names exported",number(out,"SELECT count(*) FROM main.\"quote \"\" name\" WHERE \"strange \"\" column\"='quoted'")==1);
    if( rc==SQLITE_OK ){
      check("main trigger installed",sql(out,"INSERT INTO main.t(v) VALUES('next')")==SQLITE_OK
          && number(out,"SELECT max(id) FROM main.audit")==101);
      check("TEMP trigger preserved",sql(out,"INSERT INTO temp.t(v) VALUES('temp-next')")==SQLITE_OK
          && number(out,"SELECT max(id) FROM temp.audit")==1000);
    }
    sqlite3_free(error);sqlite3_close(out);
  }
  sqlite3_close(source);
}

static void constraintStreams(void){
  sqlite3 *source=0,*out;
  Sink sink={0};
  char *error=0;
  int rc;
  sqlite3_open(":memory:",&source);
  sql(source,"CREATE TABLE bad(id INT PRIMARY KEY CHECK(id>0));"
      "PRAGMA ignore_check_constraints=ON; INSERT INTO bad VALUES(-1);");
  rc=doltlite_dump_sql(source,0,writer,&sink,&error);
  check("invalid CHECK SQL export rejected",(rc&255)==SQLITE_CONSTRAINT
      && error && strstr(error,"bad"));
  check("invalid CHECK source unchanged",number(source,"SELECT count(*) FROM bad WHERE id=-1")==1);
  free(sink.data);sqlite3_free(error);sqlite3_close(source);
  sqlite3_open(":memory:",&source);
  sql(source,"CREATE TABLE t(id INT PRIMARY KEY,v TEXT UNIQUE);"
      "SELECT dolt_commit('-Am','base'); SELECT dolt_branch('peer');"
      "INSERT INTO t VALUES(1,'duplicate'); SELECT dolt_commit('-am','main');"
      "SELECT dolt_checkout('peer'); INSERT INTO t VALUES(2,'duplicate');"
      "SELECT dolt_commit('-am','peer'); BEGIN;");
  check("unique merge reports violations",sqlite3_exec(source,"SELECT dolt_merge('main')",0,0,0)==SQLITE_ERROR);
  check("unique violation fixture retains both rows",number(source,"SELECT count(*) FROM t")==2);
  memset(&sink,0,sizeof(sink));error=0;
  rc=doltlite_dump_sql(source,0,writer,&sink,&error);
  check("invalid UNIQUE SQL export rejected",(rc&255)==SQLITE_CONSTRAINT && error);
  check("invalid UNIQUE source unchanged",number(source,"SELECT count(*) FROM t")==2);
  free(sink.data);sqlite3_free(error);error=0;
  out=openStock();
  rc=doltlite_export_sqlite(source,0,out,&error);
  check("invalid UNIQUE SQLite export rejected",(rc&255)==SQLITE_CONSTRAINT && error);
  check("invalid UNIQUE destination rolled back",number(out,"SELECT count(*) FROM main.sqlite_schema")==0);
  sqlite3_free(error);sqlite3_close(out);sqlite3_close(source);
}

static void lateCancellation(void){
  int code;
  for(code=0; code<2; code++){
    sqlite3 *source=0,*out=openStock();
    Sink sink={0};
    char *error=0;
    int rc;
    sqlite3_open(":memory:",&source);
    sql(source,"CREATE TABLE t(id INTEGER PRIMARY KEY,v BLOB);"
        "INSERT INTO t VALUES(1,zeroblob(1048576)); SELECT dolt_commit('-Am','seed');"
        "BEGIN; INSERT INTO t VALUES(2,'pending')");
    sink.limit=65536;
    sink.errorCode=code ? SQLITE_INTERRUPT : SQLITE_FULL;
    rc=doltlite_dump_sql(source,0,writer,&sink,&error);
    check("late callback failure propagated",rc==sink.errorCode && error && sink.n>32768);
    check("late cancellation keeps source transaction",!sqlite3_get_autocommit(source)
        && number(source,"SELECT count(*) FROM t")==2);
    free(sink.data);sqlite3_free(error);error=0;
    check("cancelled source can export again",doltlite_export_sqlite(source,0,out,&error)==SQLITE_OK);
    check("retry includes pending source rows",number(out,"SELECT count(*) FROM main.t")==2);
    sqlite3_free(error);sqlite3_close(out);sql(source,"ROLLBACK");sqlite3_close(source);
  }
}

static void failures(void){
  sqlite3 *source=0, *out=openStock();
  char *err=0;
  Sink s={0};
  int rc;
  sqlite3_open(":memory:",&source);
  sql(source,"CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'before'); BEGIN; INSERT INTO t VALUES(2,'pending')");
  s.fail=5;
  rc=doltlite_dump_sql(source,0,writer,&s,&err);
  check("callback failure propagated",rc==SQLITE_FULL && err && strstr(err,"callback"));
  check("error on source handle",sqlite3_errcode(source)==SQLITE_FULL);
  sqlite3_free(err); err=0; free(s.data);
  check("failed stream keeps source transaction",!sqlite3_get_autocommit(source) && number(source,"SELECT count(*) FROM t")==2);
  sql(out,"PRAGMA foreign_keys=ON; CREATE TABLE sentinel(id INT); INSERT INTO sentinel VALUES(9)");
  rc=doltlite_export_sqlite(source,0,out,&err);
  check("nonempty refused",rc!=SQLITE_OK && err && strstr(err,"empty"));
  check("nonempty data kept",number(out,"SELECT id FROM sentinel")==9);
  check("nonempty flags kept",number(out,"PRAGMA foreign_keys")==1);
  sqlite3_free(err); err=0;
  sql(out,"DROP TABLE sentinel; BEGIN");
  check("destination transaction refused",doltlite_export_sqlite(source,0,out,&err)==SQLITE_MISUSE);
  check("destination transaction kept",!sqlite3_get_autocommit(out));
  sqlite3_free(err); err=0; sql(out,"ROLLBACK");
  check("missing ref refused",doltlite_export_sqlite(source,"missing",out,&err)!=SQLITE_OK && err);
  check("missing ref destination empty",number(out,"SELECT count(*) FROM sqlite_schema")==0);
  sqlite3_free(err); err=0;
  check("native destination refused",doltlite_export_sqlite(source,0,source,&err)==SQLITE_MISUSE);
  sql(source,"ROLLBACK; PRAGMA ignore_check_constraints=ON; CREATE TABLE bad(id INT PRIMARY KEY CHECK(id>0)); INSERT INTO bad VALUES(-1)");
  rc=doltlite_export_sqlite(source,0,out,&err);
  check("constraint failure reported",(rc&255)==SQLITE_CONSTRAINT && err && strstr(err,"bad"));
  check("failed destination rolled back",number(out,"SELECT count(*) FROM sqlite_schema")==0);
  check("failed destination flags restored",number(out,"PRAGMA foreign_keys")==1);
  sqlite3_free(err); err=0;
  sql(source,"DROP TABLE bad");
  check("destination reusable",doltlite_export_sqlite(source,0,out,&err)==SQLITE_OK && !err);
  compare(source,out);
  sqlite3_close(out); sqlite3_close(source);
}

static void snapshot(void){
  sqlite3 *source=0,*peer=0,*out=openStock();
  char *err=0;
  char path[1024];
  Sink s={0};
  sqlite3_snprintf(sizeof(path),path,"%s/dump-snapshot.db",DOLTLITE_TEST_TMPDIR);
  remove(path);
  check("file source",sqlite3_open(path,&source)==SQLITE_OK);
  sql(source,"CREATE TABLE t(id INT PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'before'); CREATE TABLE other(id INT PRIMARY KEY); INSERT INTO other VALUES(1); SELECT dolt_commit('-Am','base')");
  check("peer open",sqlite3_open(path,&peer)==SQLITE_OK);
  s.peer=peer;
  check("snapshot dump",doltlite_dump_sql(source,0,writer,&s,&err)==SQLITE_OK);
  if( err ) fprintf(stderr,"snapshot: %s\n",err);
  check("snapshot replay",sql(out,s.data)==SQLITE_OK);
  check("earlier rows pinned",number(out,"SELECT v='before' FROM t")==1);
  check("later table pinned",number(out,"SELECT count(*) FROM other")==1);
  free(s.data); sqlite3_free(err); err=0;
  sqlite3_close(out); sqlite3_close(peer); sqlite3_close(source);
  check("readonly source",sqlite3_open_v2(path,&source,SQLITE_OPEN_READONLY,0)==SQLITE_OK);
  out=openStock();
  check("readonly export",doltlite_export_sqlite(source,0,out,&err)==SQLITE_OK);
  if( err ) fprintf(stderr,"readonly: %s\n",err);
  check("readonly current rows",number(out,"SELECT v='peer' FROM t")==1);
  sqlite3_free(err); sqlite3_close(out); sqlite3_close(source); remove(path);
}

#ifndef SQLITE_OMIT_AUTHORIZATION
static int snapshotAuthorizer(void *ctx, int op, const char *a, const char *b,
                              const char *db, const char *trigger){
  Sink *sink=ctx;
  if( op==SQLITE_INSERT && a && !strcmp(a,"t") && !sink->advanced ){
    sink->advanced=1;
    check("peer GC during SQLite export",sql(sink->peer,sink->action)==SQLITE_OK);
  }
  return SQLITE_OK;
}
#endif
static void movingSnapshots(void){
  const char *refs[]={"WORKING","STAGED","HEAD","pinned","pinned_branch"};
  const char *values[]={"working","staged","base","base","base"};
  int ref, format, discard;
  for(discard=0; discard<2; discard++) for(ref=0; ref<5; ref++) for(format=0; format<2; format++){
    sqlite3 *source=0,*peer=0,*out=openStock();
    Sink sink={0};
    char path[1024], query[128], *error=0;
    int rc;
    sqlite3_snprintf(sizeof(path),path,"%s/dump-moving-%d-%d.db",DOLTLITE_TEST_TMPDIR,ref,format);
    remove(path);
    check("moving source opens",sqlite3_open(path,&source)==SQLITE_OK);
    sql(source,"CREATE TABLE t(id INT PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'base');"
        "CREATE TABLE other(id INT PRIMARY KEY); INSERT INTO other VALUES(1);"
        "SELECT dolt_commit('-Am','base'); SELECT dolt_tag('pinned'); SELECT dolt_branch('pinned_branch');"
        "UPDATE t SET v='staged'; SELECT dolt_add('.'); UPDATE t SET v='working';");
    check("moving peer opens",sqlite3_open(path,&peer)==SQLITE_OK);
    sink.peer=peer;
    sink.action=discard
        ? "SELECT dolt_reset('--hard','HEAD~1');"
          "SELECT dolt_tag('-d','pinned'); SELECT dolt_tag('pinned','HEAD');"
          "SELECT dolt_branch('-f','pinned_branch','HEAD'); SELECT dolt_gc();"
        : "UPDATE t SET v='peer'; INSERT INTO other VALUES(2); SELECT dolt_commit('-am','peer');"
          "SELECT dolt_tag('-d','pinned'); SELECT dolt_tag('pinned','HEAD');"
          "SELECT dolt_branch('-f','pinned_branch','HEAD'); SELECT dolt_gc();";
    if( format ){
#ifndef SQLITE_OMIT_AUTHORIZATION
      sqlite3_set_authorizer(out,snapshotAuthorizer,&sink);
#endif
      rc=doltlite_export_sqlite(source,refs[ref],out,&error);
      sqlite3_set_authorizer(out,0,0);
    }else{
      rc=doltlite_dump_sql(source,refs[ref],writer,&sink,&error);
      if( rc==SQLITE_OK ) check("moving SQL replay",sql(out,sink.data)==SQLITE_OK);
    }
    if( rc ) fprintf(stderr,"moving snapshot %s/%d: %s\n",refs[ref],format,error ? error : "");
    check("moving export succeeds",rc==SQLITE_OK && !error);
    check("peer changed refs and ran GC",sink.advanced);
    sqlite3_snprintf(sizeof(query),query,"SELECT v='%s' FROM main.t",values[ref]);
    check("selected rows pinned",number(out,query)==1);
    check("second table pinned",number(out,"SELECT count(*) FROM main.other")==1);
    if( discard ) check("peer removed old tables",number(peer,
        "SELECT count(*) FROM main.sqlite_schema WHERE name IN ('t','other')")==0);
    else check("peer commit intact",number(peer,"SELECT count(*) FROM other")==2);
    sqlite3_free(error);free(sink.data);
    sqlite3_close(out);sqlite3_close(peer);sqlite3_close(source);remove(path);
  }
}

#ifndef SQLITE_OMIT_AUTHORIZATION
static int exportAuthorizer(void *ctx, int op, const char *a, const char *b,
                             const char *db, const char *trigger){
  if( op==SQLITE_CREATE_INDEX ) return SQLITE_DENY;
  if( op==SQLITE_SAVEPOINT && a && !strcmp(a,"ROLLBACK") ) return SQLITE_DENY;
  if( op==SQLITE_SELECT && *(int*)ctx ) return SQLITE_DENY;
  return SQLITE_OK;
}
static void authorization(void){
  sqlite3 *db=0, *out=openStock();
  char *error=0;
  int deny=0, rc;
  sqlite3_open(":memory:",&db);
  sql(db,"CREATE TABLE t(id INT PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'); CREATE INDEX tv ON t(v)");
  sqlite3_set_authorizer(out,exportAuthorizer,&deny);
  rc=doltlite_export_sqlite(db,0,out,&error);
  check("authorizer forwarded",rc==SQLITE_AUTH);
  check("authorizer failure rolled back",sqlite3_get_autocommit(out)
      && number(out,"SELECT count(*) FROM sqlite_schema")==0);
  deny=1;
  check("authorizer restored",number(out,"SELECT 1")==-1);
  sqlite3_free(error); sqlite3_close(out); sqlite3_close(db);
}
#endif

static void legacyMeta(void){
  sqlite3 *db=0;
  int version;
  sqlite3_open(":memory:",&db);
  for(version=CATALOG_FORMAT_V3; version<=CATALOG_FORMAT_V5; version++){
    unsigned char bytes[13]={0};
    unsigned char *out=0;
    ProllyHash hash;
    u32 user=99,app=99;
    int n, rc;
    bytes[0]=version;
    if( version==CATALOG_FORMAT_V5 ){bytes[5]=17; bytes[9]=42;}
    check("legacy catalog stored",chunkStorePut(doltliteGetChunkStore(db),bytes,
        version==CATALOG_FORMAT_V5 ? 13 : 5,&hash)==SQLITE_OK);
    check("legacy meta reader",doltliteCatalogReadMeta(db,&hash,&user,&app)==SQLITE_OK);
    check("legacy absent metadata",user==(version==CATALOG_FORMAT_V5 ? 17 : 0)
        && app==(version==CATALOG_FORMAT_V5 ? 42 : 0));
    rc=doltliteSerializeCatalogEntriesFromCatalog(db,0,0,&hash,&out,&n);
    check("legacy branch catalog serializes",rc==SQLITE_OK && n>=13);
    if( rc==SQLITE_OK && n>=13 ){
      check("legacy metadata preserved",prollyBtreeGetU32LE(out+5)==user
          && prollyBtreeGetU32LE(out+9)==app);
    }
    sqlite3_free(out);
  }
  {
    unsigned char *current=0, *legacy=0, *out=0;
    ProllyHash hash;
    int n=0, nOut=0, rc;
    check("nonempty legacy setup",sql(db,"CREATE TABLE legacy(id INT PRIMARY KEY);"
        "INSERT INTO legacy VALUES(1)")==SQLITE_OK);
    rc=doltliteFlushAndSerializeCatalog(db,&current,&n);
    check("current catalog serialized",rc==SQLITE_OK && n>13);
    if( rc==SQLITE_OK && n>13 ){
      legacy=sqlite3_malloc(n-8);
      check("legacy allocation",legacy!=0);
      if( legacy ){
        memcpy(legacy,current,5); legacy[0]=CATALOG_FORMAT_V4;
        memcpy(legacy+5,current+13,n-13);
        rc=chunkStorePut(doltliteGetChunkStore(db),legacy,n-8,&hash);
        check("nonempty legacy stored",rc==SQLITE_OK);
        if( rc==SQLITE_OK ){
          rc=doltliteSerializeCatalogEntriesFromCatalog(db,0,0,&hash,&out,&nOut);
          check("nonempty legacy reserialized",rc==SQLITE_OK && nOut>=13);
          if( rc==SQLITE_OK && nOut>=13 ){
            check("nonempty legacy has no user metadata",
                prollyBtreeGetU32LE(out+5)==0 && prollyBtreeGetU32LE(out+9)==0);
          }
        }
      }
    }
    sqlite3_free(current); sqlite3_free(legacy); sqlite3_free(out);
  }
  sqlite3_close(db);
}

static void existingFiles(void){
  sqlite3 *db = 0;
  const char *formats[] = {"sql", "sqlite"};
  int format, nonempty;
  check("existing destination source opens",sqlite3_open(":memory:",&db)==SQLITE_OK);
  check("existing destination source setup",sql(db,
      "CREATE TABLE t(a); INSERT INTO t VALUES(1)")==SQLITE_OK);
  for(format=0; format<2; format++) for(nonempty=0; nonempty<2; nonempty++){
    char path[1024], data[8] = {0};
    char *query, *err = 0;
    FILE *file;
    int rc;
    sqlite3_snprintf(sizeof(path),path,"%s/dump-existing-%d-%d",DOLTLITE_TEST_TMPDIR,format,nonempty);
    remove(path);
    file = fopen(path,"wb");
    check("existing destination created",file!=0);
    if( !file ) continue;
    if( nonempty ) check("existing destination seeded",fwrite("sentinel",1,8,file)==8);
    fclose(file);
    query = sqlite3_mprintf("SELECT dolt_dump('--file-name',%Q,'-r',%Q)",path,formats[format]);
    rc = sqlite3_exec(db,query,0,0,&err);
    check("existing destination refusal",rc==SQLITE_CANTOPEN);
    check("existing destination diagnostic",err && strstr(err,"destination already exists"));
    sqlite3_free(query); sqlite3_free(err);
    file = fopen(path,"rb");
    check("existing destination retained",file!=0);
    if( file ){
      size_t n = fread(data,1,sizeof(data),file);
      check("existing destination contents",(nonempty ? n==8 && !memcmp(data,"sentinel",8) : n==0) && fgetc(file)==EOF);
      fclose(file);
    }
    remove(path);
  }
  check("existing destination source preserved",number(db,"SELECT count(*) FROM t")==1);
  sqlite3_close(db);
}

int main(void){
  danglingSchema(); danglingApplications(); existingFiles(); memoryDestinations(); values(); virtualTables(); destinationScope(); constraintStreams(); lateCancellation(); failures(); snapshot(); movingSnapshots(); legacyMeta();
#ifndef SQLITE_OMIT_AUTHORIZATION
  authorization();
#endif
  printf("dump_test: %d passed, %d failed\n",passed,failed);
  return failed ? 1 : 0;
}
