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
};
static int writer(void *ctx, const void *data, int n){
  Sink *s = ctx;
  s->calls++;
  if( n>s->max ) s->max=n;
  if( s->fail && s->calls>=s->fail ) return SQLITE_FULL;
  if( s->peer && !s->advanced ){
    s->advanced=1;
    check("peer advances during snapshot",sql(s->peer,
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

int main(void){
  values(); failures(); snapshot(); legacyMeta();
#ifndef SQLITE_OMIT_AUTHORIZATION
  authorization();
#endif
  printf("dump_test: %d passed, %d failed\n",passed,failed);
  return failed ? 1 : 0;
}
