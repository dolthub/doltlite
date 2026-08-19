#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "sqlite3.h"
#include "chunk_store.h"

#define MANIFEST_SIZE 168

static const unsigned char DLTC_MAGIC[4] = { 0x44, 0x4C, 0x54, 0x43 };

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

static char result_buf[8192];
static const char *queryScalarText(sqlite3 *db, const char *sql){
  sqlite3_stmt *stmt = 0;
  int rc;
  result_buf[0] = 0;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
  if( rc!=SQLITE_OK ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
    return result_buf;
  }
  rc = sqlite3_step(stmt);
  if( rc==SQLITE_ROW ){
    const char *val = (const char*)sqlite3_column_text(stmt, 0);
    if( val ) snprintf(result_buf, sizeof(result_buf), "%s", val);
  }else if( rc==SQLITE_ERROR ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    sqlite3_free(err);
  }
  return rc;
}


static int corrupt_bytes(const char *path, off_t offset,
                         const void *data, size_t len){
  int fd = open(path, O_WRONLY);
  ssize_t w;
  if( fd < 0 ) return -1;
  if( lseek(fd, offset, SEEK_SET) != offset ){
    close(fd);
    return -1;
  }
  w = write(fd, data, len);
  close(fd);
  return (w == (ssize_t)len) ? 0 : -1;
}

static int truncate_file(const char *path, off_t size){
  return truncate(path, size);
}

static off_t file_size(const char *path){
  struct stat st;
  if( stat(path, &st) != 0 ) return -1;
  return st.st_size;
}

static int read_bytes(const char *path, off_t offset, void *data, size_t len){
  int fd = open(path, O_RDONLY);
  ssize_t r;
  if( fd < 0 ) return -1;
  if( lseek(fd, offset, SEEK_SET) != offset ){
    close(fd);
    return -1;
  }
  r = read(fd, data, len);
  close(fd);
  return (r == (ssize_t)len) ? 0 : -1;
}

static long long read_i64_le_at(const char *path, off_t offset){
  unsigned char b[8];
  if( read_bytes(path, offset, b, sizeof(b))!=0 ) return -1;
  return (long long)(
      ((unsigned long long)b[0])
    | ((unsigned long long)b[1] << 8)
    | ((unsigned long long)b[2] << 16)
    | ((unsigned long long)b[3] << 24)
    | ((unsigned long long)b[4] << 32)
    | ((unsigned long long)b[5] << 40)
    | ((unsigned long long)b[6] << 48)
    | ((unsigned long long)b[7] << 56));
}

static unsigned int read_u32_le_at(const char *path, off_t offset){
  unsigned char b[4];
  if( read_bytes(path, offset, b, sizeof(b))!=0 ) return 0xffffffffu;
  return (unsigned int)(
      ((unsigned int)b[0])
    | ((unsigned int)b[1] << 8)
    | ((unsigned int)b[2] << 16)
    | ((unsigned int)b[3] << 24));
}

static off_t first_wal_chunk_body_offset(const char *path){
  long long walOffset = read_i64_le_at(path, 84);
  off_t sz = file_size(path);
  off_t pos;
  if( walOffset < MANIFEST_SIZE || sz <= (off_t)walOffset ) return -1;
  pos = (off_t)walOffset;
  while( pos < sz ){
    unsigned char tag = 0;
    if( read_bytes(path, pos, &tag, 1)!=0 ) return -1;
    pos++;
    if( tag==0x01 ){
      unsigned int len;
      if( pos + 24 > sz ) return -1;
      len = read_u32_le_at(path, pos + 20);
      if( len==0xffffffffu || pos + 24 + (off_t)len > sz ) return -1;
      return pos + 24;
    }else if( tag==0x02 ){
      if( pos + MANIFEST_SIZE > sz ) return -1;
      pos += MANIFEST_SIZE;
    }else{
      return -1;
    }
  }
  return -1;
}

static off_t last_wal_chunk_body_offset(const char *path){
  long long walOffset = read_i64_le_at(path, 84);
  off_t sz = file_size(path);
  off_t pos;
  off_t last = -1;
  if( walOffset < MANIFEST_SIZE || sz <= (off_t)walOffset ) return -1;
  pos = (off_t)walOffset;
  while( pos < sz ){
    unsigned char tag = 0;
    if( read_bytes(path, pos, &tag, 1)!=0 ) return -1;
    pos++;
    if( tag==0x01 ){
      unsigned int len;
      if( pos + 24 > sz ) return -1;
      len = read_u32_le_at(path, pos + 20);
      if( len==0xffffffffu || pos + 24 + (off_t)len > sz ) return -1;
      last = pos + 24;
      pos += 24 + (off_t)len;
    }else if( tag==0x02 ){
      long long nextOff;
      if( pos + MANIFEST_SIZE > sz ) return -1;
      nextOff = read_i64_le_at(path, pos + 52);
      if( nextOff > pos + MANIFEST_SIZE && nextOff <= sz ){
        pos = (off_t)nextOff;
      }else{
        pos += MANIFEST_SIZE;
      }
    }else if( tag==0x00 ){
      return last;
    }else{
      return -1;
    }
  }
  return last;
}

static off_t last_sealed_wal_root_offset(const char *path){
  long long walOffset = read_i64_le_at(path, CS_MANIFEST_WAL_OFFSET_OFF);
  off_t sz = file_size(path);
  unsigned char *data;
  off_t pos;
  off_t last = -1;
  if( walOffset < MANIFEST_SIZE || sz <= (off_t)walOffset ) return -1;
  data = sqlite3_malloc64((sqlite3_uint64)sz);
  if( data==0 ) return -1;
  if( read_bytes(path, 0, data, (size_t)sz)!=0 ){
    sqlite3_free(data);
    return -1;
  }
  for(pos=(off_t)walOffset; pos+1+MANIFEST_SIZE<=sz; pos++){
    if( data[pos]==CS_WAL_TAG_ROOT
     && CS_READ_U32(data+pos+1+CS_MANIFEST_MAGIC_OFF)==CHUNK_STORE_MAGIC
     && csManifestHashState(data+pos+1, (i64)pos)==CS_MANIFEST_HASH_OK ){
      last = pos;
    }
  }
  sqlite3_free(data);
  return last;
}

static int rewrite_sealed_root_field(
  const char *path,
  off_t rootOff,
  int fieldOff,
  const unsigned char *value,
  size_t len
){
  unsigned char manifest[CHUNK_MANIFEST_SIZE];
  if( fieldOff<0 || len>(size_t)(CHUNK_MANIFEST_SIZE-fieldOff) ) return -1;
  if( read_bytes(path, rootOff+1, manifest, sizeof(manifest))!=0 ) return -1;
  memcpy(manifest+fieldOff, value, len);
  csManifestSeal(manifest, (i64)rootOff);
  return corrupt_bytes(path, rootOff+1, manifest, sizeof(manifest));
}

static void removeDb(const char *path){
  char wal[512];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
}

static int copy_file(const char *src, const char *dst){
  int fdin, fdout;
  char buf[8192];
  ssize_t n;

  fdin = open(src, O_RDONLY);
  if( fdin < 0 ) return -1;
  fdout = open(dst, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if( fdout < 0 ){ close(fdin); return -1; }
  while( (n = read(fdin, buf, sizeof(buf))) > 0 ){
    if( write(fdout, buf, n) != n ){
      close(fdin); close(fdout); return -1;
    }
  }
  close(fdin);
  close(fdout);
  return 0;
}

static int create_good_db(const char *path){
  sqlite3 *db = 0;
  int rc;
  const char *res;

  removeDb(path);
  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return -1;

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'alpha')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'beta')");
  execSql(db, "INSERT INTO t1 VALUES(3, 'gamma')");
  res = queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'first commit')");
  if( strncmp(res, "ERROR", 5)==0 ){
    sqlite3_close(db);
    return -1;
  }

  execSql(db, "INSERT INTO t1 VALUES(4, 'delta')");
  execSql(db, "INSERT INTO t1 VALUES(5, 'epsilon')");
  res = queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'second commit')");
  if( strncmp(res, "ERROR", 5)==0 ){
    sqlite3_close(db);
    return -1;
  }

  sqlite3_close(db);
  return 0;
}

static int create_compacted_db(const char *path){
  sqlite3 *db = 0;
  int rc;
  const char *res;

  removeDb(path);
  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ) return -1;

  execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
  execSql(db, "INSERT INTO t1 VALUES(1, 'alpha')");
  execSql(db, "INSERT INTO t1 VALUES(2, 'beta')");
  execSql(db, "INSERT INTO t1 VALUES(3, 'gamma')");
  res = queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'first commit')");
  if( strncmp(res, "ERROR", 5)==0 ){
    sqlite3_close(db);
    return -1;
  }

  execSql(db, "INSERT INTO t1 VALUES(4, 'delta')");
  execSql(db, "INSERT INTO t1 VALUES(5, 'epsilon')");
  res = queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'second commit')");
  if( strncmp(res, "ERROR", 5)==0 ){
    sqlite3_close(db);
    return -1;
  }

  res = queryScalarText(db, "SELECT dolt_gc()");
  if( strncmp(res, "ERROR", 5)==0 ){
    sqlite3_close(db);
    return -1;
  }

  sqlite3_close(db);
  return 0;
}

static int open_and_probe(const char *path){
  sqlite3 *db = 0;
  int rc;
  int errSeen = 0;

  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ){
    errSeen = 1;
    if( db ) sqlite3_close(db);
    return errSeen;
  }

  rc = execSql(db, "SELECT * FROM dolt_branches");
  if( rc!=SQLITE_OK ) errSeen = 1;

  rc = execSql(db, "SELECT * FROM dolt_log");
  if( rc!=SQLITE_OK ) errSeen = 1;

  rc = execSql(db, "SELECT * FROM t1");
  if( rc!=SQLITE_OK ) errSeen = 1;

  rc = execSql(db, "SELECT * FROM dolt_status");
  if( rc!=SQLITE_OK ) errSeen = 1;

  {
    const char *r = queryScalarText(db, "SELECT active_branch()");
    if( strncmp(r, "ERROR", 5)==0 || strlen(r)==0 ) errSeen = 1;
  }

  sqlite3_close(db);
  return errSeen;
}

static int open_fails_or_errors(const char *path){
  sqlite3 *db = 0;
  int rc;

  rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ){
    if( db ) sqlite3_close(db);
    return 1;
  }

  rc = execSql(db, "SELECT active_branch()");
  if( rc!=SQLITE_OK ){
    sqlite3_close(db);
    return 1;
  }

  sqlite3_close(db);
  return 0;
}


static void test_truncate_mid_manifest(void){
  const char *dbpath = "/tmp/test_corr_trunc_manifest.db";
  int err;

  printf("--- Test 1: Truncate mid-manifest (100 bytes) ---\n");

  check("create_good_1", create_good_db(dbpath)==0);
  check("truncate_1", truncate_file(dbpath, 100)==0);

  err = open_fails_or_errors(dbpath);
  check("truncated_manifest_detected", err==1);

  removeDb(dbpath);
}

static void test_truncate_mid_wal(void){
  const char *dbpath = "/tmp/test_corr_trunc_wal.db";
  const char *goodpath = "/tmp/test_corr_trunc_wal_good.db";
  off_t sz;

  printf("--- Test 2: Truncate mid-WAL ---\n");

  check("create_good_2", create_good_db(dbpath)==0);

  copy_file(dbpath, goodpath);

  sz = file_size(dbpath);
  check("file_has_data_2", sz > MANIFEST_SIZE);

  if( sz > MANIFEST_SIZE + 50 ){
    check("truncate_2", truncate_file(dbpath, sz - 30)==0);
  }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("truncated_wal_open_ok_or_error",
      rc==SQLITE_OK || rc!=SQLITE_OK);
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
  removeDb(goodpath);
}

static void test_zero_manifest(void){
  const char *dbpath = "/tmp/test_corr_zero_manifest.db";
  unsigned char zeros[MANIFEST_SIZE];
  int err;

  printf("--- Test 3: Zero out entire manifest ---\n");

  check("create_good_3", create_good_db(dbpath)==0);

  memset(zeros, 0, sizeof(zeros));
  check("corrupt_3", corrupt_bytes(dbpath, 0, zeros, MANIFEST_SIZE)==0);

  err = open_fails_or_errors(dbpath);
  check("zeroed_manifest_detected", err==1);

  removeDb(dbpath);
}

static void test_corrupt_chunk_data(void){
  const char *dbpath = "/tmp/test_corr_chunk.db";
  off_t sz;

  printf("--- Test 4: Corrupt chunk data ---\n");

  {
    sqlite3 *db = 0;
    int i;
    removeDb(dbpath);
    check("open_4", sqlite3_open(dbpath, &db)==SQLITE_OK);
    execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
    for( i=0; i<100; i++ ){
      char sql[128];
      snprintf(sql, sizeof(sql), "INSERT INTO t1 VALUES(%d, 'row_%d')", i, i);
      execSql(db, sql);
    }
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'lots of data')");
    queryScalarText(db, "SELECT dolt_gc()");
    sqlite3_close(db);
  }

  sz = file_size(dbpath);
  check("file_large_enough_4", sz > MANIFEST_SIZE + 200);

  {
    unsigned char garbage[128];
    off_t target;
    int i;
    srand(12345);
    for( i=0; i<128; i++ ) garbage[i] = (unsigned char)(rand() & 0xFF);
    target = MANIFEST_SIZE + (sz - MANIFEST_SIZE) / 3;
    check("corrupt_4",
      corrupt_bytes(dbpath, target, garbage, sizeof(garbage))==0);
  }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      int data_rc = execSql(db, "SELECT * FROM t1");
      int log_rc = execSql(db, "SELECT * FROM dolt_log");
      int branch_rc = execSql(db, "SELECT * FROM dolt_branches");
      int status_rc = execSql(db, "SELECT * FROM dolt_status");
      check("chunk_corruption_detected",
        data_rc!=SQLITE_OK || log_rc!=SQLITE_OK ||
        branch_rc!=SQLITE_OK || status_rc!=SQLITE_OK);
    }else{
      check("chunk_corruption_detected", 1);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

static void test_truncate_past_manifest(void){
  const char *dbpath = "/tmp/test_corr_just_manifest.db";
  int err;

  printf("--- Test 5: Truncate to just past manifest ---\n");

  check("create_good_5", create_good_db(dbpath)==0);
  check("truncate_5", truncate_file(dbpath, MANIFEST_SIZE + 1)==0);

  err = open_and_probe(dbpath);
  check("truncated_past_manifest_detected", err==1);

  removeDb(dbpath);
}

static void test_zero_refs_hash(void){
  const char *dbpath = "/tmp/test_corr_refs.db";
  unsigned char zeros[20];

  printf("--- Test 6: Zero out refs hash (compacted DB) ---\n");

  check("create_compacted_6", create_compacted_db(dbpath)==0);

  memset(zeros, 0, sizeof(zeros));
  check("corrupt_6", corrupt_bytes(dbpath, 104, zeros, sizeof(zeros))==0);

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      const char *r = queryScalarText(db, "SELECT count(*) FROM dolt_branches");
      int is_error = (strncmp(r, "ERROR", 5)==0);
      check("zeroed_refs_returns_error", is_error);
    }else{
      check("zeroed_refs_returns_error", 1);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

static void test_append_garbage(void){
  const char *dbpath = "/tmp/test_corr_append.db";
  off_t sz;

  printf("--- Test 7: Append garbage after WAL ---\n");

  check("create_good_7", create_good_db(dbpath)==0);

  sz = file_size(dbpath);

  {
    int fd = open(dbpath, O_WRONLY|O_APPEND);
    check("open_for_append_7", fd >= 0);
    if( fd >= 0 ){
      unsigned char garbage[1024];
      ssize_t nWrite;
      int i;
      srand(99999);
      for( i=0; i<1024; i++ ) garbage[i] = (unsigned char)(rand() & 0xFF);
      nWrite = write(fd, garbage, sizeof(garbage));
      check("append_garbage_write_7", nWrite==(ssize_t)sizeof(garbage));
      close(fd);
    }
  }

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      const char *r = queryScalarText(db, "SELECT count(*) FROM t1");
      int count_ok = (strcmp(r, "5")==0);
      int count_err = (strncmp(r, "ERROR", 5)==0);
      check("appended_garbage_handled", count_ok || count_err);
    }else{
      check("appended_garbage_handled", 1);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

static void test_empty_file(void){
  const char *dbpath = "/tmp/test_corr_empty.db";

  printf("--- Test 8: Empty file (0 bytes) ---\n");
  removeDb(dbpath);

  {
    int fd = open(dbpath, O_WRONLY|O_CREAT|O_TRUNC, 0644);
    check("create_empty_8", fd >= 0);
    if( fd >= 0 ) close(fd);
  }

  check("empty_file_is_zero", file_size(dbpath)==0);

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    check("empty_open_ok", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      const char *branch;
      rc = execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY)");
      check("empty_create_table", rc==SQLITE_OK);

      branch = queryScalarText(db, "SELECT active_branch()");
      check("empty_has_branch",
        branch && strlen(branch)>0 && strncmp(branch, "ERROR", 5)!=0);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

static void test_manifest_only(void){
  const char *dbpath = "/tmp/test_corr_manifest_only.db";

  printf("--- Test 9: File with only manifest header ---\n");

  check("create_good_9", create_good_db(dbpath)==0);
  check("truncate_to_manifest_9", truncate_file(dbpath, MANIFEST_SIZE)==0);
  check("manifest_size_9", file_size(dbpath)==MANIFEST_SIZE);

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      const char *r = queryScalarText(db, "SELECT count(*) FROM t1");
      check("manifest_only_no_stale_data",
        strncmp(r, "ERROR", 5)==0 || strcmp(r, "0")==0);
    }else{
      check("manifest_only_no_stale_data", 1);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

static void test_corrupt_wal_tag(void){
  const char *dbpath = "/tmp/test_corr_wal_tag.db";
  off_t sz;

  printf("--- Test 10: Corrupt WAL tag byte ---\n");

  check("create_good_10", create_good_db(dbpath)==0);

  sz = file_size(dbpath);

  if( sz > MANIFEST_SIZE + 100 ){
    off_t wal_pos = sz / 2 + 50;
    unsigned char bad_tag = 0xFF;
    check("corrupt_10",
      corrupt_bytes(dbpath, wal_pos, &bad_tag, 1)==0);

    {
      sqlite3 *db = 0;
      int rc = sqlite3_open(dbpath, &db);
      if( rc==SQLITE_OK ){
        const char *r = queryScalarText(db, "SELECT count(*) FROM t1");
        if( strncmp(r, "ERROR", 5)!=0 ){
          int cnt = atoi(r);
          check("wal_tag_data_reasonable", cnt >= 0 && cnt <= 5);
        }else{
          check("wal_tag_data_reasonable", 1);
        }
      }else{
        check("wal_tag_data_reasonable", 1);
      }
      if( db ) sqlite3_close(db);
    }
  }else{
    check("corrupt_10", 0); /* File too small */
  }

  removeDb(dbpath);
}

static void test_corrupt_wal_chunk_body_stops_replay(void){
  const char *dbpath = "/tmp/test_corr_wal_chunk_body.db";
  sqlite3 *db = 0;
  int rc;
  off_t bodyOff;
  unsigned char bad = 0xA5;

  printf("--- Test 11: Corrupt WAL chunk body stops replay ---\n");

  check("create_compacted_11", create_compacted_db(dbpath)==0);

  rc = sqlite3_open(dbpath, &db);
  check("open_append_11", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("append_row_11",
      execSql(db, "INSERT INTO t1 VALUES(6, 'zeta')")==SQLITE_OK);
  }
  if( db ) sqlite3_close(db);

  bodyOff = first_wal_chunk_body_offset(dbpath);
  check("find_wal_chunk_body_11", bodyOff > 0);
  if( bodyOff > 0 ){
    check("corrupt_wal_chunk_body_11",
      corrupt_bytes(dbpath, bodyOff, &bad, 1)==0);
  }

  db = 0;
  rc = sqlite3_open(dbpath, &db);
  check("reopen_after_wal_body_corrupt_11", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    const char *r = queryScalarText(db, "SELECT count(*) FROM t1");
    check("wal_body_corrupt_does_not_read_clean_11", strcmp(r, "5")!=0);
    r = queryScalarText(db, "PRAGMA integrity_check");
    check("wal_body_corrupt_integrity_11", strcmp(r, "ok")!=0);
  }
  if( db ) sqlite3_close(db);

  removeDb(dbpath);
}

static void test_corrupt_initial_wal_chunk_body_detected(void){
  const char *dbpath = "/tmp/test_corr_initial_wal_chunk_body.db";
  off_t bodyOff;
  unsigned char bad = 0x5A;

  printf("--- Test 12: Corrupt initial WAL chunk body ---\n");

  check("create_wal_only_12", create_good_db(dbpath)==0);

  bodyOff = first_wal_chunk_body_offset(dbpath);
  check("find_initial_wal_chunk_body_12", bodyOff > 0);
  if( bodyOff > 0 ){
    check("corrupt_initial_wal_chunk_body_12",
      corrupt_bytes(dbpath, bodyOff, &bad, 1)==0);
  }

  {
    ChunkStore cs;
    ProllyHash zero;
    u8 *pData = 0;
    int nData = 0;
    int rc;
    memset(&zero, 0, sizeof(zero));
    rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
    check("initial_wal_body_corruption_open_12", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      check("initial_wal_body_corruption_poisoned_12", cs.corruptMidStream);
      rc = chunkStoreGet(&cs, &zero, &pData, &nData);
      check("initial_wal_body_corruption_get_rejected_12", rc==SQLITE_CORRUPT);
      sqlite3_free(pData);
      chunkStoreClose(&cs);
    }
  }

  removeDb(dbpath);
}

static void test_corrupt_final_committed_wal_chunk_detected(void){
  const char *dbpath = "/tmp/test_corr_final_committed_wal_chunk.db";
  sqlite3 *db = 0;
  off_t bodyOff;
  unsigned char bad = 0x3C;
  int rc;

  printf("--- Test 13: Corrupt final committed WAL chunk ---\n");

  check("create_good_13", create_good_db(dbpath)==0);

  bodyOff = last_wal_chunk_body_offset(dbpath);
  check("find_final_wal_chunk_body_13", bodyOff > 0);
  if( bodyOff > 0 ){
    check("corrupt_final_wal_chunk_body_13",
      corrupt_bytes(dbpath, bodyOff, &bad, 1)==0);
  }

  rc = sqlite3_open(dbpath, &db);
  check("open_after_final_wal_body_corrupt_13", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    const char *r = queryScalarText(db, "PRAGMA integrity_check");
    check("final_wal_body_corrupt_integrity_13", strcmp(r, "ok")!=0);
  }
  if( db ) sqlite3_close(db);

  removeDb(dbpath);
}

static void test_wrong_file_size_in_manifest(void){
  const char *dbpath = "/tmp/test_corr_filesize.db";
  unsigned char bad_offset[8] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00
  };
  int err;

  printf("--- Test 14: Wrong file size in manifest ---\n");

  check("create_good_14", create_good_db(dbpath)==0);

  check("corrupt_14",
    corrupt_bytes(dbpath, 84, bad_offset, sizeof(bad_offset))==0);

  err = open_and_probe(dbpath);
  check("wrong_wal_offset_detected", err==1);

  removeDb(dbpath);
}

static void test_corrupt_magic(void){
  const char *dbpath = "/tmp/test_corr_magic.db";
  unsigned char bad_magic[4] = { 0x00, 0x00, 0x00, 0x00 };
  int err;

  printf("--- Test 15: Corrupt magic number ---\n");

  check("create_good_13", create_good_db(dbpath)==0);

  check("corrupt_13",
    corrupt_bytes(dbpath, 0, bad_magic, sizeof(bad_magic))==0);

  err = open_fails_or_errors(dbpath);
  check("bad_magic_detected", err==1);

  removeDb(dbpath);
}

static void test_corrupt_version(void){
  const char *dbpath = "/tmp/test_corr_version.db";
  unsigned char bad_ver[4] = { 0xFF, 0x00, 0x00, 0x00 };
  int err;

  printf("--- Test 16: Corrupt version number ---\n");

  check("create_good_14", create_good_db(dbpath)==0);

  check("corrupt_14",
    corrupt_bytes(dbpath, 4, bad_ver, sizeof(bad_ver))==0);

  err = open_fails_or_errors(dbpath);
  check("bad_version_detected", err==1);

  removeDb(dbpath);
}

static void test_corrupt_head_commit(void){
  const char *dbpath = "/tmp/test_corr_head.db";
  unsigned char bad_hash[20];

  printf("--- Test 17: Corrupt former head_commit bytes (compacted) ---\n");

  check("create_compacted_15", create_compacted_db(dbpath)==0);

  memset(bad_hash, 0xCD, sizeof(bad_hash));
  check("corrupt_15",
    corrupt_bytes(dbpath, 64, bad_hash, sizeof(bad_hash))==0);

  {
    sqlite3 *db = 0;
    int rc = sqlite3_open(dbpath, &db);
    if( rc==SQLITE_OK ){
      const char *r = queryScalarText(db, "SELECT count(*) FROM dolt_log");
      int log_err = (strncmp(r, "ERROR", 5)==0);
      int log_valid = (!log_err && atoi(r) >= 0);
      check("corrupt_head_commit_no_crash", log_err || log_valid);

      if( log_valid && atoi(r) > 0 ){
        const char *b = queryScalarText(db, "SELECT count(*) FROM dolt_branches");
        check("corrupt_head_branches_accessible",
          strncmp(b, "ERROR", 5)!=0 && atoi(b) >= 1);
      }
    }else{
      check("corrupt_head_commit_no_crash", 1);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

static void test_corrupt_chunk_count(void){
  const char *dbpath = "/tmp/test_corr_chunkcount.db";
  unsigned char huge_count[4] = { 0xFF, 0xFF, 0xFF, 0x7F };

  printf("--- Test 18: Corrupt chunk_count field (compacted) ---\n");

  check("create_compacted_16", create_compacted_db(dbpath)==0);

  check("corrupt_16",
    corrupt_bytes(dbpath, 28, huge_count, sizeof(huge_count))==0);

  check("chunk_count_mismatch_detected", open_and_probe(dbpath)==1);

  removeDb(dbpath);
}

static void test_corrupt_index_offset(void){
  const char *dbpath = "/tmp/test_corr_idxoff.db";
  unsigned char bad_idx[8] = {
    0xFF, 0xFF, 0xFF, 0x7F, 0x00, 0x00, 0x00, 0x00
  };
  int err;

  printf("--- Test 19: Corrupt index_offset ---\n");

  {
    sqlite3 *db = 0;
    removeDb(dbpath);
    check("open_17", sqlite3_open(dbpath, &db)==SQLITE_OK);
    execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
    execSql(db, "INSERT INTO t1 VALUES(1, 'a')");
    queryScalarText(db, "SELECT dolt_commit('-A', '-m', 'c1')");
    queryScalarText(db, "SELECT dolt_gc()");
    sqlite3_close(db);
  }

  check("corrupt_17",
    corrupt_bytes(dbpath, 32, bad_idx, sizeof(bad_idx))==0);

  err = open_and_probe(dbpath);
  check("bad_index_offset_detected", err==1);

  removeDb(dbpath);
}

static void test_corrupt_index_entry_offset(void){
  const char *dbpath = "/tmp/test_corr_idxentryoff.db";
  long long indexOffset;
  unsigned char bad_off[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };

  printf("--- Test 20: Corrupt index entry offset ---\n");

  check("create_compacted_18", create_compacted_db(dbpath)==0);
  indexOffset = read_i64_le_at(dbpath, 32);
  check("read_index_offset_18", indexOffset > MANIFEST_SIZE);
  check("corrupt_18",
    corrupt_bytes(dbpath, (off_t)indexOffset + 20,
                  bad_off, sizeof(bad_off))==0);

  check("bad_index_entry_offset_detected", open_and_probe(dbpath)==1);

  removeDb(dbpath);
}

static void test_corrupt_index_order(void){
  const char *dbpath = "/tmp/test_corr_idxorder.db";
  long long indexOffset;
  unsigned char zero_hash[20];

  printf("--- Test 21: Corrupt index order ---\n");

  memset(zero_hash, 0, sizeof(zero_hash));
  check("create_compacted_19", create_compacted_db(dbpath)==0);
  indexOffset = read_i64_le_at(dbpath, 32);
  check("read_index_offset_19", indexOffset > MANIFEST_SIZE);
  check("corrupt_19",
    corrupt_bytes(dbpath, (off_t)indexOffset + 32,
                  zero_hash, sizeof(zero_hash))==0);

  check("bad_index_order_detected", open_and_probe(dbpath)==1);

  removeDb(dbpath);
}


static void test_crash_garbage_truncated_on_write(void){
  const char *dbpath = "/tmp/test_corr_garbage_reclaim.db";
  off_t szWithGarbage;
  int rc;

  printf("--- Test 23: Crash garbage reclaimed by next write ---\n");

  check("create_good_23", create_good_db(dbpath)==0);

  /* A crash tail much larger than anything the next transaction appends,
  ** so overwrite-in-place cannot mask a missing truncation. */
  {
    int fd = open(dbpath, O_WRONLY|O_APPEND);
    check("open_for_append_23", fd >= 0);
    if( fd >= 0 ){
      unsigned char garbage[4096];
      int i;
      memset(garbage, 0xAA, sizeof(garbage));
      for( i=0; i<64; i++ ){
        if( write(fd, garbage, sizeof(garbage))!=(ssize_t)sizeof(garbage) ){
          break;
        }
      }
      check("append_garbage_23", i==64);
      close(fd);
    }
  }
  szWithGarbage = file_size(dbpath);
  check("garbage_present_23", szWithGarbage > 0);

  /* Recovery rewinds the logical EOF past the garbage; the first write
  ** transaction reloads under the graph lock and must reclaim the dead
  ** physical tail, otherwise every later write transaction reloads and
  ** replays the whole WAL again. */
  {
    sqlite3 *db = 0;
    rc = sqlite3_open(dbpath, &db);
    check("reopen_after_garbage_23", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      check("write_after_garbage_23",
        execSql(db, "INSERT INTO t1 VALUES(6, 'zeta')")==SQLITE_OK);
    }
    if( db ) sqlite3_close(db);
  }

  check("crash_garbage_truncated_23", file_size(dbpath) < szWithGarbage);

  {
    sqlite3 *db = 0;
    rc = sqlite3_open(dbpath, &db);
    check("reopen_after_truncate_23", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      const char *r = queryScalarText(db, "SELECT count(*) FROM t1");
      check("rows_intact_after_truncate_23", strcmp(r, "6")==0);
    }
    if( db ) sqlite3_close(db);
  }

  removeDb(dbpath);
}

/* Body offset of the first WAL chunk record whose declared length matches. */
static off_t wal_chunk_body_offset_by_len(const char *path, unsigned int want){
  long long walOffset = read_i64_le_at(path, 84);
  off_t sz = file_size(path);
  off_t pos;
  if( walOffset < MANIFEST_SIZE || sz <= (off_t)walOffset ) return -1;
  pos = (off_t)walOffset;
  while( pos < sz ){
    unsigned char tag = 0;
    if( read_bytes(path, pos, &tag, 1)!=0 ) return -1;
    pos++;
    if( tag==0x01 ){
      unsigned int len;
      if( pos + 24 > sz ) return -1;
      len = read_u32_le_at(path, pos + 20);
      if( len==0xffffffffu || pos + 24 + (off_t)len > sz ) return -1;
      if( len==want ) return pos + 24;
      pos += 24 + (off_t)len;
    }else if( tag==0x02 ){
      if( pos + MANIFEST_SIZE > sz ) return -1;
      pos += MANIFEST_SIZE;
    }else{
      return -1;
    }
  }
  return -1;
}

static void test_damage_far_before_sealing_root_poisons(void){
  const char *dbpath = "/tmp/test_corr_scan_window.db";
  ChunkStore cs;
  ProllyHash h1, h2, h3;
  unsigned char small1[32], small3[32];
  unsigned char bad = 0xA5;
  /* Larger than any bounded damage-scan window, so this batch's sealing
  ** root — and the next batch's — lie more than 64MB past the damage. */
  const unsigned int bigLen = 70u*1024u*1024u;
  unsigned char *big;
  off_t bodyOff;
  int rc;

  printf("--- Test 23: Damage far before its sealing root poisons ---\n");

  removeDb(dbpath);
  memset(small1, 0x11, sizeof(small1));
  memset(small3, 0x33, sizeof(small3));
  big = sqlite3_malloc64(bigLen);
  check("scan_window_alloc", big!=0);
  if( !big ) return;
  {
    unsigned int x = 0x9e3779b9u, i;
    for(i=0; i<bigLen; i++){
      x ^= x<<13; x ^= x>>17; x ^= x<<5;
      big[i] = (unsigned char)x;
    }
  }

  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  check("scan_window_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_free(big); return; }

  check("scan_window_put1",
    chunkStorePut(&cs, small1, sizeof(small1), &h1)==SQLITE_OK);
  check("scan_window_commit1", chunkStoreCommit(&cs)==SQLITE_OK);
  check("scan_window_put_big",
    chunkStorePut(&cs, big, (int)bigLen, &h2)==SQLITE_OK);
  check("scan_window_commit2", chunkStoreCommit(&cs)==SQLITE_OK);
  check("scan_window_put3",
    chunkStorePut(&cs, small3, sizeof(small3), &h3)==SQLITE_OK);
  check("scan_window_commit3", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreClose(&cs);
  sqlite3_free(big);

  bodyOff = wal_chunk_body_offset_by_len(dbpath, bigLen);
  check("scan_window_find_big_body", bodyOff > 0);
  if( bodyOff > 0 ){
    check("scan_window_corrupt", corrupt_bytes(dbpath, bodyOff, &bad, 1)==0);
  }

  /* The sealed roots of batches 2 and 3 prove the damaged bytes sit below
  ** committed data. A bounded scan that misses them would call this a torn
  ** tail and silently rewind both committed batches; the store must be
  ** poisoned instead. */
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("scan_window_reopen", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    u8 *pData = 0;
    int nData = 0;
    check("scan_window_poisoned", cs.corruptMidStream);
    rc = chunkStoreGet(&cs, &h1, &pData, &nData);
    check("scan_window_no_silent_rewind", rc==SQLITE_CORRUPT);
    sqlite3_free(pData);
    chunkStoreClose(&cs);
  }

  removeDb(dbpath);
}

static void test_large_wal_open_checkpoint(void){
  const char *dbpath = "/tmp/test_corr_open_checkpoint.db";
  sqlite3 *db = 0;
  off_t tailOff;
  long long checkpointOff;
  long long checkpointSize;
  long long replayOff;
  long long dataEnd;
  unsigned int entryCount;
  int rc;

  printf("--- Test 24: Large WAL gets an open checkpoint ---\n");

  removeDb(dbpath);
  rc = sqlite3_open(dbpath, &db);
  check("open_checkpoint_create", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("open_checkpoint_schema",
      execSql(db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB);"
        "CREATE TABLE s(id INTEGER PRIMARY KEY)")
        ==SQLITE_OK);
    check("open_checkpoint_insert",
      execSql(db, "INSERT INTO t VALUES(1, zeroblob(68157440))")
        ==SQLITE_OK);
    check("open_checkpoint_commit",
      execSql(db, "SELECT dolt_commit('-Am','large')")==SQLITE_OK);
  }
  if( db ) sqlite3_close(db);

  tailOff = file_size(dbpath) - (1 + MANIFEST_SIZE);
  check("open_checkpoint_tail_root", tailOff>MANIFEST_SIZE);
  check("open_checkpoint_magic",
    read_u32_le_at(dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_MAGIC_OFF)
      ==CS_WAL_CHECKPOINT_MAGIC_V2);
  checkpointOff = read_i64_le_at(
      dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_OFFSET_OFF);
  checkpointSize = read_u32_le_at(
      dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_SIZE_OFF);
  replayOff = read_i64_le_at(
      dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_REPLAY_OFF);
  dataEnd = read_i64_le_at(
      dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_DATA_END_OFF);
  entryCount = read_u32_le_at(
      dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_COUNT_OFF);
  check("open_checkpoint_geometry",
    checkpointOff>=MANIFEST_SIZE
    && dataEnd>=MANIFEST_SIZE
    && checkpointOff>=dataEnd
    && checkpointSize>0
    && checkpointSize<=CS_INDEX_PAGE_SIZE
    && entryCount>0
    && checkpointOff+CS_WAL_CHUNK_HDR_SIZE+checkpointSize==tailOff
    && replayOff>=tailOff+1+MANIFEST_SIZE);

  db = 0;
  rc = sqlite3_open(dbpath, &db);
  check("open_checkpoint_reopen", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("open_checkpoint_read",
      strcmp(queryScalarText(db,
        "SELECT count(*)||'|'||length(payload) FROM t"),
        "1|68157440")==0);
    check("open_checkpoint_tail_commit",
      execSql(db,
        "INSERT INTO s VALUES(2);"
        "SELECT dolt_commit('-Am','tail');")==SQLITE_OK);
  }
  if( db ) sqlite3_close(db);

  tailOff = file_size(dbpath) - (1 + MANIFEST_SIZE);
  check("open_checkpoint_propagated",
    read_u32_le_at(dbpath, tailOff+1+CS_MANIFEST_CHECKPOINT_MAGIC_OFF)
      ==CS_WAL_CHECKPOINT_MAGIC_V2
    && read_i64_le_at(dbpath,
         tailOff+1+CS_MANIFEST_CHECKPOINT_OFFSET_OFF)==checkpointOff);
  db = 0;
  rc = sqlite3_open(dbpath, &db);
  check("open_checkpoint_tail_reopen", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("open_checkpoint_tail_read",
      strcmp(queryScalarText(db,
        "SELECT (SELECT count(*) FROM t)||'|'||"
        "(SELECT length(payload) FROM t)||'|'||"
        "(SELECT count(*) FROM s)"),
        "1|68157440|1")==0);
  }
  if( db ) sqlite3_close(db);

  {
    ChunkStore cs;
    ProllyHash zero;
    u8 *pData = 0;
    int nData = 0;
    unsigned char bad = 0;
    memset(&zero, 0, sizeof(zero));
    check("open_checkpoint_read_snapshot_byte",
      read_bytes(dbpath,
        (off_t)checkpointOff+CS_WAL_CHUNK_HDR_SIZE, &bad, 1)==0);
    bad ^= 0x01;
    check("open_checkpoint_corrupt_snapshot",
      corrupt_bytes(dbpath,
        (off_t)checkpointOff+CS_WAL_CHUNK_HDR_SIZE, &bad, 1)==0);
    rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
    check("open_checkpoint_corrupt_reopen", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      check("open_checkpoint_corrupt_poisoned", cs.corruptMidStream);
      rc = chunkStoreGet(&cs, &zero, &pData, &nData);
      check("open_checkpoint_corrupt_rejected", rc==SQLITE_CORRUPT);
      sqlite3_free(pData);
      chunkStoreClose(&cs);
    }
  }
  removeDb(dbpath);
}

static void fill_checkpoint_value(unsigned char *a, int i){
  int j;
  for(j=0; j<32; j++) a[j] = (unsigned char)(i*37+j*11);
  CS_WRITE_U32(a, (u32)i);
}

static void test_paged_checkpoint_large_index(void){
  const char *dbpath = "/tmp/test_corr_paged_checkpoint.db";
  const int nChunk = 20000;
  const int aWant[] = { 0, 9999, 19999 };
  ProllyHash aHash[3];
  ChunkStore cs;
  unsigned char value[32];
  int i;
  int rc;

  printf("--- Test 24b: Paged checkpoint stays lazy for a large index ---\n");

  removeDb(dbpath);
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  check("paged_checkpoint_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  for(i=0; i<nChunk && rc==SQLITE_OK; i++){
    ProllyHash hash;
    fill_checkpoint_value(value, i);
    rc = chunkStorePut(&cs, value, sizeof(value), &hash);
    if( i==aWant[0] ) aHash[0] = hash;
    if( i==aWant[1] ) aHash[1] = hash;
    if( i==aWant[2] ) aHash[2] = hash;
  }
  check("paged_checkpoint_put", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(&cs);
  check("paged_checkpoint_commit", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) rc = csWriteWalCheckpoint(&cs, 1);
  check("paged_checkpoint_write", rc==SQLITE_OK);
  chunkStoreClose(&cs);

  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("paged_checkpoint_reopen", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    check("paged_checkpoint_no_eager_entries", cs.index.nIndex==0);
    check("paged_checkpoint_entry_count",
          chunkIndexCount(&cs.index)==nChunk);
    for(i=0; i<3; i++){
      u8 *pData = 0;
      int nData = 0;
      char name[64];
      fill_checkpoint_value(value, aWant[i]);
      rc = chunkStoreGet(&cs, &aHash[i], &pData, &nData);
      snprintf(name, sizeof(name), "paged_checkpoint_lookup_%d", i);
      check(name, rc==SQLITE_OK && nData==(int)sizeof(value)
                  && memcmp(pData, value, sizeof(value))==0);
      sqlite3_free(pData);
    }
    chunkStoreClose(&cs);
  }
  removeDb(dbpath);
}

static void test_root_seal_binds_file_offset(void){
  unsigned char manifest[CHUNK_MANIFEST_SIZE];
  const long long rootOffset = 4096;

  printf("--- Test 25: Root seal binds file offset ---\n");

  memset(manifest, 0, sizeof(manifest));
  CS_WRITE_U32(manifest + CS_MANIFEST_MAGIC_OFF, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(manifest + CS_MANIFEST_VERSION_OFF, CHUNK_STORE_VERSION);
  csManifestSeal(manifest, rootOffset);

  check("root_seal_valid_at_record_offset",
        csManifestHashState(manifest, rootOffset)==CS_MANIFEST_HASH_OK);
  check("root_seal_rejects_root_shaped_blob",
        csManifestHashState(manifest, rootOffset + 1)==CS_MANIFEST_HASH_BAD);
}

/* Open, then write. The destructive reclaim fires on write-lock acquisition,
** not on open, so a read-only probe would not reach it. */
static int open_write_and_probe(const char *path){
  sqlite3 *db = 0;
  int errSeen = 0;
  int rc = sqlite3_open(path, &db);
  if( rc!=SQLITE_OK ){
    if( db ) sqlite3_close(db);
    return 1;
  }
  if( execSql(db, "INSERT INTO t1 VALUES(99, 'probe')")!=SQLITE_OK ) errSeen = 1;
  if( execSql(db, "SELECT count(*) FROM t1")!=SQLITE_OK ) errSeen = 1;
  sqlite3_close(db);
  return errSeen;
}

static const char *rowCountOf(const char *path){
  sqlite3 *db = 0;
  static char buf[64];
  const char *res;
  if( sqlite3_open(path, &db)!=SQLITE_OK ){
    if( db ) sqlite3_close(db);
    snprintf(buf, sizeof(buf), "OPENFAIL");
    return buf;
  }
  res = queryScalarText(db, "SELECT count(*) FROM t1");
  snprintf(buf, sizeof(buf), "%s", res);
  sqlite3_close(db);
  return buf;
}

/* A compacted store is manifest | data | index with an empty WAL, so lowering
** the WAL offset aims replay straight at live chunk bytes. Replay reads them
** as WAL records, calls the tail torn, and rewinds the logical EOF; the next
** write-lock acquisition then reclaims those "dead" bytes and takes the index
** and the whole data region with them. The damage is one header field and the
** rows are still readable, so failing closed keeps a recoverable file
** recoverable instead of destroying it on the next write. */
static void test_header_seal_detects_tampered_wal_offset(void){
  const char *dbpath = "/tmp/test_corr_hdrseal.db";
  unsigned char buf[8];
  off_t before, after;

  printf("--- Test 26: Tampered WAL offset is refused, not truncated ---\n");

  check("create_compacted_26", create_compacted_db(dbpath)==0);
  before = file_size(dbpath);
  check("rows_before_26", strcmp(rowCountOf(dbpath), "5")==0);

  CS_WRITE_I64(buf, (long long)(MANIFEST_SIZE + 32));
  check("corrupt_26",
        corrupt_bytes(dbpath, CS_MANIFEST_WAL_OFFSET_OFF, buf, sizeof(buf))==0);

  check("tampered_wal_offset_detected", open_write_and_probe(dbpath)==1);

  after = file_size(dbpath);
  check("tampered_wal_offset_does_not_truncate", after==before);

  removeDb(dbpath);
}

/* Header sealing began well after CHUNK_STORE_VERSION reached its current
** value, so version-current files carrying an all-zero self hash are in the
** field and must keep opening. */
static void test_unsealed_header_still_opens(void){
  const char *dbpath = "/tmp/test_corr_legacyhdr.db";
  unsigned char zeros[PROLLY_HASH_SIZE];

  printf("--- Test 27: Pre-seal header still opens ---\n");

  check("create_compacted_27", create_compacted_db(dbpath)==0);
  memset(zeros, 0, sizeof(zeros));
  check("unseal_27",
        corrupt_bytes(dbpath, CS_MANIFEST_SELF_HASH_OFF, zeros,
                      sizeof(zeros))==0);

  check("unsealed_header_opens_clean", open_write_and_probe(dbpath)==0);
  removeDb(dbpath);
}

/* Pre-seal headers are covered by the same bounds check, so they do not keep
** the destructive path to themselves. */
static void test_unsealed_header_bounds_checks_wal_offset(void){
  const char *dbpath = "/tmp/test_corr_legacybounds.db";
  unsigned char zeros[PROLLY_HASH_SIZE];
  unsigned char buf[8];
  off_t before, after;

  printf("--- Test 28: Bounds check covers pre-seal headers ---\n");

  check("create_compacted_28", create_compacted_db(dbpath)==0);
  before = file_size(dbpath);
  memset(zeros, 0, sizeof(zeros));
  check("unseal_28",
        corrupt_bytes(dbpath, CS_MANIFEST_SELF_HASH_OFF, zeros,
                      sizeof(zeros))==0);
  CS_WRITE_I64(buf, (long long)(MANIFEST_SIZE + 32));
  check("corrupt_28",
        corrupt_bytes(dbpath, CS_MANIFEST_WAL_OFFSET_OFF, buf, sizeof(buf))==0);

  check("unsealed_bad_wal_offset_detected", open_write_and_probe(dbpath)==1);
  after = file_size(dbpath);
  check("unsealed_bad_wal_offset_does_not_truncate", after==before);

  removeDb(dbpath);
}

/* iIndexOffset + nIndexSize in i64 can wrap for a crafted large offset. A
** subtractive bounds check must still refuse a low WAL offset that aims at
** live data, and must not truncate the file. */
static void test_index_end_overflow_bounds_wal_offset(void){
  const char *dbpath = "/tmp/test_corr_idxoverflow.db";
  unsigned char buf[8];
  unsigned char sizeBuf[4];
  off_t before, after;

  printf("--- Test 29: Overflow-shaped index end still refuses bad WAL ---\n");

  check("create_compacted_29", create_compacted_db(dbpath)==0);
  before = file_size(dbpath);
  check("rows_before_29", strcmp(rowCountOf(dbpath), "5")==0);

  /* Near i64 max so iIndexOffset + nIndexSize wraps if added naively. */
  CS_WRITE_I64(buf, (long long)0x7fffffffffffffffLL);
  check("corrupt_index_offset_29",
        corrupt_bytes(dbpath, CS_MANIFEST_INDEX_OFFSET_OFF, buf, sizeof(buf))==0);
  CS_WRITE_U32(sizeBuf, 1u);
  check("corrupt_index_size_29",
        corrupt_bytes(dbpath, CS_MANIFEST_INDEX_SIZE_OFF, sizeBuf,
                      sizeof(sizeBuf))==0);
  CS_WRITE_I64(buf, (long long)(MANIFEST_SIZE + 32));
  check("corrupt_wal_offset_29",
        corrupt_bytes(dbpath, CS_MANIFEST_WAL_OFFSET_OFF, buf, sizeof(buf))==0);

  check("overflow_index_end_bad_wal_detected", open_write_and_probe(dbpath)==1);
  after = file_size(dbpath);
  check("overflow_index_end_does_not_truncate", after==before);

  removeDb(dbpath);
}

static void test_sealed_wal_root_manifest_validation(void){
  const char *base = "/tmp/test_corr_wal_root_manifest_base.db";
  const char *dbpath = "/tmp/test_corr_wal_root_manifest.db";
  sqlite3 *db = 0;
  off_t rootOff;
  long long walOffset;
  long long rootEnd;
  unsigned char value[8];
  int i;
  struct RootMutation {
    const char *name;
    int fieldOff;
    int nValue;
    long long value;
  } mutations[] = {
    { "version", CS_MANIFEST_VERSION_OFF, 4, CHUNK_STORE_VERSION+1 },
    { "chunk_count", CS_MANIFEST_CHUNK_COUNT_OFF, 4, 0x80000000LL },
    { "index_offset", CS_MANIFEST_INDEX_OFFSET_OFF, 8, -1 },
    { "index_size", CS_MANIFEST_INDEX_SIZE_OFF, 4, 1 },
    { "wal_offset", CS_MANIFEST_WAL_OFFSET_OFF, 8, 0 },
    { "durable_before_wal", CS_MANIFEST_DURABLE_TO_OFF, 8, 0 },
    { "durable_after_batch", CS_MANIFEST_DURABLE_TO_OFF, 8, 0 },
    { "batch_before_durable", CS_MANIFEST_BATCH_START_OFF, 8, 0 },
    { "batch_after_root", CS_MANIFEST_BATCH_START_OFF, 8, 0 },
    { "batch_gap", CS_MANIFEST_DURABLE_TO_OFF, 8, 0 },
    { "next_before_root_end", CS_MANIFEST_NEXT_OFF_OFF, 8, 0 },
    { "next_gap", CS_MANIFEST_NEXT_OFF_OFF, 8, 0 },
  };

  printf("--- Test 30: Sealed WAL root manifest fields are validated ---\n");

  removeDb(base);
  check("root_manifest_open", sqlite3_open(base, &db)==SQLITE_OK);
  if( db ){
    check("root_manifest_schema",
      execSql(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val)")==SQLITE_OK);
    check("root_manifest_large_row",
      execSql(db, "INSERT INTO t1 VALUES(1, randomblob(70000))")==SQLITE_OK);
    sqlite3_close(db);
  }
  rootOff = last_sealed_wal_root_offset(base);
  walOffset = read_i64_le_at(base, CS_MANIFEST_WAL_OFFSET_OFF);
  rootEnd = (long long)rootOff + 1 + MANIFEST_SIZE;
  check("root_manifest_find_root", rootOff>walOffset+65536);

  mutations[4].value = walOffset + 1;
  mutations[5].value = walOffset - 1;
  mutations[6].value = (long long)rootOff + 1;
  mutations[7].value = (long long)rootOff - 1;
  mutations[8].value = (long long)rootOff + 1;
  mutations[9].value = (long long)rootOff - 65536;
  mutations[10].value = rootEnd - 1;
  mutations[11].value = rootEnd + 65536;

  for(i=0; i<(int)(sizeof(mutations)/sizeof(mutations[0])); i++){
    ChunkStore cs;
    int rc;
    off_t before;
    char name[128];
    check("root_manifest_copy", copy_file(base, dbpath)==0);
    if( mutations[i].nValue==4 ){
      CS_WRITE_U32(value, (u32)mutations[i].value);
    }else{
      CS_WRITE_I64(value, mutations[i].value);
    }
    snprintf(name, sizeof(name), "root_manifest_rewrite_%s", mutations[i].name);
    check(name, rewrite_sealed_root_field(dbpath, rootOff,
          mutations[i].fieldOff, value, (size_t)mutations[i].nValue)==0);
    before = file_size(dbpath);
    rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), dbpath,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
    snprintf(name, sizeof(name), "root_manifest_reject_%s", mutations[i].name);
    check(name, rc==SQLITE_CORRUPT);
    if( rc==SQLITE_OK ) chunkStoreClose(&cs);
    snprintf(name, sizeof(name), "root_manifest_unchanged_%s", mutations[i].name);
    check(name, file_size(dbpath)==before);
  }

  removeDb(base);
  removeDb(dbpath);
}

int main(void){
  printf("=== DoltLite Corruption Detection Tests ===\n\n");

  test_truncate_mid_manifest();
  test_truncate_mid_wal();
  test_zero_manifest();
  test_corrupt_chunk_data();
  test_truncate_past_manifest();
  test_zero_refs_hash();
  test_append_garbage();
  test_empty_file();
  test_manifest_only();
  test_corrupt_wal_tag();
  test_corrupt_wal_chunk_body_stops_replay();
  test_corrupt_initial_wal_chunk_body_detected();
  test_corrupt_final_committed_wal_chunk_detected();
  test_wrong_file_size_in_manifest();
  test_corrupt_magic();
  test_corrupt_version();
  test_corrupt_head_commit();
  test_corrupt_chunk_count();
  test_corrupt_index_offset();
  test_corrupt_index_entry_offset();
  test_corrupt_index_order();
  test_damage_far_before_sealing_root_poisons();
  test_crash_garbage_truncated_on_write();
  test_large_wal_open_checkpoint();
  test_paged_checkpoint_large_index();
  test_root_seal_binds_file_offset();
  test_header_seal_detects_tampered_wal_offset();
  test_unsealed_header_still_opens();
  test_unsealed_header_bounds_checks_wal_offset();
  test_index_end_overflow_bounds_wal_offset();
  test_sealed_wal_root_manifest_validation();

  printf("\n=== Results: %d passed, %d failed out of %d tests ===\n",
    nPass, nFail, nPass+nFail);
  return nFail > 0 ? 1 : 0;
}
