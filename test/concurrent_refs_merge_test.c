#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"
#include "prolly_hash.h"
#include "chunk_store.h"

/* SQL tag and remote edits are durable when the call returns, so a later
** COMMIT of row writes keeps a peer's other ref edits. Uncommitted tag,
** remote, and tracking edits merge at chunk-store commit. The same name
** moved on both sides refuses with BUSY_SNAPSHOT and leaves the peer ref. */

static int nPass = 0;
static int nFail = 0;
static char gPid[32];
static char result_buf[1024];

static void check(const char *name, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

static void expectEq(const char *name, const char *got, const char *want){
  if( got && want && strcmp(got, want)==0 ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n  got:  %s\n  want: %s\n",
            name, got ? got : "(null)", want ? want : "(null)");
  }
}

static void hold(char *dst, int n, const char *src){
  snprintf(dst, n, "%s", src ? src : "");
}

static const char *queryScalar(sqlite3 *db, const char *sql){
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
  }else if( rc!=SQLITE_DONE ){
    snprintf(result_buf, sizeof(result_buf), "ERROR: %s", sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return result_buf;
}

static int execSql(sqlite3 *db, const char *sql){
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "  SQL rc=%d %s\n  SQL: %s\n", rc, err ? err : "", sql);
    sqlite3_free(err);
  }
  return rc;
}

static int execf(sqlite3 *db, const char *fmt, ...){
  va_list ap;
  char *sql;
  int rc;
  va_start(ap, fmt);
  sql = sqlite3_vmprintf(fmt, ap);
  va_end(ap);
  if( !sql ) return SQLITE_NOMEM;
  rc = execSql(db, sql);
  sqlite3_free(sql);
  return rc;
}

static void dbPath(char *out, int n, const char *role){
  snprintf(out, n, "/tmp/refsmerge_%s_%s.db", gPid, role);
}

static void wipe(const char *path){
  char side[512];
  const char *base = strrchr(path, '/');
  remove(path);
  snprintf(side, sizeof(side), "%s-wal", path);
  remove(side);
  snprintf(side, sizeof(side), "%s-journal", path);
  remove(side);
  snprintf(side, sizeof(side), "%s-shm", path);
  remove(side);
  if( base ){
    snprintf(side, sizeof(side), "%.*s.%s-lock",
             (int)(base - path) + 1, path, base + 1);
    remove(side);
  }
}

static sqlite3 *openDb(const char *path){
  sqlite3 *db = 0;
  if( sqlite3_open(path, &db)!=SQLITE_OK ){
    fprintf(stderr, "open failed: %s\n", path);
    sqlite3_close(db);
    return 0;
  }
  sqlite3_busy_timeout(db, 5000);
  return db;
}

static void closeAll(sqlite3 *a, sqlite3 *b, sqlite3 *c){
  if( a && !sqlite3_get_autocommit(a) ){
    sqlite3_exec(a, "ROLLBACK", 0, 0, 0);
  }
  sqlite3_close(a);
  sqlite3_close(b);
  sqlite3_close(c);
}

static sqlite3 *newLocal(const char *path){
  sqlite3 *db;
  wipe(path);
  db = openDb(path);
  if( !db ) return 0;
  if( execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY);"
      "INSERT INTO t VALUES(1);"
      "SELECT dolt_commit('-Am','init');")!=SQLITE_OK ){
    sqlite3_close(db);
    return 0;
  }
  return db;
}

/* Load the peer, then begin the transaction whose refs must be merged. */
static sqlite3 *peerAndBegin(sqlite3 *a, const char *path){
  sqlite3 *b = openDb(path);
  if( !b ) return 0;
  queryScalar(b, "SELECT count(*) FROM dolt_branches");
  if( execSql(a, "BEGIN")!=SQLITE_OK ){
    sqlite3_close(b);
    return 0;
  }
  return b;
}

static int commitWrite(sqlite3 *db){
  char *err = 0;
  int rc;
  if( execSql(db, "INSERT INTO t VALUES(9)")!=SQLITE_OK ) return SQLITE_ERROR;
  rc = sqlite3_exec(db, "COMMIT", 0, 0, &err);
  if( rc!=SQLITE_OK && rc!=SQLITE_BUSY_SNAPSHOT ){
    fprintf(stderr, "  COMMIT rc=%d %s\n", rc, err ? err : "");
  }
  sqlite3_free(err);
  return rc;
}

static int sideBranch(sqlite3 *db, const char *name, int rowid){
  return execf(db,
    "SELECT dolt_branch('%q');"
    "SELECT dolt_checkout('%q');"
    "INSERT INTO t VALUES(%d);"
    "SELECT dolt_commit('-am','%q');"
    "SELECT dolt_checkout('main');",
    name, name, rowid, name);
}

static int makeRemote(const char *path){
  sqlite3 *db;
  wipe(path);
  db = openDb(path);
  if( !db ) return 0;
  if( execSql(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY);"
      "INSERT INTO t VALUES(1);"
      "SELECT dolt_commit('-Am','remote init');")!=SQLITE_OK ){
    sqlite3_close(db);
    return 0;
  }
  sqlite3_close(db);
  return 1;
}

static int advanceRemote(const char *path, char *hash, int hashN){
  sqlite3 *db = openDb(path);
  if( !db ) return 0;
  if( execSql(db,
      "INSERT INTO t VALUES(2);"
      "SELECT dolt_commit('-am','remote more');")!=SQLITE_OK ){
    sqlite3_close(db);
    return 0;
  }
  hold(hash, hashN, queryScalar(db, "SELECT commit_hash FROM dolt_log LIMIT 1"));
  sqlite3_close(db);
  return hash[0] && strncmp(hash, "ERROR:", 6)!=0;
}

static void scenarioTagAdd(void){
  char path[256];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "tagadd");
  a = newLocal(path);
  check("tag_add_setup", a!=0);
  if( !a ) return;
  check("tag_add_keep", execSql(a, "SELECT dolt_tag('keep')")==SQLITE_OK);
  b = peerAndBegin(a, path);
  check("tag_add_peer", b!=0);
  if( b ){
    check("tag_add_new", execSql(a, "SELECT dolt_tag('t_a')")==SQLITE_OK);
    check("tag_add_branch", execSql(b, "SELECT dolt_branch('b_b')")==SQLITE_OK);
    check("tag_add_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("tag_add_reopen", c!=0);
  if( c ){
    expectEq("tag_add_branches", queryScalar(c,
      "SELECT group_concat(name, ',') FROM "
      "(SELECT name FROM dolt_branches ORDER BY 1)"),
      "b_b,main");
    expectEq("tag_add_tags", queryScalar(c,
      "SELECT group_concat(tag_name, ',') FROM "
      "(SELECT tag_name FROM dolt_tags ORDER BY 1)"),
      "keep,t_a");
  }
  closeAll(0, 0, c);
  wipe(path);
}

static void scenarioTagDelete(void){
  char path[256];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "tagdel");
  a = newLocal(path);
  check("tag_delete_setup", a!=0);
  if( !a ) return;
  check("tag_delete_base", execSql(a,
      "SELECT dolt_tag('base_tag');"
      "SELECT dolt_remote('add','origin','file:///tmp/refsmerge-origin');"
      )==SQLITE_OK);
  b = peerAndBegin(a, path);
  check("tag_delete_peer", b!=0);
  if( b ){
    check("tag_delete_drop", execSql(a, "SELECT dolt_tag('-d','base_tag')")==SQLITE_OK);
    check("tag_delete_branch", execSql(b, "SELECT dolt_branch('b_b')")==SQLITE_OK);
    check("tag_delete_other", execSql(b, "SELECT dolt_tag('t_b')")==SQLITE_OK);
    check("tag_delete_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("tag_delete_reopen", c!=0);
  if( c ){
    expectEq("tag_delete_branches", queryScalar(c,
      "SELECT group_concat(name, ',') FROM "
      "(SELECT name FROM dolt_branches ORDER BY 1)"),
      "b_b,main");
    expectEq("tag_delete_tags", queryScalar(c,
      "SELECT group_concat(tag_name, ',') FROM "
      "(SELECT tag_name FROM dolt_tags ORDER BY 1)"),
      "t_b");
    expectEq("tag_delete_remote", queryScalar(c,
      "SELECT group_concat(name, ',') FROM "
      "(SELECT name FROM dolt_remotes ORDER BY 1)"),
      "origin");
  }
  closeAll(0, 0, c);
  wipe(path);
}

static void scenarioRemoteAdd(void){
  char path[256];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "remoteadd");
  a = newLocal(path);
  check("remote_add_setup", a!=0);
  if( !a ) return;
  check("remote_add_origin", execSql(a,
      "SELECT dolt_remote('add','origin','file:///tmp/refsmerge-origin');"
      )==SQLITE_OK);
  b = peerAndBegin(a, path);
  check("remote_add_peer", b!=0);
  if( b ){
    check("remote_add_ra", execSql(a,
        "SELECT dolt_remote('add','ra','file:///tmp/refsmerge-ra');")==SQLITE_OK);
    check("remote_add_branch", execSql(b, "SELECT dolt_branch('b_b')")==SQLITE_OK);
    check("remote_add_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("remote_add_reopen", c!=0);
  if( c ){
    expectEq("remote_add_branches", queryScalar(c,
      "SELECT group_concat(name, ',') FROM "
      "(SELECT name FROM dolt_branches ORDER BY 1)"),
      "b_b,main");
    expectEq("remote_add_remotes", queryScalar(c,
      "SELECT group_concat(name, ',') FROM "
      "(SELECT name FROM dolt_remotes ORDER BY 1)"),
      "origin,ra");
  }
  closeAll(0, 0, c);
  wipe(path);
}

static void scenarioRemoteRetarget(void){
  char path[256];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "retarget");
  a = newLocal(path);
  check("remote_retarget_setup", a!=0);
  if( !a ) return;
  check("remote_retarget_origin", execSql(a,
      "SELECT dolt_remote('add','origin','file:///tmp/refsmerge-old');"
      )==SQLITE_OK);
  b = peerAndBegin(a, path);
  check("remote_retarget_peer", b!=0);
  if( b ){
    check("remote_retarget_remove",
          execSql(a, "SELECT dolt_remote('remove','origin')")==SQLITE_OK);
    check("remote_retarget_add", execSql(a,
        "SELECT dolt_remote('add','origin','file:///tmp/refsmerge-new');"
        )==SQLITE_OK);
    check("remote_retarget_branch",
          execSql(b, "SELECT dolt_branch('b_b')")==SQLITE_OK);
    check("remote_retarget_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("remote_retarget_reopen", c!=0);
  if( c ){
    expectEq("remote_retarget_url", queryScalar(c,
      "SELECT url FROM dolt_remotes WHERE name='origin'"),
      "file:///tmp/refsmerge-new");
    expectEq("remote_retarget_branch", queryScalar(c,
      "SELECT count(*) FROM dolt_branches WHERE name='b_b'"), "1");
  }
  closeAll(0, 0, c);
  wipe(path);
}

static void scenarioTagMove(void){
  char path[256];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "tagmove");
  a = newLocal(path);
  check("tag_move_setup", a!=0);
  if( !a ) return;
  check("tag_move_left", sideBranch(a, "left", 2)==SQLITE_OK);
  check("tag_move_base", execSql(a, "SELECT dolt_tag('t')")==SQLITE_OK);
  b = peerAndBegin(a, path);
  check("tag_move_peer", b!=0);
  if( b ){
    check("tag_move_retag", execSql(a,
        "SELECT dolt_tag('-d','t');"
        "SELECT dolt_tag('-m','from-a','t','left');")==SQLITE_OK);
    check("tag_move_branch", execSql(b, "SELECT dolt_branch('b_b')")==SQLITE_OK);
    check("tag_move_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("tag_move_reopen", c!=0);
  if( c ){
    expectEq("tag_move_message", queryScalar(c,
      "SELECT message FROM dolt_tags WHERE tag_name='t'"), "from-a");
    expectEq("tag_move_branch", queryScalar(c,
      "SELECT count(*) FROM dolt_branches WHERE name='b_b'"), "1");
  }
  closeAll(0, 0, c);
  wipe(path);
}

static void hashOf(ProllyHash *out, const char *text){
  prollyHashCompute(text, (int)strlen(text), out);
}

static int openStore(ChunkStore *cs, const char *path, int create){
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
  memset(cs, 0, sizeof(*cs));
  if( create ) flags |= SQLITE_OPEN_CREATE;
  return chunkStoreOpen(cs, sqlite3_vfs_find(0), path, flags);
}

/* Commit merges only when a pending chunk accompanies the refs edit.
** A refs blob already stored (a tag delete that restores an older blob)
** does not itself count as pending, so each commit adds a distinct chunk. */
static int commitRefs(ChunkStore *cs){
  static unsigned seq = 0;
  unsigned char payload[64];
  ProllyHash h;
  int n;
  int rc;
  seq++;
  n = snprintf((char*)payload, sizeof(payload), "refs-merge-pending-%u", seq);
  rc = chunkStorePut(cs, payload, n, &h);
  if( rc==SQLITE_OK ) rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  return rc;
}

static int seedBase(ChunkStore *cs, const char *path){
  ProllyHash h;
  hashOf(&h, "main-tip");
  wipe(path);
  if( openStore(cs, path, 1)!=SQLITE_OK ) return 0;
  if( chunkStoreAddBranch(cs, "main", &h)!=SQLITE_OK ) return 0;
  if( chunkStoreSetDefaultBranch(cs, "main")!=SQLITE_OK ) return 0;
  return commitRefs(cs)==SQLITE_OK;
}

static int peerAddBranch(const char *path, const char *name){
  ChunkStore peer;
  ProllyHash h;
  int rc;
  hashOf(&h, name);
  if( openStore(&peer, path, 0)!=SQLITE_OK ) return 0;
  rc = chunkStoreAddBranch(&peer, name, &h);
  if( rc==SQLITE_OK ) rc = commitRefs(&peer);
  chunkStoreClose(&peer);
  return rc==SQLITE_OK;
}

static int tagIs(ChunkStore *cs, const char *name, const ProllyHash *want){
  ProllyHash got;
  if( chunkStoreFindTag(cs, name, &got)!=SQLITE_OK ) return 0;
  return prollyHashCompare(&got, want)==0;
}

static int branchIs(ChunkStore *cs, const char *name){
  return chunkStoreFindBranch(cs, name, 0)==SQLITE_OK;
}

static int remoteIs(ChunkStore *cs, const char *name, const char *url){
  const char *got = 0;
  if( chunkStoreFindRemote(cs, name, &got)!=SQLITE_OK || !got ) return 0;
  return strcmp(got, url)==0;
}

static int trackingIs(ChunkStore *cs, const ProllyHash *want){
  ProllyHash got;
  if( chunkStoreFindTracking(cs, "origin", "main", &got)!=SQLITE_OK ) return 0;
  return prollyHashCompare(&got, want)==0;
}

/* Unchanged tag, remote, and tracking stay while a new branch merges in. */
static void storeEqualUntouched(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash tag, track, localTip;
  int freshOpen = 0;
  dbPath(path, (int)sizeof(path), "csequal");
  hashOf(&tag, "tag-keep");
  hashOf(&track, "track-keep");
  hashOf(&localTip, "local-tip");
  check("cs_equal_seed", seedBase(&local, path));
  check("cs_equal_refs",
        chunkStoreAddTagFull(&local, "keep", &tag, "ann", "a@x", 1, "keep")==SQLITE_OK
        && chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-origin")==SQLITE_OK
        && chunkStoreUpdateTracking(&local, "origin", "main", &track)==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_equal_peer", peerAddBranch(path, "peer_b"));
  check("cs_equal_local",
        chunkStoreAddBranch(&local, "local_b", &localTip)==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_equal_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  freshOpen = 1;
  if( freshOpen ){
    check("cs_equal_tag", tagIs(&fresh, "keep", &tag));
    check("cs_equal_remote", remoteIs(&fresh, "origin", "file:///tmp/refsmerge-origin"));
    check("cs_equal_tracking", trackingIs(&fresh, &track));
    check("cs_equal_peer_branch", branchIs(&fresh, "peer_b"));
    check("cs_equal_local_branch", branchIs(&fresh, "local_b"));
    chunkStoreClose(&fresh);
  }
  wipe(path);
}

static void storeTagAdd(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash tag;
  dbPath(path, (int)sizeof(path), "cstagadd");
  hashOf(&tag, "tag-new");
  check("cs_tag_add_seed", seedBase(&local, path));
  check("cs_tag_add_peer", peerAddBranch(path, "peer_b"));
  check("cs_tag_add_local",
        chunkStoreAddTagFull(&local, "t_new", &tag, "ann", "a@x", 4, "added")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_tag_add_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_tag_add_tag", tagIs(&fresh, "t_new", &tag));
  check("cs_tag_add_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTagReplace(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash oldH, newH;
  dbPath(path, (int)sizeof(path), "cstagmove");
  hashOf(&oldH, "tag-old");
  hashOf(&newH, "tag-new");
  check("cs_tag_move_seed", seedBase(&local, path));
  check("cs_tag_move_base",
        chunkStoreAddTagFull(&local, "t", &oldH, "ann", "a@x", 1, "old")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_tag_move_peer", peerAddBranch(path, "peer_b"));
  check("cs_tag_move_local",
        chunkStoreDeleteTag(&local, "t")==SQLITE_OK
        && chunkStoreAddTagFull(&local, "t", &newH, "ann", "a@x", 2, "new")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_tag_move_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_tag_move_tag", tagIs(&fresh, "t", &newH));
  check("cs_tag_move_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTagDelete(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash oldH;
  dbPath(path, (int)sizeof(path), "cstagdel");
  hashOf(&oldH, "tag-old");
  check("cs_tag_del_seed", seedBase(&local, path));
  check("cs_tag_del_base",
        chunkStoreAddTagFull(&local, "t", &oldH, "ann", "a@x", 1, "old")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_tag_del_peer", peerAddBranch(path, "peer_b"));
  check("cs_tag_del_local",
        chunkStoreDeleteTag(&local, "t")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_tag_del_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_tag_del_gone", chunkStoreFindTag(&fresh, "t", 0)==SQLITE_NOTFOUND);
  check("cs_tag_del_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTagRetagConflict(void){
  char path[256];
  ChunkStore local, peer, fresh;
  ProllyHash baseH, ours, theirs;
  int peerOpen = 0;
  dbPath(path, (int)sizeof(path), "csretag");
  hashOf(&baseH, "tag-base");
  hashOf(&ours, "tag-ours");
  hashOf(&theirs, "tag-theirs");
  check("cs_retag_seed", seedBase(&local, path));
  check("cs_retag_base",
        chunkStoreAddTagFull(&local, "t", &baseH, "ann", "a@x", 1, "base")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_retag_peer_open", openStore(&peer, path, 0)==SQLITE_OK);
  peerOpen = 1;
  if( peerOpen ){
    check("cs_retag_peer",
          chunkStoreDeleteTag(&peer, "t")==SQLITE_OK
          && chunkStoreAddTagFull(&peer, "t", &theirs, "bee", "b@x", 2, "from-b")==SQLITE_OK
          && commitRefs(&peer)==SQLITE_OK);
    chunkStoreClose(&peer);
  }
  check("cs_retag_local",
        chunkStoreDeleteTag(&local, "t")==SQLITE_OK
        && chunkStoreAddTagFull(&local, "t", &ours, "ann", "a@x", 3, "from-a")==SQLITE_OK);
  check("cs_retag_busy", commitRefs(&local)==SQLITE_BUSY_SNAPSHOT);
  chunkStoreClose(&local);
  check("cs_retag_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_retag_keeps_peer", tagIs(&fresh, "t", &theirs));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTagDeleteMoved(void){
  char path[256];
  ChunkStore local, peer, fresh;
  ProllyHash baseH, theirs;
  dbPath(path, (int)sizeof(path), "csdelmoved");
  hashOf(&baseH, "tag-base");
  hashOf(&theirs, "tag-moved");
  check("cs_delmoved_seed", seedBase(&local, path));
  check("cs_delmoved_base",
        chunkStoreAddTagFull(&local, "t", &baseH, "ann", "a@x", 1, "base")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_delmoved_peer_open", openStore(&peer, path, 0)==SQLITE_OK);
  check("cs_delmoved_peer",
        chunkStoreDeleteTag(&peer, "t")==SQLITE_OK
        && chunkStoreAddTagFull(&peer, "t", &theirs, "bee", "b@x", 2, "moved")==SQLITE_OK
        && commitRefs(&peer)==SQLITE_OK);
  chunkStoreClose(&peer);
  check("cs_delmoved_local", chunkStoreDeleteTag(&local, "t")==SQLITE_OK);
  check("cs_delmoved_busy", commitRefs(&local)==SQLITE_BUSY_SNAPSHOT);
  chunkStoreClose(&local);
  check("cs_delmoved_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_delmoved_keeps_peer", tagIs(&fresh, "t", &theirs));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeRemoteAdd(void){
  char path[256];
  ChunkStore local, fresh;
  dbPath(path, (int)sizeof(path), "csremadd");
  check("cs_remote_add_seed", seedBase(&local, path));
  check("cs_remote_add_peer", peerAddBranch(path, "peer_b"));
  check("cs_remote_add_local",
        chunkStoreAddRemote(&local, "ra", "file:///tmp/refsmerge-ra")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_remote_add_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_remote_add_url", remoteIs(&fresh, "ra", "file:///tmp/refsmerge-ra"));
  check("cs_remote_add_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeRemoteReplace(void){
  char path[256];
  ChunkStore local, fresh;
  dbPath(path, (int)sizeof(path), "csremmove");
  check("cs_remote_move_seed", seedBase(&local, path));
  check("cs_remote_move_base",
        chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-old")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_remote_move_peer", peerAddBranch(path, "peer_b"));
  check("cs_remote_move_local",
        chunkStoreDeleteRemote(&local, "origin")==SQLITE_OK
        && chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-new")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_remote_move_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_remote_move_url",
        remoteIs(&fresh, "origin", "file:///tmp/refsmerge-new"));
  check("cs_remote_move_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeRemoteDelete(void){
  char path[256];
  ChunkStore local, fresh;
  dbPath(path, (int)sizeof(path), "csremdel");
  check("cs_remote_del_seed", seedBase(&local, path));
  check("cs_remote_del_base",
        chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-origin")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_remote_del_peer", peerAddBranch(path, "peer_b"));
  check("cs_remote_del_local",
        chunkStoreDeleteRemote(&local, "origin")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_remote_del_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_remote_del_gone",
        chunkStoreFindRemote(&fresh, "origin", 0)==SQLITE_NOTFOUND);
  check("cs_remote_del_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTrackingAdd(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash track;
  dbPath(path, (int)sizeof(path), "cstrackadd");
  hashOf(&track, "track-new");
  check("cs_track_add_seed", seedBase(&local, path));
  check("cs_track_add_remote",
        chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-origin")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_track_add_peer", peerAddBranch(path, "peer_b"));
  check("cs_track_add_local",
        chunkStoreUpdateTracking(&local, "origin", "main", &track)==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_track_add_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_track_add_hash", trackingIs(&fresh, &track));
  check("cs_track_add_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTrackingReplace(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash oldH, newH;
  dbPath(path, (int)sizeof(path), "cstrackmove");
  hashOf(&oldH, "track-old");
  hashOf(&newH, "track-new");
  check("cs_track_move_seed", seedBase(&local, path));
  check("cs_track_move_base",
        chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-origin")==SQLITE_OK
        && chunkStoreUpdateTracking(&local, "origin", "main", &oldH)==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_track_move_peer", peerAddBranch(path, "peer_b"));
  check("cs_track_move_local",
        chunkStoreUpdateTracking(&local, "origin", "main", &newH)==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_track_move_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_track_move_hash", trackingIs(&fresh, &newH));
  check("cs_track_move_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void storeTrackingDelete(void){
  char path[256];
  ChunkStore local, fresh;
  ProllyHash oldH;
  dbPath(path, (int)sizeof(path), "cstrackdel");
  hashOf(&oldH, "track-old");
  check("cs_track_del_seed", seedBase(&local, path));
  check("cs_track_del_base",
        chunkStoreAddRemote(&local, "origin", "file:///tmp/refsmerge-origin")==SQLITE_OK
        && chunkStoreUpdateTracking(&local, "origin", "main", &oldH)==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  check("cs_track_del_peer", peerAddBranch(path, "peer_b"));
  check("cs_track_del_local",
        chunkStoreDeleteTracking(&local, "origin", "main")==SQLITE_OK
        && commitRefs(&local)==SQLITE_OK);
  chunkStoreClose(&local);
  check("cs_track_del_reopen", openStore(&fresh, path, 0)==SQLITE_OK);
  check("cs_track_del_gone",
        chunkStoreFindTracking(&fresh, "origin", "main", 0)==SQLITE_NOTFOUND);
  check("cs_track_del_remote",
        remoteIs(&fresh, "origin", "file:///tmp/refsmerge-origin"));
  check("cs_track_del_peer", branchIs(&fresh, "peer_b"));
  chunkStoreClose(&fresh);
  wipe(path);
}

static void scenarioTrackingPeerFetch(void){
  char path[256], remote[256], url[300], hash[128];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "trackpeer");
  dbPath(remote, (int)sizeof(remote), "trackpeer_remote");
  snprintf(url, sizeof(url), "file://%s", remote);
  hash[0] = 0;
  check("track_peer_remote", makeRemote(remote));
  check("track_peer_advance", advanceRemote(remote, hash, (int)sizeof(hash)));
  a = newLocal(path);
  check("track_peer_setup", a!=0);
  if( !a ){ wipe(remote); return; }
  check("track_peer_add", execf(a,
      "SELECT dolt_remote('add','origin','%q')", url)==SQLITE_OK);
  b = peerAndBegin(a, path);
  check("track_peer_begun", b!=0);
  if( b ){
    check("track_peer_tag", execSql(a, "SELECT dolt_tag('held')")==SQLITE_OK);
    check("track_peer_fetch",
          execSql(b, "SELECT dolt_fetch('origin','main')")==SQLITE_OK);
    check("track_peer_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("track_peer_reopen", c!=0);
  if( c ){
    expectEq("track_peer_tag", queryScalar(c,
      "SELECT count(*) FROM dolt_tags WHERE tag_name='held'"), "1");
    expectEq("track_peer_hash", queryScalar(c,
      "SELECT hash FROM dolt_remote_branches "
      "WHERE name='remotes/origin/main'"), hash);
  }
  closeAll(0, 0, c);
  wipe(path);
  wipe(remote);
}

static void scenarioTrackingLocalFetch(void){
  char path[256], remote[256], url[300], hash[128];
  sqlite3 *a, *b = 0, *c = 0;
  dbPath(path, (int)sizeof(path), "tracklocal");
  dbPath(remote, (int)sizeof(remote), "tracklocal_remote");
  snprintf(url, sizeof(url), "file://%s", remote);
  hash[0] = 0;
  check("track_local_remote", makeRemote(remote));
  a = newLocal(path);
  check("track_local_setup", a!=0);
  if( !a ){ wipe(remote); return; }
  check("track_local_first_fetch", execf(a,
      "SELECT dolt_remote('add','origin','%q');"
      "SELECT dolt_fetch('origin','main');", url)==SQLITE_OK);
  check("track_local_advance", advanceRemote(remote, hash, (int)sizeof(hash)));
  b = peerAndBegin(a, path);
  check("track_local_begun", b!=0);
  if( b ){
    check("track_local_fetch",
          execSql(a, "SELECT dolt_fetch('origin','main')")==SQLITE_OK);
    check("track_local_branch",
          execSql(b, "SELECT dolt_branch('peer')")==SQLITE_OK);
    check("track_local_commit", commitWrite(a)==SQLITE_OK);
  }
  closeAll(a, b, 0);
  c = openDb(path);
  check("track_local_reopen", c!=0);
  if( c ){
    expectEq("track_local_branch", queryScalar(c,
      "SELECT count(*) FROM dolt_branches WHERE name='peer'"), "1");
    expectEq("track_local_hash", queryScalar(c,
      "SELECT hash FROM dolt_remote_branches "
      "WHERE name='remotes/origin/main'"), hash);
  }
  closeAll(0, 0, c);
  wipe(path);
  wipe(remote);
}

int main(void){
  snprintf(gPid, sizeof(gPid), "%ld", (long)getpid());
  scenarioTagAdd();
  scenarioTagDelete();
  scenarioRemoteAdd();
  scenarioRemoteRetarget();
  scenarioTagMove();
  scenarioTrackingPeerFetch();
  scenarioTrackingLocalFetch();
  storeEqualUntouched();
  storeTagAdd();
  storeTagReplace();
  storeTagDelete();
  storeTagRetagConflict();
  storeTagDeleteMoved();
  storeRemoteAdd();
  storeRemoteReplace();
  storeRemoteDelete();
  storeTrackingAdd();
  storeTrackingReplace();
  storeTrackingDelete();
  printf("Results: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass + nFail);
  return nFail > 0 ? 1 : 0;
}
