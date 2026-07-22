#include <stdio.h>
#include <string.h>
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_remote.h"

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

static void mkhash(ProllyHash *h, u8 b){
  memset(h, 0, sizeof(*h));
  h->data[0] = b;
  h->data[1] = (u8)(b ^ 0x5a);
}

static int openStore(ChunkStore *cs, const char *path){
  char wal[512];
  remove(path);
  snprintf(wal, sizeof(wal), "%s-wal", path);
  remove(wal);
  memset(cs, 0, sizeof(*cs));
  return chunkStoreOpen(cs, sqlite3_vfs_find(0), path,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
}

/* Build a refs blob from a fresh store carrying main@a and foo@b, then hand
** back that same starting blob so scenarios can mutate copies of it. */
static u8 *seedRefs(ChunkStore *cs, const ProllyHash *a, const ProllyHash *b,
                    int *pn){
  u8 *blob = 0;
  if( chunkStoreSetDefaultBranch(cs, "main")!=SQLITE_OK ) return 0;
  if( chunkStoreAddBranch(cs, "main", a)!=SQLITE_OK ) return 0;
  if( chunkStoreAddBranch(cs, "foo", b)!=SQLITE_OK ) return 0;
  if( chunkStoreSerializeRefs(cs)!=SQLITE_OK ) return 0;
  if( chunkStoreCommit(cs)!=SQLITE_OK ) return 0;
  if( chunkStoreSerializeRefsToBlob(cs, &blob, pn)!=SQLITE_OK ) return 0;
  return blob;
}

int main(void){
  ChunkStore cs;
  ProllyHash Ha, Hb, Hc, Hd, found;
  u8 *curBlob = 0;
  int nCur = 0;
  int rc;
  const char *path = "/tmp/scoped_refs_push_test.db";

  sqlite3_initialize();
  printf("Scoped refs-push authorization tests\n");
  printf("====================================\n\n");

  mkhash(&Ha, 0xA1);
  mkhash(&Hb, 0xB2);
  mkhash(&Hc, 0xC3);
  mkhash(&Hd, 0xD4);

  check("open store", openStore(&cs, path)==SQLITE_OK);
  curBlob = seedRefs(&cs, &Ha, &Hb, &nCur);
  check("seed refs (main@a, foo@b)", curBlob!=0);
  if( !curBlob ){ printf("\n%d passed, %d failed\n", nPass, nFail); return 1; }

  /* Attack: a push declaring branch foo carries a blob that also rewrites
  ** main. The scoped validator must reject it, with or without force. */
  {
    ChunkStore tmp;
    u8 *blob = 0; int n = 0;
    memset(&tmp, 0, sizeof(tmp));
    chunkStoreLoadRefsFromBlob(&tmp, curBlob, nCur);
    chunkStoreUpdateBranch(&tmp, "main", &Hc);
    chunkStoreSerializeRefsToBlob(&tmp, &blob, &n);
    chunkStoreClose(&tmp);

    rc = doltliteValidateScopedRefsUpdate(&cs, blob, n, "foo", 0);
    check("reject rewriting main while pushing foo", rc==SQLITE_CONSTRAINT);
    rc = doltliteValidateScopedRefsUpdate(&cs, blob, n, "foo", 1);
    check("force does not authorize rewriting main", rc==SQLITE_CONSTRAINT);

    /* Pre-fix behavior: installing the blob wholesale (what the server used to
    ** do) silently hijacks main. This is the reported vulnerability. */
    {
      ChunkStore victim;
      check("open victim", openStore(&victim, "/tmp/scoped_refs_victim.db")==SQLITE_OK);
      chunkStoreAddBranch(&victim, "main", &Ha);
      chunkStoreAddBranch(&victim, "foo", &Hb);
      chunkStoreSerializeRefs(&victim);
      chunkStoreCommit(&victim);
      chunkStoreInstallRefsBlob(&victim, blob, n);
      chunkStoreFindBranch(&victim, "main", &found);
      check("pre-fix: raw install hijacks main (vuln demo)",
            memcmp(&found, &Hc, sizeof(found))==0);
      chunkStoreClose(&victim);
      remove("/tmp/scoped_refs_victim.db");
    }
    sqlite3_free(blob);
  }

  /* Attack: delete another branch by omitting it from the blob. */
  {
    ChunkStore tmp;
    u8 *blob = 0; int n = 0;
    memset(&tmp, 0, sizeof(tmp));
    chunkStoreSetDefaultBranch(&tmp, "foo");
    chunkStoreAddBranch(&tmp, "foo", &Hb);
    chunkStoreSerializeRefsToBlob(&tmp, &blob, &n);
    chunkStoreClose(&tmp);
    rc = doltliteValidateScopedRefsUpdate(&cs, blob, n, "foo", 1);
    check("reject deleting main via omission", rc==SQLITE_CONSTRAINT);
    sqlite3_free(blob);
  }

  /* Legitimate: move only foo. Non-fast-forward needs force (Hd has no commit
  ** chunk, so it is not an ancestor of Hb). */
  {
    ChunkStore tmp;
    u8 *blob = 0; int n = 0;
    memset(&tmp, 0, sizeof(tmp));
    chunkStoreLoadRefsFromBlob(&tmp, curBlob, nCur);
    chunkStoreUpdateBranch(&tmp, "foo", &Hd);
    chunkStoreSerializeRefsToBlob(&tmp, &blob, &n);
    chunkStoreClose(&tmp);
    rc = doltliteValidateScopedRefsUpdate(&cs, blob, n, "foo", 0);
    check("reject non-fast-forward foo without force", rc==SQLITE_CONSTRAINT);
    rc = doltliteValidateScopedRefsUpdate(&cs, blob, n, "foo", 1);
    check("allow forced move of foo (scoped)", rc==SQLITE_OK);
    sqlite3_free(blob);
  }

  /* Legitimate: an unchanged blob is always in scope. */
  rc = doltliteValidateScopedRefsUpdate(&cs, curBlob, nCur, "foo", 0);
  check("allow no-op refs update", rc==SQLITE_OK);

  sqlite3_free(curBlob);
  chunkStoreClose(&cs);
  remove(path);

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
