
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>

/* dolt_hashof(ref) → commit hash hex for the named ref. Accepts
** branch names, tag names, raw commit hashes, HEAD, and HEAD~N /
** HEAD^N shorthand — whatever doltliteResolveRef understands.
**
** In the decentralized use case the hash is the primary verifier:
** two peers at the same hash have the same history up through
** that commit, and can exchange chunks by hash-reference alone.
** Keep the output lowercase 40-char hex so a SQL-level string
** compare is the whole verification. */
static void doltliteHashofFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  const char *zRef;
  ProllyHash commitHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=1 ){
    sqlite3_result_error(ctx, "dolt_hashof() takes exactly one argument", -1);
    return;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_result_null(ctx);
    return;
  }
  zRef = (const char*)sqlite3_value_text(argv[0]);
  if( !zRef ){
    sqlite3_result_null(ctx);
    return;
  }
  db = sqlite3_context_db_handle(ctx);
  rc = doltliteResolveRef(db, zRef, &commitHash);
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_null(ctx);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof: ref resolve failed", -1);
    return;
  }
  doltliteHashToHex(&commitHash, hex);
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

/* Shared lookup: hashof a table inside a catalog (either the
** current working catalog or a catalog loaded from a ref).
** Returns SQLITE_OK with the hex hash written to pHex on success,
** SQLITE_NOTFOUND if zTable isn't in the catalog. */
static int hashofTableInCatalog(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTable,
  char *pHex
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  ProllyHash rootHash;
  int rc;

  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteFindTableRootByName(aTables, nTables, zTable, &rootHash, 0);
  doltliteFreeCatalog(aTables, nTables);
  if( rc!=SQLITE_OK ) return rc;
  doltliteHashToHex(&rootHash, pHex);
  return SQLITE_OK;
}

/* dolt_hashof_table(name)
** dolt_hashof_table(name, ref)
**
** 1-arg form returns the current working-catalog hash of the
** table's prolly root — so it tracks uncommitted edits via
** doltliteFlushCatalogToHash. 2-arg form resolves the ref, loads
** its committed catalog, and reads the table root from there.
**
** History-independence invariant: for two rowsets that reduce to
** the same set of (key,value) pairs, this function must return
** byte-identical hashes regardless of insert order, transient
** deletions, commit chain, or branch. The oracle in
** test/vc_oracle_hashof_test.sh exercises every flavour. */
static void doltliteHashofTableFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  const char *zTable;
  ProllyHash catHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=1 && argc!=2 ){
    sqlite3_result_error(ctx, "dolt_hashof_table() takes 1 or 2 arguments", -1);
    return;
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_result_null(ctx);
    return;
  }
  zTable = (const char*)sqlite3_value_text(argv[0]);
  if( !zTable || !*zTable ){
    sqlite3_result_null(ctx);
    return;
  }
  db = sqlite3_context_db_handle(ctx);

  if( argc==1 ){
    rc = doltliteFlushCatalogToHash(db, &catHash);
  }else{
    const char *zRef;
    ProllyHash commitHash;
    DoltliteCommit commit;
    if( sqlite3_value_type(argv[1])==SQLITE_NULL ){
      sqlite3_result_null(ctx);
      return;
    }
    zRef = (const char*)sqlite3_value_text(argv[1]);
    if( !zRef ){
      sqlite3_result_null(ctx);
      return;
    }
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_result_null(ctx);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_table: ref resolve failed", -1);
      return;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_table: commit load failed", -1);
      return;
    }
    catHash = commit.catalogHash;
    doltliteCommitClear(&commit);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof_table: catalog flush failed", -1);
    return;
  }

  rc = hashofTableInCatalog(db, &catHash, zTable, hex);
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_null(ctx);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "dolt_hashof_table: table not found in catalog", -1);
    return;
  }
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

/* dolt_hashof_db()
** dolt_hashof_db(ref)
**
** 0-arg returns the hash of the current working catalog — the
** content-address of the entire database's current logical state,
** which changes iff any table's root changes or a table is
** added/dropped. 1-arg resolves the ref and returns that commit's
** catalog hash.
**
** This is the single-string proof-of-equivalence between two
** doltlite peers in a decentralized setup: same hash, same state,
** no row-level diffing required. */
static void doltliteHashofDbFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db;
  ProllyHash catHash;
  char hex[PROLLY_HASH_SIZE*2+1];
  int rc;

  if( argc!=0 && argc!=1 ){
    sqlite3_result_error(ctx, "dolt_hashof_db() takes 0 or 1 argument", -1);
    return;
  }
  db = sqlite3_context_db_handle(ctx);

  if( argc==0 ){
    rc = doltliteFlushCatalogToHash(db, &catHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_db: catalog flush failed", -1);
      return;
    }
  }else{
    const char *zRef;
    ProllyHash commitHash;
    DoltliteCommit commit;
    if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
      sqlite3_result_null(ctx);
      return;
    }
    zRef = (const char*)sqlite3_value_text(argv[0]);
    if( !zRef ){
      sqlite3_result_null(ctx);
      return;
    }
    rc = doltliteResolveRef(db, zRef, &commitHash);
    if( rc==SQLITE_NOTFOUND ){
      sqlite3_result_null(ctx);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_db: ref resolve failed", -1);
      return;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &commitHash, &commit);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "dolt_hashof_db: commit load failed", -1);
      return;
    }
    catHash = commit.catalogHash;
    doltliteCommitClear(&commit);
  }

  doltliteHashToHex(&catHash, hex);
  sqlite3_result_text(ctx, hex, PROLLY_HASH_SIZE*2, SQLITE_TRANSIENT);
}

int doltliteHashofRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_hashof", 1, SQLITE_UTF8, 0,
                               doltliteHashofFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_table", 1, SQLITE_UTF8, 0,
                                 doltliteHashofTableFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_table", 2, SQLITE_UTF8, 0,
                                 doltliteHashofTableFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_db", 0, SQLITE_UTF8, 0,
                                 doltliteHashofDbFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "dolt_hashof_db", 1, SQLITE_UTF8, 0,
                                 doltliteHashofDbFunc, 0, 0);
  }
  return rc;
}

#endif
