/*
** Three-way catalog merge with conflict detection for Doltlite.
**
** Given three catalog hashes (ancestor, ours, theirs), produce a merged
** catalog. For each table:
**   - If only one side changed it, take that side's root hash.
**   - If both sides changed the same table, return SQLITE_ERROR (conflict).
**   - If a table was added on one side and doesn't exist on the other, include it.
**   - If deleted on one side and unchanged on the other, delete it.
**
** Returns the merged catalog as a serialized chunk stored in the chunk store.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>

/* TableEntry struct — must match prolly_btree.c definition */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  u8 flags;
};

/* Provided by prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/*
** Find a table entry by table number in an array of entries.
** Returns a pointer to the entry, or NULL if not found.
*/
static struct TableEntry *findTableEntry(
  struct TableEntry *aEntries,
  int nEntries,
  Pgno iTable
){
  int i;
  for(i=0; i<nEntries; i++){
    if( aEntries[i].iTable==iTable ) return &aEntries[i];
  }
  return 0;
}

/*
** Serialize a merged catalog into a chunk and store it.
**
** Format: iNextTable(4) + nTables(4) + meta(64) + entries(25 each)
**
** Meta is copied from the "ours" catalog (the current branch's metadata
** takes precedence in a merge).
*/
static int serializeMergedCatalog(
  sqlite3 *db,
  const ProllyHash *oursCatHash,     /* For copying meta header */
  struct TableEntry *aMerged,
  int nMerged,
  Pgno iNextTable,
  ProllyHash *pOutHash               /* OUT: hash of stored catalog chunk */
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int sz = 4 + 4 + 64 + nMerged * 25;
  u8 *buf;
  u8 *p;
  int rc;

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  /* Copy iNextTable and meta from ours catalog */
  {
    u8 *oursData = 0;
    int nOursData = 0;
    rc = chunkStoreGet(cs, oursCatHash, &oursData, &nOursData);
    if( rc==SQLITE_OK && nOursData>=72 ){
      memcpy(p, oursData, 72); /* iNextTable(4) + nTables(4) + meta(64) */
    }else{
      memset(p, 0, 72);
    }
    sqlite3_free(oursData);
  }

  /* Override iNextTable */
  p[0] = (u8)iNextTable;
  p[1] = (u8)(iNextTable>>8);
  p[2] = (u8)(iNextTable>>16);
  p[3] = (u8)(iNextTable>>24);

  /* Override nTables */
  p[4] = (u8)nMerged;
  p[5] = (u8)(nMerged>>8);
  p[6] = (u8)(nMerged>>16);
  p[7] = (u8)(nMerged>>24);

  p += 72;

  /* Write table entries */
  {
    int i;
    for(i=0; i<nMerged; i++){
      Pgno pg = aMerged[i].iTable;
      p[0] = (u8)pg;
      p[1] = (u8)(pg>>8);
      p[2] = (u8)(pg>>16);
      p[3] = (u8)(pg>>24);
      p += 4;
      *p++ = aMerged[i].flags;
      memcpy(p, aMerged[i].root.data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
    }
  }

  rc = chunkStorePut(cs, buf, sz, pOutHash);
  sqlite3_free(buf);
  return rc;
}

/*
** Perform a three-way catalog merge.
**
** ancestor: the common ancestor catalog hash
** ours:     the current branch's catalog hash
** theirs:   the other branch's catalog hash
**
** On success, *pMergedHash contains the hash of the merged catalog chunk.
** Returns SQLITE_OK on success, SQLITE_ERROR on conflict.
*/
int doltliteMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  ProllyHash *pMergedHash
){
  struct TableEntry *aAnc = 0, *aOurs = 0, *aTheirs = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0;
  Pgno iNextAnc = 2, iNextOurs = 2, iNextTheirs = 2;
  struct TableEntry *aMerged = 0;
  int nMerged = 0;
  int nMergedAlloc = 0;
  Pgno iNextMerged;
  int rc;
  int i;

  /* Load all three catalogs */
  rc = doltliteLoadCatalog(db, ancestor, &aAnc, &nAnc, &iNextAnc);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, ours, &aOurs, &nOurs, &iNextOurs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, theirs, &aTheirs, &nTheirs, &iNextTheirs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  /* Allocate merged array — worst case is all tables from both sides */
  nMergedAlloc = nOurs + nTheirs;
  if( nMergedAlloc==0 ) nMergedAlloc = 1;
  aMerged = sqlite3_malloc(nMergedAlloc * (int)sizeof(struct TableEntry));
  if( !aMerged ){
    rc = SQLITE_NOMEM;
    goto merge_cleanup;
  }

  /* Use ours' iNextTable as the starting point, take max with theirs */
  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;

  /*
  ** Process tables from "ours":
  ** For each table in ours, determine if it was added, modified, or
  ** unchanged relative to ancestor, then check for conflicts with theirs.
  */
  for(i=0; i<nOurs; i++){
    Pgno iTable = aOurs[i].iTable;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;

    /* Skip sqlite_master (table 1) — it's auto-generated from the catalog
    ** and always changes when tables are added/dropped. Always take ours. */
    if( iTable<=1 ){
      aMerged[nMerged++] = aOurs[i];
      continue;
    }

    ancEntry = findTableEntry(aAnc, nAnc, iTable);
    theirsEntry = findTableEntry(aTheirs, nTheirs, iTable);

    if( !ancEntry ){
      /* Table added in ours (not in ancestor) */
      /* Include from ours (if theirs also added same table number,
      ** ours wins — table number collision across branches) */
      aMerged[nMerged++] = aOurs[i];
    }else{
      /* Table existed in ancestor */
      int oursChanged = prollyHashCompare(&aOurs[i].root, &ancEntry->root)!=0;

      if( !theirsEntry ){
        /* Deleted in theirs */
        if( oursChanged ){
          /* Modified in ours, deleted in theirs — conflict */
          rc = SQLITE_ERROR;
          goto merge_cleanup;
        }
        /* Unchanged in ours, deleted in theirs — delete (skip it) */
      }else{
        /* Present in all three */
        int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;

        if( oursChanged && theirsChanged ){
          /* Both sides modified the same table — conflict */
          rc = SQLITE_ERROR;
          goto merge_cleanup;
        }else if( theirsChanged ){
          /* Only theirs changed — take theirs */
          aMerged[nMerged++] = *theirsEntry;
        }else{
          /* Only ours changed, or neither changed — take ours */
          aMerged[nMerged++] = aOurs[i];
        }
      }
    }
  }

  /*
  ** Process tables from "theirs" that are NOT in "ours":
  ** These are either new additions from theirs or tables that were deleted
  ** in ours but still exist in theirs.
  */
  for(i=0; i<nTheirs; i++){
    Pgno iTable = aTheirs[i].iTable;
    struct TableEntry *oursEntry;

    if( iTable<=1 ) continue; /* Skip sqlite_master */

    oursEntry = findTableEntry(aOurs, nOurs, iTable);

    if( !oursEntry ){
      /* Not in ours — either added in theirs or deleted in ours */
      struct TableEntry *ancEntry = findTableEntry(aAnc, nAnc, iTable);

      if( !ancEntry ){
        /* Added in theirs only. If ours already used this table number
        ** (collision), skip theirs — ours wins. */
        if( !findTableEntry(aMerged, nMerged, iTable) ){
          aMerged[nMerged++] = aTheirs[i];
        }
        /* Table number collision: theirs' table is lost. This happens when
        ** both branches independently CREATE TABLE and get the same rootpage.
        ** Known limitation — create tables on one branch and merge. */
      }else{
        /* Was in ancestor, deleted in ours */
        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          /* Modified in theirs, deleted in ours — conflict */
          rc = SQLITE_ERROR;
          goto merge_cleanup;
        }
        /* Unchanged in theirs, deleted in ours — delete (skip it) */
      }
    }
    /* If oursEntry exists, we already handled this table in the ours loop */
  }

  /* Serialize the merged catalog */
  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              pMergedHash);

merge_cleanup:
  sqlite3_free(aAnc);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  sqlite3_free(aMerged);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
