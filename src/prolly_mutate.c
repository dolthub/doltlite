/*
** Merge-flush: apply buffered edits from MutMap into an existing prolly tree.
** This is the core mutation algorithm — it merges sorted edits with the
** existing tree's leaf entries, then rebuilds the tree using the chunker.
**
** The algorithm performs a single sorted merge-walk over two streams:
**   1. The existing tree's leaf entries (via cursor)
**   2. The pending edits (via MutMap iterator)
**
** The merged output is fed to the chunker, which produces a new set of
** content-addressed nodes forming the updated tree. Unchanged subtrees
** are structurally shared with the old tree.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include <string.h>

/*
** Compare two keys according to the node flags.
**
** For INTKEY tables: compare the 64-bit integer keys.
** For BLOBKEY tables: memcmp on the shorter length, then compare lengths.
**
** Returns negative if key1 < key2, zero if equal, positive if key1 > key2.
*/
static int compareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
){
  if( flags & PROLLY_NODE_INTKEY ){
    if( iKey1 < iKey2 ) return -1;
    if( iKey1 > iKey2 ) return +1;
    return 0;
  }else{
    /* BLOBKEY comparison: memcmp then length */
    int n = nKey1 < nKey2 ? nKey1 : nKey2;
    int c = memcmp(pKey1, pKey2, n);
    if( c!=0 ) return c;
    if( nKey1 < nKey2 ) return -1;
    if( nKey1 > nKey2 ) return +1;
    return 0;
  }
}

/*
** Feed a single key-value pair into the chunker.
**
** For INTKEY tables the key is serialized as an 8-byte little-endian i64
** so that the chunker (which works on raw bytes) receives a sortable key
** representation.  For BLOBKEY tables the key bytes are passed through
** directly.
*/
static int feedChunker(
  ProllyChunker *pCh,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  if( flags & PROLLY_NODE_INTKEY ){
    /* Encode integer key as 8-byte little-endian for the chunker. */
    u8 aKeyBuf[8];
    aKeyBuf[0] = (u8)(intKey);
    aKeyBuf[1] = (u8)(intKey >> 8);
    aKeyBuf[2] = (u8)(intKey >> 16);
    aKeyBuf[3] = (u8)(intKey >> 24);
    aKeyBuf[4] = (u8)(intKey >> 32);
    aKeyBuf[5] = (u8)(intKey >> 40);
    aKeyBuf[6] = (u8)(intKey >> 48);
    aKeyBuf[7] = (u8)(intKey >> 56);
    return prollyChunkerAdd(pCh, aKeyBuf, 8, pVal, nVal);
  }else{
    return prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
  }
}

/*
** Build a new tree from scratch using only the edits in the MutMap.
** This is used when the old tree is empty (oldRoot is all-zeros).
**
** We simply iterate over all edits in sorted order. INSERT edits are
** fed to the chunker; DELETE edits are silently ignored (there is
** nothing to delete in an empty tree).
*/
static int buildFromEdits(
  ProllyMutator *pMut
){
  ProllyChunker chunker;
  ProllyMutMapIter iter;
  int rc;

  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ) return rc;

  prollyMutMapIterFirst(&iter, pMut->pEdits);
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      rc = feedChunker(&chunker, pMut->flags,
                       pEntry->pKey, pEntry->nKey, pEntry->intKey,
                       pEntry->pVal, pEntry->nVal);
      if( rc!=SQLITE_OK ){
        prollyChunkerFree(&chunker);
        return rc;
      }
    }
    /* DELETE edits on an empty tree: nothing to do, skip */
    prollyMutMapIterNext(&iter);
  }

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

  prollyChunkerFree(&chunker);
  return rc;
}

/*
** Merge-walk the existing tree with the edit map and produce a new tree.
**
** Algorithm outline:
**   1. Open a cursor positioned at the first entry of the existing tree.
**   2. Open an iterator positioned at the first edit in the MutMap.
**   3. Initialize a chunker to accumulate the output stream.
**   4. While either stream has remaining entries:
**      a. Compare the current cursor key with the current edit key.
**      b. Depending on the comparison result and the edit operation,
**         either pass through the old entry, replace it, insert a new
**         entry, delete an entry, or skip.
**   5. Finalize the chunker to obtain the new root hash.
*/
static int mergeWalk(
  ProllyMutator *pMut
){
  ProllyCursor cur;
  ProllyMutMapIter iter;
  ProllyChunker chunker;
  int rc;
  int curEmpty = 0;    /* set to 1 if cursor has no entries */
  int curValid;        /* is cursor pointing at a valid entry? */
  int iterValid;       /* is edit iterator pointing at a valid entry? */

  /* Initialize cursor on the existing tree */
  prollyCursorInit(&cur, pMut->pStore, pMut->pCache,
                   &pMut->oldRoot, pMut->flags);
  rc = prollyCursorFirst(&cur, &curEmpty);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }

  /* If the existing tree turned out to be empty, fall back to
  ** building from edits alone. */
  if( curEmpty ){
    prollyCursorClose(&cur);
    return buildFromEdits(pMut);
  }

  /* Initialize the edit iterator */
  prollyMutMapIterFirst(&iter, pMut->pEdits);

  /* Initialize the chunker */
  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }

  /*
  ** Main merge loop.
  **
  ** On each iteration we check whether the cursor and/or iterator are
  ** still valid, fetch the current keys, compare, and decide which
  ** entry (if any) to emit to the chunker.
  */
  for(;;){
    curValid = prollyCursorIsValid(&cur);
    iterValid = prollyMutMapIterValid(&iter);

    if( !curValid && !iterValid ){
      /* Both streams exhausted — done */
      break;
    }

    if( curValid && !iterValid ){
      /*
      ** Only the cursor has remaining entries. Copy current entry
      ** through to the chunker unchanged and advance the cursor.
      */
      const u8 *pKey; int nKey;
      const u8 *pVal; int nVal;
      i64 intKey = 0;

      if( pMut->flags & PROLLY_NODE_INTKEY ){
        intKey = prollyCursorIntKey(&cur);
        pKey = 0;
        nKey = 0;
      }else{
        prollyCursorKey(&cur, &pKey, &nKey);
      }
      prollyCursorValue(&cur, &pVal, &nVal);

      rc = feedChunker(&chunker, pMut->flags, pKey, nKey, intKey, pVal, nVal);
      if( rc!=SQLITE_OK ) goto merge_cleanup;

      rc = prollyCursorNext(&cur);
      if( rc!=SQLITE_OK ) goto merge_cleanup;
      continue;
    }

    if( !curValid && iterValid ){
      /*
      ** Only the iterator has remaining edits.
      ** INSERT: feed to chunker.  DELETE: skip (key absent).
      */
      ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);

      if( pEntry->op==PROLLY_EDIT_INSERT ){
        rc = feedChunker(&chunker, pMut->flags,
                         pEntry->pKey, pEntry->nKey, pEntry->intKey,
                         pEntry->pVal, pEntry->nVal);
        if( rc!=SQLITE_OK ) goto merge_cleanup;
      }
      /* DELETE with no matching old entry: nothing to do */

      prollyMutMapIterNext(&iter);
      continue;
    }

    /*
    ** Both cursor and iterator are valid. Compare keys.
    */
    {
      ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
      const u8 *pCurKey; int nCurKey;
      i64 iCurKey = 0;
      int cmp;

      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iCurKey = prollyCursorIntKey(&cur);
        pCurKey = 0;
        nCurKey = 0;
      }else{
        prollyCursorKey(&cur, &pCurKey, &nCurKey);
      }

      cmp = compareKeys(pMut->flags,
                         pCurKey, nCurKey, iCurKey,
                         pEntry->pKey, pEntry->nKey, pEntry->intKey);

      if( cmp < 0 ){
        /*
        ** Cursor key < edit key. The old entry is unchanged.
        ** Pass it through and advance the cursor.
        */
        const u8 *pVal; int nVal;
        prollyCursorValue(&cur, &pVal, &nVal);

        rc = feedChunker(&chunker, pMut->flags,
                         pCurKey, nCurKey, iCurKey, pVal, nVal);
        if( rc!=SQLITE_OK ) goto merge_cleanup;

        rc = prollyCursorNext(&cur);
        if( rc!=SQLITE_OK ) goto merge_cleanup;

      }else if( cmp == 0 ){
        /*
        ** Keys are equal. The edit replaces or deletes the old entry.
        */
        if( pEntry->op==PROLLY_EDIT_INSERT ){
          /* Replace: emit edit's value with the key */
          rc = feedChunker(&chunker, pMut->flags,
                           pEntry->pKey, pEntry->nKey, pEntry->intKey,
                           pEntry->pVal, pEntry->nVal);
          if( rc!=SQLITE_OK ) goto merge_cleanup;
        }
        /* DELETE: skip both — entry removed from new tree */

        /* Advance both streams past this key */
        rc = prollyCursorNext(&cur);
        if( rc!=SQLITE_OK ) goto merge_cleanup;
        prollyMutMapIterNext(&iter);

      }else{
        /*
        ** Cursor key > edit key. The edit references a key that does
        ** not exist in the old tree at this position.
        */
        if( pEntry->op==PROLLY_EDIT_INSERT ){
          /* New entry: emit it */
          rc = feedChunker(&chunker, pMut->flags,
                           pEntry->pKey, pEntry->nKey, pEntry->intKey,
                           pEntry->pVal, pEntry->nVal);
          if( rc!=SQLITE_OK ) goto merge_cleanup;
        }
        /* DELETE of non-existent key: silently skip */

        prollyMutMapIterNext(&iter);
      }
    }
  }

  /* Finalize the chunker to build the tree and obtain the root. */
  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

merge_cleanup:
  prollyChunkerFree(&chunker);
  prollyCursorClose(&cur);
  return rc;
}

/*
** Apply all edits in pMut->pEdits to the tree rooted at pMut->oldRoot.
** Produces a new tree whose root hash is stored in pMut->newRoot.
**
** Three cases:
**   1. No edits: newRoot = oldRoot (no work).
**   2. Old tree is empty: build from edits alone.
**   3. Otherwise: merge-walk old tree + edits.
**
** Returns SQLITE_OK on success with result in pMut->newRoot.
*/
int prollyMutateFlush(ProllyMutator *pMut){
  /* Case 1: no edits — the tree is unchanged */
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  /* Case 2: old tree is empty — build entirely from edits */
  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    return buildFromEdits(pMut);
  }

  /* Case 3: merge old tree with edits */
  return mergeWalk(pMut);
}

/*
** Convenience function: apply a single INSERT to a tree.
**
** Creates a temporary 1-entry MutMap, wraps it in a ProllyMutator,
** calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateInsert(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the INSERT */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

/*
** Convenience function: apply a single DELETE to a tree.
**
** Creates a temporary 1-entry MutMap with a DELETE edit, wraps it in a
** ProllyMutator, calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateDelete(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the DELETE */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
