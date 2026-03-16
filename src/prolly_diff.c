/*
** prolly_diff.c — Two-tree diff for prolly trees.
**
** Computes the set of changes (ADD, DELETE, MODIFY) between two prolly
** trees by walking two cursors in parallel, comparing keys in sorted
** order.  When the root hashes are identical the diff is trivially empty.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"

#include <string.h>  /* memcmp */

/*
** Compare two blob keys lexicographically.
** Returns <0, 0, or >0.
*/
static int diffBlobKeyCmp(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  int n = (nA < nB) ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  return nA - nB;
}

/*
** Compare the current keys of two cursors.
** Sets *pCmp to <0, 0, or >0 following the same convention as memcmp.
**
** For INTKEY trees the integer keys are compared directly.
** For BLOBKEY trees the raw key blobs are compared lexicographically.
*/
static void diffCompareKeys(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  int *pCmp
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 iOld = prollyCursorIntKey(pOld);
    i64 iNew = prollyCursorIntKey(pNew);
    if( iOld < iNew )      *pCmp = -1;
    else if( iOld > iNew )  *pCmp =  1;
    else                     *pCmp =  0;
  }else{
    const u8 *pKeyOld; int nKeyOld;
    const u8 *pKeyNew; int nKeyNew;
    prollyCursorKey(pOld, &pKeyOld, &nKeyOld);
    prollyCursorKey(pNew, &pKeyNew, &nKeyNew);
    *pCmp = diffBlobKeyCmp(pKeyOld, nKeyOld, pKeyNew, nKeyNew);
  }
}

/*
** Fill a ProllyDiffChange from a single cursor entry.
**   type     – PROLLY_DIFF_ADD, _DELETE, or _MODIFY
**   pCur     – cursor positioned at the entry
**   flags    – INTKEY / BLOBKEY
**   pChange  – [out] change record to populate
**
** Key fields are always set from pCur.  Caller is responsible for
** setting the value pointers that correspond to the "other" side
** (pOldVal for ADD, pNewVal for DELETE, or both for MODIFY).
*/
static void diffFillKey(
  ProllyDiffChange *pChange,
  ProllyCursor *pCur,
  u8 flags
){
  if( flags & PROLLY_NODE_INTKEY ){
    pChange->intKey = prollyCursorIntKey(pCur);
    pChange->pKey   = 0;
    pChange->nKey   = 0;
  }else{
    const u8 *pKey; int nKey;
    prollyCursorKey(pCur, &pKey, &nKey);
    pChange->pKey   = pKey;
    pChange->nKey   = nKey;
    pChange->intKey = 0;
  }
}

/*
** Emit a DELETE change for the current entry of curOld.
*/
static int diffEmitDelete(
  ProllyCursor *pOld,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pVal; int nVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_DELETE;
  diffFillKey(&change, pOld, flags);
  prollyCursorValue(pOld, &pVal, &nVal);
  change.pOldVal = pVal;
  change.nOldVal = nVal;
  change.pNewVal = 0;
  change.nNewVal = 0;
  return xCallback(pCtx, &change);
}

/*
** Emit an ADD change for the current entry of curNew.
*/
static int diffEmitAdd(
  ProllyCursor *pNew,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pVal; int nVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_ADD;
  diffFillKey(&change, pNew, flags);
  prollyCursorValue(pNew, &pVal, &nVal);
  change.pOldVal = 0;
  change.nOldVal = 0;
  change.pNewVal = pVal;
  change.nNewVal = nVal;
  return xCallback(pCtx, &change);
}

/*
** Emit a MODIFY change when the key matches but the value differs.
*/
static int diffEmitModify(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_MODIFY;
  diffFillKey(&change, pNew, flags);
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  change.pOldVal = pOldVal;
  change.nOldVal = nOldVal;
  change.pNewVal = pNewVal;
  change.nNewVal = nNewVal;
  return xCallback(pCtx, &change);
}

/*
** Return non-zero if the two values at the current cursor positions
** are identical (same length and same bytes).
*/
static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  if( nOldVal != nNewVal ) return 0;
  if( nOldVal == 0 ) return 1;
  return memcmp(pOldVal, pNewVal, nOldVal) == 0;
}

/*
** Compute the diff between two prolly trees identified by their root
** hashes.  For every difference discovered, xCallback is invoked with
** a ProllyDiffChange describing the change.  If the callback returns
** anything other than SQLITE_OK the walk is abandoned and that error
** code is returned to the caller.
**
** The flags parameter carries PROLLY_NODE_INTKEY or PROLLY_NODE_BLOBKEY
** and is forwarded to the cursors so they can decode keys properly.
*/
int prollyDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyCursor *pCurOld = 0;
  ProllyCursor *pCurNew = 0;
  int rc = SQLITE_OK;
  int emptyOld = 0;
  int emptyNew = 0;

  /* Fast path: identical roots means no changes */
  if( prollyHashCompare(pOldRoot, pNewRoot) == 0 ){
    return SQLITE_OK;
  }

  /* Allocate cursors */
  pCurOld = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pCurOld ){
    return SQLITE_NOMEM;
  }
  pCurNew = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pCurNew ){
    sqlite3_free(pCurOld);
    return SQLITE_NOMEM;
  }

  /* Initialize cursors */
  prollyCursorInit(pCurOld, pStore, pCache, pOldRoot, flags);
  prollyCursorInit(pCurNew, pStore, pCache, pNewRoot, flags);

  /* Position both cursors at the first entry */
  rc = prollyCursorFirst(pCurOld, &emptyOld);
  if( rc != SQLITE_OK ) goto diff_cleanup;

  rc = prollyCursorFirst(pCurNew, &emptyNew);
  if( rc != SQLITE_OK ) goto diff_cleanup;

  /* Main merge walk */
  while( prollyCursorIsValid(pCurOld) && prollyCursorIsValid(pCurNew) ){
    int cmp;
    diffCompareKeys(pCurOld, pCurNew, flags, &cmp);

    if( cmp < 0 ){
      /* Old key < new key → entry was deleted */
      rc = diffEmitDelete(pCurOld, flags, xCallback, pCtx);
      if( rc != SQLITE_OK ) goto diff_cleanup;
      rc = prollyCursorNext(pCurOld);
      if( rc != SQLITE_OK ) goto diff_cleanup;
    }else if( cmp > 0 ){
      /* Old key > new key → entry was added */
      rc = diffEmitAdd(pCurNew, flags, xCallback, pCtx);
      if( rc != SQLITE_OK ) goto diff_cleanup;
      rc = prollyCursorNext(pCurNew);
      if( rc != SQLITE_OK ) goto diff_cleanup;
    }else{
      /* Keys equal — check values */
      if( !diffValuesEqual(pCurOld, pCurNew) ){
        rc = diffEmitModify(pCurOld, pCurNew, flags, xCallback, pCtx);
        if( rc != SQLITE_OK ) goto diff_cleanup;
      }
      rc = prollyCursorNext(pCurOld);
      if( rc != SQLITE_OK ) goto diff_cleanup;
      rc = prollyCursorNext(pCurNew);
      if( rc != SQLITE_OK ) goto diff_cleanup;
    }
  }

  /* Drain remaining entries in old cursor → DELETEs */
  while( prollyCursorIsValid(pCurOld) ){
    rc = diffEmitDelete(pCurOld, flags, xCallback, pCtx);
    if( rc != SQLITE_OK ) goto diff_cleanup;
    rc = prollyCursorNext(pCurOld);
    if( rc != SQLITE_OK ) goto diff_cleanup;
  }

  /* Drain remaining entries in new cursor → ADDs */
  while( prollyCursorIsValid(pCurNew) ){
    rc = diffEmitAdd(pCurNew, flags, xCallback, pCtx);
    if( rc != SQLITE_OK ) goto diff_cleanup;
    rc = prollyCursorNext(pCurNew);
    if( rc != SQLITE_OK ) goto diff_cleanup;
  }

diff_cleanup:
  prollyCursorClose(pCurOld);
  prollyCursorClose(pCurNew);
  sqlite3_free(pCurOld);
  sqlite3_free(pCurNew);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
