
#ifdef DOLTLITE_PROLLY

#include "prolly_three_way_merge.h"
#include "prolly_node.h"
#include "prolly_chunker.h"

#include <string.h>

/* Internal sentinel returned by helpers when this case isn't handled
** by the fast path. Translated to *pHandled=0 at the public API. Never
** propagates out of this file. SQLITE_DONE (101) is reused because no
** other helper here returns it for any other reason. */
#define FM_FALLBACK  SQLITE_DONE

/* Local helper: compare two byte sequences as keys (memcmp + length tiebreak).
** Mirrors compareKeys() in prolly_mutate.c — duplicated rather than exposed
** to keep the merge code self-contained. */
static int fmKeyCmp(const u8 *pA, int nA, const u8 *pB, int nB){
  int n = nA < nB ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  if( nA < nB ) return -1;
  if( nA > nB ) return 1;
  return 0;
}

/* Local copy of chunkerLevelsBelowEmpty from prolly_mutate.c. Splicing
** at level L via prollyChunkerAddAtLevel is only logically correct when
** the chunker has no pending entries at any level below L — otherwise
** the spliced subtree's keys would land out of order with respect to
** entries already buffered at lower levels. */
static int fmChunkerLevelsBelowEmpty(const ProllyChunker *pCh, int level){
  int i;
  for( i = 0; i < level && i < pCh->nLevels; i++ ){
    if( pCh->aLevel[i].builder.nItems > 0 ) return 0;
  }
  return 1;
}

typedef struct FmCtx {
  ChunkStore *pStore;
  ProllyCache *pCache;
  u8 flags;
} FmCtx;

/* Forward declaration — fmEmitChild and fmWalkInterior are mutually recursive. */
static int fmEmitChild(
  FmCtx *fm, ProllyChunker *pCh,
  const u8 *pBoundKey, int nBoundKey,
  int parentLevel,
  const ProllyHash *pAnc,
  const ProllyHash *pOurs,
  const ProllyHash *pTheirs
);

/* Walk a subtree top-down and emit all its leaf rows into pCh at
** level 0. Used when a splice at the parent level is blocked because
** the chunker has pending entries below — emitting the subtree's
** rows directly costs O(rows) for that subtree but keeps the walker
** going so subsequent splices on later siblings can still apply once
** the chunker drains.
**
** Internally this is a cousin of streamingMergeNode in prolly_mutate.c
** with the "no edits" path always taken — every leaf is emitted as-is.
*/
static int fmEmitSubtreeRows(
  FmCtx *fm, ProllyChunker *pCh,
  const ProllyHash *pSubtreeRoot
){
  u8 *pData = 0;
  int nData = 0;
  ProllyNode node;
  int rc;
  int i;

  rc = chunkStoreGet(fm->pStore, pSubtreeRoot, &pData, &nData);
  if( rc != SQLITE_OK ) return rc;
  rc = prollyNodeParse(&node, pData, nData);
  if( rc != SQLITE_OK ){ sqlite3_free(pData); return rc; }

  if( node.level == 0 ){
    for( i = 0; i < (int)node.nItems; i++ ){
      const u8 *pKey, *pVal;
      int nKey, nVal;
      prollyNodeKey(&node, i, &pKey, &nKey);
      prollyNodeValue(&node, i, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
      if( rc != SQLITE_OK ){ sqlite3_free(pData); return rc; }
    }
  }else{
    for( i = 0; i < (int)node.nItems; i++ ){
      ProllyHash childHash;
      prollyNodeChildHash(&node, i, &childHash);
      rc = fmEmitSubtreeRows(fm, pCh, &childHash);
      if( rc != SQLITE_OK ){ sqlite3_free(pData); return rc; }
    }
  }
  sqlite3_free(pData);
  return SQLITE_OK;
}

/* Bounded three-way merge of three aligned leaf nodes. Walks the
** three leaves in key order, applying merge resolution per row,
** emitting the result at level 0 via prollyChunkerAdd.
**
** Returns FM_FALLBACK on any case the caller's row-by-row path needs
** to handle: cell-level conflicts (modify-modify with different
** values), modify-delete, delete-modify, add-add with different
** values. Conflict-free cases (3-way clean, identical changes,
** disjoint adds in the same chunk, disjoint deletes) emit and return
** SQLITE_OK.
*/
static int fmMergeLeaves(
  FmCtx *fm, ProllyChunker *pCh,
  const ProllyNode *pAncN,
  const ProllyNode *pOursN,
  const ProllyNode *pTheirsN
){
  int ai = 0, oi = 0, ti = 0;
  int rc = SQLITE_OK;

  while( ai < (int)pAncN->nItems
      || oi < (int)pOursN->nItems
      || ti < (int)pTheirsN->nItems ){
    const u8 *pAK = 0, *pOK = 0, *pTK = 0;
    int nAK = 0, nOK = 0, nTK = 0;
    const u8 *pAV = 0, *pOV = 0, *pTV = 0;
    int nAV = 0, nOV = 0, nTV = 0;
    i64 iAKey = 0, iOKey = 0, iTKey = 0;
    int has_a = 0, has_o = 0, has_t = 0;
    const u8 *pK = 0;
    int nK = 0;
    i64 iK = 0;

    /* Materialize each side's current key. */
    if( ai < (int)pAncN->nItems ){
      prollyNodeKey(pAncN, ai, &pAK, &nAK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iAKey = prollyNodeIntKey(pAncN, ai);
    }
    if( oi < (int)pOursN->nItems ){
      prollyNodeKey(pOursN, oi, &pOK, &nOK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iOKey = prollyNodeIntKey(pOursN, oi);
    }
    if( ti < (int)pTheirsN->nItems ){
      prollyNodeKey(pTheirsN, ti, &pTK, &nTK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iTKey = prollyNodeIntKey(pTheirsN, ti);
    }

    /* Find the smallest current key across the three sides. Compute
    ** has_a/has_o/has_t = (this side's current key equals the min). */
    {
      int hasAny[3];
      int minSide = -1;
      int s;
      const u8 *pCK; int nCK; i64 iCK;
      const u8 *pMK; int nMK; i64 iMK;
      int cmp;

      hasAny[0] = (ai < (int)pAncN->nItems);
      hasAny[1] = (oi < (int)pOursN->nItems);
      hasAny[2] = (ti < (int)pTheirsN->nItems);

      /* Pick the min by walking pairs through prollyCompareKeys. */
      for( s = 0; s < 3; s++ ){
        if( !hasAny[s] ) continue;
        if( minSide < 0 ){ minSide = s; continue; }
        if( s == 0 ){ pCK = pAK; nCK = nAK; iCK = iAKey; }
        else if( s == 1 ){ pCK = pOK; nCK = nOK; iCK = iOKey; }
        else { pCK = pTK; nCK = nTK; iCK = iTKey; }
        if( minSide == 0 ){ pMK = pAK; nMK = nAK; iMK = iAKey; }
        else if( minSide == 1 ){ pMK = pOK; nMK = nOK; iMK = iOKey; }
        else { pMK = pTK; nMK = nTK; iMK = iTKey; }
        cmp = prollyCompareKeys(fm->flags,
                                 pCK, nCK, iCK,
                                 pMK, nMK, iMK);
        if( cmp < 0 ) minSide = s;
      }
      if( minSide == 0 ){ pK = pAK; nK = nAK; iK = iAKey; }
      else if( minSide == 1 ){ pK = pOK; nK = nOK; iK = iOKey; }
      else { pK = pTK; nK = nTK; iK = iTKey; }

      /* Now mark each side that holds the min key. */
      if( hasAny[0] ){
        cmp = prollyCompareKeys(fm->flags,
                                 pAK, nAK, iAKey,
                                 pK, nK, iK);
        has_a = (cmp == 0);
      }
      if( hasAny[1] ){
        cmp = prollyCompareKeys(fm->flags,
                                     pOK, nOK, iOKey,
                                     pK, nK, iK);
        has_o = (cmp == 0);
      }
      if( hasAny[2] ){
        cmp = prollyCompareKeys(fm->flags,
                                 pTK, nTK, iTKey,
                                 pK, nK, iK);
        has_t = (cmp == 0);
      }
    }

    /* Pull values for sides that hold the min key. */
    if( has_a ) prollyNodeValue(pAncN, ai, &pAV, &nAV);
    if( has_o ) prollyNodeValue(pOursN, oi, &pOV, &nOV);
    if( has_t ) prollyNodeValue(pTheirsN, ti, &pTV, &nTV);

    /* Apply 3-way merge resolution. */
    if( has_a && has_o && has_t ){
      int same_o = (nOV == nAV) && memcmp(pOV, pAV, nOV) == 0;
      int same_t = (nTV == nAV) && memcmp(pTV, pAV, nTV) == 0;
      if( same_o && same_t ){
        rc = prollyChunkerAdd(pCh, pK, nK, pAV, nAV);
      }else if( same_o ){
        rc = prollyChunkerAdd(pCh, pK, nK, pTV, nTV);
      }else if( same_t ){
        rc = prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
      }else if( nOV == nTV && memcmp(pOV, pTV, nOV) == 0 ){
        /* Both sides made the identical change. */
        rc = prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
      }else{
        /* Modify-modify conflict — let row path handle reporting. */
        return FM_FALLBACK;
      }
    }else if( has_a && has_o && !has_t ){
      int same_o = (nOV == nAV) && memcmp(pOV, pAV, nOV) == 0;
      if( same_o ){
        /* Theirs deleted, ours unchanged: row deleted from result. */
      }else{
        return FM_FALLBACK;  /* modify-delete conflict */
      }
    }else if( has_a && !has_o && has_t ){
      int same_t = (nTV == nAV) && memcmp(pTV, pAV, nTV) == 0;
      if( same_t ){
        /* Ours deleted, theirs unchanged: row deleted. */
      }else{
        return FM_FALLBACK;  /* delete-modify conflict */
      }
    }else if( has_a && !has_o && !has_t ){
      /* Both deleted — row gone from result. */
    }else if( !has_a && has_o && has_t ){
      if( nOV == nTV && memcmp(pOV, pTV, nOV) == 0 ){
        rc = prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
      }else{
        return FM_FALLBACK;  /* add-add conflict with different values */
      }
    }else if( !has_a && has_o && !has_t ){
      rc = prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
    }else if( !has_a && !has_o && has_t ){
      rc = prollyChunkerAdd(pCh, pK, nK, pTV, nTV);
    }
    if( rc != SQLITE_OK ) return rc;

    /* Advance each side that held the min key. */
    if( has_a ) ai++;
    if( has_o ) oi++;
    if( has_t ) ti++;
  }

  return SQLITE_OK;
}

/* Walk the children of three aligned interior nodes.
**
** Eligibility: anc, ours, theirs must have the same number of children
** AND identical boundary-key sequences. If any boundary key in anc
** isn't present in ours or theirs at the same index, the trees have
** restructured (chunking changed) and we can't safely splice — fall
** back via FM_FALLBACK.
**
** This boundary-alignment requirement is what makes the MVP win on
** non-overlapping inserts in established key ranges (boundaries above
** the affected leaves are unchanged) and lose on inserts at the tree
** edge that change the rightmost boundaries.
*/
static int fmWalkInterior(
  FmCtx *fm, ProllyChunker *pCh,
  ProllyNode *pAncN, ProllyNode *pOursN, ProllyNode *pTheirsN
){
  int i;
  int parentLevel = pAncN->level;

  if( pOursN->nItems != pAncN->nItems
   || pTheirsN->nItems != pAncN->nItems ){
    return FM_FALLBACK;
  }

  for( i = 0; i < (int)pAncN->nItems; i++ ){
    const u8 *pAK; int nAK;
    const u8 *pOK; int nOK;
    const u8 *pTK; int nTK;
    ProllyHash hAnc, hOurs, hTheirs;
    int rc;

    prollyNodeKey(pAncN, i, &pAK, &nAK);
    prollyNodeKey(pOursN, i, &pOK, &nOK);
    prollyNodeKey(pTheirsN, i, &pTK, &nTK);

    if( fmKeyCmp(pAK, nAK, pOK, nOK) != 0
     || fmKeyCmp(pAK, nAK, pTK, nTK) != 0 ){
      return FM_FALLBACK;
    }

    prollyNodeChildHash(pAncN, i, &hAnc);
    prollyNodeChildHash(pOursN, i, &hOurs);
    prollyNodeChildHash(pTheirsN, i, &hTheirs);

    rc = fmEmitChild(fm, pCh, pAK, nAK, parentLevel, &hAnc, &hOurs, &hTheirs);
    if( rc != SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
}

/* Emit one (anc, ours, theirs) child triple at parentLevel into pCh.
**
** Hash short-circuits cover the splice cases:
**   - ours == theirs   : both sides agree, splice that subtree
**   - ours == anc      : only theirs changed, splice theirs
**   - theirs == anc    : only ours changed, splice ours
**
** When the splice CASE applies but the chunker has pending entries
** below parentLevel, we can't add at parentLevel without re-ordering;
** instead we descend the splice candidate's subtree and re-emit its
** rows at level 0. Costs O(rows) for that subtree but lets the walker
** keep going so later siblings with empty-below state can splice.
**
** When all three differ at an interior level, recurse into the three
** child nodes via fmWalkInterior. When all three are leaves at the
** same level, run a bounded 3-way merge of the leaf rows via
** fmMergeLeaves. Conflict-bearing leaves return FM_FALLBACK so the
** row-by-row path can report conflicts via its existing machinery.
*/
static int fmEmitChild(
  FmCtx *fm, ProllyChunker *pCh,
  const u8 *pBoundKey, int nBoundKey,
  int parentLevel,
  const ProllyHash *pAnc,
  const ProllyHash *pOurs,
  const ProllyHash *pTheirs
){
  const ProllyHash *pSplice = 0;
  u8 *pAncData = 0, *pOursData = 0, *pTheirsData = 0;
  int nAncData = 0, nOursData = 0, nTheirsData = 0;
  ProllyNode ancNode, oursNode, theirsNode;
  int rc;

  if( prollyHashCompare(pOurs, pTheirs) == 0 )      pSplice = pOurs;
  else if( prollyHashCompare(pOurs, pAnc) == 0 )    pSplice = pTheirs;
  else if( prollyHashCompare(pTheirs, pAnc) == 0 )  pSplice = pOurs;

  if( pSplice ){
    if( fmChunkerLevelsBelowEmpty(pCh, parentLevel) ){
      return prollyChunkerAddAtLevel(pCh, parentLevel,
                                      pBoundKey, nBoundKey,
                                      pSplice->data, PROLLY_HASH_SIZE);
    }
    /* Splice can't happen — descend the splice candidate and emit
    ** its rows. Boundary key is consumed by the chunker's natural
    ** chunking once enough leaf rows accumulate. */
    return fmEmitSubtreeRows(fm, pCh, pSplice);
  }

  /* All three differ. Load nodes. */
  rc = chunkStoreGet(fm->pStore, pAnc, &pAncData, &nAncData);
  if( rc != SQLITE_OK ) return rc;
  rc = prollyNodeParse(&ancNode, pAncData, nAncData);
  if( rc != SQLITE_OK ) goto done;

  rc = chunkStoreGet(fm->pStore, pOurs, &pOursData, &nOursData);
  if( rc != SQLITE_OK ) goto done;
  rc = prollyNodeParse(&oursNode, pOursData, nOursData);
  if( rc != SQLITE_OK ) goto done;

  rc = chunkStoreGet(fm->pStore, pTheirs, &pTheirsData, &nTheirsData);
  if( rc != SQLITE_OK ) goto done;
  rc = prollyNodeParse(&theirsNode, pTheirsData, nTheirsData);
  if( rc != SQLITE_OK ) goto done;

  if( oursNode.level != ancNode.level || theirsNode.level != ancNode.level ){
    /* Height mismatch — fall back. */
    rc = FM_FALLBACK;
    goto done;
  }

  if( ancNode.level == 0 ){
    /* All three are leaves at the same level. Bounded 3-way row merge. */
    rc = fmMergeLeaves(fm, pCh, &ancNode, &oursNode, &theirsNode);
  }else{
    rc = fmWalkInterior(fm, pCh, &ancNode, &oursNode, &theirsNode);
  }

done:
  sqlite3_free(pAncData);
  sqlite3_free(pOursData);
  sqlite3_free(pTheirsData);
  return rc;
}

int prollyThreeWayMergeFast(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pHandled
){
  FmCtx fm;
  ProllyChunker chunker;
  u8 *pAncData = 0, *pOursData = 0, *pTheirsData = 0;
  int nAncData = 0, nOursData = 0, nTheirsData = 0;
  ProllyNode ancNode, oursNode, theirsNode;
  int rc = SQLITE_OK;

  *pHandled = 0;

  /* Trivial top-level cases: no walker, no chunker, return the
  ** appropriate root directly. */
  if( prollyHashCompare(pOursRoot, pTheirsRoot) == 0 ){
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
    *pHandled = 1;
    return SQLITE_OK;
  }
  if( prollyHashCompare(pOursRoot, pAncRoot) == 0 ){
    memcpy(pMergedRoot, pTheirsRoot, sizeof(ProllyHash));
    *pHandled = 1;
    return SQLITE_OK;
  }
  if( prollyHashCompare(pTheirsRoot, pAncRoot) == 0 ){
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
    *pHandled = 1;
    return SQLITE_OK;
  }

  /* All three differ. The walker only handles non-empty trees that
  ** share the same height and at least one level of interior structure. */
  if( prollyHashIsEmpty(pAncRoot)
   || prollyHashIsEmpty(pOursRoot)
   || prollyHashIsEmpty(pTheirsRoot) ){
    return SQLITE_OK;  /* *pHandled stays 0 — caller falls back */
  }

  rc = chunkStoreGet(pStore, pAncRoot, &pAncData, &nAncData);
  if( rc != SQLITE_OK ) return rc;
  rc = prollyNodeParse(&ancNode, pAncData, nAncData);
  if( rc != SQLITE_OK ){ sqlite3_free(pAncData); return rc; }

  rc = chunkStoreGet(pStore, pOursRoot, &pOursData, &nOursData);
  if( rc != SQLITE_OK ){ sqlite3_free(pAncData); return rc; }
  rc = prollyNodeParse(&oursNode, pOursData, nOursData);
  if( rc != SQLITE_OK ){ sqlite3_free(pAncData); sqlite3_free(pOursData); return rc; }

  rc = chunkStoreGet(pStore, pTheirsRoot, &pTheirsData, &nTheirsData);
  if( rc != SQLITE_OK ){
    sqlite3_free(pAncData); sqlite3_free(pOursData);
    return rc;
  }
  rc = prollyNodeParse(&theirsNode, pTheirsData, nTheirsData);
  if( rc != SQLITE_OK ){
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    return rc;
  }

  /* Heights must match for the walker to apply. Tree-growth merges
  ** (one side increased the height) fall back. */
  if( oursNode.level != ancNode.level || theirsNode.level != ancNode.level ){
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    return SQLITE_OK;  /* *pHandled stays 0 */
  }

  fm.pStore = pStore;
  fm.pCache = pCache;
  fm.flags = flags;

  rc = prollyChunkerInit(&chunker, pStore, flags);
  if( rc != SQLITE_OK ){
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    return rc;
  }

  /* Root-level leaf merge: when all three roots ARE leaves (small
  ** trees), the walker's interior path doesn't apply — go straight
  ** to bounded leaf merge. */
  if( ancNode.level == 0 ){
    rc = fmMergeLeaves(&fm, &chunker, &ancNode, &oursNode, &theirsNode);
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    if( rc == SQLITE_OK ){
      rc = prollyChunkerFinish(&chunker);
      if( rc == SQLITE_OK ){
        prollyChunkerGetRoot(&chunker, pMergedRoot);
        *pHandled = 1;
      }
    }else if( rc == FM_FALLBACK ){
      rc = SQLITE_OK;
    }
    prollyChunkerFree(&chunker);
    return rc;
  }

  rc = fmWalkInterior(&fm, &chunker, &ancNode, &oursNode, &theirsNode);

  sqlite3_free(pAncData);
  sqlite3_free(pOursData);
  sqlite3_free(pTheirsData);

  if( rc == SQLITE_OK ){
    rc = prollyChunkerFinish(&chunker);
    if( rc == SQLITE_OK ){
      prollyChunkerGetRoot(&chunker, pMergedRoot);
      *pHandled = 1;
    }
  }else if( rc == FM_FALLBACK ){
    rc = SQLITE_OK;  /* *pHandled stays 0 */
  }
  prollyChunkerFree(&chunker);
  return rc;
}

#endif
