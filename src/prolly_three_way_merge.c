
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
** When all three differ, load the three nodes and recurse. Leaf-level
** divergence returns FM_FALLBACK so the row-by-row path handles the
** conflict-prone case (commit 3 will tighten this).
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
    /* Splice safety: levels below parentLevel must be empty. If a
    ** previous sibling descended and left lower-level entries pending,
    ** we can't splice this child without re-ordering the tree. */
    if( !fmChunkerLevelsBelowEmpty(pCh, parentLevel) ){
      return FM_FALLBACK;
    }
    return prollyChunkerAddAtLevel(pCh, parentLevel,
                                    pBoundKey, nBoundKey,
                                    pSplice->data, PROLLY_HASH_SIZE);
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

  if( ancNode.level == 0
   || oursNode.level != ancNode.level
   || theirsNode.level != ancNode.level ){
    /* Leaf-level divergence or height mismatch — fall back. */
    rc = FM_FALLBACK;
    goto done;
  }

  rc = fmWalkInterior(fm, pCh, &ancNode, &oursNode, &theirsNode);

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

  /* All three roots must be interior at the same level for the walker
  ** to apply. Height differences (one side grew the tree) and leaf-only
  ** trees fall through to the row-by-row path. */
  if( ancNode.level == 0
   || oursNode.level != ancNode.level
   || theirsNode.level != ancNode.level ){
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
