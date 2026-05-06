
/* Tree-walking three-way merge.
**
** prollyThreeWayMergeFast walks (anc, ours, theirs) top-down, splicing
** unchanged subtrees via interior-node hash equality. Wins on workloads
** where ours and theirs touch disjoint key ranges of the prolly tree —
** the unaffected subtrees collapse into single chunker entries instead
** of being re-emitted row by row.
**
** *pHandled is set to 1 if the fast path produced a result and
** *pMergedRoot was written; 0 if the case isn't handled and the caller
** must fall back to row-by-row merge. The return value is for actual
** I/O / corruption errors only.
**
** The fast path NEVER produces conflicts: any case where conflict
** detection would be required (both sides modified the same leaf
** subtree) sets *pHandled=0 so the caller's row-by-row path runs and
** reports conflicts via its existing machinery.
**
** Eligibility (no schema divergence, no secondary indexes, no CHECK,
** no FK actions) must be checked by the caller before invoking — see
** fastMergeIneligibleReason() in doltlite_merge.c.
*/
#ifndef SQLITE_PROLLY_THREE_WAY_MERGE_H
#define SQLITE_PROLLY_THREE_WAY_MERGE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cache.h"
#include "chunk_store.h"

int prollyThreeWayMergeFast(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pHandled
);

#endif
