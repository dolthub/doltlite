
#ifndef DOLTLITE_COMMIT_H
#define DOLTLITE_COMMIT_H

#include "sqliteInt.h"
#include "prolly_hash.h"

#define DOLTLITE_COMMIT_V2 2
#define DOLTLITE_COMMIT_VERSION DOLTLITE_COMMIT_V2

#define DOLTLITE_MAX_PARENTS 8

/* parentHash is a legacy V1 single-parent field still populated by
** deserialize so older call sites keep working. nParents/aParents is
** the canonical multi-parent list (merge commits have two). Accessors
** below prefer aParents and fall back to parentHash when aParents is
** empty — never access either field directly. */
typedef struct DoltliteCommit DoltliteCommit;
struct DoltliteCommit {
  ProllyHash parentHash;
  ProllyHash catalogHash;
  i64 timestamp;
  char *zName;
  char *zEmail;
  char *zMessage;

  ProllyHash aParents[DOLTLITE_MAX_PARENTS];
  int nParents;
};

int doltliteCommitSerialize(const DoltliteCommit *c, u8 **ppOut, int *pnOut);

int doltliteCommitDeserialize(const u8 *data, int nData, DoltliteCommit *c);

void doltliteCommitClear(DoltliteCommit *c);

static SQLITE_INLINE int doltliteCommitParentCount(const DoltliteCommit *pCommit){
  if( pCommit->nParents>0 ) return pCommit->nParents;
  return prollyHashIsEmpty(&pCommit->parentHash) ? 0 : 1;
}

static SQLITE_INLINE const ProllyHash *doltliteCommitParentHash(
  const DoltliteCommit *pCommit,
  int iParent
){
  if( iParent<0 ) return 0;
  if( pCommit->nParents>0 ){
    return iParent<pCommit->nParents ? &pCommit->aParents[iParent] : 0;
  }
  if( iParent==0 && !prollyHashIsEmpty(&pCommit->parentHash) ){
    return &pCommit->parentHash;
  }
  return 0;
}

void doltliteHashToHex(const ProllyHash *h, char *buf);

int doltliteHexToHash(const char *hex, ProllyHash *h);

#endif
