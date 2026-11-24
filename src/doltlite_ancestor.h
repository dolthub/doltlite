
#ifndef DOLTLITE_ANCESTOR_H
#define DOLTLITE_ANCESTOR_H

#include "sqliteInt.h"
#include "prolly_hash.h"

int doltliteFindAncestor(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  ProllyHash *pAncestor
);

int doltliteFindAncestors(
  sqlite3 *db,
  const ProllyHash *pLeft,
  const ProllyHash *pRight,
  ProllyHash **paAncestors,
  int *pnAncestors
);

int doltliteCommitIsReachable(
  sqlite3 *db,
  const ProllyHash *pHead,
  const ProllyHash *pTarget,
  int *pReachable
);

#endif
