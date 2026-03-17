/*
** Find the common ancestor (merge base) of two commits.
**
** Walks both commit chains via parent_hash to find the first shared
** ancestor. Used by dolt_merge for three-way merge.
*/
#ifndef DOLTLITE_ANCESTOR_H
#define DOLTLITE_ANCESTOR_H

#include "sqliteInt.h"
#include "prolly_hash.h"

/*
** Find the common ancestor of two commits identified by their hashes.
** Collects all ancestors of commitHash1 into a set, then walks commitHash2's
** chain until finding one in the set.
**
** On success, writes the ancestor hash to *pAncestor and returns SQLITE_OK.
** Returns SQLITE_NOTFOUND if no common ancestor exists (disjoint histories).
** Returns SQLITE_ERROR or SQLITE_NOMEM on failure.
*/
int doltliteFindAncestor(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  ProllyHash *pAncestor
);

#endif /* DOLTLITE_ANCESTOR_H */
