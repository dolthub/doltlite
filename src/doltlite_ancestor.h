
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

#endif
