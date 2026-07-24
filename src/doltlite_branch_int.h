#ifndef DOLTLITE_BRANCH_INT_H
#define DOLTLITE_BRANCH_INT_H

/* Private declarations shared by the branch/checkout implementation modules. */

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_internal.h"

typedef struct BranchMutationCtx BranchMutationCtx;
struct BranchMutationCtx {
  const char *zName;
  ProllyHash head;
  int isDelete;
  int force;
};

int branchNameEmpty(const char *zName);
int mutateBranchRef(sqlite3 *db, ChunkStore *cs, void *pArg);

extern sqlite3_module doltliteBranchesModule;

void doltCheckoutFunc(sqlite3_context*, int, sqlite3_value**);
void doltConnectBranchFunc(sqlite3_context*, int, sqlite3_value**);

#endif /* DOLTLITE_BRANCH_INT_H */

