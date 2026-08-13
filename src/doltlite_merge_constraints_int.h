#ifndef DOLTLITE_MERGE_CONSTRAINTS_INT_H
#define DOLTLITE_MERGE_CONSTRAINTS_INT_H

/* Private declarations shared by merge constraint-violation detectors. */

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include "doltlite_record.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

typedef struct MergePkInfo MergePkInfo;
struct MergePkInfo {
  int nPk;
  char **azPk;
  char *zPkCols;
};

void freeMergePkInfo(MergePkInfo *pPk);
int loadMergePkInfo(sqlite3 *db, const char *zTable, MergePkInfo *pPk);

int copyCursorRow(
  ProllyCursor *pCur,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
);
int fetchRowByRowid(
  ChunkStore *cs, ProllyCache *pCache, const ProllyHash *pRoot, u8 flags,
  i64 targetRowid, u8 **ppKey, int *pnKey, u8 **ppVal, int *pnVal
);
int fetchRowByBlobKey(
  ChunkStore *cs, ProllyCache *pCache, const ProllyHash *pRoot, u8 flags,
  const u8 *pKey, int nKey, u8 **ppKey, int *pnKey, u8 **ppVal, int *pnVal
);
int tableEntryDiffers(const struct TableEntry *a, const struct TableEntry *b);
int catalogTableChanged(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  const char *zTable
);
int cvTableAllowed(const char *zTable, const char **azTables, int nTables);
int loadAncestorAndCurrentCatalogs(
  sqlite3 *db, const ProllyHash *pAncCatHash,
  struct TableEntry **paAnc, int *pnAnc,
  struct TableEntry **paCur, int *pnCur
);
u8 *buildRecordFromStmtCols(
  sqlite3_stmt *pStmt, int iStart, int nField, int *pnOut
);
int recordPrefixEquals(
  const u8 *pLeft, int nLeft, const u8 *pRight, int nRight, int nField
);
int fetchRowByPkRecord(
  ChunkStore *cs, ProllyCache *pCache, const ProllyHash *pRoot, u8 flags,
  sqlite3 *db, const char *zTable,
  const u8 *pPkRec, int nPkRec, int nPkField,
  u8 **ppKey, int *pnKey, u8 **ppVal, int *pnVal
);
int fetchAncestorRowByName(
  sqlite3 *db, struct TableEntry *aAnc, int nAnc, const char *zTable,
  i64 rowid, u8 **ppAncVal, int *pnAncVal
);
int fetchAncestorRowByKey(
  sqlite3 *db, struct TableEntry *aAnc, int nAnc, const char *zTable,
  const u8 *pKey, int nKey, u8 **ppAncVal, int *pnAncVal
);
int isRowPreExisting(
  const u8 *pMergedVal, int nMergedVal,
  const u8 *pAncVal, int nAncVal
);
int tableHasRowid(sqlite3 *db, const char *zTable);
int fetchOrphanRow(
  sqlite3 *db, const char *zTable, i64 rowid,
  u8 **ppKey, int *pnKey, u8 **ppVal, int *pnVal
);
int fetchRowByPkFromTable(
  sqlite3 *db, const char *zTable,
  const u8 *pPkRec, int nPkRec, int nPkField,
  u8 **ppKey, int *pnKey, u8 **ppVal, int *pnVal
);

#endif /* DOLTLITE_MERGE_CONSTRAINTS_INT_H */
