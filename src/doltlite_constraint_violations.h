
#ifndef DOLTLITE_CONSTRAINT_VIOLATIONS_H
#define DOLTLITE_CONSTRAINT_VIOLATIONS_H

#include "sqliteInt.h"

#define DOLTLITE_CV_FOREIGN_KEY      1
#define DOLTLITE_CV_UNIQUE_INDEX     2
#define DOLTLITE_CV_CHECK_CONSTRAINT 3
#define DOLTLITE_CV_NOT_NULL         4
#define DOLTLITE_CV_STRICT_TYPE      5

typedef struct ConstraintViolationRow ConstraintViolationRow;
struct ConstraintViolationRow {
  u8 violationType;
  i64 intKey;
  u8 *pKey;  int nKey;
  u8 *pVal;  int nVal;
  char *zInfo;
};

typedef struct ConstraintViolationTable ConstraintViolationTable;
struct ConstraintViolationTable {
  char *zName;
  int nRows;
  ConstraintViolationRow *aRows;
};

int doltliteClearConstraintViolationsForTables(
  sqlite3 *db, const char *const *azTables, int nNames
);

int doltliteAppendConstraintViolation(
  sqlite3 *db,
  const char *zTable,
  u8 violationType,
  i64 intKey,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal,
  const char *zInfoJson
);

int doltliteClearAllConstraintViolations(sqlite3 *db);

/* Batch many appends into one load + one store. Begin loads the current
** violations; while a batch is open doltliteAppendConstraintViolation
** accumulates in memory; End persists once (commit!=0) or discards.
** Not nestable. */
int doltliteConstraintViolationBatchBegin(sqlite3 *db);
int doltliteConstraintViolationBatchEnd(sqlite3 *db, int commit);

int doltliteConstraintViolationsRegister(sqlite3 *db);

/* Storage for an open batch hangs off the connection's prolly Btree. */
void *doltliteGetCvBatch(sqlite3 *db);
void doltliteSetCvBatch(sqlite3 *db, void *pBatch);

#endif
