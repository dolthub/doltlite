
#ifndef DOLTLITE_CONSTRAINT_VIOLATIONS_H
#define DOLTLITE_CONSTRAINT_VIOLATIONS_H

#include "sqliteInt.h"

#define DOLTLITE_CV_FOREIGN_KEY      1
#define DOLTLITE_CV_UNIQUE_INDEX     2
#define DOLTLITE_CV_CHECK_CONSTRAINT 3

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

int doltliteConstraintViolationsRegister(sqlite3 *db);

#endif
