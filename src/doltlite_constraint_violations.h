
#ifndef DOLTLITE_CONSTRAINT_VIOLATIONS_H
#define DOLTLITE_CONSTRAINT_VIOLATIONS_H

#include "sqliteInt.h"

/* Violation type tags. Stored as a single byte in the blob
** serialization so the set is append-only across versions. String
** names exposed to SQL match Dolt's (lowercase) for oracle
** compatibility. */
#define DOLTLITE_CV_FOREIGN_KEY      1
#define DOLTLITE_CV_UNIQUE_INDEX     2
#define DOLTLITE_CV_CHECK_CONSTRAINT 3

/* In-memory row. pKey/nKey is the original sqlite KEY payload
** (empty for rowid-alias PKs, where intKey is authoritative).
** pVal/nVal is the full record payload so the vtable column
** reader can project user-declared columns via
** doltliteResultUserCol. zInfo is a JSON string describing the
** specific constraint (FK name, referenced table, columns,
** expression, etc.) and gets exposed verbatim as the
** violation_info column. */
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

/* Append a single violation to the persistent blob for zTable.
** Loads the current blob, inserts the row, rewrites the blob,
** and re-persists the working set. Callers (merge walk / post-
** merge scan) pass freshly-allocated byte buffers; this function
** duplicates them. */
int doltliteAppendConstraintViolation(
  sqlite3 *db,
  const char *zTable,
  u8 violationType,
  i64 intKey,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal,
  const char *zInfoJson
);

/* Wipe all violations. Used on merge abort / dolt_reset. */
int doltliteClearAllConstraintViolations(sqlite3 *db);

/* Register the summary + per-table vtable modules. Called from
** doltliteRegister in doltlite.c. */
int doltliteConstraintViolationsRegister(sqlite3 *db);

#endif
