
#ifndef DOLTLITE_MERGE_INT_H
#define DOLTLITE_MERGE_INT_H

/* Private declarations shared by the doltlite merge implementation modules. */

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "prolly_three_way_diff.h"
#include "prolly_three_way_merge.h"
#include "prolly_mutmap.h"
#include "prolly_mutate.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "sortkey.h"

#include <string.h>
#include <ctype.h>

typedef struct MergeIndexInfo MergeIndexInfo;
struct MergeIndexInfo {
  Pgno iTable;
  ProllyHash oursRoot;
  ProllyHash mergedRoot;
  ProllyMutMap *pEdits;
  int nColumn;
  i16 *aiColumn;
  KeyInfo *pKeyInfo;
  int iPKey;          /* IPK column in the parent table schema, -1 if none */
};

typedef struct ParsedColumn ParsedColumn;
struct ParsedColumn {
  char *zName;
  char *zDef;
};

#define SCHEMA_MERGE_DEFAULT 0
#define SCHEMA_MERGE_OURS    1
#define SCHEMA_MERGE_THEIRS  2

int canFastMerge(
  sqlite3 *db,
  const char *zName,
  int schemaUnchangedBothSides
);

int mergeTableRows(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pnConflicts,
  DoltliteConflictRow **ppConflicts,
  MergeIndexInfo *aIndexes,
  int nIndexes
);

int parseColumns(
  const char *zSql,
  ParsedColumn **ppCols, int *pnCols
);
void freeColumns(ParsedColumn *aCols, int nCols);

int trySchemaColumnMerge(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  char ***ppAddCols, int *pnAddCols,
  int *pSchemaChoice,
  char **pzErrDetail
);

#endif /* DOLTLITE_MERGE_INT_H */
