
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

typedef DoltliteConflictTable MergeConflictTable;

#define SCHEMA_MERGE_DEFAULT 0
#define SCHEMA_MERGE_OURS    1
#define SCHEMA_MERGE_THEIRS  2

/* ── rows (doltlite_merge_rows.c) ─────────────────────────────────────── */

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

/* ── schema IR (doltlite_merge_schema.c) ──────────────────────────────── */

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

/* ── catalog helpers shared with pass1 (doltlite_merge.c) ────────────── */

void freeConflictRows(DoltliteConflictRow *aRows, int nRows);
void freeAddedColumns(char **azCols, int nCols);

int appendConflictTable(
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  const char *zName,
  int nConflicts,
  DoltliteConflictRow *aConflictRows
);

int appendSchemaConflict(
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  const char *zTable,
  const char *zObject,
  int *pAddedTable
);

int recordSchemaAddColumns(
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  const char *zName,
  char **azAddCols,
  int nAddCols
);

int schemaEntryChangedByName(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aSide, int nSide,
  const char *zName
);

int hasSchemaConflictObject(
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  const char *zObject
);

int hasSchemaConflictTable(
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  const char *zTable
);

int hasAnySchemaConflict(
  MergeConflictTable *aConflictTables,
  int nConflictTables
);

int replayDropsDisjointSchemaObject(
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema
);

SchemaEntry *findSchemaEntryByRootpage(
  SchemaEntry *aSchema,
  int nSchema,
  Pgno iRootpage
);

struct TableEntry *findCatalogEntryBySchemaObject(
  struct TableEntry *aCat,
  int nCat,
  SchemaEntry *aSchema,
  int nSchema,
  const char *zType,
  const char *zName,
  const char *zTblName
);

int normalizeTheirsToMergedLayout(
  sqlite3 *db,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  const char *zOursSql,
  const char *zTheirsSql,
  ProllyHash *pOutRoot
);

int tryResolveSchemaDivergence(
  sqlite3 *db,
  const char *zName,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  int *pSkipRowMerge,
  int *pSchemaChoice,
  char **pzErrMsg
);

/* ── pass1 (doltlite_merge_pass1.c) ───────────────────────────────────── */

int mergeAppendReindexName(char ***paz, int *pn, const char *zName);

int mergeCatalogPass1(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  MergeConflictTable **ppConflictTables, int *pnConflictTables,
  int *pTotalConflicts,
  char **pzErrMsg,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions, int *pnSchemaActions,
  int bDisjointSchemaChanges,
  int bPreferOurMaster,
  char ***pazReindex, int *pnReindex
);

#endif /* DOLTLITE_MERGE_INT_H */
