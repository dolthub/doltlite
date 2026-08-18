
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
  Index *pIdx;
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

typedef struct SchemaRootpageRemap SchemaRootpageRemap;
struct SchemaRootpageRemap {
  Pgno oldPg;
  Pgno newPg;
};

/* ── rows (doltlite_merge_rows.c) ─────────────────────────────────────── */

int canFastMerge(
  sqlite3 *db,
  const char *zName,
  int schemaUnchangedBothSides
);

/* Row-merge policy. Scoped by object identity, computed where both full
** catalogs are visible, so enabling it cannot change any conflict but the ones
** it names.
**
** azRenameOverDrop holds ancestor object names one side renamed and the other
** dropped. A catalog row whose base name or tbl_name is listed resolves to the
** surviving side instead of conflicting, matching Dolt, which keeps such a
** table as an add.
**
** azDualRename holds ancestor names both sides renamed to different names.
** A modify/modify on those catalog rows is not a user conflict: pass 2 keeps
** both tables. The row merge leaves ours in place; serialize rebuilds master.
** Resolving inside the row merge keeps the merged root canonical, which the
** history-independence gate requires.
*/
typedef struct MergeRowPolicy MergeRowPolicy;
struct MergeRowPolicy {
  const char **azRenameOverDrop;
  int nRenameOverDrop;
  const char **azDualRename;
  int nDualRename;
};

int mergeTableRows(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  u8 ancFlags,
  u8 theirsFlags,
  ProllyHash *pMergedRoot,
  int *pnConflicts,
  DoltliteConflictRow **ppConflicts,
  MergeIndexInfo *aIndexes,
  int nIndexes,
  const MergeRowPolicy *pPolicy
);

/* ── schema IR (doltlite_merge_schema.c) ──────────────────────────────── */

int parseColumns(
  const char *zSql,
  ParsedColumn **ppCols, int *pnCols
);
void freeColumns(ParsedColumn *aCols, int nCols);
int parsedColumnIndexByName(
  ParsedColumn *aCols,
  int nCols,
  const char *zName
);
int parsedColumnDefinitionsMatch(
  const ParsedColumn *pA,
  const ParsedColumn *pB
);

int trySchemaColumnMerge(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  char ***ppAddCols, int *pnAddCols,
  char ***ppDropCols, int *pnDropCols,
  int *pSchemaChoice,
  int *pResolvedDivergence,
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

int recordSchemaColumnChanges(
  SchemaMergeAction **ppSchemaActions,
  int *pnSchemaActions,
  const char *zName,
  char **azAddCols,
  int nAddCols,
  char **azDropCols,
  int nDropCols
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

int mergeIndexFollowsDualRename(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const SchemaEntry *pAnc,
  const SchemaEntry *pOurs,
  const SchemaEntry *pTheirs
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

int mergeTableRenameOtherDrop(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aDropped, int nDropped,
  struct TableEntry *aRenamed, int nRenamed,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aRenamedSchema, int nRenamedSchema,
  struct TableEntry *pRenamed,
  const char **pzAncName
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

typedef struct MergeColDefaults MergeColDefaults;
struct MergeColDefaults {
  DoltliteSerialValue *aVal;
  u8 **apOwned;
  int nCol;
};
void mergeColDefaultsFree(MergeColDefaults *p);
int mergeColDefaultsLoad(const char *zSql, const char *zTable,
                         MergeColDefaults *pOut);

int normalizeSideToMergedLayout(
  sqlite3 *db,
  const char *zTable,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  const char *zAncSql,
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
int mergeFilterDerivedShadowConflicts(sqlite3 *db,
    MergeConflictTable *aConflictTables, int *pnConflictTables,
    int *pTotalConflicts, char ***pazRebuild, int *pnRebuild);

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

/* ── pass2 (doltlite_merge_pass2.c) ───────────────────────────────────── */

int mergeCatalogPass2(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  Pgno *piNextMerged,
  int bDisjointSchemaChanges,
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  SchemaRootpageRemap **ppaRemap,
  int *pnRemap,
  char ***pazReindex,
  int *pnReindex
);

#endif /* DOLTLITE_MERGE_INT_H */
