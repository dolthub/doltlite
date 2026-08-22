
#ifndef DOLTLITE_MERGE_INT_H
#define DOLTLITE_MERGE_INT_H

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
  int iPKey;          /* IPK column, -1 if none */
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

typedef struct IndexMergePatch IndexMergePatch;
struct IndexMergePatch {
  Pgno iTable;
  ProllyHash mergedRoot;
};

typedef struct MergePass1Ctx MergePass1Ctx;
struct MergePass1Ctx {
  sqlite3 *db;
  struct TableEntry *aAnc; int nAnc;
  struct TableEntry *aOurs; int nOurs;
  struct TableEntry *aTheirs; int nTheirs;
  SchemaEntry *aAncSchema; int nAncSchema;
  SchemaEntry *aOursSchema; int nOursSchema;
  SchemaEntry *aTheirsSchema; int nTheirsSchema;
  struct TableEntry *aMerged; int *pnMerged;
  MergeConflictTable **ppConflictTables; int *pnConflictTables;
  int *pTotalConflicts;
  char **pzErrMsg;
  const ProllyHash *pCatAnc;
  const ProllyHash *pCatOurs;
  const ProllyHash *pCatTheirs;
  SchemaMergeAction **ppSchemaActions; int *pnSchemaActions;
  int bDisjointSchemaChanges;
  int bPreferOurMaster;
  /* Branch merge, not revert/cherry-pick/rebase. Dolt merge refusals
  ** apply only here. */
  int bBranchMerge;
  char ***pazReindex; int *pnReindex;
  IndexMergePatch *aPatches;
  int nPatches;
  int nPatchesAlloc;
};

int mergePass1CheckIndexOverRenamedColumn(MergePass1Ctx *c);
int mergePass1CheckTriggerOverRenamedTable(MergePass1Ctx *c);
int mergePass1CheckRowEditOfDroppedColumn(MergePass1Ctx *c);
int mergePass1CheckDuplicateIndexColumns(MergePass1Ctx *c);
int mergePass1CheckDependentOverDualRename(MergePass1Ctx *c);
int mergePreNormalizeRenamedDependents(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaMergeAction **ppActions, int *pnActions
);

int mergeRowEditsColumn(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOtherRoot,
  u8 ancFlags,
  u8 otherFlags,
  int iField,
  int bNonNullOnly,
  int *pbEdited
);

int canFastMerge(
  sqlite3 *db,
  const char *zName,
  int schemaUnchangedBothSides
);

/* azRenameOverDrop: ancestor names one side renamed and the other
** dropped. Resolve to the survivor, matching Dolt.
** azDualRename: both sides renamed differently. Not a user conflict;
** pass 2 keeps both. Resolve in row merge so the root stays canonical. */
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
  char ***ppRenameCols, int *pnRenameCols,
  int *pSchemaChoice,
  int *pResolvedDivergence,
  char **pzErrDetail
);

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
  int nDropCols,
  char **azRenameCols,
  int nRenameCols
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

int mergeIndexColumnsOverlap(const char *zSqlA, const char *zSqlB);

int mergePreDetectDualIndexOverlap(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts
);

int mergeIndexColumnRenamedAway(
  const char *zIndexSql,
  const char *zAncTableSql,
  const char *zSideTableSql,
  char **pzColumn
);

int mergeIndexColumnGoneFrom(
  const char *zIndexSql,
  const char *zAncTableSql,
  const char *zSideTableSql,
  char **pzColumn
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

int mergeAppendReindexName(char ***paz, int *pn, const char *zName);
int mergeFilterDerivedShadowConflicts(sqlite3 *db,
    MergeConflictTable *aConflictTables, int *pnConflictTables,
    int *pTotalConflicts, char ***pazRebuild, int *pnRebuild,
    char **pzRefuse);

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
  int bBranchMerge,
  char ***pazReindex, int *pnReindex
);

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
