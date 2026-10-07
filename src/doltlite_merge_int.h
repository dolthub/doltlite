
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

/* Everything about a partial index that does not change per row. Callers that
** walk many rows build one and hand it to the index delta. */
typedef struct DoltlitePartialIndex DoltlitePartialIndex;
struct DoltlitePartialIndex {
  char *zWhere;
  DoltliteColInfo cols;
  sqlite3_stmt *pStmt;
  int colsInit;
};

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
  DoltlitePartialIndex part;
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

int mergePass1RunChecks(MergePass1Ctx *c);
int mergeNoteRedefinedIndex(MergePass1Ctx *c, const char *zIndex);
int rebuildDisjointSchemaRows(
  sqlite3 *db,
  struct TableEntry *aMerged, int nMerged,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap,
  const ProllyHash *pOurCatalog, const ProllyHash *pTheirCatalog,
  SchemaMergeAction *aActions, int nActions,
  char ***pazReindex, int *pnReindex
);
int mergePreNormalizeRenamedDependents(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaMergeAction **ppActions, int *pnActions,
  int bTableRenames
);

/* One side column: its stored field (-1 for VIRTUAL), the ancestor slot
** holding its name (-1 if none), and whether ADD COLUMN would fill it
** with NULL. */
typedef struct MergeLayoutCol MergeLayoutCol;
struct MergeLayoutCol {
  int iField;
  int iSrcSlot;
  u8 bAddsAsNull;
};

typedef struct MergeLayout MergeLayout;
struct MergeLayout {
  const MergeLayoutCol *aCol;
  int nCol;
  const int *aAncField;
  int nAnc;
};

/* Stored record slot of a parsed column, or -1 for VIRTUAL. The INTEGER
** PRIMARY KEY still occupies its NULL placeholder slot. */
int mergeStoredFieldIndex(ParsedColumn *aCols, int iCol);

/* Side dropped a column and renamed another onto that name. *pzReused is
** a malloc'd copy of the reused name, or NULL when the kept rows still
** match the names. aSideAnc, when non-NULL, receives the ancestor column
** each side column still holds. */
int mergeRenameHoldingDroppedName(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pSideRoot,
  u8 ancFlags,
  u8 sideFlags,
  const char *zAncSql,
  const char *zSideSql,
  const char *zTable,
  int *aSideAnc,
  int nSide,
  char **pzReused
);

void mergeMapColumnsToAncestor(
  ParsedColumn *aAnc, int nAnc,
  ParsedColumn *aSide, int nSide,
  int *aSideAnc
);

int mergeStoredFieldsEqual(
  const u8 *pA, int nA, const DoltliteRecordInfo *pAi, int iA,
  const u8 *pB, int nB, const DoltliteRecordInfo *pBi, int iB
);
int mergeLoadReaderColumns(const char *zSql, const char *zTable, DoltliteColInfo *ci);
int mergeReaderRecordSlot(const DoltliteColInfo *ci, int i);

int mergeMapUnmatchedColumns(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pSideRoot,
  u8 ancFlags, u8 sideFlags,
  const char *zAncSql, const char *zSideSql, const char *zTable,
  int *aSideAnc, int nSide, char **pzErrMsg
);

int mergePass1CheckRenameReusingColumnName(MergePass1Ctx *c);

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

int mergeRowTable(
  MergePass1Ctx *c, const char *zName, int schemaChanged, int useTheirs,
  sqlite3 **ppSchemaDb, Table **ppTab
);
int mergeRebindIndexes(
  MergePass1Ctx *c, MergeIndexInfo *aIdxInfo, int nIdxInfo,
  sqlite3 *pSchemaDb, Table *pTab
);
int mergeGeneratedRecord(
  sqlite3 *db, Table *pTab, sqlite3_stmt **ppStmt, i64 intKey,
  u8 **ppRecord, int *pnRecord
);
int mergeGeneratedSideRow(
  sqlite3 *db, Table *pTab, sqlite3_stmt **ppStmt, i64 intKey,
  const u8 **ppVal, int *pnVal, u8 **ppOwned
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
  int nDeleteCompareFields;
  int *aiDeleteCompareFields;
  int bSchemaIsTheirs;
  int nDropFields;
  int *aiDropFields;
  /* Storage indexes of columns added on both sides. The ancestor has no
  ** value there, so each side's value is a change. */
  int nDualAddFields;
  int *aiDualAddFields;
};

int mergeRowAddedDefaults(
  MergePass1Ctx *c, const char *zName, sqlite3 *pSchemaDb,
  const struct TableEntry *pOurs, const struct TableEntry *pAnc,
  const ProllyHash *pTheirsRoot, int useTheirs,
  ProllyHash *pOurOut, ProllyHash *pAncOut, ProllyHash *pTheirOut
);
int mergeRowPolicy(
  MergePass1Ctx *c, const char *zName, Table *pTab,
  int useTheirs, MergeRowPolicy *pPolicy
);

int mergeTableRows(
  sqlite3 *db,
  Table *pTab,
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
int parsedColumnIsVirtual(const ParsedColumn *pCol);
int parsedColumnAddsAsNull(const ParsedColumn *pCol);
int columnRenamedAt(
  ParsedColumn *aSide, int nSide,
  ParsedColumn *aAnc, int nAnc,
  int iAnc,
  ParsedColumn *aOther, int nOther
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

int schemaDefinitionsEquivalent(const char *zLeft, const char *zRight);
int schemaNonCheckTextMatches(const char *zA, const char *zB, int *pbMatch);
int schemaApplyDisjointCheckUnions(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs
);
int schemaTableChecksUnified(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const char *zName,
  int *pbUnion
);
int schemaRetainedClauseConflict(
  const char *zAnc, const char *zOurs, const char *zTheirs,
  int schemaChoice, char **pzErr
);
int schemaAdoptMergedTableSql(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const char *zName, const char *zFallback, int iTable,
  int schemaChoice, char **pzOursPrev,
  SchemaMergeAction *aAct, int nAct
);
int schemaInstallDeferredClauses(
  sqlite3 *db, const char *zTable, char **azClauses, int nClauses
);

/* Table-level FOREIGN KEY text collected while composing a merge. */
typedef struct DlFk DlFk;
struct DlFk {
  char *zName;
  char *zRaw;
  char *zCols;
};

int dlColsContain(const char *zCols, const char *zName);
int dlCoresMatch(const char *zA, const char *zB, int bStripUnique);
int schemaUniqueSideChoice(
  const char *zAnc, const char *zOurs, const char *zTheirs, int *pChoice
);
int dlNeutralSame(const char *zA, const char *zB, int *pb);
int dlMergedNames(
  ParsedColumn *aAnc, int nAnc,
  ParsedColumn *aWin, int nWin,
  ParsedColumn *aOth, int nOth,
  char ***paz, int *pn
);
int dlUnionCols(
  ParsedColumn *a, int na, ParsedColumn *b, int nb, ParsedColumn *c, int nc,
  ParsedColumn **pp, int *pn
);
int dlRefClass(
  const char *zCols, char **azMerged, int nMerged,
  ParsedColumn *aWin, int nWin
);
int dlCutRaw(char **pzSql, const char *zRaw);
const char *dlFindClause(const char *zSql, const char *zRaw);
int dlRewriteColumns(
  const char *zSql, char **azName, char **azDef, int nRep,
  char **pzOut, int *pChanged
);
int dlDeferRaw(
  SchemaMergeAction *a, int n, const char *zTable, const char *zRaw
);
void dlFksFree(DlFk *a, int n);
int dlCollectFks(const char *zSql, DlFk **pp, int *pn);
int dlFkSame(const DlFk *a, const DlFk *b);
int dlFkCorresponds(const DlFk *a, const DlFk *b);
void dlFreeNames(char **az, int n);
int dlPushRaw(char ***paz, int *pn, int *pAlloc, const char *z);
void dlFreeRaws(char **az, int n);
int mergePromoteMasterSchemaConflicts(
  struct TableEntry *aAnc, int nAnc,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts
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

int mergePreDetectDualIndexOverlap(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts
);

int mergeIndexNamesColumn(const char *zSql, const char *zName, int *pbFound);
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
int doltliteColumnIsVirtual(const Table *pTab, int iCol);
int doltlitePrepareIndexExpr(sqlite3 *db, Table *pTab, Expr *pExpr,
                              int iGenerated, sqlite3_stmt **ppStmt);
int doltlitePartialIndexWhereSql(sqlite3 *db, Index *pIdx, char **pzWhere);
int doltlitePartialIndexMatchesRecord(sqlite3 *db, Index *pIdx,
                                      const char *zWhere,
                                      const u8 *pRec, int nRec,
                                      const DoltliteColInfo *pCols,
                                      sqlite3_stmt **ppCached,
                                      int *pMatch);

int doltlitePartialIndexLoad(sqlite3 *db, Index *pIdx,
                             DoltlitePartialIndex *pOut);
void doltlitePartialIndexClear(DoltlitePartialIndex *p);

int doltliteIndexMutMapRowDelta(
  sqlite3 *db,
  Index *pIdx,
  ProllyMutMap *pMap,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal,
  DoltlitePartialIndex *pPart
);

int normalizeSideToMergedLayout(
  sqlite3 *db,
  const char *zTable,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  u8 srcFlags,
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  int bFillSharedDefaults,
  const char *zSharedSql,
  const ProllyHash *pAncRoot,
  u8 ancFlags,
  ProllyHash *pOutRoot,
  char **pzErrMsg
);

int tryResolveSchemaDivergence(
  sqlite3 *db,
  const char *zName,
  const struct TableEntry *pAnc,
  const struct TableEntry *pOurs,
  const struct TableEntry *pTheirs,
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
int mergeScheduleChangedDerivedShadows(sqlite3 *db,
    struct TableEntry *aAnc, int nAnc,
    struct TableEntry *aOurs, int nOurs,
    struct TableEntry *aTheirs, int nTheirs,
    char ***pazRebuild, int *pnRebuild, char **pzRefuse);

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
