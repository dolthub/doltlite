#ifndef DOLTLITE_CATALOG_TYPES_H
#define DOLTLITE_CATALOG_TYPES_H

/*
** Catalog and schema entry layouts shared by the Prolly B-tree and DoltLite
** version-control layers.  Keep these definitions here so object builds and
** the single-translation-unit amalgamation use one canonical owner.
*/

#include "sqliteInt.h"
#include "prolly_hash.h"

struct ProllyMutMap;
typedef struct TableEntry TableEntry;
typedef struct SchemaEntry SchemaEntry;

struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  u8 pendingFlushSeekEdits;
  u8 appendSeekFloorValid;
  i64 appendSeekFloor;
  ProllyHash appendSeekRoot;
  u8 tableRootKnown;
  u8 isTableRoot;
  char *zName;
  struct ProllyMutMap *pPending;
};

struct SchemaEntry {
  char *zName;
  char *zTblName;
  char *zSql;
  char *zType;
  Pgno iRootpage;
};

#endif
