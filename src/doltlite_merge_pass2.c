#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

static const char *mergeIndexSkipQuoted(const char *z, char q){
  char qEnd = (q=='[') ? ']' : q;
  z++;
  while( *z ){
    if( q!='[' && *z==q && z[1]==q ){ z += 2; continue; }
    if( *z==qEnd ) return z+1;
    z++;
  }
  return z;
}

static const char *mergeIndexSkipName(const char *z, const char *zEnd){
  while( z<zEnd && sqlite3Isspace(*z) ) z++;
  if( z>=zEnd ) return z;
  if( *z=='\'' || *z=='"' || *z=='`' || *z=='[' ) return mergeIndexSkipQuoted(z, *z);
  if( sqlite3Isalnum(*z) || *z=='_' || *z=='$' ){
    z++;
    while( z<zEnd && (sqlite3Isalnum(*z) || *z=='_' || *z=='$') ) z++;
  }
  return z;
}

static int mergeIndexIsSortKeyword(const char *z, int n){
  static const char *const azKw[] = {
    "ASC", "COLLATE", "DESC", "FIRST", "LAST", "NULLS"
  };
  int i;
  for(i=0; i<(int)(sizeof(azKw)/sizeof(azKw[0])); i++){
    if( sqlite3_strnicmp(z, azKw[i], n)==0 && azKw[i][n]==0 ) return 1;
  }
  return 0;
}

static char *mergeIndexDupIdent(const char *z, int n){
  char *zOut = n>0 ? sqlite3_malloc(n+1) : 0;
  if( zOut ){ memcpy(zOut, z, n); zOut[n] = 0; }
  return zOut;
}

static int mergeIndexEachColumn(
  const char *zIndexSql,
  int (*xEach)(void*, const char*),
  void *pCtx
){
  const char *zOpen = zIndexSql ? strchr(zIndexSql, '(') : 0;
  const char *zEnd, *z, *zIdent, *zLook;
  char *zCol;
  int depth, nIdent, rc = SQLITE_OK;

  if( !zOpen ) return SQLITE_OK;
  depth = 1;
  zEnd = zOpen + 1;
  while( *zEnd && depth>0 ){
    if( *zEnd=='\'' || *zEnd=='"' || *zEnd=='`' || *zEnd=='[' ){
      zEnd = mergeIndexSkipQuoted(zEnd, *zEnd);
      continue;
    }
    if( *zEnd=='(' ) depth++;
    else if( *zEnd==')' ) depth--;
    if( depth>0 ) zEnd++;
  }
  if( depth!=0 ) return SQLITE_OK;

  z = zOpen + 1;
  zEnd = zIndexSql + strlen(zIndexSql);
  while( z<zEnd && rc==SQLITE_OK ){
    if( *z=='\'' ){ z = mergeIndexSkipQuoted(z, '\''); continue; }
    if( *z=='"' || *z=='`' || *z=='[' ){
      zIdent = z;
      z = mergeIndexSkipQuoted(z, *z);
      zCol = mergeIndexDupIdent(zIdent, (int)(z-zIdent));
      if( !zCol ) return SQLITE_NOMEM;
      sqlite3Dequote(zCol);
      if( zCol[0] ) rc = xEach(pCtx, zCol);
      sqlite3_free(zCol);
      continue;
    }
    if( !(sqlite3Isalnum(*z) || *z=='_' || *z=='$') ){ z++; continue; }
    zIdent = z++;
    while( z<zEnd && (sqlite3Isalnum(*z) || *z=='_' || *z=='$') ) z++;
    nIdent = (int)(z-zIdent);
    zLook = z;
    while( zLook<zEnd && sqlite3Isspace(*zLook) ) zLook++;
    if( zLook<zEnd && *zLook=='(' ) continue;
    if( mergeIndexIsSortKeyword(zIdent, nIdent) ){
      if( nIdent==7 && sqlite3_strnicmp(zIdent, "COLLATE", 7)==0 ){
        z = mergeIndexSkipName(z, zEnd);
      }
      continue;
    }
    zCol = mergeIndexDupIdent(zIdent, nIdent);
    if( !zCol ) return SQLITE_NOMEM;
    rc = xEach(pCtx, zCol);
    sqlite3_free(zCol);
  }
  return rc;
}

typedef struct MergeIndexColCtx MergeIndexColCtx;
struct MergeIndexColCtx {
  ParsedColumn *aAnc; int nAnc;
  ParsedColumn *aSide; int nSide;
  char *zMissing;
};

static int mergeIndexColSurvives(void *pCtx, const char *zCol){
  MergeIndexColCtx *p = (MergeIndexColCtx*)pCtx;
  int iAnc;
  if( p->zMissing ) return SQLITE_OK;
  if( parsedColumnIndexByName(p->aSide, p->nSide, zCol)>=0 ) return SQLITE_OK;
  /* Other side added it; merge carries additions. */
  iAnc = parsedColumnIndexByName(p->aAnc, p->nAnc, zCol);
  if( iAnc<0 ) return SQLITE_OK;
  /* Same position, same definition: a rename, not a drop. Index
  ** retarget is not expressible here yet. */
  if( iAnc<p->nSide
   && parsedColumnIndexByName(p->aAnc, p->nAnc, p->aSide[iAnc].zName)<0
   && parsedColumnDefinitionsMatch(&p->aSide[iAnc], &p->aAnc[iAnc]) ){
    return SQLITE_OK;
  }
  p->zMissing = sqlite3_mprintf("%s", zCol);
  return p->zMissing ? SQLITE_OK : SQLITE_NOMEM;
}

static int mergeIndexColRenamed(void *pCtx, const char *zCol){
  MergeIndexColCtx *p = (MergeIndexColCtx*)pCtx;
  int iAnc;
  if( p->zMissing ) return SQLITE_OK;
  if( parsedColumnIndexByName(p->aSide, p->nSide, zCol)>=0 ) return SQLITE_OK;
  iAnc = parsedColumnIndexByName(p->aAnc, p->nAnc, zCol);
  if( iAnc<0 ) return SQLITE_OK;
  if( iAnc<p->nSide
   && parsedColumnIndexByName(p->aAnc, p->nAnc, p->aSide[iAnc].zName)<0
   && parsedColumnDefinitionsMatch(&p->aSide[iAnc], &p->aAnc[iAnc]) ){
    p->zMissing = sqlite3_mprintf("%s", zCol);
    return p->zMissing ? SQLITE_OK : SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int mergeIndexColumnScan(
  const char *zIndexSql,
  const char *zAncTableSql,
  const char *zSideTableSql,
  int (*xEach)(void*, const char*),
  char **pzColumn
){
  MergeIndexColCtx ctx;
  int rc;

  if( pzColumn ) *pzColumn = 0;
  if( !zIndexSql || !zAncTableSql || !zSideTableSql ) return 0;
  memset(&ctx, 0, sizeof(ctx));
  if( parseColumns(zAncTableSql, &ctx.aAnc, &ctx.nAnc)!=SQLITE_OK ) return 0;
  if( parseColumns(zSideTableSql, &ctx.aSide, &ctx.nSide)!=SQLITE_OK ){
    freeColumns(ctx.aAnc, ctx.nAnc);
    return 0;
  }
  rc = mergeIndexEachColumn(zIndexSql, xEach, &ctx);
  freeColumns(ctx.aAnc, ctx.nAnc);
  freeColumns(ctx.aSide, ctx.nSide);
  if( rc!=SQLITE_OK || !ctx.zMissing ){
    sqlite3_free(ctx.zMissing);
    return 0;
  }
  if( pzColumn ){
    *pzColumn = ctx.zMissing;
  }else{
    sqlite3_free(ctx.zMissing);
  }
  return 1;
}

typedef struct MergeIndexNameList MergeIndexNameList;
struct MergeIndexNameList { char **az; int n; };

static int mergeIndexCollectName(void *pCtx, const char *zCol){
  MergeIndexNameList *p = (MergeIndexNameList*)pCtx;
  char **azNew = sqlite3_realloc(p->az, (p->n+1)*(int)sizeof(char*));
  if( !azNew ) return SQLITE_NOMEM;
  p->az = azNew;
  p->az[p->n] = sqlite3_mprintf("%s", zCol);
  if( !p->az[p->n] ) return SQLITE_NOMEM;
  p->n++;
  return SQLITE_OK;
}

static void mergeIndexNameListFree(MergeIndexNameList *p){
  int i;
  for(i=0; i<p->n; i++) sqlite3_free(p->az[i]);
  sqlite3_free(p->az);
}

int mergeIndexColumnsOverlap(const char *zSqlA, const char *zSqlB){
  MergeIndexNameList a;
  MergeIndexNameList b;
  int i, j, bOverlap = 0;

  if( !zSqlA || !zSqlB ) return 0;
  memset(&a, 0, sizeof(a));
  memset(&b, 0, sizeof(b));
  if( mergeIndexEachColumn(zSqlA, mergeIndexCollectName, &a)==SQLITE_OK
   && mergeIndexEachColumn(zSqlB, mergeIndexCollectName, &b)==SQLITE_OK ){
    for(i=0; i<a.n && !bOverlap; i++){
      for(j=0; j<b.n && !bOverlap; j++){
        if( sqlite3_stricmp(a.az[i], b.az[j])==0 ) bOverlap = 1;
      }
    }
  }
  mergeIndexNameListFree(&a);
  mergeIndexNameListFree(&b);
  return bOverlap;
}

/* Dual added indexes over a shared column: Dolt reports a conflict;
** keeping both would impose one side's uniqueness on the other. */
int mergePreDetectDualIndexOverlap(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  MergeConflictTable **ppConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts
){
  int i, j, rc;

  for(i=0; i<nOurs; i++){
    if( !aOurs[i].zType || strcmp(aOurs[i].zType, "index")!=0 ) continue;
    if( !aOurs[i].zName || !aOurs[i].zSql || !aOurs[i].zTblName ) continue;
    if( findSchemaEntry(aAnc, nAnc, aOurs[i].zName) ) continue;
    for(j=0; j<nTheirs; j++){
      int added = 0;
      if( !aTheirs[j].zType || strcmp(aTheirs[j].zType, "index")!=0 ) continue;
      if( !aTheirs[j].zName || !aTheirs[j].zSql || !aTheirs[j].zTblName ) continue;
      if( findSchemaEntry(aAnc, nAnc, aTheirs[j].zName) ) continue;
      if( sqlite3_stricmp(aOurs[i].zName, aTheirs[j].zName)==0 ) continue;
      if( sqlite3_stricmp(aOurs[i].zTblName, aTheirs[j].zTblName)!=0 ) continue;
      if( !mergeIndexColumnsOverlap(aOurs[i].zSql, aTheirs[j].zSql) ) continue;
      rc = appendSchemaConflict(ppConflictTables, pnConflictTables,
                                aOurs[i].zTblName, aOurs[i].zName, &added);
      if( rc!=SQLITE_OK ) return rc;
      if( pTotalConflicts ) *pTotalConflicts += added;
      break;
    }
  }
  return SQLITE_OK;
}

int mergeIndexColumnRenamedAway(
  const char *zIndexSql,
  const char *zAncTableSql,
  const char *zSideTableSql,
  char **pzColumn
){
  return mergeIndexColumnScan(zIndexSql, zAncTableSql, zSideTableSql,
                              mergeIndexColRenamed, pzColumn);
}

int mergeIndexColumnGoneFrom(
  const char *zIndexSql,
  const char *zAncTableSql,
  const char *zSideTableSql,
  char **pzColumn
){
  MergeIndexColCtx ctx;
  int rc;

  if( pzColumn ) *pzColumn = 0;
  if( !zIndexSql || !zAncTableSql || !zSideTableSql ) return 0;

  memset(&ctx, 0, sizeof(ctx));
  if( parseColumns(zAncTableSql, &ctx.aAnc, &ctx.nAnc)!=SQLITE_OK ) return 0;
  if( parseColumns(zSideTableSql, &ctx.aSide, &ctx.nSide)!=SQLITE_OK ){
    freeColumns(ctx.aAnc, ctx.nAnc);
    return 0;
  }

  rc = mergeIndexEachColumn(zIndexSql, mergeIndexColSurvives, &ctx);
  freeColumns(ctx.aAnc, ctx.nAnc);
  freeColumns(ctx.aSide, ctx.nSide);
  if( rc!=SQLITE_OK || !ctx.zMissing ){
    sqlite3_free(ctx.zMissing);
    return 0;
  }
  if( pzColumn ){
    *pzColumn = ctx.zMissing;
  }else{
    sqlite3_free(ctx.zMissing);
  }
  return 1;
}


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
){
  int i, rc = SQLITE_OK;

  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 ) continue;

    if( !zName ){
      SchemaEntry *pTheirSe = findSchemaEntryByRootpage(
          aTheirsSchema, nTheirsSchema, aTheirs[i].iTable);
      if( pTheirSe && pTheirSe->zName && pTheirSe->zType
       && strcmp(pTheirSe->zType, "index")==0 ){
        SchemaEntry *pAncSe = findSchemaEntry(
            aAncSchema, nAncSchema, pTheirSe->zName);
        SchemaEntry *pOurSe = findSchemaEntry(
            aOursSchema, nOursSchema, pTheirSe->zName);
        struct TableEntry *pTheirTable = doltliteFindTableByName(
            aTheirs, nTheirs, pTheirSe->zTblName);
        if( hasSchemaConflictObject(aConflictTables, nConflictTables,
                                    pTheirSe->zName)
         || hasSchemaConflictObject(aConflictTables, nConflictTables,
                                    pTheirSe->zTblName)
         || hasSchemaConflictTable(aConflictTables, nConflictTables,
                                   pTheirSe->zTblName)
         || !pTheirTable
         || !doltliteFindTableByName(aMerged, *pnMerged,
                                     pTheirSe->zTblName)
         ){
          continue;
        }
        if( mergeIndexFollowsDualRename(
              aAncSchema, nAncSchema, aOursSchema, nOursSchema,
              aTheirsSchema, nTheirsSchema, pAncSe, pOurSe, pTheirSe) ){
          continue;
        }
        /* Index over a column we dropped or renamed cannot load.
        ** Dolt drops it with the column. */
        {
          SchemaEntry *pAncTbl = findSchemaEntry(
              aAncSchema, nAncSchema, pTheirSe->zTblName);
          SchemaEntry *pOurTbl = findSchemaEntry(
              aOursSchema, nOursSchema, pTheirSe->zTblName);
          char *zGone = 0;
          if( pAncTbl && pOurTbl
           && mergeIndexColumnGoneFrom(pTheirSe->zSql, pAncTbl->zSql,
                                           pOurTbl->zSql, &zGone) ){
            sqlite3_free(zGone);
            continue;
          }
          sqlite3_free(zGone);
        }
        oursEntry = findCatalogEntryBySchemaObject(
            aOurs, nOurs, aOursSchema, nOursSchema,
            pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
        if( oursEntry ) continue;

        {
          struct TableEntry *ancEntry = findCatalogEntryBySchemaObject(
              aAnc, nAnc, aAncSchema, nAncSchema,
              pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
          if( !ancEntry ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            int forceRemap = bDisjointSchemaChanges;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict || forceRemap ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                                     (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
            aMerged[(*pnMerged)++] = newEntry;
            /* Adopted tree is theirs-only; rebuild over merged rows. */
            rc = mergeAppendReindexName(pazReindex, pnReindex, pTheirSe->zName);
            if( rc!=SQLITE_OK ) return rc;
          }else if( schemaEntryChangedByName(
                        aAncSchema, nAncSchema,
                        aTheirsSchema, nTheirsSchema,
                        pTheirSe->zName) ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                  (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ){
              *piNextMerged = newEntry.iTable + 1;
            }
            aMerged[(*pnMerged)++] = newEntry;
            rc = mergeAppendReindexName(pazReindex, pnReindex, pTheirSe->zName);
            if( rc!=SQLITE_OK ) return rc;
          }else{
            /* Unmodified index does not override our DROP. */
          }
        }
        continue;
      }

      if( !doltliteFindTableByNumber(aOurs, nOurs, aTheirs[i].iTable) ){
        struct TableEntry newEntry = aTheirs[i];
        int j, conflict = 0;
        Pgno oldPg = newEntry.iTable;
        int forceRemap = bDisjointSchemaChanges;
        for(j=0; j<*pnMerged; j++){
          if( aMerged[j].iTable==newEntry.iTable ){
            conflict = 1;
            break;
          }
        }
        if( conflict || forceRemap ){
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = oldPg;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
        aMerged[(*pnMerged)++] = newEntry;
      }else if( bDisjointSchemaChanges ){
        struct TableEntry *oursIdx = doltliteFindTableByNumber(aOurs, nOurs, aTheirs[i].iTable);
        struct TableEntry *ancIdx = doltliteFindTableByNumber(aAnc, nAnc, aTheirs[i].iTable);
        if( oursIdx && !ancIdx
         && (prollyHashCompare(&oursIdx->root, &aTheirs[i].root)!=0
             || prollyHashCompare(&oursIdx->schemaHash, &aTheirs[i].schemaHash)!=0) ){
          struct TableEntry newEntry = aTheirs[i];
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = aTheirs[i].iTable;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
          aMerged[(*pnMerged)++] = newEntry;
        }
      }
      continue;
    }

    if( hasSchemaConflictObject(aConflictTables, nConflictTables, zName) ){
      continue;
    }
    oursEntry = doltliteFindTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;

    {
      struct TableEntry *ancEntry = doltliteFindTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){

        struct TableEntry newEntry = aTheirs[i];
        int forceRemap = bDisjointSchemaChanges;
        {
          int j, conflict = 0;
          for(j=0; j<*pnMerged; j++){
            if( aMerged[j].iTable==newEntry.iTable ){ conflict = 1; break; }
          }
          if( conflict || forceRemap ) newEntry.iTable = (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;

        newEntry.zName = sqlite3_mprintf("%s", zName);
        aMerged[(*pnMerged)++] = newEntry;
      }else{

        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          return SQLITE_ERROR;
        }

      }
    }
  }
  return SQLITE_OK;
}

/* Contentless FTS has no source to rebuild from; fts5 refuses 'rebuild'. */
static int mergeFtsIsContentless(Table *pTab){
  int i;
  for(i=3; i<pTab->u.vtab.nArg; i++){
    const char *z = pTab->u.vtab.azArg[i];
    const char *zEq;
    if( !z ) continue;
    while( *z==' ' ) z++;
    if( sqlite3_strnicmp(z, "content", 7)!=0 ) continue;
    zEq = &z[7];
    while( *zEq==' ' ) zEq++;
    if( *zEq!='=' ) continue;
    zEq++;
    while( *zEq==' ' ) zEq++;
    /* content='' is contentless; content='x' is not. */
    if( (zEq[0]=='\'' && zEq[1]=='\'') || (zEq[0]=='"' && zEq[1]=='"')
     || zEq[0]==0 ){
      return 1;
    }
    return 0;
  }
  return 0;
}

/* Derived shadow of a vtab (not %_content/%_base). Sets *pbRebuildable
** if the owner can regenerate it. Authoritative shadows stay loud. */
static char *mergeDerivedShadowOwner(
  sqlite3 *db,
  const char *zTable,
  int *pbRebuildable
){
  static const char *azSuffix[] = {
    /* vec1 */   "_idx", "_meta", "_model", "_config",
    /* fts5 */   "_data", "_docsize",
    /* fts3/4 */ "_segments", "_segdir", "_stat",
    /* rtree */  "_node", "_parent", "_rowid"
  };
  unsigned int i;
  size_t nTable = strlen(zTable);

  *pbRebuildable = 0;
  for(i=0; i<sizeof(azSuffix)/sizeof(azSuffix[0]); i++){
    size_t nSfx = strlen(azSuffix[i]);
    const char *zModule;
    char *zOwner;
    Table *pTab;
    if( nTable<=nSfx || strcmp(&zTable[nTable-nSfx], azSuffix[i])!=0 ){
      continue;
    }
    zOwner = sqlite3_mprintf("%.*s", (int)(nTable-nSfx), zTable);
    if( !zOwner ) return 0;
    pTab = sqlite3FindTable(db, zOwner, "main");
    if( !pTab || !IsVirtual(pTab) || pTab->u.vtab.nArg<=0
     || !sqlite3IsShadowTableOf(db, pTab, zTable) ){
      sqlite3_free(zOwner);
      return 0;
    }
    zModule = pTab->u.vtab.azArg[0];

    if( sqlite3_stricmp(zModule, "fts5")==0
     || sqlite3_stricmp(zModule, "fts4")==0
     || sqlite3_stricmp(zModule, "fts3")==0 ){
      /* Rebuild from merged content; contentless has none. */
      *pbRebuildable = !mergeFtsIsContentless(pTab);
      return zOwner;
    }
    if( sqlite3_stricmp(zModule, "rtree")==0
     || sqlite3_stricmp(zModule, "rtree_i32")==0 ){
      /* r-tree coordinates live in %_node; no rebuild. */
      *pbRebuildable = 0;
      return zOwner;
    }
    if( sqlite3_stricmp(zModule, "vec1")!=0 ){
      sqlite3_free(zOwner);
      return 0;
    }
    {
      /* Rebuild only if %_base holds raw vectors and the model row
      ** is present. A NULL rebuild arg keeps cached state. Uncompressed
      ** %_base stores buckets, not vectors; auto-resolve would drop them. */
      char *zQry = sqlite3_mprintf(
          "SELECT (SELECT count(*) FROM \"%w_base\""
          "         WHERE typeof(vector)!='blob')=0"
          " AND EXISTS(SELECT 1 FROM \"%w_model\""
          "             WHERE id=1 AND typeof(val)='blob' AND length(val)>0)",
          zOwner, zOwner);
      sqlite3_stmt *pStmt = 0;
      int bEligible = 0;
      if( zQry && sqlite3_prepare_v2(db, zQry, -1, &pStmt, 0)==SQLITE_OK ){
        bEligible = sqlite3_step(pStmt)==SQLITE_ROW
                 && sqlite3_column_int(pStmt, 0)==1;
      }
      sqlite3_finalize(pStmt);
      sqlite3_free(zQry);
      if( bEligible ){
        *pbRebuildable = 1;
        return zOwner;
      }
      /* Ineligible vec1 stays resolvable; not reported as derived. */
      sqlite3_free(zOwner);
      return 0;
    }
  }
  return 0;
}

/* If every conflict is a rebuildable derived shadow, drop them and
** return owners to rebuild. Any non-derived conflict keeps all loud. */
int mergeFilterDerivedShadowConflicts(
  sqlite3 *db,
  MergeConflictTable *aConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts,
  char ***pazRebuild,
  int *pnRebuild,
  char **pzRefuse
){
  int i, j, rc = SQLITE_OK;
  char **azOwner = 0;

  azOwner = sqlite3_malloc((*pnConflictTables)*(int)sizeof(char*));
  if( !azOwner ) return SQLITE_NOMEM;
  memset(azOwner, 0, (*pnConflictTables)*sizeof(char*));

  for(i=0; i<*pnConflictTables; i++){
    int bRebuildable = 0;
    if( aConflictTables[i].nSchemaObjects>0
     || aConflictTables[i].nConflicts<=0
     || (azOwner[i] = mergeDerivedShadowOwner(
            db, aConflictTables[i].zName, &bRebuildable))==0 ){
      doltliteFreeNameList(azOwner, *pnConflictTables);
      return SQLITE_OK;
    }
    if( !bRebuildable ){
      /* Neither resolution is valid: the discarded side cannot be
      ** regenerated. Refuse rather than commit a disagreeing index. */
      if( pzRefuse && *pzRefuse==0 ){
        *pzRefuse = sqlite3_mprintf(
            "cannot merge: '%s' indexes data that cannot be rebuilt from the "
            "merged rows, and both sides changed it. Merge on one branch, or "
            "drop and recreate '%s' after merging",
            azOwner[i], azOwner[i]);
      }
      doltliteFreeNameList(azOwner, *pnConflictTables);
      return SQLITE_OK;
    }
  }

  for(i=0; i<*pnConflictTables && rc==SQLITE_OK; i++){
    int seen = 0;
    for(j=0; j<i; j++){
      if( strcmp(azOwner[j], azOwner[i])==0 ){ seen = 1; break; }
    }
    if( !seen ){
      rc = mergeAppendReindexName(pazRebuild, pnRebuild, azOwner[i]);
    }
  }
  doltliteFreeNameList(azOwner, *pnConflictTables);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<*pnConflictTables; i++){
    sqlite3_free(aConflictTables[i].zName);
    aConflictTables[i].zName = 0;
    freeConflictRows(aConflictTables[i].aRows, aConflictTables[i].nConflicts);
    aConflictTables[i].aRows = 0;
    aConflictTables[i].nConflicts = 0;
  }
  *pnConflictTables = 0;
  *pTotalConflicts = 0;
  return SQLITE_OK;
}

#endif
