#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Schema-level refusals, before pass 1 mutates anything. */

/* Bare column-name keys only. Expression, partial, COLLATE, or sort
** order make two indexes different; those are not compared. */
static int mergeIndexPlainKey(const char *zSql, char ***pazCol, int *pnCol){
  const char *zOpen, *zClose, *z;
  char **az = 0;
  int n = 0, i;

  *pazCol = 0;
  *pnCol = 0;
  if( !zSql ) return 0;
  zOpen = strchr(zSql, '(');
  if( !zOpen ) return 0;
  for(zClose=zOpen+1; *zClose && *zClose!=')'; zClose++){
    /* Nested paren: function or parenthesised expression. */
    if( *zClose=='(' ) return 0;
  }
  if( *zClose!=')' ) return 0;
  for(z=zClose+1; *z; z++){
    if( !sqlite3Isspace(*z) && *z!=';' ) return 0;
  }
  z = zOpen + 1;
  while( z<zClose ){
    const char *zTok;
    int nTok;
    while( z<zClose && (sqlite3Isspace(*z) || *z==',') ) z++;
    if( z>=zClose ) break;
    zTok = z;
    while( z<zClose && *z!=',' ) z++;
    nTok = (int)(z-zTok);
    while( nTok>0 && sqlite3Isspace(zTok[nTok-1]) ) nTok--;
    if( nTok<=0 ) goto plain_key_fail;
    for(i=0; i<nTok; i++){
      /* ASC, COLLATE, or a quoted form is not a safe comparison. */
      if( !(sqlite3Isalnum(zTok[i]) || zTok[i]=='_' || zTok[i]=='$') ){
        goto plain_key_fail;
      }
    }
    {
      char **azNew = sqlite3_realloc(az, (n+1)*(int)sizeof(char*));
      if( !azNew ) goto plain_key_fail;
      az = azNew;
      az[n] = sqlite3_mprintf("%.*s", nTok, zTok);
      if( !az[n] ) goto plain_key_fail;
      n++;
    }
  }
  if( n==0 ) goto plain_key_fail;
  *pazCol = az;
  *pnCol = n;
  return 1;

plain_key_fail:
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
  return 0;
}

static int mergeIndexSameKey(const char *zSqlA, const char *zSqlB){
  char **azA = 0, **azB = 0;
  int nA = 0, nB = 0, i, same = 0;

  if( mergeIndexPlainKey(zSqlA, &azA, &nA)
   && mergeIndexPlainKey(zSqlB, &azB, &nB)
   && nA==nB ){
    same = 1;
    for(i=0; i<nA && same; i++){
      if( sqlite3_stricmp(azA[i], azB[i])!=0 ) same = 0;
    }
  }
  for(i=0; i<nA; i++) sqlite3_free(azA[i]);
  for(i=0; i<nB; i++) sqlite3_free(azB[i]);
  sqlite3_free(azA);
  sqlite3_free(azB);
  return same;
}

/* One side added an index over columns another index already covers.
** Dolt refuses ("cannot be merged"): keeping both would impose one
** side's uniqueness. Solo additions are the user's business. */
int mergePass1CheckDuplicateIndexColumns(MergePass1Ctx *c){
  int side, i, j;

  /* Merge only. Replay (revert/cherry-pick/rebase) has one intended
  ** result; refusing would also reject restoring a prior state. */
  if( !c->bBranchMerge ) return SQLITE_OK;

  for(side=0; side<2; side++){
    SchemaEntry *aNew = side ? c->aTheirsSchema : c->aOursSchema;
    int nNew = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aOther = side ? c->aOursSchema : c->aTheirsSchema;
    int nOther = side ? c->nOursSchema : c->nTheirsSchema;

    for(i=0; i<nNew; i++){
      if( !aNew[i].zType || strcmp(aNew[i].zType, "index")!=0 ) continue;
      if( !aNew[i].zName || !aNew[i].zSql || !aNew[i].zTblName ) continue;
      if( findSchemaEntry(c->aAncSchema, c->nAncSchema, aNew[i].zName) ){
        continue;
      }
      for(j=0; j<c->nAncSchema; j++){
        SchemaEntry *pOld = &c->aAncSchema[j];
        if( !pOld->zType || strcmp(pOld->zType, "index")!=0 ) continue;
        if( !pOld->zName || !pOld->zSql || !pOld->zTblName ) continue;
        if( sqlite3_stricmp(pOld->zTblName, aNew[i].zTblName)!=0 ) continue;
        /* A drop of the older index is a replacement, not a duplicate. */
        if( !findSchemaEntry(aNew, nNew, pOld->zName) ) continue;
        if( !findSchemaEntry(aOther, nOther, pOld->zName) ) continue;
        if( !mergeIndexSameKey(aNew[i].zSql, pOld->zSql) ) continue;
        if( c->pzErrMsg ){
          sqlite3_free(*c->pzErrMsg);
          *c->pzErrMsg = sqlite3_mprintf(
              "cannot merge: indexes '%s' and '%s' cover the same columns of "
              "table '%s'; drop one of them, then merge",
              aNew[i].zName, pOld->zName, aNew[i].zTblName);
        }
        return SQLITE_ERROR;
      }
    }
  }
  return SQLITE_OK;
}

/* Drop on one side, edit of that column on the other, in a shared
** row. Dolt reports a conflict; refuse rather than pick a winner. */
int mergePass1CheckRowEditOfDroppedColumn(MergePass1Ctx *c){
  int side, i, j;

  for(side=0; side<2; side++){
    SchemaEntry *aDrop = side ? c->aTheirsSchema : c->aOursSchema;
    int nDrop = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aEdit = side ? c->aOursSchema : c->aTheirsSchema;
    int nEdit = side ? c->nOursSchema : c->nTheirsSchema;
    struct TableEntry *aEditCat = side ? c->aOurs : c->aTheirs;
    int nEditCat = side ? c->nOurs : c->nTheirs;

    /* Replay: Dolt protects non-NULL live values; NULL or already-absent
    ** can drop. */
    if( !c->bBranchMerge && side==0 ) continue;

    for(i=0; i<c->nAncSchema; i++){
      const char *zTable = c->aAncSchema[i].zName;
      SchemaEntry *pDropSe;
      SchemaEntry *pEditSe;
      struct TableEntry *pAncCat;
      struct TableEntry *pEditCatEnt;
      ParsedColumn *aAncCols = 0;
      ParsedColumn *aDropCols = 0;
      int nAncCols = 0, nDropCols = 0;

      if( !zTable || !c->aAncSchema[i].zType ) continue;
      if( strcmp(c->aAncSchema[i].zType, "table")!=0 ) continue;
      pDropSe = findSchemaEntry(aDrop, nDrop, zTable);
      pEditSe = findSchemaEntry(aEdit, nEdit, zTable);
      if( !pDropSe || !pEditSe || !pDropSe->zSql || !pEditSe->zSql ) continue;
      /* Schema disagreement is judged by the schema merge. */
      if( strcmp(pEditSe->zSql, c->aAncSchema[i].zSql)!=0 ) continue;
      if( strcmp(pDropSe->zSql, c->aAncSchema[i].zSql)==0 ) continue;
      pAncCat = doltliteFindTableByName(c->aAnc, c->nAnc, zTable);
      pEditCatEnt = doltliteFindTableByName(aEditCat, nEditCat, zTable);
      if( !pAncCat || !pEditCatEnt ) continue;
      if( prollyHashCompare(&pAncCat->root, &pEditCatEnt->root)==0 ) continue;
      if( parseColumns(c->aAncSchema[i].zSql, &aAncCols, &nAncCols)!=SQLITE_OK ){
        continue;
      }
      if( parseColumns(pDropSe->zSql, &aDropCols, &nDropCols)!=SQLITE_OK ){
        freeColumns(aAncCols, nAncCols);
        continue;
      }
      for(j=0; j<nAncCols; j++){
        int bEdited = 0;
        int rc;
        if( parsedColumnIndexByName(aDropCols, nDropCols,
                                    aAncCols[j].zName)>=0 ){
          continue;
        }
        /* Rename, not drop. */
        if( j<nDropCols
         && parsedColumnIndexByName(aAncCols, nAncCols,
                                    aDropCols[j].zName)<0 ){
          continue;
        }
        rc = mergeRowEditsColumn(c->db, &pAncCat->root, &pEditCatEnt->root,
                                 pAncCat->flags, pEditCatEnt->flags,
                                 j, !c->bBranchMerge, &bEdited);
        if( rc!=SQLITE_OK ){
          freeColumns(aAncCols, nAncCols);
          freeColumns(aDropCols, nDropCols);
          return rc;
        }
        if( bEdited ){
          if( c->pzErrMsg ){
            sqlite3_free(*c->pzErrMsg);
            if( c->bBranchMerge ){
              *c->pzErrMsg = sqlite3_mprintf(
                  "cannot merge: column '%s' of table '%s' was dropped on one "
                  "branch and its value changed on the other; drop the column "
                  "on both branches, or revert the change, then merge",
                  aAncCols[j].zName, zTable);
            }else{
              *c->pzErrMsg = sqlite3_mprintf(
                  "cannot apply: column '%s' of table '%s' would be dropped, "
                  "discarding a changed value",
                  aAncCols[j].zName, zTable);
            }
          }
          freeColumns(aAncCols, nAncCols);
          freeColumns(aDropCols, nDropCols);
          return SQLITE_ERROR;
        }
      }
      freeColumns(aAncCols, nAncCols);
      freeColumns(aDropCols, nDropCols);
    }
  }
  return SQLITE_OK;
}

/* Identifier scan, quoted or bare (SQLite may quote). Indexes start
** at the column list so the index name cannot match. Over-matches by
** design: an expression index or view naming it breaks the same way. */
static int mergeSqlNamesColumn(const char *zSql, const char *zType,
                               const char *zColumn){
  const char *z;
  int nCol;

  if( !zSql || !zType || !zColumn || !zColumn[0] ) return 0;
  nCol = (int)strlen(zColumn);
  z = zSql;
  if( strcmp(zType, "index")==0 ){
    z = strchr(zSql, '(');
    if( !z ) return 0;
    z++;
  }
  while( *z ){
    const char *zTok;
    int nTok;
    if( *z=='"' || *z=='`' || *z=='[' || *z=='\'' ){
      char cEnd = *z=='[' ? ']' : *z;
      z++;
      zTok = z;
      while( *z && *z!=cEnd ) z++;
      nTok = (int)(z-zTok);
      if( *z ) z++;
    }else if( sqlite3Isalnum(*z) || *z=='_' || *z=='$' ){
      zTok = z;
      while( *z && (sqlite3Isalnum(*z) || *z=='_' || *z=='$') ) z++;
      nTok = (int)(z-zTok);
    }else{
      z++;
      continue;
    }
    if( nTok==nCol && sqlite3_strnicmp(zTok, zColumn, nCol)==0 ) return 1;
  }
  return 0;
}

static int mergeSideRenamedColumnTo(const char *zAncSql, const char *zSideSql,
                                    const char *zColumn, char **pzNew){
  ParsedColumn *aAnc = 0;
  ParsedColumn *aSide = 0;
  int nAnc = 0, nSide = 0, i, bRenamed = 0;

  if( pzNew ) *pzNew = 0;
  if( !zAncSql || !zSideSql ) return 0;
  if( parseColumns(zAncSql, &aAnc, &nAnc)!=SQLITE_OK ) return 0;
  if( parseColumns(zSideSql, &aSide, &nSide)!=SQLITE_OK ){
    freeColumns(aAnc, nAnc);
    return 0;
  }
  for(i=0; i<nAnc && i<nSide; i++){
    if( sqlite3_stricmp(aAnc[i].zName, zColumn)!=0 ) continue;
    if( sqlite3_stricmp(aSide[i].zName, aAnc[i].zName)==0 ) break;
    if( parsedColumnIndexByName(aSide, nSide, aAnc[i].zName)>=0 ) break;
    if( !parsedColumnDefinitionsMatch(&aSide[i], &aAnc[i]) ) break;
    bRenamed = 1;
    if( pzNew ) *pzNew = sqlite3_mprintf("%s", aSide[i].zName);
    break;
  }
  freeColumns(aAnc, nAnc);
  freeColumns(aSide, nSide);
  return bRenamed;
}

static int mergeSideRenamedColumn(const char *zAncSql, const char *zSideSql,
                                  const char *zColumn){
  return mergeSideRenamedColumnTo(zAncSql, zSideSql, zColumn, 0);
}

static int mergeSideRenamedAnyColumn(const char *zAncSql, const char *zSideSql){
  ParsedColumn *aAnc = 0;
  int nAnc = 0, i, bAny = 0;

  if( parseColumns(zAncSql, &aAnc, &nAnc)!=SQLITE_OK ) return 0;
  for(i=0; i<nAnc && !bAny; i++){
    if( mergeSideRenamedColumn(zAncSql, zSideSql, aAnc[i].zName) ) bAny = 1;
  }
  freeColumns(aAnc, nAnc);
  return bAny;
}

/* Dual column-rename plus a dependent that names a renamed column.
** Merged catalog takes the table from one side and the object from
** the other, so it cannot load. Dolt retargets the object; we refuse. */
int mergePass1CheckDependentOverDualRename(MergePass1Ctx *c){
  int side, i, j;

  for(side=0; side<2; side++){
    SchemaEntry *aDep = side ? c->aTheirsSchema : c->aOursSchema;
    int nDep = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aOther = side ? c->aOursSchema : c->aTheirsSchema;
    int nOther = side ? c->nOursSchema : c->nTheirsSchema;

    for(i=0; i<nDep; i++){
      SchemaEntry *pDep = &aDep[i];
      SchemaEntry *pAncTbl;
      SchemaEntry *pDepTbl;
      SchemaEntry *pOtherTbl;
      ParsedColumn *aAncCols = 0;
      int nAncCols = 0;

      if( !pDep->zType || !pDep->zSql || !pDep->zTblName ) continue;
      if( strcmp(pDep->zType, "table")==0 ) continue;
      pAncTbl = findSchemaEntry(c->aAncSchema, c->nAncSchema, pDep->zTblName);
      pDepTbl = findSchemaEntry(aDep, nDep, pDep->zTblName);
      pOtherTbl = findSchemaEntry(aOther, nOther, pDep->zTblName);
      if( !pAncTbl || !pDepTbl || !pOtherTbl ) continue;
      if( !pAncTbl->zSql || !pDepTbl->zSql || !pOtherTbl->zSql ) continue;
      /* One-sided rename already carries its objects across. */
      if( !mergeSideRenamedAnyColumn(pAncTbl->zSql, pOtherTbl->zSql) ) continue;
      if( !mergeSideRenamedAnyColumn(pAncTbl->zSql, pDepTbl->zSql) ) continue;
      /* Object sits beside the adopted table; only a cross-side pair
      ** breaks the catalog. */
      {
        char **azAdd = 0, **azDrop = 0, **azRen = 0;
        int nAdd = 0, nDrop = 0, nRen = 0, k;
        int choice = SCHEMA_MERGE_DEFAULT, resolved = 0, bLoser;
        char *zErr = 0;
        const char *zOursTblSql = side ? pOtherTbl->zSql : pDepTbl->zSql;
        const char *zTheirsTblSql = side ? pDepTbl->zSql : pOtherTbl->zSql;
        if( trySchemaColumnMerge(pAncTbl->zSql, zOursTblSql, zTheirsTblSql,
                                 &azAdd, &nAdd, &azDrop, &nDrop,
                                 &azRen, &nRen, &choice, &resolved,
                                 &zErr)!=SQLITE_OK ){
          choice = SCHEMA_MERGE_DEFAULT;
        }
        for(k=0; k<nAdd; k++) sqlite3_free(azAdd[k]);
        for(k=0; k<nDrop; k++) sqlite3_free(azDrop[k]);
        for(k=0; k<nRen; k++) sqlite3_free(azRen[k]);
        sqlite3_free(azAdd);
        sqlite3_free(azDrop);
        sqlite3_free(azRen);
        sqlite3_free(zErr);
        bLoser = side ? (choice==SCHEMA_MERGE_OURS)
                      : (choice==SCHEMA_MERGE_THEIRS);
        if( !bLoser ) continue;
      }
      if( parseColumns(pAncTbl->zSql, &aAncCols, &nAncCols)!=SQLITE_OK ){
        continue;
      }
      for(j=0; j<nAncCols; j++){
        const char *zCol = aAncCols[j].zName;
        char *zNew = 0;
        int bNames;
        if( !mergeSideRenamedColumnTo(pAncTbl->zSql, pDepTbl->zSql, zCol,
                                      &zNew) ){
          sqlite3_free(zNew);
          continue;
        }
        /* This side already rewrote the object to the new name. */
        bNames = zNew && mergeSqlNamesColumn(pDep->zSql, pDep->zType, zNew);
        sqlite3_free(zNew);
        if( !bNames ) continue;
        /* Dual rename of this column is judged elsewhere. */
        if( mergeSideRenamedColumn(pAncTbl->zSql, pOtherTbl->zSql, zCol) ){
          continue;
        }
        if( c->pzErrMsg ){
          sqlite3_free(*c->pzErrMsg);
          *c->pzErrMsg = sqlite3_mprintf(
              "cannot merge: %s '%s' covers column '%s' of table '%s', which "
              "one branch renamed while the other renamed another column of "
              "the same table; rename on one branch only, or drop the %s, "
              "then merge",
              pDep->zType, pDep->zName, zCol, pDep->zTblName, pDep->zType);
        }
        freeColumns(aAncCols, nAncCols);
        return SQLITE_ERROR;
      }
      freeColumns(aAncCols, nAncCols);
    }
  }
  return SQLITE_OK;
}

/* Trigger on a table the other side renamed or dropped. Triggers
** resolve at schema load, so a dangling one cannot be represented.
** Dolt keeps the old name; we refuse. Distinguish drop from rename. */
int mergePass1CheckTriggerOverRenamedTable(MergePass1Ctx *c){
  int side, i, j;

  for(side=0; side<2; side++){
    SchemaEntry *aTrig = side ? c->aTheirsSchema : c->aOursSchema;
    int nTrig = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aOther = side ? c->aOursSchema : c->aTheirsSchema;
    int nOther = side ? c->nOursSchema : c->nTheirsSchema;

    for(i=0; i<nTrig; i++){
      const char *zTable = aTrig[i].zTblName;
      int bRenamed = 0;
      if( !aTrig[i].zType || strcmp(aTrig[i].zType, "trigger")!=0 ) continue;
      if( !aTrig[i].zName || !zTable ) continue;
      /* Ancestor trigger already followed the rename; only one added
      ** beside the rename has nowhere to land. */
      if( findSchemaEntry(c->aAncSchema, c->nAncSchema, aTrig[i].zName) ){
        continue;
      }
      if( !findSchemaEntry(c->aAncSchema, c->nAncSchema, zTable) ) continue;
      if( findSchemaEntry(aOther, nOther, zTable) ) continue;
      /* Same columns as the ancestor table under a new name: rename,
      ** not drop. Names alone cannot tell them apart. */
      {
        SchemaEntry *pAncTbl = findSchemaEntry(c->aAncSchema, c->nAncSchema,
                                               zTable);
        ParsedColumn *aAncCols = 0;
        int nAncCols = 0;
        if( !pAncTbl || !pAncTbl->zSql ) continue;
        if( parseColumns(pAncTbl->zSql, &aAncCols, &nAncCols)!=SQLITE_OK ){
          continue;
        }
        for(j=0; j<nOther && !bRenamed; j++){
          ParsedColumn *aCand = 0;
          int nCand = 0, m, same;
          if( !aOther[j].zType || strcmp(aOther[j].zType, "table")!=0 ) continue;
          if( !aOther[j].zName || !aOther[j].zSql ) continue;
          if( findSchemaEntry(c->aAncSchema, c->nAncSchema, aOther[j].zName) ){
            continue;
          }
          if( parseColumns(aOther[j].zSql, &aCand, &nCand)!=SQLITE_OK ) continue;
          same = nCand==nAncCols;
          for(m=0; m<nCand && same; m++){
            if( sqlite3_stricmp(aCand[m].zName, aAncCols[m].zName)!=0
             || !parsedColumnDefinitionsMatch(&aCand[m], &aAncCols[m]) ){
              same = 0;
            }
          }
          freeColumns(aCand, nCand);
          if( same ) bRenamed = 1;
        }
        freeColumns(aAncCols, nAncCols);
      }
      if( c->pzErrMsg ){
        sqlite3_free(*c->pzErrMsg);
        if( bRenamed ){
          *c->pzErrMsg = sqlite3_mprintf(
              "cannot merge: trigger '%s' runs on table '%s', which the other "
              "branch renamed; drop or recreate the trigger, then merge",
              aTrig[i].zName, zTable);
        }else{
          *c->pzErrMsg = sqlite3_mprintf(
              "cannot merge: trigger '%s' runs on table '%s', which the other "
              "branch dropped; drop or recreate the trigger, then merge",
              aTrig[i].zName, zTable);
        }
      }
      return SQLITE_ERROR;
    }
  }
  return SQLITE_OK;
}

/* Index added over a column the other side renamed. Nothing retargets
** it, and the catalog cannot load. Dolt keeps the index; we refuse. */
int mergePass1CheckIndexOverRenamedColumn(MergePass1Ctx *c){
  int side, i;

  for(side=0; side<2; side++){
    SchemaEntry *aNew = side ? c->aTheirsSchema : c->aOursSchema;
    int nNew = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aOther = side ? c->aOursSchema : c->aTheirsSchema;
    int nOther = side ? c->nOursSchema : c->nTheirsSchema;

    for(i=0; i<nNew; i++){
      SchemaEntry *pAncTbl;
      SchemaEntry *pOtherTbl;
      char *zCol = 0;
      if( !aNew[i].zType || strcmp(aNew[i].zType, "index")!=0 ) continue;
      if( !aNew[i].zName || !aNew[i].zSql || !aNew[i].zTblName ) continue;
      if( findSchemaEntry(c->aAncSchema, c->nAncSchema, aNew[i].zName) ){
        continue;
      }
      if( findSchemaEntry(aOther, nOther, aNew[i].zName) ) continue;
      pAncTbl = findSchemaEntry(c->aAncSchema, c->nAncSchema, aNew[i].zTblName);
      pOtherTbl = findSchemaEntry(aOther, nOther, aNew[i].zTblName);
      if( !pAncTbl || !pOtherTbl ) continue;
      if( !mergeIndexColumnRenamedAway(aNew[i].zSql, pAncTbl->zSql,
                                       pOtherTbl->zSql, &zCol) ){
        continue;
      }
      if( c->pzErrMsg ){
        sqlite3_free(*c->pzErrMsg);
        *c->pzErrMsg = sqlite3_mprintf(
            "cannot merge: index '%s' covers column '%s' of table '%s', which "
            "the other branch renamed; drop or recreate the index, then merge",
            aNew[i].zName, zCol, aNew[i].zTblName);
      }
      sqlite3_free(zCol);
      return SQLITE_ERROR;
    }
  }
  return SQLITE_OK;
}

/* ================= dependents follow their table =================
**
** A table's dependents (indexes, triggers, views) are adopted per name,
** independently of the table, so a column rename on one side can pair an
** adopted table with a dependent that names a column it does not have,
** and the merged catalog cannot load. Dolt retargets the dependent.
**
** Rather than repairing the assembled catalog, subtract the rename from
** the merge's view of the world: rewrite the three schema arrays so the
** table and its dependents carry one baseline text on every side, equalize
** the table entries' schema hashes so pass1 sees the schema as unchanged,
** and queue the renames as post-load schema actions. ALTER TABLE RENAME
** COLUMN then rewrites the table and every dependent through SQLite's own
** machinery, and the post-action flush serializes the coherent result.
*/

/* Flat old,new rename pairs anc->side when the delta is pure renames:
** same column count, every definition matches its position, and no name
** moves to another position. Anything else (adds, drops, swaps, type
** changes) is not normalizable here. */
static int mergePureRenamePairs(
  const char *zAncSql,
  const char *zSideSql,
  char ***pazPairs, int *pnPairs,
  int *pbPure
){
  ParsedColumn *aAnc = 0, *aSide = 0;
  int nAnc = 0, nSide = 0, i, rc = SQLITE_OK;
  char **az = 0;
  int n = 0;

  *pazPairs = 0;
  *pnPairs = 0;
  *pbPure = 0;
  if( parseColumns(zAncSql, &aAnc, &nAnc)!=SQLITE_OK ) return SQLITE_OK;
  if( parseColumns(zSideSql, &aSide, &nSide)!=SQLITE_OK ){
    freeColumns(aAnc, nAnc);
    return SQLITE_OK;
  }
  if( nAnc!=nSide ) goto pure_done;
  for(i=0; i<nAnc; i++){
    if( !parsedColumnDefinitionsMatch(&aSide[i], &aAnc[i]) ) goto pure_done;
    if( sqlite3_stricmp(aAnc[i].zName, aSide[i].zName)==0 ) continue;
    if( parsedColumnIndexByName(aSide, nSide, aAnc[i].zName)>=0 ) goto pure_done;
    if( parsedColumnIndexByName(aAnc, nAnc, aSide[i].zName)>=0 ) goto pure_done;
    {
      char **azNew = sqlite3_realloc(az, (n+2)*(int)sizeof(char*));
      if( !azNew ){ rc = SQLITE_NOMEM; goto pure_done; }
      az = azNew;
      az[n] = sqlite3_mprintf("%s", aAnc[i].zName);
      az[n+1] = sqlite3_mprintf("%s", aSide[i].zName);
      if( !az[n] || !az[n+1] ){ n += 2; rc = SQLITE_NOMEM; goto pure_done; }
      n += 2;
    }
  }
  *pbPure = 1;
  *pazPairs = az;
  *pnPairs = n;
  az = 0;
  n = 0;
pure_done:
  freeAddedColumns(az, n);
  freeColumns(aAnc, nAnc);
  freeColumns(aSide, nSide);
  return rc;
}

static const char *mergeRenameMapLookup(char **az, int n, const char *zOld){
  int i;
  for(i=0; i+1<n; i+=2){
    if( sqlite3_stricmp(az[i], zOld)==0 ) return az[i+1];
  }
  return 0;
}

/* zSide is zAnc with side renames applied by SQLite's rewriter and nothing
** else: identical byte-for-byte outside identifier tokens, and each
** identifier either matches or maps old->new. */
static int mergeTextsEqualModuloRenames(
  const char *zAnc,
  const char *zSide,
  char **azPairs, int nPairs
){
  const char *a = zAnc;
  const char *s = zSide;

  if( !zAnc || !zSide ) return 0;
  while( *a || *s ){
    int aId = sqlite3Isalnum(*a) || *a=='_' || *a=='$' || *a=='"' || *a=='`' || *a=='[';
    int sId = sqlite3Isalnum(*s) || *s=='_' || *s=='$' || *s=='"' || *s=='`' || *s=='[';
    if( !aId || !sId ){
      if( *a!=*s ) return 0;
      if( *a==0 ) break;
      a++;
      s++;
      continue;
    }
    {
      const char *aTok, *sTok;
      int nATok, nSTok;
      if( *a=='"' || *a=='`' || *a=='[' ){
        char cEnd = *a=='[' ? ']' : *a;
        a++; aTok = a;
        while( *a && *a!=cEnd ) a++;
        nATok = (int)(a-aTok);
        if( *a ) a++;
      }else{
        aTok = a;
        while( sqlite3Isalnum(*a) || *a=='_' || *a=='$' ) a++;
        nATok = (int)(a-aTok);
      }
      if( *s=='"' || *s=='`' || *s=='[' ){
        char cEnd = *s=='[' ? ']' : *s;
        s++; sTok = s;
        while( *s && *s!=cEnd ) s++;
        nSTok = (int)(s-sTok);
        if( *s ) s++;
      }else{
        sTok = s;
        while( sqlite3Isalnum(*s) || *s=='_' || *s=='$' ) s++;
        nSTok = (int)(s-sTok);
      }
      if( nATok==nSTok && sqlite3_strnicmp(aTok, sTok, nATok)==0 ) continue;
      {
        int i;
        for(i=0; i+1<nPairs; i+=2){
          if( (int)strlen(azPairs[i])==nATok
           && sqlite3_strnicmp(azPairs[i], aTok, nATok)==0
           && (int)strlen(azPairs[i+1])==nSTok
           && sqlite3_strnicmp(azPairs[i+1], sTok, nSTok)==0 ){
            break;
          }
        }
        if( i+1>=nPairs ) return 0;
      }
    }
  }
  return 1;
}

static int mergeTableTextHasColumn(const char *zTblSql, const char *zName){
  ParsedColumn *aCols = 0;
  int nCols = 0, bHas;
  if( parseColumns(zTblSql, &aCols, &nCols)!=SQLITE_OK ) return 0;
  bHas = parsedColumnIndexByName(aCols, nCols, zName)>=0;
  freeColumns(aCols, nCols);
  return bHas;
}

/* Loadable against zTblSql: every rename-universe name the dependent's text
** mentions must be a column of that table text. Names outside the universe
** cannot have moved, so they are not checked. */
static int mergeDependentCoherent(
  const SchemaEntry *pDep,
  const char *zTblSql,
  char **azUniverse, int nUniverse
){
  int i;
  for(i=0; i<nUniverse; i++){
    if( !mergeSqlNamesColumn(pDep->zSql, pDep->zType, azUniverse[i]) ) continue;
    if( !mergeTableTextHasColumn(zTblSql, azUniverse[i]) ) return 0;
  }
  return 1;
}

static void mergeSetEntryText(SchemaEntry *pSe, const char *zSql){
  char *z = sqlite3_mprintf("%s", zSql);
  if( z ){
    sqlite3_free(pSe->zSql);
    pSe->zSql = z;
  }
}

/* A dependent of zTbl surviving into the merge, or a view whose cross-side
** texts are the mechanical shadow of these renames. Views carry their own
** name in zTblName, so they qualify by shape rather than by parent. */
static int mergeCollectDependent(
  const char *zTbl,
  SchemaEntry *pAnc, SchemaEntry *pOurs, SchemaEntry *pTheirs,
  char **azRenO, int nRenO,
  char **azRenT, int nRenT,
  int *pbMechanical
){
  SchemaEntry *pAny = pOurs ? pOurs : (pTheirs ? pTheirs : pAnc);

  *pbMechanical = 0;
  if( !pAny || !pAny->zType || !pAny->zSql ) return 0;
  if( strcmp(pAny->zType, "index")==0 || strcmp(pAny->zType, "trigger")==0 ){
    if( !pAny->zTblName || sqlite3_stricmp(pAny->zTblName, zTbl)!=0 ) return 0;
  }else if( strcmp(pAny->zType, "view")!=0 ){
    return 0;
  }
  /* Dropped on a side: it does not survive, so it constrains nothing. */
  if( pAnc && (!pOurs || !pTheirs) ) return 0;
  if( pAnc ){
    if( !pAnc->zSql || !pOurs->zSql || !pTheirs->zSql ) return 0;
    if( !mergeTextsEqualModuloRenames(pAnc->zSql, pOurs->zSql, azRenO, nRenO) ){
      return -1;
    }
    if( !mergeTextsEqualModuloRenames(pAnc->zSql, pTheirs->zSql, azRenT, nRenT) ){
      return -1;
    }
    *pbMechanical = 1;
  }
  return 1;
}

int mergePreNormalizeRenamedDependents(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaMergeAction **ppActions, int *pnActions
){
  int t, i, rc = SQLITE_OK;

  for(t=0; t<nAncSchema; t++){
    SchemaEntry *pAncT = &aAncSchema[t];
    SchemaEntry *pOurT, *pTheirT;
    char **azRenO = 0, **azRenT = 0, **azUni = 0;
    int nRenO = 0, nRenT = 0, nUni = 0;
    int bPureO = 0, bPureT = 0;
    const char *zBase = 0;
    int bBaseOurs = 0, bBaseTheirs = 0;
    int bBail;

    if( !pAncT->zType || strcmp(pAncT->zType, "table")!=0 ) continue;
    if( !pAncT->zName || !pAncT->zSql ) continue;
    pOurT = findSchemaEntry(aOursSchema, nOursSchema, pAncT->zName);
    pTheirT = findSchemaEntry(aTheirsSchema, nTheirsSchema, pAncT->zName);
    if( !pOurT || !pTheirT || !pOurT->zSql || !pTheirT->zSql ) continue;

    rc = mergePureRenamePairs(pAncT->zSql, pOurT->zSql, &azRenO, &nRenO, &bPureO);
    if( rc==SQLITE_OK ){
      rc = mergePureRenamePairs(pAncT->zSql, pTheirT->zSql, &azRenT, &nRenT, &bPureT);
    }
    if( rc!=SQLITE_OK ) goto table_done;
    if( !bPureO || !bPureT || (nRenO==0 && nRenT==0) ) goto table_done;
    /* The same ancestor column renamed on both sides is a real conflict
    ** (different names) or already coherent (same name); leave both. */
    for(i=0; i+1<nRenO; i+=2){
      if( mergeRenameMapLookup(azRenT, nRenT, azRenO[i]) ) goto table_done;
    }

    /* Old and new names of every rename: the only identifiers whose
    ** presence can differ between the three table texts. */
    azUni = sqlite3_malloc((nRenO+nRenT)*(int)sizeof(char*));
    if( !azUni ){ rc = SQLITE_NOMEM; goto table_done; }
    for(i=0; i<nRenO; i++) azUni[nUni++] = azRenO[i];
    for(i=0; i<nRenT; i++) azUni[nUni++] = azRenT[i];

    /* Baseline candidates, ancestor first: the text every surviving
    ** dependent can load against. */
    {
      const char *azCand[3];
      int c;
      azCand[0] = pAncT->zSql;
      azCand[1] = pOurT->zSql;
      azCand[2] = pTheirT->zSql;
      zBase = 0;
      bBail = 0;
      for(c=0; c<3 && !bBail; c++){
        int bOk = 1;
        for(i=0; i<nOursSchema+nTheirsSchema && bOk && !bBail; i++){
          SchemaEntry *pSide = i<nOursSchema ? &aOursSchema[i]
                                             : &aTheirsSchema[i-nOursSchema];
          SchemaEntry *pDepAnc, *pDepOurs, *pDepTheirs, *pPick;
          int bMech = 0, q;
          if( !pSide->zName ) continue;
          /* Each dependent once: ours' array wins when present on both. */
          if( i>=nOursSchema
           && findSchemaEntry(aOursSchema, nOursSchema, pSide->zName) ){
            continue;
          }
          pDepAnc = findSchemaEntry(aAncSchema, nAncSchema, pSide->zName);
          pDepOurs = findSchemaEntry(aOursSchema, nOursSchema, pSide->zName);
          pDepTheirs = findSchemaEntry(aTheirsSchema, nTheirsSchema, pSide->zName);
          if( pDepAnc==pAncT ) continue;
          q = mergeCollectDependent(pAncT->zName, pDepAnc, pDepOurs, pDepTheirs,
                                    azRenO, nRenO, azRenT, nRenT, &bMech);
          if( q<0 ){
            /* Materially edited beside a rename: only a problem if it
            ** names a renamed column, then no baseline is safe. */
            SchemaEntry *pDep = pDepOurs ? pDepOurs : pDepTheirs;
            int u;
            for(u=0; u<nUni; u++){
              if( mergeSqlNamesColumn(pDep->zSql, pDep->zType, azUni[u]) ){
                bBail = 1;
                break;
              }
            }
            continue;
          }
          if( q==0 ) continue;
          if( bMech ){
            /* Mechanical shadow: the version from the baseline's own side
            ** is coherent with it by construction. */
            continue;
          }
          /* New on one side: its only text must load against the baseline. */
          pPick = pDepOurs ? pDepOurs : pDepTheirs;
          if( !mergeDependentCoherent(pPick, azCand[c], azUni, nUni) ){
            bOk = 0;
          }
        }
        if( bBail ) break;
        if( bOk && c==0 ){
          /* Everything loads against the ancestor text; but is everything
          ** also coherent with the text the merge would adopt today? Probe
          ** the adopted side: with renames on it, an old-name dependent
          ** cannot load, so normalization is still required. */
          int bAdoptedIncoherent = 0;
          const char *zAdopted = 0;
          int oursChanged = strcmp(pAncT->zSql, pOurT->zSql)!=0;
          int theirsChanged = strcmp(pAncT->zSql, pTheirT->zSql)!=0;
          if( oursChanged && theirsChanged ){
            char **azA=0, **azD=0, **azR=0;
            int nA=0, nD=0, nR=0, choice=SCHEMA_MERGE_DEFAULT, res=0;
            char *zErr = 0;
            if( trySchemaColumnMerge(pAncT->zSql, pOurT->zSql, pTheirT->zSql,
                                     &azA, &nA, &azD, &nD, &azR, &nR,
                                     &choice, &res, &zErr)==SQLITE_OK ){
              zAdopted = choice==SCHEMA_MERGE_THEIRS ? pTheirT->zSql
                                                     : pOurT->zSql;
            }
            sqlite3_free(zErr);
            freeAddedColumns(azA, nA);
            freeAddedColumns(azD, nD);
            freeAddedColumns(azR, nR);
          }else{
            zAdopted = oursChanged ? pOurT->zSql : pTheirT->zSql;
          }
          if( zAdopted ){
            for(i=0; i<nOursSchema+nTheirsSchema && !bAdoptedIncoherent; i++){
              SchemaEntry *pSide = i<nOursSchema ? &aOursSchema[i]
                                                 : &aTheirsSchema[i-nOursSchema];
              SchemaEntry *pDepAnc, *pDepOurs, *pDepTheirs, *pPick;
              int bMech = 0, q;
              if( !pSide->zName ) continue;
              if( i>=nOursSchema
               && findSchemaEntry(aOursSchema, nOursSchema, pSide->zName) ){
                continue;
              }
              pDepAnc = findSchemaEntry(aAncSchema, nAncSchema, pSide->zName);
              pDepOurs = findSchemaEntry(aOursSchema, nOursSchema, pSide->zName);
              pDepTheirs = findSchemaEntry(aTheirsSchema, nTheirsSchema,
                                           pSide->zName);
              if( pDepAnc==pAncT ) continue;
              q = mergeCollectDependent(pAncT->zName, pDepAnc, pDepOurs,
                                        pDepTheirs, azRenO, nRenO,
                                        azRenT, nRenT, &bMech);
              if( q<=0 ) continue;
              /* Today's adoption: the side that changed it, ours on ties. */
              if( pDepAnc ){
                int chO = pDepOurs && strcmp(pDepAnc->zSql, pDepOurs->zSql)!=0;
                int chT = pDepTheirs && strcmp(pDepAnc->zSql, pDepTheirs->zSql)!=0;
                pPick = chT && !chO ? pDepTheirs : pDepOurs;
              }else{
                pPick = pDepOurs ? pDepOurs : pDepTheirs;
              }
              if( pPick && !mergeDependentCoherent(pPick, zAdopted, azUni, nUni) ){
                bAdoptedIncoherent = 1;
              }
            }
          }
          if( !bAdoptedIncoherent ){
            /* Today's merge already loads; do not disturb it. */
            goto table_done;
          }
        }
        if( bOk ){
          zBase = azCand[c];
          bBaseOurs = (c==1);
          bBaseTheirs = (c==2);
          break;
        }
      }
      if( bBail || !zBase ) goto table_done;
    }

    /* Queue the renames the baseline still needs before mutating any text
    ** they are derived from. */
    {
      char **azQ = 0;
      int nQ = 0, k;
      char **azFromO = bBaseOurs ? 0 : azRenO;
      int nFromO = bBaseOurs ? 0 : nRenO;
      char **azFromT = bBaseTheirs ? 0 : azRenT;
      int nFromT = bBaseTheirs ? 0 : nRenT;
      azQ = sqlite3_malloc((nFromO+nFromT)*(int)sizeof(char*)+1);
      if( !azQ ){ rc = SQLITE_NOMEM; goto table_done; }
      for(k=0; k<nFromO; k++) azQ[nQ++] = sqlite3_mprintf("%s", azFromO[k]);
      for(k=0; k<nFromT; k++) azQ[nQ++] = sqlite3_mprintf("%s", azFromT[k]);
      for(k=0; k<nQ; k++){
        if( !azQ[k] ){ freeAddedColumns(azQ, nQ); rc = SQLITE_NOMEM; goto table_done; }
      }
      if( nQ>0 ){
        /* The action owns azQ from here. */
        rc = recordSchemaColumnChanges(ppActions, pnActions, pAncT->zName,
                                       0, 0, 0, 0, azQ, nQ);
        if( rc!=SQLITE_OK ){ freeAddedColumns(azQ, nQ); goto table_done; }
      }else{
        sqlite3_free(azQ);
      }
    }

    /* One baseline text on every side: the merge no longer sees a schema
    ** change on this table or its shadowed dependents. zBase aliases one of
    ** the entries about to be rewritten, so it needs its own copy first. */
    {
      char *zBaseOwned = sqlite3_mprintf("%s", zBase);
      if( !zBaseOwned ){ rc = SQLITE_NOMEM; goto table_done; }
      mergeSetEntryText(pAncT, zBaseOwned);
      mergeSetEntryText(pOurT, zBaseOwned);
      mergeSetEntryText(pTheirT, zBaseOwned);
      sqlite3_free(zBaseOwned);
    }
    for(i=0; i<nOursSchema+nTheirsSchema; i++){
      SchemaEntry *pSide = i<nOursSchema ? &aOursSchema[i]
                                         : &aTheirsSchema[i-nOursSchema];
      SchemaEntry *pDepAnc, *pDepOurs, *pDepTheirs, *pFrom;
      char *zDepOwned;
      int bMech = 0, q;
      if( !pSide->zName ) continue;
      if( i>=nOursSchema
       && findSchemaEntry(aOursSchema, nOursSchema, pSide->zName) ){
        continue;
      }
      pDepAnc = findSchemaEntry(aAncSchema, nAncSchema, pSide->zName);
      pDepOurs = findSchemaEntry(aOursSchema, nOursSchema, pSide->zName);
      pDepTheirs = findSchemaEntry(aTheirsSchema, nTheirsSchema, pSide->zName);
      if( pDepAnc==pAncT ) continue;
      q = mergeCollectDependent(pAncT->zName, pDepAnc, pDepOurs, pDepTheirs,
                                azRenO, nRenO, azRenT, nRenT, &bMech);
      if( q<=0 || !bMech ) continue;
      pFrom = bBaseOurs ? pDepOurs : (bBaseTheirs ? pDepTheirs : pDepAnc);
      if( !pFrom || !pFrom->zSql ) continue;
      zDepOwned = sqlite3_mprintf("%s", pFrom->zSql);
      if( !zDepOwned ){ rc = SQLITE_NOMEM; goto table_done; }
      mergeSetEntryText(pDepAnc, zDepOwned);
      mergeSetEntryText(pDepOurs, zDepOwned);
      mergeSetEntryText(pDepTheirs, zDepOwned);
      sqlite3_free(zDepOwned);
    }
    {
      struct TableEntry *pEntAnc =
          doltliteFindTableByName(aAnc, nAnc, pAncT->zName);
      struct TableEntry *pEntOurs =
          doltliteFindTableByName(aOurs, nOurs, pAncT->zName);
      struct TableEntry *pEntTheirs =
          doltliteFindTableByName(aTheirs, nTheirs, pAncT->zName);
      struct TableEntry *pEntBase = bBaseOurs ? pEntOurs
                                  : (bBaseTheirs ? pEntTheirs : pEntAnc);
      if( pEntAnc && pEntOurs && pEntTheirs && pEntBase ){
        pEntAnc->schemaHash = pEntBase->schemaHash;
        pEntOurs->schemaHash = pEntBase->schemaHash;
        pEntTheirs->schemaHash = pEntBase->schemaHash;
      }
    }

table_done:
    freeAddedColumns(azRenO, nRenO);
    freeAddedColumns(azRenT, nRenT);
    sqlite3_free(azUni);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

#endif /* DOLTLITE_PROLLY */
