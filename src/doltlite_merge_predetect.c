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

#endif /* DOLTLITE_PROLLY */
