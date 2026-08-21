#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Merge pre-detection: refusals decided by reading the three schemas, before
** pass 1 walks a single entry. A merge whose correct answer this engine cannot
** represent has to be refused here, while the branches are still intact. */

/* The index's key columns, but only for the plain case: a parenthesised list of
** bare column names with nothing after it. An expression index, a partial
** index's WHERE clause, an explicit collation or a sort order all make two
** indexes over the same column different things, and this reports those as not
** plain rather than trying to compare them. Returns 0 unless every element is a
** bare name. */
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
    /* A nested paren is a function call or a parenthesised expression. */
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
      /* Anything but one bare name -- ASC, COLLATE, a quoted form -- is not a
      ** comparison this can make safely. */
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

/* Two indexes whose key is the same bare columns in the same order. */
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

/* One side added an index covering the same columns as an index that already
** existed. Dolt refuses to merge that -- "multiple indexes covering the same
** column set cannot be merged" -- because it cannot tell which of the two the
** merged table should keep, and keeping both would impose one side's
** uniqueness on the other. Only a merge that also has to reconcile the other
** branch's work reaches this; an addition on its own is the user's business. */
int mergePass1CheckDuplicateIndexColumns(MergePass1Ctx *c){
  int side, i, j;

  /* A merge only. Reverting, cherry-picking or rebasing a commit has one
  ** intended result, and the branch it replays onto asked for it, so there is
  ** no disagreement here to hand back. Refusing would also reject restoring a
  ** state the branch held before. */
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
        /* Only a merge that keeps both of them duplicates anything. A side that
        ** dropped the older index replaced it rather than doubling it, and the
        ** drop carries into the merge, so there is nothing to refuse. */
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

/* One side dropped a column while the other edited that same column's value in
** a row they both already had. The edit has nowhere to go in the merged table,
** and discarding it decides for the user which branch's intent wins. Dolt
** reports it as a conflict; refuse rather than resolve it here. Rows the other
** side added or removed are not affected, and neither is an edit to any other
** column. */
int mergePass1CheckRowEditOfDroppedColumn(MergePass1Ctx *c){
  int side, i, j;

  for(side=0; side<2; side++){
    SchemaEntry *aDrop = side ? c->aTheirsSchema : c->aOursSchema;
    int nDrop = side ? c->nTheirsSchema : c->nOursSchema;
    SchemaEntry *aEdit = side ? c->aOursSchema : c->aTheirsSchema;
    int nEdit = side ? c->nOursSchema : c->nTheirsSchema;
    struct TableEntry *aEditCat = side ? c->aOurs : c->aTheirs;
    int nEditCat = side ? c->nOurs : c->nTheirs;

    /* On replay, theirs is the patch being applied. Dolt protects non-NULL
    ** live values; a NULL or a column already absent from ours can be dropped. */
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
      /* The editing side must have left the columns alone, or this is a schema
      ** disagreement the schema merge already judges. */
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
        /* Renamed rather than dropped keeps its own behaviour. */
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

/* A trigger whose table the other side renamed or dropped. A trigger has to
** resolve when the schema loads -- unlike a view, which is only text until it
** is used -- so a merged catalog holding a trigger on a table that is no
** longer there cannot be loaded, and the merge reported the database as
** malformed.
**
** Dolt merges a rename and keeps the trigger pointing at the old name, which
** is a dangling trigger its own information_schema cannot read. That is not a
** result this engine can represent, so refuse and say why. A drop of the
** table is the same hole: refuse as dropped, not as renamed. */
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
      /* A trigger the ancestor already had went through the rename with the
      ** table on the renaming side, so the merge has a correct copy to keep.
      ** Only one added beside the rename has nowhere to land. */
      if( findSchemaEntry(c->aAncSchema, c->nAncSchema, aTrig[i].zName) ){
        continue;
      }
      if( !findSchemaEntry(c->aAncSchema, c->nAncSchema, zTable) ) continue;
      if( findSchemaEntry(aOther, nOther, zTable) ) continue;
      /* Gone from the other side under its ancestor name. A table of theirs the
      ** ancestor never had, whose columns are the ancestor table's columns, is
      ** that table under a new name. Dropping it and creating something
      ** unrelated looks the same from the names alone, so the columns decide
      ** whether this is a rename or a drop. */
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

/* An index one side added over a column the other side renamed. Nothing here
** retargets the index to the new name, and a merged catalog naming a column
** its table does not have cannot be loaded at all -- the merge used to report
** the database as corrupt. Refuse it instead, the way a primary-key change is
** refused, and say why.
**
** This is a deliberate divergence from Dolt, which merges these and keeps the
** index on the renamed column. */
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
