#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* CREATE TABLE SQL parsing, schema IR, and column-level schema merge. */

#define SCHEMA_IR_OTHER 0
#define SCHEMA_IR_FK    1
#define SCHEMA_IR_CHECK 2

typedef struct SchemaIr SchemaIr;
struct SchemaIr {
  ParsedColumn *aCols;
  int nCols;
  char *zFkSig;
  char *zCheckSig;
  int hasFk;
  int hasCheck;
};

static int schemaIrBuild(const char *zSql, SchemaIr *pIr);

int parseColumns(
  const char *zSql,
  ParsedColumn **ppCols, int *pnCols
){
  SchemaIr ir;
  int rc;

  *ppCols = 0;
  *pnCols = 0;
  rc = schemaIrBuild(zSql, &ir);
  if( rc!=SQLITE_OK ) return rc;
  *ppCols = ir.aCols;
  *pnCols = ir.nCols;
  sqlite3_free(ir.zFkSig);
  sqlite3_free(ir.zCheckSig);
  return SQLITE_OK;
}

void freeColumns(ParsedColumn *aCols, int nCols){
  int i;
  for(i=0; i<nCols; i++){
    sqlite3_free(aCols[i].zName);
    sqlite3_free(aCols[i].zDef);
  }
  sqlite3_free(aCols);
}

int parsedColumnIndexByName(
  ParsedColumn *aCols,
  int nCols,
  const char *zName
){
  int i;
  for(i=0; i<nCols; i++){
    if( aCols[i].zName && zName
     && sqlite3_stricmp(aCols[i].zName, zName)==0 ){
      return i;
    }
  }
  return -1;
}

static ParsedColumn *findColumn(ParsedColumn *aCols, int nCols, const char *zName){
  int i = parsedColumnIndexByName(aCols, nCols, zName);
  return i>=0 ? &aCols[i] : 0;
}

static int schemaGetToken(
  const char *z,
  const char *zEnd,
  int *pType,
  int *pnToken
){
  i64 n;
  if( z>=zEnd ) return SQLITE_CORRUPT;
  n = sqlite3GetToken((const u8*)z, pType);
  if( n<=0 || n>zEnd-z || *pType==TK_ILLEGAL ) return SQLITE_CORRUPT;
  *pnToken = (int)n;
  return SQLITE_OK;
}

static int schemaNextSignificantToken(
  const char *z,
  const char *zEnd,
  const char **pzToken,
  int *pType,
  int *pnToken
){
  int rc;
  while( z<zEnd ){
    rc = schemaGetToken(z, zEnd, pType, pnToken);
    if( rc!=SQLITE_OK ) return rc;
    if( *pType!=TK_SPACE && *pType!=TK_COMMENT ){
      *pzToken = z;
      return SQLITE_OK;
    }
    z += *pnToken;
  }
  return SQLITE_CORRUPT;
}

static const char *schemaSkipTrivia(const char *z, const char *zEnd){
  int type, n;
  while( z<zEnd && schemaGetToken(z, zEnd, &type, &n)==SQLITE_OK
      && (type==TK_SPACE || type==TK_COMMENT) ){
    z += n;
  }
  return z;
}

static int schemaTokensEquivalent(
  const char *zLeft,
  const char *zLeftEnd,
  const char *zRight,
  const char *zRightEnd
){
  while( 1 ){
    int leftType, leftLen;
    int rightType, rightLen;
    zLeft = schemaSkipTrivia(zLeft, zLeftEnd);
    zRight = schemaSkipTrivia(zRight, zRightEnd);
    if( zLeft==zLeftEnd || zRight==zRightEnd ){
      return zLeft==zLeftEnd && zRight==zRightEnd;
    }
    if( schemaGetToken(zLeft, zLeftEnd, &leftType, &leftLen)!=SQLITE_OK
     || schemaGetToken(zRight, zRightEnd, &rightType, &rightLen)!=SQLITE_OK
     || leftType!=rightType || leftLen!=rightLen ){
      return 0;
    }
    if( leftType==TK_STRING || leftType==TK_BLOB
     || leftType==TK_INTEGER || leftType==TK_FLOAT ){
      if( memcmp(zLeft, zRight, leftLen)!=0 ) return 0;
    }else if( sqlite3_strnicmp(zLeft, zRight, leftLen)!=0 ){
      return 0;
    }
    zLeft += leftLen;
    zRight += rightLen;
  }
}

static int schemaDefinitionsEquivalent(const char *zLeft, const char *zRight){
  return schemaTokensEquivalent(
      zLeft, zLeft + strlen(zLeft), zRight, zRight + strlen(zRight));
}

static int schemaSkipParenthesized(
  const char *z,
  const char *zEnd,
  const char **pzAfter
){
  int depth = 0;
  while( z<zEnd ){
    int type, n;
    int rc = schemaGetToken(z, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) return rc;
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      if( --depth==0 ){
        *pzAfter = z + n;
        return SQLITE_OK;
      }
      if( depth<0 ) return SQLITE_CORRUPT;
    }
    z += n;
  }
  return SQLITE_CORRUPT;
}

static int schemaSegmentIsTableConstraint(
  const char *s,
  const char *e,
  int *pIsConstraint
){
  const char *zToken;
  int type, n;
  int rc = schemaNextSignificantToken(s, e, &zToken, &type, &n);
  if( rc!=SQLITE_OK ) return rc;
  *pIsConstraint = type==TK_PRIMARY || type==TK_UNIQUE || type==TK_CHECK
                || type==TK_FOREIGN || type==TK_CONSTRAINT;
  return SQLITE_OK;
}

static const char *schemaColumnDefinitionTail(const char *zDef){
  const char *zEnd = zDef + strlen(zDef);
  const char *zToken = zDef;
  int type, n;
  if( schemaNextSignificantToken(zDef, zEnd, &zToken, &type, &n)!=SQLITE_OK ){
    return zEnd;
  }
  return schemaSkipTrivia(zToken + n, zEnd);
}

int parsedColumnDefinitionsMatch(
  const ParsedColumn *pA,
  const ParsedColumn *pB
){
  return schemaDefinitionsEquivalent(schemaColumnDefinitionTail(pA->zDef),
                                     schemaColumnDefinitionTail(pB->zDef));
}

static const char *schemaFindToken(
  const char *z,
  const char *zEnd,
  const char *zKw,
  int nKw
);

static int schemaTokenTextIs(const char *z, int n, const char *zText){
  int nText = (int)strlen(zText);
  return n==nText && sqlite3_strnicmp(z, zText, n)==0;
}

static const char *schemaGeneratedTailStart(const char *zDef){
  const char *zEnd = zDef + strlen(zDef);
  const char *zGenerated;
  const char *zAs;
  const char *p;
  int type, n;

  zGenerated = schemaFindToken(zDef, zEnd, "GENERATED", 9);
  if( zGenerated ){
    if( schemaGetToken(zGenerated, zEnd, &type, &n)!=SQLITE_OK ) return 0;
    p = schemaSkipTrivia(zGenerated + n, zEnd);
    if( p<zEnd && schemaGetToken(p, zEnd, &type, &n)==SQLITE_OK
     && type==TK_ALWAYS ){
      p = schemaSkipTrivia(p + n, zEnd);
    }
    if( p>=zEnd || schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK
     || type!=TK_AS ) return 0;
    zAs = p;
  }else{
    zAs = schemaFindToken(zDef, zEnd, "AS", 2);
    if( !zAs ) return 0;
  }

  if( schemaGetToken(zAs, zEnd, &type, &n)!=SQLITE_OK ) return 0;
  p = schemaSkipTrivia(zAs + n, zEnd);
  if( p>=zEnd || schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK
   || type!=TK_LP ) return 0;
  if( schemaSkipParenthesized(p, zEnd, &p)!=SQLITE_OK ) return 0;
  p = schemaSkipTrivia(p, zEnd);
  if( p<zEnd ){
    if( schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK ) return 0;
    if( type==TK_VIRTUAL || schemaTokenTextIs(p, n, "STORED") ){
      p += n;
    }
  }
  p = schemaSkipTrivia(p, zEnd);
  return p==zEnd ? (zGenerated ? zGenerated : zAs) : 0;
}

static int schemaColumnsMergeEquivalent(
  const char *zOurs,
  const char *zTheirs
){
  const char *zOurTail;
  const char *zTheirTail;
  int nOurs;
  int nTheirs;

  if( schemaDefinitionsEquivalent(zOurs, zTheirs) ) return 1;
  zOurTail = schemaGeneratedTailStart(zOurs);
  zTheirTail = schemaGeneratedTailStart(zTheirs);
  if( !zOurTail && !zTheirTail ) return 0;
  nOurs = zOurTail ? (int)(zOurTail-zOurs) : (int)strlen(zOurs);
  nTheirs = zTheirTail ? (int)(zTheirTail-zTheirs) : (int)strlen(zTheirs);
  return schemaTokensEquivalent(
      zOurs, zOurs + nOurs, zTheirs, zTheirs + nTheirs);
}

static const char *schemaFindToken(
  const char *z,
  const char *zEnd,
  const char *zKw,
  int nKw
){
  const char *p = z;
  if( !z || !zEnd || zEnd<=z || nKw<=0 ) return 0;
  while( p<zEnd ){
    int type, n;
    if( schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK ) return 0;
    if( type!=TK_STRING && type!=TK_BLOB && type!=TK_COMMENT
     && type!=TK_SPACE && n==nKw && sqlite3_strnicmp(p, zKw, nKw)==0 ){
      return p;
    }
    p += n;
  }
  return 0;
}

static int schemaAppendSig(char **pzSig, const char *zText, int nText){
  char *zNew;
  int nOld = *pzSig ? (int)strlen(*pzSig) : 0;
  if( nText<0 ) nText = (int)strlen(zText);
  if( nText<=0 ) return SQLITE_OK;
  zNew = sqlite3_realloc(*pzSig, nOld + nText + 2);
  if( !zNew ) return SQLITE_NOMEM;
  if( nOld ) zNew[nOld++] = '\n';
  memcpy(zNew + nOld, zText, nText);
  zNew[nOld + nText] = 0;
  *pzSig = zNew;
  return SQLITE_OK;
}

static int schemaConstraintKind(const char *s, int len){
  const char *e = s + len;
  const char *p;
  int type, n;
  if( schemaNextSignificantToken(s, e, &p, &type, &n)!=SQLITE_OK ){
    return SCHEMA_IR_OTHER;
  }
  if( type==TK_CONSTRAINT ){
    if( schemaNextSignificantToken(p+n, e, &p, &type, &n)!=SQLITE_OK ){
      return SCHEMA_IR_OTHER;
    }
    if( schemaNextSignificantToken(p+n, e, &p, &type, &n)!=SQLITE_OK ){
      return SCHEMA_IR_OTHER;
    }
  }
  if( type==TK_FOREIGN ) return SCHEMA_IR_FK;
  if( type==TK_CHECK ) return SCHEMA_IR_CHECK;
  if( schemaFindToken(p, e, "REFERENCES", 10) ) return SCHEMA_IR_FK;
  if( schemaFindToken(p, e, "CHECK", 5) ) return SCHEMA_IR_CHECK;
  return SCHEMA_IR_OTHER;
}

static char *schemaColumnWithoutChecks(const char *zDef){
  int n = (int)strlen(zDef);
  char *zOut = sqlite3_malloc(n + 1);
  const char *z = zDef;
  const char *zEnd = zDef + n;
  char *zWrite = zOut;
  if( !zOut ) return 0;
  while( z<zEnd ){
    const char *zKw = schemaFindToken(z, zEnd, "CHECK", 5);
    if( !zKw ){
      memcpy(zWrite, z, (size_t)(zEnd - z));
      zWrite += (zEnd - z);
      break;
    }
    if( zKw>z ){
      memcpy(zWrite, z, (size_t)(zKw - z));
      zWrite += (zKw - z);
    }
    z = zKw + 5;
    z = schemaSkipTrivia(z, zEnd);
    if( z<zEnd ){
      int type, nToken;
      const char *zAfter;
      if( schemaGetToken(z, zEnd, &type, &nToken)==SQLITE_OK
       && type==TK_LP
       && schemaSkipParenthesized(z, zEnd, &zAfter)==SQLITE_OK ){
        z = zAfter;
      }
    }
  }
  while( zWrite>zOut && isspace((unsigned char)zWrite[-1]) ) zWrite--;
  *zWrite = 0;
  return zOut;
}

static void schemaIrClear(SchemaIr *pIr){
  assert( pIr!=0 );
  freeColumns(pIr->aCols, pIr->nCols);
  sqlite3_free(pIr->zFkSig);
  sqlite3_free(pIr->zCheckSig);
  memset(pIr, 0, sizeof(*pIr));
}

static int schemaIrNoteColumnConstraints(SchemaIr *pIr, const char *zDef){
  const char *zEnd;
  const char *zFk;
  const char *zCk;
  int rc;
  assert( pIr!=0 && zDef!=0 );
  zEnd = zDef + strlen(zDef);
  zFk = schemaFindToken(zDef, zEnd, "REFERENCES", 10);
  zCk = zDef;
  if( zFk ){
    pIr->hasFk = 1;
    rc = schemaAppendSig(&pIr->zFkSig, zFk, (int)(zEnd - zFk));
    if( rc!=SQLITE_OK ) return rc;
  }
  while( (zCk = schemaFindToken(zCk, zEnd, "CHECK", 5))!=0 ){
    const char *zStart = zCk;
    const char *z = zCk + 5;
    pIr->hasCheck = 1;
    z = schemaSkipTrivia(z, zEnd);
    if( z<zEnd ){
      int type, nToken;
      const char *zAfter;
      rc = schemaGetToken(z, zEnd, &type, &nToken);
      if( rc!=SQLITE_OK ) return rc;
      if( type==TK_LP ){
        rc = schemaSkipParenthesized(z, zEnd, &zAfter);
        if( rc!=SQLITE_OK ) return rc;
        z = zAfter;
      }
    }
    rc = schemaAppendSig(&pIr->zCheckSig, zStart, (int)(z - zStart));
    if( rc!=SQLITE_OK ) return rc;
    zCk = z;
  }
  return SQLITE_OK;
}

static int schemaIrAddSegment(
  SchemaIr *pIr,
  int *pnAlloc,
  const char *s,
  const char *e
){
  const char *zNameToken;
  char *zDef;
  char *zName;
  int isConstraint;
  int nameType;
  int nName;
  int len;
  int rc;
  int i;

  while( s<e && isspace((unsigned char)*s) ) s++;
  while( e>s && isspace((unsigned char)e[-1]) ) e--;
  if( s==e ) return SQLITE_OK;
  rc = schemaSegmentIsTableConstraint(s, e, &isConstraint);
  if( rc!=SQLITE_OK ) return rc;
  len = (int)(e - s);
  if( isConstraint ){
    int kind = schemaConstraintKind(s, len);
    if( kind==SCHEMA_IR_FK ){
      pIr->hasFk = 1;
      return schemaAppendSig(&pIr->zFkSig, s, len);
    }
    if( kind==SCHEMA_IR_CHECK ){
      pIr->hasCheck = 1;
      return schemaAppendSig(&pIr->zCheckSig, s, len);
    }
    return SQLITE_OK;
  }

  rc = schemaNextSignificantToken(
      s, e, &zNameToken, &nameType, &nName);
  if( rc!=SQLITE_OK ) return rc;
  zDef = sqlite3_malloc(len + 1);
  zName = sqlite3_malloc(nName + 1);
  if( !zDef || !zName ){
    sqlite3_free(zDef);
    sqlite3_free(zName);
    return SQLITE_NOMEM;
  }
  memcpy(zDef, s, len);
  zDef[len] = 0;
  memcpy(zName, zNameToken, nName);
  zName[nName] = 0;
  sqlite3Dequote(zName);
  for(i=0; zName[i]; i++){
    zName[i] = (char)tolower((unsigned char)zName[i]);
  }
  rc = DOLTLITE_GROW_ARRAY(&pIr->aCols, pnAlloc, pIr->nCols+1, 8);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zDef);
    sqlite3_free(zName);
    return rc;
  }
  pIr->aCols[pIr->nCols].zName = zName;
  pIr->aCols[pIr->nCols].zDef = zDef;
  pIr->nCols++;
  return schemaIrNoteColumnConstraints(pIr, zDef);
}

static int schemaIrBuild(const char *zSql, SchemaIr *pIr){
  const char *p;
  const char *zEnd;
  const char *segStart;
  int depth = 0;
  int nAlloc = 0;
  int rc;
  assert( pIr!=0 );

  memset(pIr, 0, sizeof(*pIr));
  if( !zSql ) return SQLITE_OK;
  p = zSql;
  zEnd = zSql + strlen(zSql);
  while( p<zEnd ){
    int type, n;
    rc = schemaGetToken(p, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) goto schema_ir_build_error;
    p += n;
    if( type==TK_LP ){
      depth = 1;
      break;
    }
  }
  if( depth==0 ){
    rc = SQLITE_CORRUPT;
    goto schema_ir_build_error;
  }

  segStart = p;
  while( p<zEnd ){
    int type, n;
    rc = schemaGetToken(p, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) goto schema_ir_build_error;
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      depth--;
      if( depth==0 ){
        rc = schemaIrAddSegment(pIr, &nAlloc, segStart, p);
        if( rc!=SQLITE_OK ) goto schema_ir_build_error;
        return SQLITE_OK;
      }
      if( depth<0 ){
        rc = SQLITE_CORRUPT;
        goto schema_ir_build_error;
      }
    }else if( type==TK_COMMA && depth==1 ){
      rc = schemaIrAddSegment(pIr, &nAlloc, segStart, p);
      if( rc!=SQLITE_OK ) goto schema_ir_build_error;
      segStart = p + n;
    }
    p += n;
  }
  rc = SQLITE_CORRUPT;

schema_ir_build_error:
  schemaIrClear(pIr);
  return rc;
}

static int schemaIrAncestorColumnsSame(
  const SchemaIr *pAncestor,
  const SchemaIr *pSide,
  int ignoreChecks,
  int *pSame
){
  int i;
  *pSame = 1;
  for(i=0; i<pAncestor->nCols; i++){
    ParsedColumn *pSideCol = findColumn(
        pSide->aCols, pSide->nCols, pAncestor->aCols[i].zName);
    if( !pSideCol ){
      *pSame = 0;
      break;
    }
    if( ignoreChecks ){
      char *zAncestor = schemaColumnWithoutChecks(pAncestor->aCols[i].zDef);
      char *zSide = schemaColumnWithoutChecks(pSideCol->zDef);
      if( !zAncestor || !zSide ){
        sqlite3_free(zAncestor);
        sqlite3_free(zSide);
        return SQLITE_NOMEM;
      }
      if( !schemaDefinitionsEquivalent(zAncestor, zSide) ) *pSame = 0;
      sqlite3_free(zAncestor);
      sqlite3_free(zSide);
    }else if( !schemaDefinitionsEquivalent(
                 pAncestor->aCols[i].zDef, pSideCol->zDef) ){
      *pSame = 0;
    }
    if( !*pSame ) break;
  }
  return SQLITE_OK;
}

static int schemaIrSignaturesSame(const char *zLeft, const char *zRight){
  const char *zL = zLeft ? zLeft : "";
  const char *zR = zRight ? zRight : "";
  return schemaDefinitionsEquivalent(zL, zR);
}

static int schemaConstraintModifyDeleteChoice(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  int *pChoice
){
  SchemaIr anc, ours, theirs;
  int sameAO = 0, sameAT = 0;
  int rc;
  *pChoice = SCHEMA_MERGE_DEFAULT;

  rc = schemaIrBuild(zAncSql, &anc);
  if( rc!=SQLITE_OK ) return rc;
  rc = schemaIrBuild(zOursSql, &ours);
  if( rc!=SQLITE_OK ){ schemaIrClear(&anc); return rc; }
  rc = schemaIrBuild(zTheirsSql, &theirs);
  if( rc!=SQLITE_OK ){
    schemaIrClear(&anc);
    schemaIrClear(&ours);
    return rc;
  }

  if( anc.hasFk && ours.hasFk!=theirs.hasFk
   && schemaIrSignaturesSame(anc.zCheckSig, ours.zCheckSig)
   && schemaIrSignaturesSame(anc.zCheckSig, theirs.zCheckSig) ){
    rc = schemaIrAncestorColumnsSame(&anc, &ours, 0, &sameAO);
    if( rc==SQLITE_OK ){
      rc = schemaIrAncestorColumnsSame(&anc, &theirs, 0, &sameAT);
    }
    if( rc==SQLITE_OK && sameAO && sameAT ){
      *pChoice = ours.hasFk ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
      schemaIrClear(&anc);
      schemaIrClear(&ours);
      schemaIrClear(&theirs);
      return SQLITE_OK;
    }
  }

  if( rc==SQLITE_OK
   && anc.hasCheck && ours.hasCheck!=theirs.hasCheck
   && schemaIrSignaturesSame(anc.zFkSig, ours.zFkSig)
   && schemaIrSignaturesSame(anc.zFkSig, theirs.zFkSig) ){
    const SchemaIr *pSurvivor = ours.hasCheck ? &ours : &theirs;
    rc = schemaIrAncestorColumnsSame(&anc, &ours, 1, &sameAO);
    if( rc==SQLITE_OK ){
      rc = schemaIrAncestorColumnsSame(&anc, &theirs, 1, &sameAT);
    }
    if( rc==SQLITE_OK && sameAO && sameAT ){
      if( schemaIrSignaturesSame(anc.zCheckSig, pSurvivor->zCheckSig) ){
        *pChoice = ours.hasCheck ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
      }else{
        *pChoice = ours.hasCheck ? SCHEMA_MERGE_OURS : SCHEMA_MERGE_THEIRS;
      }
    }
  }

  schemaIrClear(&anc);
  schemaIrClear(&ours);
  schemaIrClear(&theirs);
  return rc;
}

int trySchemaColumnMerge(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  char ***ppAddCols, int *pnAddCols,
  int *pSchemaChoice,
  int *pResolvedDivergence,
  char **pzErrDetail
){
  ParsedColumn *aAnc=0, *aOurs=0, *aTheirs=0;
  int nAnc=0, nOurs=0, nTheirs=0;
  int rc;
  char **azAdd = 0;
  int nAdd = 0, nAddAlloc = 0;
  int i;

  *ppAddCols = 0;
  *pnAddCols = 0;
  *pSchemaChoice = SCHEMA_MERGE_DEFAULT;
  *pResolvedDivergence = 0;

  rc = schemaConstraintModifyDeleteChoice(
      zAncSql, zOursSql, zTheirsSql, pSchemaChoice);
  if( rc!=SQLITE_OK ) return rc;

  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); freeColumns(aOurs, nOurs); return rc; }

  if( *pSchemaChoice!=SCHEMA_MERGE_DEFAULT ){
    ParsedColumn *aSelected = *pSchemaChoice==SCHEMA_MERGE_OURS
                            ? aOurs : aTheirs;
    ParsedColumn *aOther = *pSchemaChoice==SCHEMA_MERGE_OURS
                         ? aTheirs : aOurs;
    int nSelected = *pSchemaChoice==SCHEMA_MERGE_OURS ? nOurs : nTheirs;
    int nOther = *pSchemaChoice==SCHEMA_MERGE_OURS ? nTheirs : nOurs;
    for(i=0; i<nOther; i++){
      ParsedColumn *pSelected;
      if( findColumn(aAnc, nAnc, aOther[i].zName) ) continue;
      pSelected = findColumn(aSelected, nSelected, aOther[i].zName);
      if( pSelected ){
        if( !schemaColumnsMergeEquivalent(
                pSelected->zDef, aOther[i].zDef) ){
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aOther[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        if( strcmp(pSelected->zDef, aOther[i].zDef)!=0 ){
          *pResolvedDivergence = 1;
        }
        continue;
      }
      rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
      if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
      azAdd[nAdd] = sqlite3_mprintf("%s", aOther[i].zDef);
      if( !azAdd[nAdd] ){
        rc = SQLITE_NOMEM;
        goto schema_merge_cleanup;
      }
      nAdd++;
    }
    goto schema_merge_done;
  }

  for(i=0; i<nTheirs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aTheirs[i].zName);
    if( !ancCol ){

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){

        if( !schemaColumnsMergeEquivalent(ourCol->zDef, aTheirs[i].zDef) ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){
          *pResolvedDivergence = 1;
        }

      }else if( i<nAnc
             && sqlite3_stricmp(aTheirs[i].zName, aAnc[i].zName)!=0
             && !findColumn(aTheirs, nTheirs, aAnc[i].zName)
             && parsedColumnDefinitionsMatch(&aTheirs[i], &aAnc[i]) ){
        ParsedColumn *ourAncestor = findColumn(
            aOurs, nOurs, aAnc[i].zName);
        if( ourAncestor && strcmp(ourAncestor->zDef, aAnc[i].zDef)==0 ){
          *pSchemaChoice = SCHEMA_MERGE_THEIRS;
        }else{
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' renamed on one branch and modified on another",
              aAnc[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }else{

        rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
        if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
        azAdd[nAdd] = sqlite3_mprintf("%s", aTheirs[i].zDef);
        nAdd++;
      }
    }else{

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){
        int ancToTheirs = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        int ancToOurs = strcmp(ancCol->zDef, ourCol->zDef)!=0;
        if( ancToTheirs && ancToOurs ){

          if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){

            if( pzErrDetail ){
              *pzErrDetail = sqlite3_mprintf(
                "both branches modified column '%s' differently",
                aTheirs[i].zName);
            }
            rc = SQLITE_ERROR;
            goto schema_merge_cleanup;
          }

        }
      }else{

        int theirsModified = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        if( theirsModified ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }
    }
  }

  for(i=0; i<nOurs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aOurs[i].zName);
    if( ancCol ){

      ParsedColumn *theirCol = findColumn(aTheirs, nTheirs, aOurs[i].zName);
      if( !theirCol ){

        int oursModified = strcmp(ancCol->zDef, aOurs[i].zDef)!=0;
        if( oursModified ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aOurs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }
    }

  }

  if( *pSchemaChoice==SCHEMA_MERGE_THEIRS ){
    int j;
    for(j=0; j<nAdd; j++) sqlite3_free(azAdd[j]);
    sqlite3_free(azAdd);
    azAdd = 0;
    nAdd = 0;
    nAddAlloc = 0;
    for(i=0; i<nOurs; i++){
      if( !findColumn(aAnc, nAnc, aOurs[i].zName)
       && !findColumn(aTheirs, nTheirs, aOurs[i].zName) ){
        rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
        if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
        azAdd[nAdd] = sqlite3_mprintf("%s", aOurs[i].zDef);
        if( !azAdd[nAdd] ){
          rc = SQLITE_NOMEM;
          goto schema_merge_cleanup;
        }
        nAdd++;
      }
    }
  }

schema_merge_done:
  *ppAddCols = azAdd;
  *pnAddCols = nAdd;
  azAdd = 0; nAdd = 0;

schema_merge_cleanup:
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  if( rc!=SQLITE_OK ){
    { int j; for(j=0;j<nAdd;j++) sqlite3_free(azAdd[j]); }
    sqlite3_free(azAdd);
  }
  return rc;
}

int doltliteTableSchemaConflictDetail(
  const char *zAncestorSql,
  const char *zOurSql,
  const char *zTheirSql,
  char **pzDetail
){
  char **azAdd = 0;
  int nAdd = 0;
  int schemaChoice = SCHEMA_MERGE_DEFAULT;
  int resolvedDivergence = 0;
  int i, rc;

  *pzDetail = 0;
  if( !zAncestorSql || !zOurSql || !zTheirSql ) return SQLITE_OK;
  rc = trySchemaColumnMerge(zAncestorSql, zOurSql, zTheirSql,
                            &azAdd, &nAdd, &schemaChoice,
                            &resolvedDivergence, pzDetail);
  for(i=0; i<nAdd; i++) sqlite3_free(azAdd[i]);
  sqlite3_free(azAdd);
  if( rc==SQLITE_ERROR ) return SQLITE_OK;
  return rc;
}


/* Column defaults for a table's declared columns, evaluated once.
**
** A row written before ADD COLUMN stops short of the new column, and reads
** materialize the declared default for the missing field. Rewriting such a
** row into a wider layout has to keep that promise: once any later column
** is present the record physically covers the earlier slot, so leaving it
** NULL replaces the default with a NULL the table never held. Defaults for
** ADD COLUMN are constants, so evaluating the stored text in the scratch
** database that already holds the schema yields the value to store. */
void mergeColDefaultsFree(MergeColDefaults *p){
  int i;
  if( p->apOwned ){
    for(i=0; i<p->nCol; i++) sqlite3_free(p->apOwned[i]);
    sqlite3_free(p->apOwned);
  }
  sqlite3_free(p->aVal);
  memset(p, 0, sizeof(*p));
}

int mergeColDefaultsLoad(
  const char *zSql,
  const char *zTable,
  MergeColDefaults *pOut
){
  sqlite3 *tmp = 0;
  sqlite3_stmt *pStmt = 0;
  char *zQuery = 0;
  int nCol = 0;
  int rc;

  memset(pOut, 0, sizeof(*pOut));
  rc = sqlite3_open(":memory:", &tmp);
  if( rc!=SQLITE_OK ) goto done;
  rc = sqlite3_exec(tmp, zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto done;

  zQuery = sqlite3_mprintf(
      "SELECT cid, dflt_value FROM pragma_table_info(%Q) ORDER BY cid",
      zTable);
  if( !zQuery ){ rc = SQLITE_NOMEM; goto done; }
  rc = sqlite3_prepare_v2(tmp, zQuery, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) goto done;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ) nCol++;
  if( rc!=SQLITE_DONE ) goto done;
  sqlite3_reset(pStmt);

  if( nCol>0 ){
    pOut->aVal = sqlite3_malloc(nCol * (int)sizeof(DoltliteSerialValue));
    pOut->apOwned = sqlite3_malloc(nCol * (int)sizeof(u8*));
    if( !pOut->aVal || !pOut->apOwned ){ rc = SQLITE_NOMEM; goto done; }
    memset(pOut->aVal, 0, nCol * (int)sizeof(DoltliteSerialValue));
    memset(pOut->apOwned, 0, nCol * (int)sizeof(u8*));
    pOut->nCol = nCol;
    do{
      pOut->aVal[--nCol].eType = SQLITE_NULL;
    }while( nCol>0 );
  }

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    int cid = sqlite3_column_int(pStmt, 0);
    const char *zDflt = (const char*)sqlite3_column_text(pStmt, 1);
    sqlite3_stmt *pEval = 0;
    char *zEval;
    if( !zDflt || !zDflt[0] || cid<0 || cid>=pOut->nCol ) continue;
    zEval = sqlite3_mprintf("SELECT %s", zDflt);
    if( !zEval ){ rc = SQLITE_NOMEM; goto done; }
    if( sqlite3_prepare_v2(tmp, zEval, -1, &pEval, 0)==SQLITE_OK
     && sqlite3_step(pEval)==SQLITE_ROW ){
      DoltliteSerialValue *m = &pOut->aVal[cid];
      switch( sqlite3_column_type(pEval, 0) ){
        case SQLITE_INTEGER:
          m->eType = SQLITE_INTEGER;
          m->i = sqlite3_column_int64(pEval, 0);
          break;
        case SQLITE_FLOAT:
          m->eType = SQLITE_FLOAT;
          m->r = sqlite3_column_double(pEval, 0);
          break;
        case SQLITE_TEXT:
        case SQLITE_BLOB: {
          int isText = sqlite3_column_type(pEval, 0)==SQLITE_TEXT;
          const void *p = isText
              ? (const void*)sqlite3_column_text(pEval, 0)
              : sqlite3_column_blob(pEval, 0);
          int n = sqlite3_column_bytes(pEval, 0);
          u8 *pCopy = 0;
          if( n>0 ){
            pCopy = sqlite3_malloc(n);
            if( !pCopy ){
              sqlite3_finalize(pEval);
              sqlite3_free(zEval);
              rc = SQLITE_NOMEM;
              goto done;
            }
            memcpy(pCopy, p, n);
          }
          pOut->apOwned[cid] = pCopy;
          m->eType = isText ? SQLITE_TEXT : SQLITE_BLOB;
          m->p = pCopy;
          m->n = n;
          break;
        }
        default:
          break;
      }
    }
    sqlite3_finalize(pEval);
    sqlite3_free(zEval);
  }
  rc = rc==SQLITE_DONE ? SQLITE_OK : rc;

done:
  if( pStmt ) sqlite3_finalize(pStmt);
  sqlite3_free(zQuery);
  if( tmp ) sqlite3_close(tmp);
  if( rc!=SQLITE_OK ) mergeColDefaultsFree(pOut);
  return rc;
}


/* Rewrite the tree at pTheirsRoot, whose records follow zTheirsSql, into the
** merged layout described by zOursSql. pOursRoot is the other side of the
** merge, read only to tell rows that side also holds from rows this one
** introduces. Either side of a merge can be the one converted. */
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
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);
  ParsedColumn *aAnc = 0, *aOurs = 0, *aTheirs = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0;
  int *aMap = 0;
  int nMerged;
  int nDropped = 0;
  ProllyMutMap mm;
  int mmInit = 0;
  ProllyCursor cur;
  int curInit = 0;
  int isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  MergeColDefaults oursDefaults;
  MergeColDefaults theirsDefaults;
  ProllyCursor oursCur;
  int oursCurInit = 0;
  int rc, res, j;

  memset(&oursDefaults, 0, sizeof(oursDefaults));
  memset(&theirsDefaults, 0, sizeof(theirsDefaults));

  memset(pOutRoot, 0, sizeof(*pOutRoot));
  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){
    freeColumns(aAnc, nAnc);
    freeColumns(aOurs, nOurs);
    return rc;
  }

  nMerged = nOurs;
  aMap = sqlite3_malloc((nTheirs>0 ? nTheirs : 1) * (int)sizeof(int));
  if( !aMap ){ rc = SQLITE_NOMEM; goto done; }
  for(j=0; j<nTheirs; j++){
    int found = parsedColumnIndexByName(
        aOurs, nOurs, aTheirs[j].zName);
    int bInAnc = 0;
    if( found<0 ){
      int ai = parsedColumnIndexByName(
          aAnc, nAnc, aTheirs[j].zName);
      if( ai<0 && j<nAnc
       && sqlite3_stricmp(aTheirs[j].zName, aAnc[j].zName)!=0
       && parsedColumnIndexByName(aTheirs, nTheirs, aAnc[j].zName)<0
       && parsedColumnDefinitionsMatch(&aTheirs[j], &aAnc[j]) ){
        ai = j;
      }
      if( ai>=0 ){
        bInAnc = 1;
        found = parsedColumnIndexByName(
            aOurs, nOurs, aAnc[ai].zName);
        if( found<0 && ai<nOurs
         && sqlite3_stricmp(aOurs[ai].zName, aAnc[ai].zName)!=0
         && parsedColumnIndexByName(aOurs, nOurs, aAnc[ai].zName)<0
         && parsedColumnDefinitionsMatch(&aOurs[ai], &aAnc[ai]) ){
          found = ai;
        }
      }
    }
    if( found>=0 ){
      aMap[j] = found;
    }else if( bInAnc ){
      /* The ancestor had this column and the merged layout does not, so the
      ** merged side dropped it. Appending it as a new column instead would
      ** resurrect dropped data as a trailing field the schema cannot name. */
      aMap[j] = -1;
      nDropped++;
    }else{
      aMap[j] = nMerged++;
    }
  }
  if( nMerged > DOLTLITE_MAX_RECORD_FIELDS ){ rc = SQLITE_ERROR; goto done; }

  /* Records already sit at their merged positions, so the existing tree is
  ** the answer. Trailing columns the merged layout adds read as absent from a
  ** short record, which is what a rewrite would store anyway. */
  if( nDropped==0 ){
    int bSamePositions = 1;
    for(j=0; j<nTheirs; j++){
      if( aMap[j]!=j ){ bSamePositions = 0; break; }
    }
    if( bSamePositions ){
      memcpy(pOutRoot, pTheirsRoot, sizeof(*pOutRoot));
      goto done;
    }
  }

  /* Slots this side does not supply take the declared default of whichever
  ** schema owns them: ours for the columns we keep, theirs for the ones
  ** their side appended. */
  rc = mergeColDefaultsLoad(zOursSql, zTable, &oursDefaults);
  if( rc!=SQLITE_OK ) goto done;
  rc = mergeColDefaultsLoad(zTheirsSql, zTable, &theirsDefaults);
  if( rc!=SQLITE_OK ) goto done;

  rc = prollyMutMapInit(&mm, (u8)isIntKey);
  if( rc!=SQLITE_OK ) goto done;
  mmInit = 1;

  if( !prollyHashIsEmpty(pOursRoot) ){
    prollyCursorInit(&oursCur, cs, cache, pOursRoot, flags);
    oursCurInit = 1;
  }

  prollyCursorInit(&cur, cs, cache, pTheirsRoot, flags);
  curInit = 1;
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK ) goto done;

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0; int nVal = 0;
    const u8 *pKey = 0; int nKey = 0; i64 intKey = 0;
    DoltliteRecordInfo info;
    DoltliteSerialValue aMem[DOLTLITE_MAX_RECORD_FIELDS];
    int nEmit = 0, k;
    int rowOnlyTheirs;
    u8 *pNew = 0; int nNew = 0;

    prollyCursorValue(&cur, &pVal, &nVal);
    if( isIntKey ){
      intKey = prollyCursorIntKey(&cur);
    }else{
      prollyCursorKey(&cur, &pKey, &nKey);
    }

    /* Defaults belong to rows only their side has: those are inserts, and
    ** the columns we added never applied to them, so they read as declared.
    ** A row we also hold is about to be merged cell by cell against the
    ** ancestor, and filling our column there would present their untouched
    ** column as a change they made -- turning a clean merge into a
    ** conflict. Those keep the empty slot the merge reads as "unchanged". */
    rowOnlyTheirs = 0;
    if( oursCurInit ){
      int oursRes = 0;
      if( isIntKey ){
        rc = prollyCursorSeekInt(&oursCur, intKey, &oursRes);
      }else{
        rc = prollyCursorSeekBlob(&oursCur, pKey, nKey, &oursRes);
      }
      if( rc!=SQLITE_OK ) goto done;
      rowOnlyTheirs = !(oursRes==0 && prollyCursorIsValid(&oursCur));
    }else{
      rowOnlyTheirs = 1;
    }

    doltliteParseRecord(pVal, nVal, &info);
    for(k=0; k<nMerged; k++){
      memset(&aMem[k], 0, sizeof(aMem[k]));
      aMem[k].eType = SQLITE_NULL;
      if( rowOnlyTheirs && k<oursDefaults.nCol ){
        aMem[k] = oursDefaults.aVal[k];
      }
    }
    if( rowOnlyTheirs ){
      for(j=0; j<nTheirs; j++){
        if( aMap[j]>=nOurs && j<theirsDefaults.nCol ){
          aMem[aMap[j]] = theirsDefaults.aVal[j];
        }
      }
    }
    for(j=0; j<info.nField && j<nTheirs; j++){
      int tgt = aMap[j];
      int st = info.aType[j];
      const u8 *body = pVal + info.aOffset[j];
      DoltliteSerialValue *m;
      if( tgt<0 ) continue;
      m = &aMem[tgt];
      memset(m, 0, sizeof(*m));
      m->eType = SQLITE_NULL;
      if( st==0 ){
        m->eType = SQLITE_NULL;
        if( rowOnlyTheirs && tgt+1>nEmit ) nEmit = tgt+1;
      }else if( dlSerialIsInt(st) ){
        m->eType = SQLITE_INTEGER;
        if( st==8 ) m->i = 0;
        else if( st==9 ) m->i = 1;
        else m->i = dlReadIntBytes(body, dlSerialTypeLen((u64)st));
      }else if( st==7 ){
        u64 bits = (u64)dlReadIntBytes(body, 8);
        double d;
        memcpy(&d, &bits, sizeof(d));
        m->eType = SQLITE_FLOAT;
        m->r = d;
      }else if( st>=12 ){
        m->eType = (st & 1) ? SQLITE_TEXT : SQLITE_BLOB;
        m->p = body;
        m->n = dlSerialTypeLen((u64)st);
      }
      if( m->eType!=SQLITE_NULL && tgt+1>nEmit ) nEmit = tgt+1;
    }
    for(k=0; k<nMerged; k++){
      if( aMem[k].eType!=SQLITE_NULL && k+1>nEmit ) nEmit = k+1;
    }

    if( nEmit>0 ){
      pNew = doltliteBuildRecord(aMem, nEmit, &nNew);
      if( !pNew ){ rc = SQLITE_NOMEM; goto done; }
    }
    rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pNew, nNew);
    sqlite3_free(pNew);
    if( rc!=SQLITE_OK ) goto done;

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) goto done;
  }

  {
    ProllyMutator mut;
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    memset(&mut.oldRoot, 0, sizeof(mut.oldRoot));
    mut.pEdits = &mm;
    mut.flags = flags;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ) memcpy(pOutRoot, &mut.newRoot, sizeof(ProllyHash));
  }

done:
  mergeColDefaultsFree(&oursDefaults);
  mergeColDefaultsFree(&theirsDefaults);
  if( oursCurInit ) prollyCursorClose(&oursCur);
  if( curInit ) prollyCursorClose(&cur);
  if( mmInit ) prollyMutMapFree(&mm);
  sqlite3_free(aMap);
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  return rc;
}

#endif
