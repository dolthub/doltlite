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
  return sqlite3_stricmp(schemaColumnDefinitionTail(pA->zDef),
                         schemaColumnDefinitionTail(pB->zDef))==0;
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

  if( strcmp(zOurs, zTheirs)==0 ) return 1;
  zOurTail = schemaGeneratedTailStart(zOurs);
  zTheirTail = schemaGeneratedTailStart(zTheirs);
  if( !zOurTail && !zTheirTail ) return 0;
  nOurs = zOurTail ? (int)(zOurTail-zOurs) : (int)strlen(zOurs);
  nTheirs = zTheirTail ? (int)(zTheirTail-zTheirs) : (int)strlen(zTheirs);
  while( nOurs>0 && isspace((unsigned char)zOurs[nOurs-1]) ) nOurs--;
  while( nTheirs>0 && isspace((unsigned char)zTheirs[nTheirs-1]) ) nTheirs--;
  return nOurs==nTheirs && sqlite3_strnicmp(zOurs, zTheirs, nOurs)==0;
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

static int schemaIrColumnsSame(
  const SchemaIr *pLeft,
  const SchemaIr *pRight,
  int ignoreChecks,
  int *pSame
){
  int i;
  *pSame = 0;
  if( pLeft->nCols!=pRight->nCols ) return SQLITE_OK;
  *pSame = 1;
  for(i=0; i<pLeft->nCols; i++){
    ParsedColumn *pRightCol = findColumn(
      pRight->aCols, pRight->nCols, pLeft->aCols[i].zName
    );
    if( !pRightCol ){ *pSame = 0; break; }
    if( ignoreChecks ){
      char *zLeft = schemaColumnWithoutChecks(pLeft->aCols[i].zDef);
      char *zRight = schemaColumnWithoutChecks(pRightCol->zDef);
      if( !zLeft || !zRight ){
        sqlite3_free(zLeft);
        sqlite3_free(zRight);
        return SQLITE_NOMEM;
      }
      if( strcmp(zLeft, zRight)!=0 ) *pSame = 0;
      sqlite3_free(zLeft);
      sqlite3_free(zRight);
    }else if( strcmp(pLeft->aCols[i].zDef, pRightCol->zDef)!=0 ){
      *pSame = 0;
    }
    if( !*pSame ) break;
  }
  return SQLITE_OK;
}

static int schemaConstraintModifyDeleteChoice(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  int *pChoice
){
  SchemaIr anc, ours, theirs;
  int sameAO = 0, sameAT = 0, sameOT = 0;
  int survivorSame = 0;
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
   && anc.hasCheck==ours.hasCheck && anc.hasCheck==theirs.hasCheck ){
    rc = schemaIrColumnsSame(&anc, &ours, 0, &sameAO);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&anc, &theirs, 0, &sameAT);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&ours, &theirs, 0, &sameOT);
    if( rc==SQLITE_OK && sameAO && sameAT && sameOT ){
      const SchemaIr *pSurv = ours.hasFk ? &ours : &theirs;
      const char *zAncFk = anc.zFkSig ? anc.zFkSig : "";
      const char *zSurvFk = pSurv->zFkSig ? pSurv->zFkSig : "";
      if( strcmp(zAncFk, zSurvFk)!=0 ){
        *pChoice = ours.hasFk ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
      }
      schemaIrClear(&anc);
      schemaIrClear(&ours);
      schemaIrClear(&theirs);
      return SQLITE_OK;
    }
  }

  if( rc==SQLITE_OK
   && anc.hasCheck && ours.hasCheck!=theirs.hasCheck
   && anc.hasFk==ours.hasFk && anc.hasFk==theirs.hasFk ){
    rc = schemaIrColumnsSame(&anc, &ours, 1, &sameAO);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&anc, &theirs, 1, &sameAT);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&ours, &theirs, 1, &sameOT);
    if( rc==SQLITE_OK ){
      const SchemaIr *pSurv = ours.hasCheck ? &ours : &theirs;
      rc = schemaIrColumnsSame(&anc, pSurv, 0, &survivorSame);
    }
    if( rc==SQLITE_OK && sameAO && sameAT && sameOT && !survivorSame ){
      *pChoice = ours.hasCheck ? SCHEMA_MERGE_OURS : SCHEMA_MERGE_THEIRS;
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
  if( rc!=SQLITE_OK || *pSchemaChoice!=SCHEMA_MERGE_DEFAULT ) return rc;

  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); freeColumns(aOurs, nOurs); return rc; }

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


#endif
